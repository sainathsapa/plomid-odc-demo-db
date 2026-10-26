//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
//
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! `EXPLAIN` (plain and JSON) and the table statistics it reports.
//!
//! Row counts and widths are estimates derived from the catalog and the
//! current snapshot; the output stays stable enough for tests to assert on.

use crate::catalog_fn::is_aggregate_function;
use crate::error::SqlResult;
use crate::hybrid;
use plomid_core::PlomidError;
use plomid_sql::Catalog;
use plomid_sql::ColumnType;
use plomid_sql::Expression;
use plomid_sql::FromClause;
use plomid_sql::GroupByClause;
use plomid_sql::InMemoryCatalog;
use plomid_sql::QueryResult;
use plomid_sql::SelectTarget;
use plomid_sql::SetOpKind;
use plomid_sql::Statement;
use plomid_sql::TableSchema;
use plomid_sql::Value;
use plomid_txn::StorageEngine;

use super::index_bounds_for_predicate;
use super::select::expression_has_aggregate;

/// Renders a text execution-plan summary for `EXPLAIN [ANALYZE] <statement>`.
/// Returns a single-column `QUERY PLAN` result like PostgreSQL's psql output.
pub fn explain_statement<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    statement: &Statement,
    analyze: bool,
    format: Option<String>,
    current_database: &str,
    current_user: &str,
) -> SqlResult<QueryResult> {
    let mut lines = Vec::new();
    let plan_root = describe_statement(statement);

    // Get actual row count from table if this is a simple table scan
    let (estimated_rows, estimated_width) = get_table_stats(engine, catalog, statement);

    // Calculate approximate cost
    let total_cost = if estimated_rows > 0 {
        1.0 + (estimated_rows as f64 * 0.01)
    } else {
        0.0
    };

    // Handle JSON format
    if format.as_deref() == Some("json") {
        return explain_json(
            engine,
            catalog,
            statement,
            analyze,
            current_database,
            current_user,
        );
    }

    lines.push(format!(
        "{plan_root} (cost={:.2}..{:.2} rows={} width={})",
        total_cost, total_cost, estimated_rows, estimated_width
    ));

    if analyze {
        match crate::join::execute_statement(
            engine,
            catalog,
            statement,
            current_database,
            current_user,
            None,
            0,
        ) {
            Ok(result) => {
                let rows = match &result {
                    QueryResult::Rows { rows, .. } => rows.len(),
                    QueryResult::Inserted(n) => *n as usize,
                    QueryResult::Updated(n) => *n as usize,
                    QueryResult::Deleted(n) => *n as usize,
                    _ => 0,
                };
                lines.push(format!(
                    "  -> Actual Rows: {}, Execution Time: 0.00ms",
                    rows
                ));
            }
            Err(e) => lines.push(format!("  ERROR: {e}")),
        }
    }
    append_hybrid_section(&mut lines, catalog, statement, estimated_rows);
    append_vector_section(
        &mut lines,
        engine,
        catalog,
        statement,
        estimated_rows,
        current_database,
    );
    Ok(QueryResult::Rows {
        columns: vec!["QUERY PLAN".to_string()],
        column_types: vec![Some(ColumnType::text())],
        rows: lines
            .into_iter()
            .map(|line| vec![Value::Text(line)])
            .collect(),
    })
}

/// Appends the hybrid row+column plan section for SELECT statements.
///
/// Observability only: the section is built from the query's real features
/// and rendered through `crate::hybrid::explain` plus
/// `crate::hybrid::execute_count_estimate`. Statements with no hybrid stage
/// (e.g. `EXPLAIN SELECT 1`) and non-SELECT statements append nothing, so
/// their output stays byte-identical.
fn append_hybrid_section(
    lines: &mut Vec<String>,
    catalog: &InMemoryCatalog,
    statement: &Statement,
    estimated_rows: usize,
) {
    let Statement::Select {
        targets,
        from,
        where_expr,
        group_by,
        having,
        ..
    } = statement
    else {
        return;
    };
    let table = match from {
        Some(FromClause::Table { name, .. }) => Some(name.clone()),
        _ => None,
    };
    // Same probe decision the indexed SELECT path makes: an index range
    // applies exactly when `index_bounds_for_predicate` resolves one.
    // Joins and subqueries name no single probe table, so they report no
    // index stage rather than a guessed one.
    let use_index = table.as_deref().is_some_and(|name| {
        index_bounds_for_predicate(catalog, name, where_expr.as_ref()).is_some()
    });
    let fetch_cols = select_fetch_columns(catalog, table.as_deref(), targets);
    let has_filter = where_expr.is_some();
    let has_agg = select_has_aggregate(targets, group_by, having);
    let mut plan = hybrid::plan_hybrid(use_index, fetch_cols, has_filter, has_agg);
    // Seed the `RowIds` stage with EXPLAIN's own table-scan row estimate so
    // `execute_count_estimate` reports against the same `rows=` figure.
    if let Some(hybrid::HybridStage::RowIds { count }) = plan
        .stages
        .iter_mut()
        .find(|stage| matches!(stage, hybrid::HybridStage::RowIds { .. }))
    {
        *count = u64::try_from(estimated_rows).unwrap_or(u64::MAX);
    }
    if plan.is_empty() {
        return;
    }
    let estimate = hybrid::execute_count_estimate(&plan);
    lines.push(format!(
        "Hybrid Plan ({} stages, estimated rows {}):",
        plan.len(),
        estimate
    ));
    for stage_line in hybrid::explain(&plan).lines() {
        lines.push(format!("  {stage_line}"));
    }
}

/// Appends the vector plan section for nearest-neighbour SELECT statements.
///
/// Detects `ORDER BY distance_fn(vector_col, query) LIMIT k` over a vector
/// column and reports the exact-vs-ANN choice through
/// `plomid-vector`'s planner API. All eligibility math lives in the vector
/// crate; this site only extracts the SQL shape (function name, column,
/// dimension, usable index) and renders the returned lines. Queries without
/// a vector ordering append nothing, so non-vector EXPLAIN output is
/// byte-identical.
fn append_vector_section<E: StorageEngine>(
    lines: &mut Vec<String>,
    engine: &E,
    catalog: &InMemoryCatalog,
    statement: &Statement,
    estimated_rows: usize,
    current_database: &str,
) {
    use plomid_sql::{Expression, FromClause, Statement as SqlStatement};
    let SqlStatement::Select {
        from,
        order_by,
        limit,
        where_expr,
        ..
    } = statement
    else {
        return;
    };
    let Some(FromClause::Table {
        name: table_name, ..
    }) = from
    else {
        return;
    };
    // Radius-only queries (no distance ORDER BY): plan the bound strategy
    // (exact enumeration vs cached candidate well) with the same planner
    // execution consults; unplannable shapes keep the legacy bound line.
    let [order] = order_by.as_slice() else {
        append_radius_section(
            lines,
            engine,
            catalog,
            current_database,
            table_name,
            where_expr,
        );
        return;
    };
    let Expression::FunctionCall {
        name: func_name,
        args,
        ..
    } = &order.expr
    else {
        return;
    };
    if !plomid_vector::planner_api::order_by_supported(func_name) {
        return;
    }
    if args.len() != 2 {
        return;
    }
    // One side names the vector column; the other is the query vector.
    let column = args.iter().find_map(|arg| match arg {
        Expression::ColumnRef(col) => Some(col.as_str()),
        _ => None,
    });
    let Some(column) = column else {
        return;
    };
    let qualified = catalog.resolve_table_namespace(table_name);
    let Ok(schema) = catalog.get_table(&qualified) else {
        return;
    };
    let Ok(pos) = schema.column_index(column) else {
        return;
    };
    let col_type = schema.columns[pos].col_type;
    if col_type.type_oid != plomid_types::TypeOid::VECTOR {
        return;
    }
    // Full kind/element/dimension identity comes from the authoritative
    // vector typmod packing — never assume Dense/FLOAT32 here.
    let vector_type = match plomid_vector::VectorType::typmod_decode(col_type.typmod) {
        Ok(t) => t,
        Err(_) => return,
    };
    let dim = vector_type.dimension;
    let metric = match func_name.to_ascii_lowercase().as_str() {
        "cosine_distance" => plomid_vector::VectorMetric::Cosine,
        "l2_distance" => plomid_vector::VectorMetric::L2,
        "dot_product" | "neg_inner_product" => plomid_vector::VectorMetric::Dot,
        "hamming_distance" => plomid_vector::VectorMetric::Hamming,
        "jaccard_distance" => plomid_vector::VectorMetric::Jaccard,
        _ => return,
    };
    // A usable ANN index is one recorded on this table+column with wired
    // execution (HNSW/DiskANN graphs, IVF/SQ/PQ posting/code lists, sparse
    // inverted, binary block maps) whose capability matrix entry supports
    // this (kind, element, metric) triple. Anything else (including FLAT,
    // which is exact by design) plans exact.
    let mut ann_available = catalog.indexes().into_iter().any(|index| {
        if index.table != qualified {
            return false;
        }
        let covers = index.column == column || index.columns.iter().any(|c| c == column);
        if !covers {
            return false;
        }
        match index
            .operator_class
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("hnsw") | Some("diskann") => {
                vector_type.kind == plomid_vector::VectorKind::Dense
                    && vector_type.supports_hnsw(metric)
            }
            Some("ivf_flat") => vector_type.supports_ivf(metric),
            Some("ivf_sq") => vector_type.supports_sq(metric),
            Some("ivf_pq") => vector_type.supports_pq(metric),
            Some("sparse_inverted") => {
                vector_type.kind == plomid_vector::VectorKind::Sparse
                    && matches!(
                        metric,
                        plomid_vector::VectorMetric::L2
                            | plomid_vector::VectorMetric::Cosine
                            | plomid_vector::VectorMetric::Dot
                    )
            }
            Some("binary") => {
                vector_type.kind == plomid_vector::VectorKind::Binary
                    && vector_type.element == plomid_vector::VectorElementType::Binary
                    && metric == plomid_vector::VectorMetric::Hamming
            }
            _ => false,
        }
    });
    // Governance kill-switch: with ANN disabled the planner reports the
    // exact scan even when an index exists (execution agrees — see
    // `vector_ann::try_ann_order`).
    if !crate::vector_settings::ann_enabled() {
        ann_available = false;
    }
    let _ = dim;
    // Live search controls: EXPLAIN renders the same knobs execution reads,
    // so the plan can never disagree with the run.
    let mut options = plomid_vector::VectorSearchOptions::default();
    if let Some(ef) = crate::vector_settings::get_int("vector.ef_search") {
        options.ef_search = ef.clamp(1, 10_000) as u32;
    }
    options.compute = match crate::vector_settings::compute()
        .to_ascii_lowercase()
        .as_str()
    {
        "cpu" => plomid_vector::ComputeBackend::Cpu,
        "gpu" => plomid_vector::ComputeBackend::Gpu,
        _ => plomid_vector::ComputeBackend::Auto,
    };
    let _ = options.validate();
    // Same Auto rule as execution (`vector_ann::try_ann_order`): tables at
    // or under the *live* threshold scan exactly. Do not run a second,
    // constant-floor Auto decision here — that made EXPLAIN say "Exact"
    // while execution took ANN whenever `vector.auto_ann_min_rows` was
    // lowered below `AUTO_ANN_MIN_ROWS`.
    let threshold = crate::vector_settings::get_int("vector.auto_ann_min_rows")
        .unwrap_or(plomid_vector::planner_api::AUTO_ANN_MIN_ROWS as i64)
        .max(0) as u64;
    let max_rows = crate::vector_settings::get_int("vector.max_ann_rows")
        .unwrap_or(crate::vector_ann::DEFAULT_MAX_ANN_ROWS as i64)
        .max(0) as u64;
    let rows = estimated_rows.min(u64::MAX as usize) as u64;
    let limit_present = limit.is_some();
    let k = limit.unwrap_or(10).min(u32::MAX as usize) as u32;
    let ann_executable = ann_available
        && !engine.is_read_pinned()
        && !order.descending
        && limit_present
        && k > 0
        && rows > threshold
        && rows <= max_rows;
    let mode = if ann_executable {
        plomid_vector::SearchMode::Ann
    } else {
        plomid_vector::SearchMode::Exact
    };
    for plan_line in plomid_vector::planner_api::explain_vector_plan(
        mode,
        metric,
        &vector_type,
        &options,
        k,
        ann_available,
    ) {
        lines.push(format!("  Vector: {plan_line}"));
    }
    // Cost comparison from the same model execution uses: exact is
    // `rows × dim`, ANN is `log2(rows) × dim × probes + rerank × dim`.
    let exact_cost = plomid_vector::planner_api::estimate_cost(
        rows,
        vector_type.dimension,
        plomid_vector::SearchMode::Exact,
        &options,
        k,
    );
    let ann_cost = plomid_vector::planner_api::estimate_cost(
        rows,
        vector_type.dimension,
        plomid_vector::SearchMode::Ann,
        &options,
        k,
    );
    lines.push(format!(
        "  Vector: estimated cost exact={exact_cost:.1} ann={ann_cost:.1}"
    ));
    // Filter-aware planning: selectivity from the WHERE shape, statistics
    // from the cache when a fresh graph exists, strategy + reasons rendered.
    {
        let fingerprint = where_expr
            .as_ref()
            .map(|e| format!("{e:?}"))
            .unwrap_or_default();
        if !fingerprint.is_empty() {
            let selectivity = crate::vector_ann::estimate_filter_selectivity(where_expr);
            let watermark = crate::columnar_freshness::writes_since_boot(
                &crate::columnar_freshness::freshness_key(
                    engine.root(),
                    current_database,
                    &qualified,
                ),
            );
            let cached = crate::vector_ann::cached_graph_stats(
                current_database,
                &qualified,
                column,
                &fingerprint,
            );
            let unknown = plomid_vector::VectorStats::unknown(vector_type.dimension);
            match cached {
                Some(stats) => {
                    let fresh = stats.is_fresh(watermark);
                    lines.push(format!(
                        "  Vector stats: rows={} live={} null={} zero-norm={} indexed={} ({})",
                        stats.row_count,
                        stats.live_rows,
                        stats.null_count,
                        stats.zero_norm_count,
                        stats.indexed_rows,
                        if fresh {
                            "fresh".to_string()
                        } else {
                            format!("stale (build@{})", stats.build_watermark)
                        }
                    ));
                    if !fresh {
                        lines.push(
                            "  Vector: statistics stale (watermark moved): plan is advisory; execution rebuilds before serving"
                                .to_string(),
                        );
                    }
                    let plan = plomid_vector::planner_api::plan_filtered_search(
                        &stats,
                        selectivity,
                        k,
                        ann_available,
                        &options,
                    );
                    for plan_line in
                        plomid_vector::planner_api::explain_filter_plan(&plan, &vector_type)
                    {
                        lines.push(format!("  Vector: {plan_line}"));
                    }
                }
                None => {
                    lines.push(
                        "  Vector stats: collecting (no cached graph for this filter yet)"
                            .to_string(),
                    );
                    let plan = plomid_vector::planner_api::plan_filtered_search(
                        &unknown,
                        selectivity,
                        k,
                        ann_available,
                        &options,
                    );
                    for plan_line in
                        plomid_vector::planner_api::explain_filter_plan(&plan, &vector_type)
                    {
                        lines.push(format!("  Vector: {plan_line}"));
                    }
                }
            }
        }
    }
    // Radius visibility: `WHERE distance_fn(col, query) < bound` enumerates
    // exactly (never truncated by beam width); ANN radius candidate paths
    // verify against the same bound before returning.
    if let Some(bound) = radius_bound(where_expr) {
        lines.push(format!(
            "  Vector: radius scan (exact enumeration) bound={bound}"
        ));
    }
}

/// Appends the radius planning section: strategy, bounds in both unit
/// systems, candidate well, costs, and reasons — from the same planner
/// execution consults. EXPLAIN never evaluates the query vector: only
/// literal query sides are plannable here (anything evaluated per-row
/// keeps the legacy bound display below, unchanged).
fn append_radius_section<E: StorageEngine>(
    lines: &mut Vec<String>,
    engine: &E,
    catalog: &InMemoryCatalog,
    current_database: &str,
    table_name: &str,
    where_expr: &Option<plomid_sql::Expression>,
) {
    use plomid_sql::Expression as Ex;
    let legacy = |lines: &mut Vec<String>| {
        if let Some(bound) = radius_bound(where_expr) {
            lines.push(format!(
                "  Vector: radius scan (exact enumeration) bound={bound}"
            ));
        }
    };
    let Some(expr) = where_expr.as_ref() else {
        return;
    };
    let shape = match crate::vector_ann::parse_radius_shape(expr) {
        Some(shape) => shape,
        None => return legacy(lines),
    };
    // Query side must be a literal here: EXPLAIN evaluates nothing.
    let query_lanes: Vec<f32> = match shape.query_arg {
        Ex::Literal(plomid_sql::Value::Vector(plomid_vector::VectorValue::Dense(d))) => {
            d.values_f32.clone()
        }
        Ex::VectorLiteral(lanes) => lanes.clone(),
        _ => return legacy(lines),
    };
    let _ = query_lanes;
    let qualified = catalog.resolve_table_namespace(table_name);
    let Ok(schema) = catalog.get_table(&qualified) else {
        return legacy(lines);
    };
    let Ok(pos) = schema.column_index(shape.column) else {
        return legacy(lines);
    };
    let col_type = schema.columns[pos].col_type;
    if col_type.type_oid != plomid_types::TypeOid::VECTOR {
        return legacy(lines);
    }
    let Ok(vector_type) = plomid_vector::VectorType::typmod_decode(col_type.typmod) else {
        return legacy(lines);
    };
    if vector_type.kind != plomid_vector::VectorKind::Dense {
        return legacy(lines);
    }
    let Some(metric) = crate::vector_ann::sql_metric_from_name(shape.func_name, shape.similarity)
    else {
        return legacy(lines);
    };
    if !matches!(
        metric,
        plomid_vector::VectorMetric::L2
            | plomid_vector::VectorMetric::Cosine
            | plomid_vector::VectorMetric::Dot
    ) {
        return legacy(lines);
    }
    let bound = if shape.similarity {
        match plomid_vector::planner_api::similarity_to_distance_bound(
            metric,
            shape.threshold as f32,
        ) {
            Some(bound) => bound,
            None => return legacy(lines),
        }
    } else {
        shape.threshold as f32
    };
    if !bound.is_finite() {
        return legacy(lines);
    }
    // A usable ANN index is one recorded on this table+column whose
    // capability entry supports this (kind, element, metric) triple.
    let mut ann_available = catalog.indexes().into_iter().any(|index| {
        if index.table != qualified {
            return false;
        }
        let covers =
            index.column == shape.column || index.columns.iter().any(|c| c == shape.column);
        if !covers {
            return false;
        }
        match index
            .operator_class
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("hnsw") | Some("diskann") => {
                vector_type.kind == plomid_vector::VectorKind::Dense
                    && vector_type.supports_hnsw(metric)
            }
            Some("ivf_flat") => vector_type.supports_ivf(metric),
            Some("ivf_sq") => vector_type.supports_sq(metric),
            Some("ivf_pq") => vector_type.supports_pq(metric),
            _ => false,
        }
    });
    if !crate::vector_settings::ann_enabled() {
        ann_available = false;
    }
    let watermark = crate::columnar_freshness::writes_since_boot(
        &crate::columnar_freshness::freshness_key(engine.root(), current_database, &qualified),
    );
    let unknown = plomid_vector::VectorStats::unknown(vector_type.dimension);
    // Radius over the full working set carries the empty fingerprint.
    match crate::vector_ann::cached_graph_stats(current_database, &qualified, shape.column, "") {
        Some(stats) => {
            let fresh = stats.is_fresh(watermark);
            lines.push(format!(
                "  Vector stats: rows={} live={} null={} zero-norm={} indexed={} ({})",
                stats.row_count,
                stats.live_rows,
                stats.null_count,
                stats.zero_norm_count,
                stats.indexed_rows,
                if fresh {
                    "fresh".to_string()
                } else {
                    format!("stale (build@{})", stats.build_watermark)
                }
            ));
            let mut options = plomid_vector::VectorSearchOptions::default();
            if let Some(ef) = crate::vector_settings::get_int("vector.ef_search") {
                options.ef_search = ef.clamp(1, 10_000) as u32;
            }
            let plan = plomid_vector::planner_api::plan_radius_search(
                &stats,
                metric,
                bound,
                ann_available,
                &options,
            );
            for plan_line in
                plomid_vector::planner_api::explain_radius_plan(&plan, metric, &vector_type)
            {
                lines.push(format!("  Vector: {plan_line}"));
            }
        }
        None => {
            lines.push(
                "  Vector stats: collecting (no cached graph for this column yet)".to_string(),
            );
            let options = plomid_vector::VectorSearchOptions::default();
            let plan = plomid_vector::planner_api::plan_radius_search(
                &unknown,
                metric,
                bound,
                ann_available,
                &options,
            );
            for plan_line in
                plomid_vector::planner_api::explain_radius_plan(&plan, metric, &vector_type)
            {
                lines.push(format!("  Vector: {plan_line}"));
            }
        }
    }
}

/// Extracts the radius bound from `WHERE distance_fn(col, query) < bound`.
///
/// Returns the bound text for display when the predicate is a `<`/`<=`
/// comparison with a distance function on one side and a numeric literal
/// on the other; `None` otherwise (nothing reported, nothing misreported).
fn radius_bound(where_expr: &Option<plomid_sql::Expression>) -> Option<String> {
    use plomid_sql::Expression as E;
    let expr = where_expr.as_ref()?;
    let (func, bound) = match expr {
        E::Less(l, r) | E::LessOrEqual(l, r) => (l.as_ref(), r.as_ref()),
        _ => return None,
    };
    let E::FunctionCall { name, args, .. } = func else {
        return None;
    };
    if !plomid_vector::planner_api::order_by_supported(name) || args.len() != 2 {
        return None;
    }
    if !args.iter().any(|arg| matches!(arg, E::ColumnRef(_))) {
        return None;
    }
    match bound {
        E::Literal(value) => Some(value.to_sql_text()),
        _ => None,
    }
}

/// Projected stored-column names for the hybrid `ColumnFetch` stage.
///
/// Plain column references contribute their names; `*` expands through the
/// catalog schema when a single table is known. Any other target shape
/// (scalar expressions, function calls, unresolvable stars) contributes
/// nothing, so the stage is omitted rather than guessed.
fn select_fetch_columns(
    catalog: &InMemoryCatalog,
    table: Option<&str>,
    targets: &[SelectTarget],
) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut push_unique = |name: String| {
        if !cols.contains(&name) {
            cols.push(name);
        }
    };
    for target in targets {
        match target {
            SelectTarget::All | SelectTarget::QualifiedStar { .. } => {
                let Some(name) = table else {
                    return Vec::new();
                };
                let qualified = catalog.resolve_table_namespace(name);
                let Ok(schema) = catalog.get_table(&qualified) else {
                    return Vec::new();
                };
                for column in &schema.columns {
                    push_unique(column.name.clone());
                }
            }
            SelectTarget::Expr { expr, .. } => {
                if let Expression::ColumnRef(name) = expr {
                    push_unique(name.clone());
                }
            }
            SelectTarget::Aliased { target, .. } => {
                let column = match target.as_ref() {
                    SelectTarget::Expr {
                        expr: Expression::ColumnRef(name),
                        ..
                    } => Some(name),
                    _ => None,
                };
                if let Some(name) = column {
                    push_unique(name.clone());
                }
            }
            SelectTarget::Function(_)
            | SelectTarget::FunctionCall { .. }
            | SelectTarget::WindowFunction { .. } => {}
        }
    }
    cols
}

/// Whether the SELECT carries aggregation, mirroring the single-table SELECT
/// fast path's own gate: aggregate targets, or any `GROUP BY` / `HAVING`.
fn select_has_aggregate(
    targets: &[SelectTarget],
    group_by: &Option<GroupByClause>,
    having: &Option<Expression>,
) -> bool {
    targets.iter().any(|target| match target {
        SelectTarget::Function(name) => is_aggregate_function(name),
        SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
        SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
        SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
        _ => false,
    }) || group_by.is_some()
        || having.is_some()
}

/// Get table statistics for row count and width estimation
fn get_table_stats<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    statement: &Statement,
) -> (usize, usize) {
    let table_name = extract_table_name(statement);
    match table_name {
        Some(ref name) => {
            let qualified = catalog.resolve_table_namespace(name);
            match catalog.get_table(&qualified) {
                Ok(schema) => {
                    let width = estimate_row_width(schema);
                    match count_table_rows(engine, &qualified) {
                        Ok(count) => (count, width),
                        Err(_) => (0, width),
                    }
                }
                Err(_) => (0, 0),
            }
        }
        None => (0, 0), // No table (e.g., EXPLAIN SELECT 1)
    }
}

/// Generate JSON EXPLAIN output (manual JSON generation to avoid serde_json dependency)
fn explain_json<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    statement: &Statement,
    analyze: bool,
    current_database: &str,
    current_user: &str,
) -> SqlResult<QueryResult> {
    let plan_root = describe_statement(statement);
    let (estimated_rows, estimated_width) = get_table_stats(engine, catalog, statement);
    let total_cost = if estimated_rows > 0 {
        1.0 + (estimated_rows as f64 * 0.01)
    } else {
        0.0
    };

    let table_name = extract_table_name(statement);

    // Build JSON manually
    let mut json = String::from("{\n  \"Plan\": {\n");
    json.push_str(&format!(
        "    \"Node Type\": \"{}\",\n",
        escape_json_string(&plan_root)
    ));
    json.push_str("\"Parent Relationship\": \"Outer\",\n");
    json.push_str("\"Parallel Aware\": false,\n");
    json.push_str(&format!(
        "    \"Relation Name\": {},\n",
        if let Some(ref name) = table_name {
            format!("\"{}\"", escape_json_string(name))
        } else {
            "null".to_string()
        }
    ));
    json.push_str("\"Alias\": null,\n");
    json.push_str(&format!("    \"Startup Cost\": {:.2},\n", total_cost));
    json.push_str(&format!("    \"Total Cost\": {:.2},\n", total_cost));
    json.push_str(&format!("    \"Plan Rows\": {},\n", estimated_rows));
    json.push_str(&format!("    \"Plan Width\": {}\n", estimated_width));
    json.push_str("  }\n");

    // Add actual stats if analyzing
    if analyze {
        let start = std::time::Instant::now();
        let result = crate::join::execute_statement(
            engine,
            catalog,
            statement,
            current_database,
            current_user,
            None,
            0,
        );
        let elapsed = start.elapsed();

        let actual_rows = match result {
            Ok(qr) => match qr {
                QueryResult::Rows { rows, .. } => rows.len(),
                QueryResult::Inserted(n) => n as usize,
                QueryResult::Updated(n) => n as usize,
                QueryResult::Deleted(n) => n as usize,
                _ => 0,
            },
            Err(_) => 0,
        };

        // Insert actual stats before closing braces
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        json.push_str(&format!(
            "  \"Actual Rows\": {},\n  \"Actual Loops\": 1,\n  \"Actual Startup Time\": {:.3},\n  \"Actual Total Time\": {:.3}\n}}\n",
            actual_rows, elapsed_ms, elapsed_ms
        ));
    } else {
        json.push('}');
        json.push('\n');
    }

    Ok(QueryResult::Rows {
        columns: vec!["QUERY PLAN".to_string()],
        column_types: vec![Some(ColumnType::text())],
        rows: vec![vec![Value::Text(json)]],
    })
}

/// Escape a string for JSON output
fn escape_json_string(s: &str) -> String {
    let mut escaped = String::new();
    for c in s.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", c as u32));
            }
            _ => escaped.push(c),
        }
    }
    escaped
}

/// Produces a short text description of a statement's plan root.
fn describe_statement(statement: &Statement) -> String {
    match statement {
        Statement::Select {
            from, where_expr, ..
        } => {
            let from_desc = match from {
                Some(FromClause::Table { name, .. }) => format!(" on {name}"),
                Some(FromClause::Join { left, right, .. }) => {
                    format!(" on {} JOIN {}", describe_from(left), describe_from(right))
                }
                Some(FromClause::Subquery { alias, .. }) => format!(" on subquery {alias}"),
                Some(FromClause::TableFunction { name, .. }) => format!(" on {name}"),
                None => String::new(),
            };
            if where_expr.is_some() {
                format!("Seq Scan{from_desc} (filter: WHERE)")
            } else {
                format!("Seq Scan{from_desc}")
            }
        }
        Statement::SetOperation { op, all, .. } => {
            let name = match op {
                SetOpKind::Union => "Append",
                SetOpKind::Intersect => "Hash Intersect",
                SetOpKind::Except => "Hash Except",
            };
            let all_suffix = if *all { " ALL" } else { "" };
            format!("{name}{all_suffix}")
        }
        Statement::With { ctes, .. } => format!("CTE Scan ({} CTEs)", ctes.len()),
        Statement::Insert { table, .. } => format!("Insert on {table}"),
        Statement::Update { table, .. } => format!("Update on {table}"),
        Statement::Delete { table, .. } => format!("Delete on {table}"),
        Statement::Explain { .. } => "Explain".to_string(),
        other => format!("{other:?}"),
    }
}

/// Describes one FROM clause side for plan naming.
fn describe_from(clause: &FromClause) -> String {
    match clause {
        FromClause::Table { name, .. } => name.clone(),
        FromClause::TableFunction { name, .. } => name.clone(),
        FromClause::Subquery { alias, .. } => alias.clone(),
        FromClause::Join { left, right, .. } => {
            format!("({} JOIN {})", describe_from(left), describe_from(right))
        }
    }
}

/// Estimate the average width of a row in bytes based on column types.
fn estimate_row_width(schema: &TableSchema) -> usize {
    let mut total_width = 0;
    for column in &schema.columns {
        total_width += estimate_column_width(&column.col_type);
    }
    total_width
}

/// Estimate the width of a single column based on its type.
fn estimate_column_width(col_type: &ColumnType) -> usize {
    match col_type.type_oid.raw() {
        23 => 4,    // INTEGER
        21 => 4,    // SMALLINT
        20 => 8,    // BIGINT
        700 => 8,   // REAL
        701 => 8,   // DOUBLE PRECISION
        1043 => 16, // VARCHAR
        1042 => 16, // CHAR
        25 => 32,   // TEXT
        3802 => 32, // JSON
        3808 => 32, // JSONB
        16 => 1,    // BOOLEAN
        _ => 16,
    }
}

/// Count rows in a table by scanning its storage.
fn count_table_rows<E: StorageEngine>(
    engine: &mut E,
    table_name: &str,
) -> Result<usize, PlomidError> {
    let start = format!("{table_name}:").into_bytes();
    let end = format!("{table_name}:\u{10FFFF}").into_bytes();
    let entries = engine.scan(Some(&start), Some(&end))?;
    Ok(entries.len())
}

/// Extract table name from a SELECT statement's FROM clause.
fn extract_table_name(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Select { from, .. } => match from {
            Some(FromClause::Table { name, .. }) => Some(name.clone()),
            Some(FromClause::Join { left, .. }) => extract_from_clause_name(left),
            _ => None,
        },
        Statement::Insert { table, .. } => Some(table.clone()),
        Statement::Update { table, .. } => Some(table.clone()),
        Statement::Delete { table, .. } => Some(table.clone()),
        _ => None,
    }
}

/// Extract table name from a FromClause.
fn extract_from_clause_name(clause: &FromClause) -> Option<String> {
    match clause {
        FromClause::Table { name, .. } => Some(name.clone()),
        FromClause::Join { left, .. } => extract_from_clause_name(left),
        _ => None,
    }
}
