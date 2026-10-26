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
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::{
    Catalog, Expression, FromClause, GroupByClause, InMemoryCatalog, OrderByItem, QueryResult,
    SelectTarget, Statement, Value,
};
use plomid_txn::{StorageEngine, StorageEngineTransaction};

use crate::dml::{execute_delete_txn, execute_insert_rows_txn, execute_update_txn};
use crate::encoding::decode_row;
use crate::error::{SqlError, SqlResult};
use crate::index::register_constraint_indexes;
use crate::query::{
    evaluate_expression, evaluate_predicate, expr_has_subquery, expression_has_aggregate, value_cmp,
};

/// Executes DDL statements inside an explicit transaction.
///
/// These modify the in-memory catalog but do NOT persist to storage
/// until after the transaction commits. This allows CREATE TABLE etc.
/// to be used inside BEGIN...COMMIT blocks.
fn execute_ddl_in_txn<'a, E: StorageEngineTransaction<'a>>(
    _txn: &mut E,
    catalog: &mut InMemoryCatalog,
    stmt: Statement,
    created_tables: &mut Vec<String>,
) -> SqlResult<()> {
    match stmt {
        Statement::CreateTable {
            name,
            if_not_exists,
            temporary: _,
            columns,
            constraints,
        } => {
            // Resolve an unqualified table name against the session's
            // search_path so PostgreSQL's per-schema identity is honored.
            let name = catalog.resolve_create_name(&name)?;
            if if_not_exists && catalog.has_table(&name) {
                return Ok(());
            }
            // SERIAL / BIGSERIAL / SMALLSERIAL columns auto-create a backing
            // sequence named `<table>_<column>_seq`, matching PostgreSQL.
            // The sequence uses the bare relation name so `nextval('seq')`
            // resolves it the same way as before schema-qualification.
            let base_name = name.rsplit('.').next().unwrap_or(&name);
            for col in &columns {
                if col.col_type.serial {
                    let seq_name = format!("{base_name}_{}_seq", col.name);
                    if !catalog.has_sequence(&seq_name) {
                        catalog.create_sequence(&seq_name)?;
                    }
                }
            }
            catalog.create_table(name.clone(), columns, constraints)?;
            // Record the claim: the group's committed save must re-probe it
            // against concurrent creators (F-004).
            created_tables.push(name.clone());
            // A table created inside an explicit transaction is also empty, so
            // its constraint indexes need no backfill; inserts later in the
            // same transaction maintain them through `stage_index_puts`.
            register_constraint_indexes(catalog, &name)?;
            Ok(())
        }
        Statement::CreateView {
            name,
            columns,
            query,
            or_replace,
        } => {
            // Resolve the view name through the session search_path so that
            // unqualified names land in the correct schema.
            let name = catalog.resolve_create_name(&name)?;
            if or_replace && catalog.has_view(&name) {
                catalog.drop_view(&name)?;
            }
            catalog.create_view(name, columns, query)?;
            Ok(())
        }
        Statement::DropRole { name, if_exists } => {
            if !catalog.has_role(&name) {
                if if_exists {
                    return Ok(());
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("role \"{name}\" does not exist"),
                )));
            }
            catalog.drop_role(&name)?;
            Ok(())
        }
        _ => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Unsupported,
            "statement inside explicit transaction not supported",
        ))),
    }
}

pub fn execute_transaction_group<E: StorageEngine>(
    engine: &mut E,
    catalog: &mut InMemoryCatalog,
    stmts: Vec<Statement>,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<QueryResult>> {
    execute_transaction_group_mode(engine, catalog, stmts, current_database, current_user)
}

fn execute_transaction_group_mode<E: StorageEngine>(
    engine: &mut E,
    catalog: &mut InMemoryCatalog,
    stmts: Vec<Statement>,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<QueryResult>> {
    let stmts = coalesce_insert_statements(stmts);
    // F-004: names this group's start image already knew. A create of a
    // known name is an intentional replacement (DROP + CREATE inside the
    // group), not a concurrent claim; only freshly claimed names are
    // re-probed against durable state at the save below.
    let start_tables: std::collections::BTreeSet<String> =
        catalog.table_names().into_iter().collect();
    let mut created_tables: Vec<String> = Vec::new();
    tracing::debug!(
        target: "sql::txn",
        "transaction_start stmt_count={}",
        stmts.len()
    );
    let mut txn = engine.begin()?;
    // Per-statement results in execution order (PostgreSQL returns each
    // statement's own result; the old code kept only the last one, which
    // silently dropped mid-transaction rows and RETURNING output).
    let mut outcomes: Vec<QueryResult> = Vec::new();
    // Tracks whether the group mutated the catalog (DDL): catalog images
    // persist only on successful commit (durable DDL), never on abort.
    let mut catalog_mutated = false;
    let result = (|| -> SqlResult<()> {
        for stmt in stmts.into_iter().skip(1) {
            // Each statement in the transaction gets its own statement
            // execution context (PostgreSQL: CURRENT_TIMESTAMP is the
            // transaction/statement start time; statements in one txn still
            // each observe a stable timestamp within themselves).
            let _ctx = crate::context::StatementContext::enter_if_none();
            match stmt {
                Statement::Commit => {
                    tracing::debug!(target: "sql::txn", "commit");
                    txn.commit()?;
                    outcomes.push(QueryResult::Committed);
                }
                Statement::Rollback => {
                    tracing::debug!(target: "sql::txn", "rollback");
                    txn.abort()?;
                    outcomes.push(QueryResult::RolledBack);
                }
                Statement::Insert {
                    table,
                    columns,
                    source,
                    returning,
                    on_conflict,
                } => {
                    tracing::trace!(
                        target: "sql::txn",
                        "execute_dml kind=insert"
                    );
                    outcomes.push(execute_insert_rows_txn(
                        &mut txn,
                        catalog,
                        table,
                        columns,
                        source,
                        returning,
                        on_conflict,
                    )?);
                }
                Statement::Copy { .. } => {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Unsupported,
                        "COPY inside explicit transactions requires the PostgreSQL wire-level \
                     CopyData/CopyDone/CopyFail frame protocol",
                    )));
                }
                Statement::Update {
                    table,
                    alias,
                    from,
                    assignments,
                    where_expr,
                    returning,
                } => {
                    if from.is_some() {
                        // `UPDATE ... FROM` needs the shared SELECT/join
                        // machinery (engine access) to materialize its source
                        // relation; that is unavailable inside an explicit
                        // transaction (same limitation as INSERT ... SELECT).
                        return Err(SqlError::Storage(PlomidError::new(
                            ErrorKind::Unsupported,
                            "UPDATE ... FROM inside explicit transactions is not yet supported",
                        )));
                    }
                    // Forward `UPDATE ... RETURNING` targets into the transactional
                    // DML path. `returning: None` keeps the legacy row-count
                    // result; `Some(targets)` projects each post-update row
                    // (PostgreSQL exposes the *new* tuple to RETURNING).
                    outcomes.push(execute_update_txn(
                        &mut txn,
                        catalog,
                        table,
                        alias.clone(),
                        assignments,
                        None,
                        where_expr,
                        returning,
                    )?);
                }
                Statement::Delete {
                    table,
                    alias,
                    using,
                    where_expr,
                    returning,
                } => {
                    // Materialize `DELETE ... USING` source relations. Plain
                    // table relations are read through the transaction's own
                    // scan API, so uncommitted changes from earlier statements
                    // in the same transaction are visible. Subquery/join/table
                    // function sources need the shared SELECT/join machinery
                    // (engine access) and report an unsupported error.
                    let using_source = match &using {
                        Some(clause) => Some(crate::update_from::materialize_using_in_txn(
                            &mut txn, catalog, clause,
                        )?),
                        None => None,
                    };
                    // Same contract as UPDATE: `returning: None` keeps the
                    // legacy deleted-count result; `Some(targets)` returns the
                    // deleted rows as `Rows`, evaluated against the pre-delete
                    // row (PostgreSQL exposes the deleted tuple).
                    outcomes.push(execute_delete_txn(
                        &mut txn,
                        catalog,
                        table,
                        alias.clone(),
                        using_source,
                        where_expr,
                        returning,
                    )?);
                }
                Statement::Select {
                    targets,
                    distinct,
                    distinct_on,
                    from,
                    where_expr,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                } => {
                    outcomes.push(execute_select_txn(
                        &mut txn,
                        catalog,
                        &targets,
                        distinct,
                        distinct_on.as_deref(),
                        from.as_ref(),
                        where_expr.as_ref(),
                        group_by.as_ref(),
                        having.as_ref(),
                        &order_by,
                        limit,
                        offset,
                        current_database,
                        current_user,
                    )?);
                }
                _ => {
                    // DDL statements (CREATE TABLE, CREATE VIEW, etc.) modify the
                    // in-memory catalog. We execute them here to update the catalog
                    // state, and defer catalog persistence until after commit.
                    // The command tag mirrors the autocommit path so grouped
                    // DDL reports like its standalone form.
                    let tag = ddl_command_tag(&stmt);
                    execute_ddl_in_txn(&mut txn, catalog, stmt, &mut created_tables)?;
                    catalog_mutated = true;
                    if let Some(tag) = tag {
                        outcomes.push(QueryResult::Created(tag.to_string()));
                    }
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        // A failed statement must release the transaction's storage locks and
        // staging state so a later connection cannot be stranded.
        let _ = txn.abort();
        return Err(error);
    }
    // Persist catalog mutations from in-transaction DDL exactly once, after
    // the data commit succeeded (aborted groups leave no durable trace).
    // The transaction handle is dropped first: its write-lane guards must
    // be released before the catalog save takes the engine lock.
    drop(txn);
    let committed = outcomes
        .iter()
        .any(|outcome| matches!(outcome, QueryResult::Committed));
    if catalog_mutated && committed {
        persist_group_catalog(engine, catalog, &created_tables, &start_tables)?;
    }
    tracing::debug!(
        target: "sql::txn",
        "transaction_end outcomes={}",
        outcomes.len()
    );
    Ok(outcomes)
}

/// Persists catalog changes made by a committed transaction group (F-004).
///
/// Tables the group created that its start image did not already know claim
/// names against concurrent creators (whose point-in-time images cannot see
/// each other). The durable entry for every claim is re-probed under the
/// create gate and the save commits in the same critical section, exactly
/// like autocommit `CREATE TABLE`: one concurrent creator wins and every
/// other group converges its image to durable state and reports the claim
/// as "already exists" (its own definition never persisted).
fn persist_group_catalog<E: StorageEngine>(
    engine: &mut E,
    catalog: &mut InMemoryCatalog,
    created_tables: &[String],
    start_tables: &std::collections::BTreeSet<String>,
) -> SqlResult<()> {
    let claims: Vec<&str> = created_tables
        .iter()
        .filter(|name| !start_tables.contains(name.as_str()))
        .map(String::as_str)
        .collect();
    if claims.is_empty() {
        return plomid_sql::save_catalog(catalog, engine).map_err(SqlError::Storage);
    }
    let gate = plomid_sql::catalog::CreateGate::acquire();
    for name in claims.iter().copied() {
        if gate.table_exists(engine, name).map_err(SqlError::Storage)? {
            // Lost the race: adopt the winner's durable definition so the
            // session cannot keep serving a phantom shape.
            let path = catalog.search_path().to_vec();
            *catalog = plomid_sql::load_catalog(engine).map_err(SqlError::Storage)?;
            catalog.set_search_path(path);
            return Err(SqlError::Storage(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("table \"{name}\" already exists"),
                format!("table={name}"),
            )));
        }
    }
    gate.save(catalog, engine).map_err(SqlError::Storage)
}

/// Command tag for grouped DDL, mirroring the autocommit path. `None`
/// means the statement reports nothing on its own (it only matters if the
/// group commits, which reports `Committed`).
fn ddl_command_tag(stmt: &Statement) -> Option<&'static str> {
    match stmt {
        Statement::CreateTable { .. } => Some("CREATE TABLE"),
        Statement::CreateView { .. } => Some("CREATE VIEW"),
        _ => None,
    }
}

/// Explicit-Unsupported error for shapes the group path cannot run yet.
///
/// The contract matches the historical catch-all: anything outside the
/// supported DML/DDL/single-table-SELECT surface fails the whole group
/// (which then aborts) instead of running partially.
fn unsupported_in_txn(what: &str) -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::Unsupported,
        format!("{what} inside explicit transactions is not yet supported"),
    ))
}

/// Single-table SELECT inside an explicit transaction group, reading
/// through the open transaction handle: staged writes from earlier
/// statements in the same group are visible (read-your-own-writes), and
/// rows rolled back later never escape.
///
/// Supported: one plain table, WHERE, ORDER BY, LIMIT/OFFSET, `*` /
/// column / scalar-expression projection, and bare `COUNT(*)`,
/// `COUNT(col)`, `COUNT(1)`. Everything else (joins, subqueries,
/// aggregates, GROUP BY, DISTINCT, window, CTEs, set ops, table
/// functions) keeps the explicit Unsupported error — the same contract
/// as before, narrowed to the shapes that truly need the general
/// query engine.
#[allow(clippy::too_many_arguments)]
fn execute_select_txn<'a, T: StorageEngineTransaction<'a>>(
    txn: &mut T,
    catalog: &InMemoryCatalog,
    targets: &[SelectTarget],
    distinct: bool,
    distinct_on: Option<&[Expression]>,
    from: Option<&FromClause>,
    where_expr: Option<&Expression>,
    group_by: Option<&GroupByClause>,
    having: Option<&Expression>,
    order_by: &[OrderByItem],
    limit: Option<usize>,
    offset: Option<usize>,
    current_database: &str,
    current_user: &str,
) -> SqlResult<QueryResult> {
    if distinct || distinct_on.is_some() {
        return Err(unsupported_in_txn("DISTINCT"));
    }
    if group_by.is_some() || having.is_some() {
        return Err(unsupported_in_txn("GROUP BY / HAVING"));
    }
    let (table, alias) = match from {
        Some(FromClause::Table { name, alias }) => (name.clone(), alias.clone()),
        _ => {
            return Err(unsupported_in_txn(
                "SELECT with joins, table functions, or no FROM",
            ));
        }
    };
    for target in targets {
        if matches!(target, SelectTarget::WindowFunction { .. }) {
            return Err(unsupported_in_txn("window functions"));
        }
    }
    // Reject subqueries and aggregates up front: the row evaluator below
    // cannot resolve nested scopes, and failing here (before reading any
    // rows) keeps the error deterministic.
    let mut probe = Vec::new();
    probe.extend(targets.iter().flat_map(select_target_exprs));
    if let Some(predicate) = where_expr {
        probe.push(predicate.clone());
    }
    probe.extend(order_by.iter().map(|item| item.expr.clone()));
    for expr in &probe {
        if expr_has_subquery(expr) {
            return Err(unsupported_in_txn("subqueries"));
        }
        if expression_has_aggregate(expr) && !is_bare_count(targets) {
            return Err(unsupported_in_txn("aggregate functions"));
        }
    }

    let schema = catalog.get_table(&table).map_err(SqlError::Storage)?;
    // Storage keys use the catalog-resolved qualified name (search_path
    // aware), exactly like the autocommit scan: scanning the raw AST name
    // would miss every row.
    let qualified = catalog
        .resolve_table_name(&table)
        .map_err(SqlError::Storage)?;
    let qualifier = alias.as_deref().unwrap_or(&table);
    // Expand the projection to (target, display name) pairs, mirroring
    // the session-txn naming convention (alias, unqualified column,
    // function name, or `?column?`).
    let mut expanded: Vec<(SelectTarget, String)> = Vec::new();
    for target in targets {
        match target {
            SelectTarget::All => {
                for column in &schema.columns {
                    expanded.push((
                        SelectTarget::Expr {
                            expr: Expression::ColumnRef(column.name.clone()),
                            alias: None,
                        },
                        column.name.clone(),
                    ));
                }
            }
            SelectTarget::QualifiedStar { qualifier: q } => {
                if !q.eq_ignore_ascii_case(&table)
                    && !q.eq_ignore_ascii_case(qualifier)
                    && !q.eq_ignore_ascii_case(&qualified)
                {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::NotFound,
                        format!("relation \"{q}\" does not exist"),
                    )));
                }
                for column in &schema.columns {
                    expanded.push((
                        SelectTarget::Expr {
                            expr: Expression::ColumnRef(column.name.clone()),
                            alias: None,
                        },
                        column.name.clone(),
                    ));
                }
            }
            SelectTarget::Expr { expr, alias } => {
                let display = alias.clone().unwrap_or_else(|| match expr {
                    Expression::ColumnRef(name) => {
                        name.rsplit('.').next().unwrap_or(name).to_string()
                    }
                    _ => "?column?".to_string(),
                });
                expanded.push((
                    SelectTarget::Expr {
                        expr: expr.clone(),
                        alias: alias.clone(),
                    },
                    display,
                ));
            }
            SelectTarget::Aliased { target, alias } => {
                expanded.push((target.as_ref().clone(), alias.clone()));
            }
            SelectTarget::Function(name) => {
                expanded.push((
                    SelectTarget::Expr {
                        expr: Expression::ColumnRef(name.clone()),
                        alias: None,
                    },
                    name.clone(),
                ));
            }
            SelectTarget::FunctionCall { name, .. } => {
                expanded.push((target.clone(), name.clone()));
            }
            SelectTarget::WindowFunction { .. } => {
                return Err(unsupported_in_txn("window functions"));
            }
        }
    }

    // Read every row of the table through the open transaction: staged
    // puts/deletes from earlier group statements are visible here.
    let prefix = format!("{qualified}:");
    let end = format!("{qualified}:\u{10FFFF}");
    // Decode the scan values directly (like the DML probe paths): the
    // scan merges staged overlay puts over the committed image, so its
    // values are exactly what this transaction must see. A second
    // point-`get` per key would resolve through the version store, which
    // lags rows the scan already merged.
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (_, bytes) in txn.scan(Some(prefix.as_bytes()), Some(end.as_bytes()))? {
        rows.push(decode_row(&bytes).map_err(SqlError::Storage)?);
    }
    if let Some(predicate) = where_expr {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            if evaluate_predicate(&row, schema, predicate)? {
                kept.push(row);
            }
        }
        rows = kept;
    }

    // Bare COUNT(...) aggregates the filtered set into one row.
    if is_bare_count(targets) {
        let name = "count".to_string();
        let count = match &targets[0] {
            SelectTarget::FunctionCall { args, .. } => match args.as_slice() {
                [Expression::Star] => rows.len() as i64,
                [arg] => {
                    let mut matched = 0i64;
                    for row in &rows {
                        if !evaluate_expression(row, schema, arg)?.is_null() {
                            matched += 1;
                        }
                    }
                    matched
                }
                _ => {
                    return Err(unsupported_in_txn("COUNT with multiple arguments"));
                }
            },
            _ => unreachable!("is_bare_count checked the shape"),
        };
        return Ok(QueryResult::rows(
            vec![name],
            vec![vec![Value::Int8(count)]],
        ));
    }

    // Project, then sort projected rows by ORDER BY keys. An order key
    // naming an output column resolves against the projection (PostgreSQL
    // output scope); anything else evaluates against the source row.
    let output_names: Vec<String> = expanded.iter().map(|(_, name)| name.clone()).collect();
    let mut projected: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut values = Vec::with_capacity(expanded.len());
        for (target, _) in &expanded {
            values.push(eval_txn_target(
                target,
                row,
                schema,
                current_database,
                current_user,
            )?);
        }
        projected.push((row.clone(), values));
    }
    if !order_by.is_empty() {
        let mut keyed: Vec<(Vec<Value>, Vec<Value>, Vec<Value>)> = Vec::with_capacity(rows.len());
        for (source, values) in &projected {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(resolve_order_key(
                    &item.expr,
                    source,
                    schema,
                    values,
                    &output_names,
                )?);
            }
            keyed.push((source.clone(), values.clone(), keys));
        }
        keyed.sort_by(|(_, _, a), (_, _, b)| compare_txn_order_keys(a, b, order_by));
        projected = keyed
            .into_iter()
            .map(|(source, values, _)| (source, values))
            .collect();
    }
    let offset = offset.unwrap_or(0);
    let rows: Vec<Vec<Value>> = projected
        .into_iter()
        .skip(offset)
        .map(|(_, values)| values)
        .collect();
    let rows = match limit {
        Some(n) => rows.into_iter().take(n).collect(),
        None => rows,
    };
    Ok(QueryResult::rows(output_names, rows))
}

/// True for a lone `COUNT(*)`, `COUNT(col)`, or `COUNT(1)` target.
fn is_bare_count(targets: &[SelectTarget]) -> bool {
    matches!(
        targets,
        [SelectTarget::FunctionCall { name, .. }] if name.eq_ignore_ascii_case("count")
    )
}

/// Expressions a shape gate must inspect for subqueries/aggregates.
fn select_target_exprs(target: &SelectTarget) -> Vec<Expression> {
    match target {
        SelectTarget::Expr { expr, .. } => vec![expr.clone()],
        SelectTarget::Aliased { target, .. } => select_target_exprs(target),
        SelectTarget::FunctionCall { args, .. } => args.clone(),
        SelectTarget::All
        | SelectTarget::QualifiedStar { .. }
        | SelectTarget::Function(_)
        | SelectTarget::WindowFunction { .. } => Vec::new(),
    }
}

/// Evaluates one projection target against a source row.
fn eval_txn_target(
    target: &SelectTarget,
    row: &[Value],
    schema: &plomid_sql::TableSchema,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Value> {
    match target {
        SelectTarget::Expr { expr, .. } => evaluate_expression(row, schema, expr),
        SelectTarget::Aliased { target, .. } => {
            eval_txn_target(target, row, schema, current_database, current_user)
        }
        SelectTarget::All | SelectTarget::QualifiedStar { .. } => {
            Err(unsupported_in_txn("star expansion here"))
        }
        SelectTarget::Function(name) => {
            crate::catalog_fn::function_value(name, current_database, current_user)
        }
        SelectTarget::FunctionCall { .. } => Err(unsupported_in_txn("function calls")),
        SelectTarget::WindowFunction { .. } => Err(unsupported_in_txn("window functions")),
    }
}

/// Resolves one ORDER BY key: output-column names first (projection
/// scope), otherwise the source row (table scope).
fn resolve_order_key(
    expr: &Expression,
    source: &[Value],
    schema: &plomid_sql::TableSchema,
    projected: &[Value],
    output_names: &[String],
) -> SqlResult<Value> {
    if let Expression::ColumnRef(name) = expr {
        let tail = name.rsplit('.').next().unwrap_or(name);
        if let Some(position) = output_names
            .iter()
            .position(|output| output.eq_ignore_ascii_case(tail))
        {
            if let Some(value) = projected.get(position) {
                return Ok(value.clone());
            }
        }
    }
    evaluate_expression(source, schema, expr)
}

/// ORDER BY comparison mirroring the general engine (`compare_order_keys`):
/// `NULLS FIRST` defaults from direction, ties broken by the next key.
fn compare_txn_order_keys(
    a: &[Value],
    b: &[Value],
    order_by: &[OrderByItem],
) -> std::cmp::Ordering {
    for (index, item) in order_by.iter().enumerate() {
        let (Some(va), Some(vb)) = (a.get(index), b.get(index)) else {
            continue;
        };
        let nulls_first = item.nulls_first.unwrap_or(item.descending);
        let cmp = match (va.is_null(), vb.is_null()) {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) if nulls_first => std::cmp::Ordering::Less,
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) if nulls_first => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => value_cmp(va, vb).unwrap_or(std::cmp::Ordering::Equal),
        };
        let cmp = if item.descending && !va.is_null() && !vb.is_null() {
            cmp.reverse()
        } else {
            cmp
        };
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

/// Combines adjacent plain `INSERT ... VALUES` statements into the existing
/// multi-row INSERT path. Richer forms remain separate to preserve their
/// result and conflict semantics.
fn coalesce_insert_statements(stmts: Vec<Statement>) -> Vec<Statement> {
    let mut out = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let Statement::Insert {
            table,
            columns,
            source,
            returning,
            on_conflict,
        } = stmt
        else {
            out.push(stmt);
            continue;
        };
        let mut source = Some(source);
        let mut merged = false;
        if let Some(Statement::Insert {
            table: previous_table,
            columns: previous_columns,
            source: previous_source,
            returning: previous_returning,
            on_conflict: previous_conflict,
        }) = out.last_mut()
        {
            if *previous_table == table
                && *previous_columns == columns
                && previous_returning.is_none()
                && returning.is_none()
                && previous_conflict.is_none()
                && on_conflict.is_none()
            {
                if let (
                    plomid_sql::InsertSource::Values(previous_rows),
                    Some(plomid_sql::InsertSource::Values(rows)),
                ) = (previous_source, source.as_mut())
                {
                    previous_rows.append(rows);
                    merged = true;
                }
            }
        }
        if !merged {
            out.push(Statement::Insert {
                table,
                columns,
                source: source.take().expect("insert source present"),
                returning,
                on_conflict,
            });
        }
    }
    out
}
