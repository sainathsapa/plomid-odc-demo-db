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
//! Relation materialization and the join algorithms (nested-loop, hash, indexed, lateral).

use crate::catalog_fn::is_session_function;
use crate::coerce::coerce_comparison_operands;
use crate::encoding::decode_row;
use crate::error::{SqlError, SqlResult};
use crate::row::values_equal;
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::{
    ColumnType, Expression, FromClause, InMemoryCatalog, JoinKind, QueryResult, TableSchema, Value,
};
use plomid_txn::StorageEngine;
use std::collections::HashMap;
use std::collections::HashSet;

use super::{
    aggregate::concat_slices,
    bloom::{hash_key, BloomFilter},
    eval::JoinEval,
    hash::{int_key_bytes, join_key_bytes, HashJoinTable},
    reorder::{choose_build_side, estimate_join_cardinality, BuildSide},
    scope::{effective_alias, resolve_column, synthetic_schema, JoinScope, OuterContext},
    statement::{
        execute_statement, generate_series_row_count, is_json_each_function, srf_default_columns,
        srf_result_rows,
    },
};
use plomid_sql::Catalog;
use std::hash::{Hash, Hasher};

pub fn unsupported(message: impl Into<String>) -> SqlError {
    SqlError::Storage(PlomidError::new(ErrorKind::Unsupported, message))
}

/// Adapts the join engine's expression evaluator to the document modality.
///
/// `JSON_TABLE`'s context-item and path arguments are SQL expressions, so the
/// document crate asks the host to evaluate them via
/// [`plomid_json::table::ExpressionEval`]. This adapter is what lets the
/// `JSON_TABLE` algorithm live in `plomid-json` without that crate depending on
/// the SQL AST or on this engine.
pub(super) struct SqlExpressionEval<'a, E: StorageEngine> {
    engine: &'a mut E,
    catalog: &'a InMemoryCatalog,
    outer: Option<&'a OuterContext>,
    depth: usize,
    current_database: &'a str,
    current_user: &'a str,
}

impl<E: StorageEngine> plomid_json::table::ExpressionEval for SqlExpressionEval<'_, E> {
    type Expr = Expression;

    fn eval(&mut self, expr: &Expression) -> SqlResult<Value> {
        JoinEval::eval_join_expr(
            &mut *self.engine,
            self.catalog,
            &[],
            &[],
            expr,
            self.outer,
            self.depth,
            self.current_database,
            self.current_user,
        )
    }
}

pub(super) fn postgres_keyword_rows() -> Vec<Vec<Value>> {
    // pg_get_keywords() is a stable PostgreSQL catalog function. These are
    // the keywords understood by Plomid's SQL parser; exposing them through
    // the normal table-function path lets JDBC/DBeaver complete startup
    // metadata queries without a client-specific response.
    [
        ("select", "unreserved", "U"),
        ("insert", "unreserved", "U"),
        ("update", "unreserved", "U"),
        ("delete", "unreserved", "U"),
        ("create", "unreserved", "U"),
        ("table", "unreserved", "U"),
        ("schema", "unreserved", "U"),
        ("from", "reserved", "R"),
        ("where", "reserved", "R"),
        ("join", "reserved", "R"),
    ]
    .into_iter()
    .map(|(word, description, code)| {
        vec![
            Value::Name(word.to_string()),
            Value::Text(description.to_string()),
            Value::BpChar(code.to_string()),
        ]
    })
    .collect()
}

/// Materializes a `FROM` tree into join scopes plus flattened rows.
pub(super) fn materialize_relation<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    from: &FromClause,
    outer: Option<&OuterContext>,
    depth: usize,
    current_database: &str,
    current_user: &str,
) -> SqlResult<(Vec<JoinScope>, Vec<Vec<Value>>)> {
    match from {
        FromClause::TableFunction {
            name,
            args,
            alias,
            column_aliases,
            column_defs,
            json_columns,
            lateral: _,
        } => {
            if name.eq_ignore_ascii_case("pg_catalog.pg_get_keywords")
                || name.eq_ignore_ascii_case("pg_get_keywords")
            {
                let schema = synthetic_schema(
                    alias.as_deref().unwrap_or(name),
                    &[
                        "word".to_string(),
                        "catdesc".to_string(),
                        "catcode".to_string(),
                    ],
                    &[
                        Some(ColumnType::new(
                            plomid_types::TypeOid::NAME,
                            plomid_types::NO_TYPEMOD,
                        )),
                        Some(ColumnType::new(
                            plomid_types::TypeOid::TEXT,
                            plomid_types::NO_TYPEMOD,
                        )),
                        Some(ColumnType::new(
                            plomid_types::TypeOid::CHAR,
                            plomid_types::NO_TYPEMOD,
                        )),
                    ],
                );
                return Ok((
                    vec![JoinScope {
                        alias: alias.clone().unwrap_or_else(|| name.clone()),
                        schema,
                        offset: 0,
                        is_srf: false,
                    }],
                    postgres_keyword_rows(),
                ));
            }
            if name.eq_ignore_ascii_case("generate_series") {
                if args.len() != 2 && args.len() != 3 {
                    return Err(unsupported(
                        "generate_series requires two or three integer arguments",
                    ));
                }
                let values = args
                    .iter()
                    .map(|arg| {
                        JoinEval::eval_join_expr(
                            engine,
                            catalog,
                            &[],
                            &[],
                            arg,
                            outer,
                            depth,
                            current_database,
                            current_user,
                        )
                    })
                    .collect::<SqlResult<Vec<_>>>()?;
                let as_i64 = |value: &Value| -> SqlResult<i64> {
                    match value {
                        Value::Int2(v) => Ok(i64::from(*v)),
                        Value::Int4(v) => Ok(i64::from(*v)),
                        Value::Int8(v) => Ok(*v),
                        Value::Numeric(v) => v
                            .to_string()
                            .parse::<i64>()
                            .map_err(|_| unsupported("generate_series arguments must be integers")),
                        _ => Err(unsupported("generate_series arguments must be integers")),
                    }
                };
                let start = as_i64(&values[0])?;
                let stop = as_i64(&values[1])?;
                let step = values.get(2).map(as_i64).transpose()?.unwrap_or(1);
                if step == 0 {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::InvalidArgument,
                        "step size cannot equal zero",
                    )));
                }
                // Same generation-time cap as the target-list shape
                // (`generate_series_row_count`): exact count first, reject
                // before allocating, since downstream LIMITs cannot shrink an
                // already-materialized series.
                let count = generate_series_row_count(start, stop, step)?;
                let mut rows = Vec::with_capacity(count as usize);
                let mut current = start;
                let mut produced: usize = 0;
                while (step > 0 && current <= stop) || (step < 0 && current >= stop) {
                    rows.push(vec![Value::Int8(current)]);
                    produced += 1;
                    // Cooperative statement_timeout/cancel: abort long series
                    // builds without touching sibling-owned paths.
                    if produced & 1023 == 0 {
                        crate::governance::poll_guard()?;
                    }
                    current = current.checked_add(step).ok_or_else(|| {
                        SqlError::Storage(PlomidError::new(
                            ErrorKind::InvalidArgument,
                            "generate_series result overflow",
                        ))
                    })?;
                }
                let relation_name = alias.clone().unwrap_or_else(|| name.clone());
                let column = column_aliases
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "generate_series".into());
                let schema = synthetic_schema(
                    &relation_name,
                    &[column],
                    &[Some(ColumnType::new(
                        plomid_types::TypeOid::INT8,
                        plomid_types::NO_TYPEMOD,
                    ))],
                );
                return Ok((
                    vec![JoinScope {
                        alias: relation_name,
                        schema,
                        offset: 0,
                        is_srf: true,
                    }],
                    rows,
                ));
            }
            // JSON set-returning functions in FROM (`FROM json_each(...) AS
            // t(key, value)`): evaluate the argument once and expand the
            // result into the relation's rows.
            if is_json_each_function(name)
                || matches!(
                    name.to_ascii_lowercase().as_str(),
                    "json_array_elements_text"
                        | "jsonb_array_elements_text"
                        | "jsonb_array_elements"
                        | "json_array_elements"
                )
            {
                let alias_name = alias.clone().unwrap_or_else(|| name.clone());
                // These functions convert a JSON document (object for
                // `json_each`, array for `jsonb_array_elements`/`_text`) into a
                // `Value::Array` of output rows, so evaluate the whole call --
                // including the array/record conversion and SQL NULL
                // propagation -- rather than feeding the raw argument to the
                // row expander.
                let call = Expression::FunctionCall {
                    name: name.clone(),
                    args: args.clone(),
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    returning: None,
                    null_handling: None,
                    unique_keys: None,
                };
                let value = JoinEval::eval_join_expr(
                    engine,
                    catalog,
                    &[],
                    &[],
                    &call,
                    outer,
                    depth,
                    current_database,
                    current_user,
                )?;
                let (column_types, rows) = srf_result_rows(value)?;
                let columns = if column_aliases.is_empty() {
                    srf_default_columns(name)
                } else {
                    column_aliases.clone()
                };
                let schema = synthetic_schema(
                    &alias_name,
                    &columns,
                    &column_types.into_iter().map(Some).collect::<Vec<_>>(),
                );
                return Ok((
                    vec![JoinScope {
                        alias: alias_name,
                        schema,
                        offset: 0,
                        is_srf: true,
                    }],
                    rows,
                ));
            }
            // json_to_record / jsonb_to_record / json_populate_record /
            // jsonb_populate_record / json_populate_recordset /
            // jsonb_populate_recordset: expand a JSON value into a record
            // (or set of records) with the column types declared in the
            // AS x(col type, ...) clause.
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "json_to_record"
                    | "jsonb_to_record"
                    | "json_to_recordset"
                    | "jsonb_to_recordset"
                    | "json_populate_record"
                    | "jsonb_populate_record"
                    | "json_populate_recordset"
                    | "jsonb_populate_recordset"
            ) {
                let alias_name = alias.clone().unwrap_or_else(|| name.clone());
                let json_arg = args
                    .first()
                    .ok_or_else(|| unsupported(format!("{name} requires a JSON argument")))?;
                let json_value = JoinEval::eval_join_expr(
                    engine,
                    catalog,
                    &[],
                    &[],
                    json_arg,
                    outer,
                    depth,
                    current_database,
                    current_user,
                )?;
                // Determine output columns from column_defs, falling back to
                // column_aliases, then to default names.
                let (columns, col_types) = if !column_defs.is_empty() {
                    let cols: Vec<String> = column_defs.iter().map(|(n, _)| n.clone()).collect();
                    let types: Vec<Option<ColumnType>> = column_defs
                        .iter()
                        .map(|(_, t)| ColumnType::from_type_name(t.as_str()))
                        .collect();
                    (cols, types)
                } else if !column_aliases.is_empty() {
                    (column_aliases.clone(), vec![None; column_aliases.len()])
                } else {
                    (vec![name.to_ascii_lowercase()], vec![None])
                };
                let is_recordset = name.to_ascii_lowercase().contains("recordset");
                let rows = if is_recordset {
                    plomid_json::table::json_recordset_rows(&json_value, &col_types)?
                } else {
                    plomid_json::table::json_record_rows(&json_value, &col_types)?
                };
                let schema = synthetic_schema(
                    &alias_name,
                    &columns,
                    &col_types
                        .into_iter()
                        .map(|t| t.or_else(|| Some(ColumnType::text())))
                        .collect::<Vec<_>>(),
                );
                return Ok((
                    vec![JoinScope {
                        alias: alias_name,
                        schema,
                        offset: 0,
                        is_srf: true,
                    }],
                    rows,
                ));
            }
            // JSON_TABLE: `JSON_TABLE(json_expr, 'path' COLUMNS (...))`.  The
            // COLUMNS specs were parsed by `parse_json_table_call` and attached
            // to this FromClause::TableFunction node in `json_columns`.  We
            // evaluate the JSON document and path, expand the path to a set of
            // JSON elements, and then extract each declared column from every
            // element.
            if name.eq_ignore_ascii_case("json_table") {
                let jt_rows = {
                    let mut evaluator = SqlExpressionEval {
                        engine: &mut *engine,
                        catalog,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    };
                    plomid_json::table::json_table_rows(&mut evaluator, args, json_columns)?
                };
                let alias_name = alias.clone().unwrap_or_else(|| name.clone());
                let (col_names, col_types) = plomid_json::table::json_table_schema(json_columns);
                let schema = synthetic_schema(
                    &alias_name,
                    &col_names,
                    &col_types.into_iter().map(Some).collect::<Vec<_>>(),
                );
                return Ok((
                    vec![JoinScope {
                        alias: alias_name,
                        schema,
                        offset: 0,
                        is_srf: true,
                    }],
                    jt_rows,
                ));
            }
            let relation = crate::system_catalog::SystemCatalog::new(
                catalog,
                current_database,
                current_user,
                catalog.database_names(),
            )
            .relation(name)
            .ok_or_else(|| unsupported(format!("table function \"{name}\" does not exist")))?;
            Ok((
                vec![JoinScope {
                    alias: alias.clone().unwrap_or_else(|| name.clone()),
                    schema: relation.schema,
                    offset: 0,
                    is_srf: false,
                }],
                relation.rows,
            ))
        }
        FromClause::Table { name, alias } => {
            if let Some(relation) = crate::system_catalog::SystemCatalog::new(
                catalog,
                current_database,
                current_user,
                catalog.database_names(),
            )
            .relation(name)
            {
                let alias = effective_alias(name, alias);
                return Ok((
                    vec![JoinScope {
                        alias,
                        schema: relation.schema,
                        offset: 0,
                        is_srf: false,
                    }],
                    relation.rows,
                ));
            }
            // Views resolve to their defining query materialized as a subquery.
            if let Some(view) = catalog.get_view(name) {
                let subquery = FromClause::Subquery {
                    statement: view.query.clone(),
                    alias: alias
                        .clone()
                        .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name).to_string()),
                    column_aliases: Vec::new(),
                    lateral: false,
                };
                return materialize_relation(
                    engine,
                    catalog,
                    &subquery,
                    outer,
                    depth,
                    current_database,
                    current_user,
                );
            }
            let alias = effective_alias(name, alias);
            let schema = catalog.get_table(name)?.clone();
            let entries = engine.scan(
                Some(format!("{name}:").as_bytes()),
                Some(format!("{name}:\u{10FFFF}").as_bytes()),
            )?;
            let rows = entries
                .iter()
                .map(|(_, bytes)| decode_row(bytes))
                .collect::<plomid_core::Result<Vec<_>>>()?;
            Ok((
                vec![JoinScope {
                    alias,
                    schema,
                    offset: 0,
                    is_srf: false,
                }],
                rows,
            ))
        }
        FromClause::Subquery {
            statement,
            alias,
            column_aliases,
            lateral: _,
        } => {
            let result = execute_statement(
                engine,
                catalog,
                statement,
                current_database,
                current_user,
                outer,
                depth + 1,
            )?;
            let QueryResult::Rows {
                mut columns,
                column_types,
                rows,
            } = result
            else {
                return Err(unsupported("a subquery in FROM must produce rows"));
            };
            // Apply an explicit column alias list: `FROM (...) AS x(a, b)`.
            if !column_aliases.is_empty() {
                if column_aliases.len() != columns.len() {
                    return Err(unsupported(
                        "derived table column alias count does not match subquery columns",
                    ));
                }
                columns = column_aliases.clone();
            }
            let schema = synthetic_schema(alias, &columns, &column_types);
            Ok((
                vec![JoinScope {
                    alias: alias.clone(),
                    schema,
                    offset: 0,
                    is_srf: false,
                }],
                rows,
            ))
        }
        FromClause::Join {
            left,
            kind,
            right,
            on,
        } => {
            let (lscopes, lrows) = materialize_relation(
                engine,
                catalog,
                left,
                outer,
                depth,
                current_database,
                current_user,
            )?;
            let left_width: usize = lscopes.iter().map(JoinScope::width).sum();
            // LATERAL / correlated right side: PostgreSQL evaluates the right
            // relation once per left row with the left row visible. Table
            // functions (and subqueries) may reference left columns, so when
            // the right side is lateral-capable, materialize it per left row
            // instead of once with an empty scope.
            if from_clause_is_lateral(right) {
                let mut scopes = lscopes.clone();
                let mut right_width = 0usize;
                let mut right_scopes_template: Option<Vec<JoinScope>> = None;
                let mut per_left: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
                for lrow in &lrows {
                    let current = OuterContext {
                        scopes: lscopes.clone(),
                        row: lrow.clone(),
                        parent: outer.cloned().map(Box::new),
                    };
                    let (mut rscopes, rrows) = materialize_relation(
                        engine,
                        catalog,
                        right,
                        Some(&current),
                        depth,
                        current_database,
                        current_user,
                    )?;
                    for scope in &mut rscopes {
                        scope.offset += left_width;
                    }
                    right_width = right_width.max(rscopes.iter().map(JoinScope::width).sum());
                    if right_scopes_template.is_none() {
                        right_scopes_template = Some(rscopes.clone());
                        scopes.extend(rscopes);
                    }
                    per_left.push((lrow.clone(), rrows));
                }
                let _rscopes = right_scopes_template.unwrap_or_default();
                let rows = lateral_join_rows(
                    engine,
                    catalog,
                    &scopes,
                    per_left,
                    *kind,
                    on.as_ref(),
                    left_width,
                    right_width,
                    outer,
                    depth,
                    current_database,
                    current_user,
                )?;
                return Ok((scopes, rows));
            }
            let (mut rscopes, rrows) = materialize_relation(
                engine,
                catalog,
                right,
                outer,
                depth,
                current_database,
                current_user,
            )?;
            for scope in &mut rscopes {
                scope.offset += left_width;
            }
            let right_width: usize = rscopes.iter().map(JoinScope::width).sum();
            let mut scopes = lscopes.clone();
            scopes.extend(rscopes.clone());
            let rows = perform_join(
                engine,
                catalog,
                &scopes,
                &lrows,
                &rrows,
                *kind,
                on.as_ref(),
                left_width,
                right_width,
                outer,
                depth,
                current_database,
                current_user,
            )?;
            Ok((scopes, rows))
        }
    }
}

/// True when a FROM item may reference preceding FROM items (explicit
/// LATERAL, or a table function which PostgreSQL treats as implicitly
/// lateral when it references earlier columns).
pub(super) fn from_clause_is_lateral(from: &FromClause) -> bool {
    match from {
        FromClause::TableFunction { lateral, .. } => *lateral,
        FromClause::Subquery { lateral, .. } => *lateral,
        FromClause::Join { left, right, .. } => {
            from_clause_is_lateral(left) || from_clause_is_lateral(right)
        }
        FromClause::Table { .. } => false,
    }
}

/// Nested-loop join where the right rows were produced per left row.
/// For INNER/CROSS the combined rows are emitted directly; for OUTER joins
/// an unmatched left row is padded with NULLs, matching `perform_join`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lateral_join_rows<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    scopes: &[JoinScope],
    per_left: Vec<(Vec<Value>, Vec<Vec<Value>>)>,
    kind: JoinKind,
    on: Option<&Expression>,
    left_width: usize,
    right_width: usize,
    outer: Option<&OuterContext>,
    depth: usize,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<Vec<Value>>> {
    if on.is_some() && matches!(kind, JoinKind::Cross) {
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::InvalidArgument,
            format!("{kind:?} JOIN requires an ON or USING clause"),
        )));
    }
    let right_nulls = vec![Value::Null; right_width];
    let mut out = Vec::new();
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            for (li, (lrow, rrows)) in per_left.iter().enumerate() {
                if li & 1023 == 0 {
                    crate::governance::poll_guard()?;
                }
                for rrow in rrows {
                    let combined = concat_slices(lrow, rrow);
                    if join_predicate_true(
                        engine,
                        catalog,
                        scopes,
                        &combined,
                        on,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    )? {
                        push_join_row(&mut out, combined)?;
                    }
                }
            }
        }
        JoinKind::Left => {
            for (li, (lrow, rrows)) in per_left.iter().enumerate() {
                if li & 1023 == 0 {
                    crate::governance::poll_guard()?;
                }
                let mut any = false;
                for rrow in rrows {
                    let combined = concat_slices(lrow, rrow);
                    if join_predicate_true(
                        engine,
                        catalog,
                        scopes,
                        &combined,
                        on,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    )? {
                        push_join_row(&mut out, combined)?;
                        any = true;
                    }
                }
                if !any {
                    push_join_row(&mut out, concat_slices(lrow, &right_nulls))?;
                }
            }
        }
        JoinKind::Right | JoinKind::Full => {
            return Err(unsupported(
                "RIGHT/FULL JOIN with LATERAL right side is not supported",
            ));
        }
    }
    let _ = left_width;
    Ok(out)
}

/// Evaluates the join predicate for one candidate combined row.
#[allow(clippy::too_many_arguments)]
pub(super) fn join_predicate_true<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    scopes: &[JoinScope],
    combined: &[Value],
    on: Option<&Expression>,
    outer: Option<&OuterContext>,
    depth: usize,
    current_database: &str,
    current_user: &str,
) -> SqlResult<bool> {
    let Some(expr) = on else {
        return Ok(true);
    };
    let v = JoinEval::eval_join_expr(
        engine,
        catalog,
        scopes,
        combined,
        expr,
        outer,
        depth,
        current_database,
        current_user,
    )?;
    Ok(matches!(v, Value::Bool(true)))
}

/// Returns the left/right row positions for a plain equi-join predicate.
/// The hash table is only a candidate generator; the original predicate is
/// still evaluated for every candidate so coercion and additional semantics
/// remain authoritative.
pub(super) fn equi_join_columns(
    scopes: &[JoinScope],
    on: Option<&Expression>,
    left_width: usize,
) -> Option<(usize, usize)> {
    let Some(Expression::Equal(left, right)) = on else {
        return None;
    };
    let (Expression::ColumnRef(left_name), Expression::ColumnRef(right_name)) = (&**left, &**right)
    else {
        return None;
    };
    let left_index = resolve_column(scopes, None, left_name).ok()?;
    let right_index = resolve_column(scopes, None, right_name).ok()?;
    match (left_index < left_width, right_index < left_width) {
        (true, false) => Some((left_index, right_index - left_width)),
        (false, true) => Some((right_index, left_index - left_width)),
        _ => None,
    }
}

/// Produces a typed, allocation-free hash fingerprint for the scalar values
/// normally used as join keys. The original predicate is still rechecked, so
/// a fingerprint collision can only add a candidate, never change results.
/// Complex values deliberately decline the hash path and use the existing
/// nested-loop semantics instead of inventing a second equality model.
pub(super) fn join_hash_key(value: &Value) -> Option<u64> {
    // Fixed-seed FNV-1a (see `crate::util::FastHasher`): this fingerprint is
    // only a candidate filter (every candidate is rechecked through the
    // authoritative predicate) and a partition router (equal keys share a
    // partition), so the engine — not the seed — is what matters. The
    // per-key-type call sequence is unchanged.
    let mut hasher = crate::util::FastHasher::new();
    match value {
        Value::Bool(v) => (0_u8, *v).hash(&mut hasher),
        Value::Int2(v) => (1_u8, *v as i128).hash(&mut hasher),
        Value::Int4(v) => (1_u8, *v as i128).hash(&mut hasher),
        Value::Int8(v) => (1_u8, *v as i128).hash(&mut hasher),
        Value::Numeric(v) => (4_u8, v.mantissa, v.scale).hash(&mut hasher),
        Value::Float4(v) => (5_u8, v.to_bits()).hash(&mut hasher),
        Value::Float8(v) => (6_u8, v.to_bits()).hash(&mut hasher),
        Value::Money(v) => (7_u8, *v).hash(&mut hasher),
        Value::BpChar(v) => (8_u8, v.as_bytes()).hash(&mut hasher),
        Value::VarChar(v) => (8_u8, v.as_bytes()).hash(&mut hasher),
        Value::Text(v) => (8_u8, v.as_bytes()).hash(&mut hasher),
        Value::Name(v) => (8_u8, v.as_bytes()).hash(&mut hasher),
        Value::Date(v) => (12_u8, *v).hash(&mut hasher),
        Value::Time(v) => (13_u8, *v).hash(&mut hasher),
        Value::TimeTz {
            micros,
            offset_secs,
        } => (14_u8, *micros, *offset_secs).hash(&mut hasher),
        Value::Timestamp(v) => (15_u8, *v).hash(&mut hasher),
        Value::Timestamptz(v) => (16_u8, *v).hash(&mut hasher),
        Value::Uuid(v) => (17_u8, v).hash(&mut hasher),
        Value::Bytea(v) => (18_u8, v.as_slice()).hash(&mut hasher),
        Value::Json(v) => (19_u8, v.as_bytes()).hash(&mut hasher),
        Value::Jsonb(v) => (20_u8, v.as_slice()).hash(&mut hasher),
        Value::Macaddr(v) => (21_u8, v).hash(&mut hasher),
        Value::Macaddr8(v) => (22_u8, v).hash(&mut hasher),
        Value::Oid(v) => (23_u8, *v).hash(&mut hasher),
        Value::Tid { block, offset } => (24_u8, *block, *offset).hash(&mut hasher),
        Value::Xid(v) => (25_u8, *v).hash(&mut hasher),
        Value::Cid(v) => (26_u8, *v).hash(&mut hasher),
        Value::PgLsn(v) => (27_u8, *v).hash(&mut hasher),
        _ => return None,
    }
    Some(hasher.finish())
}

/// Resolves a FROM relation that is a plain base table (not a system
/// catalog relation and not a view), which is the only shape
/// [`try_indexed_join`] serves directly.
pub(super) fn plain_table_schema(
    catalog: &InMemoryCatalog,
    current_database: &str,
    current_user: &str,
    name: &str,
) -> Option<TableSchema> {
    if crate::system_catalog::SystemCatalog::new(
        catalog,
        current_database,
        current_user,
        catalog.database_names(),
    )
    .relation(name)
    .is_some()
    {
        return None;
    }
    if catalog.get_view(name).is_some() {
        return None;
    }
    catalog.get_table(name).ok().cloned()
}

/// Serves a plain two-table INNER JOIN from indexes instead of materializing
/// both relations with full table scans.
///
/// The general path ([`materialize_relation`] + [`perform_join`]) reads every
/// row of *both* relations and only afterwards applies the WHERE, so a
/// selective join such as
///
/// ```text
/// SELECT ... FROM events e JOIN customers c ON e.customer_id = c.customer_id
/// WHERE e.id = 42
/// ```
///
/// pays for a full scan of both tables (and a hash build over the whole right
/// relation) to return a single row. This path instead:
///
/// 1. serves the LEFT relation from its own single-column index when the WHERE
///    holds a conjunct `<left>.<col> = <constant>` — the same provably-safe
///    equality the `UPDATE ... FROM` source-prune path uses — and
/// 2. when the ON is exactly `left.col = right.col` and the right column has a
///    single-column index, materializes the RIGHT relation by probing that
///    index for each left key value instead of scanning the whole table.
///
/// Both prunings yield a superset of the rows the join can match: a left
/// restriction to a WHERE conjunct can only remove rows an INNER JOIN's WHERE
/// would have rejected, and a right row can only join when its key equals some
/// left key. The pruned rows are handed to the unchanged [`perform_join`], so
/// the ON and WHERE predicates are still evaluated by the normal evaluator and
/// the result set is identical. Row order is preserved because both relations
/// are repopulated in storage-key order — which is exactly the order the full
/// scans produced.
///
/// Returns `Ok(None)` for every shape this does not prove safe (outer or cross
/// joins, non-table relations, no usable prune, a non-index-exact key), leaving
/// those queries on the existing path.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub(super) fn try_indexed_join<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    from: &FromClause,
    where_expr: Option<&Expression>,
    outer: Option<&OuterContext>,
    depth: usize,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Option<(Vec<JoinScope>, Vec<Vec<Value>>)>> {
    let FromClause::Join {
        left,
        kind: JoinKind::Inner,
        right,
        on,
    } = from
    else {
        return Ok(None);
    };
    if from_clause_is_lateral(right) {
        return Ok(None);
    }
    let (
        FromClause::Table {
            name: lname,
            alias: lalias,
        },
        FromClause::Table {
            name: rname,
            alias: ralias,
        },
    ) = (&**left, &**right)
    else {
        return Ok(None);
    };
    let Some(lschema) = plain_table_schema(catalog, current_database, current_user, lname) else {
        return Ok(None);
    };
    let Some(rschema) = plain_table_schema(catalog, current_database, current_user, rname) else {
        return Ok(None);
    };

    // (1) LEFT side: only an index-served left relation makes this path
    // worthwhile. Without a usable prune the general path runs unchanged, so
    // no behavior is altered for unselective joins.
    let lresolved = catalog
        .resolve_table_name(lname)
        .unwrap_or_else(|_| lname.to_string());
    let mut left_rows: Option<Vec<Vec<Value>>> = None;
    for prune in crate::update_from::source_prune_predicates(where_expr, left, catalog) {
        let Some(index) = catalog
            .all_indexes_for_table(&lresolved)
            .into_iter()
            .find(|ix| {
                ix.expression.is_none()
                    && crate::index::index_columns(ix) == [prune.column.as_str()]
            })
        else {
            continue;
        };
        let Some(column) = lschema.columns.get(prune.col_pos) else {
            continue;
        };
        let empty: Vec<Value> = Vec::new();
        let Ok(key) = crate::query::evaluate_expression(&empty, &lschema, &prune.const_expr) else {
            // Let the general path surface the binder/evaluator error.
            return Ok(None);
        };
        if matches!(key, Value::Null) || !crate::update_from::probe_key_is_index_exact(column, &key)
        {
            continue;
        }
        let prefix = crate::update_from::index_probe_prefix(&index.name, &key);
        let end = crate::index::prefix_end(&prefix);
        let probe_entries = engine.scan(Some(&prefix), Some(&end))?;
        // At most one decoded row per index entry (skips only shrink it).
        let mut rows = Vec::with_capacity(probe_entries.len());
        for (_, row_key) in probe_entries {
            let Some(bytes) = engine.get(&row_key)? else {
                continue; // index entry without a visible row: MVCC-safe
            };
            let row = decode_row(&bytes)?;
            // Re-check through `values_equal`, the predicate equality the
            // general path's WHERE would apply.
            if row
                .get(prune.col_pos)
                .is_some_and(|actual| values_equal(actual, &key))
            {
                rows.push(row);
            }
        }
        left_rows = Some(rows);
        break;
    }
    let Some(left_rows) = left_rows else {
        return Ok(None);
    };

    let left_width = lschema.columns.len();
    let scopes = vec![
        JoinScope {
            alias: effective_alias(lname, lalias),
            schema: lschema,
            offset: 0,
            is_srf: false,
        },
        JoinScope {
            alias: effective_alias(rname, ralias),
            schema: rschema.clone(),
            offset: left_width,
            is_srf: false,
        },
    ];
    let right_width = rschema.columns.len();
    if left_rows.is_empty() {
        // No qualifying left row means an INNER JOIN has no rows at all, so
        // the right relation need not be read.
        //
        // Observability: the index-served path answered this query, so it
        // counts even with zero rows. (The `query/select.rs`
        // `analytical_path` sites — vector, row_count, columnar_scan,
        // row_count_filter, row_group, par_scan_agg, par_streaming_agg —
        // share the same `note_analytical_path` API but are not incremented
        // here; they live in a module this change does not touch.)
        plomid_core::observability::note_analytical_path("indexed_join");
        return Ok(Some((scopes, Vec::new())));
    }

    // (2) RIGHT side: probe the right index for the left key values present.
    let mut right_rows: Option<Vec<Vec<Value>>> = None;
    if let Some((left_key, right_key)) = equi_join_columns(&scopes, on.as_ref(), left_width) {
        if let Some(column) = rschema.columns.get(right_key) {
            let rresolved = catalog
                .resolve_table_name(rname)
                .unwrap_or_else(|_| rname.to_string());
            if let Some(index) = catalog
                .all_indexes_for_table(&rresolved)
                .into_iter()
                .find(|ix| {
                    ix.expression.is_none()
                        && crate::index::index_columns(ix) == [column.name.as_str()]
                })
            {
                let mut distinct: Vec<Value> = Vec::new();
                let mut seen: HashSet<Vec<u8>> = HashSet::new();
                let mut usable = true;
                for row in &left_rows {
                    // Borrowed key read; the owned copy is made only for
                    // values that actually enter `distinct`.
                    let Some(value) = row.get(left_key) else {
                        continue;
                    };
                    if value.is_null() {
                        continue; // NULL never joins
                    }
                    if !crate::update_from::probe_key_is_index_exact(column, value) {
                        usable = false;
                        break;
                    }
                    if seen.insert(crate::index::index_value_bytes(value)) {
                        distinct.push(value.clone());
                    }
                }
                if usable {
                    // A row key may be reachable from several probe values (a
                    // non-unique index), so collect by row key: each matching
                    // row is decoded once, in storage-key (scan) order.
                    let mut collected: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
                        std::collections::BTreeMap::new();
                    for value in &distinct {
                        let prefix = crate::update_from::index_probe_prefix(&index.name, value);
                        let end = crate::index::prefix_end(&prefix);
                        for (_, row_key) in engine.scan(Some(&prefix), Some(&end))? {
                            if let Some(bytes) = engine.get(&row_key)? {
                                collected.entry(row_key).or_insert(bytes);
                            }
                        }
                    }
                    let mut rows = Vec::with_capacity(collected.len());
                    for (_, bytes) in collected {
                        rows.push(decode_row(&bytes)?);
                    }
                    right_rows = Some(rows);
                }
            }
        }
    }
    let right_rows = match right_rows {
        Some(rows) => rows,
        None => {
            // One decoded row per scanned entry: exact capacity.
            let table_entries = engine.scan(
                Some(format!("{rname}:").as_bytes()),
                Some(format!("{rname}:\u{10FFFF}").as_bytes()),
            )?;
            let mut rows = Vec::with_capacity(table_entries.len());
            for (_, bytes) in table_entries {
                rows.push(decode_row(&bytes)?);
            }
            rows
        }
    };

    let rows = perform_join(
        engine,
        catalog,
        &scopes,
        &left_rows,
        &right_rows,
        JoinKind::Inner,
        on.as_ref(),
        left_width,
        right_width,
        outer,
        depth,
        current_database,
        current_user,
    )?;
    // Observability: the index-served path answered this query (see the
    // note above about the `query/select.rs` sites sharing the same API).
    plomid_core::observability::note_analytical_path("indexed_join");
    Ok(Some((scopes, rows)))
}

/// Minimum combined probe + build row count (`lrows.len() + rrows.len()`)
/// for the parallel equi-INNER hash join to engage, on the Laptop tier
/// (see [`crate::parallel_budget::ParallelBudget`]).
///
/// Smaller joins decline (`None`) so the sequential path runs: thread
/// fan-out, per-partition table builds, and result concatenation only pay
/// off once hashing + probing dominate. The live gate reads the tiered
/// value ([`crate::parallel_budget::join_threshold`]); this const pins the
/// Laptop tier (Edge 8_000, Server 1_000).
///
/// Calibration (in-process, release profile, 12-core host, uniform INT
/// equi-INNER join of N probe rows x 500 build rows, sequential
/// `perform_join` vs `perform_join_parallel` with all workers; best-of-5/3
/// wall times from the `par_join_parallel` timing sweep):
/// * 200-probe (700 total):      sequential 0.42ms vs parallel 0.55ms (0.76x)
/// * 500-probe (1_000 total):    sequential 0.70ms vs parallel 0.74ms (0.95x)
/// * 1k-probe (1_500 total):     sequential 1.18ms vs parallel 1.07ms (1.10x)
/// * 2k-probe (2_500 total):     sequential 2.14ms vs parallel 1.62ms (1.32x)
/// * 5k-probe (5_500 total):     sequential 5.6ms  vs parallel 4.0ms  (1.40x)
/// * 20k-probe (20_500 total):   sequential 20.5ms vs parallel 14.0ms (1.46x)
/// * 100k-probe (100_500 total): sequential 106.2ms vs parallel 78.5ms (1.35x)
///
/// The crossover sits just above 1_000 total rows; the threshold keeps a
/// margin above it so small joins never pay fan-out overhead, while joins
/// past it gain ~1.3-1.5x on this class of hardware.
///
/// Laptop-tier spec: production reads the tiered delegate; tier tests
/// assert the delegate against this const under a Laptop pin.
#[allow(dead_code)]
pub const PARALLEL_JOIN_THRESHOLD: usize = 2_000;

/// Minimum build-side rows for the parallel hash-table build on the
/// Laptop tier (see [`crate::parallel_budget::ParallelBudget`]).
///
/// At or above this many build rows the sequential `perform_join` path fans
/// the build across budget-clamped workers while the probe stays sequential
/// (left-major row order is preserved either way); below it the sequential
/// `build_with_bloom` runs. The live gate reads the tiered value
/// ([`crate::parallel_budget::build_min_rows`]); this const pins the
/// Laptop tier (Edge 200_000, Server 25_000).
///
/// Laptop-tier spec: production reads the tiered delegate; tier tests
/// assert the delegate against this const under a Laptop pin.
#[allow(dead_code)]
pub const PARALLEL_BUILD_MIN_ROWS: usize = 50_000;

/// Per-partition build tables: key hash -> global build-side row indices.
/// Which relation is the build side is the caller's `choose_build_side`
/// decision; the stored indices always address that side's row slice (the
/// same shape as the sequential `right_buckets`, but split by partition).
/// Backed by the fixed-seed [`crate::util::FastBuildHasher`]: every key is a
/// `u64` fingerprint whose candidates are rechecked authoritatively, so the
/// DoS-resistant `RandomState` only costs per-row time here.
type JoinBuildTables = Vec<HashMap<u64, Vec<usize>, crate::util::FastBuildHasher>>;
/// Per-partition probe lists: `(global probe-side row index, key hash)` pairs.
/// The stored hash lets workers probe without rehashing.
type JoinProbeLists = Vec<Vec<(usize, u64)>>;

/// Partitions build + probe rows for the parallel hash join: row `r` with key
/// hash `h` belongs to partition `h % workers`, so equal keys always meet in
/// the same partition and no cross-partition candidate can exist. Which side
/// is built is the caller's `choose_build_side` decision (`build_side`
/// selects the build row slice and its key column; the other side is probed),
/// so the smaller input always owns the hash tables. Returns the
/// per-partition build tables (key hash -> global build-side row indices)
/// alongside the per-partition probe lists (`(global probe-side row index,
/// key hash)` pairs). NULL keys are dropped on both sides (an INNER join
/// never matches NULL, mirroring the sequential path). Returns `None` when
/// any non-NULL key is unhashable (`join_hash_key` declines): the caller falls
/// back to sequential, which applies nested-loop semantics for those values.
pub fn partition_hash_join_inputs(
    lrows: &[Vec<Value>],
    rrows: &[Vec<Value>],
    left_key: usize,
    right_key: usize,
    workers: usize,
    build_side: BuildSide,
) -> Option<(JoinBuildTables, JoinProbeLists)> {
    let (build_rows, build_key, probe_rows, probe_key) = match build_side {
        BuildSide::Left => (lrows, left_key, rrows, right_key),
        BuildSide::Right => (rrows, right_key, lrows, left_key),
    };
    let mut build_tables: Vec<HashMap<u64, Vec<usize>, crate::util::FastBuildHasher>> =
        Vec::with_capacity(workers);
    build_tables.resize_with(workers, HashMap::default);
    // Pre-size partitions for the even-distribution share: capacity only,
    // so skewed inputs behave exactly as before (they just grow one table).
    let build_hint = build_rows.len() / workers.max(1) + 1;
    for table in &mut build_tables {
        table.reserve(build_hint);
    }
    for (index, row) in build_rows.iter().enumerate() {
        // Borrowed key read: the hash only fingerprints the value, so no
        // owned copy is needed. Missing cells behave like NULL (never join).
        let Some(value) = row.get(build_key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let hash = join_hash_key(value)?;
        build_tables[(hash % workers as u64) as usize]
            .entry(hash)
            .or_default()
            .push(index);
    }
    let mut probe_parts: Vec<Vec<(usize, u64)>> = Vec::with_capacity(workers);
    probe_parts.resize_with(workers, Vec::new);
    let probe_hint = probe_rows.len() / workers.max(1) + 1;
    for part in &mut probe_parts {
        part.reserve(probe_hint);
    }
    for (index, row) in probe_rows.iter().enumerate() {
        let Some(value) = row.get(probe_key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let hash = join_hash_key(value)?;
        probe_parts[(hash % workers as u64) as usize].push((index, hash));
    }
    Some((build_tables, probe_parts))
}

/// Maximum rows any single join operator may emit.
///
/// Nested-loop and hash joins materialize their full output before LIMIT,
/// ORDER BY, or aggregation runs, so a cartesian (or an all-match equi-join)
/// of two large relations exhausts memory: 100k x 100k pairs is 10^10 rows
/// long before any row is consumed. The cap fails closed with a graceful
/// error instead of OOMing the backend. 10M output rows is far above
/// legitimate in-process join fan-out (the largest regression join is on the
/// order of 100k rows) while keeping one operator's footprint bounded.
/// Pure cartesians (`CROSS JOIN` / INNER with no ON, whose size is exactly
/// `left x right`) are rejected BEFORE allocating via the product pre-check
/// in [`perform_join`]; every other emission path goes through
/// [`push_join_row`], and the parallel hash workers share one atomic output
/// budget so skewed partitions cannot each grow to the cap.
///
/// Deliberately NOT a LIMIT pushdown: LIMIT is applied post-materialization
/// in `select.rs`, and threading it into every join shape (outer joins must
/// still track matched rows for NULL-padding) is not provably exact, so only
/// the cap is wired here.
pub(super) const MAX_JOIN_OUTPUT_ROWS: usize = 10_000_000;

fn join_output_limit_error(rows: u128) -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::InvalidArgument,
        format!(
            "join output of {rows} rows exceeds the maximum of {MAX_JOIN_OUTPUT_ROWS} rows; \
             narrow the join with a selective ON or WHERE clause"
        ),
    ))
}

/// Pushes one join output row, failing closed at [`MAX_JOIN_OUTPUT_ROWS`]
/// before the allocation blowup. Every `out.push` in the join operators
/// below must go through here (verified by inspection; the exact-size
/// cartesian pre-check in [`perform_join`] covers the no-predicate shape).
fn push_join_row(out: &mut Vec<Vec<Value>>, row: Vec<Value>) -> SqlResult<()> {
    if out.len() >= MAX_JOIN_OUTPUT_ROWS {
        return Err(join_output_limit_error(MAX_JOIN_OUTPUT_ROWS as u128 + 1));
    }
    out.push(row);
    Ok(())
}

/// Parallel equi-INNER hash join.
///
/// Handles ONLY `JoinKind::Inner` (plus `Cross` with an equi-ON, which takes
/// the same sequential code path today) where `equi_join_columns` resolves
/// AND every non-NULL build/probe key is hashable. Anything else — outer
/// joins, non-equi or complex predicates, unhashable key values,
/// sub-threshold inputs, fewer than 2 workers, or join keys shadowed by
/// session-function names (the evaluator substitutes the session value
/// instead of reading the column, so the hash partitioner would read a
/// different value) — returns `None` and the caller runs the unchanged
/// sequential `perform_join`.
///
/// Each worker probes its own probe slice against its own local build table
/// with the same candidate + authoritative-recheck loop as the sequential
/// path. Which side is built is the `choose_build_side` decision (the smaller
/// input is built, the larger probed); build/probe indices are mapped back to
/// left/right before the recheck and the emission, so output rows are always
/// `concat(left_row, right_row)` in original column order whatever side was
/// built. The recheck is `coerce_comparison_operands` + `values_equal`, which
/// is exactly what `JoinEval` computes for an `Equal(ColumnRef, ColumnRef)`
/// predicate (the only shape that reaches the workers), so the parallel
/// result matches the sequential result row-for-row as a multiset. Workers
/// share only `&` references (`Send + Sync`, no `unsafe`, no mutex); slices
/// concatenate in partition order, so output is deterministic but grouped by
/// partition rather than preserving the interleaved sequential order (SQL
/// result order without `ORDER BY` is unspecified; downstream `ORDER BY`,
/// aggregates, and `DISTINCT` are order-insensitive).
///
/// SKEW (explicitly no salting): a hot key lands entirely in one
/// partition, so skewed key distributions yield imbalanced partitions. The
/// `par_join_parallel` skew test records per-partition row counts to keep
/// the imbalance visible; salting is a later step.
///
/// Output cap: workers share one atomic row budget (flushed in batches of
/// 1024 to keep the atomic off the hot path). A per-worker quota would
/// false-positive on exactly this skew (one partition legitimately holding
/// most of an under-cap output), while no quota lets an all-match join OOM
/// before assembly. Whoever's flush crosses [`MAX_JOIN_OUTPUT_ROWS`] fails
/// the whole join closed; the post-assembly length check covers the
/// sub-batch overshoot (at most `workers x 1024` rows).
#[allow(clippy::too_many_arguments)]
pub fn perform_join_parallel(
    kind: JoinKind,
    on: Option<&Expression>,
    scopes: &[JoinScope],
    left_width: usize,
    lrows: &[Vec<Value>],
    rrows: &[Vec<Value>],
    workers: usize,
) -> SqlResult<Option<Vec<Vec<Value>>>> {
    if workers < 2 {
        return Ok(None);
    }
    if !matches!(kind, JoinKind::Inner | JoinKind::Cross) {
        return Ok(None);
    }
    if lrows.len().saturating_add(rrows.len()) < crate::parallel_budget::join_threshold() {
        return Ok(None);
    }
    let Some((left_key, right_key)) = equi_join_columns(scopes, on, left_width) else {
        return Ok(None);
    };
    // The evaluator substitutes session-function values for same-named column
    // references; the partitioner reads column values, so decline those
    // shapes instead of joining on a different value than the sequential path.
    if let Some(Expression::Equal(left, right)) = on {
        if let (Expression::ColumnRef(left_name), Expression::ColumnRef(right_name)) =
            (&**left, &**right)
        {
            if is_session_function(left_name) || is_session_function(right_name) {
                return Ok(None);
            }
        }
    }
    // The smaller side is the build input (`choose_build_side`); the larger
    // side streams as probes. Orientation only changes which slice owns the
    // hash tables — emitted rows stay `concat(left, right)`.
    let build_side = choose_build_side(lrows.len() as u64, rrows.len() as u64);
    let Some((build_tables, probe_parts)) =
        partition_hash_join_inputs(lrows, rrows, left_key, right_key, workers, build_side)
    else {
        return Ok(None);
    };
    let mut out: Vec<Vec<Value>> = Vec::new();
    let budget = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // `true` = a worker panicked: decline and let the caller run the
    // sequential path (which applies the same output cap).
    let declined = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for (table, probe) in build_tables.iter().zip(probe_parts.iter()) {
            let budget = std::sync::Arc::clone(&budget);
            handles.push(scope.spawn(move || {
                let mut local: Vec<Vec<Value>> = Vec::new();
                let mut pending: usize = 0;
                for &(probe_index, hash) in probe {
                    let Some(candidates) = table.get(&hash) else {
                        continue;
                    };
                    for &build_index in candidates {
                        // Indices live in build/probe space: map back to
                        // left/right so the recheck reads the original sides
                        // and the emitted row keeps left-major column order.
                        let (left_index, right_index) = match build_side {
                            BuildSide::Left => (build_index, probe_index),
                            BuildSide::Right => (probe_index, build_index),
                        };
                        let lrow = &lrows[left_index];
                        let rrow = &rrows[right_index];
                        // Borrowed recheck: `coerce_comparison_operands` is
                        // the identity for identical discriminants (and for
                        // NULLs, which never match an INNER join), so the
                        // common same-type path compares by reference with no
                        // clones. Only differing-type keys pay the owned
                        // coerce — the exact old path, unchanged.
                        let matched = match (lrow.get(left_key), rrow.get(right_key)) {
                            (Some(lval), Some(rval))
                                if std::mem::discriminant(lval) == std::mem::discriminant(rval) =>
                            {
                                values_equal(lval, rval)
                            }
                            (lval, rval) => {
                                let lval = lval.cloned().unwrap_or(Value::Null);
                                let rval = rval.cloned().unwrap_or(Value::Null);
                                let (left_coerced, right_coerced) =
                                    coerce_comparison_operands(lval, rval);
                                values_equal(&left_coerced, &right_coerced)
                            }
                        };
                        if matched {
                            local.push(concat_slices(lrow, rrow));
                            pending += 1;
                            if pending >= 1024 {
                                let prev =
                                    budget.fetch_add(pending, std::sync::atomic::Ordering::Relaxed);
                                pending = 0;
                                if prev.saturating_add(1024) > MAX_JOIN_OUTPUT_ROWS {
                                    return Err(join_output_limit_error(
                                        prev.saturating_add(1024) as u128
                                    ));
                                }
                            }
                        }
                    }
                }
                if pending > 0 {
                    budget.fetch_add(pending, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(local)
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok(part)) => out.extend(part),
                // A failed worker fails the join closed: the cap error (or
                // any future worker error) must surface, not silently fall
                // back and re-materialize the same blowup sequentially.
                Ok(Err(error)) => return Err(error),
                // A panicking worker must never lose rows: decline and let the
                // caller run the sequential path.
                Err(_) => return Ok(true),
            }
        }
        Ok(false)
    })?;
    if declined {
        return Ok(None);
    }
    if out.len() > MAX_JOIN_OUTPUT_ROWS {
        return Err(join_output_limit_error(out.len() as u128));
    }
    Ok(Some(out))
}

/// Nested-loop join over materialized left/right rows.
#[allow(clippy::too_many_arguments)]
pub(super) fn perform_join<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    scopes: &[JoinScope],
    lrows: &[Vec<Value>],
    rrows: &[Vec<Value>],
    kind: JoinKind,
    on: Option<&Expression>,
    left_width: usize,
    right_width: usize,
    outer: Option<&OuterContext>,
    depth: usize,
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<Vec<Value>>> {
    if on.is_none() && !matches!(kind, JoinKind::Inner | JoinKind::Cross) {
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::InvalidArgument,
            format!("{kind:?} JOIN requires an ON or USING clause"),
        )));
    }
    // Exact-size fast rejection for pure cartesians: INNER/CROSS with no ON
    // emits exactly `lrows.len() x rrows.len()` rows, so an oversized product
    // fails here with zero allocation and zero thread fan-out. The bound is
    // computed by `estimate_join_cardinality` with conservative selectivity
    // 1.0 (full product): every integer at or below the cap is exactly
    // representable as f64, so for all constructible inputs this decides
    // identically to an integer product (and reports the same row count);
    // astronomical products saturate to `u64::MAX` and still fail closed.
    // Equi-ON joins are deliberately NOT rejected here even though the
    // product upper-bounds their output: the product is not EXACT for them,
    // and an upper bound licenses sizing, not rejection — erroring on
    // `bound > MAX` rejects valid selective queries (falsified in-suite:
    // `par_join_parallel_inner_exactness` emits 18,184 rows from a
    // 10,001,500 product). Outer joins are excluded for the stronger reason
    // that NULL-padding adds rows beyond the product, so it is not even an
    // upper bound there. Both shapes stay on the per-push `push_join_row`
    // guard, which fails closed without false-positiving.
    if on.is_none() && matches!(kind, JoinKind::Inner | JoinKind::Cross) {
        let bound = estimate_join_cardinality(lrows.len() as u64, rrows.len() as u64, 1.0);
        if bound > MAX_JOIN_OUTPUT_ROWS as u64 {
            return Err(join_output_limit_error(bound as u128));
        }
    }
    // Route eligible equi-INNER joins to the parallel
    // partitioned hash join. A decline (`None`) falls through to the
    // unchanged sequential implementation below. The indexed terminal call
    // above targets this same function, so it benefits automatically.
    // Box-wide analytical worker budget via the
    // governance bridge: live memory-pressure level derates the claim
    // (level 0 under no pressure claims exactly as the direct budget claim).
    // On exhaustion the sequential path below runs, so tail behavior is
    // identical to the pre-budget code.
    if on.is_some() && matches!(kind, JoinKind::Inner | JoinKind::Cross) {
        let workers = crate::parallel_budget::available_workers();
        if let Some(dispatch) = crate::governance::claim_workers_pressure_aware(
            workers,
            crate::governance::current_pressure_level(),
        ) {
            if let Some(rows) = perform_join_parallel(
                kind,
                on,
                scopes,
                left_width,
                lrows,
                rrows,
                dispatch.workers(),
            )? {
                // Observability: the parallel partitioned hash join served
                // this query.
                plomid_core::observability::note_analytical_path("parallel_join");
                return Ok(rows);
            }
        }
    }
    let right_nulls = vec![Value::Null; right_width];
    let left_nulls = vec![Value::Null; left_width];
    let mut out: Vec<Vec<Value>> = Vec::new();
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            if let Some((left_key, right_key)) = equi_join_columns(scopes, on, left_width) {
                // Sequential equi-hash join on top of `HashJoinTable` with a
                // build-side Bloom pre-filter. The table keys are the
                // canonical `index_value_bytes` (exact: equal keys share a
                // bucket, no collisions); the Bloom gate (`hash_key` of the
                // canonical bytes) only skips probes that cannot match, and
                // every surviving candidate is rechecked through the
                // authoritative `join_predicate_true`, so the result multiset
                // is exact. Right-build/left-probe order is preserved: the
                // table stores build indices in build order and the probe
                // walks left rows in order.
                //
                // `join_hash_key` hashability is preserved as the fallback
                // gate: any non-NULL key it declines falls back to the
                // nested-loop path below, exactly as the old inline buckets
                // did (the canonical encoding would otherwise take the hash
                // path for those values).
                let mut hashable = true;
                for (index, row) in rrows.iter().enumerate() {
                    if index & 4095 == 0 {
                        crate::governance::poll_guard()?;
                    }
                    // Borrowed key read: hashability only fingerprints the
                    // value. A missing cell behaves like NULL (skipped).
                    if let Some(value) = row.get(right_key) {
                        if !value.is_null() && join_hash_key(value).is_none() {
                            hashable = false;
                            break;
                        }
                    }
                }
                // Large builds fan out across budget-clamped workers; the
                // probe stays sequential so output keeps left-major row
                // order. The build claims permits through the box-wide
                // budget (previously it fanned out `available_parallelism`
                // threads with no claim at all); on exhaustion the
                // sequential build below runs byte-identically.
                let built: Option<(HashJoinTable, BloomFilter)> = if hashable {
                    let parallel: Option<HashJoinTable> =
                        if rrows.len() >= crate::parallel_budget::build_min_rows() {
                            crate::parallel_budget::claim_dispatch_workers(
                                crate::parallel_budget::available_workers(),
                            )
                            .map(|dispatch| {
                                HashJoinTable::parallel_build(rrows, right_key, dispatch.workers())
                            })
                        } else {
                            None
                        };
                    if let Some(table) = parallel {
                        if table.is_empty() {
                            None
                        } else {
                            let mut bloom = BloomFilter::new(table.row_count(), 0.01);
                            for key in table.map.keys() {
                                bloom.insert(hash_key(key));
                            }
                            Some((table, bloom))
                        }
                    } else {
                        let (table, bloom) = HashJoinTable::build_with_bloom(rrows, right_key);
                        if table.is_empty() {
                            None
                        } else {
                            Some((table, bloom))
                        }
                    }
                } else {
                    None
                };
                if hashable {
                    if let Some((table, bloom)) = built.as_ref() {
                        for (li, lrow) in lrows.iter().enumerate() {
                            // Cooperative guard: abort long probes promptly.
                            if li & 1023 == 0 {
                                crate::governance::poll_guard()?;
                            }
                            let Some(value) = lrow.get(left_key) else {
                                continue;
                            };
                            if value.is_null() {
                                continue;
                            }
                            if join_hash_key(value).is_none() {
                                hashable = false;
                                break;
                            }
                            // Bloom gate before the table probe: a miss means
                            // no build row carries this key. Integer keys
                            // encode on the stack (no per-probe allocation);
                            // other keys encode once and the table is probed
                            // with those same bytes instead of re-encoding.
                            let int_bytes = int_key_bytes(value);
                            let owned_bytes;
                            let key_bytes: &[u8] = match int_bytes.as_ref() {
                                Some(buf) => &buf[..],
                                None => {
                                    let Some(bytes) = join_key_bytes(value) else {
                                        continue;
                                    };
                                    owned_bytes = bytes;
                                    &owned_bytes
                                }
                            };
                            if !bloom.may_contain(hash_key(key_bytes)) {
                                continue;
                            }
                            for &right_index in table.probe_bytes(key_bytes) {
                                let combined = concat_slices(lrow, &rrows[right_index]);
                                if join_predicate_true(
                                    engine,
                                    catalog,
                                    scopes,
                                    &combined,
                                    on,
                                    outer,
                                    depth,
                                    current_database,
                                    current_user,
                                )? {
                                    push_join_row(&mut out, combined)?;
                                }
                            }
                        }
                    }
                }
                if !hashable {
                    out.clear();
                    for (li, lrow) in lrows.iter().enumerate() {
                        if li & 255 == 0 {
                            crate::governance::poll_guard()?;
                        }
                        for rrow in rrows {
                            let combined = concat_slices(lrow, rrow);
                            if join_predicate_true(
                                engine,
                                catalog,
                                scopes,
                                &combined,
                                on,
                                outer,
                                depth,
                                current_database,
                                current_user,
                            )? {
                                push_join_row(&mut out, combined)?;
                            }
                        }
                    }
                }
            } else {
                for (li, lrow) in lrows.iter().enumerate() {
                    if li & 255 == 0 {
                        crate::governance::poll_guard()?;
                    }
                    for rrow in rrows {
                        let combined = concat_slices(lrow, rrow);
                        if join_predicate_true(
                            engine,
                            catalog,
                            scopes,
                            &combined,
                            on,
                            outer,
                            depth,
                            current_database,
                            current_user,
                        )? {
                            push_join_row(&mut out, combined)?;
                        }
                    }
                }
            }
        }
        JoinKind::Left => {
            for (li, lrow) in lrows.iter().enumerate() {
                if li & 1023 == 0 {
                    crate::governance::poll_guard()?;
                }
                let mut any = false;
                for rrow in rrows {
                    let combined = concat_slices(lrow, rrow);
                    if join_predicate_true(
                        engine,
                        catalog,
                        scopes,
                        &combined,
                        on,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    )? {
                        push_join_row(&mut out, combined)?;
                        any = true;
                    }
                }
                if !any {
                    push_join_row(&mut out, concat_slices(lrow, &right_nulls))?;
                }
            }
        }
        JoinKind::Right => {
            for (ri, rrow) in rrows.iter().enumerate() {
                if ri & 1023 == 0 {
                    crate::governance::poll_guard()?;
                }
                let mut any = false;
                for lrow in lrows {
                    let combined = concat_slices(lrow, rrow);
                    if join_predicate_true(
                        engine,
                        catalog,
                        scopes,
                        &combined,
                        on,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    )? {
                        push_join_row(&mut out, combined)?;
                        any = true;
                    }
                }
                if !any {
                    push_join_row(&mut out, concat_slices(&left_nulls, rrow))?;
                }
            }
        }
        JoinKind::Full => {
            let mut matched_right = vec![false; rrows.len()];
            for (li, lrow) in lrows.iter().enumerate() {
                if li & 1023 == 0 {
                    crate::governance::poll_guard()?;
                }
                let mut any = false;
                for (ri, rrow) in rrows.iter().enumerate() {
                    let combined = concat_slices(lrow, rrow);
                    if join_predicate_true(
                        engine,
                        catalog,
                        scopes,
                        &combined,
                        on,
                        outer,
                        depth,
                        current_database,
                        current_user,
                    )? {
                        push_join_row(&mut out, combined)?;
                        any = true;
                        matched_right[ri] = true;
                    }
                }
                if !any {
                    push_join_row(&mut out, concat_slices(lrow, &right_nulls))?;
                }
            }
            for (ri, rrow) in rrows.iter().enumerate() {
                if !matched_right[ri] {
                    push_join_row(&mut out, concat_slices(&left_nulls, rrow))?;
                }
            }
        }
    }
    Ok(out)
}
