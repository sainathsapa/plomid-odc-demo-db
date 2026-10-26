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
//! In-transaction SELECT over snapshot + write overlay (Beta 2 Phase 4).
//!
//! Explicit-transaction SELECTs cannot use the committed-state machinery:
//! freshness-gated columnar/vector/index paths would miss uncommitted
//! writes, and the shared SELECT executor resolves under committed
//! snapshots. Instead they run this dedicated row pipeline, which reads
//! every user table through the open transaction (`txn.scan`, overlay
//! merged over the statement snapshot) and reuses the engine-free
//! operators — predicate/projection/grouping/sorting evaluation and the
//! join-scope resolver — so semantics match the main path row-for-row.
//!
//! Deliberate boundaries (documented, see the transaction runbook):
//!
//! * no columnar/vector/ANN/index acceleration in explicit transactions
//!   (correctness first; overlay reads are exact scans);
//! * uncorrelated subqueries are evaluated once per statement and memoized;
//!   correlated subqueries evaluate per row with outer context (nested
//!   correlation inside compound expressions is rejected, not misrun);
//! * window functions, recursive CTEs, and table-function sources are
//!   rejected with explicit `Unsupported` errors;
//! * LIMIT/OFFSET apply after full materialization (explicit transactions
//!   are for OLTP-scale work; analytical scans belong in autocommit).

use super::group::execute_grouped_select;
use crate::distinct::apply_distinct;
use crate::encoding::decode_row;
use crate::error::{SqlError, SqlResult};
use crate::join::scope::{synthetic_schema, JoinScope, OuterContext};
use crate::join::JoinEval;
use crate::system_catalog::SystemCatalog;
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::{
    Catalog, ColumnDef, ColumnType, Expression, FromClause, InMemoryCatalog, JoinKind, OrderByItem,
    QueryResult, SelectTarget, Statement, TableSchema, Value,
};
use plomid_txn::{ConcurrentPlomidStorageEngine, ConcurrentTransaction, StorageEngineTransaction};
use std::collections::HashMap;

/// Evaluation context: the open transaction plus everything needed to
/// resolve names and evaluate engine-free operators.
pub(crate) struct TxnQuery<'a> {
    pub txn: &'a mut ConcurrentTransaction,
    pub facade: &'a mut ConcurrentPlomidStorageEngine,
    pub catalog: &'a InMemoryCatalog,
    pub database: &'a str,
    pub user: &'a str,
    pub database_names: &'a [String],
    /// Uncorrelated-subquery memo for this statement: debug-shaped AST →
    /// precomputed value/set/bool.
    memo: HashMap<String, Memo>,
}

#[derive(Clone)]
enum Memo {
    Bool(bool),
    Value(Value),
    Set(Vec<Value>),
}

/// One materialized input relation: join scopes (one entry, or the
/// concatenated scope list for joins) plus rows aligned to their
/// concatenation order.
struct Mat {
    scopes: Vec<JoinScope>,
    rows: Vec<Vec<Value>>,
}

impl Mat {
    fn single(alias: String, schema: TableSchema, rows: Vec<Vec<Value>>) -> Self {
        Self {
            scopes: vec![JoinScope {
                alias,
                schema,
                offset: 0,
                is_srf: false,
            }],
            rows,
        }
    }
}

/// Materialized query output: display columns, best-effort types, rows.
type ScopedRows = (Vec<String>, Vec<Option<ColumnType>>, Vec<Vec<Value>>);

/// A CTE/view/subquery definition available by name.
#[derive(Clone)]
struct NamedRows {
    columns: Vec<String>,
    types: Vec<Option<ColumnType>>,
    rows: Vec<Vec<Value>>,
}

/// Entry point: SELECT-family statement against the open transaction.
/// `outer` carries correlation context for subquery evaluation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn select_in_txn(
    txn: &mut ConcurrentTransaction,
    facade: &mut ConcurrentPlomidStorageEngine,
    catalog: &InMemoryCatalog,
    stmt: &Statement,
    database: &str,
    user: &str,
    database_names: &[String],
    outer: Option<&OuterContext>,
) -> SqlResult<QueryResult> {
    let mut q = TxnQuery {
        txn,
        facade,
        catalog,
        database,
        user,
        database_names,
        memo: HashMap::new(),
    };
    q.select_stmt(stmt, &[], outer)
}

impl<'a> TxnQuery<'a> {
    fn syscat(&self) -> SystemCatalog<'_> {
        SystemCatalog::new(self.catalog, self.database, self.user, self.database_names)
    }

    fn select_stmt(
        &mut self,
        stmt: &Statement,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        match stmt {
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
            } => self.select_block(
                targets,
                *distinct,
                distinct_on.as_deref().unwrap_or(&[]),
                from.as_ref(),
                where_expr.as_ref(),
                group_by.as_ref(),
                having.as_ref(),
                order_by,
                *limit,
                *offset,
                ctes,
                outer,
            ),
            Statement::With {
                recursive,
                ctes: with_ctes,
                body,
            } => {
                if *recursive {
                    return Err(unsupported(
                        "WITH RECURSIVE inside explicit transactions is not yet supported",
                    ));
                }
                let mut all = ctes.to_vec();
                for cte in with_ctes {
                    let materialized = self.materialize_query(&cte.query, &all, outer)?;
                    let mut columns: Vec<String> = materialized.0;
                    if !cte.columns.is_empty() {
                        if cte.columns.len() != columns.len() {
                            return Err(SqlError::Storage(PlomidError::new(
                                ErrorKind::InvalidArgument,
                                format!(
                                    "table \"{}\" has {} columns available but {} specified",
                                    cte.name,
                                    columns.len(),
                                    cte.columns.len()
                                ),
                            )));
                        }
                        columns = cte.columns.clone();
                    }
                    all.push((
                        cte.name.clone(),
                        NamedRows {
                            columns,
                            types: materialized.1,
                            rows: materialized.2,
                        },
                    ));
                }
                self.select_stmt(body, &all, outer)
            }
            Statement::SetOperation {
                op,
                all,
                left,
                right,
            } => self.select_setop(*op, *all, left, right, ctes, outer),
            Statement::Values(rows) => self.select_values(rows),
            _ => Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Unsupported,
                "only query statements run inside explicit transactions",
            ))),
        }
    }

    /// Materializes any query into (columns, types, rows) for CTE/subquery
    /// scopes and INSERT ... SELECT sources.
    fn materialize_query(
        &mut self,
        stmt: &Statement,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<ScopedRows> {
        match self.select_stmt(stmt, ctes, outer)? {
            QueryResult::Rows {
                columns,
                column_types,
                rows,
            } => Ok((columns, column_types, rows)),
            _ => Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "subquery did not produce rows",
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn select_block(
        &mut self,
        targets: &[SelectTarget],
        distinct: bool,
        distinct_on: &[Expression],
        from: Option<&FromClause>,
        where_expr: Option<&Expression>,
        group_by: Option<&plomid_sql::GroupByClause>,
        having: Option<&Expression>,
        order_by: &[OrderByItem],
        limit: Option<usize>,
        offset: Option<usize>,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        // Bare SELECT (no FROM): single implicit row.
        let from = match from {
            None => {
                return self.select_no_from(
                    targets,
                    distinct,
                    distinct_on,
                    where_expr,
                    group_by,
                    having,
                    order_by,
                    limit,
                    offset,
                    ctes,
                    outer,
                );
            }
            Some(from) => from,
        };
        // Materialize every scope, then run the shared pipeline.
        let mats = self.materialize_from(from, ctes, outer)?;
        self.run_pipeline(
            targets,
            distinct,
            distinct_on,
            where_expr,
            group_by,
            having,
            order_by,
            limit,
            offset,
            &mats,
            outer,
        )
    }

    /// Bare SELECT without FROM over one implicit row: an empty scope set
    /// with a single empty row runs the same pipeline (aggregates see one
    /// input row, bare expressions evaluate scopeless).
    #[allow(clippy::too_many_arguments)]
    fn select_no_from(
        &mut self,
        targets: &[SelectTarget],
        distinct: bool,
        distinct_on: &[Expression],
        where_expr: Option<&Expression>,
        group_by: Option<&plomid_sql::GroupByClause>,
        having: Option<&Expression>,
        order_by: &[OrderByItem],
        limit: Option<usize>,
        offset: Option<usize>,
        _ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        // Set-returning targets without FROM (`SELECT generate_series(1,10)`,
        // `SELECT 1000 - generate_series(1,3)`, `SELECT unnest(...)`): the
        // same SRF-target shapes the join engine serves, expanded here with
        // txn-local argument evaluation. Detection (`generate_series_target`,
        // `generate_series_expr_target`, `unnest_target`) and element
        // generation are shared with the main path — this branch adds no
        // function-specific logic.
        if targets.len() == 1 {
            if let Some(srf_rows) = self.expand_bare_generate_series(&targets[0], outer)? {
                return Ok(srf_rows);
            }
        }
        let mats = vec![Mat::single(
            "__bare".to_string(),
            synthetic_schema("__bare", &[], &[]),
            vec![Vec::new()],
        )];
        self.run_pipeline(
            targets,
            distinct,
            distinct_on,
            where_expr,
            group_by,
            having,
            order_by,
            limit,
            offset,
            &mats,
            outer,
        )
    }

    /// Shared row pipeline over materialized scopes (FROM and bare alike):
    /// WHERE → grouped-or-plain → ORDER BY → projection → DISTINCT →
    /// LIMIT/OFFSET.
    #[allow(clippy::too_many_arguments)]
    fn run_pipeline(
        &mut self,
        targets: &[SelectTarget],
        distinct: bool,
        distinct_on: &[Expression],
        where_expr: Option<&Expression>,
        group_by: Option<&plomid_sql::GroupByClause>,
        having: Option<&Expression>,
        order_by: &[OrderByItem],
        limit: Option<usize>,
        offset: Option<usize>,
        mats: &[Mat],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        if targets_need_grouping(targets, group_by) {
            return self.select_grouped(
                targets,
                distinct,
                distinct_on,
                where_expr,
                group_by,
                having,
                order_by,
                limit,
                offset,
                mats,
                outer,
            );
        }
        let combined = combine_scopes(mats);
        let mut kept = Vec::with_capacity(combined.rows.len());
        if let Some(where_expr) = where_expr {
            for row in &combined.rows {
                if self.eval_row_bool(&combined.scopes, row, where_expr, outer)? {
                    kept.push(row.clone());
                }
            }
        } else {
            kept = combined.rows;
        }
        let mut rows = kept;
        let scopes = combined.scopes;
        if !order_by.is_empty() {
            // ORDER BY aliases resolve against projection aliases first
            // (PostgreSQL); anything else resolves in the scopes.
            let resolved_order = self.resolve_order_items(targets, &scopes, order_by)?;
            rows = self.sort_scoped(&scopes, rows, &resolved_order, outer)?;
        }
        let expanded = expand_targets(targets, &scopes)?;
        let mut projected = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut out = Vec::with_capacity(expanded.len());
            for (target, _) in &expanded {
                out.push(self.eval_target(&scopes, row, target, outer)?);
            }
            projected.push(out);
        }
        let display: Vec<String> = expanded.iter().map(|(_, name)| name.clone()).collect();
        let column_types: Vec<Option<ColumnType>> = expanded
            .iter()
            .map(|(target, _)| self.target_type(&scopes, target))
            .collect();
        let mut result = QueryResult::Rows {
            columns: display.clone(),
            column_types: column_types.clone(),
            rows: projected,
        };
        if distinct {
            result = apply_distinct(result, true, &[], None, None);
        }
        if !distinct_on.is_empty() {
            return Err(unsupported(
                "DISTINCT ON inside explicit transactions is not yet supported",
            ));
        }
        let rows = match result {
            QueryResult::Rows { rows, .. } => rows,
            _ => Vec::new(),
        };
        let rows = apply_limit_offset(rows, limit, offset);
        Ok(QueryResult::Rows {
            columns: display,
            column_types,
            rows,
        })
    }

    /// Resolves ORDER BY items: output aliases first, then scope
    /// references. Positional `ORDER BY N` resolves to the Nth output
    /// column (PostgreSQL, 1-based), mirroring the main path's
    /// `order_keys` ordinal handling. The pre-sort rows here are
    /// scope-space, so the literal is replaced by the expanded
    /// target's own expression before sorting.
    fn positional_target_expr(target: &SelectTarget) -> SqlResult<Expression> {
        match target {
            SelectTarget::Expr { expr, .. } => Ok(expr.clone()),
            SelectTarget::Aliased { target, .. } => Self::positional_target_expr(target),
            // `eval_target` evaluates a bare `Function(name)` as a column
            // of that name; sort keys follow the same rule.
            SelectTarget::Function(name) => Ok(Expression::ColumnRef(name.clone())),
            SelectTarget::FunctionCall { name, args } => Ok(Expression::FunctionCall {
                name: name.clone(),
                args: args.clone(),
                distinct: false,
                filter: None,
                order_by: Vec::new(),
                returning: None,
                null_handling: None,
                unique_keys: None,
            }),
            // `All`/`QualifiedStar` are expanded before sorting and
            // `WindowFunction` is rejected there, so these are unreachable.
            other => Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!("ORDER BY position target {other:?} is not sortable"),
            ))),
        }
    }

    fn resolve_order_items(
        &self,
        targets: &[SelectTarget],
        scopes: &[JoinScope],
        order_by: &[OrderByItem],
    ) -> SqlResult<Vec<OrderByItem>> {
        // Alias map from expanded targets (display name → target expr).
        let expanded = expand_targets(targets, scopes)?;
        let mut out = Vec::with_capacity(order_by.len());
        for item in order_by {
            // Positional `ORDER BY N` resolves to the Nth output column
            // (PostgreSQL, 1-based), mirroring the main path's
            // `order_keys` ordinal handling. The pre-sort rows here are
            // scope-space, so the literal is replaced by the expanded
            // target's own expression before sorting.
            let position = match &item.expr {
                Expression::Literal(Value::Int2(n)) => Some(*n as i64),
                Expression::Literal(Value::Int4(n)) => Some(*n as i64),
                Expression::Literal(Value::Int8(n)) => Some(*n),
                _ => None,
            };
            if let Some(position) = position {
                let valid = position > 0 && (position as usize) <= expanded.len();
                if !valid {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::InvalidArgument,
                        format!("ORDER BY position {position} is not in select list"),
                    )));
                }
                let (target, _) = &expanded[(position as usize) - 1];
                out.push(OrderByItem {
                    expr: Self::positional_target_expr(target)?,
                    ..item.clone()
                });
                continue;
            }
            if let Expression::ColumnRef(name) = &item.expr {
                // Re-resolve output aliases to the target's own expression
                // in scope space.
                let aliased = expanded
                    .iter()
                    .find(|(_, display)| display.eq_ignore_ascii_case(name) || display == name)
                    .and_then(|(target, _)| match target {
                        SelectTarget::Expr { expr, .. } => Some(expr.clone()),
                        _ => None,
                    });
                if let Some(expr) = aliased {
                    out.push(OrderByItem {
                        expr,
                        ..item.clone()
                    });
                    continue;
                }
            }
            out.push(item.clone());
        }
        Ok(out)
    }

    /// Aggregate path over flattened scopes.
    #[allow(clippy::too_many_arguments)]
    fn select_grouped(
        &mut self,
        targets: &[SelectTarget],
        distinct: bool,
        distinct_on: &[Expression],
        where_expr: Option<&Expression>,
        group_by: Option<&plomid_sql::GroupByClause>,
        having: Option<&Expression>,
        order_by: &[OrderByItem],
        limit: Option<usize>,
        offset: Option<usize>,
        mats: &[Mat],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        // Flatten scopes into one synthetic schema; rewrite every reference
        // to its unique synthetic name (ambiguity fails closed here, never
        // silently bound to the wrong scope). Subqueries must be
        // uncorrelated here (no per-row context after grouping) and are
        // substituted first.
        let flat = flatten_scopes(mats)?;
        let view = FlatView::build_from_flat(&flat)?;
        let resolve = |name: &str| view.resolve(name);
        let combined = combine_scopes(mats);
        let mut rows = flat.rows;
        if let Some(where_expr) = where_expr {
            let where_sub = self.substitute(where_expr, &combined.scopes, None, outer)?;
            let where_rw = rewrite_refs(&where_sub, &resolve)?;
            rows = rows
                .into_iter()
                .map(|row| {
                    super::evaluate_predicate(&row, &flat.schema, &where_rw)
                        .map(|keep| keep.then_some(row))
                })
                .collect::<SqlResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
        }
        let targets_rw = targets
            .iter()
            .map(|t| rewrite_select_target(t, &resolve, &mut *self, &combined.scopes, outer))
            .collect::<SqlResult<Vec<_>>>()?;
        let group_rw = group_by
            .map(|g| rewrite_group_by(g, &resolve, &mut *self, &combined.scopes, outer))
            .transpose()?;
        let having_rw = having
            .map(|h| self.substitute(h, &combined.scopes, None, outer))
            .transpose()?
            .map(|h| rewrite_refs(&h, &resolve))
            .transpose()?;
        let (mut result, _display) = self.run_grouped(
            &flat.schema,
            &targets_rw,
            group_rw.as_ref(),
            having_rw.as_ref(),
            rows,
        )?;
        // Rename to canonical display columns FIRST so ORDER BY and
        // DISTINCT operate on user-visible names, not synthetic ones.
        let (columns, column_types) = self.grouped_display(targets, &combined.scopes);
        result = match result {
            QueryResult::Rows { rows, .. } => QueryResult::Rows {
                columns: columns.clone(),
                column_types: column_types.clone(),
                rows,
            },
            other => other,
        };
        // ORDER BY over grouped output: resolve against output aliases,
        // then output names, then (rewritten) input refs.
        if !order_by.is_empty() {
            result = self.sort_grouped_output(result, order_by)?;
        }
        if distinct || !distinct_on.is_empty() {
            result = apply_distinct(result, distinct, &[], None, None);
        }
        if let QueryResult::Rows { rows, .. } = result {
            let rows = apply_limit_offset(rows, limit, offset);
            return Ok(QueryResult::Rows {
                columns,
                column_types,
                rows,
            });
        }
        Ok(result)
    }

    fn run_grouped(
        &mut self,
        schema: &TableSchema,
        targets_rw: &[SelectTarget],
        group_by: Option<&plomid_sql::GroupByClause>,
        having: Option<&Expression>,
        rows: Vec<Vec<Value>>,
    ) -> SqlResult<(QueryResult, Vec<String>)> {
        let result = execute_grouped_select(
            schema,
            targets_rw,
            group_by.cloned(),
            having.cloned(),
            rows,
            self.database,
            self.user,
        )?;
        // Display names positional to targets (stars are pre-expanded by
        // the caller, so every target maps 1:1 to an output column).
        let display = match &result {
            QueryResult::Rows { columns, .. } => columns.clone(),
            _ => Vec::new(),
        };
        Ok((result, display))
    }

    /// ORDER BY over grouped output rows: items resolve against output
    /// aliases/names first (single output scope), else error.
    fn sort_grouped_output(
        &mut self,
        result: QueryResult,
        order_by: &[OrderByItem],
    ) -> SqlResult<QueryResult> {
        let (columns, column_types, rows) = match result {
            QueryResult::Rows {
                columns,
                column_types,
                rows,
            } => (columns, column_types, rows),
            other => return Ok(other),
        };
        // Output scope: display names as columns.
        let out_schema = TableSchema {
            name: "__grouped_out".to_string(),
            table_id: plomid_core::TableId::new(0),
            column_ids: (0..columns.len() as u64)
                .map(plomid_core::ColumnId::new)
                .collect(),
            columns: columns
                .iter()
                .zip(column_types.iter())
                .map(|(name, col_type)| ColumnDef {
                    name: name.clone(),
                    col_type: col_type.unwrap_or_else(ColumnType::text),
                    constraints: Vec::new(),
                })
                .collect(),
            constraints: Vec::new(),
        };
        let out_scope = JoinScope {
            alias: String::new(),
            schema: out_schema,
            offset: 0,
            is_srf: false,
        };
        let scopes = [out_scope];
        // Resolve each item against output names: qualifiers naming no
        // output scope are stripped to the bare column first (grouped
        // output scopes are anonymous; `users.name` means the `name`
        // output column). Anything else fails closed.
        let mut resolved = Vec::with_capacity(order_by.len());
        for item in order_by {
            let mut expr = self.substitute(&item.expr, &scopes, None, None)?;
            if let Expression::ColumnRef(name) = &expr {
                if let Some((qualifier, bare)) = name.rsplit_once('.') {
                    let known = scopes
                        .iter()
                        .any(|scope| scope.alias.eq_ignore_ascii_case(qualifier));
                    if !known {
                        expr = Expression::ColumnRef(bare.to_string());
                    }
                }
            }
            resolved.push(OrderByItem {
                expr,
                ..item.clone()
            });
        }
        let mut keyed = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(resolved.len());
            for item in &resolved {
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    &scopes,
                    None,
                    0,
                    self.database,
                    self.user,
                );
                keys.push(eval.eval(&row, &item.expr)?);
            }
            keyed.push((row, keys));
        }
        // Reuse the shared NULL-placement comparator via synthetic items.
        keyed.sort_by(|(_, a), (_, b)| compare_order_items(a, b, &resolved));
        Ok(QueryResult::Rows {
            columns,
            column_types,
            rows: keyed.into_iter().map(|(row, _)| row).collect(),
        })
    }

    /// Canonical output columns from the ORIGINAL targets (display names +
    /// best-effort types resolved in the pre-grouping scopes).
    fn grouped_display(
        &self,
        targets: &[SelectTarget],
        scopes: &[JoinScope],
    ) -> (Vec<String>, Vec<Option<ColumnType>>) {
        let expanded = expand_targets(targets, scopes).unwrap_or_default();
        (
            expanded.iter().map(|(_, name)| name.clone()).collect(),
            expanded
                .iter()
                .map(|(target, _)| self.target_type(scopes, target))
                .collect(),
        )
    }
}

fn unsupported(message: &str) -> SqlError {
    SqlError::Storage(PlomidError::new(ErrorKind::Unsupported, message))
}

fn apply_limit_offset(
    rows: Vec<Vec<Value>>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .skip(offset.unwrap_or(0))
        .take(limit.unwrap_or(usize::MAX))
        .collect()
}

/// True when the query needs the grouped-aggregation engine.
fn targets_need_grouping(
    targets: &[SelectTarget],
    group_by: Option<&plomid_sql::GroupByClause>,
) -> bool {
    if group_by.is_some() {
        return true;
    }
    targets.iter().any(|t| match t {
        SelectTarget::Function(name) => crate::catalog_fn::is_aggregate_function(name),
        SelectTarget::FunctionCall { name, .. } => crate::catalog_fn::is_aggregate_function(name),
        SelectTarget::Expr { expr, .. } => super::expression_has_aggregate(expr),
        SelectTarget::Aliased { target, .. } => match target.as_ref() {
            SelectTarget::Function(name) => crate::catalog_fn::is_aggregate_function(name),
            SelectTarget::FunctionCall { name, .. } => {
                crate::catalog_fn::is_aggregate_function(name)
            }
            SelectTarget::Expr { expr, .. } => super::expression_has_aggregate(expr),
            _ => false,
        },
        _ => false,
    })
}

/// Combined scopes + their row sets for the row pipeline.
struct Combined {
    scopes: Vec<JoinScope>,
    rows: Vec<Vec<Value>>,
}

/// Cartesian combination of scope row sets (joins refine it below).
/// Scope offsets are recomputed positionally.
fn combine_scopes(mats: &[Mat]) -> Combined {
    let mut scopes = Vec::new();
    let mut offset = 0;
    for mat in mats {
        for scope in &mat.scopes {
            let mut scope = scope.clone();
            scope.offset = offset;
            offset += scope.schema.columns.len();
            scopes.push(scope);
        }
    }
    let mut rows = vec![Vec::new()];
    for mat in mats {
        let mut next = Vec::new();
        for prefix in &rows {
            for row in &mat.rows {
                let mut combined = prefix.clone();
                combined.extend_from_slice(row);
                next.push(combined);
            }
        }
        rows = next;
    }
    Combined { scopes, rows }
}

impl<'a> TxnQuery<'a> {
    /// Materializes every scope of a FROM clause (tables via overlay scan,
    /// subqueries/CTEs/views via recursion, system relations via catalog).
    fn materialize_from(
        &mut self,
        from: &FromClause,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<Vec<Mat>> {
        match from {
            FromClause::Table { name, alias } => {
                Ok(vec![self.materialize_table(name, alias, ctes, outer)?])
            }
            FromClause::Subquery {
                statement,
                alias,
                column_aliases,
                ..
            } => {
                let (columns, types, rows) = self.materialize_query(statement, ctes, outer)?;
                let mut columns = columns;
                if !column_aliases.is_empty() {
                    if column_aliases.len() != columns.len() {
                        return Err(SqlError::Storage(PlomidError::new(
                            ErrorKind::InvalidArgument,
                            format!(
                                "table \"{alias}\" has {} columns available but {} specified",
                                columns.len(),
                                column_aliases.len()
                            ),
                        )));
                    }
                    columns = column_aliases.clone();
                }
                Ok(vec![Mat::single(
                    alias.clone(),
                    synthetic_schema(alias, &columns, &types),
                    rows,
                )])
            }
            FromClause::Join {
                left,
                right,
                kind,
                on,
            } => self.materialize_join(left, right, *kind, on.as_ref(), ctes, outer),
            FromClause::TableFunction { name, .. } => Err(unsupported(&format!(
                "table function \"{name}\" inside explicit transactions is not yet supported"
            ))),
        }
    }

    /// Materializes one table-like scope by name: CTE/view/subquery results
    /// first, then system relations, then overlay-scanned user tables.
    fn materialize_table(
        &mut self,
        name: &str,
        alias: &Option<String>,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<Mat> {
        // CTEs shadow catalog objects (PostgreSQL scoping).
        if let Some(cte) = ctes.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)) {
            return Ok(Mat::single(
                effective_alias(name, alias),
                synthetic_schema(&effective_alias(name, alias), &cte.1.columns, &cte.1.types),
                cte.1.rows.clone(),
            ));
        }
        // Views expand to their stored query (evaluated transactionally).
        if let Some(view) = self.catalog.get_view(name) {
            let query = view.query.clone();
            let (columns, types, rows) = self.materialize_query(&query, ctes, outer)?;
            return Ok(Mat::single(
                effective_alias(name, alias),
                synthetic_schema(&effective_alias(name, alias), &columns, &types),
                rows,
            ));
        }
        // System relations are metadata: computed from the session catalog
        // (which already reflects in-transaction DDL).
        if SystemCatalog::is_relation(name) {
            let syscat = self.syscat();
            let Some(relation) = syscat.relation(name) else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("relation \"{name}\" does not exist"),
                )));
            };
            return Ok(Mat::single(
                effective_alias(name, alias),
                relation.schema,
                relation.rows,
            ));
        }
        // Ordinary user table: overlay-merged scan under the statement
        // snapshot, decoded row by row.
        let resolved = self
            .catalog
            .resolve_table_name(name)
            .map_err(SqlError::Storage)?;
        let schema = self
            .catalog
            .get_table(&resolved)
            .map_err(SqlError::Storage)?
            .clone();
        let (start, end) = table_range(&resolved);
        let pairs = self.txn.scan(Some(&start), Some(&end)).map_err(|e| {
            SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                format!("transactional scan failed: {}", e.message()),
            ))
        })?;
        let mut rows = Vec::with_capacity(pairs.len());
        for (_, value) in pairs {
            let decoded = decode_row(&value).map_err(SqlError::Storage)?;
            rows.push(decoded);
        }
        Ok(Mat::single(effective_alias(&resolved, alias), schema, rows))
    }

    /// Nested-loop join over materialized sides; the ON condition evaluates
    /// per combined row (subqueries substituted first).
    fn materialize_join(
        &mut self,
        left: &FromClause,
        right: &FromClause,
        kind: JoinKind,
        on: Option<&Expression>,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<Vec<Mat>> {
        let left_mats = self.materialize_from(left, ctes, outer)?;
        let right_mats = self.materialize_from(right, ctes, outer)?;
        let left_combined = combine_scopes(&left_mats);
        let right_combined = combine_scopes(&right_mats);
        let pad_left = vec![Value::Null; width_of(&left_combined.scopes)];
        let pad_right = vec![Value::Null; width_of(&right_combined.scopes)];
        // Scopes concatenate left-then-right; combined rows align.
        let mut scopes = left_combined.scopes;
        scopes.extend(right_combined.scopes);
        // Re-offset the right scopes after concatenation.
        let mut offset = 0;
        for scope in &mut scopes {
            scope.offset = offset;
            offset += scope.schema.columns.len();
        }
        let mut rows = Vec::new();
        match kind {
            JoinKind::Cross => {
                for left_row in &left_combined.rows {
                    for right_row in &right_combined.rows {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(right_row);
                        if self.join_condition_holds(&scopes, &combined, on, outer)? {
                            rows.push(combined);
                        }
                    }
                }
            }
            JoinKind::Inner => {
                for left_row in &left_combined.rows {
                    for right_row in &right_combined.rows {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(right_row);
                        if self.join_condition_holds(&scopes, &combined, on, outer)? {
                            rows.push(combined);
                        }
                    }
                }
            }
            JoinKind::Left => {
                for left_row in &left_combined.rows {
                    let mut matched = false;
                    for right_row in &right_combined.rows {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(right_row);
                        if self.join_condition_holds(&scopes, &combined, on, outer)? {
                            rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(&pad_right);
                        rows.push(combined);
                    }
                }
            }
            JoinKind::Right => {
                for right_row in &right_combined.rows {
                    let mut matched = false;
                    for left_row in &left_combined.rows {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(right_row);
                        if self.join_condition_holds(&scopes, &combined, on, outer)? {
                            rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = pad_left.clone();
                        combined.extend_from_slice(right_row);
                        rows.push(combined);
                    }
                }
            }
            JoinKind::Full => {
                let mut right_matched = vec![false; right_combined.rows.len()];
                for left_row in &left_combined.rows {
                    let mut matched = false;
                    for (index, right_row) in right_combined.rows.iter().enumerate() {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(right_row);
                        if self.join_condition_holds(&scopes, &combined, on, outer)? {
                            rows.push(combined);
                            matched = true;
                            right_matched[index] = true;
                        }
                    }
                    if !matched {
                        let mut combined = left_row.clone();
                        combined.extend_from_slice(&pad_right);
                        rows.push(combined);
                    }
                }
                for (index, right_row) in right_combined.rows.iter().enumerate() {
                    if !right_matched[index] {
                        let mut combined = pad_left.clone();
                        combined.extend_from_slice(right_row);
                        rows.push(combined);
                    }
                }
            }
        }
        // Keep the concatenated scope list (offsets already fixed):
        // downstream operators resolve qualified and unambiguous bare
        // references natively; only single-schema operators flatten.
        Ok(vec![Mat { scopes, rows }])
    }

    fn join_condition_holds(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        on: Option<&Expression>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<bool> {
        let Some(on) = on else { return Ok(true) };
        let substituted = self.substitute(on, scopes, Some(row), outer)?;
        eval_join_condition(
            self.facade,
            self.catalog,
            scopes,
            outer,
            row,
            &substituted,
            self.database,
            self.user,
        )
    }
}

fn width_of(scopes: &[JoinScope]) -> usize {
    scopes.iter().map(|s| s.schema.columns.len()).sum()
}

/// Storage key range for one table's rows.
fn table_range(resolved: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::with_capacity(resolved.len() + 1);
    start.extend_from_slice(resolved.as_bytes());
    start.push(b':');
    let mut end = Vec::with_capacity(resolved.len() + 5);
    end.extend_from_slice(resolved.as_bytes());
    end.push(b':');
    end.extend_from_slice("￿".as_bytes());
    (start, end)
}

/// Splits a table name into its effective alias (explicit alias wins,
/// else the trailing name segment — PostgreSQL scoping).
fn effective_alias(name: &str, alias: &Option<String>) -> String {
    alias
        .clone()
        .unwrap_or_else(|| name.rsplit('.').next().unwrap_or(name).to_string())
}

/// Evaluates a join/ON predicate with JoinEval (pure arms; subqueries are
/// substituted by the caller).
#[allow(clippy::too_many_arguments)]
fn eval_join_condition(
    facade: &mut ConcurrentPlomidStorageEngine,
    catalog: &InMemoryCatalog,
    scopes: &[JoinScope],
    outer: Option<&OuterContext>,
    row: &[Value],
    on: &Expression,
    database: &str,
    user: &str,
) -> SqlResult<bool> {
    let mut eval = JoinEval::new(facade, catalog, scopes, outer, 0, database, user);
    match eval.eval(row, on)? {
        Value::Bool(true) => Ok(true),
        _ => Ok(false),
    }
}

/// Flattened single-schema view over scopes for operators that need one
/// schema (grouping, sorting, distinct-on). Column names are
/// `alias\x1fcolumn` (unit separator cannot appear in identifiers);
/// [`rewrite_refs`] maps references onto them, rejecting ambiguity.
struct Flat {
    schema: TableSchema,
    rows: Vec<Vec<Value>>,
}

fn flatten_scopes(mats: &[Mat]) -> SqlResult<Flat> {
    let mut columns = Vec::new();
    let mut types = Vec::new();
    let mut rows: Vec<Vec<Value>> = vec![Vec::new()];
    for mat in mats {
        for scope in &mat.scopes {
            for column in scope.schema.columns.iter() {
                columns.push(format!("{}\x1f{}", scope.alias, column.name));
                types.push(Some(column.col_type));
            }
        }
        let mut next = Vec::with_capacity(rows.len() * mat.rows.len().max(1));
        for prefix in &rows {
            if mat.rows.is_empty() {
                next.push(prefix.clone());
            } else {
                for row in &mat.rows {
                    let mut combined = prefix.clone();
                    combined.extend_from_slice(row);
                    next.push(combined);
                }
            }
        }
        rows = next;
    }
    // Positional column ids for the synthetic schema.
    let column_ids: Vec<plomid_core::ColumnId> = (0..columns.len() as u64)
        .map(plomid_core::ColumnId::new)
        .collect();
    let schema = TableSchema {
        name: "__txn_flat".to_string(),
        table_id: plomid_core::TableId::new(0),
        column_ids,
        columns: columns
            .into_iter()
            .zip(types)
            .map(|(name, col_type)| ColumnDef {
                name,
                col_type: col_type.unwrap_or_else(ColumnType::text),
                constraints: Vec::new(),
            })
            .collect(),
        constraints: Vec::new(),
    };
    Ok(Flat { schema, rows })
}

/// Rewrites every `ColumnRef` in an expression via `resolve`, preserving
/// all other structure exactly. Subquery nodes must have been substituted
/// already (defensive internal error otherwise). Exhaustive over the AST
/// with NO wildcard arm: a future variant fails compilation here instead
/// of silently passing references through.
fn rewrite_refs<E>(expr: &Expression, resolve: &E) -> SqlResult<Expression>
where
    E: Fn(&str) -> SqlResult<String>,
{
    use Expression as X;
    Ok(match expr {
        X::ColumnRef(name) => X::ColumnRef(resolve(name)?),
        X::Literal(_) | X::Star => expr.clone(),
        X::Equal(a, b) => X::Equal(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::NotEqual(a, b) => X::NotEqual(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Less(a, b) => X::Less(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::LessOrEqual(a, b) => X::LessOrEqual(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Greater(a, b) => X::Greater(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::GreaterOrEqual(a, b) => {
            X::GreaterOrEqual(rw(a, resolve)?.into(), rw(b, resolve)?.into())
        }
        X::IsNull(a) => X::IsNull(rw(a, resolve)?.into()),
        X::IsNotNull(a) => X::IsNotNull(rw(a, resolve)?.into()),
        X::And(a, b) => X::And(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Or(a, b) => X::Or(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Not(a) => X::Not(rw(a, resolve)?.into()),
        X::Add(a, b) => X::Add(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Subtract(a, b) => X::Subtract(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Multiply(a, b) => X::Multiply(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Divide(a, b) => X::Divide(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Modulo(a, b) => X::Modulo(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Concat(a, b) => X::Concat(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::Negate(a) => X::Negate(rw(a, resolve)?.into()),
        X::Power(a, b) => X::Power(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::BitAnd(a, b) => X::BitAnd(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::BitOr(a, b) => X::BitOr(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::BitXor(a, b) => X::BitXor(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::ShiftLeft(a, b) => X::ShiftLeft(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::ShiftRight(a, b) => X::ShiftRight(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::IsDistinctFrom(a, b) => {
            X::IsDistinctFrom(rw(a, resolve)?.into(), rw(b, resolve)?.into())
        }
        X::NullIf(a, b) => X::NullIf(rw(a, resolve)?.into(), rw(b, resolve)?.into()),
        X::In {
            expr,
            list,
            subquery,
            negated,
        } => {
            if subquery.is_some() {
                return Err(unsubstituted_subquery());
            }
            X::In {
                expr: rw(expr, resolve)?.into(),
                list: list
                    .iter()
                    .map(|e| rw(e, resolve))
                    .collect::<SqlResult<Vec<_>>>()?,
                subquery: None,
                negated: *negated,
            }
        }
        X::Between {
            expr,
            low,
            high,
            negated,
        } => X::Between {
            expr: rw(expr, resolve)?.into(),
            low: rw(low, resolve)?.into(),
            high: rw(high, resolve)?.into(),
            negated: *negated,
        },
        X::Like {
            expr,
            pattern,
            escape,
            negated,
            case_insensitive,
        } => X::Like {
            expr: rw(expr, resolve)?.into(),
            pattern: rw(pattern, resolve)?.into(),
            escape: *escape,
            negated: *negated,
            case_insensitive: *case_insensitive,
        },
        X::Case {
            operand,
            whens,
            default,
        } => X::Case {
            operand: operand
                .as_ref()
                .map(|o| rw(o, resolve))
                .transpose()?
                .map(Box::new),
            whens: whens
                .iter()
                .map(|(c, v)| Ok((rw(c, resolve)?, rw(v, resolve)?)))
                .collect::<SqlResult<Vec<_>>>()?,
            default: default
                .as_ref()
                .map(|d| rw(d, resolve))
                .transpose()?
                .map(Box::new),
        },
        X::Coalesce(list) => X::Coalesce(
            list.iter()
                .map(|e| rw(e, resolve))
                .collect::<SqlResult<Vec<_>>>()?,
        ),
        X::Exists(_) | X::ScalarSubquery(_) | X::QuantifiedComparison { .. } => {
            return Err(unsubstituted_subquery())
        }
        X::IsJson {
            expr,
            kind,
            negated,
        } => X::IsJson {
            expr: rw(expr, resolve)?.into(),
            kind: *kind,
            negated: *negated,
        },
        X::IsBoolean {
            expr,
            kind,
            negated,
        } => X::IsBoolean {
            expr: rw(expr, resolve)?.into(),
            kind: *kind,
            negated: *negated,
        },
        X::WindowFunction { .. } => {
            return Err(unsupported(
                "window functions inside explicit transactions are not yet supported",
            ))
        }
        X::Cast { expr, type_name } => X::Cast {
            expr: rw(expr, resolve)?.into(),
            type_name: type_name.clone(),
        },
        X::Extract { field, expr } => X::Extract {
            field: field.clone(),
            expr: rw(expr, resolve)?.into(),
        },
        X::DateLiteral(_)
        | X::TimestampLiteral(_)
        | X::TimestamptzLiteral(_)
        | X::TimeLiteral(_) => expr.clone(),
        X::TypedTimestampLiteral { .. }
        | X::TypedTimestamptzLiteral { .. }
        | X::TypedTimeLiteral { .. } => expr.clone(),
        X::FunctionCall {
            name,
            args,
            distinct,
            filter,
            order_by,
            returning,
            null_handling,
            unique_keys,
        } => X::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|e| rw(e, resolve))
                .collect::<SqlResult<Vec<_>>>()?,
            distinct: *distinct,
            filter: filter
                .as_ref()
                .map(|f| rw(f, resolve))
                .transpose()?
                .map(Box::new),
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(plomid_sql::OrderByItem {
                        expr: rw(&item.expr, resolve)?,
                        ..item.clone()
                    })
                })
                .collect::<SqlResult<Vec<_>>>()?,
            returning: returning.clone(),
            null_handling: *null_handling,
            unique_keys: *unique_keys,
        },
        X::TypeCast { expr, type_name } => X::TypeCast {
            expr: rw(expr, resolve)?.into(),
            type_name: type_name.clone(),
        },
        X::JsonArrow {
            left,
            right,
            as_text,
        } => X::JsonArrow {
            left: rw(left, resolve)?.into(),
            right: rw(right, resolve)?.into(),
            as_text: *as_text,
        },
        X::ArrayIndex { array, index } => X::ArrayIndex {
            array: rw(array, resolve)?.into(),
            index: rw(index, resolve)?.into(),
        },
        X::VectorLiteral(_) => expr.clone(),
        X::RowField { expr, field } => X::RowField {
            expr: rw(expr, resolve)?.into(),
            field: field.clone(),
        },
        X::JsonSubscript { array, index } => X::JsonSubscript {
            array: rw(array, resolve)?.into(),
            index: rw(index, resolve)?.into(),
        },
    })
}

fn rw<E>(expr: &Expression, resolve: &E) -> SqlResult<Expression>
where
    E: Fn(&str) -> SqlResult<String>,
{
    rewrite_refs(expr, resolve)
}

fn unsubstituted_subquery() -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::Internal,
        "subquery survived substitution (internal error)",
    ))
}

/// Expands `*` / `alias.*` into explicit per-column targets paired with
/// display names. Other targets pass through with their display names
/// (alias, unqualified column, function name, or `?column?`).
fn expand_targets(
    targets: &[SelectTarget],
    scopes: &[JoinScope],
) -> SqlResult<Vec<(SelectTarget, String)>> {
    let mut out = Vec::new();
    for target in targets {
        match target {
            SelectTarget::All => {
                for scope in scopes {
                    for column in &scope.schema.columns {
                        out.push((
                            SelectTarget::Expr {
                                expr: Expression::ColumnRef(format!(
                                    "{}.{}",
                                    scope.alias, column.name
                                )),
                                alias: None,
                            },
                            column.name.clone(),
                        ));
                    }
                }
            }
            SelectTarget::QualifiedStar { qualifier } => {
                let Some(scope) = scopes
                    .iter()
                    .find(|s| s.alias.eq_ignore_ascii_case(qualifier))
                else {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::NotFound,
                        format!("relation \"{qualifier}\" does not exist"),
                    )));
                };
                for column in &scope.schema.columns {
                    out.push((
                        SelectTarget::Expr {
                            expr: Expression::ColumnRef(format!("{}.{}", scope.alias, column.name)),
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
                out.push((
                    SelectTarget::Expr {
                        expr: expr.clone(),
                        alias: alias.clone(),
                    },
                    display,
                ));
            }
            SelectTarget::Function(name) => out.push((target.clone(), name.clone())),
            SelectTarget::FunctionCall { name, .. } => out.push((target.clone(), name.clone())),
            SelectTarget::WindowFunction { .. } => {
                return Err(unsupported(
                    "window functions inside explicit transactions are not yet supported",
                ))
            }
            SelectTarget::Aliased { target, alias } => {
                out.push((target.as_ref().clone(), alias.clone()))
            }
        }
    }
    Ok(out)
}

impl<'a> TxnQuery<'a> {
    /// Evaluates one projection target on one row (subqueries substituted
    /// per row, everything else via JoinEval).
    fn eval_target(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        target: &SelectTarget,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Value> {
        match target {
            SelectTarget::Function(name) => {
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    scopes,
                    outer,
                    0,
                    self.database,
                    self.user,
                );
                eval.eval(row, &Expression::ColumnRef(name.clone()))
            }
            SelectTarget::FunctionCall { .. } | SelectTarget::Expr { .. } => {
                let expr = match target {
                    SelectTarget::Expr { expr, .. } => expr.clone(),
                    SelectTarget::FunctionCall { .. } => {
                        // Rebuild as a synthetic expression is unnecessary:
                        // evaluate the call's own shape via a one-target
                        // projection is overkill — evaluate directly.
                        return self.eval_call_target(scopes, row, target, outer);
                    }
                    _ => unreachable!("matched above"),
                };
                let substituted = self.substitute(&expr, scopes, Some(row), outer)?;
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    scopes,
                    outer,
                    0,
                    self.database,
                    self.user,
                );
                eval.eval(row, &substituted)
            }
            SelectTarget::Aliased { target, .. } => {
                // Aliases were resolved during expansion; evaluate the inner.
                self.eval_target(scopes, row, target, outer)
            }
            SelectTarget::All | SelectTarget::QualifiedStar { .. } => Err(SqlError::Storage(
                PlomidError::new(ErrorKind::Internal, "unexpanded star in projection"),
            )),
            SelectTarget::WindowFunction { .. } => Err(unsupported(
                "window functions inside explicit transactions are not yet supported",
            )),
        }
    }

    /// Bare-SELECT `generate_series(start, stop[, step])` without FROM:
    /// one row per generated element. Detection
    /// (`generate_series_target`) and element generation
    /// (`generate_series_element_values`, `generate_series_rows`) are
    /// shared with the main path — only argument evaluation is txn-local
    /// (overlay-visible). Returns `None` for any other target shape.
    fn expand_bare_generate_series(
        &mut self,
        target: &SelectTarget,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Option<QueryResult>> {
        use crate::join::{
            generate_series_element_values, generate_series_rows, generate_series_target,
        };

        let Some((expr, alias)) = generate_series_target(target) else {
            return Ok(None);
        };
        let Expression::FunctionCall { args, .. } = expr else {
            unreachable!("generate_series_target yields calls")
        };
        let empty_scopes: Vec<JoinScope> = Vec::new();
        let empty_row: Vec<Value> = Vec::new();
        let mut values = Vec::with_capacity(args.len());
        for arg in &args {
            let substituted = self.substitute(arg, &empty_scopes, Some(&empty_row), outer)?;
            let mut eval = JoinEval::new(
                self.facade,
                self.catalog,
                &empty_scopes,
                outer,
                0,
                self.database,
                self.user,
            );
            values.push(eval.eval(&empty_row, &substituted)?);
        }
        let column = alias.unwrap_or_else(|| "generate_series".into());
        Ok(Some(QueryResult::Rows {
            columns: vec![column],
            column_types: vec![Some(ColumnType::new(
                plomid_types::TypeOid::INT8,
                plomid_types::NO_TYPEMOD,
            ))],
            rows: generate_series_rows(Value::Array {
                element_oid: plomid_types::TypeOid::INT8,
                elements: generate_series_element_values(&values)?,
            })?,
        }))
    }

    /// Evaluates a FunctionCall target (kept separate so aggregate-shaped
    /// calls never reach here — the grouped path owns those).
    fn eval_call_target(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        target: &SelectTarget,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Value> {
        let SelectTarget::FunctionCall { name, args } = target else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "non-call target in call evaluation",
            )));
        };
        if crate::catalog_fn::is_aggregate_function(name) {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "aggregate call reached row projection",
            )));
        }
        // Session functions (current_database, version, …) resolve from
        // session state — same rule as the main path (`join/eval.rs`,
        // `query/project.rs`), never scalar dispatch.
        if crate::catalog_fn::is_session_function(name) {
            return crate::catalog_fn::function_value(name, self.database, self.user);
        }
        // Sequences resolve against the facade (independent commits —
        // PostgreSQL: sequence advances never roll back).
        if name.eq_ignore_ascii_case("nextval") || name.eq_ignore_ascii_case("currval") {
            // Substitute subqueries inside arguments per row, then run the
            // shared scalar dispatch (sequences resolve via the facade).
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                let substituted = self.substitute(arg, scopes, Some(row), outer)?;
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    scopes,
                    outer,
                    0,
                    self.database,
                    self.user,
                );
                values.push(eval.eval(row, &substituted)?);
            }
            let argument = values.first().map(|v| v.to_sql_text()).unwrap_or_default();
            return crate::catalog_fn::sequence_value(self.facade, name, &argument);
        }
        // Substitute subqueries inside arguments per row, then run the
        // shared scalar dispatch (sequences resolve via the facade).
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            let substituted = self.substitute(arg, scopes, Some(row), outer)?;
            let mut eval = JoinEval::new(
                self.facade,
                self.catalog,
                scopes,
                outer,
                0,
                self.database,
                self.user,
            );
            values.push(eval.eval(row, &substituted)?);
        }
        // Mirror the scalar dispatch (sans engine-bound arms, which error
        // the same way they do on the main path).
        crate::scalar::scalar_function_value(name, &values)
    }

    /// Output type for one expanded target.
    fn target_type(&self, scopes: &[JoinScope], target: &SelectTarget) -> Option<ColumnType> {
        match target {
            SelectTarget::Expr {
                expr: Expression::ColumnRef(name),
                ..
            } => self.resolve_type(scopes, name),
            SelectTarget::Function(name) => crate::catalog_fn::session_function_type(name),
            SelectTarget::FunctionCall { name, args, .. } => {
                // Aggregate-typed calls report their aggregate type where
                // decidable; anything else is unknown (matches the main
                // path's `None` for undecidable expressions).
                let _ = (name, args);
                None
            }
            _ => None,
        }
    }

    fn resolve_type(&self, scopes: &[JoinScope], name: &str) -> Option<ColumnType> {
        let (qualifier, column) = match name.rsplit_once('.') {
            Some((q, c)) => (Some(q), c),
            None => (None, name),
        };
        let mut found = None;
        for scope in scopes {
            if let Some(q) = qualifier {
                if !scope.alias.eq_ignore_ascii_case(q) {
                    continue;
                }
            }
            for col in &scope.schema.columns {
                if col.name == column || col.name.eq_ignore_ascii_case(column) {
                    if found.is_some() {
                        return None;
                    }
                    found = Some(col.col_type);
                }
            }
        }
        found
    }

    /// ORDER BY over scoped rows: keys evaluated per row (subqueries
    /// substituted), ordered with the shared comparator. Stable for equal
    /// keys (input order preserved).
    fn sort_scoped(
        &mut self,
        scopes: &[JoinScope],
        rows: Vec<Vec<Value>>,
        order_by: &[OrderByItem],
        outer: Option<&OuterContext>,
    ) -> SqlResult<Vec<Vec<Value>>> {
        // Resolve ORDER BY aliases against projection is the caller's job;
        // here items reference scope columns (or output aliases rewritten
        // by the caller into scope refs).
        let mut keyed = Vec::with_capacity(rows.len());
        for row in rows {
            let mut keys = Vec::with_capacity(order_by.len());
            for item in order_by {
                keys.push(self.eval_row_for_sort(scopes, &row, &item.expr, outer)?);
            }
            keyed.push((row, keys));
        }
        keyed.sort_by(|(_, a), (_, b)| compare_order_items(a, b, order_by));
        Ok(keyed.into_iter().map(|(row, _)| row).collect())
    }

    fn eval_row_for_sort(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        expr: &Expression,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Value> {
        let substituted = self.substitute(expr, scopes, Some(row), outer)?;
        let mut eval = JoinEval::new(
            self.facade,
            self.catalog,
            scopes,
            outer,
            0,
            self.database,
            self.user,
        );
        eval.eval(row, &substituted)
    }
}

impl<'a> TxnQuery<'a> {
    /// Set operations over materialized sides. Column counts must align
    /// (PostgreSQL compatibility); output names/types come from the left.
    fn select_setop(
        &mut self,
        op: plomid_sql::SetOpKind,
        all: bool,
        left: &Statement,
        right: &Statement,
        ctes: &[(String, NamedRows)],
        outer: Option<&OuterContext>,
    ) -> SqlResult<QueryResult> {
        use plomid_sql::SetOpKind as K;
        let (lcols, ltypes, lrows) = self.materialize_query(left, ctes, outer)?;
        let (_rcols, _rtypes, rrows) = self.materialize_query(right, ctes, outer)?;
        for row in lrows.iter().chain(rrows.iter()) {
            if row.len() != lcols.len() {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    "set operation branches must return the same number of columns",
                )));
            }
        }
        let rows = match op {
            K::Union => {
                let mut out = lrows;
                out.extend(rrows);
                if all {
                    out
                } else {
                    dedupe_rows(out)
                }
            }
            K::Intersect => {
                let mut out = Vec::new();
                let mut remaining = rrows;
                for row in lrows {
                    if let Some(pos) = remaining.iter().position(|r| r == &row) {
                        remaining.remove(pos);
                        out.push(row);
                    }
                }
                if all {
                    out
                } else {
                    dedupe_rows(out)
                }
            }
            K::Except => {
                if all {
                    let mut out = Vec::new();
                    let mut remaining = rrows;
                    for row in lrows {
                        match remaining.iter().position(|r| r == &row) {
                            Some(pos) => {
                                remaining.remove(pos);
                            }
                            None => out.push(row),
                        }
                    }
                    out
                } else {
                    dedupe_rows(lrows)
                        .into_iter()
                        .filter(|row| !rrows.iter().any(|r| r == row))
                        .collect()
                }
            }
        };
        Ok(QueryResult::Rows {
            columns: lcols,
            column_types: ltypes,
            rows,
        })
    }

    /// VALUES lists: each row's expressions evaluated with no scopes
    /// (column references fail, matching the main path).
    fn select_values(&mut self, rows: &[Vec<Expression>]) -> SqlResult<QueryResult> {
        let width = rows.first().map(Vec::len).unwrap_or(0);
        if rows.iter().any(|row| row.len() != width) {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                "VALUES rows must all have the same arity",
            )));
        }
        let empty_scopes: Vec<JoinScope> = Vec::new();
        let mut out_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(width);
            for expr in row {
                let substituted = self.substitute(expr, &empty_scopes, None, None)?;
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    &empty_scopes,
                    None,
                    0,
                    self.database,
                    self.user,
                );
                out.push(eval.eval(&[], &substituted)?);
            }
            out_rows.push(out);
        }
        let columns = (1..=width)
            .map(|i| format!("column{i}"))
            .collect::<Vec<_>>();
        let column_types = (0..width)
            .map(|i| {
                out_rows
                    .iter()
                    .find_map(|row| crate::coerce::value_type(&row[i]))
            })
            .collect::<Vec<_>>();
        Ok(QueryResult::Rows {
            columns,
            column_types,
            rows: out_rows,
        })
    }
}

/// Duplicate elimination preserving first-seen order.
fn dedupe_rows(rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if !out.contains(&row) {
            out.push(row);
        }
    }
    out
}

/// Three-way row ordering under ORDER BY items (ascending/descending +
/// NULLS FIRST/LAST), mirroring the shared comparator's NULL placement.
fn compare_order_items(a: &[Value], b: &[Value], order_by: &[OrderByItem]) -> std::cmp::Ordering {
    use std::cmp::Ordering as O;
    for (index, item) in order_by.iter().enumerate() {
        let (left, right) = (&a[index], &b[index]);
        let ord = match (left.is_null(), right.is_null()) {
            (true, true) => O::Equal,
            (true, false) => {
                if item.nulls_first.unwrap_or(!item.descending) {
                    O::Less
                } else {
                    O::Greater
                }
            }
            (false, true) => {
                if item.nulls_first.unwrap_or(!item.descending) {
                    O::Greater
                } else {
                    O::Less
                }
            }
            (false, false) => crate::query::value_cmp(left, right).unwrap_or(O::Equal),
        };
        let ord = if item.descending { ord.reverse() } else { ord };
        if ord != O::Equal {
            return ord;
        }
    }
    O::Equal
}

/// Evaluates an uncorrelated subquery statement once (no outer row) and
/// memoizes it by AST shape for the statement duration.
fn memo_key(stmt: &Statement) -> String {
    format!("{stmt:?}")
}

impl<'a> TxnQuery<'a> {
    /// Substitute subqueries in `expr`, returning a subquery-free expression.
    /// Uncorrelated subqueries evaluate once (memoized); correlated ones
    /// evaluate against `row` when provided, else error (only valid where
    /// per-row evaluation exists: WHERE/ON/targets, not GROUP BY/ORDER BY).
    fn substitute(
        &mut self,
        expr: &Expression,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Expression> {
        use Expression as X;
        Ok(match expr {
            X::Exists(subq) => {
                let holds = self.eval_exists(subq, scopes, row, outer)?;
                X::Literal(Value::Bool(holds))
            }
            X::ScalarSubquery(subq) => {
                let value = self.eval_scalar_subquery(subq, scopes, row, outer)?;
                X::Literal(value)
            }
            X::In {
                expr,
                list,
                subquery,
                negated,
            } => {
                let operand = self.substitute(expr, scopes, row, outer)?;
                let mut disjuncts: Vec<Expression> = Vec::new();
                for item in list {
                    let item = self.substitute(item, scopes, row, outer)?;
                    disjuncts.push(X::Equal(operand.clone().into(), item.into()));
                }
                if let Some(subq) = subquery {
                    for value in self.eval_in_set(subq, scopes, row, outer)? {
                        disjuncts.push(X::Equal(operand.clone().into(), X::Literal(value).into()));
                    }
                }
                let mut combined = disjuncts
                    .into_iter()
                    .reduce(|a, b| X::Or(a.into(), b.into()));
                if combined.is_none() {
                    combined = Some(X::Literal(Value::Bool(false)));
                }
                let mut combined = combined.expect("nonempty");
                if *negated {
                    combined = X::Not(combined.into());
                }
                combined
            }
            X::QuantifiedComparison {
                left,
                operator,
                quantifier,
                subquery,
            } => {
                let set = self.eval_in_set(subquery, scopes, row, outer)?;
                let operand = self.substitute(left, scopes, row, outer)?;
                X::Literal(Value::Bool(self.eval_quantified(
                    &operand,
                    scopes,
                    row,
                    outer,
                    *operator,
                    *quantifier,
                    &set,
                )?))
            }
            _ => self.substitute_children(expr, scopes, row, outer)?,
        })
    }

    /// Recursively substitutes subqueries in child positions (structure
    /// otherwise preserved). Reached only for non-subquery nodes.
    fn substitute_children(
        &mut self,
        expr: &Expression,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Expression> {
        use Expression as X;
        Ok(match expr {
            X::ColumnRef(_) | X::Literal(_) | X::Star => expr.clone(),
            X::Equal(a, b) => X::Equal(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::NotEqual(a, b) => X::NotEqual(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Less(a, b) => X::Less(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::LessOrEqual(a, b) => X::LessOrEqual(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Greater(a, b) => X::Greater(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::GreaterOrEqual(a, b) => X::GreaterOrEqual(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::IsNull(a) => X::IsNull(self.sub_box(a, scopes, row, outer)?.into()),
            X::IsNotNull(a) => X::IsNotNull(self.sub_box(a, scopes, row, outer)?.into()),
            X::And(a, b) => X::And(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Or(a, b) => X::Or(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Not(a) => X::Not(self.sub_box(a, scopes, row, outer)?.into()),
            X::Add(a, b) => X::Add(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Subtract(a, b) => X::Subtract(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Multiply(a, b) => X::Multiply(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Divide(a, b) => X::Divide(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Modulo(a, b) => X::Modulo(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Concat(a, b) => X::Concat(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Negate(a) => X::Negate(self.sub_box(a, scopes, row, outer)?.into()),
            X::Power(a, b) => X::Power(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::BitAnd(a, b) => X::BitAnd(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::BitOr(a, b) => X::BitOr(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::BitXor(a, b) => X::BitXor(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::ShiftLeft(a, b) => X::ShiftLeft(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::ShiftRight(a, b) => X::ShiftRight(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::IsDistinctFrom(a, b) => X::IsDistinctFrom(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::NullIf(a, b) => X::NullIf(
                self.sub_box(a, scopes, row, outer)?.into(),
                self.sub_box(b, scopes, row, outer)?.into(),
            ),
            X::Between {
                expr,
                low,
                high,
                negated,
            } => X::Between {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                low: self.sub_box(low, scopes, row, outer)?.into(),
                high: self.sub_box(high, scopes, row, outer)?.into(),
                negated: *negated,
            },
            X::Like {
                expr,
                pattern,
                escape,
                negated,
                case_insensitive,
            } => X::Like {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                pattern: self.sub_box(pattern, scopes, row, outer)?.into(),
                escape: *escape,
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            X::Case {
                operand,
                whens,
                default,
            } => X::Case {
                operand: operand
                    .as_ref()
                    .map(|o| self.sub_box(o, scopes, row, outer))
                    .transpose()?
                    .map(Box::new),
                whens: whens
                    .iter()
                    .map(|(c, v)| {
                        Ok((
                            self.substitute(c, scopes, row, outer)?,
                            self.substitute(v, scopes, row, outer)?,
                        ))
                    })
                    .collect::<SqlResult<Vec<_>>>()?,
                default: default
                    .as_ref()
                    .map(|d| self.sub_box(d, scopes, row, outer))
                    .transpose()?
                    .map(Box::new),
            },
            X::Coalesce(list) => X::Coalesce(
                list.iter()
                    .map(|e| self.substitute(e, scopes, row, outer))
                    .collect::<SqlResult<Vec<_>>>()?,
            ),
            X::IsJson {
                expr,
                kind,
                negated,
            } => X::IsJson {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                kind: *kind,
                negated: *negated,
            },
            X::IsBoolean {
                expr,
                kind,
                negated,
            } => X::IsBoolean {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                kind: *kind,
                negated: *negated,
            },
            X::Cast { expr, type_name } => X::Cast {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                type_name: type_name.clone(),
            },
            X::Extract { field, expr } => X::Extract {
                field: field.clone(),
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
            },
            X::DateLiteral(_)
            | X::TimestampLiteral(_)
            | X::TimestamptzLiteral(_)
            | X::TimeLiteral(_) => expr.clone(),
            X::TypedTimestampLiteral { .. }
            | X::TypedTimestamptzLiteral { .. }
            | X::TypedTimeLiteral { .. } => expr.clone(),
            X::VectorLiteral(_) => expr.clone(),
            X::RowField { expr, field } => X::RowField {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                field: field.clone(),
            },
            X::JsonSubscript { array, index } => X::JsonSubscript {
                array: self.sub_box(array, scopes, row, outer)?.into(),
                index: self.sub_box(index, scopes, row, outer)?.into(),
            },
            X::JsonArrow {
                left,
                right,
                as_text,
            } => X::JsonArrow {
                left: self.sub_box(left, scopes, row, outer)?.into(),
                right: self.sub_box(right, scopes, row, outer)?.into(),
                as_text: *as_text,
            },
            X::ArrayIndex { array, index } => X::ArrayIndex {
                array: self.sub_box(array, scopes, row, outer)?.into(),
                index: self.sub_box(index, scopes, row, outer)?.into(),
            },
            X::TypeCast { expr, type_name } => X::TypeCast {
                expr: self.sub_box(expr, scopes, row, outer)?.into(),
                type_name: type_name.clone(),
            },
            X::FunctionCall {
                name,
                args,
                distinct,
                filter,
                order_by,
                returning,
                null_handling,
                unique_keys,
            } => X::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|e| self.substitute(e, scopes, row, outer))
                    .collect::<SqlResult<Vec<_>>>()?,
                distinct: *distinct,
                filter: filter
                    .as_ref()
                    .map(|f| self.sub_box(f, scopes, row, outer))
                    .transpose()?
                    .map(Box::new),
                order_by: order_by.clone(),
                returning: returning.clone(),
                null_handling: *null_handling,
                unique_keys: *unique_keys,
            },
            X::WindowFunction { .. } => {
                return Err(unsupported(
                    "window functions inside explicit transactions are not yet supported",
                ))
            }
            // Subquery forms are handled by the caller, never reach here.
            X::Exists(_) | X::ScalarSubquery(_) | X::QuantifiedComparison { .. } => {
                return Err(unsubstituted_subquery())
            }
            X::In { .. } => return Err(unsubstituted_subquery()),
        })
    }

    fn sub_box(
        &mut self,
        expr: &Expression,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Expression> {
        self.substitute(expr, scopes, row, outer)
    }
}

/// Helper: combine an OR-chain with an additional IN-list (parsed
/// `x IN (a, b, <subquery>)` mixes both — evaluate the list part too.
impl<'a> TxnQuery<'a> {
    /// Evaluates EXISTS (non-empty check). Uncorrelated subqueries memoize
    /// by AST shape; correlated ones evaluate against the current row.
    fn eval_exists(
        &mut self,
        subq: &Statement,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<bool> {
        let key = memo_key(subq);
        if row.is_none() {
            if let Some(Memo::Bool(cached)) = self.memo.get(&key) {
                return Ok(*cached);
            }
        }
        let outer_ctx = self.outer_for(scopes, row, outer);
        let result = self.select_stmt(subq, &[], outer_ctx.as_ref())?;
        let holds = match result {
            QueryResult::Rows { rows, .. } => !rows.is_empty(),
            _ => false,
        };
        // Memoize only when no row was involved (uncorrelated).
        if row.is_none() {
            self.memo.insert(key, Memo::Bool(holds));
        }
        Ok(holds)
    }

    /// Builds the correlation context for a subquery: current scopes+row
    /// chained over the incoming outer context.
    fn outer_for(
        &self,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> Option<OuterContext> {
        let row = row?;
        Some(OuterContext {
            scopes: scopes.to_vec(),
            row: row.to_vec(),
            parent: outer.cloned().map(Box::new),
        })
    }

    /// Evaluates a scalar subquery: exactly one row/col (0 rows → NULL).
    fn eval_scalar_subquery(
        &mut self,
        subq: &Statement,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Value> {
        // Scalar results memoize only when uncorrelated.
        if row.is_none() {
            let key = memo_key(subq);
            if let Some(Memo::Value(cached)) = self.memo.get(&key) {
                return Ok(cached.clone());
            }
            let outer_ctx = self.outer_for(scopes, row, outer);
            let value = self.scalar_once(subq, outer_ctx.as_ref())?;
            self.memo.insert(key, Memo::Value(value.clone()));
            return Ok(value);
        }
        let outer_ctx = self.outer_for(scopes, row, outer);
        self.scalar_once(subq, outer_ctx.as_ref())
    }

    fn scalar_once(&mut self, subq: &Statement, outer: Option<&OuterContext>) -> SqlResult<Value> {
        match self.select_stmt(subq, &[], outer)? {
            QueryResult::Rows { rows, .. } => match rows.len() {
                0 => Ok(Value::Null),
                1 => {
                    if rows[0].len() != 1 {
                        return Err(SqlError::Storage(PlomidError::new(
                            ErrorKind::InvalidArgument,
                            "subquery must return only one column",
                        )));
                    }
                    Ok(rows[0][0].clone())
                }
                _ => Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    "more than one row returned by a subquery used as an expression",
                ))),
            },
            _ => Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "subquery did not produce rows",
            ))),
        }
    }

    /// Materializes an IN/ANY/ALL subquery to its first-column value set.
    /// Multi-column subqueries are rejected (narrowing, documented).
    fn eval_in_set(
        &mut self,
        subq: &Statement,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Vec<Value>> {
        if row.is_none() {
            let key = memo_key(subq);
            if let Some(Memo::Set(cached)) = self.memo.get(&key) {
                return Ok(cached.clone());
            }
            let outer_ctx = self.outer_for(scopes, row, outer);
            let set = self.in_set_once(subq, outer_ctx.as_ref())?;
            self.memo.insert(key, Memo::Set(set.clone()));
            return Ok(set);
        }
        let outer_ctx = self.outer_for(scopes, row, outer);
        self.in_set_once(subq, outer_ctx.as_ref())
    }

    fn in_set_once(
        &mut self,
        subq: &Statement,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Vec<Value>> {
        match self.select_stmt(subq, &[], outer)? {
            QueryResult::Rows { rows, .. } => {
                let mut set = Vec::with_capacity(rows.len());
                for row in rows {
                    if row.len() != 1 {
                        return Err(SqlError::Storage(PlomidError::new(
                            ErrorKind::InvalidArgument,
                            "subquery must return only one column",
                        )));
                    }
                    set.push(row.into_iter().next().expect("one column"));
                }
                Ok(set)
            }
            _ => Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "subquery did not produce rows",
            ))),
        }
    }

    /// Evaluates `<operand> <op> ANY|ALL (<set>)` over an already
    /// substituted operand. NULL semantics mirror PostgreSQL: an empty set
    /// is false for ANY, true for ALL; NULL members propagate per three-
    /// valued logic (simplified: NULL in set + no definite match → NULL).
    #[allow(clippy::too_many_arguments)]
    fn eval_quantified(
        &mut self,
        operand: &Expression,
        scopes: &[JoinScope],
        row: Option<&[Value]>,
        outer: Option<&OuterContext>,
        operator: plomid_sql::ComparisonOperator,
        quantifier: plomid_sql::Quantifier,
        set: &[Value],
    ) -> SqlResult<bool> {
        use plomid_sql::Quantifier as Q;
        // Evaluate the operand in row context (it is already substituted).
        let probe = match row {
            Some(row) => {
                let mut eval = JoinEval::new(
                    self.facade,
                    self.catalog,
                    scopes,
                    outer,
                    0,
                    self.database,
                    self.user,
                );
                eval.eval(row, operand)?
            }
            None => {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "quantified comparison without row context",
                )));
            }
        };
        let mut saw_null = false;
        let mut matched = false;
        for member in set {
            match compare_values(&probe, operator, member)? {
                Some(true) => {
                    matched = true;
                    if quantifier == Q::Any {
                        return Ok(true);
                    }
                }
                Some(false) => {
                    if quantifier == Q::All {
                        return Ok(false);
                    }
                }
                None => saw_null = true,
            }
        }
        match quantifier {
            Q::Any => Ok(if saw_null { false } else { matched }),
            Q::All => Ok(if matched && !saw_null {
                true
            } else {
                matched && saw_null
            }),
        }
    }

    /// Evaluates one row's filter/target/ON expression: substitutes
    /// subqueries against the row, then runs the pure JoinEval.
    fn eval_row_expr(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        expr: &Expression,
        outer: Option<&OuterContext>,
    ) -> SqlResult<Value> {
        let substituted = self.substitute(expr, scopes, Some(row), outer)?;
        let mut eval = JoinEval::new(
            self.facade,
            self.catalog,
            scopes,
            outer,
            0,
            self.database,
            self.user,
        );
        eval.eval(row, &substituted)
    }

    fn eval_row_bool(
        &mut self,
        scopes: &[JoinScope],
        row: &[Value],
        expr: &Expression,
        outer: Option<&OuterContext>,
    ) -> SqlResult<bool> {
        match self.eval_row_expr(scopes, row, expr, outer)? {
            Value::Bool(true) => Ok(true),
            _ => Ok(false),
        }
    }
}

/// Compares two values under an operator: Some(result), or None when the
/// comparison is NULL (either side NULL). Ordering uses the shared
/// three-way comparator.
fn compare_values(
    left: &Value,
    operator: plomid_sql::ComparisonOperator,
    right: &Value,
) -> SqlResult<Option<bool>> {
    use plomid_sql::ComparisonOperator as Op;
    if left.is_null() || right.is_null() {
        return Ok(None);
    }
    Ok(Some(match operator {
        Op::Equal => crate::row::values_equal(left, right),
        Op::NotEqual => !crate::row::values_equal(left, right),
        Op::Less | Op::LessOrEqual | Op::Greater | Op::GreaterOrEqual => {
            let ordering = crate::query::value_cmp(left, right).ok_or_else(|| {
                SqlError::Storage(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    "values are not comparable",
                ))
            })?;
            use std::cmp::Ordering as O;
            matches!(
                (operator, ordering),
                (Op::Less, O::Less)
                    | (Op::LessOrEqual, O::Less | O::Equal)
                    | (Op::Greater, O::Greater)
                    | (Op::GreaterOrEqual, O::Greater | O::Equal)
            )
        }
    }))
}

/// Resolution map for one flattened scope set: original-or-qualified name
/// → synthetic name. Built once per query block.
struct FlatView {
    map: HashMap<String, String>,
}

impl FlatView {
    fn lookup(&self, name: &str) -> Option<String> {
        let found = self.map.get(name).cloned().or_else(|| {
            // Case-insensitive fallback for unquoted identifiers.
            self.map
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.clone())
        })?;
        if found.is_empty() {
            return None;
        }
        Some(found)
    }

    /// Builds a view directly from flattened synthetic columns
    /// (`alias\x1fcolumn` names).
    fn build_from_flat(flat: &Flat) -> SqlResult<Self> {
        let mut map = HashMap::new();
        for column in &flat.schema.columns {
            let synthetic = column.name.clone();
            // Reverse the synthetic name to qualified + bare keys.
            if let Some((alias, bare)) = synthetic.split_once('\x1f') {
                map.insert(format!("{alias}.{bare}"), synthetic.clone());
                // Ambiguity across scopes marks the bare key.
                let bare_count = flat
                    .schema
                    .columns
                    .iter()
                    .filter(|c| {
                        c.name
                            .split_once('\x1f')
                            .is_some_and(|(_, b)| b == bare || b.eq_ignore_ascii_case(bare))
                    })
                    .count();
                if bare_count > 1 {
                    map.insert(bare.to_string(), String::new());
                } else {
                    map.entry(bare.to_string()).or_insert(synthetic);
                }
            } else {
                map.insert(synthetic.clone(), synthetic);
            }
        }
        Ok(Self { map })
    }

    /// Resolves a reference to its synthetic name: ambiguity and
    /// missing columns are distinct, actionable errors.
    fn resolve(&self, name: &str) -> SqlResult<String> {
        if let Some(found) = self.lookup(name) {
            return Ok(found);
        }
        // Distinguish ambiguous (recorded as empty) from missing.
        let ambiguous = self.map.iter().any(|(key, value)| {
            value.is_empty() && (key == name || key.eq_ignore_ascii_case(name))
        });
        if ambiguous {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!("column reference \"{name}\" is ambiguous"),
            )));
        }
        Err(SqlError::Storage(PlomidError::new(
            ErrorKind::NotFound,
            format!("column \"{name}\" does not exist"),
        )))
    }
}

/// Rewrites one SELECT target for the grouped engine: stars expand (the
/// caller pre-expands, so stars here are an internal error), subqueries in
/// target expressions substitute uncorrelated-first.
fn rewrite_select_target(
    target: &SelectTarget,
    resolve: &impl Fn(&str) -> SqlResult<String>,
    q: &mut TxnQuery,
    scopes: &[JoinScope],
    outer: Option<&OuterContext>,
) -> SqlResult<SelectTarget> {
    match target {
        SelectTarget::Expr { expr, alias } => {
            let substituted = q.substitute(expr, scopes, None, outer)?;
            Ok(SelectTarget::Expr {
                expr: rewrite_refs(&substituted, resolve)?,
                alias: alias.clone(),
            })
        }
        SelectTarget::FunctionCall { name, args } => {
            let mut new_args = Vec::with_capacity(args.len());
            for arg in args {
                let substituted = q.substitute(arg, scopes, None, outer)?;
                new_args.push(rewrite_refs(&substituted, resolve)?);
            }
            Ok(SelectTarget::FunctionCall {
                name: name.clone(),
                args: new_args,
            })
        }
        SelectTarget::Function(_) | SelectTarget::Aliased { .. } => {
            // Aliases resolve during expansion; bare session functions have
            // no references to rewrite.
            Ok(target.clone())
        }
        SelectTarget::All | SelectTarget::QualifiedStar { .. } => Err(SqlError::Storage(
            PlomidError::new(ErrorKind::Internal, "unexpanded star in grouped target"),
        )),
        SelectTarget::WindowFunction { .. } => Err(unsupported(
            "window functions inside explicit transactions are not yet supported",
        )),
    }
}

/// Rewrites GROUP BY keys (subqueries must be uncorrelated here).
fn rewrite_group_by(
    group_by: &plomid_sql::GroupByClause,
    resolve: &impl Fn(&str) -> SqlResult<String>,
    q: &mut TxnQuery,
    scopes: &[JoinScope],
    outer: Option<&OuterContext>,
) -> SqlResult<plomid_sql::GroupByClause> {
    use plomid_sql::GroupByClause as G;
    Ok(match group_by {
        G::Simple(keys) => G::Simple(
            keys.iter()
                .map(|key| {
                    let substituted = q.substitute(key, scopes, None, outer)?;
                    rewrite_refs(&substituted, resolve)
                })
                .collect::<SqlResult<Vec<_>>>()?,
        ),
        G::Sets(sets) => G::Sets(
            sets.iter()
                .map(|keys| {
                    keys.iter()
                        .map(|key| {
                            let substituted = q.substitute(key, scopes, None, outer)?;
                            rewrite_refs(&substituted, resolve)
                        })
                        .collect::<SqlResult<Vec<_>>>()
                })
                .collect::<SqlResult<Vec<_>>>()?,
        ),
    })
}
