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
//! The single-table SELECT fast path.
//!
//! [`execute_select`] answers scans, predicates and simple projections
//! directly from key/value rows. Anything needing joins, CTEs, window
//! functions or subqueries is handed to the general engine in `crate::join`
//! before the fast path commits to its plan.

use super::aggregate::{collect_aggregate_slots, AggregateSlot};
use super::expr::{evaluate_expression, evaluate_predicate};
use super::group::{
    apply_distinct_on, compare_order_keys, execute_grouped_select, par_sort_expr_parallel_safe,
    par_sort_has_costly_keys, par_sort_keys_parallel_safe, par_sort_order_is_total, par_sort_rows,
    par_sort_workers_for, project_row, sort_rows, StreamingGroupedAgg, TopKHeap,
};
use super::project::{
    project_bare_select, select_bare_column_types, select_bare_columns, select_column_types,
    select_columns,
};
use super::scan::{
    columnar_requested_columns, columnar_row_to_values, normalize_time_pruning_expression,
};
use crate::catalog_fn::is_aggregate_function;
use crate::catalog_fn::is_session_function;
use crate::encoding::{decode_row, decode_row_selected, encode_row};
use crate::error::{SqlError, SqlResult};
use crate::index::{index_value_prefix, prefix_end};
use crate::projection::ColumnMask;
use crate::util::unqualify;
use plomid_core::{ErrorKind, PlomidError};
use plomid_optimizer::{CostInputs, EngineChoice, QueryShape};
use plomid_sql::Catalog;
use plomid_sql::ColumnType;
use plomid_sql::Expression;
use plomid_sql::FromClause;
use plomid_sql::InMemoryCatalog;
use plomid_sql::QueryResult;
use plomid_sql::SelectTarget;
use plomid_sql::TableSchema;
use plomid_sql::Value;
use plomid_txn::StorageEngine;
use plomid_types::TypeOid;
use std::collections::BTreeMap;

/// Rows decoded per storage callback in the streaming SELECT path.
///
/// The scan callback holds at most this many `(key, payload)` entries plus
/// whatever aggregate/heap state the query accumulates, so peak transient
/// memory is `O(chunk + groups/top-K)` instead of `O(table)`.
///
/// Measured on a 30K-row full-table walk, feeding the same statement through
/// `scan_for_each` with varying chunks (see the
/// `scan_chunk_size_sweep_informs_default` test): 256–8192 all land within
/// noise of each other, so 1024 is chosen to match the MVCC paged-scan quantum
/// (`SCAN_CHUNK_ROWS`), keeping one lock hold per chunk sub-millisecond.
pub const ANALYTICAL_SCAN_CHUNK_ROWS: usize = 1024;

/// Minimum estimated rows for parallel scan-aggregation on the Laptop
/// tier (see [`crate::parallel_budget::ParallelBudget`]).
///
/// Below this the fan-out (channel hops, per-worker hash maps, the merge)
/// costs more than it saves; above it decode/filter/feed dominates and scales
/// with workers. Set conservatively; calibration follows measurement. The
/// live gate reads the tiered value
/// ([`crate::parallel_budget::scan_min_rows`]); this const pins the Laptop
/// tier (Edge 400_000, Server 50_000).
///
/// Laptop-tier spec: production reads the tiered delegate; tier tests
/// assert the delegate against this const under a Laptop pin.
#[allow(dead_code)]
pub const PAR_MIN_ROWS: u64 = 100_000;

/// Minimum estimated rows for parallel TopK streaming (absolute, all tiers).
///
/// The TopK fan-out (keys-only `count_range` estimate, per-chunk clone +
/// channel hops, heap merge) scales with the scan and never amortized at
/// measured scales: 100k expression-key TopK runs ~1.4x SLOWER parallel than
/// sequential on the laptop reference (76 vs 53ms best-of-3, release), and a
/// 500k run directionally confirms the loss — so the gate stays closed
/// there and cheap-key shapes stay sequential at every scale. Engagement
/// needs 1M estimated rows; 1M-scale curves proving a win are still owed
/// before lowering this. The tiered sort threshold does not bind TopK: the
/// 1M floor sits above every tier's sort gate (Edge 200_000).
const PAR_TOPK_MIN_ROWS: u64 = 1_000_000;

/// Maximum exact rows for the shared-snapshot materializing parallel scan.
///
/// `par_scan_agg` captures one snapshot and materializes the full range once,
/// then index-partitions the rows across workers (perfect balance, zero split
/// discovery: `count_range` binary search costs 65-246pc of a scan and a
/// stride pre-pass 129-133pc at 100k, release, in-process — both uneconomical;
/// byte-space splits are unusable on text-encoded row keys). Above this many
/// rows the caller keeps the streaming channel fan-out, which holds
/// only `2 * workers` chunks in flight. Set from payload sizes (~50-200 B/row
/// keeps 250k rows under ~50 MB transient); calibration follows measurement.
const SCAN_MATERIALIZE_MAX_ROWS: u64 = 250_000;

/// Maximum materialized rows for the row-store ORDER BY full-sort path before
/// spilling sorted runs to bounded temp storage instead of growing the heap.
///
/// `sort_rows_spilled` runs the existing in-memory `sort_rows` byte-identical
/// at or under this bound; above it each bound-sized chunk is sorted in memory
/// and spilled as opaque records through `plomid-spill`'s `SpillManager`
/// (temp-limit enforced, files deleted on `Drop`), then k-way merged with the
/// same comparator. Set from the same payload arithmetic as
/// `SCAN_MATERIALIZE_MAX_ROWS`: decoded rows are wider than storage payloads,
/// so the full-sort budget stays conservative.
const SORT_SPILL_MAX_ROWS: usize = 250_000;

/// Default `rows_touched` for the adaptive optimizer [`QueryShape`].
///
/// No cheap row-count statistics exist at the row-vs-columnar decision site
/// (counting would cost a scan per SELECT), so the shape uses this mid-size
/// table stand-in. Only the cost fallback reads it; the structural rules
/// that decide most shapes ignore row counts. The freshness gate still
/// makes the correctness call regardless of this value.
const OPTIMIZER_DEFAULT_ROWS_TOUCHED: u64 = 100_000;

/// True when the expression tree contains a subquery node.
pub fn expr_has_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::ScalarSubquery(_) | Expression::Exists(_) => true,
        Expression::QuantifiedComparison { .. } => true,
        Expression::In { subquery, .. } => subquery.is_some(),
        Expression::ColumnRef(_) | Expression::Literal(_) | Expression::Star => false,
        Expression::FunctionCall { name, args, .. } => {
            !is_aggregate_function(name) || args.iter().any(expr_has_subquery)
        }
        Expression::Equal(l, r)
        | Expression::NotEqual(l, r)
        | Expression::Less(l, r)
        | Expression::LessOrEqual(l, r)
        | Expression::Greater(l, r)
        | Expression::GreaterOrEqual(l, r)
        | Expression::And(l, r)
        | Expression::Or(l, r)
        | Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r)
        | Expression::Modulo(l, r)
        | Expression::IsDistinctFrom(l, r)
        | Expression::NullIf(l, r) => expr_has_subquery(l) || expr_has_subquery(r),
        Expression::IsNull(i)
        | Expression::IsNotNull(i)
        | Expression::Not(i)
        | Expression::Negate(i) => expr_has_subquery(i),
        Expression::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        Expression::Like { expr, pattern, .. } => {
            expr_has_subquery(expr) || expr_has_subquery(pattern)
        }
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand.as_ref().is_some_and(|o| expr_has_subquery(o))
                || whens
                    .iter()
                    .any(|(c, v)| expr_has_subquery(c) || expr_has_subquery(v))
                || default.as_ref().is_some_and(|d| expr_has_subquery(d))
        }
        Expression::Coalesce(args) => args.iter().any(expr_has_subquery),
        _ => false,
    }
}

/// True when the select list needs the general engine (window functions or
/// embedded subqueries).
fn targets_need_general_engine(targets: &[SelectTarget]) -> bool {
    targets.iter().any(|target| match target {
        SelectTarget::WindowFunction { .. } => true,
        SelectTarget::Expr { expr, .. } => expr_has_subquery(expr),
        SelectTarget::FunctionCall { name, args } => {
            !is_aggregate_function(name) || args.iter().any(expr_has_subquery)
        }
        SelectTarget::Aliased { target, .. } => {
            targets_need_general_engine(std::slice::from_ref(target))
        }
        _ => false,
    })
}

/// True when a bare SELECT (no FROM) can be served by the simple literal /
/// session-function fast path. Anything beyond that — scalar function calls,
/// aggregates, arithmetic, predicates, measured over the single implicit row
/// — must go through the general engine's `bare_select`.
fn bare_select_fast_path_ok(targets: &[SelectTarget]) -> bool {
    targets.iter().all(|target| match target {
        SelectTarget::Expr {
            expr: Expression::Literal(_),
            ..
        } => true,
        SelectTarget::Expr { expr, .. } => match expr {
            Expression::ColumnRef(name) => is_session_function(name),
            Expression::FunctionCall { name, args, .. }
                if args.len() == 1 && matches!(args[0], Expression::Literal(Value::Text(_))) =>
            {
                matches!(name.to_ascii_lowercase().as_str(), "nextval" | "currval")
            }
            _ => false,
        },
        SelectTarget::Function(name) => is_session_function(name),
        SelectTarget::FunctionCall { name, args } => {
            if is_session_function(name) {
                true
            } else {
                args.len() == 1
                    && matches!(args[0], Expression::Literal(Value::Text(_)))
                    && matches!(name.to_ascii_lowercase().as_str(), "nextval" | "currval")
            }
        }
        SelectTarget::Aliased { target, .. } => {
            bare_select_fast_path_ok(std::slice::from_ref(target))
        }
        _ => false,
    })
}

/// True when `expr` (evaluated once per row) needs the statement execution
/// context: any bare or called session/clock function (`now()`,
/// `current_user`, ...) resolves from thread-local [`crate::context`] state
/// that worker threads do not inherit (there they would observe fresh system
/// time or default ids instead of the statement's values). Subqueries and
/// window calls fail closed here too; they never reach the streaming path
/// (the general engine owns them), so declining is behavior-preserving.
pub fn expr_needs_statement_ctx(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef(name) => crate::catalog_fn::is_session_function(name),
        Expression::FunctionCall {
            name,
            args,
            filter,
            order_by,
            ..
        } => {
            crate::catalog_fn::is_session_function(name)
                || args.iter().any(expr_needs_statement_ctx)
                || filter.as_ref().is_some_and(|a| expr_needs_statement_ctx(a))
                || order_by
                    .iter()
                    .any(|item| expr_needs_statement_ctx(&item.expr))
        }
        Expression::Equal(a, b)
        | Expression::NotEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::And(a, b)
        | Expression::Or(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b)
        | Expression::Modulo(a, b)
        | Expression::Concat(a, b)
        | Expression::Power(a, b)
        | Expression::NullIf(a, b)
        | Expression::IsDistinctFrom(a, b)
        | Expression::BitAnd(a, b)
        | Expression::BitOr(a, b)
        | Expression::BitXor(a, b)
        | Expression::ShiftLeft(a, b)
        | Expression::ShiftRight(a, b)
        | Expression::JsonArrow {
            left: a, right: b, ..
        }
        | Expression::ArrayIndex { array: a, index: b }
        | Expression::JsonSubscript { array: a, index: b } => {
            expr_needs_statement_ctx(a) || expr_needs_statement_ctx(b)
        }
        Expression::IsNull(a)
        | Expression::IsNotNull(a)
        | Expression::Not(a)
        | Expression::Negate(a)
        | Expression::Cast { expr: a, .. }
        | Expression::TypeCast { expr: a, .. }
        | Expression::Extract { expr: a, .. }
        | Expression::IsJson { expr: a, .. }
        | Expression::IsBoolean { expr: a, .. }
        | Expression::RowField { expr: a, .. } => expr_needs_statement_ctx(a),
        Expression::In {
            expr,
            list,
            subquery,
            ..
        } => {
            subquery.is_some()
                || expr_needs_statement_ctx(expr)
                || list.iter().any(expr_needs_statement_ctx)
        }
        Expression::Between {
            expr, low, high, ..
        } => {
            expr_needs_statement_ctx(expr)
                || expr_needs_statement_ctx(low)
                || expr_needs_statement_ctx(high)
        }
        Expression::Like { expr, pattern, .. } => {
            expr_needs_statement_ctx(expr) || expr_needs_statement_ctx(pattern)
        }
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand
                .as_ref()
                .is_some_and(|a| expr_needs_statement_ctx(a))
                || whens
                    .iter()
                    .any(|(a, b)| expr_needs_statement_ctx(a) || expr_needs_statement_ctx(b))
                || default
                    .as_ref()
                    .is_some_and(|a| expr_needs_statement_ctx(a))
        }
        Expression::Coalesce(v) => v.iter().any(expr_needs_statement_ctx),
        Expression::Exists(_)
        | Expression::ScalarSubquery(_)
        | Expression::QuantifiedComparison { .. }
        | Expression::WindowFunction { .. } => true,
        Expression::Literal(_)
        | Expression::VectorLiteral(_)
        | Expression::Star
        | Expression::DateLiteral(_)
        | Expression::TimestampLiteral(_)
        | Expression::TypedTimestampLiteral { .. }
        | Expression::TimestamptzLiteral(_)
        | Expression::TypedTimestamptzLiteral { .. }
        | Expression::TimeLiteral(_)
        | Expression::TypedTimeLiteral { .. } => false,
    }
}

/// True when an aggregate call in the projection carries its own ORDER BY
/// (`SUM(v ORDER BY v)`). The streaming accumulator folds rows in scan order
/// and ignores that modifier; the parallel merge must not change that, so
/// such shapes stay sequential. The match is exhaustive with no wildcard arm
/// so a new `Expression` variant is a compile error here, not a missed walk.
pub fn agg_call_has_order_by(targets: &[SelectTarget], having: Option<&Expression>) -> bool {
    targets.iter().any(target_has_agg_order_by) || having.is_some_and(expr_has_agg_order_by)
}

/// True when any target or HAVING expression needs statement context.
///
/// Scope-4 parallel finish evaluates HAVING and targets on workers without
/// TLS; the entry gate must decline those shapes (par_streaming_agg finishes
/// on the main thread, but the shared gate stays conservative for both).
pub fn targets_need_statement_ctx(targets: &[SelectTarget], having: Option<&Expression>) -> bool {
    if having
        .as_ref()
        .is_some_and(|expr| expr_needs_statement_ctx(expr))
    {
        return true;
    }
    targets.iter().any(|target| match target {
        SelectTarget::Expr { expr, .. } => expr_needs_statement_ctx(expr),
        SelectTarget::Aliased { target, .. } => match &**target {
            SelectTarget::Expr { expr, .. } => expr_needs_statement_ctx(expr),
            _ => false,
        },
        SelectTarget::FunctionCall { args, .. } => args.iter().any(expr_needs_statement_ctx),
        SelectTarget::All
        | SelectTarget::QualifiedStar { .. }
        | SelectTarget::Function(_)
        | SelectTarget::WindowFunction { .. } => false,
    })
}

fn target_has_agg_order_by(target: &SelectTarget) -> bool {
    match target {
        SelectTarget::Expr { expr, .. } => expr_has_agg_order_by(expr),
        SelectTarget::Aliased { target, .. } => target_has_agg_order_by(target),
        SelectTarget::FunctionCall { args, .. } => args.iter().any(expr_has_agg_order_by),
        SelectTarget::All
        | SelectTarget::QualifiedStar { .. }
        | SelectTarget::Function(_)
        | SelectTarget::WindowFunction { .. } => false,
    }
}

fn expr_has_agg_order_by(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall {
            name,
            args,
            filter,
            order_by,
            ..
        } => {
            (is_aggregate_function(name) && !order_by.is_empty())
                || args.iter().any(expr_has_agg_order_by)
                || filter.as_ref().is_some_and(|a| expr_has_agg_order_by(a))
                || order_by
                    .iter()
                    .any(|item| expr_has_agg_order_by(&item.expr))
        }
        Expression::Equal(a, b)
        | Expression::NotEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::And(a, b)
        | Expression::Or(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b)
        | Expression::Modulo(a, b)
        | Expression::Concat(a, b)
        | Expression::Power(a, b)
        | Expression::NullIf(a, b)
        | Expression::IsDistinctFrom(a, b)
        | Expression::BitAnd(a, b)
        | Expression::BitOr(a, b)
        | Expression::BitXor(a, b)
        | Expression::ShiftLeft(a, b)
        | Expression::ShiftRight(a, b)
        | Expression::JsonArrow {
            left: a, right: b, ..
        }
        | Expression::ArrayIndex { array: a, index: b }
        | Expression::JsonSubscript { array: a, index: b } => {
            expr_has_agg_order_by(a) || expr_has_agg_order_by(b)
        }
        Expression::IsNull(a)
        | Expression::IsNotNull(a)
        | Expression::Not(a)
        | Expression::Negate(a)
        | Expression::Cast { expr: a, .. }
        | Expression::TypeCast { expr: a, .. }
        | Expression::Extract { expr: a, .. }
        | Expression::IsJson { expr: a, .. }
        | Expression::IsBoolean { expr: a, .. }
        | Expression::RowField { expr: a, .. } => expr_has_agg_order_by(a),
        Expression::In { expr, list, .. } => {
            expr_has_agg_order_by(expr) || list.iter().any(expr_has_agg_order_by)
        }
        Expression::Between {
            expr, low, high, ..
        } => {
            expr_has_agg_order_by(expr) || expr_has_agg_order_by(low) || expr_has_agg_order_by(high)
        }
        Expression::Like { expr, pattern, .. } => {
            expr_has_agg_order_by(expr) || expr_has_agg_order_by(pattern)
        }
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand.as_ref().is_some_and(|a| expr_has_agg_order_by(a))
                || whens
                    .iter()
                    .any(|(a, b)| expr_has_agg_order_by(a) || expr_has_agg_order_by(b))
                || default.as_ref().is_some_and(|a| expr_has_agg_order_by(a))
        }
        Expression::Coalesce(v) => v.iter().any(expr_has_agg_order_by),
        Expression::ColumnRef(_)
        | Expression::Literal(_)
        | Expression::VectorLiteral(_)
        | Expression::Star
        | Expression::DateLiteral(_)
        | Expression::TimestampLiteral(_)
        | Expression::TypedTimestampLiteral { .. }
        | Expression::TimestamptzLiteral(_)
        | Expression::TypedTimestamptzLiteral { .. }
        | Expression::TimeLiteral(_)
        | Expression::TypedTimeLiteral { .. }
        | Expression::Exists(_)
        | Expression::ScalarSubquery(_)
        | Expression::QuantifiedComparison { .. }
        | Expression::WindowFunction { .. } => false,
    }
}

/// Eligibility gate for parallel scan-aggregation. ALL must hold, else the
/// caller keeps the sequential loop:
///
/// 1. `estimated_rows >= scan_min_rows` (tiered:
///    [`crate::parallel_budget::scan_min_rows`]; fan-out only pays off at
///    scale; calibration follows measurement).
/// 2. Checked by the caller: `workers >= 2` (no point fanning out to one).
/// 3. Every slot is `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `AVG(col)` /
///    `MIN(col)` / `MAX(col)` / `COUNT(DISTINCT col)` with a plain
///    `None`/`Star`/resolvable-`ColumnRef` argument, no `FILTER`, and no
///    `DISTINCT` except plain `COUNT(DISTINCT colref)`. Any other aggregate
///    name (JSON included) declines.
/// 4. Group keys: any shape the existing path accepts — the merge is
///    key-agnostic — except session-bound keys (worker threads do not inherit
///    the statement context), which decline like the caller's `WHERE` guard.
/// 5. No float/NUMERIC `SUM`/`AVG` anywhere: parallel reduction order would
///    change the last ulp vs the single-threaded fold. Float `MIN`/`MAX`/
///    `COUNT` stay eligible (order-free folds).
pub fn par_agg_eligible(
    slots: &[AggregateSlot],
    group_exprs: &[Expression],
    schema: &TableSchema,
    estimated_rows: u64,
) -> bool {
    if estimated_rows < crate::parallel_budget::scan_min_rows() {
        return false;
    }
    for slot in slots {
        let name = slot.name.to_ascii_lowercase();
        if !matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
            return false;
        }
        if slot.filter().is_some() {
            return false;
        }
        if slot.second_arg().is_some() {
            return false;
        }
        let is_count = name == "count";
        let is_sum_avg = name == "sum" || name == "avg";
        if slot.distinct() {
            if !is_count {
                return false;
            }
            match slot.arg() {
                Some(arg @ Expression::ColumnRef(col)) => {
                    if schema.column_index(unqualify(col)).is_err() || expr_needs_statement_ctx(arg)
                    {
                        return false;
                    }
                }
                _ => return false,
            }
        } else {
            match slot.arg() {
                None | Some(Expression::Star) => {
                    if !is_count {
                        return false;
                    }
                }
                Some(arg @ Expression::ColumnRef(col)) => {
                    let Ok(index) = schema.column_index(unqualify(col)) else {
                        return false;
                    };
                    if expr_needs_statement_ctx(arg) {
                        return false;
                    }
                    if is_sum_avg {
                        let oid = schema.columns[index].col_type.type_oid;
                        if !matches!(oid, TypeOid::INT2 | TypeOid::INT4 | TypeOid::INT8) {
                            return false;
                        }
                    }
                }
                _ => return false,
            }
        }
    }
    if group_exprs.iter().any(expr_needs_statement_ctx) {
        return false;
    }
    true
}

/// Parallel streaming GROUP BY / aggregation over the row store.
///
/// Correctness invariants (all load-bearing):
///
/// - Single snapshot: the MAIN thread runs the one existing
///   `engine.scan_for_each` over the FULL range (unchanged call, same chunk
///   size); workers never touch the engine or any snapshot.
/// - Disjoint chunks: each 1024-row chunk is decoded (`decode_row_selected`
///   with the projection), filtered (`evaluate_predicate`, exactly like the
///   sequential loop) and fed into a thread-local `StreamingGroupedAgg`.
///   Owned chunks move through per-worker channels round-robin; only small
///   metadata (`&TableSchema`, slots, group expressions) is shared by
///   reference, and workers never touch the engine.
/// - Deterministic merge order: worker partials union into worker 0's in
///   worker-index order (`merge_from`); integer `COUNT`/`SUM`/`AVG`,
///   `COUNT DISTINCT` sets and `MIN`/`MAX` folds are order-free, so the union
///   equals the single-threaded fold.
/// - Error determinism: each worker keeps its first decode/filter/feed error
///   and stops; the producer observes the stop flag (or a dropped receiver)
///   and ends the scan cleanly via `Ok(false)`. The first error in
///   worker-index order wins, mirroring the sequential fail-fast.
/// - Float exclusion: the gate keeps float/NUMERIC `SUM`/`AVG` sequential,
///   so no reduction-order ulp drift is possible here.
#[allow(clippy::too_many_arguments)]
pub fn par_streaming_agg<E: StorageEngine>(
    engine: &mut E,
    schema: &TableSchema,
    scan_start: &[u8],
    scan_end: &[u8],
    projection: &ColumnMask,
    where_expr: Option<&Expression>,
    slots: &[AggregateSlot],
    group_exprs: &[Expression],
    targets: &[SelectTarget],
    having: Option<&Expression>,
    current_database: &str,
    current_user: &str,
    estimated_rows: u64,
    workers: usize,
) -> SqlResult<QueryResult> {
    // `estimated_rows` is advisory (gate threshold only, decided by the
    // caller); execution never depends on it.
    let _ = estimated_rows;
    // Defensive: the gate guarantees `workers >= 2`, but running with more
    // workers is always still correct (never a zero-worker merge).
    let workers = workers.max(2);
    type StoredChunk = Vec<(Vec<u8>, Vec<u8>)>;
    // One channel per worker (bound 2 each, `2 * workers` chunks in flight
    // overall): each worker owns its receiver, so a failing worker drops it
    // and the producer's next send to that worker fails instead of blocking
    // forever — no shared receiver, no extra lock, no deadlock.
    let mut senders = Vec::with_capacity(workers);
    let mut receivers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StoredChunk>(2);
        senders.push(tx);
        receivers.push(rx);
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    let stop_flag = &stop;

    let (scan_res, partials) = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for rx in receivers {
            handles.push(scope.spawn(move || {
                let mut agg = StreamingGroupedAgg::new(slots, group_exprs);
                let mut failure: Option<SqlError> = None;
                // Ends when the main thread drops all senders after the scan;
                // a worker-side failure breaks early and drops this receiver,
                // failing the producer's next send to it.
                for chunk in rx {
                    for (_, bytes) in &chunk {
                        let row = match decode_row_selected(bytes, projection.selected()) {
                            Ok(row) => row,
                            Err(e) => {
                                failure = Some(SqlError::Storage(e));
                                break;
                            }
                        };
                        let matched = match where_expr {
                            Some(pred) => match evaluate_predicate(&row, schema, pred) {
                                Ok(matched) => matched,
                                Err(e) => {
                                    failure = Some(e);
                                    break;
                                }
                            },
                            None => true,
                        };
                        if !matched {
                            continue;
                        }
                        if let Err(e) = agg.feed(&row, schema) {
                            failure = Some(e);
                            break;
                        }
                    }
                    if failure.is_some() {
                        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
                (agg, failure)
            }));
        }
        // MAIN thread: the existing full-range scan (single snapshot,
        // unchanged call); each owned chunk goes to the next worker
        // round-robin (uniform 1024-row chunks, so static balancing is even).
        // A failed send means that worker already failed and dropped its
        // receiver: stop the scan cleanly, like the stop flag below.
        let mut next: usize = 0;
        let scan_res = engine.scan_for_each(
            Some(scan_start),
            Some(scan_end),
            ANALYTICAL_SCAN_CHUNK_ROWS,
            &mut |chunk| {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    return Ok(false);
                }
                let target = next % workers;
                next += 1;
                if senders[target].send(chunk.to_vec()).is_err() {
                    return Ok(false);
                }
                Ok(true)
            },
        );
        drop(senders);
        // Join in worker-index order for deterministic errors and merges.
        let mut partials = Vec::with_capacity(workers);
        for handle in handles {
            // A worker panic mirrors a sequential panic on the same input
            // (e.g. debug integer overflow in SUM); propagate, never mask.
            partials.push(handle.join().expect("par agg worker"));
        }
        (scan_res, partials)
    });
    // Sequential priority: a storage scan error wins over row errors.
    scan_res.map_err(SqlError::Storage)?;
    let mut merged: Option<StreamingGroupedAgg> = None;
    let mut first_error: Option<SqlError> = None;
    for (agg, err) in partials {
        if let Some(acc) = merged.as_mut() {
            acc.merge_from(agg);
        } else {
            merged = Some(agg);
        }
        if first_error.is_none() {
            first_error = err;
        }
    }
    if let Some(e) = first_error {
        return Err(e);
    }
    merged.expect("at least one worker partial").finish(
        schema,
        targets,
        having,
        current_database,
        current_user,
    )
}

/// Balanced index partitions of `len` rows across `workers`.
///
/// Returns disjoint `[start, end)` ranges covering `[0, len)` in order;
/// empty ranges are dropped, so the result is never longer than `len` and
/// never empty when `len > 0`. Balance is exact by construction (first
/// `len % n` partitions take one extra row), which is what makes the
/// shared-snapshot materializing scan viable: no split-key discovery pass.
pub fn partition_index_splits(len: usize, workers: usize) -> Vec<(usize, usize)> {
    let n = workers.max(1).min(len.max(1));
    let base = len / n;
    let extra = len % n;
    let mut splits = Vec::with_capacity(n);
    let mut start = 0;
    for i in 0..n {
        let take = base + usize::from(i < extra);
        let end = start + take;
        if take > 0 {
            splits.push((start, end));
        }
        start = end;
    }
    splits
}

/// Parallel scan-aggregation over one shared snapshot.
///
/// Returns `Ok(Some(result))` when the slice ran, `Ok(None)` when the caller
/// must fall back to `par_streaming_agg`: capture/scan failure (the fallback
/// re-scans and surfaces the real error, same precedent as the `count_range`
/// estimate), unsupported engine (default trait impl), or exact rows above
/// `SCAN_MATERIALIZE_MAX_ROWS` (memory discipline: the streaming fan-out
/// holds only bounded chunks). Row decode/filter/feed errors are `Err`, same
/// fail-fast as `par_streaming_agg` — never silent fallback.
///
/// Correctness invariants:
/// - One snapshot: captured once up front (pinned-aware per engine); every
///   worker resolves rows under that handle, so concurrent commits never
///   appear mid-scan. Workers share `&E` (`StorageEngine: Send + Sync`).
/// - Disjoint exactly-once coverage: one `scan_shared` materialization split
///   by `partition_index_splits`; concatenation in partition order preserves
///   key order.
/// - Deterministic merge: workers joined in partition order, partials merged
///   via `merge_from` (same as `par_streaming_agg`); first error in partition
///   order wins. Storage capture/scan failure falls back before spawning, so
///   no partial state exists on that path.
#[allow(clippy::too_many_arguments)]
pub fn par_scan_agg<E: StorageEngine>(
    engine: &mut E,
    schema: &TableSchema,
    scan_start: &[u8],
    scan_end: &[u8],
    projection: &ColumnMask,
    where_expr: Option<&Expression>,
    slots: &[AggregateSlot],
    group_exprs: &[Expression],
    targets: &[SelectTarget],
    having: Option<&Expression>,
    current_database: &str,
    current_user: &str,
    workers: usize,
) -> SqlResult<Option<QueryResult>> {
    let workers = workers.max(2);
    // Any pre-spawn storage failure falls back: the fallback path re-scans
    // and surfaces the real error from its own scan.
    let Ok(snapshot) = engine.capture_shared_snapshot() else {
        return Ok(None);
    };
    let shared: &E = &*engine;
    let Ok(rows) = shared.scan_shared(&snapshot, Some(scan_start), Some(scan_end)) else {
        return Ok(None);
    };
    if rows.len() as u64 > SCAN_MATERIALIZE_MAX_ROWS {
        return Ok(None);
    }
    let splits = partition_index_splits(rows.len(), workers);
    if splits.is_empty() {
        // Zero-row range: identical to the sequential empty fold.
        let agg = StreamingGroupedAgg::new(slots, group_exprs);
        return agg
            .finish(schema, targets, having, current_database, current_user)
            .map(Some);
    }
    let partials = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(splits.len());
        let rows_ref = &rows;
        for (start, end) in splits {
            handles.push(scope.spawn(move || {
                let mut agg = StreamingGroupedAgg::new(slots, group_exprs);
                let mut failure: Option<SqlError> = None;
                for (_, bytes) in &rows_ref[start..end] {
                    let row = match decode_row_selected(bytes, projection.selected()) {
                        Ok(row) => row,
                        Err(e) => {
                            failure = Some(SqlError::Storage(e));
                            break;
                        }
                    };
                    let matched = match where_expr {
                        Some(pred) => match evaluate_predicate(&row, schema, pred) {
                            Ok(matched) => matched,
                            Err(e) => {
                                failure = Some(e);
                                break;
                            }
                        },
                        None => true,
                    };
                    if !matched {
                        continue;
                    }
                    if let Err(e) = agg.feed(&row, schema) {
                        failure = Some(e);
                        break;
                    }
                }
                (agg, failure)
            }));
        }
        // Join in partition order for deterministic errors and merges; a
        // worker panic mirrors a sequential panic — propagate, never mask.
        let mut partials = Vec::with_capacity(handles.len());
        for handle in handles {
            partials.push(handle.join().expect("par scan worker"));
        }
        partials
    });
    // Error short-circuit: merging is infallible and its result is discarded
    // on error, so skip the shuffle when any worker failed. The outcome is
    // identical (first error in partition order) without the wasted work.
    let mut aggs = Vec::with_capacity(partials.len());
    let mut first_error: Option<SqlError> = None;
    for (agg, err) in partials {
        if first_error.is_none() {
            first_error = err;
        }
        aggs.push(agg);
    }
    if let Some(e) = first_error {
        return Err(e);
    }
    // Single-partition fast path: nothing to shuffle, finish directly.
    if aggs.len() == 1 {
        let only = aggs.pop().expect("one partial");
        return only
            .finish(schema, targets, having, current_database, current_user)
            .map(Some);
    }
    // Scope 2 (group shuffle): re-partition groups by key hash so each owner
    // holds a disjoint key subset. Draining moves entries (no copies); every
    // group lands on exactly one owner however many workers fed it.
    let owners = aggs.len();
    let buckets = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(aggs.len());
        for mut agg in aggs {
            handles.push(scope.spawn(move || {
                let mut mine = Vec::with_capacity(owners);
                for owner in 0..owners {
                    mine.push(agg.drain_for_owner(owner, owners));
                }
                mine
            }));
        }
        let mut buckets = Vec::with_capacity(handles.len());
        for handle in handles {
            buckets.push(handle.join().expect("par shuffle worker"));
        }
        buckets
    });
    // Transpose worker-major buckets to owner-major (pointer moves only).
    let mut per_owner: Vec<Vec<StreamingGroupedAgg>> =
        (0..owners).map(|_| Vec::with_capacity(owners)).collect();
    for mut row in buckets {
        for (owner, part) in row.drain(..).enumerate() {
            per_owner[owner].push(part);
        }
    }
    // Scope 3: owners merge their buckets in worker-index order — the same
    // per-key merge order as the unshuffled fold, so promotion-tail
    // semantics (int/float/NUMERIC SUM/AVG) are unchanged.
    let merged_owners = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(per_owner.len());
        for parts in per_owner {
            handles.push(scope.spawn(move || {
                let mut acc: Option<StreamingGroupedAgg> = None;
                for part in parts {
                    if let Some(a) = acc.as_mut() {
                        a.merge_from(part);
                    } else {
                        acc = Some(part);
                    }
                }
                acc.expect("owner has one bucket per worker")
            }));
        }
        let mut merged = Vec::with_capacity(handles.len());
        for handle in handles {
            merged.push(handle.join().expect("par owner worker"));
        }
        merged
    });
    // Scope 4 (parallel finish): each owner map is disjoint, so HAVING /
    // projection per group run in parallel and rows concat in owner order.
    // Row order without ORDER BY is unspecified (sequential finish iterates
    // HashMaps); owner-order concat is additionally deterministic across
    // runs. First error in owner order wins.
    let finished = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(merged_owners.len());
        for owner_agg in merged_owners {
            handles.push(scope.spawn(move || {
                owner_agg.finish(schema, targets, having, current_database, current_user)
            }));
        }
        let mut finished = Vec::with_capacity(handles.len());
        for handle in handles {
            finished.push(handle.join().expect("par finish worker"));
        }
        finished
    });
    let mut columns = None;
    let mut column_types = None;
    let mut rows = Vec::new();
    for result in finished {
        let result = result?;
        let QueryResult::Rows {
            columns: cols,
            column_types: types,
            rows: mut part,
        } = result
        else {
            continue;
        };
        if columns.is_none() {
            columns = Some(cols);
            column_types = Some(types);
        }
        rows.append(&mut part);
    }
    // All owners empty (grouped query, no matches): canonical empty result
    // comes from finishing an empty partial, exactly like sequential.
    if columns.is_none() {
        return StreamingGroupedAgg::new(slots, group_exprs)
            .finish(schema, targets, having, current_database, current_user)
            .map(Some);
    }
    Ok(Some(QueryResult::Rows {
        columns: columns.expect("set above"),
        column_types: column_types.expect("set above"),
        rows,
    }))
}

/// Maximum `TopKHeap` capacity eligible for parallel TopK fan-out.
///
/// Each worker retains up to `cap` rows, so transient heap state is bounded
/// by `workers x cap`; above this bound the caller keeps the sequential
/// bounded heap (which holds only `cap`). Small caps (the usual
/// `LIMIT 10/100/1000`) always qualify; huge caps stay sequential.
/// Currently staged (parallel TopK unwired pending cost-gated evidence).
#[allow(dead_code)]
const PAR_TOPK_MAX_CAP: usize = 100_000;

/// Parallel streaming `ORDER BY ... LIMIT` over the row store (TopK site).
///
/// Same contract as the sequential bounded-heap walk: every matched row is
/// key-evaluated once and offered to a `TopKHeap(cap)`; the output is the
/// heap's sorted rows with the `offset` prefix skipped. Shape of the work:
/// the MAIN thread runs the one existing `scan_for_each` over the full
/// range (unchanged call, same chunk size) and forwards owned chunks with
/// scan-order sequence numbers round-robin; each worker decodes
/// (`decode_row_selected` with the projection), filters
/// (`evaluate_predicate`, exactly like the sequential loop), key-evaluates
/// and offers into a thread-local `TopKHeap(cap)`. After the join the
/// worker heaps merge by re-offering every retained entry into one final
/// heap in worker-index order, then a single `into_sorted_rows` plus the
/// offset skip, then the unchanged sequential projection.
///
/// Exactness: the caller proves a total order
/// ([`par_sort_order_is_total`](super::group::par_sort_order_is_total))
/// before calling. Each worker's local top-`cap` is a superset of its
/// share of the global top-`cap` (a row ranked `<= cap` globally ranks
/// `<= cap` within its own partition), so the union of the locals contains
/// the global top-`cap`, and the final heap over that union equals the
/// sequential heap bit-for-bit; with no ties the sorted output is bit-exact.
/// Key/filter/decode errors match the sequential first-in-scan-order error
/// exactly: workers record the scan-order sequence number of their first
/// failing chunk (and stop at their first failing row within it), and the
/// lowest sequence wins — the same row the sequential walk stops at, with
/// the identical error value. Deterministic for every input; without the
/// gate only key-order (not tie identities) is guaranteed.
///
/// Storage errors keep sequential priority (scan error wins over row
/// errors); a worker panic mirrors a sequential panic on the same input and
/// propagates. No mutex, no global state, `std::thread::scope` only.
///
/// Wired behind the cost gate at the row-store TopK site (total order +
/// worker-safe keys/filter + costly keys + threshold estimate + budget
/// claim); the sequential heap stays the fallback on any decline.
#[allow(clippy::too_many_arguments)]
fn par_topk_streaming<E: StorageEngine>(
    engine: &mut E,
    schema: &TableSchema,
    scan_start: &[u8],
    scan_end: &[u8],
    projection: &ColumnMask,
    where_expr: Option<&Expression>,
    order_by: &[plomid_sql::OrderByItem],
    cap: usize,
    offset: usize,
    targets: &[SelectTarget],
    current_database: &str,
    current_user: &str,
    workers: usize,
) -> SqlResult<QueryResult> {
    let workers = workers.max(2);
    type StoredChunk = Vec<(Vec<u8>, Vec<u8>)>;
    // One channel per worker (bound 2 each): each worker owns its receiver,
    // so a failing worker drops it and the producer's next send to that
    // worker fails instead of blocking forever.
    let mut senders = Vec::with_capacity(workers);
    let mut receivers = Vec::with_capacity(workers);
    for _ in 0..workers {
        let (tx, rx) = std::sync::mpsc::sync_channel::<(u64, StoredChunk)>(2);
        senders.push(tx);
        receivers.push(rx);
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    let stop_flag = &stop;

    let (scan_res, mut partials) = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for rx in receivers {
            handles.push(scope.spawn(move || {
                let mut heap = TopKHeap::new(cap);
                // Scan-order sequence number of the first failing chunk, if
                // any (within the chunk the worker stops at its first
                // failing row, exactly like the sequential walk).
                let mut failure: Option<(u64, SqlError)> = None;
                for (seq, chunk) in rx {
                    for (_, bytes) in &chunk {
                        let row = match decode_row_selected(bytes, projection.selected()) {
                            Ok(row) => row,
                            Err(e) => {
                                failure = Some((seq, SqlError::Storage(e)));
                                break;
                            }
                        };
                        let matched = match where_expr {
                            Some(pred) => match evaluate_predicate(&row, schema, pred) {
                                Ok(matched) => matched,
                                Err(e) => {
                                    failure = Some((seq, e));
                                    break;
                                }
                            },
                            None => true,
                        };
                        if !matched {
                            continue;
                        }
                        // No early break: ORDER BY must observe every match;
                        // the bound is the heap itself, not the walk length.
                        let keys = match order_by
                            .iter()
                            .map(|item| evaluate_expression(&row, schema, &item.expr))
                            .collect::<SqlResult<Vec<_>>>()
                        {
                            Ok(keys) => keys,
                            Err(e) => {
                                failure = Some((seq, e));
                                break;
                            }
                        };
                        heap.offer(row, keys, order_by);
                    }
                    if failure.is_some() {
                        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
                (heap, failure)
            }));
        }
        // MAIN thread: the existing full-range scan (unchanged call); each
        // owned chunk goes to the next worker round-robin with its
        // scan-order sequence number. A failed send means that worker
        // already failed and dropped its receiver: stop cleanly.
        let mut next: u64 = 0;
        let scan_res = engine.scan_for_each(
            Some(scan_start),
            Some(scan_end),
            ANALYTICAL_SCAN_CHUNK_ROWS,
            &mut |chunk| {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    return Ok(false);
                }
                let target = (next as usize) % workers;
                let seq = next;
                next += 1;
                if senders[target].send((seq, chunk.to_vec())).is_err() {
                    return Ok(false);
                }
                Ok(true)
            },
        );
        drop(senders);
        // Join in worker-index order for deterministic merges; a worker
        // panic mirrors a sequential panic — propagate, never mask.
        let mut partials = Vec::with_capacity(handles.len());
        for handle in handles {
            partials.push(handle.join().expect("par topk worker"));
        }
        (scan_res, partials)
    });
    // Sequential priority: a storage scan error wins over row errors.
    scan_res.map_err(SqlError::Storage)?;
    let mut heaps = Vec::with_capacity(partials.len());
    let mut best_seq: Option<u64> = None;
    for (_, failure) in &partials {
        if let Some((seq, _)) = failure {
            if best_seq.is_none_or(|best| *seq < best) {
                best_seq = Some(*seq);
            }
        }
    }
    if let Some(best_seq) = best_seq {
        for (_, failure) in &mut partials {
            if failure.as_ref().is_some_and(|(seq, _)| *seq == best_seq) {
                let (_, e) = failure.take().expect("sequence matched above");
                return Err(e);
            }
        }
        unreachable!("best sequence came from one of the failures");
    }
    for (heap, _) in partials {
        heaps.push(heap);
    }
    // Merge by re-offering every retained entry into one final heap, in
    // worker-index order (deterministic). Under the gate the union holds
    // the exact global top-`cap`, so the final heap equals the sequential
    // one entry-for-entry.
    let mut merged = TopKHeap::new(cap);
    for heap in heaps {
        for (row, keys) in heap.into_entries() {
            merged.offer(row, keys, order_by);
        }
    }
    let mut rows: Vec<Vec<Value>> = merged
        .into_sorted_rows(order_by)
        .into_iter()
        .map(|row| project_row(&row, schema, targets, current_database, current_user))
        .collect::<SqlResult<_>>()?;
    if offset > 0 {
        rows = rows.into_iter().skip(offset).collect();
    }
    let cols = select_columns(schema, targets)?;
    let column_types = select_column_types(schema, targets)?;
    Ok(QueryResult::Rows {
        columns: cols,
        column_types,
        rows,
    })
}

/// Worker width for parallel TopK over `estimated` rows, or `None` when
/// the caller must stay sequential. ALL must hold, else the sequential heap:
///
/// 1. proven total order ([`par_sort_order_is_total`](super::group::par_sort_order_is_total))
///    — the deterministic merge is exact;
/// 2. worker-safe keys ([`par_sort_keys_parallel_safe`](super::group::par_sort_keys_parallel_safe))
///    and a worker-safe `WHERE` ([`par_sort_expr_parallel_safe`](super::group::par_sort_expr_parallel_safe))
///    — workers do not inherit statement context;
/// 3. costly keys ([`par_sort_has_costly_keys`](super::group::par_sort_has_costly_keys))
///    — cheap colref keys cannot amortize the fan-out (measured ~2x slower
///    than sequential at 100k rows); only real per-row evaluation wins;
/// 4. scale: `estimated` (a keys-only `count_range` figure, advisory —
///    filtered matches are fewer) clears [`PAR_TOPK_MIN_ROWS`] with at
///    least two workers ([`par_sort_workers_for`](super::group::par_sort_workers_for)
///    derives the width under the tiered sort gate, which the 1M floor
///    always clears).
///
/// The caller claims the reported width through the box-wide budget and runs
/// the sequential heap on decline. Pure over its inputs (no global state, no
/// I/O) so the gate is unit-pinned below.
pub fn par_topk_workers_for(
    schema: &TableSchema,
    order_by: &[plomid_sql::OrderByItem],
    where_expr: Option<&Expression>,
    estimated: u64,
) -> Option<usize> {
    if estimated < PAR_TOPK_MIN_ROWS
        || !par_sort_order_is_total(schema, order_by)
        || !par_sort_keys_parallel_safe(order_by)
        || !par_sort_has_costly_keys(order_by)
        || where_expr.is_some_and(|expr| !par_sort_expr_parallel_safe(expr))
    {
        return None;
    }
    par_sort_workers_for(estimated)
}

/// Maps a spill-layer failure to a structured execution error (no panic).
fn spill_err(error: plomid_spill::SpillError) -> SqlError {
    use plomid_spill::SpillError as E;
    let (kind, message) = match &error {
        E::Decode(detail) => (
            ErrorKind::RowEncoding,
            format!("sort spill decode failed: {detail}"),
        ),
        other => (
            ErrorKind::Io,
            format!("sort spill temp storage failed: {other}"),
        ),
    };
    SqlError::Storage(PlomidError::new(kind, message))
}

/// ORDER BY full sort with a bounded-temp spillover.
///
/// At or under `budget_rows` this is exactly [`sort_rows`] (same call, same
/// comparator, same `top_k` semantics). Above it, bound-sized chunks are
/// sorted with [`sort_rows`], spilled as opaque storage-codec records (key
/// frame + row frame) through one [`plomid_spill::SpillManager`] — so temp
/// bytes stay under the manager limit and temp files vanish on `Drop` — and
/// k-way merged with [`compare_order_keys`], breaking ties to the lowest run
/// index (deterministic; the sequential stable sort's tie order is preserved
/// because chunks are in row order). Key-evaluation errors surface for the
/// same first-in-row-order row as [`sort_rows`]: chunks are evaluated
/// front-to-back, so the first failing chunk holds the first failing row.
///
/// `Value` carries floats and has no `Ord`/`Hash`, so the generic spill
/// operators (`ExternalSort`, `SpillDistinct`) cannot take these rows without
/// new trait impls; this pipe uses only the untyped `SpillManager`/`SpillRun`
/// byte layer plus the existing row codec and comparator.
pub fn sort_rows_spilled(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    order_by: &[plomid_sql::OrderByItem],
    top_k: Option<usize>,
    budget_rows: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    if rows.len() <= budget_rows {
        return sort_rows(rows, schema, order_by, top_k);
    }
    if top_k == Some(0) {
        return Ok(Vec::new());
    }
    // Bound-sized chunks, never empty (a zero budget still moves forward).
    let chunk_rows = budget_rows.max(1);
    let seq = SORT_SPILL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp_dir =
        std::env::temp_dir().join(format!("plomid-sort-spill-{}-{seq}", std::process::id()));
    let manager = plomid_spill::SpillManager::with_temp_dir(temp_dir.clone()).map_err(spill_err)?;
    // The temp dir is removed on every exit (success or structured error);
    // run files themselves vanish on `Drop` regardless of how we leave.
    let result = sort_spilled_inner(rows, schema, order_by, top_k, chunk_rows, &manager);
    let _ = std::fs::remove_dir(&temp_dir);
    result
}

/// Over-budget body of [`sort_rows_spilled`]: chunk-sort, spill, k-way merge.
/// Temp-dir removal stays with the caller so every exit path cleans up.
fn sort_spilled_inner(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    order_by: &[plomid_sql::OrderByItem],
    top_k: Option<usize>,
    chunk_rows: usize,
    manager: &plomid_spill::SpillManager,
) -> SqlResult<Vec<Vec<Value>>> {
    let mut runs = Vec::with_capacity(rows.len().div_ceil(chunk_rows.max(1)));
    for chunk in rows.chunks(chunk_rows) {
        let sorted = sort_rows(chunk.to_vec(), schema, order_by, None)?;
        let mut run = manager.create_run().map_err(spill_err)?;
        for row in &sorted {
            let keys = order_by
                .iter()
                .map(|item| evaluate_expression(row, schema, &item.expr))
                .collect::<SqlResult<Vec<_>>>()?;
            run.write_record(&encode_row(&keys)?).map_err(spill_err)?;
            run.write_record(&encode_row(row)?).map_err(spill_err)?;
        }
        run.flush().map_err(spill_err)?;
        runs.push(run);
    }
    // Deterministic k-way merge over the sorted runs: the minimum head wins,
    // comparator ties break to the lowest run index. Row bytes stay opaque
    // until their run wins; only decoded keys are compared.
    let mut readers = Vec::with_capacity(runs.len());
    for run in runs.iter_mut() {
        readers.push(run.open_reader().map_err(spill_err)?);
    }
    // Prime one decoded head per run; a lone key frame is corruption.
    let mut heads: Vec<Option<(Vec<Value>, Vec<u8>)>> = Vec::with_capacity(readers.len());
    for reader in readers.iter_mut() {
        let head = match reader.next_record().map_err(spill_err)? {
            None => None,
            Some(keys_bytes) => {
                let Some(row_bytes) = reader.next_record().map_err(spill_err)? else {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::RowEncoding,
                        "sort spill run ended mid-record",
                    )));
                };
                Some((decode_row(&keys_bytes)?, row_bytes))
            }
        };
        heads.push(head);
    }
    let take = top_k.unwrap_or(usize::MAX);
    let mut out = Vec::new();
    while out.len() < take {
        let mut best: Option<usize> = None;
        for (index, head) in heads.iter().enumerate() {
            let Some((keys, _)) = head else { continue };
            let wins = match best {
                None => true,
                Some(current) => {
                    let (current_keys, _) = heads[current].as_ref().expect("best has a head");
                    compare_order_keys(keys, current_keys, order_by) == std::cmp::Ordering::Less
                }
            };
            if wins {
                best = Some(index);
            }
        }
        let Some(winner) = best else { break };
        let (_, row_bytes) = heads[winner].take().expect("winner has a head");
        out.push(decode_row(&row_bytes)?);
        heads[winner] = match readers[winner].next_record().map_err(spill_err)? {
            None => None,
            Some(keys_bytes) => {
                let Some(next_row) = readers[winner].next_record().map_err(spill_err)? else {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::RowEncoding,
                        "sort spill run ended mid-record",
                    )));
                };
                Some((decode_row(&keys_bytes)?, next_row))
            }
        };
    }
    let spilled = manager.bytes_spilled();
    plomid_core::observability::record_spill_bytes(spilled);
    drop(readers);
    drop(runs);
    tracing::debug!(
        target: "sql::query",
        event = "sort_spill",
        rows = out.len(),
        spill_bytes = spilled,
    );
    Ok(out)
}

/// Process-wide sequence numbering sort-spill temp dirs (one dir per
/// over-budget sort; run files inside never collide across sorts).
static SORT_SPILL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[allow(clippy::too_many_arguments)]
pub fn execute_select<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    targets: Vec<SelectTarget>,
    distinct: bool,
    distinct_on: Vec<Expression>,
    from: Option<FromClause>,
    where_expr: Option<Expression>,
    group_by: Option<plomid_sql::GroupByClause>,
    having: Option<Expression>,
    order_by: Vec<plomid_sql::OrderByItem>,
    limit: Option<usize>,
    offset: Option<usize>,
    current_database: &str,
    current_user: &str,
    columnar_store: Option<&plomid_columnar::ColumnarStore>,
) -> SqlResult<QueryResult> {
    // DISTINCT / DISTINCT ON must remove duplicates before LIMIT/OFFSET are
    // applied, so the inner execution runs unbounded and post-processing
    // applies both.
    let has_distinct_on = !distinct_on.is_empty();
    let (inner_limit, inner_offset) = if distinct || has_distinct_on {
        (None, None)
    } else {
        (limit, offset)
    };
    let result = execute_select_impl(
        engine,
        catalog,
        targets,
        from,
        where_expr,
        group_by,
        having,
        order_by,
        inner_limit,
        inner_offset,
        current_database,
        current_user,
        distinct_on.clone(),
        columnar_store,
    )?;
    Ok(crate::distinct::apply_distinct(
        result,
        distinct,
        &distinct_on,
        limit,
        offset,
    ))
}

#[allow(clippy::too_many_arguments)]
fn execute_select_impl<E: StorageEngine>(
    engine: &mut E,
    catalog: &InMemoryCatalog,
    targets: Vec<SelectTarget>,
    from: Option<FromClause>,
    where_expr: Option<Expression>,
    group_by: Option<plomid_sql::GroupByClause>,
    having: Option<Expression>,
    order_by: Vec<plomid_sql::OrderByItem>,
    limit: Option<usize>,
    offset: Option<usize>,
    current_database: &str,
    current_user: &str,
    distinct_on: Vec<Expression>,
    columnar_store: Option<&plomid_columnar::ColumnarStore>,
) -> SqlResult<QueryResult> {
    // Queries needing the general engine: joins, FROM subqueries, window
    // functions, or subqueries anywhere in the statement.
    let group_sets: Option<Vec<Vec<Expression>>> = group_by.as_ref().map(|g| g.to_sets());
    let group_union: Vec<Expression> = group_by
        .as_ref()
        .map(|g| g.union_exprs())
        .unwrap_or_default();
    let needs_general_engine = from.as_ref().is_some_and(|f| {
        !matches!(f, FromClause::Table { .. })
            || matches!(f, FromClause::Table { name, .. } if catalog.get_view(name).is_some() || crate::system_catalog::SystemCatalog::is_relation(name))
    }) || targets.iter().any(|t| matches!(t, SelectTarget::FunctionCall { name, .. } if name.eq_ignore_ascii_case("string_agg") || name.eq_ignore_ascii_case("array_agg") || crate::json::is_json_aggregate(name)))
        || targets.iter().any(target_has_json_aggregate)
        || group_union.iter().any(|e| !matches!(e, Expression::ColumnRef(_)))
        || group_by.is_some() && !order_by.is_empty()
        || group_sets.as_ref().is_some_and(|sets| sets.len() > 1)
        || targets_need_general_engine(&targets)
        || where_expr.as_ref().is_some_and(expr_has_subquery)
        || group_union.iter().any(expr_has_subquery)
        || having.as_ref().is_some_and(expr_has_subquery)
        || order_by.iter().any(|item| expr_has_subquery(&item.expr))
        || (from.is_none() && !bare_select_fast_path_ok(&targets));
    if needs_general_engine {
        return crate::join::execute_join_select(
            engine,
            catalog,
            from.as_ref(),
            targets,
            where_expr,
            group_by,
            having,
            order_by,
            limit,
            offset,
            current_database,
            current_user,
            None,
            0,
            distinct_on,
        );
    }
    tracing::trace!(
        target: "sql::query",
        "select from={:?}",
        from.as_ref().map(|f| match f {
            FromClause::Table { name, .. } => name.clone(),
            FromClause::TableFunction { name, .. } => name.clone(),
            FromClause::Join { .. } => "<join>".to_string(),
            FromClause::Subquery { .. } => "<subquery>".to_string(),
        })
    );
    if let Some(from) = from {
        // Multi-source FROM: joins or subqueries need the full join executor.
        if !matches!(from, FromClause::Table { .. }) {
            return crate::join::execute_join_select(
                engine,
                catalog,
                Some(&from),
                targets,
                where_expr,
                group_by,
                having,
                order_by,
                limit,
                offset,
                current_database,
                current_user,
                None,
                0,
                distinct_on,
            );
        }
        let FromClause::Table { name: from, .. } = from else {
            unreachable!("guarded above");
        };
        let schema = catalog.get_table(&from)?;
        // A pinned repeatable-read snapshot cannot use paths whose results
        // cannot honor it: post-snapshot publications would leak newer
        // committed rows through the columnar/vector branches, and a live
        // ART probe would miss rows deleted after the snapshot began. The
        // row path resolves every row through the pinned snapshot, so it
        // stays exact; index pruning and columnar stay eligible only for
        // fresh (unpinned) reads.
        let read_pinned = engine.is_read_pinned();
        // Index bounds are resolved BEFORE the columnar decision: a usable
        // index range answers selective predicates from a handful of entries,
        // while the columnar path would materialize the whole generation and
        // filter it row-at-a-time (measured 100x slower on point lookups).
        // Columnar stays eligible only when no index range applies.
        let index_range = if read_pinned {
            None
        } else {
            index_bounds_for_predicate(catalog, &from, where_expr.as_ref())
        };
        // Columnar eligibility is decided from authoritative shared state,
        // never from session-local memory: the table's published generation
        // must be proven to cover every committed write (shared freshness
        // map, maintained by all sessions' writes and vacuums), and that
        // generation must still be the currently published one — or, when
        // stale, every post-publication write must be delta-covered (the
        // per-table coverage flag, certified at publication and preserved
        // only across delta-staging DML). Fresh connections discover
        // eligibility without ever having run VACUUM; anything unproven
        // falls back to Hot Row Store state.
        //
        // The columnar projection is resolved once here because it feeds
        // both the adaptive optimizer shape below and the scan itself.
        let requested_columns = columnar_requested_columns(
            &targets,
            where_expr.as_ref(),
            &order_by,
            group_by.as_ref(),
            having.as_ref(),
            &distinct_on,
            schema,
        );
        // Early-termination cap for plain SELECT ... LIMIT/OFFSET without
        // ordering, grouping, aggregation, or dedup (DISTINCT clears the
        // limit in the outer wrapper, so `Some` here always permits capping).
        // The cap bounds *matches*: the streaming walks below stop after the
        // cap-th match (predicateless or filtered alike), so a capped plain
        // SELECT never holds more than `cap` decoded rows. Resolved here
        // (ahead of the columnar block) because the vector filter scan takes
        // it as its `LIMIT` pushdown as well as the row-store walk below.
        let needs_full_scan = !order_by.is_empty()
            || group_by.is_some()
            || having.is_some()
            || !distinct_on.is_empty()
            || count_distinct_target(&targets).is_some()
            || count_filter_target(&targets).is_some()
            || targets.iter().any(|t| match t {
                SelectTarget::Function(name) => is_aggregate_function(name),
                SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
                SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
                SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
                _ => false,
            });
        let row_cap = match limit {
            Some(take) if !needs_full_scan => Some(offset.unwrap_or(0).saturating_add(take)),
            _ => None,
        };
        // Adaptive optimizer consult: runs per single-table SELECT with
        // real inputs — selectivity from WHERE presence (index-bound
        // predicates are point-ish, unindexed filters use the unknown
        // midpoint 0.5), projection width from the requested-column
        // analysis above, aggregation presence from GROUP BY/HAVING and
        // aggregate targets. Not honestly knowable at this site and
        // therefore documented defaults: rows_touched (no cheap row-count
        // statistics exist here; the freshness gate below decides
        // correctness regardless), randomness 0.0 (key-ordered scans),
        // cpu_available 1, memory_rows matching the cost-model default.
        // The freshness gate still wins every disagreement (correctness
        // first); disagreements are only recorded via the shared
        // analytical-path counters, never acted on.
        let optimizer_shape = QueryShape {
            rows_touched: OPTIMIZER_DEFAULT_ROWS_TOUCHED,
            cols_touched: requested_columns
                .as_ref()
                .map_or(schema.columns.len(), Vec::len),
            total_cols: schema.columns.len().max(1),
            selectivity: if where_expr.is_none() {
                1.0
            } else if index_range.is_some() {
                0.01
            } else {
                0.5
            },
            has_aggregation: group_by.is_some()
                || having.is_some()
                || targets.iter().any(|target| match target {
                    SelectTarget::Function(name) => is_aggregate_function(name),
                    SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
                    SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
                    SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
                    _ => false,
                }),
            has_join: false,
            join_cardinality: 0,
            randomness: 0.0,
            cache_hit_rate: 0.0,
            cpu_available: 1,
            memory_rows: 1_000_000,
            index_available: index_range.is_some(),
        };
        let optimizer_decision =
            plomid_optimizer::choose_engine_auto(&optimizer_shape, &CostInputs::default());
        // Hybrid is a columnar-family answer for this binary gate: the
        // executor has no hybrid operator, so only a Row verdict can
        // disagree with taking the columnar path.
        let optimizer_wants_columnar = !matches!(optimizer_decision.choice, EngineChoice::Row);
        // Relaxed HTAP gate: fresh generations serve both columnar scans, and
        // stale-but-delta-covered tables serve the scalar scan (which always
        // merges confirmed deltas under snapshot visibility). `delta_admitted`
        // forces the scalar scan below: the vector fast path aggregates
        // published segments without a delta overlay and must stay
        // fresh-only. The optimizer disagreement counters key off
        // `gate_allows_columnar`, so delta-admitted reads count as
        // columnar-family there by construction; `read_pinned` and usable
        // index ranges still win outright, exactly as before.
        let mut delta_admitted = false;
        let gate_allows_columnar = index_range.is_none()
            && !read_pinned
            && columnar_store.is_some_and(|store| {
                catalog
                    .resolve_table_name(&from)
                    .ok()
                    .is_some_and(|qualified| {
                        if crate::columnar_freshness::is_generation_fresh(
                            store,
                            engine.root(),
                            current_database,
                            &qualified,
                            schema.table_id.get(),
                        ) {
                            true
                        } else {
                            let covered = crate::columnar_freshness::is_generation_delta_covered(
                                store,
                                engine.root(),
                                current_database,
                                &qualified,
                                schema.table_id.get(),
                            );
                            delta_admitted = covered;
                            covered
                        }
                    })
            });
        if columnar_store.is_some() {
            if gate_allows_columnar && !optimizer_wants_columnar {
                plomid_core::observability::note_analytical_path("optimizer_override_row");
            } else if !gate_allows_columnar
                && optimizer_wants_columnar
                && index_range.is_none()
                && !read_pinned
            {
                // The gate denied on freshness/coverage alone while the optimizer
                // prefers columnar-family execution.
                plomid_core::observability::note_analytical_path("optimizer_override_column");
            }
        }
        let columnar_rows = if gate_allows_columnar {
            let mut columns = BTreeMap::new();
            for (index, column) in schema.columns.iter().enumerate() {
                // Columnar materialization uses zero-based positional column
                // identities; the SQL catalog's ColumnIds are global metadata
                // identities and must not be passed to the pruner.
                columns.insert(
                    column.name.clone(),
                    plomid_core::ColumnId::new(index as u64),
                );
            }
            let pruning = where_expr.as_ref().and_then(|expr| {
                let normalized = normalize_time_pruning_expression(expr, schema);
                plomid_columnar::lower_sql_expression(&normalized, &columns)
                    .predicate()
                    .cloned()
            });
            // `requested_columns` is resolved once before the optimizer
            // consult above and reused here: same analysis, same result.
            let store = columnar_store.expect("columnar store checked above");
            // Vector attempt first (fresh generations only): supported
            // aggregate shapes never materialize Rows at all. Delta-admitted
            // reads skip it — the vector kernel aggregates published segments
            // without a delta overlay, so only the merging scalar scan below
            // can answer them. Anything unsupported returns None and the
            // scalar scan below answers exactly as before.
            let vector_result = if delta_admitted {
                None
            } else {
                super::vector::try_vector_columnar(
                    engine,
                    schema,
                    &targets,
                    where_expr.as_ref(),
                    group_by.as_ref(),
                    having.as_ref(),
                    &order_by,
                    limit,
                    offset,
                    &distinct_on,
                    &columns,
                    store,
                    plomid_core::ObjectId::new(schema.table_id.get()),
                )?
            };
            if let Some(vector_result) = vector_result {
                tracing::debug!(
                    target: "sql::query",
                    event = "analytical_path",
                    path = "vector",
                    table = %from,
                );
                return Ok(vector_result);
            }
            let object = plomid_core::ObjectId::new(schema.table_id.get());
            // Vector filter scan first (fresh generations only): supported
            // WHERE shapes materialize only matching rows through column
            // batches, with `row_cap` as the `LIMIT` pushdown. Anything
            // unsupported returns None and the scalar read below answers
            // exactly as before. Delta-admitted reads skip it — the kernel
            // aggregates published segments without a delta overlay, so only
            // the merging scalar scan below can answer them.
            let vector_scan = if delta_admitted {
                None
            } else {
                match (where_expr.as_ref(), requested_columns.as_deref()) {
                    (Some(predicate), Some(requested)) if !requested.is_empty() => {
                        crate::query::vector::try_vector_filter_scan(
                            engine, schema, predicate, requested, row_cap, &columns, store, object,
                        )?
                    }
                    _ => None,
                }
            };
            if let Some(vector_scan) = vector_scan {
                tracing::debug!(
                    target: "sql::query",
                    event = "analytical_path",
                    path = "vector_scan",
                    table = %from,
                );
                Some((vector_scan.rows, vector_scan.column_ids))
            } else {
                let scan = plomid_columnar::read_current_generation_rows(
                    store,
                    engine,
                    object,
                    pruning.as_ref(),
                    requested_columns.as_deref(),
                )?;
                if let Some(scan) = scan {
                    tracing::debug!(
                        target: "sql::columnar",
                        table = %from,
                        segments_considered = scan.segments_considered,
                        segments_skipped = scan.segments_skipped,
                        rows_examined = scan.rows_examined,
                        rows_skipped = scan.rows_skipped,
                        columns_read = scan.columns_read,
                        "columnar SQL scan"
                    );
                    Some((scan.rows, scan.column_ids))
                } else {
                    None
                }
            }
        } else {
            None
        };
        // Index-driven candidate discovery. A plain single-column index can
        // answer `=`, `<`, `<=`, `>`, `>=` and `BETWEEN` as one contiguous key
        // range, which turns a selective predicate from a full table scan into
        // an index range probe. When no index or no order-preserving bound
        // applies, this returns `None` and the table scan below answers the
        // statement exactly as before.
        // Which stored columns can this statement actually read? The mask is
        // built once per scan, then every row decodes only those columns;
        // when the analysis cannot prove a set it yields the all-columns mask,
        // which is exactly the historical full decode.
        let projection = crate::projection::required_columns(
            schema,
            &targets,
            where_expr.as_ref(),
            group_by.as_ref(),
            having.as_ref(),
            &order_by,
            &distinct_on,
        )
        .unwrap_or_else(|| crate::projection::ColumnMask::all(schema.columns.len()));

        // `SELECT COUNT(*)` with no predicate reads no column at all: count
        // visible rows without materializing them. This runs before any scan
        // so a million-row count walks keys only (~µs, zero transient rows)
        // instead of cloning every key and payload first (~450MB transient
        // measured at 1M rows). Row-framing validation is skipped on this
        // path; page checksums still guard storage integrity, corrupting rows
        // surface on reads that decode them, and the columnar branch never
        // validated either.
        //
        // `is_bare_count_star` is required as well as the empty column set:
        // `count(NULL)` also references no column but must count zero, so only
        // the literal `count(*)` shape may be answered by a row count.
        if projection.reads_no_column()
            && crate::projection::is_bare_count_star(&targets)
            && where_expr.is_none()
            && group_by.is_none()
            && having.is_none()
            && order_by.is_empty()
            && distinct_on.is_empty()
        {
            let visible = engine.count_range(
                Some(format!("{from}:").as_bytes()),
                Some(format!("{from}:\u{10FFFF}").as_bytes()),
            )? as i64;
            let mut rows = vec![vec![Value::Int8(visible)]];
            if offset.unwrap_or(0) > 0 || limit.is_some() {
                rows = rows
                    .into_iter()
                    .skip(offset.unwrap_or(0))
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();
            }
            tracing::trace!(
                target: "sql::query",
                "select count-only complete from={} row_count={visible}",
                from
            );
            tracing::debug!(
                target: "sql::query",
                event = "analytical_path",
                path = "row_count",
                table = %from,
            );
            return Ok(QueryResult::Rows {
                columns: select_columns(schema, &targets)?,
                column_types: select_column_types(schema, &targets)?,
                rows,
            });
        }

        // Early-termination cap for plain SELECT ... LIMIT/OFFSET without
        // ordering, grouping, aggregation, or dedup: resolved ahead of the
        // columnar block above (feeds the vector `LIMIT` pushdown there and
        // the row-store walk below).
        let row_cap = match limit {
            Some(take) if !needs_full_scan => Some(offset.unwrap_or(0).saturating_add(take)),
            _ => None,
        };

        let has_aggregation = targets.iter().any(|t| match t {
            SelectTarget::Function(name) => is_aggregate_function(name),
            SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
            SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
            SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
            _ => false,
        }) || group_by.is_some()
            || having.is_some();

        // Indexed probe path: selective by construction (one contiguous index
        // range resolved to K row keys + one batched `get_many`). K is the
        // match count, not the table size, so the historical materialized flow
        // is kept verbatim here: point-read latency is the regression guard
        // and must not change for OLAP work.
        if let Some((_index_name, idx_start, idx_end)) = index_range {
            let index_entries = engine.scan(Some(&idx_start), Some(&idx_end))?;
            let keys: Vec<Vec<u8>> = index_entries
                .into_iter()
                .map(|(_, row_key)| row_key)
                .collect();
            let values = engine.get_many(&keys)?;
            let mut entries = Vec::with_capacity(keys.len());
            for (row_key, value) in keys.into_iter().zip(values) {
                if let Some(value) = value {
                    entries.push((row_key, value));
                }
            }
            let count_distinct = count_distinct_target(&targets);
            let count_filter = count_filter_target(&targets);
            let grouping_sets_len = group_by.as_ref().map(|g| g.to_sets().len()).unwrap_or(0);
            // Streaming selection (no `filtered_rows` copy): aggregate and
            // count shapes fold each decoded row straight into accumulators.
            // The gate mirrors the scan path's `stream_agg` exactly so both
            // paths agree on which shapes stream; sort/distinct/project and
            // multi-set groupings keep the materialized flow untouched.
            // Transaction-neutral: same probe batch, same snapshot, same
            // row order, SELECT-only (no locks, no staging, no DML).
            let stream_agg = has_aggregation
                && count_distinct.is_none()
                && count_filter.is_none()
                && grouping_sets_len <= 1;
            if let Some((arg, filter)) = count_distinct {
                debug_assert!(row_cap.is_none());
                let mut values = std::collections::HashSet::new();
                for (_, value_bytes) in entries {
                    let row = decode_row_selected(&value_bytes, projection.selected())?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    if let Some(filter) = filter.as_ref() {
                        if !matches!(
                            evaluate_expression(&row, schema, filter)?,
                            Value::Bool(true)
                        ) {
                            continue;
                        }
                    }
                    let value = evaluate_expression(&row, schema, &arg)?;
                    if !value.is_null() {
                        values.insert(value.to_sql_text());
                    }
                }
                return Ok(QueryResult::Rows {
                    columns: vec!["count".into()],
                    column_types: vec![Some(ColumnType::bigint())],
                    rows: vec![vec![Value::Int8(values.len() as i64)]],
                });
            }
            if let Some(filter) = count_filter {
                debug_assert!(row_cap.is_none());
                let mut count = 0i64;
                for (_, value_bytes) in entries {
                    let row = decode_row_selected(&value_bytes, projection.selected())?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    if matches!(
                        evaluate_expression(&row, schema, &filter)?,
                        Value::Bool(true)
                    ) {
                        count += 1;
                    }
                }
                return Ok(QueryResult::Rows {
                    columns: vec!["count".into()],
                    column_types: vec![Some(ColumnType::bigint())],
                    rows: vec![vec![Value::Int8(count)]],
                });
            }
            if stream_agg {
                debug_assert!(row_cap.is_none());

                let slots = collect_aggregate_slots(&targets, having.as_ref());
                let group_exprs: Vec<Expression> = group_by
                    .as_ref()
                    .map(|g| g.to_sets().into_iter().next().unwrap_or_default())
                    .unwrap_or_default();
                let mut agg = StreamingGroupedAgg::new(&slots, &group_exprs);
                for (_, value_bytes) in entries {
                    let row = decode_row_selected(&value_bytes, projection.selected())?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    agg.feed(&row, schema)?;
                }
                return agg.finish(
                    schema,
                    &targets,
                    having.as_ref(),
                    current_database,
                    current_user,
                );
            }
            // Pure-radius vector search (no ORDER BY, no aggregates): ANN
            // candidate well over the full row set + exact verification,
            // instead of per-row SQL predicate evaluation. Any ineligibility
            // falls through to the generic scan below with byte-identical
            // results (same decoder, projection, ordering, and LIMIT tail).
            if order_by.is_empty()
                && !has_aggregation
                && distinct_on.is_empty()
                && offset.unwrap_or(0) == 0
            {
                if let Some(ref expr) = where_expr {
                    let mut full_rows: Vec<Vec<Value>> = Vec::new();
                    if let Some(cap) = row_cap {
                        full_rows.reserve(cap.min(1024));
                    }
                    for (_, value_bytes) in &entries {
                        full_rows.push(decode_row_selected(value_bytes, projection.selected())?);
                        if row_cap.is_some_and(|cap| full_rows.len() >= cap) {
                            break;
                        }
                    }
                    if let Some(radius_result) = crate::vector_ann::try_ann_radius(
                        engine,
                        catalog,
                        current_database,
                        &from,
                        schema,
                        &full_rows,
                        expr,
                        limit,
                        current_user,
                    ) {
                        let radius_rows: Vec<Vec<Value>> = radius_result?;
                        let mut rows: Vec<Vec<Value>> = radius_rows
                            .into_iter()
                            .map(|row| {
                                project_row(&row, schema, &targets, current_database, current_user)
                            })
                            .collect::<SqlResult<_>>()?;
                        if limit.is_some() {
                            rows = rows.into_iter().take(limit.unwrap_or(usize::MAX)).collect();
                        }
                        let cols = select_columns(schema, &targets)?;
                        let column_types = select_column_types(schema, &targets)?;
                        return Ok(QueryResult::Rows {
                            columns: cols,
                            column_types,
                            rows,
                        });
                    }
                }
            }
            let mut filtered_rows: Vec<Vec<Value>> = Vec::new();
            if let Some(cap) = row_cap {
                filtered_rows.reserve(cap.min(1024));
            }
            'scan: for (_, value_bytes) in entries {
                let row = decode_row_selected(&value_bytes, projection.selected())?;
                if let Some(ref expr) = where_expr {
                    if !evaluate_predicate(&row, schema, expr)? {
                        continue;
                    }
                }
                filtered_rows.push(row);
                if row_cap.is_some_and(|cap| filtered_rows.len() >= cap) {
                    break 'scan;
                }
            }
            if has_aggregation {
                return execute_grouped_select(
                    schema,
                    &targets,
                    group_by,
                    having,
                    filtered_rows,
                    current_database,
                    current_user,
                );
            }
            let mut source_rows = filtered_rows;
            if !order_by.is_empty() {
                let top_k = limit.map(|take| offset.unwrap_or(0).saturating_add(take));
                // The probe/index site stays sequential;
                // only the row-store sites parallelize (same gate would apply).
                source_rows = match crate::vector_ann::try_ann_order(
                    engine,
                    catalog,
                    current_database,
                    &from,
                    schema,
                    &source_rows,
                    &order_by,
                    top_k,
                    where_expr.as_ref().map(|e| format!("{e:?}")),
                    current_user,
                    None,
                ) {
                    Some(ann) => ann?,
                    None => sort_rows(source_rows, schema, &order_by, top_k)?,
                };
            }
            if !distinct_on.is_empty() {
                if group_by.is_some()
                    || having.is_some()
                    || targets.iter().any(|t| match t {
                        SelectTarget::Function(name) => is_aggregate_function(name),
                        SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
                        SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
                        SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
                        _ => false,
                    })
                {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Unsupported,
                        "DISTINCT ON is not supported with GROUP BY or aggregates",
                    )));
                }
                source_rows = apply_distinct_on(
                    source_rows,
                    schema,
                    &distinct_on,
                    &order_by,
                    &targets,
                    current_database,
                    current_user,
                )?;
            }
            let mut rows: Vec<Vec<Value>> = source_rows
                .into_iter()
                .map(|row| project_row(&row, schema, &targets, current_database, current_user))
                .collect::<SqlResult<_>>()?;
            if offset.unwrap_or(0) > 0 || limit.is_some() {
                rows = rows
                    .into_iter()
                    .skip(offset.unwrap_or(0))
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();
            }
            let cols = select_columns(schema, &targets)?;
            let column_types = select_column_types(schema, &targets)?;
            tracing::trace!(
                target: "sql::query",
                "select complete from={} row_count={}",
                from,
                rows.len()
            );
            return Ok(QueryResult::Rows {
                columns: cols,
                column_types,
                rows,
            });
        }

        // Non-indexed path: full-range scans (row store via `scan_for_each`,
        // or an already-materialized columnar generation). Peak memory here
        // used to be `O(table)` twice over — the storage `scan` Vec plus the
        // decoded `filtered_rows` Vec (~450MB transient at 1M rows). The
        // streaming branches below accumulate only aggregate/group/heap/count
        // state over bounded scan chunks instead.
        let count_distinct = count_distinct_target(&targets);
        let count_filter = count_filter_target(&targets);
        let grouping_sets_len = group_by.as_ref().map(|g| g.to_sets().len()).unwrap_or(0);
        let stream_agg = has_aggregation
            && count_distinct.is_none()
            && count_filter.is_none()
            && grouping_sets_len <= 1;
        // Bounded top-K applies only when LIMIT exists, no aggregation, and
        // DISTINCT ON is absent (dedup happens after the sort, so truncating
        // the pre-dedup stream to K would change results; that shape keeps the
        // historical full-sort path).
        let heap_cap: Option<usize> = if !order_by.is_empty()
            && !has_aggregation
            && count_distinct.is_none()
            && count_filter.is_none()
            && distinct_on.is_empty()
        {
            limit.map(|take| offset.unwrap_or(0).saturating_add(take))
        } else {
            None
        };

        // Columnar generation path. The generation Vec itself is still
        // materialized by `read_current_generation_rows` (a Priority-2
        // columnar-chunking follow-up); the second copy — decoded
        // `filtered_rows` — is eliminated by feeding each row straight into
        // the aggregate/heap/count state.
        if let Some((rows, columnar_columns)) = columnar_rows {
            tracing::debug!(
                target: "sql::query",
                event = "analytical_path",
                path = "columnar_scan",
                table = %from,
            );
            if let Some((arg, filter)) = count_distinct {
                let mut values = std::collections::HashSet::new();
                for row in rows {
                    let row = columnar_row_to_values(row, schema, &columnar_columns)?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    if let Some(filter) = filter.as_ref() {
                        if !matches!(
                            evaluate_expression(&row, schema, filter)?,
                            Value::Bool(true)
                        ) {
                            continue;
                        }
                    }
                    let value = evaluate_expression(&row, schema, &arg)?;
                    if !value.is_null() {
                        values.insert(value.to_sql_text());
                    }
                }
                return Ok(QueryResult::Rows {
                    columns: vec!["count".into()],
                    column_types: vec![Some(ColumnType::bigint())],
                    rows: vec![vec![Value::Int8(values.len() as i64)]],
                });
            }
            if let Some(filter) = count_filter {
                let mut count = 0i64;
                for row in rows {
                    let row = columnar_row_to_values(row, schema, &columnar_columns)?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    if matches!(
                        evaluate_expression(&row, schema, &filter)?,
                        Value::Bool(true)
                    ) {
                        count += 1;
                    }
                }
                return Ok(QueryResult::Rows {
                    columns: vec!["count".into()],
                    column_types: vec![Some(ColumnType::bigint())],
                    rows: vec![vec![Value::Int8(count)]],
                });
            }
            if stream_agg {
                let slots = collect_aggregate_slots(&targets, having.as_ref());
                let group_exprs: Vec<Expression> = group_by
                    .as_ref()
                    .map(|g| g.to_sets().into_iter().next().unwrap_or_default())
                    .unwrap_or_default();
                let mut agg = StreamingGroupedAgg::new(&slots, &group_exprs);
                for row in rows {
                    let row = columnar_row_to_values(row, schema, &columnar_columns)?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    agg.feed(&row, schema)?;
                }
                return agg.finish(
                    schema,
                    &targets,
                    having.as_ref(),
                    current_database,
                    current_user,
                );
            }
            if let Some(cap) = heap_cap.filter(|_| {
                // ANN-eligible orderings skip the streaming heap: the flow
                // falls through to the materialized sort site, which serves
                // them from the cached graph (see `vector_ann`).
                !crate::vector_ann::ann_eligible(
                    engine,
                    catalog,
                    &from,
                    schema,
                    &order_by,
                    heap_cap,
                    where_expr.is_some(),
                )
            }) {
                // The columnar TopK site stays sequential;
                // only the row-store TopK site parallelizes (same gate would apply).
                let mut heap = TopKHeap::new(cap);
                for row in rows {
                    let row = columnar_row_to_values(row, schema, &columnar_columns)?;
                    if let Some(ref expr) = where_expr {
                        if !evaluate_predicate(&row, schema, expr)? {
                            continue;
                        }
                    }
                    // No early break: ORDER BY must observe every match; the
                    // bound is the heap itself, not the walk length.
                    let keys = order_by
                        .iter()
                        .map(|item| evaluate_expression(&row, schema, &item.expr))
                        .collect::<SqlResult<Vec<_>>>()?;
                    heap.offer(row, keys, &order_by);
                }
                let mut rows: Vec<Vec<Value>> = heap
                    .into_sorted_rows(&order_by)
                    .into_iter()
                    .map(|row| project_row(&row, schema, &targets, current_database, current_user))
                    .collect::<SqlResult<_>>()?;
                if offset.unwrap_or(0) > 0 || limit.is_some() {
                    // Heap already holds only `offset + limit` rows in order;
                    // skip the offset prefix that `sort_rows` callers used to
                    // drop after the full sort.
                    rows = rows.into_iter().skip(offset.unwrap_or(0)).collect();
                }
                let cols = select_columns(schema, &targets)?;
                let column_types = select_column_types(schema, &targets)?;
                return Ok(QueryResult::Rows {
                    columns: cols,
                    column_types,
                    rows,
                });
            }
            let mut filtered_rows: Vec<Vec<Value>> = Vec::new();
            if let Some(cap) = row_cap {
                filtered_rows.reserve(cap.min(1024));
            }
            'scan: for row in rows {
                let row = columnar_row_to_values(row, schema, &columnar_columns)?;
                if let Some(ref expr) = where_expr {
                    if !evaluate_predicate(&row, schema, expr)? {
                        continue;
                    }
                }
                filtered_rows.push(row);
                if row_cap.is_some_and(|cap| filtered_rows.len() >= cap) {
                    break 'scan;
                }
            }
            if has_aggregation {
                return execute_grouped_select(
                    schema,
                    &targets,
                    group_by,
                    having,
                    filtered_rows,
                    current_database,
                    current_user,
                );
            }
            let mut source_rows = filtered_rows;
            if !order_by.is_empty() {
                let top_k = limit.map(|take| offset.unwrap_or(0).saturating_add(take));
                // The columnar site stays sequential;
                // only the row-store sites parallelize (same gate would apply).
                source_rows = match crate::vector_ann::try_ann_order(
                    engine,
                    catalog,
                    current_database,
                    &from,
                    schema,
                    &source_rows,
                    &order_by,
                    top_k,
                    where_expr.as_ref().map(|e| format!("{e:?}")),
                    current_user,
                    None,
                ) {
                    Some(ann) => ann?,
                    None => sort_rows(source_rows, schema, &order_by, top_k)?,
                };
            }
            if !distinct_on.is_empty() {
                if group_by.is_some()
                    || having.is_some()
                    || targets.iter().any(|t| match t {
                        SelectTarget::Function(name) => is_aggregate_function(name),
                        SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
                        SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
                        SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
                        _ => false,
                    })
                {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Unsupported,
                        "DISTINCT ON is not supported with GROUP BY or aggregates",
                    )));
                }
                source_rows = apply_distinct_on(
                    source_rows,
                    schema,
                    &distinct_on,
                    &order_by,
                    &targets,
                    current_database,
                    current_user,
                )?;
            }
            let mut rows: Vec<Vec<Value>> = source_rows
                .into_iter()
                .map(|row| project_row(&row, schema, &targets, current_database, current_user))
                .collect::<SqlResult<_>>()?;
            if offset.unwrap_or(0) > 0 || limit.is_some() {
                rows = rows
                    .into_iter()
                    .skip(offset.unwrap_or(0))
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();
            }
            let cols = select_columns(schema, &targets)?;
            let column_types = select_column_types(schema, &targets)?;
            return Ok(QueryResult::Rows {
                columns: cols,
                column_types,
                rows,
            });
        }

        // Byte-identical to the historical format! bounds without intermediate
        // Strings: one allocation per bound, built once per scan (not per row).
        let mut scan_start: Vec<u8> = Vec::with_capacity(from.len() + 1);
        scan_start.extend_from_slice(from.as_bytes());
        scan_start.push(b':');
        let mut scan_end: Vec<u8> = Vec::with_capacity(from.len() + 5);
        scan_end.extend_from_slice(from.as_bytes());
        scan_end.push(b':');
        scan_end.extend_from_slice("\u{10FFFF}".as_bytes());

        // Per-chunk decode + WHERE helper. Errors are funneled through the
        // enclosing `stream_err` slot because the storage callback returns
        // the storage error type, not `SqlError`.
        macro_rules! decode_filter {
            ($bytes:expr, $stream_err:ident) => {{
                let decoded = decode_row_selected($bytes, projection.selected());
                let row = match decoded {
                    Ok(row) => row,
                    Err(e) => {
                        $stream_err = Some(SqlError::Storage(e));
                        return Ok(false);
                    }
                };
                if let Some(ref expr) = where_expr {
                    match evaluate_predicate(&row, schema, expr) {
                        Ok(true) => Some(row),
                        Ok(false) => None,
                        Err(e) => {
                            $stream_err = Some(e);
                            return Ok(false);
                        }
                    }
                } else {
                    Some(row)
                }
            }};
        }

        // COUNT(DISTINCT expr): streaming distinct-value set, no row storage.
        if let Some((arg, filter)) = count_distinct {
            let mut values = std::collections::HashSet::new();
            let mut stream_err: Option<SqlError> = None;
            let scan_res = engine.scan_for_each(
                Some(&scan_start),
                Some(&scan_end),
                ANALYTICAL_SCAN_CHUNK_ROWS,
                &mut |chunk| {
                    for (_, bytes) in chunk {
                        let row = match decode_filter!(bytes, stream_err) {
                            Some(row) => row,
                            None => continue,
                        };
                        if let Some(filter) = filter.as_ref() {
                            match evaluate_expression(&row, schema, filter) {
                                Ok(Value::Bool(true)) => {}
                                Ok(_) => continue,
                                Err(e) => {
                                    stream_err = Some(e);
                                    return Ok(false);
                                }
                            }
                        }
                        match evaluate_expression(&row, schema, &arg) {
                            Ok(value) => {
                                if !value.is_null() {
                                    values.insert(value.to_sql_text());
                                }
                            }
                            Err(e) => {
                                stream_err = Some(e);
                                return Ok(false);
                            }
                        }
                    }
                    Ok(true)
                },
            );
            scan_res.map_err(SqlError::Storage)?;
            if let Some(e) = stream_err {
                return Err(e);
            }
            return Ok(QueryResult::Rows {
                columns: vec!["count".into()],
                column_types: vec![Some(ColumnType::bigint())],
                rows: vec![vec![Value::Int8(values.len() as i64)]],
            });
        }
        // COUNT(*) FILTER (WHERE ...): streaming counter, no row storage.
        if let Some(filter) = count_filter {
            tracing::debug!(
                target: "sql::query",
                event = "analytical_path",
                path = "row_count_filter",
                table = %from,
            );
            let mut count = 0i64;
            let mut stream_err: Option<SqlError> = None;
            let scan_res = engine.scan_for_each(
                Some(&scan_start),
                Some(&scan_end),
                ANALYTICAL_SCAN_CHUNK_ROWS,
                &mut |chunk| {
                    for (_, bytes) in chunk {
                        let row = match decode_filter!(bytes, stream_err) {
                            Some(row) => row,
                            None => continue,
                        };
                        match evaluate_expression(&row, schema, &filter) {
                            Ok(Value::Bool(true)) => count += 1,
                            Ok(_) => {}
                            Err(e) => {
                                stream_err = Some(e);
                                return Ok(false);
                            }
                        }
                    }
                    Ok(true)
                },
            );
            scan_res.map_err(SqlError::Storage)?;
            if let Some(e) = stream_err {
                return Err(e);
            }
            return Ok(QueryResult::Rows {
                columns: vec!["count".into()],
                column_types: vec![Some(ColumnType::bigint())],
                rows: vec![vec![Value::Int8(count)]],
            });
        }
        // Streaming GROUP BY / aggregation: bounded to groups + one chunk.
        if stream_agg {
            tracing::debug!(
                target: "sql::query",
                event = "analytical_path",
                path = "row_group",
                table = %from,
            );
            let slots = collect_aggregate_slots(&targets, having.as_ref());
            let group_exprs: Vec<Expression> = group_by
                .as_ref()
                .map(|g| g.to_sets().into_iter().next().unwrap_or_default())
                .unwrap_or_default();
            // Parallel scan-aggregation: the same single-snapshot full-range
            // scan with decode/filter/feed fanned out to workers (see
            // `par_streaming_agg`). Only order-free, integer-typed,
            // context-free shapes qualify; everything else falls through to
            // the sequential loop below, byte-identical. The worker count is
            // clamped to the effective tier cap, and the dispatch claims
            // permits from the box-wide budget: on exhaustion the sequential
            // loop below runs instead (the flagged worker-budget TODO, now
            // wired; TopK/full-sort call sites stay with the sibling).
            let workers = crate::parallel_budget::available_workers();
            let mut par_estimate: Option<u64> = None;
            if workers >= 2
                && par_agg_eligible(&slots, &group_exprs, schema, u64::MAX)
                && !agg_call_has_order_by(&targets, having.as_ref())
                && !targets_need_statement_ctx(&targets, having.as_ref())
                && where_expr
                    .as_ref()
                    .map(|expr| !expr_needs_statement_ctx(expr))
                    .unwrap_or(true)
            {
                // Keys-only estimate: advisory threshold only, under its own
                // snapshot. A count failure falls through to sequential, which
                // surfaces the real error from its own scan.
                if let Ok(n) = engine.count_range(Some(&scan_start), Some(&scan_end)) {
                    par_estimate = Some(n);
                }
            }
            if let Some(estimated_rows) = par_estimate {
                if par_agg_eligible(&slots, &group_exprs, schema, estimated_rows) {
                    // Shared-snapshot materializing scan with index-partitioned
                    // decode/feed when the exact range fits the memory cap;
                    // otherwise the streaming channel fan-out (bounded
                    // chunks). A declined budget claim falls through to the
                    // sequential loop below, byte-identical; worker *errors*
                    // (`?`) still surface instead of re-materializing.
                    if let Some(dispatch) = crate::parallel_budget::claim_dispatch_workers(workers)
                    {
                        let workers = dispatch.workers();
                        if let Some(result) = par_scan_agg(
                            engine,
                            schema,
                            &scan_start,
                            &scan_end,
                            &projection,
                            where_expr.as_ref(),
                            &slots,
                            &group_exprs,
                            &targets,
                            having.as_ref(),
                            current_database,
                            current_user,
                            workers,
                        )? {
                            tracing::debug!(
                                target: "sql::query",
                                event = "analytical_path",
                                path = "par_scan_agg",
                                workers,
                                estimated_rows,
                            );
                            return Ok(result);
                        }
                        tracing::debug!(
                            target: "sql::query",
                            event = "analytical_path",
                            path = "par_streaming_agg",
                            workers,
                            estimated_rows,
                        );
                        return par_streaming_agg(
                            engine,
                            schema,
                            &scan_start,
                            &scan_end,
                            &projection,
                            where_expr.as_ref(),
                            &slots,
                            &group_exprs,
                            &targets,
                            having.as_ref(),
                            current_database,
                            current_user,
                            estimated_rows,
                            workers,
                        );
                    }
                }
            }
            let mut agg = StreamingGroupedAgg::new(&slots, &group_exprs);
            let mut stream_err: Option<SqlError> = None;
            let scan_res = engine.scan_for_each(
                Some(&scan_start),
                Some(&scan_end),
                ANALYTICAL_SCAN_CHUNK_ROWS,
                &mut |chunk| {
                    for (_, bytes) in chunk {
                        let row = match decode_filter!(bytes, stream_err) {
                            Some(row) => row,
                            None => continue,
                        };
                        if let Err(e) = agg.feed(&row, schema) {
                            stream_err = Some(e);
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
            );
            scan_res.map_err(SqlError::Storage)?;
            if let Some(e) = stream_err {
                return Err(e);
            }
            return agg.finish(
                schema,
                &targets,
                having.as_ref(),
                current_database,
                current_user,
            );
        }
        // Streaming ORDER BY ... LIMIT via bounded heap: O(K) memory.
        // ANN-eligible orderings skip streaming (see the columnar site):
        // the materialized path below serves them from the cached graph.
        if let Some(cap) = heap_cap.filter(|_| {
            !crate::vector_ann::ann_eligible(
                engine,
                catalog,
                &from,
                schema,
                &order_by,
                heap_cap,
                where_expr.is_some(),
            )
        }) {
            // Parallel TopK: decode/filter/key-evaluation fan out to workers
            // (see `par_topk_streaming`), each holding a thread-local heap,
            // merged deterministically after the join. Engages only when ALL
            // hold, else the sequential heap below runs byte-identically:
            //
            // * total order (`par_sort_order_is_total`) — the merge is exact;
            // * worker-safe keys AND worker-safe WHERE
            //   (`par_sort_keys_parallel_safe`, `par_sort_expr_parallel_safe`)
            //   — workers do not inherit statement context;
            // * costly keys (`par_sort_has_costly_keys`) — cheap colref keys
            //   cannot amortize the fan-out (measured ~2x slower than
            //   sequential at 100k rows); only real per-row evaluation wins;
            // * scale (at least [`PAR_TOPK_MIN_ROWS`] estimated rows via a
            //   keys-only `count_range`, advisory: filtered matches are
            //   fewer — the fan-out loses below 1M at measured scales) AND
            //   a budget claim — on exhaustion the sequential heap below runs.
            //
            // Worker *errors* (`?`) surface instead of re-running sequential;
            // result projection stays on the caller thread either way.
            let topk_parallel = engine
                .count_range(Some(&scan_start), Some(&scan_end))
                .ok()
                .and_then(|estimated| {
                    par_topk_workers_for(schema, &order_by, where_expr.as_ref(), estimated)
                })
                .and_then(crate::parallel_budget::claim_dispatch_workers);
            if let Some(dispatch) = topk_parallel {
                let workers = dispatch.workers();
                tracing::debug!(
                    target: "sql::query",
                    event = "analytical_path",
                    path = "par_topk",
                    workers,
                    cap,
                );
                return par_topk_streaming(
                    engine,
                    schema,
                    &scan_start,
                    &scan_end,
                    &projection,
                    where_expr.as_ref(),
                    &order_by,
                    cap,
                    offset.unwrap_or(0),
                    &targets,
                    current_database,
                    current_user,
                    workers,
                );
            }
            let mut heap = TopKHeap::new(cap);
            let mut stream_err: Option<SqlError> = None;
            let scan_res = engine.scan_for_each(
                Some(&scan_start),
                Some(&scan_end),
                ANALYTICAL_SCAN_CHUNK_ROWS,
                &mut |chunk| {
                    for (_, bytes) in chunk {
                        let row = match decode_filter!(bytes, stream_err) {
                            Some(row) => row,
                            None => continue,
                        };
                        // No early break: ORDER BY must observe every match;
                        // the bound is the heap itself, not the walk length.
                        let keys = match order_by
                            .iter()
                            .map(|item| evaluate_expression(&row, schema, &item.expr))
                            .collect::<SqlResult<Vec<_>>>()
                        {
                            Ok(keys) => keys,
                            Err(e) => {
                                stream_err = Some(e);
                                return Ok(false);
                            }
                        };
                        heap.offer(row, keys, &order_by);
                    }
                    Ok(true)
                },
            );
            scan_res.map_err(SqlError::Storage)?;
            if let Some(e) = stream_err {
                return Err(e);
            }
            let mut rows: Vec<Vec<Value>> = heap
                .into_sorted_rows(&order_by)
                .into_iter()
                .map(|row| project_row(&row, schema, &targets, current_database, current_user))
                .collect::<SqlResult<_>>()?;
            if offset.unwrap_or(0) > 0 {
                rows = rows.into_iter().skip(offset.unwrap_or(0)).collect();
            }
            let cols = select_columns(schema, &targets)?;
            let column_types = select_column_types(schema, &targets)?;
            return Ok(QueryResult::Rows {
                columns: cols,
                column_types,
                rows,
            });
        }

        // Remaining shapes (plain SELECT, ORDER BY without LIMIT, DISTINCT ON):
        // the result itself is O(matches), so one result Vec is unavoidable,
        // but the storage scan streams in bounded chunks instead of arriving
        // as a second full-table Vec.
        let mut source_rows: Vec<Vec<Value>> = Vec::new();
        if let Some(cap) = row_cap {
            source_rows.reserve(cap.min(1024));
        }
        {
            let mut stream_err: Option<SqlError> = None;
            let mut matched = 0usize;
            let stop_at = row_cap;
            let scan_res = engine.scan_for_each(
                Some(&scan_start),
                Some(&scan_end),
                ANALYTICAL_SCAN_CHUNK_ROWS,
                &mut |chunk| {
                    for (_, bytes) in chunk {
                        let row = match decode_filter!(bytes, stream_err) {
                            Some(row) => row,
                            None => continue,
                        };
                        source_rows.push(row);
                        matched += 1;
                        if stop_at.is_some_and(|c| matched >= c) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                },
            );
            scan_res.map_err(SqlError::Storage)?;
            if let Some(e) = stream_err {
                return Err(e);
            }
        }
        if has_aggregation {
            return execute_grouped_select(
                schema,
                &targets,
                group_by,
                having,
                source_rows,
                current_database,
                current_user,
            );
        }
        if !order_by.is_empty() {
            let top_k = limit.map(|take| offset.unwrap_or(0).saturating_add(take));
            // ANN before any sort: eligible vector orderings resolve from the
            // cached graph (exact rerank); everything else keeps its path.
            if let Some(ann) = crate::vector_ann::try_ann_order(
                engine,
                catalog,
                current_database,
                &from,
                schema,
                &source_rows,
                &order_by,
                top_k,
                where_expr.as_ref().map(|e| format!("{e:?}")),
                current_user,
                None,
            ) {
                source_rows = ann?;
            } else if par_sort_order_is_total(schema, &order_by)
                && par_sort_keys_parallel_safe(&order_by)
            {
                // Parallel full sort when the ORDER BY is a proven total order
                // over worker-safe keys; anything else keeps the sequential
                // `sort_rows` byte-identically (see below). DISTINCT ON runs
                // after this sort exactly as before, so its
                // first-row-per-group survivors cannot change. The fan-out
                // claims permits from the box-wide budget: on exhaustion the
                // sequential spill path below runs byte-identically (worker
                // *errors* still surface via `?`).
                source_rows = match par_sort_workers_for(source_rows.len() as u64) {
                    Some(want) => match crate::parallel_budget::claim_dispatch_workers(want) {
                        Some(dispatch) => {
                            let workers = dispatch.workers();
                            tracing::debug!(
                                target: "sql::query",
                                event = "analytical_path",
                                path = "par_sort",
                                workers,
                            );
                            par_sort_rows(source_rows, schema, &order_by, top_k, workers)?
                        }
                        None => sort_rows_spilled(
                            source_rows,
                            schema,
                            &order_by,
                            top_k,
                            SORT_SPILL_MAX_ROWS,
                        )?,
                    },
                    None => sort_rows_spilled(
                        source_rows,
                        schema,
                        &order_by,
                        top_k,
                        SORT_SPILL_MAX_ROWS,
                    )?,
                };
            } else {
                source_rows =
                    sort_rows_spilled(source_rows, schema, &order_by, top_k, SORT_SPILL_MAX_ROWS)?;
            }
        }
        if !distinct_on.is_empty() {
            if group_by.is_some()
                || having.is_some()
                || targets.iter().any(|t| match t {
                    SelectTarget::Function(name) => is_aggregate_function(name),
                    SelectTarget::FunctionCall { name, .. } => is_aggregate_function(name),
                    SelectTarget::Expr { expr, .. } => expression_has_aggregate(expr),
                    SelectTarget::Aliased { target, .. } => matches!(target.as_ref(), SelectTarget::Expr { expr, .. } if expression_has_aggregate(expr)),
                    _ => false,
                })
            {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Unsupported,
                    "DISTINCT ON is not supported with GROUP BY or aggregates",
                )));
            }
            source_rows = apply_distinct_on(
                source_rows,
                schema,
                &distinct_on,
                &order_by,
                &targets,
                current_database,
                current_user,
            )?;
        }
        let mut rows: Vec<Vec<Value>> = source_rows
            .into_iter()
            .map(|row| project_row(&row, schema, &targets, current_database, current_user))
            .collect::<SqlResult<_>>()?;
        if offset.unwrap_or(0) > 0 || limit.is_some() {
            rows = rows
                .into_iter()
                .skip(offset.unwrap_or(0))
                .take(limit.unwrap_or(usize::MAX))
                .collect();
        }

        let cols = select_columns(schema, &targets)?;
        let column_types = select_column_types(schema, &targets)?;
        tracing::trace!(
            target: "sql::query",
            "select complete from={} row_count={}",
            from,
            rows.len()
        );
        Ok(QueryResult::Rows {
            columns: cols,
            column_types,
            rows,
        })
    } else {
        let row = project_bare_select(engine, catalog, &targets, current_database, current_user)?;
        let cols = select_bare_columns(&targets)?;
        let column_types = select_bare_column_types(&targets)?;
        tracing::trace!(target: "sql::query", "bare select complete row_count=1");
        Ok(QueryResult::Rows {
            columns: cols,
            column_types,
            rows: vec![row],
        })
    }
}

/// The column and literal of a comparison with one bare column on one side.
fn column_vs_literal(left: &Expression, right: &Expression) -> Option<(String, Value)> {
    match (left, right) {
        (Expression::ColumnRef(column), Expression::Literal(value)) => {
            Some((unqualify(column).to_string(), value.clone()))
        }
        (Expression::Literal(value), Expression::ColumnRef(column)) => {
            Some((unqualify(column).to_string(), value.clone()))
        }
        _ => None,
    }
}

/// The one-column key range a predicate implies: the compared column plus, for
/// each direction, the bound operator and its literal.
///
/// Only shapes a single index can serve are accepted. `OR` is deliberately
/// rejected — the disjunction of two ranges is not one range — and so is any
/// comparison whose column is not compared against a literal. `AND` keeps the
/// bounds of whichever conjunct is usable, and merges two conjuncts on the same
/// column, which is what makes `k >= a AND k <= b` a single bounded probe.
#[allow(clippy::type_complexity)]
fn column_range_predicate(
    expr: &Expression,
) -> Option<(
    String,
    Option<(crate::index::IndexRangeOp, Value)>,
    Option<(crate::index::IndexRangeOp, Value)>,
)> {
    use crate::index::IndexRangeOp;
    use Expression as E;
    match expr {
        E::Equal(left, right) => column_vs_literal(left, right).map(|(column, value)| {
            (
                column,
                Some((IndexRangeOp::GreaterOrEqual, value.clone())),
                Some((IndexRangeOp::LessOrEqual, value)),
            )
        }),
        E::Less(left, right) => match (
            column_vs_literal(left, right),
            column_vs_literal(right, left),
        ) {
            (Some((column, value)), _) => Some((column, None, Some((IndexRangeOp::Less, value)))),
            (None, Some((column, value))) => {
                Some((column, Some((IndexRangeOp::Greater, value)), None))
            }
            _ => None,
        },
        E::LessOrEqual(left, right) => {
            match (
                column_vs_literal(left, right),
                column_vs_literal(right, left),
            ) {
                (Some((column, value)), _) => {
                    Some((column, None, Some((IndexRangeOp::LessOrEqual, value))))
                }
                (None, Some((column, value))) => {
                    Some((column, Some((IndexRangeOp::GreaterOrEqual, value)), None))
                }
                _ => None,
            }
        }
        E::Greater(left, right) => match (
            column_vs_literal(left, right),
            column_vs_literal(right, left),
        ) {
            (Some((column, value)), _) => {
                Some((column, Some((IndexRangeOp::Greater, value)), None))
            }
            (None, Some((column, value))) => {
                Some((column, None, Some((IndexRangeOp::Less, value))))
            }
            _ => None,
        },
        E::GreaterOrEqual(left, right) => {
            match (
                column_vs_literal(left, right),
                column_vs_literal(right, left),
            ) {
                (Some((column, value)), _) => {
                    Some((column, Some((IndexRangeOp::GreaterOrEqual, value)), None))
                }
                (None, Some((column, value))) => {
                    Some((column, None, Some((IndexRangeOp::LessOrEqual, value))))
                }
                _ => None,
            }
        }
        E::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            let Expression::ColumnRef(column) = &**expr else {
                return None;
            };
            let (Expression::Literal(low), Expression::Literal(high)) = (&**low, &**high) else {
                return None;
            };
            Some((
                unqualify(column).to_string(),
                Some((IndexRangeOp::GreaterOrEqual, low.clone())),
                Some((IndexRangeOp::LessOrEqual, high.clone())),
            ))
        }
        E::And(left, right) => {
            match (column_range_predicate(left), column_range_predicate(right)) {
                (
                    Some((left_column, left_low, left_high)),
                    Some((right_column, right_low, right_high)),
                ) if left_column == right_column => Some((
                    left_column,
                    left_low.or(right_low),
                    left_high.or(right_high),
                )),
                (Some(found), _) => Some(found),
                (None, other) => other,
            }
        }
        _ => None,
    }
}

/// The index and byte bounds `[start, end)` that answer `where_expr` for
/// `table`, or `None` when no single-column index can serve it.
///
/// Only a plain single-column index is eligible: a composite index's leading
/// column is not itself the indexed value, and an expression index has no
/// column to compare. The literal must also be one whose canonical encoding is
/// order-preserving, which is why the integer family is served here and text
/// falls back to the scan (see `crate::index::index_range_bounds`).
pub fn index_bounds_for_predicate(
    catalog: &InMemoryCatalog,
    table: &str,
    where_expr: Option<&Expression>,
) -> Option<(String, Vec<u8>, Vec<u8>)> {
    let (column, lower, upper) = column_range_predicate(where_expr?)?;
    let index = catalog
        .all_indexes_for_table(table)
        .into_iter()
        .find(|index| {
            index.expression.is_none() && crate::index::index_columns(index) == [column.clone()]
        })?;
    let namespace = index_value_prefix(&index.name, None);
    let mut start = namespace.clone();
    let mut end = prefix_end(&namespace);
    // Text equality answers through the exact-match prefix before the
    // integer path runs: `col = 'lit'` on a text-family column names exactly
    // the entries carrying `lit` spelled in the column's own representation
    // (see `text_equality_bounds` for the proof obligations). Anything
    // outside that shape falls through unchanged.
    if let (
        Some((crate::index::IndexRangeOp::GreaterOrEqual, low)),
        Some((crate::index::IndexRangeOp::LessOrEqual, high)),
    ) = (&lower, &upper)
    {
        if let Some((exact_start, exact_end)) =
            crate::index::text_equality_bounds(catalog, table, &column, &index.name, low, high)
        {
            return Some((index.name.clone(), exact_start, exact_end));
        }
    }
    if let Some((op, literal)) = lower {
        start = crate::index::index_range_bounds_coerced(&index.name, op, &literal)?.0;
    }
    if let Some((op, literal)) = upper {
        end = crate::index::index_range_bounds_coerced(&index.name, op, &literal)?.1;
    }
    // An unsatisfiable conjunction (`k > 10 AND k < 5`) yields an empty range;
    // returning it is correct and lets the scan short-circuit to no rows.
    (start <= end).then_some((index.name, start, end))
}

fn count_filter_target(targets: &[SelectTarget]) -> Option<Expression> {
    if targets.len() != 1 {
        return None;
    }
    let expr = match targets.first()? {
        SelectTarget::Expr { expr, .. } => expr,
        _ => return None,
    };
    match expr {
        Expression::FunctionCall {
            name,
            args,
            filter: Some(filter),
            ..
        } if name.eq_ignore_ascii_case("count")
            && matches!(args.first(), Some(Expression::Star)) =>
        {
            Some((**filter).clone())
        }
        _ => None,
    }
}

fn count_distinct_target(targets: &[SelectTarget]) -> Option<(Expression, Option<Expression>)> {
    let target = targets.first()?;
    // COUNT(DISTINCT expr) is parsed as:
    //   SelectTarget::Expr { expr: Expression::FunctionCall { distinct: true, ... }, ... }
    // We extract the inner Expression::FunctionCall and check for the COUNT + DISTINCT pattern.
    let expr = match target {
        SelectTarget::Expr { expr, .. } => expr,
        SelectTarget::Aliased { target, .. } => match target.as_ref() {
            SelectTarget::Expr { expr, .. } => expr,
            _ => return None,
        },
        _ => return None,
    };
    match expr {
        Expression::FunctionCall {
            name,
            args,
            distinct: true,
            filter,
            order_by: _,
            returning: _,
            null_handling: _,
            unique_keys: _,
        } if name.eq_ignore_ascii_case("count")
            && args.len() == 1
            && !matches!(args[0], Expression::Star) =>
        {
            Some((args[0].clone(), filter.as_deref().cloned()))
        }
        _ => None,
    }
}

pub fn expression_has_aggregate(expr: &Expression) -> bool {
    match expr {
        // Only the outermost call is checked here historically; nested
        // aggregates inside scalar arguments are detected by recursing so
        // `jsonb_path_query_array(jsonb_agg(x), ...)` is routed to grouped
        // execution instead of the scalar "cannot be evaluated here" path.
        Expression::FunctionCall { name, args, .. } => {
            is_aggregate_function(name) || args.iter().any(expression_has_aggregate)
        }
        Expression::Equal(a, b)
        | Expression::NotEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::And(a, b)
        | Expression::Or(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b)
        | Expression::Modulo(a, b)
        | Expression::Concat(a, b)
        | Expression::Power(a, b) => expression_has_aggregate(a) || expression_has_aggregate(b),
        Expression::IsNull(a)
        | Expression::IsNotNull(a)
        | Expression::Not(a)
        | Expression::Negate(a)
        | Expression::Cast { expr: a, .. }
        | Expression::TypeCast { expr: a, .. }
        | Expression::Extract { expr: a, .. } => expression_has_aggregate(a),
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand
                .as_ref()
                .is_some_and(|a| expression_has_aggregate(a))
                || whens
                    .iter()
                    .any(|(a, b)| expression_has_aggregate(a) || expression_has_aggregate(b))
                || default
                    .as_ref()
                    .is_some_and(|a| expression_has_aggregate(a))
        }
        Expression::Coalesce(v) => v.iter().any(expression_has_aggregate),
        Expression::NullIf(a, b)
        | Expression::IsDistinctFrom(a, b)
        | Expression::JsonArrow {
            left: a, right: b, ..
        }
        | Expression::ArrayIndex { array: a, index: b } => {
            expression_has_aggregate(a) || expression_has_aggregate(b)
        }
        Expression::In { expr, list, .. } => {
            expression_has_aggregate(expr) || list.iter().any(expression_has_aggregate)
        }
        Expression::Between {
            expr, low, high, ..
        } => {
            expression_has_aggregate(expr)
                || expression_has_aggregate(low)
                || expression_has_aggregate(high)
        }
        Expression::Like { expr, pattern, .. } => {
            expression_has_aggregate(expr) || expression_has_aggregate(pattern)
        }
        _ => false,
    }
}

/// True when a select target contains a JSON aggregate call. Queries with
/// JSON aggregates run through the general engine, whose accumulator keeps
/// the full value list (the single-table fast path only tracks numeric state).
fn target_has_json_aggregate(target: &SelectTarget) -> bool {
    match target {
        SelectTarget::Expr { expr, .. } => expr_has_json_aggregate(expr),
        SelectTarget::Aliased { target, .. } => target_has_json_aggregate(target),
        _ => false,
    }
}

fn expr_has_json_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::FunctionCall { name, args, .. } => {
            crate::json::is_json_aggregate(name) || args.iter().any(expr_has_json_aggregate)
        }
        Expression::Equal(a, b)
        | Expression::NotEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::And(a, b)
        | Expression::Or(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b)
        | Expression::Modulo(a, b)
        | Expression::Concat(a, b)
        | Expression::Power(a, b)
        | Expression::NullIf(a, b)
        | Expression::IsDistinctFrom(a, b)
        | Expression::JsonArrow {
            left: a, right: b, ..
        }
        | Expression::ArrayIndex { array: a, index: b } => {
            expr_has_json_aggregate(a) || expr_has_json_aggregate(b)
        }
        Expression::IsNull(a)
        | Expression::IsNotNull(a)
        | Expression::Not(a)
        | Expression::Negate(a)
        | Expression::Cast { expr: a, .. }
        | Expression::TypeCast { expr: a, .. }
        | Expression::Extract { expr: a, .. } => expr_has_json_aggregate(a),
        Expression::In { expr, list, .. } => {
            expr_has_json_aggregate(expr) || list.iter().any(expr_has_json_aggregate)
        }
        Expression::Between {
            expr, low, high, ..
        } => {
            expr_has_json_aggregate(expr)
                || expr_has_json_aggregate(low)
                || expr_has_json_aggregate(high)
        }
        Expression::Like { expr, pattern, .. } => {
            expr_has_json_aggregate(expr) || expr_has_json_aggregate(pattern)
        }
        Expression::Coalesce(v) => v.iter().any(expr_has_json_aggregate),
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand.as_ref().is_some_and(|a| expr_has_json_aggregate(a))
                || whens
                    .iter()
                    .any(|(a, b)| expr_has_json_aggregate(a) || expr_has_json_aggregate(b))
                || default.as_ref().is_some_and(|a| expr_has_json_aggregate(a))
        }
        _ => false,
    }
}
