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
//! Grouped execution: `GROUP BY`, `ORDER BY`, `DISTINCT` and projection.
//!
//! The grouped path materialises one synthetic row per group and folds the
//! grouped targets over it, which keeps grouped projection separate from the
//! plain row projection.

use super::aggregate::{
    collect_aggregate_slots, evaluate_grouped_expression, lookup_aggregate, AggregateAccumulator,
    AggregateSlot,
};
use super::expr::evaluate_expression;
use super::project::{select_column_types, select_columns};
use super::scan::value_cmp;
use crate::catalog_fn::function_value;
use crate::catalog_fn::is_aggregate_function;
use crate::catalog_fn::is_session_function;
use crate::encoding::{decode_row, encode_row};
use crate::error::{SqlError, SqlResult};
use crate::row::{equality_signature, values_equal};
use crate::util::unqualify;
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::Expression;
use plomid_sql::QueryResult;
use plomid_sql::SelectTarget;
use plomid_sql::TableSchema;
use plomid_sql::Value;
use std::collections::{hash_map::Entry, HashMap};

#[allow(clippy::too_many_arguments)]
pub fn execute_grouped_select(
    schema: &TableSchema,
    targets: &[SelectTarget],
    group_by: Option<plomid_sql::GroupByClause>,
    having: Option<Expression>,
    rows: Vec<Vec<Value>>,
    current_database: &str,
    current_user: &str,
) -> SqlResult<QueryResult> {
    let slots = collect_aggregate_slots(targets, having.as_ref());
    let grouping_sets: Vec<Vec<Expression>> = group_by
        .as_ref()
        .map(|g| g.to_sets())
        .unwrap_or_else(|| vec![Vec::new()]);
    // Fast path: single grouping set keeps the existing behavior exactly.
    if grouping_sets.len() == 1 {
        let group_exprs: &[Expression] = &grouping_sets[0];
        return grouped_agg_spilled(
            schema,
            targets,
            group_exprs,
            having.as_ref(),
            rows,
            &slots,
            current_database,
            current_user,
            GROUP_SPILL_MAX_ROWS,
        );
    }
    let mut group_rows: Vec<Vec<Value>> = Vec::new();
    for group_exprs in &grouping_sets {
        let mut rows_for_set = grouped_agg_spilled(
            schema,
            targets,
            group_exprs,
            having.as_ref(),
            rows.clone(),
            &slots,
            current_database,
            current_user,
            GROUP_SPILL_MAX_ROWS,
        )?;
        if let QueryResult::Rows { rows, .. } = &mut rows_for_set {
            group_rows.append(rows);
        }
    }
    let cols = select_columns(schema, targets)?;
    let column_types = select_column_types(schema, targets)?;
    Ok(QueryResult::Rows {
        columns: cols,
        column_types,
        rows: group_rows,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn execute_single_grouped_select(
    schema: &TableSchema,
    targets: &[SelectTarget],
    group_exprs: &[Expression],
    having: Option<&Expression>,
    rows: Vec<Vec<Value>>,
    slots: &[AggregateSlot],
    current_database: &str,
    current_user: &str,
) -> SqlResult<QueryResult> {
    // Incremental path shared with the streaming scan: feed rows one at a
    // time so GROUP BY memory scales with group count, not input size.
    let mut streaming = StreamingGroupedAgg::new(slots, group_exprs);
    for row in &rows {
        streaming.feed(row, schema)?;
    }
    streaming.finish(schema, targets, having, current_database, current_user)
}

/// Grouped aggregation with a bounded-temp spillover.
///
/// At or under `budget_rows` this is exactly [`execute_single_grouped_select`]
/// (same call). Above it, input rows are partitioned by hash of the encoded
/// group-key bytes and spilled as opaque storage-codec records through one
/// [`plomid_spill::SpillManager`] — so temp bytes stay under the manager limit
/// and temp files vanish on `Drop` — then each partition is re-aggregated
/// through a fresh [`StreamingGroupedAgg`] (the existing feed/finish) and the
/// per-partition results concatenated.
///
/// `Value` carries floats and has no `Ord`/`Hash`, so the generic spill
/// operators (`SpillGroupedAgg`, `SpillDistinct`, `ExternalSort`) cannot take
/// these rows without new trait impls; this pipe uses only the untyped
/// `SpillManager`/`SpillRun` byte layer plus the existing row codec. No
/// aggregate state is serialized: partitions hold pre-aggregation input rows,
/// and each partition replays the existing in-memory fold, which keeps float
/// `SUM`/`AVG` bit-identical (no cross-partition merge ever runs).
///
/// Exactness: the partition function is a pure function of the evaluated group
/// keys, so every group lands wholly in one partition; within a partition rows
/// keep input order, so each group's accumulator observes the identical row
/// subsequence as the single-pass fold. Result row order without `ORDER BY` is
/// unspecified either way (both iterate hash maps), so callers compare
/// canonically. Key-evaluation errors surface for the same first-in-row-order
/// row: keys are evaluated front-to-back, and on the reachable fast path they
/// are infallible column reads; accumulator-step errors are pure functions of
/// the row, so per-partition replay in row order fails at the same row with
/// the identical error.
#[allow(clippy::too_many_arguments)]
pub fn grouped_agg_spilled(
    schema: &TableSchema,
    targets: &[SelectTarget],
    group_exprs: &[Expression],
    having: Option<&Expression>,
    rows: Vec<Vec<Value>>,
    slots: &[AggregateSlot],
    current_database: &str,
    current_user: &str,
    budget_rows: usize,
) -> SqlResult<QueryResult> {
    if rows.len() <= budget_rows {
        return execute_single_grouped_select(
            schema,
            targets,
            group_exprs,
            having,
            rows,
            slots,
            current_database,
            current_user,
        );
    }
    // Enough partitions that each one is expected to fit the budget; at least
    // two (we are over budget) and at most 64 (file-count hygiene — skew may
    // leave a partition above budget, which stays correct, just less tight).
    let partitions = (rows.len() / budget_rows.max(1)).clamp(2, 64);
    let seq = GROUP_SPILL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temp_dir =
        std::env::temp_dir().join(format!("plomid-group-spill-{}-{seq}", std::process::id()));
    let manager =
        plomid_spill::SpillManager::with_temp_dir(temp_dir.clone()).map_err(group_spill_err)?;
    // The temp dir is removed on every exit (success or structured error);
    // run files themselves vanish on `Drop` regardless of how we leave.
    let result = grouped_agg_spilled_inner(
        rows,
        schema,
        targets,
        group_exprs,
        having,
        slots,
        current_database,
        current_user,
        partitions,
        &manager,
    );
    let _ = std::fs::remove_dir(&temp_dir);
    result
}

/// Over-budget body of [`grouped_agg_spilled`]: hash-partition, spill,
/// re-aggregate per partition. Temp-dir removal stays with the caller so every
/// exit path cleans up.
#[allow(clippy::too_many_arguments)]
fn grouped_agg_spilled_inner(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    targets: &[SelectTarget],
    group_exprs: &[Expression],
    having: Option<&Expression>,
    slots: &[AggregateSlot],
    current_database: &str,
    current_user: &str,
    partitions: usize,
    manager: &plomid_spill::SpillManager,
) -> SqlResult<QueryResult> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let partitions = partitions.max(1);
    let mut runs = Vec::with_capacity(partitions);
    for _ in 0..partitions {
        runs.push(manager.create_run().map_err(group_spill_err)?);
    }
    let mut counts = vec![0usize; partitions];
    // Front-to-back key evaluation: the first key error surfaces for the same
    // row the single-pass feed would fail at. Encoded key bytes (`Vec<u8>`)
    // are hashable with no `Value` trait impls; a lossy encoding would only
    // co-locate distinct groups, never split one — the replay re-splits with
    // the exact feed keys.
    for row in &rows {
        let mut key_values = Vec::with_capacity(group_exprs.len());
        for expr in group_exprs {
            key_values.push(evaluate_expression(row, schema, expr)?);
        }
        let key_bytes = encode_row(&key_values)?;
        let mut hasher = DefaultHasher::new();
        key_bytes.hash(&mut hasher);
        let owner = (hasher.finish() as usize) % partitions;
        runs[owner]
            .write_record(&encode_row(row)?)
            .map_err(group_spill_err)?;
        counts[owner] += 1;
    }
    // The materialized input has served its purpose; release it before the
    // replay so peak memory is one partition plus its groups, not all rows.
    drop(rows);
    for run in runs.iter_mut() {
        run.flush().map_err(group_spill_err)?;
    }
    // One partition at a time through the existing fold, in partition order.
    // Empty partitions are skipped (never fed): for the global-aggregation
    // shape a fresh fold carries a pre-inserted empty group, so finishing one
    // would emit a spurious `COUNT = 0` row.
    let mut columns = None;
    let mut column_types = None;
    let mut out_rows = Vec::new();
    for (run, &count) in runs.iter_mut().zip(counts.iter()) {
        if count == 0 {
            continue;
        }
        let mut reader = run.open_reader().map_err(group_spill_err)?;
        let mut agg = StreamingGroupedAgg::new(slots, group_exprs);
        while let Some(record) = reader.next_record().map_err(group_spill_err)? {
            agg.feed(&decode_row(&record)?, schema)?;
        }
        drop(reader);
        let part = agg.finish(schema, targets, having, current_database, current_user)?;
        let QueryResult::Rows {
            columns: cols,
            column_types: types,
            rows: mut part_rows,
        } = part
        else {
            continue;
        };
        if columns.is_none() {
            columns = Some(cols);
            column_types = Some(types);
        }
        out_rows.append(&mut part_rows);
    }
    let Some(columns) = columns else {
        // No partition held rows (unreachable with non-empty input: every row
        // hashes somewhere): canonical empty result from finishing an empty
        // partial, exactly like the sequential fold.
        return StreamingGroupedAgg::new(slots, group_exprs).finish(
            schema,
            targets,
            having,
            current_database,
            current_user,
        );
    };
    let spilled = manager.bytes_spilled();
    drop(runs);
    tracing::debug!(
        target: "sql::query",
        event = "group_spill",
        rows = out_rows.len(),
        spill_bytes = spilled,
    );
    Ok(QueryResult::Rows {
        columns,
        column_types: column_types.expect("set with columns"),
        rows: out_rows,
    })
}

/// Maps a spill-layer failure to a structured execution error (no panic).
fn group_spill_err(error: plomid_spill::SpillError) -> SqlError {
    use plomid_spill::SpillError as E;
    let (kind, message) = match &error {
        E::Decode(detail) => (
            ErrorKind::RowEncoding,
            format!("group spill decode failed: {detail}"),
        ),
        other => (
            ErrorKind::Io,
            format!("group spill temp storage failed: {other}"),
        ),
    };
    SqlError::Storage(PlomidError::new(kind, message))
}

/// Process-wide sequence numbering group-spill temp dirs (one dir per
/// over-budget aggregation; run files inside never collide across spills).
static GROUP_SPILL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Incremental GROUP BY accumulator shared by the materialized and streaming
/// scan paths.
///
/// Semantics are exactly those of the former two-pass loop: one hash entry
/// per group holding the group's key values and one accumulator per aggregate
/// slot, fed row-at-a-time. The streaming SELECT path feeds decoded rows
/// directly from `scan_for_each` chunks, so peak memory is
/// `O(groups + chunk)` instead of `O(rows)`.
pub struct StreamingGroupedAgg {
    slots: Vec<AggregateSlot>,
    group_exprs: Vec<Expression>,
    grouped:
        HashMap<Vec<String>, (Vec<Value>, Vec<AggregateAccumulator>), crate::util::FastBuildHasher>,
    pub fast: KeyPlan,
    fast_groups:
        HashMap<FastKey, (Vec<Value>, Vec<AggregateAccumulator>), crate::util::FastBuildHasher>,
}

/// Group-key lookup plan: how `feed` resolves the grouping key.
///
/// `Legacy` evaluates expressions and Debug-formats every row (exact,
/// historical). `Fast` reads one resolved column position and keys by
/// `FastKey` (identical grouping — proven by unit test — minus the
/// per-row eval and formatting). Decided once per query on first feed;
/// empty inputs stay `Undecided` and behave exactly like legacy.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KeyPlan {
    Undecided,
    Legacy,
    Fast { idx: usize },
}

/// Internal grouping key. Partition-equivalent to the legacy
/// `format!("{:?}", value)` strings by construction: integer variants
/// keep their variant tag (so `Int8(1)` and `Int4(1)` stay apart,
/// exactly as today), `NULL` stands alone (no text spelling collides
/// with bare `Null`), and everything else carries the identical Debug
/// string. `debug_key` is the machine-checked proof.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FastKey {
    Null,
    I16(i16),
    I32(i32),
    I64(i64),
    Other(String),
}

impl FastKey {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Int2(v) => Self::I16(*v),
            Value::Int4(v) => Self::I32(*v),
            Value::Int8(v) => Self::I64(*v),
            other => Self::Other(format!("{other:?}")),
        }
    }

    /// Legacy grouping key this fast key stands for (used once per
    /// group at finish, never per row).
    pub fn debug_key(value: &Value) -> String {
        format!("{value:?}")
    }
}

impl StreamingGroupedAgg {
    pub fn new(slots: &[AggregateSlot], group_exprs: &[Expression]) -> Self {
        let mut grouped: HashMap<
            Vec<String>,
            (Vec<Value>, Vec<AggregateAccumulator>),
            crate::util::FastBuildHasher,
        > = HashMap::default();
        if group_exprs.is_empty() {
            grouped.insert(
                Vec::new(),
                (
                    Vec::new(),
                    slots
                        .iter()
                        .map(|slot| {
                            AggregateAccumulator::new(&slot.name, slot.arg.as_ref(), slot.distinct)
                        })
                        .collect(),
                ),
            );
        }
        Self {
            slots: slots.to_vec(),
            group_exprs: group_exprs.to_vec(),
            grouped,
            fast: KeyPlan::Undecided,
            fast_groups: HashMap::default(),
        }
    }

    /// Resolves the fast-key plan once per query: exactly one group
    /// expression that is a plain column reference (never a session
    /// function, which resolves from execution context, not rows).
    /// Anything else stays `Legacy`. Decided on first feed so empty
    /// inputs behave exactly like legacy (no groups, no error).
    pub fn resolve_fast(&mut self, schema: &TableSchema) {
        if !matches!(self.fast, KeyPlan::Undecided) {
            return;
        }
        let [Expression::ColumnRef(name)] = self.group_exprs.as_slice() else {
            self.fast = KeyPlan::Legacy;
            return;
        };
        if crate::catalog_fn::is_session_function(name) {
            self.fast = KeyPlan::Legacy;
            return;
        }
        match schema.column_index(unqualify(name)) {
            Ok(idx) => self.fast = KeyPlan::Fast { idx },
            Err(_) => self.fast = KeyPlan::Legacy,
        }
    }

    pub fn feed(&mut self, row: &[Value], schema: &TableSchema) -> SqlResult<()> {
        self.resolve_fast(schema);
        if let KeyPlan::Fast { idx } = self.fast {
            return self.feed_fast(idx, row, schema);
        }
        let mut key_values = Vec::with_capacity(self.group_exprs.len());
        let mut key_parts = Vec::with_capacity(self.group_exprs.len());
        for expr in &self.group_exprs {
            let value = evaluate_expression(row, schema, expr)?;
            key_parts.push(format!("{:?}", value));
            key_values.push(value);
        }
        let slots = &self.slots;
        // Moved (not cloned) into the vacant entry: `or_insert_with` is
        // `FnOnce`, so the common occupied case pays no copy at all.
        let (_, accs) = self.grouped.entry(key_parts).or_insert_with(|| {
            (
                key_values,
                slots
                    .iter()
                    .map(|slot| {
                        AggregateAccumulator::new(&slot.name, slot.arg.as_ref(), slot.distinct)
                    })
                    .collect(),
            )
        });
        for (index, slot) in slots.iter().enumerate() {
            let passes_filter = match slot.filter().as_ref() {
                None => true,
                Some(filter_expr) => matches!(
                    evaluate_expression(row, schema, filter_expr)?,
                    Value::Bool(true)
                ),
            };
            if !passes_filter {
                continue;
            }
            if crate::json::is_json_object_aggregate(&slot.name) {
                let key = match slot.arg().as_ref() {
                    Some(Expression::Star) | None => Value::Null,
                    Some(arg) => evaluate_expression(row, schema, arg)?,
                };
                let value = match slot.second_arg().as_ref() {
                    Some(arg) => evaluate_expression(row, schema, arg)?,
                    None => Value::Null,
                };
                accs[index].step_pair(key, value);
            } else {
                accs[index].step(slot.arg(), row, schema)?;
            }
        }
        Ok(())
    }

    /// Fast feed for a resolved single-column key: no per-row expression
    /// evaluation, no `format!("{:?}")` key building. Grouping matches
    /// legacy exactly (`FastKey` mirrors Debug partitioning, proven by
    /// unit test); accumulator feeding is byte-identical to `feed`
    /// (same slots, same FILTER checks, same `step` calls).
    fn feed_fast(&mut self, idx: usize, row: &[Value], schema: &TableSchema) -> SqlResult<()> {
        let Some(cell) = row.get(idx) else {
            // Legacy indexes `row[idx]` and would panic here; reproduce
            // the failure explicitly instead of silently continuing.
            let _ = &row[idx];
            unreachable!("row width matches schema");
        };
        let key = FastKey::from_value(cell);
        let slots = &self.slots;
        let (_, accs) = self.fast_groups.entry(key).or_insert_with(|| {
            (
                vec![cell.clone()],
                slots
                    .iter()
                    .map(|slot| {
                        AggregateAccumulator::new(&slot.name, slot.arg.as_ref(), slot.distinct)
                    })
                    .collect(),
            )
        });
        for (index, slot) in slots.iter().enumerate() {
            let passes_filter = match slot.filter().as_ref() {
                None => true,
                Some(filter_expr) => matches!(
                    evaluate_expression(row, schema, filter_expr)?,
                    Value::Bool(true)
                ),
            };
            if !passes_filter {
                continue;
            }
            if crate::json::is_json_object_aggregate(&slot.name) {
                let key = match slot.arg().as_ref() {
                    Some(Expression::Star) | None => Value::Null,
                    Some(arg) => evaluate_expression(row, schema, arg)?,
                };
                let value = match slot.second_arg().as_ref() {
                    Some(arg) => evaluate_expression(row, schema, arg)?,
                    None => Value::Null,
                };
                accs[index].step_pair(key, value);
            } else {
                accs[index].step(slot.arg(), row, schema)?;
            }
        }
        Ok(())
    }

    /// Unions another partial's groups into `self` (parallel reduction).
    ///
    /// Key-agnostic: unions BOTH maps (legacy `grouped` keyed by `Vec<String>`
    /// and `fast_groups` keyed by `FastKey`). On key conflict the existing key
    /// values are kept and the accumulator vectors merge pairwise in slot
    /// order through [`AggregateAccumulator::merge_into`]. Both sides must be
    /// partials of the SAME query (same slots, same group expressions); the
    /// caller merges in worker-index order so error reporting stays
    /// deterministic. Mixed fast/legacy/undecided plans across workers need no
    /// normalization: `finish` unions both maps defensively either way.
    pub fn merge_from(&mut self, other: Self) {
        debug_assert_eq!(
            self.slots.len(),
            other.slots.len(),
            "merge_from: both partials must come from the same slots"
        );
        for (key, (key_values, accs)) in other.grouped {
            match self.grouped.entry(key) {
                Entry::Occupied(mut slot) => {
                    for (left, right) in slot.get_mut().1.iter_mut().zip(accs) {
                        left.merge_into(right);
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert((key_values, accs));
                }
            }
        }
        for (key, (key_values, accs)) in other.fast_groups {
            match self.fast_groups.entry(key) {
                Entry::Occupied(mut slot) => {
                    for (left, right) in slot.get_mut().1.iter_mut().zip(accs) {
                        left.merge_into(right);
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert((key_values, accs));
                }
            }
        }
    }

    /// Owner of a group for key-hash routing (`owner = hash % workers`).
    ///
    /// Deterministic: the same group always routes to the same owner, so a
    /// shuffled merge is disjoint (insert-only) and per-key merge order is
    /// preserved by merging drained partials in worker-index order — the
    /// same order `merge_from` uses today, keeping promotion-tail semantics
    /// (`SumN/F/Numeric`, `AvgN/F/Numeric`) identical. `resolve_fast` is
    /// purely shape-based, so all partials of one query agree on fast vs
    /// legacy and no group ever splits across the two maps.
    fn owner_fast(key: &FastKey, workers: usize) -> usize {
        // Fixed-seed FNV-1a (see `crate::util::FastHasher`): routing only —
        // equal keys still share an owner, which is the one property the
        // shuffle relies on.
        (crate::util::fast_hash(key) as usize) % workers.max(1)
    }

    /// Owner of a legacy (`Vec<String>`) group — same contract as
    /// [`owner_fast`](Self::owner_fast).
    fn owner_legacy(key: &[String], workers: usize) -> usize {
        (crate::util::fast_hash(key) as usize) % workers.max(1)
    }

    /// Drains the entries owned by `owner` into a fresh partial (same
    /// slots, same group expressions, same key plan). Entries move out;
    /// `self` keeps the rest. The union of `drain_for_owner(o)` over all
    /// owners is exactly the original, pairwise disjoint.
    pub fn drain_for_owner(&mut self, owner: usize, workers: usize) -> Self {
        let mut out = Self {
            slots: self.slots.clone(),
            group_exprs: self.group_exprs.clone(),
            grouped: HashMap::default(),
            fast: self.fast,
            fast_groups: HashMap::default(),
        };
        let owned: Vec<Vec<String>> = self
            .grouped
            .keys()
            .filter(|key| Self::owner_legacy(key, workers) == owner)
            .cloned()
            .collect();
        for key in owned {
            if let Some(entry) = self.grouped.remove(&key) {
                out.grouped.insert(key, entry);
            }
        }
        let owned_fast: Vec<FastKey> = self
            .fast_groups
            .keys()
            .filter(|key| Self::owner_fast(key, workers) == owner)
            .cloned()
            .collect();
        for key in owned_fast {
            if let Some(entry) = self.fast_groups.remove(&key) {
                out.fast_groups.insert(key, entry);
            }
        }
        out
    }

    pub fn finish(
        self,
        schema: &TableSchema,
        targets: &[SelectTarget],
        having: Option<&Expression>,
        current_database: &str,
        current_user: &str,
    ) -> SqlResult<QueryResult> {
        // Fast-path groups convert back to legacy key strings once per
        // GROUP (never per row): the strings are identical to what the
        // legacy feed would have built from the same values, so HAVING
        // evaluation and projection below cannot tell the difference.
        // Either map may hold groups (never both for one query), so merge
        // defensively rather than assuming exclusivity.
        let mut grouped = self.grouped;
        for (fast_key, (key_values, accs)) in self.fast_groups {
            let _ = fast_key;
            let key_parts = key_values
                .iter()
                .map(FastKey::debug_key)
                .collect::<Vec<_>>();
            grouped.insert(key_parts, (key_values, accs));
        }
        // One output row per group at most (HAVING only removes): exact
        // upper-bound capacity, no behavior change.
        let mut group_rows: Vec<Vec<Value>> = Vec::with_capacity(grouped.len());
        for (key_vals, accs) in grouped.into_values() {
            if let Some(having_expr) = having {
                let synthetic = build_group_synthetic(schema, &key_vals, &self.group_exprs);
                let result = evaluate_grouped_expression(
                    &synthetic,
                    schema,
                    having_expr,
                    &self.slots,
                    &accs,
                )?;
                if !matches!(result, Value::Bool(true)) {
                    continue;
                }
            }
            // `key_vals` is only borrowed below (HAVING check and grouped
            // projection both take slices), so no copy is needed.
            let evaluated = project_grouped_row(
                &key_vals,
                &self.group_exprs,
                schema,
                targets,
                &self.slots,
                &accs,
                current_database,
                current_user,
            )?;
            group_rows.push(evaluated);
        }
        let cols = select_columns(schema, targets)?;
        let column_types = select_column_types(schema, targets)?;
        Ok(QueryResult::Rows {
            columns: cols,
            column_types,
            rows: group_rows,
        })
    }
}

pub fn sort_rows(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    order_by: &[plomid_sql::OrderByItem],
    top_k: Option<usize>,
) -> SqlResult<Vec<Vec<Value>>> {
    let mut keyed = rows
        .into_iter()
        .map(|row| {
            let keys = order_by
                .iter()
                .map(|item| evaluate_expression(&row, schema, &item.expr))
                .collect::<SqlResult<Vec<_>>>()?;
            Ok((row, keys))
        })
        .collect::<SqlResult<Vec<_>>>()?;

    if let Some(limit) = top_k {
        if limit == 0 {
            return Ok(Vec::new());
        }
        if limit < keyed.len() {
            let split = limit - 1;
            keyed
                .select_nth_unstable_by(split, |(_, a), (_, b)| compare_order_keys(a, b, order_by));
            keyed.truncate(limit);
        }
    }
    keyed.sort_by(|(_, a), (_, b)| compare_order_keys(a, b, order_by));
    Ok(keyed.into_iter().map(|(row, _)| row).collect())
}

/// Minimum materialized rows for parallel ORDER BY fan-out on the Laptop
/// tier (see [`crate::parallel_budget::ParallelBudget`]).
///
/// Below this the spawn/merge overhead exceeds the key-eval/sort saving, so
/// callers run the sequential path byte-identically. The live gate reads the
/// tiered value ([`crate::parallel_budget::sort_min_rows`]); this const pins
/// the Laptop tier (Edge 200_000, Server 25_000). The full-sort site
/// compares `source_rows.len()` against the gate; the TopK streaming site
/// compares a keys-only `count_range` estimate (advisory: filtered matches
/// are fewer, which only undersells the win, never correctness).
///
/// Laptop-tier spec: production reads the tiered delegate; tier tests
/// assert the delegate against this const under a Laptop pin.
#[allow(dead_code)]
pub const PAR_SORT_MIN_ROWS: usize = 50_000;

/// Maximum materialized input rows for the grouped-aggregation path before
/// spilling key-hash partitions to bounded temp storage instead of holding
/// every input row plus the group hash table on the heap.
///
/// [`grouped_agg_spilled`](self::grouped_agg_spilled) runs the existing
/// in-memory grouped fold byte-identical at or under this bound; above it
/// input rows are partitioned by hash of the encoded group-key bytes and
/// spilled as opaque storage-codec records through one `plomid-spill`
/// `SpillManager` (temp-limit enforced, files deleted on `Drop`), then each
/// partition is re-aggregated through the existing fold. Set from the same
/// payload arithmetic as `SORT_SPILL_MAX_ROWS`: decoded rows are wider than
/// storage payloads, so the grouped budget stays conservative.
pub(super) const GROUP_SPILL_MAX_ROWS: usize = 250_000;

/// True when `order_by` provably induces a total order over the table's rows,
/// the exactness gate for every parallel sort/TopK merge.
///
/// Proof shape: the plain-`ColumnRef` keys cover a whole single or composite
/// `UNIQUE`/`PRIMARY KEY` column set (via
/// [`TableSchema::unique_column_sets`](plomid_sql::TableSchema::unique_column_sets),
/// so composite members alone never qualify), every covering column is
/// `NOT NULL` (NULLs compare `Equal` to each other, so a nullable unique
/// column still ties) and of an exactly-comparable type. Float-family unique
/// columns (`FLOAT4`/`FLOAT8`/`NUMERIC`) are excluded: `value_cmp` projects
/// numerics through `f64`, where `NaN` compares `Equal` and distinct large
/// values can collide, so float uniqueness is not comparator-total.
/// Integer, text and the remaining families compare exactly.
///
/// Non-`ColumnRef` keys (expressions, literals) neither qualify nor
/// disqualify: only coverage of a uniqueness set by plain column keys fires
/// the gate. Resolution uses the same
/// `schema.column_index(unqualify(name))` lookup key evaluation uses, so the
/// gate can never see a different column than the sort.
///
pub fn par_sort_order_is_total(schema: &TableSchema, order_by: &[plomid_sql::OrderByItem]) -> bool {
    if order_by.is_empty() {
        return false;
    }
    let mut key_cols: Vec<usize> = Vec::with_capacity(order_by.len());
    for item in order_by {
        if let Expression::ColumnRef(name) = &item.expr {
            if crate::catalog_fn::is_session_function(name) {
                continue;
            }
            if let Ok(idx) = schema.column_index(unqualify(name)) {
                key_cols.push(idx);
            }
        }
    }
    if key_cols.is_empty() {
        return false;
    }
    schema.unique_column_sets().iter().any(|set| {
        !set.is_empty()
            && set.iter().all(|idx| key_cols.contains(idx))
            && set.iter().all(|idx| schema.is_not_null(*idx))
            && set.iter().all(|idx| {
                !matches!(
                    schema.columns[*idx].col_type.type_oid,
                    plomid_types::TypeOid::FLOAT4
                        | plomid_types::TypeOid::FLOAT8
                        | plomid_types::TypeOid::NUMERIC
                )
            })
    })
}

/// True when an expression is safe to evaluate on a sort worker thread:
/// no statement-context resolution (session/clock functions read thread-local
/// execution state that workers do not inherit) and no volatile calls.
/// `random()` reads the system clock per call (`crate::scalar`), so parallel
/// evaluation would assign different keys than the sequential scan order;
/// anything else the row path evaluates here is a pure function of the row.
/// Subqueries and window calls fail closed too (they never reach the row sort,
/// but declining stays behavior-preserving if that ever changes).
fn par_sort_expr_unsafe(expr: &Expression) -> bool {
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
                || name.eq_ignore_ascii_case("random")
                || args.iter().any(par_sort_expr_unsafe)
                || filter.as_ref().is_some_and(|e| par_sort_expr_unsafe(e))
                || order_by.iter().any(|item| par_sort_expr_unsafe(&item.expr))
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
            par_sort_expr_unsafe(a) || par_sort_expr_unsafe(b)
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
        | Expression::RowField { expr: a, .. } => par_sort_expr_unsafe(a),
        Expression::In {
            expr,
            list,
            subquery,
            ..
        } => {
            subquery.is_some()
                || par_sort_expr_unsafe(expr)
                || list.iter().any(par_sort_expr_unsafe)
        }
        Expression::Between {
            expr, low, high, ..
        } => par_sort_expr_unsafe(expr) || par_sort_expr_unsafe(low) || par_sort_expr_unsafe(high),
        Expression::Like { expr, pattern, .. } => {
            par_sort_expr_unsafe(expr) || par_sort_expr_unsafe(pattern)
        }
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            operand.as_ref().is_some_and(|e| par_sort_expr_unsafe(e))
                || whens
                    .iter()
                    .any(|(a, b)| par_sort_expr_unsafe(a) || par_sort_expr_unsafe(b))
                || default.as_ref().is_some_and(|e| par_sort_expr_unsafe(e))
        }
        Expression::Coalesce(v) => v.iter().any(par_sort_expr_unsafe),
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

/// True when every `ORDER BY` key is safe to evaluate on sort workers.
/// There is intentionally no wildcard arm: a new `Expression` variant is a
/// compile error here, not a silently-parallelized evaluation.
pub fn par_sort_keys_parallel_safe(order_by: &[plomid_sql::OrderByItem]) -> bool {
    order_by
        .iter()
        .all(|item| !par_sort_expr_unsafe(&item.expr))
}

/// True when at least one `ORDER BY` key needs real per-row evaluation
/// (function calls, arithmetic, casts, ...): the work parallel TopK fans
/// out. Pure column-reference and literal keys evaluate for free, so
/// parallel TopK cannot amortize its fan-out there (measured ~2x slower than
/// sequential at 100k rows on cheap colref keys); those shapes stay
/// sequential pending larger-scale curves.
///
/// The wildcard defaults to costly, which is safe only in combination with
/// [`par_sort_keys_parallel_safe`]: that gate is exhaustively fail-closed,
/// so no new expression kind can reach a parallel worker without an explicit
/// safety decision there first.
pub fn par_sort_has_costly_keys(order_by: &[plomid_sql::OrderByItem]) -> bool {
    order_by
        .iter()
        .any(|item| !matches!(item.expr, Expression::ColumnRef(_) | Expression::Literal(_)))
}

/// True when a filter expression is safe to evaluate on sort/scan workers
/// (same worker-visibility rule as the keys). Guards the parallel TopK
/// `WHERE` evaluation.
pub fn par_sort_expr_parallel_safe(expr: &Expression) -> bool {
    !par_sort_expr_unsafe(expr)
}

/// Parallel worker count for `row_count` rows, or `None` when the caller
/// must stay sequential: under-threshold inputs or fewer than two workers.
///
/// The row threshold is tiered ([`crate::parallel_budget::sort_min_rows`]:
/// Laptop 50_000) and the worker count is clamped to the effective tier
/// cap ([`crate::parallel_budget::available_workers`]), fixing the old
/// unclamped `available_parallelism` fan-out. Total-order and key-safety
/// proofs stay the caller's (they need `schema`/`order_by`, this needs only
/// the count); callers claim the reported width through
/// [`crate::parallel_budget::claim_dispatch_workers`] and run sequential on
/// decline (the row-store full-sort and TopK streaming sites both do).
pub fn par_sort_workers_for(row_count: u64) -> Option<usize> {
    if row_count < crate::parallel_budget::sort_min_rows() as u64 {
        return None;
    }
    let workers = crate::parallel_budget::available_workers();
    (workers >= 2).then_some(workers)
}

/// Parallel full sort over already-materialized row-store rows.
///
/// Budget-claimed by the row-store ORDER BY site (threshold width from
/// [`par_sort_workers_for`], permits from
/// [`crate::parallel_budget::claim_dispatch_workers`]); proven exact under
/// the total-order gate. Measured at parity-or-worse vs sequential at 100k
/// rows on this host class because scan/decode dominate — the threshold gate
/// (not this function) decides profitability.
///
/// Same contract as [`sort_rows`]: identical key evaluation
/// (`evaluate_expression` per `order_by` item), the same
/// [`compare_order_keys`] comparator, the same `top_k` pre-truncation
/// semantics. Shape of the work: contiguous index partitions of the input
/// (disjoint, covering, in row order) each key-evaluate then `sort_by` in
/// place on one worker, then a single deterministic k-way merge on the
/// caller thread takes `top_k` (or all) rows, breaking comparator ties to
/// the lowest partition index.
///
/// Exactness: the caller proves a total order
/// ([`par_sort_order_is_total`]) before calling, so no two distinct rows
/// compare `Equal` and per-partition stable sorts plus the deterministic
/// merge equal the sequential `sort_by` bit-for-bit. Key-evaluation errors
/// match the sequential first-in-row-order error exactly: each worker keeps
/// its partition's first error with its global row index and the lowest
/// index wins (sequential `collect` short-circuits at the same row).
/// Deterministic for every input, total order or not; without the gate the
/// merge only guarantees key-order, not tie identities.
///
/// No mutex, no global state, `std::thread::scope` only; `Send`/`Sync` by
/// construction (owned row chunks move, `schema`/`order_by` share by ref).
///
pub fn par_sort_rows(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    order_by: &[plomid_sql::OrderByItem],
    top_k: Option<usize>,
    workers: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    if rows.is_empty() {
        return Ok(rows);
    }
    if top_k == Some(0) {
        return Ok(Vec::new());
    }
    let nparts = workers.max(2).min(rows.len());
    let mut partitions = Vec::with_capacity(nparts);
    let mut start = 0;
    for i in 0..nparts {
        let take = rows.len() / nparts + usize::from(i < rows.len() % nparts);
        let end = start + take;
        partitions.push((start, end));
        start = end;
    }
    let take = top_k.unwrap_or(usize::MAX);
    let rows_ref = &rows;
    let mut sorted = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(nparts);
        for (part_start, part_end) in partitions {
            handles.push(scope.spawn(move || {
                let mut keyed: Vec<(Vec<Value>, Vec<Value>)> =
                    Vec::with_capacity(part_end - part_start);
                let mut failure: Option<(usize, SqlError)> = None;
                for (offset, row) in rows_ref[part_start..part_end].iter().enumerate() {
                    let keys = match order_by
                        .iter()
                        .map(|item| evaluate_expression(row, schema, &item.expr))
                        .collect::<SqlResult<Vec<_>>>()
                    {
                        Ok(keys) => keys,
                        Err(e) => {
                            failure = Some((part_start + offset, e));
                            break;
                        }
                    };
                    keyed.push((row.clone(), keys));
                }
                if failure.is_none() {
                    keyed.sort_by(|(_, a), (_, b)| compare_order_keys(a, b, order_by));
                }
                (keyed, failure)
            }));
        }
        let mut sorted = Vec::with_capacity(handles.len());
        for handle in handles {
            sorted.push(handle.join().expect("par sort worker"));
        }
        sorted
    });
    // Lowest failing row index wins: each worker kept its partition's first
    // error in row order, so the minimum is exactly the row the sequential
    // `collect` short-circuits at, with the identical error value.
    let best_idx: Option<usize> = sorted
        .iter()
        .filter_map(|(_, failure)| failure.as_ref().map(|(idx, _)| *idx))
        .min();
    if let Some(best_idx) = best_idx {
        for (_, failure) in &mut sorted {
            if failure.as_ref().is_some_and(|(idx, _)| *idx == best_idx) {
                let (_, e) = failure.take().expect("index matched above");
                return Err(e);
            }
        }
        unreachable!("best index came from one of the failures");
    }
    // Single-partition fast path (rows < workers): already sorted in place.
    if sorted.len() == 1 {
        let (mut keyed, _) = sorted.into_iter().next().expect("one partition");
        if take < keyed.len() {
            keyed.truncate(take);
        }
        return Ok(keyed.into_iter().map(|(row, _)| row).collect());
    }
    // Deterministic k-way merge over the sorted partitions: the minimum
    // head wins; comparator ties break to the lowest partition index.
    // Under the gate ties never occur, so every emitted row equals the
    // sequential full sort at the same position.
    let mut heads = vec![0usize; sorted.len()];
    let total: usize = sorted.iter().map(|(keyed, _)| keyed.len()).sum();
    let mut out = Vec::with_capacity(take.min(total));
    loop {
        if out.len() >= take {
            break;
        }
        let mut best: Option<usize> = None;
        for (p, (keyed, _)) in sorted.iter().enumerate() {
            if heads[p] >= keyed.len() {
                continue;
            }
            let take_head = match best {
                None => true,
                Some(b) => {
                    compare_order_keys(&keyed[heads[p]].1, &sorted[b].0[heads[b]].1, order_by)
                        == std::cmp::Ordering::Less
                }
            };
            if take_head {
                best = Some(p);
            }
        }
        match best {
            None => break,
            Some(p) => {
                let (keyed, _) = &sorted[p];
                out.push(keyed[heads[p]].0.clone());
                heads[p] += 1;
            }
        }
    }
    Ok(out)
}

pub fn compare_order_keys(
    a: &[Value],
    b: &[Value],
    order_by: &[plomid_sql::OrderByItem],
) -> std::cmp::Ordering {
    for (index, item) in order_by.iter().enumerate() {
        let va = &a[index];
        let vb = &b[index];
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

/// Applies PostgreSQL `DISTINCT ON` semantics to already `ORDER BY`-sorted
/// source (storage) rows: evaluate the ON expressions per row and keep the
/// first row of each group. Because rows are sorted, the survivor is the
/// first row per group under the ORDER BY ordering.
#[allow(clippy::too_many_arguments)]
pub fn apply_distinct_on(
    rows: Vec<Vec<Value>>,
    schema: &TableSchema,
    distinct_on: &[Expression],
    _order_by: &[plomid_sql::OrderByItem],
    _targets: &[SelectTarget],
    _current_database: &str,
    _current_user: &str,
) -> SqlResult<Vec<Vec<Value>>> {
    // Hashed path with the authoritative `values_equal` as the final word:
    // bucket candidate keys by `equality_signature` (implied by equality, so
    // no duplicate is ever missed), then confirm inside the bucket. This is
    // `O(rows)` instead of the previous `O(rows x distinct)` linear scan.
    let mut buckets: HashMap<u64, Vec<Vec<Value>>, crate::util::FastBuildHasher> =
        HashMap::default();
    let mut out: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        let mut key = Vec::with_capacity(distinct_on.len());
        for expr in distinct_on {
            key.push(evaluate_expression(&row, schema, expr)?);
        }
        let sig = equality_signature(&key);
        let bucket = buckets.entry(sig).or_default();
        let duplicate = bucket.iter().any(|prev| {
            prev.len() == key.len() && prev.iter().zip(key.iter()).all(|(l, r)| values_equal(l, r))
        });
        if !duplicate {
            bucket.push(key);
            out.push(row);
        }
    }
    Ok(out)
}

/// Bounded top-K heap for `ORDER BY ... LIMIT k`.
///
/// Holds at most `cap` entries as a max-heap over the ORDER BY comparator
/// (root = current worst), so streaming `N` rows costs `O(N log k)` time and
/// `O(k)` memory instead of materializing all `N` rows. NULL ordering and
/// direction handling delegate to [`compare_order_keys`], the same comparator
/// `sort_rows` uses, so heap output sorted at the end matches the full sort
/// for the retained prefix.
pub struct TopKHeap {
    cap: usize,
    entries: Vec<(Vec<Value>, Vec<Value>)>,
}

impl TopKHeap {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            cap,
            entries: Vec::new(),
        }
    }

    /// Offers one decoded `(row, keys)` pair to the heap.
    pub(super) fn offer(
        &mut self,
        row: Vec<Value>,
        keys: Vec<Value>,
        order_by: &[plomid_sql::OrderByItem],
    ) {
        if self.cap == 0 {
            return;
        }
        if self.entries.len() < self.cap {
            self.entries.push((row, keys));
            if self.entries.len() == self.cap {
                self.build_max_heap(order_by);
            }
            return;
        }
        // Full: keep the candidate only when it sorts before the current
        // worst (the heap root). Ties keep the incumbent, matching the
        // full-sort prefix up to tie order, which SQL leaves unspecified
        // without a unique tiebreaker.
        let dominated = match self.entries.first() {
            Some((_, worst_keys)) => {
                compare_order_keys(&keys, worst_keys, order_by) != std::cmp::Ordering::Less
            }
            None => false,
        };
        if dominated {
            return;
        }
        if let Some(root) = self.entries.first_mut() {
            *root = (row, keys);
            let len = self.entries.len();
            self.sift_down_range(0, len, order_by);
        }
    }

    /// Retained `(row, keys)` entries for a parallel TopK merge: the caller
    /// re-offers every worker's entries into one final heap, which is exact
    /// because each worker's local top-`cap` is a superset of its share of
    /// the global top-`cap` (a row ranked `<= cap` globally ranks `<= cap`
    /// within its own partition).
    pub(super) fn into_entries(self) -> Vec<(Vec<Value>, Vec<Value>)> {
        self.entries
    }

    /// Sorted top-K rows, ascending per ORDER BY.
    pub(super) fn into_sorted_rows(
        mut self,
        order_by: &[plomid_sql::OrderByItem],
    ) -> Vec<Vec<Value>> {
        if self.entries.len() < self.cap {
            // Never reached capacity: plain sort of the retained prefix.
            self.entries
                .sort_by(|(_, a), (_, b)| compare_order_keys(a, b, order_by));
        } else {
            // Heap-sort the bounded heap so output order matches `sort_rows`.
            let len = self.entries.len();
            for i in (0..len / 2).rev() {
                self.sift_down_range(i, len, order_by);
            }
            let mut out = Vec::with_capacity(len);
            let mut active = len;
            while active > 0 {
                self.entries.swap(0, active - 1);
                let (row, _) = self.entries.pop().expect("heap entry");
                out.push(row);
                active -= 1;
                if active > 0 {
                    self.sift_down_range(0, active, order_by);
                }
            }
            out.reverse();
            return out;
        }
        self.entries.into_iter().map(|(row, _)| row).collect()
    }

    fn build_max_heap(&mut self, order_by: &[plomid_sql::OrderByItem]) {
        let len = self.entries.len();
        for i in (0..len / 2).rev() {
            self.sift_down_range(i, len, order_by);
        }
    }

    fn sift_down_range(&mut self, mut i: usize, len: usize, order_by: &[plomid_sql::OrderByItem]) {
        loop {
            let left = 2 * i + 1;
            let right = left + 1;
            let mut worst = i;
            if left < len
                && compare_order_keys(&self.entries[left].1, &self.entries[worst].1, order_by)
                    == std::cmp::Ordering::Greater
            {
                worst = left;
            }
            if right < len
                && compare_order_keys(&self.entries[right].1, &self.entries[worst].1, order_by)
                    == std::cmp::Ordering::Greater
            {
                worst = right;
            }
            if worst == i {
                break;
            }
            self.entries.swap(i, worst);
            i = worst;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn project_grouped_row(
    group_key: &[Value],
    group_exprs: &[Expression],
    schema: &TableSchema,
    targets: &[SelectTarget],
    slots: &[AggregateSlot],
    accs: &[AggregateAccumulator],
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<Value>> {
    let synthetic: Vec<Value> = build_group_synthetic(schema, group_key, group_exprs);
    if targets.len() == 1 && matches!(targets[0], SelectTarget::All) {
        return Ok(synthetic);
    }
    let mut projected = Vec::with_capacity(targets.len());
    for target in targets {
        match target {
            SelectTarget::All | SelectTarget::QualifiedStar { .. } => {
                projected.extend(synthetic.iter().cloned());
            }
            SelectTarget::Expr { expr, .. } => projected.push(evaluate_grouped_expression(
                &synthetic, schema, expr, slots, accs,
            )?),
            SelectTarget::Function(name) => {
                projected.push(function_value(name, current_database, current_user)?)
            }
            SelectTarget::FunctionCall { name, args } => {
                if is_aggregate_function(name) {
                    // Unqualified `SELECT agg(...)` targets carry no FILTER, so
                    // pass `None` explicitly. Filtered forms arrive as
                    // `SelectTarget::Expr` and flow through
                    // `evaluate_grouped_expression`, which matches on the
                    // full FILTER expression.
                    projected.push(
                        lookup_aggregate(slots, accs, name, args.first(), None, false)
                            .unwrap_or(Value::Null),
                    );
                } else if is_session_function(name) {
                    projected.push(function_value(name, current_database, current_user)?)
                } else {
                    let argument = args
                        .first()
                        .and_then(|e| match e {
                            Expression::Literal(Value::Text(v)) => Some(v.as_str()),
                            _ => None,
                        })
                        .unwrap_or("");
                    let _ = (argument, slots, accs);
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Unsupported,
                        format!("function \"{name}\" is not supported in projection"),
                    )));
                }
            }
            SelectTarget::WindowFunction { name, .. } => {
                let _ = name;
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Unsupported,
                    "window functions must be evaluated by the general query engine",
                )));
            }
            SelectTarget::Aliased { target, .. } => {
                projected.extend(project_grouped_row(
                    group_key,
                    group_exprs,
                    schema,
                    std::slice::from_ref(target),
                    slots,
                    accs,
                    current_database,
                    current_user,
                )?);
            }
        }
    }
    Ok(projected)
}

fn build_group_synthetic(
    schema: &TableSchema,
    group_key: &[Value],
    group_exprs: &[Expression],
) -> Vec<Value> {
    let n = schema.columns.len();
    let mut synthetic = vec![Value::Null; n];
    for (i, expr) in group_exprs.iter().enumerate() {
        if let Some(val) = group_key.get(i) {
            if let Expression::ColumnRef(name) = expr {
                if let Ok(idx) = schema.column_index(unqualify(name)) {
                    synthetic[idx] = val.clone();
                }
            }
        }
    }
    synthetic
}

pub fn project_row(
    row: &[Value],
    schema: &TableSchema,
    targets: &[SelectTarget],
    current_database: &str,
    current_user: &str,
) -> SqlResult<Vec<Value>> {
    if targets.len() == 1 && matches!(targets[0], SelectTarget::All) {
        return Ok(row.to_vec());
    }
    let mut projected = Vec::with_capacity(targets.len());
    for target in targets {
        match target {
            SelectTarget::All | SelectTarget::QualifiedStar { .. } => {
                projected.extend_from_slice(row);
            }
            SelectTarget::Expr { expr, .. } => {
                projected.push(evaluate_expression(row, schema, expr)?)
            }
            SelectTarget::Function(name) => {
                projected.push(function_value(name, current_database, current_user)?)
            }
            SelectTarget::FunctionCall { name, args: _ } => {
                if name.eq_ignore_ascii_case("count") {
                    projected.push(Value::Int8(1));
                } else {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Unsupported,
                        format!("function \"{}\" is not supported", name),
                    )));
                }
            }
            SelectTarget::WindowFunction { name, .. } => {
                let _ = name;
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Unsupported,
                    "window functions must be evaluated by the general query engine",
                )));
            }
            SelectTarget::Aliased { target, .. } => {
                projected.extend(project_row(
                    row,
                    schema,
                    std::slice::from_ref(target),
                    current_database,
                    current_user,
                )?);
            }
        }
    }
    Ok(projected)
}
