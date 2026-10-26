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
//! Hybrid row + column single-query execution plan.
//!
//! The row fast path in `crate::query` and the columnar scan path each answer
//! a different slice of a query well. A hybrid query chains them: an index
//! lookup yields row ids, only the requested columns are fetched, an optional
//! vectorized filter narrows the batch, and an optional aggregation finishes
//! the query. This module is the planner for that chain plus a text explainer
//! and a count estimator. It reuses the existing scan/projection vocabulary
//! (`ColumnMask`, index probing) without changing any executor path.
//!
//! Production wiring (observability): the text `EXPLAIN` renderer in
//! `crate::query::explain` builds a plan from each SELECT's real features via
//! [`plan_hybrid`] and renders it with [`explain`] plus
//! [`execute_count_estimate`]. Honest-input contract and known limitations:
//!
//! * `use_index` reuses `index_bounds_for_predicate`, the exact probe
//!   decision the indexed SELECT path makes — no second index detector.
//!   Joins/subqueries name no single probe table, so they omit the stage.
//!   The SELECT path additionally skips the probe under a pinned
//!   repeatable-read snapshot (session state the generic `EXPLAIN` site
//!   cannot consult), so a pinned reader may scan where `EXPLAIN` shows a
//!   probe.
//! * `fetch_cols` carries plain projected column references, with `*`
//!   expanded through the catalog schema when a single table is known. Any
//!   other target shape (scalar expressions, function calls, unresolvable
//!   stars) contributes nothing and the `ColumnFetch` stage is omitted
//!   rather than guessed.
//! * `has_filter` is `WHERE`-present; `has_agg` mirrors the SELECT fast
//!   path's own aggregate gate. The display predicate/aggs of [`plan_hybrid`]
//!   are generic (`filter`, `count(*)`); callers holding real text should
//!   use [`plan_hybrid_full`].
//! * The EXPLAIN site seeds the `RowIds` count with its own table-scan row
//!   estimate before calling [`execute_count_estimate`], so the estimate
//!   reads against the same `rows=` figure (with the documented 10% filter
//!   selectivity when a `VectorFilter` stage follows).
//! * The JSON `EXPLAIN` format carries no hybrid section; only the text
//!   renderer appends one.

/// One stage of a hybrid query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HybridStage {
    /// Seek an index for the qualifying keys.
    IndexLookup {
        /// Index name serving the lookup.
        index: String,
        /// Key column names driving the lookup.
        keys: Vec<String>,
    },
    /// Row ids produced by the lookup (or by a full scan when unindexed).
    RowIds {
        /// Number of row ids in this stage.
        count: u64,
    },
    /// Fetch only the projected columns for those row ids.
    ColumnFetch {
        /// Column names to fetch.
        columns: Vec<String>,
    },
    /// Apply a vectorized filter over the fetched batch.
    VectorFilter {
        /// Predicate text (display only).
        predicate: String,
    },
    /// Finish with an aggregation over the filtered batch.
    Aggregation {
        /// GROUP BY columns.
        group_by: Vec<String>,
        /// Aggregate expressions (display only).
        aggs: Vec<String>,
    },
}

/// A single-query hybrid plan: an ordered chain of [`HybridStage`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HybridPlan {
    /// Stages in execution order.
    pub stages: Vec<HybridStage>,
}

impl HybridPlan {
    /// True when the plan holds no stage.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Number of stages in the plan.
    #[must_use]
    pub fn len(&self) -> usize {
        self.stages.len()
    }
}

/// Plans the hybrid chain `INDEX LOOKUP -> ROW IDS -> COLUMN FETCH ->
/// VECTOR FILTER -> AGGREGATION`, skipping stages that are unneeded.
///
/// * `use_index` — include `IndexLookup` + `RowIds`; otherwise no lookup
///   stages are emitted and the column fetch reads the scan directly.
/// * `fetch_cols` — projected columns; an empty list skips `ColumnFetch`.
/// * `has_filter` — include `VectorFilter` when true.
/// * `has_agg` — include `Aggregation` (with default `count(*)` output)
///   when true.
///
/// This is the [`plan_hybrid_full`] chain with generic display text
/// (`filter`, `count(*)`): the stage set is identical, only the
/// caller-supplied predicate/aggregation detail is defaulted.
#[must_use]
pub fn plan_hybrid(
    use_index: bool,
    fetch_cols: Vec<String>,
    has_filter: bool,
    has_agg: bool,
) -> HybridPlan {
    plan_hybrid_full(
        use_index,
        fetch_cols,
        has_filter.then(|| "filter".to_string()),
        has_agg.then(|| (Vec::new(), vec!["count(*)".to_string()])),
    )
}

/// Extended planner carrying explicit aggregation outputs.
///
/// Same stage order as [`plan_hybrid`]; the aggregation stage uses the
/// caller-supplied `group_by`/`aggs` instead of the default.
#[must_use]
pub fn plan_hybrid_full(
    use_index: bool,
    fetch_cols: Vec<String>,
    filter: Option<String>,
    aggregation: Option<(Vec<String>, Vec<String>)>,
) -> HybridPlan {
    let mut stages = Vec::new();
    if use_index {
        stages.push(HybridStage::IndexLookup {
            index: "primary".to_string(),
            keys: Vec::new(),
        });
        stages.push(HybridStage::RowIds { count: 0 });
    }
    if !fetch_cols.is_empty() {
        stages.push(HybridStage::ColumnFetch {
            columns: fetch_cols,
        });
    }
    if let Some(predicate) = filter {
        stages.push(HybridStage::VectorFilter { predicate });
    }
    if let Some((group_by, aggs)) = aggregation {
        stages.push(HybridStage::Aggregation { group_by, aggs });
    }
    HybridPlan { stages }
}

/// One-line-per-stage text explanation of the plan.
#[must_use]
pub fn explain(plan: &HybridPlan) -> String {
    if plan.stages.is_empty() {
        return "EMPTY PLAN".to_string();
    }
    plan.stages
        .iter()
        .enumerate()
        .map(|(i, stage)| {
            let body = match stage {
                HybridStage::IndexLookup { index, keys } => {
                    format!("INDEX LOOKUP index={index} keys=[{}]", keys.join(", "))
                }
                HybridStage::RowIds { count } => format!("ROW IDS count={count}"),
                HybridStage::ColumnFetch { columns } => {
                    format!("COLUMN FETCH columns=[{}]", columns.join(", "))
                }
                HybridStage::VectorFilter { predicate } => {
                    format!("VECTOR FILTER predicate={predicate}")
                }
                HybridStage::Aggregation { group_by, aggs } => format!(
                    "AGGREGATION group_by=[{}] aggs=[{}]",
                    group_by.join(", "),
                    aggs.join(", ")
                ),
            };
            format!("{}: {body}", i + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Estimated output row count for the plan.
///
/// Reads the `RowIds` count when present (applying a 10% filter selectivity
/// when a `VectorFilter` stage follows), otherwise returns `0`. The estimate
/// is advisory for costing only.
#[must_use]
pub fn execute_count_estimate(plan: &HybridPlan) -> u64 {
    let base = plan
        .stages
        .iter()
        .find_map(|stage| match stage {
            HybridStage::RowIds { count } => Some(*count),
            _ => None,
        })
        .unwrap_or(0);
    let has_filter = plan
        .stages
        .iter()
        .any(|stage| matches!(stage, HybridStage::VectorFilter { .. }));
    if has_filter {
        base.saturating_mul(9) / 10
    } else {
        base
    }
}
