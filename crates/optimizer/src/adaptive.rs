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
//! Adaptive row/column engine chooser.
//!
//! The executor currently gates columnar use on one heuristic (`is_generation_
//! fresh` plus a fixed 100k-row threshold). This module replaces the fixed
//! threshold with a shape-aware decision: cheap structural rules first, the
//! cost model as the fallback. Structural rules win because they are
//! robust when statistics are stale; costs win when the shape is ambiguous.
//!
//! Rule priority (first match decides):
//!
//! 1. Point lookup — an available index with `selectivity < POINT_SELECTIVITY`
//!    (0.02) serves a handful of rows; any scan would read orders of
//!    magnitude more. Always [`EngineChoice::Row`].
//! 2. Hybrid — a selective indexed filter (`selectivity <= HYBRID_MAX_
//!    SELECTIVITY`, 0.25) feeding an aggregation: row IDs come from the
//!    index, payload columns from the columnar store. The canonical hybrid
//!    pipeline (`HybridPlan`) lives in the executor crate's `hybrid` module,
//!    which owns the execution-side stage model; this crate only reasons
//!    about hybrid *costs* via [`EngineCosts`].
//! 3. Columnar aggregation — `has_aggregation` over a narrow projection
//!    (at most half the columns) folds many rows into few groups; vectorized
//!    execution dominates.
//! 4. Wide row materialization — every column is needed and most rows survive
//!    (`selectivity > WIDE_ROW_SELECTIVITY`, 0.5): the columnar projection
//!    advantage vanishes, and row assembly would just rebuild the row.
//! 5. Random access — `randomness > RANDOM_ACCESS_THRESHOLD` (0.7) without an
//!    indexable point lookup favors sequential columnar scans over random
//!    row hops.
//! 6. Cost fallback — the cheaper of [`EngineCosts::row`] /
//!    [`EngineCosts::column`] by [`PlanCost::total`](crate::cost::PlanCost),
//!    ties going to [`EngineChoice::Row`] (the primary store).

use crate::cost::{
    cost_column_scan, cost_column_scan_parallel, cost_row_index_scan, cost_row_seq_scan,
    CostInputs, PlanCost,
};
use crate::selectivity::{
    combine_selectivity_and, combine_selectivity_or, estimate_selectivity, ColumnSelectivityHints,
    PredicateKind,
};
use std::fmt;

/// Which engine should execute the query shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EngineChoice {
    /// Hot row store (index lookups, wide full-row materialization).
    Row,
    /// Immutable columnar segments (scans, narrow aggregations).
    Column,
    /// Index-filtered row IDs joined against columnar payloads.
    Hybrid,
}

impl fmt::Display for EngineChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            EngineChoice::Row => "row",
            EngineChoice::Column => "column",
            EngineChoice::Hybrid => "hybrid",
        };
        write!(f, "{name}")
    }
}

/// Selectivity below which an indexed filter is a point lookup.
pub const POINT_SELECTIVITY: f64 = 0.02;
/// Selectivity up to which an indexed filter can still feed a hybrid plan.
pub const HYBRID_MAX_SELECTIVITY: f64 = 0.25;
/// Selectivity above which a select-all projection stays on the row store.
pub const WIDE_ROW_SELECTIVITY: f64 = 0.5;
/// Randomness above which sequential columnar scans beat random row hops.
pub const RANDOM_ACCESS_THRESHOLD: f64 = 0.7;

/// Observable shape of one query used for engine selection.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryShape {
    /// Estimated rows the scan would touch before filtering.
    pub rows_touched: u64,
    /// Columns needed by the query (projection + filter + aggregation).
    pub cols_touched: usize,
    /// Total columns of the table.
    pub total_cols: usize,
    /// Fraction of `rows_touched` surviving the filter (`1.0` = no filter).
    pub selectivity: f64,
    /// True when the query aggregates (`GROUP BY` / aggregate functions).
    pub has_aggregation: bool,
    /// True when the query joins.
    pub has_join: bool,
    /// Estimated join output rows (0 when `!has_join`).
    pub join_cardinality: u64,
    /// Access randomness in `0.0..=1.0` (0 = sequential, 1 = uniform random).
    pub randomness: f64,
    /// Fraction of page reads served from cache, `0.0..=1.0`.
    pub cache_hit_rate: f64,
    /// CPU workers available to the query.
    pub cpu_available: usize,
    /// Rows that fit in the operator memory budget.
    pub memory_rows: u64,
    /// True when an index can serve the filter predicate.
    pub index_available: bool,
}

impl QueryShape {
    /// Builds a shape whose `selectivity` is derived from per-predicate hints.
    ///
    /// Each entry pairs a [`PredicateKind`] with optional
    /// [`ColumnSelectivityHints`]; per-predicate survival fractions come from
    /// [`estimate_selectivity`] and are combined with
    /// [`combine_selectivity_and`] when `conjunction` is true (all predicates
    /// must match) or [`combine_selectivity_or`] otherwise. An empty
    /// predicate list means "no filter" (`1.0`).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn with_selectivity_hints(
        rows_touched: u64,
        cols_touched: usize,
        total_cols: usize,
        predicates: &[(PredicateKind, Option<ColumnSelectivityHints>)],
        conjunction: bool,
        has_aggregation: bool,
        has_join: bool,
        join_cardinality: u64,
        randomness: f64,
        cache_hit_rate: f64,
        cpu_available: usize,
        memory_rows: u64,
        index_available: bool,
    ) -> Self {
        let selectivity = if predicates.is_empty() {
            1.0
        } else {
            let mut combined = if conjunction { 1.0 } else { 0.0 };
            for (kind, hints) in predicates {
                let sel = estimate_selectivity(*kind, hints.as_ref());
                combined = if conjunction {
                    combine_selectivity_and(combined, sel)
                } else {
                    combine_selectivity_or(combined, sel)
                };
            }
            combined
        };
        Self {
            rows_touched,
            cols_touched,
            total_cols,
            selectivity,
            has_aggregation,
            has_join,
            join_cardinality,
            randomness,
            cache_hit_rate,
            cpu_available,
            memory_rows,
            index_available,
        }
    }

    /// Point lookup shape: `WHERE id = 42` over a large indexed table.
    #[must_use]
    pub fn point_lookup(rows_touched: u64, total_cols: usize) -> Self {
        // One equality over a unique key: `1 / distinct` with every row
        // distinct, which is exactly `1 / rows_touched`.
        let hints = ColumnSelectivityHints {
            row_count: rows_touched,
            null_fraction: 0.0,
            distinct_estimate: rows_touched.max(1),
            min: None,
            max: None,
        };
        Self::with_selectivity_hints(
            rows_touched,
            total_cols,
            total_cols,
            &[(PredicateKind::Equals, Some(hints))],
            true,
            false,
            false,
            0,
            1.0,
            0.0,
            1,
            1_000_000,
            true,
        )
    }

    /// Full-table narrow aggregation shape: `GROUP BY country SUM(revenue)`.
    #[must_use]
    pub fn columnar_aggregation(rows_touched: u64, cols_touched: usize, total_cols: usize) -> Self {
        // No filter predicates: empty list under conjunction means "no
        // filter" (`1.0`).
        Self::with_selectivity_hints(
            rows_touched,
            cols_touched,
            total_cols,
            &[],
            true,
            true,
            false,
            0,
            0.0,
            0.0,
            4,
            1_000_000,
            false,
        )
    }
}

/// Cost triple compared by [`choose_engine`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineCosts {
    /// Cheapest row-store access for the shape (index or sequential).
    pub row: PlanCost,
    /// Columnar scan for the shape.
    pub column: PlanCost,
    /// Hybrid index-plus-columnar plan for the shape.
    pub hybrid: PlanCost,
}

impl EngineCosts {
    /// Estimates all three engine costs for a shape with the cost model.
    ///
    /// The row cost uses the index path when `index_available`, else the
    /// sequential path. The hybrid cost is the index filter plus a narrow
    /// columnar fetch of the survivors (half the touched columns, vector
    /// filtered) — cheaper than either parent exactly in the selective-plus-
    /// aggregation band the chooser reserves for it.
    #[must_use]
    pub fn estimate(shape: &QueryShape, inputs: &CostInputs) -> Self {
        let parallelism = shape.cpu_available.max(1);
        let mut inputs = *inputs;
        inputs.mem_limit_rows = shape.memory_rows.max(1);
        inputs.cache_hit_rate = shape.cache_hit_rate;
        inputs.parallelism = parallelism;

        let row = if shape.index_available {
            cost_row_index_scan(shape.selectivity, shape.rows_touched, &inputs)
        } else {
            cost_row_seq_scan(shape.rows_touched, &inputs)
        };
        // Multi-worker shapes take the explicit parallel scan entry point;
        // single-worker shapes keep the sequential path.
        let column = if inputs.parallelism > 1 {
            cost_column_scan_parallel(
                shape.rows_touched,
                shape.cols_touched,
                shape.total_cols.max(1),
                shape.selectivity,
                parallelism,
                &inputs,
            )
        } else {
            cost_column_scan(
                shape.rows_touched,
                shape.cols_touched,
                shape.total_cols.max(1),
                shape.selectivity,
                parallelism,
                &inputs,
            )
        };
        // Hybrid: pay the index descent plus matched-row fetch once (via the
        // row index cost at the filter selectivity), then a narrow columnar
        // pass over just the survivors for the aggregation payload.
        let survivors = (shape.selectivity.clamp(0.0, 1.0) * shape.rows_touched as f64) as u64;
        let fetch = cost_column_scan(
            survivors,
            shape.cols_touched.min(shape.total_cols.max(1)).max(1),
            shape.total_cols.max(1),
            1.0,
            1,
            &inputs,
        );
        let hybrid = PlanCost {
            cpu: row.cpu + fetch.cpu,
            io: row.io + fetch.io,
            mem: row.mem.max(fetch.mem),
            total: row.total + fetch.total,
        };
        Self {
            row,
            column,
            hybrid,
        }
    }
}

/// The engine decision with its human-readable justification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineDecision {
    /// Chosen engine.
    pub choice: EngineChoice,
    /// One-line reason naming the rule or cost comparison that decided.
    pub reason: String,
}

impl EngineDecision {
    fn ruled(choice: EngineChoice, reason: impl Into<String>) -> Self {
        Self {
            choice,
            reason: reason.into(),
        }
    }
}

/// Chooses the execution engine for a query shape given estimated costs.
///
/// Applies the module-level rules in order; the cost fallback picks the
/// cheapest [`EngineCosts`] total, ties going to [`EngineChoice::Row`].
#[must_use]
pub fn choose_engine(shape: &QueryShape, costs: &EngineCosts) -> EngineDecision {
    let selectivity = shape.selectivity.clamp(0.0, 1.0);

    if shape.index_available && selectivity < POINT_SELECTIVITY {
        return EngineDecision::ruled(
            EngineChoice::Row,
            format!(
                "point lookup (selectivity {selectivity:.4} < {POINT_SELECTIVITY}) served by index"
            ),
        );
    }

    if shape.has_aggregation && shape.index_available && selectivity <= HYBRID_MAX_SELECTIVITY {
        return EngineDecision::ruled(
            EngineChoice::Hybrid,
            format!(
                "selective indexed filter (selectivity {selectivity:.4}) feeding columnar aggregation"
            ),
        );
    }

    if shape.has_aggregation && is_narrow(shape) {
        return EngineDecision::ruled(
            EngineChoice::Column,
            format!(
                "aggregation over {}/{} columns vectorizes on columnar segments",
                shape.cols_touched,
                shape.total_cols.max(1),
            ),
        );
    }

    if !shape.has_aggregation
        && !shape.has_join
        && shape.cols_touched >= shape.total_cols.max(1)
        && selectivity > WIDE_ROW_SELECTIVITY
    {
        return EngineDecision::ruled(
            EngineChoice::Row,
            format!(
                "wide select-all (selectivity {selectivity:.2}) materializes full rows cheapest"
            ),
        );
    }

    if shape.randomness.clamp(0.0, 1.0) > RANDOM_ACCESS_THRESHOLD && !shape.index_available {
        return EngineDecision::ruled(
            EngineChoice::Column,
            format!(
                "random access pattern (randomness {:.2}) favors sequential columnar scan",
                shape.randomness,
            ),
        );
    }

    if costs.column.cheaper_than(&costs.row) && !costs.hybrid.cheaper_than(&costs.column) {
        return EngineDecision::ruled(
            EngineChoice::Column,
            format!(
                "cost fallback: column total {:.1} < row total {:.1}",
                costs.column.total, costs.row.total,
            ),
        );
    }
    if costs.hybrid.cheaper_than(&costs.row) && costs.hybrid.cheaper_than(&costs.column) {
        return EngineDecision::ruled(
            EngineChoice::Hybrid,
            format!(
                "cost fallback: hybrid total {:.1} < row total {:.1}",
                costs.hybrid.total, costs.row.total,
            ),
        );
    }
    EngineDecision::ruled(
        EngineChoice::Row,
        format!(
            "cost fallback: row total {:.1} <= column total {:.1}",
            costs.row.total, costs.column.total,
        ),
    )
}

/// Chooses the engine end-to-end: estimates costs then applies the rules.
#[must_use]
pub fn choose_engine_auto(shape: &QueryShape, inputs: &CostInputs) -> EngineDecision {
    let costs = EngineCosts::estimate(shape, inputs);
    choose_engine(shape, &costs)
}

fn is_narrow(shape: &QueryShape) -> bool {
    let total = shape.total_cols.max(1);
    shape.cols_touched.min(total) * 2 <= total
}
