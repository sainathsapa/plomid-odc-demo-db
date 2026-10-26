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
//! Cost-model join helpers: build-side choice and cardinality estimation.
//!
//! Live wiring:
//! * `choose_build_side` feeds `perform_join_parallel` (in `support.rs`),
//!   which builds the smaller input and probes with the larger one while
//!   still emitting `concat(left_row, right_row)` in original column order.
//! * `estimate_join_cardinality` feeds `perform_join`'s (in `support.rs`)
//!   output-cap pre-check: the exact cartesian (`INNER`/`CROSS` with no `ON`)
//!   rejection computes its bound through this function with conservative
//!   selectivity `1.0` (full product). The product is EXACT there, so
//!   rejection is sound; equi-`ON` joins are deliberately not rejected on the
//!   bound (a product upper bound licenses sizing, not rejection — erroring
//!   on it rejects valid selective queries, falsified in-suite), and outer
//!   joins are excluded because NULL-padding makes the product not even an
//!   upper bound. Both stay on the per-push row guard.
//!
//! Deliberately absent: a join-list reordering planner (`JoinEdge`,
//! `reorder_joins`, `weight`) was investigated and REMOVED rather than
//! fake-wired. There is no sound execution use in this engine today:
//! `FROM` trees are binary and both sides are fully materialized before the
//! join runs, so pairwise materialization order cannot change cost; the
//! indexed join path is driven by `WHERE`-prune index availability rather than
//! by cardinality; and ordering the probe-distinct key scan would be a no-op
//! (keys are deduplicated into a set, then collected by row key in storage
//! order). Sorting a list that nothing consumes would mislead readers, so the
//! items were deleted instead of wired to a no-effect call site.

/// Which side of a hash join should be the build input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildSide {
    /// Build from the left relation.
    Left,
    /// Build from the right relation.
    Right,
}

/// Picks the smaller side as the hash-table build input.
#[must_use]
pub fn choose_build_side(left_rows: u64, right_rows: u64) -> BuildSide {
    if left_rows <= right_rows {
        BuildSide::Left
    } else {
        BuildSide::Right
    }
}

/// Estimates `left x right x selectivity` output rows.
#[must_use]
pub fn estimate_join_cardinality(left: u64, right: u64, selectivity: f64) -> u64 {
    let s = selectivity.clamp(0.0, 1.0);
    if s == 0.0 || left == 0 || right == 0 {
        return 0;
    }
    ((left as f64) * (right as f64) * s).round() as u64
}
