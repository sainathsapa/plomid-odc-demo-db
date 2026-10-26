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
//! Predicate selectivity estimation for the adaptive engine chooser.
//!
//! The adaptive optimizer needs one number per filter predicate — the fraction
//! of rows expected to survive it — before any cost comparison is possible.
//! This module provides that number from lightweight per-column hints, without
//! depending on the executor statistics authority or the columnar segment
//! metadata. Callers that own richer statistics (executor `TableStatistics`,
//! columnar zone maps) translate them into [`ColumnSelectivityHints`] and reuse
//! these defaults; nothing here reimplements collection or persistence.
//!
//! Defaults follow the classic system-catalog heuristics (PostgreSQL-style):
//! equality is `1 / distinct`, bounded ranges keep roughly a third, `IS NULL`
//! trusts the observed null fraction, and unknown shapes stay conservative so
//! the planner would rather scan than wrongly pick an index.

use std::fmt;

/// The shape of a filter predicate, as far as selectivity is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PredicateKind {
    /// `col = <literal>`: uniform-assumption `1 / distinct`.
    Equals,
    /// `col < / <= / > / >= <literal>` or `BETWEEN`: default third of rows.
    Range,
    /// `col IN (v1, ..., vn)`: fixed default (list length unknown here).
    InList,
    /// `col IS NULL`: observed null fraction.
    IsNull,
    /// `col LIKE <pattern>`: fixed default.
    Like,
    /// Anything else (inequality, function call, opaque expression).
    Other,
}

impl fmt::Display for PredicateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            PredicateKind::Equals => "equals",
            PredicateKind::Range => "range",
            PredicateKind::InList => "in-list",
            PredicateKind::IsNull => "is-null",
            PredicateKind::Like => "like",
            PredicateKind::Other => "other",
        };
        write!(f, "{name}")
    }
}

/// Default selectivity for an equality predicate with no statistics.
pub const DEFAULT_EQUALS_SELECTIVITY: f64 = 0.1;
/// Default selectivity for a range predicate with no usable bounds.
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 0.3;
/// Default selectivity for an `IN` list (element count not modelled).
pub const DEFAULT_IN_LIST_SELECTIVITY: f64 = 0.2;
/// Default selectivity for `IS NULL` with no statistics.
pub const DEFAULT_IS_NULL_SELECTIVITY: f64 = 0.01;
/// Default selectivity for `LIKE` with no statistics.
pub const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;
/// Default selectivity for opaque predicates: keep half, never prune blind.
pub const DEFAULT_OTHER_SELECTIVITY: f64 = 0.5;

/// Lightweight per-column hints translated from any statistics authority.
///
/// Field types are plain numerics (bounds as `f64`) so both the executor row
/// statistics and the columnar segment statistics can map into this struct
/// without this crate depending on either of them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColumnSelectivityHints {
    /// Total rows in the column (0 when unknown).
    pub row_count: u64,
    /// Fraction of rows that are NULL, in `0.0..=1.0`.
    pub null_fraction: f64,
    /// Estimated distinct non-NULL values (0 when unknown).
    pub distinct_estimate: u64,
    /// Minimum non-NULL value as a number, when the type maps to one.
    pub min: Option<f64>,
    /// Maximum non-NULL value as a number, when the type maps to one.
    pub max: Option<f64>,
}

impl ColumnSelectivityHints {
    /// Creates hints with no information: every estimate falls back to defaults.
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            row_count: 0,
            null_fraction: 0.0,
            distinct_estimate: 0,
            min: None,
            max: None,
        }
    }
}

/// Estimates the fraction of rows surviving a predicate.
///
/// Returns a value in `0.0..=1.0` (NaN inputs degrade to the kind default):
///
/// - `Equals`: `1 / distinct_estimate` when that is positive,
///   else [`DEFAULT_EQUALS_SELECTIVITY`].
/// - `Range`: [`DEFAULT_RANGE_SELECTIVITY`]; numeric min/max
///   hints are informational only because the literal bound is not modelled.
/// - `InList`: [`DEFAULT_IN_LIST_SELECTIVITY`].
/// - `IsNull`: observed `null_fraction`, else
///   [`DEFAULT_IS_NULL_SELECTIVITY`].
/// - `Like`: [`DEFAULT_LIKE_SELECTIVITY`].
/// - `Other`: [`DEFAULT_OTHER_SELECTIVITY`].
#[must_use]
pub fn estimate_selectivity(
    kind: PredicateKind,
    column_stats: Option<&ColumnSelectivityHints>,
) -> f64 {
    let raw = match kind {
        PredicateKind::Equals => match column_stats {
            Some(hints) if hints.distinct_estimate > 0 => 1.0 / hints.distinct_estimate as f64,
            _ => DEFAULT_EQUALS_SELECTIVITY,
        },
        PredicateKind::Range => DEFAULT_RANGE_SELECTIVITY,
        PredicateKind::InList => DEFAULT_IN_LIST_SELECTIVITY,
        PredicateKind::IsNull => match column_stats {
            Some(hints) if hints.null_fraction.is_finite() && hints.null_fraction >= 0.0 => {
                hints.null_fraction
            }
            _ => DEFAULT_IS_NULL_SELECTIVITY,
        },
        PredicateKind::Like => DEFAULT_LIKE_SELECTIVITY,
        PredicateKind::Other => DEFAULT_OTHER_SELECTIVITY,
    };
    clamp01(raw)
}

/// Combines two independent predicate selectivities (`a AND b`).
#[must_use]
pub fn combine_selectivity_and(first: f64, second: f64) -> f64 {
    clamp01(first) * clamp01(second)
}

/// Combines two independent predicate selectivities (`a OR b`).
#[must_use]
pub fn combine_selectivity_or(first: f64, second: f64) -> f64 {
    let (a, b) = (clamp01(first), clamp01(second));
    clamp01(a + b - a * b)
}

fn clamp01(value: f64) -> f64 {
    if !value.is_finite() {
        return DEFAULT_OTHER_SELECTIVITY;
    }
    value.clamp(0.0, 1.0)
}
