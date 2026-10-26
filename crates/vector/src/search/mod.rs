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
//! Exact search primitives: top-k, radius, batched, and rerank.
//!
//! Guarantees shared by every routine here:
//!
//! * **Deterministic** — results are ordered by `(distance, row_id)` using
//!   [`f32::total_cmp`], so ties break identically on every run. No threads,
//!   no hash iteration, no time-dependent behavior.
//! * **Bounded memory** — heaps never exceed `k` entries; batch routines
//!   stream rows per query and never materialize an `N × M` distance matrix.
//! * **Overflow-checked** — capacities and counters use checked arithmetic.
//! * **Cancellation-aware** — every row loop accepts `Option<&CancelFn>`
//!   and returns [`SearchError::Cancelled`] promptly (checked every
//!   [`CANCEL_CHECK_EVERY`] rows and once per query).
//!
//! `NULL` rows (missing vectors) are skipped: a `NULL` distance cannot be
//! ordered, so such rows never appear in top-k / radius output.

pub mod batch;
pub mod candidate;
pub mod exact;
pub mod radius;
pub mod rerank;
pub mod topk;

pub use batch::{
    compute_distances_into, search_many_parallel, search_many_parallel_numa, BatchSearcher,
};
pub use candidate::{oversample_count, Candidate, MAX_CANDIDATE_CAP};
pub use exact::{exact_batch_search, exact_radius, exact_top_k};
pub use radius::{radius_search, RadiusConfig};
pub use rerank::rerank;
pub use topk::{TopKHeap, TopKMode};

use std::fmt;

/// Distance metric for dense `f32` search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchMetric {
    /// Cosine distance (smaller = closer; `NULL` on zero norm → row skipped).
    Cosine,
    /// Euclidean distance (smaller = closer).
    L2,
    /// Raw inner product (larger = closer).
    Dot,
    /// Negative inner product, pgvector `<#>` semantics (smaller = closer).
    NegDot,
}

impl SearchMetric {
    /// Stable short name for logging / `EXPLAIN` output.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2",
            Self::Dot => "dot",
            Self::NegDot => "neg_dot",
        }
    }

    /// Heap direction that ranks "better" first for this metric.
    #[must_use]
    pub fn default_mode(self) -> TopKMode {
        match self {
            Self::Cosine | Self::L2 | Self::NegDot => TopKMode::Smallest,
            Self::Dot => TopKMode::Largest,
        }
    }
}

/// Cooperative cancellation predicate: return `true` to abort the search.
///
/// Must be cheap (it is evaluated every [`CANCEL_CHECK_EVERY`] rows).
pub type CancelFn = dyn Fn() -> bool + Send + Sync;

/// Row stride between cancellation checks.
pub const CANCEL_CHECK_EVERY: usize = 64;

/// Hard cap on heap / candidate counts; bounds memory for hostile `k`.
pub const MAX_TOP_K: usize = 1_048_576;

/// Errors produced by search routines.
#[derive(Debug, Clone, PartialEq)]
pub enum SearchError {
    /// Query / row dimensionalities disagreed.
    ///
    /// Fails the whole query: a dimension mismatch is a configuration error
    /// (wrong column or truncated query vector), never silently skipped.
    DimensionMismatch {
        /// Expected lane count (query vector length).
        expected: usize,
        /// Actual lane count (row vector length).
        actual: usize,
    },
    /// A length / count argument was invalid.
    ///
    /// Covers malformed batch shapes and candidate-count overflows where a
    /// dimension error would mislead.
    InvalidLength {
        /// What was being measured.
        what: &'static str,
        /// Offending length.
        len: usize,
    },
    /// A scalar value fell outside its representable range.
    OutOfRange {
        /// Offending value.
        value: i64,
        /// Inclusive lower bound.
        lo: i64,
        /// Inclusive upper bound.
        hi: i64,
    },
    /// Checked arithmetic overflowed.
    Overflow {
        /// Operation that overflowed.
        what: &'static str,
    },
    /// `k` exceeded [`MAX_TOP_K`].
    ///
    /// Bounds heap memory against hostile `LIMIT` values; larger requests
    /// must page instead.
    InvalidK {
        /// Requested top-k.
        k: usize,
    },
    /// Radius was negative or NaN.
    ///
    /// Radius search is inclusive (`d <= radius + TOLERANCE`); negative or
    /// NaN bounds match nothing by definition and are rejected upfront.
    InvalidRadius {
        /// Offending radius.
        radius: f32,
    },
    /// Query vector contained a non-finite lane.
    ///
    /// Storage and SQL literals reject NaN/Infinity upstream, but
    /// programmatic callers bypass those gates: search entries recheck so a
    /// NaN query fails instead of ranking rows arbitrarily.
    NonFiniteQuery,
    /// Cooperative cancellation was requested.
    Cancelled,
    /// Rerank fetch failed for a row (reserved for storage adapters that
    /// treat missing rows as errors instead of skipping them).
    ///
    /// The default rerank path skips missing rows; adapters with strict
    /// durability contracts surface them through this variant instead.
    FetchFailed {
        /// Row identity that could not be fetched.
        row_id: u64,
    },
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionMismatch { expected, actual } => {
                write!(f, "dimension mismatch: expected {expected}, got {actual}")
            }
            Self::InvalidLength { what, len } => {
                write!(f, "invalid length for {what}: {len}")
            }
            Self::OutOfRange { value, lo, hi } => {
                write!(f, "value {value} out of range [{lo}, {hi}]")
            }
            Self::Overflow { what } => write!(f, "arithmetic overflow in {what}"),
            Self::InvalidK { k } => write!(f, "invalid top-k: {k}"),
            Self::InvalidRadius { radius } => write!(f, "invalid radius: {radius}"),
            Self::NonFiniteQuery => write!(f, "query vector contains non-finite values"),
            Self::Cancelled => write!(f, "operation cancelled"),
            Self::FetchFailed { row_id } => write!(f, "fetch failed for row {row_id}"),
        }
    }
}

impl std::error::Error for SearchError {}

/// Stable machine-readable error codes (same `VECTOR_*` vocabulary as
/// [`crate::VectorError::code`]).
impl SearchError {
    /// Returns the stable `VECTOR_*` code for this error.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DimensionMismatch { .. } => "VECTOR_INDEX_DIMENSION_MISMATCH",
            Self::InvalidLength { .. } => "VECTOR_INVALID_LITERAL",
            Self::OutOfRange { .. } => "VECTOR_INVALID_LITERAL",
            Self::Overflow { .. } => "VECTOR_RESOURCE_LIMIT",
            Self::InvalidK { .. } => "VECTOR_RESOURCE_LIMIT",
            Self::InvalidRadius { .. } => "VECTOR_INVALID_LITERAL",
            Self::NonFiniteQuery => "VECTOR_NONFINITE_VALUE",
            Self::Cancelled => "VECTOR_QUERY_CANCELLED",
            Self::FetchFailed { .. } => "VECTOR_CORRUPTION",
        }
    }

    /// Formats as `CODE: detail` for SQL/user surfaces.
    #[must_use]
    pub fn coded(&self) -> String {
        format!("{}: {self}", self.code())
    }
}

impl From<crate::math::MathError> for SearchError {
    fn from(e: crate::math::MathError) -> Self {
        match e {
            crate::math::MathError::DimensionMismatch { expected, actual } => {
                Self::DimensionMismatch { expected, actual }
            }
            crate::math::MathError::InvalidLength { what, len } => {
                Self::InvalidLength { what, len }
            }
            crate::math::MathError::OutOfRange { value, lo, hi } => {
                Self::OutOfRange { value, lo, hi }
            }
            crate::math::MathError::Overflow { what } => Self::Overflow { what },
        }
    }
}

impl From<crate::distance::DistanceError> for SearchError {
    fn from(e: crate::distance::DistanceError) -> Self {
        match e {
            crate::distance::DistanceError::DimensionMismatch { expected, actual } => {
                Self::DimensionMismatch { expected, actual }
            }
            crate::distance::DistanceError::InvalidLength { what, len } => {
                Self::InvalidLength { what, len }
            }
            crate::distance::DistanceError::OutOfRange { value, lo, hi } => {
                Self::OutOfRange { value, lo, hi }
            }
            crate::distance::DistanceError::Overflow { what } => Self::Overflow { what },
        }
    }
}

/// Local result alias.
pub type Result<T> = std::result::Result<T, SearchError>;

/// Rejects non-finite query lanes before any row is touched.
///
/// Every search entry point calls this first so a NaN/Infinity query fails
/// closed instead of producing arbitrarily-ordered NaN distances.
pub fn check_query(query: &[f32]) -> Result<()> {
    if query.iter().all(|x| x.is_finite()) {
        Ok(())
    } else {
        Err(SearchError::NonFiniteQuery)
    }
}

/// Validate `k` against [`MAX_TOP_K`].
pub fn check_k(k: usize) -> Result<()> {
    if k <= MAX_TOP_K {
        Ok(())
    } else {
        Err(SearchError::InvalidK { k })
    }
}

/// Evaluate `cancel` every [`CANCEL_CHECK_EVERY`] rows (call with a running
/// row counter); also evaluates on the very first row (`counter == 0`) so
/// pre-cancelled searches abort immediately.
pub fn check_cancel(cancel: Option<&CancelFn>, counter: usize) -> Result<()> {
    if counter.is_multiple_of(CANCEL_CHECK_EVERY) {
        if let Some(stop) = cancel {
            if stop() {
                return Err(SearchError::Cancelled);
            }
        }
    }
    Ok(())
}

/// Smaller-is-better distance for `metric`, or `None` for `NULL` / undefined
/// (zero-norm cosine) rows, which callers skip.
pub(crate) fn distance_for_metric(
    metric: SearchMetric,
    query: &[f32],
    row: Option<&[f32]>,
) -> Result<Option<f32>> {
    let d = match metric {
        SearchMetric::Cosine => crate::distance::cosine_distance(Some(query), row)?,
        SearchMetric::L2 => crate::distance::l2_distance(Some(query), row)?,
        SearchMetric::Dot => crate::distance::dot(Some(query), row)?,
        SearchMetric::NegDot => crate::distance::neg_dot(Some(query), row)?,
    };
    Ok(d)
}
