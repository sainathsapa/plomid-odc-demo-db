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
//! SQL-facing distance functions with `NULL` semantics.
//!
//! Conventions (shared by every function in this module):
//!
//! * `None` inputs model SQL `NULL` vectors; any `NULL` input yields
//!   `Ok(None)` — never an error, never a fabricated number.
//! * Zero-norm inputs make cosine undefined and likewise yield `Ok(None)`;
//!   the caller (executor / planner) maps `None` to SQL `NULL`.
//! * Dimension / length disagreements are hard errors ([`DistanceError`]).
//! * [`TOLERANCE`] (`1e-5`) is the boundary tolerance for callers that need
//!   inclusive comparisons (e.g. radius search `d <= radius + TOLERANCE`).
//!
//! Dense kernels delegate to [`crate::math`] (`f64` accumulators, checked
//! integer arithmetic); sparse kernels merge sorted `(dimension, value)`
//! slices with two pointers in `O(n + m)` time and `O(1)` extra memory.

pub mod cosine;
pub mod dot;
pub mod hamming;
pub mod jaccard;
pub mod l2;

pub use cosine::{cosine_distance, cosine_distance_sparse, cosine_similarity};
pub use dot::{dot, dot_sparse, neg_dot};
pub use hamming::{hamming_distance, hamming_distance_normalized};
pub use jaccard::{
    jaccard_distance_packed, jaccard_distance_sorted, jaccard_similarity_packed,
    jaccard_similarity_sets, jaccard_similarity_sorted,
};
pub use l2::{l2_distance, l2_distance_sparse, l2_distance_squared};

use std::fmt;

/// Absolute tolerance for inclusive distance-boundary comparisons.
pub const TOLERANCE: f32 = 1e-5;

/// Returns `true` when `a` and `b` agree within [`TOLERANCE`].
#[must_use]
pub fn approx_eq(a: f32, b: f32) -> bool {
    (a - b).abs() <= TOLERANCE
}

/// Errors produced by distance functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistanceError {
    /// Slice / vector dimensionalities disagreed.
    ///
    /// Carries both sides so callers can distinguish "query dim 128 vs
    /// column dim 1536" (configuration error) from transient truncation.
    DimensionMismatch {
        /// Expected lane count (usually the query vector length).
        expected: usize,
        /// Actual lane count encountered.
        actual: usize,
    },
    /// A length / count argument was invalid.
    ///
    /// Used for packed-bit length violations and empty-input rejections
    /// where a full [`DimensionMismatch`](Self::DimensionMismatch) would be
    /// misleading.
    InvalidLength {
        /// What was being measured (`"num_bits"`, `"payload"`, ...).
        what: &'static str,
        /// Offending length.
        len: usize,
    },
    /// A scalar value fell outside its representable range.
    ///
    /// Guards integer-to-float domain crossings (e.g. absurd sparse indices)
    /// before they reach allocation arithmetic.
    OutOfRange {
        /// Offending value.
        value: i64,
        /// Inclusive lower bound.
        lo: i64,
        /// Inclusive upper bound.
        hi: i64,
    },
    /// Checked arithmetic overflowed.
    ///
    /// High-dimension integer dot products accumulate in `i64`; overflow
    /// fails here instead of wrapping silently.
    Overflow {
        /// Operation that overflowed (`"int8 dot"`, `"payload bytes"`, ...).
        what: &'static str,
    },
}

impl fmt::Display for DistanceError {
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
        }
    }
}

impl std::error::Error for DistanceError {}

/// Stable machine-readable error codes (same `VECTOR_*` vocabulary as
/// [`crate::VectorError::code`]).
impl DistanceError {
    /// Returns the stable `VECTOR_*` code for this error.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DimensionMismatch { .. } => "VECTOR_INDEX_DIMENSION_MISMATCH",
            Self::InvalidLength { .. } => "VECTOR_INVALID_LITERAL",
            Self::OutOfRange { .. } => "VECTOR_INVALID_LITERAL",
            Self::Overflow { .. } => "VECTOR_RESOURCE_LIMIT",
        }
    }

    /// Formats as `CODE: detail` for SQL/user surfaces.
    #[must_use]
    pub fn coded(&self) -> String {
        format!("{}: {self}", self.code())
    }
}

impl From<crate::math::MathError> for DistanceError {
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

/// Local result alias.
pub type Result<T> = std::result::Result<T, DistanceError>;

/// `None` (SQL `NULL`) propagates: returns `true` when either side is `NULL`.
pub(crate) fn either_null<T, U>(a: Option<T>, b: Option<U>) -> bool {
    a.is_none() || b.is_none()
}
