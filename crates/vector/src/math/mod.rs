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
//! Scalar and typed math kernels for fixed-width vector element types.
//!
//! * All floating-point accumulation uses `f64` accumulators and a single
//!   final rounding to `f32`, so results are deterministic for a given input
//!   order (no threading, no data-dependent branching on values).
//! * All integer accumulation uses `i64` with *checked* arithmetic; overflow
//!   is reported as [`MathError::Overflow`] instead of wrapping.
//! * All length arithmetic uses `usize` with checked ops — no narrowing casts
//!   of lengths or indices ("int-safe").
//! * No SIMD intrinsics live in this module; [`dispatch`] selects a
//!   [`dispatch::CpuBackend`] tier at runtime and runs the hardware kernels
//!   in `plomid-hw` (AVX2/NEON widths, `f32` accumulation, tolerance
//!   contract) or these scalar kernels for the fallback tier. Tier results
//!   agree within relative `1e-4` (absolute floor `1e-5`), not bit-identically.
//!
//! Only `std` is used; `unsafe` is forbidden at the crate root.

pub mod bfloat16;
pub mod binary;
pub mod dispatch;
pub mod float16;
pub mod float32;
pub mod int;
pub mod int4;
pub mod scalar;

pub use dispatch::{active_backend, detect_backend, CpuBackend};

use std::fmt;

/// Absolute tolerance used when comparing distances for boundary inclusion
/// (e.g. radius search `d <= radius + TOLERANCE`).
pub const TOLERANCE: f32 = 1e-5;

/// Errors produced by math kernels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MathError {
    /// Slice / vector dimensionalities disagreed.
    ///
    /// Kernel entry points validate before touching lanes so mismatched
    /// inputs fail fast instead of reading out of bounds.
    DimensionMismatch {
        /// Expected lane count.
        expected: usize,
        /// Actual lane count.
        actual: usize,
    },
    /// A length / count argument was invalid.
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
}

impl fmt::Display for MathError {
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

impl std::error::Error for MathError {}

/// Stable machine-readable error codes (same `VECTOR_*` vocabulary as
/// [`crate::VectorError::code`]).
impl MathError {
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
}

/// Local result alias.
pub type Result<T> = std::result::Result<T, MathError>;

/// Checked dimension equality for a pair of slices.
///
/// Lengths are plain `usize`; no narrowing casts are performed anywhere on
/// this path.
pub fn check_dims_equal(a_len: usize, b_len: usize) -> Result<()> {
    if a_len == b_len {
        Ok(())
    } else {
        Err(MathError::DimensionMismatch {
            expected: a_len,
            actual: b_len,
        })
    }
}

/// `ceil(num_bits / 8)` with checked arithmetic.
pub fn bytes_for_bits(num_bits: usize) -> Result<usize> {
    num_bits
        .checked_add(7)
        .map(|n| n / 8)
        .ok_or(MathError::Overflow {
            what: "bits to bytes",
        })
}
