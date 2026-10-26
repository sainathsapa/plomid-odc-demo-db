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
//! Vector error hierarchy for `plomid-vector`.
//!
//! All fallible vector operations return [`Result<T>`]. Errors carry a
//! human-readable context string plus typed fields where comparison in
//! tests or planner logic is required. No error variant leaks internal
//! layout details.

use std::fmt;

/// Errors produced by the vector subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorError {
    /// Dimension out of the `1..=65536` range or otherwise malformed.
    InvalidDimension(String),
    /// Unknown or unsupported element type name.
    InvalidElementType(String),
    /// Malformed SQL/text literal.
    InvalidLiteral(String),
    /// A non-finite (`NaN`, `Inf`) value was supplied where finite is required.
    NonFiniteValue(String),
    /// A required argument was missing or null.
    NullArgument(String),
    /// Cosine (or cosine-derived) computation on a zero-norm vector.
    ZeroNorm(String),
    /// Operands have incompatible vector types.
    TypeMismatch(String),
    /// Valid types combined in an unsupported way (e.g. BINARY sparse).
    UnsupportedCombination(String),
    /// Operation not supported by this index kind.
    IndexUnsupported(String),
    /// Index build/search parameter out of range.
    IndexParameterInvalid(String),
    /// Operation illegal in the current [`crate::admin::IndexState`].
    IndexStateInvalid(String),
    /// Persisted index bytes failed structural validation.
    IndexCorrupted(String),
    /// Index is validly marked as needing a rebuild (not corruption).
    RebuildRequired(String),
    /// Query/operand dimension does not match the indexed/declared dimension.
    DimensionMismatch {
        /// Expected dimension.
        expected: u32,
        /// Supplied dimension.
        got: u32,
    },
    /// Query metric does not match the index metric.
    MetricMismatch(String),
    /// Allocation or budget (payload bytes, candidate counts, …) exceeded.
    ResourceLimit(String),
    /// Cooperative cancellation requested.
    Cancelled(String),
    /// Checksum, framing, or payload corruption in standalone artifacts.
    Corruption(String),
}

/// Convenience alias used across the crate.
pub type Result<T> = std::result::Result<T, VectorError>;

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDimension(s) => write!(f, "invalid dimension: {s}"),
            Self::InvalidElementType(s) => write!(f, "invalid element type: {s}"),
            Self::InvalidLiteral(s) => write!(f, "invalid vector literal: {s}"),
            Self::NonFiniteValue(s) => write!(f, "non-finite value rejected: {s}"),
            Self::NullArgument(s) => write!(f, "null argument: {s}"),
            Self::ZeroNorm(s) => write!(f, "zero norm: {s}"),
            Self::TypeMismatch(s) => write!(f, "type mismatch: {s}"),
            Self::UnsupportedCombination(s) => {
                write!(f, "unsupported combination: {s}")
            }
            Self::IndexUnsupported(s) => write!(f, "index unsupported: {s}"),
            Self::IndexParameterInvalid(s) => {
                write!(f, "invalid index parameter: {s}")
            }
            Self::IndexStateInvalid(s) => write!(f, "invalid index state: {s}"),
            Self::IndexCorrupted(s) => write!(f, "index corrupted: {s}"),
            Self::RebuildRequired(s) => write!(f, "rebuild required: {s}"),
            Self::DimensionMismatch { expected, got } => {
                write!(f, "dimension mismatch: expected {expected}, got {got}")
            }
            Self::MetricMismatch(s) => write!(f, "metric mismatch: {s}"),
            Self::ResourceLimit(s) => write!(f, "resource limit exceeded: {s}"),
            Self::Cancelled(s) => write!(f, "cancelled: {s}"),
            Self::Corruption(s) => write!(f, "corruption: {s}"),
        }
    }
}

impl std::error::Error for VectorError {}

/// Stable machine-readable error codes (`VECTOR_*`).
///
/// Codes are part of the compatibility contract: messages may gain detail,
/// but a code never changes meaning. SQL surfaces prefix these codes in
/// user-facing errors (see executor `scalar`/`ddl` vector paths).
impl VectorError {
    /// Returns the stable `VECTOR_*` code for this error.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidDimension(_) => "VECTOR_INVALID_DIMENSION",
            Self::InvalidElementType(_) => "VECTOR_INVALID_ELEMENT_TYPE",
            Self::InvalidLiteral(_) => "VECTOR_INVALID_LITERAL",
            Self::NonFiniteValue(_) => "VECTOR_NONFINITE_VALUE",
            Self::NullArgument(_) => "VECTOR_NULL_ARGUMENT",
            Self::ZeroNorm(_) => "VECTOR_ZERO_NORM",
            Self::TypeMismatch(_) => "VECTOR_TYPE_MISMATCH",
            Self::UnsupportedCombination(_) => "VECTOR_UNSUPPORTED_COMBINATION",
            Self::IndexUnsupported(_) => "VECTOR_INDEX_UNSUPPORTED",
            Self::IndexParameterInvalid(_) => "VECTOR_INDEX_PARAMETER_INVALID",
            Self::IndexStateInvalid(_) => "VECTOR_INDEX_STATE_INVALID",
            Self::IndexCorrupted(_) => "VECTOR_INDEX_CORRUPTED",
            Self::RebuildRequired(_) => "VECTOR_INDEX_REBUILD_REQUIRED",
            Self::DimensionMismatch { .. } => "VECTOR_INDEX_DIMENSION_MISMATCH",
            Self::MetricMismatch(_) => "VECTOR_INDEX_METRIC_MISMATCH",
            Self::ResourceLimit(_) => "VECTOR_RESOURCE_LIMIT",
            Self::Cancelled(_) => "VECTOR_QUERY_CANCELLED",
            Self::Corruption(_) => "VECTOR_CORRUPTION",
        }
    }
}
