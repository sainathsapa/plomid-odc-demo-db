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
//! Checked validation primitives: dimensions, payload sizes, finiteness.
//!
//! Every size computation uses checked arithmetic and returns
//! [`VectorError::ResourceLimit`] instead of wrapping or panicking.

use crate::error::{Result, VectorError};
use crate::r#type::VectorElementType;

/// Minimum legal dimension / bit-count.
pub const MIN_DIMENSION: u32 = 1;
/// Maximum legal dimension / bit-count.
pub const MAX_DIMENSION: u32 = 65_536;
/// Hard cap for any single decoded payload (64 MiB).
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Validate `1 <= dim <= 65536`.
pub fn validate_dimension(dim: u32) -> Result<()> {
    if !(MIN_DIMENSION..=MAX_DIMENSION).contains(&dim) {
        return Err(VectorError::InvalidDimension(format!(
            "dimension {dim} out of range 1..=65536"
        )));
    }
    Ok(())
}

/// Validate a multi-vector constituent cap.
pub fn validate_max_vectors(n: u32) -> Result<()> {
    if !(1..=MAX_DIMENSION).contains(&n) {
        return Err(VectorError::InvalidDimension(format!(
            "max_vectors {n} out of range 1..=65536"
        )));
    }
    Ok(())
}

/// Validate sparse `nnz` against its dimension.
pub fn validate_nnz(nnz: u32, dimension: u32) -> Result<()> {
    validate_dimension(dimension)?;
    if nnz > dimension {
        return Err(VectorError::InvalidDimension(format!(
            "nnz {nnz} exceeds dimension {dimension}"
        )));
    }
    Ok(())
}

/// Dense row payload bytes for `(element, dim)`, overflow-checked.
pub fn dense_payload_bytes(element: VectorElementType, dim: u32) -> Result<usize> {
    element.payload_bytes(dim)
}

/// Binary payload bytes for `bits`: `ceil(bits / 8)`.
pub fn binary_payload_bytes(bits: u32) -> Result<usize> {
    validate_dimension(bits)?;
    Ok(bits.saturating_add(7) as usize / 8)
}

/// Sparse payload bytes for `nnz` float entries: `nnz * (4 index + 4 value)`.
pub fn sparse_payload_bytes(nnz: u32, dimension: u32) -> Result<usize> {
    validate_nnz(nnz, dimension)?;
    (nnz as usize)
        .checked_mul(8)
        .ok_or_else(|| VectorError::ResourceLimit("sparse payload overflow".into()))
}

/// Multi-vector payload upper bound (`max_vectors * per-vector`), checked.
pub fn multi_payload_bytes(
    element: VectorElementType,
    dimension: u32,
    max_vectors: u32,
) -> Result<usize> {
    validate_dimension(dimension)?;
    validate_max_vectors(max_vectors)?;
    let per = element.payload_bytes(dimension)?;
    per.checked_mul(max_vectors as usize)
        .ok_or_else(|| VectorError::ResourceLimit("multi-vector payload overflow".into()))
}

/// Reject `NaN`/`±Inf` with context.
pub fn check_finite(value: f32, context: &str) -> Result<()> {
    if !value.is_finite() {
        return Err(VectorError::NonFiniteValue(context.to_string()));
    }
    Ok(())
}

/// Reject any non-finite lane.
pub fn check_finite_slice(values: &[f32]) -> Result<()> {
    for (i, v) in values.iter().enumerate() {
        if !v.is_finite() {
            return Err(VectorError::NonFiniteValue(format!(
                "coordinate {i} is non-finite"
            )));
        }
    }
    Ok(())
}

/// Validate dense lanes against a declared integer element type.
///
/// Float elements accept any finite `f32` working-precision lane
/// (quantization to FP16/BF16 storage happens at the codec, not here).
/// Integer elements require exact integral lanes inside the element range:
/// `INT8: -128..=127`, `UINT8: 0..=255`, `INT4: -8..=7`. Out-of-range or
/// fractional lanes fail with a descriptive error naming the coordinate.
pub fn check_element_lanes(element: VectorElementType, lanes: &[f32]) -> Result<()> {
    let (lo, hi) = match element {
        VectorElementType::Float32 | VectorElementType::Float16 | VectorElementType::BFloat16 => {
            return check_finite_slice(lanes)
        }
        VectorElementType::Int8 => (-128.0f32, 127.0),
        VectorElementType::Uint8 => (0.0, 255.0),
        VectorElementType::Int4 => (-8.0, 7.0),
        VectorElementType::Binary => {
            return Err(VectorError::UnsupportedCombination(
                "BINARY element has no dense lanes".into(),
            ));
        }
    };
    for (i, v) in lanes.iter().enumerate() {
        if !v.is_finite() || v.fract() != 0.0 || *v < lo || *v > hi {
            return Err(VectorError::InvalidLiteral(format!(
                "coordinate {i} value {v} out of range for {}",
                element.sql_name()
            )));
        }
    }
    Ok(())
}

/// Enforce a byte budget before allocating.
pub fn check_budget(bytes: usize) -> Result<()> {
    if bytes > MAX_PAYLOAD_BYTES {
        return Err(VectorError::ResourceLimit(format!(
            "payload {bytes} bytes exceeds {MAX_PAYLOAD_BYTES} byte cap"
        )));
    }
    Ok(())
}
