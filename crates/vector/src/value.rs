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
//! Canonical in-memory vector values.
//!
//! All floating payloads are stored as `f32` lanes regardless of the logical
//! [`VectorElementType`]; narrower types are a storage concern. Every
//! constructor validates finiteness, dimensions, ordering, and padding.

use crate::error::{Result, VectorError};
use crate::r#type::{VectorElementType, VectorKind};
use crate::validation;

/// Contiguous dense vector.
#[derive(Debug, Clone, PartialEq)]
pub struct DenseVectorValue {
    /// Logical element type.
    pub element: VectorElementType,
    /// Coordinate count.
    pub dimension: u32,
    /// `dimension` finite lanes.
    pub values_f32: Vec<f32>,
}

impl DenseVectorValue {
    /// Build from `f32` lanes (element tracks logical type).
    pub fn from_f32(element: VectorElementType, values: Vec<f32>) -> Result<Self> {
        if values.is_empty() {
            return Err(VectorError::InvalidLiteral(
                "dense vector must not be empty".into(),
            ));
        }
        if values.len() > validation::MAX_DIMENSION as usize {
            return Err(VectorError::InvalidDimension(format!(
                "dense arity {} exceeds 65536",
                values.len()
            )));
        }
        validation::check_finite_slice(&values)?;
        let dimension = values.len() as u32;
        Ok(Self {
            element,
            dimension,
            values_f32: values,
        })
    }

    /// Borrow lanes.
    #[must_use]
    pub fn to_f32(&self) -> &[f32] {
        &self.values_f32
    }
}

/// Canonical sparse vector: strictly increasing indices, no zeros, no dups.
#[derive(Debug, Clone, PartialEq)]
pub struct SparseVectorValue {
    /// Full dimension (`index < dimension`).
    pub dimension: u32,
    /// Sorted strictly increasing coordinates.
    pub indices: Vec<u32>,
    /// Finite values parallel to `indices`.
    pub values: Vec<f32>,
}

impl SparseVectorValue {
    /// Canonicalize `(indices, values)`: validates bounds/finiteness, drops
    /// exact `0.0` entries, sorts by index, and rejects duplicate indices.
    pub fn new(dimension: u32, indices: Vec<u32>, values: Vec<f32>) -> Result<Self> {
        validation::validate_dimension(dimension)?;
        if indices.len() != values.len() {
            return Err(VectorError::InvalidLiteral(format!(
                "sparse arity mismatch: {} indices vs {} values",
                indices.len(),
                values.len()
            )));
        }
        let mut pairs: Vec<(u32, f32)> = Vec::with_capacity(indices.len());
        for (i, v) in indices.into_iter().zip(values) {
            if i >= dimension {
                return Err(VectorError::InvalidLiteral(format!(
                    "sparse index {i} out of bounds for dimension {dimension}"
                )));
            }
            if !v.is_finite() {
                return Err(VectorError::NonFiniteValue(format!("sparse value at {i}")));
            }
            if v == 0.0 {
                continue; // canonical form omits default entries
            }
            pairs.push((i, v));
        }
        pairs.sort_by_key(|(i, _)| *i);
        let mut out_idx = Vec::with_capacity(pairs.len());
        let mut out_val = Vec::with_capacity(pairs.len());
        for (i, v) in pairs {
            if out_idx.last() == Some(&i) {
                return Err(VectorError::InvalidLiteral(format!(
                    "duplicate sparse index {i}"
                )));
            }
            out_idx.push(i);
            out_val.push(v);
        }
        validation::validate_nnz(out_idx.len() as u32, dimension)?;
        Ok(Self {
            dimension,
            indices: out_idx,
            values: out_val,
        })
    }

    /// Number of stored entries.
    #[must_use]
    pub fn nnz(&self) -> u32 {
        self.indices.len() as u32
    }
}

/// Up to `max_vectors` dense constituents sharing one type and dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiVectorValue {
    /// Logical element type.
    pub element: VectorElementType,
    /// Per-constituent dimension.
    pub dimension: u32,
    /// Declared cap.
    pub max_vectors: u32,
    /// `0..=max_vectors` finite dense constituents.
    pub vectors: Vec<Vec<f32>>,
}

impl MultiVectorValue {
    /// Build + validate.
    pub fn new(
        element: VectorElementType,
        dimension: u32,
        max_vectors: u32,
        vectors: Vec<Vec<f32>>,
    ) -> Result<Self> {
        validation::validate_dimension(dimension)?;
        validation::validate_max_vectors(max_vectors)?;
        if vectors.len() > max_vectors as usize {
            return Err(VectorError::InvalidLiteral(format!(
                "vector count {} exceeds max_vectors {max_vectors}",
                vectors.len()
            )));
        }
        if element == VectorElementType::Binary {
            return Err(VectorError::UnsupportedCombination(
                "BINARY multi-vectors are not supported".into(),
            ));
        }
        for (k, v) in vectors.iter().enumerate() {
            if v.len() as u32 != dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: dimension,
                    got: v.len() as u32,
                });
            }
            validation::check_finite_slice(v).map_err(|_| {
                VectorError::NonFiniteValue(format!("constituent {k} has non-finite value"))
            })?;
        }
        Ok(Self {
            element,
            dimension,
            max_vectors,
            vectors,
        })
    }

    /// Constituent count.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.vectors.len() as u32
    }
}

/// Packed bits, MSB-first within each byte; trailing padding bits are zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryVectorValue {
    /// Bit count (not bytes).
    pub dimension_bits: u32,
    /// `ceil(bits / 8)` bytes with zeroed padding.
    pub bytes: Vec<u8>,
}

impl BinaryVectorValue {
    /// Build + mask padding bits to zero.
    pub fn new(dimension_bits: u32, mut bytes: Vec<u8>) -> Result<Self> {
        validation::validate_dimension(dimension_bits)?;
        let want = validation::binary_payload_bytes(dimension_bits)?;
        if bytes.len() != want {
            return Err(VectorError::InvalidLiteral(format!(
                "binary payload {} bytes, expected {want} for {dimension_bits} bits",
                bytes.len()
            )));
        }
        mask_padding(dimension_bits, &mut bytes);
        Ok(Self {
            dimension_bits,
            bytes,
        })
    }

    /// Bit accessor, index 0 = first literal bit.
    #[must_use]
    pub fn get_bit(&self, index: u32) -> bool {
        let b = self.bytes[(index / 8) as usize];
        (b >> (7 - (index % 8))) & 1 == 1
    }
}

fn mask_padding(bits: u32, bytes: &mut [u8]) {
    let rem = (bits % 8) as u8;
    if rem == 0 || bytes.is_empty() {
        return;
    }
    if let Some(last) = bytes.last_mut() {
        // Keep the top `rem` bits (the real data), clear padding bits.
        let mask: u8 = !(0xFF >> rem);
        *last &= mask;
    }
}

/// Any vector value.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorValue {
    /// Dense payload.
    Dense(DenseVectorValue),
    /// Sparse payload.
    Sparse(SparseVectorValue),
    /// Multi payload.
    Multi(MultiVectorValue),
    /// Binary payload.
    Binary(BinaryVectorValue),
}

impl VectorValue {
    /// Storage kind.
    #[must_use]
    pub const fn kind(&self) -> VectorKind {
        match self {
            Self::Dense(_) => VectorKind::Dense,
            Self::Sparse(_) => VectorKind::Sparse,
            Self::Multi(_) => VectorKind::Multi,
            Self::Binary(_) => VectorKind::Binary,
        }
    }

    /// Logical element type.
    #[must_use]
    pub const fn element(&self) -> VectorElementType {
        match self {
            Self::Dense(v) => v.element,
            Self::Sparse(_) => VectorElementType::Float32,
            Self::Multi(v) => v.element,
            Self::Binary(_) => VectorElementType::Binary,
        }
    }

    /// Dimension (bits for binary).
    #[must_use]
    pub const fn dimension(&self) -> u32 {
        match self {
            Self::Dense(v) => v.dimension,
            Self::Sparse(v) => v.dimension,
            Self::Multi(v) => v.dimension,
            Self::Binary(v) => v.dimension_bits,
        }
    }

    /// True when every lane/bit is zero (cosine-undefined).
    #[must_use]
    pub fn is_zero_norm(&self) -> bool {
        match self {
            Self::Dense(v) => v.values_f32.iter().all(|x| *x == 0.0),
            Self::Sparse(v) => v.values.is_empty() || v.values.iter().all(|x| *x == 0.0),
            Self::Multi(v) => {
                v.vectors.is_empty() || v.vectors.iter().all(|row| row.iter().all(|x| *x == 0.0))
            }
            Self::Binary(v) => v.bytes.iter().all(|b| *b == 0),
        }
    }

    /// Materialize one `f32` row per constituent (binary maps to 0.0/1.0).
    /// Sparse materialization is bounded by the 65536-dim cap.
    pub fn to_f32_matrix(&self) -> Result<Vec<Vec<f32>>> {
        match self {
            Self::Dense(v) => Ok(vec![v.values_f32.clone()]),
            Self::Sparse(s) => {
                let mut row = vec![0.0f32; s.dimension as usize];
                for (i, v) in s.indices.iter().zip(s.values.iter()) {
                    let idx = *i as usize;
                    if idx >= row.len() {
                        return Err(VectorError::IndexCorrupted(format!(
                            "sparse index {i} out of bounds"
                        )));
                    }
                    row[idx] = *v;
                }
                Ok(vec![row])
            }
            Self::Multi(m) => Ok(m.vectors.clone()),
            Self::Binary(b) => {
                let mut row = Vec::with_capacity(b.dimension_bits as usize);
                for i in 0..b.dimension_bits {
                    row.push(i32::from(b.get_bit(i)) as f32);
                }
                Ok(vec![row])
            }
        }
    }
}
