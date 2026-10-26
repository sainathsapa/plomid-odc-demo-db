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
#![forbid(unsafe_code)]
//! Sparse vector representation (sorted `indices`, parallel `values`).

use crate::r#type::VectorKind;
use crate::value::SparseVectorValue;
use crate::{Result, VectorError, VectorValue};

/// Sorted sparse vector: `indices` strictly increasing, `values` finite.
#[derive(Clone, Debug, PartialEq)]
pub struct SparseVector {
    /// Dim.
    pub dim: u32,
    /// Indices.
    pub indices: Vec<u32>,
    /// Values.
    pub values: Vec<f32>,
}

impl SparseVector {
    /// new: see implementation for contract.
    pub fn new(dim: u32, mut pairs: Vec<(u32, f32)>) -> Result<Self> {
        if dim == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "dim must be > 0".to_string(),
            ));
        }
        pairs.sort_by_key(|(i, _)| *i);
        let mut indices = Vec::with_capacity(pairs.len());
        let mut values = Vec::with_capacity(pairs.len());
        for (i, v) in pairs {
            if i >= dim {
                return Err(VectorError::IndexParameterInvalid(format!(
                    "index {i} out of bounds for dim {dim}"
                )));
            }
            if !v.is_finite() {
                return Err(VectorError::IndexParameterInvalid(
                    "non-finite value".to_string(),
                ));
            }
            if v == 0.0 {
                continue; // canonical: drop explicit zeros
            }
            if indices.last() == Some(&i) {
                return Err(VectorError::IndexParameterInvalid(format!(
                    "duplicate index {i}"
                )));
            }
            indices.push(i);
            values.push(v);
        }
        Ok(Self {
            dim,
            indices,
            values,
        })
    }

    /// nnz: see implementation for contract.
    pub fn nnz(&self) -> usize {
        self.indices.len()
    }

    /// norm_squared: see implementation for contract.
    pub fn norm_squared(&self) -> f32 {
        self.values.iter().map(|v| v * v).sum()
    }

    /// to_value: see implementation for contract.
    pub fn to_value(&self) -> Result<VectorValue> {
        Ok(VectorValue::Sparse(SparseVectorValue::new(
            self.dim,
            self.indices.clone(),
            self.values.clone(),
        )?))
    }

    /// from_value: see implementation for contract.
    pub fn from_value(dim: u32, value: &VectorValue) -> Result<Self> {
        match value {
            VectorValue::Sparse(s) => {
                let pairs = s
                    .indices
                    .iter()
                    .copied()
                    .zip(s.values.iter().copied())
                    .collect();
                let mut v = Self::new(dim, pairs)?;
                // Preserve the source dimension view when compatible.
                if s.dimension == dim {
                    v.dim = s.dimension;
                }
                Ok(v)
            }
            other => Err(VectorError::IndexParameterInvalid(format!(
                "expected sparse value, got {:?}",
                other.kind()
            ))),
        }
    }

    /// validate: see implementation for contract.
    pub fn validate(&self) -> Result<()> {
        if self.indices.len() != self.values.len() {
            return Err(VectorError::IndexCorrupted(
                "indices/values length".to_string(),
            ));
        }
        let mut prev: Option<u32> = None;
        for (i, v) in self.indices.iter().zip(self.values.iter()) {
            if *i >= self.dim {
                return Err(VectorError::IndexCorrupted(
                    "index out of bounds".to_string(),
                ));
            }
            if !v.is_finite() {
                return Err(VectorError::IndexCorrupted("non-finite value".to_string()));
            }
            if let Some(p) = prev {
                if *i <= p {
                    return Err(VectorError::IndexCorrupted(
                        "indices not sorted".to_string(),
                    ));
                }
            }
            prev = Some(*i);
        }
        let _ = VectorKind::Sparse;
        Ok(())
    }
}
