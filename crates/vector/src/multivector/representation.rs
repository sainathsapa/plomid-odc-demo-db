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
//! Multi-vector representation: one parent row owns many same-dim constituents.

use crate::{Result, VectorError};

/// A parent row's constituent vectors (all share one dimension).
#[derive(Clone, Debug, PartialEq)]
pub struct MultiVector {
    /// Dim.
    pub dim: usize,
    /// Vectors.
    pub vectors: Vec<Vec<f32>>,
}

impl MultiVector {
    /// new: see implementation for contract.
    pub fn new(vectors: Vec<Vec<f32>>) -> Result<Self> {
        if vectors.is_empty() {
            return Err(VectorError::IndexParameterInvalid(
                "multi needs >= 1 vector".to_string(),
            ));
        }
        let dim = vectors[0].len();
        if dim == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "dim must be > 0".to_string(),
            ));
        }
        for v in &vectors {
            if v.len() != dim {
                return Err(VectorError::DimensionMismatch {
                    expected: dim as u32,
                    got: v.len() as u32,
                });
            }
            for x in v {
                if !x.is_finite() {
                    return Err(VectorError::IndexParameterInvalid(
                        "non-finite value".to_string(),
                    ));
                }
            }
        }
        Ok(Self { dim, vectors })
    }

    /// len: see implementation for contract.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// is_empty: see implementation for contract.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// validate: see implementation for contract.
    pub fn validate(&self) -> Result<()> {
        if self.vectors.is_empty() {
            return Err(VectorError::IndexCorrupted("empty multi".to_string()));
        }
        for v in &self.vectors {
            if v.len() != self.dim {
                return Err(VectorError::IndexCorrupted(
                    "multi dim mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }
}
