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
//! Two-pointer sparse distances over sorted `(index, value)` pairs.

use super::representation::SparseVector;

/// Inner product via two-pointer merge.
pub fn sparse_dot(a: &SparseVector, b: &SparseVector) -> f32 {
    let mut acc = 0.0f32;
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.indices.len() && j < b.indices.len() {
        let ai = a.indices[i];
        let bi = b.indices[j];
        if ai == bi {
            acc += a.values[i] * b.values[j];
            i += 1;
            j += 1;
        } else if ai < bi {
            i += 1;
        } else {
            j += 1;
        }
    }
    acc
}

/// Squared L2 distance via two-pointer merge.
pub fn sparse_l2_squared(a: &SparseVector, b: &SparseVector) -> f32 {
    let mut acc = 0.0f32;
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.indices.len() || j < b.indices.len() {
        if j >= b.indices.len() || (i < a.indices.len() && a.indices[i] < b.indices[j]) {
            acc += a.values[i] * a.values[i];
            i += 1;
        } else if i >= a.indices.len() || b.indices[j] < a.indices[i] {
            acc += b.values[j] * b.values[j];
            j += 1;
        } else {
            let d = a.values[i] - b.values[j];
            acc += d * d;
            i += 1;
            j += 1;
        }
    }
    acc
}

/// Cosine distance in `[0, 2]`; empty vectors map to `1.0`.
pub fn sparse_cosine_distance(a: &SparseVector, b: &SparseVector) -> f32 {
    let na = a.norm_squared().sqrt();
    let nb = b.norm_squared().sqrt();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    let cos = (sparse_dot(a, b) / (na * nb)).clamp(-1.0, 1.0);
    1.0 - cos
}
