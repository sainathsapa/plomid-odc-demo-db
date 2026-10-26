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
//! Inner-product distances with `NULL` semantics.
//!
//! * [`dot`] is the raw inner product (larger means closer — *not* a metric).
//! * [`neg_dot`] is the negative inner product, i.e. PostgreSQL pgvector's
//!   `<#>` operator distance: smaller means closer, so it sorts and indexes
//!   exactly like the other distances in this module.
//! * `None` input (SQL `NULL`) → `Ok(None)`; dimension mismatch → `Err`.

use super::{either_null, Result};

/// Raw inner product over dense `f32` vectors (larger = closer).
///
/// Runs on the active SIMD tier via dispatch (tolerance contract); sparse
/// kernels stay scalar merges below.
pub fn dot(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    let (a, b) = (a.unwrap_or(&[]), b.unwrap_or(&[]));
    let (_, value) = crate::math::dispatch::dot(a, b);
    Ok(Some(value?))
}

/// Negative inner product (`<#>` semantics: smaller = closer).
pub fn neg_dot(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    Ok(dot(a, b)?.map(|v| -v))
}

/// Raw inner product over sparse `(dimension, value)` slices.
///
/// Inputs must be sorted by dimension ascending with unique dimensions.
pub fn dot_sparse(a: &[(u32, f32)], b: &[(u32, f32)]) -> f32 {
    super::cosine::debug_assert_sorted(a);
    super::cosine::debug_assert_sorted(b);
    let mut acc: f64 = 0.0;
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                acc += f64::from(a[i].1) * f64::from(b[j].1);
                i += 1;
                j += 1;
            }
        }
    }
    acc as f32
}

/// Negative sparse inner product (smaller = closer).
pub fn neg_dot_sparse(a: &[(u32, f32)], b: &[(u32, f32)]) -> f32 {
    -dot_sparse(a, b)
}
