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
//! Euclidean (L2) distance with `NULL` semantics.
//!
//! * `None` input (SQL `NULL`) → `Ok(None)`.
//! * Dimension mismatch → `Err(DistanceError)`.

use super::{either_null, Result};

/// Squared Euclidean distance over dense `f32` vectors (tier-dispatched).
pub fn l2_distance_squared(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    let (a, b) = (a.unwrap_or(&[]), b.unwrap_or(&[]));
    let (_, value) = crate::math::dispatch::l2_squared(a, b);
    Ok(Some(value?))
}

/// Euclidean distance over dense `f32` vectors (tier-dispatched).
pub fn l2_distance(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    let (a, b) = (a.unwrap_or(&[]), b.unwrap_or(&[]));
    let (_, value) = crate::math::dispatch::l2_distance(a, b);
    Ok(Some(value?))
}

/// Squared Euclidean distance over sparse `(dimension, value)` slices.
///
/// Inputs must be sorted by dimension ascending with unique dimensions.
/// Missing dimensions read as `0.0`.
pub fn l2_distance_squared_sparse(a: &[(u32, f32)], b: &[(u32, f32)]) -> f32 {
    super::cosine::debug_assert_sorted(a);
    super::cosine::debug_assert_sorted(b);
    let mut acc: f64 = 0.0;
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => {
                let x = f64::from(a[i].1);
                acc += x * x;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                let y = f64::from(b[j].1);
                acc += y * y;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                let d = f64::from(a[i].1) - f64::from(b[j].1);
                acc += d * d;
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        let x = f64::from(a[i].1);
        acc += x * x;
        i += 1;
    }
    while j < b.len() {
        let y = f64::from(b[j].1);
        acc += y * y;
        j += 1;
    }
    acc as f32
}

/// Euclidean distance over sparse `(dimension, value)` slices.
pub fn l2_distance_sparse(a: &[(u32, f32)], b: &[(u32, f32)]) -> f32 {
    (f64::from(l2_distance_squared_sparse(a, b))).sqrt() as f32
}
