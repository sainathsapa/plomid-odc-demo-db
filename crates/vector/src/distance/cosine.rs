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
//! Cosine distance / similarity with `NULL` semantics.
//!
//! * `None` input (SQL `NULL`) → `Ok(None)`.
//! * Zero-norm input → `Ok(None)` (undefined; caller maps to `NULL`).
//! * Dimension mismatch → `Err(DistanceError)`.

use super::{either_null, Result};

/// Cosine distance `1 - similarity` over dense `f32` vectors
/// (tier-dispatched inner products, scalar-shape normalization).
pub fn cosine_distance(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    let (a, b) = (a.unwrap_or(&[]), b.unwrap_or(&[]));
    let (_, value) = crate::math::dispatch::cosine_distance(a, b);
    Ok(value?)
}

/// Cosine similarity over dense `f32` vectors (tier-dispatched).
pub fn cosine_similarity(a: Option<&[f32]>, b: Option<&[f32]>) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    let (a, b) = (a.unwrap_or(&[]), b.unwrap_or(&[]));
    let (_, value) = crate::math::dispatch::cosine_similarity(a, b);
    Ok(value?)
}

/// Cosine distance over sparse `(dimension, value)` slices.
///
/// Inputs must be sorted by dimension ascending with unique dimensions; the
/// merge is a single deterministic two-pointer pass. Empty slices are zero
/// vectors → `Ok(None)`.
pub fn cosine_distance_sparse(a: &[(u32, f32)], b: &[(u32, f32)]) -> Result<Option<f32>> {
    debug_assert_sorted(a);
    debug_assert_sorted(b);
    let (dot, na2, nb2) = sparse_parts(a, b);
    if na2 == 0.0 || nb2 == 0.0 {
        return Ok(None);
    }
    let sim = (dot / (na2.sqrt() * nb2.sqrt())).clamp(-1.0, 1.0);
    Ok(Some((1.0 - sim) as f32))
}

pub(crate) fn sparse_parts(a: &[(u32, f32)], b: &[(u32, f32)]) -> (f64, f64, f64) {
    let mut dot: f64 = 0.0;
    let mut na2: f64 = 0.0;
    let mut nb2: f64 = 0.0;
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => {
                let x = f64::from(a[i].1);
                na2 += x * x;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                let y = f64::from(b[j].1);
                nb2 += y * y;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                let x = f64::from(a[i].1);
                let y = f64::from(b[j].1);
                dot += x * y;
                na2 += x * x;
                nb2 += y * y;
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        let x = f64::from(a[i].1);
        na2 += x * x;
        i += 1;
    }
    while j < b.len() {
        let y = f64::from(b[j].1);
        nb2 += y * y;
        j += 1;
    }
    (dot, na2, nb2)
}

pub(crate) fn debug_assert_sorted(v: &[(u32, f32)]) {
    debug_assert!(
        v.windows(2).all(|w| w[0].0 < w[1].0),
        "sparse vector must be sorted by dimension with unique dimensions"
    );
}
