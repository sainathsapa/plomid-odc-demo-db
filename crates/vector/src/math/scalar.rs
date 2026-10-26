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
//! `f32` scalar kernels with `f64` accumulation.
//!
//! Reference implementations used directly and via [`crate::math::dispatch`]
//! (every backend tier delegates here, so tiers are bit-identical).
//! Accumulation order is strict left-to-right; results are deterministic for
//! a given input order.

use super::{check_dims_equal, Result};

/// Inner product with `f64` accumulation, rounded once to `f32`.
///
/// Empty (but equal-length) inputs yield `0.0`.
pub fn dot_f32(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        acc += f64::from(a[i]) * f64::from(b[i]);
    }
    Ok(acc as f32)
}

/// Squared Euclidean norm with `f64` accumulation.
#[must_use]
pub fn norm_squared_f32(a: &[f32]) -> f32 {
    let mut acc: f64 = 0.0;
    for &v in a {
        let v = f64::from(v);
        acc += v * v;
    }
    acc as f32
}

/// Squared Euclidean distance with `f64` accumulation.
pub fn l2_squared_f32(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for (&x, &y) in a.iter().zip(b.iter()) {
        let d = f64::from(x) - f64::from(y);
        acc += d * d;
    }
    Ok(acc as f32)
}

/// Euclidean distance (`sqrt` applied in `f64`, rounded once to `f32`).
pub fn l2_distance_f32(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        let d = f64::from(a[i]) - f64::from(b[i]);
        acc += d * d;
    }
    Ok(acc.sqrt() as f32)
}

/// Cosine similarity in `[-1, 1]` (clamped), or `None` when either input has
/// zero norm (undefined — the caller maps this to SQL `NULL`).
pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    let mut dot: f64 = 0.0;
    let mut na: f64 = 0.0;
    let mut nb: f64 = 0.0;
    for i in 0..a.len() {
        let x = f64::from(a[i]);
        let y = f64::from(b[i]);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return Ok(None);
    }
    let sim = dot / (na.sqrt() * nb.sqrt());
    Ok(Some(sim.clamp(-1.0, 1.0) as f32))
}

/// Cosine distance `1 - similarity`, or `None` on zero norm (see
/// [`cosine_similarity_f32`]).
pub fn cosine_distance_f32(a: &[f32], b: &[f32]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    let mut dot: f64 = 0.0;
    let mut na: f64 = 0.0;
    let mut nb: f64 = 0.0;
    for i in 0..a.len() {
        let x = f64::from(a[i]);
        let y = f64::from(b[i]);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return Ok(None);
    }
    let sim = dot / (na.sqrt() * nb.sqrt());
    let clamped = sim.clamp(-1.0, 1.0);
    Ok(Some((1.0 - clamped) as f32))
}
