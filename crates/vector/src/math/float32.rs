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
//! `f32` blocked kernels plus normalization helpers.
//!
//! The blocked dot / L2 kernels use four `f64` accumulators over a fixed
//! stride, summed in a fixed order. They are deterministic per function, but
//! may differ from [`crate::math::scalar`] in the last ulp because the
//! summation order differs; prefer `scalar` when bit-identity with the
//! reference path matters, and these when throughput matters.

use super::{check_dims_equal, Result};

/// Fixed unroll width for the blocked kernels.
const BLOCK: usize = 4;

/// Blocked inner product (four `f64` accumulators, fixed summation order).
pub fn dot_blocked(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc = [0.0f64; BLOCK];
    let head = a.len() & !(BLOCK - 1);
    for i in (0..head).step_by(BLOCK) {
        acc[0] += f64::from(a[i]) * f64::from(b[i]);
        acc[1] += f64::from(a[i + 1]) * f64::from(b[i + 1]);
        acc[2] += f64::from(a[i + 2]) * f64::from(b[i + 2]);
        acc[3] += f64::from(a[i + 3]) * f64::from(b[i + 3]);
    }
    let mut tail: f64 = 0.0;
    for i in head..a.len() {
        tail += f64::from(a[i]) * f64::from(b[i]);
    }
    Ok((acc[0] + acc[1] + acc[2] + acc[3] + tail) as f32)
}

/// Blocked squared Euclidean distance.
pub fn l2_squared_blocked(a: &[f32], b: &[f32]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc = [0.0f64; BLOCK];
    let head = a.len() & !(BLOCK - 1);
    for i in (0..head).step_by(BLOCK) {
        let d0 = f64::from(a[i]) - f64::from(b[i]);
        let d1 = f64::from(a[i + 1]) - f64::from(b[i + 1]);
        let d2 = f64::from(a[i + 2]) - f64::from(b[i + 2]);
        let d3 = f64::from(a[i + 3]) - f64::from(b[i + 3]);
        acc[0] += d0 * d0;
        acc[1] += d1 * d1;
        acc[2] += d2 * d2;
        acc[3] += d3 * d3;
    }
    let mut tail: f64 = 0.0;
    for i in head..a.len() {
        let d = f64::from(a[i]) - f64::from(b[i]);
        tail += d * d;
    }
    Ok((acc[0] + acc[1] + acc[2] + acc[3] + tail) as f32)
}

/// Cosine distance via the scalar reference kernel (bit-identical).
pub fn cosine_distance(a: &[f32], b: &[f32]) -> Result<Option<f32>> {
    super::scalar::cosine_distance_f32(a, b)
}

/// Cosine similarity via the scalar reference kernel (bit-identical).
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<Option<f32>> {
    super::scalar::cosine_similarity_f32(a, b)
}

/// Euclidean norm.
#[must_use]
pub fn norm_f32(a: &[f32]) -> f32 {
    super::scalar::norm_squared_f32(a).sqrt()
}

/// Write the unit-norm version of `src` into `dst`.
///
/// Returns `Ok(true)` on success, or `Ok(false)` when `src` has zero norm
/// (undefined direction); in the `false` case `dst` is left untouched.
/// Dimension mismatch is an error.
pub fn normalize_into(dst: &mut [f32], src: &[f32]) -> Result<bool> {
    check_dims_equal(dst.len(), src.len())?;
    let n2 = super::scalar::norm_squared_f32(src);
    if n2 == 0.0 {
        return Ok(false);
    }
    let inv = 1.0 / f64::from(n2).sqrt();
    for i in 0..src.len() {
        dst[i] = (f64::from(src[i]) * inv) as f32;
    }
    Ok(true)
}

/// Normalize `v` in place. Returns `false` (leaving `v` untouched) on zero norm.
pub fn normalize_in_place(v: &mut [f32]) -> bool {
    let n2 = super::scalar::norm_squared_f32(v);
    if n2 == 0.0 {
        return false;
    }
    let inv = 1.0 / f64::from(n2).sqrt();
    for x in v.iter_mut() {
        *x = (f64::from(*x) * inv) as f32;
    }
    true
}
