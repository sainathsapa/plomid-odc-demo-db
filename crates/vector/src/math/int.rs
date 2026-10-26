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
//! Integer kernels with `i64` checked accumulation.
//!
//! Products are widened before multiplying and every addition uses
//! [`i64::checked_add`] / [`i64::checked_mul`]; overflow yields
//! [`MathError::Overflow`] instead of wrapping. Distances derived from
//! integer accumulators are computed in `f64` and rounded once to `f32`.

use super::{check_dims_equal, MathError, Result};

/// Inner product over `i8` with `i64` checked accumulation.
pub fn dot_i8(a: &[i8], b: &[i8]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let p = (i64::from(a[i]))
            .checked_mul(i64::from(b[i]))
            .ok_or(MathError::Overflow { what: "dot_i8" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "dot_i8" })?;
    }
    Ok(acc)
}

/// Inner product over `u8` with `i64` checked accumulation.
pub fn dot_u8(a: &[u8], b: &[u8]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let p = (i64::from(a[i]))
            .checked_mul(i64::from(b[i]))
            .ok_or(MathError::Overflow { what: "dot_u8" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "dot_u8" })?;
    }
    Ok(acc)
}

/// Inner product over `i32` with `i64` checked accumulation.
pub fn dot_i32(a: &[i32], b: &[i32]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let p = (i64::from(a[i]))
            .checked_mul(i64::from(b[i]))
            .ok_or(MathError::Overflow { what: "dot_i32" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "dot_i32" })?;
    }
    Ok(acc)
}

/// Squared Euclidean distance over `i8` with `i64` checked accumulation.
pub fn l2_squared_i8(a: &[i8], b: &[i8]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let d = i64::from(a[i]) - i64::from(b[i]);
        let p = d.checked_mul(d).ok_or(MathError::Overflow {
            what: "l2_squared_i8",
        })?;
        acc = acc.checked_add(p).ok_or(MathError::Overflow {
            what: "l2_squared_i8",
        })?;
    }
    Ok(acc)
}

/// Squared Euclidean distance over `u8` with `i64` checked accumulation.
pub fn l2_squared_u8(a: &[u8], b: &[u8]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let d = i64::from(a[i]) - i64::from(b[i]);
        let p = d.checked_mul(d).ok_or(MathError::Overflow {
            what: "l2_squared_u8",
        })?;
        acc = acc.checked_add(p).ok_or(MathError::Overflow {
            what: "l2_squared_u8",
        })?;
    }
    Ok(acc)
}

/// Squared Euclidean distance over `i32` with `i64` checked accumulation.
///
/// Note: `i32` differences can reach `±2^32`, whose square overflows `i64`;
/// such inputs correctly report [`MathError::Overflow`].
pub fn l2_squared_i32(a: &[i32], b: &[i32]) -> Result<i64> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: i64 = 0;
    for i in 0..a.len() {
        let d = i64::from(a[i]) - i64::from(b[i]);
        let p = d.checked_mul(d).ok_or(MathError::Overflow {
            what: "l2_squared_i32",
        })?;
        acc = acc.checked_add(p).ok_or(MathError::Overflow {
            what: "l2_squared_i32",
        })?;
    }
    Ok(acc)
}

/// Euclidean distance over `i8` (`sqrt` in `f64`, rounded once to `f32`).
pub fn l2_distance_i8(a: &[i8], b: &[i8]) -> Result<f32> {
    Ok((l2_squared_i8(a, b)? as f64).sqrt() as f32)
}

/// Euclidean distance over `u8`.
pub fn l2_distance_u8(a: &[u8], b: &[u8]) -> Result<f32> {
    Ok((l2_squared_u8(a, b)? as f64).sqrt() as f32)
}

/// Euclidean distance over `i32`.
pub fn l2_distance_i32(a: &[i32], b: &[i32]) -> Result<f32> {
    Ok((l2_squared_i32(a, b)? as f64).sqrt() as f32)
}

fn cosine_from_parts(dot: i64, na2: i64, nb2: i64) -> Option<f32> {
    if na2 == 0 || nb2 == 0 {
        return None;
    }
    let sim = (dot as f64) / ((na2 as f64).sqrt() * (nb2 as f64).sqrt());
    Some(sim.clamp(-1.0, 1.0) as f32)
}

fn norm_squared_i8(a: &[i8]) -> Result<i64> {
    let mut acc: i64 = 0;
    for &v in a {
        let x = i64::from(v);
        let p = x
            .checked_mul(x)
            .ok_or(MathError::Overflow { what: "cosine_i8" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "cosine_i8" })?;
    }
    Ok(acc)
}

fn norm_squared_u8(a: &[u8]) -> Result<i64> {
    let mut acc: i64 = 0;
    for &v in a {
        let x = i64::from(v);
        let p = x
            .checked_mul(x)
            .ok_or(MathError::Overflow { what: "cosine_u8" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "cosine_u8" })?;
    }
    Ok(acc)
}

fn norm_squared_i32(a: &[i32]) -> Result<i64> {
    let mut acc: i64 = 0;
    for &v in a {
        let x = i64::from(v);
        let p = x
            .checked_mul(x)
            .ok_or(MathError::Overflow { what: "cosine_i32" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "cosine_i32" })?;
    }
    Ok(acc)
}

/// Cosine similarity over `i8`, or `None` on zero norm.
pub fn cosine_similarity_i8(a: &[i8], b: &[i8]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    Ok(cosine_from_parts(
        dot_i8(a, b)?,
        norm_squared_i8(a)?,
        norm_squared_i8(b)?,
    ))
}

/// Cosine distance over `i8`, or `None` on zero norm.
pub fn cosine_distance_i8(a: &[i8], b: &[i8]) -> Result<Option<f32>> {
    Ok(cosine_similarity_i8(a, b)?.map(|s| 1.0 - s))
}

/// Cosine similarity over `u8`, or `None` on zero norm.
pub fn cosine_similarity_u8(a: &[u8], b: &[u8]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    Ok(cosine_from_parts(
        dot_u8(a, b)?,
        norm_squared_u8(a)?,
        norm_squared_u8(b)?,
    ))
}

/// Cosine distance over `u8`, or `None` on zero norm.
pub fn cosine_distance_u8(a: &[u8], b: &[u8]) -> Result<Option<f32>> {
    Ok(cosine_similarity_u8(a, b)?.map(|s| 1.0 - s))
}

/// Cosine similarity over `i32`, or `None` on zero norm.
pub fn cosine_similarity_i32(a: &[i32], b: &[i32]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    Ok(cosine_from_parts(
        dot_i32(a, b)?,
        norm_squared_i32(a)?,
        norm_squared_i32(b)?,
    ))
}

/// Cosine distance over `i32`, or `None` on zero norm.
pub fn cosine_distance_i32(a: &[i32], b: &[i32]) -> Result<Option<f32>> {
    Ok(cosine_similarity_i32(a, b)?.map(|s| 1.0 - s))
}
