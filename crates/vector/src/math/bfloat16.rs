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
//! Brain-float-16 (`bfloat16`) codecs and kernels.
//!
//! `bfloat16` shares `f32`'s sign/exponent layout truncated to 16 bits, so
//! decoding is a left shift (`(bits as u32) << 16`) and encoding rounds the
//! dropped 16 bits to nearest, ties to even. No external crates, no `unsafe`.
//!
//! Kernels decode on the fly and accumulate in `f64`.

use super::{check_dims_equal, Result};

/// Decode `bf16` bits to `f32` (exact for all inputs).
#[must_use]
pub fn bf16_to_f32(bits: u16) -> f32 {
    f32::from_bits((u32::from(bits)) << 16)
}

/// Encode `f32` as `bf16` bits, rounding to nearest, ties to even.
///
/// Infinities survive; NaN maps to a quiet NaN (payload preserved on a
/// best-effort basis).
#[must_use]
pub fn f32_to_bf16(v: f32) -> u16 {
    let x = v.to_bits();
    // NaN / infinity bypass rounding.
    if x & 0x7f80_0000 == 0x7f80_0000 {
        if x & 0x007f_ffff == 0 {
            return (x >> 16) as u16; // infinity bit-identical
        }
        let mut h = (x >> 16) as u16;
        if h & 0x007f == 0 {
            h |= 0x0040; // ensure a nonzero (quiet) payload
        }
        return h;
    }
    // Round-to-nearest-even on the dropped low 16 bits. Adding
    // `0x7fff + lsb` then truncating rounds halves to even; magnitudes stay
    // far from `u32` overflow for finite inputs (max `0xff7fffff + 0x8000`).
    let lsb = (x >> 16) & 1;
    x.wrapping_add(0x7fff + lsb).wrapping_shr(16) as u16
}

/// Decode `src` bits into `dst` (lengths must match).
pub fn convert_to_f32_into(dst: &mut [f32], src: &[u16]) -> Result<()> {
    check_dims_equal(dst.len(), src.len())?;
    for i in 0..src.len() {
        dst[i] = bf16_to_f32(src[i]);
    }
    Ok(())
}

/// Decode bits to a new `f32` vector.
pub fn convert_to_f32_vec(src: &[u16]) -> Vec<f32> {
    src.iter().map(|&b| bf16_to_f32(b)).collect()
}

/// Encode `src` floats into `dst` bits (lengths must match).
pub fn convert_from_f32_into(dst: &mut [u16], src: &[f32]) -> Result<()> {
    check_dims_equal(dst.len(), src.len())?;
    for i in 0..src.len() {
        dst[i] = f32_to_bf16(src[i]);
    }
    Ok(())
}

/// Encode floats to a new `bf16`-bits vector.
pub fn convert_from_f32_vec(src: &[f32]) -> Vec<u16> {
    src.iter().map(|&v| f32_to_bf16(v)).collect()
}

/// Inner product over `bf16` bits with `f64` accumulation.
pub fn dot(a: &[u16], b: &[u16]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        acc += f64::from(bf16_to_f32(a[i])) * f64::from(bf16_to_f32(b[i]));
    }
    Ok(acc as f32)
}

/// Squared Euclidean distance over `bf16` bits.
pub fn l2_squared(a: &[u16], b: &[u16]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        let d = f64::from(bf16_to_f32(a[i])) - f64::from(bf16_to_f32(b[i]));
        acc += d * d;
    }
    Ok(acc as f32)
}

/// Euclidean distance over `bf16` bits.
pub fn l2_distance(a: &[u16], b: &[u16]) -> Result<f32> {
    let s = l2_squared(a, b)?;
    Ok((f64::from(s)).sqrt() as f32)
}

/// Cosine similarity over `bf16` bits, or `None` on zero norm.
pub fn cosine_similarity(a: &[u16], b: &[u16]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    let mut dot: f64 = 0.0;
    let mut na: f64 = 0.0;
    let mut nb: f64 = 0.0;
    for i in 0..a.len() {
        let x = f64::from(bf16_to_f32(a[i]));
        let y = f64::from(bf16_to_f32(b[i]));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return Ok(None);
    }
    Ok(Some((dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0) as f32))
}

/// Cosine distance over `bf16` bits, or `None` on zero norm.
pub fn cosine_distance(a: &[u16], b: &[u16]) -> Result<Option<f32>> {
    match cosine_similarity(a, b)? {
        None => Ok(None),
        Some(s) => Ok(Some((1.0 - f64::from(s)) as f32)),
    }
}
