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
//! IEEE-754 binary16 (`float16`) codecs and kernels.
//!
//! Values are transported as raw `u16` bits in little-endian element order.
//! Conversion is implemented manually (no external crates, no `unsafe`):
//!
//! * [`f16_to_f32`] is exact — every finite `f16` value (including
//!   subnormals) is exactly representable in `f32`, so decoding is bit-exact.
//! * [`f32_to_f16`] rounds to nearest, ties to even, per IEEE-754.
//!
//! Kernels decode on the fly and accumulate in `f64`.

use super::{check_dims_equal, Result};

/// Decode `f16` bits to `f32` (exact for all inputs).
#[must_use]
pub fn f16_to_f32(bits: u16) -> f32 {
    let sign = u32::from(bits & 0x8000) << 16;
    let exp = (bits >> 10) & 0x1f;
    let mant = u32::from(bits & 0x03ff);
    match exp {
        0 => {
            if mant == 0 {
                f32::from_bits(sign)
            } else {
                // Subnormal: mant * 2^-24. Exactly representable in f64 and
                // in f32 (exponent -24 is well above f32's subnormal range),
                // so the single rounding below is exact.
                let mag = (f64::from(mant) * 2.0f64.powi(-24)) as f32;
                if sign == 0 {
                    mag
                } else {
                    -mag
                }
            }
        }
        0x1f => {
            if mant == 0 {
                f32::from_bits(sign | 0x7f80_0000)
            } else {
                f32::from_bits(sign | 0x7fc0_0000 | (mant << 13))
            }
        }
        _ => f32::from_bits(sign | ((u32::from(exp) + 112) << 23) | (mant << 13)),
    }
}

/// Encode `f32` as `f16` bits, rounding to nearest, ties to even.
///
/// Overflow maps to signed infinity; NaN maps to a quiet NaN with a
/// best-effort payload; values too small for the smallest subnormal map to
/// signed zero.
#[must_use]
pub fn f32_to_f16(v: f32) -> u16 {
    let x = v.to_bits();
    let sign = ((x >> 16) & 0x8000) as u16;
    let exp = ((x >> 23) & 0xff) as i32;
    let mant = x & 0x007f_ffff;
    if exp == 255 {
        if mant == 0 {
            return sign | 0x7c00;
        }
        let payload = (mant >> 13) as u16;
        return sign | 0x7c00 | if payload == 0 { 0x0200 } else { payload };
    }
    let half_exp = exp - 112;
    if half_exp >= 31 {
        return sign | 0x7c00;
    }
    if half_exp <= 0 {
        if half_exp < -10 {
            return sign;
        }
        // Subnormal f16: round (mant | hidden 1) >> shift to nearest-even.
        let full = mant | 0x0080_0000;
        let shift = (14 - half_exp) as u32; // 14..=24
        let mask = (1u32 << shift) - 1;
        let dropped = full & mask;
        let half = 1u32 << (shift - 1);
        let mut k = full >> shift;
        if dropped > half || (dropped == half && (k & 1) == 1) {
            k += 1;
        }
        if k >= 0x0400 {
            // Rounded up into the normal range: smallest normal f16.
            return sign | 0x0400;
        }
        return sign | (k as u16);
    }
    let kept = mant >> 13;
    let dropped = mant & 0x1fff;
    let mut hmant = kept;
    let mut he = half_exp;
    if dropped > 0x1000 || (dropped == 0x1000 && (kept & 1) == 1) {
        hmant += 1;
        if hmant == 0x0400 {
            hmant = 0;
            he += 1;
            if he >= 31 {
                return sign | 0x7c00;
            }
        }
    }
    sign | ((he as u16) << 10) | (hmant as u16)
}

/// Decode `src` bits into `dst` (lengths must match).
pub fn convert_to_f32_into(dst: &mut [f32], src: &[u16]) -> Result<()> {
    check_dims_equal(dst.len(), src.len())?;
    for i in 0..src.len() {
        dst[i] = f16_to_f32(src[i]);
    }
    Ok(())
}

/// Decode bits to a new `f32` vector.
pub fn convert_to_f32_vec(src: &[u16]) -> Vec<f32> {
    src.iter().map(|&b| f16_to_f32(b)).collect()
}

/// Encode `src` floats into `dst` bits (lengths must match).
pub fn convert_from_f32_into(dst: &mut [u16], src: &[f32]) -> Result<()> {
    check_dims_equal(dst.len(), src.len())?;
    for i in 0..src.len() {
        dst[i] = f32_to_f16(src[i]);
    }
    Ok(())
}

/// Encode floats to a new `f16`-bits vector.
pub fn convert_from_f32_vec(src: &[f32]) -> Vec<u16> {
    src.iter().map(|&v| f32_to_f16(v)).collect()
}

/// Inner product over `f16` bits with `f64` accumulation.
pub fn dot(a: &[u16], b: &[u16]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        acc += f64::from(f16_to_f32(a[i])) * f64::from(f16_to_f32(b[i]));
    }
    Ok(acc as f32)
}

/// Squared Euclidean distance over `f16` bits.
pub fn l2_squared(a: &[u16], b: &[u16]) -> Result<f32> {
    check_dims_equal(a.len(), b.len())?;
    let mut acc: f64 = 0.0;
    for i in 0..a.len() {
        let d = f64::from(f16_to_f32(a[i])) - f64::from(f16_to_f32(b[i]));
        acc += d * d;
    }
    Ok(acc as f32)
}

/// Euclidean distance over `f16` bits.
pub fn l2_distance(a: &[u16], b: &[u16]) -> Result<f32> {
    let s = l2_squared(a, b)?;
    Ok((f64::from(s)).sqrt() as f32)
}

/// Cosine similarity over `f16` bits, or `None` on zero norm.
pub fn cosine_similarity(a: &[u16], b: &[u16]) -> Result<Option<f32>> {
    check_dims_equal(a.len(), b.len())?;
    let mut dot: f64 = 0.0;
    let mut na: f64 = 0.0;
    let mut nb: f64 = 0.0;
    for i in 0..a.len() {
        let x = f64::from(f16_to_f32(a[i]));
        let y = f64::from(f16_to_f32(b[i]));
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return Ok(None);
    }
    Ok(Some((dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0) as f32))
}

/// Cosine distance over `f16` bits, or `None` on zero norm.
pub fn cosine_distance(a: &[u16], b: &[u16]) -> Result<Option<f32>> {
    match cosine_similarity(a, b)? {
        None => Ok(None),
        Some(s) => Ok(Some((1.0 - f64::from(s)) as f32)),
    }
}
