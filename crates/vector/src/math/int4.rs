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
//! 4-bit integer (`int4` / `uint4`) nibble packing and kernels.
//!
//! Layout: two elements per byte, element `2*i` in the **low** nibble of byte
//! `i` and element `2*i+1` in the **high** nibble. When `len` is odd, the
//! high nibble of the last byte is padding and must be zero on input to the
//! packed kernels (it is ignored on decode).
//!
//! * Signed values use 4-bit two's complement (`-8..=7`).
//! * Unsigned values use plain binary (`0..=15`).
//! * Accumulation is `i64` checked, matching [`crate::math::int`].

use super::{MathError, Result};

/// Inclusive range of signed 4-bit values.
pub const I4_MIN: i8 = -8;
/// Inclusive range of signed 4-bit values.
pub const I4_MAX: i8 = 7;
/// Inclusive maximum of unsigned 4-bit values.
pub const U4_MAX: u8 = 15;

/// Packed byte count for `len` nibbles (checked).
pub fn packed_len_for(len: usize) -> Result<usize> {
    len.checked_add(1)
        .map(|n| n / 2)
        .ok_or(MathError::Overflow {
            what: "int4 packed length",
        })
}

fn check_packed(packed_len: usize, len: usize, what: &'static str) -> Result<()> {
    let want = packed_len_for(len)?;
    if packed_len == want {
        Ok(())
    } else {
        Err(MathError::InvalidLength {
            what,
            len: packed_len,
        })
    }
}

/// Pack signed values (`-8..=7`) two per byte, low nibble first.
pub fn pack_i4(values: &[i8]) -> Result<Vec<u8>> {
    let mut out = vec![0u8; packed_len_for(values.len())?];
    for (i, &v) in values.iter().enumerate() {
        if !(I4_MIN..=I4_MAX).contains(&v) {
            return Err(MathError::OutOfRange {
                value: i64::from(v),
                lo: i64::from(I4_MIN),
                hi: i64::from(I4_MAX),
            });
        }
        let nibble = (v as u8) & 0x0f;
        let byte = &mut out[i / 2];
        if i % 2 == 0 {
            *byte |= nibble;
        } else {
            *byte |= nibble << 4;
        }
    }
    Ok(out)
}

/// Pack unsigned values (`0..=15`) two per byte, low nibble first.
pub fn pack_u4(values: &[u8]) -> Result<Vec<u8>> {
    let mut out = vec![0u8; packed_len_for(values.len())?];
    for (i, &v) in values.iter().enumerate() {
        if v > U4_MAX {
            return Err(MathError::OutOfRange {
                value: i64::from(v),
                lo: 0,
                hi: i64::from(U4_MAX),
            });
        }
        let byte = &mut out[i / 2];
        if i % 2 == 0 {
            *byte |= v;
        } else {
            *byte |= v << 4;
        }
    }
    Ok(out)
}

#[inline]
fn nibble_at(packed: &[u8], i: usize) -> u8 {
    (packed[i / 2] >> ((i % 2) * 4)) & 0x0f
}

/// Decode one signed nibble (4-bit two's complement sign extension via
/// arithmetic shifts — no `unsafe`, no branches on the value).
#[inline]
fn sign_extend_nibble(nibble: u8) -> i8 {
    ((nibble as i8) << 4) >> 4
}

/// Unpack `len` signed values from `packed`.
pub fn unpack_i4_into(dst: &mut [i8], packed: &[u8]) -> Result<()> {
    let len = dst.len();
    check_packed(packed.len(), len, "int4 packed")?;
    for (slot, value) in dst.iter_mut().enumerate() {
        *value = sign_extend_nibble(nibble_at(packed, slot));
    }
    Ok(())
}

/// Unpack `len` signed values to a new vector.
pub fn unpack_i4(packed: &[u8], len: usize) -> Result<Vec<i8>> {
    check_packed(packed.len(), len, "int4 packed")?;
    let mut out = vec![0i8; len];
    unpack_i4_into(&mut out, packed)?;
    Ok(out)
}

/// Unpack `len` unsigned values from `packed`.
pub fn unpack_u4_into(dst: &mut [u8], packed: &[u8]) -> Result<()> {
    let len = dst.len();
    check_packed(packed.len(), len, "uint4 packed")?;
    for (slot, value) in dst.iter_mut().enumerate() {
        *value = nibble_at(packed, slot);
    }
    Ok(())
}

/// Unpack `len` unsigned values to a new vector.
pub fn unpack_u4(packed: &[u8], len: usize) -> Result<Vec<u8>> {
    check_packed(packed.len(), len, "uint4 packed")?;
    let mut out = vec![0u8; len];
    unpack_u4_into(&mut out, packed)?;
    Ok(out)
}

/// Inner product over packed signed nibbles with `i64` checked accumulation.
///
/// `len` is the element count; trailing padding nibbles are never read.
pub fn dot_i4_packed(a: &[u8], b: &[u8], len: usize) -> Result<i64> {
    check_packed(a.len(), len, "int4 packed a")?;
    check_packed(b.len(), len, "int4 packed b")?;
    let mut acc: i64 = 0;
    for i in 0..len {
        let x = i64::from(sign_extend_nibble(nibble_at(a, i)));
        let y = i64::from(sign_extend_nibble(nibble_at(b, i)));
        let p = x
            .checked_mul(y)
            .ok_or(MathError::Overflow { what: "dot_i4" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "dot_i4" })?;
    }
    Ok(acc)
}

/// Inner product over packed unsigned nibbles with `i64` checked accumulation.
pub fn dot_u4_packed(a: &[u8], b: &[u8], len: usize) -> Result<i64> {
    check_packed(a.len(), len, "uint4 packed a")?;
    check_packed(b.len(), len, "uint4 packed b")?;
    let mut acc: i64 = 0;
    for i in 0..len {
        let x = i64::from(nibble_at(a, i));
        let y = i64::from(nibble_at(b, i));
        let p = x
            .checked_mul(y)
            .ok_or(MathError::Overflow { what: "dot_u4" })?;
        acc = acc
            .checked_add(p)
            .ok_or(MathError::Overflow { what: "dot_u4" })?;
    }
    Ok(acc)
}

/// Squared Euclidean distance over packed signed nibbles.
pub fn l2_squared_i4_packed(a: &[u8], b: &[u8], len: usize) -> Result<i64> {
    check_packed(a.len(), len, "int4 packed a")?;
    check_packed(b.len(), len, "int4 packed b")?;
    let mut acc: i64 = 0;
    for i in 0..len {
        let d = i64::from(sign_extend_nibble(nibble_at(a, i)))
            - i64::from(sign_extend_nibble(nibble_at(b, i)));
        let p = d.checked_mul(d).ok_or(MathError::Overflow {
            what: "l2_squared_i4",
        })?;
        acc = acc.checked_add(p).ok_or(MathError::Overflow {
            what: "l2_squared_i4",
        })?;
    }
    Ok(acc)
}
