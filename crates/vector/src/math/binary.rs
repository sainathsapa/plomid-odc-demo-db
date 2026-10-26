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
//! Packed binary (bit-vector) kernels.
//!
//! Layout: bit `i` lives in byte `i / 8` at MSB-first position `7 - (i % 8)`
//! (byte 0 holds bits `[0..8)` top-to-bottom). This matches the SQL-facing
//! ingest path (`literal::parse_binary`, `BinaryVectorValue::get_bit`, and
//! `index::binary::BinaryIndex::pack_bits`, all MSB-first). When `num_bits`
//! is not a multiple of 8, the low bits of the last byte are padding; every
//! kernel masks them out, so padding content never affects results (callers
//! should still zero padding on write, as `BinaryVectorValue::new` does).
//!
//! Jaccard follows the empty-set rule: similarity is `1.0` when both sets
//! are empty (union is zero), otherwise `|A ∩ B| / |A ∪ B|`.

use super::{bytes_for_bits, check_dims_equal, Result};

/// Population count of a packed bit-vector.
#[must_use]
pub fn popcount_packed(bits: &[u8]) -> u64 {
    bits.iter().map(|b| u64::from(b.count_ones())).sum()
}

/// Hamming distance between two packed bit-vectors of `num_bits` bits.
///
/// Padding bits in the last byte are masked out before counting.
pub fn hamming_packed(a: &[u8], b: &[u8], num_bits: usize) -> Result<u64> {
    let want = bytes_for_bits(num_bits)?;
    check_dims_equal(a.len(), want)?;
    check_dims_equal(b.len(), want)?;
    if want == 0 {
        return Ok(0);
    }
    let mut total: u64 = 0;
    for i in 0..want {
        let mut x = a[i] ^ b[i];
        if i + 1 == want {
            x &= last_byte_mask(num_bits);
        }
        total += u64::from(x.count_ones());
    }
    Ok(total)
}

/// Hamming distance between two `u64` words.
#[must_use]
pub fn hamming_u64(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Keep-mask for the last byte: top `(num_bits % 8)` bits set
/// (`0xFF` when `num_bits` is byte-aligned).
fn last_byte_mask(num_bits: usize) -> u8 {
    let rem = num_bits % 8;
    if rem == 0 {
        0xff
    } else {
        !(0xff >> rem)
    }
}

/// `|A ∩ B|` with padding masked out.
pub fn and_count_packed(a: &[u8], b: &[u8], num_bits: usize) -> Result<u64> {
    let want = bytes_for_bits(num_bits)?;
    check_dims_equal(a.len(), want)?;
    check_dims_equal(b.len(), want)?;
    if want == 0 {
        return Ok(0);
    }
    let mut total: u64 = 0;
    for i in 0..want {
        let mut x = a[i] & b[i];
        if i + 1 == want {
            x &= last_byte_mask(num_bits);
        }
        total += u64::from(x.count_ones());
    }
    Ok(total)
}

/// `|A ∪ B|` with padding masked out.
pub fn or_count_packed(a: &[u8], b: &[u8], num_bits: usize) -> Result<u64> {
    let want = bytes_for_bits(num_bits)?;
    check_dims_equal(a.len(), want)?;
    check_dims_equal(b.len(), want)?;
    if want == 0 {
        return Ok(0);
    }
    let mut total: u64 = 0;
    for i in 0..want {
        let mut x = a[i] | b[i];
        if i + 1 == want {
            x &= last_byte_mask(num_bits);
        }
        total += u64::from(x.count_ones());
    }
    Ok(total)
}

/// Jaccard similarity: `1.0` when both sets are empty, else
/// `|A ∩ B| / |A ∪ B|`.
pub fn jaccard_similarity_packed(a: &[u8], b: &[u8], num_bits: usize) -> Result<f32> {
    let inter = and_count_packed(a, b, num_bits)?;
    let union = or_count_packed(a, b, num_bits)?;
    if union == 0 {
        return Ok(1.0);
    }
    Ok((inter as f64 / union as f64) as f32)
}

/// Jaccard distance: `1 - similarity` (so `0.0` for two empty sets).
pub fn jaccard_distance_packed(a: &[u8], b: &[u8], num_bits: usize) -> Result<f32> {
    Ok(1.0 - jaccard_similarity_packed(a, b, num_bits)?)
}
