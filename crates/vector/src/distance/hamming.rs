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
//! Hamming distance over packed bit-vectors with `NULL` semantics.
//!
//! * `None` input (SQL `NULL`) → `Ok(None)`.
//! * Byte-length / `num_bits` disagreements → `Err(DistanceError)`.
//! * Padding bits in the last byte are masked out (see [`crate::math::binary`]).

use super::{either_null, Result};
use crate::math::binary;

/// Hamming distance (count of differing bits) over packed bit-vectors.
pub fn hamming_distance(
    a: Option<&[u8]>,
    b: Option<&[u8]>,
    num_bits: usize,
) -> Result<Option<u64>> {
    if either_null(a, b) {
        return Ok(None);
    }
    Ok(Some(binary::hamming_packed(
        a.unwrap_or(&[]),
        b.unwrap_or(&[]),
        num_bits,
    )?))
}

/// Length-normalized Hamming distance in `[0, 1]` (`distance / num_bits`).
///
/// `num_bits == 0` denotes the empty comparison, defined as `Some(0.0)`
/// (two empty sets are identical).
pub fn hamming_distance_normalized(
    a: Option<&[u8]>,
    b: Option<&[u8]>,
    num_bits: usize,
) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    if num_bits == 0 {
        binary::hamming_packed(a.unwrap_or(&[]), b.unwrap_or(&[]), 0)?;
        return Ok(Some(0.0));
    }
    let d = binary::hamming_packed(a.unwrap_or(&[]), b.unwrap_or(&[]), num_bits)?;
    Ok(Some((d as f64 / num_bits as f64) as f32))
}
