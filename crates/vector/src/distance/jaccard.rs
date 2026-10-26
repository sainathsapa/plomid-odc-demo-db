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
//! Jaccard similarity / distance with `NULL` semantics.
//!
//! Empty-set rule: when both sets are empty the similarity is `1.0`
//! (distance `0.0`); otherwise similarity is `|A ∩ B| / |A ∪ B|`.
//!
//! * `None` packed input (SQL `NULL`) → `Ok(None)`.
//! * Byte-length / `num_bits` disagreements → `Err(DistanceError)`.
//! * Sorted-slice variants are infallible over well-formed inputs.

use super::{either_null, Result};
use crate::math::binary;

/// Jaccard similarity over packed bit-vectors.
pub fn jaccard_similarity_packed(
    a: Option<&[u8]>,
    b: Option<&[u8]>,
    num_bits: usize,
) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    Ok(Some(binary::jaccard_similarity_packed(
        a.unwrap_or(&[]),
        b.unwrap_or(&[]),
        num_bits,
    )?))
}

/// Jaccard distance (`1 - similarity`) over packed bit-vectors.
pub fn jaccard_distance_packed(
    a: Option<&[u8]>,
    b: Option<&[u8]>,
    num_bits: usize,
) -> Result<Option<f32>> {
    if either_null(a, b) {
        return Ok(None);
    }
    Ok(Some(binary::jaccard_distance_packed(
        a.unwrap_or(&[]),
        b.unwrap_or(&[]),
        num_bits,
    )?))
}

/// Jaccard similarity over sorted, deduplicated dimension-id sets.
///
/// Two-pointer merge in `O(n + m)` time and `O(1)` extra memory. Both empty →
/// `1.0`.
#[must_use]
pub fn jaccard_similarity_sorted(a: &[u32], b: &[u32]) -> f32 {
    debug_assert!(
        a.windows(2).all(|w| w[0] < w[1]),
        "set a must be sorted and deduplicated"
    );
    debug_assert!(
        b.windows(2).all(|w| w[0] < w[1]),
        "set b must be sorted and deduplicated"
    );
    let mut inter: u64 = 0;
    let mut union: u64 = 0;
    let mut i = 0;
    let mut j = 0;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                union += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                union += 1;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                inter += 1;
                union += 1;
                i += 1;
                j += 1;
            }
        }
    }
    union += (a.len() - i) as u64 + (b.len() - j) as u64;
    if union == 0 {
        return 1.0;
    }
    (inter as f64 / union as f64) as f32
}

/// Jaccard distance over sorted, deduplicated dimension-id sets.
#[must_use]
pub fn jaccard_distance_sorted(a: &[u32], b: &[u32]) -> f32 {
    1.0 - jaccard_similarity_sorted(a, b)
}

/// Jaccard similarity over arbitrary id slices.
///
/// Copies, sorts, and deduplicates both inputs (bounded `O(n + m)` extra
/// memory), then applies the sorted core. Deterministic for any input order.
#[must_use]
pub fn jaccard_similarity_sets(a: &[u32], b: &[u32]) -> f32 {
    let mut sa = a.to_vec();
    let mut sb = b.to_vec();
    sa.sort_unstable();
    sa.dedup();
    sb.sort_unstable();
    sb.dedup();
    jaccard_similarity_sorted(&sa, &sb)
}

/// Jaccard distance over arbitrary id slices (see [`jaccard_similarity_sets`]).
#[must_use]
pub fn jaccard_distance_sets(a: &[u32], b: &[u32]) -> f32 {
    1.0 - jaccard_similarity_sets(a, b)
}
