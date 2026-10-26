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
#![forbid(unsafe_code)]
//! Alpha blending of dense/sparse hit lists.
//!
//! Distances are lower-is-better; they are first mapped to higher-is-better
//! scores in `[0, 1]` (`1 / (1 + d)`) after per-list min-max stretching, then
//! blended as `alpha * dense + (1 - alpha) * sparse`.

use std::collections::HashMap;

use crate::index::{RowId, SearchResult};

/// Map lower-is-better distances to higher-is-better `[0, 1]` scores.
/// Single-element and constant lists map to `1.0`.
#[must_use]
pub fn normalize_higher_is_better(hits: &[SearchResult]) -> Vec<(RowId, f32)> {
    if hits.is_empty() {
        return Vec::new();
    }
    let min = hits.iter().map(|h| h.distance).fold(f32::MAX, f32::min);
    let max = hits.iter().map(|h| h.distance).fold(f32::MIN, f32::max);
    // Degenerate spread (all equal, or NaN-infected): every hit scores 1.0
    // rather than dividing by zero. `partial_cmp` keeps NaN handling explicit.
    if max
        .partial_cmp(&min)
        .is_none_or(|ord| ord != std::cmp::Ordering::Greater)
    {
        return hits.iter().map(|h| (h.row_id, 1.0)).collect();
    }
    hits.iter()
        .map(|h| {
            let stretched = 1.0 - (h.distance - min) / (max - min);
            (
                h.row_id,
                (1.0 / (1.0 + h.distance) * 0.5 + stretched * 0.5).clamp(0.0, 1.0),
            )
        })
        .collect()
}

/// Blend two normalized hit lists with weight `alpha` on `dense`.
/// `alpha` is clamped to `[0, 1]`. Output is descending by fused score.
#[must_use]
pub fn fuse_weighted(
    dense: &[SearchResult],
    sparse: &[SearchResult],
    alpha: f32,
) -> Vec<(RowId, f32)> {
    let a = alpha.clamp(0.0, 1.0);
    let dn = normalize_higher_is_better(dense);
    let sn = normalize_higher_is_better(sparse);
    let mut acc: HashMap<RowId, (f32, f32)> = HashMap::new(); // (dense, sparse)
    for (id, s) in dn {
        acc.entry(id).or_default().0 = s;
    }
    for (id, s) in sn {
        acc.entry(id).or_default().1 = s;
    }
    let mut out: Vec<(RowId, f32)> = acc
        .into_iter()
        .map(|(id, (d, s))| (id, a * d + (1.0 - a) * s))
        .collect();
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out
}
