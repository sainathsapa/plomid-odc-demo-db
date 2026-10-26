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
//! Candidate union: dedup hit lists by row, keeping the best distance.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::index::{RowId, SearchResult};

/// A merged hybrid candidate (lower-is-better distance).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HybridHit {
    /// Stable logical row identity (never a physical offset; revalidated against MVCC).
    pub row_id: RowId,
    /// Ascending distance; lower-is-better for every metric (dot stored negated).
    pub distance: f32,
}

/// Union of hit lists, deduped by row (minimum distance wins), ascending.
#[must_use]
pub fn merge_union(lists: &[&[SearchResult]]) -> Vec<SearchResult> {
    let mut best: HashMap<RowId, f32> = HashMap::new();
    for list in lists {
        for hit in *list {
            best.entry(hit.row_id)
                .and_modify(|d| {
                    if hit.distance < *d {
                        *d = hit.distance;
                    }
                })
                .or_insert(hit.distance);
        }
    }
    let mut out: Vec<SearchResult> = best
        .into_iter()
        .map(|(row_id, distance)| SearchResult::new(row_id, distance))
        .collect();
    out.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.row_id.cmp(&b.row_id))
    });
    out
}
