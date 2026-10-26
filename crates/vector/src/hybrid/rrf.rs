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
//! Reciprocal Rank Fusion: `score = Σ 1 / (k + rank)`.

use std::collections::HashMap;

use crate::index::{RowId, SearchResult};

/// Default RRF rank constant.
pub const DEFAULT_RRF_K: usize = 60;

/// Fuse ranked lists (each ascending distance). Output descending by RRF score.
#[must_use]
pub fn fuse_rrf(lists: &[&[SearchResult]], k: usize) -> Vec<(RowId, f32)> {
    let mut acc: HashMap<RowId, f32> = HashMap::new();
    for list in lists {
        for (rank, hit) in list.iter().enumerate() {
            let inc = 1.0 / ((k as f32) + (rank as f32) + 1.0);
            *acc.entry(hit.row_id).or_default() += inc;
        }
    }
    let mut out: Vec<(RowId, f32)> = acc.into_iter().collect();
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out
}
