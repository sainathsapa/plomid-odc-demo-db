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
//! Exact rerank of an oversampled candidate set.
//!
//! Typical ANN flow: fetch `oversample_count(k, factor, cap)` approximate
//! candidates, then [`rerank`] them with exact distances computed from
//! full-fidelity vectors loaded through `fetch`. Output is the best `k`
//! re-scored rows in deterministic `(distance, row_id)` order.
//!
//! Rows whose vectors are gone (`fetch` returns `None`, e.g. concurrently
//! deleted under MVCC) are skipped.

use super::{candidate::Candidate, check_cancel, topk::TopKHeap, CancelFn, Result, SearchMetric};

/// Re-score `candidates` exactly and keep the best `k`.
///
/// * `fetch(row_id)` returns the full-fidelity vector, or `None` when the
///   row is no longer visible (skipped).
/// * Dimension mismatch between `query` and any fetched vector is a hard
///   error (stale index entries must surface, not silently rank).
/// * `k == 0` yields an empty vector without calling `fetch`.
pub fn rerank<F>(
    candidates: &[Candidate],
    fetch: F,
    query: &[f32],
    metric: SearchMetric,
    k: usize,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>>
where
    F: Fn(u64) -> Option<Vec<f32>>,
{
    if k == 0 || candidates.is_empty() {
        return Ok(Vec::new());
    }
    let mut heap = TopKHeap::new(k, metric.default_mode());
    for (i, cand) in candidates.iter().enumerate() {
        check_cancel(cancel, i)?;
        let Some(full) = fetch(cand.row_id) else {
            continue; // row disappeared; skip deterministically.
        };
        let Some(d) = super::distance_for_metric(metric, query, Some(&full))? else {
            continue; // undefined (zero-norm cosine): not orderable.
        };
        heap.push(cand.row_id, d);
    }
    Ok(heap.into_sorted_vec())
}
