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
//! Exact (brute-force) top-k, radius, and multi-query search.
//!
//! Every routine streams rows through a bounded [`TopKHeap`](super::TopKHeap)
//! (`O(k)` extra memory); batch search streams the row set once per query and
//! never materializes an `N × M` distance matrix.

use super::{
    candidate::Candidate,
    check_cancel,
    radius::{radius_search_borrowed, RadiusConfig},
    topk::{TopKHeap, TopKMode},
    CancelFn, Result, SearchMetric,
};

/// Exact top-k: stream `rows`, keep the best `k` under `metric`.
///
/// `rows` items are `(row_id, vector)`; `None` vectors (`NULL`) and `None`
/// distances (zero-norm cosine) are skipped. Output is best-first with
/// deterministic `(distance, row_id)` tie-breaking.
pub fn exact_top_k<'a, I>(
    query: &[f32],
    rows: I,
    metric: SearchMetric,
    k: usize,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>>
where
    I: IntoIterator<Item = (u64, Option<&'a [f32]>)>,
{
    let mode = metric.default_mode();
    exact_top_k_with_mode(query, rows, metric, mode, k, cancel)
}

fn exact_top_k_with_mode<'a, I>(
    query: &[f32],
    rows: I,
    metric: SearchMetric,
    mode: TopKMode,
    k: usize,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>>
where
    I: IntoIterator<Item = (u64, Option<&'a [f32]>)>,
{
    if k == 0 {
        return Ok(Vec::new());
    }
    super::check_query(query)?;
    super::check_k(k)?;
    let mut heap = TopKHeap::new(k, mode);
    let mut seen: usize = 0;
    for (row_id, vec) in rows {
        check_cancel(cancel, seen)?;
        seen = seen.saturating_add(1);
        let Some(d) = super::distance_for_metric(metric, query, vec)? else {
            continue;
        };
        heap.push(row_id, d);
    }
    Ok(heap.into_sorted_vec())
}

/// Exact radius search; delegates to [`crate::search::radius`] with owned rows.
pub fn exact_radius(
    query: &[f32],
    rows: Vec<(u64, Option<Vec<f32>>)>,
    metric: SearchMetric,
    config: &RadiusConfig,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>> {
    radius_search_borrowed(
        query,
        rows.iter().map(|(id, v)| (*id, v.as_deref())),
        metric,
        config,
        cancel,
    )
}

/// Exact batch search: `queries.len()` top-k searches over one shared row set.
///
/// Each query streams `rows` with its own bounded heap, so peak extra memory
/// is `O(k)` plus the output — never an `N × M` matrix. Cancellation is
/// checked per row and once per query (an abort discards partial output and
/// returns [`SearchError::Cancelled`](super::SearchError::Cancelled)).
pub fn exact_batch_search(
    queries: &[&[f32]],
    rows: &[(u64, Option<Vec<f32>>)],
    metric: SearchMetric,
    k: usize,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Vec<Candidate>>> {
    if k == 0 {
        return Ok(queries.iter().map(|_| Vec::new()).collect());
    }
    super::check_k(k)?;
    let mode = metric.default_mode();
    let mut all = Vec::with_capacity(queries.len());
    for (qi, query) in queries.iter().enumerate() {
        check_cancel(cancel, qi * super::CANCEL_CHECK_EVERY)?;
        super::check_query(query)?;
        let mut heap = TopKHeap::new(k, mode);
        let mut seen: usize = 0;
        for (row_id, vec) in rows.iter() {
            check_cancel(cancel, seen)?;
            seen = seen.saturating_add(1);
            let Some(d) = super::distance_for_metric(metric, query, vec.as_deref())? else {
                continue;
            };
            heap.push(*row_id, d);
        }
        all.push(heap.into_sorted_vec());
    }
    Ok(all)
}
