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
//! Batched distance computation with scratch reuse.
//!
//! [`compute_distances_into`] streams rows into a caller-provided buffer
//! (capacity retained across calls — no per-batch allocation), and
//! [`BatchSearcher`] additionally reuses one [`TopKHeap`](super::TopKHeap)
//! allocation across many queries.

use super::{
    candidate::Candidate, check_cancel, check_k, topk::TopKHeap, CancelFn, Result, SearchMetric,
};

/// Stream `(row_id, distance)` pairs into `out`, reusing its allocation.
///
/// `out` is cleared first (capacity retained); `NULL` / undefined rows are
/// skipped. Returns the number of pairs pushed. Cancellation is checked every
/// [`super::CANCEL_CHECK_EVERY`] rows.
pub fn compute_distances_into<'a, I>(
    query: &[f32],
    rows: I,
    metric: SearchMetric,
    out: &mut Vec<Candidate>,
    cancel: Option<&CancelFn>,
) -> Result<usize>
where
    I: IntoIterator<Item = (u64, Option<&'a [f32]>)>,
{
    out.clear();
    super::check_query(query)?;
    let mut seen: usize = 0;
    for (row_id, vec) in rows {
        check_cancel(cancel, seen)?;
        seen = seen.saturating_add(1);
        let Some(d) = super::distance_for_metric(metric, query, vec)? else {
            continue;
        };
        out.push(Candidate::new(row_id, d));
    }
    Ok(out.len())
}

/// Reusable batched top-k searcher for a fixed `(metric, k)`.
///
/// Holds one heap allocation reused across [`search_one`](Self::search_one)
/// calls via [`clear`](TopKHeap::clear), so repeated queries over changing
/// row sets allocate only on first use.
pub struct BatchSearcher {
    metric: SearchMetric,
    k: usize,
    heap: TopKHeap,
    scratch: Vec<Candidate>,
}

impl BatchSearcher {
    /// Create a searcher; `k` must not exceed [`super::MAX_TOP_K`].
    pub fn new(metric: SearchMetric, k: usize) -> Result<Self> {
        check_k(k)?;
        Ok(Self {
            metric,
            k,
            heap: TopKHeap::new(k, metric.default_mode()),
            scratch: Vec::new(),
        })
    }

    /// Metric this searcher ranks with.
    #[must_use]
    pub fn metric(&self) -> SearchMetric {
        self.metric
    }

    /// Configured `k`.
    #[must_use]
    pub fn k(&self) -> usize {
        self.k
    }

    /// Top-k over `rows` for one `query`, reusing the internal heap.
    pub fn search_one(
        &mut self,
        query: &[f32],
        rows: &[(u64, Option<Vec<f32>>)],
        cancel: Option<&CancelFn>,
    ) -> Result<Vec<Candidate>> {
        if self.k == 0 {
            return Ok(Vec::new());
        }
        self.heap.clear();
        let mut seen: usize = 0;
        for (row_id, vec) in rows.iter() {
            check_cancel(cancel, seen)?;
            seen = seen.saturating_add(1);
            let Some(d) = super::distance_for_metric(self.metric, query, vec.as_deref())? else {
                continue;
            };
            self.heap.push(*row_id, d);
        }
        // Clone the heap (bounded `k` entries) to drain it while keeping
        // `self.heap`'s allocation for the next query.
        Ok(self.heap.clone().into_sorted_vec())
    }

    /// Top-k for each of `queries` over the shared `rows` without an
    /// `N × M` distance matrix (peak extra memory `O(k)` + output).
    pub fn search_many(
        &mut self,
        queries: &[&[f32]],
        rows: &[(u64, Option<Vec<f32>>)],
        cancel: Option<&CancelFn>,
    ) -> Result<Vec<Vec<Candidate>>> {
        let mut all = Vec::with_capacity(queries.len());
        for (qi, query) in queries.iter().enumerate() {
            check_cancel(cancel, qi * super::CANCEL_CHECK_EVERY)?;
            all.push(self.search_one(query, rows, cancel)?);
        }
        Ok(all)
    }

    /// Raw (unsorted, untruncated) distances into the reusable scratch
    /// buffer; returns the buffer contents as a slice valid until the next
    /// call on this searcher.
    pub fn distances_scratch(
        &mut self,
        query: &[f32],
        rows: &[(u64, Option<Vec<f32>>)],
        cancel: Option<&CancelFn>,
    ) -> Result<&[Candidate]> {
        compute_distances_into(
            query,
            rows.iter().map(|(id, v)| (*id, v.as_deref())),
            self.metric,
            &mut self.scratch,
            cancel,
        )?;
        Ok(&self.scratch)
    }
}

/// Top-k for each of `queries` over the shared `rows`, spread across worker
/// threads (Phase 36 in code).
///
/// Semantics are identical to [`BatchSearcher::search_many`]: no `N × M`
/// distance matrix, deterministic `(distance, row_id)` order per query, and
/// output position `i` always holds query `i`'s hits. Work splits by query
/// (one contiguous chunk per thread), so setup (dispatch, codebooks are the
/// caller's) amortizes exactly as in the serial path. Thread count is
/// `min(queries, max_threads, parallelism)` with at least one thread;
/// `max_threads == 0` means "use the runtime default".
///
/// `CancelFn` is `Send + Sync`, so one cancellation flag serves all threads.
///
/// NUMA (Gap G): this entry point auto-detects the single-machine topology
/// and delegates to [`search_many_parallel_numa`], so every batch dispatch
/// is NUMA-aware on the live path. Results are identical in every NUMA mode.
pub fn search_many_parallel(
    queries: &[Vec<f32>],
    rows: &[(u64, Option<Vec<f32>>)],
    metric: SearchMetric,
    k: usize,
    cancel: Option<&CancelFn>,
    max_threads: usize,
) -> Result<Vec<Vec<Candidate>>> {
    let topology = crate::numa::NumaTopology::detect();
    search_many_parallel_numa(
        queries,
        rows,
        metric,
        k,
        cancel,
        max_threads,
        &topology,
        None,
    )
}

/// NUMA-aware batch dispatch over an explicit topology (Gap G live path).
///
/// Identical semantics to [`search_many_parallel`]; the topology only sizes
/// worker chunks (contiguous query ranges per worker, concatenated in
/// order). Pass `metrics` to record the NUMA dispatch in existing
/// diagnostics. Correctness is identical regardless of NUMA mode: the same
/// heaps, the same `(distance, row_id)` order, no shared mutation.
#[allow(clippy::too_many_arguments)]
pub fn search_many_parallel_numa(
    queries: &[Vec<f32>],
    rows: &[(u64, Option<Vec<f32>>)],
    metric: SearchMetric,
    k: usize,
    cancel: Option<&CancelFn>,
    max_threads: usize,
    topology: &crate::numa::NumaTopology,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> Result<Vec<Vec<Candidate>>> {
    check_k(k)?;
    if queries.is_empty() {
        return Ok(Vec::new());
    }
    for query in queries {
        super::check_query(query)?;
    }
    // Gap G: worker sizing comes from the NUMA policy (topology-aware,
    // deterministic, capped). `max_threads == 0` means "runtime default".
    let policy = crate::numa::NumaPolicy::select(topology, queries.len(), max_threads);
    if let Some(m) = metrics {
        policy.observe(m);
    }
    let threads = policy.workers.max(1);
    if threads == 1 {
        let mut searcher = BatchSearcher::new(metric, k)?;
        let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
        return searcher.search_many(&borrowed, rows, cancel);
    }
    let mode = metric.default_mode();
    let chunks = policy.plan_chunks(queries.len());
    let mut out: Vec<Vec<Candidate>> = Vec::with_capacity(queries.len());
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (start, end) in chunks {
            let query_chunk = &queries[start..end];
            handles.push(scope.spawn(move || -> Result<Vec<Vec<Candidate>>> {
                // Each thread owns its heaps and returns its chunk; the
                // main thread concatenates chunks in order, so no shared
                // mutation (and no `unsafe`) is needed anywhere.
                let mut local = Vec::with_capacity(query_chunk.len());
                for (i, query) in query_chunk.iter().enumerate() {
                    check_cancel(cancel, i)?;
                    let mut heap = TopKHeap::new(k, mode);
                    for (row_id, vec) in rows.iter() {
                        let Some(d) = super::distance_for_metric(metric, query, vec.as_deref())?
                        else {
                            continue;
                        };
                        heap.push(*row_id, d);
                    }
                    local.push(heap.into_sorted_vec());
                }
                Ok(local)
            }));
        }
        for handle in handles {
            let mut local = handle.join().map_err(|_| super::SearchError::Cancelled)??;
            out.append(&mut local);
        }
        Ok::<(), super::SearchError>(())
    })?;
    Ok(out)
}
