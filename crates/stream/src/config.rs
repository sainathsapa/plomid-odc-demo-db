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

//! Batching, buffering, and explicit-limit configuration.

/// Default transport quantum: rows per batch when the caller does not ask for
/// fewer. This is a delivery granularity knob, **not** a result cap.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Default bound on in-flight batches between the pipelined producer thread
/// and the consumer.
pub const DEFAULT_MAX_BUFFERED_BATCHES: usize = 4;

/// Batching and buffering knobs for [`crate::ResultStream`] and
/// [`crate::PipelinedStream`].
///
/// `batch_size` controls how many rows fit in one transport batch.
/// `limit` is the only row cap in this crate and is an explicit user-level
/// operator (SQL `LIMIT`): `None` means "stream to source exhaustion", never
/// truncate.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    batch_size: usize,
    max_buffered_batches: usize,
    buffer_budget_bytes: Option<usize>,
    limit: Option<u64>,
}

impl StreamConfig {
    /// Default config: 1000-row batches, at most 4 buffered batches, no limit.
    pub fn new() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            max_buffered_batches: DEFAULT_MAX_BUFFERED_BATCHES,
            buffer_budget_bytes: None,
            limit: None,
        }
    }

    /// Rows per transport batch. Values of `0` are clamped to `1` (never panic).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Bound on in-flight batches in the pipelined producer/consumer queue.
    /// Values of `0` are clamped to `1` (never panic).
    pub fn with_max_buffered_batches(mut self, max_buffered_batches: usize) -> Self {
        self.max_buffered_batches = max_buffered_batches.max(1);
        self
    }

    /// Optional advisory byte budget for in-flight batches. When set, the
    /// effective channel capacity is tightened below `max_buffered_batches`
    /// using `batch_size * size_of::<T>()` as the per-batch estimate. `None`
    /// (default) uses the batch-count bound only.
    pub fn with_buffer_budget_bytes(mut self, budget_bytes: usize) -> Self {
        self.buffer_budget_bytes = Some(budget_bytes);
        self
    }

    /// Explicit user-level row cap (SQL `LIMIT`). `None` streams to exhaustion.
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Remove any row cap: stream to source exhaustion.
    pub fn without_limit(mut self) -> Self {
        self.limit = None;
        self
    }

    /// Rows per transport batch.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Bound on in-flight batches (before any byte-budget tightening).
    pub fn max_buffered_batches(&self) -> usize {
        self.max_buffered_batches
    }

    /// Advisory byte budget for in-flight batches, if set.
    pub fn buffer_budget_bytes(&self) -> Option<usize> {
        self.buffer_budget_bytes
    }

    /// Explicit user-level row cap, if set.
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Bounded channel capacity for rows of type `T`: the batch-count bound,
    /// tightened by the byte budget when one is configured. Always `>= 1`.
    pub fn effective_capacity<T>(&self) -> usize {
        let count_bound = self.max_buffered_batches.max(1);
        match self.buffer_budget_bytes {
            None => count_bound,
            Some(budget) => {
                let per_batch = self
                    .batch_size
                    .saturating_mul(std::mem::size_of::<T>())
                    .max(1);
                count_bound.min((budget / per_batch).max(1))
            }
        }
    }
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self::new()
    }
}
