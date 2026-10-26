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

//! Pull-based lazy result streaming.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::config::StreamConfig;
use crate::error::StreamError;
use crate::policy::StreamPolicy;

/// Boxed row source. `Err` items are terminal: rows already buffered are
/// delivered first, then the error surfaces (never silently dropped).
type RowSource<T> = Box<dyn Iterator<Item = Result<T, StreamError>> + Send + 'static>;

/// Pull-based incremental iterator over query results.
///
/// Built from any `Iterator<Item = T>` — the executor later plugs in its own
/// row type with no dependency in the other direction. The source iterator is
/// consumed **lazily**: each [`ResultStream::next_batch`] pulls at most one
/// batch worth of rows, so a 1M+ row result never requires full
/// materialization.
///
/// Batching vs. limiting: `batch_size` (from [`StreamConfig`]) bounds the
/// transport quantum of every batch; `limit` is the only row cap and is an
/// explicit user-level operator. A stream with `limit: None` runs to source
/// exhaustion.
///
/// ```
/// use plomid_stream::{ResultStream, StreamConfig};
///
/// let mut stream = ResultStream::new(0u64..10);
/// let batch = stream.next_batch(4).unwrap().unwrap();
/// assert_eq!(batch, vec![0, 1, 2, 3]);
/// ```
pub struct ResultStream<T> {
    source: Option<RowSource<T>>,
    batch_size: usize,
    limit: Option<u64>,
    policy: StreamPolicy<T>,
    yielded: u64,
    cancelled: Arc<AtomicBool>,
    pending_error: Option<StreamError>,
    closed: bool,
}

impl<T> ResultStream<T> {
    /// Stream all rows of `source` with default batching and no limit.
    pub fn new(source: impl Iterator<Item = T> + Send + 'static) -> Self
    where
        T: 'static,
    {
        Self::with_config(source, StreamConfig::default())
    }

    /// Stream all rows of `source` with the given batching/limit config.
    pub fn with_config(
        source: impl Iterator<Item = T> + Send + 'static,
        config: StreamConfig,
    ) -> Self
    where
        T: 'static,
    {
        Self::with_policy(source, config, StreamPolicy::allow_all())
    }

    /// Stream `source` with config plus a row-authorization predicate.
    /// Rows rejected by the policy are skipped, never delivered.
    pub fn with_policy(
        source: impl Iterator<Item = T> + Send + 'static,
        config: StreamConfig,
        policy: StreamPolicy<T>,
    ) -> Self
    where
        T: 'static,
    {
        let flag = Arc::new(AtomicBool::new(false));
        Self::with_shared_cancel(source, config, policy, flag)
    }

    /// Stream a fallible row source. The first `Err` aborts the stream with
    /// [`StreamError::Upstream`]; rows already buffered are delivered first so
    /// failures are never silent truncation.
    pub fn from_fallible<E>(
        source: impl Iterator<Item = Result<T, E>> + Send + 'static,
        config: StreamConfig,
        policy: StreamPolicy<T>,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
        T: 'static,
    {
        Self::assemble(
            Box::new(source.map(|item| item.map_err(StreamError::upstream))),
            config,
            policy,
            Arc::new(AtomicBool::new(false)),
        )
    }

    /// Share an external cancellation flag (used by the pipelined producer so
    /// one `cancel()` stops both the handle and the worker thread).
    pub(crate) fn with_shared_cancel(
        source: impl Iterator<Item = T> + Send + 'static,
        config: StreamConfig,
        policy: StreamPolicy<T>,
        cancelled: Arc<AtomicBool>,
    ) -> Self
    where
        T: 'static,
    {
        Self::assemble(Box::new(source.map(Ok)), config, policy, cancelled)
    }

    fn assemble(
        source: RowSource<T>,
        config: StreamConfig,
        policy: StreamPolicy<T>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            source: Some(source),
            batch_size: config.batch_size(),
            limit: config.limit(),
            policy,
            yielded: 0,
            cancelled,
            pending_error: None,
            closed: false,
        }
    }

    /// Pull up to `max_rows` rows (bounded additionally by `batch_size`).
    /// A `max_rows` of `0` requests the configured `batch_size` quantum.
    ///
    /// Returns `Ok(None)` on exhaustion — either the source ran out or an
    /// explicit limit was reached. Returns `Err` on cancellation, close, or
    /// upstream failure; never panics and never silently truncates.
    pub fn next_batch(&mut self, max_rows: usize) -> Result<Option<Vec<T>>, StreamError> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StreamError::Cancelled);
        }
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if let Some(limit) = self.limit {
            if self.yielded >= limit {
                return Ok(None);
            }
        }
        let cap = if max_rows == 0 {
            self.batch_size
        } else {
            max_rows.min(self.batch_size)
        };
        let source = match self.source.as_mut() {
            Some(source) => source,
            None => return Ok(None),
        };
        let mut batch = Vec::with_capacity(cap.min(8192));
        for item in source.by_ref() {
            let row = match item {
                Ok(row) => row,
                Err(error) => {
                    if batch.is_empty() {
                        return Err(error);
                    }
                    // Deliver what we buffered first; surface the failure on
                    // the next call so no observed row is ever lost.
                    self.pending_error = Some(error);
                    break;
                }
            };
            if !self.policy.authorize(&row) {
                continue;
            }
            batch.push(row);
            self.yielded += 1;
            if let Some(limit) = self.limit {
                if self.yielded >= limit {
                    break;
                }
            }
            if batch.len() >= cap {
                break;
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }

    /// Drain the stream, invoking `f` per batch. Returns rows delivered.
    /// Stops early on cancellation, close, or upstream failure.
    pub fn for_each_batch(&mut self, mut f: impl FnMut(Vec<T>)) -> Result<u64, StreamError> {
        let start = self.yielded;
        while let Some(batch) = self.next_batch(self.batch_size)? {
            f(batch);
        }
        Ok(self.yielded - start)
    }

    /// Explicit user-level row cap (SQL `LIMIT`): deliver at most `n` rows
    /// total. Narrows any limit already in the config; never truncates when
    /// unset. Consuming `self` makes the operator visible at the call site.
    pub fn take(mut self, n: u64) -> Self {
        self.limit = Some(match self.limit {
            Some(current) => current.min(n),
            None => n,
        });
        self
    }

    /// Signal cancellation. Checked on every batch; shared with pipelined
    /// workers and cursors so one call stops the whole pipeline.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Release the source iterator and mark the stream closed. Idempotent:
    /// rows already delivered are unaffected.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.source = None;
        self.pending_error = None;
    }

    /// Access the shared cancellation flag (pipeline/cursor wiring).
    pub fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    /// True after [`ResultStream::close`].
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// True after [`ResultStream::cancel`].
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Rows delivered so far.
    pub fn position(&self) -> u64 {
        self.yielded
    }

    /// Explicit user-level row cap, if any.
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Transport quantum bounding every batch.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}
