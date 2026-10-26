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
//! Read-mostly concurrent wrapper for any [`VectorIndex`](super::VectorIndex).
//!
//! Concurrency model (Phase 37 in code):
//!
//! ```text
//! many readers      -> shared read lock, never block each other
//! reader + writer   -> writer excludes new readers for the mutation only
//! many writers      -> serialized on the write lock (no torn graphs)
//! search + compact  -> same as reader + writer (compaction is a write)
//! checkpoint + read -> checkpoint (`flush`/`encode`) takes the read lock;
//!                      callers must quiesce writers first for a snapshotted
//!                      image, otherwise the image is merely valid, not pinned
//! ```
//!
//! The wrapper owns no vectors and performs no MVCC filtering: row
//! visibility stays with the caller (executor), exactly as with the inner
//! index. Poisoned locks fail closed with [`VectorError::IndexCorrupted`]
//! rather than panicking the query path.

use std::sync::{RwLock, RwLockReadGuard};

use crate::{Result, VectorError};

use super::{IndexStats, RowId, SearchResult, VectorIndex};

/// Lock-poison helper: a poisoned lock means a previous holder panicked
/// mid-mutation, so the structure may be torn — fail closed.
fn poisoned(context: &'static str) -> VectorError {
    VectorError::IndexCorrupted(format!("concurrent index lock poisoned during {context}"))
}

/// A [`VectorIndex`] safe to share across threads.
///
/// `I` is usually `HnswIndex`, `FlatIndex`, `IvfFlatIndex`, or any future
/// implementation. Reads (`search`, `range_search`, `stats`, `validate`)
/// proceed concurrently; mutations (`build`, `insert`, `delete`, `flush`,
/// `compact`) exclude each other and new readers for their duration.
#[derive(Debug)]
pub struct ConcurrentIndex<I> {
    inner: RwLock<I>,
}

impl<I> ConcurrentIndex<I> {
    /// Wraps an already-built (or empty) index.
    pub fn new(index: I) -> Self {
        Self {
            inner: RwLock::new(index),
        }
    }

    /// Runs `f` under the read lock (for `flush`/`encode`-style snapshots
    /// that need a stable view without mutating).
    pub fn read_with<T>(&self, f: impl FnOnce(&I) -> T) -> Result<T> {
        let guard = self.inner.read().map_err(|_| poisoned("read"))?;
        Ok(f(&guard))
    }

    /// Unwraps the inner index. Fails closed when other holders exist.
    pub fn into_inner(self) -> Result<I> {
        self.inner.into_inner().map_err(|_| poisoned("unwrap"))
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, I>> {
        self.inner.read().map_err(|_| poisoned("read"))
    }
}

impl<I: VectorIndex> VectorIndex for ConcurrentIndex<I> {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        // `build` replaces all contents: exclusive access is required, and
        // `&mut self` already guarantees no other holder aliases us.
        self.inner
            .get_mut()
            .map_err(|_| poisoned("build"))?
            .build(rows)
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        self.inner
            .get_mut()
            .map_err(|_| poisoned("insert"))?
            .insert(row_id, vector)
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        self.inner
            .get_mut()
            .map_err(|_| poisoned("delete"))?
            .delete(row_id)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.read()?.search(query, k)
    }

    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>> {
        self.read()?.range_search(query, radius)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.get_mut().map_err(|_| poisoned("flush"))?.flush()
    }

    fn validate(&self) -> Result<()> {
        self.read()?.validate()
    }

    fn stats(&self) -> IndexStats {
        self.read().map(|g| g.stats()).unwrap_or_default()
    }
}
