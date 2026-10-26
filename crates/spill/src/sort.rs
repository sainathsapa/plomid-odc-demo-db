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

//! External sort: bounded in-memory buffer, sorted runs on disk, k-way merge.
//!
//! [`ExternalSort`] buffers rows up to `mem_limit_rows`, spills each full
//! buffer as one sorted run via [`SpillManager`], and merges runs on output
//! with a heap holding only one head row per run. Input of 10x (or more) the
//! memory limit is handled: ingest memory stays under one buffer plus framing,
//! and merge memory is `O(num_runs)` heads plus whatever the caller retains.

use crate::codec::SpillEncode;
use crate::error::{Result, SpillError};
use crate::manager::{RunReader, SpillManager, SpillRun};
use crate::{cancelled, ChargeFn};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

struct HeapItem<T> {
    value: T,
    run: usize,
}

impl<T: Ord> PartialEq for HeapItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Ord> Eq for HeapItem<T> {}
impl<T: Ord> PartialOrd for HeapItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Ord> Ord for HeapItem<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

/// Bounded-memory external sort over `T`.
///
/// Row estimate charged to `charge_fn` per buffered row is
/// `encoded_len + 32` bytes of overhead; freed on spill/drop of the buffer.
pub struct ExternalSort<T> {
    buffer: Vec<T>,
    buffered_charge: i64,
    runs: Vec<SpillRun>,
    total_rows: u64,
    mem_limit_rows: usize,
    manager: SpillManager,
    cancel: Arc<AtomicBool>,
    charge: ChargeFn,
    peak_rows: usize,
    peak_bytes: u64,
    spill_bytes: u64,
}

impl<T: Ord + Clone + SpillEncode> ExternalSort<T> {
    /// Create with an in-memory budget of `mem_limit_rows` rows.
    pub fn new(manager: SpillManager, mem_limit_rows: usize) -> Result<Self> {
        if mem_limit_rows == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "external sort needs mem_limit_rows >= 1".to_string(),
            ));
        }
        Ok(Self {
            buffer: Vec::new(),
            buffered_charge: 0,
            runs: Vec::new(),
            total_rows: 0,
            mem_limit_rows,
            manager,
            cancel: Arc::new(AtomicBool::new(false)),
            charge: crate::noop_charge(),
            peak_rows: 0,
            peak_bytes: 0,
            spill_bytes: 0,
        })
    }

    /// Attach a cancellation flag checked on every push/spill/merge step.
    pub fn with_cancel(mut self, cancel: Arc<AtomicBool>) -> Self {
        self.cancel = cancel;
        self
    }

    /// Attach a memory-accounting hook (`+bytes` on alloc, `-bytes` on free).
    pub fn with_charge(mut self, charge: ChargeFn) -> Self {
        // Re-charge current buffer under the new hook is the caller's choice;
        // the default hook is a no-op so swapping is lossless.
        self.charge = charge;
        self
    }

    /// Push one row; spills a sorted run when the buffer fills.
    pub fn push(&mut self, value: T) -> Result<()> {
        cancelled(&self.cancel)?;
        let est = value.encode().len() as i64 + 32;
        self.buffer.push(value);
        self.buffered_charge += est;
        (self.charge)(est);
        self.total_rows += 1;
        if self.buffer.len() > self.peak_rows {
            self.peak_rows = self.buffer.len();
            self.peak_bytes = self.buffered_charge.max(0) as u64;
        }
        if self.buffer.len() >= self.mem_limit_rows {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Extend from an iterator, checking cancellation per row.
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) -> Result<()> {
        for v in iter {
            self.push(v)?;
        }
        Ok(())
    }

    fn spill_run(&mut self) -> Result<()> {
        cancelled(&self.cancel)?;
        self.buffer.sort();
        let mut run = self.manager.create_run()?;
        for v in self.buffer.drain(..) {
            cancelled(&self.cancel)?;
            let bytes = v.encode();
            run.write_record(&bytes)?;
        }
        run.flush()?;
        self.spill_bytes += run.len();
        self.runs.push(run);
        (self.charge)(-self.buffered_charge);
        self.buffered_charge = 0;
        Ok(())
    }

    /// Total rows pushed.
    pub fn len(&self) -> u64 {
        self.total_rows
    }

    /// Whether any rows have been pushed.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }

    /// Whether any run spilled to disk.
    pub fn fell_back_to_disk(&self) -> bool {
        !self.runs.is_empty()
    }

    /// Cumulative spilled bytes for this operator.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Peak in-memory buffer estimate in bytes.
    pub fn peak_memory(&self) -> u64 {
        self.peak_bytes
    }

    /// Peak buffered row count.
    pub fn peak_rows(&self) -> usize {
        self.peak_rows
    }

    /// Sort the tail and return a streaming k-way merge over all runs.
    pub fn finish(mut self) -> Result<SortedIter<T>> {
        cancelled(&self.cancel)?;
        self.buffer.sort();
        let mut readers = Vec::with_capacity(self.runs.len());
        for run in self.runs.iter_mut() {
            cancelled(&self.cancel)?;
            readers.push(run.open_reader()?);
        }
        let mut it = SortedIter {
            heap: BinaryHeap::new(),
            readers,
            runs: std::mem::take(&mut self.runs),
            mem: std::mem::take(&mut self.buffer),
            mem_pos: 0,
            cancel: Arc::clone(&self.cancel),
            spilled: false,
            done: false,
        };
        it.spilled = !it.runs.is_empty();
        // Release this sort's buffer charge: ownership moved into the iterator.
        (self.charge)(-self.buffered_charge);
        it.prime()?;
        Ok(it)
    }

    /// Convenience: finish and collect every row into a sorted vector.
    pub fn finish_vec(self) -> Result<Vec<T>> {
        let mut it = self.finish()?;
        it.collect_vec()
    }
}

/// Streaming k-way merge over spilled runs plus the sorted in-memory tail.
///
/// `Item = Result<T, SpillError>`: decode/cancel/limit failures surface as an
/// item (fused afterwards) instead of panicking. Prefer [`SortedIter::next_result`]
/// when the caller wants `Result<Option<T>>` directly.
pub struct SortedIter<T> {
    heap: BinaryHeap<Reverse<HeapItem<T>>>,
    readers: Vec<RunReader>,
    // Kept alive so temp files outlive their readers.
    #[allow(dead_code)]
    runs: Vec<SpillRun>,
    mem: Vec<T>,
    mem_pos: usize,
    cancel: Arc<AtomicBool>,
    spilled: bool,
    done: bool,
}

impl<T: Ord + Clone + SpillEncode> SortedIter<T> {
    fn prime(&mut self) -> Result<()> {
        for idx in 0..self.readers.len() {
            cancelled(&self.cancel)?;
            if let Some(rec) = self.readers[idx].next_record()? {
                let v = T::decode(&rec)?;
                self.heap.push(Reverse(HeapItem { value: v, run: idx }));
            }
        }
        if self.mem_pos < self.mem.len() {
            let v = self.mem[self.mem_pos].clone();
            self.mem_pos += 1;
            self.heap.push(Reverse(HeapItem {
                value: v,
                run: usize::MAX,
            }));
        }
        Ok(())
    }

    /// Next row in sort order, or `None` when exhausted.
    pub fn next_result(&mut self) -> Result<Option<T>> {
        if self.done {
            return Ok(None);
        }
        cancelled(&self.cancel)?;
        let Some(Reverse(top)) = self.heap.pop() else {
            return Ok(None);
        };
        if top.run == usize::MAX {
            if self.mem_pos < self.mem.len() {
                let v = self.mem[self.mem_pos].clone();
                self.mem_pos += 1;
                self.heap.push(Reverse(HeapItem {
                    value: v,
                    run: usize::MAX,
                }));
            }
        } else if let Some(rec) = self.readers[top.run].next_record()? {
            let v = T::decode(&rec)?;
            self.heap.push(Reverse(HeapItem {
                value: v,
                run: top.run,
            }));
        }
        Ok(Some(top.value))
    }

    /// Collect the remainder (checks cancellation per row).
    pub fn collect_vec(&mut self) -> Result<Vec<T>> {
        let mut out = Vec::new();
        while let Some(v) = self.next_result()? {
            out.push(v);
        }
        Ok(out)
    }

    /// Whether the producing sort spilled to disk.
    pub fn fell_back_to_disk(&self) -> bool {
        self.spilled
    }
}

impl<T: Ord + Clone + SpillEncode> Iterator for SortedIter<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_result() {
            Ok(None) => None,
            Ok(Some(v)) => Some(Ok(v)),
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
