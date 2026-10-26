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

//! Bounded backpressure instrumentation for the producer/consumer queue.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::error::StreamError;

/// Point-in-time snapshot of queue pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackpressureMetrics {
    /// Configured bound on in-flight batches.
    pub capacity: usize,
    /// Batches currently in flight.
    pub buffered_batches: usize,
    /// Highest in-flight count observed. Always `<= capacity` for the
    /// pipelined stream: the bounded channel, not this counter, enforces it.
    pub peak_buffered_batches: usize,
    /// Total nanoseconds the producer spent blocked on a full queue.
    pub blocked_producer_nanos: u64,
    /// Batches produced but never delivered (cancelled or closed while queued).
    pub dropped_on_cancel: u64,
}

/// Tracks a bounded producer/consumer batch queue.
///
/// The queue itself is a [`std::sync::mpsc::sync_channel`]; this controller
/// only records what happens around it: occupancy, time the producer spent
/// blocked inside `send` on a full queue, and batches dropped without
/// delivery after cancel/close.
///
/// Occupancy counts batches the producer has **admitted** (reserved) but the
/// consumer has not yet received — including at most one batch the producer
/// holds while blocked in `send` on a full queue. The reservation happens
/// *before* the blocking send (reserving after the send would let a fast
/// consumer's receive slip between the send and the counter update and inflate
/// the count with phantoms). Consequence: `peak_buffered_batches` can reach
/// `capacity + 1` (full queue plus the producer's in-hand batch) and no
/// higher, so live batches are always strictly bounded.
///
/// `notify_*` / `reserve_*` hooks are public so external producer/consumer
/// integrations can report into the same metrics; [`crate::PipelinedStream`]
/// calls them internally.
#[derive(Debug)]
pub struct BackpressureController {
    capacity: usize,
    buffered: AtomicUsize,
    peak: AtomicUsize,
    blocked_nanos: AtomicU64,
    dropped: AtomicU64,
}

impl BackpressureController {
    /// Track a queue holding at most `max_buffered_batches` batches.
    /// Values of `0` are clamped to `1` (never panic).
    pub fn new(max_buffered_batches: usize) -> Self {
        Self {
            capacity: max_buffered_batches.max(1),
            buffered: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            blocked_nanos: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }

    /// Derive a capacity from a batch-count bound plus a byte budget:
    /// `min(max_buffered_batches, budget / avg_batch_bytes)`, at least 1.
    pub fn with_budget(
        max_buffered_batches: usize,
        budget_bytes: usize,
        avg_batch_bytes: usize,
    ) -> Self {
        let by_budget = budget_bytes / avg_batch_bytes.max(1);
        Self::new(max_buffered_batches.min(by_budget.max(1)))
    }

    /// Configured bound on in-flight batches.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Snapshot all metrics.
    pub fn metrics(&self) -> BackpressureMetrics {
        BackpressureMetrics {
            capacity: self.capacity,
            buffered_batches: self.buffered_batches(),
            peak_buffered_batches: self.peak_buffered_batches(),
            blocked_producer_nanos: self.blocked_producer_nanos(),
            dropped_on_cancel: self.dropped_on_cancel(),
        }
    }

    /// Batches currently in flight.
    pub fn buffered_batches(&self) -> usize {
        self.buffered.load(Ordering::Acquire)
    }

    /// Highest in-flight count observed.
    pub fn peak_buffered_batches(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }

    /// Total nanoseconds producers spent blocked on a full queue.
    pub fn blocked_producer_nanos(&self) -> u64 {
        self.blocked_nanos.load(Ordering::Relaxed)
    }

    /// Batches produced but never delivered.
    pub fn dropped_on_cancel(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Non-blocking admission probe: `Ok` when the queue has room, otherwise
    /// [`StreamError::BufferExceeded`]. The blocking producer path does not
    /// use this — it blocks on `send` instead so memory stays bounded.
    pub fn try_acquire(&self) -> Result<(), StreamError> {
        let buffered = self.buffered.load(Ordering::Acquire);
        if buffered >= self.capacity {
            Err(StreamError::BufferExceeded {
                buffered,
                capacity: self.capacity,
            })
        } else {
            Ok(())
        }
    }

    /// Record a batch entering the queue; `blocked_nanos` is how long the
    /// producer spent blocked inside `send` on a full queue. Convenience for
    /// a completed send: reserves the slot and confirms it in one call.
    pub fn notify_send(&self, blocked_nanos: u64) {
        self.reserve_slot();
        self.confirm_send(blocked_nanos);
    }

    /// Record a batch leaving the queue toward the consumer. Saturating:
    /// external misuse can never underflow the counter.
    pub fn notify_receive(&self) {
        self.release_slot();
    }

    /// Admit a batch: must be called *before* the (possibly blocking) send so
    /// the occupancy count cannot race with a fast consumer. At most one
    /// extra batch beyond `capacity` can be live (the producer's in-hand
    /// batch blocked on admission).
    pub fn reserve_slot(&self) {
        let in_flight = self.buffered.fetch_add(1, Ordering::AcqRel) + 1;
        self.peak.fetch_max(in_flight, Ordering::Release);
    }

    /// After a successful send, record how long it blocked on a full queue.
    pub fn confirm_send(&self, blocked_nanos: u64) {
        if blocked_nanos > 0 {
            self.blocked_nanos
                .fetch_add(blocked_nanos, Ordering::Relaxed);
        }
    }

    /// Release a reserved slot: the consumer received the batch, or a
    /// reserved send aborted (e.g. the consumer is gone). Saturating.
    pub fn release_slot(&self) {
        let _ = self
            .buffered
            .try_update(Ordering::AcqRel, Ordering::Acquire, |in_flight| {
                in_flight.checked_sub(1)
            });
    }

    /// Record batches produced but never delivered (cancel/close while queued).
    pub fn notify_drop(&self, batches: u64) {
        if batches > 0 {
            self.dropped.fetch_add(batches, Ordering::Relaxed);
        }
    }
}
