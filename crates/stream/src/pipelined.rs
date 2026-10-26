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

//! Threaded result pipeline with bounded backpressure.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::backpressure::{BackpressureController, BackpressureMetrics};
use crate::config::StreamConfig;
use crate::error::StreamError;
use crate::policy::StreamPolicy;
use crate::stream::ResultStream;

/// Messages crossing the bounded producer/consumer queue.
enum PipeMsg<T> {
    Batch(Vec<T>),
    Failure(StreamError),
}

/// Pull-based result stream fed by a background producer thread.
///
/// `spawn` moves the source iterator onto a producer thread that batches rows
/// (applying the [`StreamPolicy`] and explicit limit exactly like
/// [`ResultStream`]) and pushes batches through a bounded
/// [`std::sync::mpsc::sync_channel`] sized by the effective buffer capacity.
/// When the consumer is slow, the channel fills and the producer blocks inside
/// `send` — memory never grows without bound. Blocked time is reported via
/// [`PipelinedStream::metrics`].
///
/// Cancellation is prompt: [`PipelinedStream::cancel`] sets a flag the producer
/// checks on every batch, so it exits even mid-scan. [`PipelinedStream::close`]
/// (also run from `Drop`) drains the channel and joins the thread, so no
/// thread leaks whether the source is finite or unbounded.
pub struct PipelinedStream<T> {
    rx: Option<Receiver<PipeMsg<T>>>,
    controller: Arc<BackpressureController>,
    cancelled: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    position: u64,
    failed: bool,
    closed: bool,
}

impl<T: Send + 'static> PipelinedStream<T> {
    /// Spawn the producer thread over `source` with the given config/policy.
    pub fn spawn(
        source: impl Iterator<Item = T> + Send + 'static,
        config: StreamConfig,
        policy: StreamPolicy<T>,
    ) -> Self {
        let capacity = config.effective_capacity::<T>();
        let controller = Arc::new(BackpressureController::new(capacity));
        let cancelled = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::sync_channel::<PipeMsg<T>>(capacity);
        let mut worker =
            ResultStream::with_shared_cancel(source, config, policy, Arc::clone(&cancelled));
        let worker_controller = Arc::clone(&controller);
        let worker_cancelled = Arc::clone(&cancelled);
        let worker_done = Arc::clone(&done);
        let handle = thread::spawn(move || {
            Self::run_producer(
                &mut worker,
                &tx,
                &worker_controller,
                &worker_cancelled,
                &worker_done,
            );
        });
        Self {
            rx: Some(rx),
            controller,
            cancelled,
            done,
            handle: Some(handle),
            position: 0,
            failed: false,
            closed: false,
        }
    }

    fn run_producer(
        worker: &mut ResultStream<T>,
        tx: &SyncSender<PipeMsg<T>>,
        controller: &BackpressureController,
        cancelled: &AtomicBool,
        done: &AtomicBool,
    ) {
        loop {
            if cancelled.load(Ordering::Acquire) || done.load(Ordering::Acquire) {
                break;
            }
            match worker.next_batch(worker.batch_size()) {
                Err(error) => {
                    // Terminal failure (cancel observed or upstream error):
                    // forward once so the consumer sees it, then exit.
                    let _ = tx.send(PipeMsg::Failure(error));
                    break;
                }
                Ok(None) => break,
                Ok(Some(batch)) => {
                    // Reserve before the blocking send (see controller docs):
                    // a fast consumer must never observe the batch before it
                    // is counted.
                    controller.reserve_slot();
                    let started = Instant::now();
                    match tx.send(PipeMsg::Batch(batch)) {
                        Ok(()) => {
                            controller.confirm_send(started.elapsed().as_nanos() as u64);
                        }
                        Err(_) => {
                            // Consumer is gone; release the reservation and
                            // account the batch as never delivered.
                            controller.release_slot();
                            controller.notify_drop(1);
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl<T> PipelinedStream<T> {
    /// Pull the next batch (`batch_size` rows max). `Ok(None)` means the
    /// source was exhausted or the explicit limit was reached.
    pub fn next_batch(&mut self) -> Result<Option<Vec<T>>, StreamError> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StreamError::Cancelled);
        }
        let rx = match self.rx.as_ref() {
            Some(rx) => rx,
            None => return Ok(None),
        };
        match rx.recv() {
            Ok(PipeMsg::Batch(batch)) => {
                self.controller.notify_receive();
                self.position += batch.len() as u64;
                Ok(Some(batch))
            }
            Ok(PipeMsg::Failure(error)) => {
                self.failed = true;
                self.controller.notify_receive();
                Err(error)
            }
            Err(_) => {
                // Producer exited without a terminal error: exhaustion.
                Ok(None)
            }
        }
    }

    /// Cancel delivery. The producer observes the flag on its next batch
    /// boundary and exits; queued-but-undelivered batches are accounted as
    /// drops at [`PipelinedStream::close`].
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Stop the producer, drain the queue, and join the worker thread.
    /// Prompt even for unbounded sources; idempotent.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.done.store(true, Ordering::Release);
        if let Some(rx) = self.rx.take() {
            // Drain so a producer blocked in `send` can complete and observe
            // `done`. Drained batches were produced but never delivered.
            let mut drained = 0u64;
            while let Ok(msg) = rx.recv() {
                self.controller.notify_receive();
                if matches!(msg, PipeMsg::Batch(_)) {
                    drained += 1;
                }
            }
            self.controller.notify_drop(drained);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Snapshot queue pressure and drop accounting.
    pub fn metrics(&self) -> BackpressureMetrics {
        self.controller.metrics()
    }

    /// True while the producer thread has not exited yet.
    pub fn is_running(&self) -> bool {
        match &self.handle {
            Some(handle) => !handle.is_finished(),
            None => false,
        }
    }

    /// Rows delivered to the consumer so far.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// True after [`PipelinedStream::close`].
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// True after [`PipelinedStream::cancel`].
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// True after a terminal upstream failure was delivered.
    pub fn has_failed(&self) -> bool {
        self.failed
    }
}

impl<T> Drop for PipelinedStream<T> {
    /// Guarantee no thread leak: stopping the producer and joining is safe
    /// even if the caller never called `close`.
    fn drop(&mut self) {
        self.close();
    }
}
