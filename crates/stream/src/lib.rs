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

//! Incremental query-result streaming.
//!
//! # Ownership
//!
//! Owns result transport (batching, backpressure, delivery) generic over
//! the row type. Never owns rows, execution, or sessions — the executor
//! plugs those in and owns their lifetimes.
//!
//! This crate provides the transport layer between query execution and result
//! delivery. It is deliberately generic over the row type `T` (the executor
//! later plugs in its own row representation): nothing here depends on executor
//! internals.
//!
//! # Design rules
//!
//! * **No implicit truncation.** There is no maximum-result-rows constant
//!   anywhere in this crate. A stream runs until its source iterator is
//!   exhausted unless the caller sets an explicit user-level
//!   [`StreamConfig::limit`] (or [`ResultStream::take`]). `batch_size` is a
//!   transport quantum and is orthogonal to `limit`.
//! * **Lazy sources.** [`ResultStream`] pulls from its source iterator only as
//!   batches are requested; it never materializes the full result.
//! * **Bounded buffering.** [`PipelinedStream`] moves batches from a producer
//!   thread to the consumer through a bounded
//!   [`std::sync::mpsc::sync_channel`]. A slow consumer makes the producer
//!   block instead of growing memory without bound. Occupancy, blocked time,
//!   and drops are reported via [`BackpressureController`].
//! * **Prompt cancellation.** [`ResultStream::cancel`] (and
//!   [`PipelinedStream::cancel`]) set a shared atomic flag checked on every
//!   batch; the producer thread exits promptly and [`PipelinedStream::close`]
//!   drains the channel and joins the thread so no thread leaks.
//! * **Row authorization hook.** [`StreamPolicy`] carries an optional
//!   `authorize_row` predicate. Rows rejected by the predicate are skipped and
//!   never buffered, counted, or delivered. The executor **must** install its
//!   RLS predicate here: this crate cannot enforce row-level security on its
//!   own because it never sees table ACLs.
//! * **Chunked large values.** [`ChunkedValue`] slices `bytea`/`text`/`jsonb`
//!   canonical bytes into bounded (default 64 KiB) chunks so only the active
//!   chunk is in RAM. It borrows the payload; callers must stream chunks, not
//!   collect them.
//! * **Structured errors.** [`StreamError`] is returned, never panicked, and
//!   never silently truncated: `Cancelled`, `Closed`, `BufferExceeded`, and
//!   `Upstream` (boxed source failure). There is intentionally no
//!   "limit reached" error: reaching an explicit limit looks exactly like
//!   source exhaustion (`Ok(None)`).

pub mod backpressure;
pub mod chunk;
pub mod config;
pub mod cursor;
pub mod error;
pub mod pipelined;
pub mod policy;
pub mod stream;

pub use backpressure::{BackpressureController, BackpressureMetrics};
pub use chunk::ChunkedValue;
pub use config::{StreamConfig, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BUFFERED_BATCHES};
pub use cursor::Cursor;
pub use error::StreamError;
pub use pipelined::PipelinedStream;
pub use policy::{AuthorizeRow, StreamPolicy};
pub use stream::ResultStream;
