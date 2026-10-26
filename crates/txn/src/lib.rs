#![forbid(unsafe_code)]
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
//! Transaction lifecycle and locking foundations for PLOMID.
//!
//! # Ownership
//!
//! Owns transaction identity, commit ordering, write gates, the group-commit
//! barrier, and mount/demount orchestration (`PlomidStorageEngine` is the
//! single lifecycle authority). Never owns the version chains it installs
//! (mvcc), the log it appends (wal), or the image it applies (storage).
//!
//! # Global lock order
//!
//! Locks must always be acquired outer-to-inner in this order; never hold
//! an inner lock while acquiring an outer one. The order exists to make
//! deadlocks impossible across gates, engine, version store, registry, and
//! durability barrier. Canonical statement with rules and breakage modes:
//! `docs/v0.1.0/architecture/lock-ordering.mdx`.
//!
//! ```text
//! 1. engine mutex (`Arc<Mutex<PlomidStorageEngine>>`)
//!    - single-writer `&mut self`; callers serialize behind the mutex.
//! 2. versions mutex (`SharedVersionStore = Arc<Mutex<VersionStore>>`)
//!    - publish/scan paths lock versions while holding the engine mutex,
//!      never the reverse; the fast path takes versions alone and drops it
//!      before falling back to the engine.
//! 3. leaf mutexes (brief, never held while acquiring anything else):
//!    manager registry, gate-table map, per-gate state, WAL internals,
//!    observability maps, worker `inflight` set, maintenance claims.
//!
//! Wait rules (liveness, not mutex nesting): write-gate lanes are acquired
//! WITHOUT holding the engine mutex and stay held (logically) across
//! later engine acquisitions through publication; the first lane may block
//! while further domains use `try_acquire` + conflict instead of forming
//! cross-table wait cycles. The durability barrier is waited on with no
//! engine, versions, or gate state held.
//! ```

pub mod allocator;
pub mod backup;
pub mod checkpoint_policy;
pub mod concurrent;
pub mod engine_impl;
pub mod group_commit;
pub mod hot_row;
pub mod transaction;

pub use allocator::TransactionManager;
pub use checkpoint_policy::{
    CheckpointOutcome, CheckpointPolicy, CheckpointStats, CheckpointTrigger,
};
pub use concurrent::{ConcurrentPlomidStorageEngine, ConcurrentTransaction};
pub use engine_impl::{PlomidStorageEngine, ShutdownReport, ShutdownStage};
pub use hot_row::{row_key, HotRowStore, HotRowTransaction};
pub use plomid_core::CommitTimestamp;
pub use plomid_mvcc::Snapshot;
pub use plomid_storage::{SharedSnapshot, StorageEngine, StorageEngineTransaction};
pub use transaction::{
    CommitResult as TxnCommitResult, DataStore, LogStore, Transaction, TransactionState,
};
