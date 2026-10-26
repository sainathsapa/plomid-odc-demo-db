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
//! Concrete [`StorageEngine`](plomid_storage::StorageEngine) implementation
//! for PLOMID V1.
//!
//! [`PlomidStorageEngine`] coordinates a segment-backed
//! [`StorageManager`](plomid_storage::StorageManager), a segmented WAL, and a
//! [`TransactionManager`](plomid_txn::TransactionManager) to provide the
//! transaction-aware facade consumed by higher layers.
//!
//! # V1 Concurrency
//!
//! The engine is single-writer. All public methods that mutate state require
//! `&mut self`. Callers must serialize access (for example, behind a `Mutex`).
//!
//! # Recovery
//!
//! [`PlomidStorageEngine::open`] replays committed transactions from the WAL
//! into the data segments and reinitializes the `TransactionManager` from WAL state
//! so that transaction IDs and commit timestamps never reuse values across
//! restarts.
//!
//! # Single lifecycle authority
//!
//! This engine is the single mount/demount/recovery orchestrator:
//! [`PlomidStorageEngine::create`]/[`PlomidStorageEngine::open`] mount (the
//! checkpoint-aware `recover_storage` pipeline plus repairs, sweeps, WAL
//! open, and version bootstrap), and [`PlomidStorageEngine::shutdown`]
//! demounts (stop mutations → flush → checkpoint → sync, with an explicit
//! [`ShutdownStage`] trace). There is no second lifecycle manager: the
//! former `plomid-lifecycle` orchestrator was removed and its ordered-demount
//! logic merged here, so startup, crash recovery, shutdown, and restart
//! have exactly one implementation.

use plomid_core::{ErrorKind, Lsn, PlomidError, Result, TxnId};
use plomid_mvcc::{Snapshot, VersionStore, SCAN_CHUNK_ROWS};
use plomid_storage::{has_durable_state, StorageManager};
use plomid_storage::{CommitResult, SharedSnapshot, StorageEngine, StorageEngineTransaction};
use plomid_wal::{
    recover_storage, CheckpointReplayReport, DataOperation, DurabilityMode, SegmentedWal,
};
use std::path::Path;
use std::time::Instant;

use crate::checkpoint_policy::{
    CheckpointAccounting, CheckpointOutcome, CheckpointPolicy, CheckpointStats, CheckpointTrigger,
};
use crate::concurrent::TransactionGate;
use crate::group_commit::{
    GateTable, GroupDurability, RowGateTable, RowIdAllocator, UniqueGateTable,
};
use crate::{DataStore, LogStore, Transaction, TransactionManager};

/// Everything commit phase 1 produced while the engine mutex was held.
/// The concurrent facade carries this across publication and the durability
/// barrier without holding any lock: the page batch is consumed at
/// publication, the commit LSN at the barrier.
pub(crate) struct PreparedCommit {
    pub(crate) txn_id: TxnId,
    pub(crate) commit_timestamp: plomid_core::CommitTimestamp,
    pub(crate) commit_lsn: plomid_core::Lsn,
    /// False when the transaction carries no data records: no durability
    /// barrier is required (residual `BEGIN; COMMIT` handshakes).
    pub(crate) has_data: bool,
    /// Page mutations for the storage engine, in operation order.
    pub(crate) batch: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Write set for MVCC version installation (same order as `batch`).
    pub(crate) write_set: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

/// PLOMID V1 production storage engine.
///
/// Owns the B+Tree, WAL writer, and transaction ID / commit timestamp
/// allocator. Higher layers interact with this type through the
/// [`StorageEngine`] trait.
///
/// The engine also owns the MVCC [`VersionStore`]: every committed put/delete
/// installs a [`RowVersion`] (creator TxID + commit timestamp + payload or
/// tombstone) newest-first per key. Reads resolve through the caller's
/// snapshot via [`is_version_visible`], so concurrent and historical snapshots
/// observe the correct version.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Engine-shared MVCC version store. The mutex exists so a transaction handle
/// can install committed versions at commit; the engine remains single-writer.
///
/// Read paths hold this handle directly rather than the database mutex: while
/// the completeness invariant holds (see `versions_complete`) this store *is*
/// the committed read state, so a read needs nothing the engine holds.
pub type SharedVersionStore = Arc<Mutex<VersionStore>>;

/// Commits between opportunistic MVCC version-store GC passes.
///
/// `VersionStore::gc` walks every key chain, so running it per commit would
/// tax the hot path. Every this many commits (while holding the engine mutex
/// in `commit_publish`) the engine attempts one pass when no active
/// transaction pins a snapshot. Soak evidence: the store grew
/// without bound because nothing ever called `gc_below`.
const VERSION_GC_COMMIT_INTERVAL: u64 = 256;

/// Ordered stages of the engine demount sequence, in execution order.
///
/// `Close` is recorded when every prior stage succeeded; handle release
/// itself is the caller's `Drop`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownStage {
    /// Stop mutations: the gate begins rejecting new transactions.
    StopMutations,
    /// Flush required state (storage + WAL sync).
    Flush,
    /// Checkpoint: publish the durable WAL boundary (runtime path).
    Checkpoint,
    /// Final durability synchronization after checkpoint publication.
    Sync,
    /// Close: ordering complete, handles ready to drop.
    Close,
}

/// Outcome of [`PlomidStorageEngine::shutdown`].
#[derive(Clone, Debug)]
pub struct ShutdownReport {
    /// Demount stages completed, in execution order.
    pub stages: Vec<ShutdownStage>,
    /// The final shutdown checkpoint, if one ran.
    pub checkpoint: Option<CheckpointOutcome>,
    /// Wall-clock duration of the whole demount.
    pub elapsed: std::time::Duration,
}

pub struct PlomidStorageEngine {
    pub storage: StorageManager,
    /// Shared so connection-facing read paths can capture a committed
    /// snapshot without taking the database mutex. The manager is internally
    /// synchronized (one small registry lock plus atomics) and every method
    /// takes `&self`, so sharing it adds no serialization the engine did not
    /// already have; the database mutex is only needed for the storage and
    /// version-store fields.
    pub manager: Arc<TransactionManager>,
    pub wal: SegmentedWal,
    versions: SharedVersionStore,
    /// Group durability barrier coordinating WAL flushes across commits.
    group: Arc<GroupDurability>,
    /// Recovery report of the last mount/recovery, when this engine was opened.
    ///
    /// Produced by the authoritative checkpoint-aware recovery pipeline, so the
    /// recovery boundary, validation result, and crash-tail observation are
    /// observable to the SQL layer instead of being discarded at startup. A
    /// freshly created engine has no recovery report.
    recovery: Option<CheckpointReplayReport>,
    /// Serializes connection-local write transactions per statement domain
    /// (the target table). Statements on independent tables run concurrently;
    /// statements on the same table keep serialized read/modify/write
    /// semantics. Read-only transactions do not acquire any gate.
    pub(crate) write_gates: Arc<GateTable>,
    /// Per-row write gates for UPDATE/DELETE row-level locking, with orphan
    /// eviction so the map stays bounded by contended rows, not table size.
    pub(crate) row_gates: Arc<RowGateTable>,
    /// Active unique-key reservation gates (transient runtime coordination;
    /// the durable B+Tree unique index stays the authoritative uniqueness
    /// state), with orphan eviction so the map stays bounded by concurrently
    /// contended `(index, value)` domains, never by inserted history.
    pub(crate) unique_gates: Arc<UniqueGateTable>,
    /// Atomic per-table internal row-id allocation for reservation-based
    /// INSERTs (replaces the meta-key RMW that the table lane used to
    /// serialize). Seeded once per table from committed rows.
    pub(crate) row_ids: Arc<RowIdAllocator>,
    /// True once the in-memory [`VersionStore`] is known to hold every
    /// committed key present in storage.
    ///
    /// This is the completeness invariant the read paths rely on: while it is
    /// true, no read has to touch the storage image to discover committed
    /// rows, because the version store already mirrors them exactly. It is
    /// established by [`Self::bootstrap_versions`] at mount (one sequential
    /// pass over the committed image) and maintained by the commit path, which
    /// installs a version for every key it applies to storage. While it is
    /// false the read paths keep their conservative storage fallback, so the
    /// flag can only ever remove work that provably cannot change a result.
    ///
    /// Shared through an `Arc<AtomicBool>` so connection-facing read paths
    /// observe the transition without taking the database mutex: a reader that
    /// sees `true` may resolve reads from the version store alone, and a reader
    /// that sees `false` keeps the engine path, which bootstraps first.
    versions_complete: Arc<AtomicBool>,
    /// When an automatic checkpoint is due (WAL bytes, retained segments,
    /// maximum interval). Configurable through [`Self::set_checkpoint_policy`].
    checkpoint_policy: CheckpointPolicy,
    /// Counters the policy is evaluated against, updated on the commit path.
    checkpoint_accounting: CheckpointAccounting,
    /// Commits since the last opportunistic version-store GC attempt.
    version_gc_commits_since_last: u64,
    /// Lifetime versions reclaimed by opportunistic GC (diagnostics/tests).
    version_gc_total_reclaimed: u64,
    /// Set by [`Self::shutdown`]: new transactions are rejected while
    /// committed reads still resolve (drain-friendly demount).
    shutdown: bool,
    /// Demount stages completed so far, in execution order (observability
    /// for both clean shutdowns and shutdowns that failed partway).
    shutdown_trace: Vec<ShutdownStage>,
}

impl PlomidStorageEngine {
    /// Starts a transaction without borrowing the engine for its lifetime.
    ///
    /// The concurrent PGWire facade uses this to keep transaction ownership
    /// local to a connection while retaining this engine's authoritative
    /// transaction manager and WAL.
    pub fn begin_concurrent(&mut self) -> Result<(TxnId, Snapshot)> {
        // Demount gate: no new transactions once shutdown has begun.
        self.require_running("begin")?;
        // Automatic checkpointing, reached between transactions: the statement
        // that crosses a threshold pays a bounded one-off checkpoint instead of
        // letting WAL (and the next restart's replay) grow without bound.
        let _ = self.checkpoint_if_due();
        let perf = tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf");
        let started = Instant::now();
        let txn_id = self.manager.begin()?;
        let snapshot = self.manager.snapshot(txn_id)?;
        let snapshot_us = started.elapsed().as_micros() as u64;
        let append_started = Instant::now();
        let begin_lsn = match self.wal.append(
            plomid_wal::RecordType::Begin,
            &plomid_wal::encode_begin(txn_id),
        ) {
            Ok(lsn) => lsn,
            Err(error) => {
                let _ = self.manager.abort_txn(txn_id);
                return Err(error);
            }
        };
        // Pin the checkpoint horizon below this transaction's first record
        // until it publishes or aborts (see `oldest_unfinished_lsn`).
        // Registration cannot fail after a successful append.
        let _ = self.manager.register_begin(txn_id, begin_lsn.get());
        if perf {
            tracing::debug!(
                target: "plomid::perf",
                event = "mvcc_begin",
                txn_id = txn_id.get(),
                snapshot_us,
                wal_begin_append_us = append_started.elapsed().as_micros() as u64,
            );
        }
        Ok((txn_id, snapshot))
    }

    pub fn refresh_concurrent_snapshot(&self, txn_id: TxnId) -> Result<Snapshot> {
        self.manager.snapshot(txn_id)
    }

    /// Returns the write gate for a statement domain, creating it on first use.
    pub(crate) fn gate_for(&self, domain: &[u8]) -> Result<Arc<TransactionGate>> {
        self.write_gates.gate_for(domain)
    }

    /// Returns the write gate for a row key, creating it on first use.
    pub(crate) fn row_gate_for(&self, key: &[u8]) -> Result<Arc<TransactionGate>> {
        self.row_gates.gate_for(key)
    }

    /// Returns the unique-reservation gate for a conflict domain (already
    /// fully encoded by the caller), creating it on first use.
    pub(crate) fn unique_gate_for(&self, domain: &[u8]) -> Result<Arc<TransactionGate>> {
        self.unique_gates.gate_for(domain)
    }

    /// Allocates the next internal row id for `table`.
    ///
    /// The counter is seeded once per table from committed state: the highest
    /// numeric row suffix present in committed storage plus one. Committed
    /// rows are ground truth for "which ids exist"; because seeding happens
    /// before any reservation-holder INSERT stages a row on this table, the
    /// floor is always >= every id committed before this engine opened. The
    /// meta key written by the legacy transactional path (`ROWID_META_PREFIX`)
    /// is honored when present: it is re-derived from actual rows here, and
    /// the legacy path keeps writing it so a mixed fleet stays compatible.
    pub(crate) fn allocate_concurrent_rowid(&mut self, table: &str) -> Result<i64> {
        // Fast path: counter already seeded (the common case after the first
        // INSERT on a table).
        if let Some(previous) = self.row_ids.counter_for(table) {
            let next = self.row_ids.next(table, previous);
            return Ok(next);
        }
        // Seed once per table from committed state (max numeric row suffix);
        // committed rows are ground truth for which ids already exist.
        let prefix = format!("{table}:").into_bytes();
        let mut end = prefix.clone();
        end.push(0xff);
        let snapshot = self.committed_snapshot()?;
        let rows = self.scan_snapshot(Some(prefix.as_slice()), Some(end.as_slice()), &snapshot)?;
        let mut max_existing: i64 = 0;
        for (key, _) in rows {
            if let Some(suffix) = key.strip_prefix(prefix.as_slice()) {
                if let Ok(n) = std::str::from_utf8(suffix).unwrap_or("").parse::<i64>() {
                    if n > max_existing {
                        max_existing = n;
                    }
                }
            }
        }
        self.row_ids.seed(table, max_existing);
        Ok(self.row_ids.next(table, max_existing))
    }

    /// Shared handle to the group durability barrier.
    pub(crate) fn group_handle(&self) -> Arc<GroupDurability> {
        Arc::clone(&self.group)
    }

    /// Number of live per-row write gates (diagnostics/tests).
    ///
    /// Bounded by the rows currently locked or awaited, not by the number of
    /// rows ever written: a gate is evicted when its last guard drops and no
    /// waiter holds a clone.
    #[must_use]
    pub fn row_gate_count(&self) -> usize {
        self.row_gates.len()
    }

    /// Completes a prepared transaction under the storage write boundary.
    /// Callers prepare and encode the operation list outside this method.
    pub fn commit_concurrent(
        &mut self,
        txn_id: TxnId,
        operations: Vec<DataOperation>,
    ) -> Result<CommitResult> {
        let encoded = operations
            .iter()
            .map(|operation| plomid_wal::encode_data(txn_id, operation))
            .collect::<Result<Vec<_>>>()?;
        self.commit_concurrent_prepared(txn_id, operations, encoded)
    }

    /// Applies a transaction whose WAL payloads were prepared before entering
    /// the shared storage boundary.
    ///
    /// The three phases are also available separately
    /// ([`Self::commit_prepare`], [`Self::commit_publish`], the group
    /// barrier) so the concurrent facade can publish and wait for durability
    /// with NO locks held; this wrapper blocks the engine mutex through the
    /// flush and keeps the historical durable-before-publish order.
    pub fn commit_concurrent_prepared(
        &mut self,
        txn_id: TxnId,
        operations: Vec<DataOperation>,
        encoded: Vec<Vec<u8>>,
    ) -> Result<CommitResult> {
        let prepared = self.commit_prepare(txn_id, operations, encoded)?;
        if prepared.has_data {
            self.group.wait_durable(prepared.commit_lsn)?;
        }
        let result = self.commit_publish(prepared)?;
        // Automatic checkpoint policy, evaluated *after* the commit is durable
        // and published. A checkpoint failure is recorded, never propagated:
        // the statement's durability must not depend on maintenance work. This
        // wrapper holds the engine mutex through the flush, so it runs the
        // checkpoint inline; the concurrent facade (the server path) runs the
        // same check with no locks held.
        let _ = self.checkpoint_if_due();
        Ok(result)
    }

    /// Commit phase 1 (caller holds the engine mutex): reserve the commit
    /// timestamp, append the WAL batch, and build the write set. No fsync
    /// happens here, so the phase is short and CPU-only.
    pub(crate) fn commit_prepare(
        &mut self,
        txn_id: TxnId,
        operations: Vec<DataOperation>,
        encoded: Vec<Vec<u8>>,
    ) -> Result<PreparedCommit> {
        let perf = tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf");
        let commit_started = Instant::now();
        // Fail fast on entries no backend can store: a single B-tree entry
        // needs 22 framing bytes plus key and value inside one page payload,
        // and anything larger fails deterministically at apply time — AFTER
        // the WAL batch is already durable. Validating here keeps poison
        // commits out of the WAL entirely, so recovery can never replay a
        // commit the store cannot apply, and the error carries no side
        // effects (no timestamp consumed, no WAL appended, gates untouched).
        for operation in &operations {
            if let DataOperation::Put { key, value } = operation {
                if 22 + key.len() + value.len() > plomid_storage::PAGE_DATA_SIZE {
                    return Err(plomid_core::PlomidError::new(
                        plomid_core::ErrorKind::InvalidArgument,
                        "key/value pair is too large for a page",
                    ));
                }
            }
        }
        // Reserve the timestamp, but do not publish it until the WAL batch is
        // appended below. The inline wrapper path (`commit_concurrent_prepared`)
        // additionally waits out the durability barrier before publishing, so
        // there commit visibility never precedes durability. The concurrent
        // facade (`ConcurrentTransaction::commit_phases`) publishes first and
        // barriers after, so that same-row successors can proceed to the
        // shared group-commit flush; visibility still never precedes the
        // serial order (gates are released only at publication) and no client
        // is acknowledged before its own barrier completes, so the recovered
        // state is always a prefix of the published serial order containing
        // every acknowledged commit.
        let commit_timestamp = self.manager.prepare_commit()?;
        let commit_payload = plomid_wal::encode_commit_with_timestamp(txn_id, commit_timestamp);
        let mut records = encoded
            .iter()
            .map(|payload| (plomid_wal::RecordType::Data, payload.as_slice()))
            .collect::<Vec<_>>();
        records.push((plomid_wal::RecordType::Commit, commit_payload.as_slice()));
        let wal_append_started = Instant::now();
        let has_data = !encoded.is_empty();
        let wal_bytes: u64 = encoded
            .iter()
            .map(|payload| payload.len() as u64)
            .sum::<u64>()
            + commit_payload.len() as u64;
        let commit_lsn = self
            .wal
            .append_batch(&records)?
            .last()
            .copied()
            .ok_or_else(|| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "empty commit WAL batch",
                )
            })?;
        let wal_append_us = wal_append_started.elapsed().as_micros() as u64;
        if perf {
            tracing::debug!(
                target: "plomid::perf",
                event = "commit_prepare",
                txn_id = txn_id.get(),
                ops = encoded.len(),
                wal_bytes,
                wal_append_us,
                total_us = commit_started.elapsed().as_micros() as u64,
            );
        }
        // No byte accounting here: the policy reads the appended-byte count from
        // the WAL (`SegmentedWal::bytes_since_checkpoint`), so it also sees
        // writes that arrive through the transactional path instead of this one.
        let mut batch = Vec::with_capacity(operations.len());
        let mut write_set = Vec::with_capacity(operations.len());
        for operation in operations {
            match operation {
                DataOperation::Put { key, value } => {
                    batch.push((key.clone(), Some(value.clone())));
                    write_set.push((key, Some(value)));
                }
                DataOperation::Delete { key } => {
                    batch.push((key.clone(), None));
                    write_set.push((key, None));
                }
            }
        }
        Ok(PreparedCommit {
            txn_id,
            commit_timestamp,
            commit_lsn,
            has_data,
            batch,
            write_set,
        })
    }

    /// Commit publication (caller holds the engine mutex): apply the data
    /// pages, publish the timestamp, and install the MVCC versions. The WAL
    /// batch was appended before this call; on the inline wrapper path it is
    // also already durable, while the concurrent facade defers its
    // durability barrier until after publication (and after releasing the
    // write lanes) so contending commits can share one group-commit flush.
    // That reorder is safe because durability is a WAL-prefix property and
    // acknowledgements still wait for each commit's own barrier: recovery can
    // only ever drop an unacknowledged suffix of the published serial order.
    /// Ordering stays WAL appended -> data applied -> versions
    /// visible. The WAL is the durability authority; data pages are a
    /// checkpointed cache of it. Recovery replays the WAL after the newest
    /// checkpoint, and `checkpoint()` flushes data pages *before* it reclaims
    /// any WAL, so a per-commit data fsync is redundant and doubles commit
    /// latency. PostgreSQL likewise fsyncs only WAL at commit and leaves
    /// data-page flushing to the checkpointer.
    pub(crate) fn commit_publish(&mut self, prepared: PreparedCommit) -> Result<CommitResult> {
        let perf = tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf");
        let apply_started = Instant::now();
        self.storage.apply_batch(&prepared.batch)?;
        let apply_us = apply_started.elapsed().as_micros() as u64;
        let install_started = Instant::now();
        self.manager
            .publish_commit(prepared.txn_id, prepared.commit_timestamp)?;
        let publish_commit_us = install_started.elapsed().as_micros() as u64;
        let versions_started = Instant::now();
        self.install_committed(
            prepared.txn_id,
            prepared.commit_timestamp.get(),
            prepared.write_set,
        );
        let install_versions_us = versions_started.elapsed().as_micros() as u64;
        if perf {
            let install_us = install_started.elapsed().as_micros() as u64;
            tracing::debug!(
                target: "plomid::perf",
                event = "commit_publish",
                txn_id = prepared.txn_id.get(),
                ops = prepared.batch.len(),
                apply_us,
                install_us,
                publish_commit_us,
                install_versions_us,
            );
        }
        // Opportunistic MVCC GC: hot-key UPDATE/DELETE chains grow by one
        // version per commit and nothing else reclaims them. This
        // runs after publish+install on the same mutex, throttled, and only
        // when no active transaction pins a snapshot — so it can only drop
        // versions no future snapshot can observe. Fail-open like the
        // checkpoint path: GC must never fail a commit.
        self.maybe_gc_versions();
        Ok(CommitResult {
            txn_id: prepared.txn_id,
            commit_timestamp: prepared.commit_timestamp,
        })
    }

    /// One throttled opportunistic version-store GC pass, if safe.
    ///
    /// Safety: runs only when the active set is empty, with
    /// `horizon = last_committed`. Then `gc` keeps the newest version per key
    /// plus everything newer than the horizon (nothing can be newer than the
    /// maximum committed timestamp), i.e. exactly the newest version per key.
    /// Every future snapshot has `watermark >= horizon` and no active
    /// transaction to exclude, so the newest version is visible to all of
    /// them and older versions are unobservable. With any active transaction
    /// present the pass is skipped: its snapshot watermark is untracked, so
    /// no horizon is provably safe.
    fn maybe_gc_versions(&mut self) {
        self.version_gc_commits_since_last = self.version_gc_commits_since_last.saturating_add(1);
        if self.version_gc_commits_since_last < VERSION_GC_COMMIT_INTERVAL {
            return;
        }
        self.version_gc_commits_since_last = 0;
        let active_empty = self
            .manager
            .active_txns()
            .map(|active| active.is_empty())
            .unwrap_or(false);
        if !active_empty {
            return;
        }
        let horizon = match self.manager.last_committed() {
            Ok(horizon) => horizon,
            Err(_) => return,
        };
        let reclaimed = match self.versions.lock() {
            Ok(mut store) => store.gc(horizon),
            Err(_) => return,
        };
        self.version_gc_total_reclaimed = self
            .version_gc_total_reclaimed
            .saturating_add(reclaimed as u64);
        if reclaimed > 0 {
            tracing::debug!(
                target: "transaction",
                event = "version_gc",
                horizon,
                reclaimed,
            );
        }
    }

    /// Opportunistic version-GC counters: `(commits_since_last_pass,
    /// lifetime_reclaimed)`. Diagnostics/tests only.
    #[must_use]
    pub fn version_gc_stats(&self) -> (u64, u64) {
        (
            self.version_gc_commits_since_last,
            self.version_gc_total_reclaimed,
        )
    }

    /// One fail-open drained-segment reclamation pass after a durable checkpoint.
    ///
    /// Runs inline on the checkpoint path (no new threads): sealed segments
    /// proven empty by an exact descent and absent from every durable
    /// reference are unlinked and forgotten. This engine never publishes
    /// generation metadata, so the durable referenced set is empty and the
    /// pass reclaims exactly the drained files; the empty `pinned` set is
    /// exact here because no generation reader lease can exist on this image.
    /// Errors only log: an interrupted pass leaves orphan files the next
    /// pass (or the open-time sweep) reclaims.
    fn maybe_reclaim_drained_segments(&mut self) {
        match self
            .storage
            .reclaim_drained_segments(&std::collections::BTreeSet::new())
        {
            Ok(outcome) if !outcome.reclaimed.is_empty() => {
                tracing::debug!(
                    target: "storage",
                    event = "drained_segments_reclaimed",
                    reclaimed = outcome.reclaimed.len(),
                    reclaimed_bytes = outcome.reclaimed_bytes,
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(
                    target: "storage",
                    event = "drained_segment_reclaim_failed",
                    error = %error,
                );
            }
        }
    }

    pub fn abort_concurrent(&mut self, txn_id: TxnId) -> Result<()> {
        let lsn = self.wal.append(
            plomid_wal::RecordType::Abort,
            &plomid_wal::encode_abort(txn_id),
        )?;
        self.wal.commit(lsn)?;
        self.manager.abort_txn(txn_id)
    }

    pub fn abort_concurrent_preview(&mut self, txn_id: TxnId) -> Result<()> {
        tracing::debug!(target: "transaction", "preview_abort_in_memory txn_id={}", txn_id.get());
        self.manager.abort_txn(txn_id)
    }

    /// Creates a fresh storage instance.
    ///
    /// Creates the initial data segment and WAL segment. The returned engine
    /// contains no data and is ready for new transactions.
    ///
    /// Fails with [`ErrorKind::AlreadyExists`] when `storage_path` already
    /// holds a database. Creation starts a *new* WAL segment and runs no
    /// recovery, so opening an existing database through `create` would drop
    /// committed state that still lives only in the WAL (for example the
    /// index entries that enforce uniqueness). Use [`Self::open`] to recover
    /// an existing database.
    pub fn create(storage_path: &Path, wal_path: &Path, pool_capacity: usize) -> Result<Self> {
        Self::create_with_config(
            storage_path,
            wal_path,
            pool_capacity,
            plomid_core::DEFAULT_STORAGE_SEGMENT_SIZE_BYTES,
            plomid_core::DEFAULT_WAL_SEGMENT_SIZE_BYTES,
        )
    }

    pub fn create_with_config(
        storage_path: &Path,
        _wal_path: &Path,
        pool_capacity: usize,
        segment_size_bytes: u64,
        wal_segment_size_bytes: u64,
    ) -> Result<Self> {
        if has_durable_state(storage_path)? {
            return Err(plomid_core::PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                "a database already exists at this root",
                format!(
                    "root={} — open it to run recovery instead of creating over it",
                    storage_path.display()
                ),
            ));
        }
        let storage = StorageManager::create(storage_path, pool_capacity, segment_size_bytes)?;
        let manager = Arc::new(TransactionManager::new());
        let wal_dir = storage_path.join("wal");
        let wal = SegmentedWal::create(
            &wal_dir,
            wal_segment_size_bytes,
            plomid_wal::DurabilityMode::Force,
        )?;
        Ok(Self {
            storage,
            manager,
            wal,
            versions: Arc::new(Mutex::new(VersionStore::new())),
            group: Arc::new(GroupDurability::new(wal_dir)),
            recovery: None,
            write_gates: Arc::new(GateTable::new()),
            row_gates: Arc::new(RowGateTable::new()),
            unique_gates: Arc::new(UniqueGateTable::new()),
            row_ids: Arc::new(RowIdAllocator::new()),
            // Creation starts from empty storage, so the (empty) version store
            // already mirrors it: the completeness invariant holds immediately.
            versions_complete: Arc::new(AtomicBool::new(true)),
            checkpoint_policy: CheckpointPolicy::default(),
            checkpoint_accounting: CheckpointAccounting::new(),
            version_gc_commits_since_last: 0,
            version_gc_total_reclaimed: 0,
            shutdown: false,
            shutdown_trace: Vec::new(),
        })
    }

    /// Opens an existing storage instance and recovers committed transactions.
    ///
    /// If the WAL exists and is non-empty, committed transactions are replayed
    /// into the data segments and the `TransactionManager` is initialized from the
    /// WAL to prevent ID reuse. If the WAL is absent or empty, a fresh
    /// allocator is used.
    pub fn open(storage_path: &Path, wal_path: &Path, pool_capacity: usize) -> Result<Self> {
        // The single authority for both rotation sizes: the same constants the
        // lifecycle defaults use, so a server started here and a server started
        // through the lifecycle coordinator produce identical segment sizing.
        Self::open_with_config(
            storage_path,
            wal_path,
            pool_capacity,
            plomid_core::DEFAULT_STORAGE_SEGMENT_SIZE_BYTES,
            plomid_core::DEFAULT_WAL_SEGMENT_SIZE_BYTES,
        )
    }

    pub fn open_with_config(
        storage_path: &Path,
        _wal_path: &Path,
        pool_capacity: usize,
        segment_size_bytes: u64,
        wal_segment_size_bytes: u64,
    ) -> Result<Self> {
        // First boot: the data directory exists (the server creates it) but it
        // has never stored anything. Creation is only allowed for a root with no
        // durable physical state, so an existing database that fails to open is
        // reported instead of being rebuilt over — rebuilding could truncate the
        // pages of a segment that still holds data.
        // Startup is measured in phases so an operator can tell a slow mount
        // (physical open), expensive replay (WAL), or manager rebuild apart
        // without re-instrumenting the binary. `total_ms` is the number that
        // decides whether a restart is acceptable.
        let open_started = Instant::now();
        let mut storage =
            match StorageManager::open(storage_path, pool_capacity, segment_size_bytes) {
                Ok(storage) => storage,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    if has_durable_state(storage_path)? {
                        return Err(error);
                    }
                    StorageManager::create(storage_path, pool_capacity, segment_size_bytes)?
                }
                Err(error) => return Err(error),
            };
        let storage_open = open_started.elapsed();
        // RECOVERY: the authoritative checkpoint-aware pipeline. It validates
        // the physical format and WAL environment, selects and validates the
        // newest usable checkpoint, and replays only the WAL after that
        // boundary, returning a report rather than discarding the outcome. This
        // is the same pipeline the lifecycle mount path uses, so startup
        // recovery has exactly one implementation instead of a second,
        // checkpoint-blind replay island.
        let recovery_started = Instant::now();
        let recovery = recover_storage(storage_path, &mut storage)?;
        let recovery_time = recovery_started.elapsed();
        // CRASH-TAIL TRUNCATION: a torn frame at the end of the newest
        // segment is never durable. Recovery reports the safe boundary;
        // restoring the segment to it here (before the WAL writer and the
        // transaction manager open below) keeps one torn tail from failing
        // every subsequent open. Merged from the removed lifecycle mount.
        if let Some(boundary) = recovery.crash_tail_boundary {
            plomid_wal::truncate_crash_tail(&storage_path.join("wal"), boundary)?;
        }
        // ORPHAN SWEEP: a kill between a generation's files landing and the
        // catalog pointer advancing leaves that identity on disk, referenced
        // by nothing. The allocator already sizes past such orphans, but
        // without reclamation they leak permanently. Running one GC pass here
        // — single-threaded, no readers yet, checkpoints accounted — removes
        // exactly the unreachable files and nothing else (reachability is
        // computed from the just-recovered published state). Fresh roots have
        // no published state, which is a no-op, not an error. Fail-open with
        // a warning: the sweep is cleanup, never part of recovery, so it must
        // not fail startup.
        // DANGLING-GENERATION REPAIR: a DROP that removed a table directory
        // before its exclusion publication landed durably leaves the newest
        // file-catalog referencing missing generation files, which fails
        // every subsequent open. Repair publishes exclusions through the
        // normal path so strict recovery succeeds again. Genuine corruption
        // still fails startup loudly below. Best-effort like the sweep: a
        // repair that cannot complete leaves strict recovery to report it.
        // DANGLING-STATE REPAIR: publications and GC race across manager
        // instances, so the pointer can name a catalog file (or generations)
        // that are already gone. Repair order matters: revert the pointer to
        // a resolvable catalog first, then prune missing generations, so a
        // restart always converges instead of failing every open.
        // Best-effort like the sweep: repairs that cannot complete leave
        // strict recovery to report the original problem.
        let repair_started = Instant::now();
        match plomid_storage::GenerationManager::repair_dangling_catalog(storage_path) {
            Ok(true) => {
                tracing::warn!(
                    target: "server",
                    event = "dangling_catalog_repaired",
                );
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(
                    target: "server",
                    event = "dangling_catalog_repair_failed",
                    error = %error,
                );
            }
        }
        match plomid_storage::GenerationManager::repair_dangling_generations(storage_path) {
            Ok(pruned) => {
                if !pruned.is_empty() {
                    tracing::warn!(
                        target: "server",
                        event = "dangling_generations_repaired",
                        pruned_objects = pruned.len(),
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    target: "server",
                    event = "dangling_generation_repair_failed",
                    error = %error,
                );
            }
        }
        let sweep_started = Instant::now();
        let repair_time = repair_started.elapsed();
        match plomid_storage::GenerationManager::open(storage_path).map(std::sync::Arc::new) {
            Ok(manager) => match manager.gc() {
                Ok(outcome) => {
                    tracing::info!(
                        target: "server",
                        event = "orphan_sweep",
                        reclaimed = outcome.reclaimed.len(),
                        reclaimed_catalogs = outcome.reclaimed_catalogs.len(),
                        elapsed_ms = sweep_started.elapsed().as_millis() as u64,
                    );
                }
                Err(error) if error.kind() == plomid_core::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(
                        target: "server",
                        event = "orphan_sweep_failed",
                        error = %error,
                    );
                }
            },
            Err(error) => {
                tracing::warn!(
                    target: "server",
                    event = "orphan_sweep_failed",
                    error = %error,
                );
            }
        }
        // DRAINED-SEGMENT SWEEP: sealed segments drained by deletes/updates
        // stay on disk as dead weight (their emptiness only made probes cheap,
        // never reclaimed the files). Single-threaded here, no readers yet,
        // nothing published yet beyond what recovery just validated — reclaim
        // exactly the proven-empty, unreferenced files and nothing else.
        // Fail-open with a warning, like the generation sweep above.
        match storage.reclaim_drained_segments(&std::collections::BTreeSet::new()) {
            Ok(outcome) if !outcome.reclaimed.is_empty() => {
                tracing::info!(
                    target: "server",
                    event = "drained_segment_sweep",
                    reclaimed = outcome.reclaimed.len(),
                    reclaimed_bytes = outcome.reclaimed_bytes,
                );
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    target: "server",
                    event = "drained_segment_sweep_failed",
                    error = %error,
                );
            }
        }
        // The WAL writer opens after recovery so it continues at the recovered
        // durable prefix, where an incomplete crash tail has already been
        // identified by the recovery reader.
        let wal_dir = storage_path.join("wal");
        let wal_started = Instant::now();
        let wal = SegmentedWal::open(
            &wal_dir,
            wal_segment_size_bytes,
            plomid_wal::DurabilityMode::Force,
        )?;
        let wal_open = wal_started.elapsed();
        let manager_started = Instant::now();
        let manager = Arc::new(TransactionManager::open_segmented(&wal_dir)?);
        let manager_open = manager_started.elapsed();
        let sweep_time = sweep_started.elapsed();
        let timings = EngineOpenTimings {
            storage_open,
            recovery: recovery_time,
            repair: repair_time,
            sweep: sweep_time,
            wal_open,
            manager_open,
            bootstrap: std::time::Duration::ZERO,
        };

        let mut engine = Self {
            storage,
            manager,
            wal,
            versions: Arc::new(Mutex::new(VersionStore::new())),
            group: Arc::new(GroupDurability::new(wal_dir)),
            recovery: Some(recovery),
            write_gates: Arc::new(GateTable::new()),
            row_gates: Arc::new(RowGateTable::new()),
            unique_gates: Arc::new(UniqueGateTable::new()),
            row_ids: Arc::new(RowIdAllocator::new()),
            // Not yet proven complete: the storage image has just been
            // recovered and the version store is still empty. The bootstrap
            // pass below establishes the invariant.
            versions_complete: Arc::new(AtomicBool::new(false)),
            checkpoint_policy: CheckpointPolicy::default(),
            checkpoint_accounting: CheckpointAccounting::new(),
            version_gc_commits_since_last: 0,
            version_gc_total_reclaimed: 0,
            shutdown: false,
            shutdown_trace: Vec::new(),
        };
        let bootstrap_started = Instant::now();
        engine.bootstrap_versions()?;
        engine.versions_complete.store(true, Ordering::Release);
        // Timed after the bootstrap so `total_ms` is the whole mount: the
        // previous position (before the bootstrap) reported ~100ms for a
        // mount whose version pass alone cost over a second, hiding the
        // dominant stage from every startup budget.
        let recovery_report = engine
            .recovery_report()
            .expect("engine just opened with a recovery report");
        log_open_timings(
            EngineOpenTimings {
                bootstrap: bootstrap_started.elapsed(),
                ..timings
            },
            open_started.elapsed(),
            recovery_report,
        );
        Ok(engine)
    }

    /// The recovery report of the mount that opened this engine.
    ///
    /// `None` for an engine created fresh by [`Self::create`]. When present it
    /// records the validated storage generation, the checkpoint boundary that
    /// bounded replay, the applied record/transaction counts, and any
    /// incomplete crash tail that was observed but never treated as durable.
    #[must_use]
    pub fn recovery_report(&self) -> Option<&CheckpointReplayReport> {
        self.recovery.as_ref()
    }

    /// Whether [`Self::shutdown`] has run: new transactions are rejected.
    ///
    /// Committed reads still resolve (drain-friendly demount); reopening the
    /// same root with [`Self::open`] mounts a fresh running engine.
    #[must_use]
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// Demount stages completed so far, in execution order.
    #[must_use]
    pub fn shutdown_trace(&self) -> &[ShutdownStage] {
        &self.shutdown_trace
    }

    /// Mutation gate: rejects new transactions once shutdown has begun.
    fn require_running(&self, operation: &str) -> Result<()> {
        if self.shutdown {
            return Err(PlomidError::with_detail(
                ErrorKind::Conflict,
                "engine is shut down; new transactions are rejected",
                format!("operation={operation}"),
            ));
        }
        Ok(())
    }

    /// Demounts the engine following the fixed durability ordering:
    ///
    /// stop mutations → flush required state → checkpoint → sync → close.
    ///
    /// This is the single demount authority (merged from the removed
    /// `plomid-lifecycle` orchestrator): the same crash-safe checkpoint path
    /// used at runtime publishes the final recovery boundary, so a restart
    /// replays at most the WAL written after it. The returned
    /// [`ShutdownReport`] records the completed stages; on failure the error
    /// propagates, the mutation gate stays closed, and
    /// [`Self::shutdown_trace`] shows how far demount got. Either way the
    /// caller drops the engine afterwards — reopening the root mounts fresh.
    pub fn shutdown(&mut self) -> Result<ShutdownReport> {
        if self.shutdown {
            return Err(PlomidError::with_detail(
                ErrorKind::Conflict,
                "engine is already shut down",
                "operation=shutdown".to_string(),
            ));
        }
        let started = Instant::now();
        // 1. STOP MUTATIONS: the gate begins rejecting new transactions.
        self.shutdown = true;
        self.shutdown_trace.push(ShutdownStage::StopMutations);
        // 2. FLUSH REQUIRED STATE through the existing mechanisms.
        self.storage.sync()?;
        self.wal.sync()?;
        self.shutdown_trace.push(ShutdownStage::Flush);
        // 3. CHECKPOINT: publish the durable WAL boundary and reclaim what
        //    it made unreachable (same path as the runtime checkpoint).
        let checkpoint = Some(self.checkpoint_with_report(CheckpointTrigger::Shutdown)?);
        self.shutdown_trace.push(ShutdownStage::Checkpoint);
        // 4. SYNC: final durability boundary after checkpoint publication.
        self.storage.sync()?;
        self.wal.sync()?;
        self.shutdown_trace.push(ShutdownStage::Sync);
        // 5. CLOSE: handle release is the caller's `Drop`; the ordering is
        //    recorded here so shutdown is observable end to end.
        self.shutdown_trace.push(ShutdownStage::Close);
        Ok(ShutdownReport {
            stages: self.shutdown_trace.clone(),
            checkpoint,
            elapsed: started.elapsed(),
        })
    }

    /// Returns the WAL durability mode for diagnostics.
    pub fn durability_mode(&self) -> DurabilityMode {
        self.wal.durability_mode()
    }

    /// Flushes all data segments before publishing a durable WAL checkpoint.
    /// WAL retention remains conservative until active-transaction dependency
    /// tracking is available.
    pub fn checkpoint(&mut self) -> Result<Lsn> {
        Ok(self
            .checkpoint_with_report(CheckpointTrigger::Explicit)?
            .lsn)
    }

    /// Runs a checkpoint and reports what it did.
    ///
    /// Ordering is the crash-safety contract and is unchanged by this method:
    /// data pages are flushed and fsynced *before* the checkpoint record is
    /// written, the record is fsynced before it is published as the recovery
    /// boundary, and segments are reclaimed only after that boundary is
    /// durable. A crash before, during or after the checkpoint therefore
    /// recovers to a prefix the checkpoint proves reachable.
    pub fn checkpoint_with_report(
        &mut self,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointOutcome> {
        let started = Instant::now();
        // Read the covered volume from the WAL, not from the accounting struct:
        // a shutdown or explicit checkpoint must report the same number an
        // automatic one would, and the WAL is the only counter that sees every
        // commit path.
        let wal_bytes = self.wal.bytes_since_checkpoint();
        let outcome = (|| -> Result<(Lsn, usize)> {
            self.storage.sync()?;
            let (txn_id, commit_timestamp) = self.manager.watermarks();
            // Cap the recovery boundary at the oldest prepared-but-unpublished
            // commit: such commits have WAL records on disk but no applied
            // data pages yet, so covering them with the marker while
            // reclaiming up to it would delete their only durable trace on a
            // crash. The cap keeps their records above the replay boundary
            // and out of reclamation; replay past the cap is idempotent, and
            // the next checkpoint advances once they publish or abort. With
            // nothing in flight the cap is the marker itself (no-op).
            let cap = self
                .manager
                .oldest_unfinished_lsn()?
                .map(|lsn| Lsn::new(lsn.saturating_sub(1)));
            let lsn = match cap {
                Some(cap) => {
                    self.wal
                        .checkpoint_with_watermarks_capped(txn_id, commit_timestamp, cap)?
                }
                None => self
                    .wal
                    .checkpoint_with_watermarks(txn_id, commit_timestamp)?,
            };
            // No storage-level checkpoint file is published here. A checkpoint
            // file must name a catalog version that exists on disk, and this
            // engine's catalog lives in the key-value store: publishing one
            // without a catalog made the next mount fail closed
            // ("the newest valid checkpoint names a catalog version that does
            // not exist"), which is worse than the replay it saves. The WAL
            // marker published by `checkpoint_with_watermarks` is the durable
            // boundary recovery uses for this deployment shape (see
            // `CheckpointSelection::from_wal_marker`).
            //
            // Reclaim only what the durable checkpoint proves unreachable.
            let reclaimed = self.wal.reclaim_before(lsn)?;
            Ok((lsn, reclaimed))
        })();
        let duration = started.elapsed();
        match outcome {
            Ok((lsn, reclaimed)) => {
                self.checkpoint_accounting
                    .record_checkpoint(trigger, duration, wal_bytes, reclaimed);
                plomid_core::observability::record_checkpoint_bytes(wal_bytes);
                plomid_core::observability::record_checkpoint_duration(
                    duration.as_micros().min(u64::MAX as u128) as u64,
                );
                if tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf") {
                    tracing::debug!(
                        target: "plomid::perf",
                        event = "checkpoint",
                        trigger = trigger.as_str(),
                        lsn = lsn.get(),
                        duration_us = duration.as_micros() as u64,
                        wal_bytes,
                        reclaimed_segments = reclaimed,
                    );
                }
                // INFO because this is maintenance, not per-operation noise: a
                // checkpoint is bounded by thresholds and is the event that
                // explains both a WAL shrinking on disk and a restart that did
                // not have to replay. Without it, automatic checkpointing is
                // invisible in a default (info-level) production log.
                tracing::info!(
                    target: "storage",
                    event = "checkpoint",
                    trigger = trigger.as_str(),
                    lsn = lsn.get(),
                    duration_ms = duration.as_millis() as u64,
                    wal_bytes,
                    reclaimed_segments = reclaimed,
                );
                // Observability: count every successful checkpoint, whatever
                // triggered it. Recorded here (not at call sites) so explicit,
                // automatic, and shutdown checkpoints all count exactly once.
                plomid_core::observability::record_checkpoint();
                // Post-checkpoint segment maintenance: the boundary above is
                // now durable (data synced, WAL marker synced, WAL reclaimed),
                // so every dereference the reclaim pass relies on is already
                // crash-safe. Fail-open: reclamation must never fail a commit.
                self.maybe_reclaim_drained_segments();
                Ok(CheckpointOutcome {
                    trigger,
                    lsn,
                    duration,
                    wal_bytes,
                    reclaimed_segments: reclaimed,
                })
            }
            Err(error) => {
                self.checkpoint_accounting
                    .record_failure(error.message().to_string());
                Err(error)
            }
        }
    }

    /// Runs a checkpoint when the configured policy says one is due.
    ///
    /// Called from the transaction entry points (`begin`, `begin_concurrent`)
    /// and from the concurrent commit path, so every write path is covered
    /// without adding work to the hot path: the check is a comparison of two
    /// counters already in memory.
    ///
    /// Returns `None` when no threshold has been reached. Errors are recorded
    /// in [`Self::checkpoint_stats`] and logged, never returned: this is
    /// called from the commit path, and a committed statement's success must
    /// not depend on maintenance work. Because thresholds are only reset by a
    /// *successful* checkpoint, a failure retries on the next commit.
    ///
    /// Safe to call concurrently: the due check reads live state, so if two
    /// committers both observe a due policy the first checkpoint resets the
    /// window and the second call becomes a no-op.
    pub fn checkpoint_if_due(&mut self) -> Option<CheckpointOutcome> {
        // Both signals come from the WAL itself, so they describe the log
        // rather than any one commit path: a server whose writes take the
        // transactional path must auto-checkpoint exactly like one using the
        // concurrent path.
        let retained = self.wal.segment_count() as u64;
        let appended = self.wal.bytes_since_checkpoint();
        // Keep the reported statistics on the same number the decision used.
        self.checkpoint_accounting.observe_appended(appended);
        let trigger =
            self.checkpoint_policy
                .trigger(&self.checkpoint_accounting, appended, retained)?;
        match self.checkpoint_with_report(trigger) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                tracing::warn!(
                    target: "storage::checkpoint",
                    trigger = trigger.as_str(),
                    error = %error.message(),
                    "automatic checkpoint failed; the next commit retries"
                );
                None
            }
        }
    }

    /// Replaces the automatic checkpoint policy.
    pub fn set_checkpoint_policy(&mut self, policy: CheckpointPolicy) {
        self.checkpoint_policy = policy;
    }

    /// The active automatic checkpoint policy.
    #[must_use]
    pub fn checkpoint_policy(&self) -> CheckpointPolicy {
        self.checkpoint_policy
    }

    /// Checkpoint accounting for diagnostics, metrics and tests.
    #[must_use]
    pub fn checkpoint_stats(&self) -> CheckpointStats {
        self.checkpoint_accounting.stats()
    }

    /// Installs one committed write set as newest-first versions.
    ///
    /// Called by the commit path at publication: on the inline wrapper path
    /// WAL + storage durability already hold, on the concurrent facade path
    /// the durability barrier follows. Either way the preserved ordering is
    /// WAL appended -> storage applied -> versions visible.
    pub fn install_committed(
        &mut self,
        txn_id: TxnId,
        commit_ts: u64,
        write_set: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    ) {
        use plomid_mvcc::RowVersion;
        let lock_started = Instant::now();
        let mut store = self.versions.lock().expect("version store mutex poisoned");
        let lock_us = lock_started.elapsed().as_micros() as u64;
        let install_started = Instant::now();
        for (key, staged) in write_set {
            let version = match staged {
                Some(payload) => RowVersion::committed(txn_id, commit_ts, payload),
                None => RowVersion::deleted(txn_id, commit_ts),
            };
            store.install(key, version);
        }
        if tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf") {
            tracing::debug!(
                target: "plomid::perf",
                event = "mvcc_install",
                versions_lock_us = lock_us,
                install_us = install_started.elapsed().as_micros() as u64,
            );
        }
    }

    /// Shared handle to the MVCC version store.
    ///
    /// Connection-facing read paths resolve committed reads from this store
    /// directly, without the database mutex, once the completeness flag below
    /// reports the store mirrors committed storage.
    #[must_use]
    pub fn versions_handle(&self) -> SharedVersionStore {
        Arc::clone(&self.versions)
    }

    /// Completeness flag, shared so readers track the transition live.
    #[must_use]
    pub fn versions_complete_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.versions_complete)
    }

    /// Shared transaction manager handle.
    ///
    /// Connection-facing read paths use it to capture the committed snapshot
    /// ([`Self::committed_snapshot`] is exactly `manager.snapshot(TxnId(0))`)
    /// without taking the database mutex, which would serialize every read on
    /// every connection.
    #[must_use]
    pub fn manager_handle(&self) -> Arc<TransactionManager> {
        Arc::clone(&self.manager)
    }

    /// Captures a read snapshot for ad-hoc (non-transactional) MVCC reads.
    pub fn committed_snapshot(&self) -> Result<Snapshot> {
        let owner = TxnId::new(0);
        self.manager.snapshot(owner)
    }

    /// MVCC point read: newest version of `key` visible to `snapshot`.
    ///
    /// Keys absent from the version store are lazily bootstrapped from the
    /// committed storage image (covers rows recovered from the WAL after a
    /// restart, which carry no in-memory version yet).
    pub fn get_snapshot(&mut self, key: &[u8], snapshot: &Snapshot) -> Result<Option<Vec<u8>>> {
        let perf = tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf");
        let started = Instant::now();
        self.ensure_version(key)?;
        let ensure_us = started.elapsed().as_micros() as u64;
        let lock_started = Instant::now();
        let store = self.versions.lock().expect("version store mutex poisoned");
        let versions_lock_us = lock_started.elapsed().as_micros() as u64;
        let visible_started = Instant::now();
        let visible = store.visible(key, snapshot);
        let visible_us = visible_started.elapsed().as_micros() as u64;
        if perf {
            tracing::debug!(
                target: "plomid::perf",
                event = "mvcc_get",
                ensure_us,
                versions_lock_us,
                visible_us,
                total_us = started.elapsed().as_micros() as u64,
            );
        }
        match visible {
            Some(version) if version.state == plomid_mvcc::VersionState::Deleted => Ok(None),
            Some(version) => Ok(version.payload.clone()),
            None => Ok(None),
        }
    }

    /// MVCC point reads for many keys under ONE snapshot.
    ///
    /// Each result is exactly what [`Self::get_snapshot`] would return for that
    /// key under `snapshot`; only the fixed cost is amortised, because the
    /// version-store lock is taken once for the whole batch rather than once
    /// per key.
    ///
    /// The batch shares a single snapshot by construction, which is strictly
    /// stronger than the per-key path's `N` independent committed snapshots:
    /// every candidate in one index probe is now guaranteed to be resolved
    /// against the same committed state.
    pub fn get_many_snapshot(
        &mut self,
        keys: &[Vec<u8>],
        snapshot: &Snapshot,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        // Bootstrap any key the version store has not seen yet (a no-op once
        // the completeness invariant holds).
        for key in keys {
            self.ensure_version(key)?;
        }
        let store = self.versions.lock().expect("version store mutex poisoned");
        Ok(keys
            .iter()
            .map(|key| match store.visible(key, snapshot) {
                Some(version) if version.state == plomid_mvcc::VersionState::Deleted => None,
                Some(version) => version.payload.clone(),
                None => None,
            })
            .collect())
    }

    /// MVCC range scan: visible payload per key in `[start, end)`.
    pub fn scan_snapshot(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: &Snapshot,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_snapshot_limit(start, end, snapshot, None)
    }

    /// Snapshot scan stopping after `limit` visible rows (see
    /// [`VersionStore::scan_visible_limit`]). `None` scans the full range.
    pub fn scan_snapshot_limit(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: &Snapshot,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.sync_versions_in_range(start, end)?;
        // Page the walk so no single critical section holds the version lock
        // across an unbounded scan: a 1M-row OLAP scan otherwise serializes
        // every point read and commit behind it for hundreds of milliseconds.
        // The snapshot is fixed up front, so releasing and reacquiring the
        // lock between chunks cannot change what this walk observes.
        let mut out = Vec::new();
        let mut bound = start.map_or(std::ops::Bound::Unbounded, |s| {
            std::ops::Bound::Included(s.to_vec())
        });
        let end_owned = end.map(|e| e.to_vec());
        loop {
            if limit.is_some_and(|cap| out.len() >= cap) {
                break;
            }
            let chunk_cap = limit
                .map(|cap| (cap - out.len()).min(SCAN_CHUNK_ROWS))
                .unwrap_or(SCAN_CHUNK_ROWS);
            let (chunk, resume, completed) = {
                let store = self.versions.lock().expect("version store mutex poisoned");
                store.scan_step(bound, end_owned.as_deref(), snapshot, chunk_cap.max(1))
            };
            out.extend(chunk);
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                // No key examined yet the range is not exhausted: only
                // possible with an empty chunk, which `scan_step` never
                // returns alongside `completed == false` (it always examines
                // at least one key first). Defensive break preserves
                // termination over liveness of an impossible state.
                None => break,
            }
        }
        if let Some(cap) = limit {
            out.truncate(cap);
        }
        Ok(out)
    }

    /// Counts visible rows under `snapshot` without materializing payloads.
    pub fn count_snapshot(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot: &Snapshot,
    ) -> Result<u64> {
        self.sync_versions_in_range(start, end)?;
        // Same paging discipline as `scan_snapshot_limit`: no unbounded
        // critical section even though counting allocates nothing per row.
        let mut total = 0u64;
        let mut bound = start.map_or(std::ops::Bound::Unbounded, |s| {
            std::ops::Bound::Included(s.to_vec())
        });
        let end_owned = end.map(|e| e.to_vec());
        loop {
            let (n, resume, completed) = {
                let store = self.versions.lock().expect("version store mutex poisoned");
                store.count_step(bound, end_owned.as_deref(), snapshot, SCAN_CHUNK_ROWS)
            };
            total += n;
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                None => break,
            }
        }
        Ok(total)
    }

    /// Number of versions stored for `key` (GC/tests).
    pub fn version_count(&mut self, key: &[u8]) -> Result<usize> {
        self.ensure_version(key)?;
        let store = self.versions.lock().expect("version store mutex poisoned");
        Ok(store.version_count(key))
    }

    /// Explicit MVCC garbage collection below `horizon`.
    pub fn gc_below(&mut self, horizon: u64) -> usize {
        let mut store = self.versions.lock().expect("version store mutex poisoned");
        store.gc(horizon)
    }

    /// Highest committed timestamp installed in the version store.
    pub fn last_committed_version(&self) -> u64 {
        let store = self.versions.lock().expect("version store mutex poisoned");
        store.last_committed()
    }

    /// Borrow the transaction manager (snapshot/GC introspection).
    pub fn manager(&self) -> &TransactionManager {
        &self.manager
    }

    fn ensure_version(&mut self, key: &[u8]) -> Result<()> {
        {
            let store = self.versions.lock().expect("version store mutex poisoned");
            if store.chain(key).is_some() {
                return Ok(());
            }
        }
        // When the completeness invariant holds, every committed key in
        // storage already has a chain in the version store, so the storage
        // image cannot contribute a version here. Reading it anyway was a
        // second, identical lookup per point read.
        if self.versions_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(payload) = self.storage.get(key)? {
            let mut store = self.versions.lock().expect("version store mutex poisoned");
            store.ensure_bootstrap(key.to_vec(), Some(strip_envelope(&payload)));
        }
        Ok(())
    }

    /// Seeds the in-memory version store from the committed image at mount.
    ///
    /// The MVCC engine resolves reads through the version store, and not every
    /// reader bootstraps on demand: the engine-level point and range reads do
    /// (`ensure_version`/`sync_versions_in_range`), but the transaction-handle
    /// read path and the columnar materialiser consult the store directly.
    /// Those paths therefore require the store to already reflect committed
    /// state when the engine opens, which is what this pass establishes.
    ///
    /// Cost is one sequential pass over the committed image, so mount time is
    /// proportional to database size (measured on a small database: tens of
    /// milliseconds; it grows linearly with rows). Making it lazy is a
    /// worthwhile follow-up for large databases, but it must be done in the
    /// read paths that currently assume completeness, not only at mount: an
    /// engine that skipped this pass made an INSERT after a reopen invisible to
    /// a later `SELECT COUNT(*)` (the row-id allocator and the count scan
    /// disagreed about committed state).
    fn bootstrap_versions(&mut self) -> Result<()> {
        // Mount is single-threaded with no readers yet, so one lock for the
        // whole pass: the previous per-key lock/unlock paid a mutex round
        // trip per row of the full image scan for no additional safety.
        // The scan itself runs across segments concurrently; the merge in
        // `range_parallel` is deterministic, so the installed set is
        // identical to a sequential pass.
        let rows = self.storage.range_parallel(None, None)?;
        let mut store = self.versions.lock().expect("version store mutex poisoned");
        for (key, payload) in rows {
            store.ensure_bootstrap(key, Some(strip_envelope(&payload)));
        }
        Ok(())
    }

    /// Establishes completeness for keys a range scan can reach.
    ///
    /// Once [`Self::versions_complete`] holds there is nothing for this pass to
    /// do: the version store already mirrors every committed key in storage, so
    /// walking the storage image for the same range only re-materialised rows
    /// the caller was about to read from the store anyway — a full duplicate
    /// read of the table on every scan. It remains the correctness fallback for
    /// the window before the mount-time bootstrap has run.
    fn sync_versions_in_range(&mut self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        if self.versions_complete.load(Ordering::Acquire) {
            return Ok(());
        }
        let rows = self.storage.range(start, end)?;
        let mut store = self.versions.lock().expect("version store mutex poisoned");
        for (key, payload) in rows {
            if store.chain(&key).is_none() {
                store.ensure_bootstrap(key, Some(strip_envelope(&payload)));
            }
        }
        Ok(())
    }
}

/// Per-stage durations of one engine mount, reported by `engine_open`.
///
/// Bundled so the timing call site stays a two-argument call no matter how
/// many stages startup gains; every field is one independently measured phase.
struct EngineOpenTimings {
    storage_open: std::time::Duration,
    recovery: std::time::Duration,
    repair: std::time::Duration,
    sweep: std::time::Duration,
    wal_open: std::time::Duration,
    manager_open: std::time::Duration,
    bootstrap: std::time::Duration,
}

/// Emits the phase breakdown of a mount.
///
/// Two audiences, one measurement: DEBUG under `plomid::perf` carries the
/// per-phase microseconds for profiling, and one INFO line records what the
/// restart actually cost (replay volume, segments read, whether a crash tail
/// was truncated). Without the INFO summary a slow start is invisible until
/// someone enables debug logging on a production server.
fn log_open_timings(
    timings: EngineOpenTimings,
    total: std::time::Duration,
    report: &CheckpointReplayReport,
) {
    if tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf") {
        tracing::debug!(
            target: "plomid::perf",
            event = "engine_open",
            storage_open_us = timings.storage_open.as_micros() as u64,
            recovery_us = timings.recovery.as_micros() as u64,
            repair_us = timings.repair.as_micros() as u64,
            sweep_us = timings.sweep.as_micros() as u64,
            wal_open_us = timings.wal_open.as_micros() as u64,
            manager_open_us = timings.manager_open.as_micros() as u64,
            bootstrap_us = timings.bootstrap.as_micros() as u64,
            total_us = total.as_micros() as u64,
        );
    }
    tracing::info!(
        target: "storage",
        event = "engine_open",
        storage_open_ms = timings.storage_open.as_millis() as u64,
        recovery_ms = timings.recovery.as_millis() as u64,
        repair_ms = timings.repair.as_millis() as u64,
        sweep_ms = timings.sweep.as_millis() as u64,
        wal_open_ms = timings.wal_open.as_millis() as u64,
        manager_open_ms = timings.manager_open.as_millis() as u64,
        bootstrap_ms = timings.bootstrap.as_millis() as u64,
        total_ms = total.as_millis() as u64,
        applied_records = report.applied_records,
        applied_transactions = report.applied_transactions,
        applied_bytes = report.applied_bytes,
        segments_read = report.segments_read,
        crash_tail = report.crash_tail_boundary.is_some(),
    );
}

/// Strips a versioned envelope back to the raw row payload.
///
/// Crash-recovered rows may already carry an envelope from a previous
/// versioned commit; bootstrapping must store the raw payload, not the
/// envelope bytes.
fn strip_envelope(payload: &[u8]) -> Vec<u8> {
    if plomid_mvcc::is_versioned(payload) {
        if let Some(version) = plomid_mvcc::decode_versioned(payload) {
            if let Some(raw) = version.payload {
                return raw;
            }
            return Vec::new();
        }
    }
    payload.to_vec()
}

/// Converts a native MVCC snapshot into a shareable handle.
///
/// The handle carries the same owner, watermark, and active set, so
/// resolving it through visibility observes exactly the captured state.
fn shared_from_snapshot(snapshot: &Snapshot) -> SharedSnapshot {
    SharedSnapshot::new(
        snapshot.owner().get(),
        snapshot.watermark(),
        snapshot.active().iter().copied().collect(),
    )
}

/// Rebuilds the native snapshot a shared handle was captured from.
fn snapshot_from_shared(shared: &SharedSnapshot) -> Snapshot {
    Snapshot::new(
        TxnId::new(shared.owner()),
        shared.watermark(),
        shared.active().iter().copied().collect(),
    )
}

impl StorageEngine for PlomidStorageEngine {
    type Transaction<'a> = TransactionHandle<'a, StorageManager, SegmentedWal>;

    fn open(storage_path: &Path, wal_path: &Path, pool_capacity: usize) -> Result<Self>
    where
        Self: Sized,
    {
        Self::open(storage_path, wal_path, pool_capacity)
    }

    fn create(storage_path: &Path, wal_path: &Path, pool_capacity: usize) -> Result<Self>
    where
        Self: Sized,
    {
        Self::create(storage_path, wal_path, pool_capacity)
    }

    fn begin(&mut self) -> Result<Self::Transaction<'_>> {
        // Demount gate: no new transactions once shutdown has begun.
        self.require_running("begin")?;
        // The transaction borrows manager + wal + storage for its lifetime
        // while the engine keeps the version store. The borrow checker cannot
        // split these disjoint field borrows through the return type, so the
        // engine hands out the transaction through an internal raw-pointer
        // bridge (no memory unsafety: all pointers derive from `self` and the
        // transaction cannot outlive the `&mut self` borrow).
        // Automatic checkpointing, reached between transactions for the same
        // reason as the concurrent entry point: the statement that crosses a
        // threshold pays one bounded checkpoint rather than letting WAL (and the
        // next restart's replay) grow with the write history.
        let _ = self.checkpoint_if_due();
        let txn = Transaction::begin_raw(&self.manager, &mut self.wal, &mut self.storage)?;
        tracing::info!(target: "storage", "transaction_begin txn_id={}", txn.txn_id().get());
        Ok(TransactionHandle {
            inner: Some(txn),
            versions: Arc::clone(&self.versions),
        })
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        tracing::trace!(target: "storage", "get key_len={}", key.len());
        let snapshot = self.committed_snapshot()?;
        self.get_snapshot(key, &snapshot)
    }

    fn get_many(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        tracing::trace!(target: "storage", "get_many keys={}", keys.len());
        let snapshot = self.committed_snapshot()?;
        self.get_many_snapshot(keys, &snapshot)
    }

    fn scan(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        tracing::trace!(target: "storage", "scan start={:?} end={:?}", start.map(|s| s.len()), end.map(|e| e.len()));
        let snapshot = self.committed_snapshot()?;
        self.scan_snapshot(start, end, &snapshot)
    }

    fn scan_limit(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let snapshot = self.committed_snapshot()?;
        self.scan_snapshot_limit(start, end, &snapshot, Some(limit))
    }

    fn count_range(&mut self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<u64> {
        let snapshot = self.committed_snapshot()?;
        self.count_snapshot(start, end, &snapshot)
    }

    fn scan_for_each(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        chunk_size: usize,
        f: &mut dyn FnMut(&[(Vec<u8>, Vec<u8>)]) -> Result<bool>,
    ) -> Result<()> {
        // One statement snapshot across all chunks: releasing and reacquiring
        // the version lock between chunks cannot change what this walk
        // observes, exactly as `scan_snapshot_limit` documents.
        self.sync_versions_in_range(start, end)?;
        let snapshot = self.committed_snapshot()?;
        let chunk_cap = chunk_size.max(1);
        let mut bound = start.map_or(std::ops::Bound::Unbounded, |s| {
            std::ops::Bound::Included(s.to_vec())
        });
        let end_owned = end.map(|e| e.to_vec());
        loop {
            let (chunk, resume, completed) = {
                let store = self.versions.lock().expect("version store mutex poisoned");
                store.scan_step(bound, end_owned.as_deref(), &snapshot, chunk_cap)
            };
            if !chunk.is_empty() && !f(&chunk)? {
                break;
            }
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                None => break,
            }
            if chunk.is_empty() {
                // `scan_step` never returns an empty chunk with
                // `completed == false`; the guard above preserves termination
                // if that invariant ever changes.
                continue;
            }
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        tracing::trace!(target: "storage", "sync");
        self.wal.sync()?;
        self.storage.sync()?;
        Ok(())
    }

    fn root(&self) -> &Path {
        self.storage.root()
    }

    fn mvcc_safety(&self) -> Result<(Vec<u64>, u64)> {
        let active: Vec<u64> = self.manager.active_txns()?.into_iter().collect();
        let last_committed = self.manager.last_committed()?;
        Ok((active, last_committed))
    }

    fn txn_issue_mark(&self) -> Result<u64> {
        Ok(self.manager.next_mark())
    }

    fn capture_shared_snapshot(&self) -> Result<SharedSnapshot> {
        // NOTE: this engine carries no session/pin state (`is_read_pinned`
        // reports the trait default `false`), so capture always resolves the
        // current committed horizon. That is consistent — and exactly what this
        // engine's own `scan`/`scan_for_each` would observe — but it is NOT a
        // pinned RR snapshot. Sessions pin above this layer: production
        // queries run through `ConcurrentPlomidStorageEngine`, whose override
        // IS pinned-aware. Direct-engine callers must not present a shared
        // handle captured here as pinned.
        Ok(shared_from_snapshot(&self.committed_snapshot()?))
    }

    fn scan_shared(
        &self,
        snapshot: &SharedSnapshot,
        sub_start: Option<&[u8]>,
        sub_end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // `&self` cannot bootstrap the storage image (`StorageManager::range`
        // needs `&mut`), so this path requires the completeness invariant the
        // mount-time bootstrap establishes. Fresh and reopened engines satisfy
        // it; callers fall back to `scan` when it does not hold.
        if !self.versions_complete.load(Ordering::Acquire) {
            return Err(plomid_core::PlomidError::with_detail(
                plomid_core::ErrorKind::Unsupported,
                "shared snapshot scan needs a completed version store",
                "fall back to scan() which bootstraps the range first",
            ));
        }
        let native = snapshot_from_shared(snapshot);
        // Paged like `scan_snapshot_limit`: one fixed snapshot, version lock
        // released per chunk, so a partition never holds the shared lock
        // across its whole walk and concurrent workers never serialize on it.
        let mut out = Vec::new();
        let mut bound = sub_start.map_or(std::ops::Bound::Unbounded, |s| {
            std::ops::Bound::Included(s.to_vec())
        });
        let end_owned = sub_end.map(|e| e.to_vec());
        loop {
            let (chunk, resume, completed) = {
                let store = self.versions.lock().expect("version store mutex poisoned");
                store.scan_step(bound, end_owned.as_deref(), &native, SCAN_CHUNK_ROWS)
            };
            out.extend(chunk);
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                None => break,
            }
        }
        Ok(out)
    }

    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn scan_shared_for_each(
        &self,
        snapshot: &SharedSnapshot,
        sub_start: Option<&[u8]>,
        sub_end: Option<&[u8]>,
        chunk_size: usize,
        f: &mut dyn FnMut(&[(Vec<u8>, Vec<u8>)]) -> Result<bool>,
    ) -> Result<()> {
        if !self.versions_complete.load(Ordering::Acquire) {
            return Err(plomid_core::PlomidError::with_detail(
                plomid_core::ErrorKind::Unsupported,
                "shared snapshot scan needs a completed version store",
                "fall back to scan_for_each() which bootstraps the range first",
            ));
        }
        let native = snapshot_from_shared(snapshot);
        let chunk_cap = chunk_size.max(1);
        let mut bound = sub_start.map_or(std::ops::Bound::Unbounded, |s| {
            std::ops::Bound::Included(s.to_vec())
        });
        let end_owned = sub_end.map(|e| e.to_vec());
        loop {
            let (chunk, resume, completed) = {
                let store = self.versions.lock().expect("version store mutex poisoned");
                store.scan_step(bound, end_owned.as_deref(), &native, chunk_cap)
            };
            if !chunk.is_empty() && !f(&chunk)? {
                break;
            }
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                None => break,
            }
            if chunk.is_empty() {
                continue;
            }
        }
        Ok(())
    }
}

impl PlomidStorageEngine {
    /// Engine-aware commit: the handle's `commit` performs WAL + storage
    /// durability and then installs the write set as committed versions
    /// (creator TxID + commit timestamp, newest-first per key). This method
    /// exists for callers that previously used the engine-side install path.
    pub fn commit_handle(
        &mut self,
        handle: &mut <Self as StorageEngine>::Transaction<'_>,
    ) -> Result<CommitResult> {
        handle.commit()
    }
}

/// A transaction handle that bridges the [`StorageEngineTransaction`] trait
/// and the internal [`Transaction`].
///
/// The handle owns the transaction's buffered operations plus its MVCC
/// snapshot, so `scan` overlays own-writes on the committed state. At
/// `commit` the engine installs the returned write set as committed versions.
///
/// The handle also holds a shared handle to the engine's [`VersionStore`] so
/// that transactional reads resolve through MVCC visibility and its `commit`
/// can install the write set as committed versions (WAL durable → storage
/// durable → versions visible). The store is shared through an `Arc<Mutex<_>>`
/// because the transaction cannot borrow the engine for the install step.
pub struct TransactionHandle<
    'a,
    T: DataStore = plomid_storage::BTree,
    W: LogStore = plomid_wal::WalWriter,
> {
    inner: Option<Transaction<'a, T, W>>,
    /// Shared view of the engine's MVCC version store, used by `scan`/`commit`.
    versions: SharedVersionStore,
}

impl<'a, T: DataStore, W: LogStore> StorageEngineTransaction<'a> for TransactionHandle<'a, T, W> {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner
            .as_mut()
            .expect("transaction handle has no active transaction")
            .put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner
            .as_mut()
            .expect("transaction handle has no active transaction")
            .delete(key)
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self
            .inner
            .as_mut()
            .expect("transaction handle has no active transaction");

        // The newest buffered operation is the transaction's own visible
        // state. Checking it first avoids touching the shared VersionStore
        // for the common read-your-writes case.
        for operation in inner.buffered_operations().iter().rev() {
            match operation {
                DataOperation::Put { key: op_key, value } if op_key == key => {
                    return Ok(Some(value.clone()));
                }
                DataOperation::Delete { key: op_key } if op_key == key => return Ok(None),
                _ => {}
            }
        }

        let snapshot = inner.snapshot();
        let store = self.versions.lock().expect("version store mutex poisoned");
        Ok(store.visible(key, snapshot).and_then(|version| {
            if version.state == plomid_mvcc::VersionState::Deleted {
                None
            } else {
                version.payload.clone()
            }
        }))
    }

    fn scan(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = self
            .inner
            .as_mut()
            .expect("transaction handle has no active transaction");
        // MVCC read path: resolve the committed state through the engine's
        // version store using this transaction's begin-time snapshot (so
        // concurrently committed versions remain invisible), then overlay the
        // transaction's own buffered writes (read-your-own-writes).
        let committed = {
            let store = self.versions.lock().expect("version store mutex poisoned");
            store.scan_visible(start, end, inner.snapshot())
        };
        let mut merged: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
            committed.into_iter().collect();
        for op in inner.buffered_operations() {
            let key = match op {
                DataOperation::Put { key, value } => {
                    let key = key.clone();
                    let value = value.clone();
                    if start.is_none_or(|s| key.as_slice() >= s)
                        && end.is_none_or(|e| key.as_slice() < e)
                    {
                        merged.insert(key, value);
                    }
                    continue;
                }
                DataOperation::Delete { key } => key,
            };
            if start.is_none_or(|s| key.as_slice() >= s) && end.is_none_or(|e| key.as_slice() < e) {
                merged.remove(key);
            }
        }
        Ok(merged.into_iter().collect())
    }

    fn commit(&mut self) -> Result<CommitResult> {
        use std::collections::HashMap;
        let inner = self
            .inner
            .as_mut()
            .expect("transaction handle has no active transaction");
        // Capture the write set before commit drains the buffer.
        let mut merged: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
        for op in inner.buffered_operations() {
            match op {
                DataOperation::Put { key, value } => {
                    merged.insert(key.clone(), Some(value.clone()));
                }
                DataOperation::Delete { key } => {
                    merged.insert(key.clone(), None);
                }
            }
        }
        let result = inner.commit()?;
        // Install committed versions only after WAL + storage durability:
        // creator TxID + commit timestamp, newest-first per key.
        {
            let mut store = self.versions.lock().expect("version store mutex poisoned");
            for (key, staged) in merged {
                let version = match staged {
                    Some(payload) => plomid_mvcc::RowVersion::committed(
                        result.txn_id,
                        result.commit_timestamp.get(),
                        payload,
                    ),
                    None => plomid_mvcc::RowVersion::deleted(
                        result.txn_id,
                        result.commit_timestamp.get(),
                    ),
                };
                store.install(key, version);
            }
        }
        Ok(CommitResult {
            txn_id: result.txn_id,
            commit_timestamp: result.commit_timestamp,
        })
    }

    fn abort(&mut self) -> Result<()> {
        let inner = self
            .inner
            .as_mut()
            .expect("transaction handle has no active transaction");
        inner.abort()
    }

    fn txn_id(&self) -> TxnId {
        self.inner
            .as_ref()
            .expect("transaction handle has no active transaction")
            .txn_id()
    }

    fn state(&self) -> plomid_storage::TransactionState {
        match self
            .inner
            .as_ref()
            .expect("transaction handle has no active transaction")
            .state()
        {
            crate::TransactionState::Active => plomid_storage::TransactionState::Active,
            crate::TransactionState::Committed => plomid_storage::TransactionState::Committed,
            crate::TransactionState::Aborted => plomid_storage::TransactionState::Aborted,
        }
    }
}
