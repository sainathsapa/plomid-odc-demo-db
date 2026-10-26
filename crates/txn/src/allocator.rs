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
//! Authoritative transaction identity and commit timestamp allocation for PLOMID.
//!
//! # Transaction IDs
//!
//! `TxnId` values are monotonically increasing `u64` counters starting at `1`.
//! They are allocated atomically and never reused within a single process
//! lifetime. After a restart, the allocator scans the WAL to derive the next
//! safe value, preventing reuse of IDs from prior runs.
//!
//! # Commit timestamps
//!
//! `CommitTimestamp` values are monotonic logical counters starting at `1`.
//! They provide a total order suitable for future MVCC visibility checks.
//! V1 does **not** claim wall-clock time ordering; these are purely logical
//! ticks. Like transaction IDs, they are never reused after restart because
//! the allocator scans the WAL for the highest previously committed value.
//!
//! # Overflow
//!
//! Both allocators panic with `Exhausted` if the counter would exceed
//! `u64::MAX - 1`. The value `u64::MAX` is reserved for sentinel use and is
//! never handed out by the normal allocation path.

use plomid_core::{CommitTimestamp, ErrorKind, PlomidError, Result, TxnId};
use plomid_mvcc::Snapshot;
use plomid_wal::{RecordType, WalReader};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

// Allocator bounds are defined once in `plomid_core::constants`.
use plomid_core::{INITIAL_COMMIT_TIMESTAMP, INITIAL_TXN_ID, MAX_COMMIT_TIMESTAMP, MAX_TXN_ID};

/// Authoritative allocator for transaction IDs and commit timestamps.
///
/// Tracks active transactions and committed timestamps so MVCC snapshots can
/// be captured consistently. Single-writer engines hold `&mut` access through
/// the storage engine, while the interior `Mutex` state also permits shared
/// `&self` snapshot capture for concurrent readers.
#[derive(Debug, Default)]
pub struct TxnRegistry {
    active: BTreeSet<u64>,
    committed: HashMap<u64, u64>,
    last_committed: u64,
    /// First WAL LSN per unfinished transaction (its `Begin` record).
    ///
    /// Checkpoints cap their recovery marker and reclaim horizon below the
    /// minimum here: a begun-but-unpublished transaction has WAL records on
    /// disk but no applied data pages yet, so covering it would delete its
    /// only durable trace on a crash. Entries are added when the `Begin`
    /// record is appended and removed when the transaction publishes or
    /// aborts; a transaction abandoned without either (no commit, no abort)
    /// keeps its entry exactly like it keeps its active registration, so
    /// this pinning is no weaker than the snapshot visibility it mirrors.
    first_lsn: BTreeMap<u64, u64>,
}

/// Authoritative allocator for transaction IDs and commit timestamps.
#[derive(Debug)]
pub struct TransactionManager {
    next_txn_id: AtomicU64,
    next_commit_timestamp: AtomicU64,
    registry: Mutex<TxnRegistry>,
}

impl TransactionManager {
    /// Creates a fresh allocator starting from the initial values.
    ///
    /// This does not scan any durable state and is suitable only for tests or
    /// green-field databases. Use [`Self::open`] when reopening an existing
    /// database to prevent ID reuse across restarts.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(INITIAL_TXN_ID),
            next_commit_timestamp: AtomicU64::new(INITIAL_COMMIT_TIMESTAMP),
            registry: Mutex::new(TxnRegistry::default()),
        }
    }

    /// Creates an allocator by scanning the WAL for the highest previously
    /// allocated transaction ID and commit timestamp.
    ///
    /// This is the safe entry point when reopening a database. The next
    /// allocated values will be strictly greater than any value observed in
    /// the WAL, preventing reuse across restarts.
    pub fn open(wal_path: &Path) -> Result<Self> {
        let mut reader = WalReader::open(wal_path)?;
        let mut max_txn_id: u64 = 0;
        let mut max_commit_timestamp: u64 = 0;

        while let Some(record) = reader.next_record()? {
            match record.record_type {
                RecordType::Begin => {
                    if let Some(txn_id) = decode_txn_id(&record.payload) {
                        max_txn_id = max_txn_id.max(txn_id.get());
                    }
                }
                RecordType::Data => {
                    if let Some((txn_id, _)) = decode_data(&record.payload) {
                        max_txn_id = max_txn_id.max(txn_id.get());
                    }
                }
                RecordType::Commit => {
                    if let Some((txn_id, ts)) = decode_commit(&record.payload) {
                        max_txn_id = max_txn_id.max(txn_id.get());
                        if let Some(ts_val) = ts {
                            max_commit_timestamp = max_commit_timestamp.max(ts_val.get());
                        }
                    }
                }
                RecordType::Abort => {
                    if let Some(txn_id) = decode_txn_id(&record.payload) {
                        max_txn_id = max_txn_id.max(txn_id.get());
                    }
                }
                RecordType::Checkpoint => {}
            }
        }

        let next_txn_id = max_txn_id.checked_add(1).ok_or_else(|| {
            PlomidError::new(plomid_core::ErrorKind::Internal, "WAL TxnId exhausted")
        })?;
        let next_commit_timestamp = max_commit_timestamp.checked_add(1).ok_or_else(|| {
            PlomidError::new(
                plomid_core::ErrorKind::Internal,
                "WAL commit timestamp exhausted",
            )
        })?;

        Ok(Self {
            next_txn_id: AtomicU64::new(next_txn_id),
            next_commit_timestamp: AtomicU64::new(next_commit_timestamp),
            registry: Mutex::new(TxnRegistry {
                active: BTreeSet::new(),
                committed: HashMap::new(),
                last_committed: max_commit_timestamp,
                first_lsn: BTreeMap::new(),
            }),
        })
    }

    /// Rebuilds allocator watermarks from every WAL segment in lexical order.
    pub fn open_segmented(wal_dir: &Path) -> Result<Self> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(wal_dir).map_err(PlomidError::from)? {
            let entry = entry.map_err(PlomidError::from)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if (name.starts_with("wal-") && name.ends_with(".log"))
                || (name.starts_with("WAL-") && name.ends_with(".dat"))
            {
                paths.push(entry.path());
            }
        }
        paths.sort_unstable();
        let mut max_txn_id = 0;
        let mut max_commit_timestamp = 0;
        for path in paths {
            let mut reader = WalReader::open(&path)?;
            while let Some(record) = reader.next_record()? {
                match record.record_type {
                    RecordType::Begin | RecordType::Abort => {
                        if let Some(id) = decode_txn_id(&record.payload) {
                            max_txn_id = max_txn_id.max(id.get());
                        }
                    }
                    RecordType::Data => {
                        if let Some((id, _)) = decode_data(&record.payload) {
                            max_txn_id = max_txn_id.max(id.get());
                        }
                    }
                    RecordType::Commit => {
                        if let Some((id, timestamp)) = decode_commit(&record.payload) {
                            max_txn_id = max_txn_id.max(id.get());
                            if let Some(timestamp) = timestamp {
                                max_commit_timestamp = max_commit_timestamp.max(timestamp.get());
                            }
                        }
                    }
                    RecordType::Checkpoint => {
                        if let Some((txn_id, timestamp)) = checkpoint_watermarks(&record.payload) {
                            max_txn_id = max_txn_id.max(txn_id);
                            max_commit_timestamp = max_commit_timestamp.max(timestamp);
                        }
                    }
                }
            }
        }
        Ok(Self {
            next_txn_id: AtomicU64::new(max_txn_id.checked_add(1).ok_or_else(|| {
                PlomidError::new(plomid_core::ErrorKind::Internal, "WAL TxnId exhausted")
            })?),
            next_commit_timestamp: AtomicU64::new(max_commit_timestamp.checked_add(1).ok_or_else(
                || {
                    PlomidError::new(
                        plomid_core::ErrorKind::Internal,
                        "WAL commit timestamp exhausted",
                    )
                },
            )?),
            registry: Mutex::new(TxnRegistry {
                active: BTreeSet::new(),
                committed: HashMap::new(),
                last_committed: max_commit_timestamp,
                first_lsn: BTreeMap::new(),
            }),
        })
    }

    /// Begins a new transaction and returns its unique identifier.
    ///
    /// The returned `TxnId` is strictly greater than any `TxnId` previously
    /// allocated by this process or observed in the WAL at open time.
    /// The ID is registered active so MVCC snapshots exclude it until commit.
    pub fn begin(&self) -> Result<TxnId> {
        let id = self.next_txn_id.fetch_add(1, Ordering::Relaxed);
        if id > MAX_TXN_ID {
            tracing::error!(target: "transaction", "txn_id_space_exhausted");
            return Err(PlomidError::new(
                plomid_core::ErrorKind::Internal,
                "transaction ID space exhausted",
            ));
        }
        let txn_id = TxnId::new(id);
        self.registry_lock()?.active.insert(txn_id.get());
        tracing::debug!(target: "transaction", "begin txn_id={}", txn_id.get());
        Ok(txn_id)
    }

    /// Returns the highest values already handed out. Checkpoints persist
    /// these watermarks so WAL segments can be reclaimed without reusing IDs.
    #[must_use]
    pub fn watermarks(&self) -> (u64, u64) {
        (
            self.next_txn_id.load(Ordering::Acquire).saturating_sub(1),
            self.next_commit_timestamp
                .load(Ordering::Acquire)
                .saturating_sub(1),
        )
    }

    /// Returns the next transaction identity to be handed out.
    ///
    /// Retention release uses this as a logical seal: any transaction that
    /// begins afterwards receives an identity greater than or equal to this
    /// mark, so every transaction still active *below* the mark began before
    /// the caller evaluated release. This is a monotonic counter read, not a
    /// timestamp and not wall-clock time.
    #[must_use]
    pub fn next_mark(&self) -> u64 {
        self.next_txn_id.load(Ordering::Acquire)
    }

    /// Allocates a commit timestamp for the current transaction.
    ///
    /// The returned `CommitTimestamp` is strictly greater than any previously
    /// allocated commit timestamp, providing a total order for committed
    /// transactions. Prefer [`Self::commit_txn`] so the transaction leaves the
    /// active set and enters the committed map atomically with timestamp
    /// allocation.
    pub fn commit(&self) -> Result<CommitTimestamp> {
        self.commit_txn(TxnId::new(0))
    }

    /// Allocates a commit timestamp for `txn_id` and records it committed.
    pub fn commit_txn(&self, txn_id: TxnId) -> Result<CommitTimestamp> {
        let commit_ts = self.prepare_commit()?;
        self.publish_commit(txn_id, commit_ts)?;
        Ok(commit_ts)
    }

    /// Reserves a commit timestamp without making the transaction visible.
    /// The storage layer uses this between WAL preparation and the durable
    /// commit barrier; a failed append must never leave an in-memory commit.
    pub fn prepare_commit(&self) -> Result<CommitTimestamp> {
        let ts = self.next_commit_timestamp.fetch_add(1, Ordering::Relaxed);
        if ts > MAX_COMMIT_TIMESTAMP {
            tracing::error!(target: "transaction", "commit_timestamp_space_exhausted");
            return Err(PlomidError::new(
                plomid_core::ErrorKind::Internal,
                "commit timestamp space exhausted",
            ));
        }
        Ok(CommitTimestamp::new(ts))
    }

    /// Publishes a timestamp after the corresponding WAL commit and data
    /// durability boundaries have completed.
    pub fn publish_commit(&self, txn_id: TxnId, commit_ts: CommitTimestamp) -> Result<()> {
        let mut registry = self.registry_lock()?;
        registry.active.remove(&txn_id.get());
        registry.committed.insert(txn_id.get(), commit_ts.get());
        registry.first_lsn.remove(&txn_id.get());
        if commit_ts.get() > registry.last_committed {
            registry.last_committed = commit_ts.get();
        }
        tracing::debug!(target: "transaction", "commit_timestamp allocated={}", commit_ts.get());
        Ok(())
    }

    /// Marks `txn_id` aborted: leaves the active set, never committed.
    pub fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        let mut registry = self.registry_lock()?;
        registry.active.remove(&txn_id.get());
        registry.first_lsn.remove(&txn_id.get());
        Ok(())
    }

    /// Records the WAL LSN of `txn_id`'s `Begin` record.
    ///
    /// Called once the `Begin` is appended, so from this point the
    /// transaction has durable records but no applied data pages: checkpoints
    /// cap their boundary below this LSN until publish or abort removes it.
    /// Registration cannot fail after a successful append.
    pub(crate) fn register_begin(&self, txn_id: TxnId, lsn: u64) -> Result<()> {
        self.registry_lock()?.first_lsn.insert(txn_id.get(), lsn);
        Ok(())
    }

    /// Oldest first-record LSN among unfinished transactions, if any.
    ///
    /// Checkpoints must keep their recovery marker and reclaim horizon at or
    /// below `min - 1` of this (when present): everything at or above it may
    /// belong to a transaction whose data pages are not yet applied.
    pub fn oldest_unfinished_lsn(&self) -> Result<Option<u64>> {
        Ok(self.registry_lock()?.first_lsn.values().copied().min())
    }

    /// Captures a consistent snapshot for `owner`.
    ///
    /// The watermark is the highest committed timestamp; the active set is
    /// every other currently-active transaction. Aborted transactions never
    /// enter the store so they need no snapshot representation.
    pub fn snapshot(&self, owner: TxnId) -> Result<Snapshot> {
        let registry = self.registry_lock()?;
        let mut active = registry.active.clone();
        active.remove(&owner.get());
        Ok(Snapshot::new(owner, registry.last_committed, active))
    }

    /// Highest committed timestamp observed (snapshot watermark source).
    pub fn last_committed(&self) -> Result<u64> {
        Ok(self.registry_lock()?.last_committed)
    }

    /// Currently active transaction IDs (GC horizon source).
    pub fn active_txns(&self) -> Result<BTreeSet<u64>> {
        Ok(self.registry_lock()?.active.clone())
    }

    fn registry_lock(&self) -> Result<std::sync::MutexGuard<'_, TxnRegistry>> {
        self.registry.lock().map_err(|_| {
            PlomidError::new(ErrorKind::Internal, "transaction registry lock is poisoned")
        })
    }
}

fn checkpoint_watermarks(payload: &[u8]) -> Option<(u64, u64)> {
    if payload.len() != 16 {
        return None;
    }
    Some((
        u64::from_le_bytes(payload[..8].try_into().ok()?),
        u64::from_le_bytes(payload[8..].try_into().ok()?),
    ))
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

fn decode_txn_id(payload: &[u8]) -> Option<TxnId> {
    if payload.len() != 8 {
        return None;
    }
    let bytes: [u8; 8] = payload.try_into().ok()?;
    Some(TxnId::new(u64::from_le_bytes(bytes)))
}

fn decode_data(payload: &[u8]) -> Option<(TxnId, Vec<u8>)> {
    if payload.len() < 17 {
        return None;
    }
    let txn_id = decode_txn_id(&payload[..8])?;
    Some((txn_id, payload[8..].to_vec()))
}

fn decode_commit(payload: &[u8]) -> Option<(TxnId, Option<CommitTimestamp>)> {
    if payload.len() == 8 {
        let txn_id = decode_txn_id(&payload[..8])?;
        return Some((txn_id, None));
    }
    if payload.len() == 16 {
        let txn_id = decode_txn_id(&payload[..8])?;
        let ts = CommitTimestamp::new(u64::from_le_bytes(payload[8..16].try_into().ok()?));
        return Some((txn_id, Some(ts)));
    }
    None
}
