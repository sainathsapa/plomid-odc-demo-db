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
//! Group durability barrier for the concurrent engine.
//!
//! Commit throughput is bounded by the WAL fsync when every commit performs
//! its own durability barrier while holding the database-wide engine mutex.
//! This coordinator lets concurrent commits share ONE fsync: the first
//! committer to need durability becomes the sync leader, flushes only those
//! WAL segment files whose bytes are not already known durable and are
//! required for the requested target LSN (so neither the engine mutex nor
//! any caller lock is held during the flush), and publishes the durable
//! watermark for the whole group. Later committers wait on a condition
//! variable until the watermark covers their LSN.
//!
//! # Correctness contract
//!
//! * `wait_durable(lsn)` returns only after every record with LSN <= `lsn`
//!   has been flushed to stable storage through some file handle, so callers
//!   may apply data pages and publish visibility afterwards.
//! * A failed fsync never advances the watermark: the failing leader returns
//!   an error, the next waiter becomes leader and retries.
//! * Durability is monotone: once `wait_durable(lsn)` succeeded, any later
//!   `wait_durable(lsn' <= lsn)` succeeds immediately.
//! * Segment files may be reclaimed concurrently (checkpoint retention);
//!   a reclaimed file's records were already proven durable by the
//!   checkpoint that authorized removal, so a missing file is not an error.
//!
//! # Durability tracking
//!
//! The coordinator tracks, per WAL segment sequence, the highest LSN known
//! durable in that segment after a successful group flush. Directory
//! enumeration is used only to discover which segment files currently exist;
//! the decision to fsync a segment is based on its validated header
//! (`first_lsn`) and the coordinator's per-segment durability state, not on
//! the mere presence of a file. A segment is synchronized only when its byte
//! range overlaps `(global_durable, target]` and the required bytes are not
//! already tracked as durable.
//!
//! The coordinator owns only the WAL directory path: appends continue on the
//! engine's own `SegmentedWal` writer while a flush runs, because fsync on
//! any handle to the same file flushes that file's data (POSIX `fsync(2)`,
//! macOS `F_FULLFSYNC`, Windows `FlushFileBuffers`).

use plomid_core::{ErrorKind, Lsn, PlomidError, Result};
use plomid_storage::{FileSystem, RealFs};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::concurrent::{GateMap, TransactionGate};

/// Shared state guarded by [`GroupDurability::state`].
#[derive(Debug, Default)]
struct GroupState {
    /// Highest LSN any committer has asked to become durable.
    pending_max: u64,
    /// Highest LSN known to be durable across the WAL (flushed through some
    /// handle in a previous successful group flush).
    durable: u64,
    /// Whether a sync leader is currently flushing.
    syncing: bool,
    /// Committers currently registered inside `wait_durable`. The leader
    /// snapshots this as its group size, so the coalescing ratio is
    /// observable instead of guessed.
    waiters: u64,
    /// Group size of the previous flush round. Used to tell an isolated client
    /// (every round covers one commit: no reason to linger) from a system
    /// already under commit concurrency (lingering pays for itself).
    last_group: u64,
    /// Per-segment durability tracking: sequence -> highest LSN known durable
    /// in that segment after a successful group flush.
    segment_durable: std::collections::HashMap<u64, u64>,
}

/// How long a sync leader lends to near-simultaneous commits before flushing.
///
/// Without a linger, a committer that happens to arrive microseconds before
/// its peers flushes alone: the WAL fsync (~4.5ms on this host) is fully
/// serialized, so every one-commit round is a wasted durability window.
/// Batching a few extra commits into the same fsync is a large throughput win
/// because the fsync, not the commit bookkeeping, dominates commit latency.
/// This is the same lever as PostgreSQL's `commit_delay`.
const COMMIT_LINGER: Duration = Duration::from_micros(700);
/// Stop lingering once this many committers have joined the round.
const LINGER_GROUP_TARGET: u64 = 16;

/// Group durability coordinator over a WAL directory.
pub struct GroupDurability {
    wal_dir: PathBuf,
    fs: RealFs,
    state: Mutex<GroupState>,
    released: Condvar,
}

fn poisoned() -> PlomidError {
    PlomidError::new(ErrorKind::Internal, "WAL group-commit state poisoned")
}

impl GroupDurability {
    pub fn new(wal_dir: PathBuf) -> Self {
        Self {
            wal_dir,
            fs: RealFs,
            state: Mutex::new(GroupState::default()),
            released: Condvar::new(),
        }
    }

    /// Highest LSN currently known to be durable (test assertions).
    pub fn durable_lsn(&self) -> Result<u64> {
        Ok(self.state.lock().map_err(|_| poisoned())?.durable)
    }

    /// Makes every WAL record through `lsn` durable, coalescing concurrent
    /// requests into one flush per leader round.
    pub fn wait_durable(&self, lsn: Lsn) -> Result<()> {
        let target = lsn.get();
        let entered = Instant::now();
        let mut state = self.state.lock().map_err(|_| poisoned())?;
        state.waiters += 1;
        // Whether this call led a flush round (leader) or queued behind one
        // (follower); the fast path (already durable) is neither and
        // records nothing, so the histograms describe real waits only.
        let mut flushed = false;
        let mut follower_waited = false;
        let outcome = loop {
            if state.durable >= target {
                break Ok(());
            }
            // Register interest BEFORE checking `syncing` so a leader that is
            // already flushing picks up our LSN in a follow-up round if its
            // captured target is smaller.
            state.pending_max = state.pending_max.max(target);
            if state.syncing {
                // Wait for the in-flight flush round to publish its result.
                follower_waited = true;
                state = match self.released.wait(state) {
                    Ok(guard) => guard,
                    Err(error) => {
                        let mut guard = error.into_inner();
                        guard.waiters = guard.waiters.saturating_sub(1);
                        return Err(poisoned());
                    }
                };
                continue;
            }
            // Become the sync leader for everything registered so far.
            state.syncing = true;
            // Linger briefly while the system is already commit-busy so the
            // commits that are only microseconds behind join this flush
            // instead of each paying a full serialized fsync round.
            //
            // The gate is demand-based, not time-based: a lone client, and an
            // idle barrier, see group 1 and a single waiter every round, so
            // they never linger and their latency is unchanged. Only rounds
            // that already batch more than one committer, or that have
            // another committer already waiting, pay the linger -- exactly
            // the rounds where folding more commits into one WAL fsync is a
            // net win. (Bursty arrivals thinner than the linger window itself
            // are left alone deliberately: lingering for stragglers that may
            // be milliseconds away trades a certain delay for an uncertain
            // batch.)
            if state.last_group > 1 || state.waiters > 1 {
                let deadline = Instant::now() + COMMIT_LINGER;
                while state.waiters < LINGER_GROUP_TARGET {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match self.released.wait_timeout(state, deadline - now) {
                        Ok((guard, _timed_out)) => state = guard,
                        Err(error) => {
                            let (mut guard, _) = error.into_inner();
                            guard.syncing = false;
                            guard.waiters = guard.waiters.saturating_sub(1);
                            self.released.notify_all();
                            return Err(poisoned());
                        }
                    }
                }
            }
            // Capture the target AFTER lingering so every commit that joined
            // the round is covered by this one flush.
            let flush_target = state.pending_max;
            let group = state.waiters;
            drop(state);

            let flush_started = Instant::now();
            let flush_result = self.flush_through(flush_target);
            let flush_us = flush_started.elapsed().as_micros() as u64;
            flushed = true;
            plomid_core::observability::record_wal_flush(flush_us);

            state = self.state.lock().map_err(|_| poisoned())?;
            if let Ok(synced) = &flush_result {
                // Publish durability for the segments actually synced in this
                // round so later rounds skip them. The segment set comes from
                // the same directory snapshot the flush used — no second
                // enumeration — so the watermark update cannot observe a
                // different filesystem state than the fsyncs it describes.
                state.durable = state.durable.max(flush_target);
                for (seq, durable_value) in synced {
                    state.segment_durable.insert(*seq, *durable_value);
                }
            }
            state.syncing = false;
            state.last_group = group;
            self.released.notify_all();
            if tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf") {
                tracing::debug!(
                    target: "plomid::perf",
                    event = "group_flush",
                    flush_us,
                    flush_target,
                    group,
                );
            }
            // A failed flush fails only THIS waiter; remaining waiters loop
            // around, elect a new leader, and retry the flush.
            if let Err(error) = flush_result {
                break Err(error);
            }
        };
        state.waiters = state.waiters.saturating_sub(1);
        if !flushed && follower_waited {
            plomid_core::observability::record_group_commit_wait(
                entered.elapsed().as_micros() as u64
            );
        }
        outcome
    }

    /// Flushes only WAL segment files whose bytes in `(global_durable, target]`
    /// are not already known durable, returning per-segment durable watermarks.
    ///
    /// The returned vector carries `(sequence, durable_lsn)` for every segment
    /// the flush proved durable in this round, computed from the same
    /// directory snapshot the fsyncs used — so the caller publishes watermarks
    /// without a second enumeration. An empty vector with `Ok` still advances
    /// the global watermark (nothing new needed syncing); an `Err` advances
    /// nothing so the next leader retries.
    ///
    /// Directory enumeration discovers which segment files currently exist;
    /// the decision to fsync a segment is based on its validated header
    /// (`first_lsn`) and the coordinator's per-segment durability state.
    ///
    /// # Ordering invariant (selective fsync relies on both halves)
    ///
    /// * Rotation deferral: `SegmentedWal` never rotates while a transaction
    ///   is open, so segment boundaries fall only at transaction boundaries.
    ///   A sealed segment's LSN range (`first_lsn` through the next segment's
    ///   `first_lsn - 1`) is therefore stable: discovery can never observe a
    ///   segment whose range later grows underneath the selection below.
    /// * Prepare-before-register: every WAL append — including the file
    ///   creation a rotation performs — completes under the engine mutex
    ///   before the committer registers interest (`pending_max` in
    ///   `wait_durable`), and the leader captures its flush target from
    ///   `pending_max` before enumerating the directory here. So any segment
    ///   holding records `<= flush_target` already exists when it is
    ///   enumerated, and skipping an already-durable segment can never skip
    ///   bytes the target needs. A committer that registers after the capture
    ///   simply waits for the next round.
    fn flush_through(&self, target: u64) -> Result<Vec<(u64, u64)>> {
        let global_durable = {
            let state = self.state.lock().map_err(|_| poisoned())?;
            state.durable
        };

        // Discover retained segments and read each one's validated header so
        // the sync decision is based on LSN ranges, not on file presence.
        let segments = Self::discover_segments(&self.wal_dir)?;

        // Select the segments whose byte range overlaps (global_durable, target]
        // and whose required bytes are not already tracked as durable.
        let to_sync = Self::select_segments_for_target(
            &segments,
            target,
            global_durable,
            &self.state.lock().map_err(|_| poisoned())?.segment_durable,
        )?;

        if to_sync.is_empty() {
            return Ok(Vec::new());
        }

        let mut result: Result<Vec<u64>> = Ok(Vec::new());
        for (seq, _first_lsn, path) in &to_sync {
            match self.fs.open(path) {
                Ok(mut file) => {
                    if let Err(error) = self.fs.fsync(&mut file) {
                        result = Err(error);
                        break;
                    }
                    if let Ok(synced) = result.as_mut() {
                        synced.push(*seq);
                    }
                }
                // A concurrently reclaimed segment was already proven durable
                // by the checkpoint that authorized its removal.
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        // Publish per-segment watermarks from the same snapshot the selection
        // used: sealed ranges are stable (rotation deferral), so `next
        // first_lsn - 1` observed here is the range the fsyncs just proved
        // durable. Computed only on success; a failed round advances nothing
        // and the next leader re-syncs from scratch (extra I/O, never missed
        // durability).
        result.map(|synced_seqs| {
            let mut watermarks = Vec::with_capacity(synced_seqs.len());
            for seq in &synced_seqs {
                let Some(pos) = to_sync.iter().position(|(s, _, _)| s == seq) else {
                    continue;
                };
                let first_lsn = to_sync[pos].1;
                // Skip segments concurrently reclaimed mid-round: their records
                // were already proven durable by the checkpoint that removed
                // them, and the global watermark below still advances past the
                // target.
                let Some(idx) = segments.iter().position(|(s, _, _)| s == seq) else {
                    continue;
                };
                // A reclaimed-and-recreated sequence number is impossible: WAL
                // sequences increase monotonically and are never reused within
                // a directory lifetime, so the snapshot's successor (if any)
                // still bounds this segment's range.
                let segment_end = if idx + 1 < segments.len() {
                    segments[idx + 1].1.saturating_sub(1)
                } else {
                    target
                };
                watermarks.push((*seq, segment_end.max(first_lsn.saturating_sub(1))));
            }
            watermarks
        })
    }

    /// Returns the retained WAL segments with their validated `first_lsn`.
    fn discover_segments(wal_dir: &std::path::Path) -> Result<Vec<(u64, u64, PathBuf)>> {
        let entries = std::fs::read_dir(wal_dir).map_err(PlomidError::from)?;
        let mut segments = Vec::new();
        for entry in entries {
            let entry = entry.map_err(PlomidError::from)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = parse_sequence_for_discovery(&name) {
                let path = entry.path();
                let first_lsn = read_segment_first_lsn(&path)?;
                segments.push((seq, first_lsn, path));
            }
        }
        segments.sort_by_key(|(seq, _, _)| *seq);
        Ok(segments)
    }

    /// Returns the segments that must be fsynced for `target` given the
    /// current durability tracking.
    fn select_segments_for_target(
        segments: &[(u64, u64, PathBuf)],
        target: u64,
        global_durable: u64,
        segment_durable: &std::collections::HashMap<u64, u64>,
    ) -> Result<Vec<(u64, u64, PathBuf)>> {
        if target <= global_durable {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (i, (seq, first_lsn, _path)) in segments.iter().enumerate() {
            if *first_lsn > target {
                continue;
            }
            // End of this segment's LSN range: for a sealed segment it is
            // the next segment's first_lsn - 1; for the active (last)
            // segment it is at least `target`.
            let segment_end = if i + 1 < segments.len() {
                segments[i + 1].1 - 1
            } else {
                target
            };
            if segment_end <= global_durable {
                continue;
            }
            // Check whether the required range is already known durable.
            let already = segment_durable.get(seq).copied().unwrap_or(0);
            if already >= segment_end {
                continue;
            }
            out.push((*seq, *first_lsn, segments[i].2.clone()));
        }
        Ok(out)
    }
}

/// Reads the validated `first_lsn` from a WAL segment header.
fn read_segment_first_lsn(path: &std::path::Path) -> Result<u64> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(PlomidError::from)?;
    let mut header = [0_u8; 32];
    if file.read_exact(&mut header).is_err() {
        // An unpublished (still-being-created) segment has no complete header.
        // Treat its first LSN as 1 so it is considered for syncing; the file
        // will either be completed and validated later, or reclaimed.
        return Ok(1);
    }
    // Validate magic + version before trusting the LSN field.
    if header[0..4] != plomid_core::SEGMENT_MAGIC {
        return Err(PlomidError::new(
            ErrorKind::Corruption,
            "WAL segment has invalid magic",
        ));
    }
    let version = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| PlomidError::new(ErrorKind::Corruption, "invalid WAL segment version"))?,
    );
    if version != plomid_core::SEGMENT_VERSION {
        return Err(PlomidError::new(
            ErrorKind::Corruption,
            "unsupported WAL segment version",
        ));
    }
    let first_lsn = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| PlomidError::new(ErrorKind::Corruption, "invalid WAL segment LSN"))?,
    );
    if first_lsn == 0 {
        return Err(PlomidError::new(
            ErrorKind::Corruption,
            "WAL segment has zero first LSN",
        ));
    }
    Ok(first_lsn)
}

/// Parses a sequence number from a `WAL-<n>.dat` filename for discovery.
fn parse_sequence_for_discovery(name: &str) -> Option<u64> {
    name.strip_prefix(plomid_core::WAL_NEW_PREFIX)
        .and_then(|n| n.strip_suffix(plomid_core::WAL_NEW_SUFFIX))
        .map(|n| n.trim_start_matches('0'))
        .map(|seq| if seq.is_empty() { "0" } else { seq })
        .and_then(|seq| seq.parse::<u64>().ok())
        .filter(|seq| *seq != 0)
}

/// Cache of per-domain (per-table) write gates.
///
/// The executor reserves a write lane per statement domain (the target table)
/// instead of one database-wide gate, so statements on independent tables run
/// concurrently while statements on the same table keep the historical
/// serialized semantics. Domains are tiny and bounded by the table count, so
/// gates accumulate for the lifetime of the engine.
#[derive(Default)]
pub(crate) struct GateTable {
    gates: Mutex<HashMap<Vec<u8>, Arc<TransactionGate>>>,
}

impl GateTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn gate_for(&self, domain: &[u8]) -> Result<Arc<TransactionGate>> {
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| PlomidError::new(ErrorKind::Internal, "write gate table poisoned"))?;
        Ok(gates
            .entry(domain.to_vec())
            .or_insert_with(|| Arc::new(TransactionGate::new()))
            .clone())
    }
}

/// Row-key write-gate table with orphan eviction.
///
/// Unlike [`GateTable`] (bounded by the table count), row gates are created
/// per contended row key, so entries are removed as soon as the last guard
/// drops and no waiter holds a clone: `TransactionGate::evict_if_orphaned`
/// runs under the map lock, so a concurrent lookup either cloned the gate
/// first (the entry stays and the waiter proceeds) or creates a fresh gate
/// after removal. The map is therefore bounded by concurrently held or
/// awaited row keys, not by the table size.
pub(crate) struct RowGateTable {
    gates: Arc<GateMap>,
}

impl RowGateTable {
    pub(crate) fn new() -> Self {
        Self {
            gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn gate_for(&self, key: &[u8]) -> Result<Arc<TransactionGate>> {
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| PlomidError::new(ErrorKind::Internal, "row gate table poisoned"))?;
        if let Some(gate) = gates.get(key) {
            return Ok(Arc::clone(gate));
        }
        let gate = Arc::new(TransactionGate::new_evictable(key.to_vec(), &self.gates));
        gates.insert(key.to_vec(), Arc::clone(&gate));
        Ok(gate)
    }

    /// Number of live row gates (diagnostics/tests). Must stay bounded by the
    /// concurrently held rows, never by the number of rows ever touched.
    pub(crate) fn len(&self) -> usize {
        self.gates.lock().map(|gates| gates.len()).unwrap_or(0)
    }
}

/// Unique-reservation gate table with orphan eviction, keyed by the full
/// conflict-domain bytes (`\0U\0<index>\0<canonical value>`).
///
/// TRANSIENT COORDINATION ONLY: entries exist solely while some active
/// transaction is resolving a uniqueness decision for that exact
/// `(index, value)` pair and are evicted by `evict_if_orphaned` once the
/// last guard drops — the map never grows with the number of distinct
/// unique values ever inserted. The durable B+Tree unique index remains the
/// authoritative uniqueness state; nothing here is persisted or recovered.
pub(crate) struct UniqueGateTable {
    gates: Arc<GateMap>,
}

impl UniqueGateTable {
    pub(crate) fn new() -> Self {
        Self {
            gates: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn gate_for(&self, domain: &[u8]) -> Result<Arc<TransactionGate>> {
        let mut gates = self
            .gates
            .lock()
            .map_err(|_| PlomidError::new(ErrorKind::Internal, "unique gate table poisoned"))?;
        if let Some(gate) = gates.get(domain) {
            return Ok(Arc::clone(gate));
        }
        let gate = Arc::new(TransactionGate::new_evictable(domain.to_vec(), &self.gates));
        gates.insert(domain.to_vec(), Arc::clone(&gate));
        Ok(gate)
    }
}

/// Atomic per-table internal row-id allocator for the concurrent engine.
///
/// INSERT used to allocate row ids through a transactional meta-key
/// read/modify/write that only the table write lane serialized; with the
/// lane removed for reservation-based INSERTs, two concurrent statements
/// could stage the same row id and one row would overwrite the other. The
/// allocator is a plain atomic counter seeded ONCE per table from committed
/// state (the max numeric row suffix + 1): committed rows are ground truth
/// for "which ids already exist", and the seed is taken before any
/// reservation-holder INSERT can stage a row on this table, so the floor is
/// always >= every id committed before this engine opened.
pub(crate) struct RowIdAllocator {
    counters: Mutex<HashMap<String, i64>>,
}

impl RowIdAllocator {
    pub(crate) fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the next row id for `table`.
    ///
    /// Returns `None` when the counter for `table` is not yet seeded; the
    /// caller computes the seed from committed state and registers it via
    /// [`Self::seed`]. Two-phase so the seed scan can run WITHOUT the counter
    /// lock held (it takes the engine mutex).
    pub(crate) fn counter_for(&self, table: &str) -> Option<i64> {
        self.counters
            .lock()
            .ok()
            .and_then(|counters| counters.get(table).copied())
    }

    /// Registers (or replaces) the counter for `table` with `seed`.
    pub(crate) fn seed(&self, table: &str, seed: i64) {
        if let Ok(mut counters) = self.counters.lock() {
            counters.entry(table.to_string()).or_insert(seed);
        }
    }

    /// Atomically returns the next row id after `previous` for `table`.
    pub(crate) fn next(&self, table: &str, previous: i64) -> i64 {
        if let Ok(mut counters) = self.counters.lock() {
            let counter = counters.entry(table.to_string()).or_insert(previous);
            let next = counter.wrapping_add(1);
            *counter = next;
            next
        } else {
            previous.wrapping_add(1)
        }
    }
}
