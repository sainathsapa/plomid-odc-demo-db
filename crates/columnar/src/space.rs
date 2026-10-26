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
//! Space accounting, orphan reclamation, and maintenance scheduling.
//!
//! This module is the *space layer*. It owns no storage, WAL, MVCC,
//! generation, checkpoint, recovery, or compaction implementation of its own;
//! it observes the ones that already exist:
//!
//! ```text
//! FLUSH / COMPACT (existing ColumnarStore / compact())
//!       │  caller reports sizes through record_*
//!       ▼
//! SpaceEngine (this module): per-table live / dead / orphan bytes + rows
//!       │  tables_by_debt() = compaction priority (dead_ratio desc)
//!       ▼
//! SpaceScheduler (this module): debt order with starvation aging
//!       │  next_candidate(max_concurrent), should_run_maintenance(table)
//!       ▼
//! EXECUTOR WORKER (existing): polls the advisory answer, runs compact()
//!
//! OrphanScan (this module): slices on disk referenced by no live generation
//!       │  find_orphan_slices() reuses the slice_ownership_snapshot pattern
//!       │  (on-disk set minus referenced set), reclaim_orphans() deletes
//!       │  through the existing idempotent ColumnarStore::delete_segment.
//! ```
//!
//! # Wiring (no changes to compaction.rs)
//!
//! Compaction never calls into this module. The *caller* of
//! [`crate::compaction::compact`] bridges the outcome back into the space
//! layer, in this order:
//!
//! 1. After every flush, [`SpaceEngine::record_published`] with the segment's
//!    measured image bytes and row count.
//! 2. After publication supersedes the inputs, [`SpaceEngine::record_dead`]
//!    for each superseded segment (its bytes move live → dead).
//! 3. After the run, [`SpaceEngine::observe_outcome`] with the table name and
//!    the [`crate::compaction::CompactionOutcome`]: it folds the measured
//!    row delta (`sum(input_rows) - output_rows`) into dead rows, resets live
//!    rows to the new generation's exact row count, and attributes the
//!    measured reclaimed bytes to the reclaimed segments. It never invents
//!    dead *bytes*: bytes the caller did not report stay uncounted rather
//!    than estimated.
//!
//! # Executor integration (advisory only)
//!
//! [`SpaceScheduler::should_run_maintenance`] is the single integration point
//! for the executor's maintenance worker: it answers whether a table's debt
//! currently justifies a compaction run. It dispatches nothing, spawns no
//! thread, and rewrites no worker; the worker keeps its own loop and treats a
//! `true` answer as permission to run the existing `compact()`.
//!
//! # Production guarantees
//!
//! * Saturating arithmetic everywhere: counters can never wrap, underflow, or
//!   panic on overflow; unknown-segment reclaims floor at zero.
//! * Orphan scan is fail-closed: an unloadable catalog snapshot or generation
//!   metadata aborts the scan with an empty result (never a deletion). A
//!   per-segment deletion failure skips that segment only.
//! * Scheduler starvation-freedom: every selection round ages the tables it
//!   did not pick; past [`SCHEDULER_STARVATION_WAITS`] missed rounds a table
//!   becomes eligible regardless of debt, so the lowest-debt table eventually
//!   runs.
//! * Memory bound: at most `max_tables` tables and `max_segments_per_table`
//!   ledger entries per table; zero-debt tables are evicted first and every
//!   dropped event is counted in [`SpaceEngine::dropped_events`].

use crate::compaction::CompactionOutcome;
use crate::store::{
    ColumnarStore, COLUMNAR_KEY_PREFIX, COLUMNAR_MANIFEST_KIND, COLUMNAR_SLICE_KIND,
};
use plomid_core::SegmentId;
use plomid_storage::{load_generation, GenerationManager, StorageEngine};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// Per-selection-round debt bonus applied to a table's waiting time.
///
/// A table that misses a round gains this much effective debt, so sustained
/// waiting eventually outweighs any real debt gap (see
/// [`SCHEDULER_STARVATION_WAITS`]).
pub const SCHEDULER_AGING_BONUS: f64 = 0.01;

/// Missed selection rounds after which a table becomes eligible regardless of
/// its dead ratio. The bound that makes the scheduler starvation-free.
pub const SCHEDULER_STARVATION_WAITS: u64 = 100;

/// Default cap on tracked tables in [`SpaceEngine::new`].
pub const DEFAULT_MAX_TABLES: usize = 1024;

/// Default cap on per-table segment ledger entries in [`SpaceEngine::new`].
pub const DEFAULT_MAX_SEGMENTS_PER_TABLE: usize = 8192;

/// Default cap on scheduler wait-map entries.
pub const DEFAULT_MAX_TRACKED_WAITS: usize = 4096;

// ---------------------------------------------------------------------------
// SpaceAccount
// ---------------------------------------------------------------------------

/// Measured space state of one table: live, dead (superseded but still
/// referenced on disk), and orphan (on disk, referenced by nothing) bytes,
/// plus live and dead row counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceAccount {
    /// Table these counters describe.
    pub table: String,
    /// Bytes in segments that are still referenced by a live generation.
    pub live_bytes: u64,
    /// Bytes in superseded segments not yet reclaimed.
    pub dead_bytes: u64,
    /// Bytes in slices no live generation references.
    pub orphan_bytes: u64,
    /// Rows in the current live set.
    pub live_rows: u64,
    /// Rows superseded by compaction but not yet reclaimed.
    pub dead_rows: u64,
}

impl SpaceAccount {
    /// Zero account for `table`.
    #[must_use]
    pub fn empty(table: &str) -> Self {
        Self {
            table: table.to_string(),
            live_bytes: 0,
            dead_bytes: 0,
            orphan_bytes: 0,
            live_rows: 0,
            dead_rows: 0,
        }
    }

    /// Total accounted bytes: live + dead + orphan, saturating.
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.live_bytes
            .saturating_add(self.dead_bytes)
            .saturating_add(self.orphan_bytes)
    }

    /// Fraction of accounted bytes that is still live, in `[0.0, 1.0]`.
    ///
    /// An empty account reports `0.0` (no live data), never NaN.
    #[must_use]
    pub fn live_ratio(&self) -> f64 {
        let total = self.total_bytes();
        if total == 0 {
            0.0
        } else {
            self.live_bytes as f64 / total as f64
        }
    }

    /// Fraction of accounted bytes that is reclaimable debt (dead + orphan).
    ///
    /// This is the compaction-priority signal [`SpaceEngine::tables_by_debt`]
    /// sorts by. An empty account reports `0.0`, never NaN.
    #[must_use]
    pub fn dead_ratio(&self) -> f64 {
        let total = self.total_bytes();
        if total == 0 {
            0.0
        } else {
            self.dead_bytes.saturating_add(self.orphan_bytes) as f64 / total as f64
        }
    }

    /// True when every counter is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_bytes() == 0 && self.live_rows == 0 && self.dead_rows == 0
    }
}

// ---------------------------------------------------------------------------
// SpaceEngine
// ---------------------------------------------------------------------------

/// Liveness of one tracked segment inside the per-table ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SegmentKind {
    Live,
    Dead,
    Orphan,
}

/// One ledger entry: the size the space layer recorded for a segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SegmentRecord {
    bytes: u64,
    rows: u64,
    kind: SegmentKind,
}

/// Per-table counters plus the bounded segment ledger behind them.
#[derive(Clone, Debug, Default)]
struct TableSpace {
    live_bytes: u64,
    dead_bytes: u64,
    orphan_bytes: u64,
    live_rows: u64,
    dead_rows: u64,
    segments: HashMap<SegmentId, SegmentRecord>,
}

impl TableSpace {
    /// True when the table holds no debt: nothing to compact, safe to evict.
    fn is_zero_debt(&self) -> bool {
        self.dead_bytes == 0 && self.orphan_bytes == 0 && self.dead_rows == 0
    }

    fn remove_from_bucket(&mut self, record: &SegmentRecord) {
        match record.kind {
            SegmentKind::Live => {
                self.live_bytes = self.live_bytes.saturating_sub(record.bytes);
                self.live_rows = self.live_rows.saturating_sub(record.rows);
            }
            SegmentKind::Dead => {
                self.dead_bytes = self.dead_bytes.saturating_sub(record.bytes);
            }
            SegmentKind::Orphan => {
                self.orphan_bytes = self.orphan_bytes.saturating_sub(record.bytes);
            }
        }
    }
}

/// In-memory space ledger over many tables.
///
/// The engine is deliberately lock-free: callers that share it across threads
/// wrap it in a `Mutex<SpaceEngine>`. That keeps the single-op cost to a hash
/// lookup plus saturating adds (see the `space_accounting_overhead` bench).
#[derive(Clone, Debug)]
pub struct SpaceEngine {
    tables: BTreeMap<String, TableSpace>,
    max_tables: usize,
    max_segments_per_table: usize,
    dropped_events: u64,
}

impl SpaceEngine {
    /// Creates an engine with the default memory bounds
    /// ([`DEFAULT_MAX_TABLES`] tables, [`DEFAULT_MAX_SEGMENTS_PER_TABLE`]
    /// ledger entries per table).
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            max_tables: DEFAULT_MAX_TABLES,
            max_segments_per_table: DEFAULT_MAX_SEGMENTS_PER_TABLE,
            dropped_events: 0,
        }
    }

    /// Creates an engine with explicit memory bounds.
    ///
    /// A `max_tables` of zero tracks nothing (every event is dropped and
    /// counted); prefer [`Self::new`] in production.
    #[must_use]
    pub fn with_limits(max_tables: usize, max_segments_per_table: usize) -> Self {
        Self {
            tables: BTreeMap::new(),
            max_tables,
            max_segments_per_table,
            dropped_events: 0,
        }
    }

    /// Number of tables currently tracked.
    #[must_use]
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Events dropped because no table slot or ledger slot was available.
    ///
    /// Counters of already-tracked tables stay exact when events drop; only
    /// the untracked table's state is missing.
    #[must_use]
    pub fn dropped_events(&self) -> u64 {
        self.dropped_events
    }

    /// True when `table` is currently tracked.
    #[must_use]
    pub fn contains(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Returns the table's space, creating it when a slot is available.
    ///
    /// When the table is new and the engine is full, one zero-debt table is
    /// evicted first (lowest name order, deterministic). When no zero-debt
    /// table exists the event is dropped and counted instead of corrupting a
    /// tracked table's counters.
    fn space_for(&mut self, table: &str) -> Option<&mut TableSpace> {
        if self.tables.contains_key(table) {
            return self.tables.get_mut(table);
        }
        if self.tables.len() >= self.max_tables {
            let victim = self
                .tables
                .iter()
                .find(|(_, space)| space.is_zero_debt())
                .map(|(name, _)| name.clone());
            match victim {
                Some(name) => {
                    self.tables.remove(&name);
                }
                None => {
                    self.dropped_events = self.dropped_events.saturating_add(1);
                    return None;
                }
            }
        }
        self.tables.insert(table.to_string(), TableSpace::default());
        self.tables.get_mut(table)
    }

    /// Records a newly published segment: its measured image bytes and rows
    /// become live.
    ///
    /// Re-publishing an already-tracked segment replaces its record: the old
    /// bucket contribution is removed first, so the counters stay exact.
    pub fn record_published(&mut self, table: &str, segment: SegmentId, bytes: u64, rows: u64) {
        let cap = self.max_segments_per_table;
        let Some(space) = self.space_for(table) else {
            return;
        };
        if let Some(old) = space.segments.remove(&segment) {
            space.remove_from_bucket(&old);
        }
        // The ledger is bounded: past the cap the bytes still count in the
        // aggregates, but the segment is untracked (reclaims for it fall back
        // to the dead-then-orphan attribution in record_reclaimed).
        if space.segments.len() >= cap && !space.segments.contains_key(&segment) {
            space.live_bytes = space.live_bytes.saturating_add(bytes);
            space.live_rows = space.live_rows.saturating_add(rows);
            return;
        }
        space.segments.insert(
            segment,
            SegmentRecord {
                bytes,
                rows,
                kind: SegmentKind::Live,
            },
        );
        space.live_bytes = space.live_bytes.saturating_add(bytes);
        space.live_rows = space.live_rows.saturating_add(rows);
    }

    /// Records a segment superseded by a newer publication: its bytes move
    /// live → dead.
    ///
    /// When the segment was never published through this engine its `bytes`
    /// are counted as dead directly, so debt is never silently lost.
    pub fn record_dead(&mut self, table: &str, segment: SegmentId, bytes: u64) {
        let cap = self.max_segments_per_table;
        let Some(space) = self.space_for(table) else {
            return;
        };
        match space.segments.remove(&segment) {
            Some(old) => {
                space.remove_from_bucket(&old);
                // The recorded size is authoritative over the caller's; when
                // the segment was already dead this is a net no-op, which
                // makes double reports idempotent.
                space.dead_bytes = space.dead_bytes.saturating_add(old.bytes);
                space.segments.insert(
                    segment,
                    SegmentRecord {
                        bytes: old.bytes,
                        rows: old.rows,
                        kind: SegmentKind::Dead,
                    },
                );
            }
            None => {
                if space.segments.len() < cap {
                    space.segments.insert(
                        segment,
                        SegmentRecord {
                            bytes,
                            rows: 0,
                            kind: SegmentKind::Dead,
                        },
                    );
                }
                space.dead_bytes = space.dead_bytes.saturating_add(bytes);
            }
        }
    }

    /// Records a slice set the orphan scan found on disk but referenced by no
    /// live generation.
    pub fn record_orphan(&mut self, table: &str, segment: SegmentId, bytes: u64) {
        let cap = self.max_segments_per_table;
        let Some(space) = self.space_for(table) else {
            return;
        };
        if let Some(old) = space.segments.remove(&segment) {
            space.remove_from_bucket(&old);
        }
        if space.segments.len() >= cap && !space.segments.contains_key(&segment) {
            space.orphan_bytes = space.orphan_bytes.saturating_add(bytes);
            return;
        }
        space.segments.insert(
            segment,
            SegmentRecord {
                bytes,
                rows: 0,
                kind: SegmentKind::Orphan,
            },
        );
        space.orphan_bytes = space.orphan_bytes.saturating_add(bytes);
    }

    /// Records a reclaimed segment: its bytes leave whichever bucket the
    /// ledger holds it in.
    ///
    /// A tracked segment's recorded size is authoritative and `bytes` is
    /// ignored. An untracked segment's `bytes` are attributed dead-first,
    /// then orphan (never live: reclaiming must not corrupt the live set),
    /// flooring at zero, so double reclaims cannot underflow.
    pub fn record_reclaimed(&mut self, table: &str, segment: SegmentId, bytes: u64) {
        let Some(space) = self.tables.get_mut(table) else {
            return;
        };
        if let Some(old) = space.segments.remove(&segment) {
            space.remove_from_bucket(&old);
            return;
        }
        let mut rest = bytes;
        let take = space.dead_bytes.min(rest);
        space.dead_bytes -= take;
        rest -= take;
        let take = space.orphan_bytes.min(rest);
        space.orphan_bytes -= take;
        rest -= take;
        let _ = rest;
    }

    /// Folds one [`CompactionOutcome`] into `table`'s counters.
    ///
    /// * `dead_rows` grows by `sum(input_rows) - output_rows` (saturating;
    ///   zero when compaction removed nothing).
    /// * `live_rows` is set to `output_rows`: after the run the new
    ///   generation's row set *is* the table's live set, exactly.
    /// * every reclaimed segment is passed to [`Self::record_reclaimed`],
    ///   sharing [`CompactionOutcome::reclaimed_segment_bytes`] evenly as
    ///   the size for segments this engine never tracked.
    ///
    /// Dead *bytes* are deliberately untouched: only the caller knows which
    /// superseded segments they measured, reported via [`Self::record_dead`].
    pub fn observe_outcome(&mut self, table: &str, outcome: &CompactionOutcome) {
        let input_rows: u64 = outcome
            .input_rows
            .iter()
            .fold(0u64, |total, (_, rows)| total.saturating_add(*rows));
        let output_rows = outcome.output_rows as u64;
        let reclaimed = outcome.reclaimed_segments.clone();
        let reclaimed_bytes = outcome.reclaimed_segment_bytes;
        {
            let Some(space) = self.space_for(table) else {
                return;
            };
            space.dead_rows = space
                .dead_rows
                .saturating_add(input_rows.saturating_sub(output_rows));
            space.live_rows = output_rows;
        }
        if reclaimed.is_empty() {
            return;
        }
        let count = reclaimed.len() as u64;
        let share = reclaimed_bytes / count;
        let mut remainder = reclaimed_bytes % count;
        for segment in &reclaimed {
            let mut bytes = share;
            if remainder > 0 {
                bytes = bytes.saturating_add(1);
                remainder -= 1;
            }
            self.record_reclaimed(table, *segment, bytes);
        }
    }

    /// Returns `table`'s current account (all zeros when untracked).
    #[must_use]
    pub fn account(&self, table: &str) -> SpaceAccount {
        match self.tables.get(table) {
            Some(space) => SpaceAccount {
                table: table.to_string(),
                live_bytes: space.live_bytes,
                dead_bytes: space.dead_bytes,
                orphan_bytes: space.orphan_bytes,
                live_rows: space.live_rows,
                dead_rows: space.dead_rows,
            },
            None => SpaceAccount::empty(table),
        }
    }

    /// Every tracked table's account, dead ratio descending (name ascending
    /// on ties): the compaction-priority order.
    #[must_use]
    pub fn tables_by_debt(&self) -> Vec<SpaceAccount> {
        let mut accounts: Vec<SpaceAccount> = self
            .tables
            .iter()
            .map(|(table, space)| SpaceAccount {
                table: table.clone(),
                live_bytes: space.live_bytes,
                dead_bytes: space.dead_bytes,
                orphan_bytes: space.orphan_bytes,
                live_rows: space.live_rows,
                dead_rows: space.dead_rows,
            })
            .collect();
        accounts.sort_by(|left, right| {
            right
                .dead_ratio()
                .total_cmp(&left.dead_ratio())
                .then_with(|| left.table.cmp(&right.table))
        });
        accounts
    }

    /// True when `table`'s dead ratio exceeds `max_dead_ratio`.
    ///
    /// Untracked tables report `false`: no evidence of debt is no debt.
    #[must_use]
    pub fn needs_compaction(&self, table: &str, max_dead_ratio: f64) -> bool {
        self.account(table).dead_ratio() > max_dead_ratio
    }

    /// Removes every tracked table that holds no debt; returns names removed.
    ///
    /// The memory-bound counterpart to the automatic zero-debt eviction in
    /// [`Self::space_for`]: idle tables never accumulate.
    pub fn evict_idle(&mut self) -> Vec<String> {
        let idle: Vec<String> = self
            .tables
            .iter()
            .filter(|(_, space)| space.is_zero_debt())
            .map(|(table, _)| table.clone())
            .collect();
        for table in &idle {
            self.tables.remove(table);
        }
        idle
    }
}

impl Default for SpaceEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// OrphanScan
// ---------------------------------------------------------------------------

/// Maintenance scan for columnar slices that are durable on disk but
/// referenced by no live generation.
///
/// The scan reuses the [`crate::compaction::slice_ownership_snapshot`] logic
/// pattern — on-disk set minus referenced set — but inverts the question:
/// instead of "which slices of these reclaimed generations may go", it asks
/// "which slices on disk does *any* live generation still need".
#[derive(Clone, Copy, Debug, Default)]
pub struct OrphanScan;

impl OrphanScan {
    /// Finds orphan slices: `(segment, measured bytes)` pairs on disk that no
    /// live generation references, ascending by segment.
    ///
    /// Fail-closed, never fail-deadly:
    /// * an unreadable engine scan, catalog snapshot, or *any single*
    ///   generation's metadata aborts the whole scan with an empty result —
    ///   an unloadable generation might reference the slice, so its slices
    ///   survive for a later run;
    /// * the result is measurement only; nothing is deleted here.
    pub fn find_orphan_slices<E: StorageEngine>(
        store: &ColumnarStore,
        engine: &mut E,
        manager: &Arc<GenerationManager>,
    ) -> Vec<(SegmentId, u64)> {
        let _ = store;
        let entries = match engine.scan(None, None) {
            Ok(entries) => entries,
            Err(_) => return Vec::new(),
        };
        let mut on_disk: HashMap<u64, u64> = HashMap::new();
        for (key, value) in &entries {
            // Length first: every index below is in bounds only for these
            // two key shapes (manifest 13 B, slice 17 B).
            let known_kind = match key.len() {
                13 => key[12] == COLUMNAR_MANIFEST_KIND,
                17 => key[12] == COLUMNAR_SLICE_KIND,
                _ => false,
            };
            if !known_kind || key[..4] != COLUMNAR_KEY_PREFIX {
                continue;
            }
            let id = u64::from_be_bytes(key[4..12].try_into().unwrap_or([0u8; 8]));
            if id != 0 {
                *on_disk.entry(id).or_insert(0) = on_disk
                    .get(&id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(value.len() as u64);
            }
        }
        if on_disk.is_empty() {
            return Vec::new();
        }
        // Referenced set: every segment named by any generation any live
        // record still owns. Any unloadable generation aborts the scan:
        // it may reference the slice, so nothing is reported this run.
        let snapshot = match manager.load() {
            Ok(snapshot) => snapshot,
            Err(_) => return Vec::new(),
        };
        let mut referenced: BTreeSet<u64> = BTreeSet::new();
        for record in snapshot.records() {
            for generation in record.generation_ids() {
                let metadata = match load_generation(manager.root(), generation) {
                    Ok(metadata) => metadata,
                    Err(_) => return Vec::new(),
                };
                referenced.extend(
                    metadata
                        .references
                        .iter()
                        .map(|reference| reference.structure.segment_id.get()),
                );
            }
        }
        let mut orphans: Vec<(SegmentId, u64)> = on_disk
            .into_iter()
            .filter(|(id, _)| !referenced.contains(id))
            .map(|(id, bytes)| (SegmentId::new(id), bytes))
            .collect();
        orphans.sort_unstable();
        orphans
    }

    /// Deletes `orphans` through the existing idempotent
    /// [`ColumnarStore::delete_segment`] path and returns the measured bytes
    /// removed.
    ///
    /// A segment whose manifest is already absent contributes zero bytes
    /// (idempotent retry); a segment that fails deletion is skipped without
    /// failing the run, exactly like the compaction slice-reclamation path.
    /// Bytes come from [`crate::store::DeletedSegment::bytes_removed`] as
    /// measured before deletion, never from the scan's estimate.
    pub fn reclaim_orphans<E: StorageEngine>(
        store: &ColumnarStore,
        engine: &mut E,
        orphans: &[(SegmentId, u64)],
    ) -> u64 {
        let mut reclaimed = 0u64;
        for (segment, _) in orphans {
            match store.delete_segment(engine, *segment) {
                Ok(outcome) => {
                    reclaimed = reclaimed.saturating_add(outcome.bytes_removed);
                }
                Err(_) => continue,
            }
        }
        reclaimed
    }

    /// One maintenance pass: find orphans, then reclaim them.
    ///
    /// Returns `(segments deleted, bytes reclaimed)`. "Deleted" counts
    /// segments whose manifest was present; already-gone segments still
    /// contribute their measured zero.
    pub fn scan_and_reclaim<E: StorageEngine>(
        store: &ColumnarStore,
        engine: &mut E,
        manager: &Arc<GenerationManager>,
    ) -> (usize, u64) {
        let orphans = Self::find_orphan_slices(store, engine, manager);
        if orphans.is_empty() {
            return (0, 0);
        }
        let count = orphans.len();
        let bytes = Self::reclaim_orphans(store, engine, &orphans);
        (count, bytes)
    }
}

// ---------------------------------------------------------------------------
// SpaceScheduler
// ---------------------------------------------------------------------------

/// Advisory maintenance scheduler over [`SpaceEngine`] debt.
///
/// The scheduler owns no worker and dispatches nothing: each call to
/// [`Self::next_candidate`] ranks the tracked tables by effective debt
/// (`dead_ratio` plus an aging bonus per missed round) filtered through the
/// debt threshold, and returns the top table the caller may compact within
/// its `max_concurrent` budget. The executor worker keeps its own loop;
/// [`Self::should_run_maintenance`] is the pollable form of the same answer
/// for one table.
#[derive(Clone, Debug)]
pub struct SpaceScheduler {
    max_dead_ratio: f64,
    waits: HashMap<String, u64>,
    max_tracked_waits: usize,
    in_flight: usize,
}

impl SpaceScheduler {
    /// Creates a scheduler: `max_dead_ratio` gates debt-driven compaction.
    #[must_use]
    pub fn new(max_dead_ratio: f64) -> Self {
        Self {
            max_dead_ratio,
            waits: HashMap::new(),
            max_tracked_waits: DEFAULT_MAX_TRACKED_WAITS,
            in_flight: 0,
        }
    }

    /// Debt threshold this scheduler applies.
    #[must_use]
    pub fn max_dead_ratio(&self) -> f64 {
        self.max_dead_ratio
    }

    /// Maintenance runs the caller has dispatched but not finished.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Sets the dispatched-but-unfinished count (tests and workers that track
    /// dispatch outside the scheduler).
    pub fn set_in_flight(&mut self, in_flight: usize) {
        self.in_flight = in_flight;
    }

    /// Records one dispatched run; pair with [`Self::finish`] on completion.
    pub fn begin(&mut self) {
        self.in_flight = self.in_flight.saturating_add(1);
    }

    /// Records one completed run.
    pub fn finish(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    /// Missed selection rounds currently banked for `table` (tests).
    #[must_use]
    pub fn wait_count(&self, table: &str) -> u64 {
        self.waits.get(table).copied().unwrap_or(0)
    }

    /// Effective debt: measured dead ratio plus the aging bonus.
    ///
    /// Capped at `2.0` so the ordering stays total and finite no matter how
    /// long a table waits.
    fn effective(dead_ratio: f64, waits: u64) -> f64 {
        (dead_ratio + waits as f64 * SCHEDULER_AGING_BONUS).min(2.0)
    }

    /// Prunes the wait map to tables the engine still tracks, bounding memory
    /// even under table churn.
    fn prune_waits(&mut self, engine: &SpaceEngine) {
        if self.waits.len() <= self.max_tracked_waits {
            self.waits.retain(|table, _| engine.contains(table));
            return;
        }
        let mut kept: Vec<(String, u64)> = self
            .waits
            .iter()
            .filter(|(table, _)| engine.contains(table))
            .map(|(table, waits)| (table.clone(), *waits))
            .collect();
        kept.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        kept.truncate(self.max_tracked_waits);
        self.waits = kept.into_iter().collect();
    }

    /// Ranks `debts` by effective debt and returns the top eligible table.
    ///
    /// Eligibility is `dead_ratio > max_dead_ratio` or starvation
    /// (`waits >= SCHEDULER_STARVATION_WAITS`).
    /// Every ranked-but-unpicked table ages by one round; the pick's waits
    /// reset to zero. Deterministic: effective debt descending, name ascending.
    fn select(&mut self, debts: &[SpaceAccount]) -> Option<String> {
        if debts.is_empty() {
            return None;
        }
        for debt in debts {
            let entry = self.waits.entry(debt.table.clone()).or_insert(0);
            *entry = entry.saturating_add(1);
        }
        let mut ranked: Vec<(&SpaceAccount, f64, bool)> = debts
            .iter()
            .map(|debt| {
                let waits = self.waits.get(&debt.table).copied().unwrap_or(0);
                let over_debt = debt.dead_ratio() > self.max_dead_ratio;
                let starved = waits >= SCHEDULER_STARVATION_WAITS;
                (
                    debt,
                    Self::effective(debt.dead_ratio(), waits),
                    over_debt || starved,
                )
            })
            .filter(|(_, _, eligible)| *eligible)
            .collect();
        ranked.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.table.cmp(&right.0.table))
        });
        ranked.first().map(|(debt, _, _)| {
            self.waits.insert(debt.table.clone(), 0);
            debt.table.clone()
        })
    }

    /// Returns the next table to compact within a `max_concurrent` budget, or
    /// `None` when the budget is full or no table is eligible.
    ///
    /// Advisory: the concurrency budget is enforced only against the
    /// [`Self::in_flight`] count the caller maintains via [`Self::begin`],
    /// [`Self::finish`], and [`Self::set_in_flight`].
    pub fn next_candidate(
        &mut self,
        engine: &SpaceEngine,
        max_concurrent: usize,
    ) -> Option<String> {
        if self.in_flight >= max_concurrent {
            return None;
        }
        self.prune_waits(engine);
        let debts = engine.tables_by_debt();
        self.select(&debts)
    }

    /// Advisory single-table check for the executor worker: true when
    /// `table`'s dead ratio exceeds the debt threshold or the table has
    /// waited past starvation.
    pub fn should_run_maintenance(&self, engine: &SpaceEngine, table: &str) -> bool {
        if engine.needs_compaction(table, self.max_dead_ratio) {
            return true;
        }
        self.waits.get(table).copied().unwrap_or(0) >= SCHEDULER_STARVATION_WAITS
    }
}
