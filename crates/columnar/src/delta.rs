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
//! MAIN + DELTA freshness layer for immutable columnar generations.
//!
//! The columnar store publishes *immutable* generations, and
//! `crates/executor/src/columnar_freshness.rs` tracks whether a published
//! generation is proven to cover every committed write. Without a delta
//! layer, any write after publication makes the generation stale and forces
//! readers back onto the row path until the next full flush.
//!
//! This module closes that gap without duplicating either subsystem:
//!
//! ```text
//! COMMITTED WRITE (executor DML)
//!       │  staged per row into the session catalog (values + pre-image),
//!       │  confirmed post-commit with the commit watermark as commit_seq
//!       ▼
//! DeltaStore::insert (this module, process-wide via global_delta_store):
//!       the row lands in the per-table delta, keyed by storage row id,
//!       tagged with its commit sequence number.
//!
//! ANALYTICAL SCAN at snapshot S
//!       │  read_current_generation_rows_with_deltas (compaction.rs)
//!       ├──▶ published generation rows (existing ColumnarStore)
//!       └──▶ DeltaStore::merge_bag: visible deltas (commit_seq <= S)
//!            overlay the main image in commit order.
//!
//! COMPACTION / VACUUM
//!       │  the new generation is rebuilt from the authoritative row store,
//!       │  which already contains every committed write, so absorbed
//!       └──▶ note_deltas_published clears the table's deltas.
//! ```
//!
//! # Why bag-apply instead of id-join
//!
//! Published columnar segments carry row *images* but no storage row ids
//! (the flush pipeline materializes values only), so the scan path cannot
//! pair main rows with delta row ids. [`DeltaStore::merge_bag`] therefore
//! resolves each delta to a working slot by id-chaining (a row touched
//! earlier in the window is found through its earlier delta) or, for rows
//! untouched in the window, by pre-image search (first working row equal to
//! the staged pre-image; bag semantics make first-match exact for
//! multisets). Final materialization still runs through the id-keyed
//! [`merge_rows`] kernel over the resolved slots, so there is exactly one
//! merge rule. Deltas that resolve to nothing (a delete for an unknown row,
//! a pre-image consumed by an unreachable interleaving) are ignored:
//! fail-closed, never fabricating rows.
//!
//! # What this module does NOT own
//!
//! * Freshness truth stays in the executor's freshness map.
//! * Segment storage, publication, retention, and reclamation stay in
//!   [`crate::store`], [`crate::compaction`], and the generation manager.
//! * MVCC visibility rules stay in `plomid_mvcc`; the only visibility
//!   predicate used here is the commit-sequence comparison
//!   `commit_seq <= snapshot_seq`, which mirrors "committed before the
//!   snapshot was taken".
//!
//! # Crash safety
//!
//! The delta store is a read-accelerator only: authoritative data stays in
//! the row store plus published generations. A process restart drops all
//! deltas (the store is memory-only and never persisted); readers then fall
//! back to the row path per the existing freshness semantics until the next
//! publication. Staged-but-unconfirmed rows are invisible by construction
//! (confirmation moves them into the visible history with a real commit
//! sequence), so a crash between staging and confirmation loses nothing
//! observable.

use plomid_core::ObjectId;
use plomid_storage::{Field, Row, StorageEngine};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// One delta write: the latest committed state of a single logical row.
///
/// `commit_seq` is the writer's commit sequence number (MVCC commit
/// timestamp). A delta is visible to a snapshot `S` exactly when
/// `commit_seq <= S`; anything committed after the snapshot was taken stays
/// invisible to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaRow {
    /// Storage row identity within the table (`{table}:{row_id}` suffix).
    pub row_id: u64,
    /// Row fields in ordinal order: the post-image for upserts, the
    /// pre-image for pre-imaged deletes, empty otherwise.
    pub values: Vec<Field>,
    /// True when this delta is a delete tombstone for `row_id`.
    pub deleted: bool,
    /// Commit sequence number that made this delta visible.
    pub commit_seq: u64,
    /// Pre-image in ordinal order, when the writer observed one.
    ///
    /// Updates and deletes stage the row they replaced, so the merge can
    /// locate the target by image when the row was untouched earlier in the
    /// window (no id-chain entry). `None` for inserts and for writers that
    /// did not observe a pre-image.
    pub prev: Option<Vec<Field>>,
}

impl DeltaRow {
    /// Creates an upsert delta for `row_id` at `commit_seq`.
    #[must_use]
    pub fn upsert(row_id: u64, values: Vec<Field>, commit_seq: u64) -> Self {
        Self {
            row_id,
            values,
            deleted: false,
            commit_seq,
            prev: None,
        }
    }

    /// Creates an upsert delta with a staged pre-image (updates).
    #[must_use]
    pub fn with_prev(mut self, prev: Vec<Field>) -> Self {
        self.prev = Some(prev);
        self
    }

    /// Creates a delete tombstone for `row_id` at `commit_seq`.
    ///
    /// Without a pre-image the tombstone only resolves rows already chained
    /// in the window; use [`Self::delete_row`] when the writer observed the
    /// deleted row.
    #[must_use]
    pub fn delete(row_id: u64, commit_seq: u64) -> Self {
        Self {
            row_id,
            values: Vec::new(),
            deleted: true,
            commit_seq,
            prev: None,
        }
    }

    /// Creates a delete tombstone carrying the deleted row's pre-image.
    #[must_use]
    pub fn delete_row(row_id: u64, prev: Vec<Field>, commit_seq: u64) -> Self {
        Self {
            row_id,
            values: prev.clone(),
            deleted: true,
            commit_seq,
            prev: Some(prev),
        }
    }

    /// Returns the delta payload as a [`Row`], or `None` for tombstones.
    #[must_use]
    pub fn row(&self) -> Option<Row> {
        if self.deleted {
            None
        } else {
            Some(Row::new(self.values.clone()))
        }
    }

    /// Returns the staged pre-image as a [`Row`], if one was observed.
    #[must_use]
    pub fn prev_row(&self) -> Option<Row> {
        self.prev.clone().map(Row::new)
    }

    /// Estimated encoded size of this delta, for compaction accounting.
    ///
    /// Measured from the real row encoding when possible; tombstones and
    /// unencodable rows fall back to a small constant so they still count.
    #[must_use]
    pub fn estimated_bytes(&self) -> u64 {
        if self.deleted && self.values.is_empty() {
            return 8;
        }
        Row::new(self.values.clone())
            .encode()
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(8)
            .saturating_add(
                self.prev
                    .as_ref()
                    .and_then(|prev| Row::new(prev.clone()).encode().ok())
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(0),
            )
    }
}

/// Per-table delta store: committed writes that landed after the last
/// published columnar generation.
///
/// Append-only history per table; resolution is latest-wins by `commit_seq`
/// per resolved slot, so out-of-order inserts can never resurrect a
/// superseded version under a snapshot that already observed the newer one.
#[derive(Debug, Default)]
pub struct DeltaStore {
    inner: Mutex<HashMap<String, Vec<DeltaRow>>>,
}

impl DeltaStore {
    /// Creates an empty delta store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one committed delta write for `table`.
    pub fn insert(&self, table: &str, row: DeltaRow) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.entry(table.to_string()).or_default().push(row);
        }
    }

    /// Returns `table`'s visible delta history under `snapshot_seq`,
    /// oldest-commit-first (insertion order breaks commit-seq ties).
    ///
    /// Visibility is `commit_seq <= snapshot_seq` (committed before the
    /// snapshot was taken). Unlike a latest-wins projection, the full
    /// ordered history is returned: the bag-apply merge resolves targets
    /// incrementally and needs every intermediate state, not just the final
    /// one per row id.
    pub fn scan_history(&self, table: &str, snapshot_seq: u64) -> Vec<DeltaRow> {
        let guard = match self.inner.lock() {
            Ok(guard) => guard,
            Err(_) => return Vec::new(),
        };
        let Some(history) = guard.get(table) else {
            return Vec::new();
        };
        let mut visible: Vec<(usize, &DeltaRow)> = history
            .iter()
            .enumerate()
            .filter(|(_, delta)| delta.commit_seq <= snapshot_seq)
            .collect();
        visible.sort_by_key(|(_, delta)| delta.commit_seq);
        visible
            .into_iter()
            .map(|(_, delta)| delta.clone())
            .collect()
    }

    /// Counts the distinct row ids with delta state for `table`.
    ///
    /// This is the effective override count (latest-wins per row id), which
    /// is what a compaction would have to merge, not the raw append total.
    #[must_use]
    pub fn row_count(&self, table: &str) -> usize {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| {
                guard.get(table).map(|history| {
                    let mut ids: Vec<u64> = history.iter().map(|delta| delta.row_id).collect();
                    ids.sort_unstable();
                    ids.dedup();
                    ids.len()
                })
            })
            .unwrap_or(0)
    }

    /// Estimates the total encoded bytes of `table`'s deltas.
    #[must_use]
    pub fn delta_bytes(&self, table: &str) -> u64 {
        self.inner
            .lock()
            .ok()
            .and_then(|guard| {
                guard
                    .get(table)
                    .map(|history| history.iter().map(DeltaRow::estimated_bytes).sum())
            })
            .unwrap_or(0)
    }

    /// Drops all delta state for `table` (after its deltas are published).
    pub fn clear(&self, table: &str) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.remove(table);
        }
    }

    /// Returns the MAIN + DELTA merged view of `table` at `snapshot_seq`.
    ///
    /// `main_rows` carries the published generation's row images in scan
    /// order. Visible deltas overlay them in commit order via
    /// [`apply_history`]: chained rows resolve by id, untouched rows by
    /// pre-image search, inserts append. Main order is preserved for
    /// untouched rows; appended rows follow in commit order.
    pub fn merge_bag(&self, table: &str, main_rows: &[Row], snapshot_seq: u64) -> Vec<Row> {
        apply_history(main_rows, &self.scan_history(table, snapshot_seq))
    }
}

/// Applies an ordered visible delta history to id-free main row images.
///
/// Each delta resolves to a working slot: rows already touched in the window
/// resolve through the id chain; otherwise the staged pre-image locates the
/// target (first working row equal to the pre-image — exact under bag
/// semantics); inserts with no pre-image append. Resolution is total:
/// unresolvable deletes are ignored and unresolvable upserts append, so the
/// merge never fails and never fabricates a row the history does not
/// describe. Final materialization runs through [`merge_rows`] over the
/// resolved slots, so the latest commit per slot wins.
pub fn apply_history(main_rows: &[Row], history: &[DeltaRow]) -> Vec<Row> {
    if history.is_empty() {
        return main_rows.to_vec();
    }
    // Resolve every delta to a stable surrogate slot. Slots
    // `0..main_rows.len()` are the main images in order; appended slots
    // extend the working set.
    let mut working: Vec<Option<Row>> = main_rows.iter().map(|row| Some(row.clone())).collect();
    let mut chained: HashMap<u64, usize> = HashMap::new();
    // Surrogate-keyed deltas for the merge kernel, in application order.
    let mut resolved: Vec<DeltaRow> = Vec::with_capacity(history.len());
    for delta in history {
        let slot = match chained.get(&delta.row_id).copied() {
            Some(slot) => Some(slot),
            None => match delta.prev_row() {
                Some(prev) => working.iter().position(|slot| slot.as_ref() == Some(&prev)),
                None => None,
            },
        };
        match (slot, delta.deleted) {
            (Some(slot), _) => {
                chained.insert(delta.row_id, slot);
                resolved.push(DeltaRow {
                    row_id: slot as u64,
                    values: delta.values.clone(),
                    deleted: delta.deleted,
                    commit_seq: delta.commit_seq,
                    prev: None,
                });
                // Keep the working image current so later pre-image
                // searches observe the latest in-window state.
                working[slot] = delta.row();
            }
            (None, true) => {
                // A tombstone for an untracked, imageless row names
                // nothing in the working set: ignore (fail-closed).
            }
            (None, false) => {
                let slot = working.len();
                working.push(delta.row());
                chained.insert(delta.row_id, slot);
                resolved.push(DeltaRow {
                    row_id: slot as u64,
                    values: delta.values.clone(),
                    deleted: false,
                    commit_seq: delta.commit_seq,
                    prev: None,
                });
            }
        }
    }
    // Apply latest-wins per resolved slot through the single kernel.
    let paired: Vec<(u64, Row)> = main_rows
        .iter()
        .enumerate()
        .map(|(index, row)| (index as u64, row.clone()))
        .collect();
    merge_rows(&paired, &resolved)
        .into_iter()
        .map(|(_, row)| row)
        .collect()
}

/// Applies MAIN + DELTA: `deltas` overlay `main_rows`.
///
/// Every delta whose `row_id` matches a main row replaces it; deltas for
/// unknown ids are inserts; tombstones remove the main row. The result is
/// sorted by `row_id` so scans stay deterministic. When `deltas` holds
/// several entries for one id, the highest `commit_seq` wins.
pub fn merge_rows(main_rows: &[(u64, Row)], deltas: &[DeltaRow]) -> Vec<(u64, Row)> {
    let mut merged: HashMap<u64, Option<Row>> =
        HashMap::with_capacity(main_rows.len().saturating_add(deltas.len()));
    for (row_id, row) in main_rows {
        merged.insert(*row_id, Some(row.clone()));
    }
    let mut ordered: Vec<&DeltaRow> = deltas.iter().collect();
    ordered.sort_by_key(|delta| delta.commit_seq);
    for delta in ordered {
        merged.insert(delta.row_id, delta.row());
    }
    let mut out: Vec<(u64, Row)> = merged
        .into_iter()
        .filter_map(|(row_id, row)| row.map(|row| (row_id, row)))
        .collect();
    out.sort_by_key(|(row_id, _)| *row_id);
    out
}

/// Decides when a table's deltas are worth folding into a new generation.
///
/// Thresholds are advisory: crossing either one means the MAIN + DELTA
/// overlay has grown expensive enough that compacting pays off. The actual
/// merge still runs through the existing [`crate::compaction::compact`],
/// whose inputs are selected by [`crate::compaction::select_inputs`]; the
/// advisor contributes only the threshold trip, never plan assembly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionAdvisor {
    /// Delta row count that triggers compaction.
    pub delta_row_threshold: usize,
    /// Delta byte size that triggers compaction.
    pub delta_bytes_threshold: u64,
}

/// Row count that trips delta-driven maintenance by default.
///
/// A thousand outstanding row deltas is where the per-scan overlay starts
/// to cost more than a share of one compaction pass for ordinary OLTP row
/// widths; tables that disagree construct their own advisor.
pub const DEFAULT_DELTA_ROW_THRESHOLD: usize = 1_000;
/// Delta byte size that trips delta-driven maintenance by default.
pub const DEFAULT_DELTA_BYTES_THRESHOLD: u64 = 256 * 1024;

impl CompactionAdvisor {
    /// Creates an advisor with explicit row and byte thresholds.
    #[must_use]
    pub fn new(delta_row_threshold: usize, delta_bytes_threshold: u64) -> Self {
        Self {
            delta_row_threshold,
            delta_bytes_threshold,
        }
    }

    /// Returns true when either threshold is reached.
    #[must_use]
    pub fn should_compact(&self, delta_rows: u64, delta_bytes: u64) -> bool {
        delta_rows >= self.delta_row_threshold as u64 || delta_bytes >= self.delta_bytes_threshold
    }
}

impl Default for CompactionAdvisor {
    fn default() -> Self {
        Self::new(DEFAULT_DELTA_ROW_THRESHOLD, DEFAULT_DELTA_BYTES_THRESHOLD)
    }
}

/// Process-wide delta store shared by the insert, read, publish, and
/// scheduler feeds.
///
/// The store is memory-only and never persisted: a restart drops every
/// delta and readers fall back to the row path until the next publication
/// (fail-closed, matching the freshness-map boot semantics). Lock poisoning
/// fails open per method (empty reads, skipped writes), never panicking a
/// write or a scan.
pub fn global_delta_store() -> &'static DeltaStore {
    static DELTAS: OnceLock<DeltaStore> = OnceLock::new();
    DELTAS.get_or_init(DeltaStore::new)
}

/// Delta-store key for one table: the storage root plus the columnar object
/// identity.
///
/// The root scopes the key per database directory (unit tests open many
/// temp roots in one process; servers may serve more than one). The object
/// identity is the same [`ObjectId`] the generation catalog, the freshness
/// gate, and the columnar scan path already use for the table
/// (`ObjectId::new(table_id)`), so all three feeds name a table identically
/// without sharing any other state. Table ids are catalog-global and never
/// reused, so a dropped table's key can never collide with a later table.
#[must_use]
pub fn delta_key(root: &std::path::Path, object_id: ObjectId) -> String {
    format!("{}/object{}", root.display(), object_id.get())
}

/// Reads the current commit watermark: the highest committed sequence the
/// engine reports.
///
/// Note on keys: the space ledger is keyed by table identity
/// ([`crate::store::ColumnarStore::space_table_key`]) while deltas are
/// keyed by [`delta_key`]; feed code derives each from its own side
/// (identity vs. object id) so no string round-trip ever couples them.
///
/// Deltas are stamped with a post-commit watermark read, which is always at
/// or past the true commit sequence (over-stamping only delays delta
/// visibility; the dirty-gate keeps the row path authoritative meanwhile).
/// An unreadable watermark answers zero (fail-closed: deltas stamped zero
/// stay visible only to snapshots that also observe the pre-write state,
/// and tables with staged writes are dirty-gated to the row path anyway).
pub fn commit_watermark<E: StorageEngine>(engine: &E) -> u64 {
    engine.mvcc_safety().map(|(_, last)| last).unwrap_or(0)
}

/// Clears `table`'s deltas after a publication absorbed them.
///
/// Called on every successful vacuum/compaction publication: the new
/// generation was rebuilt from the authoritative row store, which already
/// contains every committed write, so the absorbed deltas are redundant.
/// Clearing unconditionally (rather than through a watermark) is exact:
/// a concurrent commit inside the build window defeats the freshness mark
/// and keeps the table on the row path, where the next pass re-covers its
/// rows; aborted ghosts, which never reach the row store, are dropped here
/// instead of lingering.
pub fn note_deltas_published(table: &str) {
    global_delta_store().clear(table);
}
