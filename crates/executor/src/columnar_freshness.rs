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
//! Shared columnar freshness tracking.
//!
//! A published columnar generation covers committed rows up to the snapshot
//! its build observed. Any later committed write to the table makes that
//! generation stale for new statements, until the next successful
//! publication. This module tracks that property process-wide so every
//! session — including connections that never ran `VACUUM` themselves —
//! agrees on whether a table's published generation is safe to read.
//!
//! # HTAP multi-modal contract (standing rule)
//!
//! PLOMID serves three modalities with exact semantics on every path:
//!
//! * **Row path (Hot Row Store, authoritative):** all OLTP writes land here,
//!   and every read modality can always fall back to it. The document/JSON
//!   modality (`plomid-json`, `JSON_TABLE` in `join/support.rs`) evaluates
//!   over row values produced upstream of storage-path selection, so it keeps
//!   its existing paths regardless of columnar admission below.
//! * **Columnar path, fresh:** a generation proven (via [`try_mark_published`]
//!   with an empty build window) to cover every committed write. Serves both
//!   the scalar scan and the vector aggregate fast path.
//! * **Columnar path, delta-covered (stale):** a published generation plus the
//!   table's confirmed post-publication deltas merged under the reader's
//!   snapshot (`read_current_generation_rows_with_deltas`). Serves the scalar
//!   scan only — never the vector path, which aggregates published segments
//!   without a delta overlay. Admission requires [`is_generation_delta_covered`].
//!
//! Columnar admission changes must never alter results for any modality:
//! deltas merge under snapshot visibility (`commit_seq <= snapshot_seq`), and
//! JSON/document queries keep their existing row-production paths.
//!
//! # Delta-coverage rule (the per-table flag)
//!
//! `TableFreshness::delta_covered` records that **every write since the last
//! absorbed publication was delta-staged**, so the MAIN + DELTA merge is
//! complete. It is set only inside the successful [`try_mark_published`]
//! critical section (atomically with the delta absorb it certifies), and
//! cleared whenever coverage can no longer be proven:
//!
//! * process boot (the map starts empty — fail-closed, so pre-existing
//!   staleness from before the layer existed is never mistaken for covered);
//! * a failed [`try_mark_published`] (a commit landed inside the build
//!   window — the new generation may miss it while its delta may already be
//!   absorbed, so the merge is no longer provably complete);
//! * [`clear_delta_coverage`] (whole-table `TRUNCATE` bulk-clears confirmed
//!   deltas on commit, which the per-row merge cannot reconstruct);
//! * [`forget_table`] (drops, renames, and schema rewrites — anything that
//!   bypasses DML staging drops the whole entry).
//!
//! Ordinary DML dirty-marks ([`mark_table_dirty`]) deliberately preserve the
//! flag: every DML write path stages its rows pre-commit (see the staging
//! section below), so a dirty mark from DML never breaks coverage.
//!
//! # Design
//!
//! The map holds one entry per `(storage root, database, resolved table)`:
//!
//! ```text
//! writes:        commits observed since the entry was created
//! clean:         the generation proven to cover every committed write, if any
//! delta_covered: every post-absorb write was delta-staged (merge is complete)
//! ```
//!
//! * Any statement that may write table rows bumps `writes` and clears
//!   `clean` **before** it executes, so concurrent readers can never observe
//!   a generation that misses a committed write (the mark precedes
//!   visibility; rollbacks stay dirty conservatively until the next vacuum).
//! * A successful publication records `clean` only when `writes` still equals
//!   the value read before the build scan started: a commit landing anywhere
//!   inside the build window is observed and the table stays on the row path.
//!   The same critical section absorbs the table's confirmed deltas and sets
//!   `delta_covered`, so the clear and the mark cannot interleave with a
//!   concurrent commit's confirm (see `try_mark_published`).
//! * The map starts empty at process boot, which fails closed: no entry means
//!   "not proven fresh", so readers use Hot Row Store state until the first
//!   post-boot publication. No recovery-time reconstruction is needed.
//! * Table drops remove the entry, so a recreated table with reused storage
//!   identities can never inherit another table's freshness.
//!
//! All mutations hold one process-wide mutex, and the check-and-set in
//! [`try_mark_published`] is atomic under it, so the interleavings above hold
//! across concurrent sessions. Critical sections only hash short strings
//! (plus one bounded delta-map removal on the absorb path).

use plomid_core::GenerationId;
use plomid_sql::{InMemoryCatalog, StagedDelta, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Freshness state of one table's published generation.
#[derive(Clone, Copy, Debug)]
struct TableFreshness {
    /// Committed write statements observed since process boot (or entry
    /// creation). Monotonic per table; only compared for equality.
    writes: u64,
    /// Generation proven to cover every committed write, if any.
    clean: Option<GenerationId>,
    /// Every write since the last absorbed publication was delta-staged, so
    /// the MAIN + DELTA merge is complete (see the module-level
    /// delta-coverage rule). Set only on successful publication, preserved
    /// across DML dirty-marks, cleared on failed marks, truncates, and drops.
    delta_covered: bool,
}

/// Process-wide freshness map, keyed by `storage-root/database/table`.
fn map() -> &'static Mutex<HashMap<String, TableFreshness>> {
    static FRESHNESS: OnceLock<Mutex<HashMap<String, TableFreshness>>> = OnceLock::new();
    FRESHNESS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Storage key shared by writers, publishers, and readers.
///
/// Uses the same `root/database/table` spelling as the maintenance claim
/// keys so one session's writes invalidate exactly the entries every other
/// session reads.
#[must_use]
pub fn freshness_key(root: &std::path::Path, database: &str, table: &str) -> String {
    format!("{}/{}/{}", root.display(), database, table)
}

/// Marks `table` dirty before a statement that may write its rows.
///
/// Unconditional: even statements that later fail or roll back leave the
/// entry dirty, which only costs a row-path fallback until the next vacuum.
/// Deliberately preserves `delta_covered`: every DML write path stages its
/// rows for post-commit delta confirmation (see the staging section below),
/// so DML dirtiness never breaks merge completeness. Whole-table operations
/// that bulk-clear deltas (`TRUNCATE`) clear coverage separately via
/// [`clear_delta_coverage`]; schema changes that bypass staging drop the
/// entry via [`forget_table`].
pub fn mark_table_dirty(key: &str) {
    if let Ok(mut guard) = map().lock() {
        let entry = guard.entry(key.to_string()).or_insert(TableFreshness {
            writes: 0,
            clean: None,
            delta_covered: false,
        });
        entry.writes = entry.writes.saturating_add(1);
        entry.clean = None;
    }
}

/// Clears `key`'s delta coverage after a whole-table operation whose confirmed
/// deltas the per-row merge cannot reconstruct.
///
/// Called for `TRUNCATE` at the same pre-execution point as
/// [`mark_table_dirty`]: the truncate marker bulk-clears the table's deltas on
/// confirm, so from this mark forward the merge is no longer provably
/// complete. The next successful publication re-establishes coverage. A
/// missing entry stays missing (fail-closed).
pub fn clear_delta_coverage(key: &str) {
    if let Ok(mut guard) = map().lock() {
        if let Some(entry) = guard.get_mut(key) {
            entry.delta_covered = false;
        }
    }
}

/// Returns whether `key`'s post-publication writes are all delta-staged.
///
/// False for absent entries (never published since boot, dropped, or
/// rewritten outside DML staging): absence always fails closed to the row
/// path. Read by the columnar gate ([`is_generation_delta_covered`]) and by
/// targeted tests asserting the coverage lifecycle.
pub fn delta_covered(key: &str) -> bool {
    map()
        .lock()
        .ok()
        .and_then(|guard| guard.get(key).map(|entry| entry.delta_covered))
        .unwrap_or(false)
}

/// Reads the current write count for `key` (0 when absent).
pub fn writes_since_boot(key: &str) -> u64 {
    map()
        .lock()
        .ok()
        .and_then(|guard| guard.get(key).map(|entry| entry.writes))
        .unwrap_or(0)
}

/// Records a freshly published `generation` as clean, but only when no write
/// landed since `writes_before` was read (i.e. the build window observed no
/// concurrent commit). Returns whether the table is now marked clean.
///
/// On success this also absorbs the table's confirmed deltas and certifies
/// delta coverage, atomically under the freshness lock: the new generation
/// was rebuilt from the authoritative row store (which already contains every
/// committed write), so the absorbed deltas are redundant and every future
/// write stages through DML. Atomicity matters — a concurrent commit's dirty
/// mark either lands before this section (the write count differs, the mark
/// fails, coverage is cleared, and the deltas survive for the next pass) or
/// after it (the mark succeeds, coverage holds, and the later confirm lands
/// strictly after the absorb, so the merge observes it). Clearing outside
/// this section could drop a delta the new generation misses.
///
/// `delta_table` names the table in the columnar delta store
/// ([`plomid_columnar::delta::delta_key`]); `None` skips the delta clear
/// while still recording freshness (unit tests, tables never delta-staged).
/// A failed mark clears coverage: a commit landed inside the build window, so
/// the merge is no longer provably complete until the next successful pass.
pub fn try_mark_published(
    key: &str,
    generation: GenerationId,
    writes_before: u64,
    delta_table: Option<&str>,
) -> bool {
    let Ok(mut guard) = map().lock() else {
        return false;
    };
    // Absorbs confirmed deltas as part of a successful mark. Runs under the
    // freshness lock so no concurrent confirm can slip between the clear and
    // the mark (lock order here is freshness-map → delta-store; no path takes
    // them in the opposite order, so this cannot deadlock).
    let absorb = |table: Option<&str>| {
        if let Some(table) = table {
            plomid_columnar::delta::global_delta_store().clear(table);
        }
        clear_delta_lag(key);
    };
    if guard.get(key).is_none() {
        // No commits ever observed through this path (bootstrap/recovered
        // rows only): the just-published generation covers everything the
        // build scan saw, and no concurrent commit could have interleaved
        // without creating an entry. A nonzero `writes_before` with no entry
        // is incoherent (the counter reads zero when absent): fail closed.
        if writes_before != 0 {
            return false;
        }
        guard.insert(
            key.to_string(),
            TableFreshness {
                writes: 0,
                clean: Some(generation),
                delta_covered: true,
            },
        );
        // A fresh publication folds every outstanding delta into the new
        // generation, so the analytics lag resets with it.
        absorb(delta_table);
        return true;
    }
    let Some(entry) = guard.get(key) else {
        return false;
    };
    if entry.writes != writes_before {
        // A commit landed inside the build window: stay on the row path and
        // drop coverage (the generation may miss the write while its delta
        // may already be absorbed). Deltas are deliberately NOT cleared here:
        // they survive for the next pass to absorb.
        if let Some(entry) = guard.get_mut(key) {
            entry.delta_covered = false;
        }
        return false;
    }
    if let Some(entry) = guard.get_mut(key) {
        entry.clean = Some(generation);
        entry.delta_covered = true;
    }
    // Same reset as above: the published generation absorbed the deltas.
    absorb(delta_table);
    true
}

/// Returns the clean generation for `key`, if one is currently proven fresh.
pub fn clean_generation(key: &str) -> Option<GenerationId> {
    map()
        .lock()
        .ok()
        .and_then(|guard| guard.get(key).and_then(|entry| entry.clean))
}

/// Returns true when `table`'s published generation is proven to cover every
/// committed write visible to new statements.
///
/// Both halves are required: the shared map must hold a clean generation for
/// `(root, database, table)`, and that generation must still be the
/// currently published one. Any failure — missing entry, unreadable
/// publication state, unpublished object, superseded generation — answers
/// false and the caller uses Hot Row Store state. Fresh connections therefore
/// discover columnar eligibility from authoritative database state without
/// ever having run `VACUUM` themselves.
pub fn is_generation_fresh(
    store: &plomid_columnar::ColumnarStore,
    root: &std::path::Path,
    database: &str,
    resolved_table: &str,
    table_id: u64,
) -> bool {
    let key = freshness_key(root, database, resolved_table);
    let Some(clean) = clean_generation(&key) else {
        return false;
    };
    let Ok(snapshot) = store.generations().reader() else {
        return false;
    };
    let object = plomid_core::ObjectId::new(table_id);
    let Some(record) = snapshot.object(object) else {
        return false;
    };
    record.is_published() && record.current_generation == clean
}

/// Returns true when `table`'s stale published generation is delta-covered:
/// a generation is published for the table and every post-publication write
/// was delta-staged, so the MAIN + DELTA merge reproduces row-store state
/// under snapshot visibility.
///
/// This is the relaxed HTAP admission: stale-but-covered tables read through
/// the scalar columnar scan (which always merges confirmed deltas), never
/// through the vector fast path (which aggregates published segments without
/// a delta overlay). Freshness ([`is_generation_fresh`]) implies this when
/// the merge set is empty, but callers check freshness first and use this
/// only for the stale branch, so the two admission reasons stay distinct.
/// Absence, unpublished objects, and unreadable publication state all answer
/// false (fail-closed to the Hot Row Store).
pub fn is_generation_delta_covered(
    store: &plomid_columnar::ColumnarStore,
    root: &std::path::Path,
    database: &str,
    resolved_table: &str,
    table_id: u64,
) -> bool {
    let key = freshness_key(root, database, resolved_table);
    if !delta_covered(&key) {
        return false;
    }
    let Ok(snapshot) = store.generations().reader() else {
        return false;
    };
    let object = plomid_core::ObjectId::new(table_id);
    snapshot
        .object(object)
        .is_some_and(|record| record.is_published())
}

/// Removes all freshness state for `key` (table/index drops and renames).
///
/// Dropping the whole entry also drops delta coverage: a recreated table, a
/// renamed-away name, or a schema-rewritten table restarts uncovered
/// (fail-closed) until its next successful publication.
pub fn forget_table(key: &str) {
    if let Ok(mut guard) = map().lock() {
        guard.remove(key);
    }
}

/// One table's freshness state for observability.
///
/// `is_stale` is true when the entry has no proven-clean generation: either a
/// write landed after the last publication, or no publication has proven the
/// table fresh since process boot. Tables never written have no entry and
/// therefore no row — absence means "nothing outstanding", not "stale".
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FreshnessEntry {
    /// Storage key (`storage-root/database/table`, see [`freshness_key`]).
    pub key: String,
    /// Committed write statements observed since process boot.
    pub writes: u64,
    /// True when no generation is currently proven to cover every write.
    pub is_stale: bool,
}

/// Copies the whole freshness map for the `plomid_stat_tables()` surface.
///
/// Smallest additive read API: a sorted snapshot; callers render it without
/// holding the map lock.
pub fn freshness_snapshot() -> Vec<FreshnessEntry> {
    let mut entries: Vec<FreshnessEntry> = map()
        .lock()
        .ok()
        .map(|guard| {
            guard
                .iter()
                .map(|(key, state)| FreshnessEntry {
                    key: key.clone(),
                    writes: state.writes,
                    is_stale: state.clean.is_none(),
                })
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    entries
}

/// Counts tables whose published generation is not proven fresh (dirty/stale).
///
/// This is the `tables_stale` health signal: entries with a write since the
/// last proven publication, or never published since boot.
pub fn stale_table_count() -> u64 {
    map()
        .lock()
        .ok()
        .map(|guard| guard.values().filter(|state| state.clean.is_none()).count() as u64)
        .unwrap_or(0)
}
// ---------------------------------------------------------------------------
// Delta-write lag.
//
// The columnar delta store (`plomid_columnar::delta`) absorbs committed
// writes that land after the last published generation. Those writes are
// visible through MAIN + DELTA merges immediately, but the *published*
// generation still lags them until the next compaction publishes. This
// section observes that lag with a parallel per-table counter so analytics
// surfaces can report it. It reuses the same `storage-root/database/table`
// keys as the freshness map above but keeps separate state: delta lag
// answers "how many post-publication writes are waiting to be compacted",
// while the freshness map answers "is the published generation safe to
// read". Neither duplicates the other.
// ---------------------------------------------------------------------------

/// Pending delta-write counts per table key (`storage-root/database/table`).
///
/// Incremented by [`note_delta_write`]; cleared when the table's deltas are
/// published (inside [`try_mark_published`]'s success paths above via
/// [`clear_delta_lag`]).
fn delta_map() -> &'static Mutex<HashMap<String, u64>> {
    use std::sync::OnceLock;
    static DELTAS: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
    DELTAS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Records one committed delta write for `key`.
///
/// Compatible with [`mark_table_dirty`]: delta writes are the analytics-side
/// half of a post-publication commit (the row is already in the delta store
/// and merge-visible), while `mark_table_dirty` stays the sole authority for
/// generation staleness. Calling one never implies the other; writers that do
/// both keep both signals accurate.
pub fn note_delta_write(key: &str) {
    if let Ok(mut guard) = delta_map().lock() {
        let entry = guard.entry(key.to_string()).or_insert(0);
        *entry = entry.saturating_add(1);
    }
}

/// Returns the pending delta-write lag for `key` (0 when absent).
///
/// This is the analytics visibility lag: the number of delta writes absorbed
/// since the table's deltas were last published by compaction. Zero means the
/// published generation has nothing outstanding to fold in.
pub fn delta_lag_writes(key: &str) -> u64 {
    delta_map()
        .lock()
        .ok()
        .and_then(|guard| guard.get(key).copied())
        .unwrap_or(0)
}

/// Clears the pending delta lag for `key` (deltas published by compaction),
/// ignoring lock poisoning.
fn clear_delta_lag(key: &str) {
    if let Ok(mut guard) = delta_map().lock() {
        guard.remove(key);
    }
}

// ---------------------------------------------------------------------------
// Delta staging (columnar MAIN + DELTA insert feed).
//
// DML row writers stage one entry per written row key while the write
// transaction runs; the executor confirms staged entries into the columnar
// delta store after the commit succeeds, or discards them on failure. All
// helpers are infallible and callable through shared references, so staging
// can never fail a write: a skipped stage only costs delta acceleration
// (the dirty-gated row path stays authoritative).
// ---------------------------------------------------------------------------

/// Parses the storage row id out of a `{table}:{row_id}` row key.
///
/// Returns `None` for keys that do not carry the table prefix (meta keys,
/// index entries) or whose suffix is not a row id; staging sites skip those
/// keys rather than failing the write.
pub fn parse_row_id(table: &str, key: &[u8]) -> Option<i64> {
    let suffix = key.strip_prefix(format!("{table}:").as_bytes())?;
    std::str::from_utf8(suffix).ok()?.parse::<i64>().ok()
}

/// Stages one row write for post-commit delta confirmation.
///
/// `values` is the post-image in schema column order (ignored for deletes);
/// `prev` is the staged pre-image when the writer observed one (updates and
/// deletes). Non-positive row ids are skipped defensively: allocated row ids
/// are positive, and a skipped stage only costs acceleration.
pub fn stage_delta_row(
    catalog: &InMemoryCatalog,
    table: &str,
    row_id: i64,
    values: &[Value],
    prev: Option<&[Value]>,
    deleted: bool,
) {
    if row_id <= 0 {
        return;
    }
    let delta = match (deleted, prev) {
        (false, _) => StagedDelta::upsert(table, row_id, values, prev),
        (true, Some(prev)) => StagedDelta::delete(table, row_id, prev),
        // An imageless tombstone resolves only through the in-window id
        // chain (or is ignored); staging it is still exact.
        (true, None) => StagedDelta {
            table: table.to_string(),
            row_id,
            values: Vec::new(),
            prev: None,
            deleted: true,
            truncate: false,
        },
    };
    catalog.stage_delta(delta);
}

/// Stages a whole-table clear marker (`TRUNCATE`).
///
/// Confirmation clears the table's deltas instead of inserting rows: after
/// truncation no pre-truncation delta may survive to resurrect rows.
pub fn stage_truncate(catalog: &InMemoryCatalog, table: &str) {
    catalog.stage_delta(StagedDelta::truncate(table));
}
