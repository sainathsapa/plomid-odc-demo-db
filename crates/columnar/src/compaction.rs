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
//! MVCC-aware SQL compaction for immutable columnar generations.
//!
//! Compaction is an *orchestration layer*. It owns no storage, WAL, MVCC,
//! generation, checkpoint, recovery, statistics, filter, compression, or
//! checksum implementation of its own; it coordinates the ones that already
//! exist:
//!
//! ```text
//! SELECT COMPACTION INPUT   (existing catalog: same object + TableIdentity)
//!        │
//!        ▼
//! CONSISTENT MVCC SNAPSHOT  (existing TransactionManager + VersionStore)
//!        │
//!        ▼
//! MERGE VISIBLE ROW STATE   (existing VersionStore::scan_visible rules)
//!        │
//!        ▼
//! BUILD COLUMNAR SEGMENT    (existing ColumnarStore::flush_rows_with_id)
//!        │  materialize → statistics → zone maps/BRIN → compression
//!        ▼
//! VERIFY                    (existing SegmentReader::decode, CRC32C)
//!        │
//!        ▼
//! PUBLISH ATOMICALLY        (existing GenerationManager BUILD→…→PUBLISH)
//!        │
//!        ▼
//! RETAIN → RELEASE → GC     (existing retention + reachability proof)
//! ```
//!
//! # What compaction means here
//!
//! A table's durable state is a set of immutable generations: exactly one
//! *current* generation plus the *retained* generations the catalog still lists
//! for the object. Compaction replaces that set with one new generation whose
//! content is exactly the row state visible to a fresh committed snapshot.
//!
//! The update-chain merge is performed by the existing MVCC engine, not by a
//! new visibility engine: `VersionStore` already keeps one version chain per
//! storage key and answers "which version is visible to this snapshot?" through
//! [`plomid_mvcc::is_version_visible`]. Compaction asks that question once, for
//! every key, at one snapshot.
//!
//! # Active-transaction safety
//!
//! Omitting a superseded version from the compacted output is only *physically*
//! safe when no snapshot that could still require it exists. The rule used is
//! the existing GC horizon:
//!
//! ```text
//! horizon = gc_horizon(active transaction watermarks, last committed)
//! ```
//!
//! Before the horizon reaches a version's commit boundary, the generation that
//! carries it stays retained, and a retained generation is never removed from
//! the catalog by compaction. Releasing retention is a *separate* publication
//! ([`CompactionOutcome::released`]) that happens only when the horizon proves
//! no active reader can require the input generations. "Not visible to the
//! current transaction" is never used as proof.
//!
//! # Delete correctness
//!
//! A committed delete is a tombstone (`VersionState::Deleted`) in the version
//! store. `scan_visible` reports it as absent, which is exactly the logical row
//! state a fresh snapshot observes, so a compacted generation contains no
//! tombstone for a row no snapshot can still see. While an older snapshot may
//! still see the pre-delete version, that version's generation stays retained,
//! so the delete stays observable to that snapshot and the row stays deleted for
//! every newer one.
//!
//! # Crash safety
//!
//! Compaction never introduces a new durability boundary. Segment bytes ride the
//! existing engine transaction (WAL + storage sync), and the generation is
//! published by the existing `BUILD → FLUSH → VERIFY → SYNC → PUBLISH`
//! sequence. [`CompactionFailPoint`] reuses the existing columnar, publication,
//! and reclamation fail points, so each boundary is exercised against real
//! filesystem state instead of a second recovery mechanism.

use crate::column::ColumnType;
use crate::flush::FlushConfig;
use crate::read::SegmentReader;
use crate::store::{ColumnarFailPoint, ColumnarPublish, ColumnarStore, PublishedColumnarSegment};
use plomid_core::{ErrorKind, GenerationId, Lsn, ObjectId, PlomidError, Result, TableIdentity};
use plomid_htap::CompactionGate;
use plomid_storage::{
    GcFailPoint, GcOutcome, GenerationManager, PublicationFailPoint, Row, StorageEngine,
};
use std::sync::Arc;

/// Failure-injection boundary of one compaction run.
///
/// Every variant maps onto a fail point the existing subsystems already expose,
/// so no separate crash or recovery mechanism exists for compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionFailPoint {
    /// No failure; the production path.
    None,
    /// Fail while the compaction input is being verified.
    DuringInputRead,
    /// Fail after the segment image is built, before it is staged.
    AfterBuild,
    /// Fail after the slices are staged, before verification.
    AfterFlush,
    /// Fail while the staged image is verified.
    DuringVerify,
    /// Fail after verification, before synchronization.
    BeforeSync,
    /// Fail after synchronization, before the staging transaction commits.
    AfterSync,
    /// Fail before the generation is published.
    DuringPublish,
    /// Fail after the publication pointer is replaced.
    ///
    /// The generation *is* published in this case; the caller only failed to
    /// observe the success.
    AfterPublish,
    /// Fail while retention is being released.
    DuringRelease,
    /// Fail after reachability is proven but before any file is reclaimed.
    BeforeReclaim,
}

impl CompactionFailPoint {
    /// Columnar-stage fail point this boundary maps onto, if any.
    #[must_use]
    fn columnar(self) -> ColumnarFailPoint {
        match self {
            Self::AfterBuild => ColumnarFailPoint::AfterBuild,
            Self::AfterFlush => ColumnarFailPoint::AfterFlush,
            Self::DuringVerify => ColumnarFailPoint::DuringVerify,
            Self::BeforeSync => ColumnarFailPoint::BeforeSync,
            Self::AfterSync => ColumnarFailPoint::AfterSync,
            Self::DuringPublish => ColumnarFailPoint::DuringPublish,
            Self::AfterPublish => ColumnarFailPoint::AfterPublish,
            _ => ColumnarFailPoint::None,
        }
    }

    /// Publication-stage fail point this boundary maps onto, if any.
    #[must_use]
    fn publication(self) -> PublicationFailPoint {
        match self {
            Self::DuringPublish => PublicationFailPoint::DuringPublish,
            Self::AfterPublish => PublicationFailPoint::AfterPublish,
            Self::AfterSync => PublicationFailPoint::AfterSync,
            Self::AfterFlush | Self::DuringVerify | Self::BeforeSync => {
                PublicationFailPoint::AfterBuild
            }
            _ => PublicationFailPoint::None,
        }
    }

    /// True when the failure precedes publication of the new generation.
    ///
    /// Before publication the previously published generations stay valid and no
    /// partially built generation becomes visible. After it, the new generation
    /// is authoritative and recovery resolves it deterministically.
    #[must_use]
    pub fn is_pre_publication(self) -> bool {
        !matches!(
            self,
            Self::AfterPublish | Self::DuringRelease | Self::BeforeReclaim
        )
    }

    /// Reclamation-stage fail point this boundary maps onto, if any.
    #[must_use]
    fn reclamation(self) -> GcFailPoint {
        match self {
            Self::BeforeReclaim => GcFailPoint::BeforeReclaim,
            _ => GcFailPoint::None,
        }
    }
}

/// One generation selected as compaction input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionInput {
    /// Generation identity.
    pub generation: GenerationId,
    /// True when this generation is the object's current generation.
    pub current: bool,
}

/// The explicit set of generations one compaction run consumes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPlan {
    /// Owning `Database → Schema → Table` location of every input.
    pub identity: TableIdentity,
    /// Catalog object the inputs belong to.
    pub object_id: ObjectId,
    /// Input generations, ascending by identity.
    pub inputs: Vec<CompactionInput>,
    /// Commit-timestamp horizon proven safe at plan time.
    pub horizon: u64,
    /// True when the horizon proves no active snapshot predates the inputs.
    pub inputs_releasable: bool,
}

impl CompactionPlan {
    /// Returns the input generation identities, ascending.
    #[must_use]
    pub fn generation_ids(&self) -> Vec<GenerationId> {
        self.inputs.iter().map(|input| input.generation).collect()
    }

    /// True when there is nothing worth rewriting.
    ///
    /// One input generation with no superseded predecessors is already the
    /// minimal representation, so compaction is a no-op rather than a
    /// generation that would contain exactly the same rows.
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.inputs.len() <= 1
    }
}

/// Reports the MVCC safety state of the engine: every currently-active
/// transaction identity and the highest committed timestamp.
///
/// Compaction uses this to decide whether input generations may *physically* be
/// released. A transaction that is still active may hold a snapshot older than
/// the compaction, so while any transaction is active the inputs stay retained.
/// No sleep, delay, or "not visible to the current transaction" test is used.
pub fn mvcc_safety<E: StorageEngine>(engine: &E) -> Result<(Vec<u64>, u64)> {
    engine.mvcc_safety()
}

/// Returns true when no active transaction can still require a generation
/// superseded by the newest publication.
///
/// Call order matters: read the seal FIRST via
/// [`StorageEngine::txn_issue_mark`], then the active set, both strictly
/// after the new generation published. Every transaction in the set began
/// before the active read; those with identities at or above the seal began
/// after the seal read, hence after publication, so their statements can only
/// observe the newest published state. Transactions below the seal began
/// earlier and may hold an older snapshot, so their generations stay
/// retained. An empty active set releases vacuously. Reversing the read order
/// makes the rule vacuous (every listed identity predates a later-read seal),
/// so callers must not reorder the two reads. Monotonic identities only;
/// never wall-clock time.
#[must_use]
pub fn release_provable(active_transactions: &[u64], seal: u64) -> bool {
    active_transactions.iter().all(|id| *id >= seal)
}

/// Selects the compaction input for one table, deterministically.
///
/// The selection is the object's *complete* published generation set: its
/// current generation plus every generation the catalog still retains for it.
/// Selection rejects anything that is not the same object and the same
/// `Database → Schema → Table` identity, so compaction can never read or
/// rewrite another table's state, and it verifies each selected generation is
/// readable through the existing generation APIs before it is used.
pub fn select_inputs(
    manager: &Arc<GenerationManager>,
    identity: TableIdentity,
    object_id: ObjectId,
    active_transactions: &[u64],
    last_committed: u64,
) -> Result<CompactionPlan> {
    if identity.is_zero() {
        return Err(PlomidError::new(
            ErrorKind::InvalidArgument,
            "compaction identity is zero",
        ));
    }
    let reader = manager.reader()?;
    let record = reader.object(object_id).ok_or_else(|| {
        PlomidError::with_detail(
            ErrorKind::NotFound,
            "compaction target is not catalogued",
            format!("object_id={}", object_id.get()),
        )
    })?;
    // Isolation: the object must be the table the caller named, with the same
    // owning database, schema, and table identity.
    if record.table_identity != identity {
        return Err(PlomidError::with_detail(
            ErrorKind::Catalog,
            "compaction input belongs to a different table",
            format!(
                "object_id={} expected_db={} expected_schema={} expected_table={} \
                 found_db={} found_schema={} found_table={}",
                object_id.get(),
                identity.database_id.get(),
                identity.schema_id.get(),
                identity.table_id.get(),
                record.table_identity.database_id.get(),
                record.table_identity.schema_id.get(),
                record.table_identity.table_id.get(),
            ),
        ));
    }
    if !record.is_published() {
        return Err(PlomidError::with_detail(
            ErrorKind::Catalog,
            "compaction target is not a published object",
            format!("object_id={}", object_id.get()),
        ));
    }
    // Every generation the catalog owns for the object, plus their row counts
    // read back from the durable segments so the report is measured, not
    // assumed.
    let owned: Vec<GenerationId> = record.generation_ids();
    let mut inputs = Vec::with_capacity(owned.len());
    for generation in owned {
        if reader.generation(generation).is_none() {
            return Err(PlomidError::with_detail(
                ErrorKind::Corruption,
                "compaction input generation is missing from the published state",
                format!("generation_id={}", generation.get()),
            ));
        }
        inputs.push(CompactionInput {
            generation,
            current: generation == record.current_generation,
        });
    }
    inputs.sort_unstable_by_key(|input| input.generation);
    Ok(CompactionPlan {
        identity,
        object_id,
        inputs,
        horizon: last_committed,
        // Physical release requires proof that no snapshot older than this
        // compaction can be required: that holds exactly when no transaction is
        // active at all.
        inputs_releasable: active_transactions.is_empty(),
    })
}

/// Resolves the segments one generation's physical references name.
///
/// The generation metadata already carries the mapping from logical generation
/// to physical structure, so no second lookup table is needed and no
/// device-specific assumption is made: whatever device the placement layer chose
/// is named by the reference.
pub fn generation_segments(
    manager: &Arc<GenerationManager>,
    object_id: ObjectId,
    generation: GenerationId,
) -> Result<Vec<plomid_core::SegmentId>> {
    let metadata = plomid_storage::load_generation(manager.root(), generation)?;
    if metadata.object_id != object_id {
        return Err(PlomidError::with_detail(
            ErrorKind::Corruption,
            "compaction input generation belongs to a different object",
            format!("generation_id={}", generation.get()),
        ));
    }
    let mut segments: Vec<plomid_core::SegmentId> = metadata
        .references
        .iter()
        .map(|reference| reference.structure.segment_id)
        .collect();
    segments.sort_unstable();
    segments.dedup();
    Ok(segments)
}

/// Reads one generation's materialized rows back through the existing segment
/// reader, which verifies every checksum before a value is returned.
///
/// This is the verification half of input handling: it proves a selected
/// generation is independently decodable, so selection never feeds a corrupt or
/// half-written generation into a compaction.
pub fn read_generation_rows<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    object_id: ObjectId,
    generation: GenerationId,
) -> Result<Vec<Row>> {
    read_generation_rows_gated(
        store,
        engine,
        manager,
        object_id,
        generation,
        &CompactionGate::open(),
    )
}

/// Gated variant of [`read_generation_rows`]: checks `gate` before every
/// segment and enforces throttling / cancellation / deadline cooperatively.
///
/// Degraded gates yield periodically (counted on the gate); paused, cancelled,
/// or expired gates abort with `Aborted` before returning any rows. The open
/// gate (`Full`, never-cancel) never yields and preserves exact ungated
/// behavior.
pub fn read_generation_rows_gated<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    object_id: ObjectId,
    generation: GenerationId,
    gate: &CompactionGate,
) -> Result<Vec<Row>> {
    // Entry checkpoint: paused / cancelled / expired refuse before any I/O.
    gate.enforce(0)?;
    let segments = generation_segments(manager, object_id, generation)?;
    let mut rows = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        // Per-segment checkpoint so long generations stay preemptible and
        // degraded passes yield between segments.
        gate.enforce(index as u64)?;
        let bytes = store.read_segment(engine, *segment)?;
        let reader = SegmentReader::decode(&bytes)?;
        let ids: Vec<_> = reader
            .columns
            .iter()
            .map(|column| column.column_id)
            .collect();
        // Chunked row decode: enforce the gate every throttle-sized chunk so
        // a single huge segment cannot starve OLTP. Decoding itself still
        // goes through the existing reader; only the checkpoint cadence is
        // new.
        let chunk = gate.throttle().max_rows_per_slice.max(1);
        let total = reader.row_count;
        let mut start = 0u64;
        while start < total {
            let end = (start + chunk).min(total);
            rows.extend(reader.read_rows(&bytes, start, end, &ids)?);
            // rows.len() is the progress cursor for yield cadence.
            gate.enforce(rows.len() as u64)?;
            start = end;
        }
    }
    Ok(rows)
}

/// Result of reading the current published generation for SQL.
///
/// The counters are measured while the segment readers are walking their BRIN
/// candidates. They describe the immutable generation only; SQL still applies
/// its MVCC/SQL predicate path to the returned rows.
#[derive(Debug)]
pub struct ColumnarScan {
    pub rows: Vec<Row>,
    pub column_ids: Vec<plomid_core::ColumnId>,
    pub segments_considered: usize,
    pub segments_skipped: usize,
    pub rows_examined: u64,
    pub rows_skipped: u64,
    pub columns_read: usize,
}

/// Buffer-manager key for one OLAP segment read.
///
/// Segment identities are process-monotonic and never reused while a payload
/// that names them exists (see [`ColumnarStore::recover_segment_ids`]), so
/// the identity alone is a unique key without object or generation
/// qualifiers.
pub fn olap_buffer_key(segment: plomid_core::SegmentId) -> String {
    format!("columnar-segment-{}", segment.get())
}

/// Reads the current published generation's main image: the immutable rows
/// exactly as published, without any post-publication delta overlay.
///
/// Shared core behind [`read_current_generation_rows`] (which adds the
/// delta overlay) and [`read_current_generation_rows_with_deltas`]. `None`
/// means the object has no complete published generation and callers must
/// use the Hot Row Store.
fn read_main_scan<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    object_id: ObjectId,
    predicate: Option<&crate::pruning::PrunePredicate>,
    requested_columns: Option<&[plomid_core::ColumnId]>,
) -> Result<Option<ColumnarScan>> {
    let snapshot = store.generations().reader()?;
    let Some(record) = snapshot.object(object_id) else {
        return Ok(None);
    };
    if !record.is_published() {
        return Ok(None);
    }
    let generation = record.current_generation;
    let segments = generation_segments(store.generations(), object_id, generation)?;
    let mut result = ColumnarScan {
        rows: Vec::new(),
        column_ids: Vec::new(),
        segments_considered: segments.len(),
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };
    for segment in segments {
        // OLAP scan consult of the process-wide buffer manager: a reused
        // segment records a hit (promoting Streaming -> Warm -> Hot on
        // genuine re-scans); a first touch is admitted as Streaming, which
        // occupies ledger space but stays the first eviction victim, so
        // one-shot scans cannot evict the OLTP working set. Maintenance
        // input reads (`read_generation_rows_gated`) deliberately bypass
        // this: they are write-path I/O, not analytical reuse.
        let buffer_key = olap_buffer_key(segment);
        let buffer = plomid_htap::global_buffer_manager();
        let hit = buffer.record_hit(&buffer_key);
        let bytes = store.read_segment(engine, segment)?;
        if !hit {
            buffer.admit_with_temp(
                &buffer_key,
                plomid_htap::CacheClass::OlapChunk,
                bytes.len() as u64,
                plomid_htap::Temperature::Streaming,
            );
        }
        let reader = SegmentReader::decode(&bytes)?;
        let column_ids: Vec<_> = requested_columns.map_or_else(
            || {
                reader
                    .columns
                    .iter()
                    .map(|column| column.column_id)
                    .collect()
            },
            |requested| requested.to_vec(),
        );
        if result.column_ids.is_empty() {
            result.column_ids = column_ids.clone();
        }
        result.columns_read += column_ids.len();
        let plan = predicate.map_or_else(
            || crate::pruning::ScanPlan {
                candidates: vec![crate::pruning::CandidateRange::new(0, reader.row_count)],
                total_rows: reader.row_count,
                scanned_rows: reader.row_count,
            },
            |predicate| {
                crate::pruning::plan_scan(
                    reader
                        .pruning
                        .as_ref()
                        .unwrap_or(&crate::pruning::SegmentPruning::empty(reader.row_count)),
                    reader.row_count,
                    predicate,
                )
            },
        );
        result.rows_examined += plan.scanned_rows;
        result.rows_skipped += plan.pruned_rows();
        if plan.candidates.is_empty() {
            result.segments_skipped += 1;
            continue;
        }
        for candidate in plan.candidates {
            result.rows.extend(reader.read_rows(
                &bytes,
                candidate.start_row,
                candidate.end_row,
                &column_ids,
            )?);
        }
    }
    drop(snapshot);
    plomid_core::observability::record_columnar_scan(
        (result.segments_considered as u64).saturating_sub(result.segments_skipped as u64),
        result.segments_skipped as u64,
        result.columns_read as u64,
        // Scalar path evaluates no vectorized batches (see vector.rs).
        0,
    );
    Ok(Some(result))
}

/// Reads the current published generation of one object through the existing
/// segment reader and BRIN/Zone Map planner, overlaid with the table's
/// confirmed post-publication deltas. `None` means the object has no
/// complete published generation and callers must use the Hot Row Store.
///
/// The delta key and snapshot derive from what this path already holds —
/// the engine root plus the columnar object identity, and the live commit
/// watermark — so no caller changes were needed to wire the merge in.
/// Fresh tables (the only ones the SQL freshness gate admits here) carry no
/// confirmed deltas, so the overlay merges the empty set and the answer is
/// exactly the published image; the overlay exists so a future delta-aware
/// gate can admit stale tables without a second scan path.
pub fn read_current_generation_rows<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    object_id: ObjectId,
    predicate: Option<&crate::pruning::PrunePredicate>,
    requested_columns: Option<&[plomid_core::ColumnId]>,
) -> Result<Option<ColumnarScan>> {
    let key = crate::delta::delta_key(engine.root(), object_id);
    let snapshot_seq = crate::delta::commit_watermark(&*engine);
    read_current_generation_rows_with_deltas(
        store,
        engine,
        object_id,
        &key,
        snapshot_seq,
        predicate,
        requested_columns,
    )
}

/// MAIN + DELTA scan: the published generation overlaid with the committed
/// post-publication writes visible under `snapshot_seq`.
///
/// Read feed of the delta design ([`crate::delta`]): `delta_key` names the
/// table exactly as [`crate::delta::delta_key`] does, and `snapshot_seq`
/// bounds delta visibility (`commit_seq <= snapshot_seq`), so concurrent
/// commits past the snapshot never leak into the scan. Deltas merge through
/// [`crate::delta::DeltaStore::merge_bag`] over the main images; tables with
/// no confirmed deltas take the early return below and pay nothing.
///
/// Shape safety: the main scan may be column-projected, so every visible
/// delta is projected to the scan width (positional column identities are
/// schema ordinals) before merging. A delta whose image no longer matches
/// the scan width (schema drift mid-window) is skipped, never misaligned:
/// the row path stays authoritative for it until the next publication.
/// Predicate safety needs no handling here: BRIN pruning only narrows the
/// main read, and SQL callers re-evaluate `WHERE` over every returned row,
/// so merged delta rows face the same filter as published rows.
pub fn read_current_generation_rows_with_deltas<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    object_id: ObjectId,
    delta_key: &str,
    snapshot_seq: u64,
    predicate: Option<&crate::pruning::PrunePredicate>,
    requested_columns: Option<&[plomid_core::ColumnId]>,
) -> Result<Option<ColumnarScan>> {
    let Some(mut scan) = read_main_scan(store, engine, object_id, predicate, requested_columns)?
    else {
        return Ok(None);
    };
    let deltas = crate::delta::global_delta_store();
    if deltas.row_count(delta_key) == 0 {
        return Ok(Some(scan));
    }
    if scan.column_ids.is_empty() {
        // No column shape to project deltas onto (a generation that names
        // no columns): fail closed to the published image.
        return Ok(Some(scan));
    }
    let ordinals: Vec<usize> = scan.column_ids.iter().map(|id| id.get() as usize).collect();
    let project = |fields: &[plomid_storage::Field]| -> Option<Vec<plomid_storage::Field>> {
        ordinals
            .iter()
            .map(|ordinal| fields.get(*ordinal).cloned())
            .collect()
    };
    let mut shaped: Vec<crate::delta::DeltaRow> = Vec::new();
    for delta in deltas.scan_history(delta_key, snapshot_seq) {
        // A pre-less tombstone carries no image to project; it resolves
        // through the id chain (or is ignored) downstream.
        let values = if delta.values.is_empty() && delta.deleted {
            Vec::new()
        } else {
            let Some(values) = project(&delta.values) else {
                continue;
            };
            values
        };
        let prev = match &delta.prev {
            Some(prev) => match project(prev) {
                Some(prev) => Some(prev),
                None => continue,
            },
            None => None,
        };
        shaped.push(crate::delta::DeltaRow {
            row_id: delta.row_id,
            values,
            deleted: delta.deleted,
            commit_seq: delta.commit_seq,
            prev,
        });
    }
    scan.rows = crate::delta::apply_history(&scan.rows, &shaped);
    Ok(Some(scan))
}
#[derive(Clone, Debug)]
pub struct CompactionRequest {
    /// Owning `Database → Schema → Table` location of the table.
    pub identity: TableIdentity,
    /// Catalog object the table is stored as.
    pub object_id: ObjectId,
    /// Column types of the materialized rows.
    pub column_types: Vec<ColumnType>,
    /// Flush tunables (encoding, compression, statistics, pruning).
    pub config: FlushConfig,
    /// Identity of the new generation.
    pub generation: GenerationId,
    /// Durable storage generation the new state represents.
    pub storage_generation: GenerationId,
    /// WAL boundary the new state represents.
    pub checkpoint_lsn: Lsn,
}

/// Result of one compaction run.
#[derive(Clone, Debug)]
pub struct CompactionOutcome {
    /// Plan the run executed.
    pub plan: CompactionPlan,
    /// The published generation, when the run reached publication.
    pub published: Option<PublishedColumnarSegment>,
    /// Generations whose retention this run released, ascending.
    pub released: Vec<GenerationId>,
    /// Reclamation result, when a pass ran.
    pub reclaimed: Option<GcOutcome>,
    /// Logical rows visible to the compaction snapshot.
    pub visible_rows: usize,
    /// Logical rows the new generation holds.
    pub output_rows: usize,
    /// Input generations proven still required and therefore retained.
    pub retained_inputs: Vec<GenerationId>,
    /// Rows read back from each input generation, ascending by generation.
    ///
    /// Measured from the durable segments, not assumed, so a before/after
    /// comparison reports real counts.
    pub input_rows: Vec<(GenerationId, u64)>,
    /// Columnar segments whose payload slices this run deleted, ascending.
    ///
    /// A segment is listed only when the reclamation pass above proved its
    /// owning generations unreachable AND no retained generation still
    /// references it (sharing-safe). Empty when reclamation was skipped,
    /// proved nothing unreachable, or found every segment still shared.
    pub reclaimed_segments: Vec<plomid_core::SegmentId>,
    /// Manifest plus payload bytes deleted with [`Self::reclaimed_segments`],
    /// measured before deletion.
    pub reclaimed_segment_bytes: u64,
}

/// Maps every owned generation to the columnar segment identities its
/// metadata references.
///
/// Captured before reclamation deletes generation metadata files: after gc, a
/// reclaimed generation's references are unreadable, so record them first.
/// Best-effort per generation (an unloadable generation is omitted, so its
/// slices survive the run that reclaims its files; the file-level proof is
/// unaffected).
pub fn slice_ownership_snapshot(
    manager: &std::sync::Arc<plomid_storage::GenerationManager>,
) -> std::collections::BTreeMap<plomid_core::GenerationId, Vec<plomid_core::SegmentId>> {
    let mut map = std::collections::BTreeMap::new();
    let snapshot = match manager.load() {
        Ok(snapshot) => snapshot,
        Err(_) => return map,
    };
    for record in snapshot.records() {
        for generation in record.generation_ids() {
            let Ok(metadata) = plomid_storage::load_generation(manager.root(), generation) else {
                continue;
            };
            let mut segments: Vec<plomid_core::SegmentId> = metadata
                .references
                .iter()
                .map(|reference| reference.structure.segment_id)
                .collect();
            segments.sort_unstable();
            segments.dedup();
            map.insert(generation, segments);
        }
    }
    map
}

/// Deletes payload slices of reclaimed generations, honoring segment sharing.
///
/// `reclaimed` lists generations a reclamation pass proved unreachable;
/// `snapshot` maps generations to segments as captured before metadata
/// deletion. A segment is deleted only when no generation still owned by any
/// record references it (fail-closed: an unloadable retained generation
/// aborts slice deletion for this run; the next run retries). Every deletion
/// runs in a WAL-logged engine transaction, so interruption can only orphan
/// invisible slices, never remove reachable data. Returns deleted segment
/// identities (ascending) and measured bytes.
pub fn delete_unreferenced_slices<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &std::sync::Arc<plomid_storage::GenerationManager>,
    reclaimed: &[plomid_core::GenerationId],
    snapshot: &std::collections::BTreeMap<plomid_core::GenerationId, Vec<plomid_core::SegmentId>>,
) -> (Vec<plomid_core::SegmentId>, u64) {
    if reclaimed.is_empty() || snapshot.is_empty() {
        return (Vec::new(), 0);
    }
    // Retained segments: every segment referenced by any still-owned
    // generation of any record. Unloadable metadata aborts the run.
    let loaded = match manager.load() {
        Ok(loaded) => loaded,
        Err(_) => return (Vec::new(), 0),
    };
    let mut retained = std::collections::BTreeSet::new();
    for record in loaded.records() {
        for generation in record.generation_ids() {
            let Ok(metadata) = plomid_storage::load_generation(manager.root(), generation) else {
                return (Vec::new(), 0);
            };
            retained.extend(
                metadata
                    .references
                    .iter()
                    .map(|reference| reference.structure.segment_id),
            );
        }
    }
    let mut doomed_set = std::collections::BTreeSet::new();
    for generation in reclaimed {
        if let Some(segments) = snapshot.get(generation) {
            doomed_set.extend(segments.iter().copied());
        }
    }
    let mut deleted = Vec::new();
    let mut bytes = 0u64;
    for segment in doomed_set.difference(&retained) {
        match store.delete_segment(engine, *segment) {
            Ok(outcome) => {
                if !outcome.manifest_absent {
                    deleted.push(*segment);
                    bytes = bytes.saturating_add(outcome.bytes_removed);
                }
            }
            // A segment that cannot be deleted keeps its slices; files are
            // already gone per the file-level proof, so this run reports what
            // it removed and leaves the rest orphaned-but-invisible for a
            // later run. Never fail the compaction for cleanup.
            Err(_) => continue,
        }
    }
    deleted.sort_unstable();
    (deleted, bytes)
}

/// Builds, stages, verifies, syncs, and publishes one compacted generation.
///
/// Shared by [`compact`] and journaled resume: the segment identity is
/// allocated here, so every caller gets a fresh identity and a crash between
/// attempts can only orphan invisible slices for a later VACUUM pass.
pub fn build_and_publish<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    request: &CompactionRequest,
    rows: &[Row],
    columnar_fail: ColumnarFailPoint,
) -> Result<PublishedColumnarSegment> {
    let segment_id = store.allocate_segment_id();
    let publish = ColumnarPublish::new(
        request.object_id,
        ColumnarStore::columnar_schema(
            request.identity.schema_id,
            plomid_core::CatalogVersion::new(1),
            &request.column_types,
        )?,
        request.generation,
        request.storage_generation,
        request.checkpoint_lsn,
    )
    .with_identity(request.identity);
    store.flush_rows_with_id(
        engine,
        rows,
        &request.column_types,
        segment_id,
        &request.config,
        &publish,
        columnar_fail,
    )
}

/// Releases input generations whose retention the live transaction state
/// proves unnecessary, returning the released identities ascending.
///
/// The seal is read before the active set (see [`release_provable`]); only
/// generations the catalog still owns are released, so repeating the step is
/// a no-op rather than a second publication. `fail_release` injects the
/// release-boundary failure *after* the release publishes, matching the
/// [`CompactionFailPoint::DuringRelease`] contract.
pub fn release_inputs_if_provable<E: StorageEngine>(
    manager: &Arc<GenerationManager>,
    engine: &mut E,
    plan: &CompactionPlan,
    request: &CompactionRequest,
    fail_release: bool,
) -> Result<Vec<GenerationId>> {
    let release_provable = if fail_release {
        true
    } else {
        // Order is load-bearing (see `release_provable`): seal first, then
        // the active set, so identities at or above the seal provably began
        // after the publication above.
        let seal = engine.txn_issue_mark()?;
        let (fresh_active, _) = mvcc_safety(engine)?;
        release_provable(&fresh_active, seal)
    };
    let mut released = Vec::new();
    if release_provable {
        // Idempotent filter: generations already released by an earlier
        // attempt are no longer owned, so they are skipped instead of
        // re-published.
        let owned = manager.load()?;
        let inputs: Vec<GenerationId> = plan
            .generation_ids()
            .into_iter()
            .filter(|generation| owned.owns_generation(*generation))
            .collect();
        if !inputs.is_empty() {
            let release_request = plomid_storage::PublicationRequest::release(
                request.storage_generation,
                request.checkpoint_lsn,
                inputs.clone(),
            )?;
            manager.publish(release_request)?;
            released = inputs;
        }
        if fail_release {
            return Err(injected("during retention release"));
        }
    }
    Ok(released)
}

/// Runs one reclamation pass plus sharing-safe slice deletion.
///
/// Returns the reclamation outcome (when `reclaim` was requested), the deleted
/// segment identities ascending, and the measured deleted bytes. Slice
/// deletion reuses [`delete_unreferenced_slices`], whose per-segment deletion
/// is idempotent, so repeating the step is safe.
pub fn reclaim_after_compaction<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    doomed_segments: &std::collections::BTreeMap<GenerationId, Vec<plomid_core::SegmentId>>,
    reclaim: bool,
    gc_fail: GcFailPoint,
) -> Result<(Option<GcOutcome>, Vec<plomid_core::SegmentId>, u64)> {
    let reclaimed = if reclaim {
        Some(manager.gc_with_fail_point(gc_fail)?)
    } else {
        None
    };

    // Slice reclamation: delete payload slices of generations the pass above
    // proved unreachable, and only those. Segment sharing is honored by
    // subtracting every segment still referenced by a retained generation
    // (fail-closed: an unloadable retained generation aborts slice deletion
    // for this run). Deletion runs in WAL-logged engine transactions, so a
    // crash can only leave orphaned (invisible) slices behind, never remove
    // reachable data.
    let (reclaimed_segments, reclaimed_segment_bytes) = match &reclaimed {
        Some(outcome) => {
            delete_unreferenced_slices(store, engine, manager, &outcome.reclaimed, doomed_segments)
        }
        None => (Vec::new(), 0),
    };
    Ok((reclaimed, reclaimed_segments, reclaimed_segment_bytes))
}

/// Reads back the durably retained inputs after a run, ascending.
pub fn retained_after_compaction(
    manager: &Arc<GenerationManager>,
    plan: &CompactionPlan,
) -> Result<Vec<GenerationId>> {
    let after = manager.load()?;
    Ok(plan
        .generation_ids()
        .into_iter()
        .filter(|generation| after.owns_generation(*generation))
        .collect())
}

/// Measures the rows each input generation holds from its durable segments.
///
/// Long retained histories stay preemptible through per-input checkpoints.
pub fn measure_input_rows_gated<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    object_id: ObjectId,
    plan: &CompactionPlan,
    gate: &plomid_htap::CompactionGate,
) -> Result<Vec<(GenerationId, u64)>> {
    let mut input_rows = Vec::with_capacity(plan.inputs.len());
    for (index, input) in plan.inputs.iter().enumerate() {
        // Per-input checkpoint: long retained histories stay preemptible.
        gate.enforce(index as u64)?;
        let decoded =
            read_generation_rows_gated(store, engine, manager, object_id, input.generation, gate)?;
        input_rows.push((input.generation, decoded.len() as u64));
    }
    // After-input-read checkpoint: abort before building when pressure spiked,
    // cancellation arrived, or the deadline lapsed during verification.
    gate.enforce(input_rows.len() as u64)?;
    Ok(input_rows)
}

/// Runs one complete compaction of a table's published generations.
///
/// The sequence is `select → read inputs → build → flush → verify → sync →
/// publish → retain → release → GC → slice-reclaim`, where every stage delegates
/// to an existing subsystem:
///
/// * `rows` is the row state visible to a consistent snapshot, produced through
///   the existing MVCC read path. Compaction does not decide visibility.
/// * the new generation is built, verified, synced, and published by the
///   existing columnar flush and generation publication machinery.
/// * the inputs stay retained by publication itself; releasing their retention
///   is a separate publication gated on a transaction-identity horizon
///   ([`release_provable`]) rather than a delay: every transaction active
///   below the seal began before this evaluation and may hold an older
///   snapshot, so its generations stay retained.
/// * reclamation is the existing reachability proof over catalog, live reader
///   leases, and checkpoints; slice payloads of reclaimed generations are then
///   deleted from storage, and only those.
/// * the run holds a generation-reader lease across input verification (so a
///   concurrent reclamation pass retains what this run reads), dropped before
///   release so the run's own lease cannot pin superseded inputs.
///
/// `reclaim` requests a reclamation pass after a successful run. Compaction
/// never removes a generation that is still retained or still observed by a live
/// reader, because that decision belongs to the existing reclamation pass.
#[allow(clippy::too_many_arguments)]
pub fn compact<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    request: &CompactionRequest,
    rows: &[Row],
    active_transactions: &[u64],
    last_committed: u64,
    fail_at: CompactionFailPoint,
    reclaim: bool,
) -> Result<CompactionOutcome> {
    compact_gated(
        store,
        engine,
        request,
        rows,
        active_transactions,
        last_committed,
        fail_at,
        reclaim,
        &CompactionGate::open(),
    )
}

/// Gated variant of [`compact`]: identical orchestration, plus cooperative
/// throttling / cancellation / deadline checkpoints.
///
/// Checkpoints (in order): entry → after input-read → chunked row build loop
/// (yield every `yield_every_rows`) → after build → before release → before
/// reclaim → before slice-reclaim. Any abort returns `Aborted` before the next
/// visible mutation; in particular a pre-publish abort leaves the publication
/// pointer, catalog, and segments exactly as found. The open gate
/// (`Full`, never-cancel, no deadline) never yields and preserves exact
/// ungated behavior.
///
/// OLTP non-competition: a `Paused` gate (critical pressure, or a governor
/// `Queue`/`Reject` mapped through
/// [`CompactionGate::admit_or_refuse`](plomid_htap::CompactionGate::admit_or_refuse))
/// aborts at entry instead of spinning; `Degraded` makes throttled progress
/// with periodic yields; `Full` runs at full speed.
#[allow(clippy::too_many_arguments)]
pub fn compact_gated<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    request: &CompactionRequest,
    rows: &[Row],
    active_transactions: &[u64],
    last_committed: u64,
    fail_at: CompactionFailPoint,
    reclaim: bool,
    gate: &CompactionGate,
) -> Result<CompactionOutcome> {
    // Entry checkpoint: paused / cancelled / expired refuse before any work,
    // so compaction never competes with OLTP indefinitely.
    gate.enforce(0)?;
    let manager = store.generations();
    let plan = select_inputs(
        manager,
        request.identity,
        request.object_id,
        active_transactions,
        last_committed,
    )?;
    // Reader lifetime for input verification and publication only (dropped
    // before the release step below): pin the selected catalog state while
    // this run reads input generations, so a concurrent reclamation pass
    // retains them. The guard MUST NOT span release+reclamation, or this
    // run's own lease would pin the pre-run catalog and keep every input
    // reachable, defeating the reclamation that follows. After verification,
    // this run touches inputs only through in-memory captures (segment map)
    // and storage keys, which no generation-file deletion can break. A
    // missing publication pointer (nothing published yet) means there is
    // nothing to pin; any other acquisition failure aborts the run rather
    // than running unpinned.
    let _input_guard = match manager.reader() {
        Ok(reader) => Some(reader),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };

    // Input verification: every selected generation is decoded and measured.
    // A generation that cannot be read stops the run before anything is
    // written, so compaction never builds on unreadable input.
    let input_rows =
        measure_input_rows_gated(store, engine, manager, request.object_id, &plan, gate)?;
    if fail_at == CompactionFailPoint::DuringInputRead {
        return Err(injected("during compaction input read"));
    }

    // A table already represented by a single generation is minimal: reporting
    // success without publishing avoids creating a generation that would hold
    // exactly the same rows.
    if plan.is_noop() {
        let outcome = CompactionOutcome {
            retained_inputs: plan.generation_ids(),
            plan,
            published: None,
            released: Vec::new(),
            reclaimed: None,
            reclaimed_segments: Vec::new(),
            reclaimed_segment_bytes: 0,
            visible_rows: rows.len(),
            output_rows: rows.len(),
            input_rows,
        };
        store.observe_compaction_outcome(
            engine,
            request,
            &outcome,
            &std::collections::BTreeMap::new(),
        );
        return Ok(outcome);
    }

    // Chunked build loop: walk the output rows in throttle-sized slices,
    // enforcing the gate per chunk. Degraded gates yield here (counted);
    // cancelled / expired / paused gates abort before the flush below, so a
    // mid-build abort never publishes. The open gate never yields, preserving
    // exact ungated behavior with only a few cheap checkpoints.
    {
        let chunk = gate.throttle().max_rows_per_slice.max(1);
        let mut processed = 0u64;
        for chunk_rows in rows.chunks(chunk as usize) {
            processed = processed.saturating_add(chunk_rows.len() as u64);
            gate.enforce(processed)?;
        }
    }

    let published = build_and_publish(store, engine, request, rows, fail_at.columnar())?;
    // After-build checkpoint: abort before release/reclaim when the build
    // raced a cancellation, deadline, or pressure spike. (The flush above
    // already published the new generation; later checkpoints only gate the
    // release + reclamation that follows.)
    gate.enforce(rows.len() as u64)?;
    let _ = fail_at.publication();

    // RETAIN is implicit: publication listed every input generation as retained
    // for the object, and the new generation became its current generation.
    // RELEASE happens only when live transaction state proves no snapshot older
    // than this compaction can still be required. The seal is read after the
    // new generation published: every transaction active below it began before
    // this evaluation and may hold an older snapshot, so inputs stay retained
    // while any such transaction lives; transactions at or above the seal
    // began afterwards and can only observe the newest state. An empty active
    // set releases vacuously. The plan-time flag is advisory only; this fresh
    // evaluation is authoritative.
    // Segment ownership snapshot for slice reclamation below. Captured before
    // reclamation deletes generation metadata files: after gc, a reclaimed
    // generation's references are unreadable, so record them now. Best-effort
    // per generation (an unloadable generation simply keeps its slices this
    // run); the file-level proof in gc() is unaffected.
    let doomed_segments = slice_ownership_snapshot(manager);
    // End of the leased read window: drop the input guard before release so
    // this run's own lease cannot pin the pre-run catalog and defeat the
    // reclamation that follows. Peer runs hold their own guards across their
    // reads, so mutual protection is preserved.
    drop(_input_guard);
    // Before-release checkpoint: a pressure spike during the build aborts the
    // retention release instead of racing OLTP.
    gate.enforce(rows.len() as u64)?;
    let fail_release = fail_at == CompactionFailPoint::DuringRelease;
    // Fresh seal + active-set evaluation lives inside the helper (order is
    // load-bearing there); the DuringRelease injection publishes the release
    // first and then reports, matching the pre-refactor contract.
    let released = release_inputs_if_provable(manager, engine, &plan, request, fail_release)?;

    if reclaim {
        // Before-reclaim checkpoint: GC file deletion is the most I/O-heavy
        // tail of the pass; abort it under pressure rather than competing.
        gate.enforce(rows.len() as u64)?;
    }
    let (reclaimed, reclaimed_segments, reclaimed_segment_bytes) = reclaim_after_compaction(
        store,
        engine,
        manager,
        &doomed_segments,
        reclaim,
        fail_at.reclamation(),
    )?;

    // Retained inputs are read back from the published catalog rather than
    // inferred, so the outcome reports what is durably true after the run.
    let retained_inputs = retained_after_compaction(manager, &plan)?;

    let outcome = CompactionOutcome {
        plan,
        published: Some(published),
        released,
        reclaimed,
        reclaimed_segments,
        reclaimed_segment_bytes,
        visible_rows: rows.len(),
        output_rows: rows.len(),
        retained_inputs,
        input_rows,
    };
    store.observe_compaction_outcome(engine, request, &outcome, &doomed_segments);
    Ok(outcome)
}

/// Builds an injected-failure error for fail-point tests.
fn injected(what: &str) -> PlomidError {
    PlomidError::new(
        ErrorKind::Io,
        format!("injected compaction failure: {what}"),
    )
}
