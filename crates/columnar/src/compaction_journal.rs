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
//! Crash-safe incremental resumable compaction journal.
//!
//! The journal is the crash-safety boundary that [`super::compaction`] never
//! had: every state transition of one compaction job is persisted through an
//! atomic `.tmp` + rename + fsync sequence (the same pattern
//! `storage/checkpoint.rs` uses), so a crash between any two steps leaves a
//! fully readable record of how far the job progressed. Recovery is
//! idempotent by construction:
//!
//! * re-publish is a no-op when the job's output generation is already the
//!   object's current generation (re-publishing would be rejected as
//!   "already published");
//! * re-release only releases input generations the catalog still owns, so a
//!   repeated release publishes nothing new;
//! * re-reclamation reuses [`super::compaction::delete_unreferenced_slices`],
//!   whose [`super::store::ColumnarStore::delete_segment`] reports an absent
//!   manifest instead of failing.
//!
//! ```text
//! begin_job → Planned →(plan_chunks)→ Building →(chunks done)→ Built
//!     →(publish)→ Published →(release)→ Released →(gc)→ Reclaimed (terminal)
//!     ↘(abort from any non-terminal)→ Aborted (terminal)
//! ```
//!
//! # Incremental chunks
//!
//! A job's input rows are split into fixed-size row ranges
//! ([`CompactionChunk`], default [`DEFAULT_CHUNK_ROWS`] rows each). Each chunk
//! is build-validated through the existing in-memory [`crate::flush::flush`]
//! pipeline — the same code the final publication runs — and marked done.
//! A resumed job validates only its pending chunks
//! ([`CompactionJournal::next_pending_chunk`]) and continues at the first
//! undone one; the final publication is still one generation through the
//! existing flush path, so the journal never invents a durability boundary.
//!
//! # Rows are not journaled
//!
//! The journal persists the plan, the output coordinates, and per-chunk
//! progress, but never the rows themselves: rows are re-derived from the
//! committed snapshot by the caller on resume, exactly as [`super::compaction`]
//! derives them on a fresh run. Chunk ranges therefore describe the `rows`
//! slice handed to [`ColumnarStore::resume_job`](crate::store::ColumnarStore::resume_job),
//! and a resume whose rows do not cover exactly `[0, chunks_end)` is rejected.

use crate::chunk::ChunkCompression;
use crate::column::ColumnType;
use crate::compaction::{
    delete_unreferenced_slices, mvcc_safety, read_generation_rows, release_provable,
    slice_ownership_snapshot, CompactionInput, CompactionOutcome, CompactionPlan,
    CompactionRequest,
};
use crate::encoding::ColumnEncoding;
use crate::flush::FlushConfig;
use crate::store::{ColumnarPublish, ColumnarStore, PublishedColumnarSegment};
use plomid_core::{
    DatabaseId, ErrorKind, GenerationId, Lsn, ObjectId, PlomidError, Result, SchemaId, SegmentId,
    TableId, TableIdentity,
};
use plomid_storage::{GenerationManager, Row, StorageEngine};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Default number of input rows covered by one [`CompactionChunk`].
pub const DEFAULT_CHUNK_ROWS: usize = 8192;

/// Subdirectory of the store root that holds journal state.
const JOURNAL_DIR_NAME: &str = "compaction-journal";
/// Filename prefix of one persisted job record.
const JOB_FILE_PREFIX: &str = "job-";
/// Filename suffix of one persisted job record.
const JOB_FILE_SUFFIX: &str = ".cjnl";
/// Magic of one persisted job record.
const JOURNAL_MAGIC: [u8; 8] = *b"PLCMJRN\x01";
/// Version of the persisted job record format.
///
/// Version 2 appends the per-column encoding policies after
/// `collect_pruning`. Version 1 records decode with empty policies.
const JOURNAL_VERSION: u32 = 2;

/// Durable state of one compaction job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobState {
    /// The plan and output coordinates are recorded; no build work done.
    Planned,
    /// Chunk ranges are recorded; build validation is in progress.
    Building,
    /// Every chunk validated; the output is ready to publish.
    Built,
    /// The output generation is published and current.
    Published,
    /// The release step completed (inputs released when provable).
    Released,
    /// Reclamation completed. Terminal.
    Reclaimed,
    /// Abandoned by the operator. Terminal.
    Aborted,
}

impl JobState {
    fn tag(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::Building => 1,
            Self::Built => 2,
            Self::Published => 3,
            Self::Released => 4,
            Self::Reclaimed => 5,
            Self::Aborted => 6,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Planned),
            1 => Some(Self::Building),
            2 => Some(Self::Built),
            3 => Some(Self::Published),
            4 => Some(Self::Released),
            5 => Some(Self::Reclaimed),
            6 => Some(Self::Aborted),
            _ => None,
        }
    }

    /// True for [`Self::Reclaimed`] and [`Self::Aborted`]: no further step runs.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Reclaimed | Self::Aborted)
    }
}

/// One validated row range of a job's input rows, `[rows_start, rows_end)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactionChunk {
    /// Job this chunk belongs to.
    pub job_id: u64,
    /// Position of the chunk within the job, ascending from zero.
    pub chunk_idx: u32,
    /// First input row the chunk covers (inclusive).
    pub rows_start: u64,
    /// One past the last input row the chunk covers (exclusive).
    pub rows_end: u64,
    /// True once the chunk's rows passed build validation.
    pub done: bool,
}

/// One crash-safe incremental compaction job.
#[derive(Clone, Debug)]
pub struct CompactionJob {
    /// Journal-assigned identity, unique within the journal directory.
    pub job_id: u64,
    /// Owning `Database → Schema → Table` location (mirrors the plan).
    pub table: TableIdentity,
    /// Input generations selected at [`CompactionJournal::begin_job`] time.
    pub plan: CompactionPlan,
    /// Current durable milestone.
    pub state: JobState,
    /// Journal sequence at creation.
    pub created_seq: u64,
    /// Journal sequence at the last transition.
    pub updated_seq: u64,
    /// Rows per chunk used by this job.
    pub chunk_rows: usize,
    /// Chunk ranges in ascending `chunk_idx` order (empty until planned).
    pub chunks: Vec<CompactionChunk>,
    /// Column types of the rows the job compacts.
    pub column_types: Vec<ColumnType>,
    /// Flush tunables the final publication uses.
    pub config: FlushConfig,
    /// Generation identity of the compacted output.
    pub output_generation: GenerationId,
    /// Durable storage generation the output represents.
    pub storage_generation: GenerationId,
    /// WAL boundary the output represents.
    pub checkpoint_lsn: Lsn,
}

impl PartialEq for CompactionJob {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            && self.table == other.table
            && self.plan == other.plan
            && self.state == other.state
            && self.created_seq == other.created_seq
            && self.updated_seq == other.updated_seq
            && self.chunk_rows == other.chunk_rows
            && self.chunks == other.chunks
            && self.column_types == other.column_types
            && configs_equal(&self.config, &other.config)
            && self.output_generation == other.output_generation
            && self.storage_generation == other.storage_generation
            && self.checkpoint_lsn == other.checkpoint_lsn
    }
}

impl Eq for CompactionJob {}

/// Compares two flush configurations field by field (`FlushConfig` itself
/// carries no equality impl, and the journal must not require one).
fn configs_equal(left: &FlushConfig, right: &FlushConfig) -> bool {
    left.encoding == right.encoding
        && left.column_policies == right.column_policies
        && left.compression == right.compression
        && left.chunk_target_size == right.chunk_target_size
        && left.collect_statistics == right.collect_statistics
        && left.verify_checksums == right.verify_checksums
        && left.brin_rows_per_range == right.brin_rows_per_range
        && left.collect_pruning == right.collect_pruning
}

impl CompactionJob {
    /// Rebuilds the publication request this job publishes.
    #[must_use]
    pub fn request(&self) -> CompactionRequest {
        CompactionRequest {
            identity: self.table,
            object_id: self.plan.object_id,
            column_types: self.column_types.clone(),
            config: self.config.clone(),
            generation: self.output_generation,
            storage_generation: self.storage_generation,
            checkpoint_lsn: self.checkpoint_lsn,
        }
    }

    /// True when every planned chunk validated (vacuously true with no chunks).
    #[must_use]
    pub fn chunks_complete(&self) -> bool {
        self.chunks.iter().all(|chunk| chunk.done)
    }

    /// One past the last row covered by the planned chunks (zero when none).
    #[must_use]
    pub fn chunks_end(&self) -> u64 {
        self.chunks.last().map_or(0, |chunk| chunk.rows_end)
    }
}

/// Crash-safe journal of compaction jobs, rooted at the store directory.
///
/// State lives in `<root>/compaction-journal/job-<id:020>.cjnl`. Every
/// transition rewrites the job file through `.tmp` + fsync + atomic rename +
/// directory fsync, so a crash leaves either the previous or the next state,
/// never a torn record. Readers ignore `*.tmp` artifacts.
pub struct CompactionJournal {
    dir: PathBuf,
    next_job_id: AtomicU64,
    next_seq: AtomicU64,
    chunk_rows: usize,
}

impl CompactionJournal {
    /// Opens (creating when absent) the journal under `root`.
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_chunk_rows(root, DEFAULT_CHUNK_ROWS)
    }

    /// Opens the journal with an explicit chunk size for tests and tuning.
    pub fn open_with_chunk_rows(root: &Path, chunk_rows: usize) -> Result<Self> {
        if chunk_rows == 0 {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "compaction journal chunk_rows is zero",
            ));
        }
        let dir = root.join(JOURNAL_DIR_NAME);
        std::fs::create_dir_all(&dir).map_err(PlomidError::from)?;
        let mut next_job_id = 1u64;
        let mut next_seq = 1u64;
        for job in Self::read_all_in(&dir)? {
            next_job_id = next_job_id.max(job.job_id.saturating_add(1));
            next_seq = next_seq
                .max(job.created_seq.saturating_add(1))
                .max(job.updated_seq.saturating_add(1));
        }
        Ok(Self {
            dir,
            next_job_id: AtomicU64::new(next_job_id),
            next_seq: AtomicU64::new(next_seq),
            chunk_rows,
        })
    }

    /// Directory holding the persisted job records.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Rows per chunk this journal plans with.
    #[must_use]
    pub fn chunk_rows(&self) -> usize {
        self.chunk_rows
    }

    /// Records a new job in [`JobState::Planned`] and returns it.
    pub fn begin_job(
        &self,
        plan: CompactionPlan,
        request: &CompactionRequest,
    ) -> Result<CompactionJob> {
        if plan.identity != request.identity || plan.object_id != request.object_id {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "compaction job plan and request name different tables",
            ));
        }
        let job_id = self.next_job_id.fetch_add(1, Ordering::SeqCst);
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let job = CompactionJob {
            job_id,
            table: plan.identity,
            plan,
            state: JobState::Planned,
            created_seq: seq,
            updated_seq: seq,
            chunk_rows: self.chunk_rows,
            chunks: Vec::new(),
            column_types: request.column_types.clone(),
            config: request.config.clone(),
            output_generation: request.generation,
            storage_generation: request.storage_generation,
            checkpoint_lsn: request.checkpoint_lsn,
        };
        self.persist(&job)?;
        Ok(job)
    }

    /// Splits `total_rows` input rows into chunks, records them, and moves a
    /// [`JobState::Planned`] job to [`JobState::Building`].
    pub fn plan_chunks(&self, job_id: u64, total_rows: usize) -> Result<Vec<CompactionChunk>> {
        let mut job = self.get(job_id)?;
        require_state(&job, JobState::Planned, "plan chunks")?;
        let total = total_rows as u64;
        let width = job.chunk_rows as u64;
        let mut chunks = Vec::new();
        let mut start = 0u64;
        let mut idx = 0u32;
        while start < total {
            let end = start.saturating_add(width).min(total);
            chunks.push(CompactionChunk {
                job_id,
                chunk_idx: idx,
                rows_start: start,
                rows_end: end,
                done: false,
            });
            start = end;
            idx = idx.saturating_add(1);
        }
        job.chunks = chunks.clone();
        self.transition(&mut job, JobState::Building)?;
        Ok(chunks)
    }

    /// Returns the first undone chunk of `job_id`, or `None` when all are done.
    pub fn next_pending_chunk(&self, job_id: u64) -> Result<Option<CompactionChunk>> {
        let job = self.get(job_id)?;
        Ok(job.chunks.iter().find(|chunk| !chunk.done).copied())
    }

    /// Marks one chunk validated. The job must be [`JobState::Building`].
    pub fn mark_chunk_done(&self, job_id: u64, chunk_idx: u32) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        require_state(&job, JobState::Building, "mark chunk done")?;
        let Some(chunk) = job
            .chunks
            .iter_mut()
            .find(|chunk| chunk.chunk_idx == chunk_idx)
        else {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                "compaction chunk is not part of the job",
                format!("job_id={job_id} chunk_idx={chunk_idx}"),
            ));
        };
        chunk.done = true;
        self.persist(&job)?;
        Ok(job)
    }

    /// Records that every chunk validated; the job may now publish.
    pub fn mark_built(&self, job_id: u64) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        if job.state == JobState::Built {
            return Ok(job);
        }
        require_state(&job, JobState::Building, "mark built")?;
        if !job.chunks_complete() {
            return Err(PlomidError::with_detail(
                ErrorKind::InvalidArgument,
                "compaction job still has unvalidated chunks",
                format!("job_id={job_id}"),
            ));
        }
        self.transition(&mut job, JobState::Built)?;
        Ok(job)
    }

    /// Records that the output generation published.
    pub fn mark_published(&self, job_id: u64) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        if job.state == JobState::Published {
            return Ok(job);
        }
        require_state(&job, JobState::Built, "mark published")?;
        self.transition(&mut job, JobState::Published)?;
        Ok(job)
    }

    /// Records that the release step completed.
    pub fn mark_released(&self, job_id: u64) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        if job.state == JobState::Released {
            return Ok(job);
        }
        require_state(&job, JobState::Published, "mark released")?;
        self.transition(&mut job, JobState::Released)?;
        Ok(job)
    }

    /// Records that reclamation completed. Terminal.
    pub fn mark_reclaimed(&self, job_id: u64) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        if job.state == JobState::Reclaimed {
            return Ok(job);
        }
        require_state(&job, JobState::Released, "mark reclaimed")?;
        self.transition(&mut job, JobState::Reclaimed)?;
        Ok(job)
    }

    /// Abandons a non-terminal job. Terminal.
    pub fn abort_job(&self, job_id: u64) -> Result<CompactionJob> {
        let mut job = self.get(job_id)?;
        if job.state.is_terminal() {
            return Err(PlomidError::with_detail(
                ErrorKind::InvalidArgument,
                "compaction job is already terminal",
                format!("job_id={job_id}"),
            ));
        }
        self.transition(&mut job, JobState::Aborted)?;
        Ok(job)
    }

    /// Loads one job by identity.
    pub fn get(&self, job_id: u64) -> Result<CompactionJob> {
        let bytes = std::fs::read(self.job_path(job_id)).map_err(|error| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                "compaction job is not journaled",
                format!("job_id={job_id} cause={error}"),
            )
        })?;
        decode_job(&bytes)
    }

    /// Lists every non-terminal job, ascending by `job_id`.
    pub fn list_incomplete(&self) -> Result<Vec<CompactionJob>> {
        Ok(Self::read_all_in(&self.dir)?
            .into_iter()
            .filter(|job| !job.state.is_terminal())
            .collect())
    }

    /// Returns the non-terminal jobs a restart must finish, ascending by id.
    ///
    /// This is the resume-on-open source: [`crate::store::ColumnarStore::open`]
    /// reports its length without running any job; each entry is driven to
    /// completion by an explicit `resume_job` call.
    pub fn replay_incomplete(&self) -> Result<Vec<CompactionJob>> {
        self.list_incomplete()
    }

    fn job_path(&self, job_id: u64) -> PathBuf {
        Self::job_path_in(&self.dir, job_id)
    }

    fn job_path_in(dir: &Path, job_id: u64) -> PathBuf {
        dir.join(format!("{JOB_FILE_PREFIX}{job_id:020}{JOB_FILE_SUFFIX}"))
    }

    fn read_all_in(dir: &Path) -> Result<Vec<CompactionJob>> {
        let mut jobs = Vec::new();
        let entries = std::fs::read_dir(dir).map_err(PlomidError::from)?;
        for entry in entries {
            let entry = entry.map_err(PlomidError::from)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with(JOB_FILE_PREFIX) || !name.ends_with(JOB_FILE_SUFFIX) {
                continue;
            }
            let bytes = std::fs::read(entry.path()).map_err(PlomidError::from)?;
            jobs.push(decode_job(&bytes)?);
        }
        jobs.sort_unstable_by_key(|job| job.job_id);
        Ok(jobs)
    }

    fn transition(&self, job: &mut CompactionJob, next: JobState) -> Result<()> {
        job.state = next;
        job.updated_seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        self.persist(job)
    }

    /// Atomically persists one job record: `.tmp` + fsync + rename + dir sync.
    ///
    /// The directory-entry sync is Unix-only: Windows rejects opening a
    /// directory as a file, and NTFS journals the rename metadata itself.
    fn persist(&self, job: &CompactionJob) -> Result<()> {
        let final_path = self.job_path(job.job_id);
        let staged = final_path.with_extension("cjnl.tmp");
        let bytes = encode_job(job);
        std::fs::write(&staged, &bytes).map_err(PlomidError::from)?;
        // Windows requires a write handle for `sync_all` (`FlushFileBuffers`).
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&staged)
            .and_then(|file| file.sync_all())
            .map_err(PlomidError::from)?;
        std::fs::rename(&staged, &final_path).map_err(PlomidError::from)?;
        #[cfg(unix)]
        {
            std::fs::File::open(&self.dir)
                .and_then(|dir| dir.sync_all())
                .map_err(PlomidError::from)?;
        }
        Ok(())
    }
}

/// Rejects a milestone step taken from any state other than `expected`.
fn require_state(job: &CompactionJob, expected: JobState, what: &str) -> Result<()> {
    if job.state != expected {
        return Err(PlomidError::with_detail(
            ErrorKind::InvalidArgument,
            format!("cannot {what}: compaction job is not in the expected state"),
            format!(
                "job_id={} state={:?} expected={expected:?}",
                job.job_id, job.state
            ),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Length-prefixed little-endian record format (no new dependencies).
// ---------------------------------------------------------------------------

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn rest_len(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.rest_len() < len {
            return Err(corruption("compaction journal record is truncated"));
        }
        let slice = &self.bytes[self.position..self.position + len];
        self.position += len;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        let raw = self.take(4)?;
        Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let raw = self.take(8)?;
        let mut fixed = [0u8; 8];
        fixed.copy_from_slice(raw);
        Ok(u64::from_le_bytes(fixed))
    }
}

fn column_type_tag(column_type: ColumnType) -> u8 {
    match column_type {
        ColumnType::Null => 0,
        ColumnType::Integer => 1,
        ColumnType::Bytes => 2,
        ColumnType::String => 3,
        ColumnType::Bool => 4,
        ColumnType::I16 => 5,
        ColumnType::I32 => 6,
        ColumnType::F32 => 7,
        ColumnType::F64 => 8,
    }
}

fn column_type_from_tag(tag: u8) -> Result<ColumnType> {
    match tag {
        0 => Ok(ColumnType::Null),
        1 => Ok(ColumnType::Integer),
        2 => Ok(ColumnType::Bytes),
        3 => Ok(ColumnType::String),
        4 => Ok(ColumnType::Bool),
        5 => Ok(ColumnType::I16),
        6 => Ok(ColumnType::I32),
        7 => Ok(ColumnType::F32),
        8 => Ok(ColumnType::F64),
        _ => Err(corruption(
            "compaction journal names an unknown column type",
        )),
    }
}

fn encoding_tag(encoding: ColumnEncoding) -> u8 {
    match encoding {
        ColumnEncoding::Auto => 0,
        ColumnEncoding::Raw => 1,
        ColumnEncoding::Rle => 2,
        ColumnEncoding::DeltaBitpack => 3,
        ColumnEncoding::Dictionary => 4,
    }
}

fn encoding_from_tag(tag: u8) -> Result<ColumnEncoding> {
    match tag {
        0 => Ok(ColumnEncoding::Auto),
        1 => Ok(ColumnEncoding::Raw),
        2 => Ok(ColumnEncoding::Rle),
        3 => Ok(ColumnEncoding::DeltaBitpack),
        4 => Ok(ColumnEncoding::Dictionary),
        _ => Err(corruption("compaction journal names an unknown encoding")),
    }
}

fn policy_tag(policy: crate::encoding::ColumnEncodingPreference) -> u8 {
    match policy {
        crate::encoding::ColumnEncodingPreference::Auto => 0,
        crate::encoding::ColumnEncodingPreference::Raw => 1,
        crate::encoding::ColumnEncodingPreference::Rle => 2,
        crate::encoding::ColumnEncodingPreference::DeltaBitpack => 3,
        crate::encoding::ColumnEncodingPreference::Dictionary => 4,
    }
}

fn policy_from_tag(tag: u8) -> Result<crate::encoding::ColumnEncodingPreference> {
    match tag {
        0 => Ok(crate::encoding::ColumnEncodingPreference::Auto),
        1 => Ok(crate::encoding::ColumnEncodingPreference::Raw),
        2 => Ok(crate::encoding::ColumnEncodingPreference::Rle),
        3 => Ok(crate::encoding::ColumnEncodingPreference::DeltaBitpack),
        4 => Ok(crate::encoding::ColumnEncodingPreference::Dictionary),
        _ => Err(corruption(
            "compaction journal names an unknown column policy",
        )),
    }
}

fn compression_tag(compression: ChunkCompression) -> u8 {
    match compression {
        ChunkCompression::None => 0,
        ChunkCompression::Zstd => 1,
    }
}

fn compression_from_tag(tag: u8) -> Result<ChunkCompression> {
    match tag {
        0 => Ok(ChunkCompression::None),
        1 => Ok(ChunkCompression::Zstd),
        _ => Err(corruption(
            "compaction journal names an unknown compression",
        )),
    }
}

/// Encodes one job, appending a CRC32C over every preceding byte.
pub fn encode_job(job: &CompactionJob) -> Vec<u8> {
    let mut out = Writer::new();
    out.bytes.extend_from_slice(&JOURNAL_MAGIC);
    out.u32(JOURNAL_VERSION);
    out.u64(job.job_id);
    out.u8(job.state.tag());
    out.u64(job.created_seq);
    out.u64(job.updated_seq);
    out.u64(job.chunk_rows as u64);
    out.u64(job.table.database_id.get());
    out.u64(job.table.schema_id.get());
    out.u64(job.table.table_id.get());
    out.u64(job.plan.object_id.get());
    out.u64(job.plan.horizon);
    out.u8(u8::from(job.plan.inputs_releasable));
    out.u32(job.plan.inputs.len() as u32);
    for input in &job.plan.inputs {
        out.u64(input.generation.get());
        out.u8(u8::from(input.current));
    }
    out.u32(job.column_types.len() as u32);
    for column_type in &job.column_types {
        out.u8(column_type_tag(*column_type));
    }
    out.u8(encoding_tag(job.config.encoding));
    out.u8(compression_tag(job.config.compression));
    out.u64(job.config.chunk_target_size);
    out.u8(u8::from(job.config.collect_statistics));
    out.u8(u8::from(job.config.verify_checksums));
    out.u64(job.config.brin_rows_per_range);
    out.u8(u8::from(job.config.collect_pruning));
    out.u32(job.config.column_policies.len() as u32);
    for policy in &job.config.column_policies {
        out.u8(policy_tag(*policy));
    }
    out.u64(job.output_generation.get());
    out.u64(job.storage_generation.get());
    out.u64(job.checkpoint_lsn.as_u64());
    out.u32(job.chunks.len() as u32);
    for chunk in &job.chunks {
        out.u64(chunk.job_id);
        out.u32(chunk.chunk_idx);
        out.u64(chunk.rows_start);
        out.u64(chunk.rows_end);
        out.u8(u8::from(chunk.done));
    }
    let checksum = plomid_storage::compute_checksum(&out.bytes);
    out.u32(checksum);
    out.bytes
}

/// Decodes one job, rejecting torn records, version skew, and checksum drift.
pub fn decode_job(bytes: &[u8]) -> Result<CompactionJob> {
    if bytes.len() < JOURNAL_MAGIC.len() + 8 {
        return Err(corruption("compaction journal record is too short"));
    }
    let (body, tail) = bytes.split_at(bytes.len() - 4);
    let expected = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
    if plomid_storage::compute_checksum(body) != expected {
        return Err(corruption("compaction journal checksum mismatch"));
    }
    let mut read = Reader::new(body);
    let magic = read.take(JOURNAL_MAGIC.len())?;
    if magic != JOURNAL_MAGIC {
        return Err(corruption("compaction journal magic does not match"));
    }
    let version = read.u32()?;
    if version != 1 && version != JOURNAL_VERSION {
        return Err(corruption("compaction journal version is unsupported"));
    }
    let job_id = read.u64()?;
    let state = JobState::from_tag(read.u8()?)
        .ok_or_else(|| corruption("compaction journal names an unknown job state"))?;
    let created_seq = read.u64()?;
    let updated_seq = read.u64()?;
    let chunk_rows = read.u64()? as usize;
    if chunk_rows == 0 {
        return Err(corruption("compaction journal names a zero chunk size"));
    }
    let table = TableIdentity::new(
        DatabaseId::new(read.u64()?),
        SchemaId::new(read.u64()?),
        TableId::new(read.u64()?),
    );
    let object_id = ObjectId::new(read.u64()?);
    let horizon = read.u64()?;
    let inputs_releasable = read.u8()? != 0;
    let inputs_len = read.u32()? as usize;
    let mut inputs = Vec::with_capacity(inputs_len);
    for _ in 0..inputs_len {
        inputs.push(CompactionInput {
            generation: GenerationId::new(read.u64()?),
            current: read.u8()? != 0,
        });
    }
    let types_len = read.u32()? as usize;
    let mut column_types = Vec::with_capacity(types_len);
    for _ in 0..types_len {
        column_types.push(column_type_from_tag(read.u8()?)?);
    }
    let config = FlushConfig {
        encoding: encoding_from_tag(read.u8()?)?,
        compression: compression_from_tag(read.u8()?)?,
        chunk_target_size: read.u64()?,
        collect_statistics: read.u8()? != 0,
        verify_checksums: read.u8()? != 0,
        brin_rows_per_range: read.u64()?,
        collect_pruning: read.u8()? != 0,
        column_policies: if version >= 2 {
            let policies_len = read.u32()? as usize;
            let mut column_policies = Vec::with_capacity(policies_len.min(1 << 20));
            for _ in 0..policies_len {
                column_policies.push(policy_from_tag(read.u8()?)?);
            }
            column_policies
        } else {
            Vec::new()
        },
    };
    let output_generation = GenerationId::new(read.u64()?);
    let storage_generation = GenerationId::new(read.u64()?);
    let checkpoint_lsn = Lsn::new(read.u64()?);
    let chunks_len = read.u32()? as usize;
    let mut chunks = Vec::with_capacity(chunks_len);
    for _ in 0..chunks_len {
        chunks.push(CompactionChunk {
            job_id: read.u64()?,
            chunk_idx: read.u32()?,
            rows_start: read.u64()?,
            rows_end: read.u64()?,
            done: read.u8()? != 0,
        });
    }
    if read.rest_len() != 0 {
        return Err(corruption("compaction journal record has trailing bytes"));
    }
    for chunk in &chunks {
        if chunk.job_id != job_id || chunk.rows_start > chunk.rows_end {
            return Err(corruption(
                "compaction journal chunk disagrees with its job",
            ));
        }
    }
    Ok(CompactionJob {
        job_id,
        table,
        plan: CompactionPlan {
            identity: table,
            object_id,
            inputs,
            horizon,
            inputs_releasable,
        },
        state,
        created_seq,
        updated_seq,
        chunk_rows,
        chunks,
        column_types,
        config,
        output_generation,
        storage_generation,
        checkpoint_lsn,
    })
}

fn corruption(what: &str) -> PlomidError {
    PlomidError::new(ErrorKind::Corruption, what)
}

// ---------------------------------------------------------------------------
// Resume sub-steps: the same existing stages `compact` orchestrates
// (build → publish → release → reclaim), factored here so journaled resume
// reuses them without changing `compact` itself.
// ---------------------------------------------------------------------------

/// Builds, stages, verifies, syncs, and publishes one compacted generation
/// through the existing columnar flush path.
fn publish_compacted<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    request: &CompactionRequest,
    rows: &[Row],
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
        crate::store::ColumnarFailPoint::None,
    )
}

/// Releases input generations the live transaction state proves unnecessary.
///
/// Only generations the catalog still owns are released, so repeating the
/// step after a crash is a no-op rather than a second publication. The seal
/// is read before the active set (see [`release_provable`]); reversing the
/// order would make the rule vacuous.
fn release_provable_inputs<E: StorageEngine>(
    manager: &Arc<GenerationManager>,
    engine: &mut E,
    plan: &CompactionPlan,
    request: &CompactionRequest,
) -> Result<Vec<GenerationId>> {
    let seal = engine.txn_issue_mark()?;
    let (fresh_active, _) = mvcc_safety(engine)?;
    if !release_provable(&fresh_active, seal) {
        return Ok(Vec::new());
    }
    let owned = manager.load()?;
    let inputs: Vec<GenerationId> = plan
        .generation_ids()
        .into_iter()
        .filter(|generation| owned.owns_generation(*generation))
        .collect();
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let release_request = plomid_storage::PublicationRequest::release(
        request.storage_generation,
        request.checkpoint_lsn,
        inputs.clone(),
    )?;
    manager.publish(release_request)?;
    Ok(inputs)
}

/// Runs one reclamation pass plus sharing-safe slice deletion.
///
/// Slice deletion reuses [`delete_unreferenced_slices`], whose per-segment
/// deletion reports an absent manifest instead of failing, so repeating the
/// step is safe.
fn reclaim_compacted<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    doomed_segments: &std::collections::BTreeMap<GenerationId, Vec<plomid_core::SegmentId>>,
    reclaim: bool,
) -> Result<(
    Option<plomid_storage::GcOutcome>,
    Vec<plomid_core::SegmentId>,
    u64,
)> {
    let reclaimed = if reclaim {
        Some(manager.gc_with_fail_point(plomid_storage::GcFailPoint::None)?)
    } else {
        None
    };
    let (reclaimed_segments, reclaimed_segment_bytes) = match &reclaimed {
        Some(outcome) => {
            delete_unreferenced_slices(store, engine, manager, &outcome.reclaimed, doomed_segments)
        }
        None => (Vec::new(), 0),
    };
    Ok((reclaimed, reclaimed_segments, reclaimed_segment_bytes))
}

/// Reads back the durably retained inputs, ascending.
fn retained_inputs_of(
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
/// Generations the catalog no longer owns were converged by an earlier
/// attempt and report zero without being read (their metadata may already be
/// reclaimed); owned generations must decode, and a failure there aborts the
/// resume exactly as it aborts a fresh run.
fn measured_input_rows<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    manager: &Arc<GenerationManager>,
    object_id: ObjectId,
    plan: &CompactionPlan,
) -> Result<Vec<(GenerationId, u64)>> {
    let owned = manager.load()?;
    let mut input_rows = Vec::with_capacity(plan.inputs.len());
    for input in &plan.inputs {
        if !owned.owns_generation(input.generation) {
            input_rows.push((input.generation, 0));
            continue;
        }
        let decoded = read_generation_rows(store, engine, manager, object_id, input.generation)?;
        input_rows.push((input.generation, decoded.len() as u64));
    }
    Ok(input_rows)
}

// ---------------------------------------------------------------------------
// Resume: drives one journaled job to completion through the existing
// compaction sub-steps (build → publish → release → reclaim).
// ---------------------------------------------------------------------------

/// Validates one row range through the existing in-memory flush pipeline.
///
/// The output is discarded: the final publication rebuilds the whole image
/// through the same code, so this step proves encodability chunk by chunk and
/// banks resumable progress without inventing a second build path.
fn validate_chunk(
    rows: &[Row],
    column_types: &[ColumnType],
    config: &FlushConfig,
    output_generation: GenerationId,
) -> Result<()> {
    let _ = crate::flush::flush(
        rows,
        column_types,
        output_generation,
        SegmentId::new(u64::MAX),
        config,
    )?;
    Ok(())
}

impl crate::store::ColumnarStore {
    /// Lists the jobs a restart must finish (logged, not run, at open).
    pub fn pending_compaction_jobs(&self) -> Result<Vec<CompactionJob>> {
        self.journal().replay_incomplete()
    }

    /// Drives one journaled job to completion.
    ///
    /// `rows` is the visible row state for the job's table, covering exactly
    /// the row ranges the job's chunks describe (a job without planned chunks
    /// has them planned from `rows.len()`). The run is incremental: only
    /// pending chunks are build-validated, and every milestone is journaled,
    /// so interrupting and re-calling resumes instead of restarting:
    ///
    /// * `Built` + output generation already current → publish skipped
    ///   (re-publish would be rejected as already-published);
    /// * release only touches input generations the catalog still owns;
    /// * reclamation reuses the idempotent slice-deletion path.
    ///
    /// A [`JobState::Reclaimed`] job returns a no-op success; an aborted or
    /// unknown job is rejected. With `reclaim == false` the job stops after
    /// the release step and stays resumable.
    ///
    /// `active_transactions` / `last_committed` describe the caller's snapshot
    /// context for diagnostics; the release decision itself is re-evaluated
    /// authoritatively from live engine state, exactly as [`compact`](crate::compaction::compact)
    /// does, so a stale caller view can never force a release.
    pub fn resume_job<E: StorageEngine>(
        &self,
        engine: &mut E,
        job_id: u64,
        rows: &[Row],
        _active_transactions: &[u64],
        _last_committed: u64,
        reclaim: bool,
    ) -> Result<CompactionOutcome> {
        let journal = self.journal();
        let manager = self.generations();
        let mut job = journal.get(job_id)?;
        if job.state == JobState::Reclaimed {
            let retained_inputs = retained_inputs_of(manager, &job.plan)?;
            return Ok(CompactionOutcome {
                plan: job.plan.clone(),
                published: None,
                released: Vec::new(),
                reclaimed: None,
                reclaimed_segments: Vec::new(),
                reclaimed_segment_bytes: 0,
                visible_rows: rows.len(),
                output_rows: rows.len(),
                retained_inputs,
                input_rows: Vec::new(),
            });
        }
        if job.state.is_terminal() {
            return Err(PlomidError::with_detail(
                ErrorKind::InvalidArgument,
                "compaction job is aborted and cannot resume",
                format!("job_id={job_id}"),
            ));
        }
        let request = job.request();

        // Incremental build: plan chunks on first resume, then validate only
        // the chunks still pending. Done chunks are never reprocessed.
        if job.state == JobState::Planned {
            journal.plan_chunks(job_id, rows.len())?;
            job = journal.get(job_id)?;
        }
        if job.chunks_end() != rows.len() as u64 {
            return Err(PlomidError::with_detail(
                ErrorKind::InvalidArgument,
                "resume rows do not cover the job's planned chunks",
                format!(
                    "job_id={job_id} rows={} chunks_end={}",
                    rows.len(),
                    job.chunks_end()
                ),
            ));
        }
        while let Some(chunk) = journal.next_pending_chunk(job_id)? {
            let start = chunk.rows_start as usize;
            let end = chunk.rows_end.min(rows.len() as u64) as usize;
            validate_chunk(
                &rows[start..end],
                &job.column_types,
                &job.config,
                job.output_generation,
            )?;
            journal.mark_chunk_done(job_id, chunk.chunk_idx)?;
        }
        if journal.get(job_id)?.state == JobState::Building {
            journal.mark_built(job_id)?;
        }
        job = journal.get(job_id)?;

        // Idempotent publish: the output is published exactly once. A job in
        // Built whose output is not yet current publishes now; every other
        // case skips (crashed after publish, or superseded since). A second
        // publication of the same generation identity would be rejected as
        // already-published at best and regress currency at worst, so the
        // step is skipped instead of repeated.
        let published_current = manager
            .load()?
            .object(request.object_id)
            .map(|record| record.current_generation == request.generation)
            .unwrap_or(false);
        let published = if job.state == JobState::Built && !published_current {
            let outcome = publish_compacted(self, engine, &request, rows)?;
            journal.mark_published(job_id)?;
            Some(outcome)
        } else {
            if job.state == JobState::Built {
                journal.mark_published(job_id)?;
            }
            None
        };
        job = journal.get(job_id)?;

        // Release covers the job's own planned inputs (selected before the
        // output published, as in `compact`): the output generation itself is
        // never a release candidate. Only still-owned inputs are released, so
        // a retry after a crash publishes nothing new.
        let plan = job.plan.clone();
        let doomed_segments = slice_ownership_snapshot(manager);
        let released = release_provable_inputs(manager, engine, &plan, &request)?;
        journal.mark_released(job_id)?;

        let (reclaimed, reclaimed_segments, reclaimed_segment_bytes) =
            reclaim_compacted(self, engine, manager, &doomed_segments, reclaim)?;
        if reclaim {
            journal.mark_reclaimed(job_id)?;
        }

        let retained_inputs = retained_inputs_of(manager, &plan)?;
        let input_rows = measured_input_rows(self, engine, manager, request.object_id, &plan)?;
        let _ = job;
        Ok(CompactionOutcome {
            plan,
            published,
            released,
            reclaimed,
            reclaimed_segments,
            reclaimed_segment_bytes,
            visible_rows: rows.len(),
            output_rows: rows.len(),
            retained_inputs,
            input_rows,
        })
    }
}
