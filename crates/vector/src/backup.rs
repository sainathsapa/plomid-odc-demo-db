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
#![forbid(unsafe_code)]
//! Single-node vector backup and restore (Phase 48).
//!
//! NON-AUTHORITATIVE legacy file backup (see [`crate::durability`]).
//! System backup/restore authority lives in the storage-engine
//! generation/checkpoint path. This module captures the legacy file store
//! plus derived index blobs for offline tests only: rows are re-validated
//! authoritative copies, index blobs are explicitly rebuildable derived
//! artifacts (direct blobs serve only after decode + validate, everything
//! else rebuilds), and it is never consulted by the production SQL path.
//!
//! Production-grade backup for one machine: a consistent, checksummed,
//! versioned backup directory that restores to a valid, queryable,
//! mutable state without silently losing vector correctness.
//!
//! Consistency model: the backup captures **authoritative data first**
//! (every row re-read through [`crate::storage::VectorStorageAdapter`]
//! and re-validated by Self-describing codec CRCs), then table metadata,
//! statistics (derived — safe to drop), and per-index descriptors plus
//! either a validated persist blob or an explicit rebuild marker.
//! Derived structures are never trusted over authoritative rows: an index
//! that cannot prove itself (decode + [`crate::index::VectorIndex::validate`])
//! is rebuilt via [`crate::index::online_build::run_online_build`], never served.
//!
//! MVCC boundary: the caller supplies `checkpoint_lsn` (quiesced store or
//! held snapshot). Rows at/below it are captured; deltas above it are the
//! caller's (executor `CommittedDelta`) responsibility — the restore
//! report returns `manifest_lsn` for exactly that catch-up. WAL replay
//! itself stays in `plomid-wal`; this module reuses the same publication
//! pattern (stage → verify → sync → atomic rename → dir sync).
//!
//! Single node only. No replication, no cluster, no distributed work.

use crate::codec::{crc32c, decode_standalone};
use crate::index::IndexKind;
use crate::r#type::VectorMetric;
use crate::storage::{VectorStorageAdapter, VectorStoragePolicy};
use crate::{Result, VectorError};

/// Backup container magic: ASCII `PLMB`.
pub const BACKUP_MAGIC: [u8; 4] = *b"PLMB";
/// Backup format version. Bump on any layout change; older readers fail
/// closed instead of reinterpreting bytes. Reuses the existing
/// version-gate convention (no duplicate migration system).
pub const BACKUP_FORMAT_VERSION: u32 = 1;
/// Engine version this backup was written by.
pub const BACKUP_ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Backup manifest file name inside a backup directory.
pub const BACKUP_MANIFEST_FILE: &str = "BACKUP_MANIFEST";
/// Records file name shared with the file store layout.
pub const RECORDS_FILE: &str = "records.dat";
const MANIFEST_FILE: &str = "MANIFEST";
const STATS_FILE: &str = "stats.bin";
const INDEX_DIR: &str = "indexes";

/// Index-kind code in the backup manifest (strict; unknown fails closed).
fn kind_code(kind: IndexKind) -> u8 {
    match kind {
        IndexKind::Flat => 1,
        IndexKind::Hnsw => 2,
        IndexKind::IvfFlat => 3,
        IndexKind::IvfSq => 4,
        IndexKind::IvfPq => 5,
        IndexKind::DiskAnn => 6,
        IndexKind::SparseInverted => 7,
        IndexKind::Binary => 8,
    }
}

fn kind_from_code(code: u8) -> Result<IndexKind> {
    match code {
        1 => Ok(IndexKind::Flat),
        2 => Ok(IndexKind::Hnsw),
        3 => Ok(IndexKind::IvfFlat),
        4 => Ok(IndexKind::IvfSq),
        5 => Ok(IndexKind::IvfPq),
        6 => Ok(IndexKind::DiskAnn),
        7 => Ok(IndexKind::SparseInverted),
        8 => Ok(IndexKind::Binary),
        other => Err(VectorError::IndexCorrupted(format!(
            "unknown backup index kind {other}"
        ))),
    }
}

fn metric_code(metric: VectorMetric) -> u8 {
    match metric {
        VectorMetric::Cosine => 1,
        VectorMetric::L2 => 2,
        VectorMetric::Dot => 3,
        VectorMetric::Hamming => 4,
        VectorMetric::Jaccard => 5,
    }
}

fn metric_from_code(code: u8) -> Result<VectorMetric> {
    match code {
        1 => Ok(VectorMetric::Cosine),
        2 => Ok(VectorMetric::L2),
        3 => Ok(VectorMetric::Dot),
        4 => Ok(VectorMetric::Hamming),
        5 => Ok(VectorMetric::Jaccard),
        other => Err(VectorError::IndexCorrupted(format!(
            "unknown backup metric {other}"
        ))),
    }
}

/// How one index is captured in (or restored from) a backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRestoreMode {
    /// Validated persist bytes restore directly (decode + validate on restore).
    Direct,
    /// No usable persist path (or tombstones the encoding drops): restore
    /// rebuilds from authoritative rows; never served before rebuild.
    Rebuild,
    /// DiskANN file pair (`vectors.dat` + `graph.dat`) copied as a pair.
    DiskAnnPair,
}

impl IndexRestoreMode {
    const fn code(self) -> u8 {
        match self {
            Self::Direct => 1,
            Self::Rebuild => 2,
            Self::DiskAnnPair => 3,
        }
    }

    fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Direct),
            2 => Ok(Self::Rebuild),
            3 => Ok(Self::DiskAnnPair),
            other => Err(VectorError::IndexCorrupted(format!(
                "unknown backup index mode {other}"
            ))),
        }
    }
}

/// Decide the capture mode for one index.
///
/// * `Flat` / `SparseInverted`: no persist path — always rebuild.
/// * `Hnsw` / `Binary`: encodings preserve tombstones — direct eligible.
/// * `IvfFlat` / `IvfSq` / `IvfPq`: `decode` drops tombstones, so direct
///   is eligible only when nothing is tombstoned; otherwise rebuild from
///   authoritative rows (rebuilding is safer than resurrecting deletes).
/// * `DiskAnn`: durable file pair — pair copy (validated on restore via
///   `open`/`recover`; `RebuildRequired` becomes rebuild).
#[must_use]
pub const fn capture_mode(kind: IndexKind, deleted_count: u64) -> IndexRestoreMode {
    match kind {
        IndexKind::Flat | IndexKind::SparseInverted => IndexRestoreMode::Rebuild,
        IndexKind::Hnsw | IndexKind::Binary => IndexRestoreMode::Direct,
        IndexKind::IvfFlat | IndexKind::IvfSq | IndexKind::IvfPq => {
            if deleted_count == 0 {
                IndexRestoreMode::Direct
            } else {
                IndexRestoreMode::Rebuild
            }
        }
        IndexKind::DiskAnn => IndexRestoreMode::DiskAnnPair,
    }
}

/// One index as supplied to [`create_backup`].
#[derive(Debug, Clone)]
pub struct IndexBackupInput {
    /// Index name (file-safe, `1..=128` chars).
    pub name: String,
    /// Index kind.
    pub kind: IndexKind,
    /// Distance metric.
    pub metric: VectorMetric,
    /// Indexed dimension.
    pub dimension: usize,
    /// Mutation generation at backup time.
    pub generation: u64,
    /// Total entries (including tombstones).
    pub vector_count: u64,
    /// Tombstoned entries.
    pub deleted_count: u64,
    /// `encode()` bytes for direct-eligible kinds (`None` ⇒ rebuild).
    pub persist_blob: Option<Vec<u8>>,
    /// DiskANN pair contents (`vectors.dat`, `graph.dat`) for pair mode.
    pub diskann_pair: Option<(Vec<u8>, Vec<u8>)>,
}

impl IndexBackupInput {
    /// Validate identity fields before any I/O (fail fast, never partial).
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() || self.name.len() > 128 {
            return Err(VectorError::IndexParameterInvalid(
                "backup index name must be 1..=128 chars".to_string(),
            ));
        }
        if self.name.contains('/') || self.name.contains('\0') {
            return Err(VectorError::IndexParameterInvalid(
                "backup index name must be file-safe".to_string(),
            ));
        }
        if self.dimension == 0 || self.dimension > crate::index::common::MAX_DIMENSION {
            return Err(VectorError::IndexParameterInvalid(format!(
                "backup index dimension {} out of range",
                self.dimension
            )));
        }
        Ok(())
    }
}

/// Failure-injection points for backup (explicit parameter, mirroring the
/// `TestInject`/`FailPoint` style of `plomid-wal`/`plomid-storage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackupFailPoint {
    /// No injection.
    #[default]
    None,
    /// Fail before any byte is copied.
    BeforeDataCopy,
    /// Fail after `n` rows are copied (torn backup, must stay incomplete).
    DuringDataCopy(u64),
    /// Fail while copying index blobs.
    DuringIndexCopy,
    /// Fail while checksumming/finalizing the manifest.
    DuringFinalize,
}

/// Failure-injection points for restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestoreFailPoint {
    /// No injection.
    #[default]
    None,
    /// Fail before any restore I/O.
    BeforeStart,
    /// Fail after `n` rows are restored.
    DuringDataRestore(u64),
    /// Fail while restoring index blobs.
    DuringIndexRestore,
    /// Fail before finalization (target must not look healthy).
    DuringFinalize,
}

/// One file recorded in the backup manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupFileEntry {
    /// Path relative to the backup directory.
    pub name: String,
    /// Byte length.
    pub len: u64,
    /// `crc32c` of the file bytes.
    pub crc: u32,
}

/// Per-index descriptor recorded in the backup manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupIndexEntry {
    /// Index name.
    pub name: String,
    /// Index kind.
    pub kind: IndexKind,
    /// Distance metric.
    pub metric: VectorMetric,
    /// Indexed dimension.
    pub dimension: u64,
    /// Mutation generation.
    pub generation: u64,
    /// Total entries (including tombstones).
    pub vector_count: u64,
    /// Tombstoned entries.
    pub deleted_count: u64,
    /// Capture mode.
    pub mode: IndexRestoreMode,
    /// Blob file (relative path; empty for rebuild markers).
    pub blob_file: String,
    /// Blob CRC (`0` for rebuild markers).
    pub blob_crc: u32,
    /// Blob length (`0` for rebuild markers).
    pub blob_len: u64,
}

/// Parsed backup manifest (the consistency contract of one backup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    /// Must equal [`BACKUP_FORMAT_VERSION`].
    pub format_version: u32,
    /// Writer engine version (informational; format gates compatibility).
    pub engine_version: String,
    /// Checkpoint LSN the snapshot was taken at (MVCC boundary).
    pub checkpoint_lsn: u64,
    /// Rows captured.
    pub row_count: u64,
    /// Dense dimension (`0` = mixed/not enforced).
    pub dims: u32,
    /// Source table identity.
    pub table: String,
    /// Source column identity.
    pub column: String,
    /// Storage mode code at backup time (`0` when unknown).
    pub storage_mode: u8,
    /// Statistics present (`stats.bin`).
    pub has_stats: bool,
    /// Recorded files with lengths + CRCs.
    pub files: Vec<BackupFileEntry>,
    /// Recorded indexes.
    pub indexes: Vec<BackupIndexEntry>,
}

impl BackupManifest {
    /// Serialize with explicit LE framing + trailing CRC (fail-closed).
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut body = Vec::new();
        enc_u32(&mut body, self.format_version);
        enc_str(&mut body, &self.engine_version);
        enc_u64(&mut body, self.checkpoint_lsn);
        enc_u64(&mut body, self.row_count);
        enc_u32(&mut body, self.dims);
        enc_str(&mut body, &self.table);
        enc_str(&mut body, &self.column);
        body.push(self.storage_mode);
        body.push(u8::from(self.has_stats));
        enc_u32(&mut body, self.files.len() as u32);
        for f in &self.files {
            enc_str(&mut body, &f.name);
            enc_u64(&mut body, f.len);
            enc_u32(&mut body, f.crc);
        }
        enc_u32(&mut body, self.indexes.len() as u32);
        for i in &self.indexes {
            enc_str(&mut body, &i.name);
            body.push(kind_code(i.kind));
            body.push(metric_code(i.metric));
            enc_u64(&mut body, i.dimension);
            enc_u64(&mut body, i.generation);
            enc_u64(&mut body, i.vector_count);
            enc_u64(&mut body, i.deleted_count);
            body.push(i.mode.code());
            enc_str(&mut body, &i.blob_file);
            enc_u32(&mut body, i.blob_crc);
            enc_u64(&mut body, i.blob_len);
        }
        let mut out = Vec::with_capacity(4 + 4 + 4 + 8 + body.len());
        out.extend_from_slice(&BACKUP_MAGIC);
        out.extend_from_slice(&BACKUP_FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&crc32c(&body).to_le_bytes());
        out.extend_from_slice(&(body.len() as u64).to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Parse [`Self::to_bytes`] output; version skew, truncation, and CRC
    /// mismatch fail closed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 20 {
            return Err(VectorError::IndexCorrupted(
                "backup manifest truncated".to_string(),
            ));
        }
        if bytes[0..4] != BACKUP_MAGIC {
            return Err(VectorError::IndexCorrupted("bad backup magic".to_string()));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or([0; 4]));
        if version != BACKUP_FORMAT_VERSION {
            return Err(VectorError::IndexCorrupted(format!(
                "unsupported backup format version {version}"
            )));
        }
        let want_crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0; 4]));
        let body_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap_or([0; 8])) as usize;
        if bytes.len() != 20 + body_len {
            return Err(VectorError::IndexCorrupted(
                "backup manifest length mismatch".to_string(),
            ));
        }
        let body = &bytes[20..];
        if crc32c(body) != want_crc {
            return Err(VectorError::IndexCorrupted(
                "backup manifest CRC mismatch".to_string(),
            ));
        }
        let mut d = Cursor {
            bytes: body,
            off: 0,
        };
        let format_version = d.u32()?;
        if format_version != BACKUP_FORMAT_VERSION {
            return Err(VectorError::IndexCorrupted(format!(
                "unsupported backup body version {format_version}"
            )));
        }
        let engine_version = d.str()?;
        let checkpoint_lsn = d.u64()?;
        let row_count = d.u64()?;
        let dims = d.u32()?;
        let table = d.str()?;
        let column = d.str()?;
        let storage_mode = d.u8()?;
        let has_stats = d.u8()? != 0;
        let mut files = Vec::new();
        for _ in 0..d.u32()? {
            files.push(BackupFileEntry {
                name: d.str()?,
                len: d.u64()?,
                crc: d.u32()?,
            });
        }
        let mut indexes = Vec::new();
        for _ in 0..d.u32()? {
            let name = d.str()?;
            let kind = kind_from_code(d.u8()?)?;
            let metric = metric_from_code(d.u8()?)?;
            indexes.push(BackupIndexEntry {
                name,
                kind,
                metric,
                dimension: d.u64()?,
                generation: d.u64()?,
                vector_count: d.u64()?,
                deleted_count: d.u64()?,
                mode: IndexRestoreMode::from_code(d.u8()?)?,
                blob_file: d.str()?,
                blob_crc: d.u32()?,
                blob_len: d.u64()?,
            });
        }
        d.finish()?;
        Ok(Self {
            format_version,
            engine_version,
            checkpoint_lsn,
            row_count,
            dims,
            table,
            column,
            storage_mode,
            has_stats,
            files,
            indexes,
        })
    }
}

fn enc_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn enc_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn enc_str(out: &mut Vec<u8>, s: &str) {
    enc_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    off: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.bytes.len() < self.off + n {
            return Err(VectorError::IndexCorrupted(
                "backup manifest body truncated".to_string(),
            ));
        }
        let s = &self.bytes[self.off..self.off + n];
        self.off += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().unwrap_or([0; 4]),
        ))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().unwrap_or([0; 8]),
        ))
    }
    fn str(&mut self) -> Result<String> {
        let len = self.u32()? as usize;
        if len > 4096 {
            return Err(VectorError::IndexCorrupted(
                "backup manifest string too long".to_string(),
            ));
        }
        let raw = self.take(len)?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| VectorError::IndexCorrupted("backup manifest not utf-8".to_string()))
    }
    fn finish(&self) -> Result<()> {
        if self.off != self.bytes.len() {
            return Err(VectorError::IndexCorrupted(
                "backup manifest trailing bytes".to_string(),
            ));
        }
        Ok(())
    }
}

/// Outcome of [`create_backup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupReport {
    /// Rows captured.
    pub rows: u64,
    /// Indexes recorded.
    pub indexes: u64,
    /// Indexes marked rebuild (no direct blob).
    pub rebuild_marked: u64,
    /// Checkpoint LSN the snapshot was taken at.
    pub checkpoint_lsn: u64,
    /// Total backup bytes written.
    pub bytes: u64,
}

/// Create a consistent backup of one vector column store.
///
/// Reads every row through `source` (each payload re-validated by codec
/// CRC — a corrupt source row aborts the backup, never a corrupt backup),
/// copies the store `MANIFEST` when the source is a
/// [`crate::storage::FileVectorStorage`] directory (`store_dir`), and
/// records caller-supplied index inputs plus optional `stats` bytes.
/// The manifest publishes LAST (temp + rename + dir sync): a backup
/// without a valid `BACKUP_MANIFEST` is incomplete and restore refuses it.
#[allow(clippy::too_many_arguments)]
pub fn create_backup<S: VectorStorageAdapter>(
    source: &S,
    store_dir: Option<&std::path::Path>,
    table: &str,
    column: &str,
    checkpoint_lsn: u64,
    stats: Option<&[u8]>,
    indexes: &[IndexBackupInput],
    storage_mode: u8,
    out_dir: &std::path::Path,
    fail: BackupFailPoint,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> Result<BackupReport> {
    if table.trim().is_empty() || column.trim().is_empty() {
        return Err(VectorError::IndexParameterInvalid(
            "backup table/column must be non-empty".to_string(),
        ));
    }
    for input in indexes {
        input.validate()?;
    }
    if fail == BackupFailPoint::BeforeDataCopy {
        return Err(VectorError::IndexCorrupted(
            "injected backup failure before data copy".to_string(),
        ));
    }
    std::fs::create_dir_all(out_dir).map_err(io_fail)?;
    std::fs::create_dir_all(out_dir.join(INDEX_DIR)).map_err(io_fail)?;

    // Authoritative rows first: scan + re-validate each payload.
    let row_count = source.row_count();
    let records_path = out_dir.join(RECORDS_FILE);
    let mut bytes_written = 0u64;
    {
        let mut f = std::fs::File::create(&records_path).map_err(io_fail)?;
        use std::io::Write as _;
        let mut copied = 0u64;
        let mut start = 0u64;
        while start < row_count {
            if let BackupFailPoint::DuringDataCopy(n) = fail {
                if copied >= n {
                    drop(f);
                    return Err(VectorError::IndexCorrupted(format!(
                        "injected backup failure after {copied} rows"
                    )));
                }
            }
            let batch = source.scan_batch(start, 1024)?;
            if batch.is_empty() {
                break;
            }
            for value in batch.iter().map(|(_, v)| v) {
                if let BackupFailPoint::DuringDataCopy(n) = fail {
                    if copied >= n {
                        drop(f);
                        return Err(VectorError::IndexCorrupted(format!(
                            "injected backup failure after {copied} rows"
                        )));
                    }
                }
                let raw = source.encode_payload(value)?;
                // Re-validate: what we write must decode (never a corrupt backup).
                let _ = decode_standalone(&raw)?;
                f.write_all(&(raw.len() as u32).to_le_bytes())
                    .map_err(io_fail)?;
                f.write_all(&raw).map_err(io_fail)?;
                bytes_written += 4 + raw.len() as u64;
                copied += 1;
            }
            start += batch.len() as u64;
        }
        f.flush().map_err(io_fail)?;
        f.sync_all().map_err(io_fail)?;
    }
    if fail == BackupFailPoint::DuringIndexCopy {
        return Err(VectorError::IndexCorrupted(
            "injected backup failure during index copy".to_string(),
        ));
    }

    // Index blobs (validated CRC envelope comes free from the manifest).
    let mut index_entries = Vec::with_capacity(indexes.len());
    let mut rebuild_marked = 0u64;
    for input in indexes {
        let mode = capture_mode(input.kind, input.deleted_count);
        let (blob_file, blob_crc, blob_len) = match mode {
            IndexRestoreMode::Rebuild => {
                rebuild_marked += 1;
                (String::new(), 0, 0)
            }
            IndexRestoreMode::Direct => {
                let blob = input.persist_blob.as_ref().ok_or_else(|| {
                    VectorError::IndexParameterInvalid(format!(
                        "index {} marked direct but no persist blob supplied",
                        input.name
                    ))
                })?;
                let name = format!("{}/{}.blob", INDEX_DIR, input.name);
                let crc = crc32c(blob);
                std::fs::write(out_dir.join(&name), blob).map_err(io_fail)?;
                sync_file(&out_dir.join(&name))?;
                bytes_written += blob.len() as u64;
                (name, crc, blob.len() as u64)
            }
            IndexRestoreMode::DiskAnnPair => {
                let (vectors, graph) = input.diskann_pair.as_ref().ok_or_else(|| {
                    VectorError::IndexParameterInvalid(format!(
                        "diskann index {} missing file pair",
                        input.name
                    ))
                })?;
                let vname = format!("{}/{}.vectors.dat", INDEX_DIR, input.name);
                let gname = format!("{}/{}.graph.dat", INDEX_DIR, input.name);
                std::fs::write(out_dir.join(&vname), vectors).map_err(io_fail)?;
                std::fs::write(out_dir.join(&gname), graph).map_err(io_fail)?;
                sync_file(&out_dir.join(&vname))?;
                sync_file(&out_dir.join(&gname))?;
                // Pair CRC covers both files in order.
                let mut pair = vectors.clone();
                pair.extend_from_slice(graph);
                let crc = crc32c(&pair);
                bytes_written += (vectors.len() + graph.len()) as u64;
                (
                    format!("{vname}|{gname}"),
                    crc,
                    (vectors.len() + graph.len()) as u64,
                )
            }
        };
        index_entries.push(BackupIndexEntry {
            name: input.name.clone(),
            kind: input.kind,
            metric: input.metric,
            dimension: input.dimension as u64,
            generation: input.generation,
            vector_count: input.vector_count,
            deleted_count: input.deleted_count,
            mode,
            blob_file,
            blob_crc,
            blob_len,
        });
    }

    // Store manifest copy (when file-backed) + stats blob.
    if let Some(dir) = store_dir {
        let src = dir.join(MANIFEST_FILE);
        if src.exists() {
            let bytes = std::fs::read(&src).map_err(io_fail)?;
            // Validate before copying (never back up a corrupt manifest).
            let _ = crate::storage::VectorManifest::from_bytes(&bytes)?;
            std::fs::write(out_dir.join(MANIFEST_FILE), &bytes).map_err(io_fail)?;
            sync_file(&out_dir.join(MANIFEST_FILE))?;
            bytes_written += bytes.len() as u64;
        }
    }
    let has_stats = stats.is_some();
    if let Some(s) = stats {
        // Stats are derived: validate when possible, drop when corrupt.
        let _ = crate::stats::VectorStats::decode(s);
        std::fs::write(out_dir.join(STATS_FILE), s).map_err(io_fail)?;
        sync_file(&out_dir.join(STATS_FILE))?;
        bytes_written += s.len() as u64;
    }

    if fail == BackupFailPoint::DuringFinalize {
        return Err(VectorError::IndexCorrupted(
            "injected backup failure during finalize".to_string(),
        ));
    }

    // File table + manifest publish LAST (incomplete backups have no manifest).
    let mut files = Vec::new();
    for name in [RECORDS_FILE, MANIFEST_FILE, STATS_FILE] {
        let p = out_dir.join(name);
        if p.exists() {
            let b = std::fs::read(&p).map_err(io_fail)?;
            files.push(BackupFileEntry {
                name: name.to_string(),
                len: b.len() as u64,
                crc: crc32c(&b),
            });
        }
    }
    let manifest = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        engine_version: BACKUP_ENGINE_VERSION.to_string(),
        checkpoint_lsn,
        row_count,
        dims: source.dims().unwrap_or(0),
        table: table.to_string(),
        column: column.to_string(),
        storage_mode,
        has_stats,
        files,
        indexes: index_entries,
    };
    let tmp = out_dir.join(format!("{BACKUP_MANIFEST_FILE}.tmp"));
    std::fs::write(&tmp, manifest.to_bytes()).map_err(io_fail)?;
    sync_file(&tmp)?;
    std::fs::rename(&tmp, out_dir.join(BACKUP_MANIFEST_FILE)).map_err(io_fail)?;
    sync_dir(out_dir)?;
    if let Some(m) = metrics {
        m.observe_backup();
    }
    Ok(BackupReport {
        rows: row_count,
        indexes: indexes.len() as u64,
        rebuild_marked,
        checkpoint_lsn,
        bytes: bytes_written,
    })
}

/// Per-index restore action decided by [`restore_backup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRestoreAction {
    /// Index name.
    pub name: String,
    /// Whether the caller may serve this index directly or must rebuild.
    pub action: RestoredIndexAction,
    /// Human-readable reason.
    pub reason: String,
}

/// Direct-serve vs rebuild decision for one restored index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoredIndexAction {
    /// Blob decoded + validated: may serve after open.
    Direct,
    /// Must rebuild from authoritative rows before serving.
    RebuildRequired,
}

/// Outcome of [`restore_backup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReport {
    /// Rows restored.
    pub rows: u64,
    /// Manifest checkpoint LSN (MVCC catch-up boundary for deltas above it).
    pub manifest_lsn: u64,
    /// Whether the writer engine version matches this binary.
    pub engine_version_match: bool,
    /// Statistics restored (false when absent or corrupt — derived, safe).
    pub stats_restored: bool,
    /// Per-index actions.
    pub indexes: Vec<IndexRestoreAction>,
}

/// Restore a backup into `target_dir` (must not contain a live store).
///
/// Validates the manifest envelope first (magic/version/CRC/trailing
/// bytes), then every recorded file (presence/length/CRC), then every
/// row payload (codec CRC). Index blobs are staged for the caller and
/// classified direct-vs-rebuild; **a failed restore never leaves a
/// healthy-looking target**: failure before finalize removes the staged
/// `MANIFEST`, so `open` fails closed on retry.
pub fn restore_backup(
    backup_dir: &std::path::Path,
    target_dir: &std::path::Path,
    fail: RestoreFailPoint,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> Result<RestoreReport> {
    if fail == RestoreFailPoint::BeforeStart {
        return Err(VectorError::IndexCorrupted(
            "injected restore failure before start".to_string(),
        ));
    }
    if target_dir.join(RECORDS_FILE).exists() || target_dir.join(MANIFEST_FILE).exists() {
        return Err(VectorError::IndexStateInvalid(
            "restore target already contains a vector store".to_string(),
        ));
    }
    let manifest_bytes = std::fs::read(backup_dir.join(BACKUP_MANIFEST_FILE)).map_err(|e| {
        VectorError::IndexCorrupted(format!("backup manifest missing/unreadable: {e}"))
    })?;
    let manifest = BackupManifest::from_bytes(&manifest_bytes)?;
    if manifest.table.trim().is_empty() {
        return Err(VectorError::IndexCorrupted(
            "backup manifest has empty table".to_string(),
        ));
    }

    std::fs::create_dir_all(target_dir).map_err(io_fail)?;
    std::fs::create_dir_all(target_dir.join(INDEX_DIR)).map_err(io_fail)?;

    // Every recorded file: presence + length + CRC.
    for entry in &manifest.files {
        let bytes = std::fs::read(backup_dir.join(&entry.name)).map_err(|e| {
            VectorError::IndexCorrupted(format!("backup file {} missing: {e}", entry.name))
        })?;
        if bytes.len() as u64 != entry.len {
            return Err(VectorError::IndexCorrupted(format!(
                "backup file {} length mismatch",
                entry.name
            )));
        }
        if crc32c(&bytes) != entry.crc {
            return Err(VectorError::IndexCorrupted(format!(
                "backup file {} CRC mismatch",
                entry.name
            )));
        }
    }

    // Rows: copy + per-payload codec validation.
    let src_records = std::fs::read(backup_dir.join(RECORDS_FILE))
        .map_err(|e| VectorError::IndexCorrupted(format!("backup records.dat missing: {e}")))?;
    let mut rows = 0u64;
    {
        let mut off = 0usize;
        while off < src_records.len() {
            if let RestoreFailPoint::DuringDataRestore(n) = fail {
                if rows >= n {
                    cleanup_target(target_dir);
                    return Err(VectorError::IndexCorrupted(format!(
                        "injected restore failure after {rows} rows"
                    )));
                }
            }
            if src_records.len() < off + 4 {
                cleanup_target(target_dir);
                return Err(VectorError::IndexCorrupted(
                    "backup records.dat truncated".to_string(),
                ));
            }
            let len =
                u32::from_le_bytes(src_records[off..off + 4].try_into().unwrap_or([0; 4])) as usize;
            off += 4;
            if src_records.len() < off + len {
                cleanup_target(target_dir);
                return Err(VectorError::IndexCorrupted(
                    "backup records.dat truncated payload".to_string(),
                ));
            }
            decode_standalone(&src_records[off..off + len]).map_err(|e| {
                let _ = std::fs::remove_dir_all(target_dir.join(INDEX_DIR));
                VectorError::IndexCorrupted(format!("backup row {rows} corrupt: {e}"))
            })?;
            off += len;
            rows += 1;
        }
    }
    if rows != manifest.row_count {
        cleanup_target(target_dir);
        return Err(VectorError::IndexCorrupted(format!(
            "backup row count {} != manifest {}",
            rows, manifest.row_count
        )));
    }
    // Copy verified bytes (identical framing: restore reuses file replay).
    std::fs::write(target_dir.join(RECORDS_FILE), &src_records).map_err(io_fail)?;
    sync_file(&target_dir.join(RECORDS_FILE))?;
    if backup_dir.join(MANIFEST_FILE).exists() {
        let m = std::fs::read(backup_dir.join(MANIFEST_FILE)).map_err(io_fail)?;
        let _ = crate::storage::VectorManifest::from_bytes(&m)?;
        std::fs::write(target_dir.join(MANIFEST_FILE), &m).map_err(io_fail)?;
        sync_file(&target_dir.join(MANIFEST_FILE))?;
    } else {
        // No store manifest in backup (e.g. memory-mode source): synthesize
        // one at the backup checkpoint so the target opens deterministically.
        let synth = crate::storage::VectorManifest::new(
            manifest.checkpoint_lsn,
            manifest.table.clone(),
            manifest.column.clone(),
            manifest.dims,
            rows,
        );
        std::fs::write(target_dir.join(MANIFEST_FILE), synth.to_bytes()).map_err(io_fail)?;
        sync_file(&target_dir.join(MANIFEST_FILE))?;
    }

    if fail == RestoreFailPoint::DuringIndexRestore {
        cleanup_target(target_dir);
        return Err(VectorError::IndexCorrupted(
            "injected restore failure during index restore".to_string(),
        ));
    }

    // Index blobs: verify CRC, stage, classify direct vs rebuild.
    let mut actions = Vec::with_capacity(manifest.indexes.len());
    for entry in &manifest.indexes {
        let action = match entry.mode {
            IndexRestoreMode::Rebuild => IndexRestoreAction {
                name: entry.name.clone(),
                action: RestoredIndexAction::RebuildRequired,
                reason: format!(
                    "index {} ({:?}) has no persist path or dropped tombstones: rebuild from authoritative rows before serving",
                    entry.name, entry.kind
                ),
            },
            IndexRestoreMode::Direct => {
                let bytes = std::fs::read(backup_dir.join(&entry.blob_file)).map_err(|e| {
                    cleanup_target(target_dir);
                    VectorError::IndexCorrupted(format!(
                        "backup blob {} missing: {e}",
                        entry.blob_file
                    ))
                })?;
                if bytes.len() as u64 != entry.blob_len || crc32c(&bytes) != entry.blob_crc {
                    cleanup_target(target_dir);
                    return Err(VectorError::IndexCorrupted(format!(
                        "backup blob {} failed integrity",
                        entry.blob_file
                    )));
                }
                let staged = format!("{}/{}.blob", INDEX_DIR, entry.name);
                std::fs::write(target_dir.join(&staged), &bytes).map_err(io_fail)?;
                sync_file(&target_dir.join(&staged))?;
                IndexRestoreAction {
                    name: entry.name.clone(),
                    action: RestoredIndexAction::Direct,
                    reason: format!(
                        "index {} blob staged; serves after decode + validate",
                        entry.name
                    ),
                }
            }
            IndexRestoreMode::DiskAnnPair => {
                let mut parts = entry.blob_file.split('|');
                let (vname, gname) = (parts.next(), parts.next());
                let (Some(vname), Some(gname)) = (vname, gname) else {
                    cleanup_target(target_dir);
                    return Err(VectorError::IndexCorrupted(format!(
                        "backup diskann pair descriptor corrupt for {}",
                        entry.name
                    )));
                };
                let vectors = std::fs::read(backup_dir.join(vname)).map_err(|e| {
                    cleanup_target(target_dir);
                    VectorError::IndexCorrupted(format!("backup diskann vectors missing: {e}"))
                })?;
                let graph = std::fs::read(backup_dir.join(gname)).map_err(|e| {
                    cleanup_target(target_dir);
                    VectorError::IndexCorrupted(format!("backup diskann graph missing: {e}"))
                })?;
                let mut pair = vectors.clone();
                pair.extend_from_slice(&graph);
                if pair.len() as u64 != entry.blob_len || crc32c(&pair) != entry.blob_crc {
                    cleanup_target(target_dir);
                    return Err(VectorError::IndexCorrupted(format!(
                        "backup diskann pair failed integrity for {}",
                        entry.name
                    )));
                }
                // Stage into a per-index directory with the canonical
                // `vectors.dat` / `graph.dat` names `DiskAnnIndex::open`
                // expects, so `open_restored_diskann` works directly.
                let pair_dir = target_dir.join(INDEX_DIR).join(&entry.name);
                std::fs::create_dir_all(&pair_dir).map_err(io_fail)?;
                let vdst = pair_dir.join("vectors.dat");
                let gdst = pair_dir.join("graph.dat");
                std::fs::create_dir_all(vdst.parent().unwrap_or(target_dir)).map_err(io_fail)?;
                std::fs::write(&vdst, &vectors).map_err(io_fail)?;
                std::fs::write(&gdst, &graph).map_err(io_fail)?;
                sync_file(&vdst)?;
                sync_file(&gdst)?;
                IndexRestoreAction {
                    name: entry.name.clone(),
                    action: RestoredIndexAction::Direct,
                    reason: "diskann pair staged; serves after open/recover validation"
                        .to_string(),
                }
            }
        };
        actions.push(action);
    }

    // Stats: derived — restore when valid, drop when corrupt (never fatal).
    let mut stats_restored = false;
    if manifest.has_stats {
        if let Ok(bytes) = std::fs::read(backup_dir.join(STATS_FILE)) {
            if crate::stats::VectorStats::decode(&bytes).is_ok() {
                stats_restored = true;
            }
        }
    }

    if fail == RestoreFailPoint::DuringFinalize {
        cleanup_target(target_dir);
        return Err(VectorError::IndexCorrupted(
            "injected restore failure during finalize".to_string(),
        ));
    }
    sync_dir(target_dir)?;
    if let Some(m) = metrics {
        m.observe_restore();
    }
    Ok(RestoreReport {
        rows,
        manifest_lsn: manifest.checkpoint_lsn,
        engine_version_match: manifest.engine_version == BACKUP_ENGINE_VERSION,
        stats_restored,
        indexes: actions,
    })
}

/// Remove staged restore output so a failed restore cannot look healthy.
fn cleanup_target(target_dir: &std::path::Path) {
    let _ = std::fs::remove_file(target_dir.join(RECORDS_FILE));
    let _ = std::fs::remove_file(target_dir.join(MANIFEST_FILE));
    let _ = std::fs::remove_dir_all(target_dir.join(INDEX_DIR));
}

/// Decode + validate one staged direct blob for serving.
///
/// Returns `Ok(())` only when the blob decodes under `config` and passes
/// structural validation. Any failure means the caller must rebuild —
/// the index must not appear usable before that completes.
pub fn validate_restored_blob(
    kind: IndexKind,
    config: &crate::index::IndexConfig,
    blob: &[u8],
) -> Result<()> {
    use crate::index::VectorIndex as _;
    match kind {
        IndexKind::Flat | IndexKind::SparseInverted => Err(VectorError::RebuildRequired(
            "flat/sparse indexes always rebuild from authoritative rows".to_string(),
        )),
        IndexKind::Hnsw => {
            let idx = crate::index::hnsw::HnswIndex::decode(blob, config.clone())?;
            idx.validate()
        }
        IndexKind::IvfFlat => {
            let idx = crate::index::ivf::IvfFlatIndex::decode(blob, config.clone())?;
            idx.validate()
        }
        IndexKind::IvfSq => {
            let idx = crate::index::sq::Sq8Index::decode(blob, config.clone())?;
            idx.validate()
        }
        IndexKind::IvfPq => {
            let idx = crate::index::pq::PqIndex::decode(blob, config.clone())?;
            idx.validate()
        }
        IndexKind::Binary => {
            let idx = crate::index::binary::BinaryIndex::decode(blob, config.clone())?;
            idx.validate()
        }
        IndexKind::DiskAnn => Err(VectorError::RebuildRequired(
            "diskann blobs validate via open/recover on the staged pair".to_string(),
        )),
    }
}

/// Validate a staged DiskANN pair by opening it in `pair_dir`.
///
/// `pair_dir` is the per-index directory restore stages
/// (`<target>/indexes/<name>/` holding canonical `vectors.dat` +
/// `graph.dat`).
///
/// Returns the recovered index on success; `RebuildRequired` (or a
/// corruption error) on any invalid state — never a half-open index.
pub fn open_restored_diskann(
    config: crate::index::IndexConfig,
    pair_dir: &std::path::Path,
) -> Result<crate::index::diskann::DiskAnnIndex> {
    crate::index::diskann::DiskAnnIndex::open(config, pair_dir)
}

/// Recommended storage policy for a restore target: always file-backed
/// (restores must be durable; memory mode would silently drop the image
/// on the next restart).
#[must_use]
pub fn restore_storage_policy(memory_budget_bytes: u64) -> VectorStoragePolicy {
    VectorStoragePolicy {
        request: crate::storage::StorageModeRequest::File,
        mmap: crate::storage::MmapConfig::default(),
        memory_budget_bytes,
    }
}

fn io_fail(e: std::io::Error) -> VectorError {
    VectorError::IndexCorrupted(format!("backup io: {e}"))
}

fn sync_file(path: &std::path::Path) -> Result<()> {
    // Windows needs a write handle for `FlushFileBuffers` (`sync_all`).
    #[cfg(windows)]
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_fail)?;
    #[cfg(not(windows))]
    let file = std::fs::File::open(path).map_err(io_fail)?;
    file.sync_all().map_err(io_fail)?;
    Ok(())
}

fn sync_dir(dir: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)
            .and_then(|f| f.sync_all())
            .map_err(io_fail)?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}
