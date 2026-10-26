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
//! Crash-safe file adapter: the durable half of [`VectorStorageAdapter`].
//!
//! NON-AUTHORITATIVE derived cache (see [`crate::durability`]).
//! Authoritative vector values live in storage-engine rows
//! (txn/MVCC/WAL/generations). This file layout (`records.dat` + `MANIFEST`)
//! is legacy test scaffolding and a rebuildable derived image only: it is
//! never consulted by the production SQL path, and deleting or corrupting it
//! must never lose committed data — exact search serves from engine rows
//! while ANN rebuilds.
//!
//! Layout under `dir/`:
//!
//! ```text
//! records.dat   append-only [len u32 LE][standalone bytes] segments
//! MANIFEST      `VectorManifest` bytes (checkpoint LSN + identity)
//! ```
//!
//! Durability: every append/overwrite is `sync_all`-ed before success is
//! reported; checkpoints publish via temp-file + atomic rename + directory
//! sync. Recovery (`open`) replays `records.dat`, truncates a torn tail
//! record from a crash mid-write, and reports what it did — a torn tail is
//! normal crash evidence, not corruption. Row ids are append-stable slot
//! indices (`0..row_count`); overwrites append a new segment and repoint
//! the slot, so readers never observe a torn row.
//!
//! `NULL` has no encoding here: absent rows are a caller-level concept
//! (the executor skips SQL `NULL`s before touching storage).

use std::path::{Path, PathBuf};

use crate::codec::decode_standalone;
use crate::storage::manifest::VectorManifest;
use crate::validation::check_budget;
use crate::{Result, VectorError, VectorValue};

use super::adapter::{bounds_err, VectorStorageAdapter};

/// File names inside the store directory.
pub const RECORDS_FILE: &str = "records.dat";
/// Manifest file name inside the store directory.
pub const MANIFEST_FILE: &str = "MANIFEST";

/// Outcome of opening (or re-opening) a file store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRecovery {
    /// Records replayed from `records.dat`.
    pub replayed_records: u64,
    /// Manifest checkpoint LSN (`0` when no manifest was present).
    pub manifest_lsn: u64,
    /// A torn tail record was truncated (crash mid-append).
    pub truncated_tail: bool,
}

/// Crash-safe file implementation of [`VectorStorageAdapter`].
#[derive(Debug)]
pub struct FileVectorStorage {
    dir: PathBuf,
    /// Slot index → byte offset of the live record in `records.dat`.
    offsets: Vec<u64>,
    dims: Option<u32>,
    table: String,
    column: String,
}

impl FileVectorStorage {
    /// Creates a fresh store in `dir` (fails if a store already exists there).
    pub fn create(
        dir: impl Into<PathBuf>,
        table: String,
        column: String,
        dims: Option<u32>,
    ) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(io_fail)?;
        if dir.join(RECORDS_FILE).exists() || dir.join(MANIFEST_FILE).exists() {
            return Err(VectorError::IndexStateInvalid(
                "vector store already exists".to_string(),
            ));
        }
        std::fs::write(dir.join(RECORDS_FILE), []).map_err(io_fail)?;
        sync_file(&dir.join(RECORDS_FILE))?;
        sync_dir(&dir)?;
        Ok(Self {
            dir,
            offsets: Vec::new(),
            dims,
            table,
            column,
        })
    }

    /// Opens an existing store, replaying records and truncating a torn
    /// tail. Missing files mean a fresh store only when `allow_create` is
    /// set; otherwise opening a non-store fails closed.
    pub fn open(
        dir: impl Into<PathBuf>,
        table: String,
        column: String,
        dims: Option<u32>,
        allow_create: bool,
    ) -> Result<(Self, FileRecovery)> {
        let dir = dir.into();
        let records_path = dir.join(RECORDS_FILE);
        if !records_path.exists() {
            if allow_create {
                let fresh = Self::create(&dir, table, column, dims)?;
                return Ok((
                    fresh,
                    FileRecovery {
                        replayed_records: 0,
                        manifest_lsn: 0,
                        truncated_tail: false,
                    },
                ));
            }
            return Err(VectorError::IndexCorrupted(
                "vector store records.dat missing".to_string(),
            ));
        }
        let manifest_lsn = match std::fs::read(dir.join(MANIFEST_FILE)) {
            Ok(bytes) => VectorManifest::from_bytes(&bytes)?.checkpoint_lsn,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(io_fail(e)),
        };
        let (offsets, truncated_tail) = replay_records(&records_path)?;
        let replayed_records = offsets.len() as u64;
        Ok((
            Self {
                dir,
                offsets,
                dims,
                table,
                column,
            },
            FileRecovery {
                replayed_records,
                manifest_lsn,
                truncated_tail,
            },
        ))
    }

    /// Durably records `checkpoint_lsn` (deltas at/below it are durable).
    pub fn checkpoint(&self, checkpoint_lsn: u64) -> Result<()> {
        let manifest = VectorManifest::new(
            checkpoint_lsn,
            self.table.clone(),
            self.column.clone(),
            self.dims.unwrap_or(0),
            self.row_count(),
        );
        let tmp = self.dir.join("MANIFEST.tmp");
        std::fs::write(&tmp, manifest.to_bytes()).map_err(io_fail)?;
        sync_file(&tmp)?;
        std::fs::rename(&tmp, self.dir.join(MANIFEST_FILE)).map_err(io_fail)?;
        sync_dir(&self.dir)?;
        Ok(())
    }

    /// Flushes buffered OS state (writes are already write-through).
    pub fn flush(&self) -> Result<()> {
        sync_file(&self.dir.join(RECORDS_FILE))?;
        sync_dir(&self.dir)?;
        Ok(())
    }

    fn check_row(&self, row_id: u64) -> Result<usize> {
        let idx = usize::try_from(row_id).map_err(|_| bounds_err("row_id overflow"))?;
        if idx >= self.offsets.len() {
            return Err(bounds_err(&format!(
                "row_id {row_id} out of bounds ({})",
                self.offsets.len()
            )));
        }
        Ok(idx)
    }

    fn append_record(&mut self, row_bytes: &[u8]) -> Result<u64> {
        use std::io::Write;
        let _ = decode_standalone(row_bytes)?;
        check_budget(row_bytes.len())?;
        let path = self.dir.join(RECORDS_FILE);
        let offset = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_fail)?;
        f.write_all(&(row_bytes.len() as u32).to_le_bytes())
            .map_err(io_fail)?;
        f.write_all(row_bytes).map_err(io_fail)?;
        f.flush().map_err(io_fail)?;
        f.sync_all().map_err(io_fail)?;
        sync_dir(&self.dir)?;
        self.offsets.push(offset);
        Ok(self.offsets.len() as u64 - 1)
    }

    fn read_at(&self, offset: u64) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.dir.join(RECORDS_FILE);
        let mut f = std::fs::File::open(&path).map_err(io_fail)?;
        f.seek(SeekFrom::Start(offset)).map_err(io_fail)?;
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)
            .map_err(|e| VectorError::IndexCorrupted(e.to_string()))?;
        let len = u32::from_le_bytes(len_buf) as usize;
        check_budget(len)
            .map_err(|_| VectorError::IndexCorrupted("record length out of budget".to_string()))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)
            .map_err(|e| VectorError::IndexCorrupted(e.to_string()))?;
        Ok(buf)
    }
}

/// Replays record offsets; truncates a torn tail record from a crash
/// mid-append. Returns `(offsets, truncated_tail)`.
fn replay_records(path: &Path) -> Result<(Vec<u64>, bool)> {
    use std::io::{Read, Seek, SeekFrom};
    let mut offsets = Vec::new();
    let mut f = std::fs::File::open(path).map_err(io_fail)?;
    let total = f.seek(SeekFrom::End(0)).map_err(io_fail)?;
    let mut pos = 0u64;
    let mut truncated_tail = false;
    while pos < total {
        f.seek(SeekFrom::Start(pos)).map_err(io_fail)?;
        let mut len_buf = [0u8; 4];
        if f.read_exact(&mut len_buf).is_err() {
            truncated_tail = true;
            break;
        }
        let len = u32::from_le_bytes(len_buf) as u64;
        // Sanity: a record can never extend past EOF, and absurd lengths
        // fail here without allocating.
        if len > total.saturating_sub(pos + 4)
            || len > super::super::validation::MAX_PAYLOAD_BYTES as u64 + 64
        {
            truncated_tail = true;
            break;
        }
        // Skip the payload (validated on actual read, not replay).
        pos += 4 + len;
        offsets.push(pos - 4 - len);
        if pos > total {
            offsets.pop();
            truncated_tail = true;
            break;
        }
    }
    drop(f);
    if truncated_tail {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(io_fail)?;
        f.set_len(pos).map_err(io_fail)?;
        sync_file(path)?;
    }
    Ok((offsets, truncated_tail))
}

fn io_fail(e: std::io::Error) -> VectorError {
    VectorError::IndexCorrupted(format!("vector store io: {e}"))
}

fn sync_file(path: &Path) -> Result<()> {
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

fn sync_dir(dir: &Path) -> Result<()> {
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

impl VectorStorageAdapter for FileVectorStorage {
    fn row_count(&self) -> u64 {
        self.offsets.len() as u64
    }

    fn dims(&self) -> Option<u32> {
        self.dims
    }

    fn get_row_bytes(&self, row_id: u64) -> Result<Vec<u8>> {
        let idx = self.check_row(row_id)?;
        self.read_at(self.offsets[idx])
    }

    fn put_row_bytes(&mut self, row_id: u64, row_bytes: &[u8]) -> Result<()> {
        let _ = decode_standalone(row_bytes)?;
        let idx = self.check_row(row_id)?;
        // Append-and-repoint: readers never observe a torn overwrite.
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(RECORDS_FILE))
            .map_err(io_fail)?;
        use std::io::Write;
        let offset = std::fs::metadata(self.dir.join(RECORDS_FILE))
            .map(|m| m.len())
            .unwrap_or(0);
        f.write_all(&(row_bytes.len() as u32).to_le_bytes())
            .map_err(io_fail)?;
        f.write_all(row_bytes).map_err(io_fail)?;
        f.flush().map_err(io_fail)?;
        f.sync_all().map_err(io_fail)?;
        sync_dir(&self.dir)?;
        self.offsets[idx] = offset;
        Ok(())
    }

    fn append_row_bytes(&mut self, row_bytes: &[u8]) -> Result<u64> {
        self.append_record(row_bytes)
    }

    fn scan_batch(&self, start_row: u64, limit: usize) -> Result<Vec<(u64, VectorValue)>> {
        let start = usize::try_from(start_row).map_err(|_| bounds_err("start_row overflow"))?;
        if start > self.offsets.len() {
            return Err(bounds_err("start_row out of bounds"));
        }
        let end = (start + limit).min(self.offsets.len());
        let mut out = Vec::with_capacity(end - start);
        for (i, offset) in self.offsets[start..end].iter().enumerate() {
            let raw = self.read_at(*offset)?;
            out.push((start_row + i as u64, decode_standalone(&raw)?));
        }
        Ok(out)
    }
}
