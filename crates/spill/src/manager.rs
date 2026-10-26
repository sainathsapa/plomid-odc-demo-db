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

//! Bounded temp storage for spilled runs.
//!
//! [`SpillManager`] owns a temp directory plus two hard bounds:
//! `temp_limit_bytes` (default 1 GiB) and `max_files`. Every record write goes
//! through [`SpillRun::write_record`], which calls [`SpillManager::check_limit`]
//! first, so disk use can never grow unboundedly. [`SpillRun`] deletes its
//! temp file on [`Drop`]; live-file and byte counters back down accordingly.

use crate::error::{Result, SpillError};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Default bound for total temp bytes on disk (1 GiB).
pub const DEFAULT_TEMP_LIMIT_BYTES: u64 = 1 << 30;
/// Default bound for concurrently live spill files.
pub const DEFAULT_MAX_FILES: usize = 1024;
/// Largest single record accepted (256 MiB); larger is a decode/storage error.
pub const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;
const WRITER_CAPACITY: usize = 64 * 1024;

/// Configuration for [`SpillManager`].
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// Directory holding `spill-*.tmp` files. Created on manager construction.
    pub temp_dir: PathBuf,
    /// Hard bound on total live temp bytes. Every write checks this.
    pub temp_limit_bytes: u64,
    /// Hard bound on concurrently live temp files.
    pub max_files: usize,
}

impl SpillConfig {
    /// Config with default limits under `temp_dir`.
    pub fn new(temp_dir: PathBuf) -> Self {
        Self {
            temp_dir,
            temp_limit_bytes: DEFAULT_TEMP_LIMIT_BYTES,
            max_files: DEFAULT_MAX_FILES,
        }
    }
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self::new(std::env::temp_dir().join("plomid-spill"))
    }
}

pub(crate) struct Inner {
    config: SpillConfig,
    current_bytes: AtomicU64,
    total_bytes: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    live_files: AtomicUsize,
    next_id: AtomicU64,
}

/// Bounded temp-file manager. Cheaply cloneable; all state is shared.
#[derive(Clone)]
pub struct SpillManager {
    inner: Arc<Inner>,
}

impl SpillManager {
    /// Create the temp dir (if missing) and return a manager.
    pub fn new(config: SpillConfig) -> Result<Self> {
        fs::create_dir_all(&config.temp_dir)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                current_bytes: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
                reads: AtomicU64::new(0),
                writes: AtomicU64::new(0),
                live_files: AtomicUsize::new(0),
                next_id: AtomicU64::new(0),
            }),
        })
    }

    /// Manager with default limits under `temp_dir`.
    pub fn with_temp_dir(temp_dir: PathBuf) -> Result<Self> {
        Self::new(SpillConfig::new(temp_dir))
    }

    /// Directory holding temp files.
    pub fn temp_dir(&self) -> &Path {
        &self.inner.config.temp_dir
    }

    /// Configured byte limit.
    pub fn temp_limit_bytes(&self) -> u64 {
        self.inner.config.temp_limit_bytes
    }

    /// Create one temp run file in `temp_dir`.
    ///
    /// Fails with [`SpillError::StorageExhausted`] when `max_files` live runs
    /// already exist.
    pub fn create_run(&self) -> Result<SpillRun> {
        if self.inner.live_files.load(Ordering::Relaxed) >= self.inner.config.max_files {
            return Err(SpillError::StorageExhausted(format!(
                "max spill files ({}) reached",
                self.inner.config.max_files
            )));
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .inner
            .config
            .temp_dir
            .join(format!("spill-{}-{id}.tmp", std::process::id()));
        let file = File::create(&path)?;
        self.inner.live_files.fetch_add(1, Ordering::Relaxed);
        plomid_core::observability::record_spill_file();
        Ok(SpillRun {
            path,
            writer: Some(BufWriter::with_capacity(WRITER_CAPACITY, file)),
            len: 0,
            manager: Arc::clone(&self.inner),
        })
    }

    /// Fail when `additional` bytes would exceed `temp_limit_bytes`.
    ///
    /// Called by every write path before touching the filesystem.
    pub fn check_limit(&self, additional: u64) -> Result<()> {
        let cur = self.inner.current_bytes.load(Ordering::Relaxed);
        if cur.saturating_add(additional) > self.inner.config.temp_limit_bytes {
            return Err(SpillError::StorageExhausted(format!(
                "temp limit {}/{} bytes exceeded",
                cur.saturating_add(additional),
                self.inner.config.temp_limit_bytes
            )));
        }
        Ok(())
    }

    /// Cumulative bytes ever spilled (monotonic observability counter).
    pub fn bytes_spilled(&self) -> u64 {
        self.inner.total_bytes.load(Ordering::Relaxed)
    }

    /// Alias of [`SpillManager::bytes_spilled`].
    pub fn spill_bytes(&self) -> u64 {
        self.bytes_spilled()
    }

    /// Live (not yet deleted) temp bytes.
    pub fn current_bytes(&self) -> u64 {
        self.inner.current_bytes.load(Ordering::Relaxed)
    }

    /// Cumulative spilled records read.
    pub fn spill_reads(&self) -> u64 {
        self.inner.reads.load(Ordering::Relaxed)
    }

    /// Cumulative spilled records written.
    pub fn spill_writes(&self) -> u64 {
        self.inner.writes.load(Ordering::Relaxed)
    }

    /// Currently live temp files.
    pub fn live_files(&self) -> usize {
        self.inner.live_files.load(Ordering::Relaxed)
    }

    /// Alias of [`SpillManager::live_files`].
    pub fn files(&self) -> usize {
        self.live_files()
    }
}

/// One temp spill file: length-prefixed record frames.
///
/// Created by [`SpillManager::create_run`]. [`Drop`] flushes, deletes the file
/// from disk, and returns its bytes/live-file slot to the manager.
pub struct SpillRun {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    len: u64,
    manager: Arc<Inner>,
}

impl SpillRun {
    /// Temp file path (for diagnostics only; the file is managed).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Live on-disk bytes for this run.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether this run holds no records yet.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Raw byte write. Checks the manager limit first.
    pub fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.manager.config_check(bytes.len() as u64)?;
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| SpillError::Io("spill run already closed".to_string()))?;
        w.write_all(bytes)?;
        self.len += bytes.len() as u64;
        self.manager
            .current_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.manager
            .total_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Append one record frame (`u32` LE length + payload). Checks the manager
    /// limit first, so storage stays bounded.
    pub fn write_record(&mut self, payload: &[u8]) -> Result<()> {
        if payload.len() > MAX_RECORD_BYTES {
            return Err(SpillError::StorageExhausted(format!(
                "record {} bytes exceeds max {}",
                payload.len(),
                MAX_RECORD_BYTES
            )));
        }
        let need = 4 + payload.len() as u64;
        self.manager.config_check(need)?;
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| SpillError::Io("spill run already closed".to_string()))?;
        w.write_all(&(payload.len() as u32).to_le_bytes())?;
        w.write_all(payload)?;
        self.len += need;
        self.manager
            .current_bytes
            .fetch_add(need, Ordering::Relaxed);
        self.manager.total_bytes.fetch_add(need, Ordering::Relaxed);
        self.manager.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Flush buffered writes so a reader observes them.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(w) = self.writer.as_mut() {
            w.flush()?;
        }
        Ok(())
    }

    /// Open a streaming record reader over this run (flushes first).
    pub fn open_reader(&mut self) -> Result<RunReader> {
        self.flush()?;
        let file = File::open(&self.path)?;
        Ok(RunReader {
            reader: BufReader::with_capacity(WRITER_CAPACITY, file),
            manager: Arc::clone(&self.manager),
        })
    }

    /// Read every record frame into memory (flushes first).
    pub fn records(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut r = self.open_reader()?;
        let mut out = Vec::new();
        while let Some(rec) = r.next_record()? {
            out.push(rec);
        }
        Ok(out)
    }
}

impl Drop for SpillRun {
    fn drop(&mut self) {
        if let Some(mut w) = self.writer.take() {
            let _ = w.flush();
        }
        let _ = fs::remove_file(&self.path);
        self.manager
            .current_bytes
            .fetch_sub(self.len, Ordering::Relaxed);
        self.manager.live_files.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Inner {
    fn config_check(&self, additional: u64) -> Result<()> {
        let cur = self.current_bytes.load(Ordering::Relaxed);
        if cur.saturating_add(additional) > self.config.temp_limit_bytes {
            return Err(SpillError::StorageExhausted(format!(
                "temp limit {}/{} bytes exceeded",
                cur.saturating_add(additional),
                self.config.temp_limit_bytes
            )));
        }
        Ok(())
    }
}

/// Streaming reader over a [`SpillRun`]'s record frames.
pub struct RunReader {
    reader: BufReader<File>,
    manager: Arc<Inner>,
}

impl RunReader {
    /// Next record payload, or `None` at end of file.
    pub fn next_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        let mut got = 0;
        while got < 4 {
            match self.reader.read(&mut len_buf[got..]) {
                Ok(0) => {
                    if got == 0 {
                        return Ok(None);
                    }
                    return Err(SpillError::Decode(
                        "truncated record length prefix".to_string(),
                    ));
                }
                Ok(n) => got += n,
                Err(e) => return Err(SpillError::Io(e.to_string())),
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_RECORD_BYTES {
            return Err(SpillError::Decode(format!(
                "record {len} bytes exceeds max {MAX_RECORD_BYTES}"
            )));
        }
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload)?;
        self.manager.reads.fetch_add(1, Ordering::Relaxed);
        Ok(Some(payload))
    }
}
