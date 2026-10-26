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

//! Resource family: before/after OS + on-disk footprint around a live workload.
//!
//! # Required-signal coverage (honest accounting)
//!
//! * **RSS** — live via `plomid_memory::process_snapshot` (VmRSS on Linux,
//!   `ps` on macOS, 0 elsewhere).
//! * **FD** — live via `process_snapshot` (`/proc/self/fd` on Linux,
//!   `/dev/fd` on macOS, 0 elsewhere).
//! * **WAL bytes** — live via `dir_size_bytes(wal_dir)` on the harness DB.
//! * **Segments (WAL segment file count)** — live via [`wal_segment_count`],
//!   which counts files in the harness `wal_dir`. This is the file-level
//!   segment signal available from the harness; there is no live
//!   `SegmentedWal::segment_count` handle through `LiveDb`, so the ledger
//!   internals (bytes-since-checkpoint, per-segment fill) are NOT reported
//!   here — only the on-disk file count + byte total.
//! * **Data bytes** — live via `dir_size_bytes(storage_dir)`.
//! * **Temporary space** — reported as `0`: `LiveDb` owns only
//!   `storage_dir`/`wal_dir`; spill temp files (if any) live under the OS
//!   temp dir under engine-chosen names with no live handle from the
//!   harness, so we report 0 rather than guessing. A spill-aware harness
//!   handle would be needed for a live temp signal.
//! * **Cache (BufferPool dirty bytes / page-cache occupancy)** — NOT live:
//!   no cache-stats handle is exposed through `LiveDb`/executor, so only
//!   the on-disk footprint above is reported (ledger-level only).
//! * **Allocator (HTAP governor / memory-manager stats, spill counters,
//!   columnar store)** — NOT live: no governor/manager handle is reachable
//!   from the harness, so only OS-level RSS/FD (the allocator's observable
//!   effect) is reported (ledger-level only).
//!
//! # Long-soak procedure (1h / 6h / 24h)
//!
//! CI runs the minutes-scale [`run_resource`] sample below. The long-soak
//! protocol is identical except for duration: call `sample_live_db` before
//! the soak, run the same INSERT/UPDATE/DELETE mix in a wall-clock-bounded
//! loop (1h / 6h / 24h), then `sample_live_db` after and compare
//! `wal_delta_bytes` / `rss_delta_bytes` plus `fd_count` drift. The report
//! crate renders before/after with the same field names, so short and long
//! runs are directly comparable. No separate code path is needed — only a
//! larger `write_iters` (or time-bounded loop) and the soak duration.

use crate::harness::{open_live_db, LiveDb};
use plomid_memory::{dir_size_bytes, process_snapshot};
use serde::{Deserialize, Serialize};

/// Live footprint sample around a workload.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ResourceSample {
    /// Process resident set size in bytes (live OS gauge).
    pub rss_bytes: u64,
    /// Process open FD count (live OS gauge).
    pub fd_count: u64,
    /// Total bytes under the harness WAL dir (live on-disk).
    pub wal_bytes: u64,
    /// Total bytes under the harness storage dir (live on-disk).
    pub data_bytes: u64,
    /// Temp/spill bytes. Always 0: no live temp-dir handle exists on
    /// `LiveDb` (see module docs); reported honestly instead of guessed.
    pub temp_bytes: u64,
}

/// Before/after comparison of one resource run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ResourceResult {
    /// Sample taken on the freshly opened DB, before any workload SQL.
    pub before: ResourceSample,
    /// Sample taken after the workload, before harness teardown.
    pub after: ResourceSample,
    /// Wall-clock seconds of the workload section only (excludes sampling).
    pub elapsed_secs: f64,
    /// `after.wal_bytes - before.wal_bytes` (may be negative after GC).
    pub wal_delta_bytes: i64,
    /// `after.rss_bytes - before.rss_bytes` (may be negative).
    pub rss_delta_bytes: i64,
}

/// Sample the live DB's OS + on-disk footprint.
pub fn sample_live_db(db: &LiveDb) -> ResourceSample {
    let proc = process_snapshot();
    ResourceSample {
        rss_bytes: proc.rss_bytes,
        fd_count: proc.fd_count,
        wal_bytes: dir_size_bytes(&db.wal_dir),
        data_bytes: dir_size_bytes(&db.storage_dir),
        // No live spill-temp handle on LiveDb; honest 0 (see module docs).
        temp_bytes: 0,
    }
}

/// Live WAL segment-file count for the harness DB.
///
/// Counts regular files directly under `wal_dir`. This is the file-level
/// segment signal available without a `SegmentedWal` handle; per-segment
/// fill / bytes-since-checkpoint internals are not visible here.
pub fn wal_segment_count(db: &LiveDb) -> usize {
    std::fs::read_dir(&db.wal_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .count()
        })
        // Unreadable wal dir (concurrent teardown) -> 0, never panic.
        .unwrap_or(0)
}

/// Open a live DB, run CREATE + seed + a write mix, return before/after.
///
/// Workload: `CREATE TABLE res`, `seed_rows` single-row INSERTs, then
/// `write_iters` statements round-robining INSERT (new key) / UPDATE (seed
/// keyspace) / DELETE (seed keyspace) — all through the live executor, so
/// WAL, storage, and allocator paths are genuinely exercised.
pub fn run_resource(seed_rows: usize, write_iters: usize) -> ResourceResult {
    let mut db = open_live_db("resource");
    let before = sample_live_db(&db);
    let t0 = std::time::Instant::now();
    db.exec("CREATE TABLE res (id INTEGER PRIMARY KEY, v INTEGER);");
    for i in 0..seed_rows {
        db.exec(&format!("INSERT INTO res VALUES ({i}, {i});"));
    }
    let keyspace = seed_rows.max(1) as u64;
    for i in 0..write_iters {
        let i64_ = i as u64;
        match i % 3 {
            // New key each time: exercises insert + WAL append path.
            0 => db.exec(&format!(
                "INSERT INTO res VALUES ({}, {i64_});",
                seed_rows as u64 + i64_
            )),
            // Hit the seed keyspace: exercises point-update path.
            1 => db.exec(&format!(
                "UPDATE res SET v = {i64_} WHERE id = {};",
                i64_ % keyspace
            )),
            // Hit the seed keyspace: exercises point-delete path
            // (first pass deletes seed rows, later passes are live no-ops).
            _ => db.exec(&format!("DELETE FROM res WHERE id = {};", i64_ % keyspace)),
        }
    }
    let elapsed_secs = t0.elapsed().as_secs_f64();
    let after = sample_live_db(&db);
    db.teardown();
    ResourceResult {
        before,
        after,
        elapsed_secs,
        wal_delta_bytes: after.wal_bytes as i64 - before.wal_bytes as i64,
        rss_delta_bytes: after.rss_bytes as i64 - before.rss_bytes as i64,
    }
}
