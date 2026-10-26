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

//! Scale family: throughput/latency at each requested table size.
//!
//! # Honesty gate
//!
//! Only the requested points are tested — results never claim untested
//! scales. Points above [`BIG_POINT_ROWS`] (the 1M/2M scale tier and
//! anything near it) are *skipped* unless the
//! `PLOMID_GAUNTLET_SCALE=1` environment variable is set, because
//! million-row single-row INSERT loads are far outside a CI time budget.
//! Default CI calls [`run_scale`] with small points (e.g. `&[5000, 10000]`).
//! A skipped point simply does not appear in [`ScaleResult::points`]; callers
//! must report "not run", never extrapolate.

use crate::harness::open_live_db;
use crate::metrics::{lat_stats, LatencyStats};
use serde::{Deserialize, Serialize};

/// Rows above this count require `PLOMID_GAUNTLET_SCALE=1` to run.
///
/// Set below 1M so the 1M/2M tier is always gated, with margin for CI time
/// on slower runners. Small CI points (5k/10k) are unaffected.
pub const BIG_POINT_ROWS: usize = 100_000;

/// Throughput/latency observed at one table size, all via the live executor.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ScalePoint {
    /// Row count loaded for this point (the requested scale).
    pub rows: usize,
    /// Bulk-load throughput in rows/sec over the timed INSERT section.
    pub insert_rps: f64,
    /// p99 latency (ms) over 50 timed point `SELECT ... WHERE id = ?`.
    pub pk_p99_ms: f64,
    /// Throughput (scans/sec) over 20 timed 100-row range scans.
    pub range_rps: f64,
    /// Latency (ms) of one timed `SELECT COUNT(*)`.
    pub count_latency_ms: f64,
}

/// One [`ScalePoint`] per point actually run (gated points are omitted).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScaleResult {
    pub points: Vec<ScalePoint>,
}

/// Full latency distribution behind a scale point's PK probe (diagnostic).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScalePkStats {
    pub rows: usize,
    pub pk: LatencyStats,
    pub range: LatencyStats,
}

/// Run each requested point through a live DB. See module docs for gating.
pub fn run_scale(points: &[usize]) -> ScaleResult {
    // Opt-in for the big tier; default (unset/any other value) runs small only.
    let big_allowed = std::env::var("PLOMID_GAUNTLET_SCALE")
        .map(|v| v == "1")
        .unwrap_or(false);
    let mut out = Vec::with_capacity(points.len());
    for &rows in points {
        // Honesty gate: never burn CI on (or claim) the 1M/2M tier unasked.
        if rows > BIG_POINT_ROWS && !big_allowed {
            continue;
        }
        out.push(run_point(rows));
    }
    ScaleResult { points: out }
}

fn run_point(rows: usize) -> ScalePoint {
    let mut db = open_live_db("scale");
    db.exec("CREATE TABLE sc (id INTEGER PRIMARY KEY, v INTEGER);");
    // Batched single-row INSERTs, one timed section -> insert throughput.
    let t0 = std::time::Instant::now();
    for i in 0..rows {
        db.exec(&format!("INSERT INTO sc VALUES ({i}, {i});"));
    }
    let insert_secs = t0.elapsed().as_secs_f64();
    let insert_rps = rows as f64 / insert_secs.max(f64::EPSILON);

    // PK SELECT p99 over 50 timed samples spread across the keyspace.
    // (rows == 0 cannot happen here with a real table, but guard the modulo.)
    let keyspace = rows.max(1);
    let t1 = std::time::Instant::now();
    let mut pk_ms = Vec::with_capacity(50);
    for s in 0..50 {
        let id = (s * 7919) % keyspace;
        pk_ms.push(db.timed_exec(&format!("SELECT * FROM sc WHERE id = {id};")));
    }
    let pk_elapsed = t1.elapsed().as_secs_f64();
    let pk = lat_stats(pk_ms, pk_elapsed);

    // Range-scan throughput: 20 timed 100-row bounded scans.
    let t2 = std::time::Instant::now();
    let mut range_ms = Vec::with_capacity(20);
    for s in 0..20 {
        let lo = (s * 104729) % keyspace;
        let hi = lo.saturating_add(99);
        range_ms.push(db.timed_exec(&format!(
            "SELECT * FROM sc WHERE id >= {lo} AND id <= {hi};"
        )));
    }
    let range_elapsed = t2.elapsed().as_secs_f64();
    let range = lat_stats(range_ms, range_elapsed);

    // COUNT(*) latency: single timed statement.
    let count_latency_ms = db.timed_exec("SELECT COUNT(*) FROM sc;");

    db.teardown();
    ScalePoint {
        rows,
        insert_rps,
        pk_p99_ms: pk.p99_ms,
        range_rps: range.throughput_rps,
        count_latency_ms,
    }
}

/// Diagnostic variant returning full latency distributions (same gating).
pub fn run_scale_detailed(points: &[usize]) -> (ScaleResult, Vec<ScalePkStats>) {
    let big_allowed = std::env::var("PLOMID_GAUNTLET_SCALE")
        .map(|v| v == "1")
        .unwrap_or(false);
    let mut result = ScaleResult { points: Vec::new() };
    let mut detail = Vec::new();
    for &rows in points {
        if rows > BIG_POINT_ROWS && !big_allowed {
            continue;
        }
        let mut db = open_live_db("scale");
        db.exec("CREATE TABLE sc (id INTEGER PRIMARY KEY, v INTEGER);");
        let t0 = std::time::Instant::now();
        for i in 0..rows {
            db.exec(&format!("INSERT INTO sc VALUES ({i}, {i});"));
        }
        let insert_secs = t0.elapsed().as_secs_f64();
        let keyspace = rows.max(1);
        let t1 = std::time::Instant::now();
        let mut pk_ms = Vec::with_capacity(50);
        for s in 0..50 {
            let id = (s * 7919) % keyspace;
            pk_ms.push(db.timed_exec(&format!("SELECT * FROM sc WHERE id = {id};")));
        }
        let pk = lat_stats(pk_ms, t1.elapsed().as_secs_f64());
        let t2 = std::time::Instant::now();
        let mut range_ms = Vec::with_capacity(20);
        for s in 0..20 {
            let lo = (s * 104729) % keyspace;
            let hi = lo.saturating_add(99);
            range_ms.push(db.timed_exec(&format!(
                "SELECT * FROM sc WHERE id >= {lo} AND id <= {hi};"
            )));
        }
        let range = lat_stats(range_ms, t2.elapsed().as_secs_f64());
        let count_latency_ms = db.timed_exec("SELECT COUNT(*) FROM sc;");
        db.teardown();
        result.points.push(ScalePoint {
            rows,
            insert_rps: rows as f64 / insert_secs.max(f64::EPSILON),
            pk_p99_ms: pk.p99_ms,
            range_rps: range.throughput_rps,
            count_latency_ms,
        });
        detail.push(ScalePkStats { rows, pk, range });
    }
    (result, detail)
}
