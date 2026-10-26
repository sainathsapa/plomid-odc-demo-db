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

//! OLAP scan/aggregate benchmark family, wired to the live engine path.
//!
//! Every statement below goes through [`plomid_executor::Executor::execute`]
//! (the `execute_all` choke point: governance admission plus result-buffer
//! charging), so the measured latencies include full production overhead.
//! Seeds are deterministic (`id`-derived payloads, no RNG, no sleeps) so
//! reruns are reproducible and CI stays fast (`rows = 5000` is the intended
//! small default).

use crate::harness::open_live_db;
use plomid_sql::QueryResult;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// One measured OLAP query: wall latency, output cardinality, and scan
/// throughput derived from the (known) table size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OlapQueryResult {
    pub name: String,
    pub rows_per_sec: f64,
    pub gb_per_sec: f64,
    pub latency_ms: f64,
    pub rows_out: u64,
}

/// The nine-query OLAP sweep plus process-level resource observations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OlapResult {
    pub queries: Vec<OlapQueryResult>,
    pub cpu_util_pct: f64,
    pub peak_rss_mb: f64,
}

/// Rows per seed INSERT statement (bounded literal size for the parser).
const SEED_BATCH: usize = 500;

/// Estimated encoded bytes per scanned fact row backing the GB/s figure.
/// This is a documented estimate of average row width (`id` + `g` + `v` +
/// row overhead), not a per-query byte counter: every query below scans the
/// full fact table, so `rows * 32 / elapsed` is the honest scan bandwidth.
const BYTES_PER_ROW: f64 = 32.0;

/// Number of distinct `g` groups in the fact table (also the `WHERE g = 7`
/// filter target, matching roughly `rows / G_GROUPS` rows).
const G_GROUPS: i64 = 16;

/// Rows in the `ga_olap_dim` side of the join query.
const DIM_ROWS: usize = 100;

/// Peak resident set size of this process in MiB.
///
/// Linux: parsed from `/proc/self/status` (`VmHWM`, the high-water mark, so
/// it reflects the benchmark's peak rather than a post-run sample). Other
/// platforms have no dependency-free RSS source wired here, so this honestly
/// reports `0.0` there instead of fabricating a number.
#[cfg(target_os = "linux")]
fn peak_rss_mb() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(kb) = kb.parse::<f64>() {
                    return kb / 1024.0;
                }
            }
        }
    }
    0.0
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_mb() -> f64 {
    0.0
}

/// Runs the nine-query OLAP sweep against a fresh live database.
///
/// Each query is issued via `Executor::execute` directly (rather than the
/// row-discarding `timed_exec` helper) so `rows_out` is counted from the
/// returned [`QueryResult::Rows`]. Throughput figures use rows *scanned*
/// (the full fact table — every query below is a full scan, including the
/// aggregates) divided by that query's own latency.
///
/// `cpu_util_pct` is the benchmark-thread duty cycle
/// (`100 * sum(query latencies) / wall`), clamped to `[0, 100]`: the issuer
/// is serial and single-threaded, so this is the honest utilization proxy
/// for the measurement thread, not a sampled whole-process CPU counter.
pub fn run_olap(rows: usize) -> OlapResult {
    let mut db = open_live_db("olap");
    db.exec("CREATE TABLE ga_olap (id BIGINT PRIMARY KEY, g INT, v BIGINT, s TEXT);");

    let mut next = 1_i64;
    let end = 1 + rows as i64;
    while next < end {
        let n = ((end - next) as usize).min(SEED_BATCH);
        let mut sql = String::from("INSERT INTO ga_olap (id, g, v, s) VALUES ");
        for i in 0..n {
            if i > 0 {
                sql.push_str(", ");
            }
            let id = next + i as i64;
            sql.push_str(&format!(
                "({id}, {}, {}, 'row-{id}')",
                id % G_GROUPS,
                id * 3 + 1
            ));
        }
        sql.push(';');
        db.exec(&sql);
        next += n as i64;
    }

    // Small dimension table for the join query; every fact `g` (0..15) has
    // a match, so the join is full-coverage rather than selective.
    db.exec("CREATE TABLE ga_olap_dim (g INT PRIMARY KEY, name TEXT);");
    {
        let mut sql = String::from("INSERT INTO ga_olap_dim (g, name) VALUES ");
        for i in 0..DIM_ROWS {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({i}, 'dim-{i}')"));
        }
        sql.push(';');
        db.exec(&sql);
    }

    // Best-effort maintenance pass (index refresh) before measuring, exactly
    // as a production operator would run it. Absence of due tables is fine.
    let _ = db.executor.run_maintenance();

    let specs: Vec<(&str, String)> = vec![
        ("full_scan", "SELECT id, g, v FROM ga_olap;".to_string()),
        ("projection", "SELECT g, v FROM ga_olap;".to_string()),
        ("filter", "SELECT id FROM ga_olap WHERE g = 7;".to_string()),
        ("count", "SELECT COUNT(*) FROM ga_olap;".to_string()),
        ("sum", "SELECT SUM(v) FROM ga_olap;".to_string()),
        (
            "group_by",
            "SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v) FROM ga_olap GROUP BY g;".to_string(),
        ),
        (
            "order_by",
            "SELECT v FROM ga_olap ORDER BY v LIMIT 10;".to_string(),
        ),
        (
            "join",
            "SELECT COUNT(*) FROM ga_olap JOIN ga_olap_dim ON ga_olap.g = ga_olap_dim.g;"
                .to_string(),
        ),
        (
            "multi_col_agg",
            "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), SUM(g) FROM ga_olap;".to_string(),
        ),
    ];

    let wall = Instant::now();
    let mut busy_secs = 0.0;
    let mut queries = Vec::with_capacity(specs.len());
    for (name, sql) in &specs {
        let started = Instant::now();
        let rows_out = match db.executor.execute(sql) {
            Ok(QueryResult::Rows { rows, .. }) => rows.len() as u64,
            Ok(other) => panic!("olap query {name} expected rows, got {other:?}"),
            Err(error) => panic!("olap query {name} failed: {error}"),
        };
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        busy_secs += latency_ms / 1000.0;
        let elapsed_secs = (latency_ms / 1000.0).max(f64::EPSILON);
        let scanned = rows as f64;
        queries.push(OlapQueryResult {
            name: (*name).to_string(),
            rows_per_sec: scanned / elapsed_secs,
            gb_per_sec: scanned * BYTES_PER_ROW / elapsed_secs / 1e9,
            latency_ms,
            rows_out,
        });
    }
    let wall_secs = wall.elapsed().as_secs_f64().max(f64::EPSILON);
    let cpu_util_pct = (100.0 * busy_secs / wall_secs).clamp(0.0, 100.0);
    let peak_rss = peak_rss_mb();

    db.teardown();
    OlapResult {
        queries,
        cpu_util_pct,
        peak_rss_mb: peak_rss,
    }
}
