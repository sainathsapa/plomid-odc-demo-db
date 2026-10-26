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

//! OLTP point-operation benchmark family, wired to the live engine path.
//!
//! Every statement below goes through [`plomid_executor::Executor::execute`]
//! (the `execute_all` choke point: governance admission plus result-buffer
//! charging), so the measured latencies include full production overhead.
//! IDs and payloads are deterministic (`id`-derived, no RNG, no sleeps) so
//! reruns are reproducible and CI stays fast (`seed_rows = 2000, iters = 50`
//! is the intended small default).

use crate::harness::{open_live_db, LiveDb};
use crate::metrics::{lat_stats, LatencyStats};
use plomid_sql::QueryResult;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Per-family latency rollups for the OLTP point-operation mix, plus the
/// post-run row-count proof that every write landed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OltpResult {
    pub insert: LatencyStats,
    pub update: LatencyStats,
    pub delete: LatencyStats,
    pub pk_select: LatencyStats,
    pub range_select: LatencyStats,
    pub mixed: LatencyStats,
    pub correctness_rows: u64,
}

/// Rows per seed INSERT statement (bounded literal size for the parser).
const SEED_BATCH: usize = 500;

/// Deterministic payload helpers so reruns are reproducible without RNG.
fn k_of(id: i64) -> i64 {
    id % 100
}

fn v_of(id: i64) -> i64 {
    id * 10
}

/// Inserts `count` deterministic rows starting at `start` in bounded batches.
fn batched_insert(db: &mut LiveDb, start: i64, count: usize) {
    let mut next = start;
    let end = start + count as i64;
    while next < end {
        let n = ((end - next) as usize).min(SEED_BATCH);
        let mut sql = String::from("INSERT INTO ga_oltp (id, k, v) VALUES ");
        for i in 0..n {
            if i > 0 {
                sql.push_str(", ");
            }
            let id = next + i as i64;
            sql.push_str(&format!("({id}, {}, {})", k_of(id), v_of(id)));
        }
        sql.push(';');
        db.exec(&sql);
        next += n as i64;
    }
}

/// Runs the OLTP families against a fresh live database and returns the
/// per-family latency stats.
///
/// Row-count arithmetic (all deterministic):
/// * seed: `seed_rows` rows (ids `1..=seed_rows`);
/// * insert family: `+iters` rows (ids `seed_rows+1..=seed_rows+iters`);
/// * update family: in place, net `0`;
/// * delete family: each DELETE is immediately re-inserted, net `0`;
/// * mixed family: each batch inserts exactly one fresh row (then selects
///   and updates it in the same autocommit batch), net `+iters`.
///
/// Expected final count is therefore `seed_rows + 2 * iters`; a mismatch
/// panics.
pub fn run_oltp(seed_rows: usize, iters: usize) -> OltpResult {
    let mut db = open_live_db("oltp");
    db.exec("CREATE TABLE ga_oltp (id BIGINT PRIMARY KEY, k INT, v BIGINT);");
    db.exec("CREATE INDEX ga_oltp_k_idx ON ga_oltp (k);");
    batched_insert(&mut db, 1, seed_rows);

    let seed = seed_rows as i64;
    let n = iters;

    // Single-row INSERTs with fresh ids; each adds one row.
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = seed + i as i64 + 1;
        samples.push(db.timed_exec(&format!(
            "INSERT INTO ga_oltp (id, k, v) VALUES ({id}, {}, {});",
            k_of(id),
            v_of(id)
        )));
    }
    let insert = lat_stats(samples, started.elapsed().as_secs_f64());

    // Rows now present: 1..=seed+n (seed + insert family). Spread point-op
    // targets deterministically across all of them.
    let spread = (seed + n as i64).max(1);
    let pick = |i: usize| 1 + (i as i64 * 7919 % spread);

    // Point UPDATEs by primary key (in place, net zero rows).
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = pick(i);
        samples.push(db.timed_exec(&format!(
            "UPDATE ga_oltp SET v = {} WHERE id = {id};",
            v_of(id) + 1
        )));
    }
    let update = lat_stats(samples, started.elapsed().as_secs_f64());

    // DELETEs of insert-family rows, each immediately re-inserted (untimed)
    // so the table size stays stable; only DELETE latency is sampled.
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = seed + i as i64 + 1;
        samples.push(db.timed_exec(&format!("DELETE FROM ga_oltp WHERE id = {id};")));
        db.exec(&format!(
            "INSERT INTO ga_oltp (id, k, v) VALUES ({id}, {}, {});",
            k_of(id),
            v_of(id)
        ));
    }
    let delete = lat_stats(samples, started.elapsed().as_secs_f64());

    // Point SELECTs by primary key.
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = pick(i);
        samples.push(db.timed_exec(&format!("SELECT v FROM ga_oltp WHERE id = {id};")));
    }
    let pk_select = lat_stats(samples, started.elapsed().as_secs_f64());

    // 100-row PK range scans with deterministic sliding lower bounds.
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let lo = 1 + (i as i64 * 131 % spread);
        let hi = lo + 100;
        samples.push(db.timed_exec(&format!(
            "SELECT id, v FROM ga_oltp WHERE id >= {lo} AND id < {hi};"
        )));
    }
    let range_select = lat_stats(samples, started.elapsed().as_secs_f64());

    // Mixed autocommit batches (INSERT + PK SELECT + UPDATE in one
    // multi-statement string, timed as a whole). Each batch nets exactly +1
    // row via a fresh id above the insert-family high-water mark.
    let started = Instant::now();
    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let id = seed + n as i64 + i as i64 + 1;
        let (k, v) = (k_of(id), v_of(id));
        samples.push(db.timed_exec(&format!(
            "INSERT INTO ga_oltp (id, k, v) VALUES ({id}, {k}, {v}); \
             SELECT v FROM ga_oltp WHERE id = {id}; \
             UPDATE ga_oltp SET v = {} WHERE id = {id}",
            v + 1
        )));
    }
    let mixed = lat_stats(samples, started.elapsed().as_secs_f64());

    // Deterministic correctness gate: seed + insert family + mixed batches.
    let expected = seed_rows as u64 + 2 * iters as u64;
    let counted = match db.executor.execute("SELECT COUNT(*) FROM ga_oltp;") {
        Ok(QueryResult::Rows { rows, .. }) => {
            let text = rows
                .first()
                .and_then(|row| row.first())
                .map(|value| value.to_sql_text())
                .unwrap_or_default();
            text.parse::<i64>().unwrap_or_else(|_| {
                panic!("oltp correctness: COUNT(*) text {text:?} is not an integer")
            }) as u64
        }
        Ok(other) => panic!("oltp correctness: expected rows, got {other:?}"),
        Err(error) => panic!("oltp correctness COUNT(*) failed: {error}"),
    };
    if counted != expected {
        panic!("oltp correctness mismatch: COUNT(*) = {counted}, expected {expected}");
    }

    db.teardown();
    OltpResult {
        insert,
        update,
        delete,
        pk_select,
        range_select,
        mixed,
        correctness_rows: counted,
    }
}
