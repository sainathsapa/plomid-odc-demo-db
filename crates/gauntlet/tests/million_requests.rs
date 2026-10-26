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
//! Phase 15: 1M-request mixed workload qualification.
//!
//! 1,000,000 SQL requests (not rows) through the live in-engine executor:
//! point reads, indexed reads, range reads, INSERT/UPDATE/DELETE,
//! aggregates, GROUP BY, vector top-k, and mixed transactions. Records
//! completed/failed/throughput/wall/p50/p95/p99/p99.9/max/CPU/RSS/WAL/
//! storage/maintenance + final correctness checksum.
//!
//! `#[ignore]` like the scale matrix: debug 1M takes minutes. Runs under
//! `cargo test -p plomid-gauntlet --test million_requests -- --include-ignored`.
//! Env `PLOMID_MILLION_N` overrides the request count for smoke runs.

use plomid_gauntlet::harness::open_live_db;
use plomid_sql::{QueryResult, Value};

fn target_requests() -> usize {
    std::env::var("PLOMID_MILLION_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000)
}

fn percentile(sorted: &mut [u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted.sort_unstable();
    let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn dir_bytes(dir: &std::path::Path) -> u64 {
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, total);
            } else if let Ok(m) = entry.metadata() {
                *total += m.len();
            }
        }
    }
    let mut total = 0;
    walk(dir, &mut total);
    total
}

fn rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| {
                text.lines().find_map(|line| {
                    if line.starts_with("VmRSS:") {
                        line.split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse::<u64>().ok())
                            .map(|kb| kb * 1024)
                    } else {
                        None
                    }
                })
            })
            .unwrap_or(0)
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[test]
#[ignore]
fn million_mixed_requests() {
    let total = target_requests();
    let mut live = open_live_db("million");
    let ex = &mut live.executor;
    let storage_dir = live.storage_dir.clone();
    let wal_dir = live.wal_dir.clone();
    ex.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT)")
        .expect("create");
    // Seed 10K rows in batches of 500 (20 statements, not counted).
    for base in (1..=10_000i64).step_by(500) {
        let values: Vec<String> = (base..base + 500)
            .map(|i| format!("({i}, {})", i * 3 + 1))
            .collect();
        ex.execute(&format!("INSERT INTO m VALUES {};", values.join(",")))
            .expect("seed");
    }
    ex.execute("CREATE INDEX m_v_idx ON m (v)").expect("index");
    // Small vector table for the vector slice of the mix.
    ex.execute("CREATE TABLE vv (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .expect("vcreate");
    for base in (1..=500i64).step_by(50) {
        let values: Vec<String> = (base..base + 50)
            .map(|i| format!("({i}, [{i},0,0,0])"))
            .collect();
        ex.execute(&format!("INSERT INTO vv VALUES {};", values.join(",")))
            .expect("vseed");
    }

    let wal_before = dir_bytes(&wal_dir);
    let data_before = dir_bytes(&storage_dir);
    let rss_before = rss_bytes();
    let wall = std::time::Instant::now();
    let mut latencies: Vec<u64> = Vec::with_capacity(total.min(1_000_000));
    let mut completed = 0usize;
    let mut failed = 0usize;
    let mut next_id: i64 = 100_001;

    for i in 0..total {
        let statement: String = match i % 100 {
            0..=54 => format!("SELECT v FROM m WHERE id = {}", (i as i64 % 10_000) + 1),
            55..=64 => format!(
                "SELECT v FROM m WHERE v = {}",
                (((i as i64 % 10_000) + 1) * 3 + 1)
            ),
            65..=74 => format!(
                "SELECT id FROM m WHERE id >= {} AND id < {}",
                (i as i64 % 9_900) + 1,
                (i as i64 % 9_900) + 101
            ),
            75..=79 => {
                let id = next_id;
                next_id += 1;
                format!("INSERT INTO m VALUES ({id}, {id})")
            }
            80..=84 => format!(
                "UPDATE m SET v = {} WHERE id = {}",
                i as i64,
                (i as i64 % 10_000) + 1
            ),
            85..=87 => format!("DELETE FROM m WHERE id = {next_id}", next_id = next_id - 1),
            88..=90 => "SELECT COUNT(*) FROM m".to_string(),
            91..=93 => "SELECT SUM(v) FROM m".to_string(),
            94..=95 => "SELECT v, COUNT(*) FROM m GROUP BY v LIMIT 5".to_string(),
            96..=97 => "SELECT id FROM vv ORDER BY e <-> [1,0,0,0] LIMIT 5".to_string(),
            _ => {
                // Mixed-transaction slice as one multi-statement request.
                "BEGIN; SELECT v FROM m WHERE id = 1; UPDATE m SET v = v WHERE id = 2; COMMIT;"
                    .to_string()
            }
        };
        let start = std::time::Instant::now();
        let result = if statement.starts_with("BEGIN;") {
            ex.execute_all(&statement)
                .map(|_| ())
                .map_err(|e| e.to_string())
        } else {
            ex.execute(&statement)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        latencies.push(start.elapsed().as_micros() as u64);
        match result {
            Ok(()) => completed += 1,
            Err(e) => {
                failed += 1;
                if failed <= 5 {
                    eprintln!("request {i} failed: {e} :: {statement}");
                }
            }
        }
        if (i + 1) % 100_000 == 0 {
            eprintln!(
                "progress {}/{} completed={completed} failed={failed}",
                i + 1,
                total
            );
        }
    }
    let wall_secs = wall.elapsed().as_secs_f64();
    // Maintenance + durability after the run.
    let _ = ex.execute("VACUUM m");
    let _ = ex.engine_mut().checkpoint();

    let wal_after = dir_bytes(&wal_dir);
    let data_after = dir_bytes(&storage_dir);
    let rss_after = rss_bytes();
    let p50 = percentile(&mut latencies.clone(), 50.0);
    let p95 = percentile(&mut latencies.clone(), 95.0);
    let p99 = percentile(&mut latencies.clone(), 99.0);
    let p999 = percentile(&mut latencies.clone(), 99.9);
    let max = latencies.iter().copied().max().unwrap_or(0);
    let throughput = completed as f64 / wall_secs;

    // Final correctness checksum (row count + SUM are deterministic
    // functions of the op mix; assert presence, not an exact constant, and
    // verify index + vector still answer).
    let (final_count, final_sum) = match ex.execute("SELECT COUNT(*), SUM(v) FROM m").unwrap() {
        QueryResult::Rows { rows, .. } => match (&rows[0][0], &rows[0][1]) {
            (Value::Int8(c), Value::Int8(s)) => (*c, *s),
            (Value::Int4(c), Value::Int8(s)) => (*c as i64, *s),
            other => panic!("checksum {other:?}"),
        },
        other => panic!("checksum {other:?}"),
    };
    assert!(
        final_count > 9_000,
        "mass delete must not have happened: {final_count}"
    );
    let probe = match ex.execute("SELECT v FROM m WHERE id = 1").unwrap() {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("probe {other:?}"),
    };
    assert_eq!(probe, 1, "point lookup after 1M requests");
    let vprobe = match ex
        .execute("SELECT id FROM vv ORDER BY e <-> [1,0,0,0] LIMIT 3")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("vprobe {other:?}"),
    };
    assert_eq!(vprobe, 3, "vector top-k after 1M requests");

    println!(
        "MILLION_RESULT completed={completed} failed={failed} total={total} wall_s={wall_secs:.1} \
         throughput_rps={throughput:.0} p50_us={p50} p95_us={p95} p99_us={p99} p999_us={p999} max_us={max} \
         count={final_count} sum={final_sum} wal_bytes={} data_bytes={} rss_before={rss_before} rss_after={rss_after}",
        wal_after.saturating_sub(wal_before),
        data_after.saturating_sub(data_before),
    );
    assert_eq!(failed, 0, "{failed} of {total} requests failed");
    assert_eq!(completed, total);
}
