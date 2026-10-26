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
//! PERF-A baseline: end-to-end query shapes through the public SQL surface.
//!
//! Run with: `cargo run --release -p plomid-executor --bench query_shapes`
//! (or `cargo bench -p plomid-executor --bench query_shapes`).
//!
//! Shapes (10k-row `events(id, group_id, amount)` + 100-row `teams` dim):
//! point lookup, selective filter scan, group-by aggregation, equi-join.
//! Reports mean latency, result rows/s, scanned rows/s and ns/scanned-row.

use plomid_executor::Executor;
use plomid_sql::QueryResult;
use plomid_txn::PlomidStorageEngine;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const ROWS: usize = 10_000;

fn root(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "plomid-bench-shapes-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn seed_sized(rows: usize) -> Executor<PlomidStorageEngine> {
    let storage = root(&format!("seed-{rows}"));
    let wal = storage.with_extension("wal");
    let engine = PlomidStorageEngine::create(&storage, &wal, 64).expect("engine");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("CREATE TABLE events (id INTEGER, group_id INTEGER, amount INTEGER);")
        .expect("table");
    executor
        .execute("CREATE TABLE teams (group_id INTEGER, label TEXT);")
        .expect("dim");
    let dim = (0..100)
        .map(|g| format!("({g}, 't-{g:03}')"))
        .collect::<Vec<_>>()
        .join(",");
    executor
        .execute(&format!("INSERT INTO teams VALUES {dim};"))
        .expect("dim rows");
    for start in (0..rows).step_by(500) {
        let end = (start + 500).min(rows);
        let values = (start..end)
            .map(|id| format!("({}, {}, {})", id, id % 100, id % 1000))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO events VALUES {values};"))
            .expect("insert");
    }
    executor
}

fn seed() -> Executor<PlomidStorageEngine> {
    seed_sized(ROWS)
}

fn result_rows(result: &QueryResult) -> usize {
    match result {
        QueryResult::Rows { rows, .. } => rows.len(),
        _ => 0,
    }
}

/// Time `iters` executions; report mean latency plus throughput against both
/// result rows and scanned input rows.
fn bench_shape(
    executor: &mut Executor<PlomidStorageEngine>,
    name: &str,
    sql: &str,
    scanned_per_iter: usize,
    iters: u64,
) {
    // Warmup (plan caches, page caches) — not timed.
    let first = executor.execute(sql).expect("warmup query");
    let out_rows = result_rows(&first);
    let mut total = std::time::Duration::ZERO;
    for _ in 0..iters {
        let start = Instant::now();
        let out = executor.execute(sql).expect("bench query");
        std::hint::black_box(result_rows(&out));
        total += start.elapsed();
    }
    let mean_ms = total.as_secs_f64() * 1000.0 / iters as f64;
    let total_scanned = scanned_per_iter as u64 * iters;
    let total_out = out_rows as u64 * iters;
    let secs = total.as_secs_f64();
    let scanned_per_sec = total_scanned as f64 / secs;
    let out_per_sec = total_out as f64 / secs;
    let ns_per_scanned = secs * 1e9 / total_scanned as f64;
    println!(
        "  {name:.<28} out={out_rows:>6} rows | mean {mean_ms:>10.3} ms/it | \
out {out_per_sec:>12.0} rows/s | scanned {scanned_per_sec:>12.0} rows/s | \
{ns_per_scanned:>10.0} ns/scanned-row"
    );
}

fn main() {
    println!("PLOMID query-shape baseline ({ROWS} fact rows, 100 dim rows)");
    let mut executor = seed();
    let iters = 10u64;
    bench_shape(
        &mut executor,
        "point_lookup",
        "SELECT id, amount FROM events WHERE id = 5000;",
        ROWS,
        iters,
    );
    bench_shape(
        &mut executor,
        "filter_scan",
        "SELECT id, amount FROM events WHERE amount >= 900;",
        ROWS,
        iters,
    );
    bench_shape(
        &mut executor,
        "group_by_agg",
        "SELECT group_id, COUNT(*), SUM(amount) FROM events GROUP BY group_id;",
        ROWS,
        iters,
    );
    bench_shape(
        &mut executor,
        "equi_join",
        "SELECT events.id, teams.label FROM events JOIN teams ON events.group_id = teams.group_id;",
        ROWS,
        iters,
    );
    println!("Benchmarks complete.");
    println!();
    println!("--- scale check: 1k vs 10k rows (fixed vs per-row cost) ---");
    for rows in [1_000usize, ROWS] {
        let mut small = seed_sized(rows);
        println!("table size: {rows} rows");
        bench_shape(
            &mut small,
            "point_lookup",
            "SELECT id, amount FROM events WHERE id = 500;",
            rows,
            iters,
        );
        bench_shape(
            &mut small,
            "filter_scan",
            "SELECT id, amount FROM events WHERE amount >= 900;",
            rows,
            iters,
        );
    }
    println!();
    println!("Scale check complete.");
}
