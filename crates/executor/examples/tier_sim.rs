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
// PLOMID
//
// One-shot Edge-vs-Server tier simulation (EDGE-D measurement).
//
// Simulates the tier budgets siblings are implementing, using only STABLE
// public APIs so it survives mid-edit trees:
//   * `plomid-hw`  `HardwareProfile::{worker_budget, batch_rows,
//     is_small_device, vector_isa}` — Edge = 4 CPUs / 4 GiB, Server = 32+.
//   * `plomid-executor` `governance::claim_workers_pressure_aware` —
//     pressure-derated worker claims per tier want.
//   * `plomid-columnar` `flush` + `FlushConfig` — Edge = aggressive (Zstd
//     when the build provides it), Server = uncompressed/large.
//   * `plomid-executor` `Executor::execute` — identical join/scan/agg
//     fixture for both tiers (single-host codegen is tier-invariant;
//     tiers differ in budgets, footprint, and concurrency behavior).
//
// Reproduce exactly with:
//   cargo run -p plomid-executor --example tier_sim --release
//
// Prints a bench table (throughput + peak RSS per tier phase) to stdout.
// Not a criterion bench: one-shot numbers with a fixed seed fixture and
// repeat counts, so a reviewer can re-run one command and compare.

use std::sync::Mutex;
use std::time::Instant;

use plomid_columnar::{flush, ChunkCompression, ColumnType, FlushConfig};
use plomid_executor::{governance::claim_workers_pressure_aware, Executor};
use plomid_hw::HardwareProfile;
use plomid_storage::{Field, Row};
use plomid_txn::PlomidStorageEngine;

/// Rows in the shared SQL fixture (matches the relational bench's mid size).
const ROWS: usize = 100_000;
/// Timed repetitions per query (after warmup); p99 ~= max of these.
const REPEATS: usize = 21;
const WARMUP: usize = 3;
/// Rows flushed per columnar leg.
const FLUSH_ROWS: usize = 20_000;

/// Current process RSS in KiB via `ps` (macOS + Linux, std-only, no new deps).
fn rss_kib() -> u64 {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output();
    match out {
        Ok(o) => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

fn fresh_dir(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "plomid-tier-sim-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn seed(label: &str, rows: usize) -> Executor<PlomidStorageEngine> {
    let storage = fresh_dir(label);
    let wal = storage.with_extension("wal");
    let engine = PlomidStorageEngine::create(&storage, &wal, 64).expect("engine");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("CREATE TABLE events (id INTEGER, group_id INTEGER, amount INTEGER);")
        .expect("create events");
    executor
        .execute("CREATE TABLE teams (team_id INTEGER, weight INTEGER);")
        .expect("create teams");
    for start in (0..rows).step_by(500) {
        let end = (start + 500).min(rows);
        let values = (start..end)
            .map(|id| format!("({}, {}, {})", id, id % 100, id % 1000))
            .collect::<Vec<_>>()
            .join(",");
        executor
            .execute(&format!("INSERT INTO events VALUES {values};"))
            .expect("insert events");
    }
    let gvalues = (0..100)
        .map(|g| format!("({g}, {})", g * 7))
        .collect::<Vec<_>>()
        .join(",");
    executor
        .execute(&format!("INSERT INTO teams VALUES {gvalues};"))
        .expect("insert teams");
    executor
}

/// First-cell integer of a `COUNT(*)`-shaped single-row result.
fn scalar_i64(executor: &mut Executor<PlomidStorageEngine>, sql: &str) -> i64 {
    match executor.execute(sql).expect("query") {
        plomid_sql::QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "scalar query returned {} rows", rows.len());
            match &rows[0][0] {
                plomid_types::PgValue::Int8(v) => *v,
                plomid_types::PgValue::Int4(v) => i64::from(*v),
                plomid_types::PgValue::Int2(v) => i64::from(*v),
                other => panic!("unexpected scalar type: {other:?}"),
            }
        }
        other => panic!("scalar query returned non-rows: {other:?}"),
    }
}

/// Time one query `REPEATS` times after warmup. Returns (mean_ms, p50_ms, p99_ms).
fn time_query(executor: &mut Executor<PlomidStorageEngine>, sql: &str) -> (f64, f64, f64) {
    for _ in 0..WARMUP {
        executor.execute(sql).expect("warmup query");
    }
    let mut samples = Vec::with_capacity(REPEATS);
    for _ in 0..REPEATS {
        let t = Instant::now();
        let _ = executor.execute(sql).expect("timed query");
        samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() - 1];
    (mean, p50, p99)
}

fn main() {
    // ---- Tier budgets: derived from the detected host, overridden only in
    // ---- the two dimensions tiers actually vary (CPU count, RAM size).
    let host = HardwareProfile::detect();
    let edge = HardwareProfile {
        logical_cpus: 4,
        physical_cpus: Some(4),
        ram_total_bytes: Some(4 << 30),
        ram_available_bytes: Some(3 << 30),
        ..host.clone()
    };
    let server = HardwareProfile {
        logical_cpus: 32,
        physical_cpus: Some(16),
        ram_total_bytes: Some(64 << 30),
        ram_available_bytes: Some(48 << 30),
        ..host
    };

    println!("== tier budgets (plomid-hw HardwareProfile, same binary/host) ==");
    println!(
        "{:<8} {:>8} {:>12} {:>8} {:>10}",
        "tier", "workers", "batch_rows", "small?", "vector"
    );
    for (name, p) in [("edge", &edge), ("server", &server)] {
        println!(
            "{:<8} {:>8} {:>12} {:>8} {:>10}",
            name,
            p.worker_budget(),
            p.batch_rows(),
            p.is_small_device(),
            p.vector_isa(),
        );
    }
    assert_eq!(edge.worker_budget(), 4);
    assert!(edge.is_small_device());
    assert!(!server.is_small_device());
    assert!(server.worker_budget() >= 32);

    // ---- Pressure-derated worker claims through the REAL governance API
    // ---- (lease dropped immediately; no side effects on later phases).
    println!("\n== pressure-aware claims (governance::claim_workers_pressure_aware) ==");
    println!("want x pressure_level -> granted?");
    for (tier, want) in [
        ("edge", edge.worker_budget()),
        ("server", server.worker_budget()),
    ] {
        for level in 0..=3u8 {
            let granted = claim_workers_pressure_aware(want, level).is_some();
            println!("{tier:<8} want={want:<3} pressure={level} -> {granted}");
        }
    }

    // ---- Shared SQL fixture (identical for both tiers).
    let t = Instant::now();
    let mut executor = seed("shared", ROWS);
    let seed_ms = t.elapsed().as_secs_f64() * 1000.0;
    let rss_after_seed = rss_kib();
    println!("\n== fixture: events={ROWS} rows + teams=100 rows, seed={seed_ms:.0}ms ==");

    // ---- Columnar flush leg: tier compression policies, same rows.
    let flush_rows: Vec<Row> = (0..FLUSH_ROWS as i64)
        .map(|id| {
            Row::new(vec![
                Field::Integer(id),
                Field::Integer(id % 100),
                Field::Integer(id % 1000),
            ])
        })
        .collect();
    let flush_types = [
        ColumnType::Integer,
        ColumnType::Integer,
        ColumnType::Integer,
    ];
    let zstd_available = plomid_columnar::is_compression_available(ChunkCompression::Zstd);
    println!(
        "\n== columnar flush ({FLUSH_ROWS} rows x 3 int cols; zstd available: {zstd_available}) =="
    );
    println!("{:<8} {:>12} {:>10} {:>8}", "tier", "bytes", "ms", "ratio");
    let mut flush_peak = 0u64;
    // Server policy: default (uncompressed).
    let t = Instant::now();
    let server_flushed = flush(
        &flush_rows,
        &flush_types,
        plomid_core::GenerationId::new(1),
        plomid_core::SegmentId::new(1),
        &FlushConfig::default(),
    )
    .expect("server flush");
    let server_ms = t.elapsed().as_secs_f64() * 1000.0;
    flush_peak = flush_peak.max(rss_kib());
    // Edge policy: aggressive Zstd when the build provides it, else default.
    let edge_config = if zstd_available {
        FlushConfig::with_zstd()
    } else {
        FlushConfig::default()
    };
    let t = Instant::now();
    let edge_flushed = flush(
        &flush_rows,
        &flush_types,
        plomid_core::GenerationId::new(1),
        plomid_core::SegmentId::new(1),
        &edge_config,
    )
    .expect("edge flush");
    let edge_ms = t.elapsed().as_secs_f64() * 1000.0;
    flush_peak = flush_peak.max(rss_kib());
    // Determinism: same config twice => byte-identical (flush contract).
    let redo = flush(
        &flush_rows,
        &flush_types,
        plomid_core::GenerationId::new(1),
        plomid_core::SegmentId::new(1),
        &edge_config,
    )
    .expect("flush redo");
    assert_eq!(
        edge_flushed.bytes, redo.bytes,
        "flush must be deterministic"
    );
    let base = server_flushed.bytes.len() as f64;
    println!(
        "{:<8} {:>12} {:>10.1} {:>8.3}",
        "server",
        server_flushed.bytes.len(),
        server_ms,
        1.0
    );
    println!(
        "{:<8} {:>12} {:>10.1} {:>8.3}{}",
        "edge",
        edge_flushed.bytes.len(),
        edge_ms,
        edge_flushed.bytes.len() as f64 / base,
        if zstd_available {
            ""
        } else {
            "  (zstd unavailable: fell back to default)"
        },
    );

    // ---- Query leg: join/scan/agg throughput, same fixture both tiers.
    let expected_scan = (0..ROWS as i64).filter(|id| id % 1000 > 500).count() as i64;
    let scan_sql = "SELECT COUNT(*) FROM events WHERE amount > 500;";
    let agg_sql = "SELECT group_id, COUNT(*), SUM(amount) FROM events GROUP BY group_id;";
    let join_sql = "SELECT COUNT(*) FROM events JOIN teams ON events.group_id = teams.team_id;";
    assert_eq!(scalar_i64(&mut executor, scan_sql), expected_scan);
    assert_eq!(scalar_i64(&mut executor, join_sql), ROWS as i64);
    match executor.execute(agg_sql).expect("agg") {
        plomid_sql::QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 100, "100 groups expected");
        }
        other => panic!("agg returned non-rows: {other:?}"),
    }
    println!("\n== queries (same fixture; single-host codegen is tier-invariant) ==");
    println!(
        "{:<6} {:>10} {:>10} {:>10} {:>12}",
        "query", "mean_ms", "p50_ms", "p99_ms", "rows_per_s"
    );
    // %CPU sampler during the whole query leg (`ps` % = percent of ONE core).
    let cpu_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cpu_flag = cpu_stop.clone();
    let cpu_handle = std::thread::spawn(move || {
        let mut samples = Vec::new();
        let pid = std::process::id().to_string();
        while !cpu_flag.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(o) = std::process::Command::new("ps")
                .args(["-o", "%cpu=", "-p", &pid])
                .output()
            {
                if let Ok(v) = String::from_utf8_lossy(&o.stdout).trim().parse::<f64>() {
                    samples.push(v);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        samples
    });
    let mut query_peak = flush_peak;
    for (name, sql) in [("scan", scan_sql), ("agg", agg_sql), ("join", join_sql)] {
        let (mean, p50, p99) = time_query(&mut executor, sql);
        query_peak = query_peak.max(rss_kib());
        println!(
            "{:<6} {:>10.2} {:>10.2} {:>10.2} {:>12.0}",
            name,
            mean,
            p50,
            p99,
            ROWS as f64 / (mean / 1000.0)
        );
    }
    cpu_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let cpu_samples: Vec<f64> = cpu_handle.join().unwrap_or_default();
    if !cpu_samples.is_empty() {
        let mean = cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64;
        let max = cpu_samples.iter().cloned().fold(0.0f64, f64::max);
        println!(
            "query_leg_cpu_pct_of_one_core: mean={mean:.1} max={max:.1} (n={})",
            cpu_samples.len()
        );
    } else {
        println!("query_leg_cpu_pct_of_one_core: unavailable");
    }
    // Correctness is exact across repeats: re-verify after timing.
    assert_eq!(scalar_i64(&mut executor, scan_sql), expected_scan);
    assert_eq!(scalar_i64(&mut executor, join_sql), ROWS as i64);

    // ---- Interference leg: 8 threads x 1 agg through a shared executor
    // ---- (Mutex-serialized, same fixture) vs 8 sequential aggs.
    let shared = Mutex::new(executor);
    let t = Instant::now();
    for _ in 0..8 {
        shared
            .lock()
            .unwrap()
            .execute(agg_sql)
            .expect("sequential agg");
    }
    let seq_ms = t.elapsed().as_secs_f64() * 1000.0;
    let t = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                shared
                    .lock()
                    .unwrap()
                    .execute(agg_sql)
                    .expect("concurrent agg");
            });
        }
    });
    let conc_ms = t.elapsed().as_secs_f64() * 1000.0;
    let interference_peak = rss_kib().max(query_peak);
    println!("\n== interference (8x agg: sequential vs 8 threads, shared executor) ==");
    println!("sequential_8x_ms={seq_ms:.1} concurrent_8x_ms={conc_ms:.1}");

    println!("\n== peak RSS (KiB, process-wide; phases share one process) ==");
    println!("after_seed={rss_after_seed} flush_phase_peak={flush_peak} query_phase_peak={query_peak} end_peak={interference_peak}");
    println!("\nDONE tier_sim rows={ROWS} repeats={REPEATS}");
}
