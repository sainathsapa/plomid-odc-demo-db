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
//! Asserted scale matrix (F-11): 1K / 10K / 100K / 300K rows.
//!
//! Each point bulk-loads through the live executor, then proves
//! correctness (exact counts, spot-checked values, exact range widths)
//! and records latency distributions. The gates are:
//!
//! * exactness at every point (no silent loss, no wrong rows);
//! * no superlinear blowup across points (per-row load cost and PK p99
//!   degrade at most linearly-ish; COUNT may scale with N and is capped
//!   at 2x headroom over linear);
//! * generous absolute ceilings that catch hangs, sized for debug CI.
//!
//! Numbers print as `RESULT` lines for dashboards; assertions are the
//! gate. Deterministic data (`v = id*7+3`); no randomness anywhere.
//!
//! Maintenance pacing: bulk load runs with an effectively infinite
//! mutation bound and one explicit `VACUUM` afterwards, mirroring
//! production (background worker + interval gate). The default test
//! policy materializes a full generation every 64 mutations with no
//! interval gate, which turns bulk load into O(N²) rewrites and would
//! measure the maintenance policy, not engine write scaling (observed:
//! ~12x per-row load growth 1K→100K unpaced in debug). Maintenance
//! behavior itself is covered by the vacuum/background suites.
//!
//! Measured baseline (release, paced): per-row load 0.17→0.15→0.17→
//! 0.18ms (flat), PK p99 ≤0.08ms, range ~5k/sec, COUNT 0.5→3.3ms for
//! 300x rows, bytes/row 1126→425 (fixed costs amortize). Unpaced, the
//! same engine shows ~12x per-row load growth 1K→100K in debug from
//! repeated full-table passes — the pacing above is what makes this a
//! write-path gate rather than a policy measurement.
//!
//! Scale-gated (`#[ignore]`): the 100K/300K points take minutes in debug
//! builds. Runs under `make test-scale` / CI scale jobs
//! (`--include-ignored`), never in the default unit loop.

use plomid_executor::MaintenancePolicy;
use plomid_gauntlet::harness::open_live_db;
use plomid_gauntlet::metrics::lat_stats;
use plomid_sql::{QueryResult, Value};

const POINTS: [usize; 4] = [1_000, 10_000, 100_000, 300_000];
const BATCH: usize = 500;

fn row_value(id: i64) -> i64 {
    id.wrapping_mul(7).wrapping_add(3)
}

fn dir_size_bytes(dir: &std::path::Path) -> u64 {
    fn walk(dir: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
            } else if let Ok(meta) = entry.metadata() {
                *total += meta.len();
            }
        }
    }
    let mut total = 0u64;
    walk(dir, &mut total);
    total
}

fn scalar_i64(result: QueryResult) -> i64 {
    match result {
        QueryResult::Rows { rows, .. } => match rows[0][0] {
            Value::Int8(n) => n,
            Value::Int4(n) => n as i64,
            Value::Int2(n) => n as i64,
            ref other => panic!("expected int scalar, got {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

struct PointReport {
    rows: usize,
    load_secs: f64,
    load_rps: f64,
    bytes_per_row: f64,
    pk_p50_ms: f64,
    pk_p99_ms: f64,
    pk_max_ms: f64,
    range_rps: f64,
    count_ms: f64,
}

fn run_point(rows: usize) -> PointReport {
    let mut db = open_live_db("matrix");
    db.exec("CREATE TABLE sm (id BIGINT PRIMARY KEY, v BIGINT);");
    // Pace maintenance like production: no automatic full-table passes
    // during the load (see module docs); one explicit VACUUM after.
    db.executor
        .set_maintenance_policy(MaintenancePolicy::new(u64::MAX));

    // Bulk load in multi-row batches (single-row INSERTs would make the
    // 300K point a CI timeout, not a scale gate).
    let load_start = std::time::Instant::now();
    let mut start = 0usize;
    while start < rows {
        let end = (start + BATCH).min(rows);
        let mut sql = String::from("INSERT INTO sm VALUES ");
        for (k, id) in (start..end).enumerate() {
            if k > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({id}, {})", row_value(id as i64)));
        }
        db.exec(&sql);
        start = end;
    }
    let load_secs = load_start.elapsed().as_secs_f64();
    let load_rps = rows as f64 / load_secs.max(f64::EPSILON);
    db.exec("VACUUM sm;");

    // Exactness: full count plus spot-checked values across the keyspace.
    let count = scalar_i64(
        db.executor
            .execute("SELECT COUNT(*) FROM sm;")
            .expect("count"),
    );
    assert_eq!(count, rows as i64, "exact row count at {rows} rows");
    for probe in [0, 1, rows / 2, rows - 1] {
        let got = scalar_i64(
            db.executor
                .execute(&format!("SELECT v FROM sm WHERE id = {probe};"))
                .expect("spot read"),
        );
        assert_eq!(got, row_value(probe as i64), "value at id {probe}");
    }

    // PK latency distribution: 50 timed probes spread across the keyspace.
    let keyspace = rows.max(1);
    let pk_start = std::time::Instant::now();
    let mut pk_ms = Vec::with_capacity(50);
    for s in 0..50 {
        let id = (s * 7919) % keyspace;
        pk_ms.push(db.timed_exec(&format!("SELECT * FROM sm WHERE id = {id};")));
    }
    let pk = lat_stats(pk_ms, pk_start.elapsed().as_secs_f64());

    // Range scans: exact widths (100 rows, or the tail remainder).
    let range_start = std::time::Instant::now();
    let mut range_ms = Vec::with_capacity(20);
    for s in 0..20 {
        let lo = (s * 104729) % keyspace;
        let expected = ((lo + 100).min(rows) as i64 - lo as i64).max(0);
        let got = scalar_i64(
            db.executor
                .execute(&format!(
                    "SELECT COUNT(*) FROM sm WHERE id >= {lo} AND id < {};",
                    lo + 100
                ))
                .expect("range count"),
        );
        assert_eq!(got, expected, "range width at lo={lo}");
        range_ms.push(db.timed_exec(&format!(
            "SELECT * FROM sm WHERE id >= {lo} AND id < {};",
            lo + 100
        )));
    }
    let range = lat_stats(range_ms, range_start.elapsed().as_secs_f64());

    let count_ms = db.timed_exec("SELECT COUNT(*) FROM sm;");
    let bytes_per_row = dir_size_bytes(&db.storage_dir) as f64 / rows as f64;
    println!(
        "RESULT point={rows} load_rps={load_rps:.0} pk_p50_ms={:.3} pk_p99_ms={:.3} \
         pk_max_ms={:.3} range_rps={:.0} count_ms={count_ms:.1} bytes_per_row={bytes_per_row:.0}",
        pk.p50_ms, pk.p99_ms, pk.max_ms, range.throughput_rps,
    );

    db.teardown();
    PointReport {
        rows,
        load_secs,
        load_rps,
        bytes_per_row,
        pk_p50_ms: pk.p50_ms,
        pk_p99_ms: pk.p99_ms,
        pk_max_ms: pk.max_ms,
        range_rps: range.throughput_rps,
        count_ms,
    }
}

#[test]
#[ignore = "scale gate: minutes in debug; runs under make test-scale / CI scale jobs"]
fn scale_matrix_1k_10k_100k_300k() {
    let mut reports = Vec::with_capacity(POINTS.len());
    for &rows in &POINTS {
        reports.push(run_point(rows));
    }

    // Absolute ceilings (hang protection, generous for debug CI): no
    // single probe may take longer than 30 s at any point.
    for report in &reports {
        assert!(
            report.pk_p50_ms <= report.pk_p99_ms,
            "p50 above p99 at {} rows: distribution corrupt",
            report.rows
        );
        assert!(
            report.pk_max_ms < 30_000.0,
            "pk hang at {} rows: max {:.1} ms",
            report.rows,
            report.pk_max_ms
        );
        assert!(
            report.count_ms < 120_000.0,
            "count hang at {} rows: {:.1} ms",
            report.rows,
            report.count_ms
        );
    }

    // No superlinear blowup: per-row load cost and PK p99 must not grow
    // faster than the row count does (5x headroom over the 1K baseline
    // per-row rate covers log-factor growth with margin).
    let base = &reports[0];
    let base_per_row_load = base.load_secs / base.rows as f64;
    for report in &reports[1..] {
        let per_row = report.load_secs / report.rows as f64;
        assert!(
            per_row <= base_per_row_load * 5.0,
            "superlinear load at {} rows: {per_row:.6}s/row vs baseline {base_per_row_load:.6}s/row",
            report.rows
        );
        assert!(
            report.pk_p99_ms <= base.pk_p99_ms.max(1.0) * 10.0 + 100.0,
            "pk p99 blowup at {} rows: {:.2} ms vs baseline {:.2} ms",
            report.rows,
            report.pk_p99_ms,
            base.pk_p99_ms
        );
    }

    // COUNT(*) is O(N): allow linear scaling with 2x headroom, which
    // catches quadratic-or-worse blowups and nothing else.
    let base_count = base.count_ms.max(0.5);
    for report in &reports[1..] {
        let linear = base_count * (report.rows as f64 / base.rows as f64);
        assert!(
            report.count_ms <= linear * 2.0 + 5_000.0,
            "count blowup at {} rows: {:.1} ms vs linear budget {:.1} ms",
            report.rows,
            report.count_ms,
            linear * 2.0
        );
    }

    // Every point moved real data: guards against vacuous passes where
    // a point silently loads nothing.
    for report in &reports {
        assert!(report.load_rps > 0.0);
        assert!(report.bytes_per_row > 0.0);
        assert!(report.range_rps > 0.0);
    }
}
