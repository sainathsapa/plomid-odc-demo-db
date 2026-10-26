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
//! EDGE-B tier gating, end to end.
//!
//! Every test pins `PLOMID_PROFILE=edge` as its first statement (the same
//! value in all tests, so any libtest thread order is safe — tier detection
//! runs once per process). Through public SQL this proves that on the Edge
//! tier:
//!
//! * a 3_000-row hashable equi-INNER join DECLINES the parallel path
//!   (threshold 8_000; the `parallel_join` observability counter stays 0),
//! * while returning exactly the sequential multiset (hand-computed
//!   count/sum fingerprints plus a sorted row compare across two runs).
//!
//! Fallback equivalence in the other direction (parallel engages and
//! matches) is covered by `par_join_parallel` on standard-class hosts and
//! by the in-crate `tiered_join_threshold_gates_per_tier` unit test, which
//! pins all three tiers in-process.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};

/// Probe rows (matches the `par_join_parallel` generator so fingerprints
/// stay comparable): NULL every 11th key, else `i % 500`.
const PROBE_N: i64 = 2_500;
/// Build-side row count (`jr.k = 0..500`).
const JR_N: i64 = 500;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 1_000;

/// Pin the Edge tier. Must run before any executor work in each test (tier
/// detection is once-per-process; every test in this binary wants Edge, so
/// any execution order converges on the same tier).
fn pin_edge_tier() {
    std::env::set_var("PLOMID_PROFILE", "edge");
}

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("plomid-tiergate-{tag}-{id}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &Path) -> Executor<PlomidStorageEngine> {
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(root, &wal, 64))
        .expect("open engine");
    Executor::new(engine).expect("executor")
}

fn rows_of(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn query(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    rows_of(
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {sql}: {e:?}")),
    )
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

/// Order-insensitive row fingerprint for multiset comparison.
fn row_key(row: &[Value]) -> String {
    row.iter()
        .map(|v| v.to_sql_text())
        .collect::<Vec<_>>()
        .join("|")
}

/// Times the `parallel_join` analytical path fired in this process.
fn parallel_join_count() -> u64 {
    plomid_core::observability::snapshot()
        .analytical_by_path
        .into_iter()
        .find(|(path, _)| path == "parallel_join")
        .map(|(_, count)| count)
        .unwrap_or(0)
}

fn load_tables(session: &mut Executor<PlomidStorageEngine>) -> (i64, i64) {
    session
        .execute("CREATE TABLE jl (id BIGINT PRIMARY KEY, k INT, v BIGINT);")
        .unwrap();
    session
        .execute("CREATE TABLE jr (k INT PRIMARY KEY, w TEXT);")
        .unwrap();
    let mut batch = String::from("INSERT INTO jl VALUES ");
    let (mut expect_count, mut expect_sum) = (0, 0);
    let mut in_batch = 0;
    for i in 0..PROBE_N {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        if i % 11 == 0 {
            batch.push_str(&format!("({i}, NULL, {i})"));
        } else {
            batch.push_str(&format!("({i}, {}, {i})", i % 500));
            expect_count += 1;
            expect_sum += i;
        }
        in_batch += 1;
        if in_batch == BATCH || i + 1 == PROBE_N {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = String::from("INSERT INTO jl VALUES ");
            in_batch = 0;
        }
    }
    let mut batch = String::from("INSERT INTO jr VALUES ");
    for k in 0..JR_N {
        if k > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({k}, 'w{k}')"));
    }
    batch.push(';');
    session.execute(&batch).unwrap();
    (expect_count, expect_sum)
}

#[test]
fn edge_tier_declines_parallel_join_but_matches_sequential() {
    pin_edge_tier();
    let root = scratch("decline");
    let mut session = open(&root);
    let (expect_count, expect_sum) = load_tables(&mut session);

    plomid_core::observability::reset_for_tests();
    let rows = query(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        int(&rows[0][0]),
        expect_count,
        "declined join keeps the count"
    );
    assert_eq!(int(&rows[0][1]), expect_sum, "declined join keeps the sum");
    assert_eq!(
        parallel_join_count(),
        0,
        "3_000 total rows must decline on Edge (threshold 8_000)"
    );

    // Full multiset, twice, order-insensitive: the declined path returns
    // exactly what the sequential path computes.
    let mut first: Vec<String> = query(
        &mut session,
        "SELECT jl.k, jl.v, jr.k, jr.w FROM jl JOIN jr ON jl.k = jr.k;",
    )
    .iter()
    .map(|row| row_key(row))
    .collect();
    assert_eq!(first.len(), expect_count as usize);
    let mut second: Vec<String> = query(
        &mut session,
        "SELECT jl.k, jl.v, jr.k, jr.w FROM jl JOIN jr ON jl.k = jr.k;",
    )
    .iter()
    .map(|row| row_key(row))
    .collect();
    first.sort();
    second.sort();
    assert_eq!(first, second, "declined join is deterministic");
    assert_eq!(
        parallel_join_count(),
        0,
        "no parallel dispatch across either run"
    );
    cleanup(&root);
}

#[test]
fn edge_tier_small_join_stays_exact() {
    pin_edge_tier();
    let root = scratch("small");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE a (k INT PRIMARY KEY, v INT);")
        .unwrap();
    session
        .execute("CREATE TABLE b (k INT PRIMARY KEY, w TEXT);")
        .unwrap();
    session
        .execute("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30);")
        .unwrap();
    session
        .execute("INSERT INTO b VALUES (2, 'x'), (3, 'y'), (4, 'z');")
        .unwrap();
    plomid_core::observability::reset_for_tests();
    let mut rows: Vec<String> = query(
        &mut session,
        "SELECT a.k, a.v, b.w FROM a JOIN b ON a.k = b.k ORDER BY a.k;",
    )
    .iter()
    .map(|row| row_key(row))
    .collect();
    rows.sort();
    assert_eq!(rows.len(), 2);
    assert_eq!(parallel_join_count(), 0, "small join never fans out");
    cleanup(&root);
}
