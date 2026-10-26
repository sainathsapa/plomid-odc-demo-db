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
//! Query-level DoS amplifiers, in-process: `generate_series` output cap,
//! join-output row cap (nested-loop + hash + parallel hash), and the
//! linear-time LIKE path. No servers; every case runs against a scratch
//! engine and asserts graceful errors (never OOM / hangs).
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-joincap-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

fn setup() -> (Executor<PlomidStorageEngine>, PathBuf) {
    let dir = scratch("res");
    let wal = dir.with_extension("wal");
    let ex = Executor::new(PlomidStorageEngine::create(&dir, &wal, 32).expect("engine"))
        .expect("executor");
    (ex, dir)
}

fn teardown(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(dir.with_extension("wal"));
}

fn row_count(result: QueryResult) -> usize {
    match result {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn cartesian_with_limit_completes_fast() {
    let (mut ex, dir) = setup();
    // 300 x 300 = 90k join rows: under the cap, so the full output
    // materializes and LIMIT truncates it to 10 (previously — and still —
    // correct; the cap only rejects outputs past 10M rows).
    let rows = ex
        .execute(
            "SELECT * FROM generate_series(1, 300) a CROSS JOIN generate_series(1, 300) b LIMIT 10",
        )
        .expect("bounded cartesian with LIMIT must succeed");
    assert_eq!(row_count(rows), 10);
    teardown(&dir);
}

#[test]
fn huge_cartesian_errors_gracefully_not_oom() {
    let (mut ex, dir) = setup();
    // 100k x 100k = 10^10 rows: the exact-size cartesian pre-check rejects
    // before allocating a single output row.
    let err = ex
        .execute(
            "SELECT * FROM generate_series(1, 100000) a CROSS JOIN generate_series(1, 100000) b",
        )
        .expect_err("uncapped cartesian must fail closed");
    assert!(
        err.to_string().contains("exceeds the maximum"),
        "unexpected error: {err}"
    );
    teardown(&dir);
}

#[test]
fn selective_large_join_succeeds_no_false_positive() {
    let (mut ex, dir) = setup();
    // 3000 distinct keys per side: 3000 output rows, but 6000 input rows
    // engage the parallel hash path. The cap bounds OUTPUT, so a selective
    // join of large inputs must still succeed.
    ex.execute("CREATE TABLE s1 (id BIGINT PRIMARY KEY, v INT)")
        .expect("ddl");
    ex.execute("CREATE TABLE s2 (id BIGINT PRIMARY KEY, v INT)")
        .expect("ddl");
    for table in ["s1", "s2"] {
        let vals: Vec<String> = (1..=3000).map(|i| format!("({i},{})", i % 7)).collect();
        ex.execute(&format!("INSERT INTO {table} VALUES {}", vals.join(",")))
            .expect("seed");
    }
    let rows = ex
        .execute("SELECT * FROM s1 JOIN s2 ON s1.id = s2.id")
        .expect("selective join under the cap must succeed");
    assert_eq!(row_count(rows), 3000);
    teardown(&dir);
}

#[test]
fn generate_series_over_cap_errors_in_all_shapes() {
    let (mut ex, dir) = setup();
    // Target-list shape: SELECT generate_series(1, 1000001).
    let err = ex
        .execute("SELECT generate_series(1, 1000001)")
        .expect_err("oversize series must fail closed");
    assert!(
        err.to_string().contains("exceeds the maximum"),
        "unexpected error: {err}"
    );
    // FROM shape: SELECT * FROM generate_series(1, 1000001) g.
    let err = ex
        .execute("SELECT * FROM generate_series(1, 1000001) g")
        .expect_err("oversize series must fail closed");
    assert!(
        err.to_string().contains("exceeds the maximum"),
        "unexpected error: {err}"
    );
    // Just under the cap still works (10k rows keeps the test fast while
    // proving small series are unaffected).
    let rows = ex
        .execute("SELECT * FROM generate_series(1, 10000) g")
        .expect("small series must succeed");
    assert_eq!(row_count(rows), 10000);
    teardown(&dir);
}

#[test]
fn like_adversarial_pattern_completes_with_right_answer() {
    let (mut ex, dir) = setup();
    // 25 `%`-separated segments: exponential backtracking would hang; the
    // linear-time path answers instantly. Bare SELECT routes to the join
    // engine (not the sibling-owned single-table query path).
    let pattern = format!("{}b", "a%".repeat(25));
    let non_matching = "a".repeat(5000) + "c";
    let matching = "a".repeat(5000) + "b";
    let check = |ex: &mut Executor<PlomidStorageEngine>, input: &str| -> bool {
        match ex
            .execute(&format!("SELECT '{input}' LIKE '{pattern}'"))
            .expect("LIKE must evaluate")
        {
            QueryResult::Rows { rows, .. } => match rows[0][0] {
                Value::Bool(b) => b,
                ref other => panic!("expected bool, got {other:?}"),
            },
            other => panic!("expected rows, got {other:?}"),
        }
    };
    assert!(!check(&mut ex, &non_matching));
    assert!(check(&mut ex, &matching));
    // Exact-match fast path and basics still behave.
    match ex.execute("SELECT 'hello' LIKE 'h_llo'").expect("like") {
        QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Bool(true)),
        other => panic!("expected rows, got {other:?}"),
    }
    match ex.execute("SELECT 'hello' LIKE 'h%z'").expect("like") {
        QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Bool(false)),
        other => panic!("expected rows, got {other:?}"),
    }
    teardown(&dir);
}
