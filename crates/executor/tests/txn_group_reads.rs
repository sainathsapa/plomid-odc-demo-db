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
//! F-03: explicit batch transactions behave like PostgreSQL.
//!
//! The embedded batch path (`BEGIN; ...; COMMIT` in one call) executes the
//! staged group once inside a single engine transaction. This proves the
//! two historical gaps are closed:
//!
//! * mid-transaction reads: single-table SELECT sees staged writes
//!   (read-your-own-writes), honors WHERE/ORDER BY/LIMIT, and answers
//!   bare `COUNT(*)` — matching the autocommit path row-for-row;
//! * per-statement results: every statement's own result is returned in
//!   order (INSERT counts, SELECT rows, `Committed`), nothing silently
//!   dropped.
//!
//! Shapes that still need the general engine (joins, subqueries,
//! aggregates beyond COUNT, GROUP BY, DISTINCT, window, CTEs, set ops)
//! keep the explicit Unsupported error and abort the group atomically.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn scratch(label: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("plomid-txn-group-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &std::path::Path) -> Executor<PlomidStorageEngine> {
    let engine = PlomidStorageEngine::open(root, &root.with_extension("wal"), 32).expect("mount");
    Executor::new(engine).expect("executor")
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter().map(|row| int(&row[0])).collect()
}

#[test]
fn mid_transaction_select_sees_staged_writes() {
    let root = scratch("ryow");
    let mut ex = open(&root);
    ex.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    ex.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    // Staged INSERT + UPDATE + DELETE are all visible to the mid-txn read,
    // which honors WHERE / ORDER BY / LIMIT like the autocommit path.
    let out = ex
        .execute_all(
            "BEGIN; INSERT INTO t VALUES (3, 30); UPDATE t SET v = 99 WHERE id = 1; \
             DELETE FROM t WHERE id = 2; SELECT id, v FROM t ORDER BY id DESC LIMIT 2; COMMIT;",
        )
        .unwrap();
    // Per-statement results: INSERT count, UPDATE count, DELETE count,
    // SELECT rows, COMMIT tag — nothing dropped.
    assert_eq!(out.len(), 5, "{out:?}");
    let rows = match &out[3] {
        QueryResult::Rows { rows, .. } => rows.clone(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(ids(&rows), vec![3, 1]);
    assert_eq!(int(&rows[0][1]), 30);
    assert_eq!(int(&rows[1][1]), 99);
    assert!(matches!(out[4], QueryResult::Committed));

    // Committed state matches what the transaction observed.
    let after = ex.execute("SELECT id FROM t ORDER BY id").unwrap();
    let rows = match after {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(ids(&rows), vec![1, 3]);

    cleanup(&root);
}

#[test]
fn mid_transaction_read_matches_autocommit_row_for_row() {
    let root = scratch("parity");
    let mut ex = open(&root);
    ex.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT, w TEXT)")
        .unwrap();
    ex.execute("INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, NULL, 'd')")
        .unwrap();

    // Same query through the group path and the autocommit path: identical
    // rows, columns, and order — including NULL ordering and expressions.
    let sql =
        "SELECT id, v + 1 AS bump, w FROM t WHERE v >= 10 OR v IS NULL ORDER BY v DESC, id LIMIT 3";
    let grouped = ex.execute_all(&format!("BEGIN; {sql}; COMMIT;")).unwrap();
    let grouped_rows = match &grouped[0] {
        QueryResult::Rows { rows, .. } => rows.clone(),
        other => panic!("{other:?}"),
    };
    let plain = ex.execute(sql).unwrap();
    let (plain_rows, plain_cols) = match plain {
        QueryResult::Rows { rows, columns, .. } => (rows, columns),
        other => panic!("{other:?}"),
    };
    assert_eq!(grouped_rows, plain_rows);
    let grouped_cols = match &grouped[0] {
        QueryResult::Rows { columns, .. } => columns.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(grouped_cols, plain_cols);

    cleanup(&root);
}

#[test]
fn rollback_hides_staged_rows_from_mid_txn_reads_after_reopen() {
    let root = scratch("rollback");
    let mut ex = open(&root);
    ex.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    // The mid-txn read sees the staged row, then ROLLBACK discards it.
    let out = ex
        .execute_all("BEGIN; INSERT INTO t VALUES (1, 10); SELECT COUNT(*) FROM t; ROLLBACK;")
        .unwrap();
    let count = match &out[1] {
        QueryResult::Rows { rows, .. } => int(&rows[0][0]),
        other => panic!("{other:?}"),
    };
    assert_eq!(count, 1);
    assert!(matches!(out[2], QueryResult::RolledBack));
    // Nothing survived: reopened state is empty and COUNT agrees.
    drop(ex);
    let mut ex = open(&root);
    let after = ex.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = match after {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(int(&rows[0][0]), 0);
    cleanup(&root);
}

#[test]
fn unsupported_shapes_still_fail_the_whole_group_atomically() {
    let root = scratch("unsupported");
    let mut ex = open(&root);
    ex.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    ex.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, v BIGINT)")
        .unwrap();
    ex.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // A join inside the group fails closed...
    let err = ex
        .execute_all(
            "BEGIN; INSERT INTO t VALUES (2, 20); SELECT * FROM t JOIN u ON t.id = u.id; COMMIT;",
        )
        .expect_err("joins in-txn must fail");
    let message = format!("{err:?}");
    assert!(
        message.contains("not yet supported"),
        "explicit contract preserved: {message}"
    );
    // ...and atomically: the staged INSERT did not survive.
    let after = ex.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = match after {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(int(&rows[0][0]), 1);
    cleanup(&root);
}
