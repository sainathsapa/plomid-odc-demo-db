//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
//
// Regression tests for columnar projection narrowing (vector-feed fix).
//
// `columnar_requested_columns` used to force all-columns materialization
// for any aggregate target. It now narrows to the exact input set
// (aggregate args + predicate + order/group/having/distinct columns)
// while still declining session/window shapes entirely. These tests pin
// the danger shapes: unprojected GROUP BY keys (narrowing must not
// starve grouping), aggregate-only targets, predicates on unprojected
// columns, and session-function decline.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn unique_dirs(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let storage =
        std::env::temp_dir().join(format!("plomid-colproj-{tag}-{id}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-colproj-{tag}-{id}-wal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    (storage, wal)
}

fn exec<E: plomid_txn::StorageEngine>(executor: &mut Executor<E>, sql: &str) -> Vec<Vec<Value>> {
    match executor
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql} should execute: {error}"))
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn text_cols(rows: &[Vec<Value>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    other => other.to_sql_text(),
                })
                .collect()
        })
        .collect()
}

fn setup() -> Executor<PlomidStorageEngine> {
    let (storage, wal) = unique_dirs("setup");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).expect("create");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute(
            "CREATE TABLE t(id BIGINT PRIMARY KEY, g INTEGER NOT NULL, v BIGINT,
             w DOUBLE PRECISION NOT NULL, name TEXT NOT NULL, pad TEXT NOT NULL)",
        )
        .expect("create table");
    // 2000 rows across 4 groups; every 10th v is NULL; wide pad column
    // makes over-reading observable in timings, correctness here.
    for lo in (0..2000).step_by(500) {
        let values: Vec<String> = (lo..lo + 500)
            .map(|i| {
                let v = if i % 10 == 0 {
                    "NULL".to_string()
                } else {
                    (i * 3).to_string()
                };
                format!("({i},{},{v},{}.5,'n{i}','pad-pad-pad-{i}')", i % 4, i)
            })
            .collect();
        executor
            .execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
            .expect("insert");
    }
    executor.execute("VACUUM t").expect("vacuum");
    executor
}

#[test]
fn aggregate_only_targets_stay_exact() {
    let mut executor = setup();
    let rows = text_cols(&exec(&mut executor, "SELECT SUM(v) FROM t"));
    // v = 3i for i not divisible by 10, i in 0..2000.
    let expect: i64 = (0..2000).filter(|i| i % 10 != 0).map(|i| i * 3).sum();
    assert_eq!(rows, vec![vec![expect.to_string()]]);
    let rows = text_cols(&exec(&mut executor, "SELECT COUNT(*) FROM t WHERE g = 2"));
    assert_eq!(rows, vec![vec!["500".to_string()]]);
}

#[test]
fn unprojected_group_key_groups_exactly() {
    // The group key is NOT in the targets: narrowing must still
    // materialize it or grouping silently corrupts. 4 groups x 500.
    let mut executor = setup();
    let rows = text_cols(&exec(
        &mut executor,
        "SELECT COUNT(*) FROM t GROUP BY g ORDER BY COUNT(*) DESC",
    ));
    // ORDER BY on aggregates routes to the general engine; counts equal.
    assert_eq!(rows.len(), 4);
    for row in &rows {
        assert_eq!(row, &vec!["500".to_string()]);
    }
}

#[test]
fn projected_group_key_with_predicate_stays_exact() {
    let mut executor = setup();
    let rows = text_cols(&exec(
        &mut executor,
        "SELECT g, COUNT(*) FROM t WHERE g >= 1 GROUP BY g ORDER BY g",
    ));
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "500".to_string()],
            vec!["2".to_string(), "500".to_string()],
            vec!["3".to_string(), "500".to_string()],
        ]
    );
}

#[test]
fn nulls_skipped_by_aggregates() {
    let mut executor = setup();
    // 200 NULLs of 2000: COUNT(v) = 1800, COUNT(*) = 2000.
    let rows = text_cols(&exec(&mut executor, "SELECT COUNT(v), COUNT(*) FROM t"));
    assert_eq!(rows, vec![vec!["1800".to_string(), "2000".to_string()]]);
}

#[test]
fn session_function_mix_still_works() {
    // Session/window shapes decline columnar entirely; they must keep
    // working through the fallback with exact results.
    let mut executor = setup();
    let rows = exec(&mut executor, "SELECT current_timestamp, SUM(v) FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
}
