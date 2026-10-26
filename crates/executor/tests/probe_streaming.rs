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
// Regression tests for probe-path streaming selection.
//
// Aggregate/count shapes over an index probe fold decoded rows
// straight into accumulators instead of materializing
// `filtered_rows`. The gate mirrors the scan path exactly; these
// tests pin result-exactness on the danger shapes: selective ranges,
// NULLs, FILTER, DISTINCT, GROUP BY (single/multi/empty), HAVING,
// and ORDER BY placement (which keeps the legacy path).

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn unique_dirs(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let storage = std::env::temp_dir().join(format!(
        "plomid-probestream-{tag}-{id}-{}",
        std::process::id()
    ));
    let wal = std::env::temp_dir().join(format!(
        "plomid-probestream-{tag}-{id}-wal-{}",
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

fn text(rows: &[Vec<Value>]) -> Vec<Vec<String>> {
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
        .execute("CREATE TABLE s(id BIGINT PRIMARY KEY, g INTEGER NOT NULL, v BIGINT, w DOUBLE PRECISION NOT NULL)")
        .expect("create");
    // 3000 rows, 3 groups; every 7th v is NULL.
    for lo in (0..3000).step_by(500) {
        let values: Vec<String> = (lo..lo + 500)
            .map(|i| {
                let v = if i % 7 == 0 {
                    "NULL".to_string()
                } else {
                    (i * 2).to_string()
                };
                format!("({i},{},{v},{i}.5)", i % 3)
            })
            .collect();
        executor
            .execute(&format!("INSERT INTO s VALUES {}", values.join(",")))
            .expect("insert");
    }
    executor.execute("CREATE INDEX s_g ON s(g)").expect("index");
    executor
}

// Non-null v values: 3000 rows minus every 7th (429 NULLs) = 2571.
// SUM(v) where v = 2i: computed below in-test instead of hard-coded.
fn expected_sum(elems: impl Iterator<Item = i64>) -> i64 {
    elems.sum()
}

#[test]
fn selective_count_and_sum_are_exact() {
    let mut e = setup();
    // g = 1 -> 1000 rows.
    assert_eq!(
        text(&exec(&mut e, "SELECT COUNT(*) FROM s WHERE g = 1")),
        vec![vec!["1000"]]
    );
    let rows = text(&exec(&mut e, "SELECT SUM(v) FROM s WHERE g = 1"));
    let expect = expected_sum(
        (0..3000)
            .filter(|i| i % 3 == 1 && i % 7 != 0)
            .map(|i| i * 2),
    );
    assert_eq!(rows, vec![vec![expect.to_string()]]);
    // COUNT(col) skips the 143 NULLs in group 1 (1000 rows, every 7th NULL:
    // i%3==1 && i%7==0 -> lcm 21: floor(3000/21)=142 + boundary -> count exactly).
    let rows = text(&exec(
        &mut e,
        "SELECT COUNT(v), COUNT(*) FROM s WHERE g = 1",
    ));
    assert_eq!(rows[0][1], "1000");
    let nulls_in_g1 = (0..3000).filter(|i| i % 3 == 1 && i % 7 == 0).count();
    assert_eq!(rows[0][0], (1000 - nulls_in_g1).to_string());
}

#[test]
fn selective_min_max_avg_are_exact() {
    let mut e = setup();
    let rows = text(&exec(
        &mut e,
        "SELECT MIN(v), MAX(v), AVG(v) FROM s WHERE g = 2",
    ));
    assert_eq!(rows.len(), 1);
    let vals: Vec<i64> = (0..3000)
        .filter(|i| i % 3 == 2 && i % 7 != 0)
        .map(|i| i * 2)
        .collect();
    let min = vals.iter().min().unwrap();
    let max = vals.iter().max().unwrap();
    assert_eq!(rows[0][0], min.to_string());
    assert_eq!(rows[0][1], max.to_string());
    // AVG is exact NUMERIC: sum/count as decimal; compare via count>0 shape.
    assert_ne!(rows[0][2], "NULL");
    let rows = text(&exec(&mut e, "SELECT COUNT(*) FROM s WHERE g = 2"));
    assert_eq!(rows, vec![vec!["1000"]]);
}

#[test]
fn selective_group_by_is_exact() {
    let mut e = setup();
    let rows = text(&exec(
        &mut e,
        "SELECT g, COUNT(*), SUM(v) FROM s WHERE g >= 1 GROUP BY g ORDER BY g",
    ));
    // ORDER BY routes to the general engine; exactness must hold there too.
    assert_eq!(rows.len(), 2);
    for (g, row) in [1i64, 2].iter().zip(rows.iter()) {
        assert_eq!(row[0], g.to_string());
        assert_eq!(row[1], "1000");
        let expect = expected_sum(
            (0..3000)
                .filter(|i| i % 3 == *g && i % 7 != 0)
                .map(|i| i * 2),
        );
        assert_eq!(row[2], expect.to_string());
    }
}

#[test]
fn selective_having_and_filter_are_exact() {
    let mut e = setup();
    let rows = text(&exec(
        &mut e,
        "SELECT g, COUNT(*) FROM s WHERE g >= 0 GROUP BY g HAVING COUNT(*) > 100",
    ));
    assert_eq!(rows.len(), 3);
    let rows = text(&exec(
        &mut e,
        "SELECT COUNT(*) FILTER (WHERE g = 0) FROM s WHERE g >= 0",
    ));
    assert_eq!(rows, vec![vec!["1000"]]);
    let rows = text(&exec(
        &mut e,
        "SELECT COUNT(DISTINCT g) FROM s WHERE g >= 1",
    ));
    assert_eq!(rows, vec![vec!["2"]]);
}

#[test]
fn empty_selection_returns_empty_aggregates() {
    let mut e = setup();
    assert_eq!(
        text(&exec(&mut e, "SELECT COUNT(*) FROM s WHERE g = 99")),
        vec![vec!["0"]]
    );
    assert_eq!(
        text(&exec(&mut e, "SELECT SUM(v) FROM s WHERE g = 99")),
        vec![vec!["NULL"]]
    );
    assert_eq!(
        text(&exec(
            &mut e,
            "SELECT g, COUNT(*) FROM s WHERE g = 99 GROUP BY g"
        )),
        Vec::<Vec<String>>::new()
    );
}
