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
// GROUP BY fast-key routing tests. The typed grouping path
// must return byte-identical results to the legacy Debug-string path
// on: NULL group keys, mixed int widths, multi-key groups (legacy
// control), expression keys (legacy control), HAVING, and empty input.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn unique_dirs(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let storage = std::env::temp_dir().join(format!(
        "plomid-groupfast-{tag}-{id}-{}",
        std::process::id()
    ));
    let wal = std::env::temp_dir().join(format!(
        "plomid-groupfast-{tag}-{id}-wal-{}",
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
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "NULL".to_string(),
                    other => other.to_sql_text(),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

fn setup() -> Executor<PlomidStorageEngine> {
    let (storage, wal) = unique_dirs("setup");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).expect("create");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("CREATE TABLE gk(id BIGINT PRIMARY KEY, k INTEGER, v BIGINT)")
        .expect("create");
    // 600 rows, 3 int groups + NULL group (every 5th key NULL).
    for lo in (0..600).step_by(200) {
        let values: Vec<String> = (lo..lo + 200)
            .map(|i| {
                let k = if i % 5 == 0 {
                    "NULL".to_string()
                } else {
                    (i % 3).to_string()
                };
                format!("({i},{k},{})", i * 2)
            })
            .collect();
        executor
            .execute(&format!("INSERT INTO gk VALUES {}", values.join(",")))
            .expect("insert");
    }
    executor
}

#[test]
fn null_group_key_exact() {
    let mut e = setup();
    // NULL group: 120 rows (600/5), SUM(v) over i%5==0, v=2i.
    let rows = text(&exec(
        &mut e,
        "SELECT k, COUNT(*), SUM(v) FROM gk GROUP BY k ORDER BY k",
    ));
    assert_eq!(rows.len(), 4);
    let null_row = rows.iter().find(|r| r[0] == "NULL").expect("null group");
    assert_eq!(null_row[1], "120");
    let expect: i64 = (0..600).filter(|i| i % 5 == 0).map(|i| i * 2).sum();
    assert_eq!(null_row[2], expect.to_string());
}

#[test]
fn single_int_key_no_order_matches_ordered() {
    // ORDER BY forces the general engine; without it the fast path may
    // engage. Both must agree exactly.
    let mut e = setup();
    let ordered = text(&exec(
        &mut e,
        "SELECT k, COUNT(*), SUM(v) FROM gk GROUP BY k ORDER BY k",
    ));
    let mut unordered = text(&exec(
        &mut e,
        "SELECT k, COUNT(*), SUM(v) FROM gk GROUP BY k",
    ));
    unordered.sort();
    let mut ordered_sorted = ordered.clone();
    ordered_sorted.sort();
    assert_eq!(unordered, ordered_sorted);
    assert_eq!(ordered.len(), 4);
}

#[test]
fn multi_key_and_expression_keys_legacy_exact() {
    let mut e = setup();
    // Multi-key (legacy control): 3 keys x NULL handling.
    let rows = text(&exec(
        &mut e,
        "SELECT k, COUNT(*) FROM gk GROUP BY k, v % 2 ORDER BY k",
    ));
    assert!(!rows.is_empty());
    let total: i64 = rows.iter().map(|r| r[1].parse::<i64>().unwrap()).sum();
    assert_eq!(total, 600);
    // HAVING over fast groups.
    let rows = text(&exec(
        &mut e,
        "SELECT k, COUNT(*) FROM gk GROUP BY k HAVING COUNT(*) > 100",
    ));
    assert_eq!(rows.len(), 4);
}

#[test]
fn empty_input_groups_nothing() {
    let mut e = setup();
    let rows = text(&exec(
        &mut e,
        "SELECT k, COUNT(*) FROM gk WHERE k = 99 GROUP BY k",
    ));
    assert!(rows.is_empty());
    let rows = text(&exec(
        &mut e,
        "SELECT COUNT(*), SUM(v) FROM gk WHERE k = 99",
    ));
    assert_eq!(rows, vec![vec!["0".to_string(), "NULL".to_string()]]);
}
