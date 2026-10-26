//
// © 2026 PLOMID Technologies INDIA
//
// Licensed under the Apache License, Version 2.0;
// P2 regression: untyped-parameter casts (§P2).
//
// Extended-protocol parameters are textual substitution (`bind_query`), not
// typed Param nodes: declared OIDs affect only binary decoding, and untyped
// `$1` typing depends on `quote_bound_value` + coercion. Explicit
// `$1::TYPE` casts work; `CAST($1 AS TYPE)` executes but is invisible to
// Describe-time inference (documents `compatibility.mdx`).

use plomid_executor::Executor;
use plomid_network::server::handler::{bind_query, max_placeholder, quote_bound_value};
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some(v.to_string())).collect()
}

fn setup() -> Executor<PlomidStorageEngine> {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let storage = std::env::temp_dir().join(format!("plomid-param-{n}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-param-wal-{n}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::create(&storage, &wal, 64).expect("engine");
    let mut ex = Executor::new(engine).expect("executor");
    ex.execute("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)")
        .expect("create");
    ex.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .expect("insert");
    // NOTE: temp dirs leak per-test-process on purpose (parallel tests share
    // the process id); unique per-process names avoid collisions.
    ex
}

fn scalar(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Value {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        QueryResult::Rows { mut rows, .. } => rows.remove(0).remove(0),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn explicit_cast_suffix_binds_and_executes() {
    // PostgreSQL: `SELECT $1::integer` with "5" -> 5 (OID 23).
    let bound = bind_query("SELECT $1::integer", &some(&["5"]));
    assert_eq!(bound, "SELECT 5::integer");
    let mut ex = setup();
    assert_eq!(scalar(&mut ex, &bound), Value::Int4(5));

    // Text stays quoted: `$1::text` with "hello" -> 'hello'::text.
    let bound = bind_query("SELECT $1::text", &some(&["hello"]));
    assert_eq!(bound, "SELECT 'hello'::text");
    assert!(matches!(scalar(&mut ex, &bound), Value::Text(_)));
}

#[test]
fn cast_function_form_binds_and_executes() {
    // `CAST($1 AS INTEGER)` executes after textual substitution, even though
    // Describe-time `explicit_parameter_cast` only recognizes `::` form
    // (documented limitation: Describe reports UNKNOWN for this shape).
    let bound = bind_query("SELECT CAST($1 AS INTEGER)", &some(&["42"]));
    assert_eq!(bound, "SELECT CAST(42 AS INTEGER)");
    let mut ex = setup();
    assert_eq!(scalar(&mut ex, &bound), Value::Int4(42));
}

#[test]
fn untyped_param_in_comparison_coerces() {
    // PostgreSQL infers `$1` in `WHERE id = $1` as integer from context.
    // Here the heuristic quotes "1" as bare `1` (i64 parse), then coercion
    // matches the integer column.
    let bound = bind_query("SELECT name FROM users WHERE id = $1", &some(&["1"]));
    assert_eq!(bound, "SELECT name FROM users WHERE id = 1");
    let mut ex = setup();
    match ex.execute(&bound).expect("select") {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Text("alice".to_string()));
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn text_param_stays_quoted() {
    // Non-numeric strings are single-quoted (PostgreSQL unknown -> text).
    assert_eq!(quote_bound_value("alice"), "'alice'");
    let bound = bind_query("SELECT * FROM users WHERE name = $1", &some(&["bob"]));
    assert_eq!(bound, "SELECT * FROM users WHERE name = 'bob'");
    let mut ex = setup();
    match ex.execute(&bound).expect("select") {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn null_and_missing_placeholders() {
    // Explicit NULL binds as SQL NULL.
    assert_eq!(bind_query("SELECT $1", &[None]), "SELECT NULL");
    // Out-of-range `$N` is left raw for the parser to reject (never silently
    // corrupted to NULL or empty).
    assert_eq!(bind_query("SELECT $2", &some(&["7"])), "SELECT $2");
    // `$0` is never a valid placeholder (PostgreSQL: numbering starts at 1).
    assert_eq!(bind_query("SELECT $0", &some(&["7"])), "SELECT $0");
    assert_eq!(max_placeholder("SELECT $1, $10"), 10);
    assert_eq!(max_placeholder("SELECT $0"), 0);
}

#[test]
fn literals_comments_and_dollar_bodies_preserved() {
    // `$1` inside string literals, quoted identifiers, line comments, and
    // dollar-quoted function bodies must NOT bind.
    assert_eq!(
        bind_query("SELECT 'a$1b', $1", &some(&["X"])),
        "SELECT 'a$1b', 'X'"
    );
    assert_eq!(
        bind_query("SELECT $1 -- $2\n", &some(&["7", "8"])),
        "SELECT 7 -- $2\n"
    );
}
