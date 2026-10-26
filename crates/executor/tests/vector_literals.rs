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
//! Native `[...]` vector literals through the Executor path.
//!
//! Bare float lists parse as `Expression::VectorLiteral` (validated once by
//! `plomid-vector`), evaluate to `PgValue::Vector`, and round-trip through
//! storage as canonical `[..]` text. Covers single/multi-row INSERT,
//! UPDATE, bare `SELECT`, distance functions, and rejection shapes.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn unique_engine(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-vectorlit-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-vectorlit-{tag}-wal-{}", std::process::id()));
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

fn exec_ok<E: plomid_txn::StorageEngine>(executor: &mut Executor<E>, sql: &str) {
    executor
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql} should succeed: {e}"));
}

fn exec_err<E: plomid_txn::StorageEngine>(executor: &mut Executor<E>, sql: &str) -> String {
    match executor.execute(sql) {
        Ok(QueryResult::Rows { rows, .. }) => panic!("{sql} should fail, got rows={rows:?}"),
        Ok(other) => panic!("{sql} should fail, got {other:?}"),
        Err(e) => e.to_string(),
    }
}

fn vector_of(lanes: &[f32]) -> Value {
    plomid_sql::vector_literal_value(lanes).unwrap()
}

#[test]
fn native_vector_literal_insert_select_round_trip() {
    let (storage, wal) = unique_engine("roundtrip");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE d (id INTEGER, e VECTOR(FLOAT32, 4));",
    );
    // Single-row and multi-row VALUES with bare literals; signs/whitespace
    // ride along in the second row.
    exec_ok(
        &mut executor,
        "INSERT INTO d VALUES (1, [0.12,0.34,0.56,0.78]);",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO d VALUES (2, [1,2,3,4]), (3, [ -1 , +2 , 3e0 , 4 ]);",
    );

    let rows = exec(&mut executor, "SELECT e FROM d WHERE id = 1;");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], vector_of(&[0.12f32, 0.34f32, 0.56f32, 0.78f32]));
    // Storage round trip comes back as bracketed `[..]` text that re-parses
    // to identical lanes (f32 lanes widen on display; integer lanes below
    // show the exact short form).
    let text = rows[0][0].to_sql_text();
    assert!(text.starts_with('[') && text.ends_with(']'), "got: {text}");
    assert_eq!(
        plomid_types::text::parse_vector_literal(&text).unwrap(),
        vector_of(&[0.12f32, 0.34f32, 0.56f32, 0.78f32])
    );
    // Exactly-representable lanes format without float expansion.
    let rows = exec(&mut executor, "SELECT e FROM d WHERE id = 2;");
    assert_eq!(rows[0][0], vector_of(&[1.0, 2.0, 3.0, 4.0]));
    assert_eq!(rows[0][0].to_sql_text(), "[1,2,3,4]");

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn native_vector_literal_update_bare_select_and_distance() {
    let (storage, wal) = unique_engine("update");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE d (id INTEGER, e VECTOR(FLOAT32, 4));",
    );
    exec_ok(&mut executor, "INSERT INTO d VALUES (1, [1,2,3,4]);");
    // UPDATE SET with a bare literal.
    exec_ok(&mut executor, "UPDATE d SET e = [0,0,0,1] WHERE id = 1;");
    let rows = exec(&mut executor, "SELECT e FROM d WHERE id = 1;");
    assert_eq!(rows[0][0], vector_of(&[0.0, 0.0, 0.0, 1.0]));

    // Bare `SELECT [...]` evaluates to a native vector.
    let rows = exec(&mut executor, "SELECT [1,2,3];");
    assert_eq!(rows[0][0], vector_of(&[1.0, 2.0, 3.0]));

    // Distance functions accept the new literal path (columns already did).
    let rows = exec(
        &mut executor,
        "SELECT l2_distance(e, [0,0,0,1]) FROM d WHERE id = 1;",
    );
    assert_eq!(rows[0][0], Value::Float8(0.0));
    // The pre-existing text-literal path keeps working (no regression).
    let rows = exec(
        &mut executor,
        "SELECT l2_distance(e, '[0,0,0,1]') FROM d WHERE id = 1;",
    );
    assert_eq!(rows[0][0], Value::Float8(0.0));

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn native_vector_literal_rejections() {
    let (storage, wal) = unique_engine("reject");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE d (id INTEGER, e VECTOR(FLOAT32, 4));",
    );
    // Dimension mismatch against the column typmod fails with a clear error.
    let err = exec_err(&mut executor, "INSERT INTO d VALUES (1, [1,2,3]);");
    assert!(err.contains("dimension"), "got: {err}");
    // Malformed shapes fail as VECTOR_INVALID_LITERAL, never as TEXT.
    for sql in [
        "INSERT INTO d VALUES (1, []);",
        "INSERT INTO d VALUES (1, [1,2,]);",
        "INSERT INTO d VALUES (1, [1,NaN]);",
        "SELECT []; ",
    ] {
        let err = exec_err(&mut executor, sql);
        assert!(err.contains("VECTOR_INVALID_LITERAL"), "{sql} got: {err}");
    }

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_integer_element_ranges_enforced() {
    let (storage, wal) = unique_engine("ranges");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE r (id INTEGER, a VECTOR(INT8, 3), b VECTOR(UINT8, 3), c VECTOR(INT4, 4));",
    );
    // Boundary lanes accepted (inclusive on both ends).
    exec_ok(
        &mut executor,
        "INSERT INTO r VALUES (1, [-128,0,127], [0,128,255], [-8,-1,0,7]);",
    );
    // Distances across converted lanes work.
    let rows = exec(&mut executor, "SELECT l2_distance(a, [-128,0,127]) FROM r;");
    assert_eq!(rows[0][0], Value::Float8(0.0));
    // Overflow, underflow and fractional lanes rejected per element.
    for sql in [
        "INSERT INTO r VALUES (2, [0,0,128], [0,0,0], [0,0,0,0]);",
        "INSERT INTO r VALUES (2, [0,0,0], [0,0,-1], [0,0,0,0]);",
        "INSERT INTO r VALUES (2, [0,0,0], [0,0,0], [0,0,0,8]);",
        "INSERT INTO r VALUES (2, [0,0,1.5], [0,0,0], [0,0,0,0]);",
    ] {
        let err = exec_err(&mut executor, sql);
        assert!(err.contains("out of range"), "{sql} got: {err}");
    }
    // Failed inserts change nothing.
    let rows = exec(&mut executor, "SELECT count(*) FROM r;");
    assert_eq!(rows[0][0], Value::Int8(1));

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_sparse_binary_multi_distances() {
    let (storage, wal) = unique_engine("kinds");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE k (id INTEGER, s SPARSE_VECTOR(FLOAT32, 100), b VECTOR(BINARY, 8), m MULTI_VECTOR(FLOAT32, 4, 2));",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO k VALUES (1, '{1:2.0,5:3.0}/100', '01010101', '[[1,0],[0,1]]');",
    );
    // Sparse identical-vector distances are exactly zero (dot = 13.0).
    for (func, arg, expected) in [
        ("cosine_distance", "'{1:2.0,5:3.0}/100'", 0.0),
        ("l2_distance", "'{1:2.0,5:3.0}/100'", 0.0),
        ("dot_product", "'{1:2.0,5:3.0}/100'", 13.0),
    ] {
        let rows = exec(&mut executor, &format!("SELECT {func}(s, {arg}) FROM k;"));
        assert_eq!(rows[0][0], Value::Float8(expected), "{func}");
    }
    // Binary: half-overlap Hamming is 4, identical Jaccard is 0.
    let rows = exec(
        &mut executor,
        "SELECT hamming_distance(b, '11111111') FROM k;",
    );
    assert_eq!(rows[0][0], Value::Float8(4.0));
    let rows = exec(
        &mut executor,
        "SELECT jaccard_distance(b, '01010101') FROM k;",
    );
    assert_eq!(rows[0][0], Value::Float8(0.0));
    // Multi MIN_DISTANCE: exact constituent match scores 0.
    let rows = exec(
        &mut executor,
        "SELECT l2_distance(m, '[[1,0],[0,1]]') FROM k;",
    );
    assert_eq!(rows[0][0], Value::Float8(0.0));
    // Sparse rejects: dup index, index == dim, missing suffix.
    for sql in [
        "INSERT INTO k VALUES (2, '{1:1.0,1:2.0}/100', '00000000', '[[0,0]]');",
        "INSERT INTO k VALUES (2, '{100:1.0}/100', '00000000', '[[0,0]]');",
        "INSERT INTO k VALUES (2, '{1:1.0}', '00000000', '[[0,0]]');",
    ] {
        exec_err(&mut executor, sql);
    }

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_auto_hnsw_and_ann_equals_exact() {
    let (storage, wal) = unique_engine("autoann");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4));",
    );
    // Automatic HNSW exists under its deterministic name.
    let rows = exec(
        &mut executor,
        "SELECT indexname FROM pg_indexes WHERE tablename = 'd';",
    );
    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            v => v.to_sql_text(),
        })
        .collect();
    assert!(
        names.iter().any(|n| n == "d_e_vector_idx"),
        "auto index missing: {names:?}"
    );
    // 300 deterministic rows across 4 clusters with epsilon-distinct
    // lanes, so every distance is unique and ANN-vs-exact agreement is
    // order-exact rather than tie-timid.
    let mut values = Vec::new();
    for i in 0..300i64 {
        let c = (i % 4) as f32;
        let e1 = (i % 7) as f32 * 0.01;
        let e2 = (i % 11) as f32 * 0.01;
        let e3 = (i % 13) as f32 * 0.01;
        values.push(format!("({i}, [{c},{e1},{e2},{e3}])"));
        if values.len() == 50 {
            exec_ok(
                &mut executor,
                &format!("INSERT INTO d VALUES {};", values.join(",")),
            );
            values.clear();
        }
    }
    exec_ok(&mut executor, "SET vector.auto_ann_min_rows = 0;");
    let query = "SELECT id FROM d ORDER BY l2_distance(e, [1,0.01,0.02,0.03]) LIMIT 10";
    let ann = exec(&mut executor, query);
    // Drop every vector index: identical query now runs the exact scan.
    exec_ok(&mut executor, "DROP INDEX d_e_vector_idx;");
    let oracle = exec(&mut executor, query);
    assert_eq!(ann, oracle);
    // EXPLAIN reports usable while indexed, absent after the drop.
    exec_ok(
        &mut executor,
        "CREATE INDEX d_e_vector_idx ON d USING HNSW (e);",
    );
    let plan = exec(
        &mut executor,
        "EXPLAIN SELECT id FROM d ORDER BY l2_distance(e, [1,0,0,0]) LIMIT 10;",
    );
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            v => v.to_sql_text(),
        })
        .collect();
    assert!(
        text.iter().any(|l| l.contains("usable")),
        "expected usable plan: {text:?}"
    );

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_operators_lower_to_functions() {
    let (storage, wal) = unique_engine("operators");
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 3));",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO d VALUES (1, [1,0,0]), (2, [0,1,0]), (3, [0,0,1]);",
    );
    // Plan-required equivalences: function form == operator form exactly
    // (same lowered FunctionCall, same kernel).
    for (func, op) in [
        ("cosine_distance(e, [1,0,0])", "e <=> [1,0,0]"),
        ("l2_distance(e, [1,0,0])", "e <-> [1,0,0]"),
        ("hamming_distance(e, [1,0,0])", "e <~> [1,0,0]"),
        ("jaccard_distance(e, [1,0,0])", "e <%> [1,0,0]"),
    ] {
        let a = exec(
            &mut executor,
            &format!("SELECT {func} FROM d WHERE id = 2;"),
        );
        let b = exec(&mut executor, &format!("SELECT {op} FROM d WHERE id = 2;"));
        assert_eq!(a, b, "{func} vs {op}");
    }
    // `<#>` is negative inner product: dot_product == -(a <#> b).
    let dot = exec(
        &mut executor,
        "SELECT dot_product(e, [1,1,0]) FROM d WHERE id = 1;",
    );
    let neg = exec(&mut executor, "SELECT e <#> [1,1,0] FROM d WHERE id = 1;");
    match (&dot[0][0], &neg[0][0]) {
        (Value::Float8(x), Value::Float8(y)) => assert!((x + y).abs() < 1e-6),
        _ => panic!("{dot:?} vs {neg:?}"),
    }
    // Operators drive ORDER BY (incl. the ANN path) and radius predicates.
    let rows = exec(
        &mut executor,
        "SELECT id FROM d ORDER BY e <-> [1,0,0] LIMIT 2;",
    );
    assert_eq!(rows[0][0], Value::Int8(1));
    let rows = exec(
        &mut executor,
        "SELECT id FROM d ORDER BY e <=> [0,0,1] LIMIT 2;",
    );
    assert_eq!(rows[0][0], Value::Int8(3));
    let rows = exec(
        &mut executor,
        "SELECT id FROM d WHERE (e <-> [1,0,0]) < 0.5;",
    );
    assert_eq!(rows.len(), 1);
    // Legacy `<` uses lex exactly as before (no operator splintering).
    let rows = exec(&mut executor, "SELECT 1 < -1;");
    assert_eq!(rows[0][0], Value::Bool(false));
    let rows = exec(&mut executor, "SELECT 2 <= 2;");
    assert_eq!(rows[0][0], Value::Bool(true));

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_multi_max_vectors_enforced_across_restart() {
    let (storage, wal) = unique_engine("multimax");
    let engine = PlomidStorageEngine::open(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();

    exec_ok(
        &mut executor,
        "CREATE TABLE m (id BIGINT PRIMARY KEY, e MULTI_VECTOR(FLOAT32, 2, 2));",
    );
    exec_ok(&mut executor, "INSERT INTO m VALUES (1, '[[1,0],[0,1]]');");
    // 3 constituents exceed the declared max of 2.
    let err = exec_err(
        &mut executor,
        "INSERT INTO m VALUES (2, '[[1,0],[0,1],[1,1]]');",
    );
    assert!(err.contains("max_vectors"), "got: {err}");
    drop(executor);
    // Reopen: the catalog round-trips the cap and the data survives.
    let engine = PlomidStorageEngine::open(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();
    let rows = exec(&mut executor, "SELECT e FROM m;");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].to_sql_text(), "[[1,0],[0,1]]");
    let err = exec_err(
        &mut executor,
        "INSERT INTO m VALUES (3, '[[1,0],[0,1],[1,1]]');",
    );
    assert!(
        err.contains("max_vectors"),
        "cap must survive restart, got: {err}"
    );

    drop(executor);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
