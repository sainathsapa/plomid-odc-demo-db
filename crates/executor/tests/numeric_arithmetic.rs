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
//! Numeric literal typing and arithmetic dispatch (P0-002 regression).
//!
//! A decimal/exponent spelling is a floating-point literal even when its
//! value is integral: `3.0 / 2.0` must be 1.5, not integer 1. Integer pairs
//! keep truncating integer division (`3 / 2` is 1 — the documented
//! contract, matching PostgreSQL). Every case runs through the real engine
//! (parser → executor → storage); bare selects exercise the general-engine
//! evaluator while FROM selects exercise the row evaluator, and both must
//! agree on values.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn unique_engine(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-numarith-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-numarith-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    (storage, wal)
}

fn setup(tag: &str) -> Executor<PlomidStorageEngine> {
    let (storage, wal) = unique_engine(tag);
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).unwrap();
    let mut executor = Executor::new(engine).unwrap();
    executor
        .execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, i INTEGER, f DOUBLE PRECISION);")
        .unwrap();
    executor
        .execute("INSERT INTO nums VALUES (1, 7, 1.5), (2, 4, 2.0), (3, NULL, NULL);")
        .unwrap();
    executor
}

fn scalar(executor: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Value> {
    match executor.execute(sql).expect("sql should execute") {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "{sql}");
            rows.into_iter().next().unwrap()
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

fn f64_of(value: &Value) -> f64 {
    match value {
        Value::Int2(v) => f64::from(*v),
        Value::Int4(v) => f64::from(*v),
        Value::Int8(v) => *v as f64,
        Value::Float4(v) => f64::from(*v),
        Value::Float8(v) => *v,
        Value::Numeric(n) => n.clone().to_f64(),
        other => panic!("not numeric: {other:?}"),
    }
}

fn assert_approx(sql_result: &[Value], expected: f64, sql: &str) {
    assert_eq!(sql_result.len(), 1, "{sql}");
    let got = f64_of(&sql_result[0]);
    assert!(
        (got - expected).abs() < 1e-9,
        "{sql}: expected {expected}, got {got}"
    );
}

#[test]
fn decimal_spelling_forces_float_division() {
    let tag = "dec-spelling";
    let mut executor = setup(tag);
    // The P0-002 reproducer: integral-valued decimals still divide as floats.
    assert_approx(&scalar(&mut executor, "SELECT 3.0 / 2.0"), 1.5, "3.0/2.0");
    assert_approx(&scalar(&mut executor, "SELECT 10.0 / 4.0"), 2.5, "10.0/4.0");
    assert_approx(
        &scalar(&mut executor, "SELECT 1.0 / 3.0"),
        1.0 / 3.0,
        "1.0/3.0",
    );
    assert_approx(&scalar(&mut executor, "SELECT 1e3 / 4"), 250.0, "1e3/4");
    // ... and the general-engine evaluator returns Float8, matching the
    // float8 column type it now declares (previously: float value, int8
    // column — wire corruption).
    assert!(
        matches!(
            scalar(&mut executor, "SELECT 3.0 / 2.0")[0],
            Value::Float8(_)
        ),
        "bare float division evaluates to Float8"
    );
}

#[test]
fn integer_division_contract_unchanged() {
    let tag = "int-div";
    let mut executor = setup(tag);
    for (sql, expected) in [
        ("SELECT 3 / 2", 1i64),
        ("SELECT 7 / 2", 3),
        ("SELECT 4 / 2", 2),
        ("SELECT 7 % 3", 1),
        ("SELECT -7 / 2", -3),
    ] {
        let row = scalar(&mut executor, sql);
        assert!(
            matches!(row[0], Value::Int8(v) if v == expected),
            "{sql}: expected Int8({expected}), got {:?}",
            row[0]
        );
    }
}

#[test]
fn mixed_and_chained_float_arithmetic() {
    let tag = "mixed";
    let mut executor = setup(tag);
    assert_approx(&scalar(&mut executor, "SELECT 1.5 / 2"), 0.75, "1.5/2");
    assert_approx(&scalar(&mut executor, "SELECT 2 / 1.5"), 2.0 / 1.5, "2/1.5");
    assert_approx(&scalar(&mut executor, "SELECT 1.5 + 1"), 2.5, "1.5+1");
    assert_approx(&scalar(&mut executor, "SELECT 2.5 * 2"), 5.0, "2.5*2");
    assert_approx(&scalar(&mut executor, "SELECT 7.5 - 2.0"), 5.5, "7.5-2.0");
    assert_approx(&scalar(&mut executor, "SELECT 5.5 % 2"), 1.5, "5.5%2");
    assert_approx(&scalar(&mut executor, "SELECT -7.5 / 2"), -3.75, "-7.5/2");
    assert_approx(
        &scalar(&mut executor, "SELECT (1 + 2) * 3.0"),
        9.0,
        "(1+2)*3.0",
    );
    assert_approx(
        &scalar(&mut executor, "SELECT 0.1 + 0.2"),
        0.1 + 0.2,
        "0.1+0.2",
    );
    assert_approx(
        &scalar(&mut executor, "SELECT 10 - 2 * 3.0 + 8 / 4.0"),
        10.0 - 2.0 * 3.0 + 8.0 / 4.0,
        "precedence",
    );
}

#[test]
fn null_and_division_by_zero_keep_documented_semantics() {
    let tag = "null-zero";
    let mut executor = setup(tag);
    for sql in ["SELECT NULL + 1", "SELECT 1.5 + NULL", "SELECT NULL / 2"] {
        let row = scalar(&mut executor, sql);
        assert!(matches!(row[0], Value::Null), "{sql} propagates NULL");
    }
    for sql in ["SELECT 1 / 0", "SELECT 1.5 / 0", "SELECT 1 % 0"] {
        let err = executor
            .execute(sql)
            .expect_err("division by zero must error");
        assert!(err.to_string().contains("division by zero"), "{sql}: {err}");
    }
}

#[test]
fn row_path_columns_use_the_same_operator_semantics() {
    let tag = "rowpath";
    let mut executor = setup(tag);
    // Row evaluator over typed columns: int/int truncates, float widens.
    let rows = match executor
        .execute("SELECT i / 2, f / 2, i / 2.0, f + i FROM nums WHERE id IN (1, 2) ORDER BY id")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows.len(), 2);
    assert_approx(&rows[0][0..1], 3.0, "i/2 id=1");
    assert_approx(&rows[0][1..2], 0.75, "f/2 id=1");
    assert_approx(&rows[0][2..3], 3.5, "i/2.0 id=1");
    assert_approx(&rows[0][3..4], 8.5, "f+i id=1");
    assert_approx(&rows[1][0..1], 2.0, "i/2 id=2");
    // NULL row propagates through every operator.
    let rows = match executor
        .execute("SELECT i + 1, f / 2 FROM nums WHERE id = 3")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert!(matches!(rows[0][0], Value::Null));
    assert!(matches!(rows[0][1], Value::Null));
    // Predicates share the implementation: filter on a float ratio works.
    let rows = match executor
        .execute("SELECT id FROM nums WHERE f / 2.0 = 0.75 ORDER BY id")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int4(1)));
}

#[test]
fn aggregate_context_shares_expression_semantics() {
    let tag = "aggctx";
    let mut executor = setup(tag);
    // Arithmetic INSIDE aggregates shares operator semantics (NULLs skip).
    let row = scalar(&mut executor, "SELECT SUM(i + 1) FROM nums");
    assert!(matches!(row[0], Value::Int8(13)), "sum(i+1): {:?}", row[0]);
    let row = scalar(&mut executor, "SELECT AVG(i) FROM nums");
    assert!((f64_of(&row[0]) - 5.5).abs() < 1e-9, "avg(i): {:?}", row[0]);
    let row = scalar(&mut executor, "SELECT SUM(f) FROM nums");
    assert!((f64_of(&row[0]) - 3.5).abs() < 1e-9, "sum(f): {:?}", row[0]);
    // Arithmetic OVER aggregate outputs is explicitly unsupported (loud
    // `Unsupported`, never a silent wrong answer): the row evaluator has
    // no group context for the aggregate calls. Pre-existing limitation,
    // identical for integer-only shapes, out of scope for P0-002.
    for sql in [
        "SELECT SUM(i) / 2 FROM nums",
        "SELECT SUM(i) / COUNT(*) FROM nums WHERE id IN (1, 2)",
    ] {
        let err = executor.execute(sql).expect_err("must fail loudly");
        assert!(err.to_string().contains("query context"), "{sql}: {err}");
    }
}

#[test]
fn result_column_types_match_runtime_values() {
    let tag = "coltypes";
    let mut executor = setup(tag);
    // Bare (general-engine) path: float-involved declares float8 (701),
    // int/int declares bigint (20) — the wire OID must describe the value.
    for (sql, oid) in [
        ("SELECT 1.5 / 2", 701u32),
        ("SELECT 3.0 / 2.0", 701),
        ("SELECT 1.5 + 1", 701),
        ("SELECT 3 / 2", 20),
        ("SELECT 7 % 3", 20),
    ] {
        match executor.execute(sql).unwrap() {
            QueryResult::Rows { column_types, .. } => {
                assert_eq!(column_types.len(), 1, "{sql}");
                let got = column_types[0].expect("typed").type_oid.0;
                assert_eq!(got, oid, "{sql}: declared OID");
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
}
