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
//! Parallel scan-aggregation exactness.
//!
//! Tables at or above `PAR_MIN_ROWS` (100K) with integer-typed, filter-free,
//! context-free aggregate shapes take the parallel `par_streaming_agg` path
//! (one snapshot scan on the main thread, decode/filter/feed fanned out to
//! workers, deterministic index-order merge); smaller tables keep the
//! sequential loop. Both paths must answer identically, so every assertion
//! below is against independently hand-computed expectations (same formulas,
//! separate fold) rather than against the other path — plus restart-stability
//! for the large table. On a single-CPU machine the large-table queries run
//! sequentially and still assert the same values.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

fn scratch(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("plomid-paragg-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &std::path::Path) -> Executor<PlomidStorageEngine> {
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

fn int(v: &Value) -> i64 {
    match v {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

fn sorted_text(rows: &[Vec<Value>]) -> Vec<String> {
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.to_sql_text())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    out
}

/// `AVG(int)` finalizes as `NUMERIC(sum)/count`, normalized — replicate the
/// accumulator's finalize exactly (NULL on empty input).
fn avg_int(sum: i64, count: i64) -> Value {
    if count == 0 {
        return Value::Null;
    }
    plomid_types::Numeric::from_i64(sum)
        .div(plomid_types::Numeric::from_i64(count))
        .map(|v| Value::Numeric(v.normalize()))
        .unwrap_or(Value::Null)
}

/// Deterministic row generator shared by the loader and the expectation
/// folds: `g` NULL every 17th row (NULL group keys), `v` NULL every 13th row,
/// `h` a dense second key. The PRNG advances on EVERY row (even NULL ones)
/// so loader and expectations cannot diverge.
struct Gen {
    state: u64,
}

impl Gen {
    fn new() -> Self {
        Self {
            state: 0x9e37_79b9_7f4a_7c15,
        }
    }

    fn row(&mut self, i: i64) -> (Option<i64>, Option<i64>, i64) {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let g = if i % 17 == 0 { None } else { Some(i % 25) };
        let v = if i % 13 == 0 {
            None
        } else {
            Some((self.state % 2001) as i64 - 1000)
        };
        (g, v, i % 97)
    }
}

fn cell(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

fn load_big(session: &mut Executor<PlomidStorageEngine>, n: i64) {
    session
        .execute("CREATE TABLE big (g INTEGER, v INTEGER, h INTEGER);")
        .unwrap();
    let mut gen = Gen::new();
    let mut batch = String::from("INSERT INTO big VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let (g, v, h) = gen.row(i);
        batch.push_str(&format!("({}, {}, {h})", cell(g), cell(v)));
        in_batch += 1;
        if in_batch == 2000 || i + 1 == n {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = String::from("INSERT INTO big VALUES ");
            in_batch = 0;
        }
    }
}

#[derive(Default, Clone)]
struct Acc {
    count_star: i64,
    count_v: i64,
    sum_v: i64,
    min_v: Option<i64>,
    max_v: Option<i64>,
}

impl Acc {
    fn add(&mut self, v: Option<i64>) {
        self.count_star += 1;
        if let Some(x) = v {
            self.count_v += 1;
            self.sum_v += x;
            self.min_v = Some(self.min_v.map_or(x, |m| m.min(x)));
            self.max_v = Some(self.max_v.map_or(x, |m| m.max(x)));
        }
    }
}

fn int4_or_null(v: Option<i64>) -> Value {
    v.map(|n| Value::Int4(n as i32)).unwrap_or(Value::Null)
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_agg_200k_exactness_with_restart() {
    let root = scratch("big");
    let mut session = open(&root);
    let n = 200_000i64;
    let started = Instant::now();
    load_big(&mut session, n);
    eprintln!("loaded {n} rows in {:?}", started.elapsed());
    eprintln!(
        "available_parallelism={:?}",
        std::thread::available_parallelism().map(|v| v.get())
    );

    // Independent expectation fold (same generator, separate accumulation).
    let mut gen = Gen::new();
    let mut global = Acc::default();
    let mut groups: BTreeMap<Option<i64>, Acc> = BTreeMap::new();
    let mut multi: BTreeMap<(Option<i64>, i64), Acc> = BTreeMap::new();
    let mut distinct_v: HashSet<i64> = HashSet::new();
    let mut between = Acc::default();
    for i in 0..n {
        let (g, v, h) = gen.row(i);
        global.add(v);
        groups.entry(g).or_default().add(v);
        multi.entry((g, h)).or_default().add(v);
        if let Some(x) = v {
            distinct_v.insert(x);
            if (-100..=100).contains(&x) {
                between.add(v);
            }
        }
    }

    // Global aggregation (COUNT DISTINCT rides along in the same slots).
    let rows = rows_of(
        session
            .execute(
                "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v), COUNT(DISTINCT v) FROM big;",
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0][0]), global.count_star);
    assert_eq!(int(&rows[0][1]), global.count_v);
    assert_eq!(int(&rows[0][2]), global.sum_v);
    assert_eq!(rows[0][3], int4_or_null(global.min_v));
    assert_eq!(rows[0][4], int4_or_null(global.max_v));
    assert_eq!(rows[0][5], avg_int(global.sum_v, global.count_v));
    assert_eq!(int(&rows[0][6]), distinct_v.len() as i64);

    // Single-key GROUP BY (26 groups: 0..24 plus NULL).
    let started = Instant::now();
    let rows = rows_of(
        session
            .execute(
                "SELECT g, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM big GROUP BY g;",
            )
            .unwrap(),
    );
    eprintln!(
        "parallel grouped query over {n} rows: {:?}",
        started.elapsed()
    );
    let mut expected: Vec<Vec<Value>> = Vec::new();
    for (g, acc) in &groups {
        expected.push(vec![
            int4_or_null(*g),
            Value::Int8(acc.count_star),
            Value::Int8(acc.count_v),
            if acc.count_v > 0 {
                Value::Int8(acc.sum_v)
            } else {
                Value::Null
            },
            int4_or_null(acc.min_v),
            int4_or_null(acc.max_v),
            avg_int(acc.sum_v, acc.count_v),
        ]);
    }
    assert_eq!(rows.len(), expected.len(), "one row per group");
    assert_eq!(sorted_text(&rows), sorted_text(&expected));

    // Multi-key GROUP BY (legacy key path).
    let rows = rows_of(
        session
            .execute("SELECT g, h, COUNT(*), SUM(v) FROM big GROUP BY g, h;")
            .unwrap(),
    );
    let mut expected: Vec<Vec<Value>> = Vec::new();
    for ((g, h), acc) in &multi {
        expected.push(vec![
            int4_or_null(*g),
            Value::Int4(*h as i32),
            Value::Int8(acc.count_star),
            if acc.count_v > 0 {
                Value::Int8(acc.sum_v)
            } else {
                Value::Null
            },
        ]);
    }
    assert_eq!(rows.len(), expected.len());
    assert_eq!(sorted_text(&rows), sorted_text(&expected));

    // HAVING over groups.
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*) FROM big GROUP BY g HAVING COUNT(*) > 7500;")
            .unwrap(),
    );
    let expected: Vec<Vec<Value>> = groups
        .iter()
        .filter(|(_, acc)| acc.count_star > 7500)
        .map(|(g, acc)| vec![int4_or_null(*g), Value::Int8(acc.count_star)])
        .collect();
    assert_eq!(sorted_text(&rows), sorted_text(&expected));

    // WHERE predicate (range over v).
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), SUM(v) FROM big WHERE v BETWEEN -100 AND 100;")
            .unwrap(),
    );
    assert_eq!(int(&rows[0][0]), between.count_star);
    assert_eq!(int(&rows[0][1]), between.sum_v);

    // WHERE + GROUP BY narrowing to one group.
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*) FROM big WHERE g = 3 GROUP BY g;")
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int4(3));
    assert_eq!(int(&rows[0][1]), groups[&Some(3)].count_star);

    // Empty result: global COUNT is 0, SUM/AVG are NULL.
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), SUM(v), AVG(v) FROM big WHERE v > 100000;")
            .unwrap(),
    );
    assert_eq!(rows, vec![vec![Value::Int8(0), Value::Null, Value::Null]]);
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*) FROM big WHERE v > 100000 GROUP BY g;")
            .unwrap(),
    );
    assert!(rows.is_empty());

    // Restart-stability: reopen and re-read the same aggregates.
    drop(session);
    let mut session = open(&root);
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(v), SUM(v) FROM big;")
            .unwrap(),
    );
    assert_eq!(int(&rows[0][0]), global.count_star);
    assert_eq!(int(&rows[0][1]), global.count_v);
    assert_eq!(int(&rows[0][2]), global.sum_v);
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*) FROM big GROUP BY g;")
            .unwrap(),
    );
    assert_eq!(rows.len(), groups.len());
    cleanup(&root);
}

#[test]
fn par_agg_empty_table() {
    let root = scratch("empty");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE e (g INTEGER, v INTEGER);")
        .unwrap();
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM e;")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int8(0),
            Value::Int8(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]]
    );
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*) FROM e GROUP BY g;")
            .unwrap(),
    );
    assert!(rows.is_empty());
    cleanup(&root);
}

#[test]
fn par_agg_single_row() {
    let root = scratch("single");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE one (g INTEGER, v INTEGER);")
        .unwrap();
    session.execute("INSERT INTO one VALUES (5, 42);").unwrap();
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM one;")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int8(1),
            Value::Int8(1),
            Value::Int8(42),
            Value::Int4(42),
            Value::Int4(42),
            avg_int(42, 1),
        ]]
    );
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*), SUM(v) FROM one GROUP BY g;")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![Value::Int4(5), Value::Int8(1), Value::Int8(42)]]
    );
    cleanup(&root);
}

#[test]
fn par_agg_all_null_column() {
    let root = scratch("allnull");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE an (g INTEGER, v INTEGER);")
        .unwrap();
    let mut batch = String::from("INSERT INTO an VALUES ");
    for i in 0..1000 {
        if i > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({}, NULL)", i % 5));
    }
    batch.push(';');
    session.execute(&batch).unwrap();
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM an;")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int8(1000),
            Value::Int8(0),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]]
    );
    let rows = rows_of(
        session
            .execute("SELECT g, COUNT(*), COUNT(v) FROM an GROUP BY g;")
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(int(&row[1]), 200);
        assert_eq!(int(&row[2]), 0);
    }
    cleanup(&root);
}
