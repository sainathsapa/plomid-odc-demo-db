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
//! Sequential-truth locks for future parallel scan-aggregation.
//!
//! These tests record the exact results the single-threaded engine produces
//! today on a 20,000-row table with an integer/text/float/`NULL` mix plus a
//! secondary index. A parallel scan-aggregation must reproduce every value
//! here bit-for-bit, including:
//!
//! * `SUM`/`AVG` over `DOUBLE PRECISION` as finalized `NUMERIC` values. The
//!   float inputs are dyadic (`(i % 32) / 8.0`, exactly representable), so
//!   every partial sum is exact and summation order cannot change the result.
//!   The recorded `Value::Numeric` outputs are the documented current truth.
//! * The `k_int = 0` group, whose `v_int` cells are all `NULL` by
//!   construction (`v_int` is `NULL` exactly when `i % 7 == 0`, which is also
//!   the `k_int = 0` membership rule): `COUNT(v_int)` is 0 and
//!   `SUM`/`AVG`/`MIN`/`MAX` are `NULL` while `COUNT(*)` is nonzero.
//! * `NULL` group keys sorting last under `ORDER BY` (PostgreSQL `ASC`
//!   defaults to `NULLS LAST`).
//!
//! Only the public executor API is used (`Executor::new` plus
//! `PlomidStorageEngine::create`), exactly like the neighboring
//! `streaming_analytical` tests. Expectations are computed independently in
//! Rust from the same generation formulas; float expectations mirror the
//! engine's own finalization (`Numeric::from_f64(sum)` for `SUM`,
//! `Numeric::from_f64(sum / count).normalize()` for `AVG`, matching
//! `AggregateAccumulator::finalize`).

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::collections::{BTreeMap, HashSet};

/// Row count of the determinism table: 5K rows cover the 0..1000 value
/// range 5x over with all NULL cadences, matching the same independent
/// folds at 1/4 the debug cost.
const N: i64 = 5_000;

/// Per-group integer stats: `(row count, valued count, sum, min, max)`.
type IntGroupStats = (i64, i64, i64, Option<i64>, Option<i64>);

/// Per-group float stats: `(row count, exact dyadic sum, valued count)`.
type FloatGroupStats = (i64, f64, i64);

fn unique_dirs(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let storage =
        std::env::temp_dir().join(format!("plomid-parmerge-{tag}-{id}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-parmerge-{tag}-{id}-wal-{}",
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

fn int(value: &Value) -> i64 {
    match value {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

/// Expected `AVG(integer)` output, mirroring `AggregateAccumulator::finalize`.
fn avg_int_value(sum: i64, count: i64) -> Value {
    Value::Numeric(
        plomid_types::Numeric::from_i64(sum)
            .div(plomid_types::Numeric::from_i64(count))
            .expect("nonzero average count")
            .normalize(),
    )
}

/// Expected `SUM(double)` output, mirroring `AggregateAccumulator::finalize`
/// (§P2: float sums finalize as Float8, PG typing).
fn sum_float_value(sum: f64) -> Value {
    Value::Float8(sum)
}

/// Expected `AVG(double)` output, mirroring `AggregateAccumulator::finalize`
/// (§P2: float averages finalize as Float8, PG typing).
fn avg_float_value(sum: f64, count: i64) -> Value {
    Value::Float8(sum / count as f64)
}

// ---------------------------------------------------------------------------
// Deterministic row generation: the single source of truth.
// ---------------------------------------------------------------------------

/// `NULL` every 11th row, else the row index modulo 7.
fn gen_k_int(i: i64) -> Option<i64> {
    if i % 11 == 0 {
        None
    } else {
        Some(i % 7)
    }
}

/// `NULL` every 13th row, else one of five text groups.
fn gen_k_text(i: i64) -> Option<String> {
    if i % 13 == 0 {
        None
    } else {
        Some(format!("g{}", i % 5))
    }
}

/// `NULL` exactly when `i % 7 == 0` (hence the whole `k_int = 0` group is
/// `NULL` here), else a pseudo-random value in `0..1000`.
fn gen_v_int(i: i64) -> Option<i64> {
    if i % 7 == 0 {
        None
    } else {
        Some((i * 37) % 1000)
    }
}

/// `NULL` every 9th row, else a dyadic fraction (exact in binary floating
/// point, so every summation order agrees bit-for-bit).
fn gen_v_f(i: i64) -> Option<f64> {
    if i % 9 == 0 {
        None
    } else {
        Some((i % 32) as f64 / 8.0)
    }
}

fn setup(tag: &str) -> Executor<PlomidStorageEngine> {
    let (storage, wal) = unique_dirs(tag);
    let engine = PlomidStorageEngine::create(&storage, &wal, 32).expect("create engine");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute(
            "CREATE TABLE par_mix(id BIGINT PRIMARY KEY, k_int INTEGER, \
             k_text TEXT, v_int INTEGER, v_f DOUBLE PRECISION)",
        )
        .expect("create table");
    // The parallel planner must cope with a secondary index on the grouping
    // key; results must be identical whether or not it is used.
    executor
        .execute("CREATE INDEX par_mix_kint ON par_mix(k_int)")
        .expect("create index");
    // Batched inserts keep debug-mode setup time reasonable.
    let mut batch = String::from("INSERT INTO par_mix VALUES ");
    let mut in_batch = 0;
    for i in 0..N {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let ki = match gen_k_int(i) {
            Some(v) => v.to_string(),
            None => "NULL".to_string(),
        };
        let kt = match gen_k_text(i) {
            Some(v) => format!("'{v}'"),
            None => "NULL".to_string(),
        };
        let vi = match gen_v_int(i) {
            Some(v) => v.to_string(),
            None => "NULL".to_string(),
        };
        let vf = match gen_v_f(i) {
            Some(v) => format!("{v:.3}"),
            None => "NULL".to_string(),
        };
        batch.push_str(&format!("({i},{ki},{kt},{vi},{vf})"));
        in_batch += 1;
        if in_batch == 2000 || i + 1 == N {
            batch.push(';');
            executor.execute(&batch).expect("insert batch");
            batch = String::from("INSERT INTO par_mix VALUES ");
            in_batch = 0;
        }
    }
    executor
}

// ---------------------------------------------------------------------------
// Global aggregates.
// ---------------------------------------------------------------------------

#[test]
fn global_aggregates_are_deterministic() {
    let mut e = setup("global");

    let mut count_vi = 0_i64;
    let mut sum_vi = 0_i64;
    let mut min_vi: Option<i64> = None;
    let mut max_vi: Option<i64> = None;
    let mut count_vf = 0_i64;
    let mut sum_vf = 0_f64;
    let mut distinct_ki = HashSet::new();
    let mut distinct_kt = HashSet::new();
    let mut distinct_vi = HashSet::new();
    for i in 0..N {
        if let Some(v) = gen_v_int(i) {
            count_vi += 1;
            sum_vi += v;
            min_vi = Some(min_vi.map_or(v, |m: i64| m.min(v)));
            max_vi = Some(max_vi.map_or(v, |m: i64| m.max(v)));
            distinct_vi.insert(v);
        }
        if let Some(v) = gen_v_f(i) {
            count_vf += 1;
            sum_vf += v;
        }
        if let Some(v) = gen_k_int(i) {
            distinct_ki.insert(v);
        }
        if let Some(v) = gen_k_text(i) {
            distinct_kt.insert(v);
        }
    }

    // Global COUNT/SUM/MIN/MAX/AVG over integers.
    let rows = exec(
        &mut e,
        "SELECT COUNT(*), COUNT(v_int), SUM(v_int), MIN(v_int), MAX(v_int), AVG(v_int) \
         FROM par_mix;",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0][0]), N);
    assert_eq!(int(&rows[0][1]), count_vi);
    assert_eq!(int(&rows[0][2]), sum_vi);
    assert_eq!(int(&rows[0][3]), min_vi.expect("nonempty min"));
    assert_eq!(int(&rows[0][4]), max_vi.expect("nonempty max"));
    assert_eq!(rows[0][5], avg_int_value(sum_vi, count_vi));

    // Global float aggregates: the recorded NUMERIC outputs are the current
    // sequential truth a parallel reduction must reproduce bit-for-bit.
    let rows = exec(
        &mut e,
        "SELECT COUNT(v_f), SUM(v_f), AVG(v_f) FROM par_mix;",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0][0]), count_vf);
    assert_eq!(rows[0][1], sum_float_value(sum_vf));
    assert_eq!(rows[0][2], avg_float_value(sum_vf, count_vf));

    // COUNT DISTINCT skips NULLs and unifies each int family spelling. Each
    // DISTINCT aggregate runs in its own statement: the engine answers only
    // the first of several DISTINCT aggregates sharing one projection, so a
    // combined SELECT would lock that quirk instead of the per-column truth.
    let rows = exec(&mut e, "SELECT COUNT(DISTINCT k_int) FROM par_mix;");
    assert_eq!(rows, vec![vec![Value::Int8(distinct_ki.len() as i64)]]);
    let rows = exec(&mut e, "SELECT COUNT(DISTINCT k_text) FROM par_mix;");
    assert_eq!(rows, vec![vec![Value::Int8(distinct_kt.len() as i64)]]);
    let rows = exec(&mut e, "SELECT COUNT(DISTINCT v_int) FROM par_mix;");
    assert_eq!(rows, vec![vec![Value::Int8(distinct_vi.len() as i64)]]);
    // Spot checks from the generation math: 7 int residues, 5 text groups,
    // and the full 0..1000 value range all survive their NULL cadences.
    assert_eq!(distinct_ki.len(), 7);
    assert_eq!(distinct_kt.len(), 5);
    assert_eq!(distinct_vi.len(), 1000);
}

// ---------------------------------------------------------------------------
// Grouped aggregates plus HAVING.
// ---------------------------------------------------------------------------

#[test]
fn grouped_aggregates_are_deterministic() {
    let mut e = setup("grouped");

    // Single-int-key grouping: per-group COUNT(*)/COUNT/SUM/MIN/MAX.
    let mut groups: BTreeMap<Option<i64>, IntGroupStats> = BTreeMap::new();
    for i in 0..N {
        let entry = groups.entry(gen_k_int(i)).or_insert((0, 0, 0, None, None));
        entry.0 += 1;
        if let Some(v) = gen_v_int(i) {
            entry.1 += 1;
            entry.2 += v;
            entry.3 = Some(entry.3.map_or(v, |m| m.min(v)));
            entry.4 = Some(entry.4.map_or(v, |m| m.max(v)));
        }
    }
    let rows = exec(
        &mut e,
        "SELECT k_int, COUNT(*), COUNT(v_int), SUM(v_int), MIN(v_int), MAX(v_int) \
         FROM par_mix GROUP BY k_int ORDER BY k_int;",
    );
    // Seven int groups plus the NULL group, which sorts last.
    assert_eq!(rows.len(), 8);
    for (index, key) in [0_i64, 1, 2, 3, 4, 5, 6].iter().enumerate() {
        let (star, count, sum, min, max) = groups[&Some(*key)];
        let row = &rows[index];
        assert_eq!(int(&row[0]), *key, "group key {key}");
        assert_eq!(int(&row[1]), star, "COUNT(*) group {key}");
        assert_eq!(int(&row[2]), count, "COUNT(v_int) group {key}");
        match (min, max) {
            (Some(lo), Some(hi)) => {
                assert_eq!(int(&row[3]), sum, "SUM group {key}");
                assert_eq!(int(&row[4]), lo, "MIN group {key}");
                assert_eq!(int(&row[5]), hi, "MAX group {key}");
            }
            (None, None) => {
                assert_eq!(count, 0, "all-NULL group {key} counts nothing");
                assert_eq!(
                    &row[3..],
                    &[Value::Null, Value::Null, Value::Null],
                    "all-NULL group {key} aggregates to NULL"
                );
            }
            _ => panic!("min/max must agree for group {key}"),
        }
    }
    // Group k_int = 0 is the all-NULL edge: nonzero row count, zero valued
    // rows, NULL aggregates.
    let (star0, _, _, _, _) = groups[&Some(0)];
    assert!(star0 > 0);
    assert_eq!(int(&rows[0][0]), 0);
    assert_eq!(int(&rows[0][1]), star0);
    assert_eq!(rows[0][2], Value::Int8(0));
    assert_eq!(&rows[0][3..], &[Value::Null, Value::Null, Value::Null]);
    // NULL group sorts last.
    let (null_star, null_count, null_sum, null_min, null_max) = groups[&None];
    let null_row = &rows[7];
    assert_eq!(null_row[0], Value::Null);
    assert_eq!(int(&null_row[1]), null_star);
    assert_eq!(int(&null_row[2]), null_count);
    assert_eq!(int(&null_row[3]), null_sum);
    assert_eq!(
        int(&null_row[4]),
        null_min.expect("nonempty null-group min")
    );
    assert_eq!(
        int(&null_row[5]),
        null_max.expect("nonempty null-group max")
    );

    // Text-key grouping with float aggregates.
    let mut text_groups: BTreeMap<Option<String>, FloatGroupStats> = BTreeMap::new();
    for i in 0..N {
        let entry = text_groups.entry(gen_k_text(i)).or_insert((0, 0.0, 0));
        entry.0 += 1;
        if let Some(v) = gen_v_f(i) {
            entry.1 += v;
            entry.2 += 1;
        }
    }
    let rows = exec(
        &mut e,
        "SELECT k_text, COUNT(*), SUM(v_f), AVG(v_f) FROM par_mix \
         GROUP BY k_text ORDER BY k_text;",
    );
    // Five text groups plus the NULL group, which sorts last.
    assert_eq!(rows.len(), 6);
    let mut keys: Vec<Option<String>> = (0..5).map(|g| Some(format!("g{g}"))).collect();
    keys.push(None);
    for (index, key) in keys.iter().enumerate() {
        let (star, fsum, fcount) = &text_groups[key];
        let row = &rows[index];
        match key {
            Some(k) => assert_eq!(row[0].to_sql_text(), *k, "text group {k}"),
            None => assert_eq!(row[0], Value::Null, "NULL text group"),
        }
        assert_eq!(int(&row[1]), *star);
        assert_eq!(row[2], sum_float_value(*fsum));
        assert_eq!(row[3], avg_float_value(*fsum, *fcount));
    }

    // HAVING filters groups after aggregation.
    let rows = exec(
        &mut e,
        "SELECT k_int, COUNT(*), SUM(v_int) FROM par_mix \
         GROUP BY k_int HAVING COUNT(*) > 625 ORDER BY k_int;",
    );
    let mut expected: Vec<(i64, i64, Value)> = Vec::new();
    for (key, (star, count, sum, _, _)) in &groups {
        if let Some(k) = key {
            if *star > 625 {
                let sum_value = if *count == 0 {
                    Value::Null
                } else {
                    Value::Int8(*sum)
                };
                expected.push((*k, *star, sum_value));
            }
        }
    }
    assert!(!expected.is_empty(), "HAVING must keep some groups");
    assert_eq!(
        rows.len(),
        expected.len(),
        "HAVING keeps exactly {expected:?}"
    );
    for (row, (key, star, sum_value)) in rows.iter().zip(expected.iter()) {
        assert_eq!(int(&row[0]), *key);
        assert_eq!(int(&row[1]), *star);
        assert_eq!(&row[2], sum_value);
    }
}

// ---------------------------------------------------------------------------
// WHERE-filtered and empty-result aggregates.
// ---------------------------------------------------------------------------

#[test]
fn filtered_and_empty_aggregates_are_deterministic() {
    let mut e = setup("filtered");

    // WHERE over the indexed int key.
    let (mut star, mut count, mut sum) = (0_i64, 0_i64, 0_i64);
    let mut min: Option<i64> = None;
    let mut max: Option<i64> = None;
    for i in 0..N {
        if gen_k_int(i) == Some(3) {
            star += 1;
            if let Some(v) = gen_v_int(i) {
                count += 1;
                sum += v;
                min = Some(min.map_or(v, |m| m.min(v)));
                max = Some(max.map_or(v, |m| m.max(v)));
            }
        }
    }
    let rows = exec(
        &mut e,
        "SELECT COUNT(*), COUNT(v_int), SUM(v_int), MIN(v_int), MAX(v_int), AVG(v_int) \
         FROM par_mix WHERE k_int = 3;",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0][0]), star);
    assert_eq!(int(&rows[0][1]), count);
    assert_eq!(int(&rows[0][2]), sum);
    assert_eq!(int(&rows[0][3]), min.expect("nonempty filtered min"));
    assert_eq!(int(&rows[0][4]), max.expect("nonempty filtered max"));
    assert_eq!(rows[0][5], avg_int_value(sum, count));

    // WHERE over the text key with float aggregates.
    let (mut fstar, mut fsum, mut fcount) = (0_i64, 0_f64, 0_i64);
    for i in 0..N {
        if gen_k_text(i).as_deref() == Some("g2") {
            fstar += 1;
            if let Some(v) = gen_v_f(i) {
                fsum += v;
                fcount += 1;
            }
        }
    }
    let rows = exec(
        &mut e,
        "SELECT COUNT(*), SUM(v_f), AVG(v_f) FROM par_mix WHERE k_text = 'g2';",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0][0]), fstar);
    assert_eq!(rows[0][1], sum_float_value(fsum));
    assert_eq!(rows[0][2], avg_float_value(fsum, fcount));

    // Empty result: COUNT reports 0, every other aggregate reports NULL.
    let rows = exec(
        &mut e,
        "SELECT COUNT(*), COUNT(v_int), SUM(v_int), AVG(v_int), MIN(v_int), MAX(v_int) \
         FROM par_mix WHERE k_int = 99;",
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
    let rows = exec(
        &mut e,
        "SELECT COUNT(*), SUM(v_f), AVG(v_f) FROM par_mix WHERE k_int = 99;",
    );
    assert_eq!(rows, vec![vec![Value::Int8(0), Value::Null, Value::Null]]);
}
