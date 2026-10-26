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
//! Parallel equi-INNER hash join, in-process evidence.
//!
//! The parallel path (`perform_join_parallel` in `join/support.rs`) engages
//! automatically inside `perform_join` for large hashable equi-INNER joins
//! and declines (`None`) to the unchanged sequential path otherwise. These
//! tests therefore assert *observable behaviour* through public SQL
//! (`Executor::new` plus `PlomidStorageEngine::create` in temp dirs):
//!
//! * inner exactness vs hand-computed folds (20k/500 shapes with NULLs and
//!   duplicates, reusing the `par_join_partition.rs` table style),
//! * unhashable-key joins stay correct (the gate decline itself is proven by
//!   the in-crate unit test `parallel_gate_declines_unhashable_keys`),
//! * small joins stay correct (decline proven by
//!   `parallel_gate_declines_small_inputs`),
//! * multi-partition determinism (same rows twice, sorted compare),
//! * empty inputs on either side,
//! * skew visibility: per-partition row counts showing the no-salting
//!   imbalance,
//! * in-process timing (release profile preferred) for 5k/20k/100k probes.
//!
//! Timings are figures, not assertions: libtest thread contention adds noise,
//! so ratios (not absolutes) are the signal. Run the timing test in release:
//! `cargo test --release -p plomid-executor --test par_join_parallel timing
//! -- --nocapture`.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Build-side row count (`jr.k = 0..500`, covers every non-NULL probe key).
const JR_N: i64 = 500;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 1_000;
/// Hot key for the skewed table (80% of non-NULL rows).
const HOT_KEY: i64 = 42;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-parjoinpar-{tag}-{id}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &Path) -> Executor<PlomidStorageEngine> {
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

fn opt_int(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Int8(n) => Some(*n),
        Value::Int4(n) => Some(*n as i64),
        Value::Int2(n) => Some(*n as i64),
        other => panic!("expected int or NULL, got {other:?}"),
    }
}

fn query(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    rows_of(
        session
            .execute(sql)
            .unwrap_or_else(|e| panic!("SQL failed: {sql}: {e:?}")),
    )
}

/// `SELECT COUNT(*), SUM(...)` with NULL-sum (empty input) folded to 0.
fn count_sum(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> (i64, i64) {
    let rows = query(session, sql);
    assert_eq!(rows.len(), 1, "aggregate must return one row: {sql}");
    assert_eq!(rows[0].len(), 2, "expected COUNT + SUM: {sql}");
    (int(&rows[0][0]), opt_int(&rows[0][1]).unwrap_or(0))
}

fn count_only(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> i64 {
    let rows = query(session, sql);
    assert_eq!(rows.len(), 1, "aggregate must return one row: {sql}");
    int(&rows[0][0])
}

/// Type-agnostic row fingerprint for order-insensitive comparison.
fn row_key(row: &[Value]) -> String {
    row.iter()
        .map(|v| v.to_sql_text())
        .collect::<Vec<_>>()
        .join("|")
}

// ---------------------------------------------------------------------------
// Deterministic row generation: single source of truth for loader + checks.
// ---------------------------------------------------------------------------

/// Uniform probe key: NULL every 11th row, else `i % 500`.
fn gen_k(i: i64) -> Option<i64> {
    if i % 11 == 0 {
        None
    } else {
        Some(i % 500)
    }
}

/// Skewed probe key: NULL every 11th row, else the hot key 80% of the time.
fn gen_skew_k(i: i64) -> Option<i64> {
    if i % 11 == 0 {
        None
    } else if i % 10 < 8 {
        Some(HOT_KEY)
    } else {
        Some(i % 500)
    }
}

fn cell(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

struct JoinExpect {
    /// Probe rows with NULL key.
    nulls: i64,
    /// Inner-join rows (every non-NULL probe key matches; build side is 1:1).
    inner_count: i64,
    /// SUM(probe.v) over the inner join.
    inner_sum: i64,
}

fn load_probe(
    session: &mut Executor<PlomidStorageEngine>,
    table: &str,
    n: i64,
    keygen: fn(i64) -> Option<i64>,
) -> JoinExpect {
    session
        .execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k INT, v BIGINT);"
        ))
        .unwrap();
    let mut expect = JoinExpect {
        nulls: 0,
        inner_count: 0,
        inner_sum: 0,
    };
    let mut batch = format!("INSERT INTO {table} VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        match keygen(i) {
            None => expect.nulls += 1,
            Some(_) => {
                expect.inner_count += 1;
                expect.inner_sum += i;
            }
        }
        batch.push_str(&format!("({i}, {}, {i})", cell(keygen(i))));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == n {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = format!("INSERT INTO {table} VALUES ");
            in_batch = 0;
        }
    }
    expect
}

fn load_build(session: &mut Executor<PlomidStorageEngine>) {
    session
        .execute("CREATE TABLE jr (k INT PRIMARY KEY, w TEXT);")
        .unwrap();
    let mut batch = String::from("INSERT INTO jr VALUES ");
    for k in 0..JR_N {
        if k > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({k}, 'w{k}')"));
    }
    batch.push(';');
    session.execute(&batch).unwrap();
}

// ---------------------------------------------------------------------------
// 1. Inner exactness vs hand-computed folds (20k/500, NULLs + duplicates).
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_inner_exactness() {
    let root = scratch("exact");
    let mut session = open(&root);
    let n = 5_000;
    let expect = load_probe(&mut session, "jl", n, gen_k);
    load_build(&mut session);
    assert_eq!(expect.nulls + expect.inner_count, n);

    let (inner_count, inner_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(inner_count, expect.inner_count);
    assert_eq!(inner_sum, expect.inner_sum);
    eprintln!("parallel inner fingerprint: count={inner_count} sum_v={inner_sum}");

    // NULL keys never join.
    let null_inner = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl JOIN jr ON jl.k = jr.k WHERE jl.k IS NULL;",
    );
    assert_eq!(null_inner, 0, "NULL keys must never match in an inner join");

    // Duplicate fan-out on the probe side is linear.
    let probe_freq = count_only(&mut session, "SELECT COUNT(*) FROM jl WHERE k = 42;");
    let join_freq = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl JOIN jr ON jl.k = jr.k WHERE jl.k = 42;",
    );
    assert_eq!(join_freq, probe_freq, "N probe dups x 1 build row = N rows");
    session
        .execute(&format!(
            "INSERT INTO jl VALUES ({n}, 42, {n}), ({}, 42, {}), ({}, 42, {});",
            n + 1,
            n + 1,
            n + 2,
            n + 2
        ))
        .unwrap();
    let (inner_after, sum_after) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(inner_after, expect.inner_count + 3);
    assert_eq!(sum_after, expect.inner_sum + n + (n + 1) + (n + 2));
    eprintln!("fan-out after +3 dups: count={inner_after} sum_v={sum_after}");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. Unhashable keys stay correct (sequential path; the decline itself is
//    proven by the in-crate `parallel_gate_declines_unhashable_keys` unit
//    test). Arrays have no `join_hash_key`, so the parallel gate must pass.
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_unhashable_takes_sequential() {
    let root = scratch("unhash");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE al (id BIGINT PRIMARY KEY, k INT[], v BIGINT);")
        .unwrap();
    session
        .execute("CREATE TABLE ar (k INT[], w TEXT);")
        .unwrap();
    session
        .execute(
            "INSERT INTO al VALUES \
             (0, ARRAY[1,2], 10), (1, NULL, 20), (2, ARRAY[3], 30), \
             (3, ARRAY[1,2], 40), (4, ARRAY[9], 50), (5, ARRAY[3], 60);",
        )
        .unwrap();
    session
        .execute("INSERT INTO ar VALUES (ARRAY[1,2], 'a'), (ARRAY[3], 'b');")
        .unwrap();

    // Hand fold: rows 0+3 match [1,2] (v = 10+40), rows 2+5 match [3]
    // (v = 30+60); the NULL row and the [9] row match nothing.
    let (count, sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(al.v) FROM al JOIN ar ON al.k = ar.k;",
    );
    assert_eq!(count, 4);
    assert_eq!(sum, 140);
    let null_inner = count_only(
        &mut session,
        "SELECT COUNT(*) FROM al JOIN ar ON al.k = ar.k WHERE al.k IS NULL;",
    );
    assert_eq!(null_inner, 0);
    eprintln!("unhashable (INT[]) join: count={count} sum_v={sum} verdict=PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 3. Small joins stay correct (decline proven by the in-crate
//    `parallel_gate_declines_small_inputs` unit test).
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_small_join_is_correct() {
    let root = scratch("small");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE sl (id BIGINT PRIMARY KEY, k INT, v BIGINT);")
        .unwrap();
    session
        .execute("CREATE TABLE sr (k INT PRIMARY KEY, w TEXT);")
        .unwrap();
    session
        .execute("INSERT INTO sl VALUES (1, 1, 10), (2, NULL, 20), (3, 2, 30), (4, 1, 40);")
        .unwrap();
    session
        .execute("INSERT INTO sr VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .unwrap();

    let rows = query(
        &mut session,
        "SELECT sl.id, sl.k, sl.v, sr.k, sr.w FROM sl JOIN sr ON sl.k = sr.k;",
    );
    let mut got: Vec<String> = rows.iter().map(|r| row_key(r)).collect();
    got.sort();
    // Hand fold: (1,1,10)x(1,'a'), (3,2,30)x(2,'b'), (4,1,40)x(1,'a').
    let mut want = vec![
        "1|1|10|1|a".to_string(),
        "3|2|30|2|b".to_string(),
        "4|1|40|1|a".to_string(),
    ];
    want.sort();
    assert_eq!(got, want);
    eprintln!("small join rows: {got:?} verdict=PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 4. Multi-partition determinism: same rows twice (sorted compare), and the
//    sorted multiset equals the hand-computed fold.
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_multipartition_determinism() {
    let root = scratch("determ");
    let mut session = open(&root);
    let n = 5_000;
    let expect = load_probe(&mut session, "jl", n, gen_k);
    load_build(&mut session);

    let sql = "SELECT jl.id, jl.k, jl.v, jr.k, jr.w FROM jl JOIN jr ON jl.k = jr.k;";
    let run = |session: &mut Executor<PlomidStorageEngine>| {
        let mut keys: Vec<String> = query(session, sql).iter().map(|r| row_key(r)).collect();
        keys.sort();
        keys
    };
    let first = run(&mut session);
    let second = run(&mut session);
    assert_eq!(first, second, "parallel join must be deterministic");
    assert_eq!(first.len(), expect.inner_count as usize);

    // Hand-computed multiset: every non-NULL probe row joins its build row.
    let mut want = Vec::with_capacity(expect.inner_count as usize);
    for i in 0..n {
        if let Some(k) = gen_k(i) {
            want.push(format!("{i}|{k}|{i}|{k}|w{k}"));
        }
    }
    want.sort();
    assert_eq!(first, want, "parallel multiset must equal the hand fold");
    eprintln!(
        "determinism: rows={} twice-identical + hand-fold-equal verdict=PASS",
        first.len()
    );

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 5. Empty inputs on either side (and both).
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_empty_sides() {
    let root = scratch("empty");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE el (id BIGINT PRIMARY KEY, k INT, v BIGINT);")
        .unwrap();
    session
        .execute("CREATE TABLE er (k INT PRIMARY KEY, w TEXT);")
        .unwrap();

    // Both empty.
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM el JOIN er ON el.k = er.k;"
        ),
        0
    );
    // Left non-empty, right empty.
    session
        .execute("INSERT INTO el VALUES (1, 1, 10), (2, 2, 20);")
        .unwrap();
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM el JOIN er ON el.k = er.k;"
        ),
        0
    );
    // Right non-empty, left empty.
    session.execute("DELETE FROM el;").unwrap();
    session
        .execute("INSERT INTO er VALUES (1, 'a'), (2, 'b');")
        .unwrap();
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM el JOIN er ON el.k = er.k;"
        ),
        0
    );
    // Both non-empty sanity.
    session
        .execute("INSERT INTO el VALUES (1, 1, 10);")
        .unwrap();
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM el JOIN er ON el.k = er.k;"
        ),
        1
    );
    eprintln!("empty sides: 0/0/0/1 verdict=PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 6. Skew visibility: per-partition row counts (SQL-level `k % P` buckets)
//    must sum to the baseline while showing the hot-key imbalance that
//    the current no-salting partitioning leaves in place.
// ---------------------------------------------------------------------------

#[test]
fn par_join_parallel_skew_partition_counts() {
    let root = scratch("skewpar");
    let mut session = open(&root);
    let n = 5_000;
    let expect = load_probe(&mut session, "js", n, gen_skew_k);
    load_build(&mut session);
    assert!(
        expect.inner_count > 0 && expect.nulls + expect.inner_count == n,
        "loader must cover all probe rows"
    );

    let (base_count, base_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(js.v) FROM js JOIN jr ON js.k = jr.k;",
    );
    assert_eq!(
        (base_count, base_sum),
        (expect.inner_count, expect.inner_sum)
    );

    // Partition the matched key space the way the engine does (by key hash);
    // `k % P` buckets are the SQL-observable analogue: disjoint, complete,
    // NULL-free (NULLs never join, matching the per-partition NULL drop).
    for parts in [2_i64, 4] {
        let mut counts = Vec::new();
        let mut sums = Vec::new();
        for p in 0..parts {
            let (c, s) = count_sum(
                &mut session,
                &format!(
                    "SELECT COUNT(*), SUM(js.v) FROM js JOIN jr ON js.k = jr.k \
                     WHERE ((js.k % {parts}) = {p});"
                ),
            );
            counts.push(c);
            sums.push(s);
        }
        let total: i64 = counts.iter().sum();
        let total_sum: i64 = sums.iter().sum();
        assert_eq!(total, base_count, "P={parts} buckets must cover the join");
        assert_eq!(total_sum, base_sum);
        let max = *counts.iter().max().unwrap();
        let min = (*counts.iter().min().unwrap()).max(1);
        let ratio = max as f64 / min as f64;
        eprintln!(
            "skew P={parts}: counts={counts:?} max/min={ratio:.2} \
             (hot key {HOT_KEY} dominates one bucket; no salting)"
        );
    }

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 7. In-process timing (release): 5k/20k/100k-probe inner joins. Reports
//    routed-path wall times (all three sizes exceed the parallel-join threshold, so all
//    three exercise the parallel path) plus correctness at every size. The
//    sequential-vs-parallel calibration table lives on
//    `PARALLEL_JOIN_THRESHOLD` in `join/support.rs`. No speed assertions:
//    libtest contention makes absolutes noisy; ratios are the signal.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_join_parallel_timing() {
    for size in [5_000_i64, 20_000, 100_000] {
        let root = scratch(&format!("timing{size}"));
        let mut session = open(&root);
        let expect = load_probe(&mut session, "jl", size, gen_k);
        load_build(&mut session);

        let sql = "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;";
        // Warmup (page caches, plan caches) outside the measurement.
        let (warm_count, warm_sum) = count_sum(&mut session, sql);
        assert_eq!(
            (warm_count, warm_sum),
            (expect.inner_count, expect.inner_sum)
        );

        let mut best_ms = f64::INFINITY;
        for _ in 0..3 {
            let started = Instant::now();
            let (count, sum) = count_sum(&mut session, sql);
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            best_ms = best_ms.min(ms);
            assert_eq!((count, sum), (expect.inner_count, expect.inner_sum));
        }
        eprintln!(
            "timing probe={size}: best-of-3 {best_ms:.1}ms matched={} \
             (sequential-vs-parallel table on PARALLEL_JOIN_THRESHOLD)",
            expect.inner_count
        );

        cleanup(&root);
    }
}
