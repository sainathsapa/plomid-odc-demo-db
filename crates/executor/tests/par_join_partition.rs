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
//! ParallelHashJoin slice-3 executor-side evidence: partition-concat equivalence.
//!
//! These tests lock the exact semantics a parallel hash join must preserve,
//! using only EXISTING public SQL (`Executor::new` plus
//! `PlomidStorageEngine::create`). No new engine API is introduced or used:
//! partitions are simulated with an added deterministic probe-side predicate
//! `(jl.k % P) = p` (the `%` operator is existing syntax), which splits the
//! matched key space into P disjoint, complete buckets. Concatenating the P
//! partial results in partition order must equal the unpartitioned join
//! aggregate — the order-insensitive contract a parallel hash join relies on.
//!
//! NULL handling per partition: `NULL % P` is NULL, so NULL probe keys match
//! no partition predicate. That is exactly right for the inner join (NULLs
//! never join); for the LEFT JOIN variant NULL probe rows are pinned to
//! partition 0 (`OR jl.k IS NULL`), so every input row is owned by exactly
//! one partition — never lost, never duplicated.
//!
//! Evidence emitted via `eprintln!` (read with `--nocapture`):
//! * baseline fingerprints (inner count/sum, left count, NULL-pad count),
//! * per-partition counts and wall times for P in {1, 2, 4},
//! * uniform-vs-skewed (80%-hot-key) balance/imbalance ratios.
//!
//! Timings are in-process figures; libtest thread contention adds noise, so
//! ratios (not absolutes) are the signal. Release profile preferred.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Probe-side row count.
// Join parallelism engages above 2K rows; 5K exercises the
// parallel path with identical null/skew cadences at 1/4 the cost.
const N: i64 = 5_000;
/// Build-side row count (`jr.k = 0..500`, covers every non-NULL probe key).
const JR_N: i64 = 500;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 500;
/// Hot key for the skewed table (80% of non-NULL rows).
const HOT_KEY: i64 = 42;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("plomid-parjoin-{tag}-{id}-{}", std::process::id()));
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
    /// SUM(probe.v) over all probe rows (LEFT JOIN preserves each row once).
    total_sum: i64,
    /// Probe-side frequency of the hot key (fan-out canary).
    hot_freq: i64,
}

fn load_probe(
    session: &mut Executor<PlomidStorageEngine>,
    table: &str,
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
        total_sum: 0,
        hot_freq: 0,
    };
    let mut batch = format!("INSERT INTO {table} VALUES ");
    let mut in_batch = 0;
    for i in 0..N {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let k = keygen(i);
        match k {
            None => expect.nulls += 1,
            Some(kk) => {
                expect.inner_count += 1;
                expect.inner_sum += i;
                if kk == HOT_KEY {
                    expect.hot_freq += 1;
                }
            }
        }
        expect.total_sum += i;
        batch.push_str(&format!("({i}, {}, {i})", cell(k)));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == N {
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
// 1. Baseline fingerprints: inner count/sum, left count + NULL pad, fan-out,
//    NULL-never-joins.
// ---------------------------------------------------------------------------

#[test]
fn par_join_baseline_fingerprints() {
    let root = scratch("base");
    let mut session = open(&root);
    let expect = load_probe(&mut session, "jl", gen_k);
    load_build(&mut session);
    assert_eq!(expect.nulls + expect.inner_count, N);

    // Inner equi-join fingerprint vs the independent Rust fold.
    let (inner_count, inner_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(inner_count, expect.inner_count);
    assert_eq!(inner_sum, expect.inner_sum);
    eprintln!("inner fingerprint: count={inner_count} sum_v={inner_sum}");

    // LEFT JOIN preserves every probe row exactly once.
    let left_count = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl LEFT JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(left_count, N, "left join must preserve all probe rows");
    let (_, left_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl LEFT JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(left_sum, expect.total_sum);
    eprintln!("left fingerprint: count={left_count} sum_v={left_sum}");

    // NULL pad check: exactly the NULL-key rows come back padded.
    let padded = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl LEFT JOIN jr ON jl.k = jr.k WHERE jr.k IS NULL;",
    );
    assert_eq!(padded, expect.nulls);
    let padded_text = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl LEFT JOIN jr ON jl.k = jr.k WHERE jr.w IS NULL;",
    );
    assert_eq!(padded_text, expect.nulls);
    eprintln!("null pad: padded={padded} (probe nulls={})", expect.nulls);

    // NULL-never-joins: no NULL-key row appears in the inner join.
    let null_inner = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl JOIN jr ON jl.k = jr.k WHERE jl.k IS NULL;",
    );
    assert_eq!(null_inner, 0, "NULL keys must never match in an inner join");

    // Duplicate fan-out on the probe side (`jr.k` is PK so build fan-out is
    // 1): the join multiplicity of a key equals its probe-side frequency.
    let probe_freq = count_only(&mut session, "SELECT COUNT(*) FROM jl WHERE k = 42;");
    assert_eq!(probe_freq, expect.hot_freq);
    let join_freq = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl JOIN jr ON jl.k = jr.k WHERE jl.k = 42;",
    );
    assert_eq!(join_freq, probe_freq, "N probe dups x 1 build row = N rows");
    // Adding 3 duplicate probe rows fans out linearly (+3 join rows).
    session
        .execute(&format!(
            "INSERT INTO jl VALUES ({N}, 42, {N}), ({}, 42, {}), ({}, 42, {});",
            N + 1,
            N + 1,
            N + 2,
            N + 2
        ))
        .unwrap();
    let join_freq_after = count_only(
        &mut session,
        "SELECT COUNT(*) FROM jl JOIN jr ON jl.k = jr.k WHERE jl.k = 42;",
    );
    assert_eq!(join_freq_after, probe_freq + 3);
    let (inner_after, _) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(inner_after, expect.inner_count + 3);
    eprintln!(
        "fan-out: key=42 probe={probe_freq} join={join_freq} join_after_dup={join_freq_after}"
    );

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. Partition-concat equivalence on uniform keys, P in {1, 2, 4}.
// ---------------------------------------------------------------------------

#[test]
fn par_join_partition_concat_uniform() {
    let root = scratch("uniform");
    let mut session = open(&root);
    let expect = load_probe(&mut session, "jl", gen_k);
    load_build(&mut session);

    let (base_count, base_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k;",
    );
    assert_eq!(
        (base_count, base_sum),
        (expect.inner_count, expect.inner_sum)
    );

    for parts in [1i64, 2, 4] {
        let started = Instant::now();
        let mut counts = Vec::new();
        let mut sums = Vec::new();
        let mut times_ms = Vec::new();
        for p in 0..parts {
            let sql = format!(
                "SELECT COUNT(*), SUM(jl.v) FROM jl JOIN jr ON jl.k = jr.k \
                 WHERE ((jl.k % {parts}) = {p});"
            );
            let t = Instant::now();
            let (c, s) = count_sum(&mut session, &sql);
            times_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            counts.push(c);
            sums.push(s);
        }
        let total_elapsed = started.elapsed();
        let concat_count: i64 = counts.iter().sum();
        let concat_sum: i64 = sums.iter().sum();
        assert_eq!(
            concat_count, base_count,
            "P={parts} partition counts must sum to the baseline"
        );
        assert_eq!(
            concat_sum, base_sum,
            "P={parts} partition sums must sum to the baseline"
        );
        eprintln!(
            "uniform inner P={parts}: counts={counts:?} sums={sums:?} \
             per-part-ms={times_ms:.2?} total={total_elapsed:?} verdict=PASS"
        );
    }

    // LEFT JOIN variant: NULL probe rows are pinned to partition 0 so every
    // input row is owned by exactly one partition.
    for parts in [1i64, 2, 4] {
        let mut counts = Vec::new();
        let mut pads = Vec::new();
        for p in 0..parts {
            let pred = if p == 0 {
                format!("(((jl.k % {parts}) = 0) OR jl.k IS NULL)")
            } else {
                format!("(((jl.k % {parts}) = {p}))")
            };
            let (c, _) = count_sum(
                &mut session,
                &format!(
                    "SELECT COUNT(*), SUM(jl.v) FROM jl LEFT JOIN jr ON jl.k = jr.k WHERE {pred};"
                ),
            );
            let pad = count_only(
                &mut session,
                &format!(
                    "SELECT COUNT(*) FROM jl LEFT JOIN jr ON jl.k = jr.k WHERE {pred} AND jr.k IS NULL;"
                ),
            );
            counts.push(c);
            pads.push(pad);
        }
        assert_eq!(
            counts.iter().sum::<i64>(),
            N,
            "P={parts} left partitions must cover all probe rows"
        );
        assert_eq!(
            pads.iter().sum::<i64>(),
            expect.nulls,
            "P={parts} padded rows must equal the probe NULL count"
        );
        if parts > 1 {
            assert_eq!(pads[0], expect.nulls, "NULLs pinned to partition 0");
            assert!(
                pads[1..].iter().all(|&x| x == 0),
                "no padded rows outside partition 0"
            );
        }
        eprintln!("uniform left P={parts}: counts={counts:?} pads={pads:?} verdict=PASS");
    }

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 3. Skewed keys (80% hot key): concat stays exact, balance degrades.
// ---------------------------------------------------------------------------

#[test]
fn par_join_partition_concat_skewed() {
    let root = scratch("skew");
    let mut session = open(&root);
    let expect = load_probe(&mut session, "js", gen_skew_k);
    load_build(&mut session);
    assert!(
        expect.hot_freq * 10 >= expect.inner_count * 7,
        "hot key must dominate: hot={} matched={}",
        expect.hot_freq,
        expect.inner_count
    );

    let (base_count, base_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(js.v) FROM js JOIN jr ON js.k = jr.k;",
    );
    assert_eq!(
        (base_count, base_sum),
        (expect.inner_count, expect.inner_sum)
    );
    eprintln!(
        "skew baseline: matched={base_count} sum_v={base_sum} hot_freq={} ({:.1}% of matched)",
        expect.hot_freq,
        expect.hot_freq as f64 / base_count as f64 * 100.0
    );

    for parts in [1i64, 2, 4] {
        let mut counts = Vec::new();
        let mut times_ms = Vec::new();
        for p in 0..parts {
            let sql = format!(
                "SELECT COUNT(*), SUM(js.v) FROM js JOIN jr ON js.k = jr.k \
                 WHERE ((js.k % {parts}) = {p});"
            );
            let t = Instant::now();
            let (c, s) = count_sum(&mut session, &sql);
            times_ms.push(t.elapsed().as_secs_f64() * 1000.0);
            counts.push((c, s));
        }
        let concat_count: i64 = counts.iter().map(|&(c, _)| c).sum();
        let concat_sum: i64 = counts.iter().map(|&(_, s)| s).sum();
        assert_eq!(
            concat_count, base_count,
            "skewed P={parts} counts must match"
        );
        assert_eq!(concat_sum, base_sum, "skewed P={parts} sums must match");
        let (lo, hi) = counts
            .iter()
            .map(|&(c, _)| c)
            .fold((i64::MAX, i64::MIN), |(lo, hi), c| (lo.min(c), hi.max(c)));
        let ratio = hi as f64 / lo.max(1) as f64;
        eprintln!(
            "skewed inner P={parts}: counts={counts:?} per-part-ms={times_ms:.2?} \
             max/min={ratio:.2} verdict=PASS"
        );
    }

    cleanup(&root);
}
