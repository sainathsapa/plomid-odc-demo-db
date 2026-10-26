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
//! ParallelHashJoin slice-3 executor-side evidence: sequential OUTER-join
//! truth plus the pre-salt skew baseline.
//!
//! These tests lock the exact semantics the parallel OUTER path must
//! preserve, using only EXISTING public SQL (`Executor::new` plus
//! `PlomidStorageEngine::create`, temp dirs, in-process engines only).
//! Expectations are hand-computed Rust folds over the deterministic
//! generators — never path-vs-path (never SQL-vs-SQL, never
//! engine-vs-engine). Every count, pad, sum, and spot row below is derived
//! from the generator fold and then checked against one SQL query.
//!
//! Tables:
//! * `ol(id BIGINT PRIMARY KEY, k INT, v BIGINT)` 5K rows
//!   (`k = i % 500`, NULL every 11th, `v = i`) plus
//!   `orr(k INT PRIMARY KEY, w TEXT)` 500 rows (`k = 0..500`, `w = 'w{k}'`).
//!   Every non-NULL probe key matches, so LEFT pads are exactly the NULL
//!   probe rows, RIGHT pads are zero, and FULL is LEFT plus zero right pads.
//! * `sl`/`sr`: same shapes with an 80%-hot key (`42`) probe for the
//!   grouped-over-join skew baseline (exactness fingerprints only;
//!   wall-clock benchmarking lives behind the scale gate).
//!
//! Evidence emitted via `eprintln!` (read with `--nocapture`):
//! * per-kind fingerprints (inner/left/right/full counts and sums),
//! * skew grouped baseline counts plus hot-key dominance figures.
//!
//! Fingerprints are exact counts/sums, so they hold on every profile;
//! no wall-clock figures are recorded here.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Probe-side row count for both the uniform and the skewed tables.
/// 5K stays above PARALLEL_JOIN_THRESHOLD (2_000), so the parallel-join
/// path under test still engages; the build side is unchanged.
const N: i64 = 5_000;
/// Build-side row count (`k = 0..500`, covers every non-NULL probe key).
const JR_N: i64 = 500;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 500;
/// Hot key for the skewed table (80% of non-NULL rows).
const HOT_KEY: i64 = 42;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-parjoin-outer-{tag}-{id}-{}",
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
        Value::Int4(n) => i64::from(*n),
        Value::Int2(n) => i64::from(*n),
        other => panic!("expected int, got {other:?}"),
    }
}

fn opt_int(v: &Value) -> Option<i64> {
    match v {
        Value::Null => None,
        Value::Int8(n) => Some(*n),
        Value::Int4(n) => Some(i64::from(*n)),
        Value::Int2(n) => Some(i64::from(*n)),
        other => panic!("expected int or NULL, got {other:?}"),
    }
}

fn text_of(v: &Value) -> String {
    v.to_sql_text()
}

fn text_rows(rows: &[Vec<Value>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(text_of).collect())
        .collect()
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

struct OuterExpect {
    /// Probe rows with NULL key (LEFT pads; FULL left pads).
    nulls: i64,
    /// Inner-join rows (every non-NULL probe key matches; build side is 1:1).
    inner_count: i64,
    /// SUM(probe.v) over the inner join (`v = i`).
    inner_sum: i64,
    /// SUM(probe.v) over all probe rows (LEFT preserves each row once).
    total_sum: i64,
    /// Build rows with zero matches (RIGHT pads; FULL right pads).
    right_pads: i64,
}

fn fold_outer(keygen: fn(i64) -> Option<i64>) -> OuterExpect {
    let mut nulls = 0i64;
    let mut inner_count = 0i64;
    let mut inner_sum = 0i64;
    let mut total_sum = 0i64;
    let mut matched_build = vec![false; JR_N as usize];
    for i in 0..N {
        total_sum += i;
        match keygen(i) {
            None => nulls += 1,
            Some(k) => {
                inner_count += 1;
                inner_sum += i;
                if (0..JR_N).contains(&k) {
                    matched_build[k as usize] = true;
                }
            }
        }
    }
    let right_pads = matched_build.iter().filter(|m| !**m).count() as i64;
    OuterExpect {
        nulls,
        inner_count,
        inner_sum,
        total_sum,
        right_pads,
    }
}

fn load_probe(
    session: &mut Executor<PlomidStorageEngine>,
    table: &str,
    keygen: fn(i64) -> Option<i64>,
) {
    session
        .execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, k INT, v BIGINT);"
        ))
        .unwrap();
    let mut batch = format!("INSERT INTO {table} VALUES ");
    let mut in_batch = 0;
    for i in 0..N {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({i}, {}, {i})", cell(keygen(i))));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == N {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = format!("INSERT INTO {table} VALUES ");
            in_batch = 0;
        }
    }
}

fn load_build(session: &mut Executor<PlomidStorageEngine>, table: &str) {
    session
        .execute(&format!(
            "CREATE TABLE {table} (k INT PRIMARY KEY, w TEXT);"
        ))
        .unwrap();
    let mut batch = format!("INSERT INTO {table} VALUES ");
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
// 1. Sequential OUTER-join truth: inner control, LEFT, RIGHT, FULL.
// ---------------------------------------------------------------------------

#[test]
fn par_join_outer_sequential_truth() {
    let root = scratch("truth");
    let mut session = open(&root);
    load_probe(&mut session, "ol", gen_k);
    load_build(&mut session, "orr");

    // Independent Rust fold over the generator (never SQL-vs-SQL).
    let expect = fold_outer(gen_k);
    assert_eq!(expect.nulls + expect.inner_count, N);
    // Hand-computed literals: multiples of 11 in [0, 5000) are
    // 0, 11, ..., 4994 -> 455 values; the rest match 1:1.
    assert_eq!(expect.nulls, 455, "NULL-every-11th fingerprint");
    assert_eq!(expect.inner_count, 4_545, "inner-match fingerprint");
    // total = 0+...+4999 = N*(N-1)/2; null ids sum to
    // 11*(0+...+454) = 1_136_135, so inner = total - that.
    assert_eq!(expect.total_sum, 12_497_500, "total-sum fingerprint");
    assert_eq!(expect.inner_sum, 11_361_365, "inner-sum fingerprint");
    // Build 0..500 covers every probe key 0..499, so no build row is
    // left without a match.
    assert_eq!(expect.right_pads, 0, "uniform build fully matched");

    // INNER control: count and sum vs the fold.
    let (inner_count, inner_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(ol.v) FROM ol JOIN orr ON ol.k = orr.k;",
    );
    assert_eq!(inner_count, expect.inner_count);
    assert_eq!(inner_sum, expect.inner_sum);
    eprintln!("inner fingerprint: count={inner_count} sum_v={inner_sum}");

    // INNER spot literals: id=1 matches, id=0 (NULL key) never joins.
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol JOIN orr ON ol.k = orr.k WHERE ol.id = 1;",
        )),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "w1".to_string()
        ]]
    );
    assert!(
        query(
            &mut session,
            "SELECT ol.id FROM ol JOIN orr ON ol.k = orr.k WHERE ol.id = 0;",
        )
        .is_empty(),
        "NULL probe row must not appear in the inner join"
    );
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol JOIN orr ON ol.k = orr.k WHERE ol.k IS NULL;",
        ),
        0,
        "NULL keys must never match in an inner join"
    );

    // LEFT JOIN preserves every probe row exactly once.
    let left_count = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol LEFT JOIN orr ON ol.k = orr.k;",
    );
    assert_eq!(left_count, N, "left join must preserve all probe rows");
    let (_, left_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(ol.v) FROM ol LEFT JOIN orr ON ol.k = orr.k;",
    );
    assert_eq!(left_sum, expect.total_sum);
    eprintln!("left fingerprint: count={left_count} sum_v={left_sum}");

    // LEFT pads: exactly the NULL-key rows come back padded, once each.
    let padded = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE orr.k IS NULL;",
    );
    assert_eq!(padded, expect.nulls);
    let padded_text = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE orr.w IS NULL;",
    );
    assert_eq!(padded_text, expect.nulls);
    // Pad placement: every padded row carries a NULL probe key, and no
    // padded row carries a non-NULL one; NULLs never match a build row.
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE orr.k IS NULL AND ol.k IS NOT NULL;",
        ),
        0,
        "pads must sit exactly on NULL probe keys"
    );
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE ol.k IS NULL AND orr.k IS NOT NULL;",
        ),
        0,
        "NULL probe keys must never match"
    );
    eprintln!("left pads: padded={padded} (probe nulls={})", expect.nulls);

    // LEFT spot literals: NULL pads vs exact matches.
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE ol.id = 0;",
        )),
        vec![vec![
            "0".to_string(),
            "NULL".to_string(),
            "0".to_string(),
            "NULL".to_string(),
            "NULL".to_string()
        ]]
    );
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE ol.id = 1;",
        )),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "w1".to_string()
        ]]
    );
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE ol.id = 11;",
        )),
        vec![vec![
            "11".to_string(),
            "NULL".to_string(),
            "11".to_string(),
            "NULL".to_string(),
            "NULL".to_string()
        ]]
    );
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE ol.id = 500;",
        )),
        vec![vec![
            "500".to_string(),
            "0".to_string(),
            "500".to_string(),
            "0".to_string(),
            "w0".to_string()
        ]]
    );
    // First five padded ids are exactly the first five NULL-key rows.
    let first_pads = query(
        &mut session,
        "SELECT ol.id FROM ol LEFT JOIN orr ON ol.k = orr.k WHERE orr.k IS NULL ORDER BY ol.id LIMIT 5;",
    );
    let first_ids: Vec<i64> = first_pads.iter().map(|row| int(&row[0])).collect();
    assert_eq!(first_ids, vec![0, 11, 22, 33, 44]);

    // RIGHT JOIN: every build row matches, so the count equals the inner
    // count plus the (zero) right pads.
    let right_count = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol RIGHT JOIN orr ON ol.k = orr.k;",
    );
    assert_eq!(right_count, expect.inner_count + expect.right_pads);
    let right_pads = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol RIGHT JOIN orr ON ol.k = orr.k WHERE ol.id IS NULL;",
    );
    assert_eq!(right_pads, expect.right_pads);
    assert_eq!(right_pads, 0, "uniform build leaves no right pads");
    // Build side is never padded on its own side; probe NULLs contribute no
    // rows to the RIGHT result.
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol RIGHT JOIN orr ON ol.k = orr.k WHERE orr.k IS NULL;",
        ),
        0,
        "build keys are always present in a RIGHT join"
    );
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol RIGHT JOIN orr ON ol.k = orr.k WHERE ol.k IS NULL AND ol.id IS NOT NULL;",
        ),
        0,
        "no NULL probe row may surface in a RIGHT join"
    );
    eprintln!("right fingerprint: count={right_count} pads={right_pads}");

    // RIGHT spot literal: build key 1 surfaces with its probe row.
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, orr.k, orr.w FROM ol RIGHT JOIN orr ON ol.k = orr.k WHERE orr.k = 1 ORDER BY ol.id LIMIT 1;",
        )),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "w1".to_string()
        ]]
    );

    // FULL JOIN: matches + left pads + right pads.
    let full_count = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol FULL JOIN orr ON ol.k = orr.k;",
    );
    assert_eq!(
        full_count,
        expect.inner_count + expect.nulls + expect.right_pads
    );
    assert_eq!(full_count, N + expect.right_pads);
    let full_left_pads = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol FULL JOIN orr ON ol.k = orr.k WHERE orr.k IS NULL;",
    );
    assert_eq!(full_left_pads, expect.nulls);
    let full_right_pads = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol FULL JOIN orr ON ol.k = orr.k WHERE ol.id IS NULL;",
    );
    assert_eq!(full_right_pads, expect.right_pads);
    let full_matches = count_only(
        &mut session,
        "SELECT COUNT(*) FROM ol FULL JOIN orr ON ol.k = orr.k WHERE ol.id IS NOT NULL AND orr.k IS NOT NULL;",
    );
    assert_eq!(full_matches, expect.inner_count);
    // No row is padded on both sides.
    assert_eq!(
        count_only(
            &mut session,
            "SELECT COUNT(*) FROM ol FULL JOIN orr ON ol.k = orr.k WHERE ol.id IS NULL AND orr.k IS NULL;",
        ),
        0,
        "no double-padded row in a FULL join"
    );
    eprintln!(
        "full fingerprint: count={full_count} matches={full_matches} left_pads={full_left_pads} right_pads={full_right_pads}"
    );

    // FULL spot literals mirror LEFT for probe rows (no right pads here).
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol FULL JOIN orr ON ol.k = orr.k WHERE ol.id = 0;",
        )),
        vec![vec![
            "0".to_string(),
            "NULL".to_string(),
            "0".to_string(),
            "NULL".to_string(),
            "NULL".to_string()
        ]]
    );
    assert_eq!(
        text_rows(&query(
            &mut session,
            "SELECT ol.id, ol.k, ol.v, orr.k, orr.w FROM ol FULL JOIN orr ON ol.k = orr.k WHERE ol.id = 1;",
        )),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "1".to_string(),
            "w1".to_string()
        ]]
    );

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. Skew baseline: grouped-over-join exactness fingerprints.
// ---------------------------------------------------------------------------

#[test]
fn par_join_outer_skew_grouped_baseline() {
    let root = scratch("skew");
    let mut session = open(&root);
    load_probe(&mut session, "sl", gen_skew_k);
    load_build(&mut session, "sr");

    // Independent fold: per-key (count, sum) over matched rows plus totals.
    let mut groups: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    let mut nulls = 0i64;
    let mut inner_count = 0i64;
    let mut inner_sum = 0i64;
    for i in 0..N {
        match gen_skew_k(i) {
            None => nulls += 1,
            Some(k) => {
                inner_count += 1;
                inner_sum += i;
                let entry = groups.entry(k).or_insert((0, 0));
                entry.0 += 1;
                entry.1 += i;
            }
        }
    }
    assert_eq!(nulls, 455, "skew NULL fingerprint matches uniform NULLs");
    assert_eq!(nulls + inner_count, N);
    assert_eq!(
        inner_sum, 11_361_365,
        "skew inner sum matches uniform inner sum"
    );
    let hot = groups.get(&HOT_KEY).copied().unwrap_or((0, 0));
    // Hand-computed hot frequency: 4_000 rows carry i%10<8, of which 365
    // are also NULL (i%11==0), leaving 3_635 hot matches (~80% of matched).
    assert_eq!(hot.0, 3_635, "80pc-hot-key fingerprint");
    assert!(
        hot.0 * 10 >= inner_count * 7,
        "hot key must dominate: hot={} matched={}",
        hot.0,
        inner_count
    );
    // Non-hot keys are exactly the 100 residues ending in 8/9 that survive
    // the NULL filter, plus the hot key itself.
    assert_eq!(groups.len(), 101, "skew group-count fingerprint");

    // Control totals vs the fold before the grouped query.
    let (base_count, base_sum) = count_sum(
        &mut session,
        "SELECT COUNT(*), SUM(sl.v) FROM sl JOIN sr ON sl.k = sr.k;",
    );
    assert_eq!((base_count, base_sum), (inner_count, inner_sum));

    // Grouped-over-join exactness: every group's count/sum vs the fold.
    let grouped_sql =
        "SELECT sl.k, COUNT(*), SUM(sl.v) FROM sl JOIN sr ON sl.k = sr.k GROUP BY sl.k;";
    let first = query(&mut session, grouped_sql);
    assert_eq!(first.len(), groups.len(), "group count must match the fold");
    let mut got: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for row in &first {
        assert_eq!(row.len(), 3);
        let key = int(&row[0]);
        let cnt = int(&row[1]);
        let sum = opt_int(&row[2]).unwrap_or(0);
        assert!(
            got.insert(key, (cnt, sum)).is_none(),
            "duplicate group {key}"
        );
    }
    assert_eq!(got, groups, "every grouped count/sum must match the fold");
    // Totals recomposed from groups equal the control totals.
    let regroup_count: i64 = got.values().map(|(c, _)| *c).sum();
    let regroup_sum: i64 = got.values().map(|(_, s)| *s).sum();
    assert_eq!(regroup_count, inner_count);
    assert_eq!(regroup_sum, inner_sum);
    // Hot-group spot literal vs the fold.
    let hot_rows = query(
        &mut session,
        &format!(
            "SELECT COUNT(*), SUM(sl.v) FROM sl JOIN sr ON sl.k = sr.k WHERE sl.k = {HOT_KEY};"
        ),
    );
    assert_eq!(hot_rows.len(), 1);
    assert_eq!(int(&hot_rows[0][0]), hot.0);
    assert_eq!(opt_int(&hot_rows[0][1]).unwrap_or(0), hot.1);

    // Grouped query correctness (row counts per group); wall-clock
    // benchmarking lives behind the scale gate, not in this test.
    let rows = query(&mut session, grouped_sql);
    assert_eq!(rows.len(), groups.len());
    eprintln!(
        "skew grouped baseline: matched={inner_count} sum_v={inner_sum} groups={} hot_key={HOT_KEY} hot_count={}",
        groups.len(),
        hot.0,
    );

    cleanup(&root);
}
