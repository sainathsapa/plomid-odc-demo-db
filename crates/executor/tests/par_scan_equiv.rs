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
//! ParallelScan slice-1 executor-side evidence: partition-concat equivalence.
//!
//! These tests lock the exact semantics the real `ParallelScan` must preserve,
//! using only EXISTING public APIs (`Executor::new` plus
//! `PlomidStorageEngine::create`, and the `StorageEngine` reads `scan`,
//! `scan_for_each`, `count_range`). No new engine API is introduced or used:
//! partitions are simulated with existing bounded scans `[lo, hi)`.
//!
//! Contract under test: for a table range `[start, end)`, splitting at exact
//! row keys `s1 < s2 < ...` and concatenating the bounded scans
//! `[start,s1) + [s1,s2) + ... + [sk,end)` in partition order yields
//! byte-identical rows to one full scan. Every split key equals an existing
//! row key, so the boundary row belongs to the right partition — never lost,
//! never duplicated. This is precisely what a parallel worker fan-out must
//! reproduce.
//!
//! Evidence emitted via `eprintln!` (read with `--nocapture`):
//! * partition sizes per 1/2/4/8-way split of a 100K-row table,
//! * `count_range`-based binary-search split discovery cost vs full-scan cost,
//!   with the <5% verdict, plus the single-pass stride alternative,
//! * sequential `scan_for_each` + `decode_row` throughput (rows/sec baseline).
//!
//! Timings are debug-build, in-process, single-threaded figures; libtest
//! thread contention adds noise, so ratios (not absolutes) are the signal.

use plomid_executor::{decode_row, Executor};
use plomid_sql::{QueryResult, Value};
use plomid_txn::{PlomidStorageEngine, StorageEngine};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Row count of the main equivalence table.
const BIG_N: i64 = 100_000;
/// Batch size for multi-row INSERT statements (matches neighboring tests).
const BATCH: i64 = 2000;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("plomid-parscan-{tag}-{id}-{}", std::process::id()));
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

/// Catalog-qualified `[start, end)` byte range for one logical table, using
/// the exact prefix shape the executor itself uses for DML/SELECT scans.
fn table_bounds(session: &Executor<PlomidStorageEngine>, table: &str) -> (Vec<u8>, Vec<u8>) {
    let qualified = session
        .catalog()
        .resolve_table_name(table)
        .expect("resolve table");
    (
        format!("{qualified}:").into_bytes(),
        format!("{qualified}:\u{10FFFF}").into_bytes(),
    )
}

fn full_scan(session: &mut Executor<PlomidStorageEngine>, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (start, end) = table_bounds(session, table);
    session
        .engine_mut()
        .scan(Some(&start), Some(&end))
        .expect("full scan")
}

/// Simulates a P-way parallel scan with existing bounded scans only: splits
/// must be sorted row keys inside the table range; partitions are scanned in
/// order and concatenated.
fn partitioned_concat(
    session: &mut Executor<PlomidStorageEngine>,
    table: &str,
    splits: &[Vec<u8>],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (start, end) = table_bounds(session, table);
    let mut lo = start;
    let mut out = Vec::new();
    for split in splits {
        let part = session
            .engine_mut()
            .scan(Some(&lo), Some(split))
            .expect("partition scan");
        out.extend(part);
        lo = split.clone();
    }
    let tail = session
        .engine_mut()
        .scan(Some(&lo), Some(&end))
        .expect("tail scan");
    out.extend(tail);
    out
}

/// Split keys at exact row-key ranks `N*k/parts` (each split key IS a row key,
/// so the boundary row belongs to the right partition by `[lo, hi)`).
fn splits_at_ranks(keys: &[Vec<u8>], parts: usize) -> Vec<Vec<u8>> {
    let n = keys.len();
    (1..parts).map(|k| keys[n * k / parts].clone()).collect()
}

fn assert_strictly_sorted(rows: &[(Vec<u8>, Vec<u8>)]) {
    assert!(
        rows.windows(2).all(|w| w[0].0 < w[1].0),
        "scan order must be strictly increasing in key bytes"
    );
}

// ---------------------------------------------------------------------------
// Deterministic row generation: single source of truth for loader + checks.
// ---------------------------------------------------------------------------

/// `NULL` every 9th row, else a low-cardinality group in `0..40`.
fn gen_g(i: i64) -> Option<i64> {
    if i % 9 == 0 {
        None
    } else {
        Some(i % 40)
    }
}

/// `NULL` every 7th row, else a signed pseudo-random magnitude.
fn gen_v(i: i64) -> Option<i64> {
    if i % 7 == 0 {
        None
    } else {
        Some((i * 7919) % 100_000 - 50_000)
    }
}

/// `NULL` every 11th row, else one of five text tags.
fn gen_t(i: i64) -> Option<String> {
    if i % 11 == 0 {
        None
    } else {
        Some(format!("tag{}", i % 5))
    }
}

fn cell(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

struct BigExpect {
    count_star: i64,
    count_v: i64,
    sum_v: i64,
    min_v: Option<i64>,
    max_v: Option<i64>,
}

/// 100K-row `id INTEGER PRIMARY KEY` table with int/int/nullable-int/text mix.
fn load_big(session: &mut Executor<PlomidStorageEngine>, table: &str, n: i64) -> BigExpect {
    session
        .execute(&format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, t TEXT);"
        ))
        .unwrap();
    let mut expect = BigExpect {
        count_star: n,
        count_v: 0,
        sum_v: 0,
        min_v: None,
        max_v: None,
    };
    let mut batch = format!("INSERT INTO {table} VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let g = gen_g(i);
        let v = gen_v(i);
        let t = gen_t(i);
        if let Some(x) = v {
            expect.count_v += 1;
            expect.sum_v += x;
            expect.min_v = Some(expect.min_v.map_or(x, |m| m.min(x)));
            expect.max_v = Some(expect.max_v.map_or(x, |m| m.max(x)));
        }
        let t_sql = t.map(|s| format!("'{s}'")).unwrap_or_else(|| "NULL".into());
        batch.push_str(&format!("({i}, {}, {}, {t_sql})", cell(g), cell(v)));
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

// ---------------------------------------------------------------------------
// 1. Partition-concat equivalence on the 100K table.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: parallel-scan engagement needs 50K+ rows; runs in CI via make test-scale"]
fn par_scan_partition_concat_100k() {
    let root = scratch("big");
    let mut session = open(&root);
    let started = Instant::now();
    let expect = load_big(&mut session, "big_scan", BIG_N);
    eprintln!("loaded {BIG_N} rows in {:?}", started.elapsed());

    let full = full_scan(&mut session, "big_scan");
    assert_eq!(full.len(), BIG_N as usize, "one stored row per insert");
    assert_strictly_sorted(&full);
    let keys: Vec<Vec<u8>> = full.iter().map(|(k, _)| k.clone()).collect();

    // 1/2/4/8 partitions: concat in partition order is byte-identical.
    for parts in [1usize, 2, 4, 8] {
        let splits = splits_at_ranks(&keys, parts);
        // Every split key equals an existing row key (right-partition owner).
        for s in &splits {
            assert!(keys.binary_search(s).is_ok(), "split key must be a row key");
        }
        let started = Instant::now();
        let concat = partitioned_concat(&mut session, "big_scan", &splits);
        let elapsed = started.elapsed();
        assert_eq!(concat, full, "{parts}-way concat must equal the full scan");
        let sizes: Vec<usize> = {
            let mut sizes = Vec::new();
            let (start, end) = table_bounds(&session, "big_scan");
            let mut lo = start;
            for s in &splits {
                sizes.push(
                    session
                        .engine_mut()
                        .count_range(Some(&lo), Some(s))
                        .expect("count") as usize,
                );
                lo = s.clone();
            }
            sizes.push(
                session
                    .engine_mut()
                    .count_range(Some(&lo), Some(&end))
                    .expect("count") as usize,
            );
            sizes
        };
        assert_eq!(sizes.iter().sum::<usize>(), BIG_N as usize);
        eprintln!("parts={parts}: sizes={sizes:?} concat==full in {elapsed:?}");
    }

    // Boundary: split at the FIRST row key -> left empty, right is everything.
    {
        let (start, end) = table_bounds(&session, "big_scan");
        let left = session
            .engine_mut()
            .scan(Some(&start), Some(&keys[0]))
            .expect("scan");
        assert!(left.is_empty(), "nothing sorts before the first row key");
        let right = session
            .engine_mut()
            .scan(Some(&keys[0]), Some(&end))
            .expect("scan");
        assert_eq!(right, full);
    }

    // Boundary: split at the LAST row key -> left is N-1, right is one row.
    {
        let (start, end) = table_bounds(&session, "big_scan");
        let n = full.len();
        let left = session
            .engine_mut()
            .scan(Some(&start), Some(&keys[n - 1]))
            .expect("scan");
        assert_eq!(left, full[..n - 1]);
        let right = session
            .engine_mut()
            .scan(Some(&keys[n - 1]), Some(&end))
            .expect("scan");
        assert_eq!(right, full[n - 1..]);
    }

    // Boundary: single-row extraction in the middle; empty range at a key.
    {
        let (start, end) = table_bounds(&session, "big_scan");
        let mid = full.len() / 2;
        let one = session
            .engine_mut()
            .scan(Some(&keys[mid]), Some(&keys[mid + 1]))
            .expect("scan");
        assert_eq!(one, full[mid..mid + 1], "adjacent keys isolate one row");
        let empty = session
            .engine_mut()
            .scan(Some(&keys[mid]), Some(&keys[mid].clone()))
            .expect("scan");
        assert!(empty.is_empty(), "[k, k) is empty");
        // Tail past the last key but inside the table range is empty.
        let mut above = keys[keys.len() - 1].clone();
        above.push(0xFF);
        assert!(above < end, "probe key stays inside the table range");
        let tail = session
            .engine_mut()
            .scan(Some(&above), Some(&end))
            .expect("scan");
        assert!(tail.is_empty(), "no rows sort past the last key");
        let _ = start;
    }

    // Duplicate split keys (degenerate split points) yield empty middles and
    // still concatenate exactly.
    {
        let mid = full.len() / 2;
        let concat = partitioned_concat(
            &mut session,
            "big_scan",
            &[keys[mid].clone(), keys[mid].clone()],
        );
        assert_eq!(concat, full, "duplicate splits must stay exact");
    }

    // SQL-level sanity over the NULL mix (independent Rust fold from load).
    {
        let rows = rows_of(
            session
                .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v) FROM big_scan;")
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(int(&rows[0][0]), expect.count_star);
        assert_eq!(int(&rows[0][1]), expect.count_v);
        assert_eq!(int(&rows[0][2]), expect.sum_v);
        assert_eq!(int(&rows[0][3]), expect.min_v.expect("nonempty min"));
        assert_eq!(int(&rows[0][4]), expect.max_v.expect("nonempty max"));
    }

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// Skewed-key secondary table: 80%-hot-key shape.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: parallel-scan engagement needs 50K+ rows; runs in CI via make test-scale"]
fn par_scan_partition_concat_skewed() {
    let root = scratch("skew");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE sk (id INTEGER PRIMARY KEY, hot INTEGER, v INTEGER);")
        .unwrap();
    let n = 50_000i64;
    let mut counts: BTreeMap<i64, i64> = BTreeMap::new();
    let mut batch = String::from("INSERT INTO sk VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        // 80% of rows carry hot=1; the rest spread over 2..=8.
        let hot = if i % 10 < 8 { 1 } else { 2 + (i % 7) };
        *counts.entry(hot).or_default() += 1;
        let v = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            ((i * 37) % 1000).to_string()
        };
        batch.push_str(&format!("({i}, {hot}, {v})"));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == n {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = String::from("INSERT INTO sk VALUES ");
            in_batch = 0;
        }
    }
    assert_eq!(counts[&1], 40_000, "80pc-hot-key shape");

    let full = full_scan(&mut session, "sk");
    assert_eq!(full.len(), n as usize);
    assert_strictly_sorted(&full);
    let keys: Vec<Vec<u8>> = full.iter().map(|(k, _)| k.clone()).collect();
    for parts in [1usize, 2, 4, 8] {
        let splits = splits_at_ranks(&keys, parts);
        let concat = partitioned_concat(&mut session, "sk", &splits);
        assert_eq!(concat, full, "skewed table {parts}-way concat must match");
    }

    // Skew survives GROUP BY identically on the simulated partitions.
    let rows = rows_of(
        session
            .execute("SELECT hot, COUNT(*) FROM sk GROUP BY hot;")
            .unwrap(),
    );
    let mut got: BTreeMap<i64, i64> = BTreeMap::new();
    for row in &rows {
        got.insert(int(&row[0]), int(&row[1]));
    }
    assert_eq!(got, counts);
    eprintln!(
        "skewed 50K rows: hot=1 count={} groups={}",
        got[&1],
        got.len()
    );
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// NULL-heavy mix table.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: parallel-scan engagement needs 50K+ rows; runs in CI via make test-scale"]
fn par_scan_partition_concat_null_heavy() {
    let root = scratch("nullheavy");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE nh (id INTEGER PRIMARY KEY, g INTEGER, v INTEGER, t TEXT);")
        .unwrap();
    let n = 20_000i64;
    let mut null_g = 0i64;
    let mut null_v = 0i64;
    let mut null_t = 0i64;
    let mut batch = String::from("INSERT INTO nh VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        // ~33% NULL g, 50% NULL v, 25% NULL t: every aggregate input is NULL-heavy.
        let g = if i % 3 == 0 {
            null_g += 1;
            "NULL".to_string()
        } else {
            (i % 5).to_string()
        };
        let v = if i % 2 == 0 {
            null_v += 1;
            "NULL".to_string()
        } else {
            ((i * 13) % 1000).to_string()
        };
        let t = if i % 4 == 0 {
            null_t += 1;
            "NULL".to_string()
        } else {
            format!("'n{}'", i % 3)
        };
        batch.push_str(&format!("({i}, {g}, {v}, {t})"));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == n {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = String::from("INSERT INTO nh VALUES ");
            in_batch = 0;
        }
    }
    assert!(null_v == n / 2 && null_g > 0 && null_t > 0);

    let full = full_scan(&mut session, "nh");
    assert_eq!(full.len(), n as usize);
    let keys: Vec<Vec<u8>> = full.iter().map(|(k, _)| k.clone()).collect();
    for parts in [2usize, 4] {
        let splits = splits_at_ranks(&keys, parts);
        let concat = partitioned_concat(&mut session, "nh", &splits);
        assert_eq!(concat, full, "NULL-heavy {parts}-way concat must match");
    }

    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(g), COUNT(v), COUNT(t) FROM nh;")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int8(n),
            Value::Int8(n - null_g),
            Value::Int8(n - null_v),
            Value::Int8(n - null_t),
        ]]
    );
    eprintln!("null-heavy 20K rows: null_g={null_g} null_v={null_v} null_t={null_t}");
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// Empty table and single-row table.
// ---------------------------------------------------------------------------

#[test]
fn par_scan_partition_concat_empty_and_single_row() {
    let root = scratch("edges");
    let mut session = open(&root);
    session
        .execute("CREATE TABLE ps_empty (id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    session
        .execute("CREATE TABLE ps_one (id INTEGER PRIMARY KEY, v INTEGER);")
        .unwrap();
    session
        .execute("INSERT INTO ps_one VALUES (7, 42);")
        .unwrap();

    // Empty table: every bounded scan is empty; any sorted synthetic splits
    // concatenate to empty.
    {
        let (start, end) = table_bounds(&session, "ps_empty");
        let full = session
            .engine_mut()
            .scan(Some(&start), Some(&end))
            .expect("scan");
        assert!(full.is_empty());
        assert_eq!(
            session
                .engine_mut()
                .count_range(Some(&start), Some(&end))
                .expect("count"),
            0
        );
        let qualified = session
            .catalog()
            .resolve_table_name("ps_empty")
            .expect("resolve");
        for parts in [1usize, 2, 4] {
            let splits: Vec<Vec<u8>> = (1..parts)
                .map(|k| format!("{qualified}:m{k}").into_bytes())
                .collect();
            let concat = partitioned_concat(&mut session, "ps_empty", &splits);
            assert!(
                concat.is_empty(),
                "empty table {parts}-way concat stays empty"
            );
        }
    }

    // Single-row table: splits at the single row key (with duplicates for
    // higher part counts) concatenate exactly.
    {
        let full = full_scan(&mut session, "ps_one");
        assert_eq!(full.len(), 1);
        let k0 = full[0].0.clone();
        for parts in [1usize, 2, 4, 8] {
            let splits = splits_at_ranks(std::slice::from_ref(&k0), parts);
            assert!(splits.iter().all(|s| *s == k0));
            let concat = partitioned_concat(&mut session, "ps_one", &splits);
            assert_eq!(concat, full, "single-row {parts}-way concat must match");
        }
        let (start, end) = table_bounds(&session, "ps_one");
        let left = session
            .engine_mut()
            .scan(Some(&start), Some(&k0))
            .expect("scan");
        assert!(left.is_empty());
        let right = session
            .engine_mut()
            .scan(Some(&k0), Some(&end))
            .expect("scan");
        assert_eq!(right, full);
        // SQL still sees the row.
        let rows = rows_of(
            session
                .execute("SELECT COUNT(*), SUM(v) FROM ps_one;")
                .unwrap(),
        );
        assert_eq!(rows, vec![vec![Value::Int8(1), Value::Int8(42)]]);
    }
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. Split-point discovery cost: count_range binary search vs full scan.
// ---------------------------------------------------------------------------

/// Binary-search the split key for 1-based `target` rows in `[start, key)`
/// using `count_range` as the only oracle. Candidates come from the observed
/// key order (the production splitter would draw candidates from sampling or
/// `scan_limit`; the cost measured here — one `count_range` walk per probe —
/// is identical either way). Returns the split key plus probe count.
fn discover_split(
    session: &mut Executor<PlomidStorageEngine>,
    start: &[u8],
    keys: &[Vec<u8>],
    target: u64,
) -> (Vec<u8>, usize) {
    let mut lo = 0usize;
    let mut hi = keys.len();
    let mut probes = 0usize;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let counted = session
            .engine_mut()
            .count_range(Some(start), Some(&keys[mid]))
            .expect("count probe");
        probes += 1;
        if counted < target {
            lo = mid + 1;
        } else if counted > target {
            hi = mid;
        } else {
            return (keys[mid].clone(), probes);
        }
    }
    // The oracle is exact (`count_range(start, keys[i]) == i`), so the loop
    // always hits exactly; this fallback only guards the index.
    (keys[lo.min(keys.len() - 1)].clone(), probes)
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_scan_split_point_cost() {
    let root = scratch("splits");
    let mut session = open(&root);
    let expect = load_big(&mut session, "big_scan", BIG_N);
    let _ = expect;

    let (start, end) = table_bounds(&session, "big_scan");

    // Full materializing scan: the cost to beat.
    let t = Instant::now();
    let full = session
        .engine_mut()
        .scan(Some(&start), Some(&end))
        .expect("full scan");
    let scan_dur = t.elapsed();
    assert_eq!(full.len(), BIG_N as usize);
    let keys: Vec<Vec<u8>> = full.iter().map(|(k, _)| k.clone()).collect();

    // Single count_range over the table (one probe's shape).
    let t = Instant::now();
    let total = session
        .engine_mut()
        .count_range(Some(&start), Some(&end))
        .expect("count");
    let one_count_dur = t.elapsed();
    assert_eq!(total, BIG_N as u64);

    for parts in [4usize, 8] {
        let t = Instant::now();
        let mut splits = Vec::new();
        let mut probes = 0usize;
        for k in 1..parts {
            let target = total * k as u64 / parts as u64;
            let (split, used) = discover_split(&mut session, &start, &keys, target);
            probes += used;
            splits.push(split);
        }
        let discovery_dur = t.elapsed();
        // Discovered splits are exact: each owns exactly its rank...
        for (k, split) in splits.iter().enumerate() {
            let target = total * (k as u64 + 1) / parts as u64;
            let counted = session
                .engine_mut()
                .count_range(Some(&start), Some(split))
                .expect("verify");
            assert_eq!(counted, target, "split {k} of {parts} owns its rank");
            assert_eq!(*split, keys[target as usize], "split {k} is the rank key");
        }
        // ...and the simulated partitions are balanced and exact.
        let concat = partitioned_concat(&mut session, "big_scan", &splits);
        assert_eq!(concat, full, "{parts}-way discovered splits stay exact");
        let pct = discovery_dur.as_secs_f64() / scan_dur.as_secs_f64() * 100.0;
        eprintln!(
            "splits={parts}: discovery={discovery_dur:?} ({probes} count_range probes) \
             vs full scan={scan_dur:?} -> {pct:.2}% of scan (verdict <5%: {})",
            if pct < 5.0 { "PASS" } else { "FAIL" }
        );
    }

    // Constructive alternative: single streaming pass collecting the stride
    // keys (no probes, no retained payloads).
    for parts in [4usize, 8] {
        let targets: Vec<u64> = (1..parts)
            .map(|k| total * k as u64 / parts as u64)
            .collect();
        let t = Instant::now();
        let mut collected: Vec<Vec<u8>> = Vec::new();
        let mut seen = 0u64;
        let mut ti = 0usize;
        session
            .engine_mut()
            .scan_for_each(Some(&start), Some(&end), 1024, &mut |window| {
                for (k, _) in window {
                    seen += 1;
                    if ti < targets.len() && seen == targets[ti] + 1 {
                        collected.push(k.clone());
                        ti += 1;
                    }
                }
                Ok(true)
            })
            .expect("stride walk");
        let stride_dur = t.elapsed();
        assert_eq!(seen, total);
        assert_eq!(collected, splits_at_ranks(&keys, parts));
        let pct = stride_dur.as_secs_f64() / scan_dur.as_secs_f64() * 100.0;
        eprintln!("splits={parts}: single-pass stride={stride_dur:?} -> {pct:.2}% of scan");
    }

    eprintln!("one count_range(100K)={one_count_dur:?} full scan={scan_dur:?}");
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 3. Sequential analytical scan baseline: scan_for_each + decode throughput.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_scan_sequential_decode_baseline() {
    let root = scratch("baseline");
    let mut session = open(&root);
    let expect = load_big(&mut session, "big_scan", BIG_N);

    let (start, end) = table_bounds(&session, "big_scan");

    // Baseline: streaming walk with per-row decode (the work a sequential
    // analytical scan does today), chunked so executor memory stays bounded.
    let t = Instant::now();
    let mut seen = 0u64;
    let mut sum_v = 0i64;
    let mut count_v = 0u64;
    session
        .engine_mut()
        .scan_for_each(Some(&start), Some(&end), 1024, &mut |window| {
            for (_, payload) in window {
                let row = decode_row(payload)?;
                seen += 1;
                if let Some(x) = opt_int(&row[2]) {
                    count_v += 1;
                    sum_v += x;
                }
            }
            Ok(true)
        })
        .expect("streaming decode");
    let decode_dur = t.elapsed();
    assert_eq!(seen, BIG_N as u64);
    assert_eq!(count_v, expect.count_v as u64);
    assert_eq!(sum_v, expect.sum_v);
    let rows_per_sec = seen as f64 / decode_dur.as_secs_f64();
    eprintln!(
        "baseline scan_for_each+decode: {seen} rows in {decode_dur:?} = {rows_per_sec:.0} rows/s \
         (debug build, in-process)"
    );

    // SQL reference over the same rows for the report.
    let t = Instant::now();
    let rows = rows_of(
        session
            .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v) FROM big_scan;")
            .unwrap(),
    );
    let sql_dur = t.elapsed();
    assert_eq!(int(&rows[0][0]), expect.count_star);
    assert_eq!(int(&rows[0][1]), expect.count_v);
    assert_eq!(int(&rows[0][2]), expect.sum_v);
    eprintln!("baseline SQL COUNT/SUM/MIN/MAX over 100K rows: {sql_dur:?}");
    // Phase-25 server reference (given): 136.5ms/200K sequential sum.
    // Server A/B follows after ParallelScan wiring.

    cleanup(&root);
}
