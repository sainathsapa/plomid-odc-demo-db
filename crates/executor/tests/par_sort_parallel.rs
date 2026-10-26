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
//! ParallelSort slice-4 execution evidence: row-store parallel sort/TopK.
//!
//! The production gate lives in `query/group.rs`: parallel fan-out engages
//! only above `PAR_SORT_MIN_ROWS` rows AND with a proven total order
//! (`ORDER BY` covering a whole `UNIQUE`/`PRIMARY KEY` column set with
//! `NOT NULL`, exactly-comparable, worker-safe keys). Everything else keeps
//! the sequential path byte-identically.
//!
//! Regime map used by every test here (row-store only: no indexes exist, no
//! columnar store is attached, so the scan path under test is always the
//! row-store one):
//! * 20k table `os`: below threshold -> sequential. Non-total shapes must
//!   equal the pre-change sequential literals locked in `par_sort_equiv.rs`
//!   (same table, same data generation); that equality IS the proof the
//!   gate declined, because a tie-diverging parallel merge would pick
//!   different tie subsets (`par_sort_equiv.rs` proves partitioned scans
//!   diverge inside tie groups).
//! * 100k table `big`: above threshold -> total-order shapes take the
//!   parallel path. Exactness there is shown by closed forms (PK orders have
//!   no degrees of freedom), key-sortedness plus row-multiset checks, and
//!   TopK-vs-full prefix agreement. Decline for non-total 100k shapes cannot
//!   be observed directly through SQL (no sequential baseline at that size),
//!   so it is proven by composition: the total-order proof is size
//!   independent (unit-pinned in `query::group::par_sort_tests` for every
//!   non-PK shape) and the 20k literals prove decline end-to-end.
//! * Direct parallel-vs-sequential equivalence at equal sizes is pinned by
//!   the unit tests (`par_sort_matches_sequential_on_total_orders`), which
//!   call both functions on the same inputs.
//!
//! Table under test mirrors the sibling proof file exactly:
//! `os`/`big (id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT)`,
//! `a = NULL` every 17th row else `i % 50`, `b = NULL` every 13th row else
//! `s00..s06` (`i % 7`), `v = i`.
//!
//! Evidence via `eprintln!` (read with `--nocapture`); timings are
//! in-process best-of-3 figures.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Row count of the below-threshold (sequential-regime) table.
const N_SMALL: i64 = 20_000;
/// Row count of the above-threshold (parallel-regime) table.
const N_BIG: i64 = 100_000;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 500;
/// `a` is NULL exactly on these rows.
const NULL_A_EVERY: i64 = 17;
/// `b` is NULL exactly on these rows.
const NULL_B_EVERY: i64 = 13;
/// Cardinality of the non-NULL `a` domain.
const A_MOD: i64 = 50;
/// Cardinality of the non-NULL `b` domain.
const B_MOD: i64 = 7;
/// Column position of `SELECT id, a, b, v` key `a`.
const COL_A: usize = 1;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-parsortpar-{tag}-{id}-{}",
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

fn query(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}: {e:?}"))
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn int_val(v: &Value) -> i64 {
    match v {
        Value::Int8(n) => *n,
        Value::Int4(n) => i64::from(*n),
        Value::Int2(n) => i64::from(*n),
        other => panic!("expected int, got {other:?}"),
    }
}

fn opt_int(v: &Value) -> Option<i64> {
    if v.is_null() {
        None
    } else {
        Some(int_val(v))
    }
}

/// `NULL` every 17th row, else the row index modulo 50.
fn gen_a(i: i64) -> Option<i64> {
    if i % NULL_A_EVERY == 0 {
        None
    } else {
        Some(i % A_MOD)
    }
}

fn cell_int(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

/// `tbl(id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT)`, `id = v = 0..n`.
fn load_table(session: &mut Executor<PlomidStorageEngine>, tbl: &str, n: i64) {
    session
        .execute(&format!(
            "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT);"
        ))
        .unwrap();
    let mut batch = format!("INSERT INTO {tbl} VALUES ");
    let mut in_batch = 0;
    for i in 0..n {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let b_sql = if i % NULL_B_EVERY == 0 {
            "NULL".to_string()
        } else {
            format!("'s{:02}'", i % B_MOD)
        };
        batch.push_str(&format!("({i}, {}, {b_sql}, {i})", cell_int(gen_a(i))));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == n {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = format!("INSERT INTO {tbl} VALUES ");
            in_batch = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// Client-side mirror of `compare_order_keys` for the no-explicit-NULLS
// queries used here (`nulls_first` defaults to `descending`; DESC reverses
// only when both values are non-NULL). Same NULL rule as the engine.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct KeySpec {
    col: usize,
    desc: bool,
}

fn cmp_values(va: &Value, vb: &Value, desc: bool) -> Ordering {
    let nulls_first = desc;
    let ord = match (va.is_null(), vb.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => va.compare(vb),
    };
    if desc && !va.is_null() && !vb.is_null() {
        ord.reverse()
    } else {
        ord
    }
}

fn cmp_rows(a: &[Value], b: &[Value], spec: &[KeySpec]) -> Ordering {
    for key in spec {
        let ord = cmp_values(&a[key.col], &b[key.col], key.desc);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

fn assert_sorted(rows: &[Vec<Value>], spec: &[KeySpec], ctx: &str) {
    assert!(
        rows.windows(2)
            .all(|w| cmp_rows(&w[0], &w[1], spec) != Ordering::Greater),
        "rows must be sorted per {ctx}"
    );
}

fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter().map(|r| int_val(&r[0])).collect()
}

/// Row multiset equality ignoring order: sort both sides by `id`.
fn assert_same_multiset(a: &[Vec<Value>], b: &[Vec<Value>], ctx: &str) {
    let mut a: Vec<&Vec<Value>> = a.iter().collect();
    let mut b: Vec<&Vec<Value>> = b.iter().collect();
    a.sort_by_key(|r| int_val(&r[0]));
    b.sort_by_key(|r| int_val(&r[0]));
    assert_eq!(a, b, "row multisets must match: {ctx}");
}

// ---------------------------------------------------------------------------
// 1. Total-order shapes on the 100k (parallel-regime) table.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_sort_total_order_parallel_regime() {
    let root = scratch("total");
    let mut session = open(&root);
    load_table(&mut session, "big", N_BIG);

    // ORDER BY id ASC: closed form 0..N. No tie freedom exists, so this is
    // exact against any correct implementation, parallel or sequential.
    let asc1 = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY id;");
    let asc2 = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY id;");
    assert_eq!(asc1, asc2, "PK full sort must be deterministic");
    assert_eq!(asc1.len(), N_BIG as usize);
    assert_eq!(ids(&asc1), (0..N_BIG).collect::<Vec<_>>());
    eprintln!("ORDER BY id 100k: exact 0..{N_BIG} PASS");

    // Qualified PK reference resolves through the same lookup: same form.
    let q = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY big.id;");
    assert_eq!(q, asc1, "qualified PK order must equal unqualified");
    eprintln!("ORDER BY big.id 100k: exact PASS");

    // ORDER BY id DESC: closed form N-1..0.
    let desc = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id DESC;",
    );
    let desc2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id DESC;",
    );
    assert_eq!(desc, desc2, "PK DESC sort must be deterministic");
    assert_eq!(
        ids(&desc),
        (0..N_BIG).rev().collect::<Vec<_>>(),
        "DESC closed form"
    );
    eprintln!("ORDER BY id DESC 100k: exact PASS");

    // ORDER BY a, id (multi-key incl. PK): sorted per the comparator and the
    // exact row multiset of the table (no row lost/duplicated by the merge).
    let spec = [
        KeySpec {
            col: COL_A,
            desc: false,
        },
        KeySpec {
            col: 0,
            desc: false,
        },
    ];
    let multi1 = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY a, id;");
    let multi2 = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY a, id;");
    assert_eq!(multi1, multi2, "multi-key total sort must be deterministic");
    assert_eq!(multi1.len(), N_BIG as usize);
    assert_sorted(&multi1, &spec, "ORDER BY a, id");
    let unordered = query(&mut session, "SELECT id, a, b, v FROM big;");
    assert_same_multiset(&multi1, &unordered, "ORDER BY a, id multiset");
    // Within-tie ids ascend strictly (PK appends the total order).
    for w in multi1.windows(2) {
        if opt_int(&w[0][COL_A]) == opt_int(&w[1][COL_A]) {
            assert!(
                int_val(&w[0][0]) < int_val(&w[1][0]),
                "tie ids must ascend under ORDER BY a, id"
            );
        }
    }
    // Boundary keys: first key is minimum non-NULL residue, tail is NULL.
    assert_eq!(opt_int(&multi1[0][COL_A]), Some(0));
    assert!(multi1[N_BIG as usize - 1][COL_A].is_null());
    let null_a = multi1.iter().filter(|r| r[COL_A].is_null()).count();
    let expect_null_a = (0..N_BIG).filter(|i| gen_a(*i).is_none()).count();
    assert_eq!(null_a, expect_null_a, "NULL-a count matches generation");
    eprintln!("ORDER BY a, id 100k: sorted+multiset+tie-order PASS");

    // Mixed directions ORDER BY a DESC, id DESC: same checks, flipped.
    let spec_d = [
        KeySpec {
            col: COL_A,
            desc: true,
        },
        KeySpec { col: 0, desc: true },
    ];
    let mixed = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY a DESC, id DESC;",
    );
    let mixed2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY a DESC, id DESC;",
    );
    assert_eq!(mixed, mixed2, "mixed-direction sort must be deterministic");
    assert_sorted(&mixed, &spec_d, "ORDER BY a DESC, id DESC");
    assert_same_multiset(&mixed, &unordered, "mixed multiset");
    assert!(mixed[..null_a].iter().all(|r| r[COL_A].is_null()));
    assert_eq!(opt_int(&mixed[null_a][COL_A]), Some(49));
    eprintln!("ORDER BY a DESC, id DESC 100k: PASS");

    // Filtered full sort still carries the PK total order: closed-form
    // ascending subsequence (a = 5 hits rows i = 5 mod 50, never the
    // 17-cycle NULLs since 5 is coprime-adjacent... verify by generation).
    let expect_a5: Vec<i64> = (0..N_BIG).filter(|i| gen_a(*i) == Some(5)).collect();
    assert!(!expect_a5.is_empty());
    assert!(
        expect_a5.windows(2).all(|w| w[0] < w[1]),
        "generation order is ascending"
    );
    let filt = query(
        &mut session,
        "SELECT id, a, b, v FROM big WHERE a = 5 ORDER BY id;",
    );
    let filt2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big WHERE a = 5 ORDER BY id;",
    );
    assert_eq!(filt, filt2, "filtered PK sort must be deterministic");
    assert_eq!(ids(&filt), expect_a5, "filtered closed form");
    eprintln!(
        "WHERE a = 5 ORDER BY id 100k: exact PASS ({} rows)",
        filt.len()
    );

    // DISTINCT ON runs after the sort and is unaffected: first row per `a`
    // group under ORDER BY a, id is the minimum id carrying that residue
    // (plus one NULL-a survivor with the minimum NULL-a id, 0).
    let mut expect_groups: Vec<(Option<i64>, i64)> = Vec::new();
    for residue in 0..A_MOD {
        let min_id = (0..N_BIG)
            .find(|i| gen_a(*i) == Some(residue))
            .expect("every residue occurs");
        expect_groups.push((Some(residue), min_id));
    }
    expect_groups.push((None, 0));
    let distinct = query(
        &mut session,
        "SELECT DISTINCT ON (a) id, a FROM big ORDER BY a, id;",
    );
    let distinct2 = query(
        &mut session,
        "SELECT DISTINCT ON (a) id, a FROM big ORDER BY a, id;",
    );
    assert_eq!(distinct, distinct2, "DISTINCT ON must be deterministic");
    assert_eq!(distinct.len(), expect_groups.len());
    for (row, (expect_a, expect_id)) in distinct.iter().zip(expect_groups.iter()) {
        assert_eq!(opt_int(&row[1]), *expect_a);
        assert_eq!(int_val(&row[0]), *expect_id);
    }
    eprintln!("DISTINCT ON (a) ORDER BY a, id 100k: exact PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. TopK streaming on the 100k (parallel-regime) table.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_sort_topk_parallel_regime() {
    let root = scratch("topkpar");
    let mut session = open(&root);
    load_table(&mut session, "big", N_BIG);

    // Unique-key TopK: closed form, equals the full-sort prefix exactly.
    let top1 = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id DESC LIMIT 10;",
    );
    let top2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id DESC LIMIT 10;",
    );
    assert_eq!(top1, top2, "unique-key TopK must be deterministic");
    assert_eq!(
        ids(&top1),
        (N_BIG - 10..N_BIG).rev().collect::<Vec<_>>(),
        "TopK closed form"
    );
    let full_desc = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id DESC;",
    );
    assert_eq!(top1, full_desc[..10], "TopK equals the full-sort prefix");
    eprintln!("TopK ORDER BY id DESC LIMIT 10 100k: exact PASS");

    // Total-order TopK with OFFSET agrees with the full sort window.
    let full = query(&mut session, "SELECT id, a, b, v FROM big ORDER BY a, id;");
    let window = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY a, id LIMIT 5 OFFSET 3;",
    );
    let window2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY a, id LIMIT 5 OFFSET 3;",
    );
    assert_eq!(window, window2, "TopK OFFSET must be deterministic");
    assert_eq!(
        window,
        full[3..8],
        "TopK window equals the full-sort window"
    );
    assert!(window.iter().all(|r| opt_int(&r[COL_A]) == Some(0)));
    eprintln!("TopK ORDER BY a, id LIMIT 5 OFFSET 3 100k: exact PASS");

    // Filtered TopK through the parallel fan-out: closed-form ascending
    // subsequence head (a = 5 rows, smallest 10 ids).
    let expect_a5: Vec<i64> = (0..N_BIG).filter(|i| gen_a(*i) == Some(5)).collect();
    let ftop = query(
        &mut session,
        "SELECT id, a, b, v FROM big WHERE a = 5 ORDER BY id LIMIT 10;",
    );
    let ftop2 = query(
        &mut session,
        "SELECT id, a, b, v FROM big WHERE a = 5 ORDER BY id LIMIT 10;",
    );
    assert_eq!(ftop, ftop2, "filtered TopK must be deterministic");
    assert_eq!(ids(&ftop), expect_a5[..10], "filtered TopK closed form");
    eprintln!("Filtered TopK WHERE a = 5 ORDER BY id LIMIT 10 100k: exact PASS");

    // LIMIT 0 stays empty through the heap path.
    let empty_lim = query(
        &mut session,
        "SELECT id, a, b, v FROM big ORDER BY id LIMIT 0;",
    );
    assert!(empty_lim.is_empty(), "LIMIT 0 must be empty");
    eprintln!("LIMIT 0 100k: empty PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 3. Non-total shapes on the 20k table: provably sequential.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_sort_nontotal_stays_sequential() {
    let root = scratch("nontotal");
    let mut session = open(&root);
    load_table(&mut session, "os", N_SMALL);

    // Every literal below is the pre-change sequential result locked in
    // `par_sort_equiv.rs` (identical table, identical data). Equality proves
    // the gate declined: a tie-diverging parallel merge would pick different
    // subsets of the 376-row boundary ties.
    let asc = query(&mut session, "SELECT id, a, b, v FROM os ORDER BY a;");
    assert_eq!(
        ids(&asc[..8]),
        vec![10000, 1000, 10050, 100, 10100, 10150, 10250, 10300],
        "ASC tie order must match pre-change sequential"
    );
    assert_eq!(
        ids(&asc[N_SMALL as usize - 5..]),
        vec![9928, 9945, 9962, 9979, 9996],
        "ASC NULL-tail must match pre-change sequential"
    );

    let desc = query(&mut session, "SELECT id, a, b, v FROM os ORDER BY a DESC;");
    assert_eq!(
        ids(&desc[..8]),
        vec![0, 10013, 10030, 1003, 10047, 10064, 10081, 10098],
        "DESC NULL-head must match pre-change sequential"
    );
    assert_eq!(
        ids(&desc[N_SMALL as usize - 5..]),
        vec![9750, 9800, 9850, 9900, 9950],
        "DESC tail must match pre-change sequential"
    );

    let multi = query(&mut session, "SELECT id, a, b, v FROM os ORDER BY a, b;");
    assert_eq!(
        ids(&multi[..8]),
        vec![10150, 10500, 1050, 10850, 11200, 11550, 12250, 12600],
        "multi-key head must match pre-change sequential"
    );

    // Tied TopK keeps the incumbent picks of the sequential heap, locked as
    // literals in the sibling file.
    let lim = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
    );
    assert_eq!(
        ids(&lim),
        vec![10050, 100, 1000, 10000, 10100],
        "tied TopK picks must match pre-change sequential"
    );
    let off = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5 OFFSET 3;",
    );
    assert_eq!(
        ids(&off),
        vec![100, 10000, 10150, 1000, 10300],
        "tied TopK OFFSET picks must match pre-change sequential"
    );
    eprintln!("non-total 20k shapes: all pre-change literals PASS (gate declined)");

    // Non-total shapes above threshold are deterministic, key-sorted, and
    // multiset-complete (decline itself is proven by the size-independent
    // gate unit tests plus the literal equality above).
    let root_big = scratch("nontotalbig");
    let mut big = open(&root_big);
    load_table(&mut big, "big", N_BIG);
    let spec_a = [KeySpec {
        col: COL_A,
        desc: false,
    }];
    let b1 = query(&mut big, "SELECT id, a, b, v FROM big ORDER BY a;");
    let b2 = query(&mut big, "SELECT id, a, b, v FROM big ORDER BY a;");
    assert_eq!(b1, b2, "non-total 100k sort must be deterministic");
    assert_sorted(&b1, &spec_a, "ORDER BY a");
    let unordered = query(&mut big, "SELECT id, a, b, v FROM big;");
    assert_same_multiset(&b1, &unordered, "ORDER BY a multiset");
    let t1 = query(&mut big, "SELECT id, a, b, v FROM big ORDER BY a LIMIT 5;");
    let t2 = query(&mut big, "SELECT id, a, b, v FROM big ORDER BY a LIMIT 5;");
    assert_eq!(t1, t2, "non-total 100k TopK must be deterministic");
    assert!(t1.iter().all(|r| opt_int(&r[COL_A]) == Some(0)));
    eprintln!("non-total 100k shapes: deterministic+sorted+multiset PASS");
    cleanup(&root_big);

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 4. Degenerate shapes: empty, single-row, all-NULL keys.
// ---------------------------------------------------------------------------

#[test]
fn par_sort_degenerate_shapes() {
    let root = scratch("degen");
    let mut session = open(&root);

    // Empty table, with and without PK coverage and limits.
    session
        .execute("CREATE TABLE e (id BIGINT PRIMARY KEY, a INT);")
        .unwrap();
    assert!(query(&mut session, "SELECT id, a FROM e ORDER BY id;").is_empty());
    assert!(query(&mut session, "SELECT id, a FROM e ORDER BY id LIMIT 5;").is_empty());
    assert!(query(&mut session, "SELECT id, a FROM e ORDER BY a;").is_empty());
    eprintln!("empty table: all shapes empty PASS");

    // Single row: every shape returns it.
    session
        .execute("CREATE TABLE one (id BIGINT PRIMARY KEY, a INT);")
        .unwrap();
    session
        .execute("INSERT INTO one VALUES (7, NULL);")
        .unwrap();
    assert_eq!(
        query(&mut session, "SELECT id, a FROM one ORDER BY id;").len(),
        1
    );
    assert_eq!(
        query(&mut session, "SELECT id, a FROM one ORDER BY a;").len(),
        1
    );
    assert_eq!(
        query(&mut session, "SELECT id, a FROM one ORDER BY id LIMIT 5;").len(),
        1
    );
    assert_eq!(
        query(
            &mut session,
            "SELECT id, a FROM one ORDER BY a, id LIMIT 5 OFFSET 3;"
        )
        .len(),
        0,
        "OFFSET past the single row is empty"
    );
    eprintln!("single row: all shapes PASS");

    // All-NULL keys with PK tiebreak: total via `id`, closed-form ascending.
    session
        .execute("CREATE TABLE n (id BIGINT PRIMARY KEY, a INT);")
        .unwrap();
    session
        .execute("INSERT INTO n VALUES (3, NULL), (1, NULL), (2, NULL);")
        .unwrap();
    let total_null = query(&mut session, "SELECT id, a FROM n ORDER BY a, id;");
    assert_eq!(
        total_null
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![1, 2, 3],
        "all-NULL keys with PK tiebreak sort by id"
    );
    let null_only = query(&mut session, "SELECT id, a FROM n ORDER BY a;");
    assert_eq!(null_only.len(), 3, "all-NULL single-key keeps all rows");
    eprintln!("all-NULL keys: PASS");

    // Volatile ORDER BY never takes the parallel path: it runs (sequential
    // semantics, fresh keys per row) without error.
    session
        .execute("CREATE TABLE v (id BIGINT PRIMARY KEY, a INT);")
        .unwrap();
    session
        .execute("INSERT INTO v VALUES (1, 10), (2, 20), (3, 30);")
        .unwrap();
    let vol = query(&mut session, "SELECT id, a FROM v ORDER BY random(), id;");
    assert_eq!(vol.len(), 3, "volatile ORDER BY keeps all rows");
    eprintln!("volatile ORDER BY: sequential fallback PASS");

    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 5. In-process baselines (best-of-3) for sequential vs parallel regimes.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_sort_baseline_best_of_3() {
    let root = scratch("basepar");
    let mut session = open(&root);
    load_table(&mut session, "os", N_SMALL);
    load_table(&mut session, "big", N_BIG);
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    // 20k is below the fan-out threshold: sequential regime.
    for order in [
        "ORDER BY id",
        "ORDER BY a, id",
        "ORDER BY a DESC, id DESC",
        "ORDER BY v DESC",
    ] {
        let sql = format!("SELECT id, a, b, v FROM os {order};");
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, &sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows.len(), N_SMALL as usize, "baseline {order} row count");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline sequential full-sort 20k {order}: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    // 100k with the total-order gate firing: parallel regime.
    for order in [
        "ORDER BY id",
        "ORDER BY a, id",
        "ORDER BY a DESC, id DESC",
        "ORDER BY v DESC",
    ] {
        let sql = format!("SELECT id, a, b, v FROM big {order};");
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, &sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows.len(), N_BIG as usize, "baseline {order} row count");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline parallel full-sort 100k {order}: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    // Same-size sequential references at 100k (the gate declines these:
    // no PK coverage), so the parallel win is read at equal size.
    for order in ["ORDER BY a", "ORDER BY a DESC"] {
        let sql = format!("SELECT id, a, b, v FROM big {order};");
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, &sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows.len(), N_BIG as usize, "baseline {order} row count");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline sequential full-sort 100k {order}: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    for sql in [
        "SELECT id, a, b, v FROM os ORDER BY id DESC LIMIT 10;",
        "SELECT id, a, b, v FROM big ORDER BY id DESC LIMIT 10;",
        "SELECT id, a, b, v FROM big ORDER BY a, id LIMIT 5 OFFSET 3;",
    ] {
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert!(!rows.is_empty(), "baseline TopK row count: {sql}");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline topk `{sql}`: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    // Same-size sequential TopK reference at 100k (gate declines: no PK).
    for sql in ["SELECT id, a, b, v FROM big ORDER BY a LIMIT 5;"] {
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert!(!rows.is_empty(), "baseline TopK row count: {sql}");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline sequential topk 100k `{sql}`: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    cleanup(&root);
}
