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
//! ParallelSort slice-4 executor-side evidence: sort/TopK equivalence proofs.
//!
//! These tests lock the exact semantics a parallel sort must reproduce,
//! using only EXISTING public SQL (`Executor::new` plus
//! `PlomidStorageEngine::create`, temp dirs, in-process engines only).
//! No production code is touched and no new engine API is introduced:
//! parallelism is simulated with disjoint `id`-range predicates plus a
//! client-side k-way merge written in this file.
//!
//! Table under test: `os(id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT)`,
//! 8k rows (`id = 0..8000`), `a = NULL` every 17th row else `i % 50`
//! (heavy ties: ~150 rows per residue), `b = NULL` every 13th row else
//! `s00..s06` (`i % 7`, heavy ties), `v = i` (unique payload key, NaN-free:
//! ints/text only).
//!
//! The 8k size is deliberate: ties (~150/residue) stay far above the TopK
//! K values under test, NULL cadences keep hundreds of NULLs per column,
//! and id-range partitioning still spans many storage chunks — while the
//! routine gate runs ~2.5x faster than the old 20k fixture. Larger shapes
//! (100k tables, chunk sweeps) live in `#[ignore]` scale gates run via
//! `make test-scale`, not here.
//!
//! Engine semantics under test (read from `query/group.rs`, locked here):
//! * `sort_rows`: keyed `Vec<(row, keys)>`, `select_nth_unstable_by`
//!   pre-truncation when `offset + limit < rows`, then stable
//!   `sort_by(compare_order_keys)`.
//! * `TopKHeap`: max-heap over the same comparator, ties keep the incumbent.
//! * `compare_order_keys`: `nulls_first` defaults to `descending`
//!   (ASC -> NULLS LAST, DESC -> NULLS FIRST); DESC reverses only when both
//!   values are non-NULL.
//!
//! K-way merge rule used by the simulations below: repeated minimum over the
//! partition heads; ties (comparator `Equal`) break to the lowest partition
//! index. Partitions are contiguous ascending `id` ranges. Finding: this
//! reproduces the global order's KEY vector exactly, but NOT tie identities
//! (the engine's intra-tie order is storage-chunk order, and predicated
//! partition scans observe a different chunk order than the unpredicated
//! global scan) — so a parallel merge is bit-exact iff the ORDER BY is a
//! total order (unique key, or PK appended).
//!
//! Evidence emitted via `eprintln!` (read with `--nocapture`):
//! * observed NULL placement / tie literals per ORDER BY shape,
//! * partition-merge equality verdicts (+ exact divergence positions if any),
//! * TopK merge verdicts, and best-of-3 in-process baseline timings.
//!
//! Timings are in-process figures; libtest thread contention adds noise, so
//! best-of-3 in `--release` is the reported signal.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Row count of the equivalence table.
const N: i64 = 8_000;
/// Rows per multi-row INSERT statement.
const BATCH: i64 = 500;
/// `a` is NULL exactly on these rows (471 NULLs in 8k).
const NULL_A_EVERY: i64 = 17;
/// `b` is NULL exactly on these rows (615 NULLs in 8k).
const NULL_B_EVERY: i64 = 13;
/// Cardinality of the non-NULL `a` domain (heavy ties).
const A_MOD: i64 = 50;
/// Cardinality of the non-NULL `b` domain (heavy ties).
const B_MOD: i64 = 7;
/// Column positions of `SELECT id, a, b, v`.
const COL_A: usize = 1;
const COL_B: usize = 2;

fn scratch(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path =
        std::env::temp_dir().join(format!("plomid-parsort-{tag}-{id}-{}", std::process::id()));
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

fn opt_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Text(s) | Value::VarChar(s) | Value::BpChar(s) | Value::Name(s) => Some(s.clone()),
        other => panic!("expected text or NULL, got {other:?}"),
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

/// `NULL` every 13th row, else one of seven text tags.
fn gen_b(i: i64) -> Option<String> {
    if i % NULL_B_EVERY == 0 {
        None
    } else {
        Some(format!("s{:02}", i % B_MOD))
    }
}

fn cell_int(v: Option<i64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

/// `os(id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT)`, `id = v = 0..N`.
fn load_os(session: &mut Executor<PlomidStorageEngine>) {
    session
        .execute("CREATE TABLE os (id BIGINT PRIMARY KEY, a INT, b TEXT, v BIGINT);")
        .unwrap();
    let mut batch = String::from("INSERT INTO os VALUES ");
    let mut in_batch = 0;
    for i in 0..N {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let b_sql = gen_b(i)
            .map(|s| format!("'{s}'"))
            .unwrap_or_else(|| "NULL".into());
        batch.push_str(&format!("({i}, {}, {b_sql}, {i})", cell_int(gen_a(i))));
        in_batch += 1;
        if in_batch == BATCH || i + 1 == N {
            batch.push(';');
            session.execute(&batch).unwrap();
            batch = String::from("INSERT INTO os VALUES ");
            in_batch = 0;
        }
    }
}

fn expect_null_a() -> i64 {
    (0..N).filter(|i| gen_a(*i).is_none()).count() as i64
}

fn expect_null_b() -> i64 {
    (0..N).filter(|i| gen_b(*i).is_none()).count() as i64
}

// ---------------------------------------------------------------------------
// Client-side mirror of `compare_order_keys` for the no-explicit-NULLS
// queries used here (`nulls_first` defaults to `descending`; DESC reverses
// only when both values are non-NULL). Non-NULL order delegates to
// `Value::compare`, the engine's own total order, so only the NULL rule and
// the DESC guard are re-stated here.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct KeySpec {
    col: usize,
    desc: bool,
}

const SPEC_A_ASC: [KeySpec; 1] = [KeySpec {
    col: COL_A,
    desc: false,
}];
const SPEC_A_DESC: [KeySpec; 1] = [KeySpec {
    col: COL_A,
    desc: true,
}];
const SPEC_A_B: [KeySpec; 2] = [
    KeySpec {
        col: COL_A,
        desc: false,
    },
    KeySpec {
        col: COL_B,
        desc: false,
    },
];
const SPEC_A_DESC_B_ASC: [KeySpec; 2] = [
    KeySpec {
        col: COL_A,
        desc: true,
    },
    KeySpec {
        col: COL_B,
        desc: false,
    },
];
const SPEC_V_DESC: [KeySpec; 1] = [KeySpec { col: 3, desc: true }];

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

/// Stable k-way merge over already-sorted partitions: the minimum head wins;
/// full ties (`Equal`) break to the lowest partition index. This reproduces
/// the global KEY vector exactly; tie identities match the global sort only
/// when the ORDER BY is a total order (proven by the total-order shapes in
/// `sort_partition_merge_full_sort`).
fn k_way_merge(partitions: Vec<Vec<Vec<Value>>>, spec: &[KeySpec]) -> Vec<Vec<Value>> {
    let mut heads = vec![0usize; partitions.len()];
    let mut out = Vec::new();
    loop {
        let mut best: Option<usize> = None;
        for (p, part) in partitions.iter().enumerate() {
            if heads[p] >= part.len() {
                continue;
            }
            let take = match best {
                None => true,
                Some(b) => {
                    cmp_rows(&part[heads[p]], &partitions[b][heads[b]], spec) == Ordering::Less
                }
            };
            if take {
                best = Some(p);
            }
        }
        match best {
            None => break,
            Some(p) => {
                out.push(partitions[p][heads[p]].clone());
                heads[p] += 1;
            }
        }
    }
    out
}

fn id_bounds(parts: i64) -> Vec<(i64, i64)> {
    (0..parts)
        .map(|p| (N * p / parts, N * (p + 1) / parts))
        .collect()
}

fn global_order(session: &mut Executor<PlomidStorageEngine>, order: &str) -> Vec<Vec<Value>> {
    query(session, &format!("SELECT id, a, b, v FROM os {order};"))
}

fn partition_order(
    session: &mut Executor<PlomidStorageEngine>,
    lo: i64,
    hi: i64,
    order: &str,
    tail: &str,
) -> Vec<Vec<Value>> {
    query(
        session,
        &format!("SELECT id, a, b, v FROM os WHERE id >= {lo} AND id < {hi} {order}{tail};"),
    )
}

/// Positions where two full-row vectors differ.
fn diff_positions(a: &[Vec<Value>], b: &[Vec<Value>]) -> Vec<usize> {
    a.iter()
        .zip(b.iter())
        .enumerate()
        .filter_map(|(i, (x, y))| if x == y { None } else { Some(i) })
        .collect()
}

// ---------------------------------------------------------------------------
// 1. NULL/tie semantics + determinism (sorted-compare full row vectors).
// ---------------------------------------------------------------------------

#[test]
fn sort_null_tie_and_topk_determinism() {
    let root = scratch("sem");
    let mut session = open(&root);
    load_os(&mut session);
    let null_a = expect_null_a();
    let null_b = expect_null_b();
    let _ = null_b;

    // ORDER BY a (ASC default): NULLS LAST; non-NULL a=0..49 ascending.
    // Intra-tie order is storage-chunk order, NOT numeric id order (row keys
    // are `{table}:{id}` decimal strings scanned byte-wise, and ORDER BY
    // input comes from columnar chunks): the first a=0 rows are chunk-order
    // picks, stable across identical fresh engines (see the merge test, whose
    // unpartitioned globals reproduce these exact literals).
    // Observed literals: first8 ids=[1000, 100, 1050, 1100, 1150, 1200,
    // 1250, 1300] (all a=0); last5 ids=[918, 935, 952, 969, 986]
    // (all a NULL); NULL count = 471.
    let asc1 = global_order(&mut session, "ORDER BY a");
    let asc2 = global_order(&mut session, "ORDER BY a");
    assert_eq!(asc1, asc2, "full sort must be deterministic across runs");
    assert_eq!(asc1.len(), N as usize);
    assert_sorted(&asc1, &SPEC_A_ASC, "ORDER BY a");
    let asc_nulls = asc1.iter().filter(|r| r[COL_A].is_null()).count() as i64;
    assert_eq!(asc_nulls, null_a, "ASC NULL count matches generation");
    assert!(asc1[..(N as usize - null_a as usize)]
        .iter()
        .all(|r| !r[COL_A].is_null()));
    assert!(asc1[(N as usize - null_a as usize)..]
        .iter()
        .all(|r| r[COL_A].is_null()));
    // Key-level facts (independent of tie identities): first key is the
    // minimum non-NULL residue, last key is NULL.
    assert_eq!(opt_int(&asc1[0][COL_A]), Some(0));
    assert!(asc1[N as usize - 1][COL_A].is_null());
    // Tie-group sizes match the generation formulas exactly: 160 multiples
    // of 50 per residue slot minus the 17-cycle NULLs (lcm(50,17)=850, so
    // 10 NULLs land on a=0 slots: 0, 850, ..., 7650).
    let mut residue_counts = [0usize; 50];
    for r in &asc1 {
        if let Some(a) = opt_int(&r[COL_A]) {
            residue_counts[a as usize] += 1;
        }
    }
    let expect_a0 = (0..N).filter(|i| gen_a(*i) == Some(0)).count();
    assert_eq!(expect_a0, 150, "a=0 tie-group size from generation math");
    assert_eq!(residue_counts[0], expect_a0);
    for (k, count) in residue_counts.iter().enumerate() {
        let expect = (0..N).filter(|i| gen_a(*i) == Some(k as i64)).count();
        assert_eq!(*count, expect, "residue a={k} tie-group size");
    }
    eprintln!(
        "ORDER BY a boundaries: first8 ids={:?} last5 ids={:?} nulls={null_a}",
        asc1[..8].iter().map(|r| int_val(&r[0])).collect::<Vec<_>>(),
        asc1[N as usize - 5..]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        asc1[..8].iter().map(|r| int_val(&r[0])).collect::<Vec<_>>(),
        vec![1000, 100, 1050, 1100, 1150, 1200, 1250, 1300],
        "ASC tie order is chunk order, locked as literals"
    );
    assert_eq!(
        asc1[N as usize - 5..]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![918, 935, 952, 969, 986],
        "ASC NULL-tail order locked as literals"
    );

    // ORDER BY a DESC: NULLS FIRST (NULL placement flips per the
    // nulls_first-defaults-to-descending rule), then a=49..0 descending.
    // Observed literals: first8 ids=[0, 1003, 1020, 102, 1037, 1054,
    // 1071, 1088] (all a NULL); last5 ids=[7900, 7950, 800, 900, 950]
    // (all a=0).
    let desc1 = global_order(&mut session, "ORDER BY a DESC");
    let desc2 = global_order(&mut session, "ORDER BY a DESC");
    assert_eq!(desc1, desc2, "DESC sort must be deterministic across runs");
    assert_eq!(desc1.len(), N as usize);
    assert_sorted(&desc1, &SPEC_A_DESC, "ORDER BY a DESC");
    assert!(desc1[..null_a as usize].iter().all(|r| r[COL_A].is_null()));
    assert!(desc1[null_a as usize..].iter().all(|r| !r[COL_A].is_null()));
    assert_eq!(opt_int(&desc1[null_a as usize][COL_A]), Some(49));
    assert_eq!(opt_int(&desc1[N as usize - 1][COL_A]), Some(0));
    eprintln!(
        "ORDER BY a DESC boundaries: first8 ids={:?} last5 ids={:?}",
        desc1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        desc1[N as usize - 5..]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        desc1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![0, 1003, 1020, 102, 1037, 1054, 1071, 1088],
        "DESC NULL-head order locked as literals"
    );
    assert_eq!(
        desc1[N as usize - 5..]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![7900, 7950, 800, 900, 950],
        "DESC a=0 tail order locked as literals"
    );

    // ORDER BY a, b (multi-key ASC): within each `a` tie, b sorts s00..s06
    // with its own NULLS LAST.
    // Observed literals: first8 ids=[1050, 1400, 1750, 2100, 2450,
    // 2800, 3150, 3500] (all a=0, b=s00..s06 ascending runs).
    let multi1 = global_order(&mut session, "ORDER BY a, b");
    let multi2 = global_order(&mut session, "ORDER BY a, b");
    assert_eq!(multi1, multi2, "multi-key sort must be deterministic");
    assert_sorted(&multi1, &SPEC_A_B, "ORDER BY a, b");
    assert_eq!(opt_int(&multi1[0][COL_A]), Some(0));
    eprintln!(
        "ORDER BY a, b boundaries: first8 ids={:?}",
        multi1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        multi1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![1050, 1400, 1750, 2100, 2450, 2800, 3150, 3500],
        "multi-key head order locked as literals"
    );
    // Within the a=0 tie, b tags ascend and b-NULLs come last.
    let a0: Vec<&Vec<Value>> = multi1
        .iter()
        .filter(|r| opt_int(&r[COL_A]) == Some(0))
        .collect();
    let mut seen_null_b = false;
    for r in &a0 {
        if r[COL_B].is_null() {
            seen_null_b = true;
        } else if seen_null_b {
            panic!("b-NULL must sort last inside the a=0 tie");
        }
    }
    assert!(seen_null_b, "a=0 tie must contain b-NULL rows (13-cycle)");
    // Every non-NULL b tag is one of the seven generated tags.
    for r in &multi1 {
        if let Some(tag) = opt_text(&r[COL_B]) {
            assert!(
                tag.len() == 3 && tag.starts_with('s'),
                "b tag shape from generation: {tag}"
            );
        }
    }

    // Mixed directions ORDER BY a DESC, b ASC: NULL-a first, then a=49..0;
    // b still NULLS LAST inside each a group.
    // Observed literals: first8 ids=[1071, 1190, 119, 1309, 1428,
    // 1666, 1785, 1904] (all a NULL, chunk order).
    let mixed1 = global_order(&mut session, "ORDER BY a DESC, b ASC");
    let mixed2 = global_order(&mut session, "ORDER BY a DESC, b ASC");
    assert_eq!(mixed1, mixed2, "mixed-direction sort must be deterministic");
    assert_sorted(&mixed1, &SPEC_A_DESC_B_ASC, "ORDER BY a DESC, b ASC");
    assert!(mixed1[..null_a as usize].iter().all(|r| r[COL_A].is_null()));
    assert!(mixed1[null_a as usize..]
        .iter()
        .all(|r| !r[COL_A].is_null()));
    eprintln!(
        "ORDER BY a DESC, b ASC boundaries: first8 ids={:?}",
        mixed1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        mixed1[..8]
            .iter()
            .map(|r| int_val(&r[0]))
            .collect::<Vec<_>>(),
        vec![1071, 1190, 119, 1309, 1428, 1666, 1785, 1904],
        "mixed-direction head order locked as literals"
    );

    // TopK path incl. ties at the K boundary: LIMIT 5 and LIMIT 5 OFFSET 3
    // over ORDER BY a. The boundary key is a=0 (150 tied rows >> K), so the
    // retained identities are tie picks; determinism (run twice identical)
    // is the requirement. Keys must match the full-sort prefix keys.
    let lim1 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
    );
    let lim2 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
    );
    assert_eq!(lim1, lim2, "TopK LIMIT 5 tie picks must be deterministic");
    assert_eq!(lim1.len(), 5);
    assert_sorted(&lim1, &SPEC_A_ASC, "ORDER BY a LIMIT 5");
    assert!(
        lim1.iter().all(|r| opt_int(&r[COL_A]) == Some(0)),
        "LIMIT 5 keys all hit the a=0 boundary tie"
    );
    for (i, (got, full)) in lim1.iter().zip(asc1.iter()).enumerate() {
        assert_eq!(
            cmp_rows(got, full, &SPEC_A_ASC),
            Ordering::Equal,
            "LIMIT 5 row {i} keys match the full-sort prefix keys"
        );
    }
    eprintln!(
        "TopK LIMIT 5 ids={:?} vs full-prefix ids={:?}",
        lim1.iter().map(|r| int_val(&r[0])).collect::<Vec<_>>(),
        asc1[..5].iter().map(|r| int_val(&r[0])).collect::<Vec<_>>()
    );
    // Observed literals: TopK keeps [1050, 1100, 100, 1000, 1150] while
    // the full sort prefix is [1000, 100, 1050, 1100, 1150] — same keys
    // (all a=0), identities differ at positions 0, 1, 2, 3. The TopK heap
    // (tie keeps incumbent over chunk order) and the full sort
    // (`select_nth_unstable_by` truncation + stable sort) pick different
    // deterministic subsets of the 150-row boundary tie.
    assert_eq!(
        lim1.iter().map(|r| int_val(&r[0])).collect::<Vec<_>>(),
        vec![1050, 1100, 100, 1000, 1150],
        "TopK LIMIT 5 tie picks locked as literals"
    );
    assert_eq!(
        diff_positions(&lim1, &asc1[..5]),
        vec![0, 1, 2, 3],
        "TopK-vs-full tie divergence positions"
    );

    let off1 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5 OFFSET 3;",
    );
    let off2 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5 OFFSET 3;",
    );
    assert_eq!(off1, off2, "TopK LIMIT 5 OFFSET 3 must be deterministic");
    assert_eq!(off1.len(), 5);
    assert_sorted(&off1, &SPEC_A_ASC, "ORDER BY a LIMIT 5 OFFSET 3");
    assert!(off1.iter().all(|r| opt_int(&r[COL_A]) == Some(0)));
    for (i, (got, full)) in off1.iter().zip(asc1[3..8].iter()).enumerate() {
        assert_eq!(
            cmp_rows(got, full, &SPEC_A_ASC),
            Ordering::Equal,
            "OFFSET row {i} keys match full-sort keys at 3+i"
        );
    }
    eprintln!(
        "TopK LIMIT 5 OFFSET 3 ids={:?}",
        off1.iter().map(|r| int_val(&r[0])).collect::<Vec<_>>()
    );
    assert_eq!(
        off1.iter().map(|r| int_val(&r[0])).collect::<Vec<_>>(),
        vec![1000, 1100, 1200, 100, 1300],
        "TopK LIMIT 5 OFFSET 3 tie picks locked as literals"
    );

    // Unique-key TopK: ORDER BY v DESC LIMIT 10 (v = id, no ties) equals the
    // full-sort prefix exactly, ids 7999..7990.
    let v1 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY v DESC LIMIT 10;",
    );
    let v2 = query(
        &mut session,
        "SELECT id, a, b, v FROM os ORDER BY v DESC LIMIT 10;",
    );
    assert_eq!(v1, v2, "unique-key TopK must be deterministic");
    assert_sorted(&v1, &SPEC_V_DESC, "ORDER BY v DESC LIMIT 10");
    let v_ids: Vec<i64> = v1.iter().map(|r| int_val(&r[0])).collect();
    assert_eq!(
        v_ids,
        vec![7999, 7998, 7997, 7996, 7995, 7994, 7993, 7992, 7991, 7990]
    );
    let vfull = global_order(&mut session, "ORDER BY v DESC");
    assert_sorted(&vfull, &SPEC_V_DESC, "ORDER BY v DESC");
    assert_eq!(
        v1,
        vfull[..10],
        "unique-key LIMIT equals the full-sort prefix"
    );

    eprintln!("semantics: N={N} null_a={null_a} null_b={null_b} a0_tie=150 v_top10=7999..7990");
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 2. Full-sort partition simulation: disjoint id ranges, engine sorts each
//    partition, client k-way merge must equal the unpartitioned result.
// ---------------------------------------------------------------------------

#[test]
fn sort_partition_merge_full_sort() {
    let root = scratch("merge");
    let mut session = open(&root);
    load_os(&mut session);

    // Tied shapes: the merge is KEY-exact at every position, but full-row
    // identity diverges inside tie groups (the engine's intra-tie order is
    // storage-chunk order, not id order, and byte-wise scans do not visit
    // numeric id ranges contiguously). The diff positions below ARE the
    // characterization the merge design must handle.
    let tied_shapes: &[(&str, &[KeySpec])] = &[
        ("ORDER BY a", &SPEC_A_ASC),
        ("ORDER BY a DESC", &SPEC_A_DESC),
        ("ORDER BY a, b", &SPEC_A_B),
        ("ORDER BY a DESC, b ASC", &SPEC_A_DESC_B_ASC),
    ];
    for (order, spec) in tied_shapes {
        let global = global_order(&mut session, order);
        assert_eq!(global.len(), N as usize);
        for parts in [2i64, 4, 8] {
            let mut partitions = Vec::new();
            for (lo, hi) in id_bounds(parts) {
                partitions.push(partition_order(&mut session, lo, hi, order, ""));
            }
            assert_eq!(
                partitions.iter().map(Vec::len).sum::<usize>(),
                N as usize,
                "{order} P={parts} partitions cover all rows"
            );
            let merged = k_way_merge(partitions, spec);
            // Key-exactness: every merged row ties (comparator Equal) with
            // the global row at the same position.
            for (i, (m, g)) in merged.iter().zip(global.iter()).enumerate() {
                assert_eq!(
                    cmp_rows(m, g, spec),
                    Ordering::Equal,
                    "{order} P={parts} row {i} keys must match global keys"
                );
            }
            let diffs = diff_positions(&merged, &global);
            let sample: Vec<(usize, i64, i64)> = diffs
                .iter()
                .take(3)
                .map(|i| (*i, int_val(&merged[*i][0]), int_val(&global[*i][0])))
                .collect();
            eprintln!(
                "full-sort merge {order} P={parts}: KEY-EXACT {N}/{N}; \
                 identity diffs={} (all inside tie groups) sample(pos,merged_id,global_id)={sample:?}",
                diffs.len()
            );
        }
    }

    // Total-order shapes: with a unique tiebreaker the merge is exactly
    // equal, at every P. This is the green path the parallel merge design
    // must require (or enforce by appending the PK).
    let total_id = [
        SPEC_A_ASC[0],
        KeySpec {
            col: 0,
            desc: false,
        },
    ];
    let total_shapes: &[(&str, &[KeySpec])] = &[
        ("ORDER BY v DESC", &SPEC_V_DESC),
        ("ORDER BY a, id", &total_id),
    ];
    for (order, spec) in total_shapes {
        let global = global_order(&mut session, order);
        assert_eq!(global.len(), N as usize);
        for parts in [2i64, 4, 8] {
            let mut partitions = Vec::new();
            for (lo, hi) in id_bounds(parts) {
                partitions.push(partition_order(&mut session, lo, hi, order, ""));
            }
            let merged = k_way_merge(partitions, spec);
            let diffs = diff_positions(&merged, &global);
            assert!(
                diffs.is_empty(),
                "{order} P={parts}: total-order merge must equal global, diffs at {diffs:?}"
            );
            eprintln!("full-sort merge {order} P={parts}: exact equality PASS ({N} rows)");
        }
    }
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 3. TopK merge simulation: per-partition ORDER BY a LIMIT K, merge-take-K.
// ---------------------------------------------------------------------------

#[test]
fn sort_topk_partition_merge() {
    let root = scratch("topk");
    let mut session = open(&root);
    load_os(&mut session);

    // Unique-key TopK merges exactly: v is a total order, so every
    // partition's local top-10 union contains the global top-10.
    for parts in [2i64, 4, 8] {
        let global = query(
            &mut session,
            "SELECT id, a, b, v FROM os ORDER BY v DESC LIMIT 10;",
        );
        let mut partitions = Vec::new();
        for (lo, hi) in id_bounds(parts) {
            partitions.push(partition_order(
                &mut session,
                lo,
                hi,
                "ORDER BY v DESC",
                " LIMIT 10",
            ));
        }
        let merged_full = k_way_merge(partitions, &SPEC_V_DESC);
        let merged: Vec<Vec<Value>> = merged_full.into_iter().take(10).collect();
        assert_eq!(
            merged, global,
            "unique-key TopK P={parts} merge-take-10 must equal global"
        );
        eprintln!("topk merge ORDER BY v DESC LIMIT 10 P={parts}: exact equality PASS");
    }

    // Tied-key TopK: ORDER BY a LIMIT 5 with the a=0 boundary tie (150 rows).
    // Merge-take-K over per-partition LIMIT 5 candidates reproduces the
    // global TopK keys exactly; identities are tie picks, characterized below.
    for parts in [2i64, 4, 8] {
        let global = query(
            &mut session,
            "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
        );
        let rerun = query(
            &mut session,
            "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
        );
        assert_eq!(
            global, rerun,
            "global tied TopK is deterministic in-process"
        );
        let mut partitions = Vec::new();
        for (lo, hi) in id_bounds(parts) {
            partitions.push(partition_order(
                &mut session,
                lo,
                hi,
                "ORDER BY a",
                " LIMIT 5",
            ));
            assert_eq!(partitions.last().map(Vec::len), Some(5));
        }
        let merged_full = k_way_merge(partitions, &SPEC_A_ASC);
        let merged: Vec<Vec<Value>> = merged_full.into_iter().take(5).collect();
        assert_sorted(&merged, &SPEC_A_ASC, "merged TopK");
        // Keys must agree at every position (all a=0 on both sides).
        for (i, (m, g)) in merged.iter().zip(global.iter()).enumerate() {
            assert_eq!(opt_int(&m[COL_A]), Some(0), "merged TopK row {i} key");
            assert_eq!(opt_int(&g[COL_A]), Some(0), "global TopK row {i} key");
            assert_eq!(
                cmp_rows(m, g, &SPEC_A_ASC),
                Ordering::Equal,
                "row {i} keys agree"
            );
        }
        let diffs = diff_positions(&merged, &global);
        let merged_ids: Vec<i64> = merged.iter().map(|r| int_val(&r[0])).collect();
        let global_ids: Vec<i64> = global.iter().map(|r| int_val(&r[0])).collect();
        if diffs.is_empty() {
            eprintln!(
                "topk merge ORDER BY a LIMIT 5 P={parts}: exact equality PASS ids={merged_ids:?}"
            );
        } else {
            // Honest divergence: every differing position is inside the a=0
            // boundary tie (keys Equal, only scan-order identities differ).
            // Cause: the boundary tie group (150 rows) is larger than K, so
            // per-partition LIMIT 5 keeps only local tie picks and the global
            // TopK keeps its own deterministic tie picks; without a unique
            // tiebreaker the K-subset identity is an implementation detail of
            // the truncation (unstable select / incumbent-keeps), not a merge
            // defect. Design consequence: a parallel TopK merge is exact iff
            // the ORDER BY is a total order (add `id`); otherwise only the
            // key vector is exact.
            for i in &diffs {
                assert_eq!(
                    cmp_rows(&merged[*i], &global[*i], &SPEC_A_ASC),
                    Ordering::Equal
                );
            }
            eprintln!(
                "topk merge ORDER BY a LIMIT 5 P={parts}: KEY-EQUAL but ids differ at {diffs:?} \
                 merged_ids={merged_ids:?} global_ids={global_ids:?} \
                 (boundary tie a=0, 150 rows >> K=5; add `id` for exact identity)"
            );
        }

        // Total-order TopK merges exactly: ORDER BY a, id LIMIT 5 has no
        // ties, so partition-local top-5 unions contain the global top-5.
        let global_t = query(
            &mut session,
            "SELECT id, a, b, v FROM os ORDER BY a, id LIMIT 5;",
        );
        let mut tparts = Vec::new();
        for (lo, hi) in id_bounds(parts) {
            tparts.push(partition_order(
                &mut session,
                lo,
                hi,
                "ORDER BY a, id",
                " LIMIT 5",
            ));
        }
        let spec_total = [
            SPEC_A_ASC[0],
            KeySpec {
                col: 0,
                desc: false,
            },
        ];
        let merged_t: Vec<Vec<Value>> = k_way_merge(tparts, &spec_total)
            .into_iter()
            .take(5)
            .collect();
        assert_eq!(
            merged_t, global_t,
            "total-order TopK P={parts} must equal global"
        );
        eprintln!("topk merge ORDER BY a, id LIMIT 5 P={parts}: exact equality PASS");
    }
    cleanup(&root);
}

// ---------------------------------------------------------------------------
// 4. In-process baselines (best-of-3) for the full-sort 8k queries.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "scale: best-of-3 wall-clock baselines over 8K rows; runs in CI via make test-scale"]
fn sort_baseline_best_of_3() {
    let root = scratch("base");
    let mut session = open(&root);
    load_os(&mut session);
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    let full_shapes = [
        "ORDER BY a",
        "ORDER BY a DESC",
        "ORDER BY a, b",
        "ORDER BY a DESC, b ASC",
        "ORDER BY v DESC",
    ];
    for order in full_shapes {
        let sql = format!("SELECT id, a, b, v FROM os {order};");
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, &sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(rows.len(), N as usize, "baseline {order} row count");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!(
            "baseline full-sort 8k {order}: best-of-3 {best_ms:.2}ms ({profile}, in-process)"
        );
    }

    let topk_shapes = [
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5;",
        "SELECT id, a, b, v FROM os ORDER BY a LIMIT 5 OFFSET 3;",
        "SELECT id, a, b, v FROM os ORDER BY v DESC LIMIT 10;",
    ];
    for sql in topk_shapes {
        let mut best_ms = f64::MAX;
        for _ in 0..3 {
            let started = Instant::now();
            let rows = query(&mut session, sql);
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            assert!(!rows.is_empty(), "baseline TopK row count: {sql}");
            best_ms = best_ms.min(elapsed_ms);
        }
        eprintln!("baseline topk 8k `{sql}`: best-of-3 {best_ms:.2}ms ({profile}, in-process)");
    }
    cleanup(&root);
}
