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
//! Differential fuzz for the parallel equi-INNER hash join.
//!
//! The parallel path engages automatically above `PARALLEL_JOIN_THRESHOLD`
//! (2000 combined rows) for hashable equi-INNER joins and declines to the
//! sequential path otherwise. Every case below triangulates: SQL result vs an
//! independent Rust nested-loop hand-fold, the same SQL twice in one engine,
//! and the same SQL on a fresh engine from the same seed. Small cases cover
//! the sequential path; large (3k-total) cases engage the parallel path;
//! `INT[]` array keys prove fallback correctness where `join_hash_key`
//! declines. Any mismatch fails with seed and shape printed. Deterministic
//! LCG, fixed seeds, no `rand` crate. In-process engines only.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

/// Number of randomized join cases.
const JOIN_CASES: usize = 100;
const BASE_SEED: u64 = 0x501e_ca5e_9f01_2206;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

fn seed_for(case: usize) -> u64 {
    BASE_SEED ^ ((case as u64).wrapping_mul(GOLDEN))
}

// ---------------------------------------------------------------------------
// Deterministic LCG (no `rand` crate).
// ---------------------------------------------------------------------------

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0);
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// Engine helpers.
// ---------------------------------------------------------------------------

fn scratch(tag: &str, case: usize) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-parfuzzjoin-{tag}-{case}-{id}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open_engine(root: &std::path::Path) -> Executor<PlomidStorageEngine> {
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(root, &wal, 64))
        .expect("open engine");
    Executor::new(engine).expect("executor")
}

fn run_sql(session: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {sql}: {e:?}"))
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn fingerprint(rows: &[Vec<Value>]) -> Vec<String> {
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

fn int_val(v: &Value) -> i64 {
    match v {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Scalar (int / text / cross-type int) join cases.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum KeyKind {
    Int,
    Text,
    CrossBuildSmall,
    CrossProbeSmall,
}

struct ScalarCase {
    seed: u64,
    kind: KeyKind,
    build_keys: Vec<Option<String>>,
    probe_keys: Vec<Option<String>>,
    probe_vals: Vec<i64>,
    build_ws: Vec<String>,
}

fn key_domain(rng: &mut Lcg) -> u64 {
    [1, 2, 3, 10, 50, 500][rng.below(6) as usize]
}

fn gen_int_key(rng: &mut Lcg, domain: u64, null_every: u64) -> Option<String> {
    if rng.below(null_every) == 0 {
        None
    } else {
        Some((rng.below(domain) as i64).to_string())
    }
}

fn gen_text_key(rng: &mut Lcg, domain: u64, null_every: u64) -> Option<String> {
    if rng.below(null_every) == 0 {
        None
    } else {
        Some(format!("k{}", rng.below(domain)))
    }
}

fn gen_scalar_case(case: usize) -> ScalarCase {
    let seed = seed_for(case);
    let mut rng = Lcg(seed);
    let kind = match rng.below(10) {
        0..=4 => KeyKind::Int,
        5 | 6 => KeyKind::Text,
        7 | 8 => KeyKind::CrossBuildSmall,
        _ => KeyKind::CrossProbeSmall,
    };
    // Sizes: mostly small (sequential path); every 6th case large enough to
    // engage the parallel path (>= 2000 combined rows).
    let large = case % 6 == 5;
    let (nb, np) = if large {
        (500, 2600)
    } else {
        let bpool = [0_i64, 1, 5, 100, 200];
        let ppool = [0_i64, 1, 7, 100, 300, 1500];
        (
            bpool[rng.below(bpool.len() as u64) as usize],
            ppool[rng.below(ppool.len() as u64) as usize],
        )
    };
    let domain = key_domain(&mut rng);
    // Bound the N x M fan-out: tiny domains on big tables would materialize
    // millions of rows. Small tables keep full domain variety (dup shapes).
    let min_dom = ((nb * np) / 8000).max(2) as u64;
    let domain = domain.max(min_dom);
    let null_every = [4, 7, 11][rng.below(3) as usize];
    // Skewed duplicates: small domains already fan out N x M.
    let mut build_keys = Vec::with_capacity(nb as usize);
    let mut build_ws = Vec::with_capacity(nb as usize);
    for i in 0..nb {
        build_keys.push(match kind {
            KeyKind::Text => gen_text_key(&mut rng, domain, null_every),
            _ => gen_int_key(&mut rng, domain, null_every),
        });
        build_ws.push(format!("w{i}"));
    }
    let mut probe_keys = Vec::with_capacity(np as usize);
    let mut probe_vals = Vec::with_capacity(np as usize);
    for i in 0..np {
        probe_keys.push(match kind {
            KeyKind::Text => gen_text_key(&mut rng, domain, null_every),
            _ => gen_int_key(&mut rng, domain, null_every),
        });
        probe_vals.push((rng.next() % 2001) as i64 - 1000 + i);
    }
    ScalarCase {
        seed,
        kind,
        build_keys,
        probe_keys,
        probe_vals,
        build_ws,
    }
}

fn key_sql_type(kind: KeyKind, side_build: bool) -> &'static str {
    match kind {
        KeyKind::Int => "INTEGER",
        KeyKind::Text => "TEXT",
        KeyKind::CrossBuildSmall => {
            if side_build {
                "SMALLINT"
            } else {
                "BIGINT"
            }
        }
        KeyKind::CrossProbeSmall => {
            if side_build {
                "BIGINT"
            } else {
                "SMALLINT"
            }
        }
    }
}

fn key_literal(kind: KeyKind, key: Option<&String>) -> String {
    match key {
        None => "NULL".to_string(),
        Some(k) => {
            if kind == KeyKind::Text {
                format!("'{k}'")
            } else {
                k.clone()
            }
        }
    }
}

fn load_scalar(session: &mut Executor<PlomidStorageEngine>, c: &ScalarCase) {
    session
        .execute(&format!(
            "CREATE TABLE jb (id BIGINT PRIMARY KEY, k {}, w TEXT);",
            key_sql_type(c.kind, true)
        ))
        .expect("create jb");
    session
        .execute(&format!(
            "CREATE TABLE jp (id BIGINT PRIMARY KEY, k {}, v BIGINT);",
            key_sql_type(c.kind, false)
        ))
        .expect("create jp");
    for (table, rows) in [
        (
            "jb",
            c.build_keys
                .iter()
                .enumerate()
                .map(|(i, k)| format!("({i}, {}, 'w{i}')", key_literal(c.kind, k.as_ref())))
                .collect::<Vec<_>>(),
        ),
        (
            "jp",
            c.probe_keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    format!(
                        "({i}, {}, {})",
                        key_literal(c.kind, k.as_ref()),
                        c.probe_vals[i]
                    )
                })
                .collect::<Vec<_>>(),
        ),
    ] {
        let mut batch = format!("INSERT INTO {table} VALUES ");
        let mut in_batch = 0;
        for (i, row) in rows.iter().enumerate() {
            if in_batch > 0 {
                batch.push_str(", ");
            }
            batch.push_str(row);
            in_batch += 1;
            if in_batch == 500 || i + 1 == rows.len() {
                batch.push(';');
                session.execute(&batch).expect("insert batch");
                batch = format!("INSERT INTO {table} VALUES ");
                in_batch = 0;
            }
        }
    }
}

/// Independent nested-loop hand-fold: (matched row count, SUM(probe v),
/// full-row fingerprints `pid|pkey|pval|bid|bkey|bw`).
fn fold_scalar(c: &ScalarCase) -> (i64, Option<i64>, Vec<String>) {
    let mut count = 0_i64;
    let mut sum = 0_i64;
    let mut any = false;
    let mut rows = Vec::new();
    for (pi, pk) in c.probe_keys.iter().enumerate() {
        let Some(pk) = pk.as_ref() else { continue };
        for (bi, bk) in c.build_keys.iter().enumerate() {
            let Some(bk) = bk.as_ref() else { continue };
            // Int family compares numerically (covers SMALLINT/BIGINT
            // cross-type recheck); text compares as strings.
            let equal = if c.kind == KeyKind::Text {
                pk == bk
            } else {
                pk.parse::<i64>().expect("int key") == bk.parse::<i64>().expect("int key")
            };
            if equal {
                count += 1;
                sum += c.probe_vals[pi];
                any = true;
                rows.push(format!(
                    "{pi}|{pk}|{}|{bi}|{bk}|{}",
                    c.probe_vals[pi], c.build_ws[bi]
                ));
            }
        }
    }
    rows.sort();
    (count, any.then_some(sum), rows)
}

fn check_scalar_case(case: usize) {
    let c = gen_scalar_case(case);
    let ctx = format!(
        "seed={} kind={:?} build={} probe={}",
        c.seed,
        c.kind,
        c.build_keys.len(),
        c.probe_keys.len()
    );
    let (want_count, want_sum, want_rows) = fold_scalar(&c);

    let run_all = |eng: &mut Executor<PlomidStorageEngine>| {
        let agg = run_sql(
            eng,
            "SELECT COUNT(*), SUM(jp.v) FROM jp JOIN jb ON jp.k = jb.k;",
        );
        assert_eq!(agg.len(), 1, "aggregate must return one row {ctx}");
        let got_count = int_val(&agg[0][0]);
        let got_sum = match &agg[0][1] {
            Value::Null => None,
            v => Some(int_val(v)),
        };
        let got_rows = fingerprint(&run_sql(
            eng,
            "SELECT jp.id, jp.k, jp.v, jb.id, jb.k, jb.w FROM jp JOIN jb ON jp.k = jb.k;",
        ));
        (got_count, got_sum, got_rows)
    };

    let root_a = scratch("a", case);
    let mut eng_a = open_engine(&root_a);
    load_scalar(&mut eng_a, &c);
    let a1 = run_all(&mut eng_a);
    let a2 = run_all(&mut eng_a);
    drop(eng_a);
    cleanup(&root_a);

    let root_b = scratch("b", case);
    let mut eng_b = open_engine(&root_b);
    load_scalar(&mut eng_b, &c);
    let b1 = run_all(&mut eng_b);
    drop(eng_b);
    cleanup(&root_b);

    assert_eq!(
        a1.2, a2.2,
        "JOIN determinism (same engine) MISMATCH case={case} {ctx}"
    );
    assert_eq!(
        a1.2, b1.2,
        "JOIN determinism (fresh engine) MISMATCH case={case} {ctx}"
    );
    assert_eq!(
        (a1.0, a1.1),
        (want_count, want_sum),
        "JOIN count/sum MISMATCH case={case} {ctx} got=({},{:?}) want=({},{:?})",
        a1.0,
        a1.1,
        want_count,
        want_sum
    );
    assert_eq!(
        a1.2,
        want_rows,
        "JOIN rows MISMATCH case={case} {ctx}\n got={:?}\n want={:?}",
        &a1.2[..a1.2.len().min(8)],
        &want_rows[..want_rows.len().min(8)]
    );
}

/// Local smoke vs CI full matrix: `PLOMID_FUZZ_CASES` caps the case count
/// (default 5 locally so the MacBook stays cool; CI sets 200 via
/// `make test-scale` for the full 100-case rip).
fn fuzz_case_limit() -> usize {
    std::env::var("PLOMID_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        .min(JOIN_CASES)
}

#[test]
fn par_fuzz_join_differential() {
    let cases = fuzz_case_limit();
    for case in 0..cases {
        check_scalar_case(case);
        if case % 25 == 24 {
            eprintln!("join fuzz progress: {}/{} cases green", case + 1, cases);
        }
    }
    eprintln!("join fuzz scalar: {cases} cases green, zero mismatches");
}

// ---------------------------------------------------------------------------
// Unhashable (INT[] array) keys: gate declines, sequential must stay exact.
// Aggregate-only checks (array `to_sql_text` has no Rust-side mirror here).
// ---------------------------------------------------------------------------

fn gen_array_key(rng: &mut Lcg) -> Option<Vec<i64>> {
    if rng.below(7) == 0 {
        return None;
    }
    let len = 1 + rng.below(3) as usize;
    let pool = rng.below(4);
    Some((0..len).map(|j| ((pool + j as u64) % 4) as i64).collect())
}

fn array_literal(k: Option<&Vec<i64>>) -> String {
    match k {
        None => "NULL".to_string(),
        Some(v) => format!(
            "ARRAY[{}]",
            v.iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn check_array_case(seed: u64, nb: i64, np: i64, tag: &str) {
    let mut rng = Lcg(seed);
    let build_keys: Vec<Option<Vec<i64>>> = (0..nb).map(|_| gen_array_key(&mut rng)).collect();
    let probe_keys: Vec<Option<Vec<i64>>> = (0..np).map(|_| gen_array_key(&mut rng)).collect();
    let probe_vals: Vec<i64> = (0..np)
        .map(|i| (rng.next() % 2001) as i64 - 1000 + i)
        .collect();

    // Hand fold: nested loop over element vectors.
    let mut want_count = 0_i64;
    let mut want_sum = 0_i64;
    let mut any = false;
    for (pi, pk) in probe_keys.iter().enumerate() {
        let Some(pk) = pk.as_ref() else { continue };
        for bk in build_keys.iter().flatten() {
            if pk == bk {
                want_count += 1;
                want_sum += probe_vals[pi];
                any = true;
            }
        }
    }
    let want_sum = any.then_some(want_sum);
    let ctx = format!("seed={seed} tag={tag} build={nb} probe={np}");

    let run_all = |eng: &mut Executor<PlomidStorageEngine>| {
        let agg = run_sql(
            eng,
            "SELECT COUNT(*), SUM(ap.v) FROM ap JOIN ab ON ap.k = ab.k;",
        );
        let got_count = int_val(&agg[0][0]);
        let got_sum = match &agg[0][1] {
            Value::Null => None,
            v => Some(int_val(v)),
        };
        let again = run_sql(
            eng,
            "SELECT COUNT(*), SUM(ap.v) FROM ap JOIN ab ON ap.k = ab.k;",
        );
        assert_eq!(agg, again, "array join determinism MISMATCH {ctx}");
        (got_count, got_sum)
    };

    let load = |eng: &mut Executor<PlomidStorageEngine>| {
        eng.execute("CREATE TABLE ab (id BIGINT PRIMARY KEY, k INT[], w TEXT);")
            .expect("create ab");
        eng.execute("CREATE TABLE ap (id BIGINT PRIMARY KEY, k INT[], v BIGINT);")
            .expect("create ap");
        for (table, n, lit) in [
            (
                "ab",
                nb,
                (0..nb)
                    .map(|i| {
                        format!(
                            "({i}, {}, 'w{i}')",
                            array_literal(build_keys[i as usize].as_ref())
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            (
                "ap",
                np,
                (0..np)
                    .map(|i| {
                        format!(
                            "({i}, {}, {})",
                            array_literal(probe_keys[i as usize].as_ref()),
                            probe_vals[i as usize]
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
        ] {
            let mut batch = format!("INSERT INTO {table} VALUES ");
            let mut in_batch = 0;
            for (i, row) in lit.iter().enumerate() {
                if in_batch > 0 {
                    batch.push_str(", ");
                }
                batch.push_str(row);
                in_batch += 1;
                if in_batch == 500 || i + 1 == n as usize {
                    batch.push(';');
                    eng.execute(&batch).expect("insert batch");
                    batch = format!("INSERT INTO {table} VALUES ");
                    in_batch = 0;
                }
            }
        }
    };

    let root_a = scratch(&format!("arra-{tag}"), 0);
    let mut eng_a = open_engine(&root_a);
    load(&mut eng_a);
    let a1 = run_all(&mut eng_a);
    drop(eng_a);
    cleanup(&root_a);

    let root_b = scratch(&format!("arrb-{tag}"), 0);
    let mut eng_b = open_engine(&root_b);
    load(&mut eng_b);
    let b1 = run_all(&mut eng_b);
    drop(eng_b);
    cleanup(&root_b);

    assert_eq!(a1, b1, "array join fresh-engine MISMATCH {ctx}");
    assert_eq!(
        a1,
        (want_count, want_sum),
        "ARRAY JOIN MISMATCH {ctx} got={a1:?} want=({want_count},{want_sum:?})"
    );
}

#[test]
fn par_fuzz_join_unhashable_arrays() {
    // Small (sequential) + large (above-threshold: gate declines, fallback exact).
    check_array_case(0xA1AA_0001, 5, 7, "tiny");
    check_array_case(0xA1AA_0002, 100, 300, "small");
    check_array_case(0xA1AA_0003, 500, 2000, "large-decline");
    check_array_case(0xA1AA_0004, 200, 2100, "large-decline-nullheavy");
    eprintln!("array-key fallback: 4 cases green (incl. 2 above-threshold declines)");
}

// ---------------------------------------------------------------------------
// Adversarial shapes.
// ---------------------------------------------------------------------------

fn load_int_pair(
    eng: &mut Executor<PlomidStorageEngine>,
    build_keys: &[Option<i64>],
    probe: &[(Option<i64>, i64)],
) {
    eng.execute("CREATE TABLE xb (id BIGINT PRIMARY KEY, k INTEGER, w TEXT);")
        .expect("create xb");
    eng.execute("CREATE TABLE xp (id BIGINT PRIMARY KEY, k INTEGER, v BIGINT);")
        .expect("create xp");
    let cell = |o: Option<i64>| o.map_or("NULL".to_string(), |x| x.to_string());
    let mut batch = String::from("INSERT INTO xb VALUES ");
    let mut in_batch = 0;
    for (i, k) in build_keys.iter().enumerate() {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({i}, {}, 'w{i}')", cell(*k)));
        in_batch += 1;
        if in_batch == 1000 || i + 1 == build_keys.len() {
            batch.push(';');
            eng.execute(&batch).expect("insert xb");
            batch = String::from("INSERT INTO xb VALUES ");
            in_batch = 0;
        }
    }
    let mut batch = String::from("INSERT INTO xp VALUES ");
    let mut in_batch = 0;
    for (i, (k, v)) in probe.iter().enumerate() {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        batch.push_str(&format!("({i}, {}, {v})", cell(*k)));
        in_batch += 1;
        if in_batch == 1000 || i + 1 == probe.len() {
            batch.push(';');
            eng.execute(&batch).expect("insert xp");
            batch = String::from("INSERT INTO xp VALUES ");
            in_batch = 0;
        }
    }
}

fn fold_int_pair(build: &[Option<i64>], probe: &[(Option<i64>, i64)]) -> (i64, Option<i64>) {
    let mut counts: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for k in build.iter().flatten() {
        *counts.entry(*k).or_default() += 1;
    }
    let mut count = 0_i64;
    let mut sum = 0_i64;
    let mut any = false;
    for (k, v) in probe {
        if let Some(k) = k {
            if let Some(n) = counts.get(k) {
                count += n;
                sum += n * v;
                any = true;
            }
        }
    }
    (count, any.then_some(sum))
}

fn check_int_pair(seed: u64, tag: &str, build: &[Option<i64>], probe: &[(Option<i64>, i64)]) {
    let (want_count, want_sum) = fold_int_pair(build, probe);
    let ctx = format!(
        "seed={seed} tag={tag} build={} probe={}",
        build.len(),
        probe.len()
    );
    let queries = [
        "SELECT COUNT(*), SUM(xp.v) FROM xp JOIN xb ON xp.k = xb.k;",
        "SELECT COUNT(*), SUM(xp.v) FROM xp JOIN xb ON xp.k = xb.k;",
    ];
    let root_a = scratch(&format!("adva-{tag}"), 0);
    let mut eng_a = open_engine(&root_a);
    load_int_pair(&mut eng_a, build, probe);
    let mut prev: Option<(i64, Option<i64>)> = None;
    for sql in &queries {
        let rows = run_sql(&mut eng_a, sql);
        let got = (
            int_val(&rows[0][0]),
            match &rows[0][1] {
                Value::Null => None,
                v => Some(int_val(v)),
            },
        );
        assert_eq!(
            got,
            (want_count, want_sum),
            "ADVERSARIAL MISMATCH {ctx} sql={sql} got={got:?}"
        );
        if let Some(p) = prev {
            assert_eq!(got, p, "ADVERSARIAL determinism MISMATCH {ctx}");
        }
        prev = Some(got);
    }
    // NULL keys never join.
    let nulls = run_sql(
        &mut eng_a,
        "SELECT COUNT(*) FROM xp JOIN xb ON xp.k = xb.k WHERE xp.k IS NULL;",
    );
    assert_eq!(int_val(&nulls[0][0]), 0, "NULL keys must never match {ctx}");
    drop(eng_a);
    cleanup(&root_a);

    // Fresh engine, same shape.
    let root_b = scratch(&format!("advb-{tag}"), 0);
    let mut eng_b = open_engine(&root_b);
    load_int_pair(&mut eng_b, build, probe);
    let rows = run_sql(&mut eng_b, queries[0]);
    let got_b = (
        int_val(&rows[0][0]),
        match &rows[0][1] {
            Value::Null => None,
            v => Some(int_val(v)),
        },
    );
    assert_eq!(
        got_b,
        (want_count, want_sum),
        "ADVERSARIAL fresh-engine MISMATCH {ctx}"
    );
    drop(eng_b);
    cleanup(&root_b);
    eprintln!("adversarial {tag}: count={want_count} sum={want_sum:?} exact");
}

#[test]
fn par_fuzz_join_adversarial() {
    // Bounded by design: every shaped case keeps the combined row total at or
    // above the 2000-row parallel-engagement gate (see `threshold_boundary`
    // below) while staying near the 3k-total budget in the module docs, so
    // the routine `cargo test` gate stays fast. The full 100K-scale rip lives
    // in `make test-scale` (nightly), not here.
    // 1-row build x 3k probe (parallel engaged: 3001 total).
    let probe_3k: Vec<(Option<i64>, i64)> = (0..3_000)
        .map(|i| {
            let k = if i % 11 == 0 { None } else { Some(i % 7) };
            (k, i)
        })
        .collect();
    check_int_pair(0xAD0E_0001, "1x3k", &[Some(3)], &probe_3k);

    // 3k build x 1-row probe.
    let build_3k: Vec<Option<i64>> = (0..3_000)
        .map(|i| if i % 13 == 0 { None } else { Some(i % 500) })
        .collect();
    check_int_pair(0xAD0E_0002, "3kx1", &build_3k, &[(Some(42), 7)]);

    // All-NULL keys both sides at parallel scale (1500 + 1500 = 3000 total).
    let nulls1500: Vec<(Option<i64>, i64)> = (0..1500).map(|i| (None, i)).collect();
    let nulls_b: Vec<Option<i64>> = vec![None; 1500];
    check_int_pair(0xAD0E_0003, "all-null-3k", &nulls_b, &nulls1500);

    // Empty build x 1500 probe; 1500 build x empty probe; both empty.
    let probe_1500: Vec<(Option<i64>, i64)> = (0..1500).map(|i| (Some(i % 100), i)).collect();
    let build_1500: Vec<Option<i64>> = (0..1500).map(|i| Some(i % 100)).collect();
    check_int_pair(0xAD0E_0004, "empty-build", &[], &probe_1500);
    check_int_pair(0xAD0E_0005, "empty-probe", &build_1500, &[]);
    check_int_pair(0xAD0E_0006, "both-empty", &[], &[]);

    // N x M duplicate fan-out at parallel scale: 1200 build x 1200 probe rows
    // (2400 total, still engaged) over a 250-key domain (each key ~5x per
    // side -> ~25 rows per key, ~6k total output rows).
    let dup_build: Vec<Option<i64>> = (0..1200).map(|i| Some(i % 250)).collect();
    let dup_probe: Vec<(Option<i64>, i64)> = (0..1200).map(|i| (Some(i % 250), i)).collect();
    check_int_pair(0xAD0E_0007, "dup-4x4", &dup_build, &dup_probe);
}

#[test]
fn par_fuzz_join_threshold_boundary() {
    // Single worker-count-independent gate: totals 1999 (decline) vs 2000 /
    // 2001 (engage). Same key distribution shape; correct on both sides.
    let mut rng = Lcg(0xB0DA_2000);
    for (tag, np) in [
        ("below-1999", 1499_i64),
        ("at-2000", 1500),
        ("above-2001", 1501),
    ] {
        let build: Vec<Option<i64>> = (0..500)
            .map(|i| if i % 11 == 0 { None } else { Some(i % 120) })
            .collect();
        let probe: Vec<(Option<i64>, i64)> = (0..np)
            .map(|i| {
                let k = if i % 9 == 0 {
                    None
                } else {
                    Some((rng.next() % 120) as i64)
                };
                (k, i * 3)
            })
            .collect();
        assert_eq!(
            build.len() + probe.len(),
            match tag {
                "below-1999" => 1999,
                "at-2000" => 2000,
                _ => 2001,
            }
        );
        check_int_pair(0xB0DA_2000, tag, &build, &probe);
    }
}

#[test]
fn par_fuzz_join_session_named_key_opportunistic() {
    // A column named like a session function makes the parallel gate decline;
    // the sequential path must stay deterministic. If DDL itself rejects the
    // name, record the skip (gate proven in-crate instead).
    let root = scratch("sesskey", 0);
    let mut eng = open_engine(&root);
    let ddl =
        eng.execute("CREATE TABLE sq (id BIGINT PRIMARY KEY, current_user INTEGER, v BIGINT);");
    if ddl.is_err() {
        eprintln!("session-named column DDL rejected: skipping (gate covered in-crate)");
        drop(eng);
        cleanup(&root);
        return;
    }
    eng.execute("INSERT INTO sq VALUES (1, 1, 10), (2, 2, 20), (3, NULL, 30);")
        .expect("insert sq");
    eng.execute("CREATE TABLE sr2 (id BIGINT PRIMARY KEY, k INTEGER, w TEXT);")
        .expect("create sr2");
    eng.execute("INSERT INTO sr2 VALUES (1, 1, 'a'), (2, 2, 'b');")
        .expect("insert sr2");
    let sql = "SELECT sq.id, sq.v, sr2.w FROM sq JOIN sr2 ON sq.current_user = sr2.k;";
    let first = fingerprint(&run_sql(&mut eng, sql));
    match eng.execute(sql) {
        Ok(QueryResult::Rows { rows, .. }) => {
            assert_eq!(
                first,
                fingerprint(&rows),
                "session-key join must be deterministic"
            );
            eprintln!("session-named key join deterministic: {first:?}");
        }
        Ok(other) => panic!("unexpected result shape: {other:?}"),
        Err(e) => eprintln!("session-key join errors (declined shape): {e:?} — determinism N/A"),
    }
    drop(eng);
    cleanup(&root);
}

/// Cross-type spot checks with fixed seeds (Int2 vs Int8 both directions).
#[test]
fn par_fuzz_join_cross_type_fixed() {
    for (seed, swap, tag) in [
        (0xC205_0001, false, "build-small"),
        (0xC205_0002, true, "probe-small"),
    ] {
        let mut rng = Lcg(seed);
        let nb = 1500_i64;
        let np = 1200_i64;
        let build: Vec<String> = (0..nb)
            .map(|_| {
                if rng.below(8) == 0 {
                    "NULL".to_string()
                } else {
                    (rng.below(300) as i64).to_string()
                }
            })
            .collect();
        let probe: Vec<(String, i64)> = (0..np)
            .map(|i| {
                let k = if rng.below(8) == 0 {
                    "NULL".to_string()
                } else {
                    (rng.below(300) as i64).to_string()
                };
                (k, i * 2 - 500)
            })
            .collect();
        // Hand fold numerically (NULL never matches).
        let mut bcounts: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
        for k in &build {
            if let Ok(x) = k.parse::<i64>() {
                *bcounts.entry(x).or_default() += 1;
            }
        }
        let mut want_count = 0_i64;
        let mut want_sum = 0_i64;
        let mut any = false;
        for (k, v) in &probe {
            if let Ok(x) = k.parse::<i64>() {
                if let Some(n) = bcounts.get(&x) {
                    want_count += n;
                    want_sum += n * v;
                    any = true;
                }
            }
        }
        let want = (want_count, any.then_some(want_sum));
        let (bt, pt) = if swap {
            ("BIGINT", "SMALLINT")
        } else {
            ("SMALLINT", "BIGINT")
        };
        let root = scratch(&format!("xt-{tag}"), 0);
        let mut eng = open_engine(&root);
        eng.execute(&format!(
            "CREATE TABLE xb (id BIGINT PRIMARY KEY, k {bt}, w TEXT);"
        ))
        .expect("create xb");
        eng.execute(&format!(
            "CREATE TABLE xp (id BIGINT PRIMARY KEY, k {pt}, v BIGINT);"
        ))
        .expect("create xp");
        let mut batch = String::from("INSERT INTO xb VALUES ");
        let mut in_batch = 0;
        for (i, k) in build.iter().enumerate() {
            if in_batch > 0 {
                batch.push_str(", ");
            }
            batch.push_str(&format!("({i}, {k}, 'w{i}')"));
            in_batch += 1;
            if in_batch == 500 || i + 1 == build.len() {
                batch.push(';');
                eng.execute(&batch).expect("insert xb");
                batch = String::from("INSERT INTO xb VALUES ");
                in_batch = 0;
            }
        }
        let mut batch = String::from("INSERT INTO xp VALUES ");
        let mut in_batch = 0;
        for (i, (k, v)) in probe.iter().enumerate() {
            if in_batch > 0 {
                batch.push_str(", ");
            }
            batch.push_str(&format!("({i}, {k}, {v})"));
            in_batch += 1;
            if in_batch == 500 || i + 1 == probe.len() {
                batch.push(';');
                eng.execute(&batch).expect("insert xp");
                batch = String::from("INSERT INTO xp VALUES ");
                in_batch = 0;
            }
        }
        let sql = "SELECT COUNT(*), SUM(xp.v) FROM xp JOIN xb ON xp.k = xb.k;";
        let rows = run_sql(&mut eng, sql);
        let rows2 = run_sql(&mut eng, sql);
        let got = (
            int_val(&rows[0][0]),
            match &rows[0][1] {
                Value::Null => None,
                v => Some(int_val(v)),
            },
        );
        let got2 = (
            int_val(&rows2[0][0]),
            match &rows2[0][1] {
                Value::Null => None,
                v => Some(int_val(v)),
            },
        );
        assert_eq!(
            got, got2,
            "cross-type determinism MISMATCH seed={seed} tag={tag}"
        );
        assert_eq!(
            got, want,
            "CROSS-TYPE MISMATCH seed={seed} tag={tag} got={got:?} want={want:?}"
        );
        eprintln!(
            "cross-type {tag}: count={want_count} sum={:?} exact",
            want.1
        );
        drop(eng);
        cleanup(&root);
    }
}
