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
//! Differential fuzz for parallel scan-aggregation (shared snapshot,
//! index-partitioned decode/feed, deterministic merge).
//!
//! `par_scan_agg` / `par_streaming_agg` are crate-private, so integration
//! tests cannot drive them directly. Instead every case below triangulates
//! three ways over gated shapes only (the shapes the parallel tree accepts:
//! `COUNT(*)` / `COUNT(col)` / `SUM(col)` / `AVG(col)` / `MIN(col)` /
//! `MAX(col)` / single `COUNT(DISTINCT col)`, plain column args, no `FILTER`,
//! `SUM`/`AVG` over integers only, context-free `WHERE`):
//!
//! * (a) SQL on a small table (always under `PAR_MIN_ROWS`, hence the
//!   sequential fold) vs an independent Rust hand-fold of the same rows;
//! * (b) the same SQL twice in one engine (in-engine determinism);
//! * (c) the same SQL on a second fresh engine built from the same seed
//!   (cross-engine determinism).
//!
//! Plus two large (above-threshold) cases where SQL itself engages the
//! parallel path, checked against the hand-fold bit-for-bit. Any mismatch
//! fails with the seed and shape printed. Deterministic LCG, fixed seeds,
//! no `rand` crate. In-process engines only.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::collections::{HashMap, HashSet};

/// Number of randomized small-table cases (each = 1 query x 3-way check).
const AGG_CASES: usize = 200;
/// Base seed; case `i` uses `BASE_SEED ^ (i * GOLDEN)`.
const BASE_SEED: u64 = 0x51ab_3f2c_77d9_01e5;
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
/// Row-count classes per case (all under `PAR_MIN_ROWS` by design).
const SIZES: [i64; 6] = [0, 1, 2, 50, 1000, 5000];

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

fn scratch(tag: &str, case: usize, attempt: u64) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-parfuzzagg-{tag}-{case}-{attempt}-{id}-{}",
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

/// `AVG(int)` finalization, mirroring `AggregateAccumulator::finalize`.
fn avg_int(sum: i64, count: i64) -> Value {
    if count == 0 {
        return Value::Null;
    }
    plomid_types::Numeric::from_i64(sum)
        .div(plomid_types::Numeric::from_i64(count))
        .map(|v| Value::Numeric(v.normalize()))
        .unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// Fuzz shape: rows, predicates, slots, grouping.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FuzzRow {
    k1: Option<i64>,
    k2: Option<String>,
    k3: Option<i64>,
    v: Option<i64>,
    w: Option<f64>,
}

#[derive(Clone, Copy, PartialEq)]
enum Col {
    K1,
    K2,
    K3,
}

#[derive(Clone)]
enum Pred {
    None,
    K1Eq(i64),
    K2Eq(String),
    VBetween(i64, i64),
    VGt(i64),
    VLt(i64),
    VIsNull,
    VIsNotNull,
    K1EqAndVGt(i64, i64),
    False,
}

impl Pred {
    fn matches(&self, row: &FuzzRow) -> bool {
        match self {
            Pred::None => true,
            Pred::K1Eq(c) => row.k1 == Some(*c),
            Pred::K2Eq(s) => row.k2.as_deref() == Some(s.as_str()),
            Pred::VBetween(lo, hi) => row.v.is_some_and(|x| x >= *lo && x <= *hi),
            Pred::VGt(c) => row.v.is_some_and(|x| x > *c),
            Pred::VLt(c) => row.v.is_some_and(|x| x < *c),
            Pred::VIsNull => row.v.is_none(),
            Pred::VIsNotNull => row.v.is_some(),
            Pred::K1EqAndVGt(c, d) => row.k1 == Some(*c) && row.v.is_some_and(|x| x > *d),
            Pred::False => false,
        }
    }

    fn sql(&self) -> String {
        match self {
            Pred::None => String::new(),
            Pred::K1Eq(c) => format!(" WHERE k1 = {c}"),
            Pred::K2Eq(s) => format!(" WHERE k2 = '{s}'"),
            Pred::VBetween(lo, hi) => format!(" WHERE v BETWEEN {lo} AND {hi}"),
            Pred::VGt(c) => format!(" WHERE v > {c}"),
            Pred::VLt(c) => format!(" WHERE v < {c}"),
            Pred::VIsNull => " WHERE v IS NULL".to_string(),
            Pred::VIsNotNull => " WHERE v IS NOT NULL".to_string(),
            Pred::K1EqAndVGt(c, d) => format!(" WHERE k1 = {c} AND v > {d}"),
            Pred::False => " WHERE FALSE".to_string(),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Slot {
    CountStar,
    CountV,
    CountK1,
    CountK2,
    CountW,
    SumV,
    AvgV,
    MinV,
    MaxV,
    MinW,
    MaxW,
    DistinctV,
    DistinctK1,
    DistinctK2,
}

impl Slot {
    fn sql(&self) -> &'static str {
        match self {
            Slot::CountStar => "COUNT(*)",
            Slot::CountV => "COUNT(v)",
            Slot::CountK1 => "COUNT(k1)",
            Slot::CountK2 => "COUNT(k2)",
            Slot::CountW => "COUNT(w)",
            Slot::SumV => "SUM(v)",
            Slot::AvgV => "AVG(v)",
            Slot::MinV => "MIN(v)",
            Slot::MaxV => "MAX(v)",
            Slot::MinW => "MIN(w)",
            Slot::MaxW => "MAX(w)",
            Slot::DistinctV => "COUNT(DISTINCT v)",
            Slot::DistinctK1 => "COUNT(DISTINCT k1)",
            Slot::DistinctK2 => "COUNT(DISTINCT k2)",
        }
    }

    /// Whether this slot may appear in `HAVING` (kept to engine-proven shapes).
    fn having_ok(&self) -> bool {
        matches!(
            self,
            Slot::CountStar | Slot::CountV | Slot::SumV | Slot::MinV | Slot::MaxV
        )
    }
}

#[derive(Default)]
struct Acc {
    star: i64,
    count_v: i64,
    sum_v: i64,
    min_v: Option<i64>,
    max_v: Option<i64>,
    count_k1: i64,
    count_k2: i64,
    count_w: i64,
    min_w: Option<f64>,
    max_w: Option<f64>,
    distinct_v: HashSet<i64>,
    distinct_k1: HashSet<i64>,
    distinct_k2: HashSet<String>,
}

impl Acc {
    fn add(&mut self, row: &FuzzRow) {
        self.star += 1;
        if let Some(x) = row.v {
            self.count_v += 1;
            self.sum_v += x;
            self.min_v = Some(self.min_v.map_or(x, |m| m.min(x)));
            self.max_v = Some(self.max_v.map_or(x, |m| m.max(x)));
            self.distinct_v.insert(x);
        }
        if let Some(x) = row.k1 {
            self.count_k1 += 1;
            self.distinct_k1.insert(x);
        }
        if let Some(s) = row.k2.as_deref() {
            self.count_k2 += 1;
            self.distinct_k2.insert(s.to_string());
        }
        if let Some(x) = row.w {
            self.count_w += 1;
            self.min_w = Some(self.min_w.map_or(x, |m| m.min(x)));
            self.max_w = Some(self.max_w.map_or(x, |m| m.max(x)));
        }
    }

    fn value(&self, slot: Slot) -> Value {
        match slot {
            Slot::CountStar => Value::Int8(self.star),
            Slot::CountV => Value::Int8(self.count_v),
            Slot::CountK1 => Value::Int8(self.count_k1),
            Slot::CountK2 => Value::Int8(self.count_k2),
            Slot::CountW => Value::Int8(self.count_w),
            Slot::SumV => {
                if self.count_v > 0 {
                    Value::Int8(self.sum_v)
                } else {
                    Value::Null
                }
            }
            Slot::AvgV => avg_int(self.sum_v, self.count_v),
            Slot::MinV => self.min_v.map_or(Value::Null, |x| Value::Int4(x as i32)),
            Slot::MaxV => self.max_v.map_or(Value::Null, |x| Value::Int4(x as i32)),
            Slot::MinW => self.min_w.map_or(Value::Null, Value::Float8),
            Slot::MaxW => self.max_w.map_or(Value::Null, Value::Float8),
            Slot::DistinctV => Value::Int8(self.distinct_v.len() as i64),
            Slot::DistinctK1 => Value::Int8(self.distinct_k1.len() as i64),
            Slot::DistinctK2 => Value::Int8(self.distinct_k2.len() as i64),
        }
    }
}

#[derive(Clone)]
enum Having {
    None,
    CountStarGt(i64),
    CountVGt(i64),
    SumVGt(i64),
    MinVGt(i64),
    MaxVLt(i64),
}

impl Having {
    fn keeps(&self, acc: &Acc) -> bool {
        match self {
            Having::None => true,
            Having::CountStarGt(n) => acc.star > *n,
            Having::CountVGt(n) => acc.count_v > *n,
            Having::SumVGt(n) => acc.count_v > 0 && acc.sum_v > *n,
            Having::MinVGt(n) => acc.min_v.is_some_and(|m| m > *n),
            Having::MaxVLt(n) => acc.max_v.is_some_and(|m| m < *n),
        }
    }

    fn sql(&self) -> String {
        match self {
            Having::None => String::new(),
            Having::CountStarGt(n) => format!(" HAVING COUNT(*) > {n}"),
            Having::CountVGt(n) => format!(" HAVING COUNT(v) > {n}"),
            Having::SumVGt(n) => format!(" HAVING SUM(v) > {n}"),
            Having::MinVGt(n) => format!(" HAVING MIN(v) > {n}"),
            Having::MaxVLt(n) => format!(" HAVING MAX(v) < {n}"),
        }
    }
}

fn key_values(row: &FuzzRow, keys: &[Col]) -> Vec<Value> {
    keys.iter()
        .map(|c| match c {
            Col::K1 => row.k1.map_or(Value::Null, |x| Value::Int4(x as i32)),
            Col::K2 => row.k2.clone().map_or(Value::Null, Value::Text),
            Col::K3 => row.k3.map_or(Value::Null, |x| Value::Int4(x as i32)),
        })
        .collect()
}

fn key_name(c: Col) -> &'static str {
    match c {
        Col::K1 => "k1",
        Col::K2 => "k2",
        Col::K3 => "k3",
    }
}

struct CaseShape {
    seed: u64,
    nrows: i64,
    rows: Vec<FuzzRow>,
    keys: Vec<Col>,
    slots: Vec<Slot>,
    pred: Pred,
    having: Having,
    sql: String,
}

fn gen_case(case: usize) -> CaseShape {
    let seed = seed_for(case);
    let mut rng = Lcg(seed);
    let nrows = SIZES[rng.below(SIZES.len() as u64) as usize];
    // Data modes: default mix; every 7th case all-NULL `v`; every 11th skewed `k1`.
    let all_null_v = case % 7 == 6;
    let skewed = case % 11 == 10;
    let k1dom = [2_i64, 5, 25][rng.below(3) as usize];
    let k2dom = [1_usize, 3, 8][rng.below(3) as usize];
    let k3dom = [2_i64, 10][rng.below(2) as usize];
    let v_null_every = [5_i64, 8, 13][rng.below(3) as usize];
    let w_null_every = [4_i64, 6][rng.below(2) as usize];

    let mut rows = Vec::with_capacity(nrows.max(0) as usize);
    for i in 0..nrows {
        let k1 = if rng.below(9) == 0 {
            None
        } else if skewed {
            if rng.below(10) < 8 {
                Some(0)
            } else {
                Some(rng.below(k1dom as u64) as i64)
            }
        } else {
            Some(rng.below(k1dom as u64) as i64)
        };
        let k2 = if rng.below(8) == 0 {
            None
        } else {
            Some(format!("t{}", rng.below(k2dom as u64)))
        };
        let k3 = if rng.below(10) == 0 {
            None
        } else {
            Some(rng.below(k3dom as u64) as i64)
        };
        let v = if all_null_v || i % v_null_every == 0 {
            None
        } else {
            Some((rng.next() % 2001) as i64 - 1000)
        };
        let w = if i % w_null_every == 0 {
            None
        } else {
            Some((rng.below(32) as f64) / 8.0)
        };
        rows.push(FuzzRow { k1, k2, k3, v, w });
    }

    // Grouping: 0-3 keys, canonical order K1,K2,K3 subset shuffled then sorted.
    let mut cols = [Col::K1, Col::K2, Col::K3];
    for i in (1..3).rev() {
        let j = rng.below((i + 1) as u64) as usize;
        cols.swap(i, j);
    }
    let nkeys = rng.below(4) as usize;
    let mut keys: Vec<Col> = cols[..nkeys].to_vec();
    keys.sort_by_key(|c| *c as u8);

    // Slots: 1-4 distinct kinds, at most one DISTINCT.
    let pool = [
        Slot::CountStar,
        Slot::CountV,
        Slot::CountK1,
        Slot::CountK2,
        Slot::CountW,
        Slot::SumV,
        Slot::AvgV,
        Slot::MinV,
        Slot::MaxV,
        Slot::MinW,
        Slot::MaxW,
        Slot::DistinctV,
        Slot::DistinctK1,
        Slot::DistinctK2,
    ];
    let nslots = 1 + rng.below(4) as usize;
    let mut slots = Vec::new();
    let mut has_distinct = false;
    let mut guard = 0;
    while slots.len() < nslots && guard < 60 {
        guard += 1;
        let s = pool[rng.below(pool.len() as u64) as usize];
        let is_distinct = matches!(s, Slot::DistinctV | Slot::DistinctK1 | Slot::DistinctK2);
        if slots.contains(&s) || (is_distinct && has_distinct) {
            continue;
        }
        has_distinct |= is_distinct;
        slots.push(s);
    }
    if slots.is_empty() {
        slots.push(Slot::CountStar);
    }

    // WHERE: ~40% none, else one predicate (10% of those always-false).
    let pred = match rng.below(10) {
        0..=3 => Pred::None,
        4 => Pred::K1Eq(rng.below(k1dom as u64) as i64),
        5 => Pred::K2Eq(format!("t{}", rng.below(k2dom as u64))),
        6 => {
            let lo = (rng.next() % 1501) as i64 - 1000;
            Pred::VBetween(lo, lo + (rng.next() % 401) as i64)
        }
        7 => Pred::VGt((rng.next() % 1501) as i64 - 1000),
        8 => match rng.below(5) {
            0 => Pred::VLt((rng.next() % 1501) as i64 - 750),
            1 => Pred::VIsNull,
            2 => Pred::VIsNotNull,
            3 => Pred::K1EqAndVGt(
                rng.below(k1dom as u64) as i64,
                (rng.next() % 1001) as i64 - 500,
            ),
            _ => Pred::False,
        },
        _ => Pred::VIsNotNull,
    };

    // HAVING (grouped only): ~50% none, else over a selected eligible slot.
    let having = if keys.is_empty() || rng.below(2) == 0 {
        Having::None
    } else {
        let eligible: Vec<Slot> = slots.iter().copied().filter(Slot::having_ok).collect();
        if eligible.is_empty() {
            Having::None
        } else {
            let s = eligible[rng.below(eligible.len() as u64) as usize];
            let t = [-5000_i64, -100, 0, 5, 100, 1000, 50_000][rng.below(7) as usize];
            match s {
                Slot::CountStar => Having::CountStarGt(t.clamp(0, 5000)),
                Slot::CountV => Having::CountVGt(t.clamp(0, 5000)),
                Slot::SumV => Having::SumVGt(t),
                Slot::MinV => Having::MinVGt(t.clamp(-1000, 1000)),
                Slot::MaxV => Having::MaxVLt(t.clamp(-1000, 1000)),
                _ => Having::None,
            }
        }
    };

    let mut sql = String::from("SELECT ");
    let mut first = true;
    for k in &keys {
        if !first {
            sql.push_str(", ");
        }
        first = false;
        sql.push_str(key_name(*k));
    }
    for s in &slots {
        if !first {
            sql.push_str(", ");
        }
        first = false;
        sql.push_str(s.sql());
    }
    sql.push_str(" FROM fz");
    sql.push_str(&pred.sql());
    if !keys.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(
            &keys
                .iter()
                .map(|k| key_name(*k))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    sql.push_str(&having.sql());
    sql.push(';');

    CaseShape {
        seed,
        nrows,
        rows,
        keys,
        slots,
        pred,
        having,
        sql,
    }
}

fn load_table(session: &mut Executor<PlomidStorageEngine>, shape: &CaseShape) {
    session
        .execute(
            "CREATE TABLE fz (id BIGINT PRIMARY KEY, k1 INTEGER, k2 TEXT, k3 INTEGER, \
             v INTEGER, w DOUBLE PRECISION);",
        )
        .expect("create fz");
    if shape.rows.is_empty() {
        return;
    }
    let mut batch = String::from("INSERT INTO fz VALUES ");
    let mut in_batch = 0;
    for (i, row) in shape.rows.iter().enumerate() {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let k1 = row.k1.map_or("NULL".to_string(), |x| x.to_string());
        let k2 = row
            .k2
            .as_deref()
            .map_or("NULL".to_string(), |s| format!("'{s}'"));
        let k3 = row.k3.map_or("NULL".to_string(), |x| x.to_string());
        let v = row.v.map_or("NULL".to_string(), |x| x.to_string());
        let w = row.w.map_or("NULL".to_string(), |x| format!("{x:.3}"));
        batch.push_str(&format!("({i}, {k1}, {k2}, {k3}, {v}, {w})"));
        in_batch += 1;
        if in_batch == 500 || i + 1 == shape.rows.len() {
            batch.push(';');
            session.execute(&batch).expect("insert batch");
            batch = String::from("INSERT INTO fz VALUES ");
            in_batch = 0;
        }
    }
}

fn hand_fold(shape: &CaseShape) -> Vec<Vec<Value>> {
    if shape.keys.is_empty() {
        let mut acc = Acc::default();
        for row in &shape.rows {
            if shape.pred.matches(row) {
                acc.add(row);
            }
        }
        return vec![shape.slots.iter().map(|s| acc.value(*s)).collect()];
    }
    let mut groups: HashMap<String, (Vec<Value>, Acc)> = HashMap::new();
    for row in &shape.rows {
        if !shape.pred.matches(row) {
            continue;
        }
        let key = key_values(row, &shape.keys);
        let fp = key
            .iter()
            .map(|v| v.to_sql_text())
            .collect::<Vec<_>>()
            .join("|");
        groups
            .entry(fp)
            .or_insert_with(|| (key, Acc::default()))
            .1
            .add(row);
    }
    let mut out = Vec::new();
    for (key, acc) in groups.values() {
        if !shape.having.keeps(acc) {
            continue;
        }
        let mut out_row = key.clone();
        out_row.extend(shape.slots.iter().map(|s| acc.value(*s)));
        out.push(out_row);
    }
    out
}

fn check_case(case: usize) {
    let shape = gen_case(case);
    let ctx = format!(
        "seed={} nrows={} keys=[{}] slots=[{}] sql={}",
        shape.seed,
        shape.nrows,
        shape
            .keys
            .iter()
            .map(|k| key_name(*k))
            .collect::<Vec<_>>()
            .join(","),
        shape
            .slots
            .iter()
            .map(|s| s.sql())
            .collect::<Vec<_>>()
            .join(","),
        shape.sql
    );

    // (a) hand-fold expectation (independent Rust fold).
    let want = fingerprint(&hand_fold(&shape));

    // Engine A: SQL twice (in-engine determinism) + vs hand-fold.
    let root_a = scratch("a", case, 0);
    let mut eng_a = open_engine(&root_a);
    load_table(&mut eng_a, &shape);
    let got_a = fingerprint(&run_sql(&mut eng_a, &shape.sql));
    let got_a2 = fingerprint(&run_sql(&mut eng_a, &shape.sql));
    drop(eng_a);
    cleanup(&root_a);

    // (c) fresh engine B from the same seed.
    let root_b = scratch("b", case, 0);
    let mut eng_b = open_engine(&root_b);
    load_table(&mut eng_b, &shape);
    let got_b = fingerprint(&run_sql(&mut eng_b, &shape.sql));
    drop(eng_b);
    cleanup(&root_b);

    assert_eq!(
        got_a,
        got_a2,
        "AGG DETERMINISM (same engine twice) MISMATCH case={case} {ctx}\n first={got_a:?}\n second={got_a2:?}"
    );
    assert_eq!(
        got_a, got_b,
        "AGG DETERMINISM (fresh engine) MISMATCH case={case} {ctx}\n engine_a={got_a:?}\n engine_b={got_b:?}"
    );
    assert_eq!(
        got_a, want,
        "AGG SQL-vs-HANDFOLD MISMATCH case={case} {ctx}\n got={got_a:?}\n want={want:?}"
    );
}

/// Local smoke vs CI full matrix: `PLOMID_FUZZ_CASES` caps the case count
/// (default 5 locally so the MacBook stays cool; CI sets 200 via
/// `make test-scale` for the full 200-case rip).
fn fuzz_case_limit() -> usize {
    std::env::var("PLOMID_FUZZ_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
        .min(AGG_CASES)
}

#[test]
fn par_fuzz_agg_differential() {
    let cases = fuzz_case_limit();
    for case in 0..cases {
        check_case(case);
        if case % 50 == 49 {
            eprintln!("agg fuzz progress: {}/{} cases green", case + 1, cases);
        }
    }
    eprintln!("agg fuzz: {cases} cases green, zero mismatches");
}

// ---------------------------------------------------------------------------
// Above-threshold cases: SQL itself engages the parallel path here.
// ---------------------------------------------------------------------------

fn load_big_agg(session: &mut Executor<PlomidStorageEngine>, n: i64) -> Vec<FuzzRow> {
    session
        .execute(
            "CREATE TABLE fz (id BIGINT PRIMARY KEY, k1 INTEGER, k2 TEXT, k3 INTEGER, \
             v INTEGER, w DOUBLE PRECISION);",
        )
        .expect("create fz");
    let mut rng = Lcg(0x00A6_619C_E4B5_51E1);
    let mut rows = Vec::with_capacity(n as usize);
    for i in 0..n {
        rows.push(FuzzRow {
            k1: if i % 17 == 0 { None } else { Some(i % 25) },
            k2: if i % 13 == 0 {
                None
            } else {
                Some(format!("t{}", i % 7))
            },
            k3: if i % 19 == 0 { None } else { Some(i % 4) },
            v: if i % 11 == 0 {
                None
            } else {
                Some((rng.next() % 2001) as i64 - 1000)
            },
            w: if i % 9 == 0 {
                None
            } else {
                Some((rng.below(32) as f64) / 8.0)
            },
        });
    }
    let mut batch = String::from("INSERT INTO fz VALUES ");
    let mut in_batch = 0;
    for (i, row) in rows.iter().enumerate() {
        if in_batch > 0 {
            batch.push_str(", ");
        }
        let cell = |o: Option<i64>| o.map_or("NULL".to_string(), |x| x.to_string());
        let k2 = row
            .k2
            .as_deref()
            .map_or("NULL".to_string(), |s| format!("'{s}'"));
        batch.push_str(&format!(
            "({i}, {}, {k2}, {}, {}, {})",
            cell(row.k1),
            cell(row.k3),
            cell(row.v),
            row.w.map_or("NULL".to_string(), |x| format!("{x:.3}"))
        ));
        in_batch += 1;
        if in_batch == 2000 || i + 1 == rows.len() {
            batch.push(';');
            session.execute(&batch).expect("insert batch");
            batch = String::from("INSERT INTO fz VALUES ");
            in_batch = 0;
        }
    }
    rows
}

fn fold_rows(
    rows: &[FuzzRow],
    pred: &Pred,
    keys: &[Col],
    slots: &[Slot],
    having: &Having,
) -> Vec<Vec<Value>> {
    let shape = CaseShape {
        seed: 0,
        nrows: rows.len() as i64,
        rows: rows.to_vec(),
        keys: keys.to_vec(),
        slots: slots.to_vec(),
        pred: pred.clone(),
        having: having.clone(),
        sql: String::new(),
    };
    hand_fold(&shape)
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_fuzz_agg_parallel_global() {
    let n = 110_000_i64;
    let root = scratch("biglobal", 0, 0);
    let mut eng = open_engine(&root);
    let rows = load_big_agg(&mut eng, n);
    let sql =
        "SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v), COUNT(DISTINCT v) FROM fz;";
    let want = fingerprint(&fold_rows(
        &rows,
        &Pred::None,
        &[],
        &[
            Slot::CountStar,
            Slot::CountV,
            Slot::SumV,
            Slot::MinV,
            Slot::MaxV,
            Slot::AvgV,
            Slot::DistinctV,
        ],
        &Having::None,
    ));
    let got = fingerprint(&run_sql(&mut eng, sql));
    let got2 = fingerprint(&run_sql(&mut eng, sql));
    assert_eq!(got, got2, "parallel global determinism mismatch");
    assert_eq!(
        got, want,
        "PARALLEL GLOBAL MISMATCH n={n} sql={sql}\n got={got:?}\n want={want:?}"
    );
    eprintln!("parallel global over {n} rows: exact + deterministic");
    drop(eng);
    cleanup(&root);
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn par_fuzz_agg_parallel_grouped() {
    let n = 105_000_i64;
    let root = scratch("biggroup", 0, 0);
    let mut eng = open_engine(&root);
    let rows = load_big_agg(&mut eng, n);
    let sql = "SELECT k1, k2, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM fz \
               WHERE v BETWEEN -500 AND 500 GROUP BY k1, k2 HAVING COUNT(*) > 100;";
    let want = fingerprint(&fold_rows(
        &rows,
        &Pred::VBetween(-500, 500),
        &[Col::K1, Col::K2],
        &[
            Slot::CountStar,
            Slot::CountV,
            Slot::SumV,
            Slot::MinV,
            Slot::MaxV,
            Slot::AvgV,
        ],
        &Having::CountStarGt(100),
    ));
    let got = fingerprint(&run_sql(&mut eng, sql));
    let got2 = fingerprint(&run_sql(&mut eng, sql));
    assert_eq!(got, got2, "parallel grouped determinism mismatch");
    assert_eq!(
        got, want,
        "PARALLEL GROUPED MISMATCH n={n} sql={sql}\n got={got:?}\n want={want:?}"
    );
    eprintln!("parallel grouped over {n} rows: exact + deterministic");
    drop(eng);
    cleanup(&root);
}
