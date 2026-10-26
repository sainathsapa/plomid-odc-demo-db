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
//! Binary Hamming ANN through the public SQL path.
//!
//! Equivalence anchors compare the index-served shape against the exact shape
//! (`vector.ann_enabled = off`) on identical snapshots: strategies may
//! differ, rows must not. Jaccard stays on the exact scan by design.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn setup(
    tag: &str,
) -> (
    Executor<PlomidStorageEngine>,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let dir = std::env::temp_dir().join(format!("plomid-vecbin-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vecbin-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
    (Executor::new(engine).unwrap(), dir, wal)
}

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

/// 64-bit string with `ones` leading 1-bits.
fn bits(ones: usize) -> String {
    (0..64).map(|i| if i < ones { '1' } else { '0' }).collect()
}

/// Flip bit `at` in a bit-string.
fn flip(s: &str, at: usize) -> String {
    let mut chars: Vec<char> = s.chars().collect();
    chars[at] = if chars[at] == '1' { '0' } else { '1' };
    chars.into_iter().collect()
}

/// Seed clustered binary rows: every cluster is a center plus near-duplicates
/// (the ANN sweet spot), plus far random-looking rows.
fn seed(ex: &mut Executor<PlomidStorageEngine>) {
    ex.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64))")
        .unwrap();
    let centers = [bits(32), bits(10), bits(60)];
    let mut id = 1i64;
    let mut values = Vec::new();
    for center in &centers {
        values.push(format!("({id}, '{center}')"));
        id += 1;
        for f in [0usize, 7, 33, 63] {
            values.push(format!("({id}, '{}')", flip(center, f)));
            id += 1;
        }
        // Far rows: inverted center (distance 64 from center).
        let far: String = center
            .chars()
            .map(|c| if c == '1' { '0' } else { '1' })
            .collect();
        values.push(format!("({id}, '{far}')"));
        id += 1;
    }
    ex.execute(&format!("INSERT INTO b VALUES {};", values.join(", ")))
        .unwrap();
    ex.execute("CREATE INDEX b_bin ON b USING BINARY (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
}

fn teardown(dir: &std::path::PathBuf, wal: &std::path::PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(wal);
}

#[test]
fn binary_hamming_ann_matches_exact() {
    let (mut ex, dir, wal) = setup("hamming");
    seed(&mut ex);
    let query = format!("SELECT id FROM b ORDER BY e <~> '{}' LIMIT 4", bits(32));
    let ann = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(ann, exact);
    assert_eq!(ann.len(), 4);
    // The center row (id 1) is the nearest neighbor at distance 0.
    assert_eq!(ann[0][0], Value::Int8(1));
    teardown(&dir, &wal);
}

#[test]
fn binary_hamming_function_form_matches_exact() {
    let (mut ex, dir, wal) = setup("func");
    seed(&mut ex);
    let query = format!(
        "SELECT id FROM b ORDER BY hamming_distance(e, '{}') LIMIT 3",
        bits(10)
    );
    let ann = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(ann, exact);
    assert_eq!(ann.len(), 3);
    teardown(&dir, &wal);
}

#[test]
fn binary_jaccard_stays_exact_and_correct() {
    let (mut ex, dir, wal) = setup("jaccard");
    seed(&mut ex);
    // Jaccard must stay correct with the index present (explicit fallback).
    let query = format!("SELECT id FROM b ORDER BY e <%> '{}' LIMIT 4", bits(32));
    let served = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, &query);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served.len(), 4);
    teardown(&dir, &wal);
}

#[test]
fn binary_hamming_without_index_matches() {
    // No index at all: the exact scan answers; ANN path declines cleanly.
    let (mut ex, dir, wal) = setup("noindex");
    ex.execute("CREATE TABLE nb (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64))")
        .unwrap();
    ex.execute("INSERT INTO nb VALUES (1, '1111000011110000111100001111000011110000111100001111000011110000'), (2, '1111000011110000111100001111000011110000111100001111000011110001'), (3, '0000111100001111000011110000111100001111000011110000111100001111');")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let got = rows(
        &mut ex,
        "SELECT id FROM nb ORDER BY e <~> '1111000011110000111100001111000011110000111100001111000011110000' LIMIT 2",
    );
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][0], Value::Int8(1));
    assert_eq!(got[1][0], Value::Int8(2));
    teardown(&dir, &wal);
}

#[test]
fn binary_ann_sql_benchmark_vs_exact() {
    // SQL-level wall-clock: identical dataset, query, and k with ANN on vs
    // off. The dataset is tie-free by construction, so row-id equality is a
    // valid oracle: ladder members sit at strictly increasing distances
    // 0..15 from the query center (k=10 cuts between distance 10 and 11),
    // and every background row is at distance >= 100. Both strategies must
    // return the identical 10 rows in the identical order.
    let (mut ex, dir, wal) = setup("bench");
    ex.execute("CREATE TABLE bench (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 256))")
        .unwrap();
    let center: String = (0..256)
        .map(|i| if i % 3 == 0 { '1' } else { '0' })
        .collect();
    let mut batch = vec![format!("(1, '{center}')")];
    // Ladder: member j flips the first j bits -> distance exactly j
    // (unique distances 0..200, so the k=10 boundary never ties). Members
    // with a clean or near-clean 64-bit block resolve via block probes
    // (~194 of them: plenty to fill the rerank budget from the probe set).
    for j in 1..=200usize {
        let mut v: Vec<char> = center.chars().collect();
        for slot in v.iter_mut().take(j) {
            *slot = if *slot == '1' { '0' } else { '1' };
        }
        batch.push(format!(
            "({}, '{}')",
            1 + j as i64,
            v.into_iter().collect::<String>()
        ));
    }
    // Background: inverted center (distance 256) + striped rows (>= ~100).
    let inverted: String = center
        .chars()
        .map(|c| if c == '1' { '0' } else { '1' })
        .collect();
    let mut id = 202i64;
    let mut rng_state: u64 = 0x1234_5678_9abc_def0;
    let mut rng = move || {
        rng_state ^= rng_state >> 12;
        rng_state ^= rng_state << 25;
        rng_state ^= rng_state >> 27;
        rng_state = rng_state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        rng_state
    };
    while id <= 10_000 {
        let kind = rng() % 3;
        let row: String = if kind == 0 {
            inverted.clone()
        } else {
            (0..256)
                .map(|i| {
                    let base = if i % 3 == 0 { '1' } else { '0' };
                    if rng() % 100 < 45 {
                        if base == '1' {
                            '0'
                        } else {
                            '1'
                        }
                    } else {
                        base
                    }
                })
                .collect()
        };
        batch.push(format!("({id}, '{row}')"));
        id += 1;
        if batch.len() == 500 {
            ex.execute(&format!("INSERT INTO bench VALUES {};", batch.join(",")))
                .unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        ex.execute(&format!("INSERT INTO bench VALUES {};", batch.join(",")))
            .unwrap();
    }
    ex.execute("CREATE INDEX bench_bin ON bench USING BINARY (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let query =
        format!("SELECT id, e <~> '{center}' FROM bench ORDER BY e <~> '{center}' LIMIT 10");
    // Warmup (builds the cache) then timed reps.
    let _ = rows(&mut ex, &query);
    let reps = 3;
    let t0 = std::time::Instant::now();
    let mut ann_rows = Vec::new();
    for _ in 0..reps {
        ann_rows = rows(&mut ex, &query);
    }
    let ann_us = t0.elapsed().as_micros() / reps;
    // ANN determinism: identical repeats agree exactly.
    let again = rows(&mut ex, &query);
    assert_eq!(ann_rows, again, "ANN must be deterministic");
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let t0 = std::time::Instant::now();
    let mut exact_rows = Vec::new();
    for _ in 0..reps {
        exact_rows = rows(&mut ex, &query);
    }
    let exact_us = t0.elapsed().as_micros() / reps;
    ex.execute("SET vector.ann_enabled = on").unwrap();
    // Tie-free oracle: identical rows in identical order, distances 0..9.
    if ann_rows != exact_rows {
        eprintln!("ANN-ROWS: {ann_rows:?}");
        eprintln!("EXACT-ROWS: {exact_rows:?}");
    }
    assert_eq!(ann_rows, exact_rows, "tie-free top-k must agree exactly");
    let dists: Vec<String> = ann_rows.iter().map(|r| r[1].to_sql_text()).collect();
    let mut sorted = dists.clone();
    sorted.sort();
    assert_eq!(dists, sorted, "ascending distances");
    eprintln!("binary-ann-sql-bench rows=10000 dim=256 k=10 ann={ann_us}us exact={exact_us}us recall=1.0 tie_free=true");
    teardown(&dir, &wal);
}
