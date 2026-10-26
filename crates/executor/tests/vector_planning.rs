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
//! Vector planning surfaces: filter-aware strategy, radius planning,
//! and EXPLAIN diagnostics — all through the public SQL path.
//!
//! Equivalence anchors compare the ANN-served shape against the exact
//! shape (ANN disabled) on identical snapshots: strategies may differ,
//! rows must not.

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
    let dir = std::env::temp_dir().join(format!("plomid-vecplan-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vecplan-{tag}-wal-{}", std::process::id()));
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

fn text(row: &[Value], col: usize) -> String {
    match &row[col] {
        Value::Text(s) => s.clone(),
        v => v.to_sql_text(),
    }
}

fn seed_docs(ex: &mut Executor<PlomidStorageEngine>) {
    ex.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, tenant BIGINT, e VECTOR(FLOAT32, 4))")
        .unwrap();
    // Two tenants: tenant 1 holds the near rows, tenant 2 the far rows.
    ex.execute(
        "INSERT INTO docs VALUES \
        (1, 1, [1,0,0,0]), (2, 1, [0.9,0.1,0,0]), (3, 1, [0.8,0.2,0,0]), \
        (4, 2, [0,0,1,0]), (5, 2, [0,0,0.9,0.1]), (6, 2, [0,0,0.8,0.2])",
    )
    .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
}

#[test]
fn filtered_ann_matches_exact() {
    let (mut ex, dir, wal) = setup("filtered");
    seed_docs(&mut ex);
    let ann = rows(
        &mut ex,
        "SELECT id FROM docs WHERE tenant = 1 ORDER BY e <-> [1,0,0,0] LIMIT 3",
    );
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(
        &mut ex,
        "SELECT id FROM docs WHERE tenant = 1 ORDER BY e <-> [1,0,0,0] LIMIT 3",
    );
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(ann, exact);
    assert_eq!(ann.len(), 3);
    assert_eq!(ann[0][0], Value::Int8(1));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn tiny_filtered_survivors_stay_exact_and_correct() {
    let (mut ex, dir, wal) = setup("tiny");
    seed_docs(&mut ex);
    // One survivor: below every ANN gate, exact by construction.
    let got = rows(
        &mut ex,
        "SELECT id FROM docs WHERE id = 4 ORDER BY e <-> [1,0,0,0] LIMIT 2",
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], Value::Int8(4));
    // Impossible filter: valid empty, no index touch.
    let empty = rows(
        &mut ex,
        "SELECT id FROM docs WHERE id < 0 ORDER BY e <-> [1,0,0,0] LIMIT 2",
    );
    assert!(empty.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn radius_ann_matches_exact() {
    let (mut ex, _dir, _wal) = setup("radius");
    seed_docs(&mut ex);
    let ann = rows(
        &mut ex,
        "SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) < 0.5",
    );
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(
        &mut ex,
        "SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) < 0.5",
    );
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(ann, exact);
    // Rows 1 (d=0), 2 (~0.14), 3 (~0.28) match; the tenant-2 rows do not.
    assert_eq!(ann.len(), 3);
    assert_eq!(ann[0][0], Value::Int8(1));
}

#[test]
fn radius_edges_are_exact() {
    let (mut ex, dir, wal) = setup("edges");
    seed_docs(&mut ex);
    // Zero radius: only the exact duplicate (`< 0.0` matches nothing by definition).
    let zero = rows(
        &mut ex,
        "SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) <= 0.0",
    );
    assert_eq!(zero.len(), 1);
    assert_eq!(zero[0][0], Value::Int8(1));
    // Negative bound: valid empty.
    let neg = rows(
        &mut ex,
        "SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) < -1.0",
    );
    assert!(neg.is_empty());
    // Huge radius: every row, distance-ordered.
    let all = rows(
        &mut ex,
        "SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) < 100.0",
    );
    assert_eq!(all.len(), 6);
    assert_eq!(all[0][0], Value::Int8(1));
    // Cosine radius on the same shape: rows 1-3 are all within 0.1.
    let cos = rows(
        &mut ex,
        "SELECT id FROM docs WHERE cosine_distance(e, [1,0,0,0]) < 0.1",
    );
    assert_eq!(cos.len(), 3);
    assert_eq!(cos[0][0], Value::Int8(1));
    // Similarity threshold form agrees with the distance form
    // (`sim > 0.9` admits the same 3 rows as `cosine distance < 0.1`).
    let sim = rows(
        &mut ex,
        "SELECT id FROM docs WHERE cosine_similarity(e, [1,0,0,0]) > 0.9",
    );
    assert_eq!(sim.len(), 3);
    assert_eq!(sim[0][0], Value::Int8(1));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn radius_on_empty_table_is_empty() {
    let (mut ex, dir, wal) = setup("empty");
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
        .unwrap();
    let got = rows(
        &mut ex,
        "SELECT id FROM d WHERE l2_distance(e, [1,0]) < 1.0",
    );
    assert!(got.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn explain_reports_filter_and_radius_strategies() {
    let (mut ex, dir, wal) = setup("explain");
    seed_docs(&mut ex);
    // Filtered Top-K: strategy + selectivity reasoning.
    let plan = rows(
        &mut ex,
        "EXPLAIN SELECT id FROM docs WHERE tenant = 1 ORDER BY e <-> [1,0,0,0] LIMIT 2",
    );
    let lines: Vec<String> = plan.iter().map(|r| text(r, 0)).collect();
    assert!(
        lines.iter().any(|l| l.contains("Filter strategy:")),
        "filter strategy missing: {lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("Reason:")),
        "reasons missing: {lines:?}"
    );
    // Pure radius: strategy + bounds in both unit systems.
    let plan = rows(
        &mut ex,
        "EXPLAIN SELECT id FROM docs WHERE l2_distance(e, [1,0,0,0]) < 0.5",
    );
    let lines: Vec<String> = plan.iter().map(|r| text(r, 0)).collect();
    assert!(
        lines.iter().any(|l| l.contains("Radius strategy:")),
        "radius strategy missing: {lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.contains("index bound")),
        "index bound missing: {lines:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
