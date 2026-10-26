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
//! Vector administration surfaces: metrics relations, per-index state,
//! settings validation, REINDEX invalidation and DiskANN execution.
//!
//! Every assertion runs through the public SQL path (never through crate
//! internals), so these tests guard the operator contract, not the
//! implementation.

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
    let dir = std::env::temp_dir().join(format!("plomid-vecadmin-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-vecadmin-{tag}-wal-{}", std::process::id()));
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

/// Serializes tests sharing process-global vector settings: each test
/// resets every knob to its default under one lock, so execution order and
/// thread interleaving cannot leak a `SET` from one test into another.
static SETTINGS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn reset_settings(ex: &mut Executor<PlomidStorageEngine>) -> std::sync::MutexGuard<'static, ()> {
    let guard = SETTINGS_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    for (knob, default) in [
        ("vector.ann_enabled", "on"),
        ("vector.ef_search", "100"),
        ("vector.candidate_oversample", "8"),
        ("vector.max_ann_rows", "1000000"),
        ("vector.cache_graphs", "8"),
        ("vector.max_index_vectors", "1000000"),
        ("vector.auto_ann_min_rows", "4096"),
        ("vector.compute", "auto"),
        ("vector.max_memory_bytes", "1073741824"),
    ] {
        ex.execute(&format!("SET {knob} = {default}"))
            .unwrap_or_else(|e| panic!("reset {knob} -> {e}"));
    }
    guard
}

#[test]
fn vector_stats_counters_move() {
    let (mut ex, dir, wal) = setup("stats");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
        .unwrap();
    ex.execute("INSERT INTO d VALUES (1, [1,0]), (2, [0,1])")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let before = rows(&mut ex, "SELECT * FROM plomid_vector_stats()");
    let exact_before = match &before[0][0] {
        Value::Int8(n) => *n,
        v => panic!("{v:?}"),
    };
    rows(&mut ex, "SELECT l2_distance(e, [1,0]) FROM d");
    rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 1");
    let after = rows(&mut ex, "SELECT * FROM plomid_vector_stats()");
    let exact_after = match &after[0][0] {
        Value::Int8(n) => *n,
        v => panic!("{v:?}"),
    };
    assert!(exact_after > exact_before, "exact counter moved");
    // ANN served at least one query (auto-HNSW present on 2 rows).
    let ann = match &after[0][1] {
        Value::Int8(n) => *n,
        v => panic!("{v:?}"),
    };
    assert!(ann >= 1, "ann counter moved: {after:?}");

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_indexes_reports_ready() {
    let (mut ex, dir, wal) = setup("indexes");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 3))")
        .unwrap();
    let listed = rows(&mut ex, "SELECT * FROM plomid_vector_indexes()");
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(text(&listed[0], 0), "d_e_vector_idx");
    assert_eq!(text(&listed[0], 3), "hnsw");
    assert_eq!(text(&listed[0], 4), "VECTOR(FLOAT32, 3)");
    assert_eq!(text(&listed[0], 5), "READY");
    // Per-vector payload bytes come from the typmod (3 lanes × 4 bytes).
    assert_eq!(listed[0][6], Value::Int8(12));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vector_settings_validated() {
    let (mut ex, dir, wal) = setup("settings");
    let _settings = reset_settings(&mut ex);
    // Defaults visible.
    let shown = rows(&mut ex, "SHOW vector.ef_search");
    assert_eq!(text(&shown[0], 0), "100");
    // Valid write applies.
    ex.execute("SET vector.ef_search = 32").unwrap();
    let shown = rows(&mut ex, "SHOW vector.ef_search");
    assert_eq!(text(&shown[0], 0), "32");
    // Out-of-range and unknown names fail with the allowed set spelled out.
    assert!(ex.execute("SET vector.ef_search = 99999").is_err());
    assert!(ex.execute("SET vector.nope = 1").is_err());
    assert!(ex.execute("SET vector.ann_enabled = maybe").is_err());
    assert!(ex
        .execute("SET vector.max_index_vectors = 99999999")
        .is_err());
    // The ceiling is operator-governed: pin it tiny and queries stay
    // correct via the exact scan (fallback, not failure).
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
        .unwrap();
    ex.execute("INSERT INTO d VALUES (1, [1,0]), (2, [0,1])")
        .unwrap();
    ex.execute("SET vector.max_index_vectors = 1").unwrap();
    let got = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 2");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][0], Value::Int8(1));
    // Kill-switch is observable through EXPLAIN.
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let plan = rows(
        &mut ex,
        "EXPLAIN SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 1",
    );
    let text: Vec<String> = plan.iter().map(|r| text(r, 0)).collect();
    assert!(
        text.iter().any(|l| l.contains("absent")),
        "disabled ANN must report exact: {text:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn vacuum_preserves_ann_correctness() {
    let (mut ex, dir, wal) = setup("vacuum");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .unwrap();
    ex.execute(
        "INSERT INTO d VALUES (1, [1,0,0,0]), (2, [0,1,0,0]), (3, [0,0,1,0]), (4, [0,0,0,1])",
    )
    .unwrap();
    // Force the ANN path so the cache holds lane-position addressed graphs.
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let before = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0,0,0] LIMIT 4");
    // Maintenance rewrites placement: cached graphs must be dropped here,
    // and the rebuilt index must serve identical rows afterwards.
    ex.execute("VACUUM d").unwrap();
    let after = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0,0,0] LIMIT 4");
    assert_eq!(before, after);
    assert_eq!(after[0][0], Value::Int8(1));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn compute_and_memory_knobs_are_live() {
    let (mut ex, dir, wal) = setup("knobs");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
        .unwrap();
    ex.execute("INSERT INTO d VALUES (1, [1,0]), (2, [0,1])")
        .unwrap();
    // Enum knob validates its spelling.
    ex.execute("SET vector.compute = gpu").unwrap();
    assert!(ex.execute("SET vector.compute = tpu").is_err());
    let shown = rows(&mut ex, "SHOW vector.compute");
    assert_eq!(text(&shown[0], 0), "gpu");
    // No GPU kernel is linked: execution degrades to CPU and stays correct.
    let got = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 2");
    assert_eq!(got[0][0], Value::Int8(1));
    // EXPLAIN reports the live backend selection.
    let plan = rows(
        &mut ex,
        "EXPLAIN SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 1",
    );
    let lines: Vec<String> = plan.iter().map(|r| text(r, 0)).collect();
    assert!(
        lines.iter().any(|l| l.contains("Backend: cpu(")),
        "backend tier missing: {lines:?}"
    );
    // Memory budget validates and stays correct at any setting.
    assert!(ex.execute("SET vector.max_memory_bytes = 100").is_err());
    ex.execute("SET vector.max_memory_bytes = 1048576").unwrap();
    let got = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 2");
    assert_eq!(got[0][0], Value::Int8(1));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
#[test]
fn restart_preserves_vectors_and_ann() {
    let dir = std::env::temp_dir().join(format!("plomid-vecadmin-restart-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-vecadmin-restart-wal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
    let expected: Vec<Vec<Value>>;
    {
        let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
        let mut ex = Executor::new(engine).unwrap();
        let _settings = reset_settings(&mut ex);
        ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
            .unwrap();
        ex.execute("INSERT INTO d VALUES (1, [1,0]), (2, [0,1])")
            .unwrap();
        ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
        expected = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 2");
        assert_eq!(expected[0][0], Value::Int8(1));
    }
    // Reopen the same directories: committed vectors must survive via WAL,
    // and ANN must serve identical rows (rebuilt from authoritative data).
    {
        let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
        let mut ex = Executor::new(engine).unwrap();
        let _settings = reset_settings(&mut ex);
        ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
        let again = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 2");
        assert_eq!(again, expected);
    }

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn reindex_invalidates_and_reports() {
    let (mut ex, dir, wal) = setup("reindex");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
        .unwrap();
    ex.execute("INSERT INTO d VALUES (1, [1,0])").unwrap();
    // Warm the ANN cache, then REINDEX must succeed and stay correct.
    rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 1");
    match ex.execute("REINDEX").unwrap() {
        QueryResult::Created(tag) => assert_eq!(tag, "REINDEX"),
        other => panic!("{other:?}"),
    }
    let again = rows(&mut ex, "SELECT id FROM d ORDER BY e <-> [1,0] LIMIT 1");
    assert_eq!(again[0][0], Value::Int8(1));

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn diskann_executes_like_exact() {
    let (mut ex, dir, wal) = setup("diskann");
    let _settings = reset_settings(&mut ex);
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .unwrap();
    // Drop the auto-HNSW so only the explicit DiskANN serves.
    ex.execute("DROP INDEX d_e_vector_idx").unwrap();
    ex.execute("CREATE INDEX ddisk ON d USING DISKANN (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let mut values = Vec::new();
    for i in 0..120i64 {
        // Epsilon-distinct lanes: every row has a unique distance to the
        // query, so ANN-vs-exact agreement is order-exact, not set-timid.
        let c = (i % 4) as f32;
        let e1 = (i % 7) as f32 * 0.01;
        let e2 = (i % 11) as f32 * 0.01;
        let e3 = (i % 13) as f32 * 0.01;
        values.push(format!("({i}, [{c},{e1},{e2},{e3}])"));
        if values.len() == 30 {
            ex.execute(&format!("INSERT INTO d VALUES {};", values.join(",")))
                .unwrap();
            values.clear();
        }
    }
    let query = "SELECT id FROM d ORDER BY e <-> [1,0.01,0.02,0.03] LIMIT 8";
    let ann = rows(&mut ex, query);
    ex.execute("DROP INDEX ddisk").unwrap();
    let oracle = rows(&mut ex, query);
    // Distinct distances: ANN must reproduce the exact ordering entirely.
    assert_eq!(ann, oracle);
    let min_dist = rows(&mut ex, "SELECT min(e <-> [1,0.01,0.02,0.03]) FROM d");
    let top_dist = rows(
        &mut ex,
        "SELECT e <-> [1,0.01,0.02,0.03] FROM d ORDER BY e <-> [1,0.01,0.02,0.03] LIMIT 1",
    );
    assert_eq!(top_dist[0][0], min_dist[0][0]);
    // Segments live under the database directory (portable bytes only).
    let found = walk_for(&dir, "graph.dat");
    assert!(found, "diskann segments persisted under {dir:?}");

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}

fn walk_for(dir: &std::path::Path, name: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if walk_for(&path, name) {
                return true;
            }
        } else if path.file_name().is_some_and(|f| f == name) {
            return true;
        }
    }
    false
}

#[test]
fn vector_sparse_ivf_filtered_paths_match_exact() {
    let (mut ex, dir, wal) = setup("paths");
    let _settings = reset_settings(&mut ex);
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    // Sparse inverted execution matches the exact scan.
    ex.execute("CREATE TABLE s (id BIGINT PRIMARY KEY, e SPARSE_VECTOR(FLOAT32, 50))")
        .unwrap();
    ex.execute("CREATE INDEX si ON s USING SPARSE_INVERTED (e)")
        .unwrap();
    for i in 0..60i64 {
        ex.execute(&format!(
            "INSERT INTO s VALUES ({i}, '{{{}:1.0}}/50')",
            i % 50
        ))
        .unwrap();
    }
    let ann = rows(
        &mut ex,
        "SELECT id FROM s ORDER BY cosine_distance(e, '{3:1.0}/50') LIMIT 5",
    );
    ex.execute("DROP INDEX si").unwrap();
    let oracle = rows(
        &mut ex,
        "SELECT id FROM s ORDER BY cosine_distance(e, '{3:1.0}/50') LIMIT 5",
    );
    assert_eq!(ann, oracle, "sparse ANN == exact");
    // IVF-Flat execution matches the exact scan.
    ex.execute("CREATE TABLE v (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .unwrap();
    ex.execute("CREATE INDEX vi ON v USING IVF_FLAT (e)")
        .unwrap();
    for i in 0..80i64 {
        let c = (i % 4) as f32;
        ex.execute(&format!(
            "INSERT INTO v VALUES ({i}, [{c},{},{},{}])",
            (i % 5) as f32 * 0.1,
            (i % 7) as f32 * 0.1,
            (i % 3) as f32 * 0.1
        ))
        .unwrap();
    }
    let ann = rows(
        &mut ex,
        "SELECT id FROM v ORDER BY e <-> [1,0.1,0.1,0.1] LIMIT 8",
    );
    ex.execute("DROP INDEX vi").unwrap();
    let oracle = rows(
        &mut ex,
        "SELECT id FROM v ORDER BY e <-> [1,0.1,0.1,0.1] LIMIT 8",
    );
    assert_eq!(ann, oracle, "ivf == exact");
    // Filtered subsets are fingerprint-isolated: different filters never
    // share graphs, and every row respects its filter.
    let f1 = rows(
        &mut ex,
        "SELECT id FROM v WHERE id < 40 ORDER BY e <-> [1,0.1,0.1,0.1] LIMIT 5",
    );
    assert_eq!(f1.len(), 5);
    assert!(f1
        .iter()
        .all(|r| matches!(&r[0], Value::Int8(i) if *i < 40)));
    let f2 = rows(
        &mut ex,
        "SELECT id FROM v WHERE id >= 40 ORDER BY e <-> [1,0.1,0.1,0.1] LIMIT 5",
    );
    assert!(f2
        .iter()
        .all(|r| matches!(&r[0], Value::Int8(i) if *i >= 40)));
    assert_ne!(f1, f2);

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
