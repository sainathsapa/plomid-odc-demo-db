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
//! ANN contract regression tests: index/method/type combinations with no
//! execution path are **rejected at DDL time** (the executable envelope),
//! while metric-level declines inside an accepted method (e.g. Jaccard on an
//! HNSW index) still serve exact results. Equivalence anchors compare the
//! index-present shape against the exact shape (`vector.ann_enabled = off`)
//! on identical snapshots.

use plomid_core::observability;
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
    let dir = std::env::temp_dir().join(format!("plomid-vecfb-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vecfb-{tag}-wal-{}", std::process::id()));
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

fn plan_lines(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<String> {
    rows(ex, sql)
        .into_iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            v => v.to_sql_text(),
        })
        .collect()
}

fn teardown(dir: &std::path::PathBuf, wal: &std::path::PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(wal);
}

fn seed_binary(ex: &mut Executor<PlomidStorageEngine>) {
    ex.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64))")
        .unwrap();
    ex.execute(
        "INSERT INTO b VALUES (1, '0101010101010101010101010101010101010101010101010101010101010101'), \
         (2, '0101010101010101010101010101010101010101010101010101010101010100'), \
         (3, '1111000011110000111100001111000011110000111100001111000011110000');",
    )
    .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
}

#[test]
fn binary_hnsw_is_rejected_at_ddl() {
    // HNSW over bits has no execution path; the configuration is rejected
    // at DDL time instead of being accepted and silently exact forever.
    let (mut ex, dir, wal) = setup("binhnsw");
    seed_binary(&mut ex);
    let err = ex
        .execute("CREATE INDEX b_hnsw ON b USING HNSW (e)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("VECTOR_INDEX_UNSUPPORTED") && err.contains("USING BINARY"),
        "rejection must name the executable alternative: {err}"
    );
    // No index metadata was recorded; the Hamming query still answers
    // exactly and is order-identical to the ANN-disabled shape.
    let q = "SELECT id FROM b ORDER BY e <~> '0101010101010101010101010101010101010101010101010101010101010101' LIMIT 2";
    let served = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served.len(), 2);
    assert_eq!(served[0][0], Value::Int8(1));
    teardown(&dir, &wal);
}

#[test]
fn diskann_on_binary_is_rejected_at_ddl() {
    // DiskANN executes dense lanes only; a binary column must be rejected
    // at DDL time with the executable alternative named.
    let (mut ex, dir, wal) = setup("bindisk");
    seed_binary(&mut ex);
    let err = ex
        .execute("CREATE INDEX b_disk ON b USING DISKANN (e)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("VECTOR_INDEX_UNSUPPORTED") && err.contains("USING BINARY"),
        "rejection must name the executable alternative: {err}"
    );
    let q = "SELECT id FROM b ORDER BY e <~> '0101010101010101010101010101010101010101010101010101010101010101' LIMIT 2";
    let served = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served[0][0], Value::Int8(1));
    teardown(&dir, &wal);
}

#[test]
fn diskann_on_sparse_is_rejected_at_ddl() {
    let (mut ex, dir, wal) = setup("sparsedisk");
    ex.execute("CREATE TABLE s (id BIGINT PRIMARY KEY, e SPARSE_VECTOR(FLOAT32, 8))")
        .unwrap();
    ex.execute("INSERT INTO s VALUES (1, '{0:1.0,3:1.0}/8');")
        .unwrap();
    let err = ex
        .execute("CREATE INDEX s_disk ON s USING DISKANN (e)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("VECTOR_INDEX_UNSUPPORTED") && err.contains("SPARSE_INVERTED"),
        "rejection must name the executable alternative: {err}"
    );
    teardown(&dir, &wal);
}

#[test]
fn multi_vector_dense_methods_are_rejected_at_ddl() {
    // MULTI_VECTOR has no ANN execution path in v0.1.0 (queries use the
    // exact constituent kernel), so every dense method must be rejected.
    let (mut ex, dir, wal) = setup("multireject");
    ex.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, e MULTI_VECTOR(FLOAT32, 4, 2))")
        .unwrap();
    for method in ["HNSW", "DISKANN", "IVF_FLAT", "IVF_SQ", "IVF_PQ"] {
        let err = ex
            .execute(&format!("CREATE INDEX m_{method} ON m USING {method} (e)"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("VECTOR_INDEX_UNSUPPORTED") && err.contains("MULTI_VECTOR"),
            "{method} on MULTI_VECTOR must be rejected with the envelope hint: {err}"
        );
    }
    // FLAT stays accepted by design: it is the exact baseline.
    ex.execute("CREATE INDEX m_flat ON m USING FLAT (e)")
        .unwrap();
    teardown(&dir, &wal);
}

#[test]
fn dense_jaccard_with_hnsw_falls_back_exactly() {
    // HNSW serves L2/Cosine/Dot only; Jaccard over the same index is exact.
    let (mut ex, dir, wal) = setup("densejac");
    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 3))")
        .unwrap();
    ex.execute("INSERT INTO d VALUES (1, [1,0,0]), (2, [1,1,0]), (3, [0,0,1]);")
        .unwrap();
    ex.execute("CREATE INDEX d_hnsw ON d USING HNSW (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let q = "SELECT id FROM d ORDER BY jaccard_distance(e, [1,0,0]) LIMIT 2";
    let served = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served.len(), 2);
    teardown(&dir, &wal);
}

#[test]
fn sparse_hamming_falls_back_exactly() {
    // Sparse inverted serves L2/Cosine/Dot; Hamming is binarised-exact.
    let (mut ex, dir, wal) = setup("sparseham");
    ex.execute("CREATE TABLE s (id BIGINT PRIMARY KEY, e SPARSE_VECTOR(FLOAT32, 8))")
        .unwrap();
    ex.execute(
        "INSERT INTO s VALUES (1, '{0:1.0,3:1.0}/8'), (2, '{0:1.0,4:1.0}/8'), (3, '{7:1.0}/8');",
    )
    .unwrap();
    ex.execute("CREATE INDEX s_si ON s USING SPARSE_INVERTED (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let q = "SELECT id FROM s ORDER BY hamming_distance(e, '{0:1.0,3:1.0}/8') LIMIT 2";
    let served = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, q);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served[0][0], Value::Int8(1));
    teardown(&dir, &wal);
}

#[test]
fn explain_marks_binary_usable_and_hnsw_rejected() {
    // Planner behavior: a BINARY index on a binary column plans usable for
    // Hamming; HNSW on the same column is rejected at DDL (no execution
    // path), so no plan can ever claim it.
    let (mut ex, dir, wal) = setup("explain");
    seed_binary(&mut ex);
    let err = ex
        .execute("CREATE INDEX b_hnsw ON b USING HNSW (e)")
        .unwrap_err()
        .to_string();
    assert!(err.contains("VECTOR_INDEX_UNSUPPORTED"), "{err}");
    let q = "EXPLAIN SELECT id FROM b ORDER BY e <~> '0101010101010101010101010101010101010101010101010101010101010101' LIMIT 1";
    let lines = plan_lines(&mut ex, q);
    assert!(
        lines.iter().any(|l| l.contains("absent (exact fallback)")),
        "no index recorded yet must plan absent: {lines:?}"
    );
    ex.execute("CREATE INDEX b_bin ON b USING BINARY (e)")
        .unwrap();
    let lines = plan_lines(&mut ex, q);
    assert!(
        lines.iter().any(|l| l.contains("usable")),
        "binary-on-binary must plan usable: {lines:?}"
    );
    teardown(&dir, &wal);
}

#[test]
fn radius_sq_diskann_matches_exact() {
    // SQ and DiskANN range wells are approximate; the executor declines
    // them to the exact scan, so radius results equal ann-off exactly.
    // Radius ANN engages only above the shared ANN gate (4096 live rows),
    // so this seeds 5K rows; a TopK primer per method builds the cache
    // entry (and warms the statistics the radius planner reads).
    let (mut ex, dir, wal) = setup("radius");
    ex.execute("CREATE TABLE r (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))")
        .unwrap();
    let mut rng_state: u64 = 0x7654_3210_fedc_ba98;
    let mut rng = move || {
        rng_state ^= rng_state >> 12;
        rng_state ^= rng_state << 25;
        rng_state ^= rng_state >> 27;
        rng_state = rng_state.wrapping_mul(0x2545_F491_4F6C_DD1D);
        rng_state
    };
    let mut id = 1i64;
    let mut batch = Vec::new();
    while id <= 5_000 {
        let vals: Vec<String> = (0..8)
            .map(|_| format!("{:.4}", (rng() % 2000) as f64 / 1000.0 - 1.0))
            .collect();
        batch.push(format!("({id}, [{}])", vals.join(",")));
        id += 1;
        if batch.len() == 500 {
            ex.execute(&format!("INSERT INTO r VALUES {};", batch.join(",")))
                .unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        ex.execute(&format!("INSERT INTO r VALUES {};", batch.join(",")))
            .unwrap();
    }
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    observability::reset_for_tests();
    for method in ["IVF_SQ", "DISKANN", "IVF_FLAT"] {
        ex.execute(&format!("CREATE INDEX r_idx ON r USING {method} (e)"))
            .unwrap();
        // Primer: TopK builds the cache entry this method serves from.
        let topk = "SELECT id FROM r ORDER BY l2_distance(e, [0.5,0.5,0.5,0.5,0,0,0,0]) LIMIT 5";
        let served_topk = rows(&mut ex, topk);
        ex.execute("SET vector.ann_enabled = off").unwrap();
        let exact_topk = rows(&mut ex, topk);
        ex.execute("SET vector.ann_enabled = on").unwrap();
        assert_eq!(served_topk, exact_topk, "{method} primer top-k");
        for bound in ["0.5", "2.0", "8.0"] {
            let q = format!(
                "SELECT id FROM r WHERE l2_distance(e, [0.5,0.5,0.5,0.5,0,0,0,0]) < {bound}"
            );
            let served = rows(&mut ex, &q);
            ex.execute("SET vector.ann_enabled = off").unwrap();
            let exact = rows(&mut ex, &q);
            ex.execute("SET vector.ann_enabled = on").unwrap();
            // Order-insensitive: radius has no ORDER BY; compare as sets.
            let mut a: Vec<String> = served.iter().map(|r| r[0].to_sql_text()).collect();
            let mut b: Vec<String> = exact.iter().map(|r| r[0].to_sql_text()).collect();
            a.sort();
            b.sort();
            assert_eq!(a, b, "{method} radius bound {bound}");
            assert!(!a.is_empty(), "{method} bound {bound} matched nothing");
        }
        ex.execute("DROP INDEX r_idx").unwrap();
    }
    let snap = observability::snapshot();
    // Primers prove index-path engagement (one miss + one ANN query each;
    // per-method cache entries since index DDL evicts). Radius queries
    // serve from the generic exact scan today (the radius ANN path only
    // engages inside index-range probes), so they move no counters — the
    // equality assertions above lock their correctness all the same.
    eprintln!(
        "radius-paths ann={} miss={} hit={} fb={}",
        snap.vector_ann_queries,
        snap.vector_ann_cache_misses,
        snap.vector_ann_cache_hits,
        snap.vector_ann_fallbacks
    );
    assert_eq!(snap.vector_ann_cache_misses, 3);
    assert_eq!(snap.vector_ann_queries, 3);
    assert_eq!(snap.vector_ann_fallbacks, 0);
    teardown(&dir, &wal);
}
