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
//! Execution proof for every DDL-accepted dense method.
//!
//! Single test in its own binary: the ANN observability counters are
//! process-global, so nothing else may run alongside. For each accepted
//! method the first query must rebuild the cache (miss), the second must
//! reuse it (hit), no query may fall back to the exact scan, and the
//! served rows must equal the ANN-disabled exact scan on the same snapshot.

use plomid_core::observability;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

#[test]
fn every_accepted_dense_method_executes_and_reuses_cache() {
    let dir = std::env::temp_dir().join(format!("plomid-vec-exec-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vec-exec-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
    let mut ex = Executor::new(engine).unwrap();

    ex.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))")
        .unwrap();
    // Epsilon-distinct lanes per row: ANN-vs-exact agreement is order-exact,
    // not tie-timid. Dimension 8 keeps every method trainable (PQ's default
    // 8 segments divide it); the dim-4 rejection is asserted separately.
    let mut batch = Vec::new();
    for i in 0..64i64 {
        let a = (i % 4) as f64;
        let b = ((i * 3) % 7) as f64 * 0.01;
        let c = ((i * 5) % 11) as f64 * 0.001;
        let d = i as f64 * 1e-4;
        let e = ((i * 7) % 13) as f64 * 0.0007;
        let f = ((i * 11) % 5) as f64 * 0.002;
        let g = ((i * 13) % 3) as f64 * 0.003;
        let h = ((i * 17) % 9) as f64 * 0.0001;
        batch.push(format!("({i}, [{a},{b},{c},{d},{e},{f},{g},{h}])"));
    }
    ex.execute(&format!("INSERT INTO d VALUES {};", batch.join(",")))
        .unwrap();
    // Drop the auto HNSW: each method is exercised explicitly below.
    ex.execute("DROP INDEX d_e_vector_idx").unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();

    // Dimension precondition: the runtime builds PQ with 8 default segments,
    // so a dim-4 column must be refused at DDL, never recorded then doomed
    // to fall back.
    ex.execute("CREATE TABLE d4 (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .unwrap();
    let err = ex
        .execute("CREATE INDEX d4_pq ON d4 USING IVF_PQ (e)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("multiple of 8"),
        "dim-4 ivf_pq must be rejected naming the divisor rule; got {err}"
    );

    observability::reset_for_tests();
    let query =
        "SELECT id FROM d ORDER BY l2_distance(e, [1.0,0.02,0.003,0.004,0.0021,0.0003,0.0002,0.0001]) LIMIT 8";
    let mut methods = vec!["HNSW", "DISKANN", "IVF_FLAT", "IVF_SQ", "IVF_PQ"];
    for method in methods.drain(..) {
        ex.execute(&format!("CREATE INDEX d_{method} ON d USING {method} (e)"))
            .unwrap_or_else(|e| panic!("{method} must be accepted: {e}"));
        let first = rows(&mut ex, query);
        let second = rows(&mut ex, query);
        assert_eq!(
            first, second,
            "{method}: cached search must be deterministic"
        );
        assert_eq!(first.len(), 8, "{method}: top-k size");
        ex.execute("SET vector.ann_enabled = off").unwrap();
        let exact = rows(&mut ex, query);
        ex.execute("SET vector.ann_enabled = on").unwrap();
        // Full coverage here (oversample 8 x k=8 >= 64 rows), so the
        // accelerated path must reproduce the exact ordering identically.
        assert_eq!(
            first, exact,
            "{method}: ANN must equal exact with full coverage"
        );
        ex.execute(&format!("DROP INDEX d_{method}")).unwrap();
    }
    let snap = observability::snapshot();
    assert_eq!(snap.vector_ann_cache_misses, 5, "{snap:?}");
    assert_eq!(snap.vector_ann_cache_hits, 5, "{snap:?}");
    assert_eq!(snap.vector_ann_queries, 10, "{snap:?}");
    assert_eq!(
        snap.vector_ann_fallbacks, 0,
        "no accepted dense method may fall back: {snap:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
