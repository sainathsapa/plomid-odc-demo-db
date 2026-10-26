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
//! Cross-metric cache isolation: two metrics over one indexed column build
//! separate cache entries. Own test binary (separate process) so no other
//! vector test can move the process-global counters underfoot.

use plomid_core::observability;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn setup() -> (
    Executor<PlomidStorageEngine>,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let dir = std::env::temp_dir().join(format!("plomid-vecmi-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vecmi-wal-{}", std::process::id()));
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

#[test]
fn cross_metric_queries_build_separate_entries() {
    // Backends bake the metric into their structure; sharing an entry
    // across metrics would propose candidates under the wrong distance.
    // L2 then cosine over one HNSW column: both correct, two misses
    // (separate entries), and the repeats hit their own entries.
    let (mut ex, dir, wal) = setup();
    ex.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))")
        .unwrap();
    ex.execute(
        "INSERT INTO m VALUES (1, [1,0,0,0]), (2, [0.9,0.2,0,0]), (3, [0,1,0,0]), \
         (4, [0,0,1,0]), (5, [0.5,0.5,0,0]), (6, [0.2,0.2,0.9,0]);",
    )
    .unwrap();
    ex.execute("CREATE INDEX m_hnsw ON m USING HNSW (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    observability::reset_for_tests();
    let l2 = "SELECT id FROM m ORDER BY e <-> [1,0,0,0] LIMIT 3";
    let cos = "SELECT id FROM m ORDER BY e <=> [1,0,0,0] LIMIT 3";
    let l2_ann = rows(&mut ex, l2);
    let cos_ann = rows(&mut ex, cos);
    // Repeats: each metric hits its own entry.
    let l2_again = rows(&mut ex, l2);
    let cos_again = rows(&mut ex, cos);
    assert_eq!(l2_ann, l2_again);
    assert_eq!(cos_ann, cos_again);
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let l2_exact = rows(&mut ex, l2);
    let cos_exact = rows(&mut ex, cos);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(l2_ann, l2_exact, "L2 must match exact");
    assert_eq!(cos_ann, cos_exact, "cosine must match exact");
    let snap = observability::snapshot();
    assert_eq!(snap.vector_ann_queries, 4);
    assert_eq!(snap.vector_ann_cache_misses, 2, "one entry per metric");
    assert_eq!(snap.vector_ann_cache_hits, 2);
    assert_eq!(snap.vector_ann_fallbacks, 0);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
