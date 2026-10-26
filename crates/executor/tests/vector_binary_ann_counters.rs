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
//! Binary ANN path proof via process-global observability counters.
//!
//! Own test binary (separate process) so concurrent vector tests in other
//! binaries cannot move the counters underfoot; exact deltas are asserted.

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
    let dir = std::env::temp_dir().join(format!("plomid-vecbinc-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vecbinc-{tag}-wal-{}", std::process::id()));
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
fn binary_ann_path_observed_via_counters() {
    // Proves the Hamming query actually serves from the Binary index (not the
    // exact scan): ANN counters move exactly once per query, the repeat hits
    // the cache, and Jaccard / ann-off queries move nothing.
    let (mut ex, dir, wal) = setup("counters");
    seed(&mut ex);
    observability::reset_for_tests();
    let hamming = format!("SELECT id FROM b ORDER BY e <~> '{}' LIMIT 4", bits(32));
    let jaccard = format!("SELECT id FROM b ORDER BY e <%> '{}' LIMIT 4", bits(32));

    let snap0 = observability::snapshot();
    let first = rows(&mut ex, &hamming);
    let snap1 = observability::snapshot();
    assert_eq!(snap1.vector_ann_queries - snap0.vector_ann_queries, 1);
    assert_eq!(snap1.vector_ann_fallbacks - snap0.vector_ann_fallbacks, 0);
    assert_eq!(
        snap1.vector_ann_cache_misses - snap0.vector_ann_cache_misses,
        1
    );

    // Identical repeat: same cache entry, served as a hit.
    let second = rows(&mut ex, &hamming);
    let snap2 = observability::snapshot();
    assert_eq!(first, second);
    assert_eq!(snap2.vector_ann_queries - snap1.vector_ann_queries, 1);
    assert_eq!(snap2.vector_ann_cache_hits - snap1.vector_ann_cache_hits, 1);

    // Jaccard: explicit exact fallback, no ANN counters move.
    let _ = rows(&mut ex, &jaccard);
    let snap3 = observability::snapshot();
    assert_eq!(snap3.vector_ann_queries, snap2.vector_ann_queries);
    assert_eq!(snap3.vector_ann_fallbacks, snap2.vector_ann_fallbacks);

    // Kill switch: ANN off serves exactly, counters frozen.
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let off = rows(&mut ex, &hamming);
    let snap4 = observability::snapshot();
    assert_eq!(off, first);
    assert_eq!(snap4.vector_ann_queries, snap3.vector_ann_queries);
    ex.execute("SET vector.ann_enabled = on").unwrap();
    teardown(&dir, &wal);
}
