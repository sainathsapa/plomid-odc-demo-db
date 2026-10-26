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
//! Binary ANN lifecycle: write invalidation, cache eviction/rebuild,
//! engine reopen recovery, concurrent serving, and governance declines.
//! All assertions are behavioral (returned rows); the ANN cache is derived
//! state throughout — it can only accelerate, never invent or lose rows.

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
    let dir = std::env::temp_dir().join(format!(
        "plomid-vecdu-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let wal = std::env::temp_dir().join(format!(
        "plomid-vecdu-{tag}-wal-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
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

fn teardown(dir: &std::path::PathBuf, wal: &std::path::PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(wal);
}

fn seed_small(ex: &mut Executor<PlomidStorageEngine>) {
    ex.execute("CREATE TABLE b (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64))")
        .unwrap();
    ex.execute(
        "INSERT INTO b VALUES (1, '0101010101010101010101010101010101010101010101010101010101010101'), \
         (2, '0101010101010101010101010101010101010101010101010101010101010100'), \
         (3, '1111000011110000111100001111000011110000111100001111000011110000');",
    )
    .unwrap();
    ex.execute("CREATE INDEX b_bin ON b USING BINARY (e)")
        .unwrap();
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
}

fn hamming_query() -> &'static str {
    "SELECT id FROM b ORDER BY e <~> '0101010101010101010101010101010101010101010101010101010101010101' LIMIT 3"
}

#[test]
fn writes_invalidate_cache_correctly() {
    // Committed INSERT/UPDATE/DELETE bump the watermark and force a
    // rebuild: every read observes committed writes, never stale index rows.
    let (mut ex, dir, wal) = setup("writes");
    seed_small(&mut ex);
    let before = rows(&mut ex, hamming_query());
    assert_eq!(before.len(), 3);
    // INSERT a new nearest-or-equal row (duplicate of id 1 sorts by row id).
    ex.execute(
        "INSERT INTO b VALUES (4, '0101010101010101010101010101010101010101010101010101010101010101');",
    )
    .unwrap();
    let after_insert = rows(&mut ex, hamming_query());
    assert_eq!(after_insert.len(), 3);
    let ids: Vec<String> = after_insert.iter().map(|r| r[0].to_sql_text()).collect();
    assert!(ids.contains(&"4".to_string()), "new row visible: {ids:?}");
    // UPDATE id 3 into a near-duplicate: it must surface near the top.
    ex.execute(
        "UPDATE b SET e = '0101010101010101010101010101010101010101010101010101010101010101' WHERE id = 3;",
    )
    .unwrap();
    let after_update = rows(&mut ex, hamming_query());
    let ids: Vec<String> = after_update.iter().map(|r| r[0].to_sql_text()).collect();
    assert!(
        ids.contains(&"3".to_string()),
        "updated row visible: {ids:?}"
    );
    // DELETE the original nearest row: it must vanish.
    ex.execute("DELETE FROM b WHERE id = 1;").unwrap();
    let after_delete = rows(&mut ex, hamming_query());
    let ids: Vec<String> = after_delete.iter().map(|r| r[0].to_sql_text()).collect();
    assert!(!ids.contains(&"1".to_string()), "deleted row gone: {ids:?}");
    assert_eq!(after_delete.len(), 3);
    // Cross-check the final state against exact.
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, hamming_query());
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(after_delete, exact);
    teardown(&dir, &wal);
}

#[test]
fn tiny_cache_evicts_and_rebuilds_correctly() {
    // LRU eviction under a one-entry cache: alternating tables rebuild
    // from authoritative rows every time; results never degrade.
    let (mut ex, dir, wal) = setup("lru");
    seed_small(&mut ex);
    ex.execute("CREATE TABLE c (id BIGINT PRIMARY KEY, e VECTOR(BINARY, 64));")
        .unwrap();
    ex.execute(
        "INSERT INTO c VALUES (10, '1111111111111111111111111111111111111111111111111111111111111111');",
    )
    .unwrap();
    ex.execute("CREATE INDEX c_bin ON c USING BINARY (e)")
        .unwrap();
    ex.execute("SET vector.cache_graphs = 1").unwrap();
    let qb = hamming_query();
    let qc = "SELECT id FROM c ORDER BY e <~> '1111111111111111111111111111111111111111111111111111111111111111' LIMIT 1";
    for _ in 0..3 {
        let rb = rows(&mut ex, qb);
        assert_eq!(rb.len(), 3);
        assert_eq!(rb[0][0], Value::Int8(1));
        let rc = rows(&mut ex, qc);
        assert_eq!(rc.len(), 1);
        assert_eq!(rc[0][0], Value::Int8(10));
    }
    ex.execute("SET vector.cache_graphs = 8").unwrap();
    teardown(&dir, &wal);
}

#[test]
fn reopen_recovers_and_reserves_exact() {
    // Derived ANN state does not survive restart; the reopened engine
    // rebuilds from rows on demand and answers exactly.
    let (dir, wal) = {
        let (mut ex, dir, wal) = setup("reopen");
        seed_small(&mut ex);
        let before = rows(&mut ex, hamming_query());
        assert_eq!(before[0][0], Value::Int8(1));
        drop(ex);
        (dir, wal)
    };
    let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
    let mut ex = Executor::new(engine).unwrap();
    let after = rows(&mut ex, hamming_query());
    assert_eq!(after.len(), 3);
    assert_eq!(after[0][0], Value::Int8(1));
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, hamming_query());
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(after, exact);
    teardown(&dir, &wal);
}

#[test]
fn concurrent_hamming_queries_agree() {
    // Independent executors (own dirs) on threads share only the
    // process-global ANN cache: every thread observes correct rows.
    // (One shared Executor across threads is out of scope: Executor is
    // not Sync by design; engines serialize writers independently.)
    let handles: Vec<std::thread::JoinHandle<Vec<String>>> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                let (mut ex, dir, wal) = setup(&format!("conc{t}"));
                seed_small(&mut ex);
                let mut out = Vec::new();
                for _ in 0..5 {
                    let r = rows(&mut ex, hamming_query());
                    out.push(r[0][0].to_sql_text());
                }
                teardown(&dir, &wal);
                out
            })
        })
        .collect();
    for h in handles {
        let seen = h.join().expect("worker panicked");
        assert_eq!(seen.len(), 5);
        assert!(seen.iter().all(|id| id == "1"), "{seen:?}");
    }
}

#[test]
fn memory_cap_decline_stays_exact() {
    // Governance refusal (index-vector ceiling below the row count)
    // declines the ANN build and serves exactly: limits bound work, never
    // correctness.
    let (mut ex, dir, wal) = setup("memcap");
    seed_small(&mut ex);
    ex.execute("SET vector.max_index_vectors = 1").unwrap();
    let served = rows(&mut ex, hamming_query());
    ex.execute("SET vector.max_index_vectors = 1000000")
        .unwrap();
    ex.execute("SET vector.ann_enabled = off").unwrap();
    let exact = rows(&mut ex, hamming_query());
    ex.execute("SET vector.ann_enabled = on").unwrap();
    assert_eq!(served, exact);
    assert_eq!(served[0][0], Value::Int8(1));
    teardown(&dir, &wal);
}
