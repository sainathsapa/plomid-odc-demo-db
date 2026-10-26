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
//! Vector durability convergence: index payloads are derived artifacts.
//!
//! Authority rule under test (`plomid_vector::durability`): authoritative
//! vector values live in storage-engine rows (txn/MVCC/WAL/generations);
//! ANN graphs and DiskANN segments are non-authoritative derived caches
//! gated by freshness; ANN is acceleration only with exact fallback.
//!
//! Coverage:
//!
//! * both commit orders (`ROW COMMIT → INDEX UPDATE` and
//!   `INDEX BUILD → ROW COMMIT`) converge to the same visible state;
//! * kill (drop without graceful shutdown, plus torn/corrupt derived files)
//!   during row WAL commit, index write, index publish, generation publish,
//!   checkpoint, `VACUUM`, and rebuild — then reopen, compare against a
//!   brute-force oracle, verify exact fallback, rebuild, and compare again;
//! * rebuild gate: delete/corrupt derived ANN artifacts → rows remain valid,
//!   exact stays correct, ANN rebuilds, no data loss;
//! * backup/restore through the storage-engine boundary (directory copy
//!   after checkpoint, derived cache excluded) and GC (`VACUUM`) preserve
//!   correctness;
//! * EXPLAIN still reports the vector plan (behavior preserved).
//!
//! Crash-memory-loss note: a real kill also wipes the in-memory ANN graph
//! cache. Inside one test process that cache survives a drop/reopen, so
//! tests issue `REINDEX` (which drops every cached graph) after planting a
//! torn derived file — the same state a fresh process boots into.
//!
//! Tie-free data: every lane sits on the first axis (`[i, 0, 0, 0]`, patched
//! lanes likewise axis-aligned and distinct), so every distance to the query
//! is unique and ordering never depends on scan-position tie-breaks.

use plomid_executor::{derived_cache_dir, Executor};
use plomid_sql::{QueryResult, Value};
use plomid_txn::{PlomidStorageEngine, StorageEngine};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> (PathBuf, PathBuf) {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let storage =
        std::env::temp_dir().join(format!("plomid-vec-conv-{tag}-{id}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-vec-conv-{tag}-{id}-wal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    (storage, wal)
}

fn cleanup(storage: &Path, wal: &Path) {
    let _ = std::fs::remove_dir_all(storage);
    let _ = std::fs::remove_dir_all(wal);
}

fn open(storage: &Path, wal: &Path) -> Executor<PlomidStorageEngine> {
    let engine = PlomidStorageEngine::open(storage, wal, 32).expect("open engine");
    let mut ex = Executor::new(engine).expect("executor");
    // Force the ANN path even for small tables; every test in this file
    // agrees on this value, so concurrent sets are benign.
    ex.execute("SET vector.auto_ann_min_rows = 0")
        .expect("ann threshold");
    ex
}

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

fn exec(ex: &mut Executor<PlomidStorageEngine>, sql: &str) {
    ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}"));
}

fn ids(cell: &[Vec<Value>]) -> Vec<i64> {
    cell.iter()
        .map(|r| match r[0] {
            Value::Int8(n) => n,
            Value::Int4(n) => n as i64,
            ref v => panic!("expected int id, got {v:?}"),
        })
        .collect()
}

const QUERY: [f32; 4] = [0.25, 0.0, 0.0, 0.0];
const QUERY_SQL: &str = "[0.25,0,0,0]";

/// Base lane: first-axis position `i`, never zero-norm for `i >= 1`.
fn base_lane(i: i64) -> [f32; 4] {
    [i as f32, 0.0, 0.0, 0.0]
}

fn lane_sql(lane: &[f32; 4]) -> String {
    format!("[{},{},{},{}]", lane[0], lane[1], lane[2], lane[3])
}

/// Logical dataset: live ids plus per-id lane patches (UPDATEs).
#[derive(Debug, Default)]
struct Dataset {
    live: Vec<i64>,
    patch: HashMap<i64, [f32; 4]>,
}

impl Dataset {
    fn range(ids: std::ops::Range<i64>) -> Self {
        Self {
            live: ids.collect(),
            patch: HashMap::new(),
        }
    }

    fn lane(&self, id: i64) -> [f32; 4] {
        self.patch
            .get(&id)
            .copied()
            .unwrap_or_else(|| base_lane(id))
    }

    fn insert(&mut self, id: i64, lane: Option<[f32; 4]>) {
        self.live.push(id);
        if let Some(l) = lane {
            self.patch.insert(id, l);
        }
    }

    fn update(&mut self, id: i64, lane: [f32; 4]) {
        assert!(self.live.contains(&id), "update of non-live id {id}");
        self.patch.insert(id, lane);
    }

    fn delete(&mut self, id: i64) {
        self.live.retain(|i| *i != id);
        self.patch.remove(&id);
    }

    /// Brute-force oracle, `(distance asc, id asc)`. Panics on ties: test
    /// data must keep every distance distinct so scan-position tie-breaks
    /// can never matter.
    fn oracle(&self, k: usize) -> Vec<i64> {
        let mut scored: Vec<(f32, i64)> = self
            .live
            .iter()
            .map(|id| {
                let l = self.lane(*id);
                let d = l
                    .iter()
                    .zip(QUERY.iter())
                    .map(|(x, y)| (x - y) * (x - y))
                    .sum::<f32>()
                    .sqrt();
                (d, *id)
            })
            .collect();
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        for pair in scored.windows(2) {
            assert!(
                (pair[0].0 - pair[1].0).abs() > 1e-6,
                "tied distances {:?} and {:?}: test data must be tie-free",
                pair[0],
                pair[1]
            );
        }
        scored.truncate(k);
        scored.into_iter().map(|(_, id)| id).collect()
    }
}

fn create_table(ex: &mut Executor<PlomidStorageEngine>, table: &str) {
    exec(
        ex,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))"),
    );
}

fn insert_ids(ex: &mut Executor<PlomidStorageEngine>, table: &str, data: &Dataset, ids: &[i64]) {
    for chunk in ids.chunks(30) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| format!("({i}, {})", lane_sql(&data.lane(*i))))
            .collect();
        exec(
            ex,
            &format!("INSERT INTO {table} VALUES {};", values.join(",")),
        );
    }
}

fn ann_ids(ex: &mut Executor<PlomidStorageEngine>, table: &str, k: usize) -> Vec<i64> {
    ids(&rows(
        ex,
        &format!("SELECT id FROM {table} ORDER BY e <-> {QUERY_SQL} LIMIT {k}"),
    ))
}

fn use_diskann(ex: &mut Executor<PlomidStorageEngine>, table: &str, index: &str) {
    exec(ex, &format!("DROP INDEX {table}_e_vector_idx"));
    exec(
        ex,
        &format!("CREATE INDEX {index} ON {table} USING DISKANN (e)"),
    );
}

fn derived_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    walk(&root.join("vector_ann"), &mut out);
    out.sort();
    out
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir dst");
    for entry in std::fs::read_dir(src).expect("read src") {
        let entry = entry.expect("entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy file");
        }
    }
}

#[test]
fn row_commit_then_index_update_converges() {
    let (storage, wal) = scratch("row-then-index");
    let mut ex = open(&storage, &wal);
    // Unique table name: the in-memory ANN cache is process-global.
    let mut data = Dataset::range(1..61);
    create_table(&mut ex, "conv_row_first");
    insert_ids(
        &mut ex,
        "conv_row_first",
        &data,
        &(1..61).collect::<Vec<_>>(),
    );
    assert_eq!(ann_ids(&mut ex, "conv_row_first", 8), data.oracle(8));

    // ROW COMMIT → VECTOR INDEX UPDATE: commits invalidate derived graphs.
    data.insert(61, None);
    insert_ids(&mut ex, "conv_row_first", &data, &[61]);
    data.update(2, [1000.0, 0.0, 0.0, 0.0]);
    exec(
        &mut ex,
        "UPDATE conv_row_first SET e = [1000,0,0,0] WHERE id = 2",
    );
    data.delete(1);
    exec(&mut ex, "DELETE FROM conv_row_first WHERE id = 1");
    assert_eq!(ann_ids(&mut ex, "conv_row_first", 8), data.oracle(8));
    // Exact path (no index) agrees with the same oracle.
    exec(&mut ex, "DROP INDEX conv_row_first_e_vector_idx");
    assert_eq!(ann_ids(&mut ex, "conv_row_first", 8), data.oracle(8));

    cleanup(&storage, &wal);
}

#[test]
fn index_build_then_row_commit_converges() {
    let (storage, wal) = scratch("index-then-row");
    let mut ex = open(&storage, &wal);
    let mut data = Dataset::range(1..61);
    create_table(&mut ex, "conv_index_first");
    insert_ids(
        &mut ex,
        "conv_index_first",
        &data,
        &(1..61).collect::<Vec<_>>(),
    );
    // VECTOR INDEX BUILD first: warm the derived graph …
    assert_eq!(ann_ids(&mut ex, "conv_index_first", 8), data.oracle(8));
    // … then ROW COMMIT: the stale pre-commit graph must never serve.
    data.delete(1);
    exec(&mut ex, "DELETE FROM conv_index_first WHERE id = 1");
    data.insert(61, None);
    insert_ids(&mut ex, "conv_index_first", &data, &[61]);
    let got = ann_ids(&mut ex, "conv_index_first", 8);
    assert_eq!(got, data.oracle(8));
    assert_eq!(got[0], 2, "old nearest deleted: next row must lead");

    cleanup(&storage, &wal);
}

#[test]
fn kill_during_row_wal_commit_reopens_correct() {
    let (storage, wal) = scratch("kill-row-commit");
    let mut data = Dataset::range(1..81);
    {
        let mut ex = open(&storage, &wal);
        create_table(&mut ex, "conv_kill_row");
        insert_ids(
            &mut ex,
            "conv_kill_row",
            &data,
            &(1..81).collect::<Vec<_>>(),
        );
        assert_eq!(ann_ids(&mut ex, "conv_kill_row", 8), data.oracle(8));
        data.insert(81, None);
        insert_ids(&mut ex, "conv_kill_row", &data, &[81]);
        // Kill: drop without checkpoint or graceful shutdown.
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(ann_ids(&mut ex, "conv_kill_row", 9), data.oracle(9));
        // Exact fallback agrees after recovery.
        exec(&mut ex, "DROP INDEX conv_kill_row_e_vector_idx");
        assert_eq!(ann_ids(&mut ex, "conv_kill_row", 9), data.oracle(9));
    }
    cleanup(&storage, &wal);
}

#[test]
fn kill_during_index_write_rebuilds_from_rows() {
    let (storage, wal) = scratch("kill-index-write");
    let data = Dataset::range(1..121);
    let mut ex = open(&storage, &wal);
    create_table(&mut ex, "conv_kill_write");
    insert_ids(
        &mut ex,
        "conv_kill_write",
        &data,
        &(1..121).collect::<Vec<_>>(),
    );
    use_diskann(&mut ex, "conv_kill_write", "conv_kill_write_disk");
    assert_eq!(ann_ids(&mut ex, "conv_kill_write", 8), data.oracle(8));
    let root = ex.engine_mut().root().to_path_buf();
    assert!(
        !derived_files(&root).is_empty(),
        "diskann derived segments must exist under {root:?}"
    );

    // Kill during index write: torn vectors.dat.
    for file in derived_files(&root) {
        if file.file_name().is_some_and(|f| f == "vectors.dat") {
            std::fs::write(&file, b"torn").expect("plant torn write");
        }
    }
    // Fresh processes boot with no cached graphs; REINDEX is that state here.
    exec(&mut ex, "REINDEX");
    assert_eq!(
        ann_ids(&mut ex, "conv_kill_write", 8),
        data.oracle(8),
        "torn index write must fall back / rebuild, never serve wrong rows"
    );
    // Second query proves the derived cache healed itself.
    assert_eq!(ann_ids(&mut ex, "conv_kill_write", 8), data.oracle(8));
    cleanup(&storage, &wal);
}

#[test]
fn kill_during_index_publish_rebuilds_from_rows() {
    let (storage, wal) = scratch("kill-index-publish");
    let data = Dataset::range(1..121);
    let mut ex = open(&storage, &wal);
    create_table(&mut ex, "conv_kill_publish");
    insert_ids(
        &mut ex,
        "conv_kill_publish",
        &data,
        &(1..121).collect::<Vec<_>>(),
    );
    use_diskann(&mut ex, "conv_kill_publish", "conv_kill_publish_disk");
    assert_eq!(ann_ids(&mut ex, "conv_kill_publish", 8), data.oracle(8));

    // Kill during index publish: garbage graph.dat (torn rename target).
    let root = ex.engine_mut().root().to_path_buf();
    for file in derived_files(&root) {
        if file.file_name().is_some_and(|f| f == "graph.dat") {
            std::fs::write(&file, b"\x00\xffgarbage-graph").expect("plant torn publish");
        }
    }
    exec(&mut ex, "REINDEX");
    assert_eq!(
        ann_ids(&mut ex, "conv_kill_publish", 8),
        data.oracle(8),
        "torn index publish must never serve corruption"
    );
    cleanup(&storage, &wal);
}

#[test]
fn generation_publish_checkpoint_and_reopen_converge() {
    let (storage, wal) = scratch("gen-checkpoint");
    let data = Dataset::range(1..121);
    {
        let mut ex = open(&storage, &wal);
        create_table(&mut ex, "conv_gen");
        insert_ids(&mut ex, "conv_gen", &data, &(1..121).collect::<Vec<_>>());
        use_diskann(&mut ex, "conv_gen", "conv_gen_disk");
        assert_eq!(ann_ids(&mut ex, "conv_gen", 8), data.oracle(8));
        // Generation publish boundary: engine checkpoint, then kill.
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(ann_ids(&mut ex, "conv_gen", 8), data.oracle(8));
        // Re-replay converges identically (idempotent recovery).
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(ann_ids(&mut ex, "conv_gen", 8), data.oracle(8));
    }
    cleanup(&storage, &wal);
}

#[test]
fn vacuum_and_rebuild_matrix_preserves_correctness() {
    let (storage, wal) = scratch("vacuum-rebuild");
    let mut ex = open(&storage, &wal);
    let mut data = Dataset::range(1..81);
    create_table(&mut ex, "conv_maint");
    insert_ids(&mut ex, "conv_maint", &data, &(1..81).collect::<Vec<_>>());
    use_diskann(&mut ex, "conv_maint", "conv_maint_disk");
    assert_eq!(ann_ids(&mut ex, "conv_maint", 8), data.oracle(8));

    // VACUUM rewrites placement (GC authority: generations, not vector).
    exec(&mut ex, "VACUUM conv_maint");
    assert_eq!(ann_ids(&mut ex, "conv_maint", 8), data.oracle(8));

    // Deletes + VACUUM reclaim; ANN must track live rows only.
    for id in 1..11 {
        data.delete(id);
        exec(&mut ex, &format!("DELETE FROM conv_maint WHERE id = {id}"));
    }
    exec(&mut ex, "VACUUM conv_maint");
    assert_eq!(ann_ids(&mut ex, "conv_maint", 8), data.oracle(8));

    // REINDEX rebuilds derived state from rows.
    exec(&mut ex, "REINDEX");
    assert_eq!(ann_ids(&mut ex, "conv_maint", 8), data.oracle(8));

    // Kill between maintenance and reopen still converges.
    drop(ex);
    let mut ex = open(&storage, &wal);
    assert_eq!(ann_ids(&mut ex, "conv_maint", 8), data.oracle(8));
    cleanup(&storage, &wal);
}

#[test]
fn rebuild_gate_delete_and_corrupt_derived_artifacts() {
    let (storage, wal) = scratch("rebuild-gate");
    let mut ex = open(&storage, &wal);
    let data = Dataset::range(1..121);
    create_table(&mut ex, "conv_gate");
    insert_ids(&mut ex, "conv_gate", &data, &(1..121).collect::<Vec<_>>());
    use_diskann(&mut ex, "conv_gate", "conv_gate_disk");
    assert_eq!(ann_ids(&mut ex, "conv_gate", 8), data.oracle(8));
    let root = ex.engine_mut().root().to_path_buf();

    // Gate 1: delete every derived ANN artifact intentionally.
    let files = derived_files(&root);
    assert!(!files.is_empty(), "expected derived segments to delete");
    let _ = std::fs::remove_dir_all(root.join("vector_ann"));
    exec(&mut ex, "REINDEX");
    // Authoritative rows remain valid …
    assert_eq!(
        ids(&rows(&mut ex, "SELECT COUNT(*) FROM conv_gate"))[0],
        120
    );
    // … exact search remains correct (no index at all) …
    exec(&mut ex, "DROP INDEX conv_gate_disk");
    assert_eq!(ann_ids(&mut ex, "conv_gate", 8), data.oracle(8));
    // … and ANN rebuilds safely after re-creation. No data loss.
    exec(
        &mut ex,
        "CREATE INDEX conv_gate_disk2 ON conv_gate USING DISKANN (e)",
    );
    assert_eq!(ann_ids(&mut ex, "conv_gate", 8), data.oracle(8));

    // Gate 2: corrupt every derived file; same expectations, exercised
    // through the public derived-cache helper (which also drops the
    // in-memory graphs, like a fresh process boot).
    for file in derived_files(&root) {
        std::fs::write(&file, b"corrupt").expect("plant corruption");
    }
    let sample = derived_files(&root)
        .into_iter()
        .next()
        .expect("derived file");
    // …/vector_ann/<db>/<table>/<column>/<file> → recover the key.
    let column_dir = sample.parent().expect("column dir");
    let column = column_dir
        .file_name()
        .expect("column")
        .to_string_lossy()
        .into_owned();
    let table_dir = column_dir.parent().expect("table dir");
    let table = table_dir
        .file_name()
        .expect("table")
        .to_string_lossy()
        .into_owned();
    let db_dir = table_dir.parent().expect("db dir");
    let database = db_dir
        .file_name()
        .expect("db")
        .to_string_lossy()
        .into_owned();
    plomid_executor::destroy_derived_cache(&root, &database, &table, &column);
    assert_eq!(ann_ids(&mut ex, "conv_gate", 8), data.oracle(8));
    assert_eq!(
        ids(&rows(&mut ex, "SELECT COUNT(*) FROM conv_gate"))[0],
        120
    );
    cleanup(&storage, &wal);
}

#[test]
fn backup_restore_through_engine_boundary_excludes_derived_cache() {
    let (storage, wal) = scratch("backup-src");
    let data = Dataset::range(1..101);
    {
        let mut ex = open(&storage, &wal);
        create_table(&mut ex, "conv_backup");
        insert_ids(&mut ex, "conv_backup", &data, &(1..101).collect::<Vec<_>>());
        use_diskann(&mut ex, "conv_backup", "conv_backup_disk");
        assert_eq!(ann_ids(&mut ex, "conv_backup", 8), data.oracle(8));
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    // Backup = storage-engine boundary copy (the engine keeps WAL under
    // `<storage>/wal`; the separate wal dir arg is legacy). The derived
    // cache is excluded on purpose to prove restore rebuilds it from
    // authoritative rows.
    let backup = std::env::temp_dir().join(format!(
        "plomid-vec-conv-backup-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&backup);
    copy_dir(&storage, &backup);
    let _ = std::fs::remove_dir_all(backup.join("vector_ann"));

    // Restore into fresh directories and verify.
    let target = std::env::temp_dir().join(format!(
        "plomid-vec-conv-restored-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let target_wal = target.with_extension("wal");
    let _ = std::fs::remove_dir_all(&target);
    let _ = std::fs::remove_dir_all(&target_wal);
    copy_dir(&backup, &target);
    {
        let mut ex = open(&target, &target_wal);
        assert_eq!(ann_ids(&mut ex, "conv_backup", 8), data.oracle(8));
        // GC on the restored image preserves correctness too.
        exec(&mut ex, "VACUUM conv_backup");
        assert_eq!(ann_ids(&mut ex, "conv_backup", 8), data.oracle(8));
    }
    cleanup(&storage, &wal);
    let _ = std::fs::remove_dir_all(&backup);
    cleanup(&target, &target_wal);
}

#[test]
fn derived_cache_dir_is_sanitized_and_inside_root() {
    let root = Path::new("/tmp/plomid-root");
    let dir = derived_cache_dir(root, "plomid", "public.conv_gate", "e");
    assert!(dir.starts_with(root), "{dir:?} must stay inside the root");
    assert!(
        !dir.to_string_lossy().contains('.'),
        "identifiers must be sanitized: {dir:?}"
    );
}

#[test]
fn explain_behavior_preserved_for_vector_queries() {
    let (storage, wal) = scratch("explain");
    let mut ex = open(&storage, &wal);
    let data = Dataset::range(1..21);
    create_table(&mut ex, "conv_explain");
    insert_ids(&mut ex, "conv_explain", &data, &(1..21).collect::<Vec<_>>());
    let plan = rows(
        &mut ex,
        "EXPLAIN SELECT id FROM conv_explain ORDER BY e <-> [0.25,0,0,0] LIMIT 2",
    );
    let text: Vec<String> = plan
        .iter()
        .map(|r| match &r[0] {
            Value::Text(s) => s.clone(),
            v => v.to_sql_text(),
        })
        .collect();
    assert!(
        text.iter().any(|l| l.contains("Vector")),
        "EXPLAIN must keep its vector section: {text:?}"
    );
    cleanup(&storage, &wal);
}
