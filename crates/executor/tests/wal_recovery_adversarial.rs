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
//! Deterministic WAL/durability/recovery corners: idempotent re-replay,
//! uncommitted work absent after reopen, checkpoint-boundary exactness,
//! reservation release across restart, commit-order visibility after reopen.
//!
//! These simulate crash recovery deterministically (drop without graceful
//! shutdown, then reopen, then reopen again): the WAL contains whatever was
//! durable, and recovery must converge to the same committed state every
//! time. Timing-based kill -9 coverage lives in
//! benchmarks/production/phase-11/crash_matrix.py.
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_storage::{StorageEngine, StorageEngineTransaction};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type Session = Executor<ConcurrentPlomidStorageEngine>;

fn dir(tag: &str) -> (PathBuf, PathBuf) {
    let storage = std::env::temp_dir().join(format!("plomid-wal-rec-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-wal-rec-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    (storage, wal)
}

fn session(shared: &Arc<Mutex<PlomidStorageEngine>>) -> Session {
    Executor::new_shared(Arc::clone(shared)).expect("session")
}

fn scalar(session: &mut Session, sql: &str) -> i64 {
    match session.execute(sql).expect("scalar query") {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int8(n) => *n,
            Value::Int4(n) => *n as i64,
            other => panic!("unexpected scalar {other:?}"),
        },
        other => panic!("unexpected result {other:?}"),
    }
}

fn reopen(storage: &Path, wal: &Path) -> Arc<Mutex<PlomidStorageEngine>> {
    Arc::new(Mutex::new(
        PlomidStorageEngine::open(storage, wal, 64).expect("reopen"),
    ))
}

#[test]
fn replay_is_idempotent_across_repeated_reopens() {
    let (storage, wal) = dir("idempotent");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        for i in 0..100i64 {
            s.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 3))
                .expect("insert");
        }
        for i in (0..100i64).step_by(2) {
            s.execute(&format!("DELETE FROM t WHERE id = {i}"))
                .expect("delete");
        }
        // No graceful shutdown: drop everything mid-life, twice over.
    }
    for _ in 0..2 {
        let shared = reopen(&storage, &wal);
        let mut s = session(&shared);
        assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 50);
        assert_eq!(
            scalar(&mut s, "SELECT SUM(v) FROM t"),
            (1..100i64).step_by(2).map(|i| i * 3).sum::<i64>()
        );
        for i in (1..100).step_by(2) {
            assert_eq!(
                scalar(&mut s, &format!("SELECT v FROM t WHERE id = {i}")),
                i * 3,
                "value changed across re-replay id {i}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn uncommitted_update_and_delete_absent_after_reopen() {
    let (storage, wal) = dir("uncommitted");
    {
        let backing = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&backing);
        s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .expect("seed");
        // Stage an update and a delete on REAL row keys but never commit:
        // the facade buffers in memory and the WAL holds at most a Begin
        // record, so recovery must ignore them. Keys are discovered from a
        // committed scan so the staged operations target actual rows.
        let mut holder =
            ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing)).expect("holder");
        let mut staged = holder.begin().expect("begin");
        let keys: Vec<Vec<u8>> = staged
            .scan(None, None)
            .expect("scan keys")
            .into_iter()
            .map(|(k, _)| k)
            .take(2)
            .collect();
        assert_eq!(keys.len(), 2, "seed rows must exist");
        staged.put(&keys[0], b"SHOULD-NOT-APPEAR").expect("stage");
        staged.delete(&keys[1]).expect("stage");
        // Drop holder + engine without commit or abort: crash simulation.
    }
    let shared = reopen(&storage, &wal);
    let mut s = session(&shared);
    // Committed state intact; staged update/delete never happened.
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(scalar(&mut s, "SELECT v FROM t WHERE id = 2"), 20);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn checkpoint_boundary_reopen_is_exact() {
    let (storage, wal) = dir("ckptboundary");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        for i in 0..50i64 {
            s.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        // Force a checkpoint mid-history, then keep writing: recovery must
        // combine the checkpoint prefix with post-boundary replay exactly.
        shared
            .lock()
            .expect("lock")
            .checkpoint()
            .expect("checkpoint");
        for i in 50..100i64 {
            s.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                .expect("insert");
        }
        s.execute("UPDATE t SET v = v + 1000 WHERE id >= 90")
            .expect("update");
    }
    let shared = reopen(&storage, &wal);
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 100);
    assert_eq!(
        scalar(&mut s, "SELECT SUM(v) FROM t WHERE id < 90"),
        (0..90i64).sum::<i64>()
    );
    assert_eq!(
        scalar(&mut s, "SELECT SUM(v) FROM t WHERE id >= 90"),
        (90..100i64).map(|i| i + 1000).sum::<i64>()
    );
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn aborted_unique_key_reusable_after_reopen() {
    let (storage, wal) = dir("abortreopen");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, u BIGINT UNIQUE NOT NULL)")
            .expect("ddl");
        s.execute("BEGIN; INSERT INTO u VALUES (1, 42); ROLLBACK;")
            .expect("abort");
    }
    // The aborted reservation must not leak across restart: the key is free,
    // and genuine duplicates are still rejected after recovery.
    let shared = reopen(&storage, &wal);
    let mut s = session(&shared);
    s.execute("INSERT INTO u VALUES (1, 42)").expect("reinsert");
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM u"), 1);
    assert!(
        s.execute("INSERT INTO u VALUES (2, 42)").is_err(),
        "duplicate must still be rejected after reopen"
    );
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn commit_order_visible_after_reopen() {
    let (storage, wal) = dir("commitorder");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO m VALUES (1, 0)").expect("seed");
        for v in 1..=50i64 {
            s.execute(&format!("UPDATE m SET v = {v} WHERE id = 1"))
                .expect("update");
        }
    }
    let shared = reopen(&storage, &wal);
    let mut s = session(&shared);
    // Only the last committed version may be visible: no older version may
    // leak through recovery, and repeated reads agree.
    assert_eq!(scalar(&mut s, "SELECT v FROM m WHERE id = 1"), 50);
    assert_eq!(scalar(&mut s, "SELECT v FROM m WHERE id = 1"), 50);
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM m"), 1);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn oversized_commit_fails_fast_and_engine_stays_usable() {
    let (storage, wal) = dir("oversize");
    let backing = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let mut facade =
        ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing)).expect("facade");
    let mut txn = facade.begin().expect("begin");
    txn.put(b"small-key", b"small-value").expect("stage");
    txn.commit().expect("small commit");
    // Entries no page can hold are rejected BEFORE any WAL or timestamp side
    // effects (same client error as before, fail-fast instead of post-WAL).
    let big = vec![0xABu8; 40_000];
    let mut poison = facade.begin().expect("begin2");
    poison.put(b"big-key", &big).expect("stage-big");
    let err = poison.commit().expect_err("oversized commit must fail");
    assert!(
        format!("{err:?}").contains("too large"),
        "unexpected error: {err:?}"
    );
    // The failed commit must not wedge the engine: gates released, so an
    // unrelated transaction on the same domains commits immediately.
    let mut txn3 = facade.begin().expect("begin3");
    txn3.put(b"after-key", b"after-value").expect("stage3");
    txn3.commit().expect("commit after failure");
    drop(facade);
    // Recovery sees only the two small commits; the poison commit left no
    // durable trace for replay to trip over.
    let backing = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let mut facade = ConcurrentPlomidStorageEngine::from_shared(backing).expect("facade2");
    let mut txn = facade.begin().expect("begin4");
    assert_eq!(
        txn.get(b"small-key").expect("get"),
        Some(b"small-value".to_vec())
    );
    assert_eq!(
        txn.get(b"after-key").expect("get"),
        Some(b"after-value".to_vec())
    );
    assert_eq!(txn.get(b"big-key").expect("get"), None);
    txn.commit().expect("commit4");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn failed_commits_leave_no_active_transaction_pinning_snapshots() {
    let (storage, wal) = dir("noactiveleak");
    let backing = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let active = || {
        backing
            .lock()
            .expect("lock")
            .manager()
            .active_txns()
            .expect("active")
            .len()
    };
    assert_eq!(active(), 0);
    let big = vec![0xABu8; 40_000];
    for _ in 0..5 {
        let mut facade =
            ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing)).expect("facade");
        let mut txn = facade.begin().expect("begin");
        assert_eq!(active(), 1);
        txn.put(b"big-key", &big).expect("stage");
        txn.commit().expect_err("oversized commit must fail");
        drop(facade);
        drop(txn);
        assert_eq!(active(), 0, "failed commit leaked an active transaction");
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
