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
//! Single lifecycle authority: mount, demount, shutdown, and recovery.
//!
//! Ownership: [`plomid_txn::PlomidStorageEngine`] is the single
//! mount/demount/recovery orchestrator — `create`/`open` mount (through the
//! checkpoint-aware `recover_storage` pipeline), `shutdown` demounts
//! (stop mutations → flush → checkpoint → sync, with an explicit stage
//! trace), and the recovery report of the opening mount stays observable.
//! The former `plomid-lifecycle` crate was removed as a redundant second
//! authority; its ordered-demount logic was merged into the engine, and the
//! server shutdown path calls `shutdown`.
//!
//! Coverage: clean startup, startup after crash, mount, demount (stage
//! order), shutdown gate (writes rejected, committed reads still resolve),
//! restart, crash-tail recovery (torn WAL tail truncated, never durable),
//! generation recovery (delete + VACUUM + restart), and WAL recovery
//! (uncheckpointed commits replayed).

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::{PlomidStorageEngine, ShutdownStage};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> (PathBuf, PathBuf) {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let storage = std::env::temp_dir().join(format!(
        "plomid-lifecycle-{tag}-{id}-{}",
        std::process::id()
    ));
    let wal = std::env::temp_dir().join(format!(
        "plomid-lifecycle-{tag}-{id}-wal-{}",
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
    let engine = PlomidStorageEngine::open(storage, wal, 32).expect("mount");
    Executor::new(engine).expect("executor")
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

fn count(ex: &mut Executor<PlomidStorageEngine>, table: &str) -> i64 {
    match rows(ex, &format!("SELECT COUNT(*) FROM {table}"))[0][0] {
        Value::Int8(n) => n,
        Value::Int4(n) => n as i64,
        ref v => panic!("unexpected count {v:?}"),
    }
}

const FULL_DEMOUNT_ORDER: [ShutdownStage; 5] = [
    ShutdownStage::StopMutations,
    ShutdownStage::Flush,
    ShutdownStage::Checkpoint,
    ShutdownStage::Sync,
    ShutdownStage::Close,
];

#[test]
fn clean_startup_mounts_and_serves() {
    let (storage, wal) = scratch("clean-startup");
    let mut ex = open(&storage, &wal);
    exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut ex, "INSERT INTO t VALUES (1, 10), (2, 20)");
    assert_eq!(count(&mut ex, "t"), 2);
    // Clean demount then mount again: state survives the cycle.
    let report = ex.engine_mut().shutdown().expect("shutdown");
    assert_eq!(report.stages, FULL_DEMOUNT_ORDER);
    assert!(report.checkpoint.is_some(), "shutdown must checkpoint");
    drop(ex);
    let mut ex = open(&storage, &wal);
    assert_eq!(count(&mut ex, "t"), 2);
    cleanup(&storage, &wal);
}

#[test]
fn shutdown_gates_new_transactions_but_reads_drain() {
    let (storage, wal) = scratch("shutdown-gate");
    let mut ex = open(&storage, &wal);
    exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
    exec(&mut ex, "INSERT INTO t VALUES (1, 10)");
    ex.engine_mut().shutdown().expect("shutdown");
    assert!(ex.engine_mut().is_shutdown());
    // New transactions are rejected after shutdown begins …
    assert!(
        ex.execute("INSERT INTO t VALUES (2, 20)").is_err(),
        "writes must be rejected after shutdown"
    );
    assert!(
        ex.execute("CREATE TABLE u (id BIGINT PRIMARY KEY)")
            .is_err(),
        "ddl must be rejected after shutdown"
    );
    // … while committed reads still resolve (drain-friendly demount) …
    assert_eq!(count(&mut ex, "t"), 1);
    // … and a second shutdown is rejected instead of running twice.
    assert!(ex.engine_mut().shutdown().is_err());
    drop(ex);
    // Reopening the root mounts a fresh running engine.
    let mut ex = open(&storage, &wal);
    assert!(!ex.engine_mut().is_shutdown());
    exec(&mut ex, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(count(&mut ex, "t"), 2);
    cleanup(&storage, &wal);
}

#[test]
fn restart_after_shutdown_preserves_data() {
    let (storage, wal) = scratch("restart");
    {
        let mut ex = open(&storage, &wal);
        exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
        for i in 0..50i64 {
            exec(&mut ex, &format!("INSERT INTO t VALUES ({i}, {})", i * 3));
        }
        ex.engine_mut().shutdown().expect("shutdown");
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(count(&mut ex, "t"), 50);
        match rows(&mut ex, "SELECT v FROM t WHERE id = 42")[0][0] {
            Value::Int8(126) | Value::Int4(126) => {}
            ref v => panic!("unexpected value {v:?}"),
        }
    }
    cleanup(&storage, &wal);
}

#[test]
fn startup_after_crash_recovers_uncheckpointed_wal() {
    let (storage, wal) = scratch("crash");
    {
        let mut ex = open(&storage, &wal);
        exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
        for i in 0..50i64 {
            exec(&mut ex, &format!("INSERT INTO t VALUES ({i}, {})", i * 3));
        }
        // Crash: drop with no shutdown and no explicit checkpoint.
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(count(&mut ex, "t"), 50);
        assert!(
            ex.engine_mut().recovery_report().is_some(),
            "opening mount must report recovery"
        );
        // Idempotent re-replay converges identically.
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(count(&mut ex, "t"), 50);
    }
    cleanup(&storage, &wal);
}

fn last_wal_segment(storage: &Path) -> PathBuf {
    let wal_dir = storage.join("wal");
    let mut segments: Vec<PathBuf> = std::fs::read_dir(&wal_dir)
        .expect("wal dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file())
        .collect();
    segments.sort();
    segments.pop().expect("wal segment exists")
}

#[test]
fn crash_tail_recovery_truncates_torn_tail() {
    let (storage, wal) = scratch("crash-tail");
    {
        let mut ex = open(&storage, &wal);
        exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
        exec(&mut ex, "INSERT INTO t VALUES (1, 10), (2, 20)");
        drop(ex);
    }
    // Torn tail: garbage after the last valid record is crash evidence,
    // never durable state.
    {
        let segment = last_wal_segment(&storage);
        let mut bytes = std::fs::read(&segment).expect("read segment");
        bytes.extend_from_slice(&[0xDE, 0xAD, 0x00]);
        std::fs::write(&segment, bytes).expect("plant torn tail");
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(count(&mut ex, "t"), 2, "committed rows survive torn tail");
        let report = ex
            .engine_mut()
            .recovery_report()
            .expect("recovery ran during mount");
        assert!(
            report.crash_tail_boundary.is_some(),
            "tail reported as crash boundary"
        );
    }
    cleanup(&storage, &wal);
}

#[test]
fn generation_recovery_after_vacuum_restart() {
    let (storage, wal) = scratch("generation");
    {
        let mut ex = open(&storage, &wal);
        exec(&mut ex, "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)");
        for i in 0..100i64 {
            exec(&mut ex, &format!("INSERT INTO t VALUES ({i}, {i})"));
        }
        for i in (0..100i64).step_by(2) {
            exec(&mut ex, &format!("DELETE FROM t WHERE id = {i}"));
        }
        // GC authority is generations/VACUUM: reclaim then restart.
        exec(&mut ex, "VACUUM t");
        ex.engine_mut().shutdown().expect("shutdown");
        drop(ex);
    }
    {
        let mut ex = open(&storage, &wal);
        assert_eq!(count(&mut ex, "t"), 50);
        assert_eq!(
            count(&mut ex, "t"),
            50,
            "generation GC + restart must preserve live rows"
        );
    }
    cleanup(&storage, &wal);
}
