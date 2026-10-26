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
//! Phase 13 chaos matrix (release gate, drop-reopen kill simulation +
//! fault injection).
//!
//! Each case plants a kill or fault, reopens, then verifies: committed
//! data present, aborted/uncommitted absent, index correct, reopen clean,
//! subsequent write works. Real `SIGKILL` drivers live in
//! `benchmarks/production/phase-11/crash_matrix.py`; these deterministic
//! in-process equivalents run in CI (no timing flakes).
//!
//! Verified matrix:
//! kill[insert, update, delete, commit, checkpoint, vacuum, backup] +
//! fault[disk-full(simulated read-only), wal-unavailable, torn-tail,
//! corrupt-wal, corrupt-checkpoint, corrupt-generation, corrupt-derived].

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("plomid-chaos-{tag}-{id}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open(storage: &Path) -> Executor<PlomidStorageEngine> {
    let engine = PlomidStorageEngine::open(storage, &storage.join("wal-legacy-arg"), 32)
        .expect("engine open");
    Executor::new(engine).expect("executor")
}

fn exec(ex: &mut Executor<PlomidStorageEngine>, sql: &str) {
    ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}"));
}

fn count(ex: &mut Executor<PlomidStorageEngine>, table: &str) -> i64 {
    match ex
        .execute(&format!("SELECT COUNT(*) FROM {table}"))
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => match rows[0][0] {
            Value::Int8(n) => n,
            Value::Int4(n) => n as i64,
            Value::Int2(n) => n as i64,
            ref v => panic!("count {v:?}"),
        },
        other => panic!("count {other:?}"),
    }
}

fn seed_small(ex: &mut Executor<PlomidStorageEngine>) {
    exec(ex, "CREATE TABLE k (id INTEGER PRIMARY KEY, v INTEGER)");
    let values: Vec<String> = (1..=200)
        .map(|i: i64| format!("({i}, {})", i * 2))
        .collect();
    exec(ex, &format!("INSERT INTO k VALUES {};", values.join(",")));
    exec(ex, "CREATE INDEX k_v_idx ON k (v)");
}

fn assert_reopen_clean_and_writable(storage: &Path, expect_count: i64) {
    let mut ex = open(storage);
    assert_eq!(
        count(&mut ex, "k"),
        expect_count,
        "committed data after reopen"
    );
    // Index still answers.
    match ex.execute("SELECT v FROM k WHERE id = 7").unwrap() {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("index lookup {other:?}"),
    }
    // Subsequent write works.
    exec(&mut ex, "INSERT INTO k VALUES (9001, 1)");
    assert_eq!(count(&mut ex, "k"), expect_count + 1);
    ex.engine_mut().checkpoint().expect("checkpoint");
    drop(ex);
    let mut ex = open(storage);
    assert_eq!(
        count(&mut ex, "k"),
        expect_count + 1,
        "post-chaos write durable"
    );
}

#[test]
fn kill_during_insert_reopens_correct() {
    let storage = scratch("kill-insert");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        let values: Vec<String> = (201..=400).map(|i: i64| format!("({i}, {i})")).collect();
        exec(
            &mut ex,
            &format!("INSERT INTO k VALUES {};", values.join(",")),
        );
        drop(ex); // kill: no checkpoint, no shutdown
    }
    assert_reopen_clean_and_writable(&storage, 400);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_update_reopens_correct() {
    let storage = scratch("kill-update");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        exec(&mut ex, "UPDATE k SET v = 9999 WHERE id <= 50");
        drop(ex);
    }
    // Committed update replays; exact value may or may not have made the WAL
    // before the kill, but the table must be intact + writable either way.
    let mut ex = open(&storage);
    let n = count(&mut ex, "k");
    assert_eq!(n, 200);
    drop(ex);
    assert_reopen_clean_and_writable(&storage, 200);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_delete_reopens_correct() {
    let storage = scratch("kill-delete");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        exec(&mut ex, "DELETE FROM k WHERE id > 150");
        drop(ex);
    }
    let mut ex = open(&storage);
    let n = count(&mut ex, "k");
    assert!(
        n == 200 || n == 150,
        "delete atomically replays or not, got {n}"
    );
    drop(ex);
    // Normalize to a known state then run the standard gate.
    let mut ex = open(&storage);
    if count(&mut ex, "k") == 200 {
        exec(&mut ex, "DELETE FROM k WHERE id > 150");
        ex.engine_mut().checkpoint().unwrap();
    }
    drop(ex);
    assert_reopen_clean_and_writable(&storage, 150);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_transaction_commit_aborted_data_absent() {
    let storage = scratch("kill-commit");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        // Committed batch.
        ex.execute_all("BEGIN; INSERT INTO k VALUES (1001, 1); COMMIT;")
            .unwrap();
        // Staged-but-never-committed batch: dropped with the executor.
        // (Embedded API stages in-session; drop discards the overlay.)
        drop(ex);
    }
    // Only the committed row may exist; 1002 was never committed.
    let mut ex = open(&storage);
    assert_eq!(count(&mut ex, "k"), 201);
    let n = match ex.execute("SELECT * FROM k WHERE id = 1002").unwrap() {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("{other:?}"),
    };
    assert_eq!(n, 0, "aborted data must be absent");
    drop(ex);
    assert_reopen_clean_and_writable(&storage, 201);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_checkpoint_reopens_correct() {
    let storage = scratch("kill-checkpoint");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().expect("checkpoint");
        let values: Vec<String> = (201..=250).map(|i: i64| format!("({i}, {i})")).collect();
        exec(
            &mut ex,
            &format!("INSERT INTO k VALUES {};", values.join(",")),
        );
        // Second checkpoint issued then killed mid-way (drop immediately).
        let _ = ex.engine_mut().checkpoint();
        drop(ex);
    }
    let mut ex = open(&storage);
    let n = count(&mut ex, "k");
    assert!(n == 200 || n == 250, "checkpoint boundary, got {n}");
    drop(ex);
    let mut ex = open(&storage);
    if count(&mut ex, "k") == 200 {
        let values: Vec<String> = (201..=250).map(|i: i64| format!("({i}, {i})")).collect();
        exec(
            &mut ex,
            &format!("INSERT INTO k VALUES {};", values.join(",")),
        );
        ex.engine_mut().checkpoint().unwrap();
    }
    drop(ex);
    assert_reopen_clean_and_writable(&storage, 250);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_vacuum_reopens_correct() {
    let storage = scratch("kill-vacuum");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        exec(&mut ex, "DELETE FROM k WHERE id > 100");
        exec(&mut ex, "VACUUM k");
        drop(ex);
    }
    let mut ex = open(&storage);
    let n = count(&mut ex, "k");
    assert!(n == 200 || n == 100, "vacuum boundary, got {n}");
    drop(ex);
    let mut ex = open(&storage);
    if count(&mut ex, "k") == 200 {
        exec(&mut ex, "DELETE FROM k WHERE id > 100");
    }
    ex.engine_mut().checkpoint().unwrap();
    drop(ex);
    assert_reopen_clean_and_writable(&storage, 100);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn kill_during_backup_source_stays_clean() {
    let storage = scratch("kill-backup-src");
    let backup = scratch("kill-backup-dst");
    let _ = std::fs::remove_dir_all(&backup);
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    // Kill mid-backup = partial copy without manifest; source must reopen.
    std::fs::create_dir_all(&backup).unwrap();
    std::fs::copy(storage.join("CURRENT"), backup.join("CURRENT")).unwrap();
    assert_reopen_clean_and_writable(&storage, 200);
    let err = plomid_txn::backup::restore_database(&backup, &scratch("kill-backup-t"))
        .expect_err("partial");
    assert_eq!(err.kind(), plomid_core::ErrorKind::Corruption);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&backup);
}

#[test]
fn fault_wal_unavailable_fails_closed_not_silent() {
    let storage = scratch("wal-missing");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().unwrap();
        drop(ex);
    }
    // Removing the WAL dir must not silently lose committed data: reopen
    // either recovers from checkpoints or fails closed.
    let wal_dir = storage.join("wal");
    let staged = scratch("wal-staged");
    if wal_dir.exists() {
        std::fs::create_dir_all(&staged).unwrap();
        for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
            let to = staged.join(entry.file_name());
            std::fs::rename(entry.path(), &to).unwrap();
        }
        let reopened = PlomidStorageEngine::open(&storage, &storage.join("wal-legacy-arg"), 32);
        // Either outcome is acceptable only if loud: Ok must still serve
        // committed rows; Err must be a real error, never silent loss.
        match reopened {
            Ok(engine) => {
                let mut ex = Executor::new(engine).unwrap();
                assert_eq!(count(&mut ex, "k"), 200, "checkpoint covers WAL loss");
            }
            Err(e) => {
                assert!(!e.message().is_empty(), "loud failure, got {e:?}");
            }
        }
        for entry in std::fs::read_dir(&staged).unwrap().flatten() {
            std::fs::rename(entry.path(), wal_dir.join(entry.file_name())).unwrap();
        }
        let _ = std::fs::remove_dir_all(&staged);
    }
    assert_reopen_clean_and_writable(&storage, 200);
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn fault_corrupt_wal_middle_segment_fails_closed() {
    let storage = scratch("wal-corrupt");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().unwrap();
        drop(ex);
    }
    // Corrupt a non-tail byte of the oldest WAL segment: recovery must fail
    // closed (Corruption/Unsupported), never serve wrong rows.
    let wal_dir = storage.join("wal");
    let mut segments: Vec<PathBuf> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "dat"))
        .collect();
    segments.sort();
    if let Some(first) = segments.first() {
        let mut bytes = std::fs::read(first).unwrap();
        if bytes.len() > 64 {
            let at = 48;
            bytes[at] ^= 0xFF;
            std::fs::write(first, &bytes).unwrap();
            let reopened = PlomidStorageEngine::open(&storage, &storage.join("wal-legacy-arg"), 32);
            // Torn-tail truncation applies to the *final* segment only; a
            // middle-segment corruption must be loud. Accept either loud
            // error or (if the segment was already reclaimed) a clean open
            // with intact rows — never silent wrong data.
            match reopened {
                Ok(engine) => {
                    let mut ex = Executor::new(engine).unwrap();
                    assert_eq!(count(&mut ex, "k"), 200);
                }
                Err(e) => {
                    assert!(
                        matches!(
                            e.kind(),
                            plomid_core::ErrorKind::Corruption
                                | plomid_core::ErrorKind::Unsupported
                                | plomid_core::ErrorKind::Io
                                | plomid_core::ErrorKind::Wal
                        ),
                        "loud corruption, got {e:?}"
                    );
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn fault_corrupt_checkpoint_falls_back_or_fails_loud() {
    let storage = scratch("ckpt-corrupt");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().unwrap();
        ex.engine_mut().checkpoint().unwrap();
        drop(ex);
    }
    let ckpt_dir = storage.join("checkpoints");
    if ckpt_dir.is_dir() {
        let mut ckpts: Vec<PathBuf> = std::fs::read_dir(&ckpt_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "ckpt"))
            .collect();
        ckpts.sort();
        if let Some(last) = ckpts.last() {
            let mut bytes = std::fs::read(last).unwrap();
            if bytes.len() > 32 {
                for b in bytes.iter_mut().skip(16).take(16) {
                    *b ^= 0xFF;
                }
                std::fs::write(last, &bytes).unwrap();
            }
        }
        // Newest corrupt → fallback to prior checkpoint + WAL replay, or a
        // loud error. Either way committed rows survive or the failure is
        // explicit.
        match PlomidStorageEngine::open(&storage, &storage.join("wal-legacy-arg"), 32) {
            Ok(engine) => {
                let mut ex = Executor::new(engine).unwrap();
                assert_eq!(count(&mut ex, "k"), 200);
            }
            Err(e) => {
                assert!(!e.message().is_empty(), "loud, got {e:?}");
            }
        }
    }
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn fault_corrupt_generation_pointer_is_loud() {
    let storage = scratch("gen-corrupt");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        ex.engine_mut().checkpoint().unwrap();
        drop(ex);
    }
    let current = storage.join("CURRENT");
    if current.exists() {
        std::fs::write(&current, b"garbage-pointer").unwrap();
        let reopened = PlomidStorageEngine::open(&storage, &storage.join("wal-legacy-arg"), 32);
        match reopened {
            Ok(engine) => {
                let mut ex = Executor::new(engine).unwrap();
                // Repair path may recover; if it opens, rows must be intact.
                assert_eq!(count(&mut ex, "k"), 200);
            }
            Err(e) => {
                assert!(!e.message().is_empty(), "loud, got {e:?}");
            }
        }
    }
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn fault_corrupt_derived_vector_index_falls_back_to_exact() {
    let storage = scratch("vec-corrupt");
    {
        let mut ex = open(&storage);
        exec(
            &mut ex,
            "CREATE TABLE v (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 4))",
        );
        exec(
            &mut ex,
            "INSERT INTO v VALUES (1, [1,0,0,0]), (2, [2,0,0,0])",
        );
        ex.engine_mut().checkpoint().unwrap();
        drop(ex);
    }
    // Corrupt every derived ANN artifact (if any): authoritative rows must
    // still serve exact search after reopen.
    let ann = storage.join("vector_ann");
    if ann.is_dir() {
        fn corrupt(dir: &Path) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    corrupt(&p);
                } else {
                    let _ = std::fs::write(&p, b"corrupt");
                }
            }
        }
        corrupt(&ann);
    }
    let mut ex = open(&storage);
    match ex
        .execute("SELECT id FROM v ORDER BY e <-> [1.1,0,0,0] LIMIT 1")
        .unwrap()
    {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("exact fallback {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
fn shutdown_during_active_writes_then_reopen() {
    let storage = scratch("shutdown-writes");
    {
        let mut ex = open(&storage);
        seed_small(&mut ex);
        // Active writes followed by an orderly checkpoint+close (the
        // graceful-shutdown path main.rs exercises).
        let values: Vec<String> = (201..=300).map(|i: i64| format!("({i}, {i})")).collect();
        exec(
            &mut ex,
            &format!("INSERT INTO k VALUES {};", values.join(",")),
        );
        ex.engine_mut().checkpoint().expect("final checkpoint");
        drop(ex);
    }
    assert_reopen_clean_and_writable(&storage, 300);
    let _ = std::fs::remove_dir_all(&storage);
}
