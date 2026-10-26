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
//! Adversarial MVCC + concurrency correctness: barrier-coordinated races
//! over shared-engine sessions pinning the SQL-visible contract.
//!
//! Coverage: concurrent INSERT / UPDATE / DELETE / SELECT, range scan +
//! UPDATE, range scan + DELETE, writer + reader monotonicity, staged-but-
//! uncommitted invisibility (engine level), abort freeing unique keys,
//! maintenance (auto-checkpoint) under write load, N×M reader/writer stress.
//! Every statement is autocommit (one snapshot per statement); assertions
//! therefore target per-statement recency/consistency, never cross-statement
//! snapshot stability (which the engine does not promise outside an explicit
//! transaction batch).
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_storage::{StorageEngine, StorageEngineTransaction};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex};

type Session = Executor<ConcurrentPlomidStorageEngine>;

fn dir(tag: &str) -> (PathBuf, PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-mvcc-adv-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-mvcc-adv-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    (storage, wal)
}

fn shared_engine(tag: &str) -> Arc<Mutex<PlomidStorageEngine>> {
    let (storage, wal) = dir(tag);
    Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ))
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

fn ids(session: &mut Session, sql: &str) -> Vec<i64> {
    match session.execute(sql).expect("ids query") {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                Value::Int8(n) => *n,
                Value::Int4(n) => *n as i64,
                other => panic!("unexpected id {other:?}"),
            })
            .collect(),
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn concurrent_single_row_increments_are_exact() {
    let shared = shared_engine("incr");
    let mut s = session(&shared);
    s.execute("CREATE TABLE z (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO z VALUES (1, 0)").expect("seed");
    let threads: i64 = 8;
    let per_thread: i64 = 25;
    let barrier = Arc::new(Barrier::new(threads as usize));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            for _ in 0..per_thread {
                s.execute("UPDATE z SET v = v + 1 WHERE id = 1")
                    .expect("increment");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    let mut verify = session(&shared);
    assert_eq!(
        scalar(&mut verify, "SELECT v FROM z WHERE id = 1"),
        threads * per_thread,
        "single-statement read-modify-write must be atomic"
    );
}

#[test]
fn concurrent_disjoint_range_updates_all_visible() {
    let shared = shared_engine("disjoint");
    let mut s = session(&shared);
    s.execute("CREATE TABLE d (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    let threads = 8i64;
    let per_thread = 50i64;
    let mut seed = String::from("INSERT INTO d VALUES ");
    for w in 0..threads {
        for i in 0..per_thread {
            let id = w * 1000 + i;
            seed.push_str(&format!("({id}, 0), "));
        }
    }
    seed.truncate(seed.len() - 2);
    s.execute(&seed).expect("seed");
    let barrier = Arc::new(Barrier::new(threads as usize));
    let mut handles = Vec::new();
    for w in 0..threads {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            for i in 0..per_thread {
                let id = w * 1000 + i;
                s.execute(&format!("UPDATE d SET v = {id} WHERE id = {id}"))
                    .expect("update");
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    let mut verify = session(&shared);
    assert_eq!(
        scalar(&mut verify, "SELECT COUNT(*) FROM d WHERE v = id"),
        threads * per_thread
    );
}

#[test]
fn delete_commits_become_visible_to_later_statements() {
    let shared = shared_engine("delvis");
    let mut s = session(&shared);
    s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO t VALUES (7, 70)").expect("seed");
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t WHERE id = 7"), 1);
    s.execute("DELETE FROM t WHERE id = 7").expect("delete");
    // A later statement (fresh snapshot) must observe the committed delete.
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t WHERE id = 7"), 0);
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 0);
    s.execute("INSERT INTO t VALUES (7, 71)").expect("reinsert");
    assert_eq!(scalar(&mut s, "SELECT v FROM t WHERE id = 7"), 71);
}

#[test]
fn update_commits_become_visible_to_later_statements() {
    let shared = shared_engine("updvis");
    let mut s = session(&shared);
    s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO t VALUES (9, 1)").expect("seed");
    s.execute("UPDATE t SET v = 2 WHERE id = 9")
        .expect("update");
    assert_eq!(scalar(&mut s, "SELECT v FROM t WHERE id = 9"), 2);
}

#[test]
fn racy_delete_reinsert_same_key_converges() {
    for round in 0..30 {
        let shared = shared_engine(&format!("race-{round}"));
        let mut s = session(&shared);
        s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO t VALUES (1, 100)").expect("seed");
        let barrier = Arc::new(Barrier::new(2));
        let (shared_d, barrier_d) = (Arc::clone(&shared), Arc::clone(&barrier));
        let deleter = std::thread::spawn(move || {
            let mut s = session(&shared_d);
            barrier_d.wait();
            s.execute("DELETE FROM t WHERE id = 1").expect("delete");
        });
        let (shared_i, barrier_i) = (Arc::clone(&shared), Arc::clone(&barrier));
        let inserter = std::thread::spawn(move || {
            let mut s = session(&shared_i);
            barrier_i.wait();
            // May legitimately fail with duplicate-key when the delete has
            // not committed yet at this statement's snapshot.
            let _ = s.execute("INSERT INTO t VALUES (1, 200)");
        });
        deleter.join().expect("deleter");
        inserter.join().expect("inserter");
        // Convergence: heap scan and PK probe must agree, and the survivor
        // (if any) carries a value actually written.
        let mut verify = session(&shared);
        let n = scalar(&mut verify, "SELECT COUNT(*) FROM t");
        let probe = scalar(&mut verify, "SELECT COUNT(*) FROM t WHERE id = 1");
        assert_eq!(n, probe, "heap/index disagreement round {round}");
        assert!(n <= 1, "duplicate PK surfaced round {round}");
        if n == 1 {
            let v = scalar(&mut verify, "SELECT v FROM t WHERE id = 1");
            assert!(v == 100 || v == 200, "torn value {v} round {round}");
        }
    }
}

#[test]
fn scans_during_concurrent_deletes_stay_consistent() {
    let shared = shared_engine("scandel");
    let mut s = session(&shared);
    s.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    let rows = 20_000i64;
    for lo in (0..rows).step_by(2000) {
        let hi = (lo + 2000).min(rows);
        let stmt = format!(
            "INSERT INTO t VALUES {}",
            (lo..hi)
                .map(|i| format!("({i}, {i})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        s.execute(&stmt).expect("seed batch");
    }
    // Spread victims across the key space (and across 1024-row scan chunks).
    let victims: Vec<i64> = (0..200).map(|k| k * 97).collect();
    assert!(victims.iter().all(|v| *v < rows));
    let barrier = Arc::new(Barrier::new(2));
    let (shared_d, barrier_d) = (Arc::clone(&shared), Arc::clone(&barrier));
    let victims_d = victims.clone();
    let deleter = std::thread::spawn(move || {
        let mut s = session(&shared_d);
        barrier_d.wait();
        for v in &victims_d {
            s.execute(&format!("DELETE FROM t WHERE id = {v}"))
                .expect("delete");
        }
    });
    let (shared_s, barrier_s) = (Arc::clone(&shared), Arc::clone(&barrier));
    let scanner = std::thread::spawn(move || {
        let mut s = session(&shared_s);
        barrier_s.wait();
        let mut observations = Vec::new();
        for _ in 0..5 {
            observations.push(ids(&mut s, "SELECT id FROM t ORDER BY id"));
        }
        observations
    });
    deleter.join().expect("deleter");
    let observations = scanner.join().expect("scanner");
    // Every scan must be internally consistent: sorted, duplicate-free, all
    // PKs in the seeded range with intact values (no torn rows).
    for (k, got) in observations.iter().enumerate() {
        let mut sorted = got.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(*got, sorted, "scan {k} has dup/out-of-order rows");
        assert!(
            got.iter().all(|id| (0..rows).contains(id)),
            "scan {k} returned out-of-range PK"
        );
        // Counts must lie between pre- and post-delete totals.
        assert!(
            (rows - 200) <= got.len() as i64 && got.len() as i64 <= rows,
            "scan {k} count {} outside [{}, {rows}]",
            got.len(),
            rows - 200
        );
    }
    // After the join, every delete is visible.
    let mut verify = session(&shared);
    assert_eq!(scalar(&mut verify, "SELECT COUNT(*) FROM t"), rows - 200);
    for v in &victims {
        assert_eq!(
            scalar(
                &mut verify,
                &format!("SELECT COUNT(*) FROM t WHERE id = {v}")
            ),
            0,
            "victim {v} still visible"
        );
    }
}

#[test]
fn readers_observe_monotonic_commits() {
    let shared = shared_engine("monotonic");
    let mut s = session(&shared);
    s.execute("CREATE TABLE m (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO m VALUES (1, 0)").expect("seed");
    let target = 150i64;
    let barrier = Arc::new(Barrier::new(2));
    let (shared_w, barrier_w) = (Arc::clone(&shared), Arc::clone(&barrier));
    let writer = std::thread::spawn(move || {
        let mut s = session(&shared_w);
        barrier_w.wait();
        for v in 1..=target {
            s.execute(&format!("UPDATE m SET v = {v} WHERE id = 1"))
                .expect("write");
        }
    });
    let (shared_r, barrier_r) = (Arc::clone(&shared), Arc::clone(&barrier));
    let reader = std::thread::spawn(move || {
        let mut s = session(&shared_r);
        barrier_r.wait();
        let mut seen = Vec::new();
        // Poll until the final commit is observed or a wall-clock deadline
        // passes: under parallel load the writer is arbitrarily slow, so a
        // fixed poll count lets a fast reader outrun it (harness timeout,
        // not a product defect — monotonicity below is the real assertion).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while *seen.last().unwrap_or(&0) != target {
            seen.push(scalar(&mut s, "SELECT v FROM m WHERE id = 1"));
            assert!(
                std::time::Instant::now() < deadline,
                "reader never observed final commit (last={})",
                seen.last().expect("seen")
            );
        }
        seen
    });
    writer.join().expect("writer");
    let seen = reader.join().expect("reader");
    // Visibility may skip intermediates but must never go backwards, and the
    // final value must be observed.
    for w in seen.windows(2) {
        assert!(w[0] <= w[1], "visibility went backwards: {w:?}");
    }
    assert_eq!(*seen.last().expect("seen"), target);
    assert!(seen.iter().all(|v| (0..=target).contains(v)));
}

#[test]
fn uncommitted_staged_writes_invisible_to_others() {
    let backing = shared_engine("uncomm");
    // Two independent facades over one shared engine: separate connections.
    let mut first =
        ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing)).expect("facade");
    let mut second =
        ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing)).expect("facade");
    let key = b"uncommitted-key".to_vec();
    let mut writer = first.begin().expect("begin");
    writer.put(&key, b"staged").expect("stage");
    // A concurrent transaction's snapshot must not see staged data, neither
    // by point read nor by scan.
    let mut reader = second.begin().expect("begin");
    assert_eq!(reader.get(&key).expect("read"), None);
    let found = reader
        .scan(Some(key.as_slice()), None)
        .expect("scan")
        .into_iter()
        .any(|(k, _)| k == key);
    assert!(!found, "staged key visible in scan");
    drop(reader);
    // After commit, a fresh transaction sees the value (commit ordering).
    writer.commit().expect("commit");
    let mut later = second.begin().expect("begin");
    assert_eq!(later.get(&key).expect("read"), Some(b"staged".to_vec()));
    drop(later);
}

#[test]
fn aborted_batch_frees_unique_key() {
    let shared = shared_engine("abortuniq");
    let mut s = session(&shared);
    s.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, u BIGINT UNIQUE NOT NULL)")
        .expect("ddl");
    s.execute("BEGIN; INSERT INTO u VALUES (1, 42); ROLLBACK;")
        .expect("abort batch");
    // The aborted reservation must not block, and genuine duplicates still do.
    s.execute("INSERT INTO u VALUES (1, 42)").expect("reinsert");
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM u"), 1);
    assert!(
        s.execute("INSERT INTO u VALUES (2, 42)").is_err(),
        "duplicate unique must still be rejected"
    );
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn checkpoint_and_scans_under_concurrent_write_load() {
    let shared = shared_engine("maintload");
    let mut s = session(&shared);
    s.execute("CREATE TABLE w (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    let writers = 4i64;
    let per_writer = 300i64;
    let barrier = Arc::new(Barrier::new((writers + 2) as usize));
    let mut handles = Vec::new();
    for w in 0..writers {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            for i in 0..per_writer {
                let id = w * 100_000 + i;
                s.execute(&format!("INSERT INTO w VALUES ({id}, {id})"))
                    .expect("insert");
            }
        }));
    }
    // Scanners run while auto-checkpoints fire on the commit path.
    for _ in 0..2 {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            for _ in 0..10 {
                let n = scalar(&mut s, "SELECT COUNT(*) FROM w");
                assert!((0..=writers * per_writer).contains(&n));
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    // Exact final state: heap count, PK probes, and value integrity.
    let mut verify = session(&shared);
    assert_eq!(
        scalar(&mut verify, "SELECT COUNT(*) FROM w"),
        writers * per_writer
    );
    for w in 0..writers {
        for i in [0, per_writer / 2, per_writer - 1] {
            let id = w * 100_000 + i;
            assert_eq!(
                scalar(&mut verify, &format!("SELECT v FROM w WHERE id = {id}")),
                id,
                "heap/index mismatch id {id}"
            );
        }
    }
}

#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn multi_reader_writer_stress_exact_final() {
    let shared = shared_engine("nxm");
    let mut s = session(&shared);
    s.execute("CREATE TABLE s (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    let writers = 4i64;
    let readers = 4i64;
    let per_writer = 100i64;
    let barrier = Arc::new(Barrier::new((writers + readers) as usize));
    let mut handles = Vec::new();
    for w in 0..writers {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            for i in 0..per_writer {
                let id = w * 10_000 + i;
                s.execute(&format!("INSERT INTO s VALUES ({id}, 1)"))
                    .expect("insert");
                // Interleaved reads of the shared table.
                let _ = scalar(&mut s, "SELECT COUNT(*) FROM s");
            }
        }));
    }
    for _ in 0..readers {
        let (shared, barrier) = (Arc::clone(&shared), Arc::clone(&barrier));
        handles.push(std::thread::spawn(move || {
            let mut s = session(&shared);
            barrier.wait();
            let mut lo = i64::MAX;
            let mut hi = i64::MIN;
            for _ in 0..100 {
                let n = scalar(&mut s, "SELECT COUNT(*) FROM s");
                lo = lo.min(n);
                hi = hi.max(n);
            }
            assert!(0 <= lo && hi <= writers * per_writer);
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    let mut verify = session(&shared);
    assert_eq!(
        scalar(&mut verify, "SELECT COUNT(*) FROM s"),
        writers * per_writer
    );
    assert_eq!(
        scalar(&mut verify, "SELECT COUNT(*) FROM s WHERE v != 1"),
        0
    );
}
