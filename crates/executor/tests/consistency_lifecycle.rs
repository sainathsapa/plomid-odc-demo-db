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
//! Persistence consistency lifecycle: seal/reopen, multi-database isolation,
//! repeated multi-batch inserts under composite UNIQUE, index/heap agreement.
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

type Session = Executor<ConcurrentPlomidStorageEngine>;

fn dir(tag: &str) -> (PathBuf, PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-cons-life-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-cons-life-{tag}-wal-{}", std::process::id()));
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

#[test]
fn repeated_multibatch_composite_unique_inserts_all_land() {
    let (storage, wal) = dir("compuniq");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let mut s = session(&shared);
    s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY, u BIGINT NOT NULL, s INTEGER NOT NULL, UNIQUE(u, s))")
        .expect("ddl");
    for b in 0..3i64 {
        let stmt = format!(
            "INSERT INTO t VALUES {}",
            (0..2000)
                .map(|i| {
                    let id = b * 2000 + i;
                    format!("({id},{id},{})", id % 5)
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
        s.execute(&stmt).expect("batch insert");
        assert_eq!(
            scalar(&mut s, "SELECT COUNT(*) FROM t"),
            (b + 1) * 2000,
            "batch {b} lost rows"
        );
    }
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 6000);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn seal_triggers_and_survives_reopen() {
    let (storage, wal) = dir("seal");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create_with_config(&storage, &wal, 32, 8192, 65536).expect("engine"),
    ));
    let mut s = session(&shared);
    s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    for lo in (0..500i64).step_by(100) {
        let stmt = format!(
            "INSERT INTO t VALUES {}",
            (lo..lo + 100)
                .map(|i| format!("({i}, {})", i * 3 + 7))
                .collect::<Vec<_>>()
                .join(", ")
        );
        s.execute(&stmt).expect("insert");
    }
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 500);
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT COUNT(*) FROM t"), 500);
    assert_eq!(
        scalar(&mut s, "SELECT COUNT(*) FROM t WHERE v != id*3+7"),
        0
    );
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn multi_database_isolation_and_durability() {
    // In-process USE validates existence but does not switch session state
    // (switching is a server-session behavior, covered by the persistence
    // audit script); tables share one global namespace here. This pins what the
    // engine layer guarantees: database identities persist across restarts,
    // USE validates them, and per-database tables are isolated by name.
    let (storage, wal) = dir("multidb");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE DATABASE dba").expect("create db a");
        s.execute("CREATE DATABASE dbb").expect("create db b");
        s.execute("USE dba").expect("use validates existing");
        assert!(s.execute("USE nosuchdb").is_err());
        s.execute("CREATE TABLE ta(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl a");
        s.execute("INSERT INTO ta VALUES (1, 111), (2, 222)")
            .expect("insert a");
        s.execute("CREATE TABLE tb(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl b");
        s.execute("INSERT INTO tb VALUES (1, 999)")
            .expect("insert b");
        assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM ta"), 333);
        assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM tb"), 999);
    }
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
        ));
        let mut s = session(&shared);
        // Database identities survived the restart.
        s.execute("USE dba").expect("dba persists");
        s.execute("USE dbb").expect("dbb persists");
        assert!(s.execute("USE nosuchdb").is_err());
        assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM ta"), 333);
        assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM tb"), 999);
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn index_heap_agreement_after_maintenance_and_reopen() {
    let (storage, wal) = dir("idxagree");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY, u BIGINT UNIQUE NOT NULL, g INTEGER NOT NULL, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("CREATE INDEX g_idx ON t(g)")
            .expect("secondary index");
        s.execute("CREATE UNIQUE INDEX uq ON t(v)")
            .expect("standalone unique");
        for lo in (0..2000i64).step_by(500) {
            let stmt = format!(
                "INSERT INTO t VALUES {}",
                (lo..lo + 500)
                    .map(|i| format!("({i},{},{},{})", 100000 + i, i % 50, i * 2))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            s.execute(&stmt).expect("insert");
        }
        s.execute("UPDATE t SET v = v + 1 WHERE id % 3 = 0")
            .expect("update");
        s.execute("DELETE FROM t WHERE id % 11 = 0")
            .expect("delete");
        s.execute("VACUUM t").expect("vacuum");
        // Per-index probe-vs-scan agreement on samples.
        for (col, base, modulus) in [("g", 0, 50), ("u", 100000, 2000), ("v", 0, 4000)] {
            for k in [0, 7, 42] {
                let val = base + (k * 13) % modulus;
                let probe = format!("SELECT COUNT(*) FROM t WHERE {col} = {val}");
                let scan =
                    format!("SELECT COUNT(*) FROM (SELECT {col} AS c FROM t) s WHERE c = {val}");
                assert_eq!(
                    scalar(&mut s, &probe),
                    scalar(&mut s, &scan),
                    "index/scan mismatch {col}={val}"
                );
            }
        }
    }
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
        ));
        let mut s = session(&shared);
        let n = scalar(&mut s, "SELECT COUNT(*) FROM t");
        assert_eq!(n, scalar(&mut s, "SELECT COUNT(*) FROM t WHERE id >= 0"));
        for (col, base, modulus) in [("g", 0, 50), ("u", 100000, 2000), ("v", 0, 4000)] {
            for k in [1, 17, 99] {
                let val = base + (k * 29) % modulus;
                let probe = format!("SELECT COUNT(*) FROM t WHERE {col} = {val}");
                let scan =
                    format!("SELECT COUNT(*) FROM (SELECT {col} AS c FROM t) s WHERE c = {val}");
                assert_eq!(
                    scalar(&mut s, &probe),
                    scalar(&mut s, &scan),
                    "post-reopen mismatch {col}={val}"
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn concurrent_ddl_from_stale_sessions_merges() {
    // Two sessions load the same image; each creates a table the other never
    // saw. Saves must merge (persist each session's own delta), never clobber:
    // a full-image rewrite here would delete the other session's table.
    let (storage, wal) = dir("ddlmerge");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.execute("CREATE TABLE t1(id BIGINT PRIMARY KEY)")
        .expect("a creates");
    b.execute("CREATE TABLE t2(id BIGINT PRIMARY KEY)")
        .expect("b creates");
    // A stale third session doing unrelated DDL must not disturb either.
    let mut c = session(&shared);
    c.execute("CREATE TABLE t3(id BIGINT PRIMARY KEY)")
        .expect("c creates");
    let mut verify = session(&shared);
    for t in ["t1", "t2", "t3"] {
        assert_eq!(
            scalar(&mut verify, &format!("SELECT COUNT(*) FROM {t}")),
            0,
            "{t} lost to a concurrent DDL save"
        );
    }
    drop(a);
    drop(b);
    drop(c);
    drop(verify);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let mut s = session(&shared);
    for t in ["t1", "t2", "t3"] {
        assert_eq!(
            scalar(&mut s, &format!("SELECT COUNT(*) FROM {t}")),
            0,
            "{t} lost across restart"
        );
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn dropped_table_stats_do_not_resurrect() {
    let (storage, wal) = dir("statdrop");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let mut s = session(&shared);
    s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert");
    s.execute("ANALYZE t").expect("analyze");
    assert!(s.analyzed_statistics("t").is_some());
    s.execute("DROP TABLE t").expect("drop");
    // Recreate empty under the same name: no stale stats may survive, in
    // memory or durably.
    s.execute("CREATE TABLE t(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("recreate");
    assert!(
        s.analyzed_statistics("t").is_none(),
        "dropped table stats leaked into the new table"
    );
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let s = session(&shared);
    assert!(
        s.analyzed_statistics("t").is_none(),
        "dropped table stats resurrected after restart"
    );
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
