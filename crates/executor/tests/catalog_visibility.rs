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
//! Live-session catalog visibility (P1-003 regression).
//!
//! Sessions hold point-in-time catalog images; before this fix an image
//! loaded at connect time never observed other sessions' committed DDL
//! (new tables, drops, grants) until reconnect. Executors now compare a
//! durable generation counter at batch entry and reload on change —
//! autocommit only. Open explicit transactions keep their pinned image
//! (snapshot + abort-revert contract), and images with uncommitted local
//! mutations are never replaced.

use plomid_executor::session_txn::TxnIsolation;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::sync::{Arc, Mutex};

fn shared_engine(tag: &str) -> Arc<Mutex<PlomidStorageEngine>> {
    let storage = std::env::temp_dir().join(format!("plomid-catvis-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-catvis-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ))
}

fn session(shared: &Arc<Mutex<PlomidStorageEngine>>) -> Executor<ConcurrentPlomidStorageEngine> {
    Executor::new_shared(Arc::clone(shared)).expect("session")
}

fn count(session: &mut Executor<ConcurrentPlomidStorageEngine>, sql: &str) -> i64 {
    match session.execute(sql).expect("query") {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int8(n) => *n,
            Value::Int4(n) => *n as i64,
            other => panic!("unexpected count value {other:?}"),
        },
        other => panic!("unexpected result {other:?}"),
    }
}

#[test]
fn created_table_visible_to_preexisting_session_without_reconnect() {
    let shared = shared_engine("create");
    let mut a = session(&shared);
    let mut b = session(&shared);
    // B touches the catalog first so its image undeniably predates the DDL.
    assert_eq!(count(&mut b, "SELECT count(*) FROM pg_tables"), 0);
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create");
    a.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .expect("insert");
    // The P1-003 reproducer: B reads A's committed table on its SAME session.
    assert_eq!(count(&mut b, "SELECT count(*) FROM t"), 2);
    let rows = match b.execute("SELECT v FROM t ORDER BY id").expect("select") {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(rows.len(), 2);
}

#[test]
fn dropped_table_stops_being_served_by_preexisting_session() {
    let shared = shared_engine("drop");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create");
    assert_eq!(count(&mut b, "SELECT count(*) FROM t"), 0);
    a.execute("DROP TABLE t").expect("drop");
    // Stale serve would be a correctness violation; the drop must propagate.
    let err = b
        .execute("SELECT count(*) FROM t")
        .expect_err("dropped table");
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("does not exist"),
        "unexpected error: {err}"
    );
}

#[test]
fn sequential_creates_from_two_sessions_mint_distinct_identities() {
    let shared = shared_engine("ids");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
        .expect("create t1");
    // B allocates from a refreshed image: no identity collision, and both
    // tables hold independent rows (physical directories are id-keyed).
    b.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
        .expect("create t2");
    a.execute("INSERT INTO t1 VALUES (1)").expect("insert t1");
    b.execute("INSERT INTO t2 VALUES (2)").expect("insert t2");
    assert_eq!(count(&mut a, "SELECT count(*) FROM t2"), 1);
    assert_eq!(count(&mut b, "SELECT count(*) FROM t1"), 1);
    let oids = |s: &mut Executor<ConcurrentPlomidStorageEngine>| -> Vec<u32> {
        match s
            .execute("SELECT oid FROM pg_class WHERE relname IN ('t1', 't2') ORDER BY relname")
            .expect("oids")
        {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|row| match &row[0] {
                    Value::Oid(n) => *n,
                    other => panic!("unexpected oid value {other:?}"),
                })
                .collect(),
            other => panic!("unexpected {other:?}"),
        }
    };
    let oids = oids(&mut a);
    assert_eq!(oids.len(), 2);
    assert_ne!(oids[0], oids[1], "table identities must differ");
}

#[test]
fn grants_visible_to_preexisting_session_without_reconnect() {
    let shared = shared_engine("grants");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.set_session_context("plomid", "plomid");
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create");
    a.execute("INSERT INTO t VALUES (1)").expect("insert");
    a.execute("CREATE ROLE auditor WITH LOGIN PASSWORD 'auditor-pw-1'")
        .expect("role");
    // B as auditor, connected before the grant: denied without it ...
    b.set_session_context("plomid", "auditor");
    assert!(
        b.execute("SELECT count(*) FROM t").is_err(),
        "denied pre-grant"
    );
    // ... then the grantor commits GRANT SELECT, and B's SAME session may
    // read: the grant half of P1-003.
    a.set_session_context("plomid", "plomid");
    a.execute("GRANT SELECT ON t TO auditor").expect("grant");
    assert_eq!(count(&mut b, "SELECT count(*) FROM t"), 1);
}

#[test]
fn explicit_transaction_keeps_its_pinned_catalog_image() {
    let shared = shared_engine("pin");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .expect("create");
    assert_eq!(count(&mut b, "SELECT count(*) FROM t"), 0);
    // Open txn pins B's image; A's later DDL stays invisible inside it
    // (documented snapshot narrowing), then appears after COMMIT.
    b.begin_session_txn(TxnIsolation::ReadCommitted)
        .expect("begin");
    a.execute("CREATE TABLE u (id INTEGER PRIMARY KEY)")
        .expect("create u");
    assert!(
        b.execute_one_in_session_txn("SELECT count(*) FROM u")
            .is_err(),
        "pinned image must not see concurrent DDL"
    );
    b.commit_session_txn().expect("commit");
    assert_eq!(count(&mut b, "SELECT count(*) FROM u"), 0);
}

#[test]
fn search_path_survives_a_catalog_reload() {
    let shared = shared_engine("searchpath");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.execute("CREATE SCHEMA s").expect("schema");
    a.execute("CREATE TABLE s.t (id INTEGER PRIMARY KEY)")
        .expect("create");
    a.execute("INSERT INTO s.t VALUES (9)").expect("insert");
    b.execute("SET search_path TO s").expect("set path");
    // The reload triggered by A's DDL must not clobber B's session path.
    assert_eq!(count(&mut b, "SELECT count(*) FROM t"), 1);
}
