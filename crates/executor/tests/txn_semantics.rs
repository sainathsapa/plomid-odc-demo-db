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
//! True transaction staging tests (Beta 2 Phase 4): effects stage into the
//! write overlay at statement time, reads merge overlay + snapshot, COMMIT
//! only publishes. No SQL-text replay anywhere in these flows.

use plomid_executor::session_txn::TxnIsolation;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::sync::{Arc, Mutex};

fn open_shared(
    label: &str,
) -> (
    Arc<Mutex<PlomidStorageEngine>>,
    Executor<ConcurrentPlomidStorageEngine>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("plomid-txn4-{label}-{id}"));
    let wal = root.with_extension("wal");
    let _ = std::fs::remove_dir_all(&root);
    let engine = PlomidStorageEngine::create(&root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(&root, &wal, 64))
        .expect("open engine");
    let shared = Arc::new(Mutex::new(engine));
    let executor = Executor::new_shared(Arc::clone(&shared)).expect("session executor");
    (shared, executor)
}

fn exec(session: &mut Executor<ConcurrentPlomidStorageEngine>, sql: &str) -> QueryResult {
    session
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql} failed: {e:?}"))
}

fn exec_err(session: &mut Executor<ConcurrentPlomidStorageEngine>, sql: &str) -> String {
    match session.execute(sql) {
        Ok(result) => panic!("{sql} should have failed, got {result:?}"),
        Err(e) => format!("{e:?}"),
    }
}

fn begin(session: &mut Executor<ConcurrentPlomidStorageEngine>, isolation: TxnIsolation) {
    session.set_session_context("plomid", "plomid");
    session.begin_session_txn(isolation).expect("begin");
}

fn stmt(session: &mut Executor<ConcurrentPlomidStorageEngine>, sql: &str) -> QueryResult {
    session
        .execute_one_in_session_txn(sql)
        .unwrap_or_else(|e| panic!("{sql} failed: {e:?}"))
}

fn stmt_err(session: &mut Executor<ConcurrentPlomidStorageEngine>, sql: &str) -> String {
    match session.execute_one_in_session_txn(sql) {
        Ok(result) => panic!("{sql} should have failed, got {result:?}"),
        Err(e) => format!("{e:?}"),
    }
}

fn int_rows(result: &QueryResult) -> Vec<Vec<String>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        Value::Null => "NULL".to_string(),
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn staged_read_followed_by_staged_write() {
    let (_engine, mut db) = open_shared("ryw");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO t VALUES (1, 10)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    // Staged write then staged read: the read observes the write.
    stmt(&mut db, "INSERT INTO t VALUES (2, 20)");
    let rows = int_rows(&stmt(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 2, "read-your-writes: {rows:?}");
    // Staged read then staged write based on it.
    let total = int_rows(&stmt(&mut db, "SELECT SUM(v) FROM t"));
    assert_eq!(total.len(), 1);
    stmt(&mut db, "INSERT INTO t VALUES (3, 30)");
    let rows = int_rows(&stmt(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 3);
    db.commit_session_txn().expect("commit");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 3);
}

#[test]
fn update_delete_visible_before_commit() {
    let (_engine, mut db) = open_shared("updvis");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "UPDATE t SET v = v + 100 WHERE id = 1");
    stmt(&mut db, "DELETE FROM t WHERE id = 3");
    let rows = int_rows(&stmt(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(
        rows.len(),
        2,
        "updated + deleted visible pre-commit: {rows:?}"
    );
    assert!(rows[0].iter().any(|c| c.contains("110")), "{rows:?}");
    db.commit_session_txn().expect("commit");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 2);
}

#[test]
fn rollback_after_multiple_statements_changes_nothing() {
    let (_engine, mut db) = open_shared("rollback");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO t VALUES (1, 10)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t VALUES (2, 20)");
    stmt(&mut db, "UPDATE t SET v = 99 WHERE id = 1");
    stmt(&mut db, "DELETE FROM t WHERE id = 2");
    stmt(&mut db, "CREATE TABLE u (id INTEGER)");
    db.abort_session_txn().expect("rollback");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 1, "rollback discards everything: {rows:?}");
    // The DDL rolled back too (catalog image restored).
    let err = exec_err(&mut db, "SELECT * FROM u");
    assert!(
        err.contains("not found") || err.contains("NotFound"),
        "{err}"
    );
}

#[test]
fn constraint_violations_surface_at_statement_time() {
    let (_engine, mut db) = open_shared("constr");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO t VALUES (1, 10)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t VALUES (2, 20)");
    // Duplicate key fails HERE (statement time), not at COMMIT — and the
    // whole transaction aborts immediately (only ROLLBACK accepted after).
    let err = stmt_err(&mut db, "INSERT INTO t VALUES (1, 99)");
    assert!(
        err.contains("duplicate")
            || err.contains("Unique")
            || err.contains("unique")
            || err.contains("Conflict")
            || err.contains("conflict"),
        "constraint error at statement time: {err}"
    );
    // Transaction aborted: even a valid statement is refused now.
    let err = stmt_err(&mut db, "INSERT INTO t VALUES (3, 30)");
    assert!(!err.is_empty());
    db.abort_session_txn().expect("rollback recovery");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 1, "only the pre-txn row survives: {rows:?}");
}

#[test]
fn sequences_advance_outside_rollback() {
    let (_engine, mut db) = open_shared("seq");
    exec(&mut db, "CREATE TABLE t (id SERIAL PRIMARY KEY, v INTEGER)");
    exec(&mut db, "CREATE SEQUENCE s");
    begin(&mut db, TxnIsolation::ReadCommitted);
    // Identity allocation inside the transaction (statement-time).
    stmt(&mut db, "INSERT INTO t (v) VALUES (1)");
    stmt(&mut db, "INSERT INTO t (v) VALUES (2)");
    // SELECT nextval observes the shared sequence (independent commit —
    // advances never roll back, PostgreSQL semantics).
    let n1 = int_rows(&stmt(&mut db, "SELECT nextval('s')"));
    assert_eq!(n1.len(), 1);
    db.abort_session_txn().expect("rollback");
    // Standalone nextval keeps advancing past the rollback (durable).
    let n2 = int_rows(&exec(&mut db, "SELECT nextval('s')"));
    assert_ne!(n1[0][0], n2[0][0], "sequence advanced across rollback");
    // Rolled-back identity values are safely reusable (they never
    // committed, so no duplicate risk); the committed row takes id 1.
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t (v) VALUES (3)");
    db.commit_session_txn().expect("commit");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 1, "{rows:?}");
}

#[test]
fn defaults_evaluated_once_at_statement_time() {
    let (_engine, mut db) = open_shared("defaults");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 42)",
    );
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t (id) VALUES (1)");
    stmt(&mut db, "INSERT INTO t (id) VALUES (2)");
    db.commit_session_txn().expect("commit");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r[1].contains("42")), "{rows:?}");
}

#[test]
fn repeatable_read_pins_the_begin_snapshot() {
    let (shared, mut a) = open_shared("rr");
    exec(&mut a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&mut a, "INSERT INTO t VALUES (1, 10)");
    let mut b = Executor::new_shared(Arc::clone(&shared)).expect("second session");
    begin(&mut b, TxnIsolation::RepeatableRead);
    let before = int_rows(&stmt(&mut b, "SELECT * FROM t ORDER BY id"));
    assert_eq!(before.len(), 1);
    // Concurrent commit lands while B's transaction is open.
    exec(&mut a, "INSERT INTO t VALUES (2, 20)");
    // RR still sees exactly the BEGIN image (repeatable).
    let during = int_rows(&stmt(&mut b, "SELECT * FROM t ORDER BY id"));
    assert_eq!(during.len(), 1, "RR snapshot pinned: {during:?}");
    b.commit_session_txn().expect("commit");
    let after = int_rows(&exec(&mut b, "SELECT * FROM t ORDER BY id"));
    assert_eq!(after.len(), 2, "post-commit sees both: {after:?}");
}

#[test]
fn read_committed_sees_newly_committed_rows() {
    let (shared, mut a) = open_shared("rc");
    exec(&mut a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&mut a, "INSERT INTO t VALUES (1, 10)");
    let mut b = Executor::new_shared(Arc::clone(&shared)).expect("second session");
    begin(&mut b, TxnIsolation::ReadCommitted);
    assert_eq!(int_rows(&stmt(&mut b, "SELECT * FROM t")).len(), 1);
    exec(&mut a, "INSERT INTO t VALUES (2, 20)");
    // RC refreshes per statement: the new row appears mid-transaction.
    assert_eq!(int_rows(&stmt(&mut b, "SELECT * FROM t")).len(), 2);
    b.commit_session_txn().expect("commit");
}

#[test]
fn savepoint_partial_rollback() {
    let (_engine, mut db) = open_shared("savepoint");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t VALUES (1, 10)");
    db.savepoint_session_txn("sp1").expect("savepoint");
    stmt(&mut db, "INSERT INTO t VALUES (2, 20)");
    assert_eq!(int_rows(&stmt(&mut db, "SELECT * FROM t")).len(), 2);
    db.rollback_to_session_savepoint("sp1")
        .expect("rollback to");
    assert_eq!(int_rows(&stmt(&mut db, "SELECT * FROM t")).len(), 1);
    db.commit_session_txn().expect("commit");
    assert_eq!(int_rows(&exec(&mut db, "SELECT * FROM t")).len(), 1);
    // Unknown savepoints fail closed.
    begin(&mut db, TxnIsolation::ReadCommitted);
    let err = db
        .rollback_to_session_savepoint("nope")
        .expect_err("unknown savepoint");
    assert!(format!("{err:?}").contains("no such savepoint"));
    db.abort_session_txn().expect("cleanup");
}

#[test]
fn ddl_is_transactional() {
    let (_engine, mut db) = open_shared("ddltxn");
    exec(&mut db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "CREATE TABLE u (id INTEGER PRIMARY KEY)");
    stmt(&mut db, "INSERT INTO u VALUES (7)");
    assert_eq!(int_rows(&stmt(&mut db, "SELECT * FROM u")).len(), 1);
    db.commit_session_txn().expect("commit");
    // Both the catalog entry and the rows survived (persisted together).
    assert_eq!(int_rows(&exec(&mut db, "SELECT * FROM u")).len(), 1);
    // Drop + rollback restores everything.
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "DROP TABLE u");
    assert!(
        exec_err(&mut db, "SELECT * FROM u").contains("not found")
            || exec_err(&mut db, "SELECT * FROM u").contains("NotFound")
    );
    db.abort_session_txn().expect("rollback");
    assert_eq!(int_rows(&exec(&mut db, "SELECT * FROM u")).len(), 1);
}

#[test]
fn volatile_functions_evaluate_once_per_statement() {
    let (_engine, mut db) = open_shared("volatile");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, r DOUBLE PRECISION)",
    );
    begin(&mut db, TxnIsolation::ReadCommitted);
    // Two rows, one statement: each row gets its own random() draw...
    stmt(&mut db, "INSERT INTO t VALUES (1, random()), (2, random())");
    // ...but re-reading never re-evaluates: stored values are stable.
    let first = int_rows(&stmt(&mut db, "SELECT * FROM t ORDER BY id"));
    let second = int_rows(&stmt(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(first, second, "no re-evaluation on re-read");
    assert_eq!(first.len(), 2);
    db.commit_session_txn().expect("commit");
    let third = int_rows(&exec(&mut db, "SELECT * FROM t ORDER BY id"));
    assert_eq!(first, third, "committed values identical to staged reads");
}

#[test]
fn generated_columns_compute_at_statement_time() {
    let (_engine, mut db) = open_shared("generated");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER GENERATED ALWAYS AS (a * 2) STORED)",
    );
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO t (id, a) VALUES (1, 21)");
    let rows = int_rows(&stmt(&mut db, "SELECT a, b FROM t"));
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert!(
        rows[0][1].contains("42"),
        "generated at statement time: {rows:?}"
    );
    db.commit_session_txn().expect("commit");
}

#[test]
fn ddl_then_commit_does_not_self_deadlock() {
    // Regression: committing a transaction that dropped a schema used to
    // hang forever — the catalog save opened a fresh engine transaction
    // whose first write blocked on the write-lane gate the session itself
    // already held. The catalog delta now rides the session transaction.
    // Commit runs on a worker thread; recv_timeout keeps a regression
    // fail-fast instead of wedging CI.
    let (shared, mut db) = open_shared("ddlcommit");
    exec(&mut db, "CREATE SCHEMA s1");
    exec(
        &mut db,
        "CREATE TABLE s1.t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO s1.t VALUES (1, 2)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "DROP SCHEMA s1 CASCADE");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = db.commit_session_txn().map(|_| "committed".to_string());
        tx.send(format!("{outcome:?}")).expect("report");
    });
    let outcome = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("COMMIT after in-txn DROP SCHEMA must finish (no self-deadlock)");
    assert!(outcome.contains("committed"), "commit failed: {outcome}");
    // Catalog coherent afterwards: the schema is gone, and the names are
    // reusable (dirty marks cleared, no resurrection).
    let mut db2 = Executor::new_shared(shared).expect("second session");
    db2.set_session_context("plomid", "plomid");
    let missing = exec_err(&mut db2, "SELECT * FROM s1.t");
    assert!(
        missing.contains("does not exist") || missing.contains("not found"),
        "dropped table must not resolve: {missing}"
    );
    exec(&mut db2, "CREATE SCHEMA s1");
    exec(
        &mut db2,
        "CREATE TABLE s1.t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db2, "INSERT INTO s1.t VALUES (7, 8)");
    let rows = int_rows(&exec(&mut db2, "SELECT * FROM s1.t ORDER BY id"));
    assert_eq!(rows.len(), 1, "recreated table writable: {rows:?}");
}

#[test]
fn concurrent_conflicting_inserts_serialize_on_unique() {
    let (shared, mut a) = open_shared("concins");
    exec(&mut a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    begin(&mut a, TxnIsolation::ReadCommitted);
    stmt(&mut a, "INSERT INTO t VALUES (1, 100)");
    // A holds the unique reservation. B (empty-handed) BLOCKS on it
    // (PostgreSQL behavior) until A commits — then B's statement fails
    // with a duplicate-key error against the fresh image. Run B on a
    // thread; recv_timeout keeps a regression hang fail-fast, not stuck.
    let shared_b = Arc::clone(&shared);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut b = Executor::new_shared(shared_b).expect("second session");
        b.set_session_context("plomid", "plomid");
        b.begin_session_txn(TxnIsolation::ReadCommitted)
            .expect("begin B");
        let outcome = b.execute_one_in_session_txn("INSERT INTO t VALUES (1, 200)");
        let _ = b.abort_session_txn();
        tx.send(format!("{outcome:?}")).expect("report");
    });
    // Give B time to block on the reservation, then commit the winner.
    std::thread::sleep(std::time::Duration::from_millis(500));
    a.commit_session_txn().expect("commit winner");
    let outcome = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("B must finish after A commits (no indefinite block)");
    assert!(
        outcome.contains("duplicate")
            || outcome.contains("Duplicate")
            || outcome.contains("unique")
            || outcome.contains("Unique")
            || outcome.contains("conflict")
            || outcome.contains("Conflict"),
        "loser sees conflict after winner commits: {outcome}"
    );
    handle.join().expect("thread");
    let rows = int_rows(&exec(&mut a, "SELECT * FROM t"));
    assert_eq!(rows.len(), 1, "exactly one winner: {rows:?}");
    assert!(rows[0][1].contains("100"), "{rows:?}");
}

#[test]
fn concurrent_updates_do_not_lose_writes() {
    let (shared, mut a) = open_shared("concupd");
    exec(&mut a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    exec(&mut a, "INSERT INTO t VALUES (1, 0)");
    begin(&mut a, TxnIsolation::ReadCommitted);
    stmt(&mut a, "UPDATE t SET v = v + 10 WHERE id = 1");
    // B blocks on A's row lane, then re-reads fresh state after A commits
    // (lost-update protection) and applies on top — no error, no loss.
    let shared_b = Arc::clone(&shared);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let mut b = Executor::new_shared(shared_b).expect("second session");
        b.set_session_context("plomid", "plomid");
        b.begin_session_txn(TxnIsolation::ReadCommitted)
            .expect("begin B");
        let update = b.execute_one_in_session_txn("UPDATE t SET v = v + 100 WHERE id = 1");
        let commit = b.commit_session_txn();
        tx.send(format!("{update:?} :: {commit:?}"))
            .expect("report");
    });
    std::thread::sleep(std::time::Duration::from_millis(500));
    a.commit_session_txn().expect("commit A");
    let outcome = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("B must finish after A commits (no indefinite block)");
    assert!(outcome.contains("Ok"), "B proceeds after A: {outcome}");
    handle.join().expect("thread");
    let rows = int_rows(&exec(&mut a, "SELECT v FROM t WHERE id = 1"));
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0][0].contains("110"),
        "both increments applied, none lost: {rows:?}"
    );
}

#[test]
fn insert_select_sees_staged_rows() {
    let (_engine, mut db) = open_shared("insselect");
    exec(
        &mut db,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(
        &mut db,
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO src VALUES (1, 10)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO src VALUES (2, 20)");
    // The staged row (id=2) must be visible to the INSERT ... SELECT source.
    stmt(&mut db, "INSERT INTO dst SELECT * FROM src");
    let rows = int_rows(&stmt(&mut db, "SELECT * FROM dst ORDER BY id"));
    assert_eq!(rows.len(), 2, "staged source rows copied: {rows:?}");
    db.commit_session_txn().expect("commit");
}

#[test]
fn update_from_sees_staged_rows() {
    let (_engine, mut db) = open_shared("updfrom");
    exec(
        &mut db,
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, p INTEGER)",
    );
    exec(
        &mut db,
        "CREATE TABLE rates (id INTEGER PRIMARY KEY, r INTEGER)",
    );
    exec(&mut db, "INSERT INTO prices VALUES (1, 100)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO rates VALUES (1, 2)");
    stmt(
        &mut db,
        "UPDATE prices SET p = p * 2 FROM rates WHERE prices.id = rates.id",
    );
    let rows = int_rows(&stmt(&mut db, "SELECT p FROM prices WHERE id = 1"));
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].contains("200"), "staged rate applied: {rows:?}");
    db.commit_session_txn().expect("commit");
}

#[test]
fn joins_and_aggregates_over_staged_data() {
    let (_engine, mut db) = open_shared("joinagg");
    exec(
        &mut db,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    );
    exec(
        &mut db,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, uid INTEGER, amt INTEGER)",
    );
    exec(&mut db, "INSERT INTO users VALUES (1, 'a')");
    exec(&mut db, "INSERT INTO orders VALUES (10, 1, 100)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "INSERT INTO users VALUES (2, 'b')");
    stmt(
        &mut db,
        "INSERT INTO orders VALUES (11, 2, 200), (12, 2, 300)",
    );
    // Join across staged + committed rows.
    let rows = int_rows(
        &stmt(
            &mut db,
            "SELECT users.name, orders.amt FROM users JOIN orders ON orders.uid = users.id ORDER BY orders.amt",
        ),
    );
    assert_eq!(rows.len(), 3, "{rows:?}");
    // Aggregation over the staged image.
    let agg = int_rows(&stmt(
        &mut db,
        "SELECT users.name, SUM(orders.amt) FROM users JOIN orders ON orders.uid = users.id GROUP BY users.name ORDER BY users.name",
    ));
    assert_eq!(agg.len(), 2, "{agg:?}");
    db.commit_session_txn().expect("commit");
}

#[test]
fn truncate_in_txn_hides_then_persists() {
    let (_engine, mut db) = open_shared("trunc");
    exec(
        &mut db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)",
    );
    exec(&mut db, "INSERT INTO t VALUES (1, 1), (2, 2)");
    begin(&mut db, TxnIsolation::ReadCommitted);
    stmt(&mut db, "TRUNCATE t");
    assert_eq!(int_rows(&stmt(&mut db, "SELECT * FROM t")).len(), 0);
    stmt(&mut db, "INSERT INTO t VALUES (9, 9)");
    db.commit_session_txn().expect("commit");
    let rows = int_rows(&exec(&mut db, "SELECT * FROM t"));
    assert_eq!(rows.len(), 1, "{rows:?}");
}
