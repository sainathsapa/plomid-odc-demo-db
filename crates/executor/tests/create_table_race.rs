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
//! F-004 — concurrent same-name `CREATE TABLE` must have exactly one winner.
//!
//! Sessions hold point-in-time catalog images: before the create gate, two
//! concurrent same-name creators both passed their in-memory existence
//! checks, both saved, and the later save silently overwrote the earlier
//! shape while both clients were told they succeeded. Creation now re-probes
//! the durable entry under the same process-wide lock `save_catalog` uses
//! and commits in the same critical section, so exactly one creator wins —
//! whether the race is decided by the gate (overlapping statements) or by
//! the live-session refresh (a late statement).
//!
//! Every assertion here holds for any interleaving: exactly one `CREATE`
//! reports success, the loser is told the table exists, the durable
//! definition is the winner's, and the losing client observes the surviving
//! definition on its next statement instead of a phantom shape.
use plomid_executor::session_txn::TxnIsolation;
use plomid_executor::Executor;
use plomid_sql::QueryResult;
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::path::PathBuf;
use std::sync::{Arc, Barrier, Mutex};

fn dir(tag: &str) -> (PathBuf, PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-namesake-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-namesake-{tag}-wal-{}", std::process::id()));
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

fn session(shared: &Arc<Mutex<PlomidStorageEngine>>) -> Executor<ConcurrentPlomidStorageEngine> {
    Executor::new_shared(Arc::clone(shared)).expect("session")
}

/// F-014 — concurrent first sessions on a fresh root must all succeed.
///
/// Session open materializes the shared default database/schema metadata when
/// it is missing. Publishers used to share one fixed `<name>.tmp`, so the
/// first rename made every other concurrent publisher fail with ENOENT.
#[test]
fn concurrent_first_sessions_on_a_fresh_root_all_succeed() {
    let (storage, wal) = dir("first-open");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    const SESSIONS: usize = 8;
    let barrier = Arc::new(Barrier::new(SESSIONS));
    let mut handles = Vec::new();
    for _ in 0..SESSIONS {
        let shared = Arc::clone(&shared);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || -> Result<(), String> {
            barrier.wait();
            let mut session = Executor::new_shared(shared).map_err(|error| format!("{error:?}"))?;
            session
                .execute("SELECT 1")
                .map(|_| ())
                .map_err(|error| format!("{error:?}"))
        }));
    }
    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("thread"))
        .collect();
    let failures: Vec<_> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    assert!(
        failures.is_empty(),
        "all concurrent first sessions must open: {failures:?}"
    );
}

/// The definition a session sees for `name` (column names of a bare scan).
fn columns_of(session: &mut Executor<ConcurrentPlomidStorageEngine>, name: &str) -> Vec<String> {
    match session
        .execute(&format!("SELECT * FROM {name}"))
        .expect("shape probe")
    {
        QueryResult::Rows { columns, .. } => columns,
        other => panic!("unexpected shape result {other:?}"),
    }
}

#[test]
fn concurrent_same_name_create_table_has_exactly_one_winner() {
    // Each round starts two sessions from the same generation (both load
    // before either executes) and releases them at a barrier. Whatever the
    // interleaving, exactly one `CREATE` may win, the durable table must
    // carry the winner's shape, and the loser must see the winner's shape.
    let shared = shared_engine("auto");
    const ROUNDS: usize = 8;
    for round in 0..ROUNDS {
        let name = format!("namesake_{round}");
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for (shape, column) in [("INT", "id"), ("TEXT", "s")] {
            let shared = Arc::clone(&shared);
            let barrier = Arc::clone(&barrier);
            let name = name.clone();
            handles.push(std::thread::spawn(move || {
                let mut s = session(&shared);
                barrier.wait();
                let created = s
                    .execute(&format!("CREATE TABLE {name} ({column} {shape})"))
                    .map(|_| ())
                    .map_err(|e| format!("{e:?}"));
                // Whichever way the race went, this client's next statement
                // must observe the surviving definition.
                let visible = columns_of(&mut s, &name);
                (created, visible)
            }));
        }
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .collect();
        assert_eq!(
            outcomes.iter().filter(|(o, _)| o.is_ok()).count(),
            1,
            "round {round}: exactly one winner: {outcomes:?}"
        );
        let loser = outcomes
            .iter()
            .find_map(|(o, _)| o.as_ref().err())
            .expect("loser");
        assert!(
            loser.contains("already exists"),
            "round {round}: loser must be told the table exists: {loser}"
        );
        let winner_column = if outcomes[0].0.is_ok() { "id" } else { "s" };
        for (created, visible) in &outcomes {
            assert_eq!(
                visible,
                &vec![winner_column.to_string()],
                "round {round}: session sees a phantom shape (created={created:?})"
            );
        }
        // A fresh session agrees with the winner's definition.
        let mut verify = session(&shared);
        assert_eq!(
            columns_of(&mut verify, &name),
            vec![winner_column.to_string()],
            "round {round}: durable definition is not the winner's"
        );
    }
    let _ = std::fs::remove_dir_all(dir("auto").0);
}

#[test]
fn stale_session_duplicate_create_rejects() {
    // A session whose image predates the create must reject the duplicate:
    // the live-session refresh (generation bump) catches the late statement.
    let shared = shared_engine("stale");
    let mut stale = session(&shared);
    session(&shared)
        .execute("CREATE TABLE later (id INT)")
        .expect("first create");
    let error = stale
        .execute("CREATE TABLE later (id INT)")
        .expect_err("stale duplicate must reject");
    assert!(
        format!("{error:?}").contains("already exists"),
        "unexpected duplicate error: {error:?}"
    );
    let _ = std::fs::remove_dir_all(dir("stale").0);
}

#[test]
fn transactional_drop_and_recreate_commits() {
    // A DROP + CREATE of the same name inside one transaction replaces a
    // table the transaction's own BEGIN image could see; that is not a
    // concurrent claim and must not read as a conflict at COMMIT.
    let shared = shared_engine("recreate");
    session(&shared)
        .execute("CREATE TABLE replaced (id INT)")
        .expect("seed create");
    let mut s = session(&shared);
    s.begin_session_txn(TxnIsolation::ReadCommitted)
        .expect("begin");
    s.execute_session_aware("DROP TABLE replaced")
        .expect("drop");
    s.execute_session_aware("CREATE TABLE replaced (s TEXT)")
        .expect("recreate");
    s.commit_session_txn()
        .expect("replacement commit must succeed");
    let mut verify = session(&shared);
    assert_eq!(
        columns_of(&mut verify, "replaced"),
        vec!["s".to_string()],
        "replacement definition must survive"
    );
    let _ = std::fs::remove_dir_all(dir("recreate").0);
}

#[test]
fn concurrent_same_name_create_in_group_batches_has_exactly_one_winner() {
    // A single batch carrying `BEGIN; CREATE TABLE …; COMMIT` publishes its
    // catalog changes at the end of the group. Two concurrent batches
    // claiming the same name must still admit exactly one winner (the
    // end-of-group re-probe under the create gate), and the losing batch
    // must converge its image to the winner's definition.
    let shared = shared_engine("group");
    const ROUNDS: usize = 6;
    for round in 0..ROUNDS {
        let name = format!("groupnamesake_{round}");
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for (shape, column) in [("INT", "id"), ("TEXT", "s")] {
            let shared = Arc::clone(&shared);
            let barrier = Arc::clone(&barrier);
            let name = name.clone();
            handles.push(std::thread::spawn(move || {
                let mut s = session(&shared);
                barrier.wait();
                let created = s
                    .execute(&format!(
                        "BEGIN; CREATE TABLE {name} ({column} {shape}); COMMIT"
                    ))
                    .map(|_| ())
                    .map_err(|e| format!("{e:?}"));
                let visible = columns_of(&mut s, &name);
                (created, visible)
            }));
        }
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .collect();
        assert_eq!(
            outcomes.iter().filter(|(o, _)| o.is_ok()).count(),
            1,
            "round {round}: exactly one winner: {outcomes:?}"
        );
        let loser = outcomes
            .iter()
            .find_map(|(o, _)| o.as_ref().err())
            .expect("loser");
        assert!(
            loser.contains("already exists"),
            "round {round}: loser must be told the table exists: {loser}"
        );
        let winner_column = if outcomes[0].0.is_ok() { "id" } else { "s" };
        for (created, visible) in &outcomes {
            assert_eq!(
                visible,
                &vec![winner_column.to_string()],
                "round {round}: session sees a phantom shape (created={created:?})"
            );
        }
        let mut verify = session(&shared);
        assert_eq!(
            columns_of(&mut verify, &name),
            vec![winner_column.to_string()],
            "round {round}: durable definition is not the winner's"
        );
    }
    let _ = std::fs::remove_dir_all(dir("group").0);
}

#[test]
fn concurrent_same_name_create_in_transactions_has_exactly_one_winner() {
    // Transactional DDL stages the create and publishes it at COMMIT; the
    // commit-time re-probe must admit exactly one winner there too, even
    // though both transactions created locally from the same generation.
    let shared = shared_engine("txn");
    let mut a = session(&shared);
    let mut b = session(&shared);
    a.begin_session_txn(TxnIsolation::ReadCommitted)
        .expect("begin a");
    b.begin_session_txn(TxnIsolation::ReadCommitted)
        .expect("begin b");
    a.execute_session_aware("CREATE TABLE txnamesake (id INT)")
        .expect("a create");
    b.execute_session_aware("CREATE TABLE txnamesake (s TEXT)")
        .expect("b create");
    let a_commit = a
        .commit_session_txn()
        .map(|_| ())
        .map_err(|e| format!("{e:?}"));
    let b_commit = b
        .commit_session_txn()
        .map(|_| ())
        .map_err(|e| format!("{e:?}"));
    assert_eq!(
        [a_commit.is_ok(), b_commit.is_ok()]
            .iter()
            .filter(|ok| **ok)
            .count(),
        1,
        "exactly one commit may publish the name: a={a_commit:?} b={b_commit:?}"
    );
    let loser = a_commit.as_ref().err().or(b_commit.as_ref().err()).unwrap();
    assert!(
        loser.contains("already exists"),
        "loser's COMMIT must fail the claim: {loser}"
    );
    let winner_column = if a_commit.is_ok() { "id" } else { "s" };
    let mut verify = session(&shared);
    assert_eq!(
        columns_of(&mut verify, "txnamesake"),
        vec![winner_column.to_string()],
        "durable definition is not the winner's"
    );
    let _ = std::fs::remove_dir_all(dir("txn").0);
}
