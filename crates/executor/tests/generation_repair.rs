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
//! Generation перевозок: DROP ordering (exclusion before directory removal)
//! and dangling-generation repair on open.
//!
//! A DROP that deletes a table directory before its file-catalog exclusion
//! publication lands leaves the newest file-catalog referencing missing
//! generation files, which used to fail every subsequent open (total
//! unavailability persisting across restarts). These tests pin both halves:
//! DROP keeps serving, and a crafted dangling state repairs on open with
//! live data intact.
use plomid_executor::{Executor, MaintenancePolicy};
use plomid_sql::{QueryResult, Value};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

type Session = Executor<ConcurrentPlomidStorageEngine>;

fn dir(tag: &str) -> (PathBuf, PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-gen-repair-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-gen-repair-{tag}-wal-{}",
        std::process::id()
    ));
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

fn gen_dirs(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.join("objects")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path
                    .file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("GEN-"))
                {
                    out.push(path);
                } else {
                    stack.push(path);
                }
            }
        }
    }
    out
}

#[test]
fn repeated_vacuum_drop_cycles_keep_serving() {
    let (storage, wal) = dir("storm");
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
    ));
    let mut s = session(&shared);
    s.execute("CREATE TABLE live(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .expect("ddl");
    s.execute("INSERT INTO live VALUES (1, 111), (2, 222)")
        .expect("seed");
    // Repeated publish-then-drop cycles: every DROP must durably exclude the
    // table before its directory goes away, so every fresh session (which
    // opens a generation manager) keeps working.
    for round in 0..10 {
        let mut s = session(&shared);
        s.execute(&format!(
            "CREATE TABLE churn{round}(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"
        ))
        .expect("create");
        s.execute(&format!("INSERT INTO churn{round} VALUES (1, 1), (2, 2)"))
            .expect("insert");
        s.execute(&format!("VACUUM churn{round}")).expect("vacuum");
        s.execute(&format!("DROP TABLE churn{round}"))
            .expect("drop");
        let mut verify = session(&shared);
        assert_eq!(
            scalar(&mut verify, "SELECT COUNT(*) FROM live"),
            2,
            "round {round}"
        );
    }
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM live"), 333);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn dangling_generation_state_repairs_on_open() {
    let (storage, wal) = dir("dangle");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.execute("CREATE TABLE live(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO live VALUES (1, 111), (2, 222)")
            .expect("seed");
        s.execute("CREATE TABLE doomed(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO doomed VALUES (1, 1), (2, 2)")
            .expect("seed");
        s.execute("VACUUM doomed")
            .expect("vacuum publishes a generation");
    }
    // Craft the dangling state: delete every published generation's files
    // (simulating a directory removal that preceded its exclusion
    // publication). Only `doomed` was vacuumed, so every GEN dir is hers.
    let gens = gen_dirs(&storage);
    assert!(
        !gens.is_empty(),
        "VACUUM must publish a generation for the crafted state"
    );
    for gen in &gens {
        std::fs::remove_dir_all(gen).expect("remove generation dir");
    }
    // Engine open repairs (excludes the missing generations through the
    // normal publication path); strict recovery succeeds afterwards. The
    // table itself is still catalogued with its rows intact (only the
    // columnar files were deleted), so it stays queryable via row-path
    // fallback with exact results.
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen repairs"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM live"), 333);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM doomed"), 3);
    // A second open is strict-clean (repair converged exactly once).
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("second reopen"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM live"), 333);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

fn catalog_files(root: &Path) -> Vec<PathBuf> {
    let dir = root.join("catalog");
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("catalog-"))
            {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

#[test]
fn missing_newest_catalog_repairs_by_revert() {
    let (storage, wal) = dir("catrepair");
    {
        let shared = Arc::new(Mutex::new(
            PlomidStorageEngine::create(&storage, &wal, 64).expect("engine"),
        ));
        let mut s = session(&shared);
        s.set_maintenance_policy(MaintenancePolicy::new(u64::MAX));
        s.execute("CREATE TABLE live(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO live VALUES (1, 111), (2, 222)")
            .expect("seed");
        s.execute("VACUUM live").expect("vacuum publishes");
        // Publish the newer catalog on a DIFFERENT table: the older state's
        // generations stay referenced and resolvable (a second VACUUM on the
        // same table would reclaim them, leaving nothing to revert to).
        s.execute("CREATE TABLE other(id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
            .expect("ddl");
        s.execute("INSERT INTO other VALUES (9, 900), (8, 800)")
            .expect("seed");
        s.execute("VACUUM other").expect("vacuum publishes again");
    }
    // Craft the gc-race state: delete the newest catalog file while the
    // pointer still names it (the pointer update survived; the catalog file
    // did not).
    let cats = catalog_files(&storage);
    assert!(
        !cats.is_empty(),
        "publications must leave catalog files behind"
    );
    std::fs::remove_file(cats.last().expect("newest catalog")).expect("delete catalog");
    // Engine open reverts the pointer to the newest resolvable catalog;
    // strict recovery succeeds and pre-existing data is intact.
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("reopen repairs"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM live"), 333);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM other"), 1700);
    drop(s);
    drop(shared);
    let shared = Arc::new(Mutex::new(
        PlomidStorageEngine::open(&storage, &wal, 64).expect("second reopen"),
    ));
    let mut s = session(&shared);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM live"), 333);
    assert_eq!(scalar(&mut s, "SELECT SUM(v) FROM other"), 1700);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
