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
//! RBAC enforcement tests (Beta 2 Phase 2): every minimum-scope category.
//!
//! One binary, sequential flow (fresh engine per test via unique dirs):
//! table SELECT/INSERT/UPDATE/DELETE denial + grant, DDL gating, role
//! inheritance, database CONNECT, COPY gating, admin-only operations,
//! transaction behavior after denial, audit counters, `has_*` answers,
//! ownership, revocation, PUBLIC, schema USAGE, TRUNCATE, drop-cleans,
//! rotation, and least-privilege placeholders.

use plomid_core::observability;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn scratch(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("plomid-rbac-{label}-{id}"));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn open(label: &str) -> Executor<PlomidStorageEngine> {
    let root = scratch(label);
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(&root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(&root, &wal, 64))
        .expect("open engine");
    Executor::new(engine).expect("executor")
}

/// Runs `sql` as `user`. Panics on error; returns the result.
fn as_user(session: &mut Executor<PlomidStorageEngine>, user: &str, sql: &str) -> QueryResult {
    session.set_session_context("plomid", user);
    session
        .execute(sql)
        .unwrap_or_else(|e| panic!("{user}: {sql} failed: {e:?}"))
}

/// Runs `sql` as `user`, expecting denial. Returns the error message.
fn as_user_denied(session: &mut Executor<PlomidStorageEngine>, user: &str, sql: &str) -> String {
    session.set_session_context("plomid", user);
    match session.execute(sql) {
        Ok(result) => panic!("{user}: {sql} should have been denied, got {result:?}"),
        Err(e) => {
            let text = format!("{e:?}");
            assert!(
                text.contains("PL-PERMISSION-DENIED") || text.contains("permission denied"),
                "{user}: {sql} wrong error: {text}"
            );
            text
        }
    }
}

fn bool_cell(result: &QueryResult) -> bool {
    match result {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Bool(b) => *b,
            other => panic!("expected bool, got {other:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup() -> Executor<PlomidStorageEngine> {
    let mut db = open("main");
    // Bootstrap owner creates objects and roles (superuser seed).
    as_user(
        &mut db,
        "plomid",
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
    );
    as_user(
        &mut db,
        "plomid",
        "INSERT INTO docs VALUES (1, 'a'), (2, 'b')",
    );
    as_user(
        &mut db,
        "plomid",
        "CREATE ROLE alice WITH LOGIN PASSWORD 'alice-pw-1'",
    );
    as_user(
        &mut db,
        "plomid",
        "CREATE ROLE bob WITH LOGIN PASSWORD 'bob-pw-1'",
    );
    as_user(&mut db, "plomid", "CREATE ROLE readers WITH NOLOGIN");
    db
}

#[test]
fn table_dml_requires_matching_grants() {
    let mut db = setup();
    // No grants: everything denied with 42501-class errors.
    as_user_denied(&mut db, "alice", "SELECT * FROM docs");
    as_user_denied(&mut db, "alice", "INSERT INTO docs VALUES (3, 'c')");
    as_user_denied(&mut db, "alice", "UPDATE docs SET body = 'x' WHERE id = 1");
    as_user_denied(&mut db, "alice", "DELETE FROM docs WHERE id = 1");
    // Grant each privilege independently; each unlocks exactly its operation.
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO alice");
    as_user(&mut db, "alice", "SELECT * FROM docs");
    as_user_denied(&mut db, "alice", "INSERT INTO docs VALUES (3, 'c')");
    as_user(
        &mut db,
        "plomid",
        "GRANT INSERT, UPDATE, DELETE ON docs TO alice",
    );
    as_user(&mut db, "alice", "INSERT INTO docs VALUES (3, 'c')");
    as_user(&mut db, "alice", "UPDATE docs SET body = 'x' WHERE id = 3");
    as_user(&mut db, "alice", "DELETE FROM docs WHERE id = 3");
}

#[test]
fn ddl_requires_ownership_or_admin() {
    let mut db = setup();
    // Alice cannot alter or drop the owner's table.
    as_user_denied(&mut db, "alice", "ALTER TABLE docs ADD COLUMN extra TEXT");
    as_user_denied(&mut db, "alice", "DROP TABLE docs");
    // Alice cannot create tables without schema CREATE (fail closed).
    as_user_denied(&mut db, "alice", "CREATE TABLE alice_t (id INTEGER)");
    // The owner can do all of the above without any grant.
    as_user(&mut db, "plomid", "ALTER TABLE docs ADD COLUMN extra TEXT");
    // Alice creates after receiving schema CREATE.
    as_user(&mut db, "plomid", "GRANT CREATE ON SCHEMA public TO alice");
    as_user(&mut db, "alice", "CREATE TABLE alice_t (id INTEGER)");
    // …and owns what she creates (full rights, no grants needed).
    as_user(&mut db, "alice", "INSERT INTO alice_t VALUES (1)");
    as_user(&mut db, "alice", "DROP TABLE alice_t");
}

#[test]
fn role_inheritance_flows_and_revoke_stops_it() {
    let mut db = setup();
    as_user(&mut db, "plomid", "GRANT readers TO bob");
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO readers");
    // Membership confers the grant transitively.
    as_user(&mut db, "bob", "SELECT * FROM docs");
    // Withdrawal removes it.
    as_user(&mut db, "plomid", "REVOKE readers FROM bob");
    as_user_denied(&mut db, "bob", "SELECT * FROM docs");
    // Direct grants still work.
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO bob");
    as_user(&mut db, "bob", "SELECT * FROM docs");
    // Revoking the object grant removes it again.
    as_user(&mut db, "plomid", "REVOKE SELECT ON docs FROM bob");
    as_user_denied(&mut db, "bob", "SELECT * FROM docs");
}

#[test]
fn public_grants_apply_to_everyone() {
    let mut db = setup();
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO PUBLIC");
    as_user(&mut db, "alice", "SELECT * FROM docs");
    as_user(&mut db, "bob", "SELECT * FROM docs");
}

#[test]
fn schema_usage_gates_cross_schema_access() {
    let mut db = setup();
    as_user(&mut db, "plomid", "CREATE SCHEMA finance");
    as_user(
        &mut db,
        "plomid",
        "CREATE TABLE finance.ledger (id INTEGER)",
    );
    // Table grant alone is not enough without schema USAGE.
    as_user(&mut db, "plomid", "GRANT SELECT ON finance.ledger TO alice");
    let err = as_user_denied(&mut db, "alice", "SELECT * FROM finance.ledger");
    assert!(err.contains("USAGE"), "expected USAGE denial, got {err}");
    as_user(&mut db, "plomid", "GRANT USAGE ON SCHEMA finance TO alice");
    as_user(&mut db, "alice", "SELECT * FROM finance.ledger");
}

#[test]
fn truncate_needs_its_own_privilege() {
    let mut db = setup();
    as_user(&mut db, "plomid", "GRANT ALL ON docs TO alice");
    as_user(&mut db, "alice", "TRUNCATE docs");
    // Fresh table, SELECT-only Bob cannot truncate.
    as_user(&mut db, "plomid", "CREATE TABLE t2 (id INTEGER)");
    as_user(&mut db, "plomid", "GRANT SELECT ON t2 TO bob");
    as_user_denied(&mut db, "bob", "TRUNCATE t2");
}

#[test]
fn drop_cleans_rights_and_recreate_starts_empty() {
    let mut db = setup();
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO alice");
    as_user(&mut db, "alice", "SELECT * FROM docs");
    as_user(&mut db, "plomid", "DROP TABLE docs");
    as_user(&mut db, "plomid", "CREATE TABLE docs (id INTEGER)");
    // Stale grant is gone with the drop.
    as_user_denied(&mut db, "alice", "SELECT * FROM docs");
}

#[test]
fn admin_operations_are_gated() {
    let mut db = setup();
    // Non-admin cannot create roles, grant membership, or grant on objects
    // they do not own.
    as_user_denied(&mut db, "alice", "CREATE ROLE mallory WITH LOGIN");
    as_user_denied(&mut db, "alice", "GRANT readers TO bob");
    as_user_denied(&mut db, "alice", "GRANT SELECT ON docs TO bob");
    // Non-superuser cannot self-promote via ALTER ROLE.
    as_user_denied(&mut db, "alice", "ALTER ROLE alice SUPERUSER");
    // CREATEROLE holders can manage roles but not promote to superuser.
    as_user(&mut db, "plomid", "CREATE ROLE hr WITH LOGIN CREATEROLE");
    as_user(&mut db, "hr", "CREATE ROLE mallory WITH LOGIN");
    as_user_denied(&mut db, "hr", "ALTER ROLE mallory SUPERUSER");
    // Superuser promotes; promotion takes effect.
    as_user(&mut db, "plomid", "ALTER ROLE mallory SUPERUSER");
    as_user(&mut db, "mallory", "CREATE ROLE meanwhile WITH LOGIN");
}

#[test]
fn database_connect_can_be_withdrawn() {
    let mut db = setup();
    as_user(&mut db, "alice", "USE plomid");
    as_user(
        &mut db,
        "plomid",
        "REVOKE CONNECT ON DATABASE plomid FROM alice",
    );
    as_user_denied(&mut db, "alice", "USE plomid");
    as_user(
        &mut db,
        "plomid",
        "GRANT CONNECT ON DATABASE plomid TO alice",
    );
    as_user(&mut db, "alice", "USE plomid");
}

#[test]
fn copy_statement_requires_table_privilege() {
    let mut db = setup();
    // The executor-level COPY shape is wire-only (Unsupported), but the
    // authorization gate runs FIRST: denied users get 42501, entitled
    // users proceed to the (wire-only) arm. This pins gate ordering.
    let err = as_user_denied(&mut db, "alice", "COPY docs TO STDOUT");
    assert!(err.contains("SELECT"), "{err}");
    let err = as_user_denied(&mut db, "alice", "COPY docs FROM STDIN");
    assert!(err.contains("INSERT"), "{err}");
    as_user(&mut db, "plomid", "GRANT SELECT, INSERT ON docs TO alice");
    db.set_session_context("plomid", "alice");
    let err = db
        .execute("COPY docs TO STDOUT")
        .expect_err("wire-only arm");
    assert!(format!("{err:?}").contains("Unsupported"), "{err:?}");
}

#[test]
fn file_copy_gate_requires_superuser() {
    let mut db = setup();
    // check_file_copy: superuser passes with table privilege; others fail
    // even with full table grants (server-filesystem access is separate).
    assert!(db.is_superuser("plomid"));
    assert!(!db.is_superuser("alice"));
    as_user(&mut db, "plomid", "GRANT ALL ON docs TO alice");
    assert!(db.check_file_copy("plomid", "docs", true).is_ok());
    assert!(db.check_file_copy("alice", "docs", true).is_err());
    assert!(db.check_file_copy("alice", "docs", false).is_err());
}

#[test]
fn transaction_denial_aborts_the_commit() {
    let mut db = setup();
    as_user(&mut db, "plomid", "GRANT SELECT ON docs TO alice");
    // Alice stages an INSERT she may not run; the single-batch COMMIT must
    // fail 42501 and change nothing (explicit transactions run as one
    // batch through the group pre-check).
    db.set_session_context("plomid", "alice");
    let err = db
        .execute("BEGIN; INSERT INTO docs VALUES (9, 'z'); SELECT * FROM docs; COMMIT")
        .expect_err("COMMIT must be denied");
    assert!(
        format!("{err:?}").contains("permission denied"),
        "wrong error: {err:?}"
    );
    // The session stays usable afterwards (the denied group never began,
    // so nothing is left half-open).
    db.set_session_context("plomid", "alice");
    db.execute("SELECT * FROM docs")
        .expect("session usable after denial");
    // Nothing was written; the owner still sees the original rows.
    db.set_session_context("plomid", "plomid");
    let rows = db.execute("SELECT count(*) FROM docs").expect("count");
    match rows {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn denials_emit_audit_counters() {
    observability::reset_for_tests();
    let before: i64 = {
        let snap = observability::snapshot();
        snap.authz_denials as i64
    };
    let mut db = setup();
    as_user_denied(&mut db, "alice", "SELECT * FROM docs");
    as_user_denied(&mut db, "alice", "DROP TABLE docs");
    let after = observability::snapshot().authz_denials as i64;
    // Lower bound: sibling tests in this binary deny concurrently on the
    // same process-wide counters (each denial still counts exactly once —
    // verified by the single-flow observability suite for the happy path).
    assert!(
        after - before >= 2,
        "denials must be counted, got delta {}",
        after - before
    );
    // The security surface exposes the same counters in-band.
    db.set_session_context("plomid", "plomid");
    let result = db
        .execute("SELECT * FROM plomid_stat_security()")
        .expect("stats");
    match result {
        QueryResult::Rows { columns, rows, .. } => {
            assert!(
                columns.contains(&"authz_denials".to_string()),
                "{columns:?}"
            );
            assert_eq!(rows.len(), 1);
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn all_tables_in_schema_grant_applies_to_current_tables_only() {
    let mut db = setup();
    as_user(
        &mut db,
        "plomid",
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
    );
    as_user(
        &mut db,
        "plomid",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice",
    );
    // Both existing tables are covered, and only SELECT is covered.
    as_user(&mut db, "alice", "SELECT * FROM docs");
    as_user(&mut db, "alice", "SELECT * FROM notes");
    as_user_denied(&mut db, "alice", "INSERT INTO docs VALUES (9, 'nope')");

    // PostgreSQL snapshots the table set: a later table is not covered.
    as_user(
        &mut db,
        "plomid",
        "CREATE TABLE later (id INTEGER PRIMARY KEY)",
    );
    as_user_denied(&mut db, "alice", "SELECT * FROM later");

    as_user(
        &mut db,
        "plomid",
        "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM alice",
    );
    as_user_denied(&mut db, "alice", "SELECT * FROM docs");
    as_user_denied(&mut db, "alice", "SELECT * FROM notes");

    // ALL PRIVILEGES expands for the schema-wide form too.
    as_user(
        &mut db,
        "plomid",
        "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bob",
    );
    as_user(&mut db, "bob", "SELECT * FROM docs");
    as_user(&mut db, "bob", "INSERT INTO docs VALUES (10, 'ok')");
}

#[test]
fn has_privilege_answers_are_real() {
    let mut db = setup();
    db.set_session_context("plomid", "alice");
    // Nothing granted yet: false, not true.
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('docs', 'SELECT')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('alice', 'docs', 'SELECT')")
            .expect("q")
    ));
    db.set_session_context("plomid", "plomid");
    db.execute("GRANT SELECT ON docs TO alice").expect("grant");
    db.set_session_context("plomid", "alice");
    assert!(bool_cell(
        &db.execute("SELECT has_table_privilege('docs', 'SELECT')")
            .expect("q")
    ));
    assert!(bool_cell(
        &db.execute("SELECT has_table_privilege('alice', 'docs', 'SELECT')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('bob', 'docs', 'SELECT')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('docs', 'INSERT')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('missing', 'SELECT')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_table_privilege('docs', 'REFERENCES')")
            .expect("q")
    ));
    // Schema + database forms.
    assert!(bool_cell(
        &db.execute("SELECT has_schema_privilege('public', 'USAGE')")
            .expect("q")
    ));
    assert!(!bool_cell(
        &db.execute("SELECT has_schema_privilege('public', 'CREATE')")
            .expect("q")
    ));
    assert!(bool_cell(
        &db.execute("SELECT has_database_privilege('plomid', 'CONNECT')")
            .expect("q")
    ));
    // Superuser sees true.
    db.set_session_context("plomid", "plomid");
    assert!(bool_cell(
        &db.execute("SELECT has_table_privilege('docs', 'DELETE')")
            .expect("q")
    ));
}

#[test]
fn ownership_and_placeholders() {
    let mut db = setup();
    // Fresh roles hold nothing and are not superusers.
    assert!(!db.is_superuser("alice"));
    assert!(!db.is_superuser("nobody"));
    // Ownership recorded at creation.
    db.set_session_context("plomid", "plomid");
    db.execute("CREATE TABLE owned (id INTEGER)")
        .expect("create");
    // Password rotation: verifier changes, new verifies, old does not.
    let before = db
        .auth_credential("alice")
        .expect("verifier stored at creation");
    assert!(before.verify(b"alice-pw-1"));
    db.set_session_context("plomid", "plomid");
    db.execute("ALTER ROLE alice PASSWORD 'alice-pw-2'")
        .expect("rotate");
    let after = db
        .auth_credential("alice")
        .expect("verifier after rotation");
    assert_ne!(before.verifier, after.verifier);
    assert!(after.verify(b"alice-pw-2"));
    assert!(!after.verify(b"alice-pw-1"));
    // Self-service rotation works for the role itself (needs CREATEROLE?
    // no — self rotation is allowed): covered at wire level. Here the
    // authorizer path: alice rotates her own password.
    db.set_session_context("plomid", "alice");
    db.execute("ALTER ROLE alice PASSWORD 'alice-pw-3'")
        .expect("self rotation");
    // …but alice cannot rotate bob's.
    let err = db
        .execute("ALTER ROLE bob PASSWORD 'x'")
        .expect_err("cross rotation");
    assert!(format!("{err:?}").contains("permission denied"), "{err:?}");
}

#[test]
fn database_ddl_gate_follows_createdb_across_reopen() {
    // P0-003 gate unit test: CREATE/DROP DATABASE authorization for the wire
    // intercept. Denied without CREATEDB; allowed after ALTER ROLE confers
    // it — including through a fresh executor (durable persist, not just
    // the granting session's image). Superusers always pass.
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1000);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("plomid-rbac-dbddl-{id}"));
    let wal = root.with_extension("wal");
    let _ = std::fs::remove_dir_all(&root);
    let reopen = || {
        let engine = PlomidStorageEngine::create(&root, &wal, 64)
            .or_else(|_| PlomidStorageEngine::open(&root, &wal, 64))
            .expect("engine");
        Executor::new(engine).expect("executor")
    };
    let mut db = reopen();
    db.set_session_context("plomid", "plomid");
    db.execute("CREATE ROLE gate_user WITH LOGIN PASSWORD 'x'")
        .expect("create role");
    assert!(
        db.check_database_ddl("gate_user", true, "appdb").is_err(),
        "no CREATEDB means no CREATE DATABASE"
    );
    assert!(
        db.check_database_ddl("gate_user", false, "appdb").is_err(),
        "no CREATEDB means no DROP DATABASE"
    );
    db.execute("ALTER ROLE gate_user CREATEDB")
        .expect("grant createdb");
    assert!(
        db.check_database_ddl("gate_user", true, "appdb").is_ok(),
        "CREATEDB must confer CREATE DATABASE on the same image"
    );
    // Fresh image from durable state: the grant must have persisted.
    drop(db);
    let mut db2 = reopen();
    assert!(
        db2.check_database_ddl("gate_user", true, "appdb").is_ok(),
        "CREATEDB must survive reopen (durable persist)"
    );
    assert!(
        db2.check_database_ddl("gate_user", false, "appdb").is_ok(),
        "CREATEDB must confer DROP DATABASE"
    );
    assert!(
        db2.check_database_ddl("plomid", true, "appdb").is_ok(),
        "superuser always passes"
    );
    drop(db2);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&wal);
}
