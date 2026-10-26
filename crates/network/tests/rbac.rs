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
//! Wire-level authentication + authorization tests (Beta 2 Phase 2).
//!
//! Multi-user SCRAM against catalog verifiers, first-run bootstrap,
//! password rotation, unknown-user handling, account lockout, file-COPY
//! gating, transaction behavior after denial, CONNECT withdrawal, and
//! database isolation — all through real TCP + PGWire.

use plomid_network::server::PlomidServer;
use plomid_network::session::ServerAuthConfig;
use plomid_txn::PlomidStorageEngine;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

fn temp_path(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-rbac-{label}-{}-{id}", std::process::id()))
}

async fn start_scram_server() -> std::net::SocketAddr {
    let storage_path = temp_path("engine");
    let wal_path = temp_path("engine-wal");
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));
    let server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::scram("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    tokio::spawn(async move { server.run(engine).await });
    addr
}

async fn start_db_server() -> (std::net::SocketAddr, std::path::PathBuf) {
    let storage_path = temp_path("dbroot");
    let wal_path = temp_path("dbroot-wal");
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));
    let server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::scram("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    let root = storage_path.clone();
    tokio::spawn(async move { server.run_with_database_root(engine, storage_path).await });
    (addr, root)
}

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .expect("connect timeout")
        .expect("tcp connect")
}

fn frontend_message(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(1 + 4 + payload.len());
    message.push(tag);
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(payload);
    message
}

async fn send_raw(stream: &mut tokio::net::TcpStream, data: &[u8]) {
    stream.write_all(data).await.expect("write");
    stream.flush().await.expect("flush");
}

async fn read_message(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await.expect("read tag");
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.expect("read len");
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.expect("read payload");
    (tag[0], payload)
}

async fn send_startup(stream: &mut tokio::net::TcpStream, user: &str, database: &str) {
    let mut payload = b"\x00\x03\x00\x00".to_vec();
    payload.extend_from_slice(b"user\x00");
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(b"database\x00");
    payload.extend_from_slice(database.as_bytes());
    payload.push(0);
    payload.push(0);
    let mut message = Vec::with_capacity(4 + payload.len());
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(&payload);
    send_raw(stream, &message).await;
}

fn scram_attrs(message: &str) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for attr in message.split(',') {
        let parts = attr.splitn(2, '=').collect::<Vec<_>>();
        if parts.len() == 2 {
            out.insert(parts[0].to_string(), parts[1].to_string());
        }
    }
    out
}

/// Drives SCRAM-SHA-256 to ReadyForQuery. Returns Ok on success, or the
/// server's error text on authentication failure.
async fn scram_login(
    stream: &mut tokio::net::TcpStream,
    user: &str,
    password: &str,
) -> Result<(), String> {
    use plomid_network::scram::{base64_decode, base64_encode, hmac_sha256, pbkdf2_sha256, sha256};
    send_startup(stream, user, "plomid").await;
    let (tag, payload) = read_message(stream).await;
    if tag == b'E' {
        return Err(String::from_utf8_lossy(&payload).into_owned());
    }
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 10);
    let client_first_bare = format!("n={user},r=testnonce");
    let mut initial = Vec::new();
    initial.extend_from_slice(b"SCRAM-SHA-256\x00");
    initial.extend_from_slice(format!("n,,{client_first_bare}").as_bytes());
    send_raw(stream, &frontend_message(b'p', &initial)).await;
    let (tag, payload) = read_message(stream).await;
    if tag == b'E' {
        return Err(String::from_utf8_lossy(&payload).into_owned());
    }
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 11);
    let server_first = String::from_utf8_lossy(&payload[4..]).into_owned();
    let attrs = scram_attrs(&server_first);
    let salt = base64_decode(attrs.get("s").unwrap()).unwrap();
    let iterations: u32 = attrs.get("i").unwrap().parse().unwrap();
    let nonce = attrs.get("r").unwrap().clone();
    let salted = pbkdf2_sha256(password.as_bytes(), &salt, iterations, 32);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!("{},p={}", without_proof, base64_encode(&proof));
    send_raw(stream, &frontend_message(b'p', client_final.as_bytes())).await;
    let (tag, payload) = read_message(stream).await;
    if tag == b'E' {
        return Err(String::from_utf8_lossy(&payload).into_owned());
    }
    assert_eq!(tag, b'R');
    loop {
        let (tag, payload) = read_message(stream).await;
        match tag {
            b'Z' => return Ok(()),
            b'E' => return Err(String::from_utf8_lossy(&payload).into_owned()),
            _ => {}
        }
    }
}

async fn send_query_messages(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let query = format!("{sql};\x00");
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    send_raw(stream, &message).await;
    let mut response = Vec::new();
    loop {
        let (tag, payload) = read_message(stream).await;
        let done = tag == b'Z';
        response.push((tag, payload));
        if done {
            break;
        }
    }
    response
}

fn tags_of(response: &[(u8, Vec<u8>)]) -> Vec<u8> {
    response.iter().map(|(tag, _)| *tag).collect()
}

fn has_error_with(response: &[(u8, Vec<u8>)], needle: &str) -> bool {
    response
        .iter()
        .any(|(tag, payload)| *tag == b'E' && String::from_utf8_lossy(payload).contains(needle))
}

async fn bootstrap_session(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = connect(addr).await;
    scram_login(&mut stream, "plomid", "secret")
        .await
        .expect("bootstrap login");
    stream
}

#[tokio::test]
async fn multi_user_scram_against_catalog_verifiers() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    for sql in [
        "CREATE TABLE wire_t (id INTEGER)",
        "INSERT INTO wire_t VALUES (1)",
        "CREATE ROLE analyst WITH LOGIN PASSWORD 'analyst-pw'",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    // Analyst authenticates with her own catalog password over SCRAM.
    let mut analyst = connect(addr).await;
    scram_login(&mut analyst, "analyst", "analyst-pw")
        .await
        .expect("analyst login");
    // …but holds no privileges yet (fail closed).
    let denied = send_query_messages(&mut analyst, "SELECT * FROM wire_t").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    // Grant confers access on the SAME connection (§P1-003): live sessions
    // observe other sessions' committed catalog changes without reconnect.
    let granted = send_query_messages(&mut admin, "GRANT SELECT ON wire_t TO analyst").await;
    assert!(tags_of(&granted).contains(&b'C'));
    let allowed = send_query_messages(&mut analyst, "SELECT * FROM wire_t").await;
    assert_eq!(tags_of(&allowed), [b'T', b'D', b'C', b'Z']);
}

#[tokio::test]
async fn table_created_after_connect_is_visible_on_the_same_connection() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    let created =
        send_query_messages(&mut admin, "CREATE ROLE late WITH LOGIN PASSWORD 'late-pw'").await;
    assert!(tags_of(&created).contains(&b'C'));
    // Analyst connects BEFORE the table exists (the audit probe9 repro).
    let mut analyst = connect(addr).await;
    scram_login(&mut analyst, "late", "late-pw")
        .await
        .expect("analyst login");
    for sql in [
        "CREATE TABLE late_t (id INTEGER)",
        "INSERT INTO late_t VALUES (7)",
        "GRANT SELECT ON late_t TO late",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    // Same connection, no relogin: the table and the grant are visible.
    let allowed = send_query_messages(&mut analyst, "SELECT * FROM late_t").await;
    assert_eq!(tags_of(&allowed), [b'T', b'D', b'C', b'Z']);
}

#[tokio::test]
async fn wrong_password_and_unknown_user_fail_identically() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    let created = send_query_messages(
        &mut admin,
        "CREATE ROLE carol WITH LOGIN PASSWORD 'carol-pw'",
    )
    .await;
    assert!(tags_of(&created).contains(&b'C'));
    // Wrong password for a known role…
    let mut stream = connect(addr).await;
    let err = scram_login(&mut stream, "carol", "nope")
        .await
        .expect_err("must fail");
    assert!(err.contains("28P01"), "{err}");
    assert!(
        !err.contains("carol-pw"),
        "no credential material leaks: {err}"
    );
    // …and an unknown role fail with the same code and no oracle.
    let mut stream = connect(addr).await;
    let err = scram_login(&mut stream, "ghost-nobody", "whatever")
        .await
        .expect_err("must fail");
    assert!(err.contains("28P01"), "{err}");
}

#[tokio::test]
async fn password_rotation_takes_effect_immediately() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    let created =
        send_query_messages(&mut admin, "CREATE ROLE dave WITH LOGIN PASSWORD 'dave-1'").await;
    assert!(tags_of(&created).contains(&b'C'));
    let mut first = connect(addr).await;
    scram_login(&mut first, "dave", "dave-1")
        .await
        .expect("first password");
    drop(first);
    let rotated = send_query_messages(&mut admin, "ALTER ROLE dave PASSWORD 'dave-2'").await;
    assert!(tags_of(&rotated).contains(&b'C'));
    // Old password stops working…
    let mut stale = connect(addr).await;
    assert!(scram_login(&mut stale, "dave", "dave-1").await.is_err());
    // …new password works.
    let mut fresh = connect(addr).await;
    scram_login(&mut fresh, "dave", "dave-2")
        .await
        .expect("rotated password");
}

#[tokio::test]
async fn repeated_failures_lock_the_account() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    let created = send_query_messages(
        &mut admin,
        "CREATE ROLE locky WITH LOGIN PASSWORD 'locky-pw'",
    )
    .await;
    assert!(tags_of(&created).contains(&b'C'));
    let before = plomid_core::observability::snapshot();
    for _ in 0..5 {
        let mut stream = connect(addr).await;
        assert!(scram_login(&mut stream, "locky", "wrong").await.is_err());
    }
    // Even the correct password is now refused while locked.
    let mut stream = connect(addr).await;
    let err = scram_login(&mut stream, "locky", "locky-pw")
        .await
        .expect_err("locked account must refuse");
    assert!(err.contains("locked"), "{err}");
    let after = plomid_core::observability::snapshot();
    assert!(after.account_lockouts > before.account_lockouts);
    assert!(after.auth_failures > before.auth_failures);
}

#[tokio::test]
async fn file_copy_is_denied_for_non_superusers() {
    let (addr, _root) = start_db_server().await;
    let mut admin = bootstrap_session(addr).await;
    for sql in [
        "CREATE TABLE copy_t (id INTEGER)",
        "INSERT INTO copy_t VALUES (1)",
        "CREATE ROLE copier WITH LOGIN PASSWORD 'copier-pw'",
        "GRANT ALL ON copy_t TO copier",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    // Full table rights, but file COPY needs superuser: 42501.
    let mut copier = connect(addr).await;
    scram_login(&mut copier, "copier", "copier-pw")
        .await
        .expect("login");
    let denied = send_query_messages(&mut copier, "COPY copy_t TO '/tmp/x.csv'").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    // The superuser passes the privilege gate (the file itself may still
    // fail outside the jail — that is a different, non-42501 error).
    let jail_bypass = send_query_messages(&mut admin, "COPY copy_t TO '/tmp/x.csv'").await;
    assert!(
        !has_error_with(&jail_bypass, "42501"),
        "superuser must pass the privilege gate: {jail_bypass:?}"
    );
}

#[tokio::test]
async fn wire_transaction_denial_keeps_session_usable() {
    let addr = start_scram_server().await;
    let mut admin = bootstrap_session(addr).await;
    for sql in [
        "CREATE TABLE wtx (id INTEGER)",
        "INSERT INTO wtx VALUES (1)",
        "CREATE ROLE writer WITH LOGIN PASSWORD 'writer-pw'",
        "GRANT SELECT ON wtx TO writer",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    let mut writer = connect(addr).await;
    scram_login(&mut writer, "writer", "writer-pw")
        .await
        .expect("login");
    // True staging denies at STATEMENT time (not at COMMIT): the INSERT
    // fails immediately with 42501, the transaction aborts, and only
    // ROLLBACK is accepted afterwards.
    let begin = send_query_messages(&mut writer, "BEGIN").await;
    assert!(tags_of(&begin).contains(&b'C'));
    let denied = send_query_messages(&mut writer, "INSERT INTO wtx VALUES (2)").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    let rollback = send_query_messages(&mut writer, "ROLLBACK").await;
    assert!(tags_of(&rollback).contains(&b'Z'), "{rollback:?}");
    // With the grant, the write stages and commits; the row is visible.
    let granted = send_query_messages(&mut admin, "GRANT INSERT ON wtx TO writer").await;
    assert!(tags_of(&granted).contains(&b'C'));
    // Reconnect: privilege changes take effect on next connection.
    let mut writer = connect(addr).await;
    scram_login(&mut writer, "writer", "writer-pw")
        .await
        .expect("relogin");
    for sql in ["BEGIN", "INSERT INTO wtx VALUES (2)", "SELECT * FROM wtx"] {
        let response = send_query_messages(&mut writer, sql).await;
        assert!(!has_error_with(&response, "42501"), "{sql}: {response:?}");
    }
    // Read-your-writes: the staged row is visible before COMMIT.
    let commit = send_query_messages(&mut writer, "COMMIT").await;
    assert!(tags_of(&commit).contains(&b'C'), "{commit:?}");
    let count = send_query_messages(&mut admin, "SELECT * FROM wtx").await;
    assert_eq!(
        count.iter().filter(|(tag, _)| *tag == b'D').count(),
        2,
        "both rows must be visible after commit"
    );
}

#[tokio::test]
async fn database_isolation_and_connect_withdrawal() {
    let (addr, _root) = start_db_server().await;
    let mut admin = bootstrap_session(addr).await;
    for sql in [
        "CREATE DATABASE geeks",
        "CREATE TABLE iso_t (id INTEGER)",
        "INSERT INTO iso_t VALUES (1)",
        "CREATE ROLE iso_user WITH LOGIN PASSWORD 'iso-pw'",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    // Tables live per database: the new database cannot see plomid's table.
    let switched = send_query_messages(&mut admin, "USE geeks").await;
    assert!(tags_of(&switched).contains(&b'C'));
    let missing = send_query_messages(&mut admin, "SELECT * FROM iso_t").await;
    assert_eq!(missing[0].0, b'E', "cross-database table must not resolve");
    let _ = send_query_messages(&mut admin, "USE plomid").await;
    // Withdraw CONNECT: the role can no longer switch.
    let revoked =
        send_query_messages(&mut admin, "REVOKE CONNECT ON DATABASE geeks FROM iso_user").await;
    assert!(tags_of(&revoked).contains(&b'C'));
    let mut user = connect(addr).await;
    scram_login(&mut user, "iso_user", "iso-pw")
        .await
        .expect("login");
    let denied = send_query_messages(&mut user, "USE geeks").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
}

#[tokio::test]
async fn database_ddl_requires_createdb_and_reports_sqlstates() {
    // P0-003 regression: CREATE/DROP DATABASE runs outside executor
    // statement dispatch, so it needs its own authorization gate. A role
    // without CREATEDB must be denied with 42501 AND leave no side effects
    // (a bare OK from a utility statement proves nothing — every denial
    // below is verified against SHOW DATABASES).
    let (addr, _root) = start_db_server().await;
    let mut admin = bootstrap_session(addr).await;
    for sql in [
        "CREATE ROLE ddl_user WITH LOGIN PASSWORD 'ddl-pw'",
        "CREATE DATABASE victim",
    ] {
        let response = send_query_messages(&mut admin, sql).await;
        assert!(tags_of(&response).contains(&b'C'), "{sql}: {response:?}");
    }
    let mut user = connect(addr).await;
    scram_login(&mut user, "ddl_user", "ddl-pw")
        .await
        .expect("login");

    // CREATE without CREATEDB: denied, no database appears.
    let denied = send_query_messages(&mut user, "CREATE DATABASE evil").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    let listed = send_query_messages(&mut admin, "SHOW DATABASES").await;
    let text = String::from_utf8_lossy(
        &listed
            .iter()
            .flat_map(|(_, p)| p.clone())
            .collect::<Vec<_>>(),
    )
    .into_owned();
    assert!(
        !text.contains("evil"),
        "denied CREATE must leave no trace: {listed:?}"
    );

    // DROP without CREATEDB: denied, victim survives.
    let denied = send_query_messages(&mut user, "DROP DATABASE victim").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    let listed = send_query_messages(&mut admin, "SHOW DATABASES").await;
    let text = String::from_utf8_lossy(
        &listed
            .iter()
            .flat_map(|(_, p)| p.clone())
            .collect::<Vec<_>>(),
    )
    .into_owned();
    assert!(
        text.contains("victim"),
        "denied DROP must not remove: {listed:?}"
    );

    // Conditional forms are denied the same way (the flag must not bypass).
    let denied = send_query_messages(&mut user, "CREATE DATABASE IF NOT EXISTS evil2").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");
    let denied = send_query_messages(&mut user, "DROP DATABASE IF EXISTS victim").await;
    assert!(has_error_with(&denied, "42501"), "{denied:?}");

    // CREATEDB confers the gate (superuser is not the only way in).
    let granted = send_query_messages(&mut admin, "ALTER ROLE ddl_user CREATEDB").await;
    assert!(tags_of(&granted).contains(&b'C'), "{granted:?}");
    let created = send_query_messages(&mut user, "CREATE DATABASE owned").await;
    assert!(tags_of(&created).contains(&b'C'), "{created:?}");
    let listed = send_query_messages(&mut admin, "SHOW DATABASES").await;
    let text = String::from_utf8_lossy(
        &listed
            .iter()
            .flat_map(|(_, p)| p.clone())
            .collect::<Vec<_>>(),
    )
    .into_owned();
    assert!(
        text.contains("owned"),
        "granted CREATE must take effect: {listed:?}"
    );
    let dropped = send_query_messages(&mut user, "DROP DATABASE owned").await;
    assert!(tags_of(&dropped).contains(&b'C'), "{dropped:?}");

    // Superuser path still works, with PostgreSQL SQLSTATEs.
    let created = send_query_messages(&mut admin, "CREATE DATABASE appdb").await;
    assert!(tags_of(&created).contains(&b'C'), "{created:?}");
    let dup = send_query_messages(&mut admin, "CREATE DATABASE appdb").await;
    assert!(has_error_with(&dup, "42P04"), "{dup:?}");
    let noop = send_query_messages(&mut admin, "CREATE DATABASE IF NOT EXISTS appdb").await;
    assert!(tags_of(&noop).contains(&b'C'), "{noop:?}");
    let missing = send_query_messages(&mut admin, "DROP DATABASE nosuchdb").await;
    assert!(has_error_with(&missing, "3D000"), "{missing:?}");
    let noop = send_query_messages(&mut admin, "DROP DATABASE IF EXISTS nosuchdb").await;
    assert!(tags_of(&noop).contains(&b'C'), "{noop:?}");
    let dropped = send_query_messages(&mut admin, "DROP DATABASE appdb").await;
    assert!(tags_of(&dropped).contains(&b'C'), "{dropped:?}");
    let default = send_query_messages(&mut admin, "DROP DATABASE plomid").await;
    assert!(has_error_with(&default, "42501"), "{default:?}");
}
