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
//! Extended-protocol batch integrity (P0-001 regression).
//!
//! PostgreSQL allows exactly one SQL statement per Parse message; several
//! statements must travel as separate protocol messages (pipelined before a
//! Sync) or as a simple-protocol Query. A previous implementation executed
//! only the leading statement of a multi-statement Parse body and silently
//! dropped the rest — data-loss shaped. These tests drive real wire bytes
//! through a live server: every accepted command must run exactly once, and
//! a multi-statement Parse must fail explicitly with 42601.

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
    std::env::temp_dir().join(format!(
        "plomid-extbatch-{label}-{}-{id}",
        std::process::id()
    ))
}

async fn start_server() -> std::net::SocketAddr {
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

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .expect("connect timeout")
        .expect("tcp connect")
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

/// Reads backend messages until ReadyForQuery (`Z`), returning everything seen.
async fn drain_to_sync(stream: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    loop {
        let (tag, payload) = read_message(stream).await;
        let done = tag == b'Z';
        out.push((tag, payload));
        if done {
            break;
        }
    }
    out
}

async fn send_startup(stream: &mut tokio::net::TcpStream, user: &str) {
    let mut payload = b"\x00\x03\x00\x00user\x00".to_vec();
    payload.extend_from_slice(user.as_bytes());
    payload.extend_from_slice(b"\x00database\x00plomid\x00\x00");
    let mut message = Vec::with_capacity(4 + payload.len());
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(&payload);
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
}

async fn scram_login(stream: &mut tokio::net::TcpStream, user: &str, password: &str) {
    use plomid_network::scram::{base64_decode, base64_encode, hmac_sha256, pbkdf2_sha256, sha256};
    send_startup(stream, user).await;
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 10);
    let client_first_bare = format!("n={user},r=testnonce");
    let mut initial = Vec::new();
    initial.extend_from_slice(b"SCRAM-SHA-256\x00");
    initial.extend_from_slice(format!("n,,{client_first_bare}").as_bytes());
    let mut message = Vec::with_capacity(1 + 4 + initial.len());
    message.push(b'p');
    message.extend_from_slice(&((initial.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(&initial);
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    let server_first = String::from_utf8_lossy(&payload[4..]).into_owned();
    let attrs: std::collections::HashMap<String, String> = server_first
        .split(',')
        .filter_map(|part| {
            part.split_once('=')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect();
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
    let mut message = Vec::with_capacity(1 + 4 + client_final.len());
    message.push(b'p');
    message.extend_from_slice(&((client_final.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(client_final.as_bytes());
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    loop {
        let (tag, payload) = read_message(stream).await;
        match tag {
            b'Z' => return,
            b'E' => panic!("login failed: {}", String::from_utf8_lossy(&payload)),
            _ => {}
        }
    }
}

async fn session(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = connect(addr).await;
    scram_login(&mut stream, "plomid", "secret").await;
    stream
}

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(5 + payload.len());
    message.push(tag);
    message.extend_from_slice(&((payload.len() as u32 + 4).to_be_bytes()));
    message.extend_from_slice(payload);
    message
}

fn parse_frame(stmt: &str, query: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(stmt.as_bytes());
    payload.push(0);
    payload.extend_from_slice(query.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0u16.to_be_bytes());
    frame(b'P', &payload)
}

fn bind_frame(portal: &str, stmt: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal.as_bytes());
    payload.push(0);
    payload.extend_from_slice(stmt.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    frame(b'B', &payload)
}

fn execute_frame(portal: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(portal.as_bytes());
    payload.push(0);
    payload.extend_from_slice(&0i32.to_be_bytes());
    frame(b'E', &payload)
}

fn sync_frame() -> Vec<u8> {
    frame(b'S', &[])
}

fn simple_frame(sql: &str) -> Vec<u8> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    frame(b'Q', &payload)
}

/// One extended statement: Parse/Bind/Execute, no Sync yet (caller pipelines).
fn extended_stmt(name: &str, sql: &str) -> Vec<u8> {
    let mut out = parse_frame(name, sql);
    out.extend_from_slice(&bind_frame(name, name));
    out.extend_from_slice(&execute_frame(name));
    out
}

fn tags(response: &[(u8, Vec<u8>)]) -> Vec<u8> {
    response.iter().map(|(tag, _)| *tag).collect()
}

fn has_error(response: &[(u8, Vec<u8>)], needle: &str) -> bool {
    response
        .iter()
        .any(|(tag, payload)| *tag == b'E' && String::from_utf8_lossy(payload).contains(needle))
}

fn data_rows(response: &[(u8, Vec<u8>)]) -> usize {
    response.iter().filter(|(tag, _)| *tag == b'D').count()
}

/// Text of the first column of the first DataRow (`D` payload layout:
/// i16 column count, then per column i32 length + bytes).
fn first_cell_text(response: &[(u8, Vec<u8>)]) -> String {
    let payload = &response
        .iter()
        .find(|(t, _)| *t == b'D')
        .expect("a DataRow")
        .1;
    let len = i32::from_be_bytes(payload[2..6].try_into().expect("len")) as usize;
    String::from_utf8_lossy(&payload[6..6 + len]).into_owned()
}

fn ready_status(response: &[(u8, Vec<u8>)]) -> Option<u8> {
    response
        .iter()
        .rev()
        .find(|(tag, _)| *tag == b'Z')
        .map(|(_, payload)| payload[0])
}

async fn write_all(stream: &mut tokio::net::TcpStream, bytes: &[u8]) {
    stream.write_all(bytes).await.expect("write");
    stream.flush().await.expect("flush");
}

/// Writes `bytes` one byte at a time, fragmenting every frame (including
/// inside headers and payloads).
async fn write_fragmented(stream: &mut tokio::net::TcpStream, bytes: &[u8]) {
    for byte in bytes {
        stream
            .write_all(std::slice::from_ref(byte))
            .await
            .expect("write");
    }
    stream.flush().await.expect("flush");
}

#[tokio::test]
async fn extended_pipelined_statements_each_execute_exactly_once() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    // Three distinguishable statements pipelined before one Sync, sent as a
    // single coalesced socket write.
    let mut batch = extended_stmt("c", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    batch.extend_from_slice(&extended_stmt("i1", "INSERT INTO t VALUES (1)"));
    batch.extend_from_slice(&extended_stmt("i2", "INSERT INTO t VALUES (2)"));
    batch.extend_from_slice(&sync_frame());
    write_all(&mut s, &batch).await;
    let response = drain_to_sync(&mut s).await;
    assert_eq!(
        tags(&response).iter().filter(|t| **t == b'C').count(),
        3,
        "three CommandCompletes, one per statement: {response:?}"
    );
    assert_eq!(ready_status(&response), Some(b'I'));
    // Countable side effect: exactly two rows, each inserted once.
    let mut check = extended_stmt("sel", "SELECT id FROM t ORDER BY id");
    check.extend_from_slice(&sync_frame());
    write_all(&mut s, &check).await;
    let rows = drain_to_sync(&mut s).await;
    assert_eq!(data_rows(&rows), 2, "{rows:?}");
}

#[tokio::test]
async fn extended_multi_statement_parse_rejected_with_42601_and_no_side_effects() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    let mut setup = extended_stmt("c", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.extend_from_slice(&sync_frame());
    write_all(&mut s, &setup).await;
    let _ = drain_to_sync(&mut s).await;
    // The P0-001 reproducer: one Parse body, three statements.
    let mut batch = parse_frame("bad", "BEGIN; INSERT INTO t VALUES (1); COMMIT;");
    batch.extend_from_slice(&bind_frame("", "bad"));
    batch.extend_from_slice(&execute_frame(""));
    batch.extend_from_slice(&sync_frame());
    write_all(&mut s, &batch).await;
    let response = drain_to_sync(&mut s).await;
    assert!(
        has_error(
            &response,
            "cannot insert multiple commands into a prepared statement"
        ),
        "explicit rejection, got: {response:?}"
    );
    assert!(
        has_error(&response, "42601"),
        "PostgreSQL syntax-error SQLSTATE, got: {response:?}"
    );
    assert!(
        !tags(&response).contains(&b'1'),
        "no ParseComplete for the rejected body: {response:?}"
    );
    assert_eq!(
        ready_status(&response),
        Some(b'I'),
        "session idle: {response:?}"
    );
    // Nothing ran: no staged row, no open transaction.
    let mut check = extended_stmt("sel", "SELECT count(*) FROM t");
    check.extend_from_slice(&sync_frame());
    write_all(&mut s, &check).await;
    let rows = drain_to_sync(&mut s).await;
    assert_eq!(data_rows(&rows), 1);
    assert_eq!(first_cell_text(&rows), "0", "table still empty: {rows:?}");
}

#[tokio::test]
async fn extended_single_statement_with_trailing_semicolons_still_accepted() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    for sql in ["SELECT 1;", "SELECT 1;;", "SELECT ';'"] {
        let mut batch = extended_stmt("q", sql);
        batch.extend_from_slice(&sync_frame());
        write_all(&mut s, &batch).await;
        let response = drain_to_sync(&mut s).await;
        assert_eq!(data_rows(&response), 1, "{sql}: {response:?}");
        assert!(
            !has_error(&response, "multiple commands"),
            "{sql}: {response:?}"
        );
    }
}

#[tokio::test]
async fn extended_fragmented_delivery_executes_exactly_once() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    let mut batch = extended_stmt("c", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    batch.extend_from_slice(&extended_stmt("i", "INSERT INTO t VALUES (7)"));
    batch.extend_from_slice(&extended_stmt("sel", "SELECT id FROM t"));
    batch.extend_from_slice(&sync_frame());
    write_fragmented(&mut s, &batch).await;
    let response = drain_to_sync(&mut s).await;
    assert_eq!(data_rows(&response), 1, "{response:?}");
    assert_eq!(first_cell_text(&response), "7", "{response:?}");
}

#[tokio::test]
async fn extended_error_then_sync_recovers_and_txn_abort_contract_holds() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    let mut setup = extended_stmt("c", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.extend_from_slice(&sync_frame());
    write_all(&mut s, &setup).await;
    let _ = drain_to_sync(&mut s).await;
    // Pipeline with a failing middle command: BEGIN ok, bad INSERT errors.
    let mut batch = extended_stmt("b", "BEGIN");
    batch.extend_from_slice(&extended_stmt("x", "INSERT INTO t VALUES (1, 2, 3)"));
    batch.extend_from_slice(&sync_frame());
    write_all(&mut s, &batch).await;
    let response = drain_to_sync(&mut s).await;
    assert!(
        response.iter().any(|(t, _)| *t == b'E'),
        "error surfaced: {response:?}"
    );
    // Sync recovered the protocol; the failed statement aborted the txn, so
    // only ROLLBACK is accepted and nothing was written.
    let mut rollback = extended_stmt("r", "ROLLBACK");
    rollback.extend_from_slice(&sync_frame());
    write_all(&mut s, &rollback).await;
    let rolled = drain_to_sync(&mut s).await;
    assert_eq!(ready_status(&rolled), Some(b'I'), "{rolled:?}");
    let mut check = extended_stmt("sel", "SELECT count(*) FROM t");
    check.extend_from_slice(&sync_frame());
    write_all(&mut s, &check).await;
    let rows = drain_to_sync(&mut s).await;
    assert_eq!(first_cell_text(&rows), "0", "{rows:?}");
}

#[tokio::test]
async fn extended_disconnect_mid_frame_executes_nothing() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    let mut setup = extended_stmt("c", "CREATE TABLE t (id INTEGER PRIMARY KEY)");
    setup.extend_from_slice(&sync_frame());
    write_all(&mut s, &setup).await;
    let _ = drain_to_sync(&mut s).await;
    // Partial Parse frame (header + half the payload), then drop the socket.
    let full = parse_frame("i", "INSERT INTO t VALUES (99)");
    let partial = &full[..full.len() / 2];
    write_all(&mut s, partial).await;
    drop(s);
    // A fresh connection works and the partial command never ran.
    let mut s2 = session(addr).await;
    let mut check = extended_stmt("sel", "SELECT count(*) FROM t");
    check.extend_from_slice(&sync_frame());
    write_all(&mut s2, &check).await;
    let rows = drain_to_sync(&mut s2).await;
    assert_eq!(first_cell_text(&rows), "0", "{rows:?}");
}

#[tokio::test]
async fn simple_multi_statement_batch_still_works() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    // Simple-protocol batches (psql-style) keep the buffered path: the whole
    // BEGIN..COMMIT batch commits as one transaction.
    for sql in [
        "CREATE TABLE t (id INTEGER PRIMARY KEY)",
        "BEGIN; INSERT INTO t VALUES (5); COMMIT;",
    ] {
        write_all(&mut s, &simple_frame(sql)).await;
        let response = drain_to_sync(&mut s).await;
        assert!(
            !has_error(&response, "multiple commands"),
            "{sql}: {response:?}"
        );
    }
    let mut check = extended_stmt("sel", "SELECT id FROM t");
    check.extend_from_slice(&sync_frame());
    write_all(&mut s, &check).await;
    let rows = drain_to_sync(&mut s).await;
    assert_eq!(data_rows(&rows), 1, "{rows:?}");
}
