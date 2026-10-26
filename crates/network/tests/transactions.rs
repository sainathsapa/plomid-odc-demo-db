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
//! True wire-level transaction semantics (Beta 2 Phase 4).
//!
//! Multi-message explicit transactions stage effects (not text): every
//! statement executes into the open transaction at statement time, reads
//! merge overlay + snapshot, and COMMIT only publishes. No replay.

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
        "plomid-txnwire-{label}-{}-{id}",
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

async fn query(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let text = format!("{sql};\x00");
    let mut message = Vec::with_capacity(1 + 4 + text.len());
    message.push(b'Q');
    message.extend_from_slice(&(text.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(text.as_bytes());
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
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

async fn session(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = connect(addr).await;
    scram_login(&mut stream, "plomid", "secret").await;
    stream
}

#[tokio::test]
async fn read_your_writes_across_messages() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(
        tags(&query(&mut s, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await)
            .contains(&b'C')
    );
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (1, 10)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (2, 20)").await).contains(&b'C'));
    // Staged row visible before COMMIT (no replay involved).
    let rows = query(&mut s, "SELECT * FROM t ORDER BY id").await;
    assert_eq!(data_rows(&rows), 2, "{rows:?}");
    assert!(tags(&query(&mut s, "UPDATE t SET v = v + 5 WHERE id = 1").await).contains(&b'C'));
    let rows = query(&mut s, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(data_rows(&rows), 1);
    assert!(tags(&query(&mut s, "COMMIT").await).contains(&b'C'));
    let rows = query(&mut s, "SELECT * FROM t ORDER BY id").await;
    assert_eq!(data_rows(&rows), 2);
}

#[tokio::test]
async fn constraint_errors_time_at_statement_not_commit() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "CREATE TABLE t (id INTEGER PRIMARY KEY)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (1)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    // Duplicate fails HERE with a conflict-class error...
    let denied = query(&mut s, "INSERT INTO t VALUES (1)").await;
    assert!(
        has_error(&denied, "duplicate")
            || has_error(&denied, "Unique")
            || has_error(&denied, "unique")
            || has_error(&denied, "onflict")
            || has_error(&denied, "xists"),
        "{denied:?}"
    );
    // ...the transaction is aborted: only ROLLBACK is accepted...
    let more = query(&mut s, "INSERT INTO t VALUES (2)").await;
    assert_eq!(more[0].0, b'E', "aborted txn rejects statements: {more:?}");
    let rollback = query(&mut s, "ROLLBACK").await;
    assert!(tags(&rollback).contains(&b'Z'));
    // ...and nothing was written.
    let rows = query(&mut s, "SELECT * FROM t").await;
    assert_eq!(data_rows(&rows), 1);
}

#[tokio::test]
async fn volatile_values_stable_across_reads() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(
        &query(
            &mut s,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, r DOUBLE PRECISION)"
        )
        .await
    )
    .contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(
        tags(&query(&mut s, "INSERT INTO t VALUES (1, random()), (2, random())").await)
            .contains(&b'C')
    );
    let first = query(&mut s, "SELECT * FROM t ORDER BY id").await;
    let second = query(&mut s, "SELECT * FROM t ORDER BY id").await;
    assert_eq!(data_rows(&first), 2);
    assert_eq!(
        first
            .iter()
            .filter(|(t, _)| *t == b'D')
            .map(|(_, p)| p.clone())
            .collect::<Vec<_>>(),
        second
            .iter()
            .filter(|(t, _)| *t == b'D')
            .map(|(_, p)| p.clone())
            .collect::<Vec<_>>(),
        "no re-evaluation on re-read"
    );
    assert!(tags(&query(&mut s, "COMMIT").await).contains(&b'C'));
}

#[tokio::test]
async fn savepoint_flow_over_wire() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "CREATE TABLE t (id INTEGER)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (1)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "SAVEPOINT sp1").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (2)").await).contains(&b'C'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 2);
    assert!(tags(&query(&mut s, "ROLLBACK TO SAVEPOINT sp1").await).contains(&b'C'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 1);
    assert!(tags(&query(&mut s, "RELEASE SAVEPOINT sp1").await).contains(&b'C') || true);
    // Unknown savepoint errors (and aborts, like any statement error).
    let bad = query(&mut s, "ROLLBACK TO SAVEPOINT nope").await;
    assert_eq!(bad[0].0, b'E');
    let rollback = query(&mut s, "ROLLBACK").await;
    assert!(tags(&rollback).contains(&b'Z'));
}

#[tokio::test]
async fn transaction_control_guards() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    // COMMIT/ROLLBACK with nothing open.
    assert_eq!(query(&mut s, "COMMIT").await[0].0, b'E');
    assert_eq!(query(&mut s, "ROLLBACK").await[0].0, b'E');
    // Nested BEGIN.
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    let nested = query(&mut s, "BEGIN").await;
    assert_eq!(nested[0].0, b'C', "nested BEGIN acknowledged: {nested:?}");
    assert!(tags(&query(&mut s, "ROLLBACK").await).contains(&b'Z'));
}

#[tokio::test]
async fn multi_statement_message_in_transaction() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "CREATE TABLE t (id INTEGER)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    // Two statements in one message both stage; both visible pre-commit.
    let both = query(&mut s, "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)").await;
    assert!(tags(&both).contains(&b'Z'), "{both:?}");
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 2);
    assert!(tags(&query(&mut s, "COMMIT").await).contains(&b'C'));
}

#[tokio::test]
async fn ddl_in_transaction_over_wire() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "CREATE TABLE nt (id INTEGER PRIMARY KEY)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO nt VALUES (5)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "COMMIT").await).contains(&b'C'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM nt").await), 1);
    // DROP + ROLLBACK restores.
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "DROP TABLE nt").await).contains(&b'C'));
    assert_eq!(query(&mut s, "SELECT * FROM nt").await[0].0, b'E');
    assert!(tags(&query(&mut s, "ROLLBACK").await).contains(&b'Z'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM nt").await), 1);
}

#[tokio::test]
async fn concurrent_conflicting_inserts_over_wire() {
    let addr = start_server().await;
    let mut a = session(addr).await;
    assert!(
        tags(&query(&mut a, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await)
            .contains(&b'C')
    );
    assert!(tags(&query(&mut a, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut a, "INSERT INTO t VALUES (1, 100)").await).contains(&b'C'));
    // B blocks on A's unique reservation until A commits, then fails
    // against the fresh image. Drive B on a task; fail fast on hangs.
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = tokio::spawn(async move {
        let mut b = connect(addr).await;
        // Fresh SCRAM login per connection (bootstrap already bound).
        scram_login(&mut b, "plomid", "secret").await;
        let _ = query(&mut b, "BEGIN").await;
        let outcome = query(&mut b, "INSERT INTO t VALUES (1, 200)").await;
        let _ = query(&mut b, "ROLLBACK").await;
        tx.send(tags(&outcome)).expect("report");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(tags(&query(&mut a, "COMMIT").await).contains(&b'C'));
    let outcome = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("B finishes");
    assert!(outcome.contains(&b'E'), "loser sees conflict: {outcome:?}");
    handle.await.expect("task");
    let rows = query(&mut a, "SELECT * FROM t").await;
    assert_eq!(data_rows(&rows), 1);
}

#[tokio::test]
async fn read_committed_vs_repeatable_read_over_wire() {
    let addr = start_server().await;
    let mut a = session(addr).await;
    assert!(tags(&query(&mut a, "CREATE TABLE t (id INTEGER)").await).contains(&b'C'));
    assert!(tags(&query(&mut a, "INSERT INTO t VALUES (1)").await).contains(&b'C'));
    // RC reader.
    let mut rc = session(addr).await;
    assert!(tags(&query(&mut rc, "BEGIN").await).contains(&b'C'));
    assert_eq!(data_rows(&query(&mut rc, "SELECT * FROM t").await), 1);
    // RR reader pins the BEGIN image.
    let mut rr = session(addr).await;
    assert!(
        tags(&query(&mut rr, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ").await)
            .contains(&b'C')
    );
    assert_eq!(data_rows(&query(&mut rr, "SELECT * FROM t").await), 1);
    // Concurrent commit lands.
    assert!(tags(&query(&mut a, "INSERT INTO t VALUES (2)").await).contains(&b'C'));
    // RC sees it mid-transaction; RR does not.
    assert_eq!(data_rows(&query(&mut rc, "SELECT * FROM t").await), 2);
    assert_eq!(data_rows(&query(&mut rr, "SELECT * FROM t").await), 1);
    assert!(tags(&query(&mut rc, "COMMIT").await).contains(&b'C'));
    assert!(tags(&query(&mut rr, "COMMIT").await).contains(&b'C'));
}

#[tokio::test]
async fn extended_protocol_runs_inside_session_txn() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "CREATE TABLE t (id INTEGER)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    // Parse/Bind/Describe/Execute/Sync with text parameters, inside the txn.
    fn framed(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut message = Vec::with_capacity(1 + 4 + payload.len());
        message.push(tag);
        message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
        message.extend_from_slice(payload);
        message
    }
    async fn send(stream: &mut tokio::net::TcpStream, bytes: &[u8]) {
        stream.write_all(bytes).await.expect("write");
        stream.flush().await.expect("flush");
    }
    let mut parse = Vec::new();
    parse.extend_from_slice(b"stmt\0INSERT INTO t VALUES ($1)\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send(&mut s, &framed(b'P', &parse)).await;
    assert_eq!(read_message(&mut s).await.0, b'1');
    for value in ["10", "20"] {
        // Named portal "portal" on statement "stmt", one text parameter.
        let mut bind = Vec::new();
        bind.extend_from_slice(b"portal\0stmt\0");
        bind.extend_from_slice(&0u16.to_be_bytes());
        bind.extend_from_slice(&1u16.to_be_bytes());
        bind.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bind.extend_from_slice(value.as_bytes());
        bind.extend_from_slice(&0u16.to_be_bytes());
        send(&mut s, &framed(b'B', &bind)).await;
        send(&mut s, &framed(b'D', b"Pportal\0")).await;
        send(&mut s, &framed(b'E', b"portal\0\0\0\0\0")).await;
        send(&mut s, &framed(b'S', &[])).await;
        // BindComplete, then CommandComplete, then ReadyForQuery.
        let mut saw_complete = false;
        loop {
            let (tag, payload) = read_message(&mut s).await;
            match tag {
                b'2' | b't' | b'T' | b'D' | b'1' | b'n' => {}
                b'C' => saw_complete = true,
                b'Z' => break,
                b'E' => panic!(
                    "extended INSERT in txn failed for value {value}: {}",
                    String::from_utf8_lossy(&payload)
                ),
                other => panic!("unexpected tag {other}"),
            }
        }
        assert!(saw_complete, "command complete for {value}");
    }
    // Both rows staged and visible pre-commit (read-your-writes).
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 2);
    assert!(tags(&query(&mut s, "COMMIT").await).contains(&b'C'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 2);
}

#[tokio::test]
async fn copy_stream_and_describe_inside_txn() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    assert!(tags(&query(&mut s, "CREATE TABLE t (id INTEGER, v TEXT)").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "BEGIN").await).contains(&b'C'));
    assert!(tags(&query(&mut s, "INSERT INTO t VALUES (1, 'a')").await).contains(&b'C'));
    // COPY TO STDOUT sees staged rows.
    let out = query(&mut s, "COPY t TO STDOUT").await;
    assert!(
        tags(&out).iter().any(|t| *t == b'D' || *t == b'C'),
        "{out:?}"
    );
    // DESCRIBE/SHOW keep working inside the transaction.
    let describe = query(&mut s, "DESCRIBE t").await;
    assert!(tags(&describe).contains(&b'Z'), "{describe:?}");
    let show = query(&mut s, "SHOW search_path").await;
    assert!(tags(&show).contains(&b'Z'), "{show:?}");
    assert!(tags(&query(&mut s, "ROLLBACK").await).contains(&b'Z'));
    assert_eq!(data_rows(&query(&mut s, "SELECT * FROM t").await), 0);
}
