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
//! Numeric arithmetic over the live PGWire path (P0-002 regression).
//!
//! A decimal spelling is floating-point even when integral (`3.0 / 2.0` is
//! 1.5), and the declared column type must describe the value the client
//! parses (a float text behind an int8 OID corrupts the client's decoding).
//! These tests assert wire values *and* OIDs through a running server, over
//! both simple and extended protocol paths, including bound parameters.

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
        "plomid-numwire-{label}-{}-{id}",
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

async fn simple(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    let message = frame(b'Q', &payload);
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    drain_to_sync(stream).await
}

async fn extended(stream: &mut tokio::net::TcpStream, name: &str, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut parse = Vec::new();
    parse.extend_from_slice(name.as_bytes());
    parse.push(0);
    parse.extend_from_slice(sql.as_bytes());
    parse.push(0);
    parse.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&frame(b'P', &parse));
    let mut bind = Vec::new();
    bind.extend_from_slice(name.as_bytes());
    bind.push(0);
    bind.extend_from_slice(name.as_bytes());
    bind.push(0);
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&frame(b'B', &bind));
    let mut exec = Vec::new();
    exec.extend_from_slice(name.as_bytes());
    exec.push(0);
    exec.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&frame(b'E', &exec));
    out.extend_from_slice(&frame(b'S', &[]));
    stream.write_all(&out).await.expect("write");
    stream.flush().await.expect("flush");
    drain_to_sync(stream).await
}

/// (column OIDs, first-row cell texts) of a result.
fn shape(response: &[(u8, Vec<u8>)]) -> (Vec<u32>, Vec<String>) {
    assert!(
        !response.iter().any(|(t, _)| *t == b'E'),
        "unexpected error: {response:?}"
    );
    let row_desc = response
        .iter()
        .find(|(t, _)| *t == b'T')
        .expect("RowDescription");
    let fields = u16::from_be_bytes([row_desc.1[0], row_desc.1[1]]) as usize;
    let mut oids = Vec::new();
    let mut pos = 2;
    for _ in 0..fields {
        let nul = row_desc.1[pos..]
            .iter()
            .position(|b| *b == 0)
            .expect("name");
        pos += nul + 1;
        pos += 4; // table oid
        pos += 2; // column
        oids.push(u32::from_be_bytes(
            row_desc.1[pos..pos + 4].try_into().unwrap(),
        ));
        pos += 4 + 2 + 4 + 2;
    }
    let mut cells = Vec::new();
    for (_, payload) in response.iter().filter(|(t, _)| *t == b'D') {
        let cols = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        let mut at = 2;
        for _ in 0..cols {
            let len = i32::from_be_bytes(payload[at..at + 4].try_into().unwrap());
            at += 4;
            if len < 0 {
                cells.push("NULL".to_string());
            } else {
                let text = String::from_utf8_lossy(&payload[at..at + len as usize]).into_owned();
                at += len as usize;
                cells.push(text);
            }
        }
    }
    (oids, cells)
}

const INT8: u32 = 20;
const FLOAT8: u32 = 701;

#[tokio::test]
async fn wire_float_division_value_and_type() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    // The P0-002 reproducer, over both protocol paths.
    for sql in ["SELECT 3.0 / 2.0", "SELECT 10.0 / 4.0", "SELECT 1.5 / 2"] {
        let (oids, cells) = shape(&simple(&mut s, sql).await);
        assert_eq!(oids, vec![FLOAT8], "{sql}: float8 OID");
        let got: f64 = cells[0].parse().expect("float text");
        let expected = if sql.contains("10.0") {
            2.5
        } else if sql.contains("1.5") {
            0.75
        } else {
            1.5
        };
        assert!((got - expected).abs() < 1e-9, "{sql}: {cells:?}");
        let (oids, cells) = shape(&extended(&mut s, "q", sql).await);
        assert_eq!(oids, vec![FLOAT8], "{sql} extended: float8 OID");
        let got: f64 = cells[0].parse().expect("float text");
        assert!((got - expected).abs() < 1e-9, "{sql} extended: {cells:?}");
    }
}

#[tokio::test]
async fn wire_integer_division_contract_unchanged() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    for (sql, expected) in [
        ("SELECT 3 / 2", "1"),
        ("SELECT 7 % 3", "1"),
        ("SELECT 1 + 1", "2"),
    ] {
        let (oids, cells) = shape(&simple(&mut s, sql).await);
        assert_eq!(oids, vec![INT8], "{sql}: int8 OID");
        assert_eq!(cells, vec![expected.to_string()], "{sql}");
        let (oids, cells) = shape(&extended(&mut s, "q", sql).await);
        assert_eq!(oids, vec![INT8], "{sql} extended: int8 OID");
        assert_eq!(cells, vec![expected.to_string()], "{sql} extended");
    }
}

#[tokio::test]
async fn wire_float_add_multiply_modulo_and_null() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    for (sql, expected) in [
        ("SELECT 1.5 + 1", 2.5f64),
        ("SELECT 2.5 * 2", 5.0),
        ("SELECT 7.5 - 2.0", 5.5),
        ("SELECT 5.5 % 2", 1.5),
        ("SELECT -7.5 / 2", -3.75),
        ("SELECT (1 + 2) * 3.0", 9.0),
    ] {
        let (oids, cells) = shape(&extended(&mut s, "q", sql).await);
        assert_eq!(oids, vec![FLOAT8], "{sql}");
        let got: f64 = cells[0].parse().expect("float text");
        assert!((got - expected).abs() < 1e-9, "{sql}: {cells:?}");
    }
    let (_, cells) = shape(&simple(&mut s, "SELECT NULL + 1").await);
    assert_eq!(cells, vec!["NULL".to_string()]);
}

#[tokio::test]
async fn wire_division_by_zero_errors_without_panic() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    for sql in ["SELECT 1 / 0", "SELECT 1.5 / 0"] {
        let response = simple(&mut s, sql).await;
        assert!(
            response
                .iter()
                .any(|(t, p)| *t == b'E' && String::from_utf8_lossy(p).contains("division by zero")),
            "{sql}: {response:?}"
        );
    }
    // The session still serves after the errors.
    let (_, cells) = shape(&simple(&mut s, "SELECT 1").await);
    assert_eq!(cells, vec!["1".to_string()]);
}

#[tokio::test]
async fn wire_prepared_statement_with_param_uses_float_semantics() {
    let addr = start_server().await;
    let mut s = session(addr).await;
    // Extended Parse with a `$1` placeholder, then Bind carries the value:
    // the same operator semantics as literals must apply.
    let mut parse = Vec::new();
    parse.extend_from_slice(b"p\x00");
    parse.extend_from_slice(b"SELECT $1 / 2.0\x00");
    parse.extend_from_slice(&0u16.to_be_bytes());
    let mut bind = Vec::new();
    bind.extend_from_slice(b"\x00");
    bind.extend_from_slice(b"p\x00");
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&3i32.to_be_bytes());
    bind.extend_from_slice(b"3.0");
    bind.extend_from_slice(&0u16.to_be_bytes());
    let mut exec = Vec::new();
    exec.extend_from_slice(b"\x00");
    exec.extend_from_slice(&0i32.to_be_bytes());
    let mut out = frame(b'P', &parse);
    out.extend_from_slice(&frame(b'B', &bind));
    out.extend_from_slice(&frame(b'E', &exec));
    out.extend_from_slice(&frame(b'S', &[]));
    s.write_all(&out).await.expect("write");
    s.flush().await.expect("flush");
    let response = drain_to_sync(&mut s).await;
    let (oids, cells) = shape(&response);
    assert_eq!(oids, vec![FLOAT8], "{response:?}");
    let got: f64 = cells[0].parse().expect("float text");
    assert!((got - 1.5).abs() < 1e-9, "{response:?}");
}
