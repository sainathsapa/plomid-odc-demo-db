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
//! F-009 regression: a prepared ANN statement must execute its ANN pass once
//! per Execute, not once for the portal Describe and again for Execute.
//!
//! The test lives in its own binary because the ANN counters are
//! process-wide. Keeping it alone makes exact delta assertions deterministic.

use plomid_network::server::PlomidServer;
use plomid_network::session::ServerAuthConfig;
use plomid_txn::PlomidStorageEngine;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_path(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-ann-describe-{label}-{}-{}",
        std::process::id(),
        id
    ))
}

async fn start_test_server() -> std::net::SocketAddr {
    let storage_path = temp_path("store");
    let wal_path = temp_path("wal");
    let _ = std::fs::remove_file(&storage_path);
    let _ = std::fs::remove_file(&wal_path);
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));
    let server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::new("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    tokio::spawn(async move { server.run(engine).await });
    addr
}

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    tokio::net::TcpStream::connect(addr).await.expect("connect")
}

async fn send(stream: &mut tokio::net::TcpStream, data: &[u8]) {
    stream.write_all(data).await.expect("write");
    stream.flush().await.expect("flush");
}

fn front(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(1 + 4 + payload.len());
    message.push(tag);
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(payload);
    message
}

async fn read_msg(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await.expect("tag");
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("len");
    let n = u32::from_be_bytes(len) as usize - 4;
    let mut payload = vec![0u8; n];
    if n > 0 {
        stream.read_exact(&mut payload).await.expect("payload");
    }
    (tag[0], payload)
}

async fn handshake(stream: &mut tokio::net::TcpStream) {
    let startup = b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00";
    let mut len = [0u8; 4];
    len.copy_from_slice(&(startup.len() as u32 + 4).to_be_bytes());
    send(stream, &len).await;
    send(stream, startup).await;

    let (tag, payload) = read_msg(stream).await;
    assert_eq!(tag, b'R');
    let auth_code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    if auth_code == 3 {
        let password_msg = b"secret\x00";
        let mut password = Vec::with_capacity(1 + 4 + password_msg.len());
        password.push(b'p');
        password.extend_from_slice(&(password_msg.len() as u32 + 4).to_be_bytes());
        password.extend_from_slice(password_msg);
        send(stream, &password).await;
    }

    loop {
        let (tag, _) = read_msg(stream).await;
        if tag == b'Z' {
            break;
        }
        assert_ne!(tag, b'E', "handshake error");
    }
}

async fn simple(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let query = format!("{sql};\0");
    let mut message = vec![b'Q'];
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    send(stream, &message).await;
    let mut out = Vec::new();
    loop {
        let (tag, payload) = read_msg(stream).await;
        let done = tag == b'Z';
        out.push((tag, payload));
        if done {
            break;
        }
    }
    out
}

fn cell(msgs: &[(u8, Vec<u8>)]) -> String {
    let payload = &msgs
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .expect("data row")
        .1;
    let len = i32::from_be_bytes(payload[2..6].try_into().unwrap());
    assert!(len > 0, "expected a non-null counter");
    String::from_utf8(payload[6..6 + len as usize].to_vec()).expect("utf8 counter")
}

async fn ann_queries(stream: &mut tokio::net::TcpStream) -> i64 {
    let messages = simple(
        stream,
        "SELECT ann_queries_total FROM plomid_vector_stats()",
    )
    .await;
    assert!(
        !messages.iter().any(|(tag, _)| *tag == b'E'),
        "counter query failed"
    );
    cell(&messages).parse().expect("ann_queries_total is int8")
}

async fn drain_until_ready(stream: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    loop {
        let message = read_msg(stream).await;
        let done = message.0 == b'Z';
        out.push(message);
        if done {
            break;
        }
    }
    out
}

#[tokio::test]
async fn prepared_portal_describe_does_not_rerun_ann() {
    let addr = start_test_server().await;
    let mut stream = connect(addr).await;
    handshake(&mut stream).await;

    let setup = simple(&mut stream, "SET vector.auto_ann_min_rows = 0").await;
    assert!(!setup.iter().any(|(tag, _)| *tag == b'E'));
    let create = simple(
        &mut stream,
        "CREATE TABLE ann_desc (id INTEGER, e VECTOR(FLOAT32, 2))",
    )
    .await;
    assert!(!create.iter().any(|(tag, _)| *tag == b'E'));

    let rows = (0..300)
        .map(|id| format!("({id}, [{id}.0, {}])", id + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let insert = simple(&mut stream, &format!("INSERT INTO ann_desc VALUES {rows}")).await;
    assert!(!insert.iter().any(|(tag, _)| *tag == b'E'));
    let index = simple(
        &mut stream,
        "CREATE INDEX ann_desc_hnsw ON ann_desc USING HNSW (e)",
    )
    .await;
    assert!(!index.iter().any(|(tag, _)| *tag == b'E'));

    let before_describe = ann_queries(&mut stream).await;

    // Named Parse + statement Describe: the one legitimate shape probe.
    let mut parse = Vec::new();
    parse.extend_from_slice(b"ann_stmt\0");
    parse.extend_from_slice(b"SELECT id FROM ann_desc ORDER BY e <-> [0, 0] LIMIT 5\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send(&mut stream, &front(b'P', &parse)).await;
    send(&mut stream, &front(b'D', b"Sann_stmt\0")).await;
    send(&mut stream, &front(b'S', &[])).await;
    let describe = drain_until_ready(&mut stream).await;
    assert!(
        !describe.iter().any(|(tag, _)| *tag == b'E'),
        "statement Describe failed"
    );
    assert!(
        describe.iter().any(|(tag, _)| *tag == b'T'),
        "statement Describe returned no RowDescription"
    );
    let after_describe = ann_queries(&mut stream).await;
    assert_eq!(
        after_describe - before_describe,
        1,
        "the statement Describe must run exactly one shape probe"
    );

    // Every execution re-Describes its portal. The cached shape must keep
    // that Describe metadata-only: only Execute may touch the ANN graph.
    for round in 0..3 {
        let before = ann_queries(&mut stream).await;
        let mut bind = Vec::new();
        bind.extend_from_slice(b"\0ann_stmt\0");
        bind.extend_from_slice(&0u16.to_be_bytes());
        bind.extend_from_slice(&0u16.to_be_bytes());
        bind.extend_from_slice(&0u16.to_be_bytes());
        send(&mut stream, &front(b'B', &bind)).await;
        send(&mut stream, &front(b'D', b"P\0")).await;
        send(&mut stream, &front(b'E', b"\0\0\0\0\0")).await;
        send(&mut stream, &front(b'S', &[])).await;
        let messages = drain_until_ready(&mut stream).await;
        assert!(
            !messages.iter().any(|(tag, _)| *tag == b'E'),
            "round {round}: execute failed"
        );
        let rows = messages.iter().filter(|(tag, _)| *tag == b'D').count();
        assert_eq!(rows, 5, "round {round}: wrong result size");
        let after = ann_queries(&mut stream).await;
        assert_eq!(
            after - before,
            1,
            "round {round}: portal Describe must not re-run the ANN pass"
        );
    }
}
