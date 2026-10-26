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
// Vector wire-contract tests (Gap C, wire half).
//
// SQL → parser → planner → execution → result → PGWire encoding, and
// PGWire request → SQL → vector execution → PGWire response, over a real
// server: valid/invalid/NULL/empty requests, text params, both result
// formats, binary-param refusal, error behavior. Engine equivalence
// (ANN == exact) stays in executor/tests/vector_*.rs.

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
        "plomid-vector-wire-{label}-{}-{}",
        std::process::id(),
        id
    ))
}

async fn start_test_server() -> (Arc<Mutex<PlomidStorageEngine>>, std::net::SocketAddr) {
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
    {
        let engine = engine.clone();
        tokio::spawn(async move { server.run(engine).await });
    }
    (engine, addr)
}

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    tokio::net::TcpStream::connect(addr).await.expect("connect")
}

async fn send(stream: &mut tokio::net::TcpStream, data: &[u8]) {
    stream.write_all(data).await.expect("write");
    stream.flush().await.expect("flush");
}

fn front(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut m = Vec::with_capacity(1 + 4 + payload.len());
    m.push(tag);
    m.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    m.extend_from_slice(payload);
    m
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
    let q = format!("{sql};\x00");
    let mut m = vec![b'Q'];
    m.extend_from_slice(&(q.len() as u32 + 4).to_be_bytes());
    m.extend_from_slice(q.as_bytes());
    send(stream, &m).await;
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

fn data_rows(msgs: &[(u8, Vec<u8>)]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter(|(t, _)| *t == b'D')
        .map(|(_, p)| {
            let n = u16::from_be_bytes([p[0], p[1]]) as usize;
            let mut pos = 2;
            let mut vals = Vec::with_capacity(n);
            for _ in 0..n {
                let len = i32::from_be_bytes(p[pos..pos + 4].try_into().unwrap());
                pos += 4;
                if len < 0 {
                    vals.push(None);
                } else {
                    let end = pos + len as usize;
                    vals.push(Some(String::from_utf8(p[pos..end].to_vec()).unwrap()));
                    pos = end;
                }
            }
            vals
        })
        .collect()
}

fn rowdesc(msgs: &[(u8, Vec<u8>)]) -> Vec<(String, u32)> {
    let payload = &msgs
        .iter()
        .find(|(t, _)| *t == b'T')
        .expect("RowDescription")
        .1;
    let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut pos = 2;
    let mut fields = Vec::new();
    for _ in 0..n {
        let end = payload[pos..].iter().position(|b| *b == 0).unwrap() + pos;
        let name = String::from_utf8(payload[pos..end].to_vec()).unwrap();
        pos = end + 1 + 4 + 2; // table oid + attr number
        let oid = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        fields.push((name, oid));
        pos += 4 + 2 + 4 + 2; // oid + size + typmod + format
    }
    fields
}

fn has_error(msgs: &[(u8, Vec<u8>)]) -> bool {
    msgs.iter().any(|(t, _)| *t == b'E')
}

#[tokio::test]
async fn vector_simple_protocol_roundtrip() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;
    let setup = simple(
        &mut s,
        "CREATE TABLE wvec (id INTEGER, e VECTOR(FLOAT32, 2))",
    )
    .await;
    assert!(!has_error(&setup), "create table");
    let ins = simple(
        &mut s,
        "INSERT INTO wvec VALUES (1, [1,2]), (2, [0,1]), (3, NULL)",
    )
    .await;
    assert!(!has_error(&ins), "insert literals + NULL");
    let res = simple(&mut s, "SELECT id, e FROM wvec ORDER BY id").await;
    assert!(!has_error(&res));
    // RowDescription carries the vector OID on the embedding column.
    let fields = rowdesc(&res);
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[1].1, 16384, "embedding column OID must be VECTOR");
    eprintln!(
        "msgs: {:?}",
        res.iter()
            .map(|(t, p)| (*t as char, p.len()))
            .collect::<Vec<_>>()
    );
    for (t, pay) in &res {
        if *t == b'D' {
            eprintln!("D payload: {pay:?}");
        }
    }
    let rows = data_rows(&res);
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        vec![Some("1".to_string()), Some("[1,2]".to_string())]
    );
    // NULL stays NULL on the wire.
    assert_eq!(rows[2][1], None);
}

#[tokio::test]
async fn vector_text_param_and_distance_topk() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;
    assert!(!has_error(
        &simple(&mut s, "CREATE TABLE wq (id INTEGER, e VECTOR(FLOAT32, 2))").await
    ));
    // Extended protocol, text $1 param.
    let mut parse = Vec::new();
    parse.extend_from_slice(b"ins\0INSERT INTO wq(id, e) VALUES ($1, $2)\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send(&mut s, &front(b'P', &parse)).await;
    for id in 1..=2i32 {
        let v = if id == 1 { "[1,0]" } else { "[0,1]" };
        let mut bind = Vec::new();
        bind.extend_from_slice(b"\0ins\0");
        bind.extend_from_slice(&0u16.to_be_bytes()); // all-text params
        bind.extend_from_slice(&2u16.to_be_bytes());
        for value in [id.to_string(), v.to_string()] {
            bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bind.extend_from_slice(value.as_bytes());
        }
        bind.extend_from_slice(&0u16.to_be_bytes()); // text results
        send(&mut s, &front(b'B', &bind)).await;
        send(&mut s, &front(b'E', b"\0\0\0\0\0")).await;
        send(&mut s, &front(b'S', &[])).await;
        let mut ok = false;
        loop {
            let (tag, _) = read_msg(&mut s).await;
            if tag == b'C' {
                ok = true;
            }
            if tag == b'Z' {
                break;
            }
            assert_ne!(tag, b'E', "bind/execute error");
        }
        assert!(ok);
    }
    // Distance Top-K over the wire.
    let res = simple(&mut s, "SELECT id FROM wq ORDER BY e <-> [1,0] LIMIT 1").await;
    assert!(!has_error(&res));
    assert_eq!(data_rows(&res), vec![vec![Some("1".to_string())]]);
    // Radius query over the wire.
    let res = simple(
        &mut s,
        "SELECT id FROM wq WHERE l2_distance(e, [1,0]) < 0.5",
    )
    .await;
    assert!(!has_error(&res));
    assert_eq!(data_rows(&res), vec![vec![Some("1".to_string())]]);
}

#[tokio::test]
async fn vector_binary_result_format_decodes() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;
    assert!(!has_error(
        &simple(&mut s, "CREATE TABLE wb (id INTEGER, e VECTOR(FLOAT32, 2))").await
    ));
    assert!(!has_error(
        &simple(&mut s, "INSERT INTO wb VALUES (1, [1,2])").await
    ));
    // Extended: text params, binary results (format 1).
    let mut parse = Vec::new();
    parse.extend_from_slice(b"sel\0SELECT e FROM wb WHERE id = $1\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send(&mut s, &front(b'P', &parse)).await;
    let mut bind = Vec::new();
    bind.extend_from_slice(b"\0sel\0");
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1i32.to_be_bytes());
    bind.extend_from_slice(b"1");
    bind.extend_from_slice(&1u16.to_be_bytes()); // one result column
    bind.extend_from_slice(&1u16.to_be_bytes()); // binary format
    send(&mut s, &front(b'B', &bind)).await;
    // Describe to observe the RowDescription, then Execute.
    send(&mut s, &front(b'D', b"Ssel\0")).await;
    send(&mut s, &front(b'E', b"\0\0\0\0\0")).await;
    send(&mut s, &front(b'S', &[])).await;
    let mut saw_t = false;
    let mut raw_d: Vec<u8> = Vec::new();
    loop {
        let (tag, payload) = read_msg(&mut s).await;
        if tag == b'T' {
            saw_t = true;
            let n = u16::from_be_bytes([payload[0], payload[1]]);
            assert_eq!(n, 1);
        }
        if tag == b'D' {
            // Binary DataRow: count + len + 8 payload bytes (2x f32 LE).
            let n = u16::from_be_bytes([payload[0], payload[1]]);
            assert_eq!(n, 1);
            let len = i32::from_be_bytes(payload[2..6].try_into().unwrap());
            assert_eq!(len, 8, "dim-2 float32 binary payload");
            raw_d = payload[6..].to_vec();
        }
        if tag == b'Z' {
            break;
        }
        assert_ne!(tag, b'E', "binary select error");
    }
    assert!(saw_t && raw_d.len() == 8);
    let x = f32::from_le_bytes(raw_d[0..4].try_into().unwrap());
    let y = f32::from_le_bytes(raw_d[4..8].try_into().unwrap());
    assert!((x - 1.0).abs() < 1e-6 && (y - 2.0).abs() < 1e-6);
}

#[tokio::test]
async fn vector_binary_param_refused_fail_closed() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;
    assert!(!has_error(
        &simple(
            &mut s,
            "CREATE TABLE wbp (id INTEGER, e VECTOR(FLOAT32, 2))"
        )
        .await
    ));
    // Binary param format for a vector value must fail closed (text only).
    let mut parse = Vec::new();
    parse.extend_from_slice(b"ins\0INSERT INTO wbp(id, e) VALUES ($1, $2)\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send(&mut s, &front(b'P', &parse)).await;
    let mut bind = Vec::new();
    bind.extend_from_slice(b"\0ins\0");
    bind.extend_from_slice(&1u16.to_be_bytes()); // one param format...
    bind.extend_from_slice(&1u16.to_be_bytes()); // ...binary
    bind.extend_from_slice(&2u16.to_be_bytes());
    bind.extend_from_slice(&1i32.to_be_bytes());
    bind.extend_from_slice(b"1");
    bind.extend_from_slice(&8i32.to_be_bytes());
    bind.extend_from_slice(&[0u8; 8]);
    bind.extend_from_slice(&0u16.to_be_bytes());
    send(&mut s, &front(b'B', &bind)).await;
    send(&mut s, &front(b'S', &[])).await;
    let mut saw_e = false;
    loop {
        let (tag, payload) = read_msg(&mut s).await;
        if tag == b'E' {
            saw_e = true;
            // Fail-closed refusal (message names the binary-parameter rule).
            let text = String::from_utf8_lossy(&payload);
            assert!(
                text.contains("binary"),
                "refusal must name binary parameters: {text}"
            );
        }
        if tag == b'Z' {
            break;
        }
    }
    assert!(saw_e, "binary vector param must fail closed");
    // And nothing was written by the refused Bind.
    let res = simple(&mut s, "SELECT COUNT(*) FROM wbp").await;
    assert!(!has_error(&res));
    assert_eq!(data_rows(&res), vec![vec![Some("0".to_string())]]);
}

#[tokio::test]
async fn vector_invalid_null_empty_on_wire() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;
    assert!(!has_error(
        &simple(&mut s, "CREATE TABLE wi (id INTEGER, e VECTOR(FLOAT32, 2))").await
    ));
    // Invalid literal fails (VECTOR_* message, statement fails, table intact).
    let bad = simple(&mut s, "INSERT INTO wi VALUES (1, [NaN,0])").await;
    assert!(has_error(&bad));
    let bad = simple(&mut s, "INSERT INTO wi VALUES (1, [1,2,3])").await;
    assert!(has_error(&bad), "dimension mismatch must fail");
    // A valid row, then a mismatched ORDER BY must fail (non-vacuous).
    assert!(!has_error(
        &simple(&mut s, "INSERT INTO wi VALUES (1, [1,2])").await
    ));
    // Invalid metric function fails.
    let bad = simple(&mut s, "SELECT id FROM wi ORDER BY e <=> [1,2,3] LIMIT 1").await;
    assert!(has_error(&bad));
    // Empty result is valid-empty, not an error.
    let res = simple(
        &mut s,
        "SELECT id FROM wi WHERE l2_distance(e, [9,9]) < 0.001",
    )
    .await;
    assert!(!has_error(&res));
    assert!(data_rows(&res).is_empty());
    // Only the one valid row survived the failed writes.
    let res = simple(&mut s, "SELECT COUNT(*) FROM wi").await;
    assert_eq!(data_rows(&res), vec![vec![Some("1".to_string())]]);
}
