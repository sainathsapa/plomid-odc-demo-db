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
//! Audited-crypto + MD5-deprecation tests (Beta 2 Phase 3).
//!
//! The primitive vectors live in `plomid-security` unit tests (FIPS/RFC
//! vectors against the audited crates). These wire tests prove the
//! deployment behavior: the MD5 downgrade funnel (bootstrap once, then
//! SCRAM-only), downgrade refusal on SCRAM listeners, and legacy-use
//! observability.

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
    std::env::temp_dir().join(format!("plomid-crypto-{label}-{}-{id}", std::process::id()))
}

async fn start_server(auth: ServerAuthConfig) -> std::net::SocketAddr {
    let storage_path = temp_path("engine");
    let wal_path = temp_path("engine-wal");
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));
    let server = PlomidServer::bind_ephemeral(auth)
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

/// Drives an MD5 exchange to ReadyForQuery. Err carries the server error.
async fn md5_login(
    stream: &mut tokio::net::TcpStream,
    user: &str,
    password: &str,
) -> Result<(), String> {
    send_startup(stream, user).await;
    let (tag, payload) = read_message(stream).await;
    if tag == b'E' {
        return Err(String::from_utf8_lossy(&payload).into_owned());
    }
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 5);
    let salt: [u8; 4] = payload[4..8].try_into().expect("md5 salt");
    // Client side of the legacy exchange, via the same audited path the
    // server verifies with (test-only use of the response helper).
    let response = plomid_security::md5_password_response(password, user, salt);
    let mut message = Vec::with_capacity(1 + 4 + response.len() + 1);
    message.push(b'p');
    message.extend_from_slice(&((response.len() as u32) + 5).to_be_bytes());
    message.extend_from_slice(response.as_bytes());
    message.push(0);
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    loop {
        let (tag, payload) = read_message(stream).await;
        match tag {
            b'Z' => return Ok(()),
            b'E' => return Err(String::from_utf8_lossy(&payload).into_owned()),
            _ => {}
        }
    }
}

#[tokio::test]
async fn md5_bootstrap_then_scram_only_funnel() {
    let addr = start_server(ServerAuthConfig::md5("plomid", "secret").expect("CSPRNG")).await;
    let before = plomid_core::observability::snapshot();
    // First MD5 login binds the bootstrap secret (legacy downgrade path).
    let mut first = connect(addr).await;
    md5_login(&mut first, "plomid", "secret")
        .await
        .expect("md5 bootstrap");
    drop(first);
    let after_bootstrap = plomid_core::observability::snapshot();
    assert!(after_bootstrap.legacy_auth_uses > before.legacy_auth_uses);
    // The role now holds a verifier: MD5 is refused from here on (the
    // server never sees plaintext again), SCRAM takes over with the same
    // password.
    let mut second = connect(addr).await;
    let err = md5_login(&mut second, "plomid", "secret")
        .await
        .expect_err("MD5 must fail after upgrade");
    assert!(err.contains("28P01"), "{err}");
}

#[tokio::test]
async fn unsolicited_md5_response_is_refused_on_scram_listeners() {
    // Downgrade refusal: a SCRAM listener never issues an MD5 challenge; a
    // client blurting an MD5-form response into the SASL exchange gets a
    // protocol error, never a session.
    let addr = start_server(ServerAuthConfig::scram("plomid", "secret").expect("CSPRNG")).await;
    let mut stream = connect(addr).await;
    send_startup(&mut stream, "plomid").await;
    let (tag, payload) = read_message(&mut stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 10);
    // Send an MD5-styled password message where SASLInitialResponse belongs.
    let fake = b"SCRAM-SHA-256\x00md5deadbeefdeadbeefdeadbeefdeadbeef";
    let mut message = Vec::with_capacity(1 + 4 + fake.len());
    message.push(b'p');
    message.extend_from_slice(&((fake.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(fake);
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    let (tag, _) = read_message(&mut stream).await;
    assert_eq!(tag, b'E', "downgrade attempt must be refused");
}
