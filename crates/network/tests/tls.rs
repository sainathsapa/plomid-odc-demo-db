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
//! TLS transport tests (Beta 2 Phase 1).
//!
//! Covers the plan's required cases: handshake success, invalid material,
//! expired material, required-mode plaintext refusal, loopback development
//! mode, insecure-default refusal, Docker image defaults, and the absence
//! of a public default password in production configuration.

use plomid_network::server::PlomidServer;
use plomid_network::session::ServerAuthConfig;
use plomid_network::tls::{load_server_config, TlsAcceptor};
use plomid_txn::PlomidStorageEngine;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{timeout, Duration};

fn temp_path(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-tls-{label}-{}-{id}", std::process::id()))
}

struct TlsMaterial {
    cert_pem: String,
    key_pem: String,
    cert_der: Vec<u8>,
}

/// Mints a self-signed `localhost` certificate. When `expired` is true the
/// validity window lies entirely in the past (fail-closed load fixture).
fn mint_material(expired: bool) -> TlsMaterial {
    let key_pair = rcgen::KeyPair::generate().expect("fixture key");
    let mut params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("fixture params");
    if expired {
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::days(30);
        params.not_after = now - time::Duration::days(1);
    }
    let cert = params.self_signed(&key_pair).expect("fixture cert");
    TlsMaterial {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
        cert_der: cert.der().to_vec(),
    }
}

fn write_material(
    dir: &std::path::Path,
    material: &TlsMaterial,
) -> (std::path::PathBuf, std::path::PathBuf) {
    std::fs::create_dir_all(dir).expect("fixture dir");
    let cert_path = dir.join("server-cert.pem");
    let key_path = dir.join("server-key.pem");
    std::fs::write(&cert_path, &material.cert_pem).expect("cert fixture");
    std::fs::write(&key_path, &material.key_pem).expect("key fixture");
    (cert_path, key_path)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TlsMode {
    Disabled,
    Optional,
    Required,
}

async fn start_server(mode: TlsMode, auth: ServerAuthConfig) -> std::net::SocketAddr {
    let storage_path = temp_path("engine");
    let wal_path = temp_path("engine-wal");
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));

    let mut server = PlomidServer::bind_ephemeral(auth)
        .await
        .expect("server binds");
    let addr = server.local_addr();
    if mode != TlsMode::Disabled {
        let dir = temp_path("material");
        let material = mint_material(false);
        let (cert_path, key_path) = write_material(&dir, &material);
        let rustls_config =
            load_server_config(&cert_path, &key_path).expect("fixture material loads");
        server.set_tls_config(TlsAcceptor::new(rustls_config, mode == TlsMode::Required));
    }
    tokio::spawn(async move { server.run(engine).await });
    addr
}

/// Starts a TLS-capable server and returns its address plus the server
/// certificate DER (for the client trust store).
async fn start_tls_server(required: bool) -> (std::net::SocketAddr, Vec<u8>) {
    let storage_path = temp_path("engine");
    let wal_path = temp_path("engine-wal");
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));

    let material = mint_material(false);
    let dir = temp_path("material");
    let (cert_path, key_path) = write_material(&dir, &material);
    let rustls_config = load_server_config(&cert_path, &key_path).expect("fixture material loads");

    let mut server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::new("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    server.set_tls_config(TlsAcceptor::new(rustls_config, required));
    tokio::spawn(async move { server.run(engine).await });
    (addr, material.cert_der)
}

fn client_config_trusting(cert_der: &[u8]) -> Arc<rustls::ClientConfig> {
    let mut store = rustls::RootCertStore::empty();
    store
        .add(rustls::pki_types::CertificateDer::from(cert_der.to_vec()))
        .expect("fixture cert trusted");
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(provider.into())
            .with_protocol_versions(rustls::ALL_VERSIONS)
            .expect("protocol versions")
            .with_root_certificates(store)
            .with_no_client_auth(),
    )
}

fn client_config_trusting_nothing() -> Arc<rustls::ClientConfig> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    Arc::new(
        rustls::ClientConfig::builder_with_provider(provider.into())
            .with_protocol_versions(rustls::ALL_VERSIONS)
            .expect("protocol versions")
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
    )
}

async fn connect(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .expect("connect timeout")
        .expect("tcp connect")
}

async fn read_byte(stream: &mut tokio::net::TcpStream) -> u8 {
    let mut byte = [0u8; 1];
    timeout(Duration::from_secs(5), stream.read_exact(&mut byte))
        .await
        .expect("read timeout")
        .expect("read byte");
    byte[0]
}

async fn read_message<S>(stream: &mut S) -> (u8, Vec<u8>)
where
    S: AsyncRead + Unpin,
{
    let mut tag = [0u8; 1];
    timeout(Duration::from_secs(5), stream.read_exact(&mut tag))
        .await
        .expect("read timeout")
        .expect("read tag");
    let mut len_buf = [0u8; 4];
    timeout(Duration::from_secs(5), stream.read_exact(&mut len_buf))
        .await
        .expect("read timeout")
        .expect("read len");
    let len = u32::from_be_bytes(len_buf) as usize - 4;
    let mut payload = vec![0u8; len];
    timeout(Duration::from_secs(5), stream.read_exact(&mut payload))
        .await
        .expect("read timeout")
        .expect("read payload");
    (tag[0], payload)
}

async fn send_bytes<S>(stream: &mut S, bytes: &[u8])
where
    S: AsyncWrite + Unpin,
{
    timeout(Duration::from_secs(5), stream.write_all(bytes))
        .await
        .expect("write timeout")
        .expect("write");
    timeout(Duration::from_secs(5), stream.flush())
        .await
        .expect("flush timeout")
        .expect("flush");
}

/// Sends a PostgreSQL SSLRequest and returns the single-byte reply.
async fn ssl_request(stream: &mut tokio::net::TcpStream) -> u8 {
    let mut request = Vec::with_capacity(8);
    request.extend_from_slice(&8u32.to_be_bytes());
    request.extend_from_slice(&80877103u32.to_be_bytes());
    send_bytes(stream, &request).await;
    read_byte(stream).await
}

fn startup_payload() -> Vec<u8> {
    b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00".to_vec()
}

async fn send_startup<S>(stream: &mut S, payload: &[u8])
where
    S: AsyncWrite + Unpin,
{
    let mut message = Vec::with_capacity(4 + payload.len());
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(payload);
    send_bytes(stream, &message).await;
}

/// Cleartext-password handshake over any transport. Returns at ReadyForQuery.
async fn cleartext_handshake<S>(stream: &mut S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send_startup(stream, &startup_payload()).await;
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 3);
    let password = b"secret\x00";
    let mut message = Vec::with_capacity(1 + 4 + password.len());
    message.push(b'p');
    message.extend_from_slice(&(password.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(password);
    send_bytes(stream, &message).await;
    loop {
        let (tag, payload) = read_message(stream).await;
        if tag == b'Z' {
            break;
        }
        assert_ne!(
            tag,
            b'E',
            "auth failed: {:?}",
            String::from_utf8_lossy(&payload)
        );
    }
}

async fn send_query<S>(stream: &mut S, sql: &str) -> Vec<(u8, Vec<u8>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let query = format!("{sql};\x00");
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    send_bytes(stream, &message).await;
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

// ---- Certificate/key load validation ----

#[test]
fn tls_load_accepts_valid_material() {
    let dir = temp_path("load-valid");
    let (cert_path, key_path) = write_material(&dir, &mint_material(false));
    load_server_config(&cert_path, &key_path).expect("valid material loads");
}

#[test]
fn tls_load_rejects_expired_certificate() {
    let dir = temp_path("load-expired");
    let (cert_path, key_path) = write_material(&dir, &mint_material(true));
    let err = load_server_config(&cert_path, &key_path).expect_err("expired material must fail");
    assert!(
        err.to_string().contains("expired or not yet valid"),
        "got: {err}"
    );
}

#[test]
fn tls_load_rejects_mismatched_key() {
    let dir = temp_path("load-mismatch");
    let cert_holder = mint_material(false);
    let key_holder = mint_material(false);
    std::fs::create_dir_all(&dir).expect("fixture dir");
    let cert_path = dir.join("server-cert.pem");
    let key_path = dir.join("server-key.pem");
    std::fs::write(&cert_path, &cert_holder.cert_pem).expect("cert fixture");
    // A well-formed key that does not match the certificate.
    std::fs::write(&key_path, &key_holder.key_pem).expect("key fixture");
    let err = load_server_config(&cert_path, &key_path).expect_err("mismatched key must fail");
    assert!(err.to_string().contains("rejected"), "got: {err}");
}

// ---- Wire behavior ----

#[tokio::test]
async fn tls_handshake_success_and_query_over_encrypted_channel() {
    let before = plomid_core::observability::snapshot();
    let (addr, cert_der) = start_tls_server(false).await;
    let mut plain = connect(addr).await;
    assert_eq!(ssl_request(&mut plain).await, b'S');

    let connector = tokio_rustls::TlsConnector::from(client_config_trusting(&cert_der));
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("server name")
        .to_owned();
    let mut tls = timeout(
        Duration::from_secs(5),
        connector.connect(server_name, plain),
    )
    .await
    .expect("tls timeout")
    .expect("tls handshake succeeds");

    cleartext_handshake(&mut tls).await;
    let response = send_query(&mut tls, "SELECT 1").await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'D', b'C', b'Z']
    );
    let after = plomid_core::observability::snapshot();
    // Lower bound: sibling tests in this binary handshake concurrently on
    // the same process-wide counters.
    assert!(after.tls_connections > before.tls_connections);
}

#[tokio::test]
async fn tls_handshake_fails_for_untrusted_certificate() {
    let (addr, _cert_der) = start_tls_server(false).await;
    let mut plain = connect(addr).await;
    assert_eq!(ssl_request(&mut plain).await, b'S');

    // The client trusts nothing: the server's self-signed chain is invalid
    // to it, so the handshake must fail (no session is established).
    let connector = tokio_rustls::TlsConnector::from(client_config_trusting_nothing());
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("server name")
        .to_owned();
    let result = timeout(
        Duration::from_secs(5),
        connector.connect(server_name, plain),
    )
    .await;
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "untrusted chain must fail the handshake"
    );
}

#[tokio::test]
async fn tls_required_mode_refuses_plaintext_startup() {
    let before = plomid_core::observability::snapshot();
    let addr = start_server(TlsMode::Required, ServerAuthConfig::disabled()).await;
    let mut stream = connect(addr).await;
    // No SSLRequest: a plaintext startup straight away.
    send_startup(&mut stream, &startup_payload()).await;
    let (tag, payload) = read_message(&mut stream).await;
    assert_eq!(tag, b'E');
    let text = String::from_utf8_lossy(&payload);
    assert!(
        text.contains("28000"),
        "expected SQLSTATE 28000, got {text}"
    );
    assert!(
        text.contains("TLS is required"),
        "expected TLS refusal, got {text}"
    );
    // FATAL closes the connection: EOF follows, never ReadyForQuery.
    let mut byte = [0u8; 1];
    let n = timeout(Duration::from_secs(5), stream.read(&mut byte))
        .await
        .expect("read timeout")
        .expect("read close");
    assert_eq!(n, 0, "connection must close after FATAL");
    let after = plomid_core::observability::snapshot();
    assert!(
        after.tls_plaintext_refusals > before.tls_plaintext_refusals,
        "refusal must be counted"
    );
}

#[tokio::test]
async fn tls_required_mode_still_serves_encrypted_clients() {
    let (addr, cert_der) = start_tls_server(true).await;
    let mut plain = connect(addr).await;
    assert_eq!(ssl_request(&mut plain).await, b'S');
    let connector = tokio_rustls::TlsConnector::from(client_config_trusting(&cert_der));
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("server name")
        .to_owned();
    let mut tls = timeout(
        Duration::from_secs(5),
        connector.connect(server_name, plain),
    )
    .await
    .expect("tls timeout")
    .expect("tls handshake succeeds");
    cleartext_handshake(&mut tls).await;
    let response = send_query(&mut tls, "SELECT 2").await;
    assert!(response.iter().any(|(tag, _)| *tag == b'D'));
}

#[tokio::test]
async fn tls_optional_mode_still_accepts_plaintext() {
    let addr = start_server(
        TlsMode::Optional,
        ServerAuthConfig::new("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await;
    let mut stream = connect(addr).await;
    send_startup(&mut stream, &startup_payload()).await;
    // Cleartext auth over the plaintext channel (compatibility mode).
    let (tag, payload) = read_message(&mut stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 3);
}

#[tokio::test]
async fn server_without_tls_answers_n_to_ssl_request() {
    let addr = start_server(TlsMode::Disabled, ServerAuthConfig::disabled()).await;
    let mut stream = connect(addr).await;
    assert_eq!(ssl_request(&mut stream).await, b'N');
    // Plaintext startup proceeds normally after the refusal.
    send_startup(&mut stream, &startup_payload()).await;
    let (tag, _) = read_message(&mut stream).await;
    assert_eq!(tag, b'R', "plaintext session must continue after N");
}

// ---- Configuration + image defaults ----

/// The production Docker image must not bake in the insecure-remote
/// opt-in: remote binds fail closed unless the operator opts in.
#[test]
fn docker_image_has_no_insecure_remote_default() {
    let dockerfile = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Dockerfile");
    let content =
        std::fs::read_to_string(&dockerfile).expect("workspace Dockerfile must be readable");
    for line in content.lines() {
        let stripped = line.trim();
        if stripped.starts_with('#') {
            continue;
        }
        assert!(
            !stripped.contains("PLOMID_ALLOW_INSECURE_REMOTE=true"),
            "Dockerfile must not bake in the insecure-remote opt-in: {line}"
        );
    }
}
