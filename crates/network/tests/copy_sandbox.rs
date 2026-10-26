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
//! COPY file sandbox tests (B2).
//!
//! `COPY ... FROM/TO <file>` runs with server filesystem privileges, so
//! every path must be jailed to the server-configured `--copy-dir`.
//! These tests drive the real wire protocol and assert that in-jail copies
//! work while traversal, absolute, and symlink-escape paths are rejected
//! with filename-only errors (no server directory prefix on the wire).

use plomid_network::server::PlomidServer;
use plomid_network::session::ServerAuthConfig;
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

fn temp_path(label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-copy-{label}-{}-{id}", std::process::id()))
}

async fn start_copy_server(copy_dir: Option<PathBuf>) -> std::net::SocketAddr {
    let storage_path = temp_path("storage");
    let wal_path = temp_path("wal");
    let _ = std::fs::remove_file(&storage_path);
    let _ = std::fs::remove_file(&wal_path);

    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));

    let mut server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::new("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    if let Some(dir) = copy_dir {
        server.set_copy_dir(dir);
    }
    tokio::spawn(async move { server.run(engine).await });
    addr
}

async fn connect_to_server(addr: std::net::SocketAddr) -> tokio::net::TcpStream {
    timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
        .await
        .expect("connect timeout")
        .expect("tcp connect")
}

async fn send_message(stream: &mut tokio::net::TcpStream, data: &[u8]) {
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

async fn handshake(stream: &mut tokio::net::TcpStream) {
    let startup = b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00";
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&(startup.len() as u32 + 4).to_be_bytes());
    send_message(stream, &len_buf).await;
    send_message(stream, startup).await;

    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    let auth_code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    if auth_code == 3 {
        let password_msg = b"secret\x00";
        let mut password = Vec::with_capacity(1 + 4 + password_msg.len());
        password.push(b'p');
        password.extend_from_slice(&(password_msg.len() as u32 + 4).to_be_bytes());
        password.extend_from_slice(password_msg);
        send_message(stream, &password).await;
    }

    loop {
        let (tag, payload) = read_message(stream).await;
        if tag == b'Z' {
            break;
        }
        if tag == b'E' {
            let msg = String::from_utf8_lossy(&payload);
            panic!("authentication failed: {msg}");
        }
    }
}

async fn send_query_messages(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let query = format!("{sql};\x00");
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    send_message(stream, &message).await;

    let mut response = Vec::new();
    loop {
        let (tag, payload) = read_message(stream).await;
        response.push((tag, payload));
        if tag == b'Z' {
            break;
        }
    }
    response
}

fn tags(messages: &[(u8, Vec<u8>)]) -> Vec<u8> {
    messages.iter().map(|(tag, _)| *tag).collect()
}

fn error_text(messages: &[(u8, Vec<u8>)]) -> String {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'E')
        .map(|(_, payload)| String::from_utf8_lossy(payload).into_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

fn command_tag(messages: &[(u8, Vec<u8>)]) -> String {
    let (_, payload) = messages.iter().find(|(tag, _)| *tag == b'C').unwrap();
    String::from_utf8(payload[..payload.len() - 1].to_vec()).unwrap()
}

fn parse_data_row(bytes: &[u8]) -> Vec<Option<String>> {
    let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let mut position = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        if length < 0 {
            values.push(None);
        } else {
            let end = position + length as usize;
            values.push(Some(
                String::from_utf8(bytes[position..end].to_vec()).unwrap(),
            ));
            position = end;
        }
    }
    values
}

/// Server directory strings that must never appear in COPY wire errors:
/// both the raw and the canonicalized jail prefix.
fn forbidden_prefixes(jail: &PathBuf) -> Vec<String> {
    let mut out = vec![jail.display().to_string()];
    if let Ok(canonical) = std::fs::canonicalize(jail) {
        out.push(canonical.display().to_string());
    }
    out
}

fn assert_no_server_path(error: &str, jail: &PathBuf, outside: Option<&str>) {
    for prefix in forbidden_prefixes(jail) {
        assert!(
            !error.contains(&prefix),
            "COPY error leaks server path {prefix}: {error}"
        );
    }
    if let Some(path) = outside {
        assert!(
            !error.contains(path),
            "COPY error leaks outside path {path}: {error}"
        );
    }
}

#[tokio::test]
async fn copy_file_roundtrip_within_jail() {
    let jail = temp_path("jail-roundtrip");
    std::fs::create_dir_all(&jail).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let created = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;
    assert_eq!(tags(&created), vec![b'C', b'Z']);

    std::fs::write(jail.join("seed.txt"), "1\n2\n").unwrap();
    let loaded = send_query_messages(&mut stream, "COPY copy_t FROM 'seed.txt'").await;
    assert_eq!(tags(&loaded), vec![b'C', b'Z'], "in-jail FROM rejected");
    assert_eq!(command_tag(&loaded), "COPY 2");

    let count = send_query_messages(&mut stream, "SELECT count(*) FROM copy_t").await;
    assert_eq!(parse_data_row(&count[1].1), vec![Some("2".to_string())]);

    let stored = send_query_messages(&mut stream, "COPY copy_t TO 'out.txt'").await;
    assert_eq!(tags(&stored), vec![b'C', b'Z'], "in-jail TO rejected");
    assert_eq!(command_tag(&stored), "COPY 2");
    assert_eq!(
        std::fs::read_to_string(jail.join("out.txt")).unwrap(),
        "1\n2\n"
    );
}

#[tokio::test]
async fn copy_file_parent_traversal_is_rejected() {
    let jail = temp_path("jail-traversal");
    std::fs::create_dir_all(&jail).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    let from = send_query_messages(&mut stream, "COPY copy_t FROM '../../etc/passwd'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    let error = error_text(&from);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, Some("/etc/passwd"));
    // Filename-only: the leaf may be echoed, never the traversal.
    assert!(error.contains("passwd"), "got: {error}");

    let to = send_query_messages(&mut stream, "COPY copy_t TO '../../evil.txt'").await;
    assert_eq!(tags(&to), vec![b'E', b'Z']);
    let error = error_text(&to);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, None);
}

#[tokio::test]
async fn copy_file_absolute_paths_are_rejected() {
    let jail = temp_path("jail-absolute");
    std::fs::create_dir_all(&jail).unwrap();
    let outside = temp_path("outside-absolute");
    std::fs::create_dir_all(&outside).unwrap();
    let secret = outside.join("secret.txt");
    std::fs::write(&secret, "TOPSECRET-marker-12345").unwrap();
    let secret_abs = secret.display().to_string();

    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    let from = send_query_messages(&mut stream, &format!("COPY copy_t FROM '{secret_abs}'")).await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    let error = error_text(&from);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, Some(&secret_abs));
    assert!(
        !error.contains("TOPSECRET"),
        "COPY error leaks file contents: {error}"
    );

    // The secret must not have been loaded: the table stays empty.
    let count = send_query_messages(&mut stream, "SELECT count(*) FROM copy_t").await;
    assert_eq!(parse_data_row(&count[1].1), vec![Some("0".to_string())]);

    let dest_abs = outside.join("should-not-exist.txt").display().to_string();
    let to = send_query_messages(&mut stream, &format!("COPY copy_t TO '{dest_abs}'")).await;
    assert_eq!(tags(&to), vec![b'E', b'Z']);
    let error = error_text(&to);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, Some(&dest_abs));
    assert!(
        !outside.join("should-not-exist.txt").exists(),
        "absolute COPY TO escaped the jail"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn copy_file_symlink_escape_is_rejected() {
    let jail = temp_path("jail-symlink");
    std::fs::create_dir_all(&jail).unwrap();
    let outside = temp_path("outside-symlink");
    std::fs::create_dir_all(&outside).unwrap();
    let secret = outside.join("secret.txt");
    std::fs::write(&secret, "SYMLINK-SECRET-xyz").unwrap();
    std::os::unix::fs::symlink(&secret, jail.join("evil-link")).unwrap();

    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    let from = send_query_messages(&mut stream, "COPY copy_t FROM 'evil-link'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    let error = error_text(&from);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, Some(&secret.display().to_string()));
    assert!(
        !error.contains("SYMLINK-SECRET"),
        "COPY error leaks file contents: {error}"
    );

    // A symlink destination must not be followed for writes either.
    let target = outside.join("target.txt");
    std::fs::write(&target, "ORIGINAL").unwrap();
    std::os::unix::fs::symlink(&target, jail.join("out-link")).unwrap();
    let to = send_query_messages(&mut stream, "COPY copy_t TO 'out-link'").await;
    assert_eq!(tags(&to), vec![b'E', b'Z']);
    let error = error_text(&to);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, None);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "ORIGINAL",
        "symlink COPY TO escaped the jail"
    );
}

#[tokio::test]
async fn copy_file_without_jail_is_denied() {
    let addr = start_copy_server(None).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    let from = send_query_messages(&mut stream, "COPY copy_t FROM 'x.txt'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    assert!(
        error_text(&from).contains("not permitted"),
        "got: {}",
        error_text(&from)
    );
}

#[tokio::test]
async fn copy_file_dot_segments_and_trailing_slash_are_rejected() {
    let jail = temp_path("jail-dots");
    std::fs::create_dir_all(&jail).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    for bad in [
        "COPY copy_t FROM 'a/../b.csv'",
        "COPY copy_t FROM './b.csv'",
        "COPY copy_t FROM 'a/./b.csv'",
        "COPY copy_t TO 'subdir/'",
        "COPY copy_t FROM '.'",
        "COPY copy_t FROM '..'",
        "COPY copy_t FROM 'a//b.csv'",
    ] {
        let response = send_query_messages(&mut stream, bad).await;
        assert_eq!(tags(&response), vec![b'E', b'Z'], "{bad} must deny");
        let error = error_text(&response);
        assert!(error.contains("not permitted"), "{bad} got: {error}");
        assert_no_server_path(&error, &jail, None);
    }
}

#[tokio::test]
async fn copy_file_nested_parent_symlink_is_rejected() {
    let jail = temp_path("jail-nested-link");
    std::fs::create_dir_all(&jail).unwrap();
    let outside = temp_path("outside-nested-link");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), "NESTED-SECRET").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, jail.join("linksub")).unwrap();

    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    // Resolves through the linked parent: must not read outside.
    let from = send_query_messages(&mut stream, "COPY copy_t FROM 'linksub/secret.txt'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    let error = error_text(&from);
    assert!(error.contains("not permitted"), "got: {error}");
    assert_no_server_path(&error, &jail, Some(&outside.display().to_string()));
    assert!(!error.contains("NESTED-SECRET"), "content leak: {error}");

    // Missing intermediate directories fail closed instead of being created.
    let to = send_query_messages(&mut stream, "COPY copy_t TO 'no/such/dir.csv'").await;
    assert_eq!(tags(&to), vec![b'E', b'Z']);
    assert!(
        error_text(&to).contains("not permitted"),
        "got: {}",
        error_text(&to)
    );
    assert!(
        !jail.join("no").exists(),
        "missing dirs must not be created"
    );
}

#[tokio::test]
async fn copy_file_directory_targets_are_rejected() {
    let jail = temp_path("jail-isdir");
    std::fs::create_dir_all(&jail).unwrap();
    std::fs::create_dir_all(jail.join("sub")).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    let from = send_query_messages(&mut stream, "COPY copy_t FROM 'sub'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    assert!(
        error_text(&from).contains("not permitted"),
        "got: {}",
        error_text(&from)
    );

    let to = send_query_messages(&mut stream, "COPY copy_t TO 'sub'").await;
    assert_eq!(tags(&to), vec![b'E', b'Z']);
    assert!(
        error_text(&to).contains("not permitted"),
        "got: {}",
        error_text(&to)
    );
}

#[tokio::test]
async fn copy_file_written_bytes_are_owner_only() {
    let jail = temp_path("jail-perms");
    std::fs::create_dir_all(&jail).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;
    std::fs::write(jail.join("seed.txt"), "7\n").unwrap();
    let loaded = send_query_messages(&mut stream, "COPY copy_t FROM 'seed.txt'").await;
    assert_eq!(tags(&loaded), vec![b'C', b'Z']);

    let stored = send_query_messages(&mut stream, "COPY copy_t TO 'perm-out.txt'").await;
    assert_eq!(tags(&stored), vec![b'C', b'Z']);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::symlink_metadata(jail.join("perm-out.txt"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "wire COPY TO must be 0600, got {mode:o}");
    }
}

#[tokio::test]
async fn copy_file_oversized_source_fails_closed() {
    let jail = temp_path("jail-big");
    std::fs::create_dir_all(&jail).unwrap();
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    // Sparse 64 MiB + 1 file: no large allocation, must fail closed.
    let path = jail.join("big.csv");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(64 * 1024 * 1024 + 1).unwrap();
    drop(file);

    let from = send_query_messages(&mut stream, "COPY copy_t FROM 'big.csv'").await;
    assert_eq!(tags(&from), vec![b'E', b'Z']);
    let error = error_text(&from);
    assert!(
        error.contains("too large") || error.contains("could not read"),
        "oversize must fail closed, got: {error}"
    );
    assert_no_server_path(&error, &jail, None);
    // Table stays empty: no partial ingest.
    let count = send_query_messages(&mut stream, "SELECT count(*) FROM copy_t").await;
    assert_eq!(parse_data_row(&count[1].1), vec![Some("0".to_string())]);
}

#[cfg(unix)]
#[tokio::test]
async fn copy_file_fifo_source_does_not_hang() {
    use tokio::time::{timeout, Duration};
    let jail = temp_path("jail-fifo-wire");
    std::fs::create_dir_all(&jail).unwrap();
    let fifo = jail.join("pipe.csv");
    let created = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !created {
        return;
    }
    let addr = start_copy_server(Some(jail.clone())).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query_messages(&mut stream, "CREATE TABLE copy_t (id INTEGER)").await;

    // The server must refuse the FIFO promptly (O_NONBLOCK + fstat), not
    // block on the fifo open waiting for a writer.
    let query = "COPY copy_t FROM 'pipe.csv';\x00";
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    stream.write_all(&message).await.expect("write");
    stream.flush().await.expect("flush");
    let mut response = Vec::new();
    let got = timeout(Duration::from_secs(10), async {
        loop {
            let (tag, payload) = read_message(&mut stream).await;
            let done = tag == b'Z';
            response.push((tag, payload));
            if done {
                break;
            }
        }
    })
    .await;
    assert!(got.is_ok(), "COPY FROM fifo hung the connection");
    assert_eq!(tags(&response), vec![b'E', b'Z']);
    assert!(
        error_text(&response).contains("not permitted"),
        "got: {}",
        error_text(&response)
    );
}
