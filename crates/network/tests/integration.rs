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
//! Integration tests for the PLOMID server and PostgreSQL wire protocol.
//!
//! These tests start a real PLOMID server, connect through the wire
//! protocol, execute SQL, and verify the results.

use plomid_network::server::PlomidServer;
use plomid_network::session::ServerAuthConfig;
use plomid_sql::{save_catalog, Catalog, ColumnDef, ColumnType, InMemoryCatalog};
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
        "plomid-server-{label}-{}-{}",
        std::process::id(),
        id
    ))
}

async fn start_test_server() -> (Arc<Mutex<PlomidStorageEngine>>, std::net::SocketAddr) {
    let storage_path = temp_path("integration");
    let wal_path = temp_path("integration-wal");
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

async fn start_database_server() -> std::net::SocketAddr {
    let storage_path = temp_path("database-routing");
    let (addr, _server) = start_database_server_with_root(storage_path).await;
    addr
}

async fn start_database_server_with_root(
    storage_path: std::path::PathBuf,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let wal_path = temp_path("database-routing-wal");
    start_database_server_with_paths(storage_path, wal_path).await
}

async fn start_database_server_with_paths(
    storage_path: std::path::PathBuf,
    wal_path: std::path::PathBuf,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));
    let server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::new("plomid", "secret").expect("CSPRNG for auth bootstrap"),
    )
    .await
    .expect("server binds");
    let addr = server.local_addr();
    let task =
        tokio::spawn(async move { server.run_with_database_root(engine, storage_path).await });
    (addr, task)
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

fn frontend_message(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(1 + 4 + payload.len());
    message.push(tag);
    message.extend_from_slice(&((payload.len() as u32) + 4).to_be_bytes());
    message.extend_from_slice(payload);
    message
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
async fn start_scram_server() -> (Arc<Mutex<PlomidStorageEngine>>, std::net::SocketAddr) {
    let storage_path = temp_path("integration-scram");
    let wal_path = temp_path("integration-scram-wal");
    let _ = std::fs::remove_file(&storage_path);
    let _ = std::fs::remove_file(&wal_path);

    let engine =
        PlomidStorageEngine::open(&storage_path, &wal_path, 32).expect("storage engine opens");
    let engine = Arc::new(Mutex::new(engine));

    let server = PlomidServer::bind_ephemeral(
        ServerAuthConfig::scram("plomid", "secret").expect("CSPRNG for auth bootstrap"),
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

/// Parses a comma-separated `name=value` SCRAM attribute string.
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

/// Sends a raw startup packet carrying the given `startup` payload bytes.
async fn send_startup(stream: &mut tokio::net::TcpStream, startup: &[u8]) {
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&(startup.len() as u32 + 4).to_be_bytes());
    send_message(stream, &len_buf).await;
    send_message(stream, startup).await;
}

/// Drives a full SCRAM-SHA-256 client exchange against the server.
///
/// Returns once the server sends ReadyForQuery. Panics on any protocol or
/// authentication failure.
async fn scram_handshake(stream: &mut tokio::net::TcpStream, password: &str) {
    use plomid_network::scram::{base64_decode, base64_encode, hmac_sha256, pbkdf2_sha256, sha256};

    let startup = b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00";
    send_startup(stream, startup).await;

    // 1. AuthenticationSASL advertises mechanisms.
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    let code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    assert_eq!(code, 10, "expected SASL authentication request");
    let mechanisms = String::from_utf8_lossy(&payload[4..]);
    assert!(mechanisms.contains("SCRAM-SHA-256"), "got {mechanisms}");

    // 2. Send SASLInitialResponse (mechanism + \0 + client-first).
    let client_first = "n,,n=plomid,r=testnonce";
    let mut initial = Vec::new();
    initial.extend_from_slice(b"SCRAM-SHA-256\x00");
    initial.extend_from_slice(client_first.as_bytes());
    send_message(stream, &frontend_message(b'p', &initial)).await;

    // 3. Receive AuthenticationSASLContinue (server-first).
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    let code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    assert_eq!(code, 11, "expected SASL continue");
    let server_first = String::from_utf8_lossy(&payload[4..]);
    let sf_attrs = scram_attrs(&server_first);
    let salt_b64 = sf_attrs.get("s").unwrap();
    let iterations: u32 = sf_attrs.get("i").unwrap().parse().unwrap();
    let nonce = sf_attrs.get("r").unwrap();

    // 4. Compute the client proof.
    let salt_bytes = base64_decode(salt_b64).unwrap();
    let salted = pbkdf2_sha256(password.as_bytes(), &salt_bytes, iterations, 32);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_first_bare = "n=plomid,r=testnonce";
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!("{},p={}", without_proof, base64_encode(&proof));
    send_message(stream, &frontend_message(b'p', client_final.as_bytes())).await;

    // 5. AuthenticationSASLFinal then AuthenticationOk.
    let (tag, payload) = read_message(stream).await;
    assert_eq!(tag, b'R');
    let code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    assert_eq!(code, 12, "expected SASL final");

    // 6. Consume ParameterStatus / BackendKeyData until ReadyForQuery.
    loop {
        let (tag, payload) = read_message(stream).await;
        match tag {
            b'R' => {
                let auth_code = u32::from_be_bytes(payload[..4].try_into().unwrap());
                assert_eq!(auth_code, 0, "expected AuthenticationOk");
            }
            b'Z' => break,
            b'E' => panic!("authentication failed: {payload:?}"),
            _ => {}
        }
    }
}

async fn send_query(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<u8> {
    send_query_messages(stream, sql)
        .await
        .into_iter()
        .flat_map(|(tag, payload)| {
            let mut bytes = vec![tag];
            bytes.extend_from_slice(&payload);
            bytes
        })
        .collect()
}

async fn send_query_messages(stream: &mut tokio::net::TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let query = if sql.is_empty() {
        "\x00".to_string()
    } else {
        format!("{sql};\x00")
    };
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    send_message(stream, &message).await;

    let mut response = Vec::new();
    loop {
        let (tag, payload) = read_message(stream).await;
        response.push((tag, payload));
        // ErrorResponse is followed by ReadyForQuery; consume both so the
        // next query cannot mistake the prior ready marker for its response.
        if tag == b'Z' {
            break;
        }
    }
    response
}

#[tokio::test]
async fn server_accepts_connection_and_handshakes() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
}

#[tokio::test]
async fn postgres_session_initialization_and_empty_query_are_compatible() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let empty = send_query_messages(&mut stream, "").await;
    assert_eq!(
        empty.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'I', b'Z']
    );

    for (query, expected) in [
        ("SHOW server_version", "17.0"),
        ("SHOW server_version_num", "170000"),
        ("SHOW search_path", "\"$user\", public"),
        ("SHOW TimeZone", "UTC"),
    ] {
        let response = send_query_messages(&mut stream, query).await;
        assert_eq!(
            parse_data_row(&response[1].1),
            vec![Some(expected.to_string())]
        );
    }

    let set = send_query_messages(&mut stream, "SET application_name = 'driver-test'").await;
    assert_eq!(
        set.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let show = send_query_messages(&mut stream, "SHOW application_name").await;
    assert_eq!(
        parse_data_row(&show[1].1),
        vec![Some("driver-test".to_string())]
    );

    let _ = send_query_messages(&mut stream, "CREATE TABLE dbeaver_visible (id INTEGER)").await;
    let tables = send_query_messages(&mut stream, "SHOW TABLES").await;
    assert!(tables.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload) == vec![Some("dbeaver_visible".to_string())]
    }));

    for query in [
        "SET SESSION statement_timeout TO 0",
        "SET LOCAL idle_in_transaction_session_timeout = 0",
        "SET ROLE pg_show_all_settings",
    ] {
        let response = send_query_messages(&mut stream, query).await;
        assert_eq!(
            response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
            [b'C', b'Z']
        );
    }
}

#[tokio::test]
async fn clients_can_start_transactions_with_isolation_options() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(
        &mut stream,
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
    )
    .await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    assert_eq!(response[1].1, vec![b'T']);
    let _ = send_query_messages(&mut stream, "ROLLBACK").await;
}

#[tokio::test]
async fn postgres_catalog_exposes_live_typed_system_metadata() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(
        &mut stream,
        "CREATE TABLE catalog_types (id INTEGER PRIMARY KEY, body JSONB)",
    )
    .await;

    let classes = send_query_messages(
        &mut stream,
        "SELECT oid, relname, relnamespace FROM pg_catalog.pg_class",
    )
    .await;
    assert!(classes.iter().any(|(tag, payload)| *tag == b'D'
        && parse_data_row(payload)
            .iter()
            .any(|v| v.as_deref() == Some("catalog_types"))));
    let attrs = send_query_messages(
        &mut stream,
        "SELECT attname, atttypid, attnotnull FROM pg_catalog.pg_attribute",
    )
    .await;
    assert!(attrs.iter().any(|(tag, payload)| *tag == b'D'
        && parse_data_row(payload).first().and_then(Option::as_deref) == Some("id")));
    let settings = send_query_messages(
        &mut stream,
        "SELECT name, setting FROM pg_catalog.pg_settings",
    )
    .await;
    assert!(settings.iter().any(|(tag, payload)| *tag == b'D'
        && parse_data_row(payload).first().and_then(Option::as_deref) == Some("server_version")));

    let joined_types = send_query_messages(
        &mut stream,
        "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) AS base_type_name, d.description FROM pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_type et ON et.oid=t.typelem LEFT JOIN pg_catalog.pg_class c ON c.oid=t.typrelid LEFT JOIN pg_catalog.pg_description d ON t.oid=d.objoid WHERE t.typname IS NOT NULL AND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')",
    )
    .await;
    assert!(joined_types.iter().any(|(tag, payload)| {
        *tag == b'D'
            && parse_data_row(payload)
                .iter()
                .any(|value| value.as_deref() == Some("text"))
    }));

    let joined = send_query_messages(&mut stream, "SELECT c.relname, n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'catalog_types'").await;
    assert_eq!(
        parse_data_row(&joined[1].1),
        vec![Some("catalog_types".into()), Some("public".into())]
    );

    let aliased = send_query_messages(
        &mut stream,
        "SELECT n.nspname AS schema_name FROM pg_catalog.pg_namespace n WHERE n.nspname = 'public'",
    )
    .await;
    assert_eq!(parse_data_row(&aliased[1].1), vec![Some("public".into())]);

    let aliased_table = send_query_messages(
        &mut stream,
        "SELECT c.relname AS table_name FROM pg_catalog.pg_class c WHERE c.relname = 'catalog_types'",
    )
    .await;
    assert_eq!(
        parse_data_row(&aliased_table[1].1),
        vec![Some("catalog_types".into())]
    );

    let dbeaver_schema_query = send_query_messages(
        &mut stream,
        "SELECT n.oid, n.*, d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = n.oid AND d.objsubid = 0 AND d.classoid = 'pg_namespace'::regclass ORDER BY nspname",
    )
    .await;
    assert!(dbeaver_schema_query.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).contains(&Some("public".to_string()))
    }));

    // psql/DBeaver table properties use a scalar subquery in the projection.
    // The outer WHERE must still restrict pg_attribute to the selected live
    // relation; a nested WHERE must never become the catalog filter boundary.
    //
    // Resolve the relation OID first: catalog IDs are minted from
    // process-wide allocators, so the first table in a shared test binary is
    // not guaranteed to be OID 1.
    let oid_rows = send_query_messages(
        &mut stream,
        "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'catalog_types'",
    )
    .await;
    let table_oid = oid_rows
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| parse_data_row(payload)[0].clone().expect("oid cell"))
        .next()
        .expect("catalog_types must have a relation OID");
    let columns = send_query_messages(
        &mut stream,
        &format!(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = '{table_oid}' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum"
        ),
    )
    .await;
    let rows = columns
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, payload)| parse_data_row(payload))
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Some("id".into()));
    assert_eq!(rows[1][0], Some("body".into()));
}

#[tokio::test]
async fn postgres_client_catalog_queries_report_actual_objects_and_types() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(
        &mut stream,
        "CREATE TABLE visible_objects (id INTEGER PRIMARY KEY, body JSONB)",
    )
    .await;
    let _ = send_query(
        &mut stream,
        "CREATE INDEX visible_objects_body_idx ON visible_objects(body)",
    )
    .await;

    let schemas =
        send_query_messages(&mut stream, "SELECT nspname FROM pg_catalog.pg_namespace").await;
    assert!(schemas.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload) == vec![Some("public".to_string())]
    }));

    let classes = send_query_messages(
        &mut stream,
        "SELECT c.oid, n.nspname, c.relname, c.relkind FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') ORDER BY n.nspname, c.relname",
    ).await;
    assert!(classes.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).contains(&Some("visible_objects".to_string()))
    }));

    let attributes = send_query_messages(
        &mut stream,
        "SELECT n.nspname, c.relname, a.attname, a.atttypid, a.attnotnull, a.attnum FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE a.attnum > 0 AND NOT a.attisdropped ORDER BY n.nspname, c.relname, a.attnum",
    ).await;
    assert!(attributes.iter().any(|(tag, payload)| {
        *tag == b'D' && {
            let row = parse_data_row(payload);
            row.contains(&Some("visible_objects".to_string()))
                && row.contains(&Some("id".to_string()))
        }
    }));
}

#[tokio::test]
async fn postgres_startup_catalog_probes_return_scalar_results() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(
        &mut stream,
        "SELECT CASE WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd' WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END AS type",
    )
    .await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'D', b'C', b'Z']
    );
    assert_eq!(parse_data_row(&response[1].1), vec![None]);
}

#[tokio::test]
async fn pgadmin_statistics_query_returns_dashboard_rows() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(
        &mut stream,
        "SELECT 'session_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS \"Total\", (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'active') AS \"Active\", (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'idle') AS \"Idle\") t UNION ALL SELECT 'tps_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT (SELECT sum(xact_commit) FROM pg_catalog.pg_stat_database) AS \"Commits\") t",
    )
    .await;
    let data_rows = response.iter().filter(|(tag, _)| *tag == b'D').count();
    // The SQL has two UNION ALL arms, so PostgreSQL returns two dashboard
    // rows.  The old compatibility responder manufactured five rows here.
    assert_eq!(data_rows, 2);
}

#[tokio::test]
async fn pgadmin_settings_probe_returns_postgres_shaped_rows() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(
        &mut stream,
        "SELECT name, vartype, min_val, max_val, enumvals FROM (SELECT 'role'::text AS name, 'string'::text AS vartype, NULL AS min_val, NULL AS max_val, NULL::text[] AS enumvals UNION ALL SELECT name, vartype, min_val::numeric AS min_val, max_val::numeric AS max_val, enumvals FROM pg_show_all_settings() WHERE context in ('user', 'superuser')) a",
    )
    .await;
    assert!(response.iter().any(|(tag, _)| *tag == b'T'));
    assert!(response.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).first() == Some(&Some("role".to_string()))
    }));
}

#[tokio::test]
async fn roles_are_catalog_backed_and_grantable() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let created =
        send_query_messages(&mut stream, "CREATE ROLE analyst LOGIN PASSWORD 'secret'").await;
    assert!(created.iter().any(|(tag, _)| *tag == b'C'));
    let granted = send_query_messages(&mut stream, "GRANT analyst TO plomid").await;
    assert!(
        granted.iter().any(|(tag, _)| *tag == b'C'),
        "grant response: {granted:?}"
    );
    let roles = send_query_messages(
        &mut stream,
        "SELECT rolname, rolcanlogin, is_superuser FROM pg_catalog.pg_roles",
    )
    .await;
    assert!(roles.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).first() == Some(&Some("analyst".to_string()))
    }));
}

#[tokio::test]
async fn create_user_creates_a_catalog_backed_login_role() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let created =
        send_query_messages(&mut stream, "CREATE USER app_user WITH PASSWORD 'secret'").await;
    assert!(created.iter().any(|(tag, _)| *tag == b'C'));
    let users = send_query_messages(
        &mut stream,
        "SELECT rolname, rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = 'app_user'",
    )
    .await;
    assert!(users.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).first() == Some(&Some("app_user".to_string()))
    }));
}

#[tokio::test]
async fn postgres_empty_catalog_relations_are_not_resolved_as_user_tables() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(
        &mut stream,
        "SELECT oid, umuser, umserver, umoptions FROM pg_user_mapping",
    )
    .await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'C', b'Z']
    );
}

#[tokio::test]
async fn server_processes_select_one_query() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let response = send_query_messages(&mut stream, "SELECT 1").await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'D', b'C', b'Z']
    );
    let fields = parse_row_description(&response[0].1);
    assert_eq!(fields, vec![("?column?".to_string(), 23, 4)]);
    assert_eq!(parse_data_row(&response[1].1), vec![Some("1".to_string())]);
}

#[tokio::test]
async fn database_create_show_use_and_isolation_share_one_registry() {
    let addr = start_database_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let create = send_query_messages(&mut stream, "CREATE DATABASE geeks").await;
    assert_eq!(
        create.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let show = send_query_messages(&mut stream, "SHOW DATABASE").await;
    assert_eq!(parse_data_row(&show[1].1), vec![Some("geeks".to_string())]);

    let use_geeks = send_query_messages(&mut stream, "USE geeks").await;
    assert_eq!(
        use_geeks.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let _ = send_query(&mut stream, "CREATE TABLE users (id INTEGER)").await;
    let _ = send_query(&mut stream, "INSERT INTO users VALUES (1)").await;
    let geeks_rows = send_query_messages(&mut stream, "SELECT * FROM users").await;
    assert_eq!(
        parse_data_row(&geeks_rows[1].1),
        vec![Some("1".to_string())]
    );

    let use_plomid = send_query_messages(&mut stream, "USE plomid").await;
    assert_eq!(
        use_plomid.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let missing = send_query_messages(&mut stream, "SELECT * FROM users").await;
    assert_eq!(missing[0].0, b'E');

    let use_geeks_again = send_query_messages(&mut stream, "USE geeks").await;
    assert_eq!(
        use_geeks_again
            .iter()
            .map(|(tag, _)| *tag)
            .collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let persisted = send_query_messages(&mut stream, "SELECT * FROM users").await;
    assert_eq!(parse_data_row(&persisted[1].1), vec![Some("1".to_string())]);
}

#[tokio::test]
async fn pg_catalog_database_and_metadata_use_live_connection_context() {
    let addr = start_database_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let create = send_query_messages(&mut stream, "CREATE DATABASE catalog_db").await;
    assert_eq!(
        create.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z'],
        "{create:?}"
    );
    let databases = send_query_messages(&mut stream, "SELECT datname FROM pg_database").await;
    assert_eq!(
        databases.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'D', b'D', b'C', b'Z']
    );
    assert_eq!(
        parse_data_row(&databases[1].1),
        vec![Some("catalog_db".to_string())]
    );
    assert_eq!(
        parse_data_row(&databases[2].1),
        vec![Some("plomid".to_string())]
    );

    let _ = send_query(&mut stream, "USE catalog_db").await;
    let current = send_query_messages(&mut stream, "SELECT current_database()").await;
    assert_eq!(
        parse_data_row(&current[1].1),
        vec![Some("catalog_db".to_string())]
    );
    let filtered = send_query_messages(
        &mut stream,
        "SELECT datname FROM pg_database WHERE datname = current_database()",
    )
    .await;
    assert_eq!(
        parse_data_row(&filtered[1].1),
        vec![Some("catalog_db".to_string())]
    );

    let _ = send_query(
        &mut stream,
        "CREATE TABLE public.users (id INTEGER, name TEXT)",
    )
    .await;
    let columns = send_query_messages(
        &mut stream,
        "SELECT attname, atttypid FROM pg_catalog.pg_attribute",
    )
    .await;
    assert_eq!(columns[0].0, b'T');
    assert_eq!(
        parse_data_row(&columns[1].1),
        vec![Some("id".to_string()), Some("23".to_string())]
    );
    assert_eq!(
        parse_data_row(&columns[2].1),
        vec![Some("name".to_string()), Some("25".to_string())]
    );
}

#[tokio::test]
async fn database_registry_and_catalog_survive_server_restart() {
    let storage_path = temp_path("database-restart");
    let (addr, server_task) = start_database_server_with_root(storage_path.clone()).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(&mut stream, "CREATE DATABASE geeks").await;
    let _ = send_query(&mut stream, "USE geeks").await;
    let _ = send_query(&mut stream, "CREATE TABLE restart_users (id INTEGER)").await;
    let _ = send_query(&mut stream, "INSERT INTO restart_users VALUES (7)").await;
    drop(stream);
    server_task.abort();
    let _ = server_task.await;

    let (addr, restarted_task) = start_database_server_with_root(storage_path).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let databases = send_query_messages(&mut stream, "SELECT datname FROM pg_database").await;
    assert!(databases.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload) == vec![Some("geeks".to_string())]
    }));
    let switched = send_query_messages(&mut stream, "USE geeks").await;
    assert_eq!(
        switched.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let rows = send_query_messages(&mut stream, "SELECT * FROM restart_users").await;
    assert_eq!(parse_data_row(&rows[1].1), vec![Some("7".to_string())]);
    restarted_task.abort();
    let _ = restarted_task.await;
}

/// F-007 regression: a stored credential and a large catalog must survive an
/// abrupt restart on the same storage + WAL, and the first correct login must
/// succeed. The incident showed correct-credential denials after a crash on a
/// big dirty directory; this pins the recovery + authentication seam.
#[tokio::test]
async fn auth_succeeds_after_dirty_restart_with_large_catalog() {
    use plomid_security::RoleCredential;

    let storage_path = temp_path("f007-dirty");
    let wal_path = temp_path("f007-dirty-wal");
    let _ = std::fs::remove_file(&storage_path);
    let _ = std::fs::remove_file(&wal_path);

    // Seed a persistent root: a stored verifier plus enough catalog objects
    // to exercise the sharded loader on reopen.
    {
        let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)
            .expect("seed storage engine opens");
        let mut catalog = InMemoryCatalog::new();
        for i in 0..2_000 {
            catalog
                .create_table(
                    format!("public.f007_t{i:04}"),
                    vec![ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::bigint(),
                        constraints: Vec::new(),
                    }],
                    Vec::new(),
                )
                .expect("seed table");
        }
        catalog.ensure_superuser("plomid");
        let credential = RoleCredential::derive(
            b"secret",
            plomid_security::random_salt().expect("CSPRNG salt"),
        );
        catalog.set_role_credential("plomid", &credential);
        save_catalog(&mut catalog, &mut engine).expect("seed catalog saves");
    }

    // First server: authenticate, commit writes, then abort without a clean
    // shutdown (the dirty state the audit's SIGKILL scenario left behind).
    let (addr, server_task) =
        start_database_server_with_paths(storage_path.clone(), wal_path.clone()).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(&mut stream, "CREATE TABLE dirty_state (id INTEGER)").await;
    let _ = send_query(&mut stream, "INSERT INTO dirty_state VALUES (7)").await;
    drop(stream);
    server_task.abort();
    let _ = server_task.await;

    // Reopen on the same storage + WAL. A correct credential must authenticate
    // on the first attempt, and committed data must survive.
    let (addr, restarted_task) = start_database_server_with_paths(storage_path, wal_path).await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let rows = send_query_messages(&mut stream, "SELECT * FROM dirty_state").await;
    assert_eq!(parse_data_row(&rows[1].1), vec![Some("7".to_string())]);
    restarted_task.abort();
    let _ = restarted_task.await;
}

#[tokio::test]
async fn empty_catalog_projections_keep_requested_field_structure() {
    let addr = start_database_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let schemata = send_query_messages(
        &mut stream,
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'missing'",
    )
    .await;
    assert_eq!(
        schemata.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'C', b'Z']
    );
    assert_eq!(
        parse_row_description(&schemata[0].1),
        // information_schema.sql_identifier is PostgreSQL's `name` type.
        vec![("schema_name".to_string(), 19, -1)]
    );

    let columns = send_query_messages(
        &mut stream,
        "SELECT table_name, column_name FROM information_schema.columns WHERE table_name = 'missing'",
    )
    .await;
    assert_eq!(
        columns.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'C', b'Z']
    );
    assert_eq!(
        parse_row_description(&columns[0].1),
        vec![
            ("table_name".to_string(), 19, -1),
            ("column_name".to_string(), 19, -1)
        ]
    );
}

#[tokio::test]
async fn server_processes_create_table_and_select() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query(&mut stream, "CREATE TABLE users (id INTEGER, name TEXT)").await;
    let _ = send_query(&mut stream, "INSERT INTO users VALUES (1, 'Alice')").await;
    let response = send_query_messages(&mut stream, "SELECT * FROM users").await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'D', b'C', b'Z']
    );
    assert_eq!(
        parse_row_description(&response[0].1),
        vec![("id".to_string(), 23, 4), ("name".to_string(), 25, -1),]
    );
    assert_eq!(
        parse_data_row(&response[1].1),
        vec![Some("1".to_string()), Some("Alice".to_string())]
    );
}

#[tokio::test]
async fn server_describes_empty_select_without_data_rows() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query(
        &mut stream,
        "CREATE TABLE empty_users (id INTEGER, name TEXT)",
    )
    .await;
    let response = send_query_messages(&mut stream, "SELECT * FROM empty_users").await;
    assert_eq!(
        response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'T', b'C', b'Z']
    );
    assert_eq!(
        parse_row_description(&response[0].1),
        vec![("id".to_string(), 23, 4), ("name".to_string(), 25, -1),]
    );
}

#[tokio::test]
async fn server_supports_client_compatibility_functions() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    for function in [
        "version()",
        "current_database()",
        "current_schema()",
        "current_user()",
        "session_user",
    ] {
        let response = send_query_messages(&mut stream, &format!("SELECT {function}")).await;
        assert_eq!(
            response.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
            [b'T', b'D', b'C', b'Z']
        );
        assert_eq!(parse_row_description(&response[0].1)[0].1, 25);
        assert!(parse_data_row(&response[1].1)[0].is_some());
    }
}

#[tokio::test]
async fn extended_describe_and_execute_return_field_structure_for_show() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let mut parse = Vec::new();
    parse.extend_from_slice(b"stmt\0SHOW search_path\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'P', &parse)).await;

    let mut bind = Vec::new();
    bind.extend_from_slice(b"\0stmt\0");
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'B', &bind)).await;

    let describe = b"Sstmt\0";
    send_message(&mut stream, &frontend_message(b'D', describe)).await;
    send_message(&mut stream, &frontend_message(b'E', b"\0\0\0\0\0")).await;
    send_message(&mut stream, &frontend_message(b'S', &[])).await;

    let mut tags = Vec::new();
    loop {
        let (tag, _) = read_message(&mut stream).await;
        tags.push(tag);
        if tag == b'Z' {
            break;
        }
    }
    assert_eq!(tags, vec![b'1', b'2', b't', b'T', b'D', b'C', b'Z']);
}

#[tokio::test]
async fn extended_protocol_supports_named_parameters_and_binary_integer_results() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let mut parse = Vec::new();
    parse.extend_from_slice(b"stmt\0SELECT $1::integer\0");
    parse.extend_from_slice(&1u16.to_be_bytes());
    parse.extend_from_slice(&23u32.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'P', &parse)).await;

    let mut bind = Vec::new();
    bind.extend_from_slice(b"portal\0stmt\0");
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&4i32.to_be_bytes());
    bind.extend_from_slice(&42i32.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'B', &bind)).await;

    send_message(&mut stream, &frontend_message(b'D', b"Pportal\0")).await;
    send_message(&mut stream, &frontend_message(b'E', b"portal\0\0\0\0\0")).await;
    send_message(&mut stream, &frontend_message(b'S', &[])).await;

    let mut messages = Vec::new();
    loop {
        let message = read_message(&mut stream).await;
        let done = message.0 == b'Z';
        messages.push(message);
        if done {
            break;
        }
    }
    assert_eq!(
        messages.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'1', b'2', b'T', b'D', b'C', b'Z']
    );
    assert_eq!(parse_row_description(&messages[2].1)[0].1, 23);
    let data = &messages[3].1;
    assert_eq!(&data[6..10], &42i32.to_be_bytes());
}

#[tokio::test]
async fn extended_catalog_lookup_returns_bound_namespace_rows() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let create =
        send_query_messages(&mut stream, "CREATE TABLE jdbc_catalog_table (id INTEGER)").await;
    assert!(create.iter().any(|(tag, _)| *tag == b'C'));

    let mut parse = Vec::new();
    parse.extend_from_slice(
        b"stmt\0SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relnamespace = $1\0",
    );
    parse.extend_from_slice(&1u16.to_be_bytes());
    parse.extend_from_slice(&26u32.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'P', &parse)).await;

    let mut bind = Vec::new();
    bind.extend_from_slice(b"portal\0stmt\0");
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&1u16.to_be_bytes());
    bind.extend_from_slice(&4i32.to_be_bytes());
    bind.extend_from_slice(&2200u32.to_be_bytes());
    bind.extend_from_slice(&0u16.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'B', &bind)).await;
    send_message(&mut stream, &frontend_message(b'D', b"Pportal\0")).await;
    send_message(&mut stream, &frontend_message(b'E', b"portal\0\0\0\0\0")).await;
    send_message(&mut stream, &frontend_message(b'S', &[])).await;

    let mut messages = Vec::new();
    loop {
        let message = read_message(&mut stream).await;
        let done = message.0 == b'Z';
        messages.push(message);
        if done {
            break;
        }
    }
    assert_eq!(
        messages.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'1', b'2', b'T', b'D', b'C', b'Z']
    );
    assert_eq!(parse_row_description(&messages[2].1)[0].1, 19);
    assert!(messages.iter().any(|(tag, payload)| {
        *tag == b'D' && parse_data_row(payload).contains(&Some("jdbc_catalog_table".into()))
    }));
}

#[tokio::test]
async fn server_preserves_transaction_state_between_queries() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(
        &mut stream,
        "CREATE TABLE session_users (id INTEGER, name TEXT)",
    )
    .await;

    let begin = send_query_messages(&mut stream, "BEGIN").await;
    assert_eq!(
        begin.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    assert_eq!(begin[1].1, vec![b'T']);
    let _ = send_query(&mut stream, "INSERT INTO session_users VALUES (1, 'Alice')").await;
    let before_commit = send_query_messages(&mut stream, "SELECT * FROM session_users").await;
    assert_eq!(
        before_commit
            .iter()
            .map(|(tag, _)| *tag)
            .collect::<Vec<_>>(),
        [b'T', b'D', b'C', b'Z']
    );
    assert_eq!(
        parse_data_row(&before_commit[1].1),
        vec![Some("1".to_string()), Some("Alice".to_string())]
    );
    let commit = send_query_messages(&mut stream, "COMMIT").await;
    assert_eq!(
        commit.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    assert_eq!(commit[1].1, vec![b'I']);
    let after_commit = send_query_messages(
        &mut stream,
        "SELECT session_users.id FROM public.session_users",
    )
    .await;
    assert_eq!(
        parse_data_row(&after_commit[1].1),
        vec![Some("1".to_string())]
    );

    let _ = send_query(&mut stream, "BEGIN").await;
    let _ = send_query(&mut stream, "INSERT INTO session_users VALUES (2, 'Bob')").await;
    let _ = send_query(&mut stream, "ROLLBACK").await;
    let after_rollback = send_query_messages(&mut stream, "SELECT * FROM session_users").await;
    assert_eq!(
        after_rollback
            .iter()
            .filter(|(tag, _)| *tag == b'D')
            .count(),
        1
    );
}

#[tokio::test]
async fn server_reports_actual_dml_counts_in_transactions() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(
        &mut stream,
        "CREATE TABLE dml_counts (id INTEGER PRIMARY KEY, salary INTEGER)",
    )
    .await;
    let _ = send_query(&mut stream, "INSERT INTO dml_counts VALUES (1, 55000)").await;

    let update = send_query_messages(
        &mut stream,
        "UPDATE dml_counts SET salary = salary + 1000 WHERE id = 1",
    )
    .await;
    assert_eq!(command_tag(&update), "UPDATE 1");
    let update_none = send_query_messages(
        &mut stream,
        "UPDATE dml_counts SET salary = salary + 1000 WHERE id = 999",
    )
    .await;
    assert_eq!(command_tag(&update_none), "UPDATE 0");

    let _ = send_query(&mut stream, "BEGIN").await;
    let staged = send_query_messages(
        &mut stream,
        "UPDATE dml_counts SET salary = salary + 1000 WHERE id = 1",
    )
    .await;
    assert_eq!(command_tag(&staged), "UPDATE 1");
    let _ = send_query(&mut stream, "ROLLBACK").await;
    let rows = send_query_messages(&mut stream, "SELECT salary FROM dml_counts WHERE id = 1").await;
    assert_eq!(parse_data_row(&rows[1].1), vec![Some("56000".to_string())]);

    let _ = send_query(&mut stream, "BEGIN").await;
    let staged_commit = send_query_messages(
        &mut stream,
        "UPDATE dml_counts SET salary = salary + 1000 WHERE id = 1",
    )
    .await;
    assert_eq!(command_tag(&staged_commit), "UPDATE 1");
    let _ = send_query(&mut stream, "COMMIT").await;
    let rows = send_query_messages(&mut stream, "SELECT salary FROM dml_counts WHERE id = 1").await;
    assert_eq!(parse_data_row(&rows[1].1), vec![Some("57000".to_string())]);
}
/// Explicit-transaction DML stages effects, not text: each statement
/// executes once, immediately, into the open transaction. Constraint
/// violations surface at statement time (aborting the transaction), and
/// COMMIT only publishes already-validated effects.
#[tokio::test]
async fn explicit_transaction_dml_executes_once_and_enforces_constraints() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;
    let _ = send_query(
        &mut stream,
        "CREATE TABLE once_dml (id INTEGER PRIMARY KEY, v INTEGER)",
    )
    .await;

    // The first insert stages; the duplicate key fails HERE (statement
    // time), aborting the transaction — only ROLLBACK is accepted after.
    let _ = send_query(&mut stream, "BEGIN").await;
    let first = send_query_messages(&mut stream, "INSERT INTO once_dml VALUES (1, 10)").await;
    assert_eq!(command_tag(&first), "INSERT 0 1");
    let second = send_query_messages(&mut stream, "INSERT INTO once_dml VALUES (1, 20)").await;
    assert!(
        second.iter().any(|(tag, _)| *tag == b'E'),
        "duplicate key must fail at statement time: {second:?}"
    );
    let _ = send_query(&mut stream, "ROLLBACK").await;
    let count = send_query_messages(&mut stream, "SELECT count(*) FROM once_dml").await;
    assert_eq!(parse_data_row(&count[1].1), vec![Some("0".to_string())]);

    // A valid transaction persists exactly once and is visible afterwards.
    let _ = send_query(&mut stream, "BEGIN").await;
    let _ = send_query(&mut stream, "INSERT INTO once_dml VALUES (2, 20)").await;
    let _ = send_query(&mut stream, "COMMIT").await;
    let rows = send_query_messages(&mut stream, "SELECT id FROM once_dml").await;
    assert_eq!(parse_data_row(&rows[1].1), vec![Some("2".to_string())]);
}

#[tokio::test]
async fn scram_authentication_completes_and_runs_queries() {
    let (_engine, addr) = start_scram_server().await;
    let mut stream = connect_to_server(addr).await;

    scram_handshake(&mut stream, "secret").await;

    let response = send_query_messages(&mut stream, "SELECT 1").await;
    let tags: Vec<u8> = response.iter().map(|(tag, _)| *tag).collect();
    assert!(tags.contains(&b'T'), "expected RowDescription in {tags:?}");
    assert!(tags.contains(&b'D'), "expected DataRow in {tags:?}");
    assert!(tags.contains(&b'C'), "expected CommandComplete in {tags:?}");
    assert!(tags.contains(&b'Z'), "expected ReadyForQuery in {tags:?}");
}

#[tokio::test]
async fn scram_rejects_wrong_password() {
    let (_engine, addr) = start_scram_server().await;
    let mut stream = connect_to_server(addr).await;

    // Drive the exchange with the wrong password; the server must reply with an
    // ErrorResponse (SQLSTATE 28P01) rather than proceed.
    let startup = b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00";
    send_startup(&mut stream, startup).await;

    let (_tag, payload) = read_message(&mut stream).await;
    let code = u32::from_be_bytes(payload[..4].try_into().unwrap());
    assert_eq!(code, 10);

    let client_first = "n,,n=plomid,r=testnonce";
    let mut initial = Vec::new();
    initial.extend_from_slice(b"SCRAM-SHA-256\x00");
    initial.extend_from_slice(client_first.as_bytes());
    send_message(&mut stream, &frontend_message(b'p', &initial)).await;

    let (_tag, payload) = read_message(&mut stream).await;
    assert_eq!(u32::from_be_bytes(payload[..4].try_into().unwrap()), 11);
    let server_first = String::from_utf8_lossy(&payload[4..]);

    // Client computes a proof for the WRONG password and sends it.
    let attrs = scram_attrs(&server_first);
    let salt = plomid_network::scram::base64_decode(attrs.get("s").unwrap()).unwrap();
    let iterations: u32 = attrs.get("i").unwrap().parse().unwrap();
    let nonce = attrs.get("r").unwrap();
    let salted = plomid_network::scram::pbkdf2_sha256(b"wrongpw".as_slice(), &salt, iterations, 32);
    let client_key = plomid_network::scram::hmac_sha256(&salted, b"Client Key");
    let stored_key = plomid_network::scram::sha256(&client_key);
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("n=plomid,r=testnonce,{server_first},{without_proof}");
    let client_sig = plomid_network::scram::hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!(
        "{},p={}",
        without_proof,
        plomid_network::scram::base64_encode(&proof)
    );
    send_message(
        &mut stream,
        &frontend_message(b'p', client_final.as_bytes()),
    )
    .await;

    // The server sends an ErrorResponse (28P01).
    let (tag, payload) = read_message(&mut stream).await;
    assert_eq!(tag, b'E');
    let error_text = String::from_utf8_lossy(&payload);
    assert!(
        error_text.contains("28P01"),
        "expected 28P01 error, got {error_text}"
    );
}

#[tokio::test]
async fn invalid_startup_does_not_crash_the_server() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;

    // A startup payload whose version field is garbage must be rejected
    // without killing the process.
    let mut startup: Vec<u8> = Vec::new();
    startup.extend_from_slice(&0x0001_9999u32.to_be_bytes());
    startup.extend_from_slice(b"user\x00plomid\x00\x00");
    send_startup(&mut stream, &startup).await;
    let response = read_first_byte_or_none(&mut stream).await;
    assert!(
        response.is_none() || response == Some(b'E'),
        "server must reject an invalid startup message"
    );
    drop(stream);

    // The listener is still healthy: a normal client can connect afterwards.
    let mut second = connect_to_server(addr).await;
    handshake(&mut second).await;
    let rows = send_query_messages(&mut second, "SELECT 1").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

#[tokio::test]
async fn unknown_frontend_tag_is_rejected_without_a_panic() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    // 0x7F is not a valid frontend message tag.
    send_message(&mut stream, &frontend_message(0x7F, b"")).await;
    let response = read_first_byte_or_none(&mut stream).await;
    assert!(
        response.is_none() || response == Some(b'E'),
        "server must close a connection that sends an unknown tag"
    );
    drop(stream);

    // A following normal connection still works.
    let mut second = connect_to_server(addr).await;
    handshake(&mut second).await;
    let rows = send_query_messages(&mut second, "SELECT 2").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

#[tokio::test]
async fn truncated_parse_message_returns_error_not_a_crash() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    // A Parse message that ends before its required fields is malformed.
    send_message(&mut stream, &frontend_message(b'P', &[0x00u8, 0x00])).await;
    let response = read_first_byte_or_none(&mut stream).await;
    assert!(
        response.is_none() || response == Some(b'E'),
        "server must report or close on a truncated Parse"
    );
    drop(stream);

    let mut second = connect_to_server(addr).await;
    handshake(&mut second).await;
    let rows = send_query_messages(&mut second, "SELECT 3").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

#[tokio::test]
async fn cancel_request_closes_only_the_cancel_connection() {
    let (_engine, addr) = start_test_server().await;

    // A CancelRequest is an untagged 16-byte message.
    let mut cancel: Vec<u8> = Vec::new();
    cancel.extend_from_slice(&16u32.to_be_bytes());
    cancel.extend_from_slice(&80877102u32.to_be_bytes());
    cancel.extend_from_slice(&1u32.to_be_bytes());
    cancel.extend_from_slice(&2u32.to_be_bytes());
    let mut stream = connect_to_server(addr).await;
    send_message(&mut stream, &cancel).await;
    // The server closes without a response.
    let response = read_first_byte_or_none(&mut stream).await;
    assert!(
        response.is_none(),
        "cancel connection must be closed without a protocol response"
    );
    drop(stream);

    // Other sessions are unaffected.
    let mut second = connect_to_server(addr).await;
    handshake(&mut second).await;
    let rows = send_query_messages(&mut second, "SELECT 4").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

/// Reads one byte from the stream within a timeout.
///
/// Returns `None` when the connection closed, errored, or produced nothing
/// within the window — i.e. the server did not send a protocol response.
async fn read_first_byte_or_none(stream: &mut tokio::net::TcpStream) -> Option<u8> {
    let mut buf = [0u8; 1];
    match timeout(Duration::from_secs(3), stream.read(&mut buf)).await {
        Err(_) => None,
        Ok(Ok(0)) => None,
        Ok(Ok(_)) => Some(buf[0]),
        Ok(Err(_)) => None,
    }
}

fn command_tag(messages: &[(u8, Vec<u8>)]) -> String {
    let (_, payload) = messages.iter().find(|(tag, _)| *tag == b'C').unwrap();
    String::from_utf8(payload[..payload.len() - 1].to_vec()).unwrap()
}

fn parse_row_description(bytes: &[u8]) -> Vec<(String, u32, i16)> {
    let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let mut position = 2;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let end = bytes[position..]
            .iter()
            .position(|byte| *byte == 0)
            .unwrap()
            + position;
        let name = String::from_utf8(bytes[position..end].to_vec()).unwrap();
        position = end + 1;
        position += 6;
        let type_oid = u32::from_be_bytes(bytes[position..position + 4].try_into().unwrap());
        position += 4;
        let type_size = i16::from_be_bytes(bytes[position..position + 2].try_into().unwrap());
        position += 8;
        fields.push((name, type_oid, type_size));
    }
    fields
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

/// Autocommit extended-protocol INSERTs persist immediately: with no explicit
/// transaction open there is no coming COMMIT, so staging rows would silently
/// discard acknowledged writes. Regression test for silent loss of prepared
/// INSERTs (fake `INSERT 0 1` + Sync, rows missing afterwards).
#[tokio::test]
async fn autocommit_extended_insert_persists_without_commit() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query(
        &mut stream,
        "CREATE TABLE auto_ext (id INTEGER PRIMARY KEY, v INTEGER)",
    )
    .await;

    // Named prepared statement, text params, Parse once + Bind/Execute/Sync
    // per row, no COMMIT — the standard psycopg-style autocommit flow.
    let mut parse = Vec::new();
    parse.extend_from_slice(b"ins\0INSERT INTO auto_ext(id, v) VALUES ($1, $2)\0");
    parse.extend_from_slice(&0u16.to_be_bytes());
    send_message(&mut stream, &frontend_message(b'P', &parse)).await;
    for id in 1..=3i32 {
        let mut bind = Vec::new();
        bind.extend_from_slice(b"\0ins\0");
        bind.extend_from_slice(&0u16.to_be_bytes());
        bind.extend_from_slice(&2u16.to_be_bytes());
        for value in [id.to_string(), (id * 10).to_string()] {
            bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
            bind.extend_from_slice(value.as_bytes());
        }
        bind.extend_from_slice(&0u16.to_be_bytes());
        send_message(&mut stream, &frontend_message(b'B', &bind)).await;
        send_message(&mut stream, &frontend_message(b'E', b"\0\0\0\0\0")).await;
        send_message(&mut stream, &frontend_message(b'S', &[])).await;
        // Drain to ReadyForQuery; every Execute must report real success.
        let mut saw_complete = false;
        loop {
            let (tag, _) = read_message(&mut stream).await;
            if tag == b'C' {
                saw_complete = true;
            }
            if tag == b'Z' {
                break;
            }
        }
        assert!(saw_complete, "row {id} got no command completion");
    }

    // Same session, plain query: all three rows must be there.
    let rows = send_query_messages(&mut stream, "SELECT COUNT(*) FROM auto_ext").await;
    let count_row = rows
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .expect("count query returned no data row");
    assert_eq!(parse_data_row(&count_row.1), vec![Some("3".to_string())]);

    // A second connection must see them too (durable, not session-staged).
    let mut stream2 = connect_to_server(addr).await;
    handshake(&mut stream2).await;
    let rows2 = send_query_messages(&mut stream2, "SELECT SUM(v) FROM auto_ext").await;
    let sum_row = rows2
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .expect("sum query returned no data row");
    assert_eq!(parse_data_row(&sum_row.1), vec![Some("60".to_string())]);
}

/// Describe must report exactly the shape direct execution reports:
/// the LIMIT-0 shape probe may skip row reads but never column metadata.
/// Compares RowDescription bytes (names, OIDs, formats) between the
/// extended Describe path and the simple-query path across a shape matrix.
#[tokio::test]
async fn describe_shape_matches_execution_across_shapes() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    let _ = send_query(
        &mut stream,
        "CREATE TABLE shapes(id INTEGER PRIMARY KEY, name TEXT, score DOUBLE PRECISION, payload JSONB)",
    )
    .await;
    let _ = send_query(
        &mut stream,
        "INSERT INTO shapes(id, name, score, payload) VALUES (1, 'a', 1.5, '{\"k\":1}'), (2, 'b', 2.5, '{\"k\":2}')",
    )
    .await;

    // (label, SQL with optional $1 text param bound to '1')
    let shapes = [
        (
            "point_lookup",
            "SELECT id, name, score FROM shapes WHERE id = $1",
        ),
        ("literals", "SELECT 1, 'abc', 1.5, NULL"),
        ("casts", "SELECT '2024-01-01'::DATE, $1::INTEGER, 1::BIGINT"),
        (
            "aggregate",
            "SELECT COUNT(*), SUM(score), AVG(score) FROM shapes",
        ),
        (
            "group_by",
            "SELECT name, COUNT(*) FROM shapes GROUP BY name",
        ),
        (
            "join_self",
            "SELECT a.id, b.score FROM shapes a JOIN shapes b ON a.id = b.id WHERE a.id = $1",
        ),
        (
            "order_limit",
            "SELECT id, name FROM shapes ORDER BY id DESC LIMIT 1",
        ),
        (
            "cte",
            "WITH x AS (SELECT id, score FROM shapes WHERE id = $1) SELECT * FROM x",
        ),
        (
            "subquery",
            "SELECT * FROM (SELECT id, name FROM shapes WHERE id = $1) AS s",
        ),
        (
            "union_all",
            "SELECT id FROM shapes WHERE id = $1 UNION ALL SELECT 99",
        ),
        (
            "json_ops",
            "SELECT payload->'k', payload->>'k' FROM shapes WHERE id = $1",
        ),
        (
            "case_expr",
            "SELECT CASE WHEN id = $1 THEN 'one' ELSE 'other' END FROM shapes",
        ),
        ("values", "VALUES (1, 'a'), (2, 'b')"),
        ("count_star", "SELECT COUNT(*) FROM shapes"),
    ];

    for (label, sql) in shapes {
        // Extended Describe path (NULL placeholders, LIMIT-0 probe inside).
        let mut parse = Vec::new();
        parse.extend_from_slice(b"sh\0");
        parse.extend_from_slice(sql.as_bytes());
        parse.push(0);
        parse.extend_from_slice(&0u16.to_be_bytes());
        send_message(&mut stream, &frontend_message(b'P', &parse)).await;
        let mut bind = Vec::new();
        bind.extend_from_slice(b"\0sh\0");
        bind.extend_from_slice(&0u16.to_be_bytes());
        if sql.contains("$1") {
            bind.extend_from_slice(&1u16.to_be_bytes());
            bind.extend_from_slice(&1i32.to_be_bytes());
            bind.extend_from_slice(b"1");
        } else {
            bind.extend_from_slice(&0u16.to_be_bytes());
        }
        bind.extend_from_slice(&0u16.to_be_bytes());
        send_message(&mut stream, &frontend_message(b'B', &bind)).await;
        send_message(&mut stream, &frontend_message(b'D', b"P\0")).await;
        send_message(&mut stream, &frontend_message(b'E', b"\0\0\0\0\0")).await;
        send_message(&mut stream, &frontend_message(b'S', &[])).await;
        let mut described: Option<Vec<u8>> = None;
        loop {
            let (tag, payload) = read_message(&mut stream).await;
            if tag == b'E' {
                panic!(
                    "{label}: describe/execute failed: {}",
                    String::from_utf8_lossy(&payload)
                );
            }
            if tag == b'T' {
                described = Some(payload);
            }
            if tag == b'Z' {
                break;
            }
        }
        let described = described.unwrap_or_else(|| panic!("{label}: no RowDescription"));

        // Simple-query path for the same shape (NULL literal for $1).
        let direct_sql = sql.replace("$1", "NULL");
        let direct = send_query_messages(&mut stream, &direct_sql).await;
        let direct_t = direct
            .iter()
            .find(|(tag, _)| *tag == b'T')
            .unwrap_or_else(|| panic!("{label}: direct execution returned no description"));
        assert_eq!(
            described, direct_t.1,
            "{label}: describe shape differs from execution shape"
        );
    }
}

// ---------------------------------------------------------------------------
// Query guardrails.
//
// In-process tests only: each spins its own server on an ephemeral port.
// ---------------------------------------------------------------------------

/// Like [`handshake`], but captures the per-connection BackendKeyData
/// (`pid`, `secret`) the server emits at startup.
async fn handshake_capture_keys(stream: &mut tokio::net::TcpStream) -> (u32, u32) {
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

    let mut keys = None;
    loop {
        let (tag, payload) = read_message(stream).await;
        if tag == b'K' {
            assert_eq!(payload.len(), 8, "BackendKeyData carries pid + secret");
            keys = Some((
                u32::from_be_bytes(payload[..4].try_into().unwrap()),
                u32::from_be_bytes(payload[4..8].try_into().unwrap()),
            ));
        }
        if tag == b'Z' {
            break;
        }
        if tag == b'E' {
            let msg = String::from_utf8_lossy(&payload);
            panic!("authentication failed: {msg}");
        }
    }
    keys.expect("server must emit BackendKeyData at startup")
}

fn simple_query_bytes(sql: &str) -> Vec<u8> {
    let query = format!("{sql};\x00");
    let mut message = Vec::with_capacity(1 + 4 + query.len());
    message.push(b'Q');
    message.extend_from_slice(&(query.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(query.as_bytes());
    message
}

fn error_text(messages: &[(u8, Vec<u8>)]) -> String {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'E')
        .map(|(_, payload)| String::from_utf8_lossy(payload).into_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

/// `statement_timeout` aborts a long query with SQLSTATE 57014 while the
/// connection stays usable; `statement_timeout = 0` disables enforcement.
#[tokio::test]
async fn statement_timeout_aborts_long_query_and_zero_disables() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    // Sanity: a small series works with the default (disabled) timeout.
    let small =
        send_query_messages(&mut stream, "SELECT COUNT(*) FROM generate_series(1, 1000)").await;
    assert!(
        small.iter().any(|(tag, _)| *tag == b'D'),
        "small query must succeed: {small:?}"
    );

    // The knob is accepted and visible back via SHOW.
    let set = send_query_messages(&mut stream, "SET statement_timeout = '100ms'").await;
    assert_eq!(
        set.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let show = send_query_messages(&mut stream, "SHOW statement_timeout").await;
    assert_eq!(parse_data_row(&show[1].1), vec![Some("100ms".to_string())]);

    // A cross join far too large to finish in 100ms must abort, not hang.
    // (Single generate_series are capped at 1M rows; the join's nested-loop
    // probe path is where the cooperative guard polls.)
    let timed = timeout(
        Duration::from_secs(30),
        send_query_messages(
            &mut stream,
            "SELECT COUNT(*) FROM generate_series(1, 1500) a, generate_series(1, 1500) b",
        ),
    )
    .await
    .expect("timed-out query must answer, not hang the connection");
    assert!(
        timed.iter().any(|(tag, _)| *tag == b'E'),
        "long query must error under statement_timeout: {timed:?}"
    );
    assert!(
        error_text(&timed).contains("57014"),
        "timeout must report SQLSTATE 57014: {}",
        error_text(&timed)
    );

    // The connection survives a timeout: disable and run again.
    let _ = send_query_messages(&mut stream, "SET statement_timeout = 0").await;
    let rows = send_query_messages(&mut stream, "SELECT 1").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

/// A CancelRequest carrying the target's BackendKeyData aborts its running
/// query: the victim connection drops, the server stays up, and other
/// sessions are unaffected.
///
/// Needs a multi-thread runtime: the victim's synchronous execution blocks
/// its worker (via `block_in_place`, as in production), so the cancel
/// connection requires a sibling worker to be served concurrently. A
/// current-thread runtime would serialize the two and the cancel could never
/// land mid-query.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_request_aborts_running_query_and_server_stays_up() {
    let (_engine, addr) = start_test_server().await;
    let mut victim = connect_to_server(addr).await;
    let (pid, secret) = handshake_capture_keys(&mut victim).await;

    // Fire a query far too large to finish before the cancel arrives.
    // (Single generate_series are capped at 1M rows and fail fast; the
    // cross join's nested-loop probe path runs long enough to cancel.)
    send_message(
        &mut victim,
        &simple_query_bytes(
            "SELECT COUNT(*) FROM generate_series(1, 2500) a, generate_series(1, 2500) b",
        ),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Cancel from a second connection (untagged 16-byte message, no response).
    let mut cancel = connect_to_server(addr).await;
    let mut request: Vec<u8> = Vec::new();
    request.extend_from_slice(&16u32.to_be_bytes());
    request.extend_from_slice(&80877102u32.to_be_bytes());
    request.extend_from_slice(&pid.to_be_bytes());
    request.extend_from_slice(&secret.to_be_bytes());
    send_message(&mut cancel, &request).await;
    assert!(
        read_first_byte_or_none(&mut cancel).await.is_none(),
        "cancel connection must be closed without a protocol response"
    );
    drop(cancel);

    // The victim sees the cancel (57014) and then the connection drops.
    let (tag, payload) = timeout(Duration::from_secs(30), read_message(&mut victim))
        .await
        .expect("victim must answer after cancel");
    assert_eq!(tag, b'E', "victim query must be cancelled, got tag {tag}");
    assert!(
        String::from_utf8_lossy(&payload).contains("57014"),
        "cancel must report SQLSTATE 57014"
    );
    assert!(
        read_first_byte_or_none(&mut victim).await.is_none(),
        "victim connection must drop after cancel"
    );
    drop(victim);

    // The server is unaffected: a fresh session works.
    let mut other = connect_to_server(addr).await;
    handshake(&mut other).await;
    let rows = send_query_messages(&mut other, "SELECT 4").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

/// `idle_in_transaction_session_timeout` reaps an idle-in-transaction
/// connection with FATAL 25P03; idle connections without a transaction are
/// left alone.
#[tokio::test]
async fn idle_in_transaction_timeout_reaps_idle_connection() {
    let (_engine, addr) = start_test_server().await;
    let mut stream = connect_to_server(addr).await;
    handshake(&mut stream).await;

    // No transaction: idleness is fine even past the timeout.
    let _ = send_query_messages(
        &mut stream,
        "SET idle_in_transaction_session_timeout = '200ms'",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let rows = send_query_messages(&mut stream, "SELECT 1").await;
    assert_eq!(command_tag(&rows), "SELECT 1");

    // Open a transaction, then go idle past the timeout.
    let begin = send_query_messages(&mut stream, "BEGIN").await;
    assert_eq!(
        begin.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        [b'C', b'Z']
    );
    let (tag, payload) = timeout(Duration::from_secs(10), read_message(&mut stream))
        .await
        .expect("reaped connection must send FATAL");
    assert_eq!(
        tag, b'E',
        "reap must terminate with an error, got tag {tag}"
    );
    assert!(
        String::from_utf8_lossy(&payload).contains("25P03"),
        "reap must report SQLSTATE 25P03"
    );
    assert!(
        read_first_byte_or_none(&mut stream).await.is_none(),
        "reaped connection must be closed"
    );
    drop(stream);

    // The server is unaffected.
    let mut other = connect_to_server(addr).await;
    handshake(&mut other).await;
    let rows = send_query_messages(&mut other, "SELECT 2").await;
    assert_eq!(command_tag(&rows), "SELECT 1");
}

/// Drives one cleartext login attempt, returning (elapsed, success).
async fn auth_attempt(addr: std::net::SocketAddr, password: &str) -> (Duration, bool) {
    let started = std::time::Instant::now();
    let mut stream = connect_to_server(addr).await;
    let startup = b"\x00\x03\x00\x00user\x00plomid\x00database\x00plomid\x00\x00";
    send_startup(&mut stream, startup).await;
    let (tag, payload) = read_message(&mut stream).await;
    // A locked account is refused before the password prompt.
    if tag == b'E' {
        return (started.elapsed(), false);
    }
    assert_eq!(tag, b'R');
    assert_eq!(
        u32::from_be_bytes(payload[..4].try_into().unwrap()),
        3,
        "expected cleartext auth request"
    );
    let body = format!("{password}\x00");
    let mut message = Vec::with_capacity(1 + 4 + body.len());
    message.push(b'p');
    message.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    message.extend_from_slice(body.as_bytes());
    send_message(&mut stream, &message).await;
    let (tag, _) = read_message(&mut stream).await;
    let elapsed = started.elapsed();
    if tag == b'E' {
        return (elapsed, false);
    }
    assert_eq!(tag, b'R', "expected AuthenticationOk, got tag {tag}");
    loop {
        let (tag, _) = read_message(&mut stream).await;
        if tag == b'Z' {
            break;
        }
    }
    (elapsed, true)
}

/// Locked-out rejections have their own counter (F-007 monitoring): a new
/// lockout is not the same signal as an attempt refused while locked.
#[tokio::test]
async fn locked_out_attempts_increment_the_dedicated_counter() {
    let (_engine, addr) = start_test_server().await;
    let before = plomid_core::observability::snapshot().auth_locked_out;

    for _ in 0..5 {
        let (_, ok) = auth_attempt(addr, "wrong").await;
        assert!(!ok, "failures before the threshold must be rejected");
    }
    let (_, ok) = auth_attempt(addr, "wrong").await;
    assert!(!ok, "locked account must reject even the next attempt");

    let after = plomid_core::observability::snapshot().auth_locked_out;
    assert!(
        after > before,
        "locked-out rejection must increment auth_locked_out (before={before}, after={after})"
    );
}

/// Repeated failures from one IP face a growing delay; a success resets it.
#[tokio::test]
async fn auth_rate_limit_delays_repeated_failures_and_resets_on_success() {
    let (_engine, addr) = start_test_server().await;

    let (t1, ok1) = auth_attempt(addr, "wrong").await;
    assert!(!ok1);
    let (t2, ok2) = auth_attempt(addr, "wrong").await;
    assert!(!ok2);
    let (t3, ok3) = auth_attempt(addr, "wrong").await;
    assert!(!ok3);
    // Growing: ~100ms before attempt 2, ~200ms before attempt 3.
    assert!(
        t2 >= Duration::from_millis(80),
        "second failure must be delayed, took {t2:?} (first: {t1:?})"
    );
    assert!(
        t3 >= Duration::from_millis(150),
        "third failure must face a larger delay, took {t3:?}"
    );

    // A correct login still succeeds (delay applies, then AuthenticationOk).
    let (_, ok) = auth_attempt(addr, "secret").await;
    assert!(ok, "correct password must succeed");

    // The success resets the counter: the next failure is fast again.
    let (t4, ok4) = auth_attempt(addr, "wrong").await;
    assert!(!ok4);
    assert!(
        t4 < t3,
        "post-success failure must be fast again (reset), took {t4:?} vs {t3:?}"
    );
}
