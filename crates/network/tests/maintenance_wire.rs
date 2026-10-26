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
// F-002/F-003 wire contract: REINDEX and ANALYZE are executed by the
// engine over client connections — never answered with a fabricated
// CommandComplete. Observable proof:
//   REINDEX → `plomid_stat_admin()` gains `reindex` and the next ANN query
//             misses + rebuilds (cached graphs were dropped).
//   ANALYZE → `plomid_stat_admin()` gains `analyze` (statistics really
//             collected), and inside an explicit transaction it refuses
//             with an engine error instead of silently succeeding.
//
// This is the only test in this integration binary on purpose: the
// `plomid_stat_*` counters and `vector.*` settings are process-global, so
// a shared binary would let concurrent tests move the counters between
// measurements.

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
        "plomid-maintenance-wire-{label}-{}-{}",
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

fn error_text(msgs: &[(u8, Vec<u8>)]) -> String {
    msgs.iter()
        .find(|(t, _)| *t == b'E')
        .map(|(_, p)| String::from_utf8_lossy(p).to_string())
        .unwrap_or_default()
}

/// CommandComplete tag of a single statement's response.
fn command_tag(msgs: &[(u8, Vec<u8>)]) -> String {
    let payload = &msgs
        .iter()
        .find(|(t, _)| *t == b'C')
        .expect("CommandComplete")
        .1;
    let end = payload
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(payload.len());
    String::from_utf8(payload[..end].to_vec()).unwrap()
}

/// One textual cell of the first data row, by column name.
fn cell(msgs: &[(u8, Vec<u8>)], column: &str) -> String {
    let fields = rowdesc(msgs);
    let index = fields
        .iter()
        .position(|(name, _)| name == column)
        .unwrap_or_else(|| panic!("missing column {column}: {fields:?}"));
    data_rows(msgs)[0][index]
        .clone()
        .unwrap_or_else(|| panic!("NULL cell {column}"))
}

fn int_cell(msgs: &[(u8, Vec<u8>)], column: &str) -> i64 {
    cell(msgs, column)
        .parse()
        .unwrap_or_else(|e| panic!("cell {column}: {e}"))
}

/// Count for an admin action in `plomid_stat_admin()`, when present.
fn admin_count(msgs: &[(u8, Vec<u8>)], action: &str) -> Option<i64> {
    let fields = rowdesc(msgs);
    let action_index = fields
        .iter()
        .position(|(name, _)| name == "action")
        .expect("action column");
    let count_index = fields
        .iter()
        .position(|(name, _)| name == "count")
        .expect("count column");
    data_rows(msgs)
        .iter()
        .find(|row| row[action_index].as_deref() == Some(action))
        .map(|row| row[count_index].as_ref().unwrap().parse().unwrap())
}

const ANN_QUERY: &str = "SELECT id FROM mw ORDER BY e <-> [1,0] LIMIT 1";
const ADMIN_SQL: &str = "SELECT * FROM plomid_stat_admin()";
const VECTOR_STATS_SQL: &str = "SELECT * FROM plomid_vector_stats()";

#[tokio::test]
async fn reindex_and_analyze_execute_over_the_wire() {
    let (_engine, addr) = start_test_server().await;
    let mut s = connect(addr).await;
    handshake(&mut s).await;

    assert!(!has_error(
        &simple(&mut s, "CREATE TABLE mw (id INTEGER, e VECTOR(FLOAT32, 2))").await
    ));
    assert!(!has_error(
        &simple(
            &mut s,
            "INSERT INTO mw VALUES (1, [1,0]), (2, [0,1]), (3, [1,1])"
        )
        .await
    ));
    // Force the ANN path at tiny sizes so graph caches are exercised.
    assert!(!has_error(
        &simple(&mut s, "SET vector.auto_ann_min_rows = 0").await
    ));

    // Warm the ANN cache and capture the miss/build counters.
    let warm = simple(&mut s, ANN_QUERY).await;
    assert!(!has_error(&warm), "warm ANN query: {:?}", error_text(&warm));
    assert_eq!(data_rows(&warm), vec![vec![Some("1".to_string())]]);
    let stats = simple(&mut s, VECTOR_STATS_SQL).await;
    let misses_warm = int_cell(&stats, "ann_cache_misses_total");
    let builds_warm = int_cell(&stats, "index_builds_total");
    assert!(misses_warm >= 1, "warm query built a graph: {stats:?}");
    assert!(builds_warm >= 1, "warm query built a graph: {stats:?}");

    // REINDEX (autocommit) runs in the engine: real tag, admin record.
    let reindex = simple(&mut s, "REINDEX").await;
    assert!(
        !has_error(&reindex),
        "REINDEX must execute, not stub: {:?}",
        error_text(&reindex)
    );
    assert_eq!(command_tag(&reindex), "REINDEX");
    let admin = simple(&mut s, ADMIN_SQL).await;
    assert!(
        admin_count(&admin, "reindex").is_some_and(|count| count >= 1),
        "reindex must be recorded: {:?}",
        data_rows(&admin)
    );

    // The next query must miss and rebuild: invalidation is observable.
    let after = simple(&mut s, ANN_QUERY).await;
    assert!(!has_error(&after));
    assert_eq!(data_rows(&after), vec![vec![Some("1".to_string())]]);
    let stats = simple(&mut s, VECTOR_STATS_SQL).await;
    let misses_after = int_cell(&stats, "ann_cache_misses_total");
    let builds_after = int_cell(&stats, "index_builds_total");
    assert!(
        misses_after > misses_warm,
        "post-REINDEX query missed the dropped graph ({misses_warm} -> {misses_after})"
    );
    assert!(
        builds_after > builds_warm,
        "post-REINDEX query rebuilt the graph ({builds_warm} -> {builds_after})"
    );

    // REINDEX inside an explicit transaction block: executed (safe at any
    // time), never a silent fabricated CommandComplete.
    assert!(!has_error(&simple(&mut s, "BEGIN").await));
    let in_tx = simple(&mut s, "REINDEX").await;
    assert!(
        !has_error(&in_tx),
        "in-transaction REINDEX must execute: {:?}",
        error_text(&in_tx)
    );
    assert_eq!(command_tag(&in_tx), "REINDEX");
    assert!(!has_error(&simple(&mut s, "COMMIT").await));
    let admin = simple(&mut s, ADMIN_SQL).await;
    assert!(
        admin_count(&admin, "reindex").is_some_and(|count| count >= 2),
        "in-transaction reindex must be recorded: {:?}",
        data_rows(&admin)
    );
    let after_tx = simple(&mut s, ANN_QUERY).await;
    assert_eq!(data_rows(&after_tx), vec![vec![Some("1".to_string())]]);
    let stats = simple(&mut s, VECTOR_STATS_SQL).await;
    assert!(
        int_cell(&stats, "ann_cache_misses_total") > misses_after,
        "in-transaction REINDEX must invalidate the cache too"
    );

    // ANALYZE (autocommit) executes real statistics collection.
    let analyze = simple(&mut s, "ANALYZE mw").await;
    assert!(
        !has_error(&analyze),
        "ANALYZE must execute, not stub: {:?}",
        error_text(&analyze)
    );
    assert_eq!(command_tag(&analyze), "ANALYZE");
    let admin = simple(&mut s, ADMIN_SQL).await;
    assert!(
        admin_count(&admin, "analyze").is_some_and(|count| count >= 1),
        "analyze must be recorded: {:?}",
        data_rows(&admin)
    );

    // Inside a transaction block ANALYZE refuses with an engine error
    // (statistics read committed rows only) — explicit, never silent.
    assert!(!has_error(&simple(&mut s, "BEGIN").await));
    let in_tx_analyze = simple(&mut s, "ANALYZE mw").await;
    let text = error_text(&in_tx_analyze);
    assert!(
        has_error(&in_tx_analyze) && text.contains("transaction block"),
        "in-transaction ANALYZE must refuse explicitly: {text:?}"
    );
    // The failed transaction recovers with ROLLBACK like any statement error.
    assert!(!has_error(&simple(&mut s, "ROLLBACK").await));
}
