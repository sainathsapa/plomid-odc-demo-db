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

//! Live-path proof that SQL results flow through the streaming bridge.
//!
//! Uses only existing execution machinery (`Executor::execute` on the real
//! storage engine) and pipes the returned `QueryResult` through
//! `plomid_executor::result_stream`. No new row types, no rewritten operators.

use plomid_executor::result_stream::{cursor_for_rows, rows_to_stream, rows_to_stream_limited};
use plomid_sql::{QueryResult, Value};
use plomid_stream::StreamConfig;
use plomid_txn::PlomidStorageEngine;

fn live_executor(
    tag: &str,
) -> (
    plomid_executor::Executor<plomid_txn::PlomidStorageEngine>,
    std::path::PathBuf,
) {
    let storage_path =
        std::env::temp_dir().join(format!("plomid-stream-{tag}-{}", std::process::id()));
    let wal_path = storage_path.with_extension("wal");
    let _ = std::fs::remove_dir_all(&storage_path);
    let _ = std::fs::remove_dir_all(&wal_path);
    let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32).expect("create engine");
    (
        plomid_executor::Executor::new(engine).expect("create executor"),
        storage_path,
    )
}

fn bulk_insert(
    executor: &mut plomid_executor::Executor<plomid_txn::PlomidStorageEngine>,
    table: &str,
    cols: &str,
    values: impl Iterator<Item = String>,
) {
    // One multi-row statement per 500 rows: identical rows, one WAL commit
    // per batch instead of one fsync per row.
    let mut batch = values.peekable();
    loop {
        let chunk: Vec<String> = batch.by_ref().take(500).collect();
        if chunk.is_empty() {
            break;
        }
        executor
            .execute(&format!(
                "INSERT INTO {table} ({cols}) VALUES {};",
                chunk.join(", ")
            ))
            .expect("insert");
    }
}

#[test]
fn sql_rows_stream_in_bounded_batches() {
    let (mut executor, root) = live_executor("wiring");
    executor
        .execute("CREATE TABLE t (id INTEGER, name TEXT);")
        .expect("create");
    bulk_insert(
        &mut executor,
        "t",
        "id, name",
        (1..=2500).map(|id| format!("({id}, 'row{id}')")),
    );
    // Real SQL execution through the existing engine.
    let result = executor.execute("SELECT * FROM t;").expect("select");
    let QueryResult::Rows { rows, .. } = &result else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 2500);

    // Pipe the real result through the streaming bridge in small batches.
    let config = StreamConfig::new().with_batch_size(500);
    let (columns, mut stream) = rows_to_stream(result, config).expect("rows stream");
    assert!(!columns.is_empty());
    let mut delivered = 0u64;
    let mut peak_batch = 0usize;
    while let Some(batch) = stream.next_batch(0).expect("batch") {
        assert!(batch.len() <= 500, "batches stay bounded");
        peak_batch = peak_batch.max(batch.len());
        delivered += batch.len() as u64;
    }
    assert_eq!(delivered, 2500, "no silent truncation");
    assert!(peak_batch <= 500);

    drop(executor);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

#[test]
fn explicit_sql_limit_streams_exactly_n() {
    let (mut executor, root) = live_executor("limit");
    executor
        .execute("CREATE TABLE u (id INTEGER);")
        .expect("create");
    bulk_insert(
        &mut executor,
        "u",
        "id",
        (1..=100).map(|id| format!("({id})")),
    );
    let result = executor.execute("SELECT * FROM u;").expect("select");
    let config = StreamConfig::new().with_batch_size(10);
    let (_, mut stream) = rows_to_stream_limited(result, config, Some(25)).expect("limited stream");
    let mut delivered = 0u64;
    while stream.next_batch(0).expect("batch").is_some() {
        delivered = stream.position();
    }
    assert_eq!(delivered, 25, "explicit LIMIT is the only cap");

    let result = executor.execute("SELECT * FROM u;").expect("select");
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows");
    };
    let mut cursor = cursor_for_rows(rows, StreamConfig::new().with_batch_size(30));
    let page1: Vec<Vec<Value>> = cursor.fetch(30).expect("fetch");
    assert_eq!(page1.len(), 30);
    assert_eq!(cursor.position(), 30);
    cursor.close();
    assert!(cursor.is_closed());

    drop(executor);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}
