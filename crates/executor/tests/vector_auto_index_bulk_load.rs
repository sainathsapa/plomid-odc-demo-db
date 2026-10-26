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
//! F-001 regression: bulk load with the automatic vector index enabled.
//!
//! `CREATE TABLE` registers an HNSW index for every dense vector column, so
//! incremental inserts maintain multi-kilobyte index keys inside the same
//! storage B+Tree that also stores short primary-key entries. A count-based
//! internal split could leave a half exceeding the page payload once a page
//! mixed both key sizes, failing batch inserts past a few thousand rows with
//! "B+Tree node exceeds page payload". Loading 10,000 dim-64 rows in
//! 1,000-row batches with the auto index left in place must succeed, and
//! every row must remain readable afterwards.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::sync::atomic::{AtomicU64, Ordering};

const ROWS: i64 = 10_000;
const BATCH: i64 = 1_000;
const DIM: u32 = 64;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn lane(index: i64) -> String {
    let mut out = String::with_capacity(8 * DIM as usize);
    out.push('[');
    for axis in 0..DIM {
        if axis > 0 {
            out.push(',');
        }
        let value = ((index * 31 + i64::from(axis) * 17) % 997) as f32 / 997.0;
        out.push_str(&value.to_string());
    }
    out.push(']');
    out
}

#[test]
fn bulk_load_with_dim64_auto_vector_index() {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let storage = std::env::temp_dir().join(format!("plomid-auto-vec-{}-{id}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-auto-vec-wal-{}-{id}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);

    let engine = PlomidStorageEngine::open(&storage, &wal, 32).expect("open engine");
    let mut ex = Executor::new(engine).expect("executor");
    ex.execute(&format!(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, {DIM}))"
    ))
    .expect("create table");

    let mut start = 0_i64;
    while start < ROWS {
        let stop = (start + BATCH).min(ROWS);
        let values: Vec<String> = (start..stop)
            .map(|index| format!("({index}, {})", lane(index)))
            .collect();
        ex.execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
            .unwrap_or_else(|error| panic!("insert rows {start}..{stop} failed: {error}"));
        start = stop;
    }

    let count = match ex.execute("SELECT COUNT(*) FROM t").expect("count") {
        QueryResult::Rows { rows, .. } => match rows[0][0] {
            Value::Int8(n) => n,
            Value::Int4(n) => i64::from(n),
            ref value => panic!("unexpected count type: {value:?}"),
        },
        other => panic!("count -> {other:?}"),
    };
    assert_eq!(count, ROWS, "every row must survive the load");

    match ex
        .execute("SELECT id FROM t WHERE id = 9999")
        .expect("point query")
    {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1, "point query finds the last row");
            match rows[0][0] {
                Value::Int8(n) => assert_eq!(n, 9999),
                ref value => panic!("unexpected id type: {value:?}"),
            }
        }
        other => panic!("point query -> {other:?}"),
    }

    // The automatic index is still registered and usable after the load.
    let listed = match ex
        .execute("SELECT index_name, state FROM plomid_vector_indexes()")
        .expect("index list")
    {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("index list -> {other:?}"),
    };
    assert!(
        listed
            .iter()
            .any(|row| matches!(&row[1], Value::Text(state) if state == "READY")),
        "auto vector index must report READY: {listed:?}"
    );

    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
