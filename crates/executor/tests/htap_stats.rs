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
//
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
//! HTAP operator surface: `plomid_htap_stats()` through a real Executor.
//!
//! One test function (not several) so the process-wide counters need no
//! inter-test synchronization: this binary runs a single flow with one reset
//! at the start.

use plomid_core::observability;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn scratch(label: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("plomid-htap-stats-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &std::path::Path) -> Executor<PlomidStorageEngine> {
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(root, &wal, 64))
        .expect("open engine");
    Executor::new(engine).expect("executor")
}

fn rows_of(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn columns_of(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows { columns, .. } => columns.clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

fn column<'a>(columns: &[String], row: &'a [Value], name: &str) -> &'a Value {
    let index = columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("missing htap column {name}"));
    &row[index]
}

/// Runs the HTAP stats query and returns `(columns, first row)`.
fn htap_query(session: &mut Executor<PlomidStorageEngine>) -> (Vec<String>, Vec<Value>) {
    let result = session
        .execute("SELECT * FROM plomid_htap_stats()")
        .expect("htap stats query");
    let columns = columns_of(&result);
    let mut rows = rows_of(result);
    assert_eq!(rows.len(), 1, "htap surface is exactly one row");
    (columns, rows.remove(0))
}

#[test]
fn htap_stats_columns_exist_and_values_move() {
    observability::reset_for_tests();
    let root = scratch("flow");
    let mut session = open(&root);

    session
        .execute("CREATE TABLE htap_t (id INTEGER);")
        .expect("create");
    session
        .execute("INSERT INTO htap_t VALUES (1), (2);")
        .expect("insert");
    let selected = rows_of(session.execute("SELECT * FROM htap_t;").expect("select"));
    assert_eq!(selected.len(), 2);

    // The stats row renders before its own batch is recorded, so it matches
    // the three batches above exactly.
    let (columns, row) = htap_query(&mut session);
    assert_eq!(
        columns,
        vec![
            "queries_total",
            "errors_total",
            "slow_queries_total",
            "slow_max_ms",
            "top_slow_class",
            "top_slow_class_max_ms",
            "spill_bytes_total",
            "result_buffer_bytes",
            "result_buffer_peak",
            "admission_admitted",
            "admission_rejected",
            "admission_waiting",
            "pressure_level",
            "global_current_bytes",
            "global_peak_bytes",
            "oltp_used_bytes",
            "oltp_reserved_bytes",
            "olap_used_bytes",
            "last_query_class",
            "last_query_current_bytes",
            "last_query_peak_bytes",
            "last_query_result_bytes",
        ]
    );
    assert_eq!(int(column(&columns, &row, "queries_total")), 3);
    assert_eq!(int(column(&columns, &row, "errors_total")), 0);
    // Latency driver mirrors the core per-class map, computed the same way
    // the catalog builder does (strictly-greatest max, NULL when idle).
    let live = observability::snapshot();
    let mut expected_class: Option<&str> = None;
    let mut expected_max: u64 = 0;
    for (class, stat) in &live.slow_by_class {
        if stat.max_ms > expected_max {
            expected_max = stat.max_ms;
            expected_class = Some(class);
        }
    }
    match expected_class {
        Some(class) => assert!(
            matches!(column(&columns, &row, "top_slow_class"), Value::Text(text) if text == class),
            "top class must mirror core, got {:?}",
            column(&columns, &row, "top_slow_class")
        ),
        None => assert_eq!(
            column(&columns, &row, "top_slow_class"),
            &Value::Null,
            "no queries means no driver"
        ),
    }
    assert_eq!(
        int(column(&columns, &row, "top_slow_class_max_ms")),
        expected_max as i64
    );
    // Nothing has spilled, waited, or held result bytes on this fresh flow.
    assert_eq!(int(column(&columns, &row, "spill_bytes_total")), 0);
    assert_eq!(int(column(&columns, &row, "admission_waiting")), 0);
    // Live governance gauges: fresh process, no pressure; result charging
    // released its bytes (current back to zero) but the peak persists, and
    // the OLTP reservation mirrors `MemoryConfig::default`.
    assert_eq!(int(column(&columns, &row, "pressure_level")), 0);
    assert_eq!(int(column(&columns, &row, "global_current_bytes")), 0);
    assert!(
        int(column(&columns, &row, "global_peak_bytes")) > 0,
        "result charging must leave a global high-water mark"
    );
    assert_eq!(int(column(&columns, &row, "oltp_used_bytes")), 0);
    assert_eq!(
        int(column(&columns, &row, "oltp_reserved_bytes")),
        (256 << 20) as i64
    );
    assert_eq!(int(column(&columns, &row, "olap_used_bytes")), 0);
    // The ring tail is the SELECT above (the stats query itself is still
    // in flight, so it cannot appear yet): honest encoded bytes held.
    assert!(
        matches!(column(&columns, &row, "last_query_class"), Value::Text(class) if class == "SELECT"),
        "ring tail must be the SELECT, got {:?}",
        column(&columns, &row, "last_query_class")
    );
    assert!(
        int(column(&columns, &row, "last_query_result_bytes")) > 0,
        "SELECT returned rows, so its result bytes must be positive"
    );
    assert!(
        int(column(&columns, &row, "last_query_peak_bytes"))
            >= int(column(&columns, &row, "last_query_current_bytes"))
    );

    // Counters recorded through the public API surface through SQL.
    observability::record_spill_bytes(4096);
    observability::record_admission(true);
    observability::record_admission(false);
    observability::record_admission_queued();
    observability::record_result_buffer_grow(512);
    let (columns, row) = htap_query(&mut session);
    assert_eq!(int(column(&columns, &row, "spill_bytes_total")), 4096);
    assert_eq!(int(column(&columns, &row, "admission_admitted")), 1);
    assert_eq!(int(column(&columns, &row, "admission_rejected")), 1);
    assert_eq!(int(column(&columns, &row, "admission_waiting")), 1);
    assert_eq!(int(column(&columns, &row, "result_buffer_bytes")), 512);
    assert_eq!(int(column(&columns, &row, "result_buffer_peak")), 512);
    assert_eq!(int(column(&columns, &row, "pressure_level")), 0);
    assert!(
        matches!(column(&columns, &row, "last_query_class"), Value::Text(class) if class == "SELECT"),
        "ring tail must be the previous stats query, got {:?}",
        column(&columns, &row, "last_query_class")
    );
    observability::record_result_buffer_shrink(512);
    observability::record_admission_dequeued();
    let (columns, row) = htap_query(&mut session);
    assert_eq!(int(column(&columns, &row, "result_buffer_bytes")), 0);
    assert_eq!(int(column(&columns, &row, "result_buffer_peak")), 512);
    assert_eq!(int(column(&columns, &row, "admission_waiting")), 0);

    // The bare-table spelling resolves through the same catalog arm.
    let bare = rows_of(
        session
            .execute("SELECT spill_bytes_total FROM plomid_htap_stats")
            .expect("bare htap stats"),
    );
    assert_eq!(bare.len(), 1);
    assert_eq!(int(&bare[0][0]), 4096);

    cleanup(&root);
}
