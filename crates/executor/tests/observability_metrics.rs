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
//! Phase 9 observability: every required metric family moves through real
//! workloads and is exposed on its stats SQL surface.
//!
//! One test function (not several) so the process-wide counters need no
//! inter-test synchronization: a single flow with one reset at the start.
//! Covers transactions, WAL, contention, buffer/storage, columnar, vector
//! (counts, rebuilds, freshness, backend inventory), and admin actions.

use plomid_core::observability;
use plomid_executor::{Executor, MaintenancePolicy, MaintenanceWorker};
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;
use std::sync::{Arc, Mutex};

fn scratch(label: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("plomid-obs-metrics-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

fn rows_of(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
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

fn text(value: &Value) -> String {
    match value {
        Value::Text(s) => s.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

fn cell(rows: &[Vec<Value>], column: &str, all_columns: &[String]) -> i64 {
    let position = all_columns
        .iter()
        .position(|name| name == column)
        .unwrap_or_else(|| panic!("no column {column} in {all_columns:?}"));
    int(&rows[0][position])
}

fn columns_of(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows { columns, .. } => columns.clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn phase9_metrics_move_through_sql() {
    observability::reset_for_tests();
    let root = scratch("all");
    let wal = root.with_extension("wal");

    // ---- Single-writer workload: DDL/DML/reads/rollback/vacuum/reindex.
    {
        let engine = PlomidStorageEngine::open(&root, &wal, 32).expect("mount");
        let mut ex = Executor::new(engine).expect("executor");
        ex.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
            .expect("ddl");
        for i in 0..20i64 {
            ex.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
                .expect("insert");
        }
        for _ in 0..5 {
            rows(&mut ex, "SELECT COUNT(*) FROM t");
            rows(&mut ex, "SELECT v FROM t WHERE id = 7");
        }
        ex.execute("BEGIN; INSERT INTO t VALUES (1000, 1); ROLLBACK;")
            .expect("rollback");
        ex.execute("VACUUM t").expect("vacuum");
        ex.execute("REINDEX").expect("reindex");
        // Vector workload: ANN serve, rebuild after invalidation, freshness.
        ex.execute("SET vector.auto_ann_min_rows = 0").expect("ann");
        ex.execute("CREATE TABLE v (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 2))")
            .expect("vector ddl");
        ex.execute("INSERT INTO v VALUES (1, [1,0]), (2, [0,1]), (3, [0.5,0.5])")
            .expect("vector insert");
        rows(&mut ex, "SELECT id FROM v ORDER BY e <-> [1,0] LIMIT 2");
        ex.execute("REINDEX").expect("reindex again");
        rows(&mut ex, "SELECT id FROM v ORDER BY e <-> [1,0] LIMIT 2");
        // Engine checkpoint: covers WAL bytes, records durations.
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }

    // ---- Concurrent workload: mutex/gate/barrier/latency samples.
    {
        let engine = PlomidStorageEngine::open(&root, &wal, 32).expect("reopen");
        let shared = Arc::new(Mutex::new(engine));
        let mut ex = Executor::new_shared(Arc::clone(&shared)).expect("session");
        for i in 20..30i64 {
            ex.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
                .expect("concurrent insert");
        }
        drop(ex);
        drop(shared);
    }

    // ---- Background worker: maintenance queue wait samples.
    {
        let engine = PlomidStorageEngine::open(&root, &wal, 32).expect("reopen");
        let shared = Arc::new(Mutex::new(engine));
        let worker = MaintenanceWorker::spawn(Arc::clone(&shared));
        let link = worker.link();
        let mut ex = Executor::new_shared(Arc::clone(&shared)).expect("session");
        ex.set_background_maintenance(link.clone());
        ex.set_maintenance_policy(MaintenancePolicy::new(1));
        for i in 30..36i64 {
            ex.execute(&format!("INSERT INTO t VALUES ({i}, {})", i * 2))
                .expect("worker insert");
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while link.stats().1 < 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        worker.shutdown();
        drop(ex);
        drop(shared);
    }

    // ---- Assert every surface.
    {
        let engine = PlomidStorageEngine::open(&root, &wal, 32).expect("reopen");
        let mut ex = Executor::new(engine).expect("executor");

        let result = ex
            .execute("SELECT * FROM plomid_stat_transactions()")
            .expect("txn");
        let columns = columns_of(&result);
        let body = rows_of(result);
        assert!(cell(&body, "txn_started", &columns) >= 40, "{body:?}");
        assert!(cell(&body, "txn_committed", &columns) >= 40, "{body:?}");
        assert!(cell(&body, "txn_aborted", &columns) >= 1, "{body:?}");
        // Conservation: every begun transaction has ended or is the
        // in-flight querying transaction itself.
        assert_eq!(
            cell(&body, "txn_started", &columns),
            cell(&body, "txn_committed", &columns)
                + cell(&body, "txn_aborted", &columns)
                + cell(&body, "txn_active", &columns),
            "{body:?}"
        );
        assert!(
            cell(&body, "txn_duration_us_count", &columns) >= 40,
            "{body:?}"
        );
        assert!(
            cell(&body, "txn_commit_latency_us_count", &columns) >= 40,
            "{body:?}"
        );

        let result = ex.execute("SELECT * FROM plomid_stat_wal()").expect("wal");
        let columns = columns_of(&result);
        let body = rows_of(result);
        assert!(cell(&body, "wal_bytes_total", &columns) > 0, "{body:?}");
        assert!(cell(&body, "wal_appends_total", &columns) > 0, "{body:?}");
        assert!(
            cell(&body, "wal_checkpoint_bytes_total", &columns) > 0,
            "{body:?}"
        );

        let result = ex
            .execute("SELECT * FROM plomid_stat_contention()")
            .expect("contention");
        let columns = columns_of(&result);
        let body = rows_of(result);
        // Concurrent commits must have waited on the engine mutex, the
        // group barrier, and (gated writes) the write gates.
        assert!(
            cell(&body, "engine_mutex_wait_us_count", &columns) > 0,
            "{body:?}"
        );
        assert!(cell(&body, "gate_wait_us_count", &columns) > 0, "{body:?}");
        assert!(
            cell(&body, "wal_barrier_wait_us_count", &columns) > 0,
            "{body:?}"
        );
        assert!(
            cell(&body, "maintenance_queue_wait_us_count", &columns) > 0,
            "{body:?}"
        );

        let result = ex
            .execute("SELECT * FROM plomid_stat_buffer()")
            .expect("buffer");
        let columns = columns_of(&result);
        let body = rows_of(result);
        let hits = cell(&body, "buffer_hits", &columns);
        let misses = cell(&body, "buffer_misses", &columns);
        assert!(hits + misses > 0, "pool must observe traffic: {body:?}");
        assert!(
            cell(&body, "checkpoint_bytes_total", &columns) > 0,
            "{body:?}"
        );
        assert!(
            cell(&body, "checkpoint_duration_us_count", &columns) > 0,
            "{body:?}"
        );

        let result = ex
            .execute("SELECT * FROM plomid_stat_columnar()")
            .expect("columnar");
        let columns = columns_of(&result);
        let body = rows_of(result);
        for required in [
            "segments_examined",
            "segments_skipped",
            "columns_read",
            "batches",
            "spill_bytes_total",
            "spill_files_total",
            "tables_stale",
        ] {
            assert!(
                columns.iter().any(|name| name == required),
                "missing {required} in {columns:?}"
            );
        }
        let _ = cell(&body, "segments_examined", &columns);

        let result = ex
            .execute("SELECT * FROM plomid_vector_stats()")
            .expect("vector stats");
        let columns = columns_of(&result);
        let body = rows_of(result);
        assert!(cell(&body, "ann_queries_total", &columns) >= 2, "{body:?}");
        assert!(cell(&body, "rebuilds_total", &columns) >= 1, "{body:?}");
        assert!(
            cell(&body, "fresh_total", &columns) + cell(&body, "stale_total", &columns) >= 2,
            "{body:?}"
        );

        let result = ex
            .execute("SELECT * FROM plomid_vector_backend()")
            .expect("backend");
        let names: Vec<String> = rows_of(result).iter().map(|row| text(&row[0])).collect();
        for required in [
            "compute",
            "cpu_arch",
            "cpu_threads",
            "gpu_requested",
            "gpu_available",
        ] {
            assert!(names.iter().any(|name| name == required), "{names:?}");
        }

        let result = ex
            .execute("SELECT * FROM plomid_stat_admin()")
            .expect("admin");
        let names: Vec<String> = rows_of(result).iter().map(|row| text(&row[0])).collect();
        assert!(names.iter().any(|name| name == "reindex"), "{names:?}");
        assert!(names.iter().any(|name| name == "vacuum"), "{names:?}");
    }

    cleanup(&root);
}
