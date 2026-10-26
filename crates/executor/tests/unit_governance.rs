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
// Moved from src/governance.rs inline #[cfg(test)].
use plomid_executor::error::SqlError;
use plomid_executor::governance::*;
use plomid_htap::{Admission, HtapConfig, ResourceGovernor, WorkloadClass as HtapClass};
use plomid_sql::QueryResult;
use std::time::Duration;

fn tiny_config() -> HtapConfig {
    HtapConfig {
        global_budget_bytes: 1000,
        oltp_reservation_bytes: 200,
        olap_budget_bytes: 650,
        maintenance_budget_bytes: 100,
        system_reserve_bytes: 50,
        oltp_max_concurrency: 256,
        olap_max_concurrency: 2,
        maintenance_max_concurrency: 4,
        system_max_concurrency: 8,
        oltp_max_parallelism: 4,
        olap_max_parallelism: 8,
        olap_max_bytes_per_query: 650,
        statement_timeout_ms: None,
        oltp_priority_latency: true,
        warning_threshold_pct: 70,
        pressure_threshold_pct: 85,
        critical_threshold_pct: 95,
        small_txn_row_threshold: plomid_htap::SMALL_TXN_ROW_THRESHOLD,
    }
}

#[test]
fn classify_maps_writes_to_oltp_and_selects_to_olap() {
    assert_eq!(
        workload_class_for_sql("INSERT INTO t VALUES (1)"),
        HtapClass::Oltp
    );
    assert_eq!(
        workload_class_for_sql("UPDATE t SET a = 1"),
        HtapClass::Oltp
    );
    assert_eq!(
        workload_class_for_sql("CREATE TABLE t (a INT)"),
        HtapClass::Oltp
    );
    assert_eq!(workload_class_for_sql("BEGIN"), HtapClass::Oltp);
    assert_eq!(workload_class_for_sql("SELECT * FROM t"), HtapClass::Olap);
    assert_eq!(
        workload_class_for_sql("WITH x AS (SELECT 1) SELECT * FROM x"),
        HtapClass::Olap
    );
}

#[test]
fn oltp_admitted_under_olap_saturation() {
    let governor = ResourceGovernor::new(tiny_config()).expect("tiny config valid");
    // Saturate OLAP: fill every concurrency slot (pool budget untouched,
    // so the Queue below is purely the concurrency gate).
    for _ in 0..tiny_config().olap_max_concurrency {
        assert!(governor.admit(HtapClass::Olap, 10, 1).is_admitted());
    }
    // The pool is now slot-full: another OLAP queues.
    assert!(matches!(
        governor.admit(HtapClass::Olap, 10, 1),
        Admission::Queue { .. }
    ));
    // OLTP bypasses the fence and is still admitted through our path.
    let guard = admit_with(
        &governor,
        "INSERT INTO t VALUES (1)",
        Duration::from_secs(0),
    )
    .expect("OLTP admitted under OLAP saturation");
    // The test governor (not the global one) owns this slot: release it
    // there, then forget the guard so its Drop does not also decrement
    // the global governor's saturating counters.
    governor.release(HtapClass::Oltp, 0, 1);
    std::mem::forget(guard);
}

#[test]
fn queued_olap_times_out_without_wait() {
    let governor = ResourceGovernor::new(tiny_config()).expect("tiny config valid");
    for _ in 0..tiny_config().olap_max_concurrency {
        assert!(governor.admit(HtapClass::Olap, 10, 1).is_admitted());
    }
    let err = admit_with(&governor, "SELECT * FROM t", Duration::from_secs(0))
        .expect_err("saturated OLAP must not admit with zero wait");
    assert!(matches!(err, SqlError::Storage(_)), "{err}");
    assert!(format!("{err}").contains("queue timeout"), "{err}");
}

#[test]
fn reject_maps_to_structured_resource_exhausted_error() {
    let governor = ResourceGovernor::new(tiny_config()).expect("tiny config valid");
    let reason = match governor.admit(
        HtapClass::Olap,
        tiny_config().olap_max_bytes_per_query + 1,
        1,
    ) {
        Admission::Reject { reason } => reason,
        other => panic!("expected Reject, got {other:?}"),
    };
    let err = admission_reject_error(&reason);
    assert!(matches!(err, SqlError::Storage(_)), "{err}");
    assert!(format!("{err}").contains("resource exhausted"), "{err}");
}

#[test]
fn oltp_admission_never_rejects_under_default_config() {
    for sql in [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "CREATE TABLE t (a INT)",
        "BEGIN",
        "SELECT 1",
    ] {
        admit_query(sql).expect("default config must admit");
    }
}

#[test]
fn pressure_accessor_tracks_live_manager_usage() {
    let manager = global_memory_manager();
    let fill = manager.config().global_budget * 8 / 10;
    let context = manager.global_context();
    context.try_allocate(fill).expect("80% of 1GiB fits");
    assert!(current_pressure_level() >= 1, "80% usage must leave Normal");
    context.release(fill);
    assert_eq!(current_pressure_level(), 0);
}

#[test]
fn result_charging_records_ring_stat() {
    use plomid_sql::Value;
    let results = vec![QueryResult::Rows {
        columns: vec!["a".to_string()],
        column_types: vec![None],
        rows: vec![vec![Value::Null], vec![Value::Null]],
    }];
    charge_result_buffers("SELECT a FROM t", &results).expect("charge fits");
    let stats = recent_query_stats();
    let last = stats.last().expect("ring holds the charge");
    assert_eq!(last.class, "SELECT");
    assert!(last.result_bytes > 0, "{}", last.result_bytes);
    assert_eq!(last.spill_bytes, 0);
    assert!(last.peak_bytes >= last.current_bytes);
    assert!(stats.len() <= 128, "{}", stats.len());
}
