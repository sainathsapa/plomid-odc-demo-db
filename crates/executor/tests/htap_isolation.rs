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

//! Phase-27 closing proof: OLTP stays responsive while OLAP floods the system.
//!
//! Strategy: drive the REAL process-global governor through the public
//! [`plomid_executor::governance::admit_query`] path (held `AdmissionGuard`s
//! are the flood — no test doubles on the hot path), and use tiny local
//! [`plomid_htap::ResourceGovernor`]s for the admission-outcome matrix that
//! would otherwise need a 5-second queue wait against the global singleton.
//! No production files were touched; there are no test hooks.
//!
//! Parallel-safety: exactly one test below touches the global governor
//! (`oltp_responsive_while_olap_floods_global_path`, fully sequential
//! inside), so the suite needs no inter-test synchronization and no
//! `--test-threads=1`. Tiny governors are per-test locals; the memory test
//! only moves the memory-manager singleton, which governor admission never
//! reads (the governor keeps its own utilization counters).
//!
//! Classification premise (asserted, not assumed): on the SQL-text path there
//! is no row estimate, so `workload_class_for_sql` maps every `SELECT` to
//! `Olap` and every write to `Oltp`. A bare point `SELECT` therefore takes
//! the OLAP pool and queues under saturation — only writes take the OLTP
//! fast path here. The tests prove exactly that.

use plomid_executor::governance::{
    admit_query, current_pressure_level, global_memory_manager, workload_class_for_sql,
};
use plomid_executor::Executor;
use plomid_htap::{Admission, HtapConfig, ResourceGovernor, WorkloadClass};
use plomid_sql::QueryResult;
use plomid_txn::PlomidStorageEngine;

/// Tiny but valid governor config (mirrors the unit config in
/// `governance.rs`): small enough to saturate deterministically, large
/// enough to separate the OLTP reserve (200) from the OLAP pool (650).
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

/// Unique scratch dir per run (pid-scoped, after `htap_stats.rs`): the
/// pre-existing suite has fixed-`/tmp`-path collisions under parallel runs.
fn scratch(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "plomid-htap-isolation-{label}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

/// The real closing proof, on the real path: saturate every OLAP
/// concurrency slot of the process-global governor with held
/// `AdmissionGuard`s, then show OLTP still admits (writes execute
/// end-to-end through a live `Executor`, whose `execute_all` choke point
/// calls `admit_query`), and that dropping the flood restores OLAP
/// admission — no leaks. Asserts admission outcomes only, never latency.
#[test]
fn oltp_responsive_while_olap_floods_global_path() {
    // Classification premise this proof rests on.
    assert_eq!(
        workload_class_for_sql("INSERT INTO t (id) VALUES (1)"),
        WorkloadClass::Oltp
    );
    assert_eq!(
        workload_class_for_sql("SELECT * FROM t WHERE id = 1"),
        WorkloadClass::Olap,
        "SQL-text SELECTs take the OLAP pool (no row estimate on this path)"
    );

    // Slot count read from the same default the global governor was built
    // with (`governance.rs`), so the flood stays exact if it ever changes.
    let olap_slots = HtapConfig::default().olap_max_concurrency;
    assert!(olap_slots >= 1, "governor must have at least one OLAP slot");

    // Saturate OLAP: hold every concurrency slot on this thread.
    let mut flood = Vec::with_capacity(olap_slots);
    for _ in 0..olap_slots {
        flood.push(admit_query("SELECT * FROM t").expect("flood slot admits"));
    }

    // OLTP admits while saturated: returns Ok, never queues behind OLAP.
    let oltp_guard =
        admit_query("INSERT INTO t (id) VALUES (1)").expect("OLTP admits under OLAP flood");
    drop(oltp_guard);

    // End-to-end: OLTP writes execute through the live engine while flooded.
    // (`execute` -> `execute_all` -> `admit_query`; the OLTP slot above
    // proves the gate, this proves the write path behind it.) Reads are
    // deliberately absent here: bare SELECTs are OLAP-classified and would
    // queue behind the flood by design (proved as `Queue` on the tiny
    // governor below); they run again after the release.
    let root = scratch("flood-live");
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(&root, &wal, 32).expect("create engine");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("CREATE TABLE t (id INTEGER);")
        .expect("DDL admits while flooded");
    for id in 1..=5 {
        executor
            .execute(&format!("INSERT INTO t (id) VALUES ({id});"))
            .expect("OLTP write stays responsive while OLAP floods");
    }
    let stats = plomid_executor::governance::recent_query_stats();
    assert!(
        stats.iter().any(|s| s.class == "INSERT"),
        "executed writes are accounted in the stats ring"
    );

    // Release the flood: OLAP admission is restored (no leaked slots).
    drop(flood);
    admit_query("SELECT * FROM t").expect("OLAP admits again after release");
    let result = executor
        .execute("SELECT * FROM t;")
        .expect("read after release");
    match result {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 5, "no write lost to the flood"),
        other => panic!("expected rows, got {other:?}"),
    }
    cleanup(&root);
}

/// Unit-level admission matrix on a tiny governor (direct `admit` returns
/// the `Admission` outcome immediately — no queue waits, no timing):
/// saturated OLAP queues, OLAP gets reduced parallelism under pressure,
/// OLTP still admits through both.
#[test]
fn olap_queues_and_derates_while_oltp_admits() {
    let governor = ResourceGovernor::new(tiny_config()).expect("tiny config valid");

    // Fill every OLAP concurrency slot.
    assert!(governor.admit(WorkloadClass::Olap, 10, 1).is_admitted());
    assert!(governor.admit(WorkloadClass::Olap, 10, 1).is_admitted());
    assert_eq!(governor.olap_in_use_slots(), 2);

    // One more OLAP queues (retryable) instead of starving or failing.
    assert!(
        matches!(
            governor.admit(WorkloadClass::Olap, 10, 1),
            Admission::Queue { .. }
        ),
        "saturated OLAP must queue"
    );

    // OLTP bypasses the saturated OLAP pool entirely.
    assert!(
        governor.admit(WorkloadClass::Oltp, 10, 1).is_admitted(),
        "OLTP admits while OLAP is saturated"
    );

    // A bare point SELECT is OLAP-classified, so it queues too.
    assert_eq!(
        workload_class_for_sql("SELECT * FROM t WHERE id = 1"),
        WorkloadClass::Olap
    );

    governor.release(WorkloadClass::Olap, 10, 1);
    governor.release(WorkloadClass::Olap, 10, 1);
    governor.release(WorkloadClass::Oltp, 10, 1);
    assert_eq!(governor.olap_in_use_slots(), 0);
    assert_eq!(governor.global_used_bytes(), 0);

    // Reduced parallelism under Warning pressure: push global utilization
    // to 70% via cache accounting, then OLAP admits with derated
    // parallelism 8 -> 4 while OLTP still admits untouched.
    let pressured = ResourceGovernor::new(tiny_config()).expect("tiny config valid");
    pressured.account_cache_bytes(700);
    assert_eq!(pressured.utilization_pct(), 70);
    let reduced = match pressured.admit(WorkloadClass::Olap, 10, 1) {
        Admission::Admit {
            reduced_parallelism,
        } => reduced_parallelism,
        other => panic!("OLAP must admit (derated) at Warning, got {other:?}"),
    };
    assert_eq!(
        reduced, 4,
        "Warning derates OLAP parallelism 8 -> 4, got {reduced}"
    );
    assert_eq!(pressured.effective_olap_concurrency(), 1);
    assert!(
        pressured.admit(WorkloadClass::Oltp, 10, 1).is_admitted(),
        "OLTP admits at Warning pressure"
    );
    pressured.release(WorkloadClass::Olap, 10, 1);
    pressured.release(WorkloadClass::Oltp, 10, 1);
    pressured.release_cache_bytes(700);
    assert_eq!(pressured.global_used_bytes(), 0);
}

/// The OLTP reserve fence: with OLTP pressure present, OLAP cannot push
/// global usage into the reserve (`Reject`), while OLTP still admits; after
/// release the counters return to baseline (no leaks).
#[test]
fn oltp_reserve_fence_rejects_olap_but_not_oltp() {
    let governor = ResourceGovernor::new(tiny_config()).expect("tiny config valid");
    assert_eq!(governor.global_used_bytes(), 0);

    // OLTP holds 150 of the 200-byte reserve (>= half => fence is armed).
    assert!(governor.admit(WorkloadClass::Oltp, 150, 1).is_admitted());
    assert!(governor.oltp_pressure_exists());

    // OLAP fills its own pool: 150 + 650 = 800 global, ceiling is
    // 1000 - (200 - 150) = 950, so this fits outside the reserve.
    assert!(governor.admit(WorkloadClass::Olap, 650, 1).is_admitted());

    // OLAP pushing into the fenced reserve is rejected (hard, not queued:
    // 800 + 200 = 1000 > ceiling 950).
    let fenced = governor.admit(WorkloadClass::Olap, 200, 1);
    match &fenced {
        Admission::Reject { reason } => assert!(
            reason.contains("reservation"),
            "fence must name the reserve, got: {reason}"
        ),
        other => panic!("OLAP breaching the reserve must Reject, got {other:?}"),
    }

    // OLTP bypasses the fence it armed.
    assert!(
        governor.admit(WorkloadClass::Oltp, 10, 1).is_admitted(),
        "OLTP admits past its own reserve fence"
    );

    // Release everything: counters return to baseline, OLAP admits again.
    governor.release(WorkloadClass::Oltp, 150, 1);
    governor.release(WorkloadClass::Olap, 650, 1);
    governor.release(WorkloadClass::Oltp, 10, 1);
    assert_eq!(governor.global_used_bytes(), 0);
    assert_eq!(governor.olap_in_use_slots(), 0);
    assert_eq!(governor.oltp_in_use_slots(), 0);
    assert!(
        governor.admit(WorkloadClass::Olap, 10, 1).is_admitted(),
        "OLAP admits again after release (no leaked slots)"
    );
    governor.release(WorkloadClass::Olap, 10, 1);
    assert_eq!(governor.global_used_bytes(), 0);
}

/// Memory pressure on the real global manager: drive global usage to 75%
/// (Warning band is [70, 85)) with accounting-only reservations — no real
/// RAM, ~858M of counters against the 1 GiB default budget — assert the
/// live pressure level rises, then release and assert it clears.
#[test]
fn memory_pressure_rises_and_clears() {
    let manager = global_memory_manager();
    // Fraction needed for Warning: thresholds are 0.70 / 0.85 / 0.95, so
    // 3/4 lands squarely in Warning without touching Pressure.
    let fill = manager.config().global_budget * 3 / 4;
    assert!(fill > 0);
    let context = manager.global_context();
    context
        .try_allocate(fill)
        .expect("75% of the global budget fits");
    assert!(
        current_pressure_level() >= 1,
        "75% usage must leave Normal (level 0)"
    );
    context.release(fill);
    assert_eq!(
        current_pressure_level(),
        0,
        "pressure must clear after release"
    );
}
