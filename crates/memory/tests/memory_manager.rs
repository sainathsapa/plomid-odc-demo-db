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

//! Integration tests for the `plomid-memory` crate: accounting at every
//! hierarchy level, peak correctness, error (never panic) semantics,
//! concurrent allocation within the global limit, and pressure
//! transitions.

use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use plomid_memory::context::{CacheKind, MaintenanceOp, WorkloadClass};
use plomid_memory::operator::ALL_KINDS;
use plomid_memory::{
    MemoryConfig, MemoryError, MemoryManager, OperatorKind, PressureState, PressureThresholds,
};

/// Config where every sub-budget equals `global`, isolating the level
/// under test unless a test overrides a specific budget.
fn flat_config(global: u64) -> MemoryConfig {
    MemoryConfig {
        global_budget: global,
        oltp_reservation: global,
        olap_budget: global,
        maintenance_budget: global,
        query_budget_default: global,
        connection_budget: global,
        worker_budget: global,
        result_buffer_budget: global,
        temp_limit: global,
        pressure_thresholds: PressureThresholds::default(),
    }
}

#[test]
fn global_accounting_current_peak_count() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let global = mgr.global_context();
    global.try_allocate(100).unwrap();
    assert_eq!(global.current(), 100);
    assert_eq!(global.peak(), 100);
    assert_eq!(global.alloc_count(), 1);
    global.release(40);
    assert_eq!(global.current(), 60);
    assert_eq!(global.peak(), 100);
    assert_eq!(global.alloc_count(), 1);
    global.try_allocate(10).unwrap();
    assert_eq!(
        (global.current(), global.peak(), global.alloc_count()),
        (70, 100, 2)
    );
}

#[test]
fn query_accounting_nests_under_workload_and_global() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Oltp, 7);
    query.try_allocate(1024).unwrap();
    assert_eq!(query.current(), 1024);
    assert_eq!(mgr.workload_context(WorkloadClass::Oltp).current(), 1024);
    assert_eq!(mgr.global_context().current(), 1024);
    // A sibling workload branch is unaffected.
    assert_eq!(mgr.workload_context(WorkloadClass::Olap).current(), 0);
    query.release(1024);
    assert_eq!(mgr.global_context().current(), 0);
}

#[test]
fn operator_accounting_nests_under_query() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Olap, 11);
    let join = mgr.create_operator_context(&query, OperatorKind::HashJoin);
    join.try_allocate(2048).unwrap();
    assert_eq!(join.current(), 2048);
    assert_eq!(query.current(), 2048);
    assert_eq!(mgr.global_context().current(), 2048);
    assert_eq!(mgr.global_context().peak(), 2048);
    join.release(2048);
    assert_eq!(query.current(), 0);
    assert_eq!(query.peak(), 2048);
}

#[test]
fn connection_accounting_is_global_branch() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let conn = mgr.create_connection_context(3);
    conn.try_allocate(512).unwrap();
    assert_eq!(conn.current(), 512);
    assert_eq!(mgr.global_context().current(), 512);
    let stats = mgr.connection_stats(&conn);
    assert_eq!(stats.connection_id, 3);
    assert_eq!(
        (stats.current, stats.peak, stats.allocations),
        (512, 512, 1)
    );
    conn.release(512);
    assert_eq!(mgr.global_context().current(), 0);
}

#[test]
fn worker_accounting_nests_under_query() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Oltp, 21);
    let worker = mgr.create_worker_context(&query, 0);
    worker.try_allocate(256).unwrap();
    assert_eq!(worker.current(), 256);
    assert_eq!(query.current(), 256);
    assert_eq!(mgr.global_context().current(), 256);
    let stats = mgr.worker_stats(&worker);
    assert_eq!(stats.worker_id, 0);
    assert_eq!(stats.current, 256);
    worker.release(256);
    assert_eq!(query.peak(), 256);
}

#[test]
fn maintenance_accounting_and_stats() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let vacuum = mgr.create_maintenance_task(MaintenanceOp::Vacuum);
    vacuum.try_allocate(4096).unwrap();
    assert_eq!(vacuum.current(), 4096);
    assert_eq!(mgr.maintenance_context().current(), 4096);
    assert_eq!(mgr.global_context().current(), 4096);
    let stats = mgr.maintenance_stats();
    assert_eq!(
        (stats.current, stats.peak, stats.allocations),
        (4096, 4096, 1)
    );
    vacuum.release(4096);
    assert_eq!(mgr.global_context().current(), 0);
}

#[test]
fn cache_accounting_and_stats() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let page_cache = mgr.cache_partition(CacheKind::PageCache);
    page_cache.try_allocate(777).unwrap();
    assert_eq!(page_cache.current(), 777);
    assert_eq!(mgr.cache_context().current(), 777);
    assert_eq!(mgr.global_context().current(), 777);
    // Partitions are cached singletons per kind.
    assert!(Arc::ptr_eq(
        &page_cache,
        &mgr.cache_partition(CacheKind::PageCache)
    ));
    let stats = mgr.cache_stats();
    assert_eq!(stats.current, 777);
    assert_eq!(stats.partitions, 1);
}

#[test]
fn peak_tracks_high_water_across_alloc_release_cycles() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let ctx = mgr.create_connection_context(1);
    ctx.try_allocate(100).unwrap();
    ctx.release(50);
    ctx.try_allocate(30).unwrap();
    assert_eq!(ctx.current(), 80);
    assert_eq!(ctx.peak(), 100);
    ctx.release(80);
    ctx.try_allocate(100).unwrap();
    assert_eq!((ctx.current(), ctx.peak()), (100, 100));
    ctx.try_allocate(1).unwrap();
    assert_eq!((ctx.current(), ctx.peak()), (101, 101));
}

#[test]
fn four_level_nesting_propagates_to_every_ancestor() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Oltp, 99);
    let op = mgr.create_operator_context(&query, OperatorKind::Sort);
    let workload = mgr.workload_context(WorkloadClass::Oltp);
    let global = mgr.global_context();
    {
        let _guard = op.allocate(300).expect("within budget");
        assert_eq!(op.current(), 300);
        assert_eq!(query.current(), 300);
        assert_eq!(workload.current(), 300);
        assert_eq!(global.current(), 300);
        assert_eq!(global.depth(), 0);
        assert!(op.depth() > query.depth());
    }
    // Guard drop releases through the whole chain.
    assert_eq!(global.current(), 0);
    assert_eq!(global.peak(), 300);
}

#[test]
fn operator_budget_exceeded_returns_structured_error() {
    let mgr = MemoryManager::new(flat_config(4096));
    let query = mgr.create_query_context(WorkloadClass::Oltp, 1);
    let op = mgr.create_operator_context_with_budget(&query, OperatorKind::Sort, 256);
    let err = op.try_allocate(512).unwrap_err();
    match &err {
        MemoryError::BudgetExceeded {
            scope,
            requested,
            available,
        } => {
            assert!(scope.contains("operator"), "scope was {scope}");
            assert!(scope.contains("Sort"), "scope was {scope}");
            assert_eq!(*requested, 512);
            assert_eq!(*available, 256);
        }
    }
    // Failed reservation leaves every counter untouched.
    assert_eq!(op.current(), 0);
    assert_eq!(query.current(), 0);
    assert_eq!(mgr.global_context().current(), 0);
}

#[test]
fn global_budget_exceeded_reports_zero_available_when_full() {
    let mgr = MemoryManager::new(flat_config(1000));
    let a = mgr.create_connection_context(1);
    a.try_allocate(1000).unwrap();
    let b = mgr.create_connection_context(2);
    let err = b.try_allocate(1).unwrap_err();
    match err {
        MemoryError::BudgetExceeded {
            scope,
            requested,
            available,
        } => {
            assert_eq!(scope, "global");
            assert_eq!((requested, available), (1, 0));
        }
    }
    assert_eq!(mgr.global_context().current(), 1000);
}

#[test]
fn over_release_saturates_at_zero_without_panic() {
    let mgr = MemoryManager::new(flat_config(1024));
    let ctx = mgr.create_connection_context(5);
    ctx.try_allocate(100).unwrap();
    ctx.release(10_000);
    assert_eq!(ctx.current(), 0);
    assert_eq!(mgr.global_context().current(), 0);
}

#[test]
fn reallocate_grow_and_shrink() {
    let mgr = MemoryManager::new(flat_config(1024));
    let ctx = mgr.create_connection_context(6);
    ctx.try_allocate(100).unwrap();
    ctx.reallocate(100, 400).unwrap();
    assert_eq!((ctx.current(), ctx.peak()), (400, 400));
    ctx.reallocate(400, 50).unwrap();
    assert_eq!((ctx.current(), ctx.peak()), (50, 400));
    // Growing past the budget fails and keeps the old reservation.
    let err = ctx.reallocate(50, 2000).unwrap_err();
    assert!(matches!(err, MemoryError::BudgetExceeded { .. }));
    assert_eq!(ctx.current(), 50);
}

#[test]
fn concurrent_allocate_never_exceeds_global_limit() {
    const GLOBAL: u64 = 4096;
    const UNIT: u64 = 64;
    const THREADS: usize = 8;
    const ITERS: usize = 200;
    let mgr = MemoryManager::new(flat_config(GLOBAL));
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::with_capacity(THREADS);
    for t in 0..THREADS {
        let mgr = Arc::clone(&mgr);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let ctx = mgr.create_connection_context(t as u64);
            barrier.wait();
            let mut ok = 0u64;
            for _ in 0..ITERS {
                if ctx.try_allocate(UNIT).is_ok() {
                    ok += 1;
                }
            }
            ok
        }));
    }
    let mut total_ok = 0u64;
    for h in handles {
        total_ok += h.join().expect("worker thread panicked");
    }
    let global = mgr.global_context();
    // CAS reservation guarantees the hard cap under contention.
    assert!(global.current() <= GLOBAL, "current={}", global.current());
    assert!(global.peak() <= GLOBAL, "peak={}", global.peak());
    assert_eq!(global.current(), total_ok * UNIT);
    assert_eq!(global.alloc_count(), total_ok);
    assert_eq!(global.current() % UNIT, 0);
}

#[test]
fn pressure_state_transitions_with_listener() {
    let mgr = MemoryManager::new(flat_config(1000));
    let seen: Arc<Mutex<Vec<PressureState>>> = Arc::new(Mutex::new(Vec::new()));
    mgr.on_pressure({
        let seen = Arc::clone(&seen);
        move |state| seen.lock().unwrap().push(state)
    });

    let ctx = mgr.create_connection_context(1);
    assert_eq!(mgr.pressure(), PressureState::Normal);

    ctx.try_allocate(699).unwrap();
    assert_eq!(mgr.pressure(), PressureState::Normal);
    assert!(seen.lock().unwrap().is_empty());

    ctx.try_allocate(1).unwrap(); // 0.70 -> Warning
    assert_eq!(mgr.pressure(), PressureState::Warning);

    ctx.try_allocate(150).unwrap(); // 0.85 -> Pressure
    assert_eq!(mgr.pressure(), PressureState::Pressure);

    ctx.try_allocate(100).unwrap(); // 0.95 -> Critical
    assert_eq!(mgr.pressure(), PressureState::Critical);

    ctx.release(950);
    assert_eq!(mgr.pressure(), PressureState::Normal);

    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            PressureState::Warning,
            PressureState::Pressure,
            PressureState::Critical,
            PressureState::Normal,
        ]
    );
    // Explicit poll reports the current state without spurious events.
    assert_eq!(mgr.check_pressure(), PressureState::Normal);
    assert_eq!(mgr.notified_state(), PressureState::Normal);
    assert_eq!(seen.lock().unwrap().len(), 4);
}

#[test]
fn query_stats_breakdown_spill_workers_result_buffer() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Olap, 42);
    let sort = mgr.create_operator_context(&query, OperatorKind::Sort);
    sort.try_allocate(500).unwrap();
    sort.record_spill(5000);
    let rb = mgr.create_operator_context(&query, OperatorKind::ResultBuffer);
    rb.try_allocate(120).unwrap();
    let _w0 = mgr.create_worker_context(&query, 0);
    let _w1 = mgr.create_worker_context(&query, 1);

    let stats = mgr.query_stats(&query);
    assert_eq!(stats.query_id, 42);
    assert_eq!(stats.workload_class, WorkloadClass::Olap);
    assert_eq!(stats.current, 620);
    assert_eq!(stats.operators.len(), 2);
    assert_eq!(stats.spill_bytes, 5000);
    assert_eq!(stats.worker_count, 2);
    assert_eq!(stats.result_buffer, 120);

    let global = mgr.global_stats();
    assert_eq!(global.current, 620);
    assert_eq!(global.pressure, PressureState::Normal);
}

#[test]
fn every_operator_kind_is_accountable() {
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::System, 100);
    for (i, kind) in ALL_KINDS.iter().enumerate() {
        let op = mgr.create_operator_context(&query, *kind);
        op.try_allocate(8)
            .unwrap_or_else(|_| panic!("kind {kind} should account"));
        assert_eq!(op.current(), 8, "kind {kind}");
        op.release(8);
        let _ = i;
    }
    assert_eq!(query.current(), 0);
    assert!(query.peak() > 0);
}

#[test]
fn vector_search_and_blob_buffer_are_plain_byte_buckets() {
    // No-op accounting: no new types involved, just byte counts.
    let mgr = MemoryManager::new(flat_config(1 << 20));
    let query = mgr.create_query_context(WorkloadClass::Olap, 77);
    let vec_op = mgr.create_operator_context(&query, OperatorKind::VectorSearch);
    vec_op.try_allocate(1024).unwrap();
    let blob = mgr.create_operator_context(&query, OperatorKind::BlobBuffer);
    blob.try_allocate(2048).unwrap();
    // bytea/text chunks are plain u64 byte counts end to end.
    let chunks: Vec<u64> = vec![512, 1024, 512];
    let total: u64 = chunks.iter().sum();
    blob.try_allocate(total).unwrap();
    assert_eq!(query.current(), 1024 + 2048 + total);
}
