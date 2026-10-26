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
// Moved from `src/context.rs` inline `#[cfg(test)]` — public-API only.
use plomid_memory::context::*;
use plomid_memory::operator::OperatorKind;
use std::sync::Arc;

fn chain(
    global_budget: u64,
    query_budget: u64,
    op_budget: u64,
) -> (Arc<MemoryContext>, Arc<MemoryContext>, Arc<MemoryContext>) {
    let global = MemoryContext::new_root(ContextKind::Global, global_budget);
    let query = global.create_child(
        ContextKind::Query {
            query_id: 1,
            workload: WorkloadClass::Oltp,
        },
        query_budget,
    );
    let op = query.create_child(
        ContextKind::Operator {
            query_id: 1,
            kind: OperatorKind::Sort,
        },
        op_budget,
    );
    (global, query, op)
}

#[test]
fn reservation_propagates_to_all_parents() {
    let (global, query, op) = chain(10_000, 5_000, 2_000);
    op.try_allocate(512).unwrap();
    assert_eq!(op.current(), 512);
    assert_eq!(query.current(), 512);
    assert_eq!(global.current(), 512);
    assert_eq!(
        (op.alloc_count(), query.alloc_count(), global.alloc_count()),
        (1, 1, 1)
    );
}

#[test]
fn mid_chain_failure_rolls_back_leaf() {
    // Operator budget is generous; the query budget enforces.
    let (global, query, op) = chain(10_000, 100, 10_000);
    let err = op.try_allocate(200).unwrap_err();
    assert!(
        err.scope().contains("query:1"),
        "unexpected scope: {}",
        err.scope()
    );
    assert_eq!(op.current(), 0);
    assert_eq!(query.current(), 0);
    assert_eq!(global.current(), 0);
    assert_eq!(op.alloc_count(), 0);
}

#[test]
fn zero_alloc_is_noop_success() {
    let global = MemoryContext::new_root(ContextKind::Global, 0);
    global.try_allocate(0).unwrap();
    assert_eq!((global.current(), global.alloc_count()), (0, 0));
}

#[test]
fn release_saturates_without_panic() {
    let global = MemoryContext::new_root(ContextKind::Global, 100);
    global.release(999);
    assert_eq!(global.current(), 0);
}

#[test]
fn reallocate_grow_can_fail_leaving_old_intact() {
    let (global, _, op) = chain(150, 150, 100);
    op.try_allocate(60).unwrap();
    let err = op.reallocate(60, 120).unwrap_err();
    assert_eq!(op.current(), 60);
    assert_eq!(global.current(), 60);
    let _ = err;
    op.reallocate(60, 40).unwrap();
    assert_eq!(op.current(), 40);
}

#[test]
fn allocation_guard_releases_on_drop() {
    let (global, _, op) = chain(10_000, 10_000, 10_000);
    {
        let _guard = op.allocate(300).unwrap();
        assert_eq!(global.current(), 300);
    }
    assert_eq!(global.current(), 0);
    assert_eq!(global.peak(), 300);
}

#[test]
fn detach_keeps_reservation_charged() {
    let (global, _, op) = chain(10_000, 10_000, 10_000);
    let held = op.allocate(300).unwrap().detach();
    assert_eq!(held, 300);
    assert_eq!(global.current(), 300);
    op.release(held);
    assert_eq!(global.current(), 0);
}

#[test]
fn spill_propagates_but_ignores_budget() {
    let (global, query, op) = chain(100, 100, 100);
    op.record_spill(10_000);
    assert_eq!(op.spill_bytes(), 10_000);
    assert_eq!(query.spill_bytes(), 10_000);
    assert_eq!(global.spill_bytes(), 10_000);
    assert_eq!(global.current(), 0);
}

#[test]
fn scope_names_cover_every_kind() {
    let wl = ContextKind::WorkloadClass {
        class: WorkloadClass::Olap,
    }
    .scope_name();
    assert_eq!(wl, "workload:olap");
    let q = ContextKind::Query {
        query_id: 9,
        workload: WorkloadClass::System,
    }
    .scope_name();
    assert!(q.starts_with("query:9"));
    let c = ContextKind::Connection { connection_id: 3 }.scope_name();
    assert_eq!(c, "connection:3");
    let w = ContextKind::Worker { worker_id: 2 }.scope_name();
    assert_eq!(w, "worker:2");
    let m = ContextKind::Maintenance {
        operation: MaintenanceOp::Vacuum,
    }
    .scope_name();
    assert_eq!(m, "maintenance:vacuum");
    let k = ContextKind::Cache {
        kind: CacheKind::BufferPool,
    }
    .scope_name();
    assert_eq!(k, "cache:buffer_pool");
    let o = ContextKind::Operator {
        query_id: 9,
        kind: OperatorKind::HashJoin,
    }
    .scope_name();
    assert!(o.contains("HashJoin"));
}
