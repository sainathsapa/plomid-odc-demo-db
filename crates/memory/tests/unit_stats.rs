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
// Moved from `src/stats.rs` inline `#[cfg(test)]` — public-API only.
use plomid_memory::context::{ContextKind, MemoryContext, WorkloadClass};
use plomid_memory::operator::OperatorKind;
use plomid_memory::pressure::PressureState;
use plomid_memory::stats::*;

#[test]
fn query_snapshot_folds_operators_workers_and_result_buffer() {
    let global = MemoryContext::new_root(ContextKind::Global, 1 << 20);
    let query = global.create_child(
        ContextKind::Query {
            query_id: 5,
            workload: WorkloadClass::Olap,
        },
        1 << 20,
    );
    let sort = query.create_child(
        ContextKind::Operator {
            query_id: 5,
            kind: OperatorKind::Sort,
        },
        1 << 20,
    );
    sort.try_allocate(100).unwrap();
    sort.record_spill(50);
    let rb = query.create_child(
        ContextKind::Operator {
            query_id: 5,
            kind: OperatorKind::ResultBuffer,
        },
        1 << 20,
    );
    rb.try_allocate(25).unwrap();
    let _worker = query.create_child(ContextKind::Worker { worker_id: 1 }, 1 << 20);

    let stats = QueryStats::snapshot(&query);
    assert_eq!(stats.query_id, 5);
    assert_eq!(stats.workload_class, WorkloadClass::Olap);
    assert_eq!(stats.current, 125);
    assert_eq!(stats.peak, 125);
    assert_eq!(stats.operators.len(), 2);
    assert_eq!(stats.spill_bytes, 50);
    assert_eq!(stats.worker_count, 1);
    assert_eq!(stats.result_buffer, 25);
}

#[test]
fn operator_snapshot_rejects_non_operators() {
    let global = MemoryContext::new_root(ContextKind::Global, 1 << 20);
    assert!(OperatorStat::snapshot(&global).is_none());
    let conn = global.create_child(ContextKind::Connection { connection_id: 1 }, 1024);
    let cs = ConnectionStats::snapshot(&conn);
    assert_eq!((cs.connection_id, cs.budget), (1, 1024));
    let ws = WorkerStats::snapshot(&conn);
    assert_eq!(ws.worker_id, 0);
}

#[test]
fn global_snapshot_carries_pressure() {
    let global = MemoryContext::new_root(ContextKind::Global, 100);
    global.try_allocate(90).unwrap();
    let gs = GlobalStats::snapshot(&global, PressureState::Pressure);
    assert_eq!((gs.current, gs.peak, gs.budget), (90, 90, 100));
    assert_eq!(gs.pressure, PressureState::Pressure);
}
