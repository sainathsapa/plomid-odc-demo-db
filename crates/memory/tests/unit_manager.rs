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
// Moved from `src/manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_memory::config::MemoryConfig;
use plomid_memory::context::{ContextKind, WorkloadClass};
use plomid_memory::manager::*;
use plomid_memory::operator::OperatorKind;
use std::sync::Arc;

fn small_manager() -> Arc<MemoryManager> {
    MemoryManager::new(MemoryConfig {
        global_budget: 8192,
        oltp_reservation: 8192,
        olap_budget: 8192,
        maintenance_budget: 2048,
        query_budget_default: 4096,
        connection_budget: 2048,
        worker_budget: 1024,
        result_buffer_budget: 1024,
        temp_limit: 2048,
        pressure_thresholds: Default::default(),
    })
}

#[test]
fn workload_contexts_are_cached_per_class() {
    let mgr = small_manager();
    let a = mgr.workload_context(WorkloadClass::Oltp);
    let b = mgr.workload_context(WorkloadClass::Oltp);
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(a.budget(), 8192);
    let olap = mgr.workload_context(WorkloadClass::Olap);
    assert!(!Arc::ptr_eq(&a, &olap));
}

#[test]
fn result_buffer_operators_use_result_buffer_budget() {
    let mgr = small_manager();
    let query = mgr.create_query_context(WorkloadClass::Oltp, 1);
    let rb = mgr.create_operator_context(&query, OperatorKind::ResultBuffer);
    assert_eq!(rb.budget(), 1024);
    let sort = mgr.create_operator_context(&query, OperatorKind::Sort);
    assert_eq!(sort.budget(), query.budget());
}

#[test]
fn temp_context_uses_temp_limit_and_temptable_kind() {
    let mgr = small_manager();
    let query = mgr.create_query_context(WorkloadClass::Olap, 9);
    let temp = mgr.create_temp_context(&query);
    assert_eq!(temp.budget(), 2048);
    assert_eq!(
        temp.kind(),
        ContextKind::Operator {
            query_id: 9,
            kind: OperatorKind::TempTable
        }
    );
}
