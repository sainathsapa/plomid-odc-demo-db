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

//! Point-in-time stats snapshots. All structs are `Clone + Debug`
//! value types; take them via [`crate::MemoryManager`] accessors or the
//! `snapshot` constructors here.

use crate::context::{ContextKind, MemoryContext, WorkloadClass};
use crate::operator::OperatorKind;
use crate::pressure::PressureState;

/// Engine-wide rollup.
#[derive(Clone, Debug)]
pub struct GlobalStats {
    /// Global budget in bytes.
    pub budget: u64,
    /// Currently held bytes across all contexts.
    pub current: u64,
    /// High-water mark of `current`.
    pub peak: u64,
    /// Total successful reservations at the root.
    pub allocations: u64,
    /// Pressure derived from `current / budget`.
    pub pressure: PressureState,
}

impl GlobalStats {
    /// Snapshot the global root with a precomputed pressure state.
    pub fn snapshot(global: &MemoryContext, pressure: PressureState) -> Self {
        Self {
            budget: global.budget(),
            current: global.current(),
            peak: global.peak(),
            allocations: global.alloc_count(),
            pressure,
        }
    }
}

/// Per-operator accounting row inside [`QueryStats`].
#[derive(Clone, Debug)]
pub struct OperatorStat {
    /// Operator accounting bucket.
    pub kind: OperatorKind,
    /// Owning query id.
    pub query_id: u64,
    /// Currently held bytes.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
    /// Spilled bytes recorded on this operator.
    pub spill_bytes: u64,
}

impl OperatorStat {
    /// Snapshot an operator context; `None` when `ctx` is not an operator.
    pub fn snapshot(ctx: &MemoryContext) -> Option<Self> {
        match ctx.kind() {
            ContextKind::Operator { query_id, kind } => Some(Self {
                kind,
                query_id,
                current: ctx.current(),
                peak: ctx.peak(),
                allocations: ctx.alloc_count(),
                spill_bytes: ctx.spill_bytes(),
            }),
            _ => None,
        }
    }
}

/// Per-query rollup including the operator breakdown.
#[derive(Clone, Debug)]
pub struct QueryStats {
    /// Engine-assigned query id.
    pub query_id: u64,
    /// Classified workload.
    pub workload_class: WorkloadClass,
    /// Currently held bytes in this query subtree.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
    /// One row per live child operator context.
    pub operators: Vec<OperatorStat>,
    /// Spilled bytes in this query subtree.
    pub spill_bytes: u64,
    /// Number of live child worker contexts.
    pub worker_count: usize,
    /// Bytes held by `ResultBuffer` operator children.
    pub result_buffer: u64,
}

impl QueryStats {
    /// Snapshot a query context, folding in live operator children,
    /// worker count, result-buffer bytes, and spill totals.
    pub fn snapshot(query: &MemoryContext) -> Self {
        let (query_id, workload_class) = match query.kind() {
            ContextKind::Query { query_id, workload } => (query_id, workload),
            _ => (0, WorkloadClass::System),
        };
        let mut operators = Vec::new();
        let mut worker_count = 0usize;
        let mut result_buffer = 0u64;
        for child in query.children() {
            match child.kind() {
                ContextKind::Operator { kind, .. } => {
                    if let Some(stat) = OperatorStat::snapshot(&child) {
                        if kind == OperatorKind::ResultBuffer {
                            result_buffer += stat.current;
                        }
                        operators.push(stat);
                    }
                }
                ContextKind::Worker { .. } => worker_count += 1,
                _ => {}
            }
        }
        Self {
            query_id,
            workload_class,
            current: query.current(),
            peak: query.peak(),
            allocations: query.alloc_count(),
            operators,
            spill_bytes: query.spill_bytes(),
            worker_count,
            result_buffer,
        }
    }
}

/// Per-connection rollup.
#[derive(Clone, Debug)]
pub struct ConnectionStats {
    /// Connection id.
    pub connection_id: u64,
    /// Connection budget in bytes.
    pub budget: u64,
    /// Currently held bytes in this connection subtree.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
}

impl ConnectionStats {
    /// Snapshot a connection context (`connection_id` is 0 when `ctx`
    /// is not a connection).
    pub fn snapshot(ctx: &MemoryContext) -> Self {
        let connection_id = match ctx.kind() {
            ContextKind::Connection { connection_id } => connection_id,
            _ => 0,
        };
        Self {
            connection_id,
            budget: ctx.budget(),
            current: ctx.current(),
            peak: ctx.peak(),
            allocations: ctx.alloc_count(),
        }
    }
}

/// Per-worker rollup.
#[derive(Clone, Debug)]
pub struct WorkerStats {
    /// Worker id (unique within its parent).
    pub worker_id: u64,
    /// Currently held bytes.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
}

impl WorkerStats {
    /// Snapshot a worker context (`worker_id` is 0 when `ctx` is not
    /// a worker).
    pub fn snapshot(ctx: &MemoryContext) -> Self {
        let worker_id = match ctx.kind() {
            ContextKind::Worker { worker_id } => worker_id,
            _ => 0,
        };
        Self {
            worker_id,
            current: ctx.current(),
            peak: ctx.peak(),
            allocations: ctx.alloc_count(),
        }
    }
}

/// Maintenance-branch rollup.
#[derive(Clone, Debug)]
pub struct MaintenanceStats {
    /// Maintenance branch budget in bytes.
    pub budget: u64,
    /// Currently held bytes in the maintenance subtree.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
}

impl MaintenanceStats {
    /// Snapshot the maintenance branch root.
    pub fn snapshot(ctx: &MemoryContext) -> Self {
        Self {
            budget: ctx.budget(),
            current: ctx.current(),
            peak: ctx.peak(),
            allocations: ctx.alloc_count(),
        }
    }
}

/// Cache-branch rollup.
#[derive(Clone, Debug)]
pub struct CacheStats {
    /// Cache branch budget in bytes.
    pub budget: u64,
    /// Currently held bytes in the cache subtree.
    pub current: u64,
    /// High-water mark.
    pub peak: u64,
    /// Total successful reservations.
    pub allocations: u64,
    /// Number of live cache-partition children.
    pub partitions: usize,
}

impl CacheStats {
    /// Snapshot the cache branch root.
    pub fn snapshot(ctx: &MemoryContext) -> Self {
        Self {
            budget: ctx.budget(),
            current: ctx.current(),
            peak: ctx.peak(),
            allocations: ctx.alloc_count(),
            partitions: ctx.children().len(),
        }
    }
}
