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

#![forbid(unsafe_code)]
//! Hierarchical memory accounting and budget enforcement for PLOMID.
//!
//! # Ownership
//!
//! Owns budget ledgers and admission accounting only. Never owns the
//! operators that spend memory, the spill files they write (spill), or
//! scheduling decisions (executor/HTAP governance reads these ledgers).
//!
//! The hierarchy is:
//!
//! ```text
//! Global
//! ├── WorkloadClass (OLTP / OLAP / System)
//! │   └── Query
//! │       └── Operator
//! │           └── Allocation (RAII guard, not a context)
//! ├── Connection
//! │   └── Worker
//! ├── Maintenance
//! │   └── task (Vacuum / Checkpoint / Compaction / …)
//! └── Cache
//!     └── partition (BufferPool / PageCache / ResultCache / PlanCache)
//! ```
//!
//! Every byte charged to a leaf context is also charged to each of its
//! ancestors, so [`MemoryContext`] counters always reflect subtree usage.
//! Reservations are enforced with atomic compare-and-swap loops only — the
//! allocation fast path takes no locks. The child set is touched solely
//! during hierarchy creation/teardown and stats snapshots.
//!
//! Budget violations are reported as [`MemoryError::BudgetExceeded`]; this
//! crate never panics on over-budget allocations.
//!
//! # Example
//!
//! ```rust
//! use plomid_memory::{MemoryConfig, MemoryManager, OperatorKind};
//! use plomid_memory::context::WorkloadClass;
//!
//! let mgr = MemoryManager::new(MemoryConfig::default());
//! let query = mgr.create_query_context(WorkloadClass::Oltp, 42);
//! let scan = mgr.create_operator_context(&query, OperatorKind::TableScan);
//! scan.try_allocate(1024).expect("within budget");
//! assert_eq!(mgr.global_context().current(), 1024);
//! scan.release(1024);
//! ```
//!
//! # Public-surface keep-list (wiring audit)
//!
//! Every item below has a live production caller or is an obvious
//! constructor/accessor of a live-called type. Anything else was deleted
//! (see git history) rather than left as unwired `pub` surface:
//!
//! * **Live production paths** — called from `plomid-executor`
//!   (`governance.rs`, `system_catalog.rs`):
//!   [`MemoryManager::new`], [`MemoryConfig::default`] (plus every
//!   [`MemoryConfig`] field, all read inside the manager factories or
//!   downstream), [`MemoryManager::pressure`],
//!   [`MemoryManager::global_context`],
//!   [`MemoryManager::workload_context`],
//!   [`MemoryManager::create_query_context`],
//!   [`MemoryManager::config`], [`MemoryContext::try_allocate`],
//!   [`MemoryContext::release`], [`MemoryContext::current`],
//!   [`MemoryContext::peak`], [`WorkloadClass::{Oltp, Olap, System}`]
//!   (`System` via the governance workload-class map),
//!   [`PressureState::from_usage`] (implements live `pressure()`).
//! * **Test-seam factories/accessors** — no production caller yet, retained
//!   because the crate's own integration suite (`tests/memory_manager.rs`)
//!   exercises them as the pending wiring surface:
//!   [`MemoryManager::create_operator_context`] (+ `_with_budget`),
//!   [`MemoryManager::create_temp_context`],
//!   [`MemoryManager::create_connection_context`],
//!   [`MemoryManager::create_worker_context`],
//!   [`MemoryManager::maintenance_context`],
//!   [`MemoryManager::create_maintenance_task`],
//!   [`MemoryManager::cache_context`], [`MemoryManager::cache_partition`],
//!   [`MemoryManager::notified_state`], [`MemoryManager::check_pressure`],
//!   [`MemoryManager::on_pressure`], [`MemoryManager::global_stats`],
//!   [`MemoryManager::query_stats`],
//!   [`MemoryManager::connection_stats`],
//!   [`MemoryManager::worker_stats`],
//!   [`MemoryManager::maintenance_stats`], [`MemoryManager::cache_stats`],
//!   the `stats::{GlobalStats, QueryStats, OperatorStat, ConnectionStats,
//!   WorkerStats, MaintenanceStats, CacheStats}` snapshot types,
//!   [`OperatorKind`] + [`operator::ALL_KINDS`], [`MemoryContext::depth`],
//!   [`Allocation::detach`].
//! * **Obvious constructors/builders/accessors of live-called types**:
//!   [`MemoryContext::new_root`], [`MemoryContext::create_child`],
//!   [`PressureThresholds::new`], [`MemoryConfig::with_global_budget`],
//!   [`Allocation::bytes`], [`MemoryError::scope`].
//!
//! Narrowed in this pass (deleted or made private; zero callers in
//! production, unit tests, and integration tests): the `budget` newtype
//! module (`GlobalMemoryBudget`, `WorkloadBudget`, `QueryMemoryBudget`,
//! `OperatorMemoryBudget`, `ConnectionBudget`, `WorkerBudget`,
//! `MaintenanceBudget`), `MemoryManager::create_query_context_with_budget`,
//! `MemoryManager::create_connection_context_with_budget`,
//! `MemoryManager::create_worker_context_with_budget` (now private helpers),
//! `MemoryManager::operator_stats`, the `PressureCallback` alias (now
//! private), `MemoryContext::set_budget`, `Allocation::context`.
//! `#[cfg(test)]`-gating was rejected for these: integration tests build
//! against the library without `cfg(test)`, so gating would break the
//! crate's own suite (and the crate defines no features for a
//! `test-utils` gate).

pub mod config;
pub mod context;
pub mod error;
pub mod manager;
pub mod operator;
pub mod pressure;
pub mod process;
pub mod stats;

pub use config::{MemoryConfig, PressureThresholds};
pub use context::{
    Allocation, CacheKind, ContextKind, MaintenanceOp, MemoryContext, WorkloadClass,
};
pub use error::MemoryError;
pub use manager::MemoryManager;
pub use operator::OperatorKind;
pub use pressure::PressureState;
pub use process::{dir_size_bytes, process_snapshot, ProcessSnapshot};
pub use stats::{
    CacheStats, ConnectionStats, GlobalStats, MaintenanceStats, OperatorStat, QueryStats,
    WorkerStats,
};
