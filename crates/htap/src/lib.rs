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

//! HTAP workload governance for PLOMID.
//!
//! # Ownership
//!
//! Owns workload classification and resource budgets (what a query may
//! consume). Never owns execution, scheduling, or memory itself —
//! executors enforce, memory accounts, this crate decides the allowance.
//!
//! This crate separates *what a query is* ([`WorkloadClass`]) from *what
//! resources it may consume* ([`ResourceGovernor`]).
//!
//! Design rules (workload governance):
//! - Classification is best-effort and never required: [`classify`] infers a
//!   safe default (write/small => [`WorkloadClass::Oltp`], else the caller
//!   hint or [`WorkloadClass::Olap`]).
//! - OLTP is always preferred. An [`HtapConfig::oltp_reservation_bytes`]
//!   slice of the global budget is fenced off: OLAP / batch / background /
//!   maintenance traffic cannot consume it while OLTP pressure exists.
//! - OLAP traffic is bounded by pool budgets, per-query byte limits, and
//!   pressure-derated concurrency caps, so one query cannot starve others.
//! - OLAP parallelism degrades 8 -> 4 -> 2 -> 1 as memory pressure rises;
//!   OLTP parallelism is never derated.
//! - Maintenance is throttled ([`MaintenanceThrottle`]) and paused at
//!   critical pressure; it never starves OLTP.
//! - Statement timeouts ([`StatementPolicy`]) bound wall-clock execution and
//!   are orthogonal to row limits (`LIMIT n`).
//! - [`HtapSnapshot`] answers "why is OLTP slow / who consumes memory".
//!
//! Only `std` is used; all shared counters are atomics.

pub mod buffer_manager;
pub mod compaction_guard;
pub mod config;
pub mod governor;
pub mod maintenance;
pub mod parallelism;
pub mod pool;
pub mod snapshot;
pub mod tenant;
pub mod timeout;
pub mod workload;

pub use buffer_manager::{
    global_buffer_manager, AdmitDecision, BufferStats, CacheClass, CacheEntry, CachePolicy,
    Temperature, UnifiedBufferManager, GLOBAL_BUFFER_CAPACITY_BYTES,
    GLOBAL_BUFFER_OLTP_RESERVE_BYTES,
};
pub use compaction_guard::{
    CancelToken, CompactionGate, CompactionThrottle, Deadline, GateDecision,
};
pub use config::{ConfigError, HtapConfig, PressureState};
pub use governor::{Admission, ResourceGovernor};
pub use maintenance::{throttle, MaintenanceThrottle};
pub use parallelism::{derate_concurrency, parallelism_for};
pub use pool::{MaintenancePool, OlapPool, OltpPool, PoolKind, ResourcePool, SystemReservePool};
pub use snapshot::{HtapSnapshot, QueryStat};
pub use tenant::{TenantId, TenantQuota, TenantRegistry};
pub use timeout::{StatementPolicy, TimeoutError};
pub use workload::{classify, Priority, WorkloadClass, SMALL_TXN_ROW_THRESHOLD};
