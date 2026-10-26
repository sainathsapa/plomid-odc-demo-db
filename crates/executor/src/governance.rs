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

//! HTAP governance bridge.
//!
//! This module does NOT reimplement budgeting, timeouts, or cancellation.
//! It pipes the existing executor primitives:
//!
//! * [`crate::guard::check_guard`] — statement timeout + user cancel.
//! * [`crate::parallel_budget::claim_dispatch_workers`] — analytical
//!   worker semaphore (adaptive parallelism under pressure).
//! * `plomid-memory` [`MemoryManager`] — hierarchical accounting only.
//!
//! Pressure-aware dispatch = ask the memory manager for its pressure state,
//! derate the requested worker count, then claim through the existing budget.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use plomid_core::{ErrorKind, PlomidError};
use plomid_htap::{Admission, HtapConfig, ResourceGovernor, WorkloadClass as HtapClass};
use plomid_memory::{MemoryConfig, MemoryManager, WorkloadClass as MemoryClass};
use plomid_sql::QueryResult;

use crate::error::{SqlError, SqlResult};
use crate::parallel_budget::{claim_dispatch_workers, ClaimedDispatch};

/// Poll the existing guard (timeout + cancel). Thin alias so operators call
/// one governance entry point.
pub fn poll_guard() -> SqlResult<()> {
    crate::guard::check_guard()
}

/// Claim analytical workers through the existing budget, derating `want`
/// under memory pressure instead of implementing a second semaphore.
///
/// `pressure_level`: 0 = normal, 1 = warning, 2 = pressure, 3 = critical.
/// Derate: 8 -> 4 -> 2 -> 1 (OLTP paths pass 0 and are unaffected).
/// Returns `None` to run sequential — the same contract as
/// `claim_dispatch_workers`.
pub fn claim_workers_pressure_aware(want: usize, pressure_level: u8) -> Option<ClaimedDispatch> {
    let derated = match pressure_level {
        0 => want,
        1 => (want / 2).max(1),
        2 => (want / 4).max(1),
        _ => 1,
    };
    claim_dispatch_workers(derated)
}

/// Map a `plomid-memory` pressure state to the 0-3 derate level above.
/// Takes the state as a `u8` discriminant to avoid a hard dependency cycle
/// in docs; callers pass `MemoryManager::pressure() as u8`.
pub fn pressure_level_from_order(order: u8) -> u8 {
    order.min(3)
}

// ---- HTAP enforcement: process-global singletons ----

/// Process-global memory manager (default config).
///
/// Cloning the `Arc` is atomics-only, so the hot path stays negligible when
/// idle. No `Executor` struct changes: the singleton is reachable from the
/// single `execute_all` choke point without touching constructors.
pub fn global_memory_manager() -> Arc<MemoryManager> {
    static MANAGER: OnceLock<Arc<MemoryManager>> = OnceLock::new();
    Arc::clone(MANAGER.get_or_init(|| MemoryManager::new(MemoryConfig::default())))
}

/// Process-global HTAP resource governor (default config).
///
/// `HtapConfig::default` is a compile-time-known-valid budget, but a
/// construction failure degrades to admit-all (`None`) rather than panicking.
fn global_governor() -> Option<&'static ResourceGovernor> {
    static GOVERNOR: OnceLock<Option<ResourceGovernor>> = OnceLock::new();
    GOVERNOR
        .get_or_init(|| ResourceGovernor::new(HtapConfig::default()).ok())
        .as_ref()
}

/// Map SQL text to an HTAP workload class using the existing
/// [`plomid_core::observability::statement_class`] keyword.
///
/// Writes, DDL, and transaction/session statements take the OLTP path;
/// analytical reads (`SELECT`/`WITH`/`VALUES`/`EXPLAIN`/`ANALYZE`) go through
/// [`plomid_htap::classify`] with an OLAP hint (no row estimate, so the hint
/// wins and they land in `Olap`).
pub fn workload_class_for_sql(sql: &str) -> HtapClass {
    match plomid_core::observability::statement_class(sql) {
        "SELECT" | "WITH" | "VALUES" | "EXPLAIN" | "ANALYZE" => {
            plomid_htap::classify(Some(HtapClass::Olap), false, None)
        }
        _ => plomid_htap::classify(None, true, None),
    }
}

/// Map an HTAP workload class onto the coarser `plomid-memory` branches
/// (`Oltp` / `Olap` / `System`).
fn memory_class_for(class: HtapClass) -> MemoryClass {
    match class {
        HtapClass::Oltp => MemoryClass::Oltp,
        HtapClass::System => MemoryClass::System,
        _ => MemoryClass::Olap,
    }
}

/// Live pressure level (0-3) read from the process-global memory manager.
/// Pure atomic loads; negligible cost when idle.
pub fn current_pressure_level() -> u8 {
    pressure_level_from_order(global_memory_manager().pressure() as u8)
}

// ---- HTAP enforcement: admission ----

/// Bounded wait for a `Queue`d admission before giving up.
pub const ADMISSION_QUEUE_WAIT: Duration = Duration::from_secs(5);
/// Poll interval while queued: each tick re-checks the statement guard so
/// cancel/statement-timeout still aborts the wait.
const ADMISSION_QUEUE_POLL: Duration = Duration::from_millis(10);

/// A held governor admission: one concurrency slot for the executing batch.
/// Dropping releases the slot back to the process-global governor. Releases
/// saturate at zero, so a drop can never panic or drive counters negative.
#[derive(Debug)]
pub struct AdmissionGuard {
    class: HtapClass,
    bytes: u64,
    workers: usize,
    held: bool,
}

impl AdmissionGuard {
    fn admitted(class: HtapClass, bytes: u64, workers: usize) -> Self {
        Self {
            class,
            bytes,
            workers,
            held: true,
        }
    }

    /// Admission bypassed (governor unavailable): nothing to release.
    fn bypassed(class: HtapClass) -> Self {
        Self {
            class,
            bytes: 0,
            workers: 0,
            held: false,
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        if self.held {
            if let Some(governor) = global_governor() {
                governor.release(self.class, self.bytes, self.workers);
            }
        }
    }
}

/// Admit one batch through the process-global governor.
///
/// `Admit` returns a guard holding the slot until the end of `execute_all`;
/// `Queue` waits up to [`ADMISSION_QUEUE_WAIT`] (polling the statement guard
/// so cancel/timeout still aborts); `Reject` becomes a structured
/// resource-exhausted error. Never panics.
///
/// Live rescale: refreshes the elastic worker budget first (interval-gated
/// to one sysfs/proc re-read per 60 s, otherwise a single `Instant` check),
/// so a container resize or VPA change applies at the next batch boundary.
/// In-flight batches keep their permits; only new admissions see the new
/// cap (see `parallel_budget::apply_redetected_profile`).
pub fn admit_query(sql: &str) -> SqlResult<AdmissionGuard> {
    crate::parallel_budget::maybe_recheck_if_due();
    match global_governor() {
        Some(governor) => admit_with(governor, sql, ADMISSION_QUEUE_WAIT),
        None => Ok(AdmissionGuard::bypassed(workload_class_for_sql(sql))),
    }
}

/// Admission against an explicit governor (production passes the global one;
/// tests inject tiny configs). Same `Admit`/`Queue`/`Reject` contract as
/// [`admit_query`], with a caller-chosen queue bound.
pub fn admit_with(
    governor: &ResourceGovernor,
    sql: &str,
    queue_wait: Duration,
) -> SqlResult<AdmissionGuard> {
    let class = workload_class_for_sql(sql);
    // Nominal byte token: governor admission here gates concurrency slots;
    // per-query memory is enforced after execution by `charge_result_buffers`
    // against the memory manager. Zero bytes keeps OLTP admission total:
    // privileged classes then only fail when the global budget is already
    // over budget, which saturating accounting makes unreachable.
    const ADMISSION_BYTES: u64 = 0;
    const ADMISSION_WORKERS: usize = 1;
    let deadline = Instant::now() + queue_wait;
    loop {
        match governor.admit(class, ADMISSION_BYTES, ADMISSION_WORKERS) {
            Admission::Admit { .. } => {
                return Ok(AdmissionGuard::admitted(
                    class,
                    ADMISSION_BYTES,
                    ADMISSION_WORKERS,
                ));
            }
            Admission::Reject { reason } => return Err(admission_reject_error(&reason)),
            Admission::Queue { reason } => {
                if Instant::now() >= deadline {
                    return Err(admission_queue_timeout_error(&reason));
                }
                // Cancel/statement-timeout aborts the wait; otherwise park.
                poll_guard()?;
                let remaining = deadline.saturating_duration_since(Instant::now());
                std::thread::sleep(remaining.min(ADMISSION_QUEUE_POLL));
            }
        }
    }
}

/// Structured resource-exhausted error for a governor `Reject`. Never panics.
pub fn admission_reject_error(reason: &str) -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::Conflict,
        format!("resource exhausted: HTAP admission rejected query ({reason})"),
    ))
}

fn admission_queue_timeout_error(reason: &str) -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::Aborted,
        format!("HTAP admission queue timeout: no resources became available ({reason})"),
    ))
}

// ---- HTAP enforcement: result-buffer charging + retained stats ----

/// One executed batch's memory footprint, retained in a bounded ring for the
/// observability worker (see [`recent_query_stats`]).
#[derive(Debug, Clone)]
pub struct QueryStat {
    /// [`plomid_core::observability::statement_class`] keyword (`"SELECT"`, …).
    pub class: &'static str,
    /// Live bytes on the per-execute context at charge time.
    pub current_bytes: u64,
    /// High-water mark while the results were held.
    pub peak_bytes: u64,
    /// Spilled bytes; 0 until spill accounting pipes through here.
    pub spill_bytes: u64,
    /// Honest encoded result size (sum of [`crate::encoding::encode_row`]).
    pub result_bytes: u64,
}

/// Retained ring capacity: the last 128 executed batches.
const RECENT_STATS_CAP: usize = 128;

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

fn recent_stats_cell() -> &'static Mutex<VecDeque<QueryStat>> {
    static RECENT: OnceLock<Mutex<VecDeque<QueryStat>>> = OnceLock::new();
    RECENT.get_or_init(|| Mutex::new(VecDeque::with_capacity(RECENT_STATS_CAP)))
}

/// Snapshot of the retained per-query stats ring (oldest first, at most the
/// last 128 executed batches). Lock failures degrade to an empty snapshot;
/// never panics.
pub fn recent_query_stats() -> Vec<QueryStat> {
    recent_stats_cell()
        .lock()
        .map(|guard| guard.iter().cloned().collect())
        .unwrap_or_default()
}

fn record_query_stat(stat: QueryStat) {
    if let Ok(mut guard) = recent_stats_cell().lock() {
        guard.push_back(stat);
        while guard.len() > RECENT_STATS_CAP {
            guard.pop_front();
        }
    }
}

/// Charge an executed batch's result bytes to a per-execute memory context.
///
/// Lifecycle (documented honestly): a fresh query context is created under
/// the batch's workload branch, the honest encoded result size is reserved
/// on it (and on every ancestor up to global, so pressure reflects live
/// results), the `(current, peak)` snapshot is copied into the retained
/// 128-entry ring, and the reservation is then released and the context
/// dropped. Only the ring snapshot survives the call — `peak` is the
/// high-water mark observed while the results were held, `spill_bytes` is 0
/// until spill accounting pipes through here.
///
/// A charge failure returns a structured resource-exhausted error instead of
/// the results. Row encode failures contribute 0 bytes for that row.
pub fn charge_result_buffers(sql: &str, results: &[QueryResult]) -> SqlResult<()> {
    let mut result_bytes: u64 = 0;
    for outcome in results {
        if let QueryResult::Rows { rows, .. } = outcome {
            for row in rows {
                let encoded = crate::encoding::encode_row(row)
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(0);
                result_bytes = result_bytes.saturating_add(encoded);
            }
        }
    }
    let manager = global_memory_manager();
    let query_id = NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed);
    let context =
        manager.create_query_context(memory_class_for(workload_class_for_sql(sql)), query_id);
    if let Err(reason) = context.try_allocate(result_bytes) {
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Conflict,
            format!(
                "resource exhausted: result buffer charge of {result_bytes}B failed ({reason})"
            ),
        )));
    }
    record_query_stat(QueryStat {
        class: plomid_core::observability::statement_class(sql),
        current_bytes: context.current(),
        peak_bytes: context.peak(),
        spill_bytes: 0,
        result_bytes,
    });
    context.release(result_bytes);
    Ok(())
}
