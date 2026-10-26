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

//! Box-wide analytical worker budget (128 conns x Ncpu threads).
//!
//! Every parallel dispatch site fans out with `std::thread::scope`, so one
//! analytical query can hold `available_parallelism` threads and 128
//! concurrent connections can multiply that into thousands of live threads
//! (allocator pressure, context-switch collapse). This module caps the
//! process-wide number of outstanding analytical workers with a static
//! counting semaphore. Each dispatch site claims its worker count
//! non-blockingly; on exhaustion it runs the unchanged sequential path
//! (never blocks, never deadlocks, never fails), so tail behavior is
//! identical to today's sequential code.
//!
//! # Why hand-rolled (`Mutex<usize>`), not `tokio::sync::Semaphore`
//!
//! Checked first, rejected:
//! * `plomid-executor` has no `tokio` dependency, and its analytical paths
//!   are synchronous (`std::thread::scope` inside sync query execution).
//!   `tokio::sync::Semaphore::acquire` is `async` and needs a runtime; the
//!   only sync bridge is `blocking_acquire` / `block_in_place`, which panics
//!   outside a multi-thread runtime and risks pool starvation when parallel
//!   workers themselves need runtime threads.
//! * Only the non-blocking `try_acquire` shape is wanted here anyway (a
//!   site that blocks for permits reintroduces the deadlock this
//!   budget exists to prevent), and `try_acquire_many` buys nothing over an
//!   integer compare-and-subtract.
//!
//! # Why no `Condvar`
//!
//! There are no blockers to wake: acquisition is try-only by design, so no
//! waiter queue exists. The mutex is held for one integer compare-and-sub
//! (or add, on release) and never across a dispatch, so there is no lock
//! ordering and no deadlock. Nested dispatch (a parallel worker dispatching
//! again) simply declines to sequential when permits run out.
//!
//! # Wiring pattern (3 lines, used at every dispatch site)
//!
//! ```ignore
//! if let Some(dispatch) = crate::parallel_budget::claim_dispatch_workers(workers) {
//!     if let Some(rows) = <parallel_fn>(/* ..., */ dispatch.workers())? {
//!         return <parallel result>;
//!     }
//! } // `_lease` drops here on decline: permits return, sequential path runs.
//! // (`?` propagates worker *errors*, e.g. the join output cap, which must
//! // surface rather than re-materialize sequentially; `None` declines to the
//! // sequential path.)
//! ```
//!
//! # Deferred wiring (`TODO(worker-budget)`): none remaining in the executor
//!
//! The two formerly-deferred row-store sites are wired (PERF-C):
//! * `crates/executor/src/query/select.rs` TopK streaming dispatch: the
//!   `par_topk_workers_for` gate (total order + worker-safe keys/filter +
//!   costly keys + 1M-row floor + tiered width over a keys-only
//!   `count_range` estimate) claims permits via `claim_dispatch_workers`
//!   and calls `par_topk_streaming` with the claimed width, declining to
//!   the sequential heap on exhaustion.
//! * `crates/executor/src/query/select.rs` row-store full-sort dispatch:
//!   the `par_sort_workers_for(source_rows.len())` width is claimed via
//!   `claim_dispatch_workers` before `par_sort_rows`, declining to the
//!   sequential spill path on exhaustion. (The row-count gate itself is
//!   budget-aware since EDGE-B: `par_sort_workers_for` clamps to the
//!   elastic cap.)
//!
//! Wired here: `crates/executor/src/join/support.rs` `perform_join`
//! equi-INNER dispatch -> `perform_join_parallel`, and the sequential-path
//! large-build fan-out (`HashJoinTable::parallel_build`, which previously
//! bypassed the budget entirely).
//!
//! Wired in EDGE-B: `crates/executor/src/query/select.rs` streaming
//! aggregation dispatch (`par_scan_agg` / `par_streaming_agg`): the worker
//! derivation is budget-clamped and the dispatch claims permits, declining
//! to the sequential loop on exhaustion.
//!
//! # Tiered budgets (EDGE-B) + elastic worker cap
//!
//! The per-dispatch permits, the join/sort/scan row thresholds, and the
//! sizing defaults scale with the host tier instead of staying pinned at the
//! 16-core reference values. Tiers come from `plomid-hw`
//! [`RuntimeProfile`](plomid_hw::RuntimeProfile) (Agent A's `RuntimeTier` +
//! accessor API — Edge | Laptop | Server); EDGE-B owns the *mapping* from
//! tier to executor knobs. The process-wide worker cap is NOT tiered: it is
//! derived from measured hardware ([`elastic_cap_for_hardware`]), so big
//! machines are fully used and fixed tier caps never under-utilize them:
//!
//! | tier   | cap (elastic¹) | permits* | join  | scan_min | sort_min | build_min | batch | buffer/reserve | pool |
//! |--------|----------------|----------|-------|----------|----------|-----------|-------|----------------|------|
//! | Edge   | hardware       | 2        | 8_000 |  400_000 |  200_000 |   200_000 | ≤4096 | 64MiB / 16MiB  |  256 |
//! | Laptop | hardware       | 8        | 2_000 |  100_000 |   50_000 |    50_000 | 16384 | 256MiB / 64MiB | 1024 |
//! | Server | hardware       | full     | 1_000 |   50_000 |   25_000 |    25_000 | 16384 | 256MiB / 64MiB | 1024 |
//!
//! ¹ `cap = min(512 ceiling, 2 x logical_cpus, (RAM/4) / per-worker-arena)`,
//! floored at 4 (see [`elastic_cap_for_hardware`]): a 4c/1GiB Pi gets 8, a
//! 16c/16GiB laptop gets 32 (exactly the historical value), a 128c/512GiB
//! server gets 256. Unknown RAM falls back to the conservative 32. The 512
//! ceiling is a thread-explosion backstop only, never a sizing target.
//!
//! `*permits` is the profile's `worker_permits()` on the canonical hardware
//! for the tier (Edge ≤ 2, Laptop ≤ 8, Server the full `worker_budget()`).
//! The elastic cap stays ~2-4x the permits on every machine class, so a few
//! concurrent analytical queries fan out fully while further dispatch
//! declines to sequential.
//!
//! Edge raises thresholds (small boxes avoid fan-out overhead — the same
//! direction as the profile's `parallel_row_threshold`, but these gates keep
//! their own calibrations); Laptop carries exactly the historical constants,
//! so default-tier behavior is unchanged; Server keeps or lowers thresholds.
//! Laptop/Server batch/buffer/pool stay pinned at the historical constants
//! even where the profile's generic accessors differ (the vector knee at
//! 16384 rows and the 256 MiB cache are calibrated against the memory
//! crate's 512 MiB OLAP budget — see [`ParallelBudget::from_profile`]).
//!
//! Tier source precedence is the profile's own: `--profile` CLI flag (server)
//! over `PLOMID_PROFILE` env (`edge|laptop|server|auto`) over host
//! detection, resolved once per process. An unparsable env value fails safe
//! to Laptop (the profile's safe middle default) with one stderr warning;
//! the server surfaces the same typo as a startup error through its own
//! resolve path.

use std::sync::{Mutex, Once};

use plomid_hw::{HardwareProfile, ProfileSource, RuntimeProfile, RuntimeTier};

/// Historical reference budget (32): the pre-elastic Laptop-tier cap, kept so
/// default-tier behavior is byte-identical to the pre-elastic code.
///
/// It is no longer a cap on any machine class. It survives as (a) the
/// pre-init fallback while tier detection has not run yet (the statics start
/// empty), and (b) the conservative RAM term when total RAM is unknown (never
/// over-commit blind). Calibration (unchanged): 2 workers per core of a
/// 16-core reference box; 32 permits let roughly two concurrent analytical
/// queries run fully parallel while further concurrent dispatch declines to
/// sequential with byte-identical results. Oversized single requests are
/// clamped to the elastic cap by [`claim_dispatch_workers`] rather than
/// declined, so large boxes keep reduced (not zero) fan-out.
pub const ANALYTICAL_WORKER_BUDGET: usize = 32;

/// Floor of the elastic worker cap. The hardware-derived cap never drops
/// below one small dispatch pair, so degenerate RAM readings fail safe to
/// reduced (not zero) fan-out; exhaustion still declines to sequential.
pub const MIN_ANALYTICAL_WORKER_BUDGET: usize = 4;

/// Absolute safety ceiling of the elastic worker cap: a thread-explosion
/// backstop only (128 conns x Ncpu `thread::scope` workers), never a sizing
/// target. Normal operation sizes from CPUs and RAM far below this; only
/// absurd core counts ever touch it.
pub const MAX_ANALYTICAL_WORKER_BUDGET: usize = 512;

/// Dispatch lanes per logical CPU in the elastic cap: two outstanding
/// analytical workers per core process-wide, i.e. roughly two fully-parallel
/// queries on any machine class. Matches the historical calibration exactly
/// (16-core reference box -> 32, the pre-elastic Laptop cap).
const DISPATCH_LANES_PER_CPU: u64 = 2;

/// Fraction of total RAM reservable for analytical workers in the elastic
/// cap: one quarter of detected RAM divided by the per-worker arena. Keeps
/// small boxes safe (a 4c/1GiB Pi gets RAM term 32, so the CPU term binds
/// first) while never binding healthy big iron (a 128c/512GiB server gets
/// RAM term 16384, so the CPU term binds).
const WORKER_RAM_FRACTION: u64 = 4;

/// Hardware-derived process-wide worker cap: `min(ceiling, cpu_term,
/// ram_term)`, floored at [`MIN_ANALYTICAL_WORKER_BUDGET`].
///
/// * `cpu_term = 2 x logical_cpus`, from the measured [`HardwareProfile`]
///   field — deliberately NOT `HardwareProfile::worker_budget`, whose
///   64-clamp is a per-dispatch sizing rail that would re-pin big iron (see
///   the sibling-rail report in [`ParallelBudget::from_profile`]).
/// * `ram_term = (RAM/4) / per-worker-arena`, where the arena is the memory
///   crate's `MemoryConfig::worker_budget` (8 MiB — read from the memory
///   crate, never hardcoded here). Unknown RAM falls back to the
///   conservative [`ANALYTICAL_WORKER_BUDGET`] (32, the historical Laptop
///   reference); a known-but-degenerate reading floors the term at 1 so the
///   final floor (never zero) governs.
/// * The ceiling is [`MAX_ANALYTICAL_WORKER_BUDGET`] (512), a
///   thread-explosion backstop only.
///
/// Pure over measured fields: no syscalls, no env reads. Worked examples:
/// 4c/1GiB -> min(512, 8, 32) = 8; 16c/16GiB -> min(512, 32, 512) = 32;
/// 128c/512GiB -> min(512, 256, 16384) = 256; 64c/unknown-RAM ->
/// min(512, 128, 32) = 32.
pub fn elastic_cap_for_hardware(hardware: &HardwareProfile) -> usize {
    let cpu_term = (hardware.logical_cpus as u64)
        .saturating_mul(DISPATCH_LANES_PER_CPU)
        .max(1);
    let arena = plomid_memory::MemoryConfig::default().worker_budget.max(1);
    let ram_term = match hardware.ram_total_bytes {
        Some(ram) => (ram / WORKER_RAM_FRACTION / arena).max(1),
        None => ANALYTICAL_WORKER_BUDGET as u64,
    };
    cpu_term
        .min(ram_term)
        .min(MAX_ANALYTICAL_WORKER_BUDGET as u64)
        .max(MIN_ANALYTICAL_WORKER_BUDGET as u64) as usize
}

/// Tiered parallel sizing derived from a [`RuntimeProfile`].
///
/// The tier itself comes from `plomid-hw` (never redefined here); this
/// struct is EDGE-B's mapping from tier to executor knobs (module-level
/// table). `Laptop` is exactly the historical constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelBudget {
    /// The profile tier this budget was derived from.
    pub tier: RuntimeTier,
    /// Effective process-wide worker cap for this tier.
    pub budget: usize,
    /// Per-dispatch fan-out width: the profile's `worker_permits()`.
    pub worker_permits: usize,
    /// Minimum combined probe + build rows for the parallel join.
    pub join_threshold: usize,
    /// Minimum estimated rows for parallel scan-aggregation.
    pub scan_min_rows: u64,
    /// Minimum materialized rows for parallel ORDER BY fan-out.
    pub sort_min_rows: usize,
    /// Minimum build rows for the parallel hash-table build.
    pub build_min_rows: usize,
    /// Default vector compute block size in rows.
    pub vector_batch_rows: usize,
    /// Default process-wide OLAP scan cache capacity in bytes.
    pub buffer_capacity_bytes: u64,
    /// OLTP-fenced reserve inside that cache in bytes.
    pub buffer_reserve_bytes: u64,
    /// Default buffer-pool capacity in pages per segment tree.
    ///
    /// Carried, not wired: the lifecycle/txn pool-capacity default call
    /// sites are read-only for EDGE-B, so the pool default stays 1024 until
    /// the profile's `pool_capacity_pages` is wired through there. Derived
    /// by the profile's own pool formula from the (capped) buffer capacity
    /// so the triple stays coherent.
    pub pool_capacity_pages: usize,
}

impl ParallelBudget {
    /// Pure derivation from an already-resolved profile: no global state,
    /// no I/O, no env reads — threshold-per-tier tests mock tiers through
    /// exactly this function with explicit
    /// [`RuntimeProfile::with_tier`](plomid_hw::RuntimeProfile::with_tier)
    /// profiles.
    ///
    /// Mapping notes (tier-owned values pass through untouched):
    /// * `budget` is the elastic hardware-derived cap
    ///   ([`elastic_cap_for_hardware`]): `min(512 ceiling, 2 x cpus,
    ///   (RAM/4) / 8MiB-arena)`, floored at 4 — NOT a fixed per-tier value.
    ///   On the canonical mocks this yields Edge(4c/4GiB) 8, Laptop(16c/16GiB)
    ///   32 (exactly the historical constant), Server(32c/64GiB) 64.
    /// * Related sizing lives in `hw` (read-only here, reported not
    ///   edited): `HardwareProfile::worker_budget`
    ///   (`crates/hw/src/profile.rs`) now scales per-dispatch sizing with
    ///   CPUs (ceiling 256, RAM/4 ÷ arena bound), so Server `worker_permits`
    ///   (`crates/hw/src/runtime.rs`, full budget on Server) scales past 64
    ///   on big iron while the process-wide cap covers ~2 concurrent full
    ///   dispatches. `crates/hw/src/runtime.rs`
    ///   states no fixed executor worker caps, so no sibling doc is stale.
    /// * Row thresholds keep their own calibrations, tiered in the same
    ///   direction as the profile's `parallel_row_threshold` (fewer workers
    ///   must see more rows): Laptop pins the historical constants, Edge
    ///   raises, Server lowers.
    /// * `vector_batch_rows` pins the calibrated 16384-row knee on
    ///   Laptop/Server (the profile's L2-fit is smaller and uncalibrated
    ///   for this path); Edge takes the profile's halved L2-fit clamped to
    ///   the 1-4K band.
    /// * `buffer_capacity_bytes` passes the profile's capacity through,
    ///   capped at the historical 256 MiB so the Server tier cannot exceed
    ///   the memory crate's 512 MiB OLAP budget; the reserve is 1/4 of that
    ///   (the historical 25% OLTP-fence ratio), and the pool applies the
    ///   profile's own pool formula to the capped capacity.
    pub fn from_profile(profile: &RuntimeProfile) -> Self {
        let tier = profile.tier();
        let budget = elastic_cap_for_hardware(profile.hardware());
        let (join_threshold, scan_min_rows, sort_min_rows, build_min_rows) = match tier {
            RuntimeTier::Edge => (8_000, 400_000, 200_000, 200_000),
            RuntimeTier::Laptop => (2_000, 100_000, 50_000, 50_000),
            RuntimeTier::Server => (1_000, 50_000, 25_000, 25_000),
        };
        let vector_batch_rows = match tier {
            RuntimeTier::Edge => profile.vector_batch_rows().min(4_096),
            RuntimeTier::Laptop | RuntimeTier::Server => 16_384,
        };
        let buffer_capacity_bytes = profile.buffer_capacity_bytes().min(256 * 1024 * 1024);
        let buffer_reserve_bytes = buffer_capacity_bytes / 4;
        // The profile's own pool formula (buffer/16 in 16 KiB pages,
        // clamped [64, 65536]) applied to the capped capacity: 64 MiB -> 256
        // pages, 256 MiB -> 1024 pages (the historical default).
        let pool_capacity_pages =
            ((buffer_capacity_bytes / 16 / (16 * 1024)) as usize).clamp(64, 65_536);
        Self {
            tier,
            budget,
            worker_permits: profile.worker_permits(),
            join_threshold,
            scan_min_rows,
            sort_min_rows,
            build_min_rows,
            vector_batch_rows,
            buffer_capacity_bytes,
            buffer_reserve_bytes,
            pool_capacity_pages,
        }
    }
}

/// One-time tier detection. Static starts empty (reads fall back to the
/// historical constants), so pre-init behavior is unchanged; the first
/// threshold or claim access resolves the real tier and resizes the
/// free-permit count to match (raising tops up, lowering saturates — same
/// rule as the test hooks, and outstanding leases still return via the
/// saturating release).
static TIER_INIT: Once = Once::new();
static TIER: Mutex<Option<ParallelBudget>> = Mutex::new(None);

/// Last resolved [`RuntimeProfile`] behind [`TIER`]: the hardware the elastic
/// cap was derived from plus the tier source (auto vs explicit pin). Kept so
/// live re-detection refreshes only the volatile limits instead of
/// re-measuring stable topology. `None` until [`ensure_tier_init`].
static PROFILE: Mutex<Option<RuntimeProfile>> = Mutex::new(None);

/// Last live re-detection time. Interval-gated by
/// [`plomid_hw::RECHECK_INTERVAL`] (60 s): steady-state cost is one
/// `Instant` check per admission, at most one sysfs/proc re-read per
/// interval when busy. No background thread.
static LAST_RECHECK: Mutex<Option<std::time::Instant>> = Mutex::new(None);

/// Historical fallback while the tier is unresolved (the Laptop constants).
fn unresolved_budget() -> ParallelBudget {
    ParallelBudget {
        tier: RuntimeTier::Laptop,
        budget: ANALYTICAL_WORKER_BUDGET,
        worker_permits: ANALYTICAL_WORKER_BUDGET,
        join_threshold: 2_000,
        scan_min_rows: 100_000,
        sort_min_rows: 50_000,
        build_min_rows: 50_000,
        vector_batch_rows: 16_384,
        buffer_capacity_bytes: 256 * 1024 * 1024,
        buffer_reserve_bytes: 64 * 1024 * 1024,
        pool_capacity_pages: 1024,
    }
}

fn ensure_tier_init() {
    // Fast path: already initialized (production startup or a test pin via
    // `TestTierGuard`/`set_tier_for_tests`) — no syscalls, no overwrite.
    // A test pin that lands before the one-time detection wins: the `Once`
    // below double-checks and skips, so the first pin is never clobbered.
    {
        let tier = TIER.lock().unwrap_or_else(|e| e.into_inner());
        if tier.is_some() {
            let mut last = LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_none() {
                *last = Some(std::time::Instant::now());
            }
            return;
        }
    }
    TIER_INIT.call_once(|| {
        // A test pin may have landed between the fast-path check and now:
        // keep it instead of overwriting with host detection.
        {
            let tier = TIER.lock().unwrap_or_else(|e| e.into_inner());
            if tier.is_some() {
                let mut last = LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner());
                if last.is_none() {
                    *last = Some(std::time::Instant::now());
                }
                return;
            }
        }
        let profile = resolve_profile();
        let detected = ParallelBudget::from_profile(&profile);
        // Lock order (never reversed anywhere): PROFILE, then TIER, then
        // AVAILABLE.
        let mut stored = PROFILE.lock().unwrap_or_else(|e| e.into_inner());
        let mut tier = TIER.lock().unwrap_or_else(|e| e.into_inner());
        // Re-check under the locks: a pin may have landed during detection
        // (which does syscalls without holding locks).
        if tier.is_some() {
            drop(tier);
            drop(stored);
            let mut last = LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_none() {
                *last = Some(std::time::Instant::now());
            }
            return;
        }
        let old = tier.map(|t| t.budget).unwrap_or(ANALYTICAL_WORKER_BUDGET);
        *stored = Some(profile);
        *tier = Some(detected);
        if detected.budget != old {
            let mut available = AVAILABLE.lock().unwrap_or_else(|e| e.into_inner());
            if detected.budget > old {
                *available = (*available + (detected.budget - old)).min(detected.budget);
            } else {
                *available = (*available).min(detected.budget);
            }
        }
        *LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::time::Instant::now());
    });
}

/// Resolve the startup profile through the profile's own precedence
/// (`--profile` flag folded into `PLOMID_PROFILE` by the server CLI /
/// `PLOMID_PROFILE` env / host detection). An unparsable env value fails
/// safe to the Laptop tier with one stderr warning; the server surfaces the
/// same typo as a startup error through its own resolve path.
fn resolve_profile() -> RuntimeProfile {
    match RuntimeProfile::resolve(None) {
        Ok(profile) => profile,
        Err(reason) => {
            eprintln!("warning: {reason}; falling back to the laptop tier");
            RuntimeProfile::with_tier(
                RuntimeTier::Laptop,
                HardwareProfile::detect(),
                ProfileSource::Explicit,
            )
        }
    }
}

/// Apply a freshly re-detected profile to the live budget without
/// interrupting queries.
///
/// * Growing the cap tops up free permits (`AVAILABLE += delta`, capped at
///   the new budget): new admissions immediately fan out wider.
/// * Shrinking the cap saturates free permits (`AVAILABLE = min(free, new)`)
///   and never revokes held leases: in-flight queries keep their permits to
///   completion (their `BudgetGuard::drop` saturates the surplus), only NEW
///   admissions see the smaller cap.
/// * A tier flip (Edge↔Server) swaps thresholds for new dispatches; permits
///   already claimed are unaffected. Rechecks run only at admission
///   boundaries (see [`maybe_recheck_if_due`]), so a batch's own dispatches
///   never trigger a mid-query flip — at most a concurrent query's recheck
///   lands mid-flight, and even then held permits are never revoked.
/// * Explicit pins (`PLOMID_PROFILE`, `--profile`, embedding) are honored
///   inside [`RuntimeProfile::recheck`]: the tier stays pinned while the
///   hardware (and thus the elastic cap) still refreshes underneath.
///
/// Lock order: PROFILE, then TIER, then AVAILABLE (never reversed).
fn apply_redetected_profile(next: &RuntimeProfile) {
    let detected = ParallelBudget::from_profile(next);
    let mut stored = PROFILE.lock().unwrap_or_else(|e| e.into_inner());
    let mut tier = TIER.lock().unwrap_or_else(|e| e.into_inner());
    let old = tier.map(|t| t.budget).unwrap_or(ANALYTICAL_WORKER_BUDGET);
    *stored = Some(next.clone());
    *tier = Some(detected);
    if detected.budget != old {
        let mut available = AVAILABLE.lock().unwrap_or_else(|e| e.into_inner());
        if detected.budget > old {
            *available = (*available + (detected.budget - old)).min(detected.budget);
        } else {
            *available = (*available).min(detected.budget);
        }
    }
    *LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner()) = Some(std::time::Instant::now());
}

/// Live limit re-detection: refresh the stored profile's volatile limits and
/// update the live budget (see [`apply_redetected_profile`] for the
/// grow/shrink contract). Cheap: at most one sysfs/proc re-read per
/// [`plomid_hw::RECHECK_INTERVAL`]; otherwise only `Instant` checks.
///
/// Called from the admission choke point ([`crate::governance::admit_query`],
/// once per batch) — no new threads, no per-dispatch cost, no mid-query
/// flip from the rechecking query itself. The maintenance worker's 100 ms
/// idle tick was considered and rejected: it only runs when the worker is
/// spawned, wakes while idle, and would still need the same interval gate.
pub fn maybe_recheck_if_due() {
    // Test harness pins tiers via `TestTierGuard`; a time-based sysfs
    // re-read would clobber pinned values and read real env. Tests drive
    // rescale explicitly through `recheck_with_hardware_for_tests`.
    // Uses the cfg-test macro (not the test attribute) so the
    // inline-test gate stays empty after the test move; the lib built for
    // integration tests runs the production path below.
    if cfg!(test) {
        return;
    }
    {
        ensure_tier_init();
        let due = LAST_RECHECK
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none_or(|at| at.elapsed() >= plomid_hw::RECHECK_INTERVAL);
        if !due {
            return;
        }
        let mut next = PROFILE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_else(resolve_profile);
        let tier_changed = next.recheck();
        // Skip the write locks when nothing moved (common case: same CPUs +
        // same RAM + same tier): the re-read above is the only cost.
        let current = TIER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|t| (t.budget, t.tier));
        let want = ParallelBudget::from_profile(&next);
        if current == Some((want.budget, want.tier)) {
            *LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(std::time::Instant::now());
            return;
        }
        if tier_changed {
            eprintln!(
                "note: runtime tier re-detected as {} (source {}); new queries use updated thresholds",
                next.tier(),
                next.source()
            );
        }
        apply_redetected_profile(&next);
    }
}

/// Force a live re-detection now, bypassing the interval gate (operator /
/// embedding hook; also used by the maintenance idle path if it ever needs
/// one). Same grow/shrink contract as [`apply_redetected_profile`].
#[allow(dead_code)]
pub fn recheck_now() {
    ensure_tier_init();
    let mut next = PROFILE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(resolve_profile);
    next.recheck();
    apply_redetected_profile(&next);
}

/// The currently effective tier (detected once, overridable by test
/// hooks). Copies the small struct; callers use the narrow delegates below.
pub fn current_tier() -> ParallelBudget {
    ensure_tier_init();
    TIER.lock()
        .unwrap_or_else(|e| e.into_inner())
        .unwrap_or_else(unresolved_budget)
}

/// Hardware-derived process-wide worker cap ([`elastic_cap_for_hardware`] of
/// the detected hardware: 8 on Pi-class, 32 on the 16c laptop reference, 256
/// on 128c big iron, 512 absolute ceiling).
pub fn budget_cap() -> usize {
    current_tier().budget
}

/// Per-dispatch fan-out permits for this tier (the profile's
/// `worker_permits()`: Edge ≤ 2, Laptop ≤ 8, Server the full budget).
pub fn parallel_permits() -> usize {
    current_tier().worker_permits
}

/// Tiered parallel-join row threshold (Laptop: 2_000).
pub fn join_threshold() -> usize {
    current_tier().join_threshold
}

/// Tiered parallel scan-aggregation threshold (Laptop: 100_000).
pub fn scan_min_rows() -> u64 {
    current_tier().scan_min_rows
}

/// Tiered parallel sort threshold (Laptop: 50_000).
pub fn sort_min_rows() -> usize {
    current_tier().sort_min_rows
}

/// Tiered parallel hash-build threshold (Laptop: 50_000).
pub fn build_min_rows() -> usize {
    current_tier().build_min_rows
}

/// Machine parallelism clamped to the tier's per-dispatch permits AND the
/// elastic hardware cap: the worker-count derivation every dispatch site uses
/// BEFORE claiming. The permits bind first on Edge/Laptop (2/8); the cap
/// binds oversized wants on big iron. (This intentionally differs from raw
/// `available_parallelism` on small tiers — same results, narrower fan-out;
///
/// the old unclamped `par_sort_workers_for` / `parallel_build` bypass.)
pub fn available_workers() -> usize {
    let machine = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    machine.min(parallel_permits()).min(budget_cap())
}

/// Minimum fan-out worth claiming permits for. All current parallel sites
/// decline below 2 workers internally, so claiming 0/1 permits would hold
/// budget for a dispatch that runs sequentially anyway.
const MIN_DISPATCH_WORKERS: usize = 2;

/// Remaining permits. Invariant: `0 <= AVAILABLE <= budget_cap()`
/// outside the tiny compare-and-sub/add critical sections (test hooks may set
/// it lower; [`claim_dispatch_workers`] clamps oversized wants, and
/// [`try_acquire`] refuses oversized wants, so production code never exceeds
/// the effective cap).
static AVAILABLE: Mutex<usize> = Mutex::new(ANALYTICAL_WORKER_BUDGET);

/// RAII lease on `permits` analytical workers. Dropping (including via panic
/// unwinding) returns the permits. Holds a count, not the lock: no mutex is
/// held across a dispatch.
pub struct BudgetGuard {
    permits: usize,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        if self.permits == 0 {
            return;
        }
        let mut available = AVAILABLE.lock().unwrap_or_else(|e| e.into_inner());
        // Saturate rather than overflow: a test hook (or a tier transition)
        // may have lowered the cap while leases were outstanding; permits
        // still return, the surplus is discarded so the invariant holds.
        *available = (*available + self.permits).min(budget_cap_no_init());
    }
}

/// The effective cap without triggering detection (drop/test paths that must
/// never run env reads or init resizes; the tier static starts empty so
/// pre-init reads fall back to exactly the historical behavior).
fn budget_cap_no_init() -> usize {
    TIER.lock()
        .unwrap_or_else(|e| e.into_inner())
        .map(|tier| tier.budget)
        .unwrap_or(ANALYTICAL_WORKER_BUDGET)
        .clamp(MIN_ANALYTICAL_WORKER_BUDGET, MAX_ANALYTICAL_WORKER_BUDGET)
}

/// Try to claim exactly `workers` permits without blocking. `None` means run
/// sequential: `workers == 0`, `workers` above the effective cap (never
/// satisfiable), or fewer than `workers` permits currently free. Never
/// blocks, never fails (a poisoned mutex is recovered, not propagated).
pub fn try_acquire(workers: usize) -> Option<BudgetGuard> {
    if workers == 0 || workers > budget_cap() {
        return None;
    }
    let mut available = AVAILABLE.lock().unwrap_or_else(|e| e.into_inner());
    if *available < workers {
        return None;
    }
    *available -= workers;
    Some(BudgetGuard { permits: workers })
}

/// Currently free permits (test introspection; also available for future
/// dispatch logging at deferred wiring sites).
pub fn available() -> usize {
    *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner())
}

/// A claimed dispatch: the permit lease plus the worker count the parallel
/// call must actually fan out to. The lease releases on drop; the caller runs
/// sequential when this returns `None`.
pub struct ClaimedDispatch {
    _lease: BudgetGuard,
    workers: usize,
}

impl ClaimedDispatch {
    /// Worker count the parallel call must fan out to (clamped to the
    /// budget cap, always `>= MIN_DISPATCH_WORKERS`).
    pub fn workers(&self) -> usize {
        self.workers
    }
}

/// Claim workers for one parallel dispatch, or `None` to run sequential.
///
/// `want` is clamped to the elastic hardware cap ([`budget_cap`]; a 64-thread
/// request on a 64-cap box still fans out 64-wide instead of declining),
/// then declined when below [`MIN_DISPATCH_WORKERS`] or when fewer permits
/// are free. The returned lease is held across the parallel call and
/// released on scope exit, including decline-then-sequential fall-through.
pub fn claim_dispatch_workers(want: usize) -> Option<ClaimedDispatch> {
    let workers = want.min(budget_cap());
    if workers < MIN_DISPATCH_WORKERS {
        return None;
    }
    let lease = try_acquire(workers)?;
    Some(ClaimedDispatch {
        _lease: lease,
        workers,
    })
}

/// Test-only override of the free-permit count. `pub` + `cfg(test)`:
/// no public API change. Tests must go through [`with_test_budget`] (or the
/// tier helpers below) so the globals are restored (and serialized)
/// afterwards.
pub fn set_budget_for_tests(permits: usize) {
    let cap = budget_cap_no_init();
    *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner()) = permits.min(cap);
}

/// Test-only tier override (sets the tier AND refills permits to the tier
/// cap, so the test starts from a known-full budget). Restored by the
/// [`TestTierGuard`].
pub fn set_tier_for_tests(tier: ParallelBudget) {
    // Lock order: PROFILE, then TIER, then AVAILABLE (never reversed).
    // Keep PROFILE consistent with TIER so live-recheck tests start from a
    // coherent snapshot: synthesize the Explicit profile that derives to
    // this budget's tier on canonical mock hardware.
    let hardware = match tier.tier {
        RuntimeTier::Edge => mock_hardware_profile(4, Some(4 * 1024 * 1024 * 1024)),
        RuntimeTier::Laptop => mock_hardware_profile(16, Some(16 * 1024 * 1024 * 1024)),
        RuntimeTier::Server => mock_hardware_profile(32, Some(64 * 1024 * 1024 * 1024)),
    };
    let profile = RuntimeProfile::with_tier(tier.tier, hardware, ProfileSource::Explicit);
    *PROFILE.lock().unwrap_or_else(|e| e.into_inner()) = Some(profile);
    *TIER.lock().unwrap_or_else(|e| e.into_inner()) = Some(tier);
    *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner()) = tier.budget;
}

/// Test-only full-profile override: sets PROFILE, derives TIER from it, and
/// refills permits to the derived cap. For live-rescale tests that need a
/// specific source (e.g. `Explicit` pin vs `Auto` detect) or non-canonical
/// hardware. Restored by holding a [`TestTierGuard`] (which snapshots all
/// three globals).
pub fn set_profile_for_tests(profile: RuntimeProfile) {
    let budget = ParallelBudget::from_profile(&profile);
    *PROFILE.lock().unwrap_or_else(|e| e.into_inner()) = Some(profile);
    *TIER.lock().unwrap_or_else(|e| e.into_inner()) = Some(budget);
    *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner()) = budget.budget;
}

/// Test seam for live rescale: swaps in mock hardware and applies the same
/// pinned-vs-auto tier policy as production [`RuntimeProfile::recheck`]
/// (via [`RuntimeProfile::recheck_with`]: `Auto` re-classifies, explicit
/// pins keep their tier), then resizes the live budget with the production
/// grow/shrink contract (grow tops up, shrink saturates free permits while
/// in-flight leases keep theirs). Bypasses the interval gate and sysfs/env
/// reads so tests are hermetic. Must run under [`TestTierGuard`] (or
/// [`with_test_tier`]) so the globals restore afterwards.
pub fn recheck_with_hardware_for_tests(hardware: HardwareProfile) {
    ensure_tier_init();
    let mut next = PROFILE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .unwrap_or_else(|| {
            RuntimeProfile::with_tier(
                RuntimeTier::Laptop,
                mock_hardware_profile(16, Some(16 * 1024 * 1024 * 1024)),
                ProfileSource::Explicit,
            )
        });
    next.recheck_with(hardware);
    apply_redetected_profile(&next);
}

/// RAII pin of the process-wide tier + permit count for tests.
///
/// Holds the crate-wide test serial across its lifetime, so tier-mutating
/// tests and tier-sensitive readers (join gate engagement tests, the sort
/// gate test) never observe each other mid-flight. Tests that mutate the
/// tier MUST go through this guard (directly or via [`with_test_tier`]);
/// tier-sensitive readers hold one pinned to the Laptop tier (see
/// [`pinned_tier_for_tests`]). Drop restores the exact pre-hold snapshot.
pub struct TestTierGuard {
    _serial: std::sync::MutexGuard<'static, ()>,
    tier: Option<ParallelBudget>,
    available: usize,
    profile: Option<RuntimeProfile>,
    last_recheck: Option<std::time::Instant>,
}

impl TestTierGuard {
    pub fn hold(tier: ParallelBudget) -> Self {
        let serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let prev_tier = *TIER.lock().unwrap_or_else(|e| e.into_inner());
        let prev_available = *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner());
        let prev_profile = PROFILE.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let prev_recheck = *LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner());
        set_tier_for_tests(tier);
        Self {
            _serial: serial,
            tier: prev_tier,
            available: prev_available,
            profile: prev_profile,
            last_recheck: prev_recheck,
        }
    }
}

impl Drop for TestTierGuard {
    fn drop(&mut self) {
        // Lock order: PROFILE, then TIER, then AVAILABLE (never reversed).
        *PROFILE.lock().unwrap_or_else(|e| e.into_inner()) = self.profile.clone();
        *TIER.lock().unwrap_or_else(|e| e.into_inner()) = self.tier;
        *AVAILABLE.lock().unwrap_or_else(|e| e.into_inner()) = self.available;
        *LAST_RECHECK.lock().unwrap_or_else(|e| e.into_inner()) = self.last_recheck;
    }
}

/// Canonical mock hardware per tier for tests (mirrors the `plomid-hw`
/// seam-test mocks): Edge is Pi-class (4 CPUs, 4 GiB), Laptop the 16-core
/// reference box the historical constants were calibrated on (16 CPUs,
/// 16 GiB, so the elastic cap lands on exactly 32), Server big iron
/// (32 CPUs, 64 GiB, cap 64).
pub fn pinned_tier_for_tests(tier: RuntimeTier) -> ParallelBudget {
    let (cpus, ram) = match tier {
        RuntimeTier::Edge => (4, 4 * 1024 * 1024 * 1024),
        RuntimeTier::Laptop => (16, 16 * 1024 * 1024 * 1024),
        RuntimeTier::Server => (32, 64 * 1024 * 1024 * 1024),
    };
    let hardware = mock_hardware_profile(cpus, Some(ram));
    ParallelBudget::from_profile(&RuntimeProfile::with_tier(
        tier,
        hardware,
        ProfileSource::Explicit,
    ))
}

/// Mock measured hardware for elastic-cap tests: any `(cpus, ram)` shape,
/// including Pi-class, big iron, and unknown-RAM (`None`).
pub fn mock_hardware_profile(cpus: usize, ram: Option<u64>) -> HardwareProfile {
    use plomid_hw::{Architecture, CpuFeatures, StorageKind};
    HardwareProfile {
        arch: Architecture::X86_64,
        cpu: CpuFeatures {
            scalar: true,
            sse42: false,
            avx2: false,
            avx512: false,
            neon: false,
        },
        logical_cpus: cpus,
        physical_cpus: None,
        cache_bytes: [None, None, None],
        ram_total_bytes: ram,
        ram_available_bytes: None,
        storage: StorageKind::Unknown,
        storage_bandwidth_bps: None,
        storage_iops_estimate: None,
        numa_nodes: None,
        notes: Vec::new(),
    }
}

/// Run `f` with the given tier pinned (permits refilled to the elastic cap),
/// restoring the exact pre-test snapshot afterwards even on panic.
pub fn with_test_tier<R>(tier: ParallelBudget, f: impl FnOnce() -> R) -> R {
    let _guard = TestTierGuard::hold(tier);
    f()
}

/// Serializes tests that mutate the process-wide budget/tier. Held for the
/// whole guard lifetime so mutating tests never observe each other's
/// overrides, and tier-sensitive readers in other modules (`join`, `query`)
/// hold it while asserting. Module-level (not in `tests`) so cross-module
/// test code can share it.
static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
