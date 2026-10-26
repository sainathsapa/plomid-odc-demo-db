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

//! Cooperative throttling + cancellation for compaction.
//!
//! Compaction must never starve OLTP. It is the lowest-ranked work in the
//! [`ResourceGovernor`](crate::governor::ResourceGovernor) (`Maintenance`
//! is queued at critical pressure). This module turns that policy into
//! enforcement points long-running maintenance passes consult
//! cooperatively:
//!
//! ```text
//! OLTP pressure rises
//!   Full      → full speed (no yield)
//!   Degraded  → throttled progress (small slices + yield)
//!   Paused    → abort, never spin (Queue/Reject → AbortPaused)
//! ```
//!
//! Rules:
//! - `Paused` aborts at entry and at every recheck; it never spins or retries
//!   internally. The caller drops its work and lets a later pass retry when
//!   pressure drops.
//! - `Degraded` makes bounded progress: small slices plus a cooperative yield
//!   every `yield_every_rows` rows so OLTP threads get CPU and I/O windows.
//! - Cancellation and deadlines abort before any visible publication. A run
//!   that aborts never publishes, releases, or reclaims.
//! - The pre-existing [`crate::maintenance::MaintenanceThrottle`] and
//!   [`crate::governor::ResourceGovernor`] are reused; nothing here duplicates
//!   their budgets, caps, or pressure mapping.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::PressureState;
use crate::governor::{Admission, ResourceGovernor};
use crate::maintenance::{throttle, MaintenanceThrottle};
use crate::workload::WorkloadClass;

/// Cooperative cancellation handle shared between the requesting thread and a
/// running compaction pass.
#[derive(Clone, Debug, Default)]
pub struct CancelToken {
    cancelled: Arc<AtomicBool>,
}

impl CancelToken {
    /// A token that has not been cancelled.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A token that is already cancelled (for tests).
    pub fn cancelled() -> Self {
        let token = Self::new();
        token.cancel();
        token
    }

    /// Signal cancellation. Idempotent.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// True once [`cancel`](Self::cancel) has been called.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

/// Wall-clock deadline for one maintenance pass.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    at: Option<Instant>,
}

impl Deadline {
    /// No deadline: never expires.
    pub fn never() -> Self {
        Self { at: None }
    }

    /// Expire at `at`.
    pub fn at(at: Instant) -> Self {
        Self { at: Some(at) }
    }

    /// Expire `after` from now.
    pub fn after(after: Duration) -> Self {
        Self {
            at: Some(Instant::now() + after),
        }
    }

    /// A deadline that is already expired (for tests).
    pub fn expired() -> Self {
        Self {
            at: Some(
                Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
            ),
        }
    }

    /// True when the deadline has passed. A missing deadline never expires.
    pub fn is_expired(&self) -> bool {
        match self.at {
            None => false,
            Some(at) => Instant::now() >= at,
        }
    }

    /// Alias for [`is_expired`](Self::is_expired) used at gate checkpoints.
    pub fn check(&self) -> bool {
        self.is_expired()
    }
}

impl Default for Deadline {
    fn default() -> Self {
        Self::never()
    }
}

/// Throttle-derived slicing tunables for one compaction pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionThrottle {
    /// Current maintenance throttle.
    pub throttle: MaintenanceThrottle,
    /// Rows per cooperative chunk while building / reading.
    pub max_rows_per_slice: u64,
    /// Yield (sleep + recheck) every this many rows when degraded.
    pub yield_every_rows: u64,
}

impl CompactionThrottle {
    /// Full speed: large slices, never yields.
    pub fn full() -> Self {
        Self {
            throttle: MaintenanceThrottle::Full,
            max_rows_per_slice: 8192,
            yield_every_rows: u64::MAX,
        }
    }

    /// Degraded: small slices plus a periodic cooperative yield.
    pub fn degraded() -> Self {
        Self {
            throttle: MaintenanceThrottle::Degraded,
            max_rows_per_slice: 256,
            yield_every_rows: 256,
        }
    }

    /// Paused: no work is allowed.
    pub fn paused() -> Self {
        Self {
            throttle: MaintenanceThrottle::Paused,
            max_rows_per_slice: 0,
            yield_every_rows: 1,
        }
    }

    /// Derive slicing tunables from pressure via the shared
    /// [`throttle`] mapping (no duplicated thresholds).
    pub fn from_pressure(pressure: PressureState) -> Self {
        match throttle(pressure) {
            MaintenanceThrottle::Full => Self::full(),
            MaintenanceThrottle::Degraded => Self::degraded(),
            MaintenanceThrottle::Paused => Self::paused(),
        }
    }
}

/// What a gate checkpoint decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateDecision {
    /// Keep going.
    Proceed,
    /// Yield (sleep) then recheck aborts before continuing.
    YieldAndRecheck,
    /// Maintenance is paused under pressure: abort, do not spin.
    AbortPaused,
    /// Cancellation was requested: abort before any publication.
    AbortCancelled,
    /// The deadline expired: abort before any publication.
    AbortExpired,
}

impl GateDecision {
    /// True for any abort variant.
    pub fn is_abort(self) -> bool {
        !matches!(self, GateDecision::Proceed | GateDecision::YieldAndRecheck)
    }

    /// True only for [`GateDecision::YieldAndRecheck`].
    pub fn is_yield(self) -> bool {
        matches!(self, GateDecision::YieldAndRecheck)
    }

    /// Map an abort decision to a stable error. `Proceed`/`YieldAndRecheck`
    /// map to `None`.
    pub fn abort_error(self) -> Option<plomid_core::PlomidError> {
        match self {
            GateDecision::Proceed | GateDecision::YieldAndRecheck => None,
            GateDecision::AbortPaused => Some(plomid_core::PlomidError::with_detail(
                plomid_core::ErrorKind::Aborted,
                "compaction paused under OLTP pressure",
                "throttle=Paused",
            )),
            GateDecision::AbortCancelled => Some(plomid_core::PlomidError::with_detail(
                plomid_core::ErrorKind::Aborted,
                "compaction cancelled",
                "cancel_token=cancelled",
            )),
            GateDecision::AbortExpired => Some(plomid_core::PlomidError::with_detail(
                plomid_core::ErrorKind::Aborted,
                "compaction deadline expired",
                "deadline=expired",
            )),
        }
    }
}

/// Cooperative gate for one compaction / vacuum / GC pass.
///
/// Holds a throttle snapshot plus the cancel token and deadline the pass must
/// honor. Checkpoints call [`check`](Self::check) (or [`enforce`](Self::enforce));
/// degraded passes yield via [`cooperative_yield`](Self::cooperative_yield)
/// so OLTP work gets scheduling windows.
///
/// Yield counting: every [`cooperative_yield`](Self::cooperative_yield) call
/// increments [`yield_count`](Self::yield_count); tests assert degraded passes
/// yield at least once.
#[derive(Debug)]
pub struct CompactionGate {
    throttle: CompactionThrottle,
    cancel: CancelToken,
    deadline: Deadline,
    yields: AtomicU64,
}

impl CompactionGate {
    /// A fully-open gate: full speed, never cancelled, no deadline.
    /// `compact()` delegates through this so ungated behavior is bit-identical.
    pub fn open() -> Self {
        Self {
            throttle: CompactionThrottle::full(),
            cancel: CancelToken::new(),
            deadline: Deadline::never(),
            yields: AtomicU64::new(0),
        }
    }

    /// A gate that refuses all work (paused throttle).
    pub fn paused() -> Self {
        Self {
            throttle: CompactionThrottle::paused(),
            cancel: CancelToken::new(),
            deadline: Deadline::never(),
            yields: AtomicU64::new(0),
        }
    }

    /// Build a gate from explicit parts.
    pub fn new(throttle: CompactionThrottle, cancel: CancelToken, deadline: Deadline) -> Self {
        Self {
            throttle,
            cancel,
            deadline,
            yields: AtomicU64::new(0),
        }
    }

    /// Build a gate for `pressure` with the given cancel/deadline hooks.
    pub fn from_pressure(pressure: PressureState, cancel: CancelToken, deadline: Deadline) -> Self {
        Self::new(
            CompactionThrottle::from_pressure(pressure),
            cancel,
            deadline,
        )
    }

    /// Consult the governor before starting maintenance.
    ///
    /// OLTP non-competition proof: `Maintenance` is the lowest-ranked class.
    /// Any non-admit (`Queue` or `Reject` — including the critical-pressure
    /// maintenance pause and the OLTP-reserve fence) maps to `AbortPaused`:
    /// the pass aborts at entry instead of competing with OLTP. A paused
    /// throttle snapshot also refuses even if admission somehow succeeded.
    /// Only [`Admission::Admit`] proceeds, and then under the pressure-derived
    /// throttle (degraded → throttled progress, full → full speed).
    pub fn admit_or_refuse(governor: &ResourceGovernor, bytes: u64) -> Result<Self, GateDecision> {
        Self::admit_or_refuse_with(governor, bytes, CancelToken::new(), Deadline::never())
    }

    /// [`admit_or_refuse`](Self::admit_or_refuse) with caller-owned
    /// cancellation and deadline hooks.
    pub fn admit_or_refuse_with(
        governor: &ResourceGovernor,
        bytes: u64,
        cancel: CancelToken,
        deadline: Deadline,
    ) -> Result<Self, GateDecision> {
        let throttle = CompactionThrottle::from_pressure(governor.pressure());
        if throttle.throttle.is_paused() {
            return Err(GateDecision::AbortPaused);
        }
        match governor.admit(WorkloadClass::Maintenance, bytes, 1) {
            Admission::Admit { .. } => Ok(Self::new(throttle, cancel, deadline)),
            Admission::Queue { .. } | Admission::Reject { .. } => Err(GateDecision::AbortPaused),
        }
    }

    /// Current throttle snapshot.
    pub fn throttle(&self) -> CompactionThrottle {
        self.throttle
    }

    /// Number of cooperative yields performed so far.
    pub fn yield_count(&self) -> u64 {
        self.yields.load(Ordering::SeqCst)
    }

    /// Pure abort check: cancellation, deadline, pause. Never yields.
    pub fn check_abort(&self) -> GateDecision {
        if self.cancel.is_cancelled() {
            return GateDecision::AbortCancelled;
        }
        if self.deadline.is_expired() {
            return GateDecision::AbortExpired;
        }
        if self.throttle.throttle.is_paused() {
            return GateDecision::AbortPaused;
        }
        GateDecision::Proceed
    }

    /// Full checkpoint at `rows_so_far` rows of progress.
    ///
    /// Order is load-bearing: cancellation first, then deadline, then pause,
    /// then the degraded yield. Degraded yields every `yield_every_rows`
    /// rows (row 0 never yields so empty passes stay free).
    pub fn check(&self, rows_so_far: u64) -> GateDecision {
        if self.cancel.is_cancelled() {
            return GateDecision::AbortCancelled;
        }
        if self.deadline.is_expired() {
            return GateDecision::AbortExpired;
        }
        if self.throttle.throttle.is_paused() {
            return GateDecision::AbortPaused;
        }
        if self.throttle.throttle == MaintenanceThrottle::Degraded
            && self.throttle.yield_every_rows != 0
            && self.throttle.yield_every_rows != u64::MAX
            && rows_so_far > 0
            && rows_so_far.is_multiple_of(self.throttle.yield_every_rows)
        {
            return GateDecision::YieldAndRecheck;
        }
        GateDecision::Proceed
    }

    /// Yield the thread cooperatively and count it.
    ///
    /// `yield_now` plus a short sleep gives OLTP threads an I/O and CPU
    /// window without parking maintenance for a full timeslice.
    pub fn cooperative_yield(&self) {
        std::thread::yield_now();
        std::thread::sleep(Duration::from_micros(100));
        self.yields.fetch_add(1, Ordering::SeqCst);
    }

    /// Enforce the gate at `rows_so_far`:
    /// - `Proceed` → `Ok(false)` (no yield was needed),
    /// - `YieldAndRecheck` → yield, recheck aborts, `Ok(true)`,
    /// - any abort → `Err(PlomidError{Aborted})` before any publication.
    pub fn enforce(&self, rows_so_far: u64) -> plomid_core::Result<bool> {
        match self.check(rows_so_far) {
            GateDecision::Proceed => Ok(false),
            GateDecision::YieldAndRecheck => {
                self.cooperative_yield();
                match self.check_abort() {
                    GateDecision::Proceed => Ok(true),
                    abort => Err(abort
                        .abort_error()
                        .expect("abort decision must map to an error")),
                }
            }
            abort => Err(abort
                .abort_error()
                .expect("abort decision must map to an error")),
        }
    }
}

impl Default for CompactionGate {
    fn default() -> Self {
        Self::open()
    }
}
