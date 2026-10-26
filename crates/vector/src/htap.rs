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
//! Single-machine HTAP interference policy (Phase 58).
//!
//! Predictable behavior when one machine simultaneously performs OLTP
//! writes, SQL reads, exact/ANN/filtered/radius search, reranking,
//! batch workloads, index builds, maintenance, background work, and
//! backups. Objective: **predictable and measurable interference** —
//! never zero interference, never incorrect results.
//!
//! This module reuses existing controls and adds no scheduler, no
//! resource manager, no threads of its own:
//!
//! * worker/memory/batch sizing: [`crate::scale::ScaleDecision`] knobs,
//!   themselves derived from `plomid-hw` budgets;
//! * pressure thresholds mirror `plomid-memory` (`0.70/0.85/0.95`);
//! * cancellation/timeout reuse [`crate::search::CancelFn`] (checked every
//!   64 rows) and index-build cancel (`never publishes on cancel`);
//! * memory estimates reuse [`crate::options::memory_estimate_bytes`].
//!
//! Admission and throttling here are pure functions so the executor (which
//! owns the global governor, guard deadlines, and the maintenance worker)
//! can adopt them without re-measuring. Degradation is always toward
//! exact search or paused background work — correctness never degrades,
//! only performance.
//!
//! Single machine only. No distributed workloads, no replication.

use crate::VectorError;

/// Env var for the HTAP switch: `auto` (default) | `off` (never degrade;
/// benchmarks and over-provisioned hosts only).
pub const HTAP_ENV_VAR: &str = "PLOMID_VECTOR_HTAP";

/// Memory-pressure level, mirroring `plomid-memory` thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PressureLevel {
    /// Usage below 70%: full speed.
    #[default]
    Normal,
    /// Usage 70–85%: halve fan-out/oversampling.
    Elevated,
    /// Usage 85–95%: exact-leaning, background work degraded.
    High,
    /// Usage at/above 95%: shed load, pause background builds.
    Critical,
}

impl PressureLevel {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Elevated => "elevated",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }

    /// Map a `used/budget` ratio to a level (`NaN` → Normal, `Inf` → Critical,
    /// same fail-safe as the memory crate).
    #[must_use]
    pub fn from_usage(ratio: f64) -> Self {
        if !ratio.is_finite() {
            return if ratio.is_infinite() && ratio.is_sign_positive() {
                Self::Critical
            } else {
                Self::Normal
            };
        }
        if ratio >= 0.95 {
            Self::Critical
        } else if ratio >= 0.85 {
            Self::High
        } else if ratio >= 0.70 {
            Self::Elevated
        } else {
            Self::Normal
        }
    }
}

/// Background-work throttle for vector builds/maintenance/backups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Throttle {
    /// Run at the tiered concurrency.
    Full,
    /// Run single-file (concurrency 1, longer batches).
    Degraded,
    /// Do not start new background work (in-flight work drains).
    Paused,
}

impl Throttle {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Degraded => "degraded",
            Self::Paused => "paused",
        }
    }

    /// Throttle for background vector work under `pressure`.
    #[must_use]
    pub const fn for_pressure(pressure: PressureLevel) -> Self {
        match pressure {
            PressureLevel::Normal | PressureLevel::Elevated => Self::Full,
            PressureLevel::High => Self::Degraded,
            PressureLevel::Critical => Self::Paused,
        }
    }
}

/// Admission decision for one vector operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorAdmission {
    /// Proceed with `workers` threads.
    Admit {
        /// Worker threads granted.
        workers: usize,
    },
    /// Proceed exactly (no ANN/graph): correct, slower.
    DegradeExact {
        /// Human-readable reason.
        reason: String,
    },
    /// Refuse with a defined error (caller maps to `ResourceLimit`).
    Reject {
        /// Human-readable reason.
        reason: String,
    },
}

impl VectorAdmission {
    /// True when the operation may proceed (possibly degraded).
    #[must_use]
    pub const fn may_proceed(&self) -> bool {
        !matches!(self, Self::Reject { .. })
    }

    /// Record this decision in existing diagnostics.
    pub fn observe(&self, metrics: &crate::metrics::VectorMetrics) {
        match self {
            Self::Admit { .. } => {}
            Self::DegradeExact { .. } => metrics.observe_htap_decision(true, false),
            Self::Reject { .. } => metrics.observe_htap_decision(false, true),
        }
    }
}

/// Decide admission for one vector operation (pure function).
///
/// * `query_bytes`: caller estimate for this query (e.g. candidate scratch
///   `k * dim * 4 * oversample` via [`crate::options::memory_estimate_bytes`]).
/// * `build_bytes`: estimate for a requested index build (`0` = no build).
/// * `budget_bytes`: per-operation memory budget (tier-derived).
/// * `pressure`: current memory-pressure level.
/// * `active_builds`/`max_builds`: build single-flight accounting owned by
///   the caller (mirrors the executor maintenance-claim pattern).
/// * `htap_off`: `PLOMID_VECTOR_HTAP=off` (never degrade; benchmarks only).
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn admit(
    query_bytes: u64,
    build_bytes: u64,
    budget_bytes: u64,
    pressure: PressureLevel,
    active_builds: usize,
    max_builds: usize,
    htap_off: bool,
) -> VectorAdmission {
    if query_bytes > budget_bytes {
        return VectorAdmission::Reject {
            reason: format!(
                "query estimate {query_bytes} bytes exceeds budget {budget_bytes}: defined rejection, never uncontrolled growth"
            ),
        };
    }
    if build_bytes > 0 {
        if build_bytes > budget_bytes {
            return VectorAdmission::Reject {
                reason: format!("build estimate {build_bytes} bytes exceeds budget {budget_bytes}"),
            };
        }
        if active_builds >= max_builds.max(1) {
            return VectorAdmission::DegradeExact {
                reason: format!(
                    "build single-flight saturated ({active_builds} active): serve exact until a build slot frees"
                ),
            };
        }
        if pressure == PressureLevel::Critical && !htap_off {
            return VectorAdmission::DegradeExact {
                reason: "critical memory pressure: new builds paused, exact serves".to_string(),
            };
        }
    }
    if htap_off {
        return VectorAdmission::Admit {
            workers: usize::MAX,
        };
    }
    match pressure {
        PressureLevel::Normal => VectorAdmission::Admit {
            workers: usize::MAX,
        },
        PressureLevel::Elevated => VectorAdmission::Admit {
            workers: usize::MAX,
        },
        PressureLevel::High => VectorAdmission::DegradeExact {
            reason: "high memory pressure: exact fallback keeps results correct".to_string(),
        },
        PressureLevel::Critical => VectorAdmission::DegradeExact {
            reason: "critical memory pressure: exact fallback keeps results correct".to_string(),
        },
    }
}

/// Effective worker cap for vector dispatch under HTAP load.
///
/// `tier_workers` comes from [`crate::scale::ScaleDecision::workers`];
/// pressure derates it (elevated halves, high quarters with a floor of 1).
/// `usize::MAX` grants from [`admit`] mean "no HTAP cap" (tier decides).
#[must_use]
pub fn worker_cap(tier_workers: usize, granted: usize, pressure: PressureLevel) -> usize {
    let mut cap = tier_workers.max(1).min(granted);
    match pressure {
        PressureLevel::Normal => {}
        PressureLevel::Elevated => cap = (cap / 2).max(1),
        PressureLevel::High | PressureLevel::Critical => cap = (cap / 4).max(1),
    }
    cap.max(1)
}

/// Whether the HTAP degradation switch is disabled (`off`).
///
/// Unparsable values fail safe to enabled degradation (never silently `off`).
#[must_use]
pub fn htap_disabled() -> bool {
    match std::env::var(HTAP_ENV_VAR) {
        Err(_) => false,
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false" | "no" | "disabled"
        ),
    }
}

/// Cancellation predicate that fires after `deadline` (timeout reuse).
///
/// Returns a `Send + Sync` closure for [`crate::search::CancelFn`] slots:
/// timed-out work aborts with the existing `Cancelled` path and leaves no
/// corrupted state (heaps are caller-owned and simply dropped).
pub fn deadline_cancel_at(deadline: std::time::Instant) -> impl Fn() -> bool + Send + Sync {
    move || std::time::Instant::now() >= deadline
}

/// Latency summary for one side of a mixed workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySummary {
    /// Iterations timed.
    pub iters: u64,
    /// Median per-iteration latency, nanoseconds.
    pub p50_ns: u64,
    /// 95th percentile, nanoseconds.
    pub p95_ns: u64,
    /// 99th percentile, nanoseconds.
    pub p99_ns: u64,
}

/// Outcome of [`run_mixed`]: foreground latencies plus background volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MixedWorkloadReport {
    /// Foreground latency distribution.
    pub foreground: LatencySummary,
    /// Background operations completed while foreground ran.
    pub background_ops: u64,
    /// Wall-clock milliseconds for the whole run.
    pub elapsed_ms: u64,
}

/// Quantile over sorted nanosecond samples (`q` in `0.0..=1.0`).
#[must_use]
pub fn percentile_ns(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Run foreground work timed while background work floods another thread.
///
/// Both closures run `iters` times over `0..iters`; the background thread
/// is joined before returning. Measures CPU/cache interference between
/// two single-machine loads with no shared mutation required (callers pass
/// disjoint stores, or their own locks). Correctness assertions belong to
/// the caller: compare foreground results against the serial oracle.
pub fn run_mixed<F, B>(fg_iters: usize, bg_iters: usize, fg: F, bg: B) -> MixedWorkloadReport
where
    F: Fn(usize) + Send + Sync,
    B: Fn(usize) + Send,
{
    let start = std::time::Instant::now();
    let mut samples = Vec::with_capacity(fg_iters.max(1));
    std::thread::scope(|scope| {
        let bg_handle = scope.spawn(move || {
            let mut ops = 0u64;
            for i in 0..bg_iters {
                bg(i);
                ops += 1;
            }
            ops
        });
        for i in 0..fg_iters {
            let t = std::time::Instant::now();
            fg(i);
            samples.push(t.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        }
        let background_ops = bg_handle.join().unwrap_or(0);
        samples.sort_unstable();
        MixedWorkloadReport {
            foreground: LatencySummary {
                iters: fg_iters as u64,
                p50_ns: percentile_ns(&samples, 0.50),
                p95_ns: percentile_ns(&samples, 0.95),
                p99_ns: percentile_ns(&samples, 0.99),
            },
            background_ops,
            elapsed_ms: start.elapsed().as_millis() as u64,
        }
    })
}

/// HTAP policy diagnostics for existing admin/diagnostics export.
#[must_use]
pub fn explain_htap(
    pressure: PressureLevel,
    throttle: Throttle,
    admission: &VectorAdmission,
    tier: &crate::scale::ScaleDecision,
) -> Vec<String> {
    vec![
        format!("HTAP pressure: {}", pressure.as_str()),
        format!("Background throttle: {}", throttle.as_str()),
        format!(
            "Admission: {}",
            match admission {
                VectorAdmission::Admit { workers } => format!("admit (workers={workers})"),
                VectorAdmission::DegradeExact { reason } => format!("degrade-exact ({reason})"),
                VectorAdmission::Reject { reason } => format!("reject ({reason})"),
            }
        ),
        format!(
            "Tier: {} (workers={} build={} maint={})",
            tier.tier.as_str(),
            tier.workers,
            tier.build_concurrency,
            tier.maintenance_concurrency
        ),
        "Scope: single-machine interference only (no distributed workloads)".to_string(),
    ]
}

/// Map an admission refusal to the defined resource error.
#[must_use]
pub fn admission_error(reason: &str) -> VectorError {
    VectorError::ResourceLimit(format!("htap admission refused: {reason}"))
}
