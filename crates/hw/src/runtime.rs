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

//! Runtime tier classification and tier-derived budgets.
//!
//! `HardwareProfile::detect()` measures the host; this module turns that
//! measurement into one coarse [`RuntimeTier`] (`Edge` | `Laptop` | `Server`)
//! plus the small set of derived knobs the rest of the system consumes.
//! Agents B (buffer pool) and C (maintenance) read **only** the accessors on
//! [`RuntimeProfile`] — the tier enum itself is just the selector.
//!
//! # Tier table
//!
//! | Tier   | Condition (evaluated in order)                                   |
//! |--------|------------------------------------------------------------------|
//! | Edge   | `HardwareProfile::is_small_device()`: ≤ 4 logical CPUs **and** known RAM ≤ 4 GiB (Pi-class) |
//! | Server | ≥ 16 logical CPUs **and** known RAM ≥ 32 GiB                    |
//! | Laptop | everything else, including unknown RAM (safe middle default)     |
//!
//! Edge and Server are disjoint (≤ 4 vs ≥ 16 cores), so evaluation order
//! between them is irrelevant. Unknown RAM never yields Edge
//! (`is_small_device()` is false) and never yields Server (the RAM gate
//! requires a known value), so unmeasurable hosts land on Laptop.
//!
//! # Override
//!
//! `PLOMID_PROFILE=edge|laptop|server|auto` (case-insensitive, `auto` =
//! detect) or the server `--profile` flag, which wins over the env var.
//! Invalid values are an error, never a silent fallback.
//!
//! # Budgets
//!
//! Every accessor is a pure function of already-detected fields
//! (`worker_budget()`, `batch_rows()`, RAM, storage kind) — no new syscalls,
//! so classification can run once at startup and the values stay valid.

use std::fmt;

use crate::profile::{HardwareProfile, StorageKind};

/// Name of the environment variable that overrides tier detection.
pub const PROFILE_ENV_VAR: &str = "PLOMID_PROFILE";

/// Server-class gate: minimum logical CPUs.
pub const SERVER_MIN_CPUS: usize = 16;
/// Server-class gate: minimum known RAM (32 GiB).
pub const SERVER_MIN_RAM_BYTES: u64 = 32 * 1024 * 1024 * 1024;

/// Minimum interval between live limit re-reads (see
/// [`RuntimeProfile::recheck`]). One `Instant` check per admission when idle;
/// at most one sysfs/proc re-read per interval when busy. Chosen so a k8s
/// VPA resize or hot-plugged CPUs apply within a minute without adding a
/// background thread or per-query syscall burden.
pub const RECHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Coarse host class. Selected from measured fields, never guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTier {
    /// Pi-class: ≤ 4 cores and ≤ 4 GiB RAM.
    Edge,
    /// Default: developer laptop / container / anything unmeasurable.
    Laptop,
    /// ≥ 16 cores and ≥ 32 GiB RAM.
    Server,
}

impl RuntimeTier {
    /// Machine name used in the env var, CLI flag, and logs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Edge => "edge",
            Self::Laptop => "laptop",
            Self::Server => "server",
        }
    }

    /// Parse an override value. `Ok(None)` means "auto" (detect from the
    /// host); `Err` names the accepted values. Case-insensitive, padded
    /// whitespace and empty strings are treated as auto.
    pub fn parse_override(raw: &str) -> Result<Option<Self>, String> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(None),
            "edge" => Ok(Some(Self::Edge)),
            "laptop" => Ok(Some(Self::Laptop)),
            "server" => Ok(Some(Self::Server)),
            other => Err(format!(
                "invalid runtime profile '{other}': expected edge|laptop|server|auto"
            )),
        }
    }

    /// Classify already-measured hardware. Pure: no syscalls, no env reads.
    #[must_use]
    pub fn classify(hardware: &HardwareProfile) -> Self {
        if hardware.is_small_device() {
            return Self::Edge;
        }
        let big_iron = hardware.logical_cpus >= SERVER_MIN_CPUS
            && hardware
                .ram_total_bytes
                .is_some_and(|ram| ram >= SERVER_MIN_RAM_BYTES);
        if big_iron {
            return Self::Server;
        }
        Self::Laptop
    }
}

impl fmt::Display for RuntimeTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Where the winning tier came from (for the one startup log line).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileSource {
    /// Measured from the host.
    Auto,
    /// `PLOMID_PROFILE` env var.
    Env,
    /// `--profile` CLI flag (wins over env).
    Cli,
    /// Explicit constructor (tests / embedding).
    Explicit,
}

impl fmt::Display for ProfileSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Env => write!(f, "env"),
            Self::Cli => write!(f, "cli"),
            Self::Explicit => write!(f, "explicit"),
        }
    }
}

/// Measured hardware plus its tier plus tier-derived budgets.
///
/// Resolve once at startup via [`RuntimeProfile::resolve`]; share by value
/// (`Clone`) or `Arc`. All budget accessors are pure functions of the
/// captured [`HardwareProfile`].
#[derive(Debug, Clone)]
pub struct RuntimeProfile {
    tier: RuntimeTier,
    hardware: HardwareProfile,
    source: ProfileSource,
}

impl RuntimeProfile {
    /// Detect from the host, ignoring any override (env still wins in
    /// [`Self::resolve`]).
    #[must_use]
    pub fn detect() -> Self {
        Self::from_hardware(HardwareProfile::detect(), ProfileSource::Auto)
    }

    /// Classify an already-measured profile. Pure; the seam unit tests use.
    #[must_use]
    pub fn from_hardware(hardware: HardwareProfile, source: ProfileSource) -> Self {
        let tier = RuntimeTier::classify(&hardware);
        Self::with_tier(tier, hardware, source)
    }

    /// Explicit constructor for tests and embedding: pins the tier instead
    /// of classifying. The hardware is still carried for the accessors that
    /// derive from measured fields.
    #[must_use]
    pub fn with_tier(tier: RuntimeTier, hardware: HardwareProfile, source: ProfileSource) -> Self {
        Self {
            tier,
            hardware,
            source,
        }
    }

    /// Read `PLOMID_PROFILE`. `Ok(None)` = unset or `auto` (detect);
    /// `Err` = set but unparsable (fail closed, never silently auto).
    pub fn override_from_env() -> Result<Option<RuntimeTier>, String> {
        match std::env::var(PROFILE_ENV_VAR) {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(format!(
                "{PROFILE_ENV_VAR} is not valid UTF-8: expected edge|laptop|server|auto"
            )),
            Ok(raw) => {
                RuntimeTier::parse_override(&raw).map_err(|e| format!("{PROFILE_ENV_VAR}: {e}"))
            }
        }
    }

    /// Lightweight live re-detection for a running process (k8s VPA,
    /// container resize, hot-plugged CPUs).
    ///
    /// Only re-reads the volatile limits
    /// ([`HardwareProfile::refresh_dynamic_limits`]: logical CPUs + RAM —
    /// sysfs/proc numbers, no topology walks, no heavy work). Stable fields
    /// (arch, features, caches, storage, NUMA) stay as detected at startup.
    ///
    /// Tier policy (explicit overrides always win, per axis):
    /// * Startup `Auto` + env unset/`auto`: re-classifies from the fresh
    ///   hardware, so Edge↔Server flips apply to new admissions.
    /// * Startup `Auto` + env now pinned: adopts the pin once (source
    ///   becomes `Env`) and disables further auto-shift.
    /// * Startup `Env` + env still pinned: follows the latest env value
    ///   (an operator change mid-run wins); env removed resumes auto.
    /// * Startup `Cli` / `Explicit` (server `--profile` flag, embedding,
    ///   tests): the tier stays pinned forever — the flag wins over any env
    ///   change — while the hardware (and thus the elastic cap) still
    ///   refreshes underneath.
    /// * Unparsable env mid-run fails safe to the Laptop tier with one
    ///   stderr warning on change (CLI/explicit pins are unaffected); fixing
    ///   the value resumes the policy above.
    ///
    /// Returns `true` when the tier changed (callers use it to gate log
    /// lines; hardware-only movement still matters for elastic caps even
    /// when this returns `false`).
    pub fn recheck(&mut self) -> bool {
        self.hardware.refresh_dynamic_limits();
        self.apply_tier_policy()
    }

    /// Test seam for live-rescale tests: swaps in mock hardware and applies
    /// the same pinned-vs-auto tier policy as [`Self::recheck`], but without
    /// sysfs reads and without env reads (hermetic under parallel tests).
    /// `Auto` sources re-classify; `Cli`/`Env`/`Explicit` pins keep their
    /// tier while the hardware still refreshes underneath.
    pub fn recheck_with(&mut self, hardware: HardwareProfile) -> bool {
        self.hardware = hardware;
        match self.source {
            ProfileSource::Auto => {
                let next = RuntimeTier::classify(&self.hardware);
                let changed = next != self.tier;
                self.tier = next;
                changed
            }
            ProfileSource::Cli | ProfileSource::Env | ProfileSource::Explicit => false,
        }
    }

    /// Shared tier-policy step behind [`Self::recheck`]: re-reads the env
    /// override and updates `(tier, source)` per the documented pin rules.
    /// Returns whether the tier changed.
    fn apply_tier_policy(&mut self) -> bool {
        match Self::override_from_env() {
            Ok(Some(pinned)) => match self.source {
                ProfileSource::Auto => {
                    let changed = self.tier != pinned;
                    self.tier = pinned;
                    self.source = ProfileSource::Env;
                    changed
                }
                ProfileSource::Env => {
                    let changed = self.tier != pinned;
                    self.tier = pinned;
                    changed
                }
                // `--profile` / explicit pins win over any env change: the
                // hardware above already refreshed, the tier stays.
                ProfileSource::Cli | ProfileSource::Explicit => false,
            },
            Ok(None) => match self.source {
                ProfileSource::Auto => {
                    let next = RuntimeTier::classify(&self.hardware);
                    let changed = next != self.tier;
                    self.tier = next;
                    changed
                }
                ProfileSource::Env => {
                    // Operator removed the override mid-run: resume auto.
                    let next = RuntimeTier::classify(&self.hardware);
                    let changed = next != self.tier;
                    self.tier = next;
                    self.source = ProfileSource::Auto;
                    changed
                }
                ProfileSource::Cli | ProfileSource::Explicit => false,
            },
            Err(reason) => match self.source {
                ProfileSource::Auto | ProfileSource::Env => {
                    let changed = self.tier != RuntimeTier::Laptop;
                    if changed {
                        eprintln!("warning: {reason}; falling back to the laptop tier");
                    }
                    self.tier = RuntimeTier::Laptop;
                    changed
                }
                ProfileSource::Cli | ProfileSource::Explicit => false,
            },
        }
    }

    /// Resolve with a CLI value that wins over the env var that wins over
    /// detection. Pure apart from the env read; [`Self::resolve_with`]
    /// takes both inputs explicitly for tests.
    pub fn resolve(cli: Option<RuntimeTier>) -> Result<Self, String> {
        Ok(Self::resolve_with(cli, Self::override_from_env()?))
    }

    /// Pure resolution given an explicit env value (see [`Self::resolve`]).
    #[must_use]
    pub fn resolve_with(cli: Option<RuntimeTier>, env: Option<RuntimeTier>) -> Self {
        if let Some(tier) = cli {
            return Self::with_tier(tier, HardwareProfile::detect(), ProfileSource::Cli);
        }
        if let Some(tier) = env {
            return Self::with_tier(tier, HardwareProfile::detect(), ProfileSource::Env);
        }
        Self::detect()
    }

    /// The classified (or overridden) tier.
    #[must_use]
    pub fn tier(&self) -> RuntimeTier {
        self.tier
    }

    /// Where the tier came from.
    #[must_use]
    pub fn source(&self) -> ProfileSource {
        self.source
    }

    /// The underlying measurement (for tier-aware code that needs raw fields).
    #[must_use]
    pub fn hardware(&self) -> &HardwareProfile {
        &self.hardware
    }

    /// Worker permits for data-parallel / background work. Derived from the
    /// measured `worker_budget()` (up to 256 on big iron, RAM-clamped):
    /// Edge caps at 2 to leave headroom on tiny boxes, Laptop caps at 8
    /// (interactive latency over throughput), Server takes the full budget
    /// so 128+ core machines fan out past the old 64 rail.
    #[must_use]
    pub fn worker_permits(&self) -> usize {
        let budget = self.hardware.worker_budget();
        match self.tier {
            RuntimeTier::Edge => budget.min(2),
            RuntimeTier::Laptop => budget.min(8),
            RuntimeTier::Server => budget,
        }
    }

    /// Vector batch size in rows. The L2-derived `batch_rows()` already
    /// fits the cache, so only Edge shrinks it (halved, floored at 1024) to
    /// cut resident memory on tiny boxes; Laptop/Server keep the L2 fit.
    #[must_use]
    pub fn vector_batch_rows(&self) -> usize {
        let base = self.hardware.batch_rows();
        match self.tier {
            RuntimeTier::Edge => (base / 2).max(1024),
            RuntimeTier::Laptop | RuntimeTier::Server => base,
        }
    }

    /// Total buffer/memory budget in bytes for tier-sized components (Agent B
    /// sizes the pool from this via [`Self::pool_capacity_pages`]). Tier cap
    /// — Edge 64 MiB, Laptop 256 MiB, Server 1 GiB — further limited to
    /// RAM/8 when RAM is known (floored at 1 MiB so degenerate readings stay
    /// usable); unknown RAM yields the cap.
    #[must_use]
    pub fn buffer_capacity_bytes(&self) -> u64 {
        let cap = match self.tier {
            RuntimeTier::Edge => 64 * 1024 * 1024,
            RuntimeTier::Laptop => 256 * 1024 * 1024,
            RuntimeTier::Server => 1024 * 1024 * 1024,
        };
        match self.hardware.ram_total_bytes {
            None => cap,
            Some(ram) => cap.min((ram / 8).max(1024 * 1024)),
        }
    }

    /// Buffer-pool capacity in 16 KiB pages (mirrors
    /// `plomid_core::PAGE_SIZE_BYTES`; kept local so this crate stays
    /// dependency-free). Sized as ~1/16th of [`Self::buffer_capacity_bytes`]
    /// — the pool is one consumer of the budget, not all of it — clamped to
    /// [64, 65536] pages. A 16 GiB laptop lands on exactly 1024 pages, the
    /// historical server default.
    #[must_use]
    pub fn pool_capacity_pages(&self) -> usize {
        const PAGE_BYTES: u64 = 16 * 1024;
        ((self.buffer_capacity_bytes() / 16 / PAGE_BYTES) as usize).clamp(64, 65_536)
    }

    /// Whether tier-sized components should prefer compression. Edge (slow
    /// flash, tiny disks) and Server (capacity at scale, plentiful CPU)
    /// compress; Laptop favors latency except on HDDs, where I/O always wins.
    #[must_use]
    pub fn prefer_compression(&self) -> bool {
        match self.tier {
            RuntimeTier::Edge | RuntimeTier::Server => true,
            RuntimeTier::Laptop => self.hardware.storage == StorageKind::Hdd,
        }
    }

    /// Multiplier applied to base maintenance intervals **and** base
    /// mutation-count thresholds (Agent C). > 1 = less frequent (for
    /// constrained devices), < 1 = more aggressive. Edge 2.0, Laptop 1.0,
    /// Server 0.5.
    #[must_use]
    pub fn maintenance_scale(&self) -> f64 {
        match self.tier {
            RuntimeTier::Edge => 2.0,
            RuntimeTier::Laptop => 1.0,
            RuntimeTier::Server => 0.5,
        }
    }

    /// Minimum row count before taking a data-parallel path. Derived from
    /// [`Self::worker_permits`]: fewer workers must see more rows to amortize
    /// fan-out. Typical: Edge(2 permits) → 65536, Laptop(8) → 16384,
    /// Server(32) → 4096; clamped to [1024, 1M] (very big iron bottoms out
    /// at the 1024 floor).
    #[must_use]
    pub fn parallel_row_threshold(&self) -> usize {
        (131_072 / self.worker_permits().max(1)).clamp(1024, 1_048_576)
    }
}
