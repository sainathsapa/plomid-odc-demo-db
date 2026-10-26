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
//! Single-machine scale tiers (Gap L).
//!
//! Five *engineering* tiers describe one machine's operating range — not
//! marketing labels, not node counts. Each tier carries measured worker,
//! memory, batch, cache, mmap, maintenance, and planner assumptions, and
//! explicit operator configuration always overrides automatic detection.
//!
//! Tiers build on the existing `plomid_hw` budgets (`worker_budget`,
//! `RuntimeProfile` accessors) rather than duplicating them: this module
//! maps measured hardware + dataset size onto the five vector operating
//! points and exposes the derived knobs the storage, NUMA, and planner
//! paths consume.
//!
//! Single machine only: there is no cluster/node-count tier here.

use crate::{Result, VectorError};

/// Env var for the tier override: `auto` (default) or a tier name.
pub const SCALE_ENV_VAR: &str = "PLOMID_VECTOR_TIER";

/// Single-machine operating tier (engineering ranges, not marketing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ScaleTier {
    /// Developer laptop (≤ 4 cores or ≤ 8 GiB RAM class).
    #[default]
    Laptop,
    /// Developer workstation (mid cores, mid RAM, fast local SSD).
    Workstation,
    /// Small server (rack/single-socket, modest RAM, always-on).
    SmallServer,
    /// Production server (many cores, large RAM, fast storage).
    ProductionServer,
    /// Large-memory / high-I/O single server (huge RAM and/or NVMe).
    LargeMemoryHighIo,
}

impl ScaleTier {
    /// Stable lowercase name for config / logs / diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Laptop => "laptop",
            Self::Workstation => "workstation",
            Self::SmallServer => "small_server",
            Self::ProductionServer => "production_server",
            Self::LargeMemoryHighIo => "large_memory_high_io",
        }
    }

    /// Parse a tier name (case-insensitive, `-`/space tolerant).
    pub fn parse(s: &str) -> Result<Self> {
        let norm: String = s
            .trim()
            .to_ascii_lowercase()
            .chars()
            .map(|c| if c == '-' || c == ' ' { '_' } else { c })
            .collect();
        match norm.as_str() {
            "laptop" | "dev" | "developer" => Ok(Self::Laptop),
            "workstation" | "work_station" | "ws" => Ok(Self::Workstation),
            "small_server" | "smallserver" | "small" => Ok(Self::SmallServer),
            "production_server" | "productionserver" | "production" | "server" => {
                Ok(Self::ProductionServer)
            }
            "large_memory_high_io" | "large" | "high_io" | "highio" | "big" => {
                Ok(Self::LargeMemoryHighIo)
            }
            other => Err(VectorError::IndexParameterInvalid(format!(
                "unknown scale tier '{other}': expected laptop|workstation|small_server|production_server|large_memory_high_io"
            ))),
        }
    }
}

/// Measured inputs for tier selection (one machine + one dataset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScaleInputs {
    /// Logical CPUs visible to this process.
    pub cpus: usize,
    /// Total RAM in bytes (`None` = unmeasurable; never assumed large).
    pub ram_bytes: Option<u64>,
    /// Storage class behind the data directory.
    pub storage_class: crate::storage::StorageClass,
    /// Estimated dataset/index bytes (drives mmap + cache reasoning).
    pub dataset_bytes: u64,
    /// Expected concurrent vector queries.
    pub concurrency: usize,
}

impl ScaleInputs {
    /// Detect inputs on this machine (`dataset_bytes`/`concurrency` supplied
    /// by the caller; hardware measured, never guessed).
    #[must_use]
    pub fn detect(dataset_bytes: u64, concurrency: usize) -> Self {
        let hw = plomid_hw::HardwareProfile::detect();
        Self {
            cpus: hw.logical_cpus.max(1),
            ram_bytes: hw.ram_total_bytes,
            storage_class: crate::storage::StorageClass::from_hw(hw.storage),
            dataset_bytes,
            concurrency: concurrency.max(1),
        }
    }

    /// Deterministic test seam.
    #[must_use]
    pub const fn test_seam(
        cpus: usize,
        ram_bytes: Option<u64>,
        storage_class: crate::storage::StorageClass,
        dataset_bytes: u64,
        concurrency: usize,
    ) -> Self {
        Self {
            cpus,
            ram_bytes,
            storage_class,
            dataset_bytes,
            concurrency,
        }
    }
}

/// Explicit operator override (wins over automatic selection).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScaleConfig {
    /// Pinned tier (`None` = automatic).
    pub override_tier: Option<ScaleTier>,
}

impl ScaleConfig {
    /// Read [`SCALE_ENV_VAR`]: unset/`auto` = automatic; unparsable fails
    /// closed (never silently auto).
    pub fn from_env() -> Result<Self> {
        match std::env::var(SCALE_ENV_VAR) {
            Err(std::env::VarError::NotPresent) => Ok(Self {
                override_tier: None,
            }),
            Err(std::env::VarError::NotUnicode(_)) => Err(VectorError::IndexParameterInvalid(
                format!("{SCALE_ENV_VAR} is not valid UTF-8"),
            )),
            Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "" | "auto" => Ok(Self {
                    override_tier: None,
                }),
                other => Ok(Self {
                    override_tier: Some(ScaleTier::parse(other)?),
                }),
            },
        }
    }

    /// Explicit constructor (tests / embedding).
    #[must_use]
    pub const fn explicit(override_tier: Option<ScaleTier>) -> Self {
        Self { override_tier }
    }
}

/// Selected tier plus the derived policy knobs and the reason.
#[derive(Debug, Clone, PartialEq)]
pub struct ScaleDecision {
    /// Selected tier.
    pub tier: ScaleTier,
    /// Why this tier (deterministic per inputs).
    pub reason: String,
    /// Whether an explicit override won over detection.
    pub overridden: bool,
    /// Default worker threads for vector dispatch.
    pub workers: usize,
    /// Default batch rows for scans/builds.
    pub batch_rows: usize,
    /// Per-index memory budget in bytes.
    pub memory_budget_bytes: u64,
    /// Index-build concurrency.
    pub build_concurrency: usize,
    /// Maintenance concurrency.
    pub maintenance_concurrency: usize,
    /// Whether mmap is preferred for large indexes on this tier.
    pub mmap_preferred: bool,
    /// Cache capacity hint in bytes.
    pub cache_bytes: u64,
}

impl ScaleDecision {
    /// Select a tier from measured inputs + explicit config.
    ///
    /// Pure and deterministic: the same inputs always yield the same tier
    /// and knobs. Explicit configuration wins; otherwise CPU/RAM/storage/
    /// dataset/concurrency thresholds decide (see `reason`).
    #[must_use]
    pub fn select(inputs: &ScaleInputs, config: &ScaleConfig) -> Self {
        if let Some(pinned) = config.override_tier {
            return Self::for_tier(
                pinned,
                inputs,
                format!("explicit override {SCALE_ENV_VAR}"),
                true,
            );
        }
        let tier = classify(inputs);
        let reason = format!(
            "auto: {} cpus, {} ram, {} storage, {} dataset bytes, {} concurrency",
            inputs.cpus,
            inputs
                .ram_bytes
                .map_or_else(|| "unknown".to_string(), |r| format!("{r} bytes")),
            inputs.storage_class.as_str(),
            inputs.dataset_bytes,
            inputs.concurrency,
        );
        Self::for_tier(tier, inputs, reason, false)
    }

    fn for_tier(tier: ScaleTier, inputs: &ScaleInputs, reason: String, overridden: bool) -> Self {
        let cpus = inputs.cpus.max(1);
        let ram = inputs.ram_bytes.unwrap_or(8 * 1024 * 1024 * 1024);
        let (workers, batch_rows, mem_frac, build, maint, mmap, cache_frac) = match tier {
            ScaleTier::Laptop => (cpus.min(4), 4096usize, 16u64, 1usize, 1usize, false, 32u64),
            ScaleTier::Workstation => (
                cpus.min(8),
                8192,
                8,
                2,
                1,
                inputs.storage_class.random_access_cheap(),
                16,
            ),
            ScaleTier::SmallServer => (
                cpus.min(16),
                8192,
                8,
                4,
                2,
                inputs.storage_class.random_access_cheap(),
                8,
            ),
            ScaleTier::ProductionServer => (
                cpus.min(64),
                16_384,
                4,
                8.min(cpus),
                4.min(cpus),
                inputs.storage_class != crate::storage::StorageClass::Hdd,
                4,
            ),
            ScaleTier::LargeMemoryHighIo => (
                cpus.min(128),
                32_768,
                2,
                16.min(cpus.max(1)),
                8.min(cpus.max(1)),
                true,
                2,
            ),
        };
        let memory_budget_bytes = (ram / mem_frac).max(64 * 1024 * 1024);
        let cache_bytes = (ram / cache_frac).max(32 * 1024 * 1024);
        Self {
            tier,
            reason,
            overridden,
            workers: workers.max(1),
            batch_rows,
            memory_budget_bytes,
            build_concurrency: build.max(1),
            maintenance_concurrency: maint.max(1),
            mmap_preferred: mmap,
            cache_bytes,
        }
    }

    /// Build a storage policy consistent with this tier (mmap preference +
    /// tier memory budget). Explicit caller policy still wins field-by-field;
    /// this is the default the tier recommends.
    #[must_use]
    pub fn storage_policy(&self) -> crate::storage::VectorStoragePolicy {
        crate::storage::VectorStoragePolicy {
            request: crate::storage::StorageModeRequest::Auto,
            mmap: crate::storage::MmapConfig {
                enabled: self.mmap_preferred,
                ..crate::storage::MmapConfig::default()
            },
            memory_budget_bytes: self.memory_budget_bytes,
        }
    }

    /// Worker count for a NUMA topology under this tier (tier cap wins over
    /// raw topology width on small tiers).
    #[must_use]
    pub fn numa_workers(&self, topology_cpus: usize, max_threads: usize) -> usize {
        let want = if max_threads == 0 {
            topology_cpus.max(1)
        } else {
            max_threads
        };
        want.min(self.workers).max(1)
    }

    /// Diagnostic lines (tier + reason observable).
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        vec![
            format!("Scale tier: {}", self.tier.as_str()),
            format!("Reason: {}", self.reason),
            format!("Overridden: {}", self.overridden),
            format!("Workers: {}", self.workers),
            format!("Batch rows: {}", self.batch_rows),
            format!("Memory budget: {} bytes", self.memory_budget_bytes),
            format!("Build concurrency: {}", self.build_concurrency),
            format!("Maintenance concurrency: {}", self.maintenance_concurrency),
            format!("Mmap preferred: {}", self.mmap_preferred),
            format!("Cache hint: {} bytes", self.cache_bytes),
        ]
    }
}

fn classify(inputs: &ScaleInputs) -> ScaleTier {
    const GIB: u64 = 1024 * 1024 * 1024;
    let ram = inputs.ram_bytes.unwrap_or(0);
    let fast = inputs.storage_class.random_access_cheap()
        || inputs.storage_class == crate::storage::StorageClass::Nvme;
    // Large-memory / high-I/O: huge RAM or (NVMe + sizable dataset).
    if ram >= 128 * GIB
        || (inputs.storage_class == crate::storage::StorageClass::Nvme
            && (ram >= 64 * GIB || inputs.dataset_bytes >= 64 * GIB))
    {
        return ScaleTier::LargeMemoryHighIo;
    }
    // Production server: many cores and large RAM together.
    if inputs.cpus >= 16 && ram >= 32 * GIB {
        return ScaleTier::ProductionServer;
    }
    // Small server: server-ish shape without production scale, or explicit
    // always-on concurrency hint.
    if (inputs.cpus >= 8 && ram >= 16 * GIB) || inputs.concurrency >= 16 {
        return ScaleTier::SmallServer;
    }
    // Workstation: mid cores/RAM with fast storage.
    if (inputs.cpus >= 6 && ram >= 8 * GIB) || (inputs.cpus >= 4 && fast && ram >= 8 * GIB) {
        return ScaleTier::Workstation;
    }
    ScaleTier::Laptop
}
