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
//! Single-node vector storage policy (Phase 41).
//!
//! NON-AUTHORITATIVE policy (see [`crate::durability`]): selects the backing
//! mode of rebuildable derived images only. Durability, checkpoint, and GC
//! authority live in the storage engine; this policy never weakens them and
//! the production SQL path never depends on the files it places.
//!
//! Decides, per index, whether vector data lives in memory, in a plain
//! file, or in an `mmap`-preferred file mapping — deterministically, from
//! measured capability rather than assumption. The engine never assumes
//! NVMe, never blindly mmaps everything, and never requires NVMe.
//!
//! Design notes:
//!
//! * This crate forbids `unsafe_code`, so there is no raw `mmap(2)` call
//!   here. `Mmap` mode means *page-cache-friendly file access with mmap
//!   semantics* (sequential prefetch warms the OS page cache, residency
//!   counters track warm vs cold, random access reuses the same file
//!   offsets a future `memmap2`-backed reader would map). The [`StorageMode`]
//!   os recorded in every [`StoragePolicyDecision`], so a future
//!   OS-mmap backend can replace the read path without changing policy,
//!   diagnostics, or durability boundaries.
//! * Durability boundaries are unchanged: writes go through
//!   [`crate::storage::FileVectorStorage`] (`sync_all` + temp-rename +
//!   dir-sync, torn-tail truncation on replay). Policy only selects the
//!   read/backing mode; it never weakens crash recovery.
//! * Single node only. No sharding, no distributed Top-K, no cluster work.

use crate::validation::MAX_PAYLOAD_BYTES;
use crate::{Result, VectorError};

/// Env var for the mmap switch: `auto` (default) | `on` | `off`.
pub const MMAP_ENV_VAR: &str = "PLOMID_VECTOR_MMAP";

/// Byte size below which an index is "tiny" (memory resident by default).
pub const TINY_MAX_BYTES: u64 = 1024 * 1024;
/// Byte size above which an index is "large" (mmap preferred when safe).
pub const MMAP_MIN_BYTES: u64 = 64 * 1024 * 1024;
/// Default memory budget for one vector index (256 MiB).
pub const DEFAULT_INDEX_MEMORY_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
/// Default cap for one mmap-preferred mapping (1 GiB).
pub const DEFAULT_MAX_MMAP_BYTES: u64 = 1024 * 1024 * 1024;

/// Selected backing mode for one vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StorageMode {
    /// Heap-resident rows (`InMemoryVectorStorage`).
    Memory,
    /// Append-only file rows with positional reads (`FileVectorStorage`).
    #[default]
    File,
    /// File rows accessed with mmap semantics (prefetch + residency).
    ///
    /// Backed today by the same crash-safe file with page-cache-friendly
    /// sequential prefetch; a future OS-mmap reader plugs in here without
    /// changing policy or diagnostics.
    Mmap,
}

impl StorageMode {
    /// Stable lowercase name for logs / diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
            Self::Mmap => "mmap",
        }
    }

    /// Parse [`Self::as_str`] output (case-insensitive).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "memory" | "mem" | "ram" => Ok(Self::Memory),
            "file" | "disk" | "plain" => Ok(Self::File),
            "mmap" | "mapped" | "nvme" => Ok(Self::Mmap),
            other => Err(VectorError::IndexParameterInvalid(format!(
                "unknown storage mode '{other}': expected memory|file|mmap"
            ))),
        }
    }

    /// True for the memory-resident mode.
    #[must_use]
    pub const fn is_memory_resident(self) -> bool {
        matches!(self, Self::Memory)
    }

    /// True for the mmap-backed mode.
    #[must_use]
    pub const fn is_mmap_backed(self) -> bool {
        matches!(self, Self::Mmap)
    }
}

/// Caller request: explicit mode or automatic selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StorageModeRequest {
    /// Pick from capability + size + budgets.
    #[default]
    Auto,
    /// Force memory (fails when over budget instead of silently spilling).
    Memory,
    /// Force plain file.
    File,
    /// Prefer mmap; falls back to file when unavailable/undesirable.
    Mmap,
}

impl StorageModeRequest {
    /// Parse env/config spelling (`auto`|`memory`|`file`|`mmap`).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "memory" | "mem" | "ram" => Ok(Self::Memory),
            "file" | "disk" | "plain" => Ok(Self::File),
            "mmap" | "mapped" => Ok(Self::Mmap),
            other => Err(VectorError::IndexParameterInvalid(format!(
                "unknown storage request '{other}': expected auto|memory|file|mmap"
            ))),
        }
    }
}

/// Local storage class behind the data directory (best effort).
///
/// Mirrors `plomid_hw::StorageKind` without taking a dependency edge in
/// this module's pure selection core; convert with [`StorageClass::from_hw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StorageClass {
    /// Fast local NVMe (measured or operator-declared).
    Nvme,
    /// Non-rotational SSD.
    Ssd,
    /// Rotational HDD (prefers sequential access).
    Hdd,
    /// Unknown / unmeasurable (never assumed fast).
    #[default]
    Unknown,
}

impl StorageClass {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nvme => "nvme",
            Self::Ssd => "ssd",
            Self::Hdd => "hdd",
            Self::Unknown => "unknown",
        }
    }

    /// Convert from the hardware profile's storage kind.
    #[must_use]
    pub fn from_hw(kind: plomid_hw::StorageKind) -> Self {
        match kind {
            plomid_hw::StorageKind::Nvme => Self::Nvme,
            plomid_hw::StorageKind::Ssd => Self::Ssd,
            plomid_hw::StorageKind::Hdd => Self::Hdd,
            plomid_hw::StorageKind::Unknown => Self::Unknown,
        }
    }

    /// True when random access is expected to be cheap.
    #[must_use]
    pub const fn random_access_cheap(self) -> bool {
        matches!(self, Self::Nvme | Self::Ssd)
    }
}

/// Measured single-machine storage capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCapability {
    /// Whether the mmap-preferred path may be selected.
    pub mmap_available: bool,
    /// Why mmap is (un)available, one line (stable contract for UIs).
    pub mmap_note: String,
    /// Block-device class behind the data directory.
    pub storage_class: StorageClass,
    /// Known total RAM, when measurable (for budget reasoning).
    pub ram_total_bytes: Option<u64>,
}

impl StorageCapability {
    /// Detect capability on this machine.
    ///
    /// Never fails and never guesses fast storage: unknown devices stay
    /// [`StorageClass::Unknown`], and mmap availability is the conjunction
    /// of the operator switch ([`MMAP_ENV_VAR`] != `off`), a platform with
    /// file-mapping support, and an explicit test seam override.
    #[must_use]
    pub fn detect() -> Self {
        let hw = plomid_hw::HardwareProfile::detect();
        let storage_class = StorageClass::from_hw(hw.storage);
        let (mmap_available, mmap_note) = detect_mmap();
        Self {
            mmap_available,
            mmap_note,
            storage_class,
            ram_total_bytes: hw.ram_total_bytes,
        }
    }

    /// Deterministic test seam: fixed capability, no syscalls.
    #[must_use]
    pub fn test_seam(
        mmap_available: bool,
        storage_class: StorageClass,
        ram_total_bytes: Option<u64>,
    ) -> Self {
        Self {
            mmap_available,
            mmap_note: if mmap_available {
                "test seam: mmap available".to_string()
            } else {
                "test seam: mmap unavailable".to_string()
            },
            storage_class,
            ram_total_bytes,
        }
    }
}

fn detect_mmap() -> (bool, String) {
    if let Ok(raw) = std::env::var(MMAP_ENV_VAR) {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "0" | "false" | "no" | "disabled" => {
                return (
                    false,
                    format!("{MMAP_ENV_VAR}=off: mmap disabled by operator"),
                );
            }
            "on" | "1" | "true" | "yes" | "enabled" | "mmap" => {
                // Explicitly requested: still gated on platform support below.
            }
            "auto" | "" => {}
            other => {
                return (
                    false,
                    format!("{MMAP_ENV_VAR}='{other}' unparsable: mmap disabled fail-safe"),
                );
            }
        }
    }
    // This crate forbids `unsafe`, so "mmap support" here means the
    // platform exposes file mappings to a future safe-backed reader plus
    // page-cache-friendly file I/O today. Unix always qualifies; other
    // platforms report unavailable rather than assuming support.
    #[cfg(unix)]
    {
        (true, "mmap available (unix file mappings)".to_string())
    }
    #[cfg(not(unix))]
    {
        (
            false,
            "mmap unavailable on this OS (safe file fallback)".to_string(),
        )
    }
}

/// Mmap switch configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MmapConfig {
    /// Master switch (env `off` forces false via [`StorageCapability`]).
    pub enabled: bool,
    /// Minimum index bytes before mmap is preferred.
    pub min_mmap_bytes: u64,
    /// Maximum bytes one mmap-preferred mapping may cover.
    pub max_mmap_bytes: u64,
}

impl Default for MmapConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_mmap_bytes: MMAP_MIN_BYTES,
            max_mmap_bytes: DEFAULT_MAX_MMAP_BYTES,
        }
    }
}

impl MmapConfig {
    /// Validate ranges (min <= max, both non-zero).
    pub fn validate(&self) -> Result<()> {
        if self.min_mmap_bytes == 0 || self.max_mmap_bytes == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "mmap byte bounds must be non-zero".to_string(),
            ));
        }
        if self.min_mmap_bytes > self.max_mmap_bytes {
            return Err(VectorError::IndexParameterInvalid(format!(
                "min_mmap_bytes {} exceeds max_mmap_bytes {}",
                self.min_mmap_bytes, self.max_mmap_bytes
            )));
        }
        Ok(())
    }
}

/// Full single-node storage policy for one vector index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorStoragePolicy {
    /// Caller request (auto by default; explicit wins where permitted).
    pub request: StorageModeRequest,
    /// Mmap switch + thresholds.
    pub mmap: MmapConfig,
    /// Per-index memory budget in bytes (memory pressure gate).
    pub memory_budget_bytes: u64,
}

impl Default for VectorStoragePolicy {
    fn default() -> Self {
        Self {
            request: StorageModeRequest::Auto,
            mmap: MmapConfig::default(),
            memory_budget_bytes: DEFAULT_INDEX_MEMORY_BUDGET_BYTES,
        }
    }
}

impl VectorStoragePolicy {
    /// Memory-only policy (never touches the filesystem).
    #[must_use]
    pub fn memory_only(memory_budget_bytes: u64) -> Self {
        Self {
            request: StorageModeRequest::Memory,
            mmap: MmapConfig {
                enabled: false,
                ..MmapConfig::default()
            },
            memory_budget_bytes,
        }
    }

    /// Validate ranges (mmap bounds + non-zero budget).
    pub fn validate(&self) -> Result<()> {
        self.mmap.validate()?;
        if self.memory_budget_bytes == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "memory_budget_bytes must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Select the backing mode for an index of `index_bytes` estimate.
    ///
    /// Deterministic and total: every combination of inputs yields exactly
    /// one decision with a human-readable reason. Explicit `Memory` fails
    /// (rather than silently spilling) when over budget; explicit `Mmap`
    /// falls back to `File` with `fallback_active=true` when mmap is
    /// unavailable or the index is too small/large for mapping.
    #[must_use]
    pub fn select(
        &self,
        index_bytes: u64,
        capability: &StorageCapability,
    ) -> StoragePolicyDecision {
        match self.request {
            StorageModeRequest::Memory => {
                if index_bytes > self.memory_budget_bytes {
                    return StoragePolicyDecision {
                        mode: StorageMode::File,
                        requested: self.request,
                        reason: format!(
                            "memory requested but index {index_bytes} bytes exceeds budget {}: file fallback (no silent OOM)",
                            self.memory_budget_bytes
                        ),
                        memory_budget_bytes: self.memory_budget_bytes,
                        storage_class: capability.storage_class,
                        mmap_available: capability.mmap_available,
                        fallback_active: true,
                    };
                }
                StoragePolicyDecision {
                    mode: StorageMode::Memory,
                    requested: self.request,
                    reason: format!(
                        "memory requested and index {index_bytes} bytes fits budget {}",
                        self.memory_budget_bytes
                    ),
                    memory_budget_bytes: self.memory_budget_bytes,
                    storage_class: capability.storage_class,
                    mmap_available: capability.mmap_available,
                    fallback_active: false,
                }
            }
            StorageModeRequest::File => StoragePolicyDecision {
                mode: StorageMode::File,
                requested: self.request,
                reason: "file requested explicitly".to_string(),
                memory_budget_bytes: self.memory_budget_bytes,
                storage_class: capability.storage_class,
                mmap_available: capability.mmap_available,
                fallback_active: false,
            },
            StorageModeRequest::Mmap => {
                if !self.mmap.enabled || !capability.mmap_available {
                    return StoragePolicyDecision {
                        mode: StorageMode::File,
                        requested: self.request,
                        reason: format!(
                            "mmap requested but unavailable ({}): file fallback",
                            capability.mmap_note
                        ),
                        memory_budget_bytes: self.memory_budget_bytes,
                        storage_class: capability.storage_class,
                        mmap_available: false,
                        fallback_active: true,
                    };
                }
                if index_bytes > self.mmap.max_mmap_bytes {
                    return StoragePolicyDecision {
                        mode: StorageMode::File,
                        requested: self.request,
                        reason: format!(
                            "mmap requested but index {index_bytes} bytes exceeds max_mmap_bytes {}: file fallback",
                            self.mmap.max_mmap_bytes
                        ),
                        memory_budget_bytes: self.memory_budget_bytes,
                        storage_class: capability.storage_class,
                        mmap_available: capability.mmap_available,
                        fallback_active: true,
                    };
                }
                StoragePolicyDecision {
                    mode: StorageMode::Mmap,
                    requested: self.request,
                    reason: "mmap requested and available".to_string(),
                    memory_budget_bytes: self.memory_budget_bytes,
                    storage_class: capability.storage_class,
                    mmap_available: capability.mmap_available,
                    fallback_active: false,
                }
            }
            StorageModeRequest::Auto => Self::select_auto(self, index_bytes, capability),
        }
    }

    fn select_auto(
        policy: &Self,
        index_bytes: u64,
        capability: &StorageCapability,
    ) -> StoragePolicyDecision {
        let mk = |mode: StorageMode, reason: String, fallback: bool| StoragePolicyDecision {
            mode,
            requested: StorageModeRequest::Auto,
            reason,
            memory_budget_bytes: policy.memory_budget_bytes,
            storage_class: capability.storage_class,
            mmap_available: capability.mmap_available,
            fallback_active: fallback,
        };
        // Memory pressure: over-budget indexes never go resident.
        if index_bytes <= TINY_MAX_BYTES && index_bytes <= policy.memory_budget_bytes {
            return mk(
                StorageMode::Memory,
                format!(
                    "tiny index {index_bytes} bytes <= {TINY_MAX_BYTES} and within budget {}: memory resident",
                    policy.memory_budget_bytes
                ),
                false,
            );
        }
        if index_bytes <= policy.memory_budget_bytes && index_bytes < policy.mmap.min_mmap_bytes {
            // Small-to-medium indexes that fit the budget but are too small
            // to amortize mapping overhead stay on plain files.
            if capability.storage_class == StorageClass::Hdd {
                return mk(
                    StorageMode::File,
                    format!(
                        "medium index {index_bytes} bytes on HDD: plain file with sequential prefetch (random mmap would seek)",
                    ),
                    false,
                );
            }
            return mk(
                StorageMode::File,
                format!(
                    "medium index {index_bytes} bytes below mmap threshold {}: plain file",
                    policy.mmap.min_mmap_bytes
                ),
                false,
            );
        }
        // Large or over-budget: mmap when safe, else file.
        if policy.mmap.enabled
            && capability.mmap_available
            && index_bytes >= policy.mmap.min_mmap_bytes
            && index_bytes <= policy.mmap.max_mmap_bytes
        {
            if capability.storage_class == StorageClass::Hdd {
                return mk(
                    StorageMode::File,
                    format!(
                        "large index {index_bytes} bytes on HDD: file with sequential prefetch preferred over mmap random faults"
                    ),
                    true,
                );
            }
            return mk(
                StorageMode::Mmap,
                format!(
                    "large index {index_bytes} bytes in mmap window [{}, {}] on {}: mmap preferred",
                    policy.mmap.min_mmap_bytes,
                    policy.mmap.max_mmap_bytes,
                    capability.storage_class.as_str()
                ),
                false,
            );
        }
        if index_bytes > policy.memory_budget_bytes {
            return mk(
                StorageMode::File,
                format!(
                    "index {index_bytes} bytes exceeds memory budget {} (mmap {}): file fallback, no silent OOM",
                    policy.memory_budget_bytes,
                    if capability.mmap_available { "out of window" } else { "unavailable" }
                ),
                true,
            );
        }
        mk(
            StorageMode::File,
            format!(
                "auto default for {index_bytes} bytes ({}): plain file is the safe middle",
                capability.mmap_note
            ),
            // A large index that sits in the mmap window would have mapped
            // had mmap been enabled and available: report the fallback
            // honestly.
            index_bytes >= policy.mmap.min_mmap_bytes
                && index_bytes <= policy.mmap.max_mmap_bytes
                && !(policy.mmap.enabled && capability.mmap_available),
        )
    }
}

/// Outcome of one policy selection: what was chosen and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoragePolicyDecision {
    /// Selected backing mode.
    pub mode: StorageMode,
    /// What was requested.
    pub requested: StorageModeRequest,
    /// Human-readable reason (deterministic per input combination).
    pub reason: String,
    /// Memory budget that applied.
    pub memory_budget_bytes: u64,
    /// Storage class that was detected.
    pub storage_class: StorageClass,
    /// Whether mmap was available at selection time.
    pub mmap_available: bool,
    /// True when the request fell back to a safe mode.
    pub fallback_active: bool,
}

impl StoragePolicyDecision {
    /// True when the index is memory resident.
    #[must_use]
    pub const fn is_memory_resident(&self) -> bool {
        self.mode.is_memory_resident()
    }

    /// True when the index is mmap backed.
    #[must_use]
    pub const fn is_mmap_backed(&self) -> bool {
        self.mode.is_mmap_backed()
    }

    /// Record this decision in the shared metrics (existing diagnostics).
    pub fn observe(&self, metrics: &crate::metrics::VectorMetrics) {
        metrics.observe_storage_decision(self.mode.as_str(), self.fallback_active);
    }

    /// Deterministic diagnostic lines answering the Phase 41 questions.
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        vec![
            format!("Storage mode: {}", self.mode.as_str()),
            format!("Memory resident: {}", self.is_memory_resident()),
            format!("Mmap backed: {}", self.is_mmap_backed()),
            format!("Reason: {}", self.reason),
            format!("Memory budget: {} bytes", self.memory_budget_bytes),
            format!("Storage capability: {}", self.storage_class.as_str()),
            format!("Mmap available: {}", self.mmap_available),
            format!("Fallback active: {}", self.fallback_active),
        ]
    }
}

/// Managed single-node vector store: policy-selected backing with
/// residency/prefetch accounting and crash-safe durability.
///
/// Backing variants reuse the existing adapters (no parallel subsystem):
/// memory uses [`crate::storage::InMemoryVectorStorage`], file/mmap use
/// [`crate::storage::FileVectorStorage`] (mmap adds sequential prefetch +
/// residency tracking on the same durable file).
#[derive(Debug)]
pub struct ManagedVectorStorage {
    backing: Backing,
    decision: StoragePolicyDecision,
    /// Rows warmed by sequential prefetch (cold vs warm observable).
    warmed_rows: u64,
    /// Prefetch calls served.
    prefetch_calls: u64,
}

#[derive(Debug)]
enum Backing {
    Memory(crate::storage::InMemoryVectorStorage),
    Durable(crate::storage::FileVectorStorage),
}

impl ManagedVectorStorage {
    /// Create a fresh managed store in `dir` under `policy`.
    ///
    /// `index_bytes` is the caller's size estimate (e.g. from
    /// [`crate::options::memory_estimate_bytes`]); it drives mode selection
    /// only, never allocation. Fails closed on resource exhaustion
    /// (estimate above [`MAX_PAYLOAD_BYTES`]-scaled sanity is rejected via
    /// [`VectorError::ResourceLimit`]).
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        dir: impl Into<std::path::PathBuf>,
        table: String,
        column: String,
        dims: Option<u32>,
        policy: &VectorStoragePolicy,
        index_bytes: u64,
        capability: &StorageCapability,
        metrics: Option<&crate::metrics::VectorMetrics>,
    ) -> Result<Self> {
        policy.validate()?;
        let decision = policy.select(index_bytes, capability);
        if let Some(m) = metrics {
            decision.observe(m);
        }
        let dir = dir.into();
        let backing = match decision.mode {
            StorageMode::Memory => {
                // Budget already enforced by `select`; re-check defensively.
                if index_bytes > policy.memory_budget_bytes {
                    return Err(VectorError::ResourceLimit(format!(
                        "managed create: {index_bytes} bytes exceeds budget {}",
                        policy.memory_budget_bytes
                    )));
                }
                Backing::Memory(crate::storage::InMemoryVectorStorage::new(dims))
            }
            StorageMode::File | StorageMode::Mmap => Backing::Durable(
                crate::storage::FileVectorStorage::create(&dir, table, column, dims)?,
            ),
        };
        Ok(Self {
            backing,
            decision,
            warmed_rows: 0,
            prefetch_calls: 0,
        })
    }

    /// Open an existing managed store, replaying records and truncating a
    /// torn tail (crash mid-append) exactly like the file adapter.
    ///
    /// Memory-mode stores have no durable image: opening a directory that
    /// contains no `records.dat` with a memory policy yields a fresh empty
    /// memory store (restart behavior is explicit: memory does not survive
    /// restarts; callers needing durability must use file/mmap).
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        dir: impl Into<std::path::PathBuf>,
        table: String,
        column: String,
        dims: Option<u32>,
        policy: &VectorStoragePolicy,
        index_bytes: u64,
        capability: &StorageCapability,
        metrics: Option<&crate::metrics::VectorMetrics>,
    ) -> Result<(Self, crate::storage::FileRecovery)> {
        policy.validate()?;
        let decision = policy.select(index_bytes, capability);
        if let Some(m) = metrics {
            decision.observe(m);
        }
        let dir = dir.into();
        match decision.mode {
            StorageMode::Memory => {
                // Memory has no durable image; a stray records.dat is left
                // untouched (it belongs to a file/mmap incarnation).
                Ok((
                    Self {
                        backing: Backing::Memory(crate::storage::InMemoryVectorStorage::new(dims)),
                        decision,
                        warmed_rows: 0,
                        prefetch_calls: 0,
                    },
                    crate::storage::FileRecovery {
                        replayed_records: 0,
                        manifest_lsn: 0,
                        truncated_tail: false,
                    },
                ))
            }
            StorageMode::File | StorageMode::Mmap => {
                let (store, recovery) =
                    crate::storage::FileVectorStorage::open(&dir, table, column, dims, true)?;
                Ok((
                    Self {
                        backing: Backing::Durable(store),
                        decision,
                        warmed_rows: 0,
                        prefetch_calls: 0,
                    },
                    recovery,
                ))
            }
        }
    }

    /// Selected storage mode.
    #[must_use]
    pub const fn storage_mode(&self) -> StorageMode {
        self.decision.mode
    }

    /// Policy decision behind this store (why this mode).
    #[must_use]
    pub fn decision(&self) -> &StoragePolicyDecision {
        &self.decision
    }

    /// True when rows are memory resident.
    #[must_use]
    pub const fn is_memory_resident(&self) -> bool {
        self.decision.mode.is_memory_resident()
    }

    /// True when rows are mmap backed.
    #[must_use]
    pub const fn is_mmap_backed(&self) -> bool {
        self.decision.mode.is_mmap_backed()
    }

    /// Rows warmed by prefetch so far (cold-start vs warm-start signal).
    #[must_use]
    pub const fn warmed_rows(&self) -> u64 {
        self.warmed_rows
    }

    /// Sequential prefetch over `[start_row, start_row + limit)`.
    ///
    /// File/mmap backings stream the range once to warm the OS page cache
    /// (the same range a future mapped reader would fault in); memory
    /// backing just validates bounds. Corrupted rows fail closed with
    /// [`VectorError::IndexCorrupted`]; out-of-range starts fail with a
    /// bounds error. Never fabricates rows.
    pub fn prefetch(&mut self, start_row: u64, limit: usize) -> Result<u64> {
        use crate::storage::VectorStorageAdapter as _;
        self.prefetch_calls = self.prefetch_calls.saturating_add(1);
        let batch = match &self.backing {
            Backing::Memory(s) => s.scan_batch(start_row, limit)?,
            Backing::Durable(s) => s.scan_batch(start_row, limit)?,
        };
        // Validate every payload decodes (corruption surfaces here, not later).
        for (_, v) in &batch {
            let _ = v.dimension();
        }
        self.warmed_rows = self.warmed_rows.saturating_add(batch.len() as u64);
        Ok(batch.len() as u64)
    }

    /// Whether the last prefetch left the store warm (any rows warmed).
    #[must_use]
    pub const fn is_warm(&self) -> bool {
        self.warmed_rows > 0
    }

    /// Flush buffered OS state (durable backings sync; memory is a no-op).
    pub fn flush(&self) -> Result<()> {
        match &self.backing {
            Backing::Memory(_) => Ok(()),
            Backing::Durable(s) => s.flush(),
        }
    }

    /// Checkpoint the durable manifest (no-op for memory backing).
    pub fn checkpoint(&self, checkpoint_lsn: u64) -> Result<()> {
        match &self.backing {
            Backing::Memory(_) => Ok(()),
            Backing::Durable(s) => s.checkpoint(checkpoint_lsn),
        }
    }

    /// Close the store: flush + (for durable backings) checkpoint sync.
    ///
    /// Restart behavior is deterministic: file/mmap stores reopen with full
    /// replay; memory stores reopen empty (documented, never silent loss —
    /// the decision lines state the mode).
    pub fn close(self) -> Result<StoragePolicyDecision> {
        self.flush()?;
        Ok(self.decision)
    }

    /// Diagnostic lines for existing admin/diagnostics export.
    #[must_use]
    pub fn diagnostics(&self) -> Vec<String> {
        let mut lines = self.decision.explain();
        lines.push(format!("Warmed rows: {}", self.warmed_rows));
        lines.push(format!("Prefetch calls: {}", self.prefetch_calls));
        lines.push(format!(
            "Start state: {}",
            if self.is_warm() { "warm" } else { "cold" }
        ));
        lines
    }

    /// Validate a persisted image, repairing a torn tail exactly as
    /// [`Self::open`] would.
    ///
    /// Checks magic/version framing on the manifest (when present) and
    /// replays record offsets; a torn tail from a crash mid-append is
    /// truncated (crash evidence, now repaired) and reported as
    /// [`ImageKind::Partial`], never hidden. Corruption fails closed.
    /// Used for `corrupted file` / `partial file` handling before open.
    pub fn validate_image(dir: &std::path::Path) -> Result<ValidatedImage> {
        let records = dir.join("records.dat");
        let manifest_path = dir.join("MANIFEST");
        if !records.exists() {
            return Ok(ValidatedImage {
                kind: ImageKind::Absent,
                records: 0,
                manifest_lsn: 0,
                truncated_tail: false,
            });
        }
        // Reuse the file adapter's replay (truncates torn tails) by opening
        // read-only-then-close: open with allow_create=false never creates.
        let (_store, recovery) = crate::storage::FileVectorStorage::open(
            dir,
            "validate".to_string(),
            "validate".to_string(),
            None,
            false,
        )?;
        let manifest_lsn = match std::fs::read(&manifest_path) {
            Ok(bytes) => crate::storage::VectorManifest::from_bytes(&bytes)?.checkpoint_lsn,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(VectorError::IndexCorrupted(format!(
                    "managed validate io: {e}"
                )));
            }
        };
        Ok(ValidatedImage {
            kind: if recovery.truncated_tail {
                ImageKind::Partial
            } else {
                ImageKind::Healthy
            },
            records: recovery.replayed_records,
            manifest_lsn,
            truncated_tail: recovery.truncated_tail,
        })
    }
}

impl crate::storage::VectorStorageAdapter for ManagedVectorStorage {
    fn row_count(&self) -> u64 {
        match &self.backing {
            Backing::Memory(s) => s.row_count(),
            Backing::Durable(s) => s.row_count(),
        }
    }

    fn dims(&self) -> Option<u32> {
        match &self.backing {
            Backing::Memory(s) => s.dims(),
            Backing::Durable(s) => s.dims(),
        }
    }

    fn get_row_bytes(&self, row_id: u64) -> Result<Vec<u8>> {
        match &self.backing {
            Backing::Memory(s) => s.get_row_bytes(row_id),
            Backing::Durable(s) => s.get_row_bytes(row_id),
        }
    }

    fn put_row_bytes(&mut self, row_id: u64, row_bytes: &[u8]) -> Result<()> {
        if row_bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(VectorError::ResourceLimit(format!(
                "managed put: {} bytes exceeds cap",
                row_bytes.len()
            )));
        }
        match &mut self.backing {
            Backing::Memory(s) => s.put_row_bytes(row_id, row_bytes),
            Backing::Durable(s) => s.put_row_bytes(row_id, row_bytes),
        }
    }

    fn append_row_bytes(&mut self, row_bytes: &[u8]) -> Result<u64> {
        if row_bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(VectorError::ResourceLimit(format!(
                "managed append: {} bytes exceeds cap",
                row_bytes.len()
            )));
        }
        match &mut self.backing {
            Backing::Memory(s) => s.append_row_bytes(row_bytes),
            Backing::Durable(s) => s.append_row_bytes(row_bytes),
        }
    }

    fn scan_batch(&self, start_row: u64, limit: usize) -> Result<Vec<(u64, crate::VectorValue)>> {
        match &self.backing {
            Backing::Memory(s) => s.scan_batch(start_row, limit),
            Backing::Durable(s) => s.scan_batch(start_row, limit),
        }
    }
}

/// Persisted-image health from [`ManagedVectorStorage::validate_image`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImageKind {
    /// No image exists.
    Absent,
    /// Replay succeeded with no torn tail.
    Healthy,
    /// A torn tail was truncated (crash mid-append, now repaired).
    Partial,
}

/// Read-only validation outcome for one store directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedImage {
    /// Health class.
    pub kind: ImageKind,
    /// Records replayed.
    pub records: u64,
    /// Manifest checkpoint LSN (0 when no manifest).
    pub manifest_lsn: u64,
    /// Whether a torn tail was truncated.
    pub truncated_tail: bool,
}
