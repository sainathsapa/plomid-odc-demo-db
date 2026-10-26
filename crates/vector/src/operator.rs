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
//! Self-hosted single-node operability contract (Gap O).
//!
//! What an operator can expect from a production single-node PLOMID
//! Vector Engine — not a managed-service control plane. Everything here
//! composes **existing** systems (no duplicate observability):
//!
//! * status views compose `IndexState`, `Freshness`, `ScaleDecision`,
//!   `StoragePolicyDecision`, `NumaTopology`, and `VectorMetrics`;
//! * the configuration inventory enumerates the real knobs (9 SQL +
//!   6 env) with defaults, ranges, effects, and restart behavior;
//! * the error taxonomy maps every `VectorError`/`SearchError` variant to
//!   a stable operational category (and the observed SQLSTATE behavior).

use crate::admin::IndexState;
use crate::freshness::Freshness;

/// Stable operational error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Bad SQL literal, dimension, element, or argument.
    InvalidInput,
    /// Bad server/executor setting or env override.
    InvalidConfiguration,
    /// Index absent, building, or otherwise not servable right now.
    UnavailableIndex,
    /// Persisted bytes failed structural validation.
    IndexCorruption,
    /// File/device/IO failure (records, manifests, pairs, backup files).
    StorageFailure,
    /// Budgets, caps, admission refusals.
    ResourceExhaustion,
    /// Deadline exceeded (maps to existing `Cancelled` path).
    Timeout,
    /// Cooperative cancellation requested.
    Cancellation,
    /// Mount/replay/restart behavior.
    Recovery,
    /// Version skew, rebuild-required, unsupported artifacts.
    Migration,
    /// Newer-than-binary versions.
    UnsupportedVersion,
    /// Anything else (bug surface — never user blame).
    InternalFailure,
}

impl ErrorCategory {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid-input",
            Self::InvalidConfiguration => "invalid-configuration",
            Self::UnavailableIndex => "unavailable-index",
            Self::IndexCorruption => "index-corruption",
            Self::StorageFailure => "storage-failure",
            Self::ResourceExhaustion => "resource-exhaustion",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::Recovery => "recovery",
            Self::Migration => "migration",
            Self::UnsupportedVersion => "unsupported-version",
            Self::InternalFailure => "internal-failure",
        }
    }

    /// Whether the operator can retry without changing anything.
    #[must_use]
    pub const fn retryable_without_change(self) -> bool {
        matches!(
            self,
            Self::UnavailableIndex | Self::Recovery | Self::Timeout | Self::Cancellation
        )
    }
}

/// Map one [`crate::VectorError`] to its operational category.
#[must_use]
pub const fn categorize_vector_error(e: &crate::VectorError) -> ErrorCategory {
    match e {
        crate::VectorError::InvalidDimension(_)
        | crate::VectorError::InvalidElementType(_)
        | crate::VectorError::InvalidLiteral(_)
        | crate::VectorError::NonFiniteValue(_)
        | crate::VectorError::NullArgument(_)
        | crate::VectorError::ZeroNorm(_)
        | crate::VectorError::TypeMismatch(_)
        | crate::VectorError::UnsupportedCombination(_)
        | crate::VectorError::DimensionMismatch { .. }
        | crate::VectorError::MetricMismatch(_) => ErrorCategory::InvalidInput,
        crate::VectorError::IndexUnsupported(_) => ErrorCategory::UnavailableIndex,
        crate::VectorError::IndexParameterInvalid(_) => ErrorCategory::InvalidConfiguration,
        crate::VectorError::IndexStateInvalid(_) => ErrorCategory::UnavailableIndex,
        crate::VectorError::IndexCorrupted(_) => ErrorCategory::IndexCorruption,
        crate::VectorError::RebuildRequired(_) => ErrorCategory::Migration,
        crate::VectorError::ResourceLimit(_) => ErrorCategory::ResourceExhaustion,
        crate::VectorError::Cancelled(_) => ErrorCategory::Cancellation,
        crate::VectorError::Corruption(_) => ErrorCategory::StorageFailure,
    }
}

/// Map one [`crate::search::SearchError`] to its operational category.
#[must_use]
pub const fn categorize_search_error(e: &crate::search::SearchError) -> ErrorCategory {
    match e {
        crate::search::SearchError::DimensionMismatch { .. }
        | crate::search::SearchError::InvalidLength { .. }
        | crate::search::SearchError::OutOfRange { .. }
        | crate::search::SearchError::InvalidK { .. }
        | crate::search::SearchError::InvalidRadius { .. }
        | crate::search::SearchError::NonFiniteQuery => ErrorCategory::InvalidInput,
        crate::search::SearchError::Overflow { .. } => ErrorCategory::ResourceExhaustion,
        crate::search::SearchError::Cancelled => ErrorCategory::Cancellation,
        crate::search::SearchError::FetchFailed { .. } => ErrorCategory::StorageFailure,
    }
}

/// Observed SQLSTATE behavior for vector errors (from the wire handler's
/// `ErrorKind`+keyword mapping — documented here so operators can alert).
#[must_use]
pub const fn sqlstate_for_category(c: ErrorCategory) -> &'static str {
    match c {
        ErrorCategory::InvalidInput => "22023 (22003 when 'out of range')",
        ErrorCategory::InvalidConfiguration => "22023",
        ErrorCategory::UnavailableIndex => "0A000",
        ErrorCategory::IndexCorruption => "XX001",
        ErrorCategory::StorageFailure => "58000/XX000",
        ErrorCategory::ResourceExhaustion => "53200/22003",
        ErrorCategory::Timeout | ErrorCategory::Cancellation => "57014",
        ErrorCategory::Recovery => "XX000",
        ErrorCategory::Migration => "0A000",
        ErrorCategory::UnsupportedVersion => "0A000",
        ErrorCategory::InternalFailure => "XX000",
    }
}

/// One production-relevant setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingInfo {
    /// `vector.ef_search` or `PLOMID_VECTOR_TIER` style name.
    pub name: &'static str,
    /// `bool`, `integer`, `enum`, `bytes`.
    pub kind: &'static str,
    /// Default rendering.
    pub default: &'static str,
    /// Valid range rendering.
    pub range: &'static str,
    /// One-line effect.
    pub effect: &'static str,
    /// Whether a restart is required (env vars) or immediate (SQL knobs).
    pub restart_required: bool,
    /// Failure behavior rendering.
    pub on_invalid: &'static str,
}

/// Complete configuration inventory: every production-relevant vector
/// setting that exists in this binary. The test pins the count so new
/// knobs cannot land undocumented.
#[must_use]
pub const fn config_inventory() -> [SettingInfo; 15] {
    [
        SettingInfo {
            name: "vector.ann_enabled",
            kind: "bool",
            default: "on",
            range: "on|off",
            effect: "ANN execution allowed; off forces exact",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.ef_search",
            kind: "integer",
            default: "100",
            range: "1..=4096",
            effect: "HNSW/DiskANN candidate list size",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.candidate_oversample",
            kind: "integer",
            default: "8",
            range: "1..=64",
            effect: "candidate oversample factor before rerank",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.max_ann_rows",
            kind: "integer",
            default: "1000000",
            range: "0..=16777216",
            effect: "ANN refused above this row count (exact serves)",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.cache_graphs",
            kind: "integer",
            default: "8",
            range: "1..=64",
            effect: "ANN graph cache entries",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.max_index_vectors",
            kind: "integer",
            default: "1000000",
            range: "1..=16777216",
            effect: "index build row cap",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.auto_ann_min_rows",
            kind: "integer",
            default: "4096",
            range: "0..=1000000000",
            effect: "Auto prefers ANN above this row count",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.compute",
            kind: "enum",
            default: "auto",
            range: "auto|cpu|gpu",
            effect: "compute backend; gpu degrades to CPU",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "vector.max_memory_bytes",
            kind: "bytes",
            default: "1GiB",
            range: "1MiB..=1TiB",
            effect: "per-build memory budget; over-budget falls back to exact",
            restart_required: false,
            on_invalid: "statement fails",
        },
        SettingInfo {
            name: "PLOMID_VECTOR_MMAP",
            kind: "enum",
            default: "auto",
            range: "auto|on|off",
            effect: "mmap-preferred storage path allowed",
            restart_required: true,
            on_invalid: "fail-safe: mmap disabled",
        },
        SettingInfo {
            name: "PLOMID_VECTOR_NUMA",
            kind: "enum",
            default: "auto",
            range: "auto|off|<N>",
            effect: "NUMA striping for batch dispatch",
            restart_required: true,
            on_invalid: "fail-safe: single-node mode",
        },
        SettingInfo {
            name: "PLOMID_VECTOR_TIER",
            kind: "enum",
            default: "auto",
            range: "auto|laptop|workstation|small_server|production_server|large_memory_high_io",
            effect: "single-machine scale tier override",
            restart_required: true,
            on_invalid: "fail-safe: automatic selection",
        },
        SettingInfo {
            name: "PLOMID_VECTOR_SIMD",
            kind: "enum",
            default: "auto",
            range: "scalar|off|avx2|avx512|neon|sve",
            effect: "SIMD dispatch tier; unavailable falls back to scalar",
            restart_required: true,
            on_invalid: "fail-safe: auto-detection",
        },
        SettingInfo {
            name: "PLOMID_VECTOR_HTAP",
            kind: "enum",
            default: "auto",
            range: "auto|off",
            effect: "HTAP degradation allowed; off never degrades",
            restart_required: true,
            on_invalid: "fail-safe: degradation enabled",
        },
        SettingInfo {
            name: "PLOMID_PROFILE",
            kind: "enum",
            default: "auto",
            range: "auto|edge|laptop|server",
            effect: "hardware runtime tier (CLI --profile wins)",
            restart_required: true,
            on_invalid: "startup fails closed",
        },
    ]
}

/// Operator-facing index status: lifecycle + freshness in one view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexStatusView {
    /// Index name.
    pub name: String,
    /// Lifecycle state.
    pub state: IndexState,
    /// Freshness label.
    pub freshness: Freshness,
    /// Whether ANN queries may be served right now.
    pub queryable: bool,
    /// Human-readable reason.
    pub reason: String,
}

impl IndexStatusView {
    /// Compose from lifecycle state + freshness (single source of truth:
    /// [`crate::freshness::is_index_queryable`]).
    #[must_use]
    pub fn new(name: String, state: IndexState, freshness: Freshness) -> Self {
        let queryable = crate::freshness::is_index_queryable(state, freshness);
        let reason = if queryable {
            format!("{} is {} and fresh: serving ANN", name, state.as_str())
        } else {
            format!(
                "{} is {} with freshness {}: exact fallback, never wrong",
                name,
                state.as_str(),
                freshness.as_str()
            )
        };
        Self {
            name,
            state,
            freshness,
            queryable,
            reason,
        }
    }
}

/// Engine liveness for operator dashboards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineLiveness {
    /// Serving (degraded details live in per-index views + metrics).
    Ready,
    /// Recovering (restart/replay in progress; refresh required after).
    Recovering,
}

impl EngineLiveness {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Recovering => "recovering",
        }
    }
}

/// Supportability checklist: symptom → existing diagnostic source.
#[must_use]
pub fn support_checklist() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "slow vector queries",
            "EXPLAIN vector plan + VectorMetrics exact/ann counters",
        ),
        (
            "poor ANN recall",
            "recall_at_k in VectorStats + candidate_k + rerank lines",
        ),
        (
            "stale statistics",
            "FreshnessTracker::explain + stale_plan_fallbacks counter",
        ),
        (
            "stale indexes",
            "IndexStatusView + check_index_freshness watermarks",
        ),
        (
            "failed index builds",
            "IndexState::Failed + build error codes (VECTOR_INDEX_*)",
        ),
        (
            "storage pressure",
            "StoragePolicyDecision::explain + storage_fallbacks",
        ),
        (
            "memory pressure",
            "PressureLevel::from_usage + admission Reject reasons",
        ),
        (
            "resource exhaustion",
            "VectorError::ResourceLimit + htap_rejected counter",
        ),
        (
            "recovery",
            "FileRecovery{replayed,manifest_lsn,truncated_tail} + Freshness::Recovering",
        ),
        (
            "migration failures",
            "migration::enforce codes + matrix lines",
        ),
        (
            "backup/restore failures",
            "BackupReport/RestoreReport + backups/restores_completed",
        ),
        (
            "lifecycle problems",
            "admin::IndexState machine + maintenance::advise",
        ),
    ]
}
