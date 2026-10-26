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
//! Lifecycle freshness for one node (Gap J).
//!
//! Replaces every implicit "fresh enough" assumption with explicit state.
//! Freshness covers index metadata, planner/vector statistics, cached
//! metadata, build state, maintenance state, persisted state, recovery
//! state, and configuration-derived state — all on a single node.
//!
//! Representation reuses the existing project vocabulary: [`crate::admin::IndexState`]
//! remains the index lifecycle authority and [`crate::stats::VectorStats`]
//! (`build_watermark` + `stats_tick` + `is_fresh`) remains the statistics
//! authority. This module adds the explicit [`Freshness`] label, the
//! deterministic invalidation triggers ([`FreshnessCause`]), and the
//! conservative-planning gates that keep stale information from producing
//! incorrect results (performance may degrade; correctness never does).
//!
//! Single node only: no distributed invalidation, no cluster coordination.

use crate::admin::IndexState;
use crate::stats::VectorStats;
use crate::{Result, VectorError};

/// Explicit freshness label for any cached/derived vector state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Freshness {
    /// Current with the latest committed watermark.
    Fresh,
    /// Superseded by a newer watermark; safe only for conservative use.
    Stale,
    /// An index build is in progress; the derived image is not usable.
    Building,
    /// Persisted state failed validation; rebuild required, never served.
    Invalid,
    /// Restart/recovery in progress; usability unknown until replay ends.
    Recovering,
    /// No observation yet (cold start, missing stats, fresh mount).
    #[default]
    Unknown,
}

impl Freshness {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Stale => "stale",
            Self::Building => "building",
            Self::Invalid => "invalid",
            Self::Recovering => "recovering",
            Self::Unknown => "unknown",
        }
    }

    /// Parse [`Self::as_str`] output (case-insensitive).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "fresh" => Ok(Self::Fresh),
            "stale" => Ok(Self::Stale),
            "building" => Ok(Self::Building),
            "invalid" => Ok(Self::Invalid),
            "recovering" | "recovery" => Ok(Self::Recovering),
            "unknown" => Ok(Self::Unknown),
            other => Err(VectorError::IndexParameterInvalid(format!(
                "unknown freshness '{other}': expected fresh|stale|building|invalid|recovering|unknown"
            ))),
        }
    }

    /// True when ANN serving is permitted (only fresh statistics/indexes).
    #[must_use]
    pub const fn usable_for_ann(self) -> bool {
        matches!(self, Self::Fresh)
    }

    /// True when exact fallback is permitted (fresh, stale, or unknown with
    /// a stated reason — exact scans read authoritative rows, so they are
    /// always correct).
    #[must_use]
    pub const fn usable_for_exact(self) -> bool {
        matches!(self, Self::Fresh | Self::Stale | Self::Unknown)
    }
}

/// Deterministic invalidation trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FreshnessCause {
    /// Row insertion.
    Insert,
    /// Row update (delete + insert).
    Update,
    /// Row deletion.
    Delete,
    /// Direct index mutation (insert/delete outside SQL DML).
    IndexMutation,
    /// Full index rebuild started/finished.
    IndexRebuild,
    /// Process restart (state must re-prove freshness).
    Restart,
    /// Crash recovery replay.
    CrashRecovery,
    /// Configuration change affecting derived state.
    ConfigChange,
    /// Statistics refresh completed.
    StatsRefresh,
    /// Maintenance (compaction/vacuum) ran.
    Maintenance,
    /// Maintenance interrupted mid-run.
    MaintenanceInterrupted,
    /// Restore from backup/snapshot.
    Restore,
    /// Persisted metadata failed validation.
    CorruptedMetadata,
}

impl FreshnessCause {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::IndexMutation => "index_mutation",
            Self::IndexRebuild => "index_rebuild",
            Self::Restart => "restart",
            Self::CrashRecovery => "crash_recovery",
            Self::ConfigChange => "config_change",
            Self::StatsRefresh => "stats_refresh",
            Self::Maintenance => "maintenance",
            Self::MaintenanceInterrupted => "maintenance_interrupted",
            Self::Restore => "restore",
            Self::CorruptedMetadata => "corrupted_metadata",
        }
    }
}

/// Tracker for one freshness domain (statistics, index image, or cache entry).
///
/// Deterministic: the same cause sequence always yields the same state.
/// Starts [`Freshness::Unknown`] (cold start proves freshness via refresh,
/// never assumes it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshnessTracker {
    state: Freshness,
    watermark: u64,
    tick: u64,
    cause: Option<FreshnessCause>,
    detail: String,
}

impl Default for FreshnessTracker {
    fn default() -> Self {
        Self::unknown()
    }
}

impl FreshnessTracker {
    /// Cold-start tracker (unknown until refreshed).
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            state: Freshness::Unknown,
            watermark: 0,
            tick: 0,
            cause: None,
            detail: String::new(),
        }
    }

    /// Current freshness.
    #[must_use]
    pub const fn state(&self) -> Freshness {
        self.state
    }

    /// Watermark this tracker was refreshed at.
    #[must_use]
    pub const fn watermark(&self) -> u64 {
        self.watermark
    }

    /// Last invalidation cause, if any.
    #[must_use]
    pub const fn cause(&self) -> Option<FreshnessCause> {
        self.cause
    }

    /// Apply an invalidation trigger. Any write-like cause moves
    /// Fresh→Stale; build/recovery causes move to their dedicated states;
    /// corruption always moves to Invalid (never served).
    pub fn invalidate(&mut self, cause: FreshnessCause, detail: impl Into<String>) {
        let detail = detail.into();
        self.cause = Some(cause);
        self.detail = detail;
        self.tick = self.tick.saturating_add(1);
        self.state = match cause {
            FreshnessCause::Insert
            | FreshnessCause::Update
            | FreshnessCause::Delete
            | FreshnessCause::IndexMutation
            | FreshnessCause::ConfigChange
            | FreshnessCause::Maintenance
            | FreshnessCause::Restore => match self.state {
                Freshness::Fresh | Freshness::Stale | Freshness::Unknown => Freshness::Stale,
                other => other,
            },
            FreshnessCause::IndexRebuild => Freshness::Building,
            FreshnessCause::Restart | FreshnessCause::CrashRecovery => Freshness::Recovering,
            FreshnessCause::MaintenanceInterrupted => Freshness::Recovering,
            FreshnessCause::CorruptedMetadata => Freshness::Invalid,
            FreshnessCause::StatsRefresh => self.state,
        };
    }

    /// Mark a build/rebuild as complete and fresh at `watermark`.
    pub fn mark_built(&mut self, watermark: u64) {
        self.state = Freshness::Fresh;
        self.watermark = watermark;
        self.tick = self.tick.saturating_add(1);
        self.cause = None;
        self.detail = format!("built at watermark {watermark}");
    }

    /// Refresh statistics at `watermark` (post-refresh state is fresh).
    pub fn refresh(&mut self, watermark: u64) {
        self.state = Freshness::Fresh;
        self.watermark = watermark;
        self.tick = self.tick.saturating_add(1);
        self.cause = None;
        self.detail = format!("refreshed at watermark {watermark}");
    }

    /// Mark recovery complete: recovering→unknown (must refresh before use).
    pub fn recovery_complete(&mut self) {
        if self.state == Freshness::Recovering {
            self.state = Freshness::Unknown;
            self.tick = self.tick.saturating_add(1);
            self.detail = "recovery complete: refresh required".to_string();
        }
    }

    /// Mark persisted state invalid (corruption detected, never served).
    pub fn mark_invalid(&mut self, detail: impl Into<String>) {
        self.state = Freshness::Invalid;
        self.cause = Some(FreshnessCause::CorruptedMetadata);
        self.detail = detail.into();
        self.tick = self.tick.saturating_add(1);
    }

    /// Whether ANN serving is permitted in this state.
    #[must_use]
    pub const fn usable_for_ann(&self) -> bool {
        self.state.usable_for_ann()
    }

    /// Diagnostic lines.
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        vec![
            format!("Freshness: {}", self.state.as_str()),
            format!("Watermark: {}", self.watermark),
            format!("Tick: {}", self.tick),
            format!(
                "Cause: {}",
                self.cause
                    .map_or_else(|| "none".to_string(), |c| c.as_str().to_string())
            ),
            format!(
                "Detail: {}",
                if self.detail.is_empty() {
                    "(none)"
                } else {
                    &self.detail
                }
            ),
        ]
    }
}

/// Classify statistics freshness against the current commit watermark.
///
/// * Unobserved stats → [`Freshness::Unknown`] (safe exact fallback).
/// * `stats.is_fresh(watermark)` → [`Freshness::Fresh`].
/// * Otherwise → [`Freshness::Stale`] (conservative planning only).
#[must_use]
pub fn check_stats_freshness(stats: &VectorStats, watermark: u64) -> Freshness {
    if !stats.is_observed() {
        return Freshness::Unknown;
    }
    if stats.is_fresh(watermark) {
        Freshness::Fresh
    } else {
        Freshness::Stale
    }
}

/// Classify index-image freshness from lifecycle state + build watermark.
///
/// * `Failed` or generation mismatch → [`Freshness::Invalid`] (never served).
/// * `Building`/`CatchingUp`/`Rebuilding` → [`Freshness::Building`].
/// * `Absent` → [`Freshness::Unknown`].
/// * `Ready`/`Degraded` + watermark match → [`Freshness::Fresh`], else [`Freshness::Stale`].
#[must_use]
pub fn check_index_freshness(
    state: IndexState,
    build_watermark: u64,
    current_watermark: u64,
) -> Freshness {
    match state {
        IndexState::Failed => Freshness::Invalid,
        IndexState::Building | IndexState::CatchingUp | IndexState::Rebuilding => {
            Freshness::Building
        }
        IndexState::Absent => Freshness::Unknown,
        IndexState::Ready | IndexState::Degraded => {
            if build_watermark == current_watermark {
                Freshness::Fresh
            } else {
                Freshness::Stale
            }
        }
    }
}

/// Whether an index may serve ANN queries under lifecycle + freshness.
///
/// Incomplete builds never appear usable: non-queryable states and any
/// non-fresh label fail closed to exact (or to an error when even exact is
/// impossible). `Degraded` + `Fresh` stays queryable (reduced recall, still
/// correct candidates + exact rerank).
#[must_use]
pub fn is_index_queryable(state: IndexState, freshness: Freshness) -> bool {
    state.is_queryable() && freshness.usable_for_ann()
}

/// Whether an interrupted-maintenance image may serve.
///
/// Interrupted maintenance always recovers to `Recovering` first (never
/// directly to usable): the caller must replay/validate then refresh.
#[must_use]
pub fn maintenance_recovery_state(interrupted: bool) -> Freshness {
    if interrupted {
        Freshness::Recovering
    } else {
        Freshness::Fresh
    }
}
