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
//! Maintenance heuristics: tombstone cleanup, index rebuilds, and
//! a fragmentation metric to drive them.
//!
//! NON-AUTHORITATIVE advisory heuristics (see [`crate::durability`]).
//! Compaction/GC authority lives in the storage engine (`VACUUM` +
//! generation GC); these numbers advise derived-index rebuilds only and can
//! never reclaim authoritative rows.

/// Tunables for maintenance decisions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaintenanceConfig {
    /// Cleanup when `tombstones / total_rows >= this` (0.0..=1.0).
    pub tombstone_ratio_cleanup: f64,
    /// Cleanup when absolute tombstone count reaches this.
    pub tombstone_count_cleanup: u64,
    /// Rebuild when fragmentation reaches this (0.0..=1.0).
    pub rebuild_fragmentation: f64,
    /// Minimum rows before a rebuild is even considered.
    pub min_rows_for_rebuild: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            tombstone_ratio_cleanup: 0.2,
            tombstone_count_cleanup: 10_000,
            rebuild_fragmentation: 0.35,
            min_rows_for_rebuild: 1_000,
        }
    }
}

/// Advisory outcome of a maintenance check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAdvice {
    /// No action needed.
    Healthy,
    /// Vacuum tombstones.
    CleanupTombstones {
        /// Current tombstone count.
        tombstones: u64,
    },
    /// Rebuild the index.
    RebuildIndex {
        /// Fragmentation permille (0..=1000).
        fragmentation: u32,
    },
}

impl MaintenanceAdvice {
    /// Fragmentation as permille for stable formatting without floats.
    fn frag_permille(f: f64) -> u32 {
        (f.clamp(0.0, 1.0) * 1000.0).round() as u32
    }
}

/// Fraction of slots that are dead or wasted: `1 - live/total`.
/// Returns `0.0` for an empty store.
#[must_use]
pub fn fragmentation_metric(total_rows: u64, live_rows: u64) -> f64 {
    fragmentation_metric_full(total_rows, live_rows, 0)
}

/// Fragmentation including explicit tombstones:
/// `(total - live + tombstones) / (total + tombstones)`, clamped.
#[must_use]
pub fn fragmentation_metric_full(total_rows: u64, live_rows: u64, tombstones: u64) -> f64 {
    let denom = total_rows.saturating_add(tombstones);
    if denom == 0 {
        return 0.0;
    }
    let live = live_rows.min(denom);
    1.0 - (live as f64 / denom as f64)
}

/// Tombstone ratio: `tombstones / total_rows` (`0.0` when empty).
#[must_use]
pub fn tombstone_ratio(tombstones: u64, total_rows: u64) -> f64 {
    if total_rows == 0 {
        return 0.0;
    }
    tombstones as f64 / total_rows as f64
}

/// `true` when tombstone pressure crosses count or ratio thresholds.
#[must_use]
pub fn should_cleanup_tombstones(
    tombstones: u64,
    total_rows: u64,
    cfg: &MaintenanceConfig,
) -> bool {
    tombstones >= cfg.tombstone_count_cleanup
        || tombstone_ratio(tombstones, total_rows) >= cfg.tombstone_ratio_cleanup
}

/// `true` when fragmentation crosses the rebuild threshold and the
/// store is large enough to justify a rebuild.
#[must_use]
pub fn should_rebuild(
    total_rows: u64,
    live_rows: u64,
    tombstones: u64,
    cfg: &MaintenanceConfig,
) -> bool {
    if total_rows < cfg.min_rows_for_rebuild {
        return false;
    }
    fragmentation_metric_full(total_rows, live_rows, tombstones) >= cfg.rebuild_fragmentation
}

/// Single advisory combining both checks (cleanup wins ties).
#[must_use]
pub fn advise(
    total_rows: u64,
    live_rows: u64,
    tombstones: u64,
    cfg: &MaintenanceConfig,
) -> MaintenanceAdvice {
    if should_cleanup_tombstones(tombstones, total_rows, cfg) {
        return MaintenanceAdvice::CleanupTombstones { tombstones };
    }
    if should_rebuild(total_rows, live_rows, tombstones, cfg) {
        return MaintenanceAdvice::RebuildIndex {
            fragmentation: MaintenanceAdvice::frag_permille(fragmentation_metric_full(
                total_rows, live_rows, tombstones,
            )),
        };
    }
    MaintenanceAdvice::Healthy
}
