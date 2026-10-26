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
//! Cost-model statistics for vector planning (Phase 33 in code).
//!
//! [`VectorStats`] is the single observation record the planner consumes:
//! working-set cardinality, live/dead/null shape, zero-norm share,
//! indexed share, dimension, build watermark (freshness), last observed
//! filter selectivity, and last measured recall — each with a defined
//! "unknown" state. The planner treats unknown as *conservative*
//! (exact-leaning, never wrong), never as zero.
//!
//! Freshness: statistics are collected at ANN-graph build time and die
//! with the cache entry (watermark invalidation), so [`VectorStats::is_fresh`]
//! against the current watermark is the only validity check needed.
//! The versioned [`encode`](VectorStats::encode)/[`decode`](VectorStats::decode)
//! framing exists for future manifest embedding; statistics are derived
//! and rebuildable, never authoritative.

use crate::{Result, VectorError};

/// Statistics encoding version. Bump on any layout change; older readers
/// fail closed instead of reinterpreting bytes.
pub const STATS_VERSION: u32 = 1;

/// Sentinel: no selectivity has been observed.
pub const SELECTIVITY_UNKNOWN: f64 = -1.0;
/// Sentinel: no recall has been measured.
pub const RECALL_UNKNOWN: f32 = -1.0;

/// Encoded length: version u32 + seven u64 + dimension u32 + selectivity
/// f64 + recall f32 + candidate u32.
pub const STATS_LEN: usize = 4 + 8 * 7 + 4 + 8 + 4 + 4;

/// One observation record for a vector column working set.
///
/// All counts are plain `u64`; fractions derive on demand so the encoding
/// stays total (no NaN fields). `selectivity`/`recall_at_k` use negative
/// sentinels for "unknown" — never `0.0`, which would mean "matches
/// nothing" / "total failure" and would mislead the planner.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorStats {
    /// Encoding version (always [`STATS_VERSION`]).
    pub version: u32,
    /// Working-set rows observed (including NULLs).
    pub row_count: u64,
    /// Non-NULL rows observed.
    pub live_rows: u64,
    /// NULL rows skipped during the build scan.
    pub null_count: u64,
    /// Zero-norm rows (cosine-undefined, excluded from cosine indexes).
    pub zero_norm_count: u64,
    /// Rows actually indexed (live minus skipped kinds).
    pub indexed_rows: u64,
    /// Vector dimension of the observed working set.
    pub dimension: u32,
    /// Committed-write watermark the observation was taken at.
    pub build_watermark: u64,
    /// Local monotonic tick ordering observations (freshness tie-break).
    pub stats_tick: u64,
    /// Last observed filter selectivity in `0.0..=1.0`, or
    /// [`SELECTIVITY_UNKNOWN`].
    pub selectivity: f64,
    /// Last measured Recall@k in `0.0..=1.0`, or [`RECALL_UNKNOWN`].
    pub recall_at_k: f32,
    /// Candidate depth the recall measurement used (0 when unmeasured).
    pub candidate_k: u32,
}

impl VectorStats {
    /// Empty observation: every count zero, selectivity/recall unknown.
    ///
    /// The planner treats this as "no information" (exact-leaning with a
    /// stated reason), never as an empty table.
    #[must_use]
    pub fn unknown(dimension: u32) -> Self {
        Self {
            version: STATS_VERSION,
            row_count: 0,
            live_rows: 0,
            null_count: 0,
            zero_norm_count: 0,
            indexed_rows: 0,
            dimension,
            build_watermark: 0,
            stats_tick: 0,
            selectivity: SELECTIVITY_UNKNOWN,
            recall_at_k: RECALL_UNKNOWN,
            candidate_k: 0,
        }
    }

    /// Null fraction in `0.0..=1.0` (`0.0` when nothing observed).
    #[must_use]
    pub fn null_fraction(&self) -> f64 {
        if self.row_count == 0 {
            return 0.0;
        }
        self.null_count as f64 / self.row_count as f64
    }

    /// Zero-norm fraction of live rows (`0.0` when none live).
    #[must_use]
    pub fn zero_norm_fraction(&self) -> f64 {
        if self.live_rows == 0 {
            return 0.0;
        }
        self.zero_norm_count as f64 / self.live_rows as f64
    }

    /// Indexed share of live rows (`0.0` when none live).
    #[must_use]
    pub fn indexed_fraction(&self) -> f64 {
        if self.live_rows == 0 {
            return 0.0;
        }
        (self.indexed_rows.min(self.live_rows)) as f64 / self.live_rows as f64
    }

    /// Whether selectivity was actually observed (not the sentinel).
    #[must_use]
    pub fn has_selectivity(&self) -> bool {
        self.selectivity >= 0.0 && self.selectivity <= 1.0
    }

    /// Whether recall was actually measured (not the sentinel).
    #[must_use]
    pub fn has_recall(&self) -> bool {
        self.recall_at_k >= 0.0 && self.recall_at_k <= 1.0
    }

    /// Whether anything was observed at all.
    #[must_use]
    pub fn is_observed(&self) -> bool {
        self.row_count > 0
    }

    /// Fresh when built at `watermark` (same committed state).
    ///
    /// Any committed write bumps the watermark and invalidates the cache
    /// entry carrying these statistics, so equality — not ordering — is
    /// the correct check.
    #[must_use]
    pub fn is_fresh(&self, watermark: u64) -> bool {
        self.is_observed() && self.build_watermark == watermark
    }

    /// Record one observed row: exactly one of the flags applies.
    pub fn observe_row(&mut self, null: bool, zero_norm: bool, indexed: bool) {
        self.row_count = self.row_count.saturating_add(1);
        if null {
            self.null_count = self.null_count.saturating_add(1);
            return;
        }
        self.live_rows = self.live_rows.saturating_add(1);
        if zero_norm {
            self.zero_norm_count = self.zero_norm_count.saturating_add(1);
        }
        if indexed {
            self.indexed_rows = self.indexed_rows.saturating_add(1);
        }
    }

    /// Record an observed filter selectivity (clamped to `0.0..=1.0`;
    /// non-finite input clears back to unknown).
    pub fn observe_selectivity(&mut self, selectivity: f64) {
        if selectivity.is_finite() {
            self.selectivity = selectivity.clamp(0.0, 1.0);
        } else {
            self.selectivity = SELECTIVITY_UNKNOWN;
        }
    }

    /// Record a measured recall at a candidate depth (validated ranges;
    /// anything else clears back to unknown rather than storing lies).
    pub fn observe_recall(&mut self, recall_at_k: f32, candidate_k: u32) {
        if recall_at_k.is_finite() && (0.0..=1.0).contains(&recall_at_k) && candidate_k > 0 {
            self.recall_at_k = recall_at_k;
            self.candidate_k = candidate_k;
        } else {
            self.recall_at_k = RECALL_UNKNOWN;
            self.candidate_k = 0;
        }
    }

    /// Expected surviving rows under `selectivity` (`None` when unknown).
    #[must_use]
    pub fn expected_rows(&self, selectivity: f64) -> Option<u64> {
        if !(0.0..=1.0).contains(&selectivity) || !selectivity.is_finite() {
            return None;
        }
        Some((self.live_rows as f64 * selectivity).floor() as u64)
    }

    /// Deterministic little-endian encoding (fixed field order, no padding).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(STATS_LEN);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.live_rows.to_le_bytes());
        out.extend_from_slice(&self.null_count.to_le_bytes());
        out.extend_from_slice(&self.zero_norm_count.to_le_bytes());
        out.extend_from_slice(&self.indexed_rows.to_le_bytes());
        out.extend_from_slice(&self.dimension.to_le_bytes());
        out.extend_from_slice(&self.build_watermark.to_le_bytes());
        out.extend_from_slice(&self.stats_tick.to_le_bytes());
        out.extend_from_slice(&self.selectivity.to_le_bytes());
        out.extend_from_slice(&self.recall_at_k.to_le_bytes());
        out.extend_from_slice(&self.candidate_k.to_le_bytes());
        out
    }

    /// Decode [`encode`](Self::encode) output; version skew and trailing
    /// bytes fail closed.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const LEN: usize = STATS_LEN;
        if bytes.len() != LEN {
            return Err(VectorError::Corruption(format!(
                "stats length {} != {LEN}",
                bytes.len()
            )));
        }
        let mut off = 0usize;
        // Straight-line decode keeps field order obvious and auditable.
        let u32_of = |bytes: &[u8], off: &mut usize| -> Result<u32> {
            let b: [u8; 4] = bytes[*off..*off + 4]
                .try_into()
                .map_err(|_| VectorError::Corruption("stats int".to_string()))?;
            *off += 4;
            Ok(u32::from_le_bytes(b))
        };
        let u64_of = |bytes: &[u8], off: &mut usize| -> Result<u64> {
            let b: [u8; 8] = bytes[*off..*off + 8]
                .try_into()
                .map_err(|_| VectorError::Corruption("stats int".to_string()))?;
            *off += 8;
            Ok(u64::from_le_bytes(b))
        };
        let f64_of = |bytes: &[u8], off: &mut usize| -> Result<f64> {
            let b: [u8; 8] = bytes[*off..*off + 8]
                .try_into()
                .map_err(|_| VectorError::Corruption("stats float".to_string()))?;
            *off += 8;
            Ok(f64::from_le_bytes(b))
        };
        let f32_of = |bytes: &[u8], off: &mut usize| -> Result<f32> {
            let b: [u8; 4] = bytes[*off..*off + 4]
                .try_into()
                .map_err(|_| VectorError::Corruption("stats float".to_string()))?;
            *off += 4;
            Ok(f32::from_le_bytes(b))
        };
        let version = u32_of(bytes, &mut off)?;
        if version != STATS_VERSION {
            return Err(VectorError::Corruption(format!(
                "unsupported stats version {version}"
            )));
        }
        let stats = Self {
            version,
            row_count: u64_of(bytes, &mut off)?,
            live_rows: u64_of(bytes, &mut off)?,
            null_count: u64_of(bytes, &mut off)?,
            zero_norm_count: u64_of(bytes, &mut off)?,
            indexed_rows: u64_of(bytes, &mut off)?,
            dimension: u32_of(bytes, &mut off)?,
            build_watermark: u64_of(bytes, &mut off)?,
            stats_tick: u64_of(bytes, &mut off)?,
            selectivity: f64_of(bytes, &mut off)?,
            recall_at_k: f32_of(bytes, &mut off)?,
            candidate_k: u32_of(bytes, &mut off)?,
        };
        debug_assert_eq!(off, LEN);
        Ok(stats)
    }
}
