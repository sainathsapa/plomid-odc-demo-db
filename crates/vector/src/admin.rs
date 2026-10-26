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
//! Index lifecycle: persistent states, manifests, and statistics.
//!
//! State machine: `Absent → Building → CatchingUp → Ready ⇄ Degraded`,
//! with `Rebuilding` and terminal `Failed`. Recovery yields either
//! `Ready` or a valid rebuild-required marker — never silent corruption.

use crate::error::{Result, VectorError};
use crate::r#type::{VectorMetric, VectorType};

/// Persistent lifecycle state of one vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum IndexState {
    /// No index files exist.
    #[default]
    Absent,
    /// Initial snapshot scan / graph build in progress.
    Building,
    /// Base graph built; applying the concurrent mutation delta.
    CatchingUp,
    /// Serving queries.
    Ready,
    /// Serving with reduced recall/availability; rebuild advised.
    Degraded,
    /// Online rebuild in progress (old image still serves).
    Rebuilding,
    /// Build or validation failed; queries must not use the index.
    Failed,
}

impl IndexState {
    /// Canonical uppercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "ABSENT",
            Self::Building => "BUILDING",
            Self::CatchingUp => "CATCHING_UP",
            Self::Ready => "READY",
            Self::Degraded => "DEGRADED",
            Self::Rebuilding => "REBUILDING",
            Self::Failed => "FAILED",
        }
    }

    /// Parse [`Self::as_str`] output (case-insensitive).
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_uppercase().as_str() {
            "ABSENT" => Ok(Self::Absent),
            "BUILDING" => Ok(Self::Building),
            "CATCHING_UP" | "CATCHINGUP" => Ok(Self::CatchingUp),
            "READY" => Ok(Self::Ready),
            "DEGRADED" => Ok(Self::Degraded),
            "REBUILDING" => Ok(Self::Rebuilding),
            "FAILED" => Ok(Self::Failed),
            other => Err(VectorError::IndexStateInvalid(format!(
                "unknown index state '{other}'"
            ))),
        }
    }

    /// True when the planner may route ANN queries here.
    #[must_use]
    pub const fn is_queryable(self) -> bool {
        matches!(self, Self::Ready | Self::Degraded)
    }
}

/// Checkpointed identity + parameters of one vector index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexManifest {
    /// Catalog index name.
    pub name: String,
    /// Lifecycle state at checkpoint time.
    pub state: IndexState,
    /// Indexed column type identity.
    pub vector_type: VectorType,
    /// Indexed distance metric.
    pub metric: VectorMetric,
    /// Manifest format version (currently 1).
    pub format_version: u32,
    /// Recovery position of the source data snapshot.
    pub checkpoint_lsn: u64,
    /// Builder parameters (opaque, e.g. `"m=16,ef_construction=200"`).
    pub build_params: String,
}

impl IndexManifest {
    /// Validate before persist or publish.
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() || self.name.len() > 128 {
            return Err(VectorError::IndexParameterInvalid(
                "index name must be 1..=128 chars".into(),
            ));
        }
        if self.format_version != 1 {
            return Err(VectorError::IndexCorrupted(format!(
                "unsupported manifest version {}",
                self.format_version
            )));
        }
        self.vector_type.validate()?;
        if !self.vector_type.supports_exact(self.metric) {
            return Err(VectorError::MetricMismatch(format!(
                "metric {:?} unsupported for {}",
                self.metric,
                self.vector_type.sql_name()
            )));
        }
        Ok(())
    }
}

/// Runtime statistics for diagnostics and rebuild heuristics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IndexStats {
    /// Indexed (live) vector count.
    pub vector_count: u64,
    /// Tombstoned entries awaiting compaction.
    pub tombstone_count: u64,
    /// On-disk + in-memory footprint.
    pub index_bytes: u64,
    /// Last successful build latency.
    pub build_latency_us: u64,
}

impl IndexStats {
    /// Tombstone permille `0..=1000` for compaction heuristics.
    #[must_use]
    pub fn tombstone_ratio_x1000(&self) -> u64 {
        if self.vector_count == 0 {
            return 0;
        }
        (self.tombstone_count.saturating_mul(1000) / self.vector_count).min(1000)
    }
}
