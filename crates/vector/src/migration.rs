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
//! Single-node migration matrix (Gap N).
//!
//! Precise compatibility answers for every versioned vector artifact —
//! no "should work", no "probably compatible". Categories:
//!
//! * [`Compatibility::Supported`] — open, query, mutate, maintain.
//! * [`Compatibility::SupportedWithMigration`] — reserved: no migration
//!   exists today (every version is 1); the category, policy hook, and
//!   test shape exist so a future `v1→v2` lands without redesign.
//! * [`Compatibility::RebuildRequired`] — never served; rebuild from
//!   authoritative rows via `run_online_build`, then validate, then enable.
//! * [`Compatibility::NotSupported`] — fail closed, never touched.
//!
//! The matrix keys on `(artifact, magic, version)` because version `1`
//! alone is ambiguous across domains. It reuses existing decoders —
//! there is no duplicate migration system and no converter tool: the
//! migration vehicle is backup/restore plus rebuild from rows.

use crate::index::IndexKind;
use crate::{Result, VectorError};

/// Compatibility verdict for one version transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compatibility {
    /// Open, query, mutate, maintain, restart, recover.
    Supported,
    /// Opens after a defined migration (none defined today).
    SupportedWithMigration,
    /// Must rebuild from authoritative rows; never served before that.
    RebuildRequired,
    /// Fail closed.
    NotSupported,
}

impl Compatibility {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::SupportedWithMigration => "supported-with-migration",
            Self::RebuildRequired => "rebuild-required",
            Self::NotSupported => "not-supported",
        }
    }

    /// Whether the artifact may be opened at all.
    #[must_use]
    pub const fn can_open(self) -> bool {
        matches!(self, Self::Supported | Self::SupportedWithMigration)
    }

    /// Whether queries may be served without further work.
    #[must_use]
    pub const fn can_query(self) -> bool {
        matches!(self, Self::Supported)
    }

    /// Whether the artifact must be rebuilt before serving.
    #[must_use]
    pub const fn must_rebuild(self) -> bool {
        matches!(self, Self::RebuildRequired)
    }
}

/// One versioned artifact in the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionedArtifact {
    /// Standalone/row codec (`PVEC`, `VERSION: u8`).
    Codec,
    /// `VectorManifest` (`PLMM`, u32 v1).
    StorageManifest,
    /// Storage pages (`PLMV`, u16 v1).
    StoragePage,
    /// Read snapshot (`PLMS`, u32 v1).
    Snapshot,
    /// Persisted index descriptor (LE u32 v1, no magic).
    IndexManifest,
    /// HNSW graph blob (u32 v1).
    Hnsw,
    /// IVF-Flat blob (u32 v1).
    Ivf,
    /// SQ blob (u32 v1).
    Sq,
    /// PQ blob (u32 v1).
    Pq,
    /// Binary blob (u32 v1).
    Binary,
    /// DiskANN graph blob (u32 v1 + `DANN` records).
    DiskAnn,
    /// Planner statistics blob (u32 v1).
    Stats,
    /// Backup container (`PLMB`, u32 v1, double-gated).
    Backup,
}

impl VersionedArtifact {
    /// Stable identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Codec => "codec",
            Self::StorageManifest => "storage-manifest",
            Self::StoragePage => "storage-page",
            Self::Snapshot => "snapshot",
            Self::IndexManifest => "index-manifest",
            Self::Hnsw => "hnsw",
            Self::Ivf => "ivf",
            Self::Sq => "sq",
            Self::Pq => "pq",
            Self::Binary => "binary",
            Self::DiskAnn => "diskann",
            Self::Stats => "stats",
            Self::Backup => "backup",
        }
    }

    /// Current version this binary reads and writes.
    #[must_use]
    pub const fn current_version(self) -> u64 {
        match self {
            Self::Codec => crate::codec::VERSION as u64,
            Self::StorageManifest => crate::storage::manifest::MANIFEST_FORMAT_VERSION as u64,
            Self::StoragePage => crate::storage::pages::PAGE_FORMAT_VERSION as u64,
            Self::Snapshot => crate::storage::snapshot::SNAPSHOT_VERSION as u64,
            Self::IndexManifest => crate::index::manifest::MANIFEST_VERSION as u64,
            Self::Hnsw => crate::index::hnsw::HNSW_VERSION as u64,
            Self::Ivf => crate::index::ivf::IVF_VERSION as u64,
            Self::Sq => crate::index::sq::SQ_VERSION as u64,
            Self::Pq => crate::index::pq::PQ_VERSION as u64,
            Self::Binary => crate::index::binary::BINARY_VERSION as u64,
            Self::DiskAnn => crate::index::diskann::DISKANN_VERSION as u64,
            Self::Stats => crate::stats::STATS_VERSION as u64,
            Self::Backup => crate::backup::BACKUP_FORMAT_VERSION as u64,
        }
    }

    /// Every artifact in the matrix.
    #[must_use]
    pub const fn all() -> [Self; 13] {
        [
            Self::Codec,
            Self::StorageManifest,
            Self::StoragePage,
            Self::Snapshot,
            Self::IndexManifest,
            Self::Hnsw,
            Self::Ivf,
            Self::Sq,
            Self::Pq,
            Self::Binary,
            Self::DiskAnn,
            Self::Stats,
            Self::Backup,
        ]
    }
}

/// Transition verdict for persisted bytes at `found_version`.
///
/// * Equal to current: [`Compatibility::Supported`].
/// * Lower (older than current — none exist yet): [`Compatibility::RebuildRequired`]
///   for indexes (rows re-encode on rewrite), [`Compatibility::NotSupported`]
///   for framing artifacts (codec/manifest/page/snapshot/backup), which
///   have no row-level rebuild path.
/// * Higher (newer than this binary): always [`Compatibility::NotSupported`]
///   — never reinterpret bytes from the future.
#[must_use]
pub const fn check_transition(artifact: VersionedArtifact, found_version: u64) -> Compatibility {
    let current = artifact.current_version();
    if found_version == current {
        return Compatibility::Supported;
    }
    if found_version > current {
        return Compatibility::NotSupported;
    }
    match artifact {
        VersionedArtifact::Hnsw
        | VersionedArtifact::Ivf
        | VersionedArtifact::Sq
        | VersionedArtifact::Pq
        | VersionedArtifact::Binary
        | VersionedArtifact::DiskAnn
        | VersionedArtifact::IndexManifest => Compatibility::RebuildRequired,
        _ => Compatibility::NotSupported,
    }
}

/// Classify one persisted index blob for serving.
///
/// Reads the leading version u32 every index `encode()` writes first:
/// match ⇒ [`Compatibility::Supported`] (caller still runs decode +
/// `validate()` before serving); older ⇒ [`Compatibility::RebuildRequired`];
/// newer or truncated ⇒ [`Compatibility::NotSupported`]. Truncation (< 4
/// bytes) is corruption, never a version verdict.
pub fn classify_index_blob(kind: IndexKind, blob: &[u8]) -> Compatibility {
    if blob.len() < 4 {
        return Compatibility::NotSupported;
    }
    let found = u32::from_le_bytes([blob[0], blob[1], blob[2], blob[3]]) as u64;
    let artifact = match kind {
        IndexKind::Flat | IndexKind::SparseInverted => return Compatibility::RebuildRequired,
        IndexKind::Hnsw => VersionedArtifact::Hnsw,
        IndexKind::IvfFlat => VersionedArtifact::Ivf,
        IndexKind::IvfSq => VersionedArtifact::Sq,
        IndexKind::IvfPq => VersionedArtifact::Pq,
        IndexKind::DiskAnn => VersionedArtifact::DiskAnn,
        IndexKind::Binary => VersionedArtifact::Binary,
    };
    check_transition(artifact, found)
}

/// Enforce a transition verdict: `Supported` passes, anything else becomes
/// the defined typed error (rebuild-required vs corruption are distinct
/// `VECTOR_*` codes so operators can tell them apart without parsing).
pub fn enforce(artifact: VersionedArtifact, found_version: u64) -> Result<()> {
    match check_transition(artifact, found_version) {
        Compatibility::Supported => Ok(()),
        Compatibility::RebuildRequired => Err(VectorError::RebuildRequired(format!(
            "{} v{found_version} is older than v{}: rebuild from authoritative rows",
            artifact.as_str(),
            artifact.current_version()
        ))),
        Compatibility::SupportedWithMigration => Err(VectorError::RebuildRequired(format!(
            "{} v{found_version}: no migration defined yet",
            artifact.as_str()
        ))),
        Compatibility::NotSupported => Err(VectorError::IndexCorrupted(format!(
            "{} v{found_version} is not supported by this binary (current v{})",
            artifact.as_str(),
            artifact.current_version()
        ))),
    }
}

/// One matrix row for documentation/diagnostics export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatrixRow {
    /// Artifact identifier.
    pub artifact: &'static str,
    /// Current version.
    pub current: u64,
    /// Same-version verdict.
    pub same: Compatibility,
    /// Older-version verdict.
    pub older: Compatibility,
    /// Newer-version verdict.
    pub newer: Compatibility,
    /// Rollback possible (downgrade reads what this version wrote).
    pub rollback_possible: bool,
    /// Backup required before touching.
    pub backup_required: bool,
}

/// Full matrix: every artifact × {same, older, newer}.
#[must_use]
pub fn matrix() -> Vec<MatrixRow> {
    VersionedArtifact::all()
        .iter()
        .map(|a| {
            let current = a.current_version();
            // Rollback (a newer binary's output read back here) is possible
            // only while versions are equal — any bump breaks it, by design
            // (fail closed beats silent reinterpretation).
            MatrixRow {
                artifact: a.as_str(),
                current,
                same: Compatibility::Supported,
                older: check_transition(*a, current.saturating_sub(1)),
                newer: check_transition(*a, current + 1),
                rollback_possible: false,
                backup_required: true,
            }
        })
        .collect()
}

/// Deterministic diagnostic lines for the whole matrix.
#[must_use]
pub fn explain_matrix() -> Vec<String> {
    let mut lines = vec!["Migration matrix (single-node, backup-or-rebuild vehicle):".to_string()];
    for row in matrix() {
        lines.push(format!(
            "{} v{}: same={} older={} newer={} rollback={} backup-required={}",
            row.artifact,
            row.current,
            row.same.as_str(),
            row.older.as_str(),
            row.newer.as_str(),
            row.rollback_possible,
            row.backup_required,
        ));
    }
    lines.push("No migrations defined (all v1); newer versions fail closed.".to_string());
    lines
}
