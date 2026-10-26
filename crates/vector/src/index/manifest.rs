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
//! Persisted index descriptor. Mirrors the `admin::IndexManifest` shape so a
//! future admin crate can reuse the binary layout: version + kind + metric +
//! dimension + generation + counters + opaque params.
//!
//! NON-AUTHORITATIVE (see [`crate::durability`]): describes a rebuildable
//! derived image only. Catalog/generation authority lives in the storage
//! engine; a missing or mismatched manifest forces rebuild from
//! authoritative rows, never data loss.

use crate::{Result, VectorError};

use super::common::{enc_u32, enc_u64, enc_usize, Decoder};
use super::{IndexKind, Metric};

/// Persisted manifest format version.
///
/// Bumped whenever the on-disk layout changes; older readers fail closed
/// with `IndexCorrupted` instead of reinterpreting bytes.
pub const MANIFEST_VERSION: u32 = 1;

/// Checkpointed descriptor persisted alongside every index artifact.
///
/// Records the (kind, metric, dimension) identity triple plus generation
/// and counters, so WAL recovery and replica promotion can distinguish a
/// valid READY index from one that must rebuild. Encoded with explicit
/// little-endian framing; layout changes bump [`MANIFEST_VERSION`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexManifest {
    /// Index implementation selector (Flat/HNSW/IVF/PQ/DiskANN/sparse/binary).
    pub kind: IndexKind,
    /// Fixed distance metric; must match the query metric.
    pub metric: Metric,
    /// Vector dimension; part of the type identity (1..=65536).
    pub dimension: usize,
    /// Monotonic mutation counter; persisted for crash-recovery ordering.
    pub generation: u64,
    /// Total entries stored, including tombstones awaiting compaction.
    pub vector_count: usize,
    /// Tombstoned entries awaiting background compaction.
    pub deleted_count: usize,
}

impl IndexManifest {
    /// Snapshots index identity for checkpoint/recovery.
    ///
    /// Records the (kind, metric, dimension) triple plus generation and
    /// counters so recovery can detect mismatches and choose rebuild.
    #[must_use]
    pub const fn new(kind: IndexKind, metric: Metric, dimension: usize, generation: u64) -> Self {
        Self {
            kind,
            metric,
            dimension,
            generation,
            vector_count: 0,
            deleted_count: 0,
        }
    }

    fn kind_code(kind: IndexKind) -> u32 {
        match kind {
            IndexKind::Flat => 1,
            IndexKind::Hnsw => 2,
            IndexKind::IvfFlat => 3,
            IndexKind::IvfSq => 4,
            IndexKind::IvfPq => 5,
            IndexKind::DiskAnn => 6,
            IndexKind::SparseInverted => 7,
            IndexKind::Binary => 8,
        }
    }

    fn kind_from(code: u32) -> Result<IndexKind> {
        match code {
            1 => Ok(IndexKind::Flat),
            2 => Ok(IndexKind::Hnsw),
            3 => Ok(IndexKind::IvfFlat),
            4 => Ok(IndexKind::IvfSq),
            5 => Ok(IndexKind::IvfPq),
            6 => Ok(IndexKind::DiskAnn),
            7 => Ok(IndexKind::SparseInverted),
            8 => Ok(IndexKind::Binary),
            _ => Err(VectorError::IndexCorrupted(format!(
                "unknown index kind {code}"
            ))),
        }
    }

    fn metric_code(m: Metric) -> u32 {
        match m {
            Metric::Cosine => 1,
            Metric::L2 => 2,
            Metric::Dot => 3,
            Metric::Hamming => 4,
            Metric::Jaccard => 5,
        }
    }

    fn metric_from(code: u32) -> Result<Metric> {
        match code {
            1 => Ok(Metric::Cosine),
            2 => Ok(Metric::L2),
            3 => Ok(Metric::Dot),
            4 => Ok(Metric::Hamming),
            5 => Ok(Metric::Jaccard),
            _ => Err(VectorError::IndexCorrupted(format!(
                "unknown metric {code}"
            ))),
        }
    }

    /// encode: see implementation for contract.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(40);
        enc_u32(&mut out, MANIFEST_VERSION);
        enc_u32(&mut out, Self::kind_code(self.kind));
        enc_u32(&mut out, Self::metric_code(self.metric));
        enc_usize(&mut out, self.dimension);
        enc_u64(&mut out, self.generation);
        enc_usize(&mut out, self.vector_count);
        enc_usize(&mut out, self.deleted_count);
        out
    }

    /// decode: see implementation for contract.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let v = d.u32()?;
        if v != MANIFEST_VERSION {
            return Err(VectorError::IndexCorrupted(format!(
                "unsupported manifest version {v}"
            )));
        }
        let kind = Self::kind_from(d.u32()?)?;
        let metric = Self::metric_from(d.u32()?)?;
        let dimension = d.usize_val()?;
        let generation = d.u64()?;
        let vector_count = d.usize_val()?;
        let deleted_count = d.usize_val()?;
        d.finish()?;
        Ok(Self {
            kind,
            metric,
            dimension,
            generation,
            vector_count,
            deleted_count,
        })
    }
}
