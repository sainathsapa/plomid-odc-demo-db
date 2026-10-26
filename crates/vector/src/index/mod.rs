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
//! Vector index abstractions: trait, config, stats and implementations.
//!
//! This module is the single seam between the SQL/executor layers and every
//! ANN structure in PLOMID. The executor never touches HNSW graphs, IVF
//! posting lists, or PQ codebooks directly: it builds an [`IndexConfig`],
//! drives the [`VectorIndex`] trait, and reads back [`SearchResult`] rows
//! ordered by ascending distance. MVCC visibility and exact reranking stay
//! in the executor; the index only proposes candidates by stable [`RowId`].
//!
//! Submodules: `flat` (brute force oracle), `hnsw`, `ivf` (IVF-Flat), `pq`,
//! `sq`, `diskann`, `binary`, plus `common` (tombstones, RNG, codec,
//! distances) and `manifest` (persisted descriptor).

pub mod binary;
pub mod common;
pub mod concurrent;
pub mod diskann;
pub mod flat;
pub mod hnsw;
pub mod ivf;
pub mod manifest;
pub mod online_build;
pub mod pq;
pub mod sq;

pub use common::TombstoneMap;
pub use concurrent::ConcurrentIndex;
pub use flat::FlatIndex;
pub use manifest::IndexManifest;

use crate::{Result, VectorError};

/// Row identifier addressed by every index.
///
/// This is the database's stable logical row identity (never a physical
/// page offset). Index entries are always revalidated against MVCC
/// visibility before a row is returned, so a stale entry can never surface
/// an uncommitted or deleted version.
pub type RowId = u64;

/// A single search hit.
///
/// `distance` is lower-is-better for every metric: L2 and cosine distance
/// are natively ascending, and inner product is stored negated (`-dot`) so
/// that a plain ascending `ORDER BY` remains a nearest-neighbour operation.
/// Ties are broken by ascending `row_id` for deterministic output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SearchResult {
    /// Stable logical row identity of the candidate row.
    pub row_id: RowId,
    /// Ascending distance (or negated similarity for dot-product indexes).
    pub distance: f32,
}

impl SearchResult {
    /// Builds a candidate hit from a row identity and an ascending distance.
    ///
    /// Callers must supply the metric-consistent distance (negate raw inner
    /// products first); the constructor performs no conversion so that hot
    /// search loops avoid redundant branches.
    #[must_use]
    pub const fn new(row_id: RowId, distance: f32) -> Self {
        Self { row_id, distance }
    }
}

impl Eq for SearchResult {}

/// Distance metric selector.
///
/// The metric is fixed at index build time and must match the query metric:
/// a cosine index never answers an L2 query. Hamming/Jaccard apply to
/// binary vectors; dense float indexes use Cosine/L2/Dot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metric {
    /// Cosine distance (`1 - cosine_similarity`); zero-norm vectors are
    /// excluded from the indexed set and surface as SQL NULL.
    Cosine,
    /// Squared-then-rooted Euclidean distance over `f32` lanes.
    L2,
    /// Negative inner product (`-dot`), so ascending order is nearest-first.
    Dot,
    /// Bit-level Hamming distance over packed binary vectors.
    Hamming,
    /// Jaccard distance (`1 - |A&B|/|A|B|`) over binary sets; both-empty is 0.
    Jaccard,
}

impl Metric {
    /// Whether smaller values mean more similar.
    ///
    /// Always true by construction: dot-product indexes store the negated
    /// inner product precisely so that every metric shares one ascending
    /// ranking direction and one Top-K implementation.
    #[must_use]
    pub const fn lower_is_better(self) -> bool {
        let _ = self;
        true
    }
}

/// Index implementation selector.
///
/// `Flat` is the exact oracle (brute force, always correct). The remaining
/// kinds are approximate unless followed by exact rerank from authoritative
/// table rows. `DiskAnn` targets SSD-resident estates larger than RAM.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IndexKind {
    /// Brute-force exact scan; the correctness baseline for recall tests.
    Flat,
    /// Hierarchical navigable small-world graph (first production ANN).
    Hnsw,
    /// Inverted file with exact per-list vectors (k-means centroids,
    /// posting lists, `nprobe`).
    IvfFlat,
    /// Global SQ8 scalar-quantized scan. The name is a compatibility label:
    /// no coarse quantizer or posting lists exist in v1.
    IvfSq,
    /// Global 8-bit product-quantized ADC scan. The name is a compatibility
    /// label: no coarse quantizer or posting lists exist in v1.
    IvfPq,
    /// SSD-oriented graph with bounded RAM resident set.
    DiskAnn,
    /// Sparse inverted index over non-zero coordinates.
    SparseInverted,
    /// Binary bit-packed scan / partitioned index.
    Binary,
}

/// Tunable build/search parameters.
///
/// Every field is optional: `None` selects the per-index production
/// default (documented on each index type, e.g. HNSW `m = 16`,
/// `ef_construction = 200`). Explicit values are validated on build and
/// rejected with `IndexParameterInvalid` instead of being clamped.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IndexParams {
    /// HNSW graph degree (typical 8..64, default 16).
    pub m: Option<usize>,
    /// HNSW build beam width (typical 64..512, default 200).
    pub ef_construction: Option<usize>,
    /// HNSW query beam width (typical 16..512, default 100).
    pub ef_search: Option<usize>,
    /// IVF list count (coarse centroids).
    pub nlist: Option<usize>,
    /// IVF probed lists per query.
    pub nprobe: Option<usize>,
    /// PQ subvector segment count (must divide the dimension).
    pub n_segments: Option<usize>,
    /// Deterministic seed for k-means / level sampling / codebook training.
    pub seed: Option<u64>,
    /// Reserved for forward-compatible extras (`key=value` pairs).
    ///
    /// Unknown keys are preserved verbatim in the manifest so that a newer
    /// reader can round-trip parameters an older writer did not understand.
    pub extra: Vec<(String, String)>,
}

impl IndexParams {
    /// Empty parameters: every index applies its own production defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            m: None,
            ef_construction: None,
            ef_search: None,
            nlist: None,
            nprobe: None,
            n_segments: None,
            seed: None,
            extra: Vec::new(),
        }
    }
}

/// Configuration for building or opening an index.
///
/// The triple `(kind, metric, dimension)` fully determines layout
/// compatibility: opening persisted bytes with a mismatched triple fails
/// closed with `IndexCorrupted` instead of returning wrong neighbours.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexConfig {
    /// Which ANN structure to build.
    pub kind: IndexKind,
    /// Fixed distance metric for this index.
    pub metric: Metric,
    /// Vector dimension this index was trained for.
    pub dimension: usize,
    /// Tunable build/search parameters.
    pub params: IndexParams,
}

impl IndexConfig {
    /// Builds a configuration with default parameters.
    ///
    /// Dimensional validation is deferred to [`validate`](Self::validate)
    /// so that configs can be constructed before the column typmod is known.
    #[must_use]
    pub const fn new(kind: IndexKind, metric: Metric, dimension: usize) -> Self {
        Self {
            kind,
            metric,
            dimension,
            params: IndexParams::new(),
        }
    }

    /// Validates dimension bounds and resource safety.
    ///
    /// Rejects zero dimensions and dimensions above the portable maximum;
    /// per-index parameter ranges are validated by each implementation's
    /// constructor rather than here.
    pub fn validate(&self) -> Result<()> {
        if self.dimension == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "dimension must be > 0".to_string(),
            ));
        }
        if self.dimension > crate::index::common::MAX_DIMENSION {
            return Err(VectorError::ResourceLimit(format!(
                "dimension {} exceeds max {}",
                self.dimension,
                crate::index::common::MAX_DIMENSION
            )));
        }
        Ok(())
    }
}

/// Point-in-time counters for an index.
///
/// `num_vectors` counts every stored entry including tombstones;
/// [`live`](Self::live) subtracts the tombstoned share. Operators watch
/// `num_deleted / num_vectors` to decide when compaction or rebuild pays.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndexStats {
    /// Total entries ever inserted and not yet compacted away.
    pub num_vectors: usize,
    /// Entries tombstoned by SQL DELETE/UPDATE, awaiting compaction.
    pub num_deleted: usize,
    /// Monotonic mutation counter, bumped on every build/insert/compact.
    pub generation: u64,
}

impl IndexStats {
    /// Live (searchable) entry count.
    ///
    /// Saturating: a corrupted counter pair reports 0 rather than wrapping.
    #[must_use]
    pub const fn live(&self) -> usize {
        self.num_vectors.saturating_sub(self.num_deleted)
    }
}

/// Common behaviour implemented by every vector index.
///
/// Implementations own only derived acceleration state (see
/// [`crate::durability`]): they never own the authoritative vectors (those
/// live in storage-engine table rows) and must be rebuildable from a full
/// scan. Deletes tombstone rather than physically unlinking so concurrent
/// readers never observe a torn graph. Any separately persisted artifact is
/// a non-authoritative derived cache: deleting or corrupting it must force
/// rebuild from rows, never data loss.
pub trait VectorIndex {
    /// Replaces contents with `rows`.
    ///
    /// Deterministic when the config seed is fixed: the same input rows in
    /// any arrival order produce the same searchable structure after the
    /// implementation's documented canonical ordering pass.
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()>;
    /// Inserts or overwrites a single row.
    ///
    /// Overwrites drop old adjacency first and reinsert, so an UPDATE never
    /// leaves two live entries for one [`RowId`].
    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()>;
    /// Tombstones a row.
    ///
    /// Returns `true` when a live row was removed. Physical cleanup happens
    /// in background compaction, never inline on the write path.
    fn delete(&mut self, row_id: RowId) -> Result<bool>;
    /// Approximates or exactly computes top-`k` in ascending distance order.
    ///
    /// Callers must still MVCC-recheck and optionally exact-rerank before
    /// presenting results; the index promises candidates, not committed rows.
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>>;
    /// Returns all live rows with `distance <= radius`, ascending.
    ///
    /// Approximate indexes scan their live set exactly here (bounded cost)
    /// so radius results stay correct even before rerank.
    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>>;
    /// Persists or flushes buffered state.
    ///
    /// No-op for purely in-memory indexes; durable indexes fsync and update
    /// their manifest generation.
    fn flush(&mut self) -> Result<()>;
    /// Performs structural validation; errors on any inconsistency.
    ///
    /// Checks headers, dimensions, finite lanes, dangling edges, degree
    /// bounds, and entry-point reachability. Corruption fails closed.
    fn validate(&self) -> Result<()>;
    /// Returns current counters.
    ///
    /// Used for observability, build-progress reporting, and
    /// tombstone-ratio-driven compaction scheduling.
    fn stats(&self) -> IndexStats;
}
