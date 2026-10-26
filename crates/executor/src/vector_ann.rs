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
//! ANN execution path for vector nearest-neighbour queries.
//!
//! Durability convergence (see `plomid_vector::durability`): authoritative
//! vector values live in storage-engine rows ordered through
//! txn/MVCC/WAL → storage-engine generations. Everything here is derived,
//! non-authoritative, rebuildable state gated by freshness, and ANN is
//! acceleration only:
//!
//! ```text
//! VECTOR VALUES (storage-engine rows)
//!       ↓
//!  txn / MVCC / WAL (commit authority; watermark source)
//!       ↓
//!  Storage Engine generations (durable images; GC/VACUUM authority)
//!       ↓
//!  vector index payloads as derived/rebuildable data (this cache + segments)
//!       ↓
//!  vector freshness state (watermark + row count binding)
//!       ↓
//!  ANN acceleration (candidates + exact rerank from authoritative rows)
//! ```
//!
//! Commit-order convergence: row commits bump the watermark, which
//! invalidates cached graphs, so both `ROW COMMIT → INDEX UPDATE` and
//! `INDEX BUILD → ROW COMMIT` orders serve the post-commit snapshot (rebuilt
//! or exact). DiskANN segments under `vector_ann/` are a persisted derived
//! cache: always rebuilt from snapshot rows before serving, safe to delete,
//! corruption falls back to exact. There is no second WAL, MVCC, backup,
//! recovery, or GC authority here.
//!
//! When a query is a pure `ORDER BY distance_fn(vector_col, query) LIMIT k`
//! over a dense column with a usable HNSW-class index, this module serves it
//! from a cached in-memory HNSW instead of the exact per-row scan: candidates
//! come from the graph, then every candidate is **exactly reranked** from its
//! authoritative row value before the final Top-K cut. Recall therefore
//! degrades gracefully (a missed graph candidate can only drop a row the
//! oversampled shortlist missed) and never fabricates a distance.
//!
//! Correctness contract:
//!
//! * The cache key covers `(database, table, column, watermark, row count)`.
//!   The watermark is the committed-write counter, so any committed INSERT,
//!   UPDATE or DELETE invalidates the entry and forces a rebuild from the
//!   current snapshot. Uncommitted writes never enter the cache because the
//!   build scans only snapshot-visible materialized rows.
//! * Filtered queries (`WHERE`), joins, aggregation, multi-key orderings and
//!   non-dense kinds bypass ANN entirely and keep the exact scan.
//! * Every failure inside the ANN path (build error, search error, empty
//!   cache slot) falls back to `None`, and callers run the exact scan.
//!   ANN is strictly an accelerator; it can never fail a query.
//! * Result order is `(exact_distance asc, scan_position asc)`: deterministic
//!   across runs for identical snapshots.
//!
//! Resource bounds: at most [`MAX_CACHED_INDEXES`] graphs process-wide
//! (LRU-evicted), ANN builds above [`MAX_ANN_ROWS`] rows decline to exact,
//! and candidate oversampling is capped by the vector crate.

use std::collections::HashMap;
use std::sync::Mutex;

use plomid_sql::{Expression, OrderByItem, TableSchema, Value};
use plomid_txn::StorageEngine;
use plomid_vector::index::VectorIndex as _;

use crate::query::evaluate_expression;
use plomid_sql::SqlResult;

/// Maximum cached HNSW graphs process-wide (LRU-evicted past this).
///
/// Default from `vector.cache_graphs`; operators tune via
/// `SET vector.cache_graphs`.
const DEFAULT_CACHED_INDEXES: usize = 8;
/// Row count above which ANN build cost is declined in favour of exact scan.
///
/// Default from `vector.max_ann_rows`.
pub const DEFAULT_MAX_ANN_ROWS: usize = 1_000_000;
/// Candidate oversample factor applied to `top_k` before exact rerank.
///
/// Default from `vector.candidate_oversample`.
const DEFAULT_OVERSAMPLE_FACTOR: usize = 8;

/// Executable ANN backend selected from the recorded index method.
#[derive(Clone)]
enum Backend {
    /// In-memory HNSW graph (default auto-index).
    Hnsw(plomid_vector::index::hnsw::HnswIndex),
    /// SSD-backed graph with bounded RAM resident set.
    DiskAnn(plomid_vector::index::diskann::DiskAnnIndex),
    /// IVF-Flat: coarse centroids with exact posting lists.
    IvfFlat(plomid_vector::index::ivf::IvfFlatIndex),
    /// IVF-SQ8: scalar-quantized posting lists.
    Sq8(plomid_vector::index::sq::Sq8Index),
    /// IVF-PQ: product-quantized posting lists.
    Pq(plomid_vector::index::pq::PqIndex),
    /// Sparse inverted index (exact-verified candidates).
    Sparse(plomid_vector::sparse::SparseInvertedIndex),
    /// Binary bit-packed index (Hamming ANN with exact rerank).
    Binary(plomid_vector::index::binary::BinaryIndex),
}

impl Backend {
    /// Builds the backend from snapshot lanes (`RowId` == lane position).
    fn build(
        method: AnnMethod,
        vector_type: &plomid_vector::VectorType,
        metric: plomid_vector::index::Metric,
        lanes: &[Vec<f32>],
        data_dir: Option<std::path::PathBuf>,
    ) -> Result<Self, plomid_vector::VectorError> {
        use plomid_vector::index::{IndexConfig, IndexKind};
        let rows: Vec<(u64, Vec<f32>)> = lanes
            .iter()
            .enumerate()
            .map(|(i, lane)| (i as u64, lane.clone()))
            .collect();
        match method {
            AnnMethod::Hnsw => {
                let config = IndexConfig {
                    kind: IndexKind::Hnsw,
                    metric,
                    dimension: vector_type.dimension as usize,
                    params: plomid_vector::index::IndexParams::new(),
                };
                let mut index = plomid_vector::index::hnsw::HnswIndex::new(config)?;
                // Per-query beam width from governance (no restart needed).
                let ef = crate::vector_settings::get_int("vector.ef_search")
                    .unwrap_or(plomid_vector::index::hnsw::DEFAULT_EF_SEARCH as i64)
                    .clamp(1, 4096) as usize;
                index.with_ef_search(ef);
                index.build(&rows)?;
                Ok(Backend::Hnsw(index))
            }
            AnnMethod::DiskAnn => {
                let config = IndexConfig {
                    kind: IndexKind::DiskAnn,
                    metric,
                    dimension: vector_type.dimension as usize,
                    params: plomid_vector::index::IndexParams::new(),
                };
                let dir = data_dir.expect("diskann requires a data dir");
                // Crash-recovery classification first: a healthy checkpoint
                // is reported (operators can see restarts reusing segments),
                // but the authoritative rebuild below always wins — derived
                // graphs never outrank table rows.
                match plomid_vector::index::diskann::DiskAnnIndex::recover(config.clone(), &dir) {
                    Ok((_, plomid_vector::index::diskann::DiskAnnRecovery::Ready)) => {
                        tracing::debug!(
                            target: "sql::vector_ann",
                            "diskann checkpoint ready; rebuilding from snapshot rows"
                        );
                    }
                    Ok((
                        _,
                        plomid_vector::index::diskann::DiskAnnRecovery::RebuildRequired { reason },
                    )) => {
                        tracing::warn!(
                            target: "sql::vector_ann",
                            "diskann checkpoint unusable ({reason}); rebuilding from snapshot rows"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            target: "sql::vector_ann",
                            "diskann recovery failed ({e}); rebuilding from snapshot rows"
                        );
                    }
                }
                let mut index = plomid_vector::index::diskann::DiskAnnIndex::new(config, dir)?;
                index.build(&rows)?;
                // Persist graph metadata (vectors.dat is write-through;
                // graph.dat needs an explicit flush) so the segments
                // survive restarts and `recover()` classifies them.
                index.flush()?;
                Ok(Backend::DiskAnn(index))
            }
            // IVF/PQ/SQ train from the snapshot rows inside `build()`;
            // training failures (e.g. too few rows for the list count)
            // fall back to exact at the call site.
            AnnMethod::IvfFlat => {
                let config = IndexConfig {
                    kind: IndexKind::IvfFlat,
                    metric,
                    dimension: vector_type.dimension as usize,
                    params: plomid_vector::index::IndexParams::new(),
                };
                let mut index = plomid_vector::index::ivf::IvfFlatIndex::new(config)?;
                index.build(&rows)?;
                Ok(Backend::IvfFlat(index))
            }
            AnnMethod::Sq8 => {
                let config = IndexConfig {
                    kind: IndexKind::IvfSq,
                    metric,
                    dimension: vector_type.dimension as usize,
                    params: plomid_vector::index::IndexParams::new(),
                };
                let mut index = plomid_vector::index::sq::Sq8Index::new(config)?;
                index.build(&rows)?;
                Ok(Backend::Sq8(index))
            }
            AnnMethod::Pq => {
                let config = IndexConfig {
                    kind: IndexKind::IvfPq,
                    metric,
                    dimension: vector_type.dimension as usize,
                    params: plomid_vector::index::IndexParams::new(),
                };
                let mut index = plomid_vector::index::pq::PqIndex::new(config)?;
                index.build(&rows)?;
                Ok(Backend::Pq(index))
            }
            // Sparse builds from pair vectors via run_sparse_ann, never here.
            // Binary builds from packed words via run_binary_ann, never here.
            AnnMethod::Sparse | AnnMethod::Binary => Err(
                plomid_vector::VectorError::IndexUnsupported("sparse builds via pair path".into()),
            ),
        }
    }

    /// Candidate search; all errors become exact-fallback at the call site.
    fn search(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<plomid_vector::index::SearchResult>, plomid_vector::VectorError> {
        match self {
            Backend::Hnsw(index) => index.search(query, k),
            Backend::DiskAnn(index) => index.search(query, k),
            Backend::IvfFlat(index) => index.search(query, k),
            Backend::Sq8(index) => index.search(query, k),
            Backend::Pq(index) => index.search(query, k),
            // Sparse serves through its native pair API (see run_sparse_ann).
            // Binary serves through its native words API (see run_binary_ann).
            Backend::Sparse(_) | Backend::Binary(_) => {
                Err(plomid_vector::VectorError::IndexUnsupported(
                    "sparse/binary use native search".into(),
                ))
            }
        }
    }

    /// Live (non-tombstoned) entry count for staleness checks.
    fn live(&self) -> usize {
        match self {
            Backend::Hnsw(index) => index.stats().live(),
            Backend::DiskAnn(index) => index.stats().live(),
            Backend::IvfFlat(index) => index.stats().live(),
            Backend::Sq8(index) => index.stats().live(),
            Backend::Pq(index) => index.stats().live(),
            Backend::Sparse(index) => index.len(),
            Backend::Binary(index) => index.stats().live(),
        }
    }

    /// Sparse pair search over an inverted backend.
    fn search_sparse(
        &self,
        query: &plomid_vector::sparse::SparseVector,
        k: usize,
    ) -> Result<Vec<plomid_vector::index::SearchResult>, plomid_vector::VectorError> {
        match self {
            Backend::Sparse(index) => index.search(query, k),
            _ => Err(plomid_vector::VectorError::IndexUnsupported(
                "not a sparse backend".into(),
            )),
        }
    }

    /// Bounded radius candidates (`distance <= radius`, ascending).
    ///
    /// Only exact-enumeration backends (HNSW, IVF-Flat) serve radius through
    /// this well: every live row is scored with exact distances, so callers
    /// exact-verify against the SQL bound before returning. Approximate
    /// wells (SQ/PQ/DiskANN) must not reach here — `run_ann_radius`
    /// declines them to the exact scan, because rows the well misses can
    /// never be resurrected by verification.
    /// Sparse serves through its native pair API instead.
    fn range_search(
        &self,
        query: &[f32],
        radius: f32,
    ) -> Result<Vec<plomid_vector::index::SearchResult>, plomid_vector::VectorError> {
        match self {
            Backend::Hnsw(index) => index.range_search(query, radius),
            Backend::DiskAnn(index) => index.range_search(query, radius),
            Backend::IvfFlat(index) => index.range_search(query, radius),
            Backend::Sq8(index) => index.range_search(query, radius),
            Backend::Pq(index) => index.range_search(query, radius),
            // Sparse serves through its native pair API; binary through its
            // native words API (see run_sparse_ann / run_binary_ann).
            Backend::Sparse(_) | Backend::Binary(_) => {
                Err(plomid_vector::VectorError::IndexUnsupported(
                    "sparse/binary use native search".into(),
                ))
            }
        }
    }
}

/// ANN methods with wired execution.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnnMethod {
    /// In-memory HNSW graph.
    Hnsw,
    /// Binary bit-packed Hamming index.
    Binary,
    /// SSD-backed disk-oriented graph.
    DiskAnn,
    /// IVF-Flat posting lists (exact per-list vectors).
    IvfFlat,
    /// IVF-SQ8 scalar-quantized posting lists.
    Sq8,
    /// IVF-PQ product-quantized posting lists.
    Pq,
    /// Sparse inverted index (exact-verified candidates).
    Sparse,
}

/// Resolves the recorded index method to an executable ANN backend (shared
/// by the row-store and join-engine call sites).
///
/// Returns `None` for exact-only methods (`flat`): those stay on the exact
/// scan by design.
pub(crate) fn exec_method(operator_class: Option<&str>) -> Option<AnnMethod> {
    match operator_class.map(str::to_ascii_lowercase).as_deref() {
        Some("hnsw") => Some(AnnMethod::Hnsw),
        Some("diskann") => Some(AnnMethod::DiskAnn),
        Some("ivf_flat") => Some(AnnMethod::IvfFlat),
        Some("ivf_sq") => Some(AnnMethod::Sq8),
        Some("ivf_pq") => Some(AnnMethod::Pq),
        Some("sparse_inverted") => Some(AnnMethod::Sparse),
        Some("binary") => Some(AnnMethod::Binary),
        _ => None,
    }
}

/// Whether `method` has wired execution for at least one query metric on a
/// column declared as `vector_type`.
///
/// This is the DDL acceptance envelope: `CREATE INDEX` accepts exactly the
/// recorded methods the runtime can execute for some query, and explicitly
/// rejects everything else — metadata is never recorded for a configuration
/// that could only ever fall back to the exact scan. `flat` is accepted for
/// every type by design (it *is* the exact baseline and never accelerates).
///
/// The dense predicates are metric-uniform across L2/Cosine/Dot, so probing
/// with one metric answers the whole set. Per-query metric gates still run
/// through [`resolve_ann_method`] at execution time.
///
/// `ivf_pq` additionally carries a **dimension** precondition: the runtime
/// always builds it with default params (8 segments), so a dimension that is
/// not a multiple of [`PQ_DEFAULT_SEGMENTS`] can never train — such a column
/// is rejected here rather than recorded and left falling back forever. The
/// same divisor governs `PQ`'s own constructor, so the two cannot drift.
pub(crate) fn method_executable_for_type(
    method: &str,
    vector_type: &plomid_vector::VectorType,
) -> bool {
    use plomid_vector::VectorMetric;
    match method.to_ascii_lowercase().as_str() {
        "flat" => true,
        "hnsw" | "diskann" => vector_type.supports_hnsw(VectorMetric::Cosine),
        "ivf_flat" => vector_type.supports_ivf(VectorMetric::Cosine),
        "ivf_sq" => vector_type.supports_sq(VectorMetric::Cosine),
        "ivf_pq" => {
            vector_type.supports_pq(VectorMetric::Cosine)
                && pq_dimension_executable(vector_type.dimension)
        }
        "sparse_inverted" => vector_type.kind == plomid_vector::VectorKind::Sparse,
        "binary" => {
            vector_type.kind == plomid_vector::VectorKind::Binary
                && vector_type.element == plomid_vector::VectorElementType::Binary
        }
        _ => false,
    }
}

/// Whether an `ivf_pq` index can train on this dimension with the runtime's
/// default build params (one segment count, one owner: the PQ constructor).
pub(crate) fn pq_dimension_executable(dimension: u32) -> bool {
    (dimension as usize).is_multiple_of(plomid_vector::index::pq::PQ_DEFAULT_SEGMENTS)
        && dimension as usize >= plomid_vector::index::pq::PQ_DEFAULT_SEGMENTS
}

/// Operator-facing hint for a method/type rejection, naming the executable
/// alternative instead of leaving the caller to guess.
pub(crate) fn method_rejection_hint(
    method: &str,
    vector_type: &plomid_vector::VectorType,
) -> &'static str {
    use plomid_vector::VectorKind;
    match vector_type.kind {
        VectorKind::Binary => {
            "binary columns accept USING BINARY (Hamming ANN) or USING FLAT (exact)"
        }
        VectorKind::Sparse => {
            "sparse columns accept USING SPARSE_INVERTED or USING FLAT (exact)"
        }
        VectorKind::Multi => {
            "MULTI_VECTOR columns have no ANN method in v0.1.0; queries use the exact constituent kernel"
        }
        VectorKind::Dense => match method {
            "ivf_pq" if !pq_dimension_executable(vector_type.dimension) => {
                "ivf_pq requires the dimension to be a multiple of 8 (the default segment count); use a divisible dimension or another dense method"
            }
            "hnsw" | "diskann" | "ivf_flat" | "ivf_sq" | "ivf_pq" => {
                "dense numeric lanes (FLOAT32/16, BFLOAT16, INT8/UINT8/INT4 as documented) serve L2/Cosine/Dot for this method"
            }
            _ => "use one of the documented vector index methods",
        },
    }
}

/// Filesystem home for one column's DiskANN segments.
///
/// Names are sanitized to `[A-Za-z0-9_-]` so catalog identifiers can never
/// escape the vector directory (no path traversal from index/table names).
///
/// NON-AUTHORITATIVE derived cache (see `plomid_vector::durability`):
/// safe to delete at any time — the next query rebuilds from authoritative
/// storage-engine rows or falls back to exact. Never back this directory up
/// as a system of record; back up storage-engine generations instead.
fn diskann_dir(
    root: &std::path::Path,
    database: &str,
    table: &str,
    column: &str,
) -> std::path::PathBuf {
    derived_cache_dir(root, database, table, column)
}

/// Filesystem home for one column's derived ANN artifacts.
///
/// Public so operators and tests can implement the rebuild gate directly:
/// delete or corrupt this directory, then verify authoritative rows stay
/// valid, exact search stays correct, and ANN rebuilds safely. Names are
/// sanitized to `[A-Za-z0-9_-]` so catalog identifiers can never escape the
/// vector directory.
pub fn derived_cache_dir(
    root: &std::path::Path,
    database: &str,
    table: &str,
    column: &str,
) -> std::path::PathBuf {
    let clean = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
    };
    root.join("vector_ann")
        .join(clean(database))
        .join(clean(table))
        .join(clean(column))
}

/// Destroys one column's derived ANN artifacts (rebuild gate).
///
/// Deletes the [`derived_cache_dir`] tree **and** drops matching in-memory
/// graphs, forcing the next query to rebuild from authoritative rows or fall
/// back to exact. Always safe: derived state only, no data loss. Returns the
/// number of removed files plus dropped cache entries. `table` is the
/// catalog-resolved qualified name.
pub fn destroy_derived_cache(
    root: &std::path::Path,
    database: &str,
    qualified_table: &str,
    column: &str,
) -> usize {
    let dir = derived_cache_dir(root, database, qualified_table, column);
    let mut removed = 0usize;
    if dir.exists() {
        removed += walk_count(&dir);
        let _ = std::fs::remove_dir_all(&dir);
    }
    removed + invalidate_table(database, qualified_table)
}

fn walk_count(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += walk_count(&path);
        } else {
            count += 1;
        }
    }
    count
}

/// One cached graph: the snapshot identity it was built from plus the live backend.
///
/// `RowId` inside the graph is the lane position in build-time scan order;
/// the search closure maps it back to materialized row offsets (which differ
/// when NULLs are skipped during the build).
struct CachedGraph {
    /// Committed-write watermark at build time.
    watermark: u64,
    /// Materialized row count at build time.
    row_count: usize,
    /// Last-use tick for LRU eviction.
    last_used: u64,
    /// Executable backend (HNSW memory graph or DiskANN segments).
    backend: Backend,
    /// Build-time observation record for cost-model planning and EXPLAIN.
    /// Dies with this entry on watermark invalidation or LRU eviction.
    stats: plomid_vector::VectorStats,
}

/// Snapshot of one cached graph's planning statistics for diagnostics.
///
/// Returns the build-time [`plomid_vector::VectorStats`] when a graph is
/// cached for `(database, table, column, fingerprint)`; `None` means no
/// observation exists yet (planner uses safe defaults). Freshness is the
/// caller's check: `stats.is_fresh(current_watermark)`. Entries are keyed
/// per metric; diagnostics take any metric's entry (first match).
pub(crate) fn cached_graph_stats(
    database: &str,
    qualified_table: &str,
    column: &str,
    fingerprint: &str,
) -> Option<plomid_vector::VectorStats> {
    let cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache
        .entries
        .iter()
        .find(|((db, table, col, fp, _), _)| {
            db == database && table == qualified_table && col == column && fp == fingerprint
        })
        .map(|(_, entry)| entry.stats.clone())
}

/// Process-wide ANN graph cache and LRU tick.
struct AnnCache {
    entries: HashMap<(String, String, String, String, String), CachedGraph>,
    tick: u64,
}

fn ann_cache() -> &'static Mutex<AnnCache> {
    static CACHE: std::sync::OnceLock<Mutex<AnnCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(AnnCache {
            entries: HashMap::new(),
            tick: 0,
        })
    })
}

/// Drops every cached ANN graph, forcing rebuild on next eligible query.
///
/// Called by `REINDEX` and usable from maintenance paths: graphs are purely
/// derived state, so invalidation is always safe — the next query rebuilds
/// from authoritative rows or falls back to exact. Returns the dropped count.
pub fn invalidate_all() -> usize {
    let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
    let dropped = cache.entries.len();
    cache.entries.clear();
    dropped
}

/// Drops cached ANN graphs for one table, keeping all others hot.
///
/// Called by maintenance (`VACUUM`) after physical placement changes:
/// cached graphs are lane-position addressed, and placement rewrites do not
/// bump the write watermark, so the table's entries must be rebuilt from
/// the post-maintenance snapshot on next use. Returns the dropped count.
pub fn invalidate_table(database: &str, qualified_table: &str) -> usize {
    let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
    let before = cache.entries.len();
    cache
        .entries
        .retain(|(db, table, _, _, _), _| !(db == database && table == qualified_table));
    before - cache.entries.len()
}

/// Metadata-only ANN eligibility: the shape checks of [`try_ann_order`]
/// that need no materialized rows.
///
/// Streaming TopK sites call this to decline their bounded heap when ANN
/// will serve the query: the flow then falls through to the materialized
/// path whose sort site runs the graph. Returns `false` (keep streaming)
/// for every non-vector ordering.
pub fn ann_eligible(
    engine: &impl StorageEngine,
    catalog: &plomid_sql::InMemoryCatalog,
    table: &str,
    schema: &TableSchema,
    order_by: &[OrderByItem],
    top_k: Option<usize>,
    has_filter: bool,
) -> bool {
    if engine.is_read_pinned() {
        return false;
    }
    let Some(k) = top_k else {
        return false;
    };
    if k == 0 {
        return false;
    }
    if !crate::vector_settings::ann_enabled() {
        return false;
    }
    if has_filter {
        return false;
    }
    let [order] = order_by else {
        return false;
    };
    if order.descending {
        return false;
    }
    let Expression::FunctionCall { name, args, .. } = &order.expr else {
        return false;
    };
    if !plomid_vector::planner_api::order_by_supported(name) {
        return false;
    }
    if args.len() != 2 {
        return false;
    }
    let Some(column) = args.iter().find_map(|arg| match arg {
        Expression::ColumnRef(col) => Some(col.as_str()),
        _ => None,
    }) else {
        return false;
    };
    let Ok(pos) = schema.column_index(column) else {
        return false;
    };
    let col_type = match schema.columns.get(pos) {
        Some(col) => col.col_type,
        None => return false,
    };
    if col_type.type_oid != plomid_types::TypeOid::VECTOR {
        return false;
    }
    let Ok(vector_type) = plomid_vector::VectorType::typmod_decode(col_type.typmod) else {
        return false;
    };
    // Dense methods and sparse inverted serve their own kinds; binary/multi
    // keep the optimal exact kernels (packed popcount scan, constituent
    // MIN_DISTANCE).
    let metric = match name.to_ascii_lowercase().as_str() {
        "cosine_distance" => plomid_vector::VectorMetric::Cosine,
        "l2_distance" => plomid_vector::VectorMetric::L2,
        "dot_product" | "neg_inner_product" => plomid_vector::VectorMetric::Dot,
        "hamming_distance" => plomid_vector::VectorMetric::Hamming,
        "jaccard_distance" => plomid_vector::VectorMetric::Jaccard,
        _ => return false,
    };
    let qualified = catalog.resolve_table_namespace(table);
    catalog.indexes().into_iter().any(|index| {
        if index.table != qualified {
            return false;
        }
        let covers = index.column == column || index.columns.iter().any(|c| c == column);
        if !covers {
            return false;
        }
        exec_method(index.operator_class.as_deref()).is_some_and(|method| match method {
            AnnMethod::Hnsw | AnnMethod::DiskAnn => {
                vector_type.kind == plomid_vector::VectorKind::Dense
                    && vector_type.supports_hnsw(metric)
            }
            AnnMethod::IvfFlat => vector_type.supports_ivf(metric),
            AnnMethod::Sq8 => vector_type.supports_sq(metric),
            AnnMethod::Pq => vector_type.supports_pq(metric),
            AnnMethod::Sparse => {
                vector_type.kind == plomid_vector::VectorKind::Sparse
                    && matches!(
                        metric,
                        plomid_vector::VectorMetric::L2
                            | plomid_vector::VectorMetric::Cosine
                            | plomid_vector::VectorMetric::Dot
                    )
            }
            AnnMethod::Binary => {
                // Hamming only: Jaccard has no block-equality signal (see
                // `BinaryIndex::ann_search_words`) and stays on the exact scan.
                vector_type.kind == plomid_vector::VectorKind::Binary
                    && vector_type.element == plomid_vector::VectorElementType::Binary
                    && metric == plomid_vector::VectorMetric::Hamming
            }
        })
    })
}

/// Whether a query-vector argument is row-independent (safe to evaluate once).
///
/// Literals, casts thereof and pure scalar combinations are constant across
/// rows; column references, subqueries and set-returning shapes may vary per
/// row and must keep the exact per-row path.
pub fn query_arg_independent(expr: &plomid_sql::Expression) -> bool {
    use plomid_sql::Expression as E;
    match expr {
        E::ColumnRef(_) | E::ScalarSubquery(_) | E::Exists(_) | E::QuantifiedComparison { .. } => {
            false
        }
        E::In { subquery, .. } => subquery.is_none(),
        E::FunctionCall { args, .. } => args.iter().all(query_arg_independent),
        E::Cast { expr, .. }
        | E::TypeCast { expr, .. }
        | E::Negate(expr)
        | E::Not(expr)
        | E::IsNull(expr)
        | E::IsNotNull(expr) => query_arg_independent(expr),
        E::Equal(l, r)
        | E::NotEqual(l, r)
        | E::Less(l, r)
        | E::LessOrEqual(l, r)
        | E::Greater(l, r)
        | E::GreaterOrEqual(l, r)
        | E::And(l, r)
        | E::Or(l, r)
        | E::Add(l, r)
        | E::Subtract(l, r)
        | E::Multiply(l, r)
        | E::Divide(l, r)
        | E::Modulo(l, r)
        | E::Concat(l, r)
        | E::Power(l, r)
        | E::BitAnd(l, r)
        | E::BitOr(l, r)
        | E::BitXor(l, r)
        | E::ShiftLeft(l, r)
        | E::ShiftRight(l, r)
        | E::IsDistinctFrom(l, r)
        | E::NullIf(l, r) => query_arg_independent(l) && query_arg_independent(r),
        E::Between {
            expr, low, high, ..
        } => {
            query_arg_independent(expr) && query_arg_independent(low) && query_arg_independent(high)
        }
        E::Like { expr, pattern, .. } => {
            query_arg_independent(expr) && query_arg_independent(pattern)
        }
        // Literals (scalar, vector, temporal), parameters and anything not
        // enumerated above are row-independent by construction.
        _ => true,
    }
}

/// Resolves the recorded ANN method for one vector column, gated by the
/// capability matrix (shared by the Top-K and radius paths so eligibility
/// can never disagree between them).
fn resolve_ann_method(
    catalog: &plomid_sql::InMemoryCatalog,
    qualified_table: &str,
    column: &str,
    vector_type: &plomid_vector::VectorType,
    metric: plomid_vector::VectorMetric,
) -> Option<AnnMethod> {
    catalog.indexes().into_iter().find_map(|index| {
        if index.table != qualified_table {
            return None;
        }
        let covers = index.column == column || index.columns.iter().any(|c| c == column);
        if !covers {
            return None;
        }
        let method = exec_method(index.operator_class.as_deref())?;
        let supported = match method {
            AnnMethod::Hnsw | AnnMethod::DiskAnn => {
                vector_type.kind == plomid_vector::VectorKind::Dense
                    && vector_type.supports_hnsw(metric)
            }
            AnnMethod::IvfFlat => vector_type.supports_ivf(metric),
            AnnMethod::Sq8 => vector_type.supports_sq(metric),
            AnnMethod::Pq => vector_type.supports_pq(metric),
            AnnMethod::Sparse => {
                vector_type.kind == plomid_vector::VectorKind::Sparse
                    && matches!(
                        metric,
                        plomid_vector::VectorMetric::L2
                            | plomid_vector::VectorMetric::Cosine
                            | plomid_vector::VectorMetric::Dot
                    )
            }
            AnnMethod::Binary => {
                // Hamming only: Jaccard has no block-equality signal (see
                // `BinaryIndex::ann_search_words`) and stays on the exact scan.
                vector_type.kind == plomid_vector::VectorKind::Binary
                    && vector_type.element == plomid_vector::VectorElementType::Binary
                    && metric == plomid_vector::VectorMetric::Hamming
            }
        };
        supported.then_some(method)
    })
}

/// Maps a SQL distance/similarity function name to its metric.
///
/// Distance spellings pair with `similarity == false`, similarity
/// spellings with `true`; anything else declines (`None`). Shared by the
/// Top-K, radius, and EXPLAIN paths so function coverage can never drift
/// between planning and execution.
pub(crate) fn sql_metric_from_name(
    name: &str,
    similarity: bool,
) -> Option<plomid_vector::VectorMetric> {
    match (name.to_ascii_lowercase().as_str(), similarity) {
        ("cosine_distance", false) => Some(plomid_vector::VectorMetric::Cosine),
        ("l2_distance", false) => Some(plomid_vector::VectorMetric::L2),
        ("dot_product" | "neg_inner_product", false) => Some(plomid_vector::VectorMetric::Dot),
        ("hamming_distance", false) => Some(plomid_vector::VectorMetric::Hamming),
        ("jaccard_distance", false) => Some(plomid_vector::VectorMetric::Jaccard),
        ("cosine_similarity", true) => Some(plomid_vector::VectorMetric::Cosine),
        ("l2_similarity", true) => Some(plomid_vector::VectorMetric::L2),
        ("dot_product" | "neg_inner_product", true) => Some(plomid_vector::VectorMetric::Dot),
        _ => None,
    }
}

/// Maps a SQL vector metric to the index metric (total: all five metrics).
fn sql_to_index_metric(metric: plomid_vector::VectorMetric) -> plomid_vector::index::Metric {
    match metric {
        plomid_vector::VectorMetric::L2 => plomid_vector::index::Metric::L2,
        plomid_vector::VectorMetric::Cosine => plomid_vector::index::Metric::Cosine,
        plomid_vector::VectorMetric::Dot => plomid_vector::index::Metric::Dot,
        plomid_vector::VectorMetric::Hamming => plomid_vector::index::Metric::Hamming,
        plomid_vector::VectorMetric::Jaccard => plomid_vector::index::Metric::Jaccard,
    }
}

/// Tries the ANN path for `ORDER BY distance LIMIT` over materialized rows.
///
/// Returns `None` when the query is not ANN-eligible (callers run the exact
/// scan); otherwise returns the ordered Top-K rows. See the module docs for
/// the eligibility and correctness contract.
#[allow(clippy::too_many_arguments)]
pub fn try_ann_order<E: StorageEngine>(
    engine: &E,
    catalog: &plomid_sql::InMemoryCatalog,
    database: &str,
    table: &str,
    schema: &TableSchema,
    rows: &[Vec<Value>],
    order_by: &[OrderByItem],
    top_k: Option<usize>,
    where_fingerprint: Option<String>,
    current_user: &str,
    query_override: Option<Value>,
) -> Option<SqlResult<Vec<Vec<Value>>>> {
    let _ = current_user;
    // Pinned repeatable-read snapshots resolve every row through the pinned
    // state: a cached graph built from newer commits would leak them, and a
    // live probe could miss rows deleted after the snapshot began. The row
    // path stays exact here, mirroring index pruning and columnar.
    if engine.is_read_pinned() {
        return None;
    }
    // Eligibility: single distance ordering, LIMIT present, no filter.
    let k = top_k?;
    if k == 0 || rows.is_empty() {
        return None;
    }
    // Governance: operators can disable ANN or bound its build cost without
    // restarting (`SET vector.ann_enabled`, `vector.max_ann_rows`).
    if !crate::vector_settings::ann_enabled() {
        return None;
    }
    let max_rows = crate::vector_settings::get_int("vector.max_ann_rows")
        .unwrap_or(DEFAULT_MAX_ANN_ROWS as i64)
        .max(0) as usize;
    if rows.len() > max_rows {
        return None;
    }
    // Auto threshold shared with EXPLAIN (`planner_api::AUTO_ANN_MIN_ROWS`,
    // tunable via `vector.auto_ann_min_rows`): small tables scan exactly.
    let threshold = crate::vector_settings::get_int("vector.auto_ann_min_rows")
        .unwrap_or(plomid_vector::planner_api::AUTO_ANN_MIN_ROWS as i64)
        .max(0) as usize;
    if rows.len() <= threshold {
        return None;
    }
    let [order] = order_by else {
        return None;
    };
    if order.descending {
        return None;
    }
    let Expression::FunctionCall { name, args, .. } = &order.expr else {
        return None;
    };
    if !plomid_vector::planner_api::order_by_supported(name) {
        return None;
    }
    if args.len() != 2 {
        return None;
    }
    let column = args.iter().find_map(|arg| match arg {
        Expression::ColumnRef(col) => Some(col.as_str()),
        _ => None,
    })?;
    let Ok(pos) = schema.column_index(column) else {
        return None;
    };
    let col_type = schema.columns.get(pos)?.col_type;
    if col_type.type_oid != plomid_types::TypeOid::VECTOR {
        return None;
    }
    let vector_type = plomid_vector::VectorType::typmod_decode(col_type.typmod).ok()?;
    let metric = sql_metric_from_name(name, false)?;
    // A usable executable index must be recorded on this table+column, and
    // the capability matrix must support this (kind, element, metric).
    // Sparse inverted indexes serve sparse columns; dense methods serve
    // dense columns; binary columns keep the optimal packed scan.
    let qualified = catalog.resolve_table_namespace(table);
    let method = resolve_ann_method(catalog, &qualified, column, &vector_type, metric)?;
    // The query vector is either supplied pre-evaluated (join engine, whose
    // evaluator understands session/subquery shapes) or evaluated here with
    // the row-store evaluator (literals and casts never touch the row).
    let query_value = match query_override {
        Some(value) => value,
        None => {
            // Evaluate the query vector against the first row (literals and casts
            // never touch the row; column refs other than the target column fail
            // closed below when the value is not a vector).
            let query_arg = args
                .iter()
                .find(|arg| !matches!(arg, Expression::ColumnRef(_)))?;
            evaluate_expression(&rows[0], schema, query_arg).ok()?
        }
    };
    // Binary Hamming ANN resolves before the Vector destructure so text
    // literals ('0101…') coerce via the canonical literal parser — the same
    // forms the exact scan accepts. Jaccard (and anything unparsable)
    // declines here, which is the explicit exact fallback.
    if vector_type.kind == plomid_vector::VectorKind::Binary {
        if metric != plomid_vector::VectorMetric::Hamming || method != AnnMethod::Binary {
            return None;
        }
        let query_words =
            crate::vector_ann::binary_query_words(&query_value, vector_type.dimension)?;
        let fingerprint = where_fingerprint.clone().unwrap_or_default();
        return Some(run_binary_ann(
            engine,
            database,
            &qualified,
            column,
            &vector_type,
            fingerprint,
            &query_words,
            rows,
            pos,
            k,
        ));
    }
    let Value::Vector(query_native) = query_value else {
        return None;
    };
    // Sparse columns take the inverted path; dense columns the graph path.
    // Binary/multi keep the optimal exact kernels (packed popcount scan and
    // constituent MIN_DISTANCE respectively).
    if vector_type.kind == plomid_vector::VectorKind::Sparse {
        let plomid_vector::VectorValue::Sparse(query_sparse) = &query_native else {
            return None;
        };
        if query_sparse.dimension != vector_type.dimension {
            return None;
        }
        let zipped: Vec<(u32, f32)> = query_sparse
            .indices
            .iter()
            .zip(query_sparse.values.iter())
            .map(|(i, v)| (*i, *v))
            .collect();
        let Ok(query) = plomid_vector::sparse::SparseVector::new(vector_type.dimension, zipped)
        else {
            return None;
        };
        let index_metric = match metric {
            plomid_vector::VectorMetric::L2
            | plomid_vector::VectorMetric::Cosine
            | plomid_vector::VectorMetric::Dot => sql_to_index_metric(metric),
            _ => return None,
        };
        return Some(run_sparse_ann(
            engine,
            database,
            &qualified,
            column,
            where_fingerprint.clone().unwrap_or_default(),
            vector_type.dimension,
            index_metric,
            &query,
            rows,
            pos,
            k,
        ));
    }
    if vector_type.kind != plomid_vector::VectorKind::Dense {
        return None;
    }
    let query_lanes: Vec<f32> = match &query_native {
        plomid_vector::VectorValue::Dense(d) => d.values_f32.clone(),
        _ => return None,
    };
    if query_lanes.len() as u32 != vector_type.dimension {
        return None;
    }
    // Filtered subsets are served from fingerprint-keyed graphs: the
    // oversample-free shortlist cannot under-return because the graph
    // covers exactly the filtered working set (watermark + fingerprint).
    let fingerprint = where_fingerprint.clone().unwrap_or_default();
    Some(run_ann(
        engine,
        database,
        &qualified,
        column,
        &vector_type,
        metric,
        method,
        fingerprint,
        &query_lanes,
        rows,
        pos,
        k,
    ))
}

/// Scalar bound literal to `f64` for radius planning (ints, floats and
/// numerics via their canonical text; anything else declines to exact).
fn bound_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Int2(n) => Some(f64::from(*n)),
        Value::Int4(n) => Some(f64::from(*n)),
        Value::Int8(n) => Some(*n as f64),
        Value::Float4(n) => Some(f64::from(*n)),
        Value::Float8(n) => Some(*n),
        Value::Numeric(_) => value.to_sql_text().parse::<f64>().ok(),
        _ => None,
    }
}

/// Estimates the fraction of rows surviving a `WHERE` expression.
///
/// Maps SQL predicate shapes to the optimizer's selectivity model:
/// column-vs-literal comparisons classify by operator, `AND`/`OR` combine
/// recursively, and everything else is opaque. No column statistics exist
/// yet, so estimates use the documented defaults (conservative: the
/// planner would rather scan than wrongly pick an index). `None` (no
/// filter) is selectivity `1.0`.
pub(crate) fn estimate_filter_selectivity(where_expr: &Option<Expression>) -> f64 {
    use plomid_optimizer::{
        combine_selectivity_and, combine_selectivity_or, estimate_selectivity,
        ColumnSelectivityHints, PredicateKind,
    };
    fn estimate(expr: &Expression) -> f64 {
        use plomid_sql::Expression as E;
        let hints: Option<&ColumnSelectivityHints> = None;
        let column_vs_literal = |l: &Expression, r: &Expression| {
            matches!(l, E::ColumnRef(_)) || matches!(r, E::ColumnRef(_))
        };
        match expr {
            E::Equal(l, r) if column_vs_literal(l, r) => {
                estimate_selectivity(PredicateKind::Equals, hints)
            }
            E::NotEqual(l, r) if column_vs_literal(l, r) => {
                1.0 - estimate_selectivity(PredicateKind::Equals, hints)
            }
            E::Less(l, r) | E::LessOrEqual(l, r) | E::Greater(l, r) | E::GreaterOrEqual(l, r)
                if column_vs_literal(l, r) =>
            {
                estimate_selectivity(PredicateKind::Range, hints)
            }
            E::In { subquery: None, .. } => estimate_selectivity(PredicateKind::InList, hints),
            E::IsNull(inner) if matches!(inner.as_ref(), E::ColumnRef(_)) => {
                estimate_selectivity(PredicateKind::IsNull, hints)
            }
            E::Like { .. } => estimate_selectivity(PredicateKind::Like, hints),
            E::And(l, r) => combine_selectivity_and(estimate(l), estimate(r)),
            E::Or(l, r) => combine_selectivity_or(estimate(l), estimate(r)),
            E::Not(inner) => 1.0 - estimate(inner).clamp(0.0, 1.0),
            // BETWEEN x AND y with a column operand is a bounded range.
            E::Between { expr, .. } if matches!(expr.as_ref(), E::ColumnRef(_)) => {
                estimate_selectivity(PredicateKind::Range, hints)
            }
            _ => estimate_selectivity(PredicateKind::Other, hints),
        }
    }
    match where_expr {
        None => 1.0,
        Some(expr) => estimate(expr).clamp(0.0, 1.0),
    }
}

/// Builds (or reuses) the cached sparse backend and serves exact-verified Top-K.
///
/// Sparse search is candidate retrieval plus exact verification inside the
/// inverted index itself, so no separate rerank pass is needed; output is
/// truncated to `k` with deterministic ties from the index.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_sparse_ann<E: StorageEngine>(
    engine: &E,
    database: &str,
    qualified_table: &str,
    column: &str,
    where_fingerprint: String,
    dim: u32,
    metric: plomid_vector::index::Metric,
    query: &plomid_vector::sparse::SparseVector,
    rows: &[Vec<Value>],
    col_pos: usize,
    k: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    use plomid_vector::sparse::SparseVector;
    // Materialize canonical sparse vectors in scan order, skipping NULLs.
    // Build-time statistics ride along: NULL skips and empty (zero-norm)
    // vectors are counted over data already in hand.
    let mut pairs: Vec<SparseVector> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    let mut null_count: u64 = 0;
    let mut zero_norm_count: u64 = 0;
    for (idx, row) in rows.iter().enumerate() {
        match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Sparse(s))) if s.dimension == dim => {
                let zipped: Vec<(u32, f32)> = s
                    .indices
                    .iter()
                    .zip(s.values.iter())
                    .map(|(i, v)| (*i, *v))
                    .collect();
                match SparseVector::new(dim, zipped) {
                    Ok(canonical) => {
                        if canonical.indices.is_empty() {
                            zero_norm_count += 1;
                        }
                        pairs.push(canonical);
                        positions.push(idx);
                    }
                    // Uncanonical stored shapes stay visible to exact scan only.
                    Err(_) => return exact_sparse_fallback(rows, col_pos, query, metric, k),
                }
            }
            Some(Value::Null) => {
                null_count += 1;
                continue;
            }
            _ => return exact_sparse_fallback(rows, col_pos, query, metric, k),
        }
    }
    if pairs.is_empty() {
        return Ok(Vec::new());
    }
    // Memory governance, as in `run_ann`: sparse postings above
    // `vector.max_memory_bytes` stay correct via the exact scan.
    let sparse_nnz: u64 = pairs.iter().map(|v| v.indices.len() as u64).sum();
    let memory_limit = crate::vector_settings::get_int("vector.max_memory_bytes")
        .unwrap_or(1_073_741_824)
        .max(0) as u64;
    if plomid_vector::options::memory_estimate_bytes(sparse_nnz, 12) > memory_limit {
        return exact_sparse_fallback(rows, col_pos, query, metric, k);
    }
    let watermark = crate::columnar_freshness::writes_since_boot(
        &crate::columnar_freshness::freshness_key(engine.root(), database, qualified_table),
    );
    // The cache key carries the index metric: backends bake the metric into
    // their structure (graph beam, codebooks, block maps), so sharing an
    // entry across metrics would propose candidates under the wrong
    // distance. Same-metric queries still share entries as before.
    let key = (
        database.to_string(),
        qualified_table.to_string(),
        column.to_string(),
        where_fingerprint.clone(),
        format!("{metric:?}"),
    );
    let ordered_positions = {
        let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.tick = cache.tick.wrapping_add(1);
        let tick = cache.tick;
        let fresh = cache.entries.get(&key).is_some_and(|entry| {
            entry.watermark == watermark
                && entry.row_count == rows.len()
                && entry.backend.live() == pairs.len()
                && matches!(entry.backend, Backend::Sparse(_))
        });
        plomid_core::observability::record_vector_freshness(fresh);
        if !fresh {
            plomid_core::observability::record_vector_ann_cache_miss();
            let rebuild_started = std::time::Instant::now();
            let mut index = plomid_vector::sparse::SparseInvertedIndex::new(dim, metric);
            for (i, vec) in pairs.iter().enumerate() {
                // Canonical by construction above; failures fall back exact.
                if index.insert(i as u64, vec.clone()).is_err() {
                    drop(cache);
                    return exact_sparse_fallback(rows, col_pos, query, metric, k);
                }
            }
            plomid_core::observability::record_vector_index_build();
            plomid_core::observability::record_vector_rebuild(
                rebuild_started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
            let cache_cap = crate::vector_settings::get_int("vector.cache_graphs")
                .unwrap_or(DEFAULT_CACHED_INDEXES as i64)
                .clamp(1, 64) as usize;
            if cache.entries.len() >= cache_cap {
                if let Some(victim) = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                {
                    cache.entries.remove(&victim);
                }
            }
            let mut stats = plomid_vector::VectorStats::unknown(dim);
            stats.row_count = rows.len() as u64;
            stats.live_rows = (rows.len() as u64).saturating_sub(null_count);
            stats.null_count = null_count;
            stats.zero_norm_count = zero_norm_count;
            stats.indexed_rows = pairs.len() as u64;
            stats.build_watermark = watermark;
            stats.stats_tick = tick;
            if where_fingerprint.is_empty() {
                stats.observe_selectivity(1.0);
            }
            cache.entries.insert(
                key.clone(),
                CachedGraph {
                    watermark,
                    row_count: rows.len(),
                    last_used: tick,
                    backend: Backend::Sparse(index),
                    stats,
                },
            );
        }
        let entry = match cache.entries.get_mut(&key) {
            Some(entry) => {
                entry.last_used = tick;
                if fresh {
                    plomid_core::observability::record_vector_ann_cache_hit();
                }
                plomid_core::observability::record_vector_ann();
                entry
                    .backend
                    .search_sparse(query, k.max(1))
                    .unwrap_or_default()
            }
            None => {
                drop(cache);
                return exact_sparse_fallback(rows, col_pos, query, metric, k);
            }
        };
        entry
            .into_iter()
            .filter_map(|hit| {
                let lane_idx = hit.row_id as usize;
                pairs.get(lane_idx)?;
                positions.get(lane_idx).copied()
            })
            .collect::<Vec<usize>>()
    };
    Ok(ordered_positions
        .into_iter()
        .filter_map(|pos| rows.get(pos).cloned())
        .collect())
}

/// Exact sparse Top-K fallback (scalar kernels over materialized rows).
fn exact_sparse_fallback(
    rows: &[Vec<Value>],
    col_pos: usize,
    query: &plomid_vector::sparse::SparseVector,
    metric: plomid_vector::index::Metric,
    k: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    plomid_core::observability::record_vector_ann_fallback();
    let qpairs: Vec<(u32, f32)> = query
        .indices
        .iter()
        .zip(query.values.iter())
        .map(|(i, v)| (*i, *v))
        .collect();
    let mut scored: Vec<(f32, usize)> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let pairs: Option<Vec<(u32, f32)>> = match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Sparse(s))) => Some(
                s.indices
                    .iter()
                    .zip(s.values.iter())
                    .map(|(i, v)| (*i, *v))
                    .collect(),
            ),
            _ => None,
        };
        let Some(pairs) = pairs else {
            continue;
        };
        let d = match metric {
            plomid_vector::index::Metric::L2 => {
                Some(plomid_vector::distance::l2_distance_sparse(&qpairs, &pairs))
            }
            plomid_vector::index::Metric::Cosine => {
                plomid_vector::distance::cosine_distance_sparse(&qpairs, &pairs).unwrap_or(None)
            }
            plomid_vector::index::Metric::Dot => {
                Some(plomid_vector::distance::dot_sparse(&qpairs, &pairs))
            }
            plomid_vector::index::Metric::Hamming | plomid_vector::index::Metric::Jaccard => None,
        };
        if let Some(d) = d {
            scored.push((d, idx));
        }
    }
    scored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    scored.truncate(k);
    Ok(scored
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}
/// Builds (or reuses) the cached backend and runs candidate + exact-rerank Top-K.
///
/// Shared by the row-store and join-engine call sites: `rows` are the full
/// snapshot-visible working set in a stable order, `col_pos` indexes the
/// vector column within them.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ann<E: StorageEngine>(
    engine: &E,
    database: &str,
    qualified_table: &str,
    column: &str,
    vector_type: &plomid_vector::VectorType,
    metric: plomid_vector::VectorMetric,
    method: AnnMethod,
    where_fingerprint: String,
    query: &[f32],
    rows: &[Vec<Value>],
    col_pos: usize,
    k: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    // Materialize dense lanes in scan order, skipping NULLs. Build-time
    // statistics ride along for free: NULL skips are counted here, and the
    // squared norm accumulates over data already in cache (zero-norm rows
    // are cosine-undefined and excluded from cosine indexes).
    let mut lanes: Vec<Vec<f32>> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    let mut null_count: u64 = 0;
    let mut zero_norm_count: u64 = 0;
    for (idx, row) in rows.iter().enumerate() {
        match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Dense(d)))
                if d.dimension == vector_type.dimension =>
            {
                let norm_sq: f64 = d
                    .values_f32
                    .iter()
                    .map(|v| f64::from(*v) * f64::from(*v))
                    .sum();
                if norm_sq == 0.0 {
                    zero_norm_count += 1;
                }
                lanes.push(d.values_f32.clone());
                positions.push(idx);
            }
            // Non-dense or NULL rows stay visible to the exact scan only;
            // ANN never invents them. If any row needs them, decline.
            Some(Value::Null) => {
                null_count += 1;
                continue;
            }
            _ => return exact_fallback(rows, col_pos, query, metric, k),
        }
    }
    if lanes.is_empty() {
        return Ok(Vec::new());
    }
    // Operator-governed build ceiling: estates above it stay correct via
    // the exact scan (shard into smaller indexes for ANN at that scale).
    let index_cap = crate::vector_settings::get_int("vector.max_index_vectors")
        .unwrap_or(1_000_000)
        .max(1) as usize;
    if lanes.len() > index_cap {
        return exact_fallback(rows, col_pos, query, metric, k);
    }
    // Memory governance (`vector.max_memory_bytes`): refuse an in-memory
    // build whose payload estimate exceeds the budget. The exact scan below
    // streams rows and stays correct at any scale.
    let payload_bytes = vector_type
        .row_payload_bytes()
        .unwrap_or_else(|_| vector_type.dimension as usize * 4)
        .max(1) as u64;
    let memory_limit = crate::vector_settings::get_int("vector.max_memory_bytes")
        .unwrap_or(1_073_741_824)
        .max(0) as u64;
    if plomid_vector::options::memory_estimate_bytes(lanes.len() as u64, payload_bytes)
        > memory_limit
    {
        return exact_fallback(rows, col_pos, query, metric, k);
    }
    let watermark = crate::columnar_freshness::writes_since_boot(
        &crate::columnar_freshness::freshness_key(engine.root(), database, qualified_table),
    );
    // The cache key carries the index metric: backends bake the metric into
    // their structure (graph beam, codebooks, block maps), so sharing an
    // entry across metrics would propose candidates under the wrong
    // distance. Same-metric queries still share entries as before.
    let key = (
        database.to_string(),
        qualified_table.to_string(),
        column.to_string(),
        where_fingerprint.clone(),
        format!("{metric:?}"),
    );
    let index_metric = sql_to_index_metric(metric);
    let candidate_k = {
        let oversample = crate::vector_settings::get_int("vector.candidate_oversample")
            .unwrap_or(DEFAULT_OVERSAMPLE_FACTOR as i64)
            .clamp(1, 64) as usize;
        (k.saturating_mul(oversample).max(k.saturating_add(64))).min(lanes.len())
    };
    let search = |backend: &Backend| -> Vec<(f32, usize)> {
        let hits = backend.search(query, candidate_k).unwrap_or_default();
        // Exact rerank from authoritative lanes; deterministic ties.
        let mut reranked: Vec<(f32, usize)> = hits
            .into_iter()
            .filter_map(|hit| {
                let lane_idx = hit.row_id as usize;
                let exact = exact_lane_distance(query, lanes.get(lane_idx)?, metric)?;
                Some((exact, positions.get(lane_idx).copied()?))
            })
            .collect();
        reranked.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        reranked.truncate(k);
        reranked
    };
    // Cache lookup / rebuild under one lock; builds are bounded by
    // `vector.max_ann_rows` and run off the already-materialized lanes.
    // DiskANN additionally persists segments under the database directory so
    // graphs survive restarts; HNSW rebuilds from rows on miss.
    let ordered_positions = {
        let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.tick = cache.tick.wrapping_add(1);
        let tick = cache.tick;
        let fresh = cache.entries.get(&key).is_some_and(|entry| {
            entry.watermark == watermark
                && entry.row_count == rows.len()
                && entry.backend.live() == lanes.len()
        });
        plomid_core::observability::record_vector_freshness(fresh);
        if !fresh {
            plomid_core::observability::record_vector_ann_cache_miss();
            let data_dir = match method {
                AnnMethod::Hnsw
                | AnnMethod::IvfFlat
                | AnnMethod::Sq8
                | AnnMethod::Pq
                | AnnMethod::Sparse
                | AnnMethod::Binary => None,
                AnnMethod::DiskAnn => Some(diskann_dir(
                    engine.root(),
                    database,
                    qualified_table,
                    column,
                )),
            };
            let rebuild_started = std::time::Instant::now();
            let backend = match Backend::build(method, vector_type, index_metric, &lanes, data_dir)
            {
                Ok(backend) => backend,
                Err(_) => {
                    drop(cache);
                    return exact_fallback(rows, col_pos, query, metric, k);
                }
            };
            plomid_core::observability::record_vector_index_build();
            plomid_core::observability::record_vector_rebuild(
                rebuild_started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
            let cache_cap = crate::vector_settings::get_int("vector.cache_graphs")
                .unwrap_or(DEFAULT_CACHED_INDEXES as i64)
                .clamp(1, 64) as usize;
            if cache.entries.len() >= cache_cap {
                // LRU eviction: drop the stalest entry.
                if let Some(victim) = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                {
                    cache.entries.remove(&victim);
                }
            }
            // Build-time observation: counts measured during the
            // materialization scan above (NULL skips, zero norms over
            // cache-resident lanes, indexed lanes).
            let mut stats = plomid_vector::VectorStats::unknown(vector_type.dimension);
            stats.row_count = rows.len() as u64;
            stats.live_rows = (rows.len() as u64).saturating_sub(null_count);
            stats.null_count = null_count;
            stats.zero_norm_count = zero_norm_count;
            stats.indexed_rows = lanes.len() as u64;
            stats.build_watermark = watermark;
            stats.stats_tick = tick;
            if where_fingerprint.is_empty() {
                // No filter: the working set is the table.
                stats.observe_selectivity(1.0);
            }
            cache.entries.insert(
                key.clone(),
                CachedGraph {
                    watermark,
                    row_count: rows.len(),
                    last_used: tick,
                    backend,
                    stats,
                },
            );
        }
        let entry = match cache.entries.get_mut(&key) {
            Some(entry) => {
                entry.last_used = tick;
                if fresh {
                    plomid_core::observability::record_vector_ann_cache_hit();
                }
                plomid_core::observability::record_vector_ann();
                search(&entry.backend)
            }
            None => {
                drop(cache);
                return exact_fallback(rows, col_pos, query, metric, k);
            }
        };
        entry
    };
    Ok(ordered_positions
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}

/// Builds (or reuses) the cached Binary index and runs Hamming ANN with an
/// exact rerank from authoritative storage bytes.
///
/// Contract mirror of [`run_ann`] for bit-packed lanes: `RowId` == lane
/// position, result order is `(exact_distance asc, scan_position asc)`, and
/// every decline (mixed values, over-cap builds, backend errors) returns
/// `None` so callers keep the exact packed scan. Jaccard never reaches here
/// (the dispatcher declines it); the [`BinaryIndex`] itself additionally
/// rejects non-Hamming ANN calls.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_binary_ann<E: StorageEngine>(
    engine: &E,
    database: &str,
    qualified_table: &str,
    column: &str,
    vector_type: &plomid_vector::VectorType,
    where_fingerprint: String,
    query_words: &[u64],
    rows: &[Vec<Value>],
    col_pos: usize,
    k: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    use plomid_vector::index::binary::{BinaryAnnProbes, BinaryIndex};
    use plomid_vector::index::{IndexConfig, IndexKind};
    // Materialize packed lanes in scan order, skipping NULLs. Any non-binary
    // value declines: ANN must never invent rows the exact scan would skip.
    let mut packed: Vec<Vec<u8>> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Binary(b)))
                if b.dimension_bits == vector_type.dimension =>
            {
                packed.push(b.bytes.clone());
                positions.push(idx);
            }
            Some(Value::Null) => continue,
            _ => {
                return Ok(exact_binary_scan(
                    rows,
                    col_pos,
                    query_words,
                    vector_type.dimension,
                    k,
                ))
            }
        }
    }
    if packed.is_empty() {
        return Ok(Vec::new());
    }
    // Operator-governed build ceiling and memory governance, mirroring run_ann.
    let index_cap = crate::vector_settings::get_int("vector.max_index_vectors")
        .unwrap_or(1_000_000)
        .max(1) as usize;
    if packed.len() > index_cap {
        return Ok(exact_binary_scan(
            rows,
            col_pos,
            query_words,
            vector_type.dimension,
            k,
        ));
    }
    let payload_bytes = vector_type
        .row_payload_bytes()
        .unwrap_or_else(|_| vector_type.dimension.div_ceil(8) as usize)
        .max(1) as u64;
    let memory_limit = crate::vector_settings::get_int("vector.max_memory_bytes")
        .unwrap_or(1_073_741_824)
        .max(0) as u64;
    if plomid_vector::options::memory_estimate_bytes(packed.len() as u64, payload_bytes)
        > memory_limit
    {
        return Ok(exact_binary_scan(
            rows,
            col_pos,
            query_words,
            vector_type.dimension,
            k,
        ));
    }
    let dim_bits = vector_type.dimension as usize;
    let expected_words = query_words.len();
    let watermark = crate::columnar_freshness::writes_since_boot(
        &crate::columnar_freshness::freshness_key(engine.root(), database, qualified_table),
    );
    // The binary lane serves Hamming only (dispatcher-enforced): the metric
    // component is constant. See the key comment in `run_ann` for why the
    // metric is part of the key at all.
    let key = (
        database.to_string(),
        qualified_table.to_string(),
        column.to_string(),
        where_fingerprint.clone(),
        "Hamming".to_string(),
    );
    let candidate_k = {
        let oversample = crate::vector_settings::get_int("vector.candidate_oversample")
            .unwrap_or(DEFAULT_OVERSAMPLE_FACTOR as i64)
            .clamp(1, 64) as usize;
        (k.saturating_mul(oversample).max(k.saturating_add(64))).min(packed.len())
    };
    let search = |backend: &Backend, lanes: &[Vec<u8>]| -> Vec<(u64, usize)> {
        let index = match backend {
            Backend::Binary(index) => index,
            _ => return Vec::new(),
        };
        let hits = index
            .ann_search_words(query_words, candidate_k, BinaryAnnProbes::default())
            .unwrap_or_default();
        // Exact rerank from authoritative storage bytes; deterministic ties.
        let mut reranked: Vec<(u64, usize)> = hits
            .into_iter()
            .filter_map(|hit| {
                let lane_idx = hit.row_id as usize;
                let lane = lanes.get(lane_idx)?;
                let exact = query_lane_hamming(query_words, lane, dim_bits)?;
                Some((exact, positions.get(lane_idx).copied()?))
            })
            .collect();
        reranked.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        reranked.truncate(k);
        reranked
    };
    let ordered_positions = {
        let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.tick = cache.tick.wrapping_add(1);
        let tick = cache.tick;
        let fresh = cache.entries.get(&key).is_some_and(|entry| {
            entry.watermark == watermark
                && entry.row_count == rows.len()
                && entry.backend.live() == packed.len()
        });
        plomid_core::observability::record_vector_freshness(fresh);
        if !fresh {
            plomid_core::observability::record_vector_ann_cache_miss();
            let config = IndexConfig::new(
                IndexKind::Binary,
                plomid_vector::index::Metric::Hamming,
                dim_bits,
            );
            let backend = match BinaryIndex::new(config) {
                Ok(mut index) => {
                    let mut ok = true;
                    for (lane_idx, lane) in packed.iter().enumerate() {
                        let words = match BinaryIndex::pack_bits(lane, dim_bits) {
                            Ok(words) => words,
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        };
                        if words.len() != expected_words
                            || index.insert_words(lane_idx as u64, &words).is_err()
                        {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        drop(cache);
                        return Ok(exact_binary_scan(
                            rows,
                            col_pos,
                            query_words,
                            vector_type.dimension,
                            k,
                        ));
                    }
                    Backend::Binary(index)
                }
                Err(_) => {
                    drop(cache);
                    return Ok(exact_binary_scan(
                        rows,
                        col_pos,
                        query_words,
                        vector_type.dimension,
                        k,
                    ));
                }
            };
            plomid_core::observability::record_vector_index_build();
            let cache_cap = crate::vector_settings::get_int("vector.cache_graphs")
                .unwrap_or(DEFAULT_CACHED_INDEXES as i64)
                .clamp(1, 64) as usize;
            if cache.entries.len() >= cache_cap {
                if let Some(victim) = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                {
                    cache.entries.remove(&victim);
                }
            }
            let mut stats = plomid_vector::VectorStats::unknown(vector_type.dimension);
            stats.row_count = rows.len() as u64;
            stats.live_rows = packed.len() as u64;
            stats.indexed_rows = packed.len() as u64;
            stats.build_watermark = watermark;
            stats.stats_tick = tick;
            if where_fingerprint.is_empty() {
                stats.observe_selectivity(1.0);
            }
            cache.entries.insert(
                key.clone(),
                CachedGraph {
                    watermark,
                    row_count: rows.len(),
                    last_used: tick,
                    backend,
                    stats,
                },
            );
        }
        let entry = match cache.entries.get_mut(&key) {
            Some(entry) => {
                entry.last_used = tick;
                if fresh {
                    plomid_core::observability::record_vector_ann_cache_hit();
                }
                plomid_core::observability::record_vector_ann();
                search(&entry.backend, &packed)
            }
            None => {
                drop(cache);
                return Ok(exact_binary_scan(
                    rows,
                    col_pos,
                    query_words,
                    vector_type.dimension,
                    k,
                ));
            }
        };
        entry
    };
    Ok(ordered_positions
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}

/// Exact Hamming distance between packed query words and storage bytes.
fn query_lane_hamming(query_words: &[u64], lane_bytes: &[u8], dim_bits: usize) -> Option<u64> {
    use plomid_vector::index::binary::BinaryIndex;
    let lane_words = BinaryIndex::pack_bits(lane_bytes, dim_bits).ok()?;
    if lane_words.len() != query_words.len() {
        return None;
    }
    Some(
        query_words
            .iter()
            .zip(lane_words.iter())
            .map(|(a, b)| (a ^ b).count_ones() as u64)
            .sum(),
    )
}

/// Exact packed Hamming top-k over snapshot rows (the ANN decline path and
/// the Jaccard route, which never enters the index).
fn exact_binary_scan(
    rows: &[Vec<Value>],
    col_pos: usize,
    query_words: &[u64],
    dimension_bits: u32,
    k: usize,
) -> Vec<Vec<Value>> {
    plomid_core::observability::record_vector_ann_fallback();
    let dim_bits = dimension_bits as usize;
    let mut scored: Vec<(u64, usize)> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let lane_bytes = match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Binary(b)))
                if b.dimension_bits == dimension_bits =>
            {
                &b.bytes
            }
            _ => continue,
        };
        if let Some(d) = query_lane_hamming(query_words, lane_bytes, dim_bits) {
            scored.push((d, idx));
        }
    }
    scored.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.truncate(k);
    scored
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect()
}

/// Packed query words for a binary Hamming ANN probe.
///
/// Accepts native binary vectors and the canonical text literal forms (the
/// same `parse_vector_literal` the scalar distance functions use, so ANN
/// eligibility matches exactly what the exact scan accepts). Dimension
/// mismatches and unparsable text decline (`None`) to the exact scan.
pub(crate) fn binary_query_words(query_value: &Value, dimension: u32) -> Option<Vec<u64>> {
    use plomid_vector::index::binary::BinaryIndex;
    let binary = match query_value {
        Value::Vector(plomid_vector::VectorValue::Binary(b)) => {
            if b.dimension_bits != dimension {
                return None;
            }
            b.bytes.clone()
        }
        Value::Text(s) | Value::VarChar(s) | Value::Unknown(s) => {
            match plomid_types::text::parse_vector_literal(s).ok()? {
                Value::Vector(plomid_vector::VectorValue::Binary(b)) => {
                    if b.dimension_bits != dimension {
                        return None;
                    }
                    b.bytes.clone()
                }
                _ => return None,
            }
        }
        _ => return None,
    };
    BinaryIndex::pack_bits(&binary, dimension as usize).ok()
}

/// Parsed pure-radius predicate: `distance_fn(vector_col, query) </<= bound`
/// or `similarity_fn(vector_col, query) >/>= threshold` (converted).
struct RadiusRequest {
    column: String,
    vector_type: plomid_vector::VectorType,
    /// SQL-facing metric.
    metric: plomid_vector::VectorMetric,
    /// Index-facing metric.
    index_metric: plomid_vector::index::Metric,
    method: AnnMethod,
    query_lanes: Vec<f32>,
    /// SQL-facing bound (rooted for L2, native otherwise).
    bound: f32,
}

/// Parses a pure-radius `WHERE` clause, or declines (`None`) so the generic
/// exact predicate path preserves semantics byte-for-byte.
///
/// Accepted shape is exactly one comparison with a distance/similarity
/// function of `(vector_column, query)` on one side and a numeric literal
/// on the other. Anything else — conjunctions, reversed operands, mixed
/// metrics, non-literal bounds — keeps the generic path.
/// Pure radius predicate shape, shared by execution and EXPLAIN so the
/// two can never disagree about what counts as a radius query.
pub(crate) struct RadiusShape<'a> {
    /// Distance/similarity function name as written.
    pub func_name: &'a str,
    /// Vector column name.
    pub column: &'a str,
    /// Numeric threshold as written (bound or similarity threshold).
    pub threshold: f64,
    /// Whether the comparison selects by similarity (`>`/`>=`).
    pub similarity: bool,
    /// The non-column argument (query vector side).
    pub query_arg: &'a Expression,
}

/// Extracts the radius shape: exactly one comparison with a distance or
/// similarity function of `(vector_column, query)` on one side and a
/// numeric literal on the other. Anything else declines.
pub(crate) fn parse_radius_shape(where_expr: &Expression) -> Option<RadiusShape<'_>> {
    use plomid_sql::Expression as E;
    // (function, bound, similarity?) for the four accepted shapes.
    let (func, bound_value, similarity) = match where_expr {
        E::Less(func, bound) | E::LessOrEqual(func, bound) => {
            (func.as_ref(), bound.as_ref(), false)
        }
        E::Greater(func, bound) | E::GreaterOrEqual(func, bound) => {
            (func.as_ref(), bound.as_ref(), true)
        }
        _ => return None,
    };
    let E::FunctionCall { name, args, .. } = func else {
        return None;
    };
    if args.len() != 2 {
        return None;
    }
    let column = args.iter().find_map(|arg| match arg {
        Expression::ColumnRef(col) => Some(col.as_str()),
        _ => None,
    })?;
    let E::Literal(bound_literal) = bound_value else {
        return None;
    };
    let threshold = bound_to_f64(bound_literal)?;
    let query_arg = args
        .iter()
        .find(|arg| !matches!(arg, Expression::ColumnRef(_)))?;
    Some(RadiusShape {
        func_name: name,
        column,
        threshold,
        similarity,
        query_arg,
    })
}

fn parse_radius_request(
    catalog: &plomid_sql::InMemoryCatalog,
    table: &str,
    schema: &TableSchema,
    where_expr: &Expression,
    probe_row: &[Value],
) -> Option<RadiusRequest> {
    let shape = parse_radius_shape(where_expr)?;
    let name = shape.func_name;
    let column = shape.column;
    let similarity = shape.similarity;
    let threshold = shape.threshold;
    let query_arg = shape.query_arg;
    // Distance functions pair with `<`; similarity functions with `>`.
    // A distance threshold can never be a similarity query and vice versa.
    let metric = sql_metric_from_name(name, similarity)?;
    // Radius execution covers dense L2/Cosine/Dot (f32 lanes). Sparse,
    // binary and multi keep their exact kernels via the generic path.
    let Ok(pos) = schema.column_index(column) else {
        return None;
    };
    let col_type = schema.columns.get(pos)?.col_type;
    if col_type.type_oid != plomid_types::TypeOid::VECTOR {
        return None;
    }
    let vector_type = plomid_vector::VectorType::typmod_decode(col_type.typmod).ok()?;
    if vector_type.kind != plomid_vector::VectorKind::Dense {
        return None;
    }
    if !vector_type.supports_hnsw(metric)
        && !(matches!(
            metric,
            plomid_vector::VectorMetric::L2
                | plomid_vector::VectorMetric::Cosine
                | plomid_vector::VectorMetric::Dot
        ))
    {
        return None;
    }
    let qualified = catalog.resolve_table_namespace(table);
    let method = resolve_ann_method(catalog, &qualified, column, &vector_type, metric)?;
    if matches!(method, AnnMethod::Sparse) {
        return None;
    }
    // Evaluate the query side (literals/casts never touch the row);
    // anything unevaluatable keeps the generic path.
    let query_value = evaluate_expression(probe_row, schema, query_arg).ok()?;
    let Value::Vector(plomid_vector::VectorValue::Dense(d)) = query_value else {
        return None;
    };
    if d.dimension != vector_type.dimension {
        return None;
    }
    // Similarity thresholds convert to distance bounds; distance bounds
    // pass through. Unconvertible or non-finite thresholds decline.
    let bound = if similarity {
        plomid_vector::planner_api::similarity_to_distance_bound(metric, threshold as f32)?
    } else {
        threshold as f32
    };
    if !bound.is_finite() {
        return None;
    }
    Some(RadiusRequest {
        column: column.to_string(),
        vector_type,
        metric,
        index_metric: sql_to_index_metric(metric),
        method,
        query_lanes: d.values_f32.clone(),
        bound,
    })
}

/// Exact radius over materialized rows (fallback and small inputs).
///
/// Records the fallback counter so operators can see how often
/// ANN-eligible radius shapes still resolve exactly.
fn exact_radius_fallback(
    rows: &[Vec<Value>],
    col_pos: usize,
    query: &[f32],
    metric: plomid_vector::VectorMetric,
    bound: f32,
    limit: Option<usize>,
) -> SqlResult<Vec<Vec<Value>>> {
    plomid_core::observability::record_vector_ann_fallback();
    let ceiling = bound + plomid_vector::distance::TOLERANCE;
    let mut scored: Vec<(f32, usize)> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let lane: &[f32] = match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Dense(d))) => &d.values_f32,
            _ => continue,
        };
        if let Some(d) = exact_lane_distance(query, lane, metric) {
            if d <= ceiling {
                scored.push((d, idx));
            }
        }
    }
    scored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    if let Some(k) = limit {
        scored.truncate(k);
    }
    Ok(scored
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}

/// Builds (or reuses) the cached backend and serves exact-verified radius.
///
/// Mirrors [`run_ann`]: the candidate well comes from the backend's
/// `range_search` over the bounded live set (HNSW and IVF-Flat enumerate
/// every live row with exact distances), then every candidate is exactly
/// re-scored from authoritative lanes and filtered against the SQL bound.
/// The well bounds *work*; results are every row within the bound, sorted
/// `(distance asc, scan position asc)`, never truncated to fit the well.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_ann_radius<E: StorageEngine>(
    engine: &E,
    database: &str,
    qualified_table: &str,
    column: &str,
    vector_type: &plomid_vector::VectorType,
    metric: plomid_vector::VectorMetric,
    index_metric: plomid_vector::index::Metric,
    method: AnnMethod,
    where_fingerprint: String,
    query: &[f32],
    rows: &[Vec<Value>],
    col_pos: usize,
    bound: f32,
    limit: Option<usize>,
) -> SqlResult<Vec<Vec<Value>>> {
    // SQ/PQ/DiskANN range wells are approximate (quantized or beam
    // distances): rows inside the SQL bound can be missed by the well and
    // exact verification cannot resurrect them, so index radius would
    // under-return. Decline to the exact scan; only exact-enumeration wells
    // (HNSW, IVF-Flat) serve radius.
    if matches!(method, AnnMethod::Sq8 | AnnMethod::Pq | AnnMethod::DiskAnn) {
        return exact_radius_fallback(rows, col_pos, query, metric, bound, limit);
    }
    // Materialize dense lanes in scan order, skipping NULLs, with the
    // same build-time statistics the Top-K path collects.
    let mut lanes: Vec<Vec<f32>> = Vec::new();
    let mut positions: Vec<usize> = Vec::new();
    let mut null_count: u64 = 0;
    let mut zero_norm_count: u64 = 0;
    for (idx, row) in rows.iter().enumerate() {
        match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Dense(d)))
                if d.dimension == vector_type.dimension =>
            {
                let norm_sq: f64 = d
                    .values_f32
                    .iter()
                    .map(|v| f64::from(*v) * f64::from(*v))
                    .sum();
                if norm_sq == 0.0 {
                    zero_norm_count += 1;
                }
                lanes.push(d.values_f32.clone());
                positions.push(idx);
            }
            Some(Value::Null) => {
                null_count += 1;
                continue;
            }
            _ => return exact_radius_fallback(rows, col_pos, query, metric, bound, limit),
        }
    }
    if lanes.is_empty() {
        return Ok(Vec::new());
    }
    let index_cap = crate::vector_settings::get_int("vector.max_index_vectors")
        .unwrap_or(1_000_000)
        .max(1) as usize;
    if lanes.len() > index_cap {
        return exact_radius_fallback(rows, col_pos, query, metric, bound, limit);
    }
    let payload_bytes = vector_type
        .row_payload_bytes()
        .unwrap_or_else(|_| vector_type.dimension as usize * 4)
        .max(1) as u64;
    let memory_limit = crate::vector_settings::get_int("vector.max_memory_bytes")
        .unwrap_or(1_073_741_824)
        .max(0) as u64;
    if plomid_vector::options::memory_estimate_bytes(lanes.len() as u64, payload_bytes)
        > memory_limit
    {
        return exact_radius_fallback(rows, col_pos, query, metric, bound, limit);
    }
    let watermark = crate::columnar_freshness::writes_since_boot(
        &crate::columnar_freshness::freshness_key(engine.root(), database, qualified_table),
    );
    // The cache key carries the index metric: backends bake the metric into
    // their structure (graph beam, codebooks, block maps), so sharing an
    // entry across metrics would propose candidates under the wrong
    // distance. Same-metric queries still share entries as before.
    let key = (
        database.to_string(),
        qualified_table.to_string(),
        column.to_string(),
        where_fingerprint.clone(),
        format!("{metric:?}"),
    );
    // Index-facing bound: index L2 distances are squared.
    let index_bound = plomid_vector::planner_api::index_radius_bound(metric, bound);
    let ceiling = bound + plomid_vector::distance::TOLERANCE;
    let ordered_positions = {
        let mut cache = ann_cache().lock().unwrap_or_else(|e| e.into_inner());
        cache.tick = cache.tick.wrapping_add(1);
        let tick = cache.tick;
        let fresh = cache.entries.get(&key).is_some_and(|entry| {
            entry.watermark == watermark
                && entry.row_count == rows.len()
                && entry.backend.live() == lanes.len()
        });
        plomid_core::observability::record_vector_freshness(fresh);
        if !fresh {
            plomid_core::observability::record_vector_ann_cache_miss();
            let rebuild_started = std::time::Instant::now();
            let data_dir = match method {
                AnnMethod::Hnsw
                | AnnMethod::IvfFlat
                | AnnMethod::Sq8
                | AnnMethod::Pq
                | AnnMethod::Sparse
                | AnnMethod::Binary => None,
                AnnMethod::DiskAnn => Some(diskann_dir(
                    engine.root(),
                    database,
                    qualified_table,
                    column,
                )),
            };
            let backend = match Backend::build(method, vector_type, index_metric, &lanes, data_dir)
            {
                Ok(backend) => backend,
                Err(_) => {
                    drop(cache);
                    return exact_radius_fallback(rows, col_pos, query, metric, bound, limit);
                }
            };
            plomid_core::observability::record_vector_index_build();
            plomid_core::observability::record_vector_rebuild(
                rebuild_started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
            let cache_cap = crate::vector_settings::get_int("vector.cache_graphs")
                .unwrap_or(DEFAULT_CACHED_INDEXES as i64)
                .clamp(1, 64) as usize;
            if cache.entries.len() >= cache_cap {
                if let Some(victim) = cache
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(key, _)| key.clone())
                {
                    cache.entries.remove(&victim);
                }
            }
            let mut stats = plomid_vector::VectorStats::unknown(vector_type.dimension);
            stats.row_count = rows.len() as u64;
            stats.live_rows = (rows.len() as u64).saturating_sub(null_count);
            stats.null_count = null_count;
            stats.zero_norm_count = zero_norm_count;
            stats.indexed_rows = lanes.len() as u64;
            stats.build_watermark = watermark;
            stats.stats_tick = tick;
            if where_fingerprint.is_empty() {
                stats.observe_selectivity(1.0);
            }
            cache.entries.insert(
                key.clone(),
                CachedGraph {
                    watermark,
                    row_count: rows.len(),
                    last_used: tick,
                    backend,
                    stats,
                },
            );
        }
        let entry = match cache.entries.get_mut(&key) {
            Some(entry) => {
                entry.last_used = tick;
                if fresh {
                    plomid_core::observability::record_vector_ann_cache_hit();
                }
                plomid_core::observability::record_vector_ann();
                // Candidate well, then exact verification against the SQL
                // bound: approximate candidates can never leak into results.
                let mut verified: Vec<(f32, usize)> = entry
                    .backend
                    .range_search(query, index_bound)
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|hit| {
                        let lane_idx = hit.row_id as usize;
                        let exact = exact_lane_distance(query, lanes.get(lane_idx)?, metric)?;
                        if exact <= ceiling {
                            Some((exact, positions.get(lane_idx).copied()?))
                        } else {
                            None
                        }
                    })
                    .collect();
                verified.sort_by(|a, b| {
                    a.0.partial_cmp(&b.0)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.1.cmp(&b.1))
                });
                if let Some(k) = limit {
                    verified.truncate(k);
                }
                verified
            }
            None => {
                drop(cache);
                return exact_radius_fallback(rows, col_pos, query, metric, bound, limit);
            }
        };
        entry
    };
    Ok(ordered_positions
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}

/// Tries the ANN radius path for a pure-radius `WHERE` over decoded rows.
///
/// Returns `None` when the shape is not radius-eligible (callers run the
/// generic exact predicate path); otherwise the bound-filtered rows. The
/// planner gates the strategy first: negative bounds return valid empty
/// without touching storage, and exact-planned shapes decline here.
///
/// Reachability note: the only live call site sits inside the indexed-probe
/// path, which requires a btree index range over the same `WHERE` — but a
/// bare radius comparison yields no such range, and any conjunction fails
/// shape parsing. Pure-radius queries therefore reach this only in narrow
/// hybrid cases today; the generic predicate scan serves the rest exactly.
/// The SQ/PQ/DiskANN decline inside `run_ann_radius` guards the path
/// wherever it engages.
#[allow(clippy::too_many_arguments)]
pub(crate) fn try_ann_radius<E: StorageEngine>(
    engine: &E,
    catalog: &plomid_sql::InMemoryCatalog,
    database: &str,
    table: &str,
    schema: &TableSchema,
    rows: &[Vec<Value>],
    where_expr: &Expression,
    limit: Option<usize>,
    current_user: &str,
) -> Option<SqlResult<Vec<Vec<Value>>>> {
    let _ = current_user;
    if engine.is_read_pinned() {
        return None;
    }
    if rows.is_empty() {
        return Some(Ok(Vec::new()));
    }
    if !crate::vector_settings::ann_enabled() {
        return None;
    }
    let req = parse_radius_request(catalog, table, schema, where_expr, &rows[0])?;
    // Negative bounds match nothing: valid empty, no storage touch.
    if req.bound < 0.0 {
        return Some(Ok(Vec::new()));
    }
    let Ok(pos) = schema.column_index(&req.column) else {
        return None;
    };
    // Planner gate on live build-time statistics when a fresh cache entry
    // exists; otherwise the safe default (exact) applies.
    let qualified = catalog.resolve_table_namespace(table);
    let live_options = plomid_vector::VectorSearchOptions::default();
    let cached = cached_graph_stats(database, &qualified, &req.column, "");
    let stats;
    let stats_ref = match cached {
        Some(s) => {
            stats = s;
            &stats
        }
        None => {
            // No observation yet: collect one cheaply over the working set
            // (NULL skips + zero norms; no distances computed).
            stats = collect_radius_stats(rows, pos, req.vector_type.dimension);
            &stats
        }
    };
    let plan = plomid_vector::planner_api::plan_radius_search(
        stats_ref,
        req.metric,
        req.bound,
        true,
        &live_options,
    );
    match plan.strategy {
        plomid_vector::planner_api::RadiusStrategy::EmptyResult => {
            return Some(Ok(Vec::new()));
        }
        plomid_vector::planner_api::RadiusStrategy::ExactEnumeration => return None,
        plomid_vector::planner_api::RadiusStrategy::AnnCandidateVerify => {}
    }
    Some(run_ann_radius(
        engine,
        database,
        &qualified,
        &req.column,
        &req.vector_type,
        req.metric,
        req.index_metric,
        req.method,
        String::new(),
        &req.query_lanes,
        rows,
        pos,
        req.bound,
        limit,
    ))
}

/// Cheap build-time statistics over a decoded working set (no distances).
fn collect_radius_stats(
    rows: &[Vec<Value>],
    col_pos: usize,
    dimension: u32,
) -> plomid_vector::VectorStats {
    let mut stats = plomid_vector::VectorStats::unknown(dimension);
    for row in rows {
        match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Dense(d)))
                if d.dimension == dimension =>
            {
                let zero = d.values_f32.iter().all(|v| *v == 0.0);
                stats.observe_row(false, zero, true);
            }
            _ => stats.observe_row(true, false, false),
        }
    }
    stats
}

/// Exact Top-K over already-materialized rows (ANN fallback and smallest inputs).
///
/// Records the fallback counter so operators can see how often ANN-eligible
/// shapes still resolve exactly.
fn exact_fallback(
    rows: &[Vec<Value>],
    col_pos: usize,
    query: &[f32],
    metric: plomid_vector::VectorMetric,
    k: usize,
) -> SqlResult<Vec<Vec<Value>>> {
    plomid_core::observability::record_vector_ann_fallback();
    let mut scored: Vec<(f32, usize)> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let lane: &[f32] = match row.get(col_pos) {
            Some(Value::Vector(plomid_vector::VectorValue::Dense(d))) => &d.values_f32,
            _ => continue,
        };
        if let Some(d) = exact_lane_distance(query, lane, metric) {
            scored.push((d, idx));
        }
    }
    scored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    scored.truncate(k);
    Ok(scored
        .into_iter()
        .filter_map(|(_, pos)| rows.get(pos).cloned())
        .collect())
}

/// Exact distance for one lane, or `None` for undefined (zero-norm cosine).
fn exact_lane_distance(
    query: &[f32],
    lane: &[f32],
    metric: plomid_vector::VectorMetric,
) -> Option<f32> {
    use plomid_vector::distance as dist;
    if query.len() != lane.len() {
        return None;
    }
    match metric {
        plomid_vector::VectorMetric::Cosine => {
            dist::cosine_distance(Some(query), Some(lane)).ok()?
        }
        plomid_vector::VectorMetric::L2 => dist::l2_distance(Some(query), Some(lane)).ok()?,
        plomid_vector::VectorMetric::Dot => dist::dot(Some(query), Some(lane)).ok()?,
        plomid_vector::VectorMetric::Hamming | plomid_vector::VectorMetric::Jaccard => None,
    }
}
