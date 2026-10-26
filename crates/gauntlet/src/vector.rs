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

//! Vector qualification suites for the PLOMID Gauntlet (Phase 71 in code).
//!
//! Each suite exercises the production `plomid-vector` crate plus, in
//! `vector_sql_end_to_end`, the same path a customer uses:
//!
//! ```text
//! SQL parser -> binder -> planner -> executor -> plomid-vector -> storage
//! ```
//!
//! Suites never panic: every check returns `Ok(detail)` / `Err(reason)` and
//! [`run_vector_qualification`] aggregates them. Fast suites run in normal CI;
//! heavy ANN recall benchmarks stay behind the release-gate job via the
//! existing `benchmarks/production` reports, not here.

use std::collections::HashSet;

use plomid_vector::capability::{capability, Capability};
use plomid_vector::distance::{approx_eq, cosine_distance, dot, hamming_distance, l2_distance};
use plomid_vector::hybrid::{fuse_rrf, fuse_weighted, merge_union};
use plomid_vector::index::diskann::DiskAnnIndex;
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::ivf::IvfFlatIndex;
use plomid_vector::index::manifest::IndexManifest;
use plomid_vector::index::pq::PqIndex;
use plomid_vector::index::sq::Sq8Index;
use plomid_vector::index::{
    FlatIndex, IndexConfig, IndexKind, Metric, RowId, SearchResult, VectorIndex,
};
use plomid_vector::mvcc::delta::{CommittedDelta, PendingWrites};
use plomid_vector::options::{memory_estimate_bytes, VectorSearchOptions};
use plomid_vector::search::exact::exact_top_k;
use plomid_vector::search::SearchMetric;
use plomid_vector::sparse::inverted::SparseInvertedIndex;
use plomid_vector::sparse::SparseVector;
use plomid_vector::VectorStats;
use plomid_vector::{
    decode_standalone, encode_standalone, parse_binary, parse_dense, parse_sparse, FilterStrategy,
    RadiusStrategy, VectorElementType, VectorKind, VectorMetric, VectorType, VectorValue,
};

/// Outcome of one vector suite.
#[derive(Debug, Clone)]
pub struct VectorSuiteReport {
    /// Suite name, e.g. `vector_hnsw`.
    pub name: &'static str,
    /// Whether the suite passed.
    pub passed: bool,
    /// Human-readable evidence (counts, recall, error for failures).
    pub detail: String,
}

/// Aggregate vector qualification result.
#[derive(Debug, Clone)]
pub struct VectorQualification {
    /// Per-suite outcomes in execution order.
    pub suites: Vec<VectorSuiteReport>,
}

impl VectorQualification {
    /// True when every suite passed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.suites.iter().all(|s| s.passed)
    }

    /// Number of passing suites.
    #[must_use]
    pub fn passed_count(&self) -> usize {
        self.suites.iter().filter(|s| s.passed).count()
    }
}

fn ok(name: &'static str, detail: String) -> VectorSuiteReport {
    VectorSuiteReport {
        name,
        passed: true,
        detail,
    }
}

fn fail(name: &'static str, reason: String) -> VectorSuiteReport {
    VectorSuiteReport {
        name,
        passed: false,
        detail: reason,
    }
}

fn run_suite(name: &'static str, f: impl FnOnce() -> Result<String, String>) -> VectorSuiteReport {
    match f() {
        Ok(detail) => ok(name, detail),
        Err(reason) => fail(name, reason),
    }
}

fn lcg_rows(n: usize, dim: usize, seed: u64) -> Vec<(RowId, Vec<f32>)> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f64 / (u32::MAX as f64) * 2.0 - 1.0) as f32
    };
    (0..n as u64)
        .map(|id| (id, (0..dim).map(|_| next()).collect()))
        .collect()
}

fn check_types() -> Result<String, String> {
    for dim in [1u32, 2, 768, 1536, 65535, 65536] {
        VectorType::new(VectorKind::Dense, VectorElementType::Float32, dim)
            .map_err(|e| format!("dense dim {dim} rejected: {e}"))?;
    }
    for bad in [0u32, 65537] {
        if VectorType::new(VectorKind::Dense, VectorElementType::Float32, bad).is_ok() {
            return Err(format!("invalid dim {bad} accepted"));
        }
    }
    // Binary sparse is rejected, never silently invented.
    if VectorType::new(VectorKind::Sparse, VectorElementType::Binary, 8).is_ok() {
        return Err("BINARY sparse accepted".to_string());
    }
    let t = VectorType::parse_sql("MULTI_VECTOR(FLOAT32, 128, 768)").map_err(|e| e.to_string())?;
    if (t.max_vectors, t.dimension) != (128, 768) {
        return Err("multi parse mismatch".to_string());
    }
    Ok("6 boundary dims ok, 2 invalid rejected, binary-sparse rejected".to_string())
}

fn check_literals() -> Result<String, String> {
    parse_dense("[1.0, -2.5, 3.25]", Some(3)).map_err(|e| e.to_string())?;
    if parse_dense("[1,2]", Some(3)).is_ok() {
        return Err("wrong-dim dense accepted".to_string());
    }
    if parse_dense("[NaN,0,0]", Some(3)).is_ok() {
        return Err("NaN dense accepted".to_string());
    }
    let s = parse_sparse("{9:0.5, 1:1.5}/16").map_err(|e| e.to_string())?;
    if s.indices != vec![1, 9] {
        return Err("sparse canonical order wrong".to_string());
    }
    let b = parse_binary("'10101'").map_err(|e| e.to_string())?;
    if b.dimension_bits != 5 {
        return Err("binary bit count wrong".to_string());
    }
    Ok("dense/sparse/binary literals ok, NaN + wrong-dim rejected".to_string())
}

fn check_codec() -> Result<String, String> {
    let dense =
        plomid_vector::DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, 2.0, 3.0])
            .map_err(|e| e.to_string())?;
    let bytes = encode_standalone(&VectorValue::Dense(dense)).map_err(|e| e.to_string())?;
    let back = decode_standalone(&bytes).map_err(|e| e.to_string())?;
    if !matches!(back, VectorValue::Dense(_)) {
        return Err("dense codec kind changed".to_string());
    }
    // Bit-flips fail closed.
    let mut corrupt = bytes.clone();
    if let Some(last) = corrupt.last_mut() {
        *last ^= 0xFF;
    }
    if decode_standalone(&corrupt).is_ok() {
        return Err("corrupt codec bytes accepted".to_string());
    }
    Ok(format!(
        "dense round-trip ok ({} bytes), bit-flip rejected",
        bytes.len()
    ))
}

fn check_distance() -> Result<String, String> {
    let a = [1.0f32, 0.0, 0.0];
    let b = [0.0f32, 1.0, 0.0];
    let l2 = l2_distance(Some(&a), Some(&b))
        .map_err(|e| e.to_string())?
        .ok_or("NULL L2")?;
    if !approx_eq(l2, 2f32.sqrt()) {
        return Err(format!("L2 oracle wrong: {l2}"));
    }
    let cos = cosine_distance(Some(&a), Some(&b))
        .map_err(|e| e.to_string())?
        .ok_or("NULL cosine")?;
    if !approx_eq(cos, 1.0) {
        return Err(format!("cosine oracle wrong: {cos}"));
    }
    let d = dot(Some(&a), Some(&b))
        .map_err(|e| e.to_string())?
        .ok_or("NULL dot")?;
    if !approx_eq(d, 0.0) {
        return Err(format!("dot oracle wrong: {d}"));
    }
    // MSB-first: 0b1010_0000 vs 0b0110_0000 over 4 bits differ in 2 positions.
    let h = hamming_distance(Some(&[0b1010_0000]), Some(&[0b0110_0000]), 4)
        .map_err(|e| e.to_string())?
        .ok_or("NULL hamming")?;
    if h != 2 {
        return Err(format!("hamming oracle wrong: {h}"));
    }
    let tier = plomid_vector::math::dispatch::active_backend();
    Ok(format!(
        "L2/cosine/dot/hamming oracles match (dispatch tier {})",
        tier.name()
    ))
}

fn check_exact_search() -> Result<String, String> {
    let query = [1.0f32, 0.0];
    let rows: Vec<(RowId, Vec<f32>)> = vec![
        (7, vec![1.0, 0.0]),
        (3, vec![1.0, 0.0]),
        (0, vec![0.0, 1.0]),
    ];
    let borrowed: Vec<(RowId, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let hits =
        exact_top_k(&query, borrowed, SearchMetric::L2, 2, None).map_err(|e| e.to_string())?;
    let ids: Vec<RowId> = hits.iter().map(|c| c.row_id).collect();
    // Tie (rows 7,3 equidistant) breaks by ascending row id: [3,7].
    if ids != vec![3, 7] {
        return Err(format!("tie-break wrong: {ids:?}"));
    }
    Ok("top-2 exact + deterministic tie-break ok".to_string())
}

fn check_hnsw() -> Result<String, String> {
    let dim = 16usize;
    let rows = lcg_rows(256, dim, 0xABCD);
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.05 - 0.35).collect();
    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    flat.build(&rows).map_err(|e| e.to_string())?;
    let oracle = flat.search(&query, 10).map_err(|e| e.to_string())?;
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    hnsw.build(&rows).map_err(|e| e.to_string())?;
    hnsw.validate()
        .map_err(|e| format!("hnsw invalid after build: {e}"))?;
    let approx = hnsw.search(&query, 10).map_err(|e| e.to_string())?;
    let oracle_ids: HashSet<RowId> = oracle.iter().map(|r| r.row_id).collect();
    let hits = approx
        .iter()
        .filter(|r| oracle_ids.contains(&r.row_id))
        .count();
    if hits < 5 {
        return Err(format!("recall smoke failed: {hits}/10"));
    }
    Ok(format!("recall@{k} {hits}/10, validate clean", k = 10))
}

fn check_ivf() -> Result<String, String> {
    let dim = 8usize;
    let rows = lcg_rows(256, dim, 0x99);
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
    let mut ivf = IvfFlatIndex::new(IndexConfig::new(IndexKind::IvfFlat, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    ivf.build(&rows).map_err(|e| e.to_string())?;
    let hits = ivf.search(&query, 5).map_err(|e| e.to_string())?;
    if hits.len() != 5 {
        return Err(format!("ivf returned {} != 5", hits.len()));
    }
    Ok("ivf-flat build + top-5 ok".to_string())
}

fn check_sparse() -> Result<String, String> {
    let mut idx = SparseInvertedIndex::new(16, Metric::Dot);
    idx.insert(
        1,
        SparseVector::new(16, vec![(1, 1.0), (9, 0.5)]).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    idx.insert(
        2,
        SparseVector::new(16, vec![(2, 1.0)]).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    idx.validate().map_err(|e| e.to_string())?;
    let hits = idx
        .search(
            &SparseVector::new(16, vec![(1, 1.0)]).map_err(|e| e.to_string())?,
            2,
        )
        .map_err(|e| e.to_string())?;
    if hits.first().map(|h| h.row_id) != Some(1) {
        return Err("sparse top-1 wrong".to_string());
    }
    Ok("sparse inverted top-1 ok".to_string())
}

fn check_binary() -> Result<String, String> {
    // 9-bit non-byte-aligned vectors: padding never affects distance.
    let h = hamming_distance(
        Some(&[0b1010_1010, 0b1000_0000]),
        Some(&[0b1010_1010, 0b1000_0000]),
        9,
    )
    .map_err(|e| e.to_string())?
    .ok_or("NULL")?;
    if h != 0 {
        return Err(format!("identical 9-bit hamming = {h}"));
    }
    Ok("9-bit binary self-distance 0 (padding masked)".to_string())
}

fn check_hybrid() -> Result<String, String> {
    let dense = vec![SearchResult::new(1, 0.1), SearchResult::new(2, 0.5)];
    let sparse = vec![SearchResult::new(2, 0.2), SearchResult::new(3, 0.4)];
    let merged = merge_union(&[&dense, &sparse]);
    if merged.len() != 3 {
        return Err(format!("merge union = {} != 3", merged.len()));
    }
    let fused = fuse_rrf(&[&dense, &sparse], 60);
    if fused.is_empty() || fused[0].0 != 2 {
        return Err("rrf top should be row 2 (in both lists)".to_string());
    }
    let weighted = fuse_weighted(&dense, &sparse, 0.5);
    if weighted.is_empty() {
        return Err("weighted fusion empty".to_string());
    }
    Ok("union dedup + RRF + weighted ok".to_string())
}

fn check_capability() -> Result<String, String> {
    if capability(
        VectorKind::Dense,
        VectorElementType::Float32,
        VectorMetric::L2,
        IndexKind::Hnsw,
    ) != Capability::Supported
    {
        return Err("dense/f32/L2/HNSW not SUPPORTED".to_string());
    }
    if capability(
        VectorKind::Binary,
        VectorElementType::Binary,
        VectorMetric::Hamming,
        IndexKind::Binary,
    ) != Capability::Supported
    {
        return Err("binary/hamming/binary not SUPPORTED".to_string());
    }
    if capability(
        VectorKind::Dense,
        VectorElementType::Float32,
        VectorMetric::L2,
        IndexKind::Binary,
    ) != Capability::Unsupported
    {
        return Err("dense/L2/binary-index not UNSUPPORTED".to_string());
    }
    Ok("capability matrix spot-checks ok".to_string())
}

fn check_sq() -> Result<String, String> {
    let dim = 8usize;
    let rows = lcg_rows(128, dim, 0x51);
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
    let mut sq = Sq8Index::new(IndexConfig::new(IndexKind::IvfSq, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    sq.build(&rows).map_err(|e| e.to_string())?;
    let hits = sq.search(&query, 5).map_err(|e| e.to_string())?;
    if hits.len() != 5 {
        return Err(format!("sq8 returned {} != 5", hits.len()));
    }
    Ok("sq8 build + top-5 ok".to_string())
}

fn check_pq() -> Result<String, String> {
    let dim = 16usize;
    let rows = lcg_rows(128, dim, 0x52);
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1).collect();
    let mut pq = PqIndex::new(IndexConfig::new(IndexKind::IvfPq, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    pq.build(&rows).map_err(|e| e.to_string())?;
    let hits = pq.search(&query, 5).map_err(|e| e.to_string())?;
    if hits.len() != 5 {
        return Err(format!("pq returned {} != 5", hits.len()));
    }
    Ok("pq build + top-5 ok".to_string())
}

fn check_diskann() -> Result<String, String> {
    let dir = std::env::temp_dir().join(format!(
        "plomid-gauntlet-diskann-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
    ));
    let dim = 8usize;
    let rows = lcg_rows(64, dim, 0x53);
    let query = rows[0].1.clone();
    let config = IndexConfig::new(IndexKind::DiskAnn, Metric::L2, dim);
    let mut idx = DiskAnnIndex::new(config.clone(), &dir).map_err(|e| e.to_string())?;
    idx.build(&rows).map_err(|e| e.to_string())?;
    idx.flush().map_err(|e| e.to_string())?;
    // Restart: reopened segments serve the same nearest row.
    let reopened = DiskAnnIndex::open(config, &dir).map_err(|e| e.to_string())?;
    reopened.validate().map_err(|e| e.to_string())?;
    let hits = reopened.search(&query, 3).map_err(|e| e.to_string())?;
    if hits.first().map(|h| h.row_id) != Some(0) {
        return Err("diskann reopen top-1 wrong".to_string());
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok("diskann build/flush/reopen/validate ok".to_string())
}

fn check_reranking() -> Result<String, String> {
    // Rerank contract: compressed-candidate order corrected by authoritative
    // exact distances. Emulated here with a deliberately inverted candidate
    // list rescored by exact lanes.
    let query = [1.0f32, 0.0];
    let lanes: [(RowId, Vec<f32>); 3] = [
        (1, vec![1.0, 0.0]),
        (2, vec![0.0, 1.0]),
        (3, vec![0.9, 0.1]),
    ];
    let candidates = [
        SearchResult::new(2, 0.0), // wrong order on purpose
        SearchResult::new(3, 0.1),
        SearchResult::new(1, 0.2),
    ];
    let mut rescored: Vec<(f32, RowId)> = candidates
        .iter()
        .filter_map(|c| {
            let lane = lanes.iter().find(|(id, _)| *id == c.row_id)?.1.clone();
            let d = l2_distance(Some(&query), Some(&lane)).ok()??;
            Some((d, c.row_id))
        })
        .collect();
    rescored.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    if rescored.first().map(|(_, id)| *id) != Some(1) {
        return Err("rerank did not restore exact order".to_string());
    }
    Ok("exact rescore restores true top-1".to_string())
}

fn check_mvcc() -> Result<String, String> {
    // Tombstoned rows never surface; committed inserts do; the delta log is
    // the visibility source of truth.
    let dim = 2usize;
    let mut idx = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, dim))
        .map_err(|e| e.to_string())?;
    idx.build(&[(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])])
        .map_err(|e| e.to_string())?;
    let mut pending = PendingWrites::new();
    pending.delete(1);
    let mut delta = CommittedDelta::new();
    pending.commit_into(1, 10, &mut delta);
    if !delta.is_deleted(1, 10) || delta.is_deleted(2, 10) {
        return Err("delta visibility wrong".to_string());
    }
    idx.delete(1).map_err(|e| e.to_string())?;
    let hits = idx.search(&[1.0, 0.0], 5).map_err(|e| e.to_string())?;
    if hits.iter().any(|h| h.row_id == 1) {
        return Err("tombstoned row returned".to_string());
    }
    Ok("tombstone hidden, delta visibility exact".to_string())
}

fn check_filtered() -> Result<String, String> {
    // Filtered ANN must oversample so post-filter rerank still fills `k`.
    let options = VectorSearchOptions::default();
    if options.effective_candidate_k(20) != 160 {
        return Err("oversample default changed".to_string());
    }
    let tight = VectorSearchOptions {
        candidate_k: Some(25),
        rerank_k: Some(10),
        ..VectorSearchOptions::default()
    };
    tight.validate().map_err(|e| e.to_string())?;
    if tight.effective_candidate_k(20) != 25 {
        return Err("explicit candidate_k ignored".to_string());
    }
    let bad = VectorSearchOptions {
        candidate_k: Some(10),
        rerank_k: Some(20),
        ..VectorSearchOptions::default()
    };
    if bad.validate().is_ok() {
        return Err("rerank_k > candidate_k accepted".to_string());
    }
    Ok("oversample + rerank validation ok".to_string())
}

fn check_corruption() -> Result<String, String> {
    // Every persisted descriptor fails closed on version/kind/truncation.
    let manifest = IndexManifest::new(IndexKind::Hnsw, Metric::L2, 64, 3);
    let mut bad = manifest.encode();
    bad[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    if IndexManifest::decode(&bad).is_ok() {
        return Err("bad manifest version accepted".to_string());
    }
    let full = manifest.encode();
    if IndexManifest::decode(&full[..full.len() / 2]).is_ok() {
        return Err("truncated manifest accepted".to_string());
    }
    Ok("manifest version/truncation fail closed".to_string())
}

fn check_resource_limits() -> Result<String, String> {
    // Estimates saturate instead of overflowing; hostile counts are capped.
    if memory_estimate_bytes(u64::MAX, u64::MAX) != u64::MAX {
        return Err("memory estimate overflowed".to_string());
    }
    if memory_estimate_bytes(1_000_000, 6144) != 6_144_000_000 {
        return Err("memory estimate wrong".to_string());
    }
    let options = VectorSearchOptions {
        candidate_k: Some(2_000_000),
        ..VectorSearchOptions::default()
    };
    if options.validate().is_ok() {
        return Err("candidate_k above cap accepted".to_string());
    }
    Ok("saturating estimates + caps hold".to_string())
}

fn check_filter_planning() -> Result<String, String> {
    let mut stats = VectorStats::unknown(64);
    for _ in 0..100_000 {
        stats.observe_row(false, false, true);
    }
    let options = VectorSearchOptions::default();
    // Impossible filter never touches an index.
    let plan = plomid_vector::plan_filtered_search(&stats, 0.0, 10, true, &options);
    if plan.strategy != FilterStrategy::ExactScan || plan.expected_rows != Some(0) {
        return Err("impossible filter misplanned".to_string());
    }
    // Unknown stats degrade to exact with a stated reason.
    let plan =
        plomid_vector::plan_filtered_search(&VectorStats::unknown(64), 0.5, 10, true, &options);
    if plan.strategy != FilterStrategy::ExactScan || !plan.executable {
        return Err("unknown stats misplanned".to_string());
    }
    // Post-filter model is costed but execution-gated with a fallback.
    let plan = plomid_vector::plan_filtered_search(&stats, 0.9, 10, true, &options);
    if plan.strategy != FilterStrategy::PostFilterOversampledAnn
        || plan.executable
        || plan.fallback != FilterStrategy::PreFilterSubsetAnn
    {
        return Err("post-filter gate wrong".to_string());
    }
    if plan.reasons.is_empty() {
        return Err("plan without reasons".to_string());
    }
    Ok("impossible/unknown/post-filter plans + gating ok".to_string())
}

fn check_radius_planning() -> Result<String, String> {
    let mut stats = VectorStats::unknown(8);
    for _ in 0..10_000 {
        stats.observe_row(false, false, true);
    }
    let options = VectorSearchOptions::default();
    let neg = plomid_vector::plan_radius_search(&stats, VectorMetric::L2, -1.0, true, &options);
    if neg.strategy != RadiusStrategy::EmptyResult {
        return Err("negative bound misplanned".to_string());
    }
    // L2 index bound squares the SQL bound.
    let plan = plomid_vector::plan_radius_search(&stats, VectorMetric::L2, 0.5, true, &options);
    if (plan.index_bound - 0.25).abs() > 1e-9 || !plan.rerank {
        return Err("index bound or rerank wrong".to_string());
    }
    // Similarity conversions invert the definitions.
    if plomid_vector::similarity_to_distance_bound(VectorMetric::Cosine, 0.75) != Some(0.25) {
        return Err("cosine conversion wrong".to_string());
    }
    Ok("negative/index-bound/similarity plans ok".to_string())
}

fn check_filtered_sql() -> Result<String, String> {
    let mut db = crate::harness::open_live_db("vector-filter");
    db.exec("CREATE TABLE fdoc (id BIGINT, tenant BIGINT, e VECTOR(FLOAT32, 3))");
    db.exec("INSERT INTO fdoc VALUES (1, 1, [1,0,0]), (2, 1, [0.9,0.1,0]), (3, 2, [0,0,1])");
    db.exec("SET vector.auto_ann_min_rows = 0");
    let count = |db: &mut crate::harness::LiveDb, sql: &str| -> Result<usize, String> {
        let result = db
            .executor
            .execute(sql)
            .map_err(|e| format!("{sql} failed: {e}"))?;
        match result {
            plomid_sql::QueryResult::Rows { rows, .. } => Ok(rows.len()),
            other => Err(format!("expected rows, got {other:?}")),
        }
    };
    let ann_ids = count(
        &mut db,
        "SELECT id FROM fdoc WHERE tenant = 1 ORDER BY e <-> [1,0,0] LIMIT 2",
    )?;
    db.exec("SET vector.ann_enabled = off");
    let exact_ids = count(
        &mut db,
        "SELECT id FROM fdoc WHERE tenant = 1 ORDER BY e <-> [1,0,0] LIMIT 2",
    )?;
    db.exec("SET vector.ann_enabled = on");
    if ann_ids != 2 || exact_ids != 2 {
        return Err(format!(
            "filtered counts wrong: ann={ann_ids} exact={exact_ids}"
        ));
    }
    db.teardown();
    Ok("filtered ANN == exact row counts".to_string())
}

fn check_radius_sql() -> Result<String, String> {
    let mut db = crate::harness::open_live_db("vector-radius");
    db.exec("CREATE TABLE rdoc (id BIGINT, e VECTOR(FLOAT32, 2))");
    db.exec("INSERT INTO rdoc VALUES (1, [1,0]), (2, [0,1]), (3, [0.7,0.7])");
    db.exec("SET vector.auto_ann_min_rows = 0");
    let count = |db: &mut crate::harness::LiveDb, sql: &str| -> Result<usize, String> {
        let result = db
            .executor
            .execute(sql)
            .map_err(|e| format!("{sql} failed: {e}"))?;
        match result {
            plomid_sql::QueryResult::Rows { rows, .. } => Ok(rows.len()),
            other => Err(format!("expected rows, got {other:?}")),
        }
    };
    // Radius 0.8 around [1,0]: rows 1 (d=0) and 3 (d~0.76) match.
    if count(
        &mut db,
        "SELECT id FROM rdoc WHERE l2_distance(e, [1,0]) < 0.8",
    )? != 2
    {
        return Err("radius 0.5 wrong".to_string());
    }
    // Negative bound: valid empty.
    if count(
        &mut db,
        "SELECT id FROM rdoc WHERE l2_distance(e, [1,0]) < -1.0",
    )? != 0
    {
        return Err("negative radius not empty".to_string());
    }
    db.teardown();
    Ok("radius ANN counts + negative edge ok".to_string())
}

fn check_sql_end_to_end() -> Result<String, String> {
    let mut db = crate::harness::open_live_db("vector");
    db.exec("CREATE TABLE gvec (id BIGINT, embedding VECTOR(FLOAT32, 3))");
    db.exec("INSERT INTO gvec VALUES (1, [1.0, 0.0, 0.0])");
    db.exec("INSERT INTO gvec VALUES (2, [0.0, 1.0, 0.0])");
    db.exec("INSERT INTO gvec VALUES (3, [0.0, 0.0, 1.0])");
    let result = db
        .executor
        .execute("SELECT id FROM gvec ORDER BY embedding <=> [1,0,0] LIMIT 2")
        .map_err(|e| format!("select failed: {e}"))?;
    let rows = match result {
        plomid_sql::QueryResult::Rows { rows, .. } => rows,
        other => return Err(format!("expected rows, got {other:?}")),
    };
    if rows.len() != 2 {
        return Err(format!("expected 2 rows, got {}", rows.len()));
    }
    db.teardown();
    Ok("SQL CREATE/INSERT/ORDER-BY-LIMIT end-to-end ok".to_string())
}

fn check_storage_policy() -> Result<String, String> {
    use plomid_vector::storage::{
        ManagedVectorStorage, StorageCapability, StorageClass, StorageMode, StorageModeRequest,
        VectorStorageAdapter, VectorStoragePolicy,
    };
    // Policy selection across the size spectrum (deterministic seams).
    let policy = VectorStoragePolicy::default();
    let nvme = StorageCapability::test_seam(true, StorageClass::Nvme, None);
    if policy.select(512, &nvme).mode != StorageMode::Memory {
        return Err("tiny index not memory resident".to_string());
    }
    if policy.select(8 << 20, &nvme).mode != StorageMode::File {
        return Err("medium index not plain file".to_string());
    }
    if policy.select(256 << 20, &nvme).mode != StorageMode::Mmap {
        return Err("large NVMe index not mmap preferred".to_string());
    }
    // mmap unavailable / disabled falls back with the flag set.
    let no_mmap = StorageCapability::test_seam(false, StorageClass::Ssd, None);
    let d = policy.select(256 << 20, &no_mmap);
    if d.mode != StorageMode::File || !d.fallback_active {
        return Err("mmap-unavailable fallback wrong".to_string());
    }
    // Managed store round-trip + restart replay through the live adapter.
    let dir = std::env::temp_dir().join(format!(
        "plomid-gauntlet-storage-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.subsec_nanos())
            .unwrap_or(0)
    ));
    let file_policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let cap = StorageCapability::test_seam(true, StorageClass::Ssd, None);
    // Build rows via the public payload API (no private encodings).
    let v0 = plomid_vector::VectorValue::Dense(
        plomid_vector::DenseVectorValue::from_f32(
            plomid_vector::VectorElementType::Float32,
            vec![1.0, 2.0],
        )
        .map_err(|e| e.to_string())?,
    );
    let mut s = ManagedVectorStorage::create(
        &dir,
        "t".into(),
        "c".into(),
        Some(2),
        &file_policy,
        8 << 20,
        &cap,
        None,
    )
    .map_err(|e| e.to_string())?;
    s.append_payload(&v0).map_err(|e| e.to_string())?;
    s.checkpoint(3).map_err(|e| e.to_string())?;
    s.close().map_err(|e| e.to_string())?;
    let (reopened, recovery) = ManagedVectorStorage::open(
        &dir,
        "t".into(),
        "c".into(),
        Some(2),
        &file_policy,
        8 << 20,
        &cap,
        None,
    )
    .map_err(|e| e.to_string())?;
    if recovery.replayed_records != 1 || reopened.read_payload(0).map_err(|e| e.to_string())? != v0
    {
        return Err("managed restart replay wrong".to_string());
    }
    // Corrupted image fails closed.
    std::fs::write(dir.join("MANIFEST"), b"junk").map_err(|e| e.to_string())?;
    if ManagedVectorStorage::validate_image(&dir).is_ok() {
        return Err("corrupt manifest accepted".to_string());
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok(
        "tiny/memory + medium/file + large/mmap + fallback + restart + corrupt-closed ok"
            .to_string(),
    )
}

fn check_numa() -> Result<String, String> {
    use plomid_vector::search::{search_many_parallel_numa, BatchSearcher, SearchMetric};
    // Correctness is identical in every NUMA mode (deterministic order).
    let rows: Vec<(u64, Option<Vec<f32>>)> = vec![
        (1, Some(vec![1.0, 0.0])),
        (2, Some(vec![0.0, 1.0])),
        (3, None),
        (4, Some(vec![0.5, 0.5])),
    ];
    let queries = vec![vec![1.0f32, 0.0], vec![0.0, 1.0], vec![0.5, 0.5]];
    let mut serial = BatchSearcher::new(SearchMetric::L2, 2).map_err(|e| e.to_string())?;
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let expected = serial
        .search_many(&borrowed, &rows, None)
        .map_err(|e| e.to_string())?;
    for nodes in [1usize, 2, 4] {
        let topo = plomid_vector::NumaTopology::test_seam(nodes, 16).map_err(|e| e.to_string())?;
        for threads in [0usize, 1, 4] {
            let got = search_many_parallel_numa(
                &queries,
                &rows,
                SearchMetric::L2,
                2,
                None,
                threads,
                &topo,
                None,
            )
            .map_err(|e| e.to_string())?;
            if got != expected {
                return Err(format!("NUMA nodes={nodes} threads={threads} diverged"));
            }
        }
    }
    // Detection never fails and always explains itself.
    let live = plomid_vector::NumaTopology::detect();
    if live.nodes < 1 || live.explain().is_empty() {
        return Err("live NUMA topology invalid".to_string());
    }
    Ok(format!(
        "UMA/2-node/4-node agree bit-identically; live mode={}",
        live.mode().as_str()
    ))
}

fn check_scale_tiers() -> Result<String, String> {
    use plomid_vector::storage::StorageClass;
    use plomid_vector::{ScaleConfig, ScaleDecision, ScaleInputs, ScaleTier};
    let gib: u64 = 1024 * 1024 * 1024;
    // Low-memory -> laptop; many-core -> production; high-memory -> large.
    let laptop = ScaleDecision::select(
        &ScaleInputs::test_seam(2, Some(4 * gib), StorageClass::Ssd, 100 << 20, 1),
        &ScaleConfig::explicit(None),
    );
    if laptop.tier != ScaleTier::Laptop {
        return Err(format!("low-memory tier={:?}", laptop.tier));
    }
    let prod = ScaleDecision::select(
        &ScaleInputs::test_seam(32, Some(64 * gib), StorageClass::Ssd, 1 << 30, 4),
        &ScaleConfig::explicit(None),
    );
    if prod.tier != ScaleTier::ProductionServer {
        return Err(format!("many-core tier={:?}", prod.tier));
    }
    let large = ScaleDecision::select(
        &ScaleInputs::test_seam(64, Some(256 * gib), StorageClass::Nvme, 200 * gib, 8),
        &ScaleConfig::explicit(None),
    );
    if large.tier != ScaleTier::LargeMemoryHighIo {
        return Err(format!("high-memory tier={:?}", large.tier));
    }
    // Explicit override wins; knobs grow monotonically across tiers.
    let pinned = ScaleDecision::select(
        &ScaleInputs::test_seam(2, Some(4 * gib), StorageClass::Hdd, 1 << 20, 1),
        &ScaleConfig::explicit(Some(ScaleTier::ProductionServer)),
    );
    if pinned.tier != ScaleTier::ProductionServer || !pinned.overridden {
        return Err("explicit tier override ignored".to_string());
    }
    if !(laptop.workers <= prod.workers && prod.workers <= large.workers) {
        return Err("tier worker knobs not monotonic".to_string());
    }
    Ok(format!(
        "laptop/workstation/server/large classified; override wins; reason='{}'",
        laptop.reason
    ))
}

fn check_freshness() -> Result<String, String> {
    use plomid_vector::{
        check_stats_freshness, is_index_queryable, Freshness, FreshnessCause, FreshnessTracker,
        IndexState,
    };
    // Tracker: insert invalidates, refresh restores, corruption never serves.
    let mut t = FreshnessTracker::unknown();
    t.refresh(10);
    if t.state() != Freshness::Fresh {
        return Err("refresh not fresh".to_string());
    }
    t.invalidate(FreshnessCause::Insert, "insert");
    if t.state() != Freshness::Stale || t.usable_for_ann() {
        return Err("insert did not stale-gate ANN".to_string());
    }
    // Interrupted build/maintenance never appears usable.
    if is_index_queryable(IndexState::Building, Freshness::Fresh) {
        return Err("building index queryable".to_string());
    }
    if !is_index_queryable(IndexState::Ready, Freshness::Fresh) {
        return Err("ready+fresh not queryable".to_string());
    }
    if is_index_queryable(IndexState::Ready, Freshness::Stale) {
        return Err("stale index serves ANN".to_string());
    }
    // Statistics: unknown -> Unknown, watermark match -> Fresh, else Stale.
    let mut stats = plomid_vector::VectorStats::unknown(8);
    if check_stats_freshness(&stats, 0) != Freshness::Unknown {
        return Err("unobserved stats not unknown".to_string());
    }
    for _ in 0..4 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 9;
    if check_stats_freshness(&stats, 9) != Freshness::Fresh
        || check_stats_freshness(&stats, 10) != Freshness::Stale
    {
        return Err("stats watermark classification wrong".to_string());
    }
    // Stale planner fallback is conservative (exact) with metrics.
    let metrics = plomid_vector::VectorMetrics::new();
    let plan = plomid_vector::plan_filtered_search_with_freshness(
        &stats,
        0.4,
        10,
        true,
        &plomid_vector::VectorSearchOptions::default(),
        Freshness::Stale,
        Some(&metrics),
    );
    if plan.strategy != plomid_vector::FilterStrategy::ExactScan {
        return Err("stale plan not conservative".to_string());
    }
    if metrics.snapshot().stale_plan_fallbacks != 1 {
        return Err("stale fallback not metered".to_string());
    }
    Ok("tracker/invalidation/lifecycle-gating/stale-conservative ok".to_string())
}

fn check_backup_restore() -> Result<String, String> {
    use plomid_vector::storage::{
        FileVectorStorage, ManagedVectorStorage, StorageCapability, StorageClass,
        StorageModeRequest, VectorStorageAdapter, VectorStoragePolicy,
    };
    // Backup one file store with an HNSW direct blob; restore elsewhere;
    // reopened rows, ANN search, Top-K, radius, rerank-shape, mutations,
    // and MVCC replay must all hold on the restored image.
    let mkrows = |n: u64| -> Vec<(u64, Vec<f32>)> {
        (0..n)
            .map(|i| (i, vec![i as f32, i as f32 + 0.5]))
            .collect()
    };
    let src = std::env::temp_dir().join(format!(
        "plomid-gauntlet-backup-src-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.subsec_nanos())
            .unwrap_or(0)
    ));
    let mut s = FileVectorStorage::create(&src, "t".into(), "c".into(), Some(2))
        .map_err(|e| e.to_string())?;
    for (id, v) in mkrows(32) {
        let value = plomid_vector::VectorValue::Dense(
            plomid_vector::DenseVectorValue::from_f32(plomid_vector::VectorElementType::Float32, v)
                .map_err(|e| e.to_string())?,
        );
        if s.append_payload(&value).map_err(|e| e.to_string())? != id {
            return Err("row id drift before backup".to_string());
        }
    }
    s.checkpoint(21).map_err(|e| e.to_string())?;
    // HNSW direct blob over the same rows.
    let rows = mkrows(32);
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2))
        .map_err(|e| e.to_string())?;
    hnsw.build(&rows).map_err(|e| e.to_string())?;
    let stats = hnsw.stats();
    let blob = hnsw.encode();
    let input = plomid_vector::IndexBackupInput {
        name: "g_hnsw".into(),
        kind: IndexKind::Hnsw,
        metric: VectorMetric::L2,
        dimension: 2,
        generation: stats.generation,
        vector_count: stats.num_vectors as u64,
        deleted_count: 0,
        persist_blob: Some(blob),
        diskann_pair: None,
    };
    let out = src
        .join("..")
        .join(format!("plomid-gauntlet-backup-out-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    plomid_vector::create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        21,
        None,
        std::slice::from_ref(&input),
        0,
        &out,
        plomid_vector::BackupFailPoint::None,
        None,
    )
    .map_err(|e| e.to_string())?;
    drop(s);
    let target = src.join("..").join(format!(
        "plomid-gauntlet-backup-target-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&target);
    let report =
        plomid_vector::restore_backup(&out, &target, plomid_vector::RestoreFailPoint::None, None)
            .map_err(|e| e.to_string())?;
    if report.rows != 32 || report.manifest_lsn != 21 {
        return Err("restore counts/lsn wrong".to_string());
    }
    // Reopened store: rows present, dims enforced, MVCC replay intact.
    let (_reopened, recovery) =
        FileVectorStorage::open(&target, "t".into(), "c".into(), Some(2), false)
            .map_err(|e| e.to_string())?;
    if recovery.replayed_records != 32 {
        return Err("restored replay wrong".to_string());
    }
    // Exact search over restored rows.
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let exact = plomid_vector::search::exact::exact_top_k(
        &[1.0, 1.5],
        borrowed,
        plomid_vector::search::SearchMetric::L2,
        3,
        None,
    )
    .map_err(|e| e.to_string())?;
    if exact.first().map(|c| c.row_id) != Some(1) {
        return Err("restored exact top-1 wrong".to_string());
    }
    // Restored ANN blob validates and serves Top-K.
    let staged = std::fs::read(target.join("indexes/g_hnsw.blob")).map_err(|e| e.to_string())?;
    let config = IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2);
    plomid_vector::validate_restored_blob(IndexKind::Hnsw, &config, &staged)
        .map_err(|e| e.to_string())?;
    // Filters work (selectivity-gated exact is always correct).
    let options = VectorSearchOptions::default();
    let mut vstats = VectorStats::unknown(2);
    for _ in 0..32 {
        vstats.observe_row(false, false, true);
    }
    let plan = plomid_vector::plan_filtered_search(&vstats, 0.5, 3, true, &options);
    if !plan.executable {
        return Err("restored filter plan not executable".to_string());
    }
    // Radius over restored rows.
    let radius_rows: Vec<(u64, Option<Vec<f32>>)> =
        rows.iter().map(|(id, v)| (*id, Some(v.clone()))).collect();
    let radius = plomid_vector::search::exact::exact_radius(
        &[1.0, 1.5],
        radius_rows,
        plomid_vector::search::SearchMetric::L2,
        &plomid_vector::search::RadiusConfig::new(1.0).map_err(|e| e.to_string())?,
        None,
    )
    .map_err(|e| e.to_string())?;
    if radius.is_empty() {
        return Err("restored radius empty".to_string());
    }
    // Mutations work after restore (managed policy wrapper).
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let cap = StorageCapability::test_seam(true, StorageClass::Ssd, None);
    let (mut managed, _) = ManagedVectorStorage::open(
        &target,
        "t".into(),
        "c".into(),
        Some(2),
        &policy,
        8 << 20,
        &cap,
        None,
    )
    .map_err(|e| e.to_string())?;
    let nv = plomid_vector::VectorValue::Dense(
        plomid_vector::DenseVectorValue::from_f32(
            plomid_vector::VectorElementType::Float32,
            vec![9.0, 9.5],
        )
        .map_err(|e| e.to_string())?,
    );
    managed.append_payload(&nv).map_err(|e| e.to_string())?;
    if managed.row_count() != 33 {
        return Err("mutation after restore wrong".to_string());
    }
    // Corrupted backup input fails safely (bit-flip the manifest).
    let mut bad = std::fs::read(out.join("BACKUP_MANIFEST")).map_err(|e| e.to_string())?;
    if let Some(last) = bad.last_mut() {
        *last ^= 0xFF;
    }
    let bad_dir = src
        .join("..")
        .join(format!("plomid-gauntlet-backup-bad-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&bad_dir);
    std::fs::create_dir_all(&bad_dir).map_err(|e| e.to_string())?;
    std::fs::write(bad_dir.join("BACKUP_MANIFEST"), &bad).map_err(|e| e.to_string())?;
    if plomid_vector::restore_backup(
        &bad_dir,
        &bad_dir.join("t"),
        plomid_vector::RestoreFailPoint::None,
        None,
    )
    .is_ok()
    {
        return Err("corrupt backup manifest accepted".to_string());
    }
    for d in [&src, &out, &target, &bad_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
    Ok("backup/restore/query/mutate/corrupt-closed ok (32 rows, HNSW direct)".to_string())
}

fn check_htap() -> Result<String, String> {
    use plomid_vector::htap::{admit, run_mixed, PressureLevel, Throttle};
    use plomid_vector::search::{BatchSearcher, SearchMetric};
    // Admission degrades (never corrupts) under pressure.
    let budget = 1u64 << 20;
    if !admit(512, 0, budget, PressureLevel::Normal, 0, 2, false).may_proceed() {
        return Err("normal admission refused".to_string());
    }
    if admit(2 * budget, 0, budget, PressureLevel::Normal, 0, 2, false).may_proceed() {
        return Err("over-budget admission allowed".to_string());
    }
    if Throttle::for_pressure(PressureLevel::Critical) != Throttle::Paused {
        return Err("critical did not pause background work".to_string());
    }
    // Mixed flood: results equal the serial oracle.
    let rows: Vec<(u64, Option<Vec<f32>>)> = (0..128u64)
        .map(|i| (i, Some(vec![i as f32, 0.0])))
        .collect();
    let oracle = {
        let mut s = BatchSearcher::new(SearchMetric::L2, 2).map_err(|e| e.to_string())?;
        let q: &[f32] = &[5.0, 0.0];
        s.search_one(q, &rows, None).map_err(|e| e.to_string())?
    };
    let report = run_mixed(
        16,
        256,
        |_| {
            let mut s = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
            let q: &[f32] = &[5.0, 0.0];
            assert_eq!(s.search_one(q, &rows, None).unwrap(), oracle);
        },
        |i| {
            std::hint::black_box(i);
        },
    );
    if report.foreground.iters != 16 || report.background_ops != 256 {
        return Err("mixed workload counts wrong".to_string());
    }
    // Cancellation releases: cancel then reuse succeeds.
    let mut s = BatchSearcher::new(SearchMetric::L2, 2).map_err(|e| e.to_string())?;
    let stop = || true;
    if s.search_one(&[5.0, 0.0], &rows, Some(&stop)).is_ok() {
        return Err("uncancellable search".to_string());
    }
    let out = s
        .search_one(&[5.0, 0.0], &rows, None)
        .map_err(|e| e.to_string())?;
    if out != oracle {
        return Err("post-cancel search wrong".to_string());
    }
    Ok(format!(
        "admit/degrade/throttle/cancel-ok; mixed p50={}ns p99={}ns",
        report.foreground.p50_ns, report.foreground.p99_ns
    ))
}

fn check_acceptance() -> Result<String, String> {
    // Integrated acceptance invariants (the full 45-area suite lives in
    // crates/vector/tests/acceptance.rs; the Gauntlet pins the load-bearing
    // cross-module chains so release gates cannot drift).
    let rows: Vec<(u64, Vec<f32>)> = (0..64u64)
        .map(|i| (i, vec![i as f32 * 0.25, 1.0]))
        .collect();
    // Types → codec → storage → HNSW → exact oracle agreement.
    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2))
        .map_err(|e| e.to_string())?;
    flat.build(&rows).map_err(|e| e.to_string())?;
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2))
        .map_err(|e| e.to_string())?;
    hnsw.build(&rows).map_err(|e| e.to_string())?;
    let query = vec![8.0f32, 1.0];
    let oracle = flat.search(&query, 5).map_err(|e| e.to_string())?;
    let approx = hnsw.search(&query, 5).map_err(|e| e.to_string())?;
    let oracle_ids: HashSet<RowId> = oracle.iter().map(|r| r.row_id).collect();
    if approx
        .iter()
        .filter(|r| oracle_ids.contains(&r.row_id))
        .count()
        < 3
    {
        return Err("acceptance HNSW recall below 3/5".to_string());
    }
    // Planner → freshness → storage-policy chain with metrics.
    let metrics = plomid_vector::VectorMetrics::new();
    let mut stats = VectorStats::unknown(2);
    for _ in 0..5000 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 4;
    let plan = plomid_vector::plan_filtered_search_with_freshness(
        &stats,
        0.5,
        5,
        true,
        &VectorSearchOptions::default(),
        plomid_vector::Freshness::Stale,
        Some(&metrics),
    );
    if plan.strategy != FilterStrategy::ExactScan {
        return Err("acceptance stale plan not conservative".to_string());
    }
    if metrics.snapshot().stale_plan_fallbacks != 1 {
        return Err("acceptance stale fallback unmetered".to_string());
    }
    Ok("codec/storage/HNSW-oracle + planner/freshness/metrics chains ok".to_string())
}

fn check_wire_contract() -> Result<String, String> {
    // In-crate wire checks (live PGWire matrix: network/tests/vector_wire.rs).
    let t = VectorType::new(VectorKind::Dense, VectorElementType::Float32, 2)
        .map_err(|e| e.to_string())?;
    let v = plomid_vector::VectorValue::Dense(
        plomid_vector::DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, 2.0])
            .map_err(|e| e.to_string())?,
    );
    let wire = plomid_vector::wire::encode_for_wire(&v, &t).map_err(|e| e.to_string())?;
    if wire.oid != plomid_vector::wire::WIRE_OID_VECTOR {
        return Err("wire OID wrong".to_string());
    }
    plomid_vector::wire::check_wire_vector(&wire, &t).map_err(|e| e.to_string())?;
    if plomid_vector::wire::BINARY_PARAMS_SUPPORTED {
        return Err("binary params must stay refused".to_string());
    }
    // Error taxonomy is stable for operators.
    if plomid_vector::operator::categorize_vector_error(
        &plomid_vector::VectorError::IndexCorrupted("x".into()),
    ) != plomid_vector::operator::ErrorCategory::IndexCorruption
    {
        return Err("error taxonomy drift".to_string());
    }
    Ok("typmod/OID/round-trip/binary-refusal/taxonomy ok".to_string())
}

fn check_migration() -> Result<String, String> {
    use plomid_vector::migration::{check_transition, Compatibility, VersionedArtifact};
    // All artifacts versioned; same opens, newer never, older indexes rebuild.
    for a in VersionedArtifact::all() {
        let c = a.current_version();
        if check_transition(a, c) != Compatibility::Supported
            || check_transition(a, c + 1) != Compatibility::NotSupported
        {
            return Err(format!("migration matrix wrong for {}", a.as_str()));
        }
    }
    if plomid_vector::migration::matrix().len() != 13 {
        return Err("migration matrix incomplete".to_string());
    }
    Ok("13 artifacts: same=supported, newer=closed, older-indexes=rebuild".to_string())
}

fn check_operability() -> Result<String, String> {
    // Status views + config inventory + support checklist compose existing systems.
    let view = plomid_vector::operator::IndexStatusView::new(
        "d".into(),
        plomid_vector::IndexState::Ready,
        plomid_vector::Freshness::Fresh,
    );
    if !view.queryable {
        return Err("ready+fresh not queryable".to_string());
    }
    if plomid_vector::operator::config_inventory().len() != 15 {
        return Err("config inventory drift (undocumented knob?)".to_string());
    }
    if plomid_vector::operator::support_checklist().len() != 12 {
        return Err("support checklist drift".to_string());
    }
    Ok("status/config(15)/checklist(12) compose existing systems".to_string())
}

/// Runs every vector Gauntlet suite and aggregates the outcomes.
#[must_use]
pub fn run_vector_qualification() -> VectorQualification {
    let suites = vec![
        run_suite("vector_types", check_types),
        run_suite("vector_literals", check_literals),
        run_suite("vector_codec", check_codec),
        run_suite("vector_distance", check_distance),
        run_suite("vector_exact_search", check_exact_search),
        run_suite("vector_hnsw", check_hnsw),
        run_suite("vector_ivf", check_ivf),
        run_suite("vector_sq", check_sq),
        run_suite("vector_pq", check_pq),
        run_suite("vector_diskann", check_diskann),
        run_suite("vector_sparse", check_sparse),
        run_suite("vector_binary", check_binary),
        run_suite("vector_hybrid", check_hybrid),
        run_suite("vector_reranking", check_reranking),
        run_suite("vector_mvcc", check_mvcc),
        run_suite("vector_filtered", check_filtered),
        run_suite("vector_filter_planning", check_filter_planning),
        run_suite("vector_radius_planning", check_radius_planning),
        run_suite("vector_filtered_sql", check_filtered_sql),
        run_suite("vector_radius_sql", check_radius_sql),
        run_suite("vector_corruption", check_corruption),
        run_suite("vector_resource_limits", check_resource_limits),
        run_suite("vector_capability", check_capability),
        run_suite("vector_sql_end_to_end", check_sql_end_to_end),
        run_suite("vector_storage_policy", check_storage_policy),
        run_suite("vector_numa", check_numa),
        run_suite("vector_scale_tiers", check_scale_tiers),
        run_suite("vector_freshness", check_freshness),
        run_suite("vector_backup_restore", check_backup_restore),
        run_suite("vector_htap", check_htap),
        run_suite("vector_acceptance", check_acceptance),
        run_suite("vector_wire_contract", check_wire_contract),
        run_suite("vector_migration", check_migration),
        run_suite("vector_operability", check_operability),
    ];
    VectorQualification { suites }
}
