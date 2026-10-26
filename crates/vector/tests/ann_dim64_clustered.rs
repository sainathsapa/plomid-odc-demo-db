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
//! F-008 regression: clustered dim-64 ANN quality with a declared floor.
//!
//! The 2026-10-16 audit measured recall@10 = 0.00 (2 k rows) / 0.20 (8 k)
//! for DiskANN on the lifecycle bench's clustered dim-64 fixture, with the
//! returned ids all from one (wrong) cluster while the exact top-10 lived in
//! another — recall gates built on uniform/random fixtures passed anyway.
//! This file pins the bench fixture itself as a regression tripwire.
//!
//! ## Fixture provenance
//!
//! Same generator family and parameters as `tests/python/vector_lifecycle_bench.py`
//! (the audit evidence generator), reimplemented here independently of index
//! code with the same LCG
//! (`state = state * 6364136223846793005 + 1442695040888963407 mod 2^64`,
//! `next = (state >> 33) / (2^32 - 1) * 2 - 1` in `[-1, 1)`):
//!
//! - 8 centers, each lane uniform in `[-1, 1)` rounded to 4 decimals;
//! - row `i` = center `i mod 8` + per-lane `next() * 0.05` rounded to 6
//!   decimals; row id = `i`;
//! - one probe vector from seed `0xBEEF`, lanes rounded to 6 decimals.
//!
//! ## Recall methodology (declared)
//!
//! The executor issues ANN with candidate breadth
//! `max(k * candidate_oversample, k + 64)` = 80 for `k = 10` at default
//! settings, then reranks the returned candidates with the exact distance
//! from authoritative rows and keeps the top-10. This file mirrors that
//! path in-process: `search(probe, CANDIDATE_K)` + exact-L2 rerank +
//! truncate to `K`. `recall@10 = |ANN_10 ∩ ORACLE_10| / 10` where
//! `ORACLE_10` is [`FlatIndex`] over the same rows (set membership).
//!
//! Ground truth is verified, not assumed: every oracle id must belong to
//! the probe's nearest cluster (`id % 8`), so the fixture cannot silently
//! change which region is nearest without failing. The wrong-region
//! signature is also asserted directly: the nearest ANN candidate may not
//! be farther than the farthest oracle candidate.
//!
//! ## Floors
//!
//! Calibrated with margin against the printed values (debug build,
//! post-fix); the printed line is the evidence, the assertion is the
//! tripwire. Raising a floor follows a measured improvement:
//!
//! - 2 k rows: DiskANN 1.00, HNSW 1.00 → floor 0.90 (both).
//! - 8 k rows (ignored scale tier): DiskANN 0.90, HNSW 0.90 → floor 0.80.
//!
//! The 8 k case is `#[ignore]`d because the debug-profile DiskANN build is
//! ~1–2 minutes; `make test-scale` / `cargo test -- --include-ignored`
//! runs it nightly.

use plomid_vector::index::diskann::DiskAnnIndex;
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::{
    FlatIndex, IndexConfig, IndexKind, Metric, RowId, SearchResult, VectorIndex,
};

const DIM: usize = 64;
const K: usize = 10;
/// Executor candidate breadth at defaults (`k = 10`, oversample 8):
/// `max(k * 8, k + 64)` = 80.
const CANDIDATE_K: usize = 80;

/// The bench's LCG in `[-1, 1)`; f64 arithmetic like the Python generator.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed & 0xFFFF_FFFF_FFFF_FFFF)
    }

    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as f64 / 4_294_967_295.0) * 2.0 - 1.0
    }
}

fn round_to(x: f64, decimals: i32) -> f64 {
    let scale = 10f64.powi(decimals);
    (x * scale).round() / scale
}

/// The bench's clustered fixture at `n` rows; lanes land in f32 like the
/// authoritative FLOAT32 column does after SQL text round-trips.
fn clustered(n: usize) -> Vec<(RowId, Vec<f32>)> {
    let mut rng = Lcg::new(0x51D ^ n as u64);
    let centers: Vec<Vec<f64>> = (0..8)
        .map(|_| (0..DIM).map(|_| round_to(rng.next(), 4)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let center = &centers[i % 8];
            let lanes: Vec<f32> = center
                .iter()
                .map(|lane| round_to(lane + rng.next() * 0.05, 6) as f32)
                .collect();
            (i as RowId, lanes)
        })
        .collect()
}

fn probe_vector() -> Vec<f32> {
    let mut rng = Lcg::new(0xBEEF);
    (0..DIM).map(|_| round_to(rng.next(), 6) as f32).collect()
}

/// The executor's exact-L2 rerank kernel (authoritative lanes, same
/// distance function the ANN path uses to re-score candidates).
fn exact_l2(a: &[f32], b: &[f32]) -> f32 {
    plomid_vector::distance::l2_distance(Some(a), Some(b))
        .expect("same-dimension lanes")
        .expect("defined distance")
}

/// ANN candidates → exact rerank → top-`K` ids with their exact distances.
fn rerank(hits: &[SearchResult], rows: &[(RowId, Vec<f32>)], probe: &[f32]) -> Vec<(RowId, f32)> {
    let mut scored: Vec<(RowId, f32)> = hits
        .iter()
        .map(|hit| (hit.row_id, exact_l2(probe, &rows[hit.row_id as usize].1)))
        .collect();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    scored.truncate(K);
    scored
}

fn recall(ann: &[(RowId, f32)], oracle: &[RowId]) -> f64 {
    let oracle: std::collections::HashSet<RowId> = oracle.iter().copied().collect();
    ann.iter().filter(|(id, _)| oracle.contains(id)).count() as f64 / oracle.len() as f64
}

fn run_case(n: usize, diskann_floor: f64, hnsw_floor: f64) {
    let rows = clustered(n);
    let probe = probe_vector();

    // Ground truth: exact top-10, all in the probe's nearest cluster.
    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, DIM)).unwrap();
    flat.build(&rows).unwrap();
    let oracle_hits = flat.search(&probe, K).unwrap();
    let oracle_ids: Vec<RowId> = oracle_hits.iter().map(|hit| hit.row_id).collect();
    assert_eq!(oracle_ids.len(), K, "oracle must return K ids");
    let oracle_cluster = oracle_ids[0] % 8;
    for id in &oracle_ids {
        assert_eq!(
            id % 8,
            oracle_cluster,
            "oracle id {id} left the probe's nearest cluster {oracle_cluster}"
        );
    }
    // Oracle ordering holds under the exact kernel, and its first id is the
    // true nearest over every row (not just its own cluster).
    let oracle_distances: Vec<f32> = oracle_ids
        .iter()
        .map(|id| exact_l2(&probe, &rows[*id as usize].1))
        .collect();
    for window in oracle_distances.windows(2) {
        assert!(
            window[0] <= window[1],
            "oracle order regressed under the exact kernel: {oracle_distances:?}"
        );
    }
    let (_, true_nearest) = rows
        .iter()
        .map(|(id, lanes)| (exact_l2(&probe, lanes), *id))
        .fold(
            (f32::INFINITY, RowId::MAX),
            |(best, best_id), (distance, id)| {
                if distance < best || (distance == best && id < best_id) {
                    (distance, id)
                } else {
                    (best, best_id)
                }
            },
        );
    assert_eq!(
        oracle_ids[0], true_nearest,
        "FlatIndex must return the true nearest neighbour first"
    );
    let oracle_farthest = oracle_distances
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);

    // HNSW control (never the F-008 worst case, but same fixture/tripwire).
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, DIM)).unwrap();
    hnsw.build(&rows).unwrap();
    let hnsw_ids = rerank(&hnsw.search(&probe, CANDIDATE_K).unwrap(), &rows, &probe);
    let hnsw_recall = recall(&hnsw_ids, &oracle_ids);

    // DiskANN (the audit's worst case) through the executor-style path.
    let dir = std::env::temp_dir().join(format!(
        "plomid-ann-dim64-clustered-{n}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut diskann =
        DiskAnnIndex::new(IndexConfig::new(IndexKind::DiskAnn, Metric::L2, DIM), &dir).unwrap();
    diskann.build(&rows).unwrap();
    let diskann_ids = rerank(&diskann.search(&probe, CANDIDATE_K).unwrap(), &rows, &probe);
    let diskann_recall = recall(&diskann_ids, &oracle_ids);
    let _ = std::fs::remove_dir_all(&dir);

    eprintln!(
        "ann-f008 clustered dim64 n={n} k={K} candidate_k={CANDIDATE_K}: \
         diskann recall={diskann_recall:.2} hnsw recall={hnsw_recall:.2} \
         oracle_cluster={oracle_cluster}"
    );

    // Wrong-region guard: the audit failure returned only ids farther than
    // every oracle id (ann best d=2.3151 vs exact worst 1.9935).
    let diskann_nearest = diskann_ids
        .iter()
        .map(|(_, distance)| *distance)
        .fold(f32::INFINITY, f32::min);
    assert!(
        diskann_nearest <= oracle_farthest,
        "n={n}: DiskANN's best candidate ({diskann_nearest}) is farther than the oracle's \
         worst ({oracle_farthest}) — the search never reached the probe's region"
    );
    assert!(
        diskann_recall >= diskann_floor,
        "n={n}: DiskANN recall@{K} {diskann_recall:.2} below floor {diskann_floor:.2}"
    );
    assert!(
        hnsw_recall >= hnsw_floor,
        "n={n}: HNSW recall@{K} {hnsw_recall:.2} below floor {hnsw_floor:.2}"
    );
}

/// CI tier: 2 k rows, floors calibrated post-fix (measured 1.00/1.00).
#[test]
fn clustered_dim64_recall_2k() {
    run_case(2_000, 0.90, 0.90);
}

/// Scale tier: 8 k rows (measured 0.90/0.90; debug build ~1–2 minutes).
/// Run nightly via `make test-scale` (`--include-ignored`).
#[test]
#[ignore = "scale tier: 8k DiskANN build is ~1-2 min in debug; nightly runs it via --include-ignored"]
fn clustered_dim64_recall_8k() {
    run_case(8_000, 0.80, 0.80);
}
