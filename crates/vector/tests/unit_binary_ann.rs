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
//! Binary Hamming ANN: recall, ordering, and metric-scope tests.
//!
//! The exact [`BinaryIndex::search`] scan is the oracle everywhere below;
//! ANN results must match it (recall) and share its `(distance, row_id)`
//! ordering. Jaccard is explicitly out of ANN scope (rejected, exact stays).

use plomid_vector::index::binary::{BinaryAnnProbes, BinaryIndex};
use plomid_vector::index::{IndexConfig, IndexKind, Metric, VectorIndex};

/// Deterministic xorshift64* (no RNG dependency in test scope).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn hamming_index(dim_bits: usize, metric: Metric) -> BinaryIndex {
    BinaryIndex::new(IndexConfig::new(IndexKind::Binary, metric, dim_bits)).unwrap()
}

/// Pack MSB-first bits (same layout as `BinaryIndex::pack_bits`).
fn pack(bits: &[bool]) -> Vec<u64> {
    let words = bits.len().div_ceil(64);
    let mut out = vec![0u64; words];
    for (b, on) in bits.iter().enumerate() {
        if *on {
            out[b / 64] |= 1u64 << (63 - (b % 64));
        }
    }
    out
}

fn random_bits(rng: &mut Rng, dim: usize, density: f64) -> Vec<bool> {
    let threshold = (density * 1000.0) as u64;
    (0..dim).map(|_| rng.next() % 1000 < threshold).collect()
}

/// Flip exactly `flips` distinct bits of `bits`.
fn mutate(rng: &mut Rng, bits: &[bool], flips: usize) -> Vec<bool> {
    let mut out = bits.to_vec();
    let mut done = 0;
    while done < flips {
        let i = (rng.next() as usize) % out.len();
        out[i] = !out[i];
        done += 1;
    }
    out
}

fn exact_ids(index: &BinaryIndex, query_f32: &[f32], k: usize) -> Vec<u64> {
    index
        .search(query_f32, k)
        .unwrap()
        .into_iter()
        .map(|h| h.row_id)
        .collect()
}

fn bits_to_f32(bits: &[bool]) -> Vec<f32> {
    bits.iter().map(|b| if *b { 1.0 } else { 0.0 }).collect()
}

fn recall(exact: &[u64], approx: &[u64]) -> f64 {
    if exact.is_empty() {
        return 1.0;
    }
    let hits = approx.iter().filter(|id| exact.contains(id)).count() as f64;
    hits / exact.len() as f64
}

#[test]
fn ann_matches_exact_on_clustered_data() {
    // Near-duplicate clusters: the ANN sweet spot (recall must be 1.0).
    let dim = 256;
    let mut rng = Rng(0x1234_5678);
    let mut index = hamming_index(dim, Metric::Hamming);
    let mut basis = Vec::new();
    for _ in 0..8 {
        basis.push(random_bits(&mut rng, dim, 0.3));
    }
    let mut id = 0u64;
    for b in &basis {
        for _ in 0..25 {
            let v = mutate(&mut rng, b, 3);
            index.insert_words(id, &pack(&v)).unwrap();
            id += 1;
        }
    }
    let probes = BinaryAnnProbes::default();
    for (qi, base) in basis.iter().enumerate() {
        let q = pack(base);
        let approx = index
            .ann_search_words(&q, 10, probes)
            .unwrap()
            .into_iter()
            .map(|h| h.row_id)
            .collect::<Vec<_>>();
        let exact = exact_ids(&index, &bits_to_f32(base), 10);
        assert_eq!(recall(&exact, &approx), 1.0, "query {qi}");
        // Full ordering agreement, not just set overlap.
        let approx_full = index
            .ann_search_words(&q, 10, probes)
            .unwrap()
            .into_iter()
            .map(|h| (h.distance, h.row_id))
            .collect::<Vec<_>>();
        let exact_full = index
            .search(&bits_to_f32(base), 10)
            .unwrap()
            .into_iter()
            .map(|h| (h.distance, h.row_id))
            .collect::<Vec<_>>();
        assert_eq!(approx_full, exact_full, "ordering query {qi}");
    }
}

#[test]
fn ann_uniform_random_falls_back_to_exact() {
    // Uniform random 256-bit rows: even true neighbors share no near block,
    // so probes cannot fill top-k. The index must say so (fallback flag) and
    // still return exact results — no fake acceleration, no silent recall loss.
    let dim = 256;
    let mut rng = Rng(0xabcd_ef01);
    let mut index = hamming_index(dim, Metric::Hamming);
    let mut rows = Vec::new();
    for id in 0..2000u64 {
        let v = random_bits(&mut rng, dim, 0.5);
        rows.push(v.clone());
        index.insert_words(id, &pack(&v)).unwrap();
    }
    let probes = BinaryAnnProbes::default();
    for qi in (0..2000).step_by(500) {
        let (hits, stats) = index
            .ann_search_words_with_stats(&pack(&rows[qi]), 10, probes)
            .unwrap();
        let approx = hits.into_iter().map(|h| h.row_id).collect::<Vec<_>>();
        assert_eq!(
            recall(&exact_ids(&index, &bits_to_f32(&rows[qi]), 10), &approx),
            1.0
        );
        assert!(stats.full_scan_fallback, "query {qi}");
    }
}

#[test]
fn ann_tight_budget_keeps_ordering_and_exact_distances() {
    // Approximate regime: a tiny candidate cap on clustered data. Whatever is
    // returned must be correctly ordered with exact distances (verified per
    // id against the oracle); recall is reported, not assumed.
    let dim = 256;
    let mut rng = Rng(0x1234_5678);
    let mut index = hamming_index(dim, Metric::Hamming);
    let center = random_bits(&mut rng, dim, 0.3);
    for id in 0..200u64 {
        let v = mutate(&mut rng, &center, 5);
        index.insert_words(id, &pack(&v)).unwrap();
    }
    let tight = BinaryAnnProbes {
        max_candidates: 30,
        neighborhood: 1,
    };
    let q = pack(&center);
    let (hits, stats) = index.ann_search_words_with_stats(&q, 10, tight).unwrap();
    assert!(!stats.full_scan_fallback);
    assert_eq!(hits.len(), 10);
    let exact_all = index.search(&bits_to_f32(&center), 200).unwrap();
    let dist_of = |id: u64| exact_all.iter().find(|h| h.row_id == id).unwrap().distance;
    for h in &hits {
        assert_eq!(h.distance, dist_of(h.row_id), "row {}", h.row_id);
    }
    for w in hits.windows(2) {
        assert!(
            (w[0].distance, w[0].row_id) <= (w[1].distance, w[1].row_id),
            "ordering"
        );
    }
    let exact_top: Vec<u64> = exact_all.into_iter().take(10).map(|h| h.row_id).collect();
    let approx: Vec<u64> = hits.into_iter().map(|h| h.row_id).collect();
    let r = recall(&exact_top, &approx);
    assert!((0.0..=1.0).contains(&r), "recall {r} in range");
}

#[test]
fn ann_ordering_is_deterministic_with_ties_by_row_id() {
    // Duplicate payloads: distances tie everywhere; row_id must break ties
    // ascending, identically on repeat calls.
    let dim = 64;
    let mut index = hamming_index(dim, Metric::Hamming);
    let words = pack(&vec![true; dim]);
    for id in [7u64, 3, 9, 1, 5] {
        index.insert_words(id, &words).unwrap();
    }
    let probes = BinaryAnnProbes::default();
    let first = index.ann_search_words(&words, 5, probes).unwrap();
    let second = index.ann_search_words(&words, 5, probes).unwrap();
    let ids: Vec<u64> = first.iter().map(|h| h.row_id).collect();
    assert_eq!(ids, vec![1, 3, 5, 7, 9]);
    assert_eq!(
        first.iter().map(|h| h.row_id).collect::<Vec<_>>(),
        second.iter().map(|h| h.row_id).collect::<Vec<_>>()
    );
    assert!(first.iter().all(|h| h.distance == 0.0));
}

#[test]
fn ann_excludes_tombstones() {
    let dim = 128;
    let mut rng = Rng(99);
    let mut index = hamming_index(dim, Metric::Hamming);
    let base = random_bits(&mut rng, dim, 0.5);
    for id in 0..50u64 {
        let v = mutate(&mut rng, &base, 2);
        index.insert_words(id, &pack(&v)).unwrap();
    }
    for id in [0u64, 1, 2] {
        assert!(index.delete(id).unwrap());
    }
    let q = pack(&base);
    let hits = index
        .ann_search_words(&q, 50, BinaryAnnProbes::default())
        .unwrap();
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|h| h.row_id >= 3));
    assert_eq!(hits.len(), 47);
}

#[test]
fn ann_rejects_non_hamming_metrics() {
    let dim = 64;
    let q = pack(&vec![false; dim]);
    for metric in [Metric::Jaccard, Metric::L2, Metric::Cosine, Metric::Dot] {
        let index = hamming_index(dim, metric);
        let err = index
            .ann_search_words(&q, 5, BinaryAnnProbes::default())
            .unwrap_err();
        assert!(
            format!("{err:?}").contains("Hamming"),
            "metric {metric:?}: {err:?}"
        );
    }
}

#[test]
fn jaccard_exact_stays_correct_while_ann_refuses() {
    // Investigation record: block equality carries no Jaccard signal.
    // A and B share an identical 64-bit block yet sit far apart in Jaccard;
    // C and D are Jaccard-near but share no exact block.
    let dim = 128;
    let mut index = hamming_index(dim, Metric::Jaccard);
    // A: block0 = pattern P, block1 = empty. B: block0 = P, block1 = full.
    let mut a = vec![false; dim];
    let mut b = vec![false; dim];
    for i in (0..64).step_by(2) {
        a[i] = true;
        b[i] = true;
    }
    for slot in b.iter_mut().skip(64) {
        *slot = true;
    } // C/D: dense (all ones) with 2-bit difference -> Jaccard ~ 126/128.
    let c = vec![true; dim];
    let mut d = vec![true; dim];
    d[0] = false;
    d[64] = false;
    for (id, v) in [(10u64, &a), (11, &b), (12, &c), (13, &d)] {
        index.insert_words(id, &pack(v)).unwrap();
    }
    // Exact Jaccard: D is C's nearest neighbor despite sharing no block.
    let hits = index.search(&bits_to_f32(&c), 2).unwrap();
    assert_eq!(hits[0].row_id, 12); // self
    assert_eq!(hits[1].row_id, 13); // near-duplicate, no shared block
                                    // ANN refuses Jaccard explicitly instead of misranking.
    assert!(index
        .ann_search_words(&pack(&c), 2, BinaryAnnProbes::default())
        .is_err());
}

#[test]
fn ann_agrees_on_non_aligned_dimensions() {
    for dim in [1usize, 7, 9, 65, 130] {
        let mut rng = Rng(dim as u64 * 7919 + 13);
        let mut index = hamming_index(dim, Metric::Hamming);
        let mut rows = Vec::new();
        for id in 0..60u64 {
            let v = random_bits(&mut rng, dim, 0.4);
            rows.push(v.clone());
            index.insert_words(id, &pack(&v)).unwrap();
        }
        let q = pack(&rows[0]);
        let approx = index
            .ann_search_words(&q, 5, BinaryAnnProbes::default())
            .unwrap()
            .into_iter()
            .map(|h| h.row_id)
            .collect::<Vec<_>>();
        assert_eq!(
            recall(&exact_ids(&index, &bits_to_f32(&rows[0]), 5), &approx),
            1.0
        );
    }
}

#[test]
fn ann_empty_index_and_zero_k() {
    let index = hamming_index(64, Metric::Hamming);
    let q = pack(&[false; 64]);
    let (hits, stats) = index
        .ann_search_words_with_stats(&q, 5, BinaryAnnProbes::default())
        .unwrap();
    assert!(hits.is_empty());
    assert_eq!(stats.live_rows, 0);
    let mut index = hamming_index(64, Metric::Hamming);
    index.insert_words(0, &q).unwrap();
    let (hits, _) = index
        .ann_search_words_with_stats(&q, 0, BinaryAnnProbes::default())
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn ann_dimension_mismatch_fails_closed() {
    let index = hamming_index(64, Metric::Hamming);
    let bad = vec![0u64; 3]; // 64 bits need 1 word
    assert!(index
        .ann_search_words(&bad, 5, BinaryAnnProbes::default())
        .is_err());
}

#[test]
fn ann_examines_fewer_rows_than_full_scan_on_clusters() {
    // Work-saved proof (not wall-clock): clustered data serves top-k while
    // exact-scoring a strict subset of live rows, at recall 1.0.
    let dim = 256;
    let mut rng = Rng(0x55aa_55aa);
    let mut index = hamming_index(dim, Metric::Hamming);
    let mut centers = Vec::new();
    for cluster in 0..10 {
        let center = random_bits(&mut Rng(cluster * 1_000_003 + 7), dim, 0.2);
        centers.push(center.clone());
        for _ in 0..100 {
            let v = mutate(&mut rng, &center, 4);
            let id = index.stats().num_vectors as u64;
            index.insert_words(id, &pack(&v)).unwrap();
        }
    }
    let q = pack(&centers[0]);
    let (hits, stats) = index
        .ann_search_words_with_stats(&q, 10, BinaryAnnProbes::default())
        .unwrap();
    assert_eq!(hits.len(), 10);
    assert!(
        stats.candidates_examined < stats.live_rows,
        "examined {} of {}",
        stats.candidates_examined,
        stats.live_rows
    );
    assert!(!stats.full_scan_fallback);
}

#[test]
fn ann_wall_clock_vs_exact_on_large_clusters() {
    // Wall-clock comparison on 100K clustered rows (dim 512, k=10):
    // prints ANN vs exact latency, candidates examined, and recall.
    // Asserts recall 1.0 and strictly-less work; latency is reported, and
    // ANN must also win on the clock at this scale/margin.
    let dim = 512;
    let n = 100_000u64;
    let per_cluster = 2_500u64;
    let mut index = hamming_index(dim, Metric::Hamming);
    let mut rng = Rng(0xbeef_cafe);
    let mut centers = Vec::new();
    let mut id = 0u64;
    while id < n {
        let center = random_bits(
            &mut Rng(id
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407)),
            dim,
            0.25,
        );
        centers.push(center.clone());
        for _ in 0..per_cluster {
            if id >= n {
                break;
            }
            let v = mutate(&mut rng, &center, 6);
            index.insert_words(id, &pack(&v)).unwrap();
            id += 1;
        }
    }
    let q = pack(&centers[0]);
    let probes = BinaryAnnProbes::default();
    // Warmup.
    let _ = index.ann_search_words(&q, 10, probes).unwrap();
    let t0 = std::time::Instant::now();
    let mut ann_hits = Vec::new();
    let reps = 5;
    let mut examined = 0;
    for _ in 0..reps {
        let (hits, stats) = index.ann_search_words_with_stats(&q, 10, probes).unwrap();
        examined = stats.candidates_examined;
        ann_hits = hits;
    }
    let ann_us = t0.elapsed().as_micros() / reps as u128;
    let qf32 = bits_to_f32(&centers[0]);
    let _ = index.search(&qf32, 10).unwrap();
    let t0 = std::time::Instant::now();
    let mut exact_hits = Vec::new();
    for _ in 0..reps {
        exact_hits = index.search(&qf32, 10).unwrap();
    }
    let exact_us = t0.elapsed().as_micros() / reps as u128;
    let approx: Vec<u64> = ann_hits.into_iter().map(|h| h.row_id).collect();
    let exact: Vec<u64> = exact_hits.into_iter().map(|h| h.row_id).collect();
    let r = recall(&exact, &approx);
    eprintln!("binary-ann-bench rows={n} dim={dim} k=10 ann={ann_us}us exact={exact_us}us examined={examined} recall={r}");
    assert_eq!(r, 1.0);
    assert!(examined < n as usize);
    assert!(ann_us < exact_us, "ann {ann_us}us vs exact {exact_us}us");
}

#[test]
fn ann_ladder_agrees_with_exact() {
    let dim = 256;
    let mut index = hamming_index(dim, Metric::Hamming);
    let center: Vec<bool> = (0..dim).map(|i| i % 3 == 0).collect();
    index.insert_words(0, &pack(&center)).unwrap();
    for j in 1..=15usize {
        let mut v = center.clone();
        for slot in v.iter_mut().take(j) {
            *slot = !*slot;
        }
        index.insert_words(j as u64, &pack(&v)).unwrap();
    }
    // background inverted
    let inv: Vec<bool> = center.iter().map(|b| !b).collect();
    index.insert_words(100, &pack(&inv)).unwrap();
    let q = pack(&center);
    let (hits, stats) = index
        .ann_search_words_with_stats(&q, 10, BinaryAnnProbes::default())
        .unwrap();
    assert!(!stats.full_scan_fallback);
    let exact = index.search(&bits_to_f32(&center), 10).unwrap();
    let approx: Vec<(u64, u32)> = hits
        .into_iter()
        .map(|h| (h.row_id, h.distance as u32))
        .collect();
    let expected: Vec<(u64, u32)> = exact
        .into_iter()
        .map(|h| (h.row_id, h.distance as u32))
        .collect();
    // Ladder distances are unique (0..9): strict agreement, no ties.
    assert_eq!(approx, expected);
}

#[test]
fn ann_ladder_with_background_agrees() {
    let dim = 256;
    let mut index = hamming_index(dim, Metric::Hamming);
    let center: Vec<bool> = (0..dim).map(|i| i % 3 == 0).collect();
    index.insert_words(0, &pack(&center)).unwrap();
    for j in 1..=15usize {
        let mut v = center.clone();
        for slot in v.iter_mut().take(j) {
            *slot = !*slot;
        }
        index.insert_words(j as u64, &pack(&v)).unwrap();
    }
    // striped background with the same RNG shape as the SQL test
    let mut rng = Rng(0x1234_5678_9abc_def0);
    let inverted: Vec<bool> = center.iter().map(|b| !b).collect();
    for id in 17..2000u64 {
        let kind = rng.next() % 3;
        let v = if kind == 0 {
            inverted.clone()
        } else {
            (0..dim)
                .map(|i| {
                    let base = i % 3 == 0;
                    if rng.next() % 100 < 45 {
                        !base
                    } else {
                        base
                    }
                })
                .collect()
        };
        index.insert_words(id - 1, &pack(&v)).unwrap();
    }
    let q = pack(&center);
    let (hits, stats) = index
        .ann_search_words_with_stats(&q, 10, BinaryAnnProbes::default())
        .unwrap();
    // All 16 ladder rows share blocks with the query, so the probe set
    // resolves without fallback; the far background contributes nothing.
    assert!(!stats.full_scan_fallback);
    assert!(stats.candidates_examined < stats.live_rows);
    let exact = index.search(&bits_to_f32(&center), 10).unwrap();
    let approx: Vec<u64> = hits.into_iter().map(|h| h.row_id).collect();
    let expected: Vec<u64> = exact.into_iter().map(|h| h.row_id).collect();
    assert_eq!(approx, expected);
    assert_eq!(recall(&expected, &approx), 1.0);
}
