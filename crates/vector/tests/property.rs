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

//! Property / differential tests for `plomid-vector` (Phase 50 + 52 in code).
//!
//! Covers: codec round-trips for every kind, distance oracle against
//! hand-computed values, sparse canonicalization idempotence, Top-K
//! deterministic tie-breaking, dispatch == scalar, and HNSW recall vs the
//! Flat exact oracle on a small deterministic dataset.

use plomid_vector::distance::{
    approx_eq, cosine_distance, cosine_similarity, dot, hamming_distance, l2_distance, neg_dot,
};
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::math::{dispatch, scalar};
use plomid_vector::search::exact::exact_top_k;
use plomid_vector::search::SearchMetric;
use plomid_vector::sparse::representation::SparseVector;
use plomid_vector::{
    decode_standalone, encode_standalone, parse_binary, parse_dense, parse_multi, parse_sparse,
    BinaryVectorValue, DenseVectorValue, MultiVectorValue, SparseVectorValue, VectorElementType,
    VectorValue,
};

// --- codec round-trips -------------------------------------------------------

#[test]
fn codec_round_trips_every_kind() {
    // Dense.
    let dense =
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, -2.5, 3.25, 0.0]).unwrap();
    let back = decode_standalone(&encode_standalone(&VectorValue::Dense(dense)).unwrap()).unwrap();
    assert!(matches!(back, VectorValue::Dense(_)));

    // Sparse (unsorted input canonicalizes to sorted form).
    let sparse = parse_sparse("{9:0.5, 1:1.5, 4:-2.0}/16").unwrap();
    let back =
        decode_standalone(&encode_standalone(&VectorValue::Sparse(sparse)).unwrap()).unwrap();
    let VectorValue::Sparse(v) = back else {
        panic!("expected sparse");
    };
    assert_eq!(v.indices, vec![1, 4, 9]);

    // Binary incl. non-byte-aligned dimension (9 bits -> 2 bytes, padded).
    let binary = parse_binary("'101010101'").unwrap();
    assert_eq!(binary.dimension_bits, 9);
    assert!(matches!(
        decode_standalone(&encode_standalone(&VectorValue::Binary(binary)).unwrap()).unwrap(),
        VectorValue::Binary(_)
    ));

    // Multi-vector: two 3-dim constituents.
    let m = MultiVectorValue::new(
        VectorElementType::Float32,
        3,
        4,
        vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]],
    )
    .unwrap();
    assert!(matches!(
        decode_standalone(&encode_standalone(&VectorValue::Multi(m)).unwrap()).unwrap(),
        VectorValue::Multi(_)
    ));

    // Direct value constructors reject non-finite lanes.
    assert!(DenseVectorValue::from_f32(VectorElementType::Float32, vec![f32::NAN]).is_err());
    assert!(SparseVectorValue::new(8, vec![0], vec![f32::INFINITY]).is_err());
    let _ = BinaryVectorValue::new(8, vec![0xFF]).unwrap();

    // Multi literal parses with explicit cap.
    let parsed = parse_multi("[[1,0,0],[0,1,0]]", 4).unwrap();
    assert_eq!(parsed.count(), 2);
}

// --- distance oracle: hand-computed values -----------------------------------

#[test]
fn distance_matches_hand_computed_oracle() {
    let a = [1.0f32, 0.0, 0.0];
    let b = [0.0f32, 1.0, 0.0];
    // Orthogonal unit vectors: L2 = sqrt(2), cosine distance = 1, dot = 0.
    assert!(approx_eq(
        l2_distance(Some(&a), Some(&b)).unwrap().unwrap(),
        2f32.sqrt()
    ));
    assert!(approx_eq(
        cosine_distance(Some(&a), Some(&b)).unwrap().unwrap(),
        1.0
    ));
    assert!(approx_eq(dot(Some(&a), Some(&b)).unwrap().unwrap(), 0.0));
    assert!(approx_eq(
        neg_dot(Some(&a), Some(&b)).unwrap().unwrap(),
        -0.0
    ));

    // Identical vectors: zero distance, similarity 1, dot = norm^2.
    let c = [3.0f32, 4.0];
    assert!(approx_eq(
        l2_distance(Some(&c), Some(&c)).unwrap().unwrap(),
        0.0
    ));
    assert!(approx_eq(
        cosine_distance(Some(&c), Some(&c)).unwrap().unwrap(),
        0.0
    ));
    assert!(approx_eq(
        cosine_similarity(Some(&c), Some(&c)).unwrap().unwrap(),
        1.0
    ));
    assert!(approx_eq(dot(Some(&c), Some(&c)).unwrap().unwrap(), 25.0));

    // Opposite vectors: cosine distance = 2.
    let d = [1.0f32, 0.0];
    let e = [-1.0f32, 0.0];
    assert!(approx_eq(
        cosine_distance(Some(&d), Some(&e)).unwrap().unwrap(),
        2.0
    ));

    // NULL propagation.
    assert_eq!(l2_distance(None, Some(&a)).unwrap(), None);
    assert_eq!(cosine_distance(Some(&a), None).unwrap(), None);

    // Zero-norm cosine is NULL, never NaN.
    let zero = [0.0f32, 0.0, 0.0];
    assert_eq!(cosine_distance(Some(&zero), Some(&a)).unwrap(), None);

    // Hamming: 0b1010 vs 0b0110 over 4 bits differ in 2 positions.
    assert_eq!(
        hamming_distance(Some(&[0b1010_0000]), Some(&[0b0110_0000]), 4).unwrap(),
        Some(2)
    );
    // NULL Hamming.
    assert_eq!(hamming_distance(None, Some(&[0xFF]), 8).unwrap(), None);

    // Dimension mismatch is a hard error, not a fabricated number.
    assert!(l2_distance(Some(&[1.0, 2.0]), Some(&[1.0])).is_err());
}

// --- sparse canonicalization --------------------------------------------------

#[test]
fn sparse_canonicalization_is_idempotent() {
    // SparseVector sorts by index; duplicates are rejected (fail closed).
    let once = SparseVector::new(16, vec![(9, 0.5), (1, 1.5), (4, -2.0)]).unwrap();
    assert_eq!(once.indices, vec![1, 4, 9]);
    let twice = SparseVector::new(
        16,
        once.indices
            .iter()
            .zip(once.values.iter())
            .map(|(i, v)| (*i, *v))
            .collect(),
    )
    .unwrap();
    assert_eq!(once, twice);
    twice.validate().unwrap();
    // Duplicate index fails closed rather than silently merging.
    assert!(SparseVector::new(16, vec![(9, 0.5), (9, 0.25)]).is_err());

    // Out-of-range index fails closed.
    assert!(SparseVector::new(4, vec![(4, 1.0)]).is_err());
}

// --- Top-K determinism ---------------------------------------------------------

#[test]
fn topk_tie_breaks_by_row_id() {
    // Three identical vectors: all distances equal, output must be row-id order.
    let query = [1.0f32, 0.0];
    let rows: Vec<(u64, Vec<f32>)> = vec![
        (7, vec![1.0, 0.0]),
        (3, vec![1.0, 0.0]),
        (5, vec![1.0, 0.0]),
    ];
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let hits = exact_top_k(&query, borrowed, SearchMetric::L2, 3, None).unwrap();
    let ids: Vec<u64> = hits.iter().map(|c| c.row_id).collect();
    assert_eq!(ids, vec![3, 5, 7]);
}

// --- dispatch == scalar oracle --------------------------------------------------

#[test]
fn dispatch_matches_scalar_oracle() {
    // Odd dimension incl. non-multiple-of-lane-count length. Tier
    // tolerance contract applies (see `tier_close`).
    let a: Vec<f32> = (0..33).map(|i| (i as f32) * 0.25 - 4.0).collect();
    let b: Vec<f32> = (0..33).map(|i| (i as f32) * -0.125 + 1.0).collect();
    let (_, d_dot) = dispatch::dot(&a, &b);
    assert!(tier_close(d_dot.unwrap(), scalar::dot_f32(&a, &b).unwrap()));
    let (_, d_l2) = dispatch::l2_squared(&a, &b);
    assert!(tier_close(
        d_l2.unwrap(),
        scalar::l2_squared_f32(&a, &b).unwrap()
    ));
    let (_, d_cos) = dispatch::cosine_distance(&a, &b);
    match (d_cos.unwrap(), scalar::cosine_distance_f32(&a, &b).unwrap()) {
        (Some(got), Some(exact)) => assert!(tier_close(got, exact)),
        (None, None) => {}
        (got, exact) => panic!("cosine presence diverged: {got:?} vs {exact:?}"),
    }
}

// --- ANN recall vs Flat oracle ----------------------------------------------------

fn lcg_rows(n: usize, dim: usize, seed: u64) -> Vec<(u64, Vec<f32>)> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // Uniform in [-1, 1).
        ((state >> 33) as f64 / (u32::MAX as f64) * 2.0 - 1.0) as f32
    };
    (0..n as u64)
        .map(|id| (id, (0..dim).map(|_| next()).collect()))
        .collect()
}

#[test]
fn hnsw_recall_smoke_vs_flat_oracle() {
    // Fixed seed reported on failure so the fixture replays standalone.
    const SEED: u64 = 0x1234_5678;
    let dim = 16usize;
    let rows = lcg_rows(512, dim, SEED);
    let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.05 - 0.35).collect();

    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, dim)).unwrap();
    flat.build(&rows).unwrap();
    let oracle = flat.search(&query, 10).unwrap();
    assert_eq!(oracle.len(), 10);
    // Oracle is ascending by construction.
    for w in oracle.windows(2) {
        assert!(w[0].distance <= w[1].distance);
    }

    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, dim)).unwrap();
    hnsw.build(&rows).unwrap();
    hnsw.validate().unwrap();
    let approx = hnsw.search(&query, 10).unwrap();
    assert_eq!(approx.len(), 10);

    // Recall@10 against the exact oracle on this deterministic fixture.
    let oracle_ids: std::collections::HashSet<u64> = oracle.iter().map(|r| r.row_id).collect();
    let hits = approx
        .iter()
        .filter(|r| oracle_ids.contains(&r.row_id))
        .count();
    let recall = hits as f64 / 10.0;
    assert!(
        recall >= 0.5,
        "seed={SEED:#x} dim={dim} hnsw recall smoke failed: {hits}/10 (recall {recall})"
    );
}

// --- literal edge matrix ----------------------------------------------------------

#[test]
fn literal_edges_fail_closed() {
    assert!(parse_dense("", Some(3)).is_err());
    assert!(parse_dense("[]", Some(3)).is_err());
    assert!(parse_dense("[1,2]", Some(3)).is_err());
    assert!(parse_dense("[NaN, 0, 0]", Some(3)).is_err());
    assert!(parse_dense("[1, 2, 3]", Some(3)).is_ok());
    assert!(parse_binary("10102").is_err());
    assert!(parse_sparse("{1:NaN}/8").is_err());
}

// --- seeded dimension sweep: codec + dispatch vs scalar --------------------------

/// Tier tolerance contract (dispatch docs): relative `1e-4`, absolute
/// floor `1e-5`. SIMD tiers accumulate in `f32`; the oracle in `f64`.
fn tier_close(got: f32, exact: f32) -> bool {
    let diff = (got - exact).abs();
    diff <= 1e-5 || diff <= exact.abs() * 1e-4
}

#[test]
fn dimension_sweep_matches_scalar_oracle() {
    // Odd, lane-misaligned, and production-scale widths (no huge allocs:
    // the widest case here is 1536 lanes).
    for dim in [1usize, 3, 7, 15, 31, 33, 127, 128, 129, 511, 512, 1536] {
        let a: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.03125 - 1.0).collect();
        let b: Vec<f32> = (0..dim).map(|i| (i as f32) * -0.015625 + 0.5).collect();
        // Codec round-trip at this width.
        let dense =
            plomid_vector::DenseVectorValue::from_f32(VectorElementType::Float32, a.clone())
                .unwrap();
        let back =
            decode_standalone(&encode_standalone(&VectorValue::Dense(dense)).unwrap()).unwrap();
        let VectorValue::Dense(restored) = back else {
            panic!("dim={dim}: kind changed");
        };
        assert_eq!(restored.values_f32.len(), dim, "dim={dim}");
        // Dispatch agrees with the scalar oracle within the tier
        // tolerance contract (not bit-identically).
        let (_, d) = dispatch::dot(&a, &b);
        assert!(
            tier_close(d.unwrap(), scalar::dot_f32(&a, &b).unwrap()),
            "dim={dim}: dot diverged"
        );
        let (_, l) = dispatch::l2_squared(&a, &b);
        assert!(
            tier_close(l.unwrap(), scalar::l2_squared_f32(&a, &b).unwrap()),
            "dim={dim}: l2 diverged"
        );
    }
}
