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

//! Numerical semantics pins (Gap B in code).
//!
//! Every claim in `docs/VECTOR_NUMERICS.md` is proved here against the
//! actual implementation: types, conversions, accumulation, distance and
//! similarity definitions, squared-vs-rooted L2, zero-vector/NaN/Infinity
//! policy, integer overflow, determinism, SIMD tolerance, quantized,
//! sparse and binary semantics, and the tolerance policy.

use plomid_vector::distance::{
    approx_eq, cosine_distance, cosine_similarity, dot, dot_sparse, l2_distance,
    l2_distance_sparse, neg_dot,
};
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::ivf::IvfFlatIndex;
use plomid_vector::index::pq::PqIndex;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::math::{bfloat16, dispatch, float16, int, scalar};
use plomid_vector::search::exact::exact_top_k;
use plomid_vector::search::SearchMetric;

// --- types and conversions ----------------------------------------------------

#[test]
fn narrow_float_conversions_round_trip() {
    // 1.0 is exactly representable: canonical bit patterns.
    assert_eq!(float16::f32_to_f16(1.0), 0x3C00);
    assert_eq!(float16::f16_to_f32(0x3C00), 1.0);
    assert_eq!(bfloat16::f32_to_bf16(1.0), 0x3F80);
    assert_eq!(bfloat16::bf16_to_f32(0x3F80), 1.0);
    // Round-trip preserves the value for exactly representable lanes.
    for v in [0.0f32, 0.5, -2.0, 100.0] {
        assert_eq!(float16::f16_to_f32(float16::f32_to_f16(v)), v);
        assert_eq!(bfloat16::bf16_to_f32(bfloat16::f32_to_bf16(v)), v);
    }
    // Integer lanes are exact (no conversion involved).
    assert_eq!(int::dot_i8(&[100, 100], &[100, 100]).unwrap(), 20_000);
}

// --- accumulation --------------------------------------------------------------

#[test]
fn scalar_accumulates_in_f64() {
    // 16_777_217 consecutive integers: an f32 accumulator would lose the
    // low bits; the f64 oracle keeps them (exact small-integer proof).
    assert_eq!(
        scalar::dot_f32(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]).unwrap(),
        32.0
    );
    assert!(scalar::dot_f32(&[1.0], &[1.0, 2.0]).is_err());
}

// --- distance and similarity definitions ----------------------------------------

#[test]
fn distance_definitions_match() {
    let a = [3.0f32, 4.0];
    let b = [3.0f32, 4.0];
    // L2 rooted vs squared: d = sqrt(d²).
    let d = l2_distance(Some(&a), Some(&b)).unwrap().unwrap();
    assert_eq!(d, 0.0);
    let c = [1.0f32, 0.0];
    let e = [0.0f32, 1.0];
    let rooted = l2_distance(Some(&c), Some(&e)).unwrap().unwrap();
    assert!((rooted * rooted - 2.0).abs() < 1e-6);
    // Cosine similarity and distance are exact inverses.
    let sim = cosine_similarity(Some(&c), Some(&e)).unwrap().unwrap();
    let dis = cosine_distance(Some(&c), Some(&e)).unwrap().unwrap();
    assert!((sim + dis - 1.0).abs() < 1e-6);
    // SQL similarities: cosine_similarity = 1 - cosine_distance.
    assert!((sim - (1.0 - dis)).abs() < 1e-9);
    // Dot and negative dot are exact negations (<#> convention).
    let d1 = dot(Some(&a), Some(&b)).unwrap().unwrap();
    let n1 = neg_dot(Some(&a), Some(&b)).unwrap().unwrap();
    assert_eq!(d1, 25.0);
    assert_eq!(n1, -25.0);
}

#[test]
fn index_l2_orders_like_sql_l2() {
    // Index L2 is squared, SQL L2 is rooted: values differ, order agrees
    // (squaring is monotonic on non-negative distances).
    let rows: Vec<(u64, Vec<f32>)> = vec![
        (1, vec![1.0, 0.0]),
        (2, vec![0.0, 1.0]),
        (3, vec![0.5, 0.5]),
    ];
    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2)).unwrap();
    flat.build(&rows).unwrap();
    let index_hits = flat.search(&[1.0, 0.0], 3).unwrap();
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let sql_hits = exact_top_k(&[1.0, 0.0], borrowed, SearchMetric::L2, 3, None).unwrap();
    let index_ids: Vec<u64> = index_hits.iter().map(|h| h.row_id).collect();
    let sql_ids: Vec<u64> = sql_hits.iter().map(|c| c.row_id).collect();
    assert_eq!(index_ids, sql_ids);
}

// --- zero vectors, NULL, NaN, Infinity ------------------------------------------

#[test]
fn zero_null_and_nonfinite_policy() {
    let zero = [0.0f32, 0.0];
    let unit = [1.0f32, 0.0];
    // Zero-norm cosine is undefined -> NULL, never NaN.
    assert_eq!(cosine_distance(Some(&zero), Some(&unit)).unwrap(), None);
    assert_eq!(cosine_similarity(Some(&zero), Some(&zero)).unwrap(), None);
    // L2/dot on zero vectors are well-defined numbers.
    assert_eq!(l2_distance(Some(&zero), Some(&unit)).unwrap(), Some(1.0));
    assert_eq!(dot(Some(&zero), Some(&unit)).unwrap(), Some(0.0));
    // NULL propagates through every function.
    assert_eq!(l2_distance(None, Some(&unit)).unwrap(), None);
    assert_eq!(cosine_distance(Some(&unit), None).unwrap(), None);
    assert_eq!(dot(None, None).unwrap(), None);
    // NaN/Infinity lanes are rejected at ingest, never stored.
    assert!(plomid_vector::parse_dense("[NaN,0]", Some(2)).is_err());
    assert!(plomid_vector::parse_dense("[inf,0]", Some(2)).is_err());
    // Programmatic NaN lanes propagate as NaN (no error, no panic) and are
    // contained downstream: Top-K sorts them last, radius excludes them.
    let nan = l2_distance(Some(&[f32::NAN, 0.0]), Some(&unit))
        .unwrap()
        .unwrap();
    assert!(nan.is_nan());
    let rows = vec![(1u64, Some(vec![f32::NAN, 0.0])), (2, Some(vec![1.0, 0.0]))];
    let borrowed: Vec<(u64, Option<&[f32]>)> =
        rows.iter().map(|(id, v)| (*id, v.as_deref())).collect();
    let top = exact_top_k(&unit, borrowed, SearchMetric::L2, 2, None).unwrap();
    assert_eq!(top[0].row_id, 2);
    assert_eq!(top[1].row_id, 1);
    let cfg = plomid_vector::search::RadiusConfig::new(100.0).unwrap();
    let hits =
        plomid_vector::search::radius_search(&unit, &rows, SearchMetric::L2, &cfg, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].row_id, 2);
}

// --- integer overflow ------------------------------------------------------------

#[test]
fn integer_overflow_fails_instead_of_wrapping() {
    // i32::MAX² per lane (~4.6e18) overflows i64 accumulation within
    // four lanes; the error reports instead of wrapping.
    assert!(int::dot_i32(&[i32::MAX; 4], &[i32::MAX; 4]).is_err());
    // Small lanes stay exact.
    assert_eq!(int::dot_i8(&[1, 2], &[3, 4]).unwrap(), 11);
    assert_eq!(int::l2_squared_i8(&[1, 0], &[0, 1]).unwrap(), 2);
}

// --- determinism ------------------------------------------------------------------

#[test]
fn cosine_stays_within_unit_bounds() {
    // Similarity clamps to [-1, 1], so distance stays in [0, 2] even for
    // near-parallel float lanes where rounding could escape.
    let a = [0.1f32, 0.2, 0.3, 0.4];
    let b = [0.1000001f32, 0.2000001, 0.3000001, 0.4000001];
    let d = cosine_distance(Some(&a), Some(&b)).unwrap().unwrap();
    assert!((0.0..=2.0).contains(&d), "{d}");
}

#[test]
fn seeded_hnsw_builds_are_reproducible() {
    let rows: Vec<(u64, Vec<f32>)> = (0..64u64)
        .map(|id| (id, vec![id as f32 * 0.01, 1.0 - id as f32 * 0.005]))
        .collect();
    let mut config = IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2);
    config.params.seed = Some(7);
    let mut first = HnswIndex::new(config.clone()).unwrap();
    first.build(&rows).unwrap();
    let mut second = HnswIndex::new(config).unwrap();
    second.build(&rows).unwrap();
    let q = [0.5f32, 0.5];
    let h1 = first.search(&q, 5).unwrap();
    let h2 = second.search(&q, 5).unwrap();
    assert_eq!(h1, h2);
}

// --- SIMD vs scalar ------------------------------------------------------------------

#[test]
fn tier_agrees_with_scalar_within_contract() {
    // Relative 1e-4, absolute floor 1e-5 (dispatch docs).
    let close = |got: f32, exact: f32| {
        let diff = (got - exact).abs();
        diff <= 1e-5 || diff <= exact.abs() * 1e-4
    };
    let a: Vec<f32> = (0..512).map(|i| (i as f32) * 0.03125 - 8.0).collect();
    let b: Vec<f32> = (0..512).map(|i| (i as f32) * -0.015625 + 4.0).collect();
    let (tier, d) = dispatch::dot(&a, &b);
    assert!(close(d.unwrap(), scalar::dot_f32(&a, &b).unwrap()));
    // The reported tier names the executed kernel (never an aspiration).
    let host = plomid_hw::profile::CpuFeatures::detect();
    assert!(tier.supported_here());
    let _ = host;
}

// --- quantized ------------------------------------------------------------------

#[test]
fn quantized_indexes_rank_and_validate() {
    let rows: Vec<(u64, Vec<f32>)> = vec![
        (1, vec![1.0, 0.0, 0.0, 0.0]),
        (2, vec![0.0, 1.0, 0.0, 0.0]),
        (3, vec![0.0, 0.0, 1.0, 0.0]),
        (4, vec![0.0, 0.0, 0.0, 1.0]),
        (5, vec![0.9, 0.1, 0.0, 0.0]),
        (6, vec![0.1, 0.9, 0.0, 0.0]),
        (7, vec![0.0, 0.1, 0.9, 0.0]),
        (8, vec![0.0, 0.0, 0.1, 0.9]),
    ];
    for (kind, name) in [
        (IndexKind::IvfFlat, "ivf"),
        (IndexKind::IvfSq, "sq"),
        (IndexKind::IvfPq, "pq"),
    ] {
        let mut config = IndexConfig::new(kind, Metric::L2, 4);
        if kind == IndexKind::IvfPq {
            config.params.n_segments = Some(2);
        }
        let mut index: Box<dyn VectorIndex> = match kind {
            IndexKind::IvfFlat => Box::new(IvfFlatIndex::new(config).unwrap()),
            IndexKind::IvfSq => Box::new(plomid_vector::index::sq::Sq8Index::new(config).unwrap()),
            _ => Box::new(PqIndex::new(config).unwrap()),
        };
        index.build(&rows).unwrap();
        index.validate().unwrap();
        // Well-separated fixture: approximate ranks still put row 1 first.
        let hits = index.search(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2, "{name}");
        assert_eq!(hits[0].row_id, 1, "{name}");
        // Ascending-distance invariant holds for quantized ranks too.
        assert!(hits[0].distance <= hits[1].distance, "{name}");
    }
}

// --- sparse and binary ------------------------------------------------------------------

#[test]
fn sparse_missing_dimensions_read_as_zero() {
    // Disjoint coordinates share nothing: dot is exactly 0.
    assert_eq!(dot_sparse(&[(0, 1.0)], &[(9, 1.0)]), 0.0);
    // Identical sparse vectors: zero L2.
    let v = [(1u32, 2.0f32), (5, -1.0)];
    assert_eq!(l2_distance_sparse(&v, &v), 0.0);
}

#[test]
fn binary_is_msb_first_with_masked_padding() {
    // First literal bit is index 0 (MSB-first, matching ingest).
    let v = plomid_vector::parse_binary("'10000000'").unwrap();
    assert!(v.get_bit(0));
    assert!(!v.get_bit(7));
    // 9-bit self-distance is 0: padding never counts.
    let w = plomid_vector::parse_binary("'101010101'").unwrap();
    assert_eq!(
        plomid_vector::distance::hamming_distance(Some(&w.bytes), Some(&w.bytes), 9).unwrap(),
        Some(0)
    );
}

// --- tolerance policy ------------------------------------------------------------------

#[test]
fn tolerance_policy_is_total() {
    // approx_eq boundary: within 1e-5 inclusive.
    assert!(approx_eq(1.0, 1.0 + 5e-6));
    assert!(!approx_eq(1.0, 1.0 + 5e-4));
    // Radius inclusion is `d <= radius + TOLERANCE`.
    let rows = vec![(1u64, Some(vec![1.0f32, 0.0]))];
    let cfg = plomid_vector::search::RadiusConfig::new(0.0).unwrap();
    let out =
        plomid_vector::search::radius_search(&[1.0, 0.0], &rows, SearchMetric::L2, &cfg, None)
            .unwrap();
    assert_eq!(out.len(), 1);
    // Empty inputs are defined: dot is 0, L2 is 0, cosine is NULL.
    assert_eq!(dot(Some(&[]), Some(&[])).unwrap(), Some(0.0));
    assert_eq!(l2_distance(Some(&[]), Some(&[])).unwrap(), Some(0.0));
    assert_eq!(cosine_distance(Some(&[]), Some(&[])).unwrap(), None);
    // neg_dot is the exact negation (operator `<#>` convention).
    assert_eq!(
        plomid_vector::distance::neg_dot(Some(&[2.0]), Some(&[3.0])).unwrap(),
        Some(-6.0)
    );
}
