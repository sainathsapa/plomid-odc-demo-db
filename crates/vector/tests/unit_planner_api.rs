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
// Moved from `src/planner_api.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::admin::IndexState;
use plomid_vector::options::{SearchMode, VectorSearchOptions};
use plomid_vector::planner_api::*;
use plomid_vector::r#type::{VectorElementType, VectorKind};
use plomid_vector::r#type::{VectorMetric, VectorType};

#[test]
fn eligibility_and_choice() {
    assert!(order_by_supported("<=>"));
    assert!(order_by_supported("cosine_distance"));
    assert!(!order_by_supported("ORDER BY id"));
    let t = VectorType::new(VectorKind::Dense, VectorElementType::Float32, 8).unwrap();
    assert!(index_usable(
        IndexState::Ready,
        &t,
        VectorMetric::Cosine,
        &t,
        VectorMetric::Cosine
    ));
    assert!(!index_usable(
        IndexState::Building,
        &t,
        VectorMetric::Cosine,
        &t,
        VectorMetric::Cosine
    ));
    assert_eq!(
        choose_exact_vs_ann(SearchMode::Auto, 100_000, true),
        SearchMode::Ann
    );
    assert_eq!(
        choose_exact_vs_ann(SearchMode::Auto, 100, true),
        SearchMode::Exact
    );
    let lines = explain_vector_plan(
        SearchMode::Ann,
        VectorMetric::Cosine,
        &t,
        &VectorSearchOptions::default(),
        10,
        true,
    );
    assert_eq!(lines.len(), 4);
    assert!(lines[3].starts_with("Backend: cpu("));
    assert!(
        estimate_cost(
            1000,
            128,
            SearchMode::Exact,
            &VectorSearchOptions::default(),
            10
        ) > 0.0
    );
}

#[test]
fn filtered_planning_prefers_exact_when_tiny_or_impossible() {
    use plomid_vector::stats::VectorStats;
    let mut stats = VectorStats::unknown(64);
    for _ in 0..20_000 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 5;
    // Impossible filter: exact scan returns empty without touching ANN.
    let plan = plan_filtered_search(&stats, 0.0, 10, true, &VectorSearchOptions::default());
    assert_eq!(plan.strategy, FilterStrategy::ExactScan);
    assert_eq!(plan.expected_rows, Some(0));
    assert!(plan.executable);
    // Unknown stats: conservative exact with a stated reason.
    let plan = plan_filtered_search(
        &VectorStats::unknown(64),
        0.5,
        10,
        true,
        &VectorSearchOptions::default(),
    );
    assert_eq!(plan.strategy, FilterStrategy::ExactScan);
    assert!(plan.reasons.iter().any(|r| r.contains("no statistics")));
    // Unknown selectivity: exact, never a guessed well.
    let plan = plan_filtered_search(&stats, f64::NAN, 10, true, &VectorSearchOptions::default());
    assert_eq!(plan.strategy, FilterStrategy::ExactScan);
    assert_eq!(plan.expected_rows, None);
    // Large survivors + usable index: pre-filter subset ANN.
    let plan = plan_filtered_search(&stats, 0.4, 10, true, &VectorSearchOptions::default());
    assert_eq!(plan.strategy, FilterStrategy::PreFilterSubsetAnn);
    assert_eq!(plan.expected_rows, Some(8_000)); // 0.4 selectivity over 20K rows
    assert!(plan.candidate_k >= 10);
    assert!(plan.rerank);
    // No usable index: filter-first exact, never ANN.
    let plan = plan_filtered_search(&stats, 0.5, 10, false, &VectorSearchOptions::default());
    assert_eq!(plan.strategy, FilterStrategy::FilterFirstExact);
    // Explain lines are deterministic and reasoned.
    let t = VectorType::new(
        plomid_vector::r#type::VectorKind::Dense,
        plomid_vector::r#type::VectorElementType::Float32,
        64,
    )
    .unwrap();
    let lines = explain_filter_plan(&plan, &t);
    assert!(lines.iter().any(|l| l.starts_with("Filter strategy:")));
    assert!(lines.iter().any(|l| l.starts_with("Reason:")));
}

#[test]
fn postfilter_is_evaluated_but_gated() {
    use plomid_vector::stats::VectorStats;
    let mut stats = VectorStats::unknown(64);
    for _ in 0..10_000 {
        stats.observe_row(false, false, true);
    }
    // High selectivity + large survivors: post-filter model wins on
    // cost, but execution is gated until stable row ids land.
    let plan = plan_filtered_search(&stats, 0.9, 10, true, &VectorSearchOptions::default());
    assert_eq!(plan.strategy, FilterStrategy::PostFilterOversampledAnn);
    assert!(!plan.executable);
    assert!(plan.reasons.iter().any(|r| r.contains("stable row")));
    assert_eq!(plan.fallback, FilterStrategy::PreFilterSubsetAnn);
    // Scaled well covers the selectivity gap: limit/sel * 8.
    assert_eq!(plan.candidate_k, 89);
}

#[test]
fn radius_planning_edges() {
    use plomid_vector::stats::VectorStats;
    let mut stats = VectorStats::unknown(8);
    for _ in 0..10_000 {
        stats.observe_row(false, false, true);
    }
    // Negative bound: empty result without touching storage.
    let plan = plan_radius_search(
        &stats,
        VectorMetric::L2,
        -0.5,
        true,
        &VectorSearchOptions::default(),
    );
    assert_eq!(plan.strategy, RadiusStrategy::EmptyResult);
    assert_eq!(plan.candidate_well, 0);
    // NaN bound: decline (exact path preserves SQL NULL semantics).
    let plan = plan_radius_search(
        &stats,
        VectorMetric::L2,
        f32::NAN,
        true,
        &VectorSearchOptions::default(),
    );
    assert_eq!(plan.strategy, RadiusStrategy::ExactEnumeration);
    assert!(plan.reasons.iter().any(|r| r.contains("non-finite")));
    // Zero radius on a large set: above the gate with ANN -> verify path.
    let plan = plan_radius_search(
        &stats,
        VectorMetric::L2,
        0.0,
        true,
        &VectorSearchOptions::default(),
    );
    assert_eq!(plan.strategy, RadiusStrategy::AnnCandidateVerify);
    assert_eq!(plan.index_bound, 0.0);
    // L2 index bound squares the SQL bound.
    let plan = plan_radius_search(
        &stats,
        VectorMetric::L2,
        0.5,
        true,
        &VectorSearchOptions::default(),
    );
    assert!((plan.index_bound - 0.25).abs() < 1e-9);
    assert!(plan.rerank);
    // Small table: exact regardless of bound.
    let tiny = VectorStats::unknown(8);
    let plan = plan_radius_search(
        &tiny,
        VectorMetric::L2,
        0.5,
        true,
        &VectorSearchOptions::default(),
    );
    assert_eq!(plan.strategy, RadiusStrategy::ExactEnumeration);
    // Similarity conversions are exact inverses of the definitions.
    assert_eq!(
        similarity_to_distance_bound(VectorMetric::Cosine, 0.75),
        Some(0.25)
    );
    assert_eq!(
        similarity_to_distance_bound(VectorMetric::Dot, 0.5),
        Some(-0.5)
    );
    assert_eq!(
        similarity_to_distance_bound(VectorMetric::Hamming, 0.5),
        None
    );
    let l2 = similarity_to_distance_bound(VectorMetric::L2, 0.5).unwrap();
    assert!((l2 - 1.0).abs() < 1e-6);
    assert_eq!(similarity_to_distance_bound(VectorMetric::L2, 0.0), None);
    // Explain lines carry strategy, bounds, and reasons.
    let t = VectorType::new(
        plomid_vector::r#type::VectorKind::Dense,
        plomid_vector::r#type::VectorElementType::Float32,
        8,
    )
    .unwrap();
    let lines = explain_radius_plan(&plan, VectorMetric::L2, &t);
    assert!(lines.iter().any(|l| l.contains("Radius strategy:")));
    assert!(lines.iter().any(|l| l.contains("index bound")));
}

#[test]
fn freshness_gates_are_conservative_never_wrong() {
    use plomid_vector::freshness::Freshness;
    use plomid_vector::metrics::VectorMetrics;
    use plomid_vector::stats::VectorStats;
    let mut stats = VectorStats::unknown(64);
    for _ in 0..20_000 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 5;
    let options = VectorSearchOptions::default();
    let metrics = VectorMetrics::new();
    // Fresh delegates exactly (same strategy as the base planner).
    let base = plan_filtered_search(&stats, 0.4, 10, true, &options);
    let fresh = plan_filtered_search_with_freshness(
        &stats,
        0.4,
        10,
        true,
        &options,
        Freshness::Fresh,
        Some(&metrics),
    );
    assert_eq!(fresh, base);
    // Stale forces exact with a stated reason + metrics bump.
    let stale = plan_filtered_search_with_freshness(
        &stats,
        0.4,
        10,
        true,
        &options,
        Freshness::Stale,
        Some(&metrics),
    );
    assert_eq!(stale.strategy, FilterStrategy::ExactScan);
    assert!(stale.reasons.iter().any(|r| r.contains("stale")));
    assert_eq!(metrics.snapshot().stale_plan_fallbacks, 1);
    // Invalid/recovering also decline to exact, never ANN.
    for f in [
        Freshness::Invalid,
        Freshness::Recovering,
        Freshness::Building,
    ] {
        let p = plan_filtered_search_with_freshness(&stats, 0.4, 10, true, &options, f, None);
        assert_eq!(p.strategy, FilterStrategy::ExactScan, "freshness={f:?}");
        let r = plan_radius_search_with_freshness(
            &stats,
            VectorMetric::L2,
            0.5,
            true,
            &options,
            f,
            None,
        );
        assert_eq!(
            r.strategy,
            RadiusStrategy::ExactEnumeration,
            "freshness={f:?}"
        );
    }
    // Exact-vs-ANN choice: stale forces exact even above the ANN gate.
    assert_eq!(
        choose_exact_vs_ann_with_freshness(
            SearchMode::Auto,
            100_000,
            true,
            Freshness::Stale,
            Some(&metrics)
        ),
        SearchMode::Exact
    );
    assert_eq!(
        choose_exact_vs_ann_with_freshness(SearchMode::Auto, 100_000, true, Freshness::Fresh, None),
        SearchMode::Ann
    );
    // Lifecycle gating: building index never serves ANN.
    let t = VectorType::new(
        plomid_vector::r#type::VectorKind::Dense,
        plomid_vector::r#type::VectorElementType::Float32,
        64,
    )
    .unwrap();
    assert!(!index_usable_with_freshness(
        IndexState::Building,
        Freshness::Fresh,
        &t,
        VectorMetric::Cosine,
        &t,
        VectorMetric::Cosine
    ));
    assert!(index_usable_with_freshness(
        IndexState::Ready,
        Freshness::Fresh,
        &t,
        VectorMetric::Cosine,
        &t,
        VectorMetric::Cosine
    ));
    assert!(!index_usable_with_freshness(
        IndexState::Ready,
        Freshness::Stale,
        &t,
        VectorMetric::Cosine,
        &t,
        VectorMetric::Cosine
    ));
}

#[test]
fn tiered_costs_bound_the_base_model() {
    use plomid_vector::scale::ScaleTier;
    let options = VectorSearchOptions::default();
    let base = estimate_cost(1_000_000, 128, SearchMode::Ann, &options, 10);
    // No tier == base model exactly.
    assert_eq!(
        estimate_cost_for_tier(1_000_000, 128, SearchMode::Ann, &options, 10, None),
        base
    );
    // Large-memory iron discounts ANN probes; laptops surcharge them —
    // bounded so tiers tune planning without flipping correctness.
    let large = estimate_cost_for_tier(
        1_000_000,
        128,
        SearchMode::Ann,
        &options,
        10,
        Some(ScaleTier::LargeMemoryHighIo),
    );
    let laptop = estimate_cost_for_tier(
        1_000_000,
        128,
        SearchMode::Ann,
        &options,
        10,
        Some(ScaleTier::Laptop),
    );
    assert!(large < base);
    assert!(laptop > base);
    assert!(large > 0.0 && laptop.is_finite());
    // Exact costs are tier-independent (authoritative scans don't fault).
    assert_eq!(
        estimate_cost_for_tier(
            1_000_000,
            128,
            SearchMode::Exact,
            &options,
            10,
            Some(ScaleTier::Laptop)
        ),
        estimate_cost_for_tier(
            1_000_000,
            128,
            SearchMode::Exact,
            &options,
            10,
            Some(ScaleTier::LargeMemoryHighIo)
        ),
    );
}

#[test]
fn full_explain_keeps_base_contract_and_adds_single_node_lines() {
    use plomid_vector::freshness::Freshness;
    let t = VectorType::new(
        plomid_vector::r#type::VectorKind::Dense,
        plomid_vector::r#type::VectorElementType::Float32,
        8,
    )
    .unwrap();
    let options = VectorSearchOptions::default();
    let numa = plomid_vector::numa::NumaTopology::test_seam(1, 8).unwrap();
    let scale = plomid_vector::scale::ScaleDecision::select(
        &plomid_vector::scale::ScaleInputs::test_seam(
            8,
            Some(16 * 1024 * 1024 * 1024),
            plomid_vector::storage::StorageClass::Ssd,
            1 << 30,
            2,
        ),
        &plomid_vector::scale::ScaleConfig::explicit(None),
    );
    let storage = plomid_vector::storage::VectorStoragePolicy::default().select(
        8 << 20,
        &plomid_vector::storage::StorageCapability::test_seam(
            true,
            plomid_vector::storage::StorageClass::Ssd,
            None,
        ),
    );
    let lines = explain_vector_plan_full(
        SearchMode::Ann,
        VectorMetric::Cosine,
        &t,
        &options,
        10,
        true,
        Some(&numa),
        Some(&scale),
        Some(&storage),
        Some(Freshness::Fresh),
    );
    // First four lines are exactly the base contract.
    assert_eq!(
        &lines[..4],
        &explain_vector_plan(
            SearchMode::Ann,
            VectorMetric::Cosine,
            &t,
            &options,
            10,
            true
        )
    );
    assert!(lines.iter().any(|l| l.starts_with("NUMA:")));
    assert!(lines.iter().any(|l| l.starts_with("Scale tier:")));
    assert!(lines.iter().any(|l| l.starts_with("Storage:")));
    assert!(lines.iter().any(|l| l.starts_with("Freshness:")));
    assert!(lines.iter().any(|l| l.contains("single-node")));
}
