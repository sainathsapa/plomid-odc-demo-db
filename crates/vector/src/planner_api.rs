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
//! Planner interface: eligibility, exact-vs-ANN choice, cost model, EXPLAIN.
//!
//! This module never touches SQL ASTs or storage pages; it answers small
//! typed questions the SQL planner asks through a stable boundary.

use crate::admin::IndexState;
use crate::options::{SearchMode, VectorSearchOptions};
use crate::r#type::{VectorMetric, VectorType};
use crate::stats::SELECTIVITY_UNKNOWN;

/// True when `ORDER BY <expr>` is a supported distance ordering.
///
/// Accepts operator spellings (`<=>`, `<->`, `<#>`, `<~>`, `<%>`) and
/// function spellings (`cosine_distance`, `l2_distance`, `dot_product`,
/// `neg_inner_product` for `<#>`, `hamming_distance`, `jaccard_distance`).
#[must_use]
pub fn order_by_supported(expr: &str) -> bool {
    matches!(
        expr.trim().to_ascii_lowercase().as_str(),
        "<=>"
            | "<->"
            | "<#>"
            | "<~>"
            | "<%>"
            | "cosine_distance"
            | "l2_distance"
            | "dot_product"
            | "neg_inner_product"
            | "hamming_distance"
            | "jaccard_distance"
    )
}

/// True when an ANN index may serve `(query_type, query_metric)`:
/// state queryable, type identity equal, metric supported by the index.
#[must_use]
pub fn index_usable(
    state: IndexState,
    indexed: &VectorType,
    index_metric: VectorMetric,
    query: &VectorType,
    query_metric: VectorMetric,
) -> bool {
    index_usable_with_freshness(
        state,
        crate::freshness::Freshness::Fresh,
        indexed,
        index_metric,
        query,
        query_metric,
    )
}

/// Freshness-aware index usability (Gap J live path).
///
/// Lifecycle gating ([`crate::freshness::is_index_queryable`]) applies on
/// top of the type/metric checks: incomplete builds, stale images, and
/// invalid/recovering states never serve ANN even when the triple matches.
/// Stale statistics cannot silently produce wrong results — the planner
/// falls back to exact (performance degrades, correctness holds).
#[must_use]
pub fn index_usable_with_freshness(
    state: IndexState,
    freshness: crate::freshness::Freshness,
    indexed: &VectorType,
    index_metric: VectorMetric,
    query: &VectorType,
    query_metric: VectorMetric,
) -> bool {
    if !crate::freshness::is_index_queryable(state, freshness) {
        return false;
    }
    if indexed != query || index_metric != query_metric {
        return false;
    }
    indexed.supports_exact(query_metric)
}

/// Row count above which `Auto` prefers ANN (exact below).
///
/// Mirrors the executor's execution gate: the same constant decides in both
/// places so EXPLAIN and execution can never disagree. Small tables scan
/// exactly (graph overhead would lose); large tables ride the graph.
pub const AUTO_ANN_MIN_ROWS: u64 = 4096;

/// Choose exact vs ANN for one query. ANN requires an explicitly usable
/// index; `Auto` prefers ANN above 4096 rows, exact below.
#[must_use]
pub fn choose_exact_vs_ann(mode: SearchMode, row_count: u64, ann_available: bool) -> SearchMode {
    choose_exact_vs_ann_with_freshness(
        mode,
        row_count,
        ann_available,
        crate::freshness::Freshness::Fresh,
        None,
    )
}

/// Freshness-aware exact-vs-ANN choice (Gap J live path).
///
/// Stale/missing freshness (`Stale`, `Unknown`, `Building`, `Invalid`,
/// `Recovering`) forces conservative exact planning with a metrics bump —
/// performance degrades, results stay correct. Only [`Freshness::Fresh`]
/// may ride the graph. Pass `metrics` to record stale fallbacks in the
/// existing diagnostics.
#[must_use]
pub fn choose_exact_vs_ann_with_freshness(
    mode: SearchMode,
    row_count: u64,
    ann_available: bool,
    freshness: crate::freshness::Freshness,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> SearchMode {
    if !freshness.usable_for_ann() {
        if let Some(m) = metrics {
            m.observe_stale_fallback();
        }
        return SearchMode::Exact;
    }
    match mode {
        SearchMode::Exact => SearchMode::Exact,
        SearchMode::Ann => {
            if ann_available {
                SearchMode::Ann
            } else {
                SearchMode::Exact
            }
        }
        SearchMode::Auto => {
            if ann_available && row_count > AUTO_ANN_MIN_ROWS {
                SearchMode::Ann
            } else {
                SearchMode::Exact
            }
        }
    }
}

/// Rough cost in abstract units: exact is `rows * dim`; ANN is
/// `log2(rows) * dim * probe_penalty + rerank * dim`.
#[must_use]
pub fn estimate_cost(
    row_count: u64,
    dimension: u32,
    mode: SearchMode,
    options: &VectorSearchOptions,
    limit: u32,
) -> f64 {
    estimate_cost_for_tier(row_count, dimension, mode, options, limit, None)
}

/// Tier-adjusted cost (Gap L live path).
///
/// The base model is unchanged; the tier scales the ANN probe term to
/// reflect single-machine reality (large-memory/high-I/O servers fault less,
/// laptops fault more). `None` yields exactly [`estimate_cost`], so existing
/// callers are unaffected. Explicit tier selection flows from
/// [`crate::scale::ScaleDecision`]; never a node count.
#[must_use]
pub fn estimate_cost_for_tier(
    row_count: u64,
    dimension: u32,
    mode: SearchMode,
    options: &VectorSearchOptions,
    limit: u32,
    tier: Option<crate::scale::ScaleTier>,
) -> f64 {
    let rows = row_count.max(1) as f64;
    let dim = u64::from(dimension.max(1)) as f64;
    // Single-machine I/O factor: cheaper ANN probes where page residency is
    // high, pricier where memory is tight. Bounded [0.5, 2.0] so tiers tune
    // planning without ever flipping correctness.
    let io_factor = match tier {
        None => 1.0,
        Some(crate::scale::ScaleTier::Laptop) => 1.5,
        Some(crate::scale::ScaleTier::Workstation) => 1.0,
        Some(crate::scale::ScaleTier::SmallServer) => 1.0,
        Some(crate::scale::ScaleTier::ProductionServer) => 0.8,
        Some(crate::scale::ScaleTier::LargeMemoryHighIo) => 0.5,
    };
    match mode {
        SearchMode::Exact => rows * dim,
        SearchMode::Ann => {
            let probes = f64::from(options.nprobe.max(1));
            let rerank = f64::from(options.rerank_k.unwrap_or(limit).max(limit).max(1));
            (rows.log2() * dim * probes + rerank * dim) * io_factor
        }
        SearchMode::Auto => {
            let exact = rows * u64::from(dimension.max(1)) as f64;
            let ann =
                estimate_cost_for_tier(row_count, dimension, SearchMode::Ann, options, limit, tier);
            exact.min(ann)
        }
    }
}

/// EXPLAIN helper: deterministic `String` lines describing the vector plan.
#[must_use]
pub fn explain_vector_plan(
    mode: SearchMode,
    metric: VectorMetric,
    vector_type: &VectorType,
    options: &VectorSearchOptions,
    limit: u32,
    ann_available: bool,
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Vector {} scan on {} metric {} limit {limit}",
        match mode {
            SearchMode::Exact => "Exact",
            SearchMode::Ann => "ANN",
            SearchMode::Auto => "Auto",
        },
        vector_type.sql_name(),
        metric.operator()
    ));
    lines.push(format!(
        "Options: ef_search={} nprobe={} candidate_k={} rerank_k={} filter={:?} compute={:?}",
        options.ef_search,
        options.nprobe,
        options
            .candidate_k
            .map_or_else(|| "auto".to_string(), |k| k.to_string()),
        options
            .rerank_k
            .map_or_else(|| "none".to_string(), |k| k.to_string()),
        options.filter_mode,
        options.compute
    ));
    lines.push(format!(
        "Index: {}",
        if ann_available {
            "usable"
        } else {
            "absent (exact fallback)"
        }
    ));
    lines.push(format!(
        "Backend: cpu({}) distance kernels (portable scalar; tier labels the dispatch path that would accelerate it)",
        crate::math::dispatch::active_backend().name()
    ));
    lines
}

// ---------------------------------------------------------------------------
// Filter-aware planning (Phase 34) and radius planning (Phase 35).
// ---------------------------------------------------------------------------

/// Filter execution strategy chosen by [`plan_filtered_search`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterStrategy {
    /// Brute-force exact scan (also covers empty/impossible filters and
    /// unknown statistics — always correct, never touches an index).
    ExactScan,
    /// Apply the relational predicate first, then exact-scan the survivors.
    FilterFirstExact,
    /// Build/serve a fingerprint-keyed graph over exactly the filtered
    /// working set, then exact-rerank. The graph covers precisely the rows
    /// passing the filter, so it cannot under-return by construction.
    PreFilterSubsetAnn,
    /// Score an unfiltered index first, then verify the predicate per
    /// candidate with oversampling (`candidate_k` scaled by selectivity).
    /// Evaluated and costed by the planner, but execution-gated: serving
    /// it needs stable row ids (positional lanes cannot map unfiltered
    /// candidates back to filtered rows), so the executor declines it
    /// with a stated reason until that lands (see `fallback`).
    PostFilterOversampledAnn,
}

/// Filter plan: strategy, sizing, costs, and human-readable reasons.
///
/// `executable` is false only for [`FilterStrategy::PostFilterOversampledAnn`];
/// `fallback` names the strategy execution uses instead (always executable).
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPlan {
    /// Chosen strategy.
    pub strategy: FilterStrategy,
    /// Whether the executor can serve `strategy` today.
    pub executable: bool,
    /// Executable strategy used when `strategy` is gated.
    pub fallback: FilterStrategy,
    /// Expected surviving rows (`None` when selectivity is unknown).
    pub expected_rows: Option<u64>,
    /// Oversampled candidate depth for ANN strategies.
    pub candidate_k: u32,
    /// Whether exact rerank from authoritative rows applies.
    pub rerank: bool,
    /// Estimated exact-scan cost (abstract units, [`estimate_cost`]).
    pub cost_exact: f64,
    /// Estimated ANN cost (abstract units, [`estimate_cost`]).
    pub cost_ann: f64,
    /// Why, in deterministic order; never empty.
    pub reasons: Vec<String>,
}

/// Normalize a selectivity input: finite `0.0..=1.0` passes through,
/// anything else (NaN, negative, `> 1`, [`SELECTIVITY_UNKNOWN`]) is
/// "unknown" (`None`) — never clamped into a false precision.
fn normalize_selectivity(selectivity: f64) -> Option<f64> {
    if !selectivity.is_finite() || selectivity < 0.0 || selectivity > 1.0 {
        return None;
    }
    let _ = SELECTIVITY_UNKNOWN;
    Some(selectivity)
}

/// Choose a filter execution strategy from statistics and selectivity.
///
/// Cost-based, not threshold-magic: below the shared
/// [`AUTO_ANN_MIN_ROWS`] gate (which execution enforces identically) the
/// graph overhead loses, so filter-first exact wins; above it the
/// [`estimate_cost`] comparison decides. Unknown statistics or unknown
/// selectivity degrades to exact with a stated reason. Highly selective
/// filters shrink `expected_rows` into the exact band automatically, so
/// correctness under aggressive filtering falls out of the cost model —
/// no special case can silently under-return.
#[must_use]
pub fn plan_filtered_search(
    stats: &crate::stats::VectorStats,
    selectivity: f64,
    limit: u32,
    ann_available: bool,
    options: &VectorSearchOptions,
) -> FilterPlan {
    let dim = stats.dimension;
    let limit = limit.max(1);
    let base_reasons: Vec<String> = Vec::new();
    let _ = base_reasons;
    if !stats.is_observed() {
        return FilterPlan {
            strategy: FilterStrategy::ExactScan,
            executable: true,
            fallback: FilterStrategy::ExactScan,
            expected_rows: None,
            candidate_k: limit,
            rerank: false,
            cost_exact: estimate_cost(0, dim, SearchMode::Exact, options, limit),
            cost_ann: f64::INFINITY,
            reasons: vec!["no statistics observed: exact scan is the safe default".to_string()],
        };
    }
    let Some(sel) = normalize_selectivity(selectivity) else {
        return FilterPlan {
            strategy: FilterStrategy::ExactScan,
            executable: true,
            fallback: FilterStrategy::ExactScan,
            expected_rows: None,
            candidate_k: limit,
            rerank: false,
            cost_exact: estimate_cost(stats.live_rows, dim, SearchMode::Exact, options, limit),
            cost_ann: f64::INFINITY,
            reasons: vec![
                "selectivity unknown: exact scan instead of guessing a candidate well".to_string(),
            ],
        };
    };
    if sel == 0.0 {
        return FilterPlan {
            strategy: FilterStrategy::ExactScan,
            executable: true,
            fallback: FilterStrategy::ExactScan,
            expected_rows: Some(0),
            candidate_k: limit,
            rerank: false,
            cost_exact: 0.0,
            cost_ann: f64::INFINITY,
            reasons: vec![
                "impossible filter (selectivity 0): exact scan returns empty without touching the index"
                    .to_string(),
            ],
        };
    }
    let expected = (stats.live_rows as f64 * sel).floor() as u64;
    let cost_exact = estimate_cost(expected, dim, SearchMode::Exact, options, limit);
    let candidate_pre = options.effective_candidate_k(limit);
    let cost_ann_pre = estimate_cost(
        expected,
        dim,
        SearchMode::Ann,
        options,
        limit.min(candidate_pre),
    );
    if !ann_available {
        return FilterPlan {
            strategy: FilterStrategy::FilterFirstExact,
            executable: true,
            fallback: FilterStrategy::FilterFirstExact,
            expected_rows: Some(expected),
            candidate_k: limit,
            rerank: false,
            cost_exact,
            cost_ann: f64::INFINITY,
            reasons: vec!["no usable ANN index: filter first, then exact scan".to_string()],
        };
    }
    if expected <= AUTO_ANN_MIN_ROWS {
        return FilterPlan {
            strategy: FilterStrategy::FilterFirstExact,
            executable: true,
            fallback: FilterStrategy::FilterFirstExact,
            expected_rows: Some(expected),
            candidate_k: limit,
            rerank: false,
            cost_exact,
            cost_ann: cost_ann_pre,
            reasons: vec![format!(
                "expected {expected} survivors below the shared ANN gate ({AUTO_ANN_MIN_ROWS}): graph overhead loses"
            )],
        };
    }
    if cost_ann_pre < cost_exact {
        // Highly selective-but-large survivors: the post-filter model wins
        // on cost, but serving it needs stable row ids — gate it loudly.
        if sel >= 0.5 {
            let scaled = (limit as f64 * 8.0 / sel).ceil().clamp(1.0, 1_000_000.0) as u32;
            return FilterPlan {
                strategy: FilterStrategy::PostFilterOversampledAnn,
                executable: false,
                fallback: FilterStrategy::PreFilterSubsetAnn,
                expected_rows: Some(expected),
                candidate_k: scaled,
                rerank: true,
                cost_exact,
                cost_ann: cost_ann_pre,
                reasons: vec![
                    format!(
                        "post-filter model cheaper (selectivity {sel}, candidate well {scaled})"
                    ),
                    "post-filter serving needs stable row ids: subset path executes instead"
                        .to_string(),
                ],
            };
        }
        return FilterPlan {
            strategy: FilterStrategy::PreFilterSubsetAnn,
            executable: true,
            fallback: FilterStrategy::PreFilterSubsetAnn,
            expected_rows: Some(expected),
            candidate_k: candidate_pre,
            rerank: true,
            cost_exact,
            cost_ann: cost_ann_pre,
            reasons: vec![
                format!(
                    "ANN over the filtered subset wins on cost ({cost_ann_pre:.1} < {cost_exact:.1})"
                ),
                "subset graph covers exactly the filtered rows: cannot under-return".to_string(),
            ],
        };
    }
    FilterPlan {
        strategy: FilterStrategy::FilterFirstExact,
        executable: true,
        fallback: FilterStrategy::FilterFirstExact,
        expected_rows: Some(expected),
        candidate_k: limit,
        rerank: false,
        cost_exact,
        cost_ann: cost_ann_pre,
        reasons: vec![format!(
            "exact wins on cost ({cost_exact:.1} <= {cost_ann_pre:.1}): filter first, then scan"
        )],
    }
}

/// Render a [`FilterPlan`] as deterministic EXPLAIN lines.
#[must_use]
pub fn explain_filter_plan(plan: &FilterPlan, vector_type: &VectorType) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Filter strategy: {:?} on {} (expected rows {})",
        plan.strategy,
        vector_type.sql_name(),
        plan.expected_rows
            .map_or_else(|| "unknown".to_string(), |r| r.to_string())
    ));
    if !plan.executable {
        lines.push(format!(
            "Strategy gated: executing fallback {:?} instead",
            plan.fallback
        ));
    }
    lines.push(format!(
        "Candidate well: k={} rerank={} cost exact={:.1} ann={:.1}",
        plan.candidate_k,
        if plan.rerank { "yes" } else { "no" },
        plan.cost_exact,
        plan.cost_ann
    ));
    for reason in &plan.reasons {
        lines.push(format!("Reason: {reason}"));
    }
    lines
}

/// Radius execution strategy chosen by [`plan_radius_search`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RadiusStrategy {
    /// Enumerate rows and keep `distance <= bound + TOLERANCE`.
    ExactEnumeration,
    /// Candidate well from a cached backend `range_search`, then exact
    /// verification against the bound. The well is the bounded live set —
    /// it bounds *work*, never truncates *results*.
    AnnCandidateVerify,
    /// Negative bound: matches nothing without touching storage.
    EmptyResult,
}

/// Radius plan: strategy, bounds in both unit systems, and reasons.
#[derive(Debug, Clone, PartialEq)]
pub struct RadiusPlan {
    /// Chosen strategy.
    pub strategy: RadiusStrategy,
    /// SQL-facing bound (rooted for L2, native otherwise).
    pub bound: f32,
    /// Index-facing bound (squared for L2, native otherwise).
    pub index_bound: f32,
    /// Bounded candidate well (live indexed rows, capped to `u32`).
    pub candidate_well: u32,
    /// Whether exact verification from authoritative rows applies.
    pub rerank: bool,
    /// Estimated exact-enumeration cost.
    pub cost_exact: f64,
    /// Estimated candidate-verify cost.
    pub cost_ann: f64,
    /// Why, in deterministic order; never empty.
    pub reasons: Vec<String>,
}

/// Convert a similarity threshold to a distance bound for `metric`.
///
/// - Cosine (`similarity = 1 - distance`): `1 - threshold`.
/// - L2 (`similarity = 1 / (1 + distance)`): `1/threshold - 1` for
///   `threshold > 0`; otherwise `None` (unbounded/empty, not plannable).
/// - Dot (`<#>` stores negative inner product): `-threshold`.
/// - Hamming/Jaccard expose no similarity functions: `None`.
#[must_use]
pub fn similarity_to_distance_bound(metric: VectorMetric, threshold: f32) -> Option<f32> {
    if !threshold.is_finite() {
        return None;
    }
    match metric {
        VectorMetric::Cosine => Some(1.0 - threshold),
        VectorMetric::L2 => {
            if threshold <= 0.0 {
                None
            } else {
                Some(1.0 / threshold - 1.0)
            }
        }
        VectorMetric::Dot => Some(-threshold),
        VectorMetric::Hamming | VectorMetric::Jaccard => None,
    }
}

/// Convert a SQL-facing radius bound to the index-facing bound.
///
/// Index L2 distances are squared (`common::dense_distance`), so the bound
/// squares too (`b²`); every other metric shares units between SQL and
/// index. Non-finite bounds pass through for the caller to decline.
#[must_use]
pub fn index_radius_bound(metric: VectorMetric, bound: f32) -> f32 {
    match metric {
        VectorMetric::L2 => bound * bound,
        _ => bound,
    }
}

/// Choose a radius execution strategy.
///
/// A radius query is NOT Top-K with a large K: termination is by bound,
/// output is every row within it (never truncated to fit a well), and the
/// candidate well bounds *work*, not results. Negative bounds match
/// nothing (valid empty, no storage touch); non-finite bounds decline to
/// exact so SQL NULL semantics survive; unobserved statistics or small
/// live sets enumerate exactly; large live sets with a usable index verify
/// candidates from the cached backend well.
#[must_use]
pub fn plan_radius_search(
    stats: &crate::stats::VectorStats,
    metric: VectorMetric,
    bound: f32,
    ann_available: bool,
    options: &VectorSearchOptions,
) -> RadiusPlan {
    if !bound.is_finite() {
        let live = stats.live_rows;
        return RadiusPlan {
            strategy: RadiusStrategy::ExactEnumeration,
            bound,
            index_bound: bound,
            candidate_well: live.min(u32::MAX as u64) as u32,
            rerank: false,
            cost_exact: estimate_cost(live, stats.dimension, SearchMode::Exact, options, 10),
            cost_ann: f64::INFINITY,
            reasons: vec![
                "non-finite bound: exact enumeration preserves SQL NULL semantics".to_string(),
            ],
        };
    }
    if bound < 0.0 {
        return RadiusPlan {
            strategy: RadiusStrategy::EmptyResult,
            bound,
            index_bound: bound,
            candidate_well: 0,
            rerank: false,
            cost_exact: 0.0,
            cost_ann: f64::INFINITY,
            reasons: vec![
                "negative distance bound matches nothing: valid empty result without touching storage"
                    .to_string(),
            ],
        };
    }
    let live = stats.live_rows;
    let well = stats.indexed_rows.min(live).min(u32::MAX as u64) as u32;
    if !stats.is_observed() {
        return RadiusPlan {
            strategy: RadiusStrategy::ExactEnumeration,
            bound,
            index_bound: index_radius_bound(metric, bound),
            candidate_well: well,
            rerank: false,
            cost_exact: estimate_cost(live, stats.dimension, SearchMode::Exact, options, 10),
            cost_ann: f64::INFINITY,
            reasons: vec![
                "no statistics observed: exact enumeration is the safe default".to_string(),
            ],
        };
    }
    if live <= AUTO_ANN_MIN_ROWS || !ann_available {
        let reason = if !ann_available {
            "no usable ANN index: exact enumeration".to_string()
        } else {
            format!(
                "live set {live} below the shared ANN gate ({AUTO_ANN_MIN_ROWS}): enumeration wins"
            )
        };
        return RadiusPlan {
            strategy: RadiusStrategy::ExactEnumeration,
            bound,
            index_bound: index_radius_bound(metric, bound),
            candidate_well: well,
            rerank: false,
            cost_exact: estimate_cost(live, stats.dimension, SearchMode::Exact, options, 10),
            cost_ann: f64::INFINITY,
            reasons: vec![reason],
        };
    }
    let rerank_options = VectorSearchOptions {
        rerank_k: Some(well.max(1)),
        ..options.clone()
    };
    RadiusPlan {
        strategy: RadiusStrategy::AnnCandidateVerify,
        bound,
        index_bound: index_radius_bound(metric, bound),
        candidate_well: well,
        rerank: true,
        cost_exact: estimate_cost(live, stats.dimension, SearchMode::Exact, options, 10),
        cost_ann: estimate_cost(live, stats.dimension, SearchMode::Ann, &rerank_options, 10),
        reasons: vec![
            format!(
                "live set {live} above the gate with a usable index: bounded well ({well}) + exact verification"
            ),
            "well bounds work, never results: every row within bound is returned".to_string(),
        ],
    }
}

/// Render a [`RadiusPlan`] as deterministic EXPLAIN lines.
#[must_use]
pub fn explain_radius_plan(
    plan: &RadiusPlan,
    metric: VectorMetric,
    vector_type: &VectorType,
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "Radius strategy: {:?} on {} metric {} bound {} (index bound {})",
        plan.strategy,
        vector_type.sql_name(),
        metric.operator(),
        plan.bound,
        plan.index_bound
    ));
    lines.push(format!(
        "Candidate well: {} rerank={} cost exact={:.1} ann={:.1}",
        plan.candidate_well,
        if plan.rerank { "yes" } else { "no" },
        plan.cost_exact,
        plan.cost_ann
    ));
    for reason in &plan.reasons {
        lines.push(format!("Reason: {reason}"));
    }
    lines
}

// ---------------------------------------------------------------------------
// Gap J freshness-aware planning + Gap L/G single-machine EXPLAIN.
// ---------------------------------------------------------------------------

/// Freshness-aware filtered planning (Gap J live path).
///
/// Non-ANN-usable freshness (`Stale`, `Building`, `Invalid`, `Recovering`)
/// forces conservative exact planning — stale statistics trigger a
/// conservative plan with a stated reason, missing statistics keep the
/// existing safe fallback. `Fresh` and `Unknown` delegate to
/// [`plan_filtered_search`] unchanged (unknown already degrades to exact
/// there). Pass `metrics` to record stale fallbacks in existing diagnostics.
#[must_use]
pub fn plan_filtered_search_with_freshness(
    stats: &crate::stats::VectorStats,
    selectivity: f64,
    limit: u32,
    ann_available: bool,
    options: &VectorSearchOptions,
    freshness: crate::freshness::Freshness,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> FilterPlan {
    use crate::freshness::Freshness as F;
    match freshness {
        F::Fresh | F::Unknown => {
            plan_filtered_search(stats, selectivity, limit, ann_available, options)
        }
        F::Stale | F::Building | F::Invalid | F::Recovering => {
            if let Some(m) = metrics {
                m.observe_stale_fallback();
            }
            let dim = stats.dimension;
            let limit = limit.max(1);
            FilterPlan {
                strategy: FilterStrategy::ExactScan,
                executable: true,
                fallback: FilterStrategy::ExactScan,
                expected_rows: stats.expected_rows(selectivity),
                candidate_k: limit,
                rerank: false,
                cost_exact: estimate_cost(stats.live_rows, dim, SearchMode::Exact, options, limit),
                cost_ann: f64::INFINITY,
                reasons: vec![format!(
                    "freshness {}: stale statistics trigger conservative exact planning (never wrong)",
                    freshness.as_str()
                )],
            }
        }
    }
}

/// Freshness-aware radius planning (Gap J live path).
///
/// Same contract as [`plan_filtered_search_with_freshness`]: non-usable
/// freshness enumerates exactly; otherwise delegates unchanged.
#[must_use]
pub fn plan_radius_search_with_freshness(
    stats: &crate::stats::VectorStats,
    metric: VectorMetric,
    bound: f32,
    ann_available: bool,
    options: &VectorSearchOptions,
    freshness: crate::freshness::Freshness,
    metrics: Option<&crate::metrics::VectorMetrics>,
) -> RadiusPlan {
    use crate::freshness::Freshness as F;
    match freshness {
        F::Fresh | F::Unknown => plan_radius_search(stats, metric, bound, ann_available, options),
        F::Stale | F::Building | F::Invalid | F::Recovering => {
            if let Some(m) = metrics {
                m.observe_stale_fallback();
            }
            let live = stats.live_rows;
            RadiusPlan {
                strategy: RadiusStrategy::ExactEnumeration,
                bound,
                index_bound: index_radius_bound(metric, bound),
                candidate_well: live.min(u32::MAX as u64) as u32,
                rerank: false,
                cost_exact: estimate_cost(live, stats.dimension, SearchMode::Exact, options, 10),
                cost_ann: f64::INFINITY,
                reasons: vec![format!(
                    "freshness {}: conservative exact enumeration (never wrong)",
                    freshness.as_str()
                )],
            }
        }
    }
}

/// Full single-machine EXPLAIN: vector plan plus NUMA / scale-tier /
/// storage-policy / freshness lines (Phase 41 + Gaps G/L/J).
///
/// The first four lines are exactly [`explain_vector_plan`] (stable
/// contract); the trailing lines expose the single-node decisions. All
/// inputs are optional except the base plan: pass `None` for any subsystem
/// the caller did not consult.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn explain_vector_plan_full(
    mode: SearchMode,
    metric: VectorMetric,
    vector_type: &VectorType,
    options: &VectorSearchOptions,
    limit: u32,
    ann_available: bool,
    numa: Option<&crate::numa::NumaTopology>,
    scale: Option<&crate::scale::ScaleDecision>,
    storage: Option<&crate::storage::StoragePolicyDecision>,
    freshness: Option<crate::freshness::Freshness>,
) -> Vec<String> {
    let mut lines = explain_vector_plan(mode, metric, vector_type, options, limit, ann_available);
    if let Some(n) = numa {
        lines.push(format!(
            "NUMA: {} (nodes={}, intra-machine only)",
            n.mode().as_str(),
            n.nodes
        ));
    }
    if let Some(s) = scale {
        lines.push(format!(
            "Scale tier: {} (single-machine, overridden={})",
            s.tier.as_str(),
            s.overridden
        ));
    }
    if let Some(d) = storage {
        lines.push(format!(
            "Storage: {} (resident={}, mmap={}, fallback={}; {})",
            d.mode.as_str(),
            d.is_memory_resident(),
            d.is_mmap_backed(),
            d.fallback_active,
            d.reason
        ));
    }
    if let Some(f) = freshness {
        lines.push(format!("Freshness: {}", f.as_str()));
    }
    lines.push("Scope: single-node (no sharding, no distributed Top-K, no cluster)".to_string());
    lines
}
