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
// Moved from `src/htap.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::htap::*;

#[test]
fn pressure_boundaries_match_memory_thresholds() {
    assert_eq!(PressureLevel::from_usage(0.0), PressureLevel::Normal);
    assert_eq!(PressureLevel::from_usage(0.699), PressureLevel::Normal);
    assert_eq!(PressureLevel::from_usage(0.70), PressureLevel::Elevated);
    assert_eq!(PressureLevel::from_usage(0.849), PressureLevel::Elevated);
    assert_eq!(PressureLevel::from_usage(0.85), PressureLevel::High);
    assert_eq!(PressureLevel::from_usage(0.949), PressureLevel::High);
    assert_eq!(PressureLevel::from_usage(0.95), PressureLevel::Critical);
    assert_eq!(PressureLevel::from_usage(4.0), PressureLevel::Critical);
    // Fail-safe float handling mirrors the memory crate.
    assert_eq!(PressureLevel::from_usage(f64::NAN), PressureLevel::Normal);
    assert_eq!(
        PressureLevel::from_usage(f64::INFINITY),
        PressureLevel::Critical
    );
}

#[test]
fn admission_matrix() {
    // Normal load admits.
    assert!(admit(1024, 0, 1 << 20, PressureLevel::Normal, 0, 2, false).may_proceed());
    // Query over budget is a defined rejection, never growth.
    let r = admit(2 << 20, 0, 1 << 20, PressureLevel::Normal, 0, 2, false);
    assert!(!r.may_proceed());
    assert!(matches!(r, VectorAdmission::Reject { .. }));
    // Build over budget rejected too.
    assert!(!admit(512, 2 << 20, 1 << 20, PressureLevel::Normal, 0, 2, false).may_proceed());
    // Saturated build single-flight degrades to exact (still correct).
    let r = admit(512, 512, 1 << 20, PressureLevel::Normal, 2, 2, false);
    assert!(r.may_proceed());
    assert!(matches!(r, VectorAdmission::DegradeExact { .. }));
    // High/critical pressure degrades queries to exact.
    for p in [PressureLevel::High, PressureLevel::Critical] {
        let r = admit(512, 0, 1 << 20, p, 0, 2, false);
        assert!(matches!(r, VectorAdmission::DegradeExact { .. }), "{p:?}");
    }
    // Critical pauses new builds.
    let r = admit(512, 512, 1 << 20, PressureLevel::Critical, 0, 2, false);
    assert!(matches!(r, VectorAdmission::DegradeExact { .. }));
    // Benchmark override admits everything except over-budget.
    assert!(admit(512, 0, 1 << 20, PressureLevel::Critical, 0, 2, true).may_proceed());
    assert!(!admit(2 << 20, 0, 1 << 20, PressureLevel::Normal, 0, 2, true).may_proceed());
}

#[test]
fn worker_caps_derate_with_pressure() {
    assert_eq!(worker_cap(8, usize::MAX, PressureLevel::Normal), 8);
    assert_eq!(worker_cap(8, usize::MAX, PressureLevel::Elevated), 4);
    assert_eq!(worker_cap(8, usize::MAX, PressureLevel::High), 2);
    assert_eq!(worker_cap(8, usize::MAX, PressureLevel::Critical), 2);
    assert_eq!(worker_cap(1, usize::MAX, PressureLevel::Critical), 1);
    // Explicit grant below the tier wins.
    assert_eq!(worker_cap(32, 2, PressureLevel::Normal), 2);
}

#[test]
fn throttle_mapping() {
    assert_eq!(
        Throttle::for_pressure(PressureLevel::Normal),
        Throttle::Full
    );
    assert_eq!(
        Throttle::for_pressure(PressureLevel::Elevated),
        Throttle::Full
    );
    assert_eq!(
        Throttle::for_pressure(PressureLevel::High),
        Throttle::Degraded
    );
    assert_eq!(
        Throttle::for_pressure(PressureLevel::Critical),
        Throttle::Paused
    );
}

#[test]
fn mixed_workload_stays_correct_under_flood() {
    use plomid_vector::search::{BatchSearcher, SearchMetric};
    use std::sync::{Arc, Mutex};
    // Foreground: exact top-k over a fixed row set. Background: appends
    // to a disjoint store (CPU/cache interference, no data race by
    // construction — measures single-machine contention honestly).
    let rows: Vec<(u64, Option<Vec<f32>>)> = (0..256u64)
        .map(|i| (i, Some(vec![i as f32, (i % 16) as f32])))
        .collect();
    let oracle = {
        let mut s = BatchSearcher::new(SearchMetric::L2, 4).unwrap();
        let q: &[f32] = &[7.0, 7.0];
        s.search_one(q, &rows, None).unwrap()
    };
    let sink = Arc::new(Mutex::new(Vec::<u64>::new()));
    let report = run_mixed(
        32,
        512,
        |_| {
            let mut s = BatchSearcher::new(SearchMetric::L2, 4).unwrap();
            let q: &[f32] = &[7.0, 7.0];
            let got = s.search_one(q, &rows, None).unwrap();
            assert_eq!(got, oracle, "flood must not corrupt results");
        },
        |i| {
            sink.lock().unwrap().push(i as u64);
        },
    );
    assert_eq!(report.foreground.iters, 32);
    assert_eq!(report.background_ops, 512);
    assert!(report.foreground.p50_ns > 0);
    assert!(report.foreground.p95_ns >= report.foreground.p50_ns);
    assert!(report.foreground.p99_ns >= report.foreground.p95_ns);
    assert_eq!(sink.lock().unwrap().len(), 512);
}

#[test]
fn cancelled_search_releases_resources() {
    use plomid_vector::search::{BatchSearcher, SearchMetric};
    let rows: Vec<(u64, Option<Vec<f32>>)> = (0..512u64)
        .map(|i| (i, Some(vec![i as f32, 0.0])))
        .collect();
    let mut s = BatchSearcher::new(SearchMetric::L2, 4).unwrap();
    // Cancel immediately: defined Cancelled, heap allocation retained.
    let stop = || true;
    assert_eq!(
        s.search_one(&[1.0, 0.0], &rows, Some(&stop)),
        Err(plomid_vector::search::SearchError::Cancelled)
    );
    // Same searcher serves correctly after cancel (resources released).
    let out = s.search_one(&[1.0, 0.0], &rows, None).unwrap();
    assert_eq!(out[0].row_id, 1);
}

#[test]
fn timed_out_query_aborts_without_corruption() {
    use plomid_vector::search::exact::exact_top_k;
    use plomid_vector::search::SearchMetric;
    let owned: Vec<(u64, Option<Vec<f32>>)> = (0..256u64)
        .map(|i| (i, Some(vec![i as f32, 0.0])))
        .collect();
    let borrowed: Vec<(u64, Option<&[f32]>)> =
        owned.iter().map(|(id, v)| (*id, v.as_deref())).collect();
    // Already-expired deadline: aborts on the first cancel check.
    let past = std::time::Instant::now() - std::time::Duration::from_secs(1);
    let cancel = deadline_cancel_at(past);
    assert_eq!(
        exact_top_k(
            &[1.0, 0.0],
            borrowed.iter().copied(),
            SearchMetric::L2,
            4,
            Some(&cancel)
        ),
        Err(plomid_vector::search::SearchError::Cancelled)
    );
    // Live deadline far out: completes normally.
    let future = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let ok = deadline_cancel_at(future);
    let out = exact_top_k(
        &[1.0, 0.0],
        borrowed.iter().copied(),
        SearchMetric::L2,
        4,
        Some(&ok),
    )
    .unwrap();
    assert_eq!(out[0].row_id, 1);
}

#[test]
fn cancelled_build_never_publishes() {
    use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex as _};
    use plomid_vector::mvcc::delta::CommittedDelta;
    let rows: Vec<(u64, Vec<f32>)> = (0..512u64).map(|i| (i, vec![i as f32, 0.0])).collect();
    let mut idx = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2)).unwrap();
    let delta = CommittedDelta::new();
    let stop = || true;
    let before = idx.stats();
    let err = plomid_vector::index::online_build::run_online_build(
        &mut idx,
        &rows,
        10,
        &delta,
        Some(&stop),
    );
    assert!(err.is_err(), "cancelled build must fail, not publish");
    // State untouched by the aborted build.
    assert_eq!(idx.stats(), before);
}

#[test]
fn degraded_exact_matches_oracle() {
    use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex as _};
    use plomid_vector::search::exact::exact_top_k;
    use plomid_vector::search::SearchMetric;
    // Degraded (stale) planning serves exact: prove it equals the oracle.
    let rows: Vec<(u64, Vec<f32>)> = (0..128u64)
        .map(|i| (i, vec![i as f32 * 0.5, 1.0]))
        .collect();
    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2)).unwrap();
    flat.build(&rows).unwrap();
    let query = vec![10.0f32, 1.0];
    let oracle = flat.search(&query, 5).unwrap();
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let degraded = exact_top_k(&query, borrowed, SearchMetric::L2, 5, None).unwrap();
    assert_eq!(degraded.len(), oracle.len());
    for (a, b) in degraded.iter().zip(oracle.iter()) {
        assert_eq!(a.row_id, b.row_id);
    }
}

#[test]
fn percentile_math() {
    assert_eq!(percentile_ns(&[], 0.5), 0);
    let s = vec![10u64, 20, 30, 40];
    assert_eq!(percentile_ns(&s, 0.0), 10);
    assert_eq!(percentile_ns(&s, 0.5), 30);
    assert_eq!(percentile_ns(&s, 1.0), 40);
}
