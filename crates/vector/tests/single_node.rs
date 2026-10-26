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
// Single-node policy integration: Phase 41 + Gaps G/L/J wired together.
// Actual behavior only; no multi-node constructs anywhere in this file.

use plomid_vector::search::{search_many_parallel_numa, BatchSearcher, SearchMetric};
use plomid_vector::storage::{
    ManagedVectorStorage, StorageCapability, StorageClass, StorageModeRequest,
    VectorStorageAdapter, VectorStoragePolicy,
};
use plomid_vector::{
    Freshness, FreshnessCause, FreshnessTracker, NumaTopology, ScaleConfig, ScaleDecision,
    ScaleInputs, VectorMetrics, VectorSearchOptions,
};

fn dense(values: Vec<f32>) -> plomid_vector::VectorValue {
    plomid_vector::VectorValue::Dense(
        plomid_vector::DenseVectorValue::from_f32(
            plomid_vector::VectorElementType::Float32,
            values,
        )
        .unwrap(),
    )
}

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "plomid-vector-single-node-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.subsec_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn single_node_stack_end_to_end() {
    let metrics = VectorMetrics::new();
    // Detection never fails on any machine (laptop, server, ARM, x86).
    let capability = StorageCapability::detect();
    let topology = NumaTopology::detect();
    assert!(topology.nodes >= 1);
    let inputs = ScaleInputs::detect(8 << 20, 2);
    let scale = ScaleDecision::select(&inputs, &ScaleConfig::explicit(None));
    assert!(!scale.reason.is_empty());

    // Storage: tier-derived policy drives a managed file store.
    let d = dir("stack");
    let policy = scale.storage_policy();
    let mut store = ManagedVectorStorage::create(
        &d,
        "public.docs".into(),
        "embedding".into(),
        Some(2),
        &policy,
        8 << 20,
        &capability,
        Some(&metrics),
    )
    .unwrap();
    for i in 0..8u64 {
        store
            .append_payload(&dense(vec![i as f32, i as f32 + 0.5]))
            .unwrap();
    }
    store.checkpoint(11).unwrap();
    assert_eq!(store.prefetch(0, 8).unwrap(), 8);
    assert!(store.is_warm());
    let decision = store.close().unwrap();
    assert!(!decision.reason.is_empty());
    // Diagnostics answer the Phase 41 questions.
    let lines = decision.explain();
    assert_eq!(lines.len(), 8);

    // Reopen cold, verify rows, warm via prefetch.
    let (mut reopened, recovery) = ManagedVectorStorage::open(
        &d,
        "public.docs".into(),
        "embedding".into(),
        Some(2),
        &policy,
        8 << 20,
        &capability,
        Some(&metrics),
    )
    .unwrap();
    assert_eq!(recovery.replayed_records, 8);
    assert!(!reopened.is_warm());
    assert_eq!(reopened.read_payload(3).unwrap(), dense(vec![3.0, 3.5]));
    reopened.prefetch(0, 8).unwrap();
    assert!(reopened.is_warm());

    // NUMA: parallel agrees with serial on the stored rows.
    let rows: Vec<(u64, Option<Vec<f32>>)> = (0..8u64)
        .map(|i| (i, Some(vec![i as f32, i as f32 + 0.5])))
        .collect();
    let queries = vec![vec![1.0f32, 1.5], vec![7.0, 7.5]];
    let mut serial = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let expected = serial.search_many(&borrowed, &rows, None).unwrap();
    let numa = NumaTopology::test_seam(2, 16).unwrap();
    let got = search_many_parallel_numa(
        &queries,
        &rows,
        SearchMetric::L2,
        2,
        None,
        0,
        &numa,
        Some(&metrics),
    )
    .unwrap();
    assert_eq!(got, expected);

    // Freshness: write invalidates, planner goes conservative, refresh heals.
    let mut tracker = FreshnessTracker::unknown();
    tracker.refresh(11);
    tracker.invalidate(FreshnessCause::Insert, "appended row");
    assert_eq!(tracker.state(), Freshness::Stale);
    let mut stats = plomid_vector::VectorStats::unknown(2);
    for _ in 0..5000 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 10; // behind the store watermark: stale
    let options = VectorSearchOptions::default();
    let plan = plomid_vector::plan_filtered_search_with_freshness(
        &stats,
        0.5,
        5,
        true,
        &options,
        plomid_vector::check_stats_freshness(&stats, 11),
        Some(&metrics),
    );
    assert_eq!(
        plan.strategy,
        plomid_vector::FilterStrategy::ExactScan,
        "stale stats must plan conservatively"
    );

    // Full EXPLAIN carries every single-node decision.
    let t = plomid_vector::VectorType::new(
        plomid_vector::VectorKind::Dense,
        plomid_vector::VectorElementType::Float32,
        2,
    )
    .unwrap();
    let lines = plomid_vector::explain_vector_plan_full(
        plomid_vector::SearchMode::Auto,
        plomid_vector::VectorMetric::L2,
        &t,
        &options,
        5,
        true,
        Some(&topology),
        Some(&scale),
        Some(&decision),
        Some(Freshness::Stale),
    );
    assert!(lines.iter().any(|l| l.starts_with("NUMA:")));
    assert!(lines.iter().any(|l| l.starts_with("Scale tier:")));
    assert!(lines.iter().any(|l| l.starts_with("Storage:")));
    assert!(lines.iter().any(|l| l.starts_with("Freshness:")));
    assert!(lines.iter().any(|l| l.contains("single-node")));

    // Metrics observed the whole stack through existing diagnostics.
    let snap = metrics.snapshot();
    assert!(
        snap.storage_memory_selected + snap.storage_file_selected + snap.storage_mmap_selected >= 2
    );
    assert!(snap.numa_single_node + snap.numa_multi_node >= 1);
    assert!(snap.stale_plan_fallbacks >= 1);

    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn explicit_configuration_overrides_everything() {
    use plomid_vector::storage::MmapConfig;
    use plomid_vector::storage::StorageMode;
    use plomid_vector::ScaleTier;
    // Tier override.
    let inputs = ScaleInputs::test_seam(2, Some(4 << 30), StorageClass::Hdd, 1 << 20, 1);
    let pinned = ScaleDecision::select(
        &inputs,
        &ScaleConfig::explicit(Some(ScaleTier::ProductionServer)),
    );
    assert_eq!(pinned.tier, ScaleTier::ProductionServer);
    // Storage request override: explicit mmap on capable NVMe maps.
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::Mmap,
        mmap: MmapConfig::default(),
        memory_budget_bytes: 256 << 20,
    };
    let cap = StorageCapability::test_seam(true, StorageClass::Nvme, None);
    let d = policy.select(256 << 20, &cap);
    assert_eq!(d.mode, StorageMode::Mmap);
    assert!(!d.fallback_active);
    // Same request where mmap is unavailable falls back loudly.
    let no_mmap = StorageCapability::test_seam(false, StorageClass::Ssd, None);
    let f = policy.select(256 << 20, &no_mmap);
    assert_eq!(f.mode, StorageMode::File);
    assert!(f.fallback_active);
}
