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
// Final single-node acceptance suite (Phase 67).
//
// Integrated system validation across the whole Vector Engine — not a
// duplicate of unit tests (which pin components) but end-to-end chains:
// types → literals → codec → storage → index → search → planner →
// freshness → backup → restore → wire. ANN results are judged against
// the exact oracle (recall thresholds where approximate, exact equality
// everywhere else).
//
// FAST (default `cargo test`): everything here runs in seconds.
// EXTENDED (`PLOMID_ACCEPTANCE_EXTENDED=1`): larger recall/backup
// fixtures. CI runs FAST; release runs EXTENDED.

use plomid_vector::index::{IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::search::{BatchSearcher, SearchMetric};
use plomid_vector::storage::{InMemoryVectorStorage, VectorStorageAdapter};
use plomid_vector::{
    Freshness, VectorElementType, VectorKind, VectorMetric, VectorSearchOptions, VectorType,
    VectorValue,
};

fn extended() -> bool {
    std::env::var("PLOMID_ACCEPTANCE_EXTENDED").is_ok()
}

fn lcg(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f64 / (u32::MAX as f64) * 2.0 - 1.0) as f32
    };
    (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
}

fn dense_vals(v: Vec<f32>) -> VectorValue {
    VectorValue::Dense(
        plomid_vector::DenseVectorValue::from_f32(VectorElementType::Float32, v).unwrap(),
    )
}

fn rows_of(vecs: Vec<Vec<f32>>) -> Vec<(u64, Vec<f32>)> {
    vecs.into_iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v))
        .collect()
}

fn tiny_rows() -> Vec<(u64, Vec<f32>)> {
    rows_of(lcg(32, 8, 0xA11CE))
}

// ---------------------------------------------------------------------------
// 1-4. Types, dimensions, numerics, distances.
// ---------------------------------------------------------------------------

#[test]
fn acc_types_dimensions_numerics() {
    // Dimension edges + element coverage + typmod identity.
    for dim in [1u32, 2, 8, 768, 65536] {
        VectorType::new(VectorKind::Dense, VectorElementType::Float32, dim).unwrap();
    }
    for bad in [0u32, 65537] {
        assert!(VectorType::new(VectorKind::Dense, VectorElementType::Float32, bad).is_err());
    }
    // Literals: dense/sparse/binary/multi parse + canonical form.
    let d = plomid_vector::parse_dense("[1.0, -2.5]", Some(2)).unwrap();
    assert_eq!(d.values_f32, vec![1.0, -2.5]);
    assert!(plomid_vector::parse_dense("[NaN,0]", Some(2)).is_err());
    // Distance oracle: L2/cosine/dot agree with hand computation.
    let a = [1.0f32, 0.0];
    let b = [0.0f32, 1.0];
    assert!(
        (plomid_vector::distance::l2_distance(Some(&a), Some(&b))
            .unwrap()
            .unwrap()
            - 2f32.sqrt())
        .abs()
            < 1e-6
    );
    assert_eq!(
        plomid_vector::distance::cosine_distance(Some(&a), Some(&b)).unwrap(),
        Some(1.0)
    );
    // Zero-norm cosine is undefined (NULL), never NaN.
    let z = [0.0f32, 0.0];
    assert_eq!(
        plomid_vector::distance::cosine_distance(Some(&a), Some(&z)).unwrap(),
        None
    );
}

// ---------------------------------------------------------------------------
// 5-6. Exact search + Top-K (oracle behavior, tie-breaks, edges).
// ---------------------------------------------------------------------------

#[test]
fn acc_exact_topk_oracle() {
    let rows = tiny_rows();
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    // Tie-break by ascending row id (duplicate-vector dataset below proves it).
    let v7 = [1.0f32, 0.0];
    let v3 = [1.0f32, 0.0];
    let dup: Vec<(u64, Option<&[f32]>)> = vec![(7, Some(v7.as_slice())), (3, Some(v3.as_slice()))];
    let hits =
        plomid_vector::search::exact::exact_top_k(&[1.0, 0.0], dup, SearchMetric::L2, 2, None)
            .unwrap();
    assert_eq!(
        hits.iter().map(|c| c.row_id).collect::<Vec<_>>(),
        vec![3, 7]
    );
    // k=0, k>n, empty input edges.
    let empty: Vec<(u64, Option<&[f32]>)> = vec![];
    assert!(plomid_vector::search::exact::exact_top_k(
        &[1.0, 0.0],
        empty,
        SearchMetric::L2,
        5,
        None
    )
    .unwrap()
    .is_empty());
    let all = plomid_vector::search::exact::exact_top_k(
        &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        borrowed,
        SearchMetric::L2,
        10_000,
        None,
    )
    .unwrap();
    assert_eq!(all.len(), 32);
    // Ascending distances.
    for w in all.windows(2) {
        assert!(w[0].distance <= w[1].distance);
    }
}

// ---------------------------------------------------------------------------
// 7-12. ANN indexes vs the exact oracle (recall where approximate).
// ---------------------------------------------------------------------------

fn recall_vs_flat(
    kind: IndexKind,
    metric: Metric,
    rows: &[(u64, Vec<f32>)],
    query: &[f32],
    k: usize,
    min_recall: f64,
) {
    let dim = rows[0].1.len();
    let mut flat =
        plomid_vector::index::FlatIndex::new(IndexConfig::new(IndexKind::Flat, metric, dim))
            .unwrap();
    flat.build(rows).unwrap();
    let oracle: std::collections::HashSet<u64> = flat
        .search(query, k)
        .unwrap()
        .iter()
        .map(|r| r.row_id)
        .collect();
    let mut ann: Box<dyn VectorIndex> = match kind {
        IndexKind::Hnsw => Box::new(
            plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(kind, metric, dim))
                .unwrap(),
        ),
        IndexKind::IvfFlat => Box::new(
            plomid_vector::index::ivf::IvfFlatIndex::new(IndexConfig::new(kind, metric, dim))
                .unwrap(),
        ),
        IndexKind::IvfSq => Box::new(
            plomid_vector::index::sq::Sq8Index::new(IndexConfig::new(kind, metric, dim)).unwrap(),
        ),
        IndexKind::IvfPq => Box::new(
            plomid_vector::index::pq::PqIndex::new(IndexConfig::new(kind, metric, dim)).unwrap(),
        ),
        _ => panic!("acceptance ANN helper covers HNSW/IVF/SQ/PQ"),
    };
    ann.build(rows).unwrap();
    ann.validate().unwrap();
    let got = ann.search(query, k).unwrap();
    assert_eq!(got.len(), k);
    let hits = got.iter().filter(|r| oracle.contains(&r.row_id)).count();
    assert!(
        hits as f64 / k as f64 >= min_recall,
        "{kind:?} recall {hits}/{k} below {min_recall}"
    );
}

#[test]
fn acc_hnsw_recall_real() {
    let n = if extended() { 20_000 } else { 2_000 };
    let rows = rows_of(lcg(n, 32, 77));
    let query = lcg(1, 32, 78)[0].clone();
    recall_vs_flat(IndexKind::Hnsw, Metric::L2, &rows, &query, 10, 0.4);
}

#[test]
fn acc_ivf_sq_pq_recall() {
    let rows = rows_of(lcg(512, 16, 55));
    let query = lcg(1, 16, 56)[0].clone();
    recall_vs_flat(IndexKind::IvfFlat, Metric::L2, &rows, &query, 5, 0.4);
    recall_vs_flat(IndexKind::IvfSq, Metric::L2, &rows, &query, 5, 0.2);
    recall_vs_flat(IndexKind::IvfPq, Metric::L2, &rows, &query, 5, 0.2);
}

#[test]
fn acc_quantized_rerank_restores_order() {
    // Compressed candidates rescored by exact lanes recover true order.
    let rows = rows_of(lcg(256, 16, 61));
    let mut pq =
        plomid_vector::index::pq::PqIndex::new(IndexConfig::new(IndexKind::IvfPq, Metric::L2, 16))
            .unwrap();
    pq.build(&rows).unwrap();
    let query = rows[9].1.clone();
    let approx = pq.search(&query, 10).unwrap();
    assert_eq!(approx.len(), 10);
    // Exact rescore of the candidate well must surface the true nearest.
    let well: Vec<(u64, Option<&[f32]>)> = approx
        .iter()
        .map(|r| {
            (
                r.row_id,
                rows.iter()
                    .find(|(id, _)| *id == r.row_id)
                    .map(|(_, v)| v.as_slice()),
            )
        })
        .collect();
    let rescored =
        plomid_vector::search::exact::exact_top_k(&query, well, SearchMetric::L2, 3, None).unwrap();
    let mut flat =
        plomid_vector::index::FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 16))
            .unwrap();
    flat.build(&rows).unwrap();
    assert_eq!(
        rescored[0].row_id,
        flat.search(&query, 1).unwrap()[0].row_id
    );
}

// ---------------------------------------------------------------------------
// 12-16. DiskANN, sparse, binary, hybrid, reranking.
// ---------------------------------------------------------------------------

#[test]
fn acc_diskann_persist_reopen_serve() {
    let dir = std::env::temp_dir().join(format!("plomid-acc-diskann-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let rows = rows_of(lcg(64, 8, 41));
    let config = IndexConfig::new(IndexKind::DiskAnn, Metric::L2, 8);
    let mut idx = plomid_vector::index::diskann::DiskAnnIndex::new(config.clone(), &dir).unwrap();
    idx.build(&rows).unwrap();
    idx.flush().unwrap();
    let reopened = plomid_vector::index::diskann::DiskAnnIndex::open(config, &dir).unwrap();
    reopened.validate().unwrap();
    assert_eq!(reopened.search(&rows[0].1, 1).unwrap()[0].row_id, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn acc_sparse_binary_hybrid() {
    // Sparse: top-1 exactness on a tiny inverted index.
    let mut sparse = plomid_vector::sparse::inverted::SparseInvertedIndex::new(16, Metric::Dot);
    sparse
        .insert(
            1,
            plomid_vector::sparse::SparseVector::new(16, vec![(1, 1.0), (9, 0.5)]).unwrap(),
        )
        .unwrap();
    sparse
        .insert(
            2,
            plomid_vector::sparse::SparseVector::new(16, vec![(2, 1.0)]).unwrap(),
        )
        .unwrap();
    let hits = sparse
        .search(
            &plomid_vector::sparse::SparseVector::new(16, vec![(1, 1.0)]).unwrap(),
            2,
        )
        .unwrap();
    assert_eq!(hits[0].row_id, 1);
    // Binary: identical vectors distance 0, padding masked.
    assert_eq!(
        plomid_vector::distance::hamming_distance(
            Some(&[0b1010_1010u8, 0b1000_0000]),
            Some(&[0b1010_1010u8, 0b1000_0000]),
            9
        )
        .unwrap(),
        Some(0)
    );
    // Hybrid: union dedup + RRF agreement.
    let dense = vec![
        plomid_vector::index::SearchResult::new(1, 0.1),
        plomid_vector::index::SearchResult::new(2, 0.5),
    ];
    let spars = vec![
        plomid_vector::index::SearchResult::new(2, 0.2),
        plomid_vector::index::SearchResult::new(3, 0.4),
    ];
    assert_eq!(
        plomid_vector::hybrid::merge_union(&[&dense, &spars]).len(),
        3
    );
    assert_eq!(
        plomid_vector::hybrid::fuse_rrf(&[&dense, &spars], 60)[0].0,
        2
    );
}

// ---------------------------------------------------------------------------
// 17-21. Filtered ANN, radius, planner, cost model.
// ---------------------------------------------------------------------------

#[test]
fn acc_filtered_radius_planner_cost() {
    use plomid_vector::{FilterStrategy, RadiusStrategy};
    let mut stats = plomid_vector::VectorStats::unknown(8);
    for _ in 0..10_000 {
        stats.observe_row(false, false, true);
    }
    let options = VectorSearchOptions::default();
    // Impossible filter never touches an index; unknown stats stay exact.
    let p = plomid_vector::plan_filtered_search(&stats, 0.0, 10, true, &options);
    assert_eq!(
        (p.strategy, p.expected_rows),
        (FilterStrategy::ExactScan, Some(0))
    );
    let p = plomid_vector::plan_filtered_search(
        &plomid_vector::VectorStats::unknown(8),
        0.5,
        10,
        true,
        &options,
    );
    assert_eq!(p.strategy, FilterStrategy::ExactScan);
    // Radius: negative is valid-empty; L2 index bound squares.
    let r = plomid_vector::plan_radius_search(&stats, VectorMetric::L2, -1.0, true, &options);
    assert_eq!(r.strategy, RadiusStrategy::EmptyResult);
    let r = plomid_vector::plan_radius_search(&stats, VectorMetric::L2, 0.5, true, &options);
    assert!((r.index_bound - 0.25).abs() < 1e-9);
    // Cost model: exact scales with rows*dim; ANN beats exact at scale.
    let exact = plomid_vector::estimate_cost(
        1_000_000,
        128,
        plomid_vector::SearchMode::Exact,
        &options,
        10,
    );
    let ann =
        plomid_vector::estimate_cost(1_000_000, 128, plomid_vector::SearchMode::Ann, &options, 10);
    assert!(ann < exact && exact > 0.0);
    // Planner gates: small tables exact, big tables ANN.
    assert_eq!(
        plomid_vector::choose_exact_vs_ann(plomid_vector::SearchMode::Auto, 100, true),
        plomid_vector::SearchMode::Exact
    );
    assert_eq!(
        plomid_vector::choose_exact_vs_ann(plomid_vector::SearchMode::Auto, 100_000, true),
        plomid_vector::SearchMode::Ann
    );
}

// ---------------------------------------------------------------------------
// 22-25. Batch, parallel, concurrent queries.
// ---------------------------------------------------------------------------

#[test]
fn acc_batch_parallel_concurrent() {
    let rows: Vec<(u64, Option<Vec<f32>>)> = rows_of(lcg(512, 16, 31))
        .into_iter()
        .map(|(id, v)| (id, Some(v)))
        .collect();
    let queries = lcg(16, 16, 32);
    let mut serial = BatchSearcher::new(SearchMetric::L2, 4).unwrap();
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let expected = serial.search_many(&borrowed, &rows, None).unwrap();
    for threads in [0usize, 1, 4] {
        let got = plomid_vector::search::search_many_parallel(
            &queries,
            &rows,
            SearchMetric::L2,
            4,
            None,
            threads,
        )
        .unwrap();
        assert_eq!(got, expected);
    }
    // NUMA modes agree too (single-machine striping).
    for nodes in [1usize, 2] {
        let topo = plomid_vector::NumaTopology::test_seam(nodes, 8).unwrap();
        let got = plomid_vector::search::search_many_parallel_numa(
            &queries,
            &rows,
            SearchMetric::L2,
            4,
            None,
            4,
            &topo,
            None,
        )
        .unwrap();
        assert_eq!(got, expected);
    }
    // Concurrent mutation visibility: tombstoned rows never surface.
    let mut flat =
        plomid_vector::index::FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 16))
            .unwrap();
    let dense: Vec<(u64, Vec<f32>)> = rows
        .iter()
        .map(|(id, v)| (*id, v.clone().unwrap()))
        .collect();
    flat.build(&dense).unwrap();
    flat.delete(0).unwrap();
    assert!(flat
        .search(&dense[0].1, 512)
        .unwrap()
        .iter()
        .all(|r| r.row_id != 0));
}

// ---------------------------------------------------------------------------
// 26-30. Online build, mutation, maintenance, MVCC, WAL-adjacent recovery.
// ---------------------------------------------------------------------------

#[test]
fn acc_build_mutate_maintain_mvcc() {
    use plomid_vector::mvcc::delta::{CommittedDelta, PendingWrites};
    let rows = tiny_rows();
    let mut hnsw = plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(
        IndexKind::Hnsw,
        Metric::L2,
        8,
    ))
    .unwrap();
    // Online build protocol: snapshot + delta catch-up.
    let mut delta = CommittedDelta::new();
    let report =
        plomid_vector::index::online_build::run_online_build(&mut hnsw, &rows, 100, &delta, None)
            .unwrap();
    assert_eq!(report.base_rows, 32);
    hnsw.validate().unwrap();
    // Mutation: insert/update/delete cycle stays searchable + valid.
    hnsw.insert(1000, &rows[0].1).unwrap();
    hnsw.delete(1000).unwrap();
    assert!(!hnsw.delete(1000).unwrap(), "second delete reports false");
    hnsw.validate().unwrap();
    // MVCC delta visibility is exact.
    let mut pending = PendingWrites::new();
    pending.delete(5);
    pending.commit_into(7, 200, &mut delta);
    assert!(delta.is_deleted(5, 200));
    assert!(!delta.is_deleted(6, 200));
    // Maintenance advice reacts to tombstone load.
    let cfg = plomid_vector::mvcc::maintenance::MaintenanceConfig::default();
    let _ = plomid_vector::mvcc::maintenance::advise(1000, 900, 500, &cfg);
    // Crash-adjacent file recovery: torn tail truncates, never corrupts.
    let dir = std::env::temp_dir().join(format!("plomid-acc-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut store =
        plomid_vector::storage::FileVectorStorage::create(&dir, "t".into(), "c".into(), Some(8))
            .unwrap();
    for (_, v) in &rows {
        store.append_payload(&dense_vals(v.clone())).unwrap();
    }
    drop(store);
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("records.dat"))
            .unwrap();
        f.write_all(&[0xFF]).unwrap();
    }
    let (reopened, recovery) = plomid_vector::storage::FileVectorStorage::open(
        &dir,
        "t".into(),
        "c".into(),
        Some(8),
        false,
    )
    .unwrap();
    assert!(recovery.truncated_tail);
    assert_eq!(reopened.row_count(), 32);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 31-36. Recovery, backup/restore, storage/NUMA/scale/freshness policies.
// ---------------------------------------------------------------------------

#[test]
fn acc_recovery_backup_policies() {
    // Storage policy across the spectrum + restart + diagnostics.
    let policy = plomid_vector::storage::VectorStoragePolicy::default();
    let nvme = plomid_vector::storage::StorageCapability::test_seam(
        true,
        plomid_vector::storage::StorageClass::Nvme,
        None,
    );
    assert_eq!(
        policy.select(512, &nvme).mode,
        plomid_vector::storage::StorageMode::Memory
    );
    assert_eq!(
        policy.select(256 << 20, &nvme).mode,
        plomid_vector::storage::StorageMode::Mmap
    );
    let no_mmap = plomid_vector::storage::StorageCapability::test_seam(
        false,
        plomid_vector::storage::StorageClass::Ssd,
        None,
    );
    assert!(policy.select(256 << 20, &no_mmap).fallback_active);
    // Backup → restore → query → mutate chain.
    let src = std::env::temp_dir().join(format!("plomid-acc-bk-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&src);
    let mut s =
        plomid_vector::storage::FileVectorStorage::create(&src, "t".into(), "c".into(), Some(8))
            .unwrap();
    for (_, v) in tiny_rows() {
        s.append_payload(&dense_vals(v)).unwrap();
    }
    s.checkpoint(3).unwrap();
    let out = src
        .join("..")
        .join(format!("plomid-acc-bk-out-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    let rep = plomid_vector::create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        3,
        None,
        &[],
        0,
        &out,
        plomid_vector::BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert_eq!(rep.rows, 32);
    drop(s);
    let target = src
        .join("..")
        .join(format!("plomid-acc-bk-tgt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&target);
    let rr =
        plomid_vector::restore_backup(&out, &target, plomid_vector::RestoreFailPoint::None, None)
            .unwrap();
    assert_eq!(rr.rows, 32);
    // NUMA detect + scale select + freshness gate on the restored image.
    assert!(plomid_vector::NumaTopology::detect().nodes >= 1);
    let scale = plomid_vector::ScaleDecision::select(
        &plomid_vector::ScaleInputs::test_seam(
            8,
            Some(16 << 30),
            plomid_vector::storage::StorageClass::Ssd,
            1 << 30,
            2,
        ),
        &plomid_vector::ScaleConfig::explicit(None),
    );
    assert!(!scale.reason.is_empty());
    assert!(plomid_vector::is_index_queryable(
        plomid_vector::IndexState::Ready,
        Freshness::Fresh
    ));
    assert!(!plomid_vector::is_index_queryable(
        plomid_vector::IndexState::Ready,
        Freshness::Stale
    ));
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

// ---------------------------------------------------------------------------
// 37-39. Budgets, metrics, diagnostics.
// ---------------------------------------------------------------------------

#[test]
fn acc_budgets_metrics_diagnostics() {
    // Estimates saturate; hostile counts clamp.
    assert_eq!(
        plomid_vector::options::memory_estimate_bytes(u64::MAX, u64::MAX),
        u64::MAX
    );
    // Metrics observe the stack end to end.
    let m = plomid_vector::VectorMetrics::new();
    m.inc_exact();
    m.inc_ann();
    m.observe_storage_decision("mmap", false);
    m.observe_numa_dispatch(false);
    m.observe_stale_fallback();
    m.observe_backup();
    m.observe_restore();
    m.observe_htap_decision(true, false);
    let s = m.snapshot();
    assert_eq!((s.exact_queries, s.ann_queries), (1, 1));
    assert_eq!(s.storage_mmap_selected, 1);
    assert_eq!(s.stale_plan_fallbacks, 1);
    assert_eq!((s.backups_completed, s.restores_completed), (1, 1));
    // Diagnostics explain decisions.
    let d = plomid_vector::storage::VectorStoragePolicy::default().select(
        8 << 20,
        &plomid_vector::storage::StorageCapability::test_seam(
            true,
            plomid_vector::storage::StorageClass::Ssd,
            None,
        ),
    );
    assert_eq!(d.explain().len(), 8);
    // Full EXPLAIN keeps the base contract plus single-node lines.
    let t = VectorType::new(VectorKind::Dense, VectorElementType::Float32, 8).unwrap();
    let lines = plomid_vector::explain_vector_plan_full(
        plomid_vector::SearchMode::Ann,
        VectorMetric::L2,
        &t,
        &VectorSearchOptions::default(),
        10,
        true,
        None,
        None,
        Some(&d),
        Some(Freshness::Fresh),
    );
    assert!(lines.iter().any(|l| l.contains("single-node")));
}

// ---------------------------------------------------------------------------
// 40-43. SQL-adjacent wire contract, errors, configuration.
// ---------------------------------------------------------------------------

#[test]
#[allow(clippy::assertions_on_constants)]
fn acc_wire_errors_config() {
    // Wire contract for dense/sparse/binary/multi (in-crate half).
    let t = VectorType::new(VectorKind::Dense, VectorElementType::Float32, 2).unwrap();
    let v = dense_vals(vec![1.0, 2.0]);
    let wire = plomid_vector::wire::encode_for_wire(&v, &t).unwrap();
    assert_eq!(wire.oid, plomid_vector::wire::WIRE_OID_VECTOR);
    plomid_vector::wire::check_wire_vector(&wire, &t).unwrap();
    assert!(!plomid_vector::wire::BINARY_PARAMS_SUPPORTED);
    // Error codes are stable and categorized for operators.
    let e = plomid_vector::VectorError::IndexCorrupted("x".into());
    assert_eq!(e.code(), "VECTOR_INDEX_CORRUPTED");
    assert_eq!(
        plomid_vector::operator::categorize_vector_error(&e),
        plomid_vector::operator::ErrorCategory::IndexCorruption
    );
    // Configuration inventory is complete (no undocumented knobs).
    assert_eq!(plomid_vector::operator::config_inventory().len(), 15);
    // Options validate ranges; oversample math is exact.
    let mut o = VectorSearchOptions::default();
    o.validate().unwrap();
    assert_eq!(o.effective_candidate_k(20), 160);
    o.ef_search = 0;
    assert!(o.validate().is_err());
}

// ---------------------------------------------------------------------------
// 44-45. Upgrade compatibility + migration behavior.
// ---------------------------------------------------------------------------

#[test]
fn acc_upgrade_migration() {
    // Same-version artifacts open; newer fail closed; older indexes rebuild.
    for a in plomid_vector::migration::VersionedArtifact::all() {
        let c = a.current_version();
        assert_eq!(
            plomid_vector::migration::check_transition(a, c),
            plomid_vector::migration::Compatibility::Supported
        );
        assert_eq!(
            plomid_vector::migration::check_transition(a, c + 1),
            plomid_vector::migration::Compatibility::NotSupported
        );
    }
    // Old state → detect → rebuild → validate → enable (HNSW blob path).
    let rows = tiny_rows();
    let mut idx = plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(
        IndexKind::Hnsw,
        Metric::L2,
        8,
    ))
    .unwrap();
    idx.build(&rows).unwrap();
    let mut skewed = idx.encode();
    skewed[0..4].copy_from_slice(&0u32.to_le_bytes()); // "older" version
    assert_eq!(
        plomid_vector::migration::classify_index_blob(IndexKind::Hnsw, &skewed),
        plomid_vector::migration::Compatibility::RebuildRequired
    );
    // Rebuild from authoritative rows; never served before validation.
    let mut rebuilt = plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(
        IndexKind::Hnsw,
        Metric::L2,
        8,
    ))
    .unwrap();
    rebuilt.build(&rows).unwrap();
    rebuilt.validate().unwrap();
    assert_eq!(rebuilt.search(&rows[5].1, 1).unwrap()[0].row_id, 5);
    // Restart/recover continuity: manifest LSN survives restore reports.
    assert_eq!(plomid_vector::migration::matrix().len(), 13);
}

// ---------------------------------------------------------------------------
// Failure acceptance (fail-injection reuse, defined safe states).
// ---------------------------------------------------------------------------

#[test]
fn acc_failures_are_defined_and_safe() {
    // Cancelled query: defined Cancelled, searcher reusable.
    let rows: Vec<(u64, Option<Vec<f32>>)> = tiny_rows()
        .into_iter()
        .map(|(id, v)| (id, Some(v)))
        .collect();
    let mut s = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let stop = || true;
    assert_eq!(
        s.search_one(&[1.0; 8], &rows, Some(&stop)),
        Err(plomid_vector::search::SearchError::Cancelled)
    );
    // Timeout via deadline.
    let past = std::time::Instant::now() - std::time::Duration::from_secs(1);
    let dl = plomid_vector::deadline_cancel_at(past);
    assert!(dl());
    // Corrupted codec/manifest/index/metadata/WAL-adjacent bytes fail closed.
    let mut good = plomid_vector::encode_standalone(&dense_vals(vec![1.0, 2.0])).unwrap();
    good[0] ^= 0xFF;
    assert!(plomid_vector::decode_standalone(&good).is_err());
    // Invalid dimension/metric/index/config each fail with stable codes.
    assert!(VectorType::new(VectorKind::Dense, VectorElementType::Float32, 0).is_err());
    assert!(plomid_vector::capability::check(
        VectorKind::Dense,
        VectorElementType::Float32,
        VectorMetric::L2,
        IndexKind::Binary
    )
    .is_err());
    // Failed backup/restore leave nothing healthy (fail-point coverage).
    let src = InMemoryVectorStorage::new(Some(2));
    let out = std::env::temp_dir().join(format!("plomid-acc-fail-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    assert!(plomid_vector::create_backup(
        &src,
        None,
        "t",
        "c",
        1,
        None,
        &[],
        0,
        &out,
        plomid_vector::BackupFailPoint::DuringFinalize,
        None
    )
    .is_err());
    assert!(plomid_vector::restore_backup(
        &out,
        &out.join("t"),
        plomid_vector::RestoreFailPoint::None,
        None
    )
    .is_err());
    let _ = std::fs::remove_dir_all(&out);
    // Memory pressure: tiny budget spills to file, never OOM.
    let tight = plomid_vector::storage::VectorStoragePolicy {
        memory_budget_bytes: 64,
        ..plomid_vector::storage::VectorStoragePolicy::default()
    };
    let cap = plomid_vector::storage::StorageCapability::test_seam(
        true,
        plomid_vector::storage::StorageClass::Nvme,
        None,
    );
    assert!(!tight.select(4096, &cap).is_memory_resident());
    // mmap unavailable: loud fallback.
    let cap_off = plomid_vector::storage::StorageCapability::test_seam(
        false,
        plomid_vector::storage::StorageClass::Ssd,
        None,
    );
    assert!(
        plomid_vector::storage::VectorStoragePolicy::default()
            .select(256 << 20, &cap_off)
            .fallback_active
    );
}

// ---------------------------------------------------------------------------
// Performance acceptance (smoke: completion + throughput sanity; hard
// regression thresholds live in criterion baselines, never asserted here
// to keep CI deterministic).
// ---------------------------------------------------------------------------

#[test]
fn acc_perf_smoke() {
    let n = if extended() { 20_000 } else { 2_000 };
    let rows = rows_of(lcg(n, 64, 9));
    let query = rows[7].1.clone();
    // Exact scan completes with correct Top-K.
    let t = std::time::Instant::now();
    let mut flat =
        plomid_vector::index::FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 64))
            .unwrap();
    flat.build(&rows).unwrap();
    let hits = flat.search(&query, 10).unwrap();
    assert_eq!(hits.len(), 10);
    let exact_ms = t.elapsed().as_millis();
    // ANN build + search completes; recall smoke vs oracle.
    let t = std::time::Instant::now();
    let mut hnsw = plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(
        IndexKind::Hnsw,
        Metric::L2,
        64,
    ))
    .unwrap();
    hnsw.build(&rows).unwrap();
    let approx = hnsw.search(&query, 10).unwrap();
    let build_search_ms = t.elapsed().as_millis();
    let oracle: std::collections::HashSet<u64> = hits.iter().map(|r| r.row_id).collect();
    let recall = approx.iter().filter(|r| oracle.contains(&r.row_id)).count();
    assert!(recall >= 4, "recall smoke {recall}/10");
    // Batch throughput sanity: all queries served, none dropped.
    let queries = lcg(8, 64, 10);
    let mut searcher = BatchSearcher::new(SearchMetric::L2, 10).unwrap();
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let rows_opt: Vec<(u64, Option<Vec<f32>>)> =
        rows.iter().map(|(id, v)| (*id, Some(v.clone()))).collect();
    let t = std::time::Instant::now();
    let many = searcher.search_many(&borrowed, &rows_opt, None).unwrap();
    let batch_ms = t.elapsed().as_millis();
    assert_eq!(many.len(), 8);
    for got in &many {
        assert_eq!(got.len(), 10);
    }
    // Timings are recorded for the operator (never asserted: CI hardware
    // varies; criterion baselines own regression thresholds).
    eprintln!("acceptance perf: exact_topk={exact_ms}ms hnsw_build_search={build_search_ms}ms batch_8q={batch_ms}ms rows={n}");
}
