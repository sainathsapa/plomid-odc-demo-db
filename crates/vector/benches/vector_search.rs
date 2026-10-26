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
// Recall/latency smoke benches for `plomid-vector` (Phase 53-57 in code).
//
// `exact_top_k_*` pins the correctness baseline cost; `hnsw_*` measures the
// ANN path on the same deterministic fixture. Recall itself is asserted in
// `tests/property.rs` (`hnsw_recall_smoke_vs_flat_oracle`); these benches
// record latency/QPS only.
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use plomid_vector::distance::{cosine_distance, l2_distance};
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::search::exact::exact_top_k;
use plomid_vector::search::SearchMetric;
use plomid_vector::{VectorMetric, VectorSearchOptions, VectorStats};

fn lcg_vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut state = seed;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f64 / (u32::MAX as f64) * 2.0 - 1.0) as f32
    };
    (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
}

fn bench_distance(c: &mut Criterion) {
    let dim = 1536;
    let a = lcg_vecs(1, dim, 1)[0].clone();
    let b = lcg_vecs(1, dim, 2)[0].clone();
    let mut group = c.benchmark_group("distance_1536");
    group.throughput(Throughput::Elements(1));
    group.bench_function("l2", |f| {
        f.iter(|| l2_distance(Some(black_box(&a)), Some(black_box(&b))).unwrap())
    });
    group.bench_function("cosine", |f| {
        f.iter(|| cosine_distance(Some(black_box(&a)), Some(black_box(&b))).unwrap())
    });
    group.finish();
}

fn bench_exact_topk(c: &mut Criterion) {
    // 10K rows x 128 dims; measures the exact-scan baseline bandwidth.
    let (n, dim, k) = (10_000usize, 128usize, 10usize);
    let vecs = lcg_vecs(n, dim, 7);
    let rows: Vec<(u64, Vec<f32>)> = vecs
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v))
        .collect();
    let query = lcg_vecs(1, dim, 8)[0].clone();
    let borrowed: Vec<(u64, Option<&[f32]>)> = rows
        .iter()
        .map(|(id, v)| (*id, Some(v.as_slice())))
        .collect();
    let mut group = c.benchmark_group("exact_topk_10k_x_128");
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("l2_k10", |f| {
        f.iter(|| {
            exact_top_k(
                black_box(&query),
                borrowed.iter().copied(),
                SearchMetric::L2,
                black_box(k),
                None,
            )
            .unwrap()
        })
    });
    group.finish();
}

fn bench_hnsw(c: &mut Criterion) {
    // 5K rows x 128 dims; same fixture for Flat (oracle) and HNSW.
    let (n, dim, k) = (5_000usize, 128usize, 10usize);
    let rows: Vec<(u64, Vec<f32>)> = lcg_vecs(n, dim, 42)
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v))
        .collect();
    let query = lcg_vecs(1, dim, 43)[0].clone();

    let mut flat = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, dim)).unwrap();
    flat.build(&rows).unwrap();
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, dim)).unwrap();
    hnsw.build(&rows).unwrap();

    let mut group = c.benchmark_group("ann_5k_x_128");
    group.throughput(Throughput::Elements(k as u64));
    group.bench_function("flat_top10", |f| {
        f.iter(|| flat.search(black_box(&query), black_box(k)).unwrap())
    });
    group.bench_function("hnsw_top10", |f| {
        f.iter(|| hnsw.search(black_box(&query), black_box(k)).unwrap())
    });
    group.finish();
}

fn bench_planner(c: &mut Criterion) {
    // Planning overhead must stay noise next to execution (µs vs ms).
    let mut stats = VectorStats::unknown(1536);
    for _ in 0..1_000_000 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 9;
    let options = VectorSearchOptions::default();
    let mut group = c.benchmark_group("planner");
    group.bench_function("filter_plan", |f| {
        f.iter(|| {
            plomid_vector::plan_filtered_search(
                black_box(&stats),
                black_box(0.1),
                black_box(10),
                black_box(true),
                black_box(&options),
            )
        })
    });
    group.bench_function("radius_plan", |f| {
        f.iter(|| {
            plomid_vector::plan_radius_search(
                black_box(&stats),
                black_box(VectorMetric::L2),
                black_box(0.5),
                black_box(true),
                black_box(&options),
            )
        })
    });
    group.bench_function("stats_observe_1k", |f| {
        f.iter(|| {
            let mut s = VectorStats::unknown(128);
            for _ in 0..1000 {
                s.observe_row(black_box(false), black_box(false), black_box(true));
            }
            s
        })
    });
    // Gap J: freshness-gated planning must cost the same as the base path
    // (one enum match, no syscalls).
    group.bench_function("filter_plan_fresh", |f| {
        f.iter(|| {
            plomid_vector::plan_filtered_search_with_freshness(
                black_box(&stats),
                black_box(0.1),
                black_box(10),
                black_box(true),
                black_box(&options),
                black_box(plomid_vector::Freshness::Fresh),
                black_box(None),
            )
        })
    });
    // Gap L: tier selection is pure (no syscalls on the hot path).
    group.bench_function("scale_select", |f| {
        let inputs = plomid_vector::ScaleInputs::test_seam(
            32,
            Some(64 * 1024 * 1024 * 1024),
            plomid_vector::storage::StorageClass::Nvme,
            10 * 1024 * 1024 * 1024,
            8,
        );
        let config = plomid_vector::ScaleConfig::explicit(None);
        f.iter(|| plomid_vector::ScaleDecision::select(black_box(&inputs), black_box(&config)))
    });
    // Phase 41: policy selection is pure (detection happens once, outside).
    group.bench_function("storage_select", |f| {
        let policy = plomid_vector::storage::VectorStoragePolicy::default();
        let cap = plomid_vector::storage::StorageCapability::test_seam(
            true,
            plomid_vector::storage::StorageClass::Nvme,
            Some(16 * 1024 * 1024 * 1024),
        );
        f.iter(|| policy.select(black_box(256 * 1024 * 1024), black_box(&cap)))
    });
    group.finish();
}

fn bench_numa_batch(c: &mut Criterion) {
    // Gap G: batch dispatch across NUMA modes on one deterministic fixture.
    // Correctness across modes is asserted in `search::batch` unit tests;
    // these benches record dispatch latency only.
    use plomid_vector::search::{search_many_parallel_numa, SearchMetric};
    let rows: Vec<(u64, Option<Vec<f32>>)> = lcg_vecs(2000, 64, 11)
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as u64, Some(v)))
        .collect();
    let queries: Vec<Vec<f32>> = lcg_vecs(16, 64, 12);
    let single = plomid_vector::NumaTopology::test_seam(1, 8).unwrap();
    let striped = plomid_vector::NumaTopology::test_seam(2, 16).unwrap();
    let mut group = c.benchmark_group("numa_batch_2k_x_64");
    group.bench_function("single_node", |f| {
        f.iter(|| {
            search_many_parallel_numa(
                black_box(&queries),
                black_box(&rows),
                SearchMetric::L2,
                black_box(10),
                None,
                black_box(4),
                black_box(&single),
                black_box(None),
            )
            .unwrap()
        })
    });
    group.bench_function("striped_2node", |f| {
        f.iter(|| {
            search_many_parallel_numa(
                black_box(&queries),
                black_box(&rows),
                SearchMetric::L2,
                black_box(10),
                None,
                black_box(4),
                black_box(&striped),
                black_box(None),
            )
            .unwrap()
        })
    });
    group.finish();
}

fn bench_storage_backings(c: &mut Criterion) {
    // Phase 41: file-backed vs mmap-mode open + cold vs warm query.
    // Hardware-dependent numbers are recorded by the harness, never asserted.
    use plomid_vector::storage::{
        ManagedVectorStorage, StorageCapability, StorageClass, StorageModeRequest,
        VectorStorageAdapter, VectorStoragePolicy,
    };
    use plomid_vector::DenseVectorValue;
    use plomid_vector::{VectorElementType, VectorValue};
    let dir = std::env::temp_dir().join(format!(
        "plomid-vector-bench-storage-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cap = StorageCapability::test_seam(true, StorageClass::Ssd, None);
    for mode in [StorageModeRequest::File, StorageModeRequest::Mmap] {
        let policy = VectorStoragePolicy {
            request: mode,
            ..VectorStoragePolicy::default()
        };
        let tag = match mode {
            StorageModeRequest::Mmap => "mmap",
            _ => "file",
        };
        let path = dir.join(tag);
        let mut s = ManagedVectorStorage::create(
            &path,
            "t".into(),
            "c".into(),
            Some(8),
            &policy,
            8 << 20,
            &cap,
            None,
        )
        .unwrap();
        for i in 0..2000u64 {
            let v = VectorValue::Dense(
                DenseVectorValue::from_f32(VectorElementType::Float32, vec![i as f32; 8]).unwrap(),
            );
            s.append_payload(&v).unwrap();
        }
        s.checkpoint(1).unwrap();
        s.close().unwrap();
    }
    let mut group = c.benchmark_group("storage_open_scan");
    for tag in ["file", "mmap"] {
        let path = dir.join(tag);
        let mode = if tag == "mmap" {
            StorageModeRequest::Mmap
        } else {
            StorageModeRequest::File
        };
        group.bench_function(format!("reopen_{tag}"), |f| {
            f.iter(|| {
                let policy = VectorStoragePolicy {
                    request: mode,
                    ..VectorStoragePolicy::default()
                };
                ManagedVectorStorage::open(
                    black_box(&path),
                    "t".into(),
                    "c".into(),
                    Some(8),
                    black_box(&policy),
                    black_box(8 << 20),
                    black_box(&cap),
                    black_box(None),
                )
                .unwrap()
            })
        });
        // Cold (no prefetch) vs warm (prefetched) sequential scan.
        let policy = VectorStoragePolicy {
            request: mode,
            ..VectorStoragePolicy::default()
        };
        group.bench_function(format!("scan_cold_{tag}"), |f| {
            f.iter(|| {
                let (s, _) = ManagedVectorStorage::open(
                    &path,
                    "t".into(),
                    "c".into(),
                    Some(8),
                    &policy,
                    8 << 20,
                    &cap,
                    None,
                )
                .unwrap();
                black_box(s.scan_batch(black_box(0), black_box(2000)).unwrap())
            })
        });
        group.bench_function(format!("scan_warm_{tag}"), |f| {
            f.iter(|| {
                let (mut s, _) = ManagedVectorStorage::open(
                    &path,
                    "t".into(),
                    "c".into(),
                    Some(8),
                    &policy,
                    8 << 20,
                    &cap,
                    None,
                )
                .unwrap();
                s.prefetch(0, 2000).unwrap();
                black_box(s.scan_batch(black_box(0), black_box(2000)).unwrap())
            })
        });
    }
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(
    benches,
    bench_distance,
    bench_exact_topk,
    bench_hnsw,
    bench_planner,
    bench_numa_batch,
    bench_storage_backings
);
criterion_main!(benches);
