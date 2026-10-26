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
// Single-machine HTAP interference baselines (Phase 58).
//
// Synthetic single-machine loads: the write flood stands in for OLTP
// (concurrent row appends + point reads on a disjoint store — CPU/cache
// contention without data races), the vector side runs the real search /
// build / maintenance / backup paths. Baselines A–H below; criterion
// reports p50/p95/p99 + throughput, recorded in
// `docs/VECTOR_BACKUP_HTAP.md`. Numbers are hardware-specific and are
// never asserted — correctness under flood is asserted in `src/htap.rs`
// unit tests instead.
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use plomid_vector::htap::run_mixed;
use plomid_vector::index::hnsw::HnswIndex;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric, VectorIndex};
use plomid_vector::search::exact::exact_top_k;
use plomid_vector::search::{BatchSearcher, SearchMetric};
use plomid_vector::storage::{InMemoryVectorStorage, VectorStorageAdapter};
use plomid_vector::{DenseVectorValue, VectorElementType, VectorValue};
use std::sync::{Arc, Mutex};

fn dense(v: Vec<f32>) -> VectorValue {
    VectorValue::Dense(DenseVectorValue::from_f32(VectorElementType::Float32, v).unwrap())
}

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

/// Shared fixture: 4k write rows, 5k x 64 search rows, prebuilt HNSW.
struct Fixture {
    write_vals: Vec<VectorValue>,
    search_rows: Vec<(u64, Option<Vec<f32>>)>,
    flat_rows: Vec<(u64, Vec<f32>)>,
    queries: Vec<Vec<f32>>,
}

fn fixture() -> Fixture {
    let write_vals: Vec<VectorValue> = lcg_vecs(4096, 8, 100).into_iter().map(dense).collect();
    let vecs = lcg_vecs(5000, 64, 200);
    let search_rows: Vec<(u64, Option<Vec<f32>>)> = vecs
        .iter()
        .enumerate()
        .map(|(i, v)| (i as u64, Some(v.clone())))
        .collect();
    let flat_rows: Vec<(u64, Vec<f32>)> = vecs
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as u64, v))
        .collect();
    let queries = lcg_vecs(8, 64, 300);
    Fixture {
        write_vals,
        search_rows,
        flat_rows,
        queries,
    }
}

/// OLTP stand-in: appends + point reads against a mutex-guarded memory store.
fn write_flood(store: &Arc<Mutex<InMemoryVectorStorage>>, vals: &[VectorValue], i: usize) {
    let v = &vals[i % vals.len()];
    let mut s = store.lock().unwrap();
    let id = s.append_payload(v).unwrap();
    let _ = s.read_payload(id).unwrap();
}

fn bench_baseline_a_oltp_only(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_a_oltp_only");
    group.bench_function("writes_4k", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(64),
                black_box(4096),
                |_| {
                    let s = store.lock().unwrap();
                    black_box(s.row_count());
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_b_vector_only(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_b_vector_only");
    group.bench_function("exact_topk", |f| {
        f.iter(|| {
            let borrowed: Vec<(u64, Option<&[f32]>)> = fx
                .search_rows
                .iter()
                .map(|(id, v)| (*id, v.as_deref()))
                .collect();
            black_box(
                exact_top_k(
                    black_box(&fx.queries[0]),
                    borrowed,
                    SearchMetric::L2,
                    black_box(10),
                    None,
                )
                .unwrap(),
            )
        })
    });
    group.bench_function("batch_8q", |f| {
        f.iter(|| {
            let mut s = BatchSearcher::new(SearchMetric::L2, 10).unwrap();
            let qs: Vec<&[f32]> = fx.queries.iter().map(Vec::as_slice).collect();
            black_box(
                s.search_many(black_box(&qs), black_box(&fx.search_rows), None)
                    .unwrap(),
            )
        })
    });
    group.finish();
}

fn bench_baseline_c_oltp_exact(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_c_oltp_exact");
    group.bench_function("mixed", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(16),
                black_box(1024),
                |_| {
                    let borrowed: Vec<(u64, Option<&[f32]>)> = fx
                        .search_rows
                        .iter()
                        .map(|(id, v)| (*id, v.as_deref()))
                        .collect();
                    black_box(
                        exact_top_k(&fx.queries[0], borrowed, SearchMetric::L2, 10, None).unwrap(),
                    );
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_d_oltp_ann(c: &mut Criterion) {
    let fx = fixture();
    let mut hnsw = HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, 64)).unwrap();
    hnsw.build(&fx.flat_rows).unwrap();
    let mut group = c.benchmark_group("htap_d_oltp_ann");
    group.bench_function("mixed", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(16),
                black_box(1024),
                |qi| {
                    black_box(
                        hnsw.search(black_box(&fx.queries[qi % fx.queries.len()]), black_box(10))
                            .unwrap(),
                    );
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_e_oltp_batch(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_e_oltp_batch");
    group.bench_function("mixed", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(8),
                black_box(1024),
                |_| {
                    let mut s = BatchSearcher::new(SearchMetric::L2, 10).unwrap();
                    let qs: Vec<&[f32]> = fx.queries.iter().map(Vec::as_slice).collect();
                    black_box(s.search_many(&qs, &fx.search_rows, None).unwrap());
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_f_oltp_build(c: &mut Criterion) {
    let fx = fixture();
    let small: Vec<(u64, Vec<f32>)> = fx.flat_rows.iter().take(1000).cloned().collect();
    let mut group = c.benchmark_group("htap_f_oltp_build");
    group.sample_size(10);
    group.bench_function("mixed", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(2),
                black_box(512),
                |_| {
                    let mut h =
                        HnswIndex::new(IndexConfig::new(IndexKind::Hnsw, Metric::L2, 64)).unwrap();
                    h.build(black_box(&small)).unwrap();
                    black_box(h.stats());
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_g_oltp_maintenance(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_g_oltp_maintenance");
    group.bench_function("mixed", |f| {
        f.iter(|| {
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(8),
                black_box(1024),
                |i| {
                    // Maintenance stand-in: tombstone churn + validate on a
                    // scratch flat index (delete/compact path, bounded).
                    let mut flat =
                        FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 64)).unwrap();
                    flat.build(&fx.flat_rows).unwrap();
                    for d in 0..8u64 {
                        flat.delete((i as u64 * 8 + d) % 5000).unwrap();
                    }
                    flat.validate().unwrap();
                    black_box(flat.stats());
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

fn bench_baseline_h_oltp_backup(c: &mut Criterion) {
    let fx = fixture();
    let mut group = c.benchmark_group("htap_h_oltp_backup");
    group.sample_size(10);
    group.bench_function("mixed", |f| {
        f.iter(|| {
            // Quiesced source store (backup always runs on a snapshot).
            let mut src = InMemoryVectorStorage::new(Some(64));
            for v in fx.flat_rows.iter().take(512) {
                src.append_payload(&dense(v.1.clone())).unwrap();
            }
            let store = Arc::new(Mutex::new(InMemoryVectorStorage::new(Some(8))));
            let report = run_mixed(
                black_box(2),
                black_box(512),
                |i| {
                    let dir = std::env::temp_dir()
                        .join(format!("plomid-htap-bench-h-{}-{i}", std::process::id()));
                    let _ = std::fs::remove_dir_all(&dir);
                    let r = plomid_vector::create_backup(
                        &src,
                        None,
                        "t",
                        "c",
                        1,
                        None,
                        &[],
                        0,
                        &dir,
                        plomid_vector::BackupFailPoint::None,
                        None,
                    )
                    .unwrap();
                    black_box(r);
                    let _ = std::fs::remove_dir_all(&dir);
                },
                |i| write_flood(&store, &fx.write_vals, i),
            );
            black_box(report)
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_baseline_a_oltp_only,
    bench_baseline_b_vector_only,
    bench_baseline_c_oltp_exact,
    bench_baseline_d_oltp_ann,
    bench_baseline_e_oltp_batch,
    bench_baseline_f_oltp_build,
    bench_baseline_g_oltp_maintenance,
    bench_baseline_h_oltp_backup,
);
criterion_main!(benches);
