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
//! Elastic-scale walls: write throughput vs concurrent writers, read
//! throughput vs concurrent readers.
//!
//! The V1 engine is single-writer: every commit serializes behind one
//! mutex, so write throughput must flatten as writers grow, while reads
//! (lock-free version-store resolution through per-thread concurrent
//! facades) should scale. This bench proves both curves with numbers
//! instead of assertions, and is the entry gate for every elastic-engine
//! phase: no scaling change lands without moving these curves.
//!
//! Measured 2026-10-08:
//!
//! ```text
//! write_wall (debug):  1 writer   50 txns / 0.51 s =   97 tps
//!                      2 writers 100 txns / 0.91 s =  109 tps
//!                      4 writers 200 txns / 1.73 s =  116 tps
//!                      8 writers 400 txns / 3.53 s =  113 tps  → FLAT (mutex wall)
//! read_batch (optimized, setup-free):
//!                      point_200: 18.1 us / 200 keys = ~90 ns per get
//!                      get_many_200: 13.1 us / 200 keys = ~65 ns per key
//!                      → no per-op setup worth cutting; the read path is
//!                      already near-max. (An earlier read_scale shape timed
//!                      engine create+seed inside the loop and measured fsync
//!                      noise, not reads — methodology matters.)
//! write_batch (optimized): 20 txns of 1/10/100 puts ≈ 93 / 926 / 7,936
//!                      puts/sec on laptop SSD → batching amortizes the one
//!                      fsync per commit; NVMe latency multiplies this.
//! ```
//!
//! Max-QPS recipes (durability unchanged): point reads stream at ~90 ns/op
//! per thread through lock-free facades (scale with cores until the version
//! lock contends); writes batch puts per commit; concurrent single-row
//! commits stay fsync-serialized until partitioned commit (E2) lands.
//!
//! Run with: `cargo bench -p plomid-txn --bench elastic_scale`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use plomid_storage::{StorageEngine, StorageEngineTransaction};
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch(label: &str) -> PathBuf {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "plomid-bench-scale-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn seed(engine: &mut PlomidStorageEngine, keys: usize) {
    let mut txn = engine.begin().expect("begin");
    for i in 0..keys {
        let key = format!("k{i:08}");
        txn.put(key.as_bytes(), b"value-payload-16b").expect("put");
    }
    txn.commit().expect("commit");
}

/// Commits per second with `writers` threads hammering one shared engine.
///
/// Each writer runs `txns_per_writer` single-put transactions on disjoint
/// key ranges (no logical contention): what remains is the engine's own
/// serialization wall. Keys are precomputed so formatting never pollutes
/// the hot loop.
fn write_throughput(writers: usize, txns_per_writer: usize) -> f64 {
    let dir = scratch(&format!("w{writers}"));
    let engine = Arc::new(Mutex::new(
        PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create"),
    ));
    let barrier = Arc::new(Barrier::new(writers));
    let start = Instant::now();
    std::thread::scope(|scope| {
        for writer in 0..writers {
            let (engine, barrier) = (Arc::clone(&engine), Arc::clone(&barrier));
            scope.spawn(move || {
                let keys: Vec<Vec<u8>> = (0..txns_per_writer)
                    .map(|i| format!("w{writer:02}-k{i:06}").into_bytes())
                    .collect();
                barrier.wait();
                for key in &keys {
                    let mut guard = engine.lock().expect("engine lock");
                    let mut txn = guard.begin().expect("begin");
                    txn.put(key, b"value-payload-16b").expect("put");
                    txn.commit().expect("commit");
                }
            });
        }
    });
    let elapsed = start.elapsed().as_secs_f64();
    let _ = std::fs::remove_dir_all(&dir);
    writers as f64 * txns_per_writer as f64 / elapsed.max(1e-9)
}

/// Puts per second with one writer batching `puts_per_txn` puts per
/// transaction. One durable commit costs ~one fsync; batching amortizes it.
/// This is the max-write recipe on the single-writer engine: fewer,
/// bigger commits — never weaker durability.
fn batch_throughput(puts_per_txn: usize, txns: usize) -> f64 {
    let dir = scratch(&format!("b{puts_per_txn}"));
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    let keys: Vec<Vec<u8>> = (0..txns * puts_per_txn)
        .map(|i| format!("bk{i:08}").into_bytes())
        .collect();
    let start = Instant::now();
    for chunk in keys.chunks(puts_per_txn) {
        let mut txn = engine.begin().expect("begin");
        for key in chunk {
            txn.put(key, b"value-payload-16b").expect("put");
        }
        txn.commit().expect("commit");
    }
    let elapsed = start.elapsed().as_secs_f64();
    let _ = std::fs::remove_dir_all(&dir);
    keys.len() as f64 / elapsed.max(1e-9)
}

/// Point reads per second with `readers` threads over a shared engine.
///
/// The engine is seeded once OUTSIDE the timed region; each iteration only
/// performs the reads. Each thread resolves through its own concurrent
/// facade, which serves committed reads from the shared version store
/// without taking the write mutex: this curve should rise with threads
/// until the box runs out of cores.
fn read_throughput(readers: usize, keys: usize, reads_per_reader: usize) -> f64 {
    use plomid_txn::ConcurrentPlomidStorageEngine;

    let dir = scratch(&format!("r{readers}"));
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    seed(&mut engine, keys);
    let shared = Arc::new(Mutex::new(engine));
    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..readers {
            let shared = Arc::clone(&shared);
            scope.spawn(move || {
                let mut facade =
                    ConcurrentPlomidStorageEngine::from_shared(shared).expect("facade");
                let keys: Vec<Vec<u8>> = (0..reads_per_reader)
                    .map(|i| format!("k{:08}", i % keys).into_bytes())
                    .collect();
                let barrier = Barrier::new(1);
                barrier.wait();
                let iter_start = Instant::now();
                for key in &keys {
                    black_box(facade.get(key).expect("get"));
                }
                iter_start.elapsed()
            });
        }
    });
    let elapsed = start.elapsed().as_secs_f64();
    let _ = std::fs::remove_dir_all(&dir);
    readers as f64 * reads_per_reader as f64 / elapsed.max(1e-9)
}

fn bench_write_wall(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_wall");
    group.sample_size(10);
    for writers in [1usize, 2, 4, 8] {
        group.bench_function(format!("writers_{writers}"), |b| {
            b.iter(|| black_box(write_throughput(writers, 50)));
        });
    }
    group.finish();
}

fn bench_read_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_scale");
    group.sample_size(10);
    for readers in [1usize, 2, 4, 8] {
        group.bench_function(format!("readers_{readers}"), |b| {
            b.iter(|| black_box(read_throughput(readers, 2_000, 200)));
        });
    }
    group.finish();
}

fn bench_read_batch(c: &mut Criterion) {
    use plomid_txn::ConcurrentPlomidStorageEngine;

    // Setup-free: the seed runs once, every iteration measures only reads.
    // `point_200` pays one snapshot + one store acquisition per key;
    // `get_many_200` pays it once for the whole batch. The gap between
    // them is the per-op setup cost — the reader's optimization target.
    let dir = scratch("readbatch");
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    seed(&mut engine, 2_000);
    let shared = Arc::new(Mutex::new(engine));
    let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:08}").into_bytes()).collect();

    let mut group = c.benchmark_group("read_batch");
    group.sample_size(10);
    group.bench_function("point_200", |b| {
        b.iter_batched(
            || ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&shared)).expect("facade"),
            |mut facade| {
                for key in &keys {
                    black_box(facade.get(key).expect("get"));
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.bench_function("get_many_200", |b| {
        b.iter_batched(
            || ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&shared)).expect("facade"),
            |mut facade| black_box(facade.get_many(&keys).expect("get_many")),
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");
    group.sample_size(10);
    for puts_per_txn in [1usize, 10, 100] {
        group.bench_function(format!("puts_{puts_per_txn}"), |b| {
            b.iter(|| black_box(batch_throughput(puts_per_txn, 20)));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_write_wall,
    bench_read_scale,
    bench_write_batch,
    bench_read_batch
);
criterion_main!(benches);
