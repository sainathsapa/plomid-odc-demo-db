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
//! PLOMID columnar space-layer benchmarks.
//!
//! Run with: `cargo bench -p plomid-columnar --bench space`.
//!
//! * `space_accounting_overhead`: ns/op of the record_* ledger calls.
//! * `orphan_scan_time`: wall time vs segment count (referenced + orphaned).
//! * `gc_reclaim_throughput`: bytes/s of orphan payload reclamation.
//! * `fixture_footprint`: N-row table bytes before/after vacuum.

use plomid_columnar::{
    ColumnType, ColumnarPublish, ColumnarStore, FlushConfig, OrphanScan, SpaceAccount, SpaceEngine,
    SpaceScheduler,
};
use plomid_core::{
    CatalogVersion, DatabaseId, GenerationId, Lsn, ObjectId, SchemaId, SegmentId, TableId,
    TableIdentity,
};
use plomid_storage::{Field, PublicationRequest, Row};
use plomid_txn::PlomidStorageEngine;
use std::time::Instant;

fn identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(11), SchemaId::new(11), TableId::new(11))
}

fn scratch(label: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("plomid-space-bench-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("dir");
    path
}

fn dir_bytes(path: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                Err(_) => continue,
            };
            if meta.is_dir() {
                walk(&entry.path(), total);
            } else {
                *total = total.saturating_add(meta.len());
            }
        }
    }
    let mut total = 0u64;
    walk(path, &mut total);
    total
}

fn publish_generation(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    generation: u64,
    rows: usize,
    base: i64,
) -> (SegmentId, u64) {
    let data: Vec<Row> = (0..rows)
        .map(|i| {
            Row::new(vec![
                Field::Integer(base + i as i64),
                Field::String(format!("name-{:06}", base + i as i64)),
            ])
        })
        .collect();
    let types = vec![ColumnType::Integer, ColumnType::String];
    let publish = ColumnarPublish::new(
        ObjectId::new(11),
        ColumnarStore::columnar_schema(SchemaId::new(11), CatalogVersion::new(1), &types)
            .expect("schema"),
        GenerationId::new(generation),
        GenerationId::new(1),
        Lsn::new(1),
    )
    .with_identity(identity());
    let segment = store.allocate_segment_id();
    let out = store
        .flush_rows_with_id(
            engine,
            &data,
            &types,
            segment,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish");
    (segment, out.bytes.len() as u64)
}

fn bench_accounting_overhead() {
    let mut engine = SpaceEngine::new();
    let ops = 300_000u64;
    let start = Instant::now();
    for i in 0..ops {
        let segment = SegmentId::new(1 + (i % 50_000));
        engine.record_published("bench", segment, 4096, 50);
    }
    let publish_secs = start.elapsed().as_secs_f64();
    let start = Instant::now();
    for i in 0..ops {
        let segment = SegmentId::new(1 + (i % 50_000));
        engine.record_dead("bench", segment, 4096);
    }
    let dead_secs = start.elapsed().as_secs_f64();
    let start = Instant::now();
    for i in 0..ops {
        let segment = SegmentId::new(1 + (i % 50_000));
        engine.record_reclaimed("bench", segment, 4096);
    }
    let reclaim_secs = start.elapsed().as_secs_f64();
    std::hint::black_box(engine.account("bench").total_bytes());
    println!("space_accounting_overhead ({} ops each, 1 table):", ops);
    println!(
        "  record_published.... {:8.1} ns/op",
        publish_secs * 1e9 / ops as f64
    );
    println!(
        "  record_dead......... {:8.1} ns/op",
        dead_secs * 1e9 / ops as f64
    );
    println!(
        "  record_reclaimed.... {:8.1} ns/op",
        reclaim_secs * 1e9 / ops as f64
    );
}

fn scan_fixture(
    segments: usize,
    rows_each: usize,
) -> (
    std::path::PathBuf,
    PlomidStorageEngine,
    ColumnarStore,
    Vec<(SegmentId, u64)>,
) {
    let dir = scratch(&format!("scan-{segments}"));
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    let store = ColumnarStore::open(&dir).expect("store");
    let mut published = Vec::with_capacity(segments);
    for generation in 1..=segments as u64 {
        published.push(publish_generation(
            &store,
            &mut engine,
            generation,
            rows_each,
            generation as i64 * 10_000,
        ));
    }
    (dir, engine, store, published)
}

fn bench_orphan_scan_time() {
    println!("orphan_scan_time vs segment count (200 rows/segment):");
    for segments in [8usize, 32usize] {
        let (dir, mut engine, store, _published) = scan_fixture(segments, 200);
        // All referenced: scan must prove every slice reachable.
        let start = Instant::now();
        let iters = 5u32;
        let mut found = 0usize;
        for _ in 0..iters {
            found = OrphanScan::find_orphan_slices(&store, &mut engine, store.generations()).len();
        }
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(found, 0);
        println!(
            "  {:>3} referenced segments... {:8.3} ms/scan ({:7.1} us/segment)",
            segments,
            secs * 1e3 / iters as f64,
            secs * 1e6 / iters as f64 / segments as f64,
        );
        // Release the first half + gc: their slices become orphans.
        let doomed: Vec<GenerationId> = (1..=segments as u64 / 2).map(GenerationId::new).collect();
        let release = PublicationRequest::release(GenerationId::new(1), Lsn::new(1), doomed)
            .expect("release");
        store
            .generations()
            .publish(release)
            .expect("publish release");
        store.generations().gc().expect("gc");
        let start = Instant::now();
        let mut found = 0usize;
        for _ in 0..iters {
            found = OrphanScan::find_orphan_slices(&store, &mut engine, store.generations()).len();
        }
        let secs = start.elapsed().as_secs_f64();
        assert_eq!(found, segments / 2);
        println!(
            "  {:>3} segments ({} orphaned).. {:8.3} ms/scan ({:7.1} us/segment)",
            segments,
            segments / 2,
            secs * 1e3 / iters as f64,
            secs * 1e6 / iters as f64 / segments as f64,
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}

fn bench_gc_reclaim_throughput() {
    let segments = 16usize;
    let (dir, mut engine, store, _published) = scan_fixture(segments, 200);
    let doomed: Vec<GenerationId> = (1..=segments as u64 / 2).map(GenerationId::new).collect();
    let release =
        PublicationRequest::release(GenerationId::new(1), Lsn::new(1), doomed).expect("release");
    store
        .generations()
        .publish(release)
        .expect("publish release");
    store.generations().gc().expect("gc");
    let orphans = OrphanScan::find_orphan_slices(&store, &mut engine, store.generations());
    let estimate: u64 = orphans.iter().map(|(_, bytes)| *bytes).sum();
    let start = Instant::now();
    let bytes = OrphanScan::reclaim_orphans(&store, &mut engine, &orphans);
    let secs = start.elapsed().as_secs_f64();
    println!(
        "gc_reclaim_throughput: {} segments, {} bytes (est {} bytes) in {:.4}s = {:.1} MiB/s ({:.0} seg/s)",
        orphans.len(),
        bytes,
        estimate,
        secs,
        bytes as f64 / secs.max(1e-9) / (1024.0 * 1024.0),
        orphans.len() as f64 / secs.max(1e-9),
    );
    std::fs::remove_dir_all(&dir).ok();
}

fn bench_fixture_footprint() {
    // N-row table, then vacuum (release + gc + slice reclaim): the space
    // layer's byte accounting against the directory size on disk.
    let rows_each = 500usize;
    let segments = 4usize;
    let (dir, mut engine, store, published) = scan_fixture(segments, rows_each);
    let total_rows = rows_each * segments;
    let segment_bytes: u64 = published.iter().map(|(_, bytes)| *bytes).sum();
    let listed = store.list_segments(&mut engine).expect("list");
    let before = dir_bytes(&dir);
    let doomed: Vec<GenerationId> = (1..=(segments as u64 - 1)).map(GenerationId::new).collect();
    let release =
        PublicationRequest::release(GenerationId::new(1), Lsn::new(1), doomed).expect("release");
    store
        .generations()
        .publish(release)
        .expect("publish release");
    let gc_outcome = store.generations().gc().expect("gc");
    let (deleted, reclaimed) =
        OrphanScan::scan_and_reclaim(&store, &mut engine, store.generations());
    let after = dir_bytes(&dir);
    println!(
        "fixture_footprint ({} rows in {} segments):",
        total_rows, segments
    );
    println!("  segment payload bytes....... {}", segment_bytes);
    println!("  segments listed............. {}", listed.len());
    println!("  dir bytes before vacuum..... {}", before);
    println!(
        "  generations reclaimed....... {}",
        gc_outcome.reclaimed.len()
    );
    println!("  orphan segments deleted..... {}", deleted);
    println!("  orphan bytes reclaimed...... {}", reclaimed);
    println!("  dir bytes after vacuum...... {}", after);
    std::fs::remove_dir_all(&dir).ok();
}

fn bench_type_sizes() {
    println!("space_type_sizes (bytes, stack part only):");
    println!("  SpaceAccount.... {}", std::mem::size_of::<SpaceAccount>());
    println!("  SpaceEngine..... {}", std::mem::size_of::<SpaceEngine>());
    println!(
        "  SpaceScheduler.. {}",
        std::mem::size_of::<SpaceScheduler>()
    );
    println!("  OrphanScan...... {}", std::mem::size_of::<OrphanScan>());
}

fn main() {
    println!("PLOMID columnar space-layer benchmarks");
    println!();
    bench_type_sizes();
    println!();
    bench_accounting_overhead();
    println!();
    bench_orphan_scan_time();
    println!();
    bench_gc_reclaim_throughput();
    println!();
    bench_fixture_footprint();
    println!();
    println!("Benchmarks complete.");
}
