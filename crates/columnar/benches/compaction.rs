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
//! PLOMID compaction benchmarks: merge throughput and write amplification.
//!
//! Run with: `cargo bench -p plomid-columnar --bench compaction`.
//!
//! * merge-throughput: logical rows per second folded into one generation by
//!   `compact` (two input generations → one output).
//! * write-amplification: `output_bytes / input_bytes`, where `input_bytes`
//!   is the sum of the input generations' durable segment images and
//!   `output_bytes` is the published compacted image.

use plomid_columnar::{
    compact, mvcc_safety, read_generation_rows, ColumnType, ColumnarPublish, ColumnarStore,
    CompactionFailPoint, CompactionRequest, FlushConfig, PublishedColumnarSegment,
};
use plomid_core::{
    CatalogVersion, DatabaseId, GenerationId, Lsn, ObjectId, SchemaId, TableId, TableIdentity,
};
use plomid_storage::{Field, Row};
use plomid_txn::PlomidStorageEngine;
use std::time::Instant;

fn identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(3), SchemaId::new(4), TableId::new(1))
}

fn object() -> ObjectId {
    ObjectId::new(1)
}

fn sample_rows(base: i64, count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| {
            Row::new(vec![
                Field::Integer(base + i as i64),
                Field::String(format!("name-{:06}", base + i as i64)),
            ])
        })
        .collect()
}

fn column_types() -> Vec<ColumnType> {
    vec![ColumnType::Integer, ColumnType::String]
}

fn publish_generation(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    generation: u64,
    rows: &[Row],
) -> PublishedColumnarSegment {
    let publish = ColumnarPublish::new(
        object(),
        ColumnarStore::columnar_schema(SchemaId::new(4), CatalogVersion::new(1), &column_types())
            .expect("schema"),
        GenerationId::new(generation),
        GenerationId::new(1),
        Lsn::new(1),
    )
    .with_identity(identity());
    let segment_id = store.allocate_segment_id();
    store
        .flush_rows_with_id(
            engine,
            rows,
            &column_types(),
            segment_id,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish generation")
}

/// Sums the durable segment image bytes of every input generation.
fn input_image_bytes(published: &[PublishedColumnarSegment]) -> u64 {
    published
        .iter()
        .map(|segment| segment.bytes.len() as u64)
        .sum()
}

fn request(generation: u64) -> CompactionRequest {
    CompactionRequest {
        identity: identity(),
        object_id: object(),
        column_types: column_types(),
        config: FlushConfig::default(),
        generation: GenerationId::new(generation),
        storage_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
    }
}

fn bench_merge_throughput() {
    const PER_GEN: usize = 5_000;
    let dir = std::env::temp_dir().join(format!("plomid-compaction-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("engine");
    let store = ColumnarStore::open(&dir).expect("store");
    let gen5 = publish_generation(&store, &mut engine, 5, &sample_rows(0, PER_GEN));
    let gen6 = publish_generation(
        &store,
        &mut engine,
        6,
        &sample_rows(PER_GEN as i64, PER_GEN),
    );
    let rows = sample_rows(0, 2 * PER_GEN);

    let input_bytes = input_image_bytes(&[gen5, gen6]);
    let raw_input: usize = rows
        .iter()
        .map(|row| row.encode().map(|bytes| bytes.len()).unwrap_or(0))
        .sum();

    let iters = 3u64;
    let mut output_bytes = 0u64;
    let start = Instant::now();
    for i in 0..iters {
        // A fresh store per iteration keeps every timing a two-input merge:
        // select → verify → build → publish → release over the same shape.
        let iter_dir = std::env::temp_dir().join(format!(
            "plomid-compaction-bench-{i}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&iter_dir).expect("dir");
        let mut iter_engine =
            PlomidStorageEngine::create(&iter_dir, &iter_dir.join("wal"), 32).expect("engine");
        let iter_store = ColumnarStore::open(&iter_dir).expect("store");
        publish_generation(&iter_store, &mut iter_engine, 5, &sample_rows(0, PER_GEN));
        publish_generation(
            &iter_store,
            &mut iter_engine,
            6,
            &sample_rows(PER_GEN as i64, PER_GEN),
        );
        let (active, last) = mvcc_safety(&iter_engine).expect("safety");
        let outcome = compact(
            &iter_store,
            &mut iter_engine,
            &request(7),
            &rows,
            &active,
            last,
            CompactionFailPoint::None,
            false,
        )
        .expect("compact");
        output_bytes += outcome
            .published
            .as_ref()
            .map(|published| published.bytes.len() as u64)
            .unwrap_or(0);
        let back = read_generation_rows(
            &iter_store,
            &mut iter_engine,
            iter_store.generations(),
            object(),
            GenerationId::new(7),
        )
        .expect("read back");
        assert_eq!(back.len(), rows.len());
        std::hint::black_box(back.len());
        drop(iter_engine);
        std::fs::remove_dir_all(&iter_dir).ok();
    }
    let secs = start.elapsed().as_secs_f64();
    let total_rows = rows.len() as u64 * iters;
    let rows_sec = total_rows as f64 / secs;
    let write_amp = output_bytes as f64 / (input_bytes.max(1) as f64 * iters as f64);
    let vs_raw = output_bytes as f64 / (raw_input.max(1) as f64 * iters as f64);
    println!("  merge_throughput ......... {total_rows} rows in {secs:.4}s = {rows_sec:.0} rows/s");
    println!(
        "  write_amplification ...... output_bytes/input_bytes = {write_amp:.3} \
         (output={output_bytes}B input_image={}B over {iters} runs; output/raw_row_bytes={vs_raw:.3})",
        input_bytes * iters,
    );
    std::fs::remove_dir_all(&dir).ok();
}

fn bench_journal_overhead() {
    use plomid_columnar::{select_inputs, CompactionJournal};
    let dir = std::env::temp_dir().join(format!(
        "plomid-compaction-journal-bench-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("engine");
    let store = ColumnarStore::open(&dir).expect("store");
    publish_generation(&store, &mut engine, 5, &sample_rows(0, 1_000));
    publish_generation(&store, &mut engine, 6, &sample_rows(1_000, 1_000));
    let (active, last) = mvcc_safety(&engine).expect("safety");
    let plan =
        select_inputs(store.generations(), identity(), object(), &active, last).expect("plan");

    let iters = 50u64;
    let start = Instant::now();
    for i in 0..iters {
        let journal = CompactionJournal::open(&dir).expect("journal");
        let job = journal
            .begin_job(plan.clone(), &request(900 + i))
            .expect("begin");
        journal.plan_chunks(job.job_id, 2_000).expect("chunks");
        while journal
            .next_pending_chunk(job.job_id)
            .expect("next")
            .is_some()
        {
            let chunk = journal
                .next_pending_chunk(job.job_id)
                .expect("next")
                .expect("chunk");
            journal
                .mark_chunk_done(job.job_id, chunk.chunk_idx)
                .expect("done");
        }
        journal.mark_built(job.job_id).expect("built");
        std::hint::black_box(job.job_id);
    }
    let secs = start.elapsed().as_secs_f64();
    println!(
        "  journal_transitions ...... {} jobs (begin+chunks+built) in {secs:.4}s = {:.0} jobs/s",
        iters,
        iters as f64 / secs
    );
    std::fs::remove_dir_all(&dir).ok();
}

fn main() {
    println!("PLOMID compaction benchmarks (merge throughput, write amplification)");
    println!();
    bench_merge_throughput();
    bench_journal_overhead();
    println!();
    println!("Benchmarks complete.");
}
