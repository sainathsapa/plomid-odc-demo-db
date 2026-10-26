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
//! Crash-safe incremental resumable compaction journal integration tests.
//!
//! Every test runs against real engine, generation, and filesystem state:
//! journal roundtrip, kill-simulation (begin → build chunks → simulated
//! crash → resume completes), chunk-resume skipping, idempotent re-publish
//! and re-release, and restart visibility of incomplete jobs.

use plomid_columnar::{
    mvcc_safety, read_generation_rows, select_inputs, ColumnType, ColumnarPublish, ColumnarStore,
    CompactionJournal, CompactionRequest, FlushConfig, JobState,
};
use plomid_core::{
    CatalogVersion, DatabaseId, GenerationId, Lsn, ObjectId, SchemaId, TableId, TableIdentity,
};
use plomid_storage::{Field, Row};
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(3), SchemaId::new(4), TableId::new(1))
}

fn object() -> ObjectId {
    ObjectId::new(1)
}

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-compaction-journal-it-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}

fn int_rows(values: &[i64]) -> Vec<Row> {
    values
        .iter()
        .map(|value| Row::new(vec![Field::Integer(*value)]))
        .collect()
}

fn request(generation: u64) -> CompactionRequest {
    CompactionRequest {
        identity: identity(),
        object_id: object(),
        column_types: vec![ColumnType::Integer],
        config: FlushConfig::default(),
        generation: GenerationId::new(generation),
        storage_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
    }
}

fn publish_generation(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    generation: u64,
    rows: &[Row],
) {
    let publish = ColumnarPublish::new(
        object(),
        ColumnarStore::columnar_schema(
            SchemaId::new(4),
            CatalogVersion::new(1),
            &[ColumnType::Integer],
        )
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
            &[ColumnType::Integer],
            segment_id,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish generation");
}

fn setup(label: &str) -> (PathBuf, PlomidStorageEngine, ColumnarStore) {
    let dir = scratch(label);
    let engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("engine");
    let store = ColumnarStore::open(&dir).expect("store");
    (dir, engine, store)
}

/// Begins a journaled job over two published generations; returns its id.
fn begin_two_gen_job(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    output_generation: u64,
) -> u64 {
    publish_generation(store, engine, 5, &int_rows(&[1]));
    publish_generation(store, engine, 6, &int_rows(&[1, 2]));
    let (active, last) = mvcc_safety(engine).expect("safety");
    let plan =
        select_inputs(store.generations(), identity(), object(), &active, last).expect("plan");
    assert_eq!(plan.inputs.len(), 2);
    let job = store
        .journal()
        .begin_job(plan, &request(output_generation))
        .expect("begin");
    assert_eq!(job.state, JobState::Planned);
    assert!(!job.state.is_terminal());
    job.job_id
}

#[test]
fn journal_roundtrip_preserves_plan_state_and_chunks() {
    let (dir, mut engine, store) = setup("roundtrip");
    let result = (|| {
        let job_id = begin_two_gen_job(&store, &mut engine, 7);
        let journal = store.journal();
        journal.plan_chunks(job_id, 3)?;
        journal.mark_chunk_done(job_id, 0)?;
        let before = journal.get(job_id)?;

        // A fresh journal over the same directory reads the identical record.
        let reopened = CompactionJournal::open(&dir)?;
        let after = reopened.get(job_id)?;
        assert_eq!(before, after);
        assert_eq!(after.state, JobState::Building);
        assert_eq!(after.chunks.len(), 1);
        assert!(after.chunks[0].done);
        assert_eq!(
            after.plan.generation_ids(),
            vec![GenerationId::new(5), GenerationId::new(6)]
        );
        assert_eq!(after.output_generation, GenerationId::new(7));
        assert_eq!(after.created_seq, before.created_seq);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn kill_simulation_resumes_to_a_complete_compaction() {
    let dir = scratch("kill");
    let result = (|| {
        let job_id = {
            let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
            let store = ColumnarStore::open(&dir)?;
            let job_id = begin_two_gen_job(&store, &mut engine, 7);
            // Build half the chunks, then "die": drop everything without
            // marking built, published, released, or reclaimed.
            let store_journal = store.journal();
            store_journal.plan_chunks(job_id, 2)?;
            store_journal.mark_chunk_done(job_id, 0)?;
            engine.checkpoint()?;
            job_id
        };

        // Simulated restart: fresh engine + store over the same directory.
        let mut engine = PlomidStorageEngine::open(&dir, &dir.join("wal"), 32)?;
        let store = ColumnarStore::open(&dir)?;
        // Open reported the incomplete job without running it.
        let incomplete = store.pending_compaction_jobs()?;
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].job_id, job_id);
        assert_eq!(incomplete[0].state, JobState::Building);
        let replayed = store.journal().replay_incomplete()?;
        assert_eq!(replayed.len(), 1);

        // Resume drives Built → Published → Released → Reclaimed.
        let (active, last) = mvcc_safety(&engine)?;
        let outcome =
            store.resume_job(&mut engine, job_id, &int_rows(&[1, 2]), &active, last, true)?;
        assert_eq!(outcome.output_rows, 2);
        assert!(outcome.published.is_some());
        assert_eq!(store.journal().get(job_id)?.state, JobState::Reclaimed);
        assert!(store.pending_compaction_jobs()?.is_empty());

        // The compacted generation is durable and complete.
        let rows = read_generation_rows(
            &store,
            &mut engine,
            store.generations(),
            object(),
            GenerationId::new(7),
        )?;
        assert_eq!(rows, int_rows(&[1, 2]));
        store.generations().validate()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn chunk_resume_skips_done_chunks() {
    let (dir, mut engine, store) = setup("skip");
    let result = (|| {
        let job_id = begin_two_gen_job(&store, &mut engine, 7);
        let journal = store.journal();
        // Four one-row chunks over four input rows.
        let chunks = journal.plan_chunks(job_id, 2)?;
        assert_eq!(chunks.len(), 1, "chunk_rows default exceeds 2 rows");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");

    // Small chunk width exercises multi-chunk resume deterministically.
    let (dir, mut engine, store) = setup("skip-small");
    let result = (|| {
        let journal = CompactionJournal::open_with_chunk_rows(&dir, 1)?;
        publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
        publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
        let (active, last) = mvcc_safety(&engine)?;
        let plan = select_inputs(store.generations(), identity(), object(), &active, last)?;
        let job = journal.begin_job(plan, &request(7))?;
        let chunks = journal.plan_chunks(job.job_id, 3)?;
        assert_eq!(chunks.len(), 3);
        journal.mark_chunk_done(job.job_id, 0)?;
        journal.mark_chunk_done(job.job_id, 1)?;

        // Resume starts at the first undone chunk, not at zero.
        let next = journal.next_pending_chunk(job.job_id)?.expect("pending");
        assert_eq!(next.chunk_idx, 2);
        assert_eq!((next.rows_start, next.rows_end), (2, 3));

        // Drive the remainder through the store; done chunks are untouched.
        // Note: only the `journal` handle creates jobs here. The store's own
        // journal handle shares the same directory files, so creating through
        // both handles concurrently could reuse an identity; resume only reads.
        let before = journal.get(job.job_id)?;
        assert!(before.chunks[0].done && before.chunks[1].done && !before.chunks[2].done);
        let (active, last) = mvcc_safety(&engine)?;
        let outcome = store.resume_job(
            &mut engine,
            job.job_id,
            &int_rows(&[1, 2, 3]),
            &active,
            last,
            true,
        )?;
        assert_eq!(outcome.output_rows, 3);
        assert!(outcome.published.is_some());
        let done = journal.get(job.job_id)?;
        assert!(done.chunks.iter().all(|chunk| chunk.done));
        assert_eq!(store.journal().get(job.job_id)?.state, JobState::Reclaimed);
        // The compacted generation holds every input row.
        let rows = read_generation_rows(
            &store,
            &mut engine,
            store.generations(),
            object(),
            GenerationId::new(7),
        )?;
        assert_eq!(rows, int_rows(&[1, 2, 3]));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn re_publish_after_a_crash_is_a_no_op() {
    let (dir, mut engine, store) = setup("republish");
    let result = (|| {
        let job_id = begin_two_gen_job(&store, &mut engine, 7);
        let journal = store.journal();
        journal.plan_chunks(job_id, 2)?;
        journal.mark_chunk_done(job_id, 0)?;
        journal.mark_built(job_id)?;

        // Simulate the AfterPublish crash window: the output generation IS
        // published and current, but the journal still says Built.
        publish_generation(&store, &mut engine, 7, &int_rows(&[1, 2]));
        let reader = store.generations().reader()?;
        assert_eq!(
            reader.object(object()).expect("record").current_generation,
            GenerationId::new(7)
        );
        drop(reader);

        // Resume must NOT attempt a second publication (rejected as
        // already-published); it skips to release and reclamation.
        let (active, last) = mvcc_safety(&engine)?;
        let outcome =
            store.resume_job(&mut engine, job_id, &int_rows(&[1, 2]), &active, last, true)?;
        assert!(outcome.published.is_none(), "re-publish must be skipped");
        assert_eq!(store.journal().get(job_id)?.state, JobState::Reclaimed);
        let rows = read_generation_rows(
            &store,
            &mut engine,
            store.generations(),
            object(),
            GenerationId::new(7),
        )?;
        assert_eq!(rows, int_rows(&[1, 2]));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn re_release_after_a_crash_releases_nothing_new() {
    let (dir, mut engine, store) = setup("rerelease");
    let result = (|| {
        let job_id = begin_two_gen_job(&store, &mut engine, 7);
        let (active, last) = mvcc_safety(&engine)?;
        // First resume runs to Released without reclaiming.
        let outcome = store.resume_job(
            &mut engine,
            job_id,
            &int_rows(&[1, 2]),
            &active,
            last,
            false,
        )?;
        assert!(!outcome.released.is_empty());
        assert_eq!(store.journal().get(job_id)?.state, JobState::Released);

        // A second resume (the crash-retry) finds no still-owned inputs, so
        // the release step publishes nothing and still succeeds.
        let (active, last) = mvcc_safety(&engine)?;
        let retry = store.resume_job(
            &mut engine,
            job_id,
            &int_rows(&[1, 2]),
            &active,
            last,
            false,
        )?;
        assert!(retry.published.is_none());
        assert!(retry.released.is_empty(), "re-release must be a no-op");
        assert_eq!(store.journal().get(job_id)?.state, JobState::Released);

        // A completed job re-resumes as a no-op success (idempotent).
        let (active, last) = mvcc_safety(&engine)?;
        let done =
            store.resume_job(&mut engine, job_id, &int_rows(&[1, 2]), &active, last, true)?;
        assert!(done.published.is_none());
        assert!(done.released.is_empty());
        assert_eq!(store.journal().get(job_id)?.state, JobState::Reclaimed);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn incomplete_jobs_survive_a_restart_and_aborted_ones_do_not() {
    let dir = scratch("restart-list");
    let result = (|| {
        let (first, second) = {
            let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
            let store = ColumnarStore::open(&dir)?;
            publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
            publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
            let (active, last) = mvcc_safety(&engine)?;
            let plan = select_inputs(store.generations(), identity(), object(), &active, last)?;
            let first = store.journal().begin_job(plan.clone(), &request(7))?;
            let second = store.journal().begin_job(plan, &request(8))?;
            store.journal().abort_job(second.job_id)?;
            engine.checkpoint()?;
            (first.job_id, second.job_id)
        };

        // New journal handle on the same directory (simulated restart).
        let reopened = CompactionJournal::open(&dir)?;
        let incomplete = reopened.list_incomplete()?;
        assert_eq!(incomplete.len(), 1);
        assert_eq!(incomplete[0].job_id, first);
        assert_eq!(reopened.replay_incomplete()?.len(), 1);
        // The aborted job is durable but never listed.
        assert_eq!(reopened.get(second)?.state, JobState::Aborted);

        // ColumnarStore::open surfaces the same count without running jobs.
        let engine = PlomidStorageEngine::open(&dir, &dir.join("wal"), 32)?;
        let _ = engine;
        let store = ColumnarStore::open(&dir)?;
        assert_eq!(store.pending_compaction_jobs()?.len(), 1);
        // Nothing auto-ran: no new generation exists.
        assert!(store
            .generations()
            .reader()?
            .generation(GenerationId::new(7))
            .is_none());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn resume_rejects_rows_outside_the_planned_chunks() {
    let (dir, mut engine, store) = setup("mismatch");
    let result = (|| {
        let job_id = begin_two_gen_job(&store, &mut engine, 7);
        store.journal().plan_chunks(job_id, 2)?;
        let (active, last) = mvcc_safety(&engine)?;
        let error = store
            .resume_job(
                &mut engine,
                job_id,
                &int_rows(&[1, 2, 3, 4]),
                &active,
                last,
                false,
            )
            .expect_err("row/chunk mismatch must fail");
        assert_eq!(error.kind(), plomid_core::ErrorKind::InvalidArgument);
        // Unknown jobs are rejected too.
        let error = store
            .resume_job(&mut engine, 999_999, &int_rows(&[1]), &active, last, false)
            .expect_err("unknown job must fail");
        assert_eq!(error.kind(), plomid_core::ErrorKind::NotFound);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}
