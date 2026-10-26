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
// Moved from src/compaction_journal.rs inline #[cfg(test)].
use plomid_columnar::compaction::{CompactionInput, CompactionPlan};
use plomid_columnar::compaction_journal::{
    decode_job, encode_job, CompactionChunk, CompactionJournal, JobState, DEFAULT_CHUNK_ROWS,
};
use plomid_columnar::flush::FlushConfig;
use plomid_columnar::ColumnType;
use plomid_core::{DatabaseId, GenerationId, Lsn, ObjectId, SchemaId, TableId, TableIdentity};
use std::sync::atomic::{AtomicU64, Ordering};

fn identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(3), SchemaId::new(4), TableId::new(1))
}

fn plan() -> CompactionPlan {
    CompactionPlan {
        identity: identity(),
        object_id: ObjectId::new(1),
        inputs: vec![
            CompactionInput {
                generation: GenerationId::new(5),
                current: false,
            },
            CompactionInput {
                generation: GenerationId::new(6),
                current: true,
            },
        ],
        horizon: 9,
        inputs_releasable: true,
    }
}

fn request(generation: u64) -> plomid_columnar::compaction::CompactionRequest {
    plomid_columnar::compaction::CompactionRequest {
        identity: identity(),
        object_id: ObjectId::new(1),
        column_types: vec![ColumnType::Integer, ColumnType::String],
        config: FlushConfig::default(),
        generation: GenerationId::new(generation),
        storage_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
    }
}

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-compaction-journal-ut-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

#[test]
fn job_record_round_trips_through_bytes() {
    let dir = scratch("roundtrip");
    let journal = CompactionJournal::open(&dir).expect("open");
    let mut job = journal.begin_job(plan(), &request(7)).expect("begin");
    journal.plan_chunks(job.job_id, 10).expect("chunks");
    journal.mark_chunk_done(job.job_id, 0).expect("done");
    job = journal.get(job.job_id).expect("get");
    let bytes = encode_job(&job);
    let back = decode_job(&bytes).expect("decode");
    assert_eq!(job, back);
    assert_eq!(back.chunks.len(), 1);
    assert!(back.chunks[0].done);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn job_record_round_trips_column_policies() {
    use plomid_columnar::encoding::ColumnEncodingPreference;
    let dir = scratch("policies");
    let journal = CompactionJournal::open(&dir).expect("open");
    let mut req = request(7);
    req.config = req.config.with_column_policies(vec![
        ColumnEncodingPreference::DeltaBitpack,
        ColumnEncodingPreference::Auto,
        ColumnEncodingPreference::Dictionary,
    ]);
    let job = journal.begin_job(plan(), &req).expect("begin");
    let stored = journal.get(job.job_id).expect("get");
    let back = decode_job(&encode_job(&stored)).expect("decode");
    assert_eq!(stored, back);
    assert_eq!(back.config.column_policies, req.config.column_policies);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn torn_and_version_skewed_records_are_rejected() {
    let dir = scratch("torn");
    let journal = CompactionJournal::open(&dir).expect("open");
    let job = journal.begin_job(plan(), &request(7)).expect("begin");
    let mut bytes = encode_job(&journal.get(job.job_id).expect("get"));
    bytes.pop();
    assert!(decode_job(&bytes).is_err(), "truncation must fail");
    let mut tampered = encode_job(&journal.get(job.job_id).expect("get"));
    let last = tampered.len() - 1;
    tampered[last] ^= 0xFF;
    assert!(decode_job(&tampered).is_err(), "checksum drift must fail");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn terminal_states_report_correctly() {
    assert!(!JobState::Planned.is_terminal());
    assert!(!JobState::Building.is_terminal());
    assert!(!JobState::Built.is_terminal());
    assert!(!JobState::Published.is_terminal());
    assert!(!JobState::Released.is_terminal());
    assert!(JobState::Reclaimed.is_terminal());
    assert!(JobState::Aborted.is_terminal());
}

#[test]
fn chunks_tile_the_row_range() {
    let dir = scratch("tiling");
    let journal = CompactionJournal::open_with_chunk_rows(&dir, 4).expect("open");
    let job = journal.begin_job(plan(), &request(7)).expect("begin");
    let chunks = journal.plan_chunks(job.job_id, 10).expect("chunks");
    assert_eq!(chunks.len(), 3);
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| (chunk.rows_start, chunk.rows_end))
            .collect::<Vec<_>>(),
        vec![(0, 4), (4, 8), (8, 10)]
    );
    assert_eq!(
        journal.next_pending_chunk(job.job_id).expect("next"),
        chunks[0..1].first().copied()
    );
    journal.mark_chunk_done(job.job_id, 0).expect("done");
    assert_eq!(
        journal.next_pending_chunk(job.job_id).expect("next"),
        chunks[1..2].first().copied()
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn milestone_order_is_enforced() {
    let dir = scratch("order");
    let journal = CompactionJournal::open(&dir).expect("open");
    let job = journal.begin_job(plan(), &request(7)).expect("begin");
    assert!(
        journal.mark_built(job.job_id).is_err(),
        "build before chunks"
    );
    assert!(
        journal.mark_published(job.job_id).is_err(),
        "publish before build"
    );
    assert!(
        journal.mark_released(job.job_id).is_err(),
        "release before publish"
    );
    assert!(
        journal.mark_reclaimed(job.job_id).is_err(),
        "reclaim before release"
    );
    journal.plan_chunks(job.job_id, 0).expect("chunks");
    journal.mark_built(job.job_id).expect("built");
    journal.mark_published(job.job_id).expect("published");
    journal
        .mark_published(job.job_id)
        .expect("re-mark is a no-op");
    journal.mark_released(job.job_id).expect("released");
    journal.mark_reclaimed(job.job_id).expect("reclaimed");
    assert!(
        journal.abort_job(job.job_id).is_err(),
        "terminal cannot abort"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn chunk_progress_is_part_of_the_record() {
    let chunk = CompactionChunk {
        job_id: 3,
        chunk_idx: 1,
        rows_start: 100,
        rows_end: 200,
        done: false,
    };
    assert!(!chunk.done);
    assert_eq!(DEFAULT_CHUNK_ROWS, 8192);
}
