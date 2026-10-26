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
// Moved from `src/observability.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::observability::*;
use std::sync::Mutex as StdMutex;

/// Serializes the counter tests: they share the process-wide atomics and
/// each resets them, so they must never interleave.
static TEST_LOCK: StdMutex<()> = StdMutex::new(());

fn lock_for_test() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

#[test]
fn statement_class_covers_common_statements() {
    let _guard = lock_for_test();
    assert_eq!(statement_class("SELECT 1"), "SELECT");
    assert_eq!(statement_class("  insert into t values (1)"), "INSERT");
    assert_eq!(statement_class("VACUUM;"), "VACUUM");
    assert_eq!(statement_class("begin"), "BEGIN");
    assert_eq!(statement_class(""), "OTHER");
    assert_eq!(statement_class("SAVEPOINT x"), "OTHER");
}

#[test]
fn query_error_and_slow_accounting() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_query("SELECT", 5, false);
    record_query("SELECT", SLOW_QUERY_THRESHOLD_MS, false);
    record_query("INSERT", 7, true);
    let snap = snapshot();
    assert_eq!(snap.queries_total, 3);
    assert_eq!(snap.errors_total, 1);
    assert_eq!(snap.slow_queries_total, 1);
    assert_eq!(snap.slow_max_ms, SLOW_QUERY_THRESHOLD_MS);
    let select = snap
        .slow_by_class
        .iter()
        .find(|(class, _)| class == "SELECT")
        .expect("SELECT class recorded");
    assert_eq!(select.1.count, 1);
    assert_eq!(select.1.max_ms, SLOW_QUERY_THRESHOLD_MS);
    reset_for_tests();
    assert_eq!(snapshot().queries_total, 0);
}

#[test]
fn analytical_paths_include_skipped_markers() {
    let _guard = lock_for_test();
    reset_for_tests();
    note_analytical_path("indexed_join");
    note_analytical_path("indexed_join");
    let snap = snapshot();
    assert_eq!(snap.analytical_total, 2);
    let indexed = snap
        .analytical_by_path
        .iter()
        .find(|(path, _)| path == "indexed_join")
        .expect("wired path recorded");
    assert_eq!(indexed.1, 2);
    for skipped in SKIPPED_ANALYTICAL_PATHS {
        assert!(
            snap.analytical_by_path
                .iter()
                .any(|(path, count)| path == skipped && *count == 0),
            "missing zero marker for {skipped}"
        );
    }
    reset_for_tests();
}

#[test]
fn htap_spill_result_buffer_and_admission_count() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_spill_bytes(1_000);
    record_spill_bytes(500);
    record_result_buffer_grow(512);
    record_result_buffer_grow(512);
    record_result_buffer_shrink(512);
    record_admission(true);
    record_admission(false);
    record_admission_queued();
    record_admission_queued();
    record_admission_dequeued();
    let snap = snapshot();
    assert_eq!(snap.spill_bytes_total, 1_500);
    assert_eq!(snap.result_buffer_bytes, 512);
    assert_eq!(snap.result_buffer_peak, 1_024);
    assert_eq!(snap.admission_admitted, 1);
    assert_eq!(snap.admission_rejected, 1);
    assert_eq!(snap.admission_waiting, 1);
    // Gauges saturate instead of underflowing on unmatched releases.
    record_result_buffer_shrink(u64::MAX);
    record_admission_dequeued();
    record_admission_dequeued();
    let snap = snapshot();
    assert_eq!(snap.result_buffer_bytes, 0);
    assert_eq!(snap.result_buffer_peak, 1_024);
    assert_eq!(snap.admission_waiting, 0);
    reset_for_tests();
    let snap = snapshot();
    assert_eq!(snap.spill_bytes_total, 0);
    assert_eq!(snap.admission_admitted, 0);
}

#[test]
fn connections_maintenance_and_checkpoints_count() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_connection_open();
    record_connection_open();
    record_connection_close();
    record_maintenance_pass();
    record_checkpoint();
    let snap = snapshot();
    assert_eq!(snap.connections_opened, 2);
    assert_eq!(snap.connections_closed, 1);
    assert_eq!(snap.maintenance_passes_total, 1);
    assert_eq!(snap.checkpoints_total, 1);
    reset_for_tests();
}

#[test]
fn latency_histograms_bucket_count_and_reset() {
    let _guard = lock_for_test();
    reset_for_tests();
    // 500µs → bucket 0; 5ms → bucket 1; 500ms → bucket 3; 5s → bucket 4.
    record_commit_latency(500);
    record_commit_latency(5_000);
    record_commit_latency(500_000);
    record_commit_latency(5_000_000);
    let snap = snapshot();
    assert_eq!(snap.txn_commit_latency_us.count, 4);
    assert_eq!(snap.txn_commit_latency_us.total_us, 5_505_500);
    assert_eq!(snap.txn_commit_latency_us.max_us, 5_000_000);
    assert_eq!(
        snap.txn_commit_latency_us.buckets,
        [1, 1, 0, 1, 1, 0],
        "buckets are ≤1ms, ≤10ms, ≤100ms, ≤1s, ≤10s, >10s"
    );
    reset_for_tests();
    let snap = snapshot();
    assert_eq!(snap.txn_commit_latency_us.count, 0);
    assert_eq!(snap.txn_commit_latency_us.total_us, 0);
    assert_eq!(snap.txn_commit_latency_us.buckets, [0; 6]);
}

#[test]
fn transaction_lifecycle_and_conflicts_count() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_txn_begin();
    record_txn_begin();
    record_txn_end(true, 1_000);
    record_txn_end(false, 2_000);
    // Unmatched end saturates the gauge instead of underflowing it.
    record_txn_end(false, 3_000);
    record_txn_conflict();
    let snap = snapshot();
    assert_eq!(snap.txn_started, 2);
    assert_eq!(snap.txn_committed, 1);
    assert_eq!(snap.txn_aborted, 2);
    assert_eq!(snap.txn_conflicts, 1);
    assert_eq!(snap.txn_active, 0);
    assert_eq!(snap.txn_duration_us.count, 3);
    assert_eq!(snap.txn_duration_us.total_us, 6_000);
    reset_for_tests();
    assert_eq!(snapshot().txn_started, 0);
}

#[test]
fn contention_waits_record() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_engine_mutex_wait(100);
    record_gate_wait(200);
    record_wal_barrier_wait(300);
    record_group_commit_wait(400);
    record_maintenance_queue_wait(500);
    let snap = snapshot();
    assert_eq!(snap.engine_mutex_wait_us.count, 1);
    assert_eq!(snap.gate_wait_us.total_us, 200);
    assert_eq!(snap.wal_barrier_wait_us.max_us, 300);
    assert_eq!(snap.group_commit_wait_us.count, 1);
    assert_eq!(snap.maintenance_queue_wait_us.total_us, 500);
    reset_for_tests();
}

#[test]
fn wal_volume_backlog_and_rotations_count() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_wal_append(1_000);
    record_wal_append(500);
    assert_eq!(snapshot().wal_backlog_bytes, 1_500);
    record_wal_checkpoint_covered(1_000);
    let snap = snapshot();
    assert_eq!(snap.wal_bytes_total, 1_500);
    assert_eq!(snap.wal_appends_total, 2);
    assert_eq!(snap.wal_checkpoint_bytes_total, 1_000);
    assert_eq!(snap.wal_backlog_bytes, 500);
    // Covering more than the backlog saturates at zero, never underflows.
    record_wal_checkpoint_covered(u64::MAX);
    assert_eq!(snapshot().wal_backlog_bytes, 0);
    record_wal_rotation();
    record_wal_flush(7_000);
    record_wal_sync(8_000);
    let snap = snapshot();
    assert_eq!(snap.wal_rotations, 1);
    assert_eq!(snap.wal_flush_us.count, 1);
    assert_eq!(snap.wal_sync_us.total_us, 8_000);
    reset_for_tests();
}

#[test]
fn buffer_checkpoint_columnar_spill_vector_admin_count() {
    let _guard = lock_for_test();
    reset_for_tests();
    record_buffer_hit();
    record_buffer_hit();
    record_buffer_miss();
    record_buffer_eviction();
    record_buffer_dirty_pages(9);
    record_checkpoint_bytes(1 << 20);
    record_checkpoint_duration(11_000);
    record_columnar_scan(7, 3, 4, 2);
    record_spill_file();
    record_vector_rebuild(12_000);
    record_vector_freshness(true);
    record_vector_freshness(false);
    note_admin_action("reindex");
    note_admin_action("vacuum");
    note_admin_action("vacuum");
    let snap = snapshot();
    assert_eq!(snap.buffer_hits, 2);
    assert_eq!(snap.buffer_misses, 1);
    assert_eq!(snap.buffer_evictions, 1);
    assert_eq!(snap.buffer_dirty_pages_last_sync, 9);
    assert_eq!(snap.checkpoint_bytes_total, 1 << 20);
    assert_eq!(snap.checkpoint_duration_us.count, 1);
    assert_eq!(snap.columnar_segments_examined, 7);
    assert_eq!(snap.columnar_segments_skipped, 3);
    assert_eq!(snap.columnar_columns_read, 4);
    assert_eq!(snap.columnar_batches, 2);
    assert_eq!(snap.spill_files_total, 1);
    assert_eq!(snap.vector_rebuilds, 1);
    assert_eq!(snap.vector_rebuild_us.total_us, 12_000);
    assert_eq!(snap.vector_fresh, 1);
    assert_eq!(snap.vector_stale, 1);
    assert_eq!(
        snap.admin_by_action,
        vec![("reindex".to_string(), 1), ("vacuum".to_string(), 2),]
    );
    reset_for_tests();
    let snap = snapshot();
    assert!(snap.admin_by_action.is_empty());
    assert_eq!(snap.spill_files_total, 0);
}
