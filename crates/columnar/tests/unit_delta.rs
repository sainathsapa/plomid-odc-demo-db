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
// Moved from src/delta.rs inline #[cfg(test)].
use plomid_columnar::delta::*;
use plomid_core::ObjectId;
use plomid_storage::{Field, Row, StorageEngine};

fn int_row(value: i64) -> Row {
    Row::new(vec![Field::Integer(value)])
}

fn fields(value: i64) -> Vec<Field> {
    vec![Field::Integer(value)]
}

#[test]
fn insert_without_pre_image_appends_to_scan() {
    let store = DeltaStore::new();
    let main = vec![int_row(50)];
    store.insert("t", DeltaRow::upsert(7, fields(75), 2));
    // No pre-image and no id chain: the insert appends after main order.
    assert_eq!(
        store.merge_bag("t", &main, 2),
        vec![int_row(50), int_row(75)]
    );
    // Snapshots predating the commit see the main image only.
    assert_eq!(store.merge_bag("t", &main, 1), vec![int_row(50)]);
}

#[test]
fn update_with_pre_image_replaces_in_place() {
    let store = DeltaStore::new();
    let main = vec![int_row(50), int_row(60)];
    store.insert(
        "t",
        DeltaRow::upsert(9, fields(75), 2).with_prev(fields(50)),
    );
    // Main order is preserved; the update lands where its pre-image was.
    assert_eq!(
        store.merge_bag("t", &main, 2),
        vec![int_row(75), int_row(60)]
    );
}

#[test]
fn delete_tombstone_hides_main_row() {
    let store = DeltaStore::new();
    let main = vec![int_row(50), int_row(60)];
    store.insert("t", DeltaRow::delete_row(9, fields(50), 3));
    assert_eq!(store.merge_bag("t", &main, 3), vec![int_row(60)]);
    // Pre-commit snapshots still see the row.
    assert_eq!(
        store.merge_bag("t", &main, 2),
        vec![int_row(50), int_row(60)]
    );
}

#[test]
fn future_commit_is_invisible_to_older_snapshot() {
    let store = DeltaStore::new();
    let main = vec![int_row(50)];
    store.insert(
        "t",
        DeltaRow::upsert(1, fields(75), 10).with_prev(fields(50)),
    );
    assert_eq!(store.merge_bag("t", &main, 9), vec![int_row(50)]);
    assert_eq!(store.merge_bag("t", &main, 10), vec![int_row(75)]);
    assert!(store.scan_history("t", 9).is_empty());
    assert_eq!(store.scan_history("t", 10).len(), 1);
}

#[test]
fn chained_updates_resolve_by_row_id() {
    let store = DeltaStore::new();
    let main = vec![int_row(50)];
    // Two updates to the same row: the second carries a stale pre-image
    // (the row is already 75 in-window) but chains through the row id.
    store.insert(
        "t",
        DeltaRow::upsert(1, fields(75), 2).with_prev(fields(50)),
    );
    store.insert(
        "t",
        DeltaRow::upsert(1, fields(90), 3).with_prev(fields(50)),
    );
    assert_eq!(store.merge_bag("t", &main, 3), vec![int_row(90)]);
}

#[test]
fn duplicate_rows_update_exactly_one() {
    let store = DeltaStore::new();
    let main = vec![int_row(50), int_row(50)];
    store.insert(
        "t",
        DeltaRow::upsert(1, fields(75), 2).with_prev(fields(50)),
    );
    // Bag semantics: one pre-image consumed, one duplicate survives.
    assert_eq!(
        store.merge_bag("t", &main, 2),
        vec![int_row(75), int_row(50)]
    );
}

#[test]
fn insert_then_delete_in_window_vanishes() {
    let store = DeltaStore::new();
    let main = vec![int_row(50)];
    store.insert("t", DeltaRow::upsert(7, fields(75), 2));
    store.insert("t", DeltaRow::delete(7, 3));
    assert_eq!(store.merge_bag("t", &main, 3), vec![int_row(50)]);
}

#[test]
fn imageless_delete_of_unknown_row_is_ignored() {
    let store = DeltaStore::new();
    let main = vec![int_row(50)];
    // No pre-image and no in-window chain: the tombstone names nothing.
    store.insert("t", DeltaRow::delete(99, 2));
    assert_eq!(store.merge_bag("t", &main, 2), vec![int_row(50)]);
}

#[test]
fn clear_drops_table_deltas() {
    let store = DeltaStore::new();
    store.insert("t", DeltaRow::upsert(1, fields(75), 2));
    assert_eq!(store.row_count("t"), 1);
    assert!(store.delta_bytes("t") > 0);
    store.clear("t");
    assert_eq!(store.row_count("t"), 0);
    assert_eq!(store.delta_bytes("t"), 0);
    assert!(store.scan_history("t", u64::MAX).is_empty());
}

#[test]
fn merge_rows_kernel_is_latest_wins_per_slot() {
    let main = vec![(0u64, int_row(50)), (1u64, int_row(60))];
    let deltas = vec![
        DeltaRow::upsert(0, fields(1), 1),
        DeltaRow::upsert(0, fields(2), 2),
        DeltaRow::delete(1, 3),
        DeltaRow::upsert(9, fields(70), 4),
    ];
    assert_eq!(
        merge_rows(&main, &deltas),
        vec![(0, int_row(2)), (9, int_row(70))]
    );
}

#[test]
fn compaction_advisor_fires_past_threshold() {
    let advisor = CompactionAdvisor::new(10, 1024);
    assert!(!advisor.should_compact(9, 100));
    assert!(advisor.should_compact(10, 100));
    assert!(advisor.should_compact(2, 2048));
}

#[test]
fn delta_key_scopes_by_root_and_object() {
    use std::path::Path;
    let left = delta_key(Path::new("/a"), ObjectId::new(1));
    let right = delta_key(Path::new("/b"), ObjectId::new(1));
    assert_ne!(left, right);
    assert_eq!(left, delta_key(Path::new("/a"), ObjectId::new(1)));
    assert_ne!(left, delta_key(Path::new("/a"), ObjectId::new(2)));
}

#[test]
fn commit_watermark_reports_the_engine_value() {
    let dir = std::env::temp_dir().join(format!("plomid-delta-watermark-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(dir.with_extension("wal"));
    let engine =
        plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    let (_, last) = engine.mvcc_safety().expect("safety");
    assert_eq!(commit_watermark(&engine), last);
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(dir.with_extension("wal")).ok();
}
