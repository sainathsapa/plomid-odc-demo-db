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
// Moved from `src/manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::DeviceId;
use plomid_storage::device::{register_device, DeviceRegistry};
use plomid_storage::layout::DatabaseLayout;
use plomid_storage::manager::StorageManager;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "plomid-storage-manager-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn rotates_real_segments_and_reopens_them() {
    let path = root();
    let result = (|| {
        let mut manager = StorageManager::create(&path, 32, 1)?;
        manager.insert(b"alpha", b"one")?;
        manager.insert(b"beta", b"two")?;
        assert!(manager.segment_count() >= 2);
        manager.sync()?;
        drop(manager);
        let mut reopened = StorageManager::open(&path, 32, 1)?;
        assert_eq!(reopened.get(b"alpha")?, Some(b"one".to_vec()));
        assert_eq!(reopened.get(b"beta")?, Some(b"two".to_vec()));
        assert!(reopened.segment_count() >= 2);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn updating_a_key_in_a_sealed_segment_does_not_shadow_it() {
    let path = root();
    let result = (|| {
        // A 1-byte segment rotates on every insert, so `alpha` is owned by
        // a sealed segment by the time it is updated.
        let mut manager = StorageManager::create(&path, 32, 1)?;
        manager.insert(b"alpha", b"one")?;
        manager.insert(b"beta", b"two")?;
        assert!(manager.segment_count() >= 2);
        manager.insert(b"alpha", b"updated")?;
        manager.sync()?;
        drop(manager);

        let mut reopened = StorageManager::open(&path, 32, 1)?;
        assert_eq!(reopened.get(b"alpha")?, Some(b"updated".to_vec()));
        let rows = reopened.range(None, None)?;
        assert_eq!(
            rows.iter().filter(|(key, _)| key == b"alpha").count(),
            1,
            "replacing a sealed key must not leave a second copy"
        );
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn every_segment_belongs_to_a_registered_device() {
    use plomid_storage::capacity_for_extents;

    let path = root();
    let result = (|| {
        let mut manager = StorageManager::create(&path, 32, 1)?;
        let second = DeviceId::new(7);
        let layout = DatabaseLayout::new(&path);
        // A capacity within one allocator snapshot page and larger than the
        // device the database created for itself (8 192 extents).
        let capacity = capacity_for_extents(10_000).expect("capacity");
        register_device(&layout, second, capacity)?;
        manager.insert(b"first", b"one")?;
        manager.insert(b"second", b"two")?;
        // Placement is capacity-first: the device with the most free
        // capacity receives the next segment, so a device registered after
        // creation wins as soon as it has the most room.
        assert!(
            manager
                .segments()
                .any(|segment| segment.device_id == second),
            "the newly registered device receives segments"
        );
        // Every segment lives in the pack directory of its own device, and
        // never in the logical object tree.
        for segment in manager.segments() {
            assert!(segment
                .path
                .starts_with(layout.device_packs_dir(segment.device_id)));
            assert!(!segment.path.starts_with(layout.objects_dir()));
        }
        assert!(DeviceRegistry::discover(&layout)?.contains(second));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn drained_sealed_segments_stay_exact_and_skip_definitively() {
    use plomid_storage::manager::SegmentState;

    let path = root();
    let result = (|| {
        // A 1-byte segment rotates on every insert, so the first keys end
        // up owned by sealed segments; deleting them drains those
        // segments completely (the tree is empty but the segment files
        // remain until reclamation).
        let mut manager = StorageManager::create(&path, 32, 1)?;
        manager.insert(b"alpha", b"one")?;
        manager.insert(b"beta", b"two")?;
        manager.insert(b"gamma", b"three")?;
        assert!(manager.segment_count() >= 3);
        manager.apply_batch(&[
            (b"alpha".to_vec(), None),
            (b"beta".to_vec(), None),
            (b"gamma".to_vec(), None),
        ])?;
        assert_eq!(manager.get(b"alpha")?, None);
        manager.sync()?;
        drop(manager);

        // Reopen: bounds start unknown, exactly like a restarted server.
        let mut reopened = StorageManager::open(&path, 32, 1)?;
        let sealed_before: Vec<u64> = reopened
            .segments()
            .filter(|segment| segment.state == SegmentState::Sealed)
            .map(|segment| segment.id)
            .collect();
        assert!(!sealed_before.is_empty());
        // A fresh key probes every sealed segment; each must resolve to
        // a definitive skip (no exact descent) and the write must land
        // exactly once in the active segment. (The insert tail may seal
        // the previously-active segment if it is already full, so only
        // the pre-batch sealed set is asserted below.)
        let (probes0, skips0) = reopened.filter_stats();
        reopened.apply_batch(&[(b"fresh".to_vec(), Some(b"row".to_vec()))])?;
        let (probes1, skips1) = reopened.filter_stats();
        assert_eq!(probes1, probes0, "drained segments caused exact probes");
        assert_eq!(
            skips1 - skips0,
            sealed_before.len() as u64,
            "every sealed segment must skip a drained key"
        );
        assert_eq!(reopened.get(b"fresh")?, Some(b"row".to_vec()));
        // The drained segments are proven empty: the "no bounds" answer
        // is memoized, so later batches skip them without re-running the
        // edge descent. (Without memoization every key of every batch
        // re-paid O(depth) page reads per drained segment.)
        for id in &sealed_before {
            let segment = reopened
                .segments()
                .find(|segment| segment.id == *id)
                .expect("sealed segment present");
            assert!(
                segment.bounds_known_empty(),
                "sealed segment {id} must memoize its empty bounds"
            );
        }
        // A second batch observes the same definitive skips (covering any
        // segment sealed by the first batch's insert tail as well).
        let sealed_mid: Vec<u64> = reopened
            .segments()
            .filter(|segment| segment.state == SegmentState::Sealed)
            .map(|segment| segment.id)
            .collect();
        let (probes2, skips2) = reopened.filter_stats();
        reopened.apply_batch(&[(b"fresh2".to_vec(), Some(b"row".to_vec()))])?;
        let (probes3, skips3) = reopened.filter_stats();
        assert_eq!(probes3, probes2, "second batch caused exact probes");
        assert_eq!(skips3 - skips2, sealed_mid.len() as u64);
        assert_eq!(reopened.get(b"fresh2")?, Some(b"row".to_vec()));
        // Memoization persists across batches.
        for id in &sealed_before {
            let segment = reopened
                .segments()
                .find(|segment| segment.id == *id)
                .expect("sealed segment present");
            assert!(
                segment.bounds_known_empty(),
                "sealed segment {id} must keep its memoized empty bounds"
            );
        }
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn reclaim_removes_drained_sealed_files_and_keeps_live_reads_exact() {
    use plomid_storage::manager::SegmentState;

    let path = root();
    let result = (|| {
        // 1-byte segments rotate on every insert: 50 keys build ~50
        // sealed segments plus the active tail, mirroring the drained
        // dead-weight fixture (dozens of sealed files holding no keys).
        let mut manager = StorageManager::create(&path, 32, 1)?;
        const KEYS: u64 = 50;
        for index in 0..KEYS {
            let key = format!("drain-{index:04}");
            manager.insert(key.as_bytes(), b"value")?;
        }
        let sealed_built: Vec<u64> = manager
            .segments()
            .filter(|segment| segment.state == SegmentState::Sealed)
            .map(|segment| segment.id)
            .collect();
        assert!(
            sealed_built.len() >= 46,
            "fixture must hold dozens of sealed segments, found {}",
            sealed_built.len()
        );
        // Drain every sealed segment, then add one live key that must
        // survive reclamation byte-for-byte.
        let deletes: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..KEYS)
            .map(|index| (format!("drain-{index:04}").into_bytes(), None))
            .collect();
        manager.apply_batch(&deletes)?;
        manager.insert(b"live", b"yes")?;
        manager.sync()?;
        let sealed_drained: Vec<(u64, std::path::PathBuf)> = manager
            .segments()
            .filter(|segment| segment.state == SegmentState::Sealed)
            .map(|segment| (segment.id, segment.path.clone()))
            .collect();
        assert!(
            sealed_drained.len() >= 46,
            "fixture must hold dozens of drained sealed segments, found {}",
            sealed_drained.len()
        );
        let bytes_before: u64 = manager
            .segments()
            .map(|segment| {
                std::fs::metadata(&segment.path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0)
            })
            .sum();
        assert!(bytes_before > 0);

        let outcome = manager.reclaim_drained_segments(&std::collections::BTreeSet::new())?;
        // Nothing is published on this root, so no segment is durably
        // referenced: every drained sealed segment goes.
        assert_eq!(outcome.reclaimed.len(), sealed_drained.len());
        assert!(outcome.reclaimed_bytes > 0);
        assert!(outcome.retained_referenced.is_empty());
        // The files are actually gone from disk, not just forgotten.
        for (_, file) in &sealed_drained {
            assert!(!file.exists(), "reclaimed file must disappear: {file:?}");
            assert!(
                !file.with_extension("xor").exists(),
                "reclaimed sidecar must disappear: {file:?}"
            );
        }
        // Live data reads exact; drained keys stay absent.
        assert_eq!(manager.get(b"live")?, Some(b"yes".to_vec()));
        assert_eq!(manager.get(b"drain-0000")?, None);
        let rows = manager.range(None, None)?;
        assert_eq!(rows, vec![(b"live".to_vec(), b"yes".to_vec())]);
        // Rerun is idempotent: nothing left to reclaim.
        let again = manager.reclaim_drained_segments(&std::collections::BTreeSet::new())?;
        assert!(again.reclaimed.is_empty());
        assert_eq!(again.reclaimed_bytes, 0);
        // Reopening rediscovers exactly the survivors and still reads live.
        drop(manager);
        let mut reopened = StorageManager::open(&path, 32, 1)?;
        assert_eq!(reopened.get(b"live")?, Some(b"yes".to_vec()));
        assert!(reopened.segment_count() < sealed_drained.len());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn reclaim_honors_pinned_segments_and_leaves_nonempty_ones() {
    use plomid_storage::manager::SegmentState;

    let path = root();
    let result = (|| {
        let mut manager = StorageManager::create(&path, 32, 1)?;
        manager.insert(b"keep-a", b"one")?;
        manager.insert(b"keep-b", b"two")?;
        manager.insert(b"keep-c", b"three")?;
        // Drain every sealed segment: the last insert (`keep-c`) sits in
        // the active tail, so deleting the older keys leaves all sealed
        // segments empty while one live key remains.
        manager.apply_batch(&[(b"keep-a".to_vec(), None), (b"keep-b".to_vec(), None)])?;
        manager.sync()?;
        assert_eq!(manager.get(b"keep-c")?, Some(b"three".to_vec()));
        let sealed: Vec<(u64, std::path::PathBuf)> = manager
            .segments()
            .filter(|segment| segment.state == SegmentState::Sealed)
            .map(|segment| (segment.id, segment.path.clone()))
            .collect();
        assert!(!sealed.is_empty());
        let by_id: std::collections::HashMap<u64, std::path::PathBuf> =
            sealed.iter().cloned().collect();
        let (pinned_id, pinned_file) = sealed[0].clone();

        // A pinned (lease-held) segment is retained even when drained,
        // and its file stays on disk, while every other drained sealed
        // segment is still reclaimed.
        let mut pinned = std::collections::BTreeSet::new();
        pinned.insert(pinned_id);
        let held = manager.reclaim_drained_segments(&pinned)?;
        assert_eq!(held.retained_referenced, vec![pinned_id]);
        assert!(pinned_file.exists());
        let mut expected: Vec<u64> = sealed
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| *id != pinned_id)
            .collect();
        expected.sort_unstable();
        assert_eq!(held.reclaimed, expected);
        for id in &held.reclaimed {
            assert!(
                !by_id[id].exists(),
                "reclaimed file must disappear: {:?}",
                by_id[id]
            );
        }
        // Live reads stay exact throughout.
        assert_eq!(manager.get(b"keep-c")?, Some(b"three".to_vec()));
        assert_eq!(manager.get(b"keep-a")?, None);
        // Unpinned, the held segment is reclaimed too; live data survives.
        let freed = manager.reclaim_drained_segments(&std::collections::BTreeSet::new())?;
        assert_eq!(freed.reclaimed, vec![pinned_id]);
        assert!(!pinned_file.exists());
        assert_eq!(manager.get(b"keep-c")?, Some(b"three".to_vec()));
        assert_eq!(manager.get(b"keep-b")?, None);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(path);
    assert!(result.is_ok(), "{result:?}");
}
