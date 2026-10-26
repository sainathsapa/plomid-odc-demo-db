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
// Shared-snapshot partitioned scan engine API regression tests.
//
// Covers the additive contract only: one captured handle shared across
// partitions, disjoint exactly-once coverage, per-partition order,
// snapshot isolation, early-exit parity, and pinned reads. Existing
// scan/commit/snapshot paths are untouched.
use plomid_storage::{SharedSnapshot, StorageEngine, StorageEngineTransaction};
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use plomid_wal::DataOperation;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn assert_send_sync<T: Send + Sync>() {}

/// Half-open key range `[start, end)` with unbounded ends as `None`.
type KeyBounds = Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>;

#[test]
fn snapshot_handles_are_send_sync() {
    assert_send_sync::<SharedSnapshot>();
    assert_send_sync::<plomid_mvcc::Snapshot>();
}

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-partitioned-scan-{label}-{}-{id}",
        std::process::id()
    ))
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("v{i:04}").into_bytes()
}

fn populate_single(engine: &mut PlomidStorageEngine, count: usize) {
    let mut txn = engine.begin().expect("begin");
    for i in 0..count {
        txn.put(&key(i), &value(i)).expect("put");
    }
    txn.commit().expect("commit");
}

fn commit_put(engine: &mut PlomidStorageEngine, k: &[u8], v: &[u8]) {
    let (txn, _) = engine.begin_concurrent().expect("begin");
    engine
        .commit_concurrent(
            txn,
            vec![DataOperation::Put {
                key: k.to_vec(),
                value: v.to_vec(),
            }],
        )
        .expect("commit");
}

fn commit_del(engine: &mut PlomidStorageEngine, k: &[u8]) {
    let (txn, _) = engine.begin_concurrent().expect("begin");
    engine
        .commit_concurrent(txn, vec![DataOperation::Delete { key: k.to_vec() }])
        .expect("commit");
}

fn concat_partitions<E: StorageEngine>(
    engine: &E,
    snapshot: &SharedSnapshot,
    splits: &[Vec<u8>],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    // Builds disjoint [start, end) partitions from split keys and joins them
    // in partition order (error determinism: first error in order wins).
    let mut bounds: KeyBounds = Vec::new();
    let mut prev: Option<Vec<u8>> = None;
    for split in splits {
        bounds.push((prev, Some(split.clone())));
        prev = Some(split.clone());
    }
    bounds.push((prev, None));
    let mut out = Vec::new();
    for (start, end) in &bounds {
        out.extend(
            engine
                .scan_shared(snapshot, start.as_deref(), end.as_deref())
                .expect("partition scan"),
        );
    }
    out
}

#[test]
fn single_engine_partition_concat_equivalence() {
    let dir = scratch("single-concat");
    let result = (|| -> plomid_core::Result<()> {
        let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
        populate_single(&mut engine, 20);
        let snapshot = engine.capture_shared_snapshot()?;
        let full = engine.scan_shared(&snapshot, None, None)?;
        assert_eq!(full.len(), 20);
        // Per-partition key order.
        let mut sorted = full.clone();
        sorted.sort();
        assert_eq!(full, sorted);

        // Two-partition splits: exact key boundary, mid-range, edges.
        for split in [key(0), key(1), key(7), key(19), key(20), b"m".to_vec()] {
            let left = engine.scan_shared(&snapshot, None, Some(&split))?;
            let right = engine.scan_shared(&snapshot, Some(&split), None)?;
            let mut joined = left;
            joined.extend(right);
            assert_eq!(joined, full, "split={split:?}");
        }
        // Multi-partition helper (three splits -> four partitions).
        let joined = concat_partitions(&engine, &snapshot, &[key(5), key(10), key(15)]);
        assert_eq!(joined, full);
        // Empty ranges.
        assert!(engine
            .scan_shared(&snapshot, Some(&key(5)), Some(&key(5)))?
            .is_empty());
        assert!(engine
            .scan_shared(&snapshot, Some(&key(30)), None)?
            .is_empty());
        assert!(engine
            .scan_shared(&snapshot, Some(&key(0)), Some(&key(0)))?
            .is_empty());
        // Single row and exact boundaries.
        assert_eq!(
            engine.scan_shared(&snapshot, Some(&key(5)), Some(&key(6)))?,
            vec![(key(5), value(5))]
        );
        assert_eq!(
            engine.scan_shared(&snapshot, Some(&key(5)), Some(&key(10)))?,
            (5..10).map(|i| (key(i), value(i))).collect::<Vec<_>>()
        );
        // Matches the fresh `scan` path when nothing changed mid-scan.
        assert_eq!(engine.scan(None, None)?, full);
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}

#[test]
fn single_engine_snapshot_isolation() {
    let dir = scratch("single-isolation");
    let result = (|| -> plomid_core::Result<()> {
        let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
        populate_single(&mut engine, 10);
        let snapshot = engine.capture_shared_snapshot()?;
        // Concurrent write (after capture) commits before partitions run.
        {
            let mut txn = engine.begin()?;
            txn.put(&key(10), &value(10))?;
            txn.delete(&key(0))?;
            txn.commit()?;
        }
        // Partitions under the old handle never see the write.
        let left = engine.scan_shared(&snapshot, None, Some(&key(5)))?;
        let right = engine.scan_shared(&snapshot, Some(&key(5)), None)?;
        let mut joined = left;
        joined.extend(right);
        assert_eq!(joined.len(), 10);
        assert!(joined.contains(&(key(0), value(0))));
        assert!(!joined.iter().any(|(k, _)| k == &key(10)));
        // A fresh handle observes the write.
        let fresh = engine.capture_shared_snapshot()?;
        let now = engine.scan_shared(&fresh, None, None)?;
        assert_eq!(now.len(), 10);
        assert!(!now.iter().any(|(k, _)| k == &key(0)));
        assert!(now.contains(&(key(10), value(10))));
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}

#[test]
fn single_engine_early_exit_parity() {
    let dir = scratch("single-early-exit");
    let result = (|| -> plomid_core::Result<()> {
        let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
        populate_single(&mut engine, 20);
        let snapshot = engine.capture_shared_snapshot()?;
        let full = engine.scan_shared(&snapshot, None, None)?;
        // Partitioned early-exit: stop after the first chunk.
        let mut first_chunk = Vec::new();
        engine.scan_shared_for_each(&snapshot, None, None, 5, &mut |chunk| {
            first_chunk.extend(chunk.to_vec());
            Ok(false)
        })?;
        assert_eq!(first_chunk, full[..5]);
        // Matches the `scan_for_each` early-exit prefix.
        let mut baseline = Vec::new();
        engine.scan_for_each(None, None, 5, &mut |chunk| {
            baseline.extend(chunk.to_vec());
            Ok(false)
        })?;
        assert_eq!(baseline, full[..5]);
        assert_eq!(first_chunk, baseline);
        // Full walk through the callback equals the materialized scan.
        let mut walked = Vec::new();
        engine.scan_shared_for_each(&snapshot, None, None, 6, &mut |chunk| {
            walked.extend(chunk.to_vec());
            Ok(true)
        })?;
        assert_eq!(walked, full);
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}

#[test]
fn single_engine_concurrent_workers_share_one_snapshot() {
    let dir = scratch("single-workers");
    let result = (|| -> plomid_core::Result<()> {
        let mut engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32)?;
        populate_single(&mut engine, 40);
        let engine = Arc::new(engine);
        let snapshot = engine.capture_shared_snapshot()?;
        let snapshot = Arc::new(snapshot);
        // Four disjoint partitions scanned from N threads via `&self`.
        let splits = [key(10), key(20), key(30)];
        let mut bounds: KeyBounds = Vec::new();
        let mut prev: Option<Vec<u8>> = None;
        for split in &splits {
            bounds.push((prev, Some(split.clone())));
            prev = Some(split.clone());
        }
        bounds.push((prev, None));
        let handles: Vec<_> = bounds
            .into_iter()
            .map(|(start, end)| {
                let engine = Arc::clone(&engine);
                let snapshot = Arc::clone(&snapshot);
                std::thread::spawn(move || {
                    engine.scan_shared(&snapshot, start.as_deref(), end.as_deref())
                })
            })
            .collect();
        // Join in partition order: error determinism by partition order.
        let mut joined = Vec::new();
        for handle in handles {
            joined.extend(handle.join().expect("worker panicked")?);
        }
        let full = engine.scan_shared(&snapshot, None, None)?;
        assert_eq!(joined, full);
        assert_eq!(joined.len(), 40);
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}

#[test]
fn concurrent_facade_partition_concat_and_isolation() {
    let dir = scratch("facade-concat");
    let result = (|| -> plomid_core::Result<()> {
        let backing = Arc::new(Mutex::new(PlomidStorageEngine::create(
            &dir,
            &dir.join("wal"),
            32,
        )?));
        {
            let mut guard = backing.lock().expect("lock");
            for i in 0..20 {
                commit_put(&mut guard, &key(i), &value(i));
            }
        }
        let facade = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
        let snapshot = facade.capture_shared_snapshot()?;
        let full = facade.scan_shared(&snapshot, None, None)?;
        assert_eq!(full.len(), 20);
        let joined = concat_partitions(&facade, &snapshot, &[key(5), key(10), key(15)]);
        assert_eq!(joined, full);
        // Empty, single-row, exact boundaries.
        assert!(facade
            .scan_shared(&snapshot, Some(&key(5)), Some(&key(5)))?
            .is_empty());
        assert_eq!(
            facade.scan_shared(&snapshot, Some(&key(5)), Some(&key(6)))?,
            vec![(key(5), value(5))]
        );
        // Write after capture stays invisible to old partitions.
        {
            let mut guard = backing.lock().expect("lock");
            commit_put(&mut guard, &key(20), &value(20));
            commit_del(&mut guard, &key(0));
        }
        let mut old = concat_partitions(&facade, &snapshot, &[key(10)]);
        old.sort();
        assert_eq!(old.len(), 20);
        assert!(old.contains(&(key(0), value(0))));
        let fresh = facade.capture_shared_snapshot()?;
        let now = facade.scan_shared(&fresh, None, None)?;
        assert_eq!(now.len(), 20);
        assert!(now.contains(&(key(20), value(20))));
        // Early-exit parity against `scan_for_each`.
        let mut shared_prefix = Vec::new();
        facade.scan_shared_for_each(&snapshot, None, None, 5, &mut |chunk| {
            shared_prefix.extend(chunk.to_vec());
            Ok(false)
        })?;
        let mut facade_for_each = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
        let mut baseline = Vec::new();
        facade_for_each.scan_for_each(None, None, 5, &mut |chunk| {
            baseline.extend(chunk.to_vec());
            Ok(true)
        })?;
        // The shared prefix is the old full scan's head (old snapshot);
        // the live `scan_for_each` prefix is the fresh scan's head.
        assert_eq!(shared_prefix, full[..5]);
        assert_eq!(&baseline[..5], &now[..5]);
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}

#[test]
fn concurrent_facade_workers_and_pinned_snapshot() {
    let dir = scratch("facade-pinned");
    let result = (|| -> plomid_core::Result<()> {
        let backing = Arc::new(Mutex::new(PlomidStorageEngine::create(
            &dir,
            &dir.join("wal"),
            32,
        )?));
        {
            let mut guard = backing.lock().expect("lock");
            commit_put(&mut guard, b"pin-k1", b"v1");
            commit_put(&mut guard, b"pin-k2", b"w1");
        }
        let facade = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
        let (txn_id, pinned) = facade.begin_pinned_read()?;
        facade.pin_read_snapshot(pinned.clone())?;
        assert!(facade.is_read_pinned());
        // Capture honors the pin: same owner/watermark/active as the pinned
        // snapshot, so partitions resolve under it.
        let captured = facade.capture_shared_snapshot()?;
        let expected = SharedSnapshot::new(
            pinned.owner().get(),
            pinned.watermark(),
            pinned.active().iter().copied().collect(),
        );
        assert_eq!(captured, expected);
        {
            let mut guard = backing.lock().expect("lock");
            commit_put(&mut guard, b"pin-k1", b"v2");
            commit_put(&mut guard, b"pin-k3", b"n3");
        }
        let rows = facade.scan_shared(&captured, None, None)?;
        assert_eq!(
            rows,
            vec![
                (b"pin-k1".to_vec(), b"v1".to_vec()),
                (b"pin-k2".to_vec(), b"w1".to_vec()),
            ]
        );
        // N workers share the pinned handle concurrently via `&self`.
        let facade = Arc::new(facade);
        let captured = Arc::new(captured);
        let worker = |start: Option<Vec<u8>>, end: Option<Vec<u8>>| {
            let facade = Arc::clone(&facade);
            let captured = Arc::clone(&captured);
            std::thread::spawn(move || {
                facade.scan_shared(&captured, start.as_deref(), end.as_deref())
            })
        };
        let a = worker(None, Some(b"pin-k2".to_vec()));
        let b = worker(Some(b"pin-k2".to_vec()), None);
        let mut joined = a.join().expect("worker")?;
        joined.extend(b.join().expect("worker")?);
        assert_eq!(
            joined,
            vec![
                (b"pin-k1".to_vec(), b"v1".to_vec()),
                (b"pin-k2".to_vec(), b"w1".to_vec()),
            ]
        );
        // Release returns to fresh reads.
        facade.unpin_read_snapshot()?;
        facade.abort_pinned_read(txn_id)?;
        assert!(!facade.is_read_pinned());
        let fresh = facade.capture_shared_snapshot()?;
        let now = facade.scan_shared(&fresh, None, None)?;
        assert!(now.contains(&(b"pin-k1".to_vec(), b"v2".to_vec())));
        assert!(now.contains(&(b"pin-k3".to_vec(), b"n3".to_vec())));
        Ok(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "failed: {result:?}");
}
