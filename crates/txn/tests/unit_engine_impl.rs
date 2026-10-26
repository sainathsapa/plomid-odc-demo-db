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
// Moved from `src/engine_impl.rs` inline `#[cfg(test)]` — public-API only.
mod tests {
    use plomid_core::Result;
    use plomid_storage::TransactionState;
    use plomid_storage::{StorageEngine, StorageEngineTransaction};
    use plomid_txn::engine_impl::*;
    use std::{
        fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    fn temp_path(label: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("plomid-engine-{label}-{}-{id}", std::process::id()))
    }

    fn cleanup(paths: &[&Path]) {
        for path in paths {
            if path.is_dir() {
                let _ = fs::remove_dir_all(path);
            } else {
                let _ = fs::remove_file(path);
            }
        }
    }

    /// A transaction whose records exceed the WAL segment target must stay in
    /// one segment: the segment overshoots instead of rotating inside the
    /// transaction.
    ///
    /// This used to assert the opposite (a transaction spanning segments), which
    /// is what broke reclamation: once a `Begin` and its `Commit` live in
    /// different segments, no whole segment can be removed without stranding one
    /// of them, and recovery rejects a `Data`/`Commit` whose `Begin` is gone.
    /// Rotation therefore resumes between transactions, which this test also
    /// checks, together with the fact that both transactions survive a reopen.
    #[test]
    fn oversized_transaction_is_not_split_across_wal_segments() {
        let storage_path = temp_path("segmented-transaction");
        let wal_path = temp_path("segmented-transaction-wal");
        let result = (|| {
            let mut engine =
                PlomidStorageEngine::create_with_config(&storage_path, &wal_path, 32, 64, 500)?;
            let mut txn = engine.begin()?;
            for index in 0..8 {
                let key = format!("key-{index}");
                txn.put(key.as_bytes(), b"segment-value")?;
            }
            txn.commit()?;
            assert!(engine.storage.segment_count() >= 2);
            assert_eq!(
                engine.wal.segment_count(),
                1,
                "a transaction larger than the segment target stays in one segment"
            );
            drop(engine);

            // A second transaction rotates the WAL between transaction
            // boundaries, so the size target still governs segment count.
            {
                let mut engine =
                    PlomidStorageEngine::open_with_config(&storage_path, &wal_path, 32, 64, 500)?;
                let mut txn = engine.begin()?;
                for index in 8..16 {
                    let key = format!("key-{index}");
                    txn.put(key.as_bytes(), b"segment-value")?;
                }
                txn.commit()?;
                assert!(
                    engine.wal.segment_count() > 1,
                    "rotation must resume between transactions, count={}",
                    engine.wal.segment_count()
                );
            }

            let mut reopened =
                PlomidStorageEngine::open_with_config(&storage_path, &wal_path, 32, 64, 500)?;
            for index in 0..16 {
                let key = format!("key-{index}");
                assert_eq!(
                    reopened.get(key.as_bytes())?,
                    Some(b"segment-value".to_vec())
                );
            }
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "segmented transaction failed: {result:?}");
    }

    #[test]
    fn checkpoint_survives_restart_without_losing_data() {
        let storage_path = temp_path("checkpoint");
        let wal_path = temp_path("checkpoint-wal");
        let result = (|| {
            let checkpoint_lsn;
            {
                let mut engine =
                    PlomidStorageEngine::create_with_config(&storage_path, &wal_path, 32, 64, 500)?;
                let mut txn = engine.begin()?;
                txn.put(b"checkpointed", b"value")?;
                txn.commit()?;
                checkpoint_lsn = engine.checkpoint()?;
                assert_eq!(engine.wal.checkpoint_lsn()?, Some(checkpoint_lsn));
                assert_eq!(engine.wal.segment_count(), 1);
            }
            let mut reopened =
                PlomidStorageEngine::open_with_config(&storage_path, &wal_path, 32, 64, 500)?;
            assert_eq!(reopened.get(b"checkpointed")?, Some(b"value".to_vec()));
            assert_eq!(reopened.wal.checkpoint_lsn()?, Some(checkpoint_lsn));
            let txn = reopened.begin()?;
            assert_eq!(txn.txn_id().get(), 2);
            drop(txn);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "checkpoint recovery failed: {result:?}");
    }

    #[test]
    fn engine_open_creates_storage() {
        let storage_path = temp_path("open-creates");
        let wal_path = temp_path("open-creates-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"key", b"value")?;
            txn.commit()?;
            drop(engine);

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            assert_eq!(engine.get(b"key")?, Some(b"value".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "engine_open_creates_storage failed: {result:?}"
        );
    }

    #[test]
    fn begin_transaction() {
        let storage_path = temp_path("begin-txn");
        let wal_path = temp_path("begin-txn-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            assert_eq!(txn.state(), TransactionState::Active);
            assert_eq!(txn.txn_id().get(), 1);
            txn.abort()?;
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "begin_transaction failed: {result:?}");
    }

    #[test]
    fn put_and_commit() {
        let storage_path = temp_path("put-commit");
        let wal_path = temp_path("put-commit-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"alpha", b"one")?;
            txn.commit()?;
            assert_eq!(engine.get(b"alpha")?, Some(b"one".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "put_and_commit failed: {result:?}");
    }

    #[test]
    fn get_committed_value() {
        let storage_path = temp_path("get-committed");
        let wal_path = temp_path("get-committed-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"key", b"value")?;
            txn.commit()?;
            assert_eq!(engine.get(b"key")?, Some(b"value".to_vec()));
            assert_eq!(engine.get(b"missing")?, None);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "get_committed_value failed: {result:?}");
    }

    #[test]
    fn delete_and_commit() {
        let storage_path = temp_path("delete-commit");
        let wal_path = temp_path("delete-commit-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"gamma", b"three")?;
            txn.commit()?;
            assert_eq!(engine.get(b"gamma")?, Some(b"three".to_vec()));

            let mut txn = engine.begin()?;
            txn.delete(b"gamma")?;
            txn.commit()?;
            assert_eq!(engine.get(b"gamma")?, None);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "delete_and_commit failed: {result:?}");
    }

    #[test]
    fn abort_does_not_persist() {
        let storage_path = temp_path("abort-no-persist");
        let wal_path = temp_path("abort-no-persist-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"aborted", b"should-not-appear")?;
            txn.abort()?;
            assert_eq!(engine.get(b"aborted")?, None);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "abort_does_not_persist failed: {result:?}");
    }

    #[test]
    fn multiple_operations_in_one_transaction() {
        let storage_path = temp_path("multi-ops");
        let wal_path = temp_path("multi-ops-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"a", b"1")?;
            txn.put(b"b", b"2")?;
            txn.put(b"c", b"3")?;
            txn.delete(b"b")?;
            txn.commit()?;
            assert_eq!(engine.get(b"a")?, Some(b"1".to_vec()));
            assert_eq!(engine.get(b"b")?, None);
            assert_eq!(engine.get(b"c")?, Some(b"3".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "multiple_operations_in_one_transaction failed: {result:?}"
        );
    }

    #[test]
    fn committed_data_survives_engine_reopen() {
        let storage_path = temp_path("committed-reopen");
        let wal_path = temp_path("committed-reopen-wal");
        let result = (|| {
            {
                let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
                let mut txn = engine.begin()?;
                txn.put(b"survive", b"yes")?;
                txn.commit()?;
                drop(engine);
            }

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            assert_eq!(engine.get(b"survive")?, Some(b"yes".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "committed_data_survives_engine_reopen failed: {result:?}"
        );
    }

    #[test]
    fn aborted_data_does_not_survive_reopen() {
        let storage_path = temp_path("aborted-reopen");
        let wal_path = temp_path("aborted-reopen-wal");
        let result = (|| {
            {
                let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
                let mut txn = engine.begin()?;
                txn.put(b"aborted", b"no")?;
                txn.abort()?;
                drop(engine);
            }

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            assert_eq!(engine.get(b"aborted")?, None);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "aborted_data_does_not_survive_reopen failed: {result:?}"
        );
    }

    #[test]
    fn incomplete_transaction_does_not_survive_recovery() {
        let storage_path = temp_path("incomplete-recovery");
        let wal_path = temp_path("incomplete-recovery-wal");
        let result = (|| {
            {
                let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
                let mut txn = engine.begin()?;
                txn.put(b"incomplete", b"no-commit")?;
                drop(txn);
                drop(engine);
            }

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            assert_eq!(engine.get(b"incomplete")?, None);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "incomplete_transaction_does_not_survive_recovery failed: {result:?}"
        );
    }

    #[test]
    fn transaction_state_errors_are_preserved() {
        let storage_path = temp_path("state-errors");
        let wal_path = temp_path("state-errors-wal");
        let result: Result<()> = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.commit()?;
            let err = txn.put(b"k", b"v");
            assert!(err.is_err(), "put after commit must fail");
            let mut txn = engine.begin()?;
            txn.abort()?;
            let err = txn.commit();
            assert!(err.is_err(), "commit after abort must fail");
            Ok(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "transaction_state_errors_are_preserved failed: {result:?}"
        );
    }

    #[test]
    fn transaction_ids_remain_unique_across_reopen() {
        let storage_path = temp_path("unique-txn-reopen");
        let wal_path = temp_path("unique-txn-reopen-wal");
        let result = (|| {
            {
                let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
                let mut txn = engine.begin()?;
                txn.put(b"first", b"1")?;
                txn.commit()?;
                drop(engine);
            }

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            let txn = engine.begin()?;
            assert!(
                txn.txn_id().get() > 1,
                "TxnId must not reuse prior IDs after reopen"
            );
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "transaction_ids_remain_unique_across_reopen failed: {result:?}"
        );
    }

    #[test]
    fn commit_timestamps_remain_unique_across_reopen() {
        let storage_path = temp_path("unique-ts-reopen");
        let wal_path = temp_path("unique-ts-reopen-wal");
        let result = (|| {
            {
                let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
                let mut txn = engine.begin()?;
                txn.put(b"first", b"1")?;
                txn.commit()?;
                drop(engine);
            }

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"second", b"2")?;
            let result = txn.commit()?;
            assert!(
                result.commit_timestamp.get() > 1,
                "CommitTimestamp must not reuse prior values after reopen"
            );
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "commit_timestamps_remain_unique_across_reopen failed: {result:?}"
        );
    }

    #[test]
    fn direct_btree_access_is_not_required_by_facade_users() {
        let storage_path = temp_path("no-btree");
        let wal_path = temp_path("no-btree-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"facade", b"only")?;
            txn.commit()?;
            assert_eq!(engine.get(b"facade")?, Some(b"only".to_vec()));
            let entries = engine.scan(None, None)?;
            assert_eq!(entries, vec![(b"facade".to_vec(), b"only".to_vec())]);
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "direct_BTree_access_is_not_required_by_facade_users failed: {result:?}"
        );
    }

    #[test]
    fn scan_returns_committed_data() {
        let storage_path = temp_path("scan");
        let wal_path = temp_path("scan-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"a", b"1")?;
            txn.put(b"b", b"2")?;
            txn.put(b"c", b"3")?;
            txn.commit()?;

            let all = engine.scan(None, None)?;
            assert_eq!(all.len(), 3);

            let range = engine.scan(Some(b"b"), Some(b"c"))?;
            assert_eq!(range.len(), 1);
            assert_eq!(range[0], (b"b".to_vec(), b"2".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "scan_returns_committed_data failed: {result:?}"
        );
    }

    #[test]
    fn mixed_committed_aborted_incomplete_via_facade() {
        let storage_path = temp_path("mixed-facade");
        let wal_path = temp_path("mixed-facade-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;

            let mut txn1 = engine.begin()?;
            txn1.put(b"committed1", b"yes")?;
            txn1.commit()?;

            let mut txn2 = engine.begin()?;
            txn2.put(b"aborted", b"no")?;
            txn2.abort()?;

            let mut txn3 = engine.begin()?;
            txn3.put(b"incomplete", b"no")?;
            drop(txn3);

            let mut txn4 = engine.begin()?;
            txn4.put(b"committed2", b"yes")?;
            txn4.commit()?;

            drop(engine);

            let mut engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            assert_eq!(engine.get(b"committed1")?, Some(b"yes".to_vec()));
            assert_eq!(engine.get(b"aborted")?, None);
            assert_eq!(engine.get(b"incomplete")?, None);
            assert_eq!(engine.get(b"committed2")?, Some(b"yes".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "mixed_committed_aborted_incomplete_via_facade failed: {result:?}"
        );
    }

    #[test]
    fn sync_is_callable_between_transactions() {
        let storage_path = temp_path("sync-between");
        let wal_path = temp_path("sync-between-wal");
        let result = (|| {
            let mut engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut txn = engine.begin()?;
            txn.put(b"synced", b"data")?;
            txn.commit()?;
            engine.sync()?;
            assert_eq!(engine.get(b"synced")?, Some(b"data".to_vec()));
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(
            result.is_ok(),
            "sync_is_callable_between_transactions failed: {result:?}"
        );
    }
}

mod cap_tests {
    use plomid_core::Result;
    use plomid_storage::{StorageEngine, StorageEngineTransaction};
    use plomid_txn::concurrent::ConcurrentPlomidStorageEngine;
    use plomid_txn::engine_impl::*;
    use plomid_txn::CheckpointTrigger;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    fn cap_temp_path(label: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "plomid-engine-cap-{label}-{}-{id}",
            std::process::id()
        ))
    }

    fn cleanup(paths: &[&std::path::Path]) {
        for path in paths {
            let _ = std::fs::remove_dir_all(path);
        }
    }

    fn pin_of(backing: &Arc<Mutex<PlomidStorageEngine>>) -> Result<Option<u64>> {
        backing
            .lock()
            .map_err(|_| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "engine lock poisoned",
                )
            })?
            .manager
            .oldest_unfinished_lsn()
    }

    #[test]
    fn checkpoint_caps_boundary_below_open_transaction() {
        let storage_path = cap_temp_path("cap-begin");
        let wal_path = cap_temp_path("cap-begin-wal");
        let result = (|| {
            let backing = Arc::new(Mutex::new(PlomidStorageEngine::create(
                &storage_path,
                &wal_path,
                64,
            )?));
            let mut facade = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
            // An open (begun, unpublished) transaction pins the checkpoint
            // boundary below its first record, even before it stages writes.
            let mut txn = facade.begin()?;
            let pin = pin_of(&backing)?.expect("open txn must pin");
            let capped = backing.lock().map_err(|_| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "engine lock poisoned",
                )
            })?;
            // NB: checkpoint needs &mut engine; take it in a short scope.
            drop(capped);
            let mut guard = backing.lock().map_err(|_| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "engine lock poisoned",
                )
            })?;
            let capped = guard.checkpoint_with_report(CheckpointTrigger::Explicit)?;
            drop(guard);
            assert!(
                capped.lsn.get() < pin,
                "checkpoint boundary must stay below the open transaction"
            );
            // Publishing releases the pin: the next checkpoint advances past.
            txn.put(b"capped", b"1")?;
            txn.commit()?;
            assert!(pin_of(&backing)?.is_none());
            let mut guard = backing.lock().map_err(|_| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "engine lock poisoned",
                )
            })?;
            let advanced = guard.checkpoint_with_report(CheckpointTrigger::Explicit)?;
            drop(guard);
            assert!(
                advanced.lsn >= capped.lsn,
                "boundary must advance after publish"
            );
            let mut facade2 = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
            let mut txn2 = facade2.begin()?;
            assert_eq!(txn2.get(b"capped")?, Some(b"1".to_vec()));
            txn2.abort()?;
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "cap test failed: {result:?}");
    }

    #[test]
    fn checkpoint_advances_after_open_transaction_aborts() {
        let storage_path = cap_temp_path("cap-abort");
        let wal_path = cap_temp_path("cap-abort-wal");
        let result = (|| {
            let backing = Arc::new(Mutex::new(PlomidStorageEngine::create(
                &storage_path,
                &wal_path,
                64,
            )?));
            let mut facade = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
            let mut txn = facade.begin()?;
            txn.put(b"doomed", b"1")?;
            txn.abort()?;
            assert!(pin_of(&backing)?.is_none());
            let mut guard = backing.lock().map_err(|_| {
                plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::Internal,
                    "engine lock poisoned",
                )
            })?;
            let outcome = guard.checkpoint_with_report(CheckpointTrigger::Explicit)?;
            drop(guard);
            assert!(outcome.lsn.get() > 0);
            let mut facade2 = ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&backing))?;
            let mut txn2 = facade2.begin()?;
            assert_eq!(txn2.get(b"doomed")?, None);
            txn2.abort()?;
            Ok::<(), plomid_core::PlomidError>(())
        })();
        cleanup(&[&storage_path, &wal_path]);
        assert!(result.is_ok(), "cap-abort test failed: {result:?}");
    }
}
