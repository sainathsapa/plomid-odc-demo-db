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
// Moved from `src/transaction.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::BTree;
use plomid_txn::transaction::*;
use plomid_txn::TransactionManager;
use plomid_wal::WalWriter;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    sync::Mutex,
    thread,
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-txn-{label}-{}-{id}", std::process::id()))
}

#[test]
fn put_transaction_survives_restart() {
    let wal_path = temp_path("put-restart");
    let storage_path = temp_path("put-restart-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.put(b"alpha", b"one")?;
            txn.put(b"beta", b"two")?;
            txn.commit()?;
            drop(tree);
            drop(wal);
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"alpha")?, Some(b"one".to_vec()));
        assert_eq!(tree.get(b"beta")?, Some(b"two".to_vec()));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("put-restart-wal"));
    let _ = fs::remove_file(temp_path("put-restart-storage"));
    assert!(
        result.is_ok(),
        "put transaction restart test failed: {result:?}"
    );
}

#[test]
fn delete_transaction_survives_restart() {
    let wal_path = temp_path("delete-restart-wal");
    let storage_path = temp_path("delete-restart-storage");
    let result = (|| {
        {
            let mut tree = BTree::create(&storage_path, 32)?;
            tree.insert(b"gamma", b"three")?;
            tree.sync()?;
            drop(tree);

            let manager = TransactionManager::new();
            let mut tree = BTree::open(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.delete(b"gamma")?;
            txn.commit()?;
            drop(tree);
            drop(wal);
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"gamma")?, None);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("delete-restart-wal"));
    let _ = fs::remove_file(temp_path("delete-restart-storage"));
    assert!(
        result.is_ok(),
        "delete transaction restart test failed: {result:?}"
    );
}

#[test]
fn aborted_transaction_does_not_appear_after_restart() {
    let wal_path = temp_path("aborted-restart-wal");
    let storage_path = temp_path("aborted-restart-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.put(b"aborted", b"should-not-appear")?;
            txn.abort()?;
            drop(tree);
            drop(wal);
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"aborted")?, None);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("aborted-restart-wal"));
    let _ = fs::remove_file(temp_path("aborted-restart-storage"));
    assert!(
        result.is_ok(),
        "aborted transaction restart test failed: {result:?}"
    );
}

#[test]
fn incomplete_transaction_does_not_appear_after_restart() {
    let wal_path = temp_path("incomplete-restart-wal");
    let storage_path = temp_path("incomplete-restart-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.put(b"incomplete", b"no-commit")?;
            drop(txn);
            drop(tree);
            drop(wal);
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"incomplete")?, None);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("incomplete-restart-wal"));
    let _ = fs::remove_file(temp_path("incomplete-restart-storage"));
    assert!(
        result.is_ok(),
        "incomplete transaction restart test failed: {result:?}"
    );
}

#[test]
fn mixed_committed_aborted_incomplete_transactions_survive_restart() {
    let wal_path = temp_path("mixed-restart");
    let storage_path = temp_path("mixed-restart-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;

            let mut txn1 = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn1.put(b"committed1", b"yes")?;
            txn1.commit()?;

            let mut txn2 = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn2.put(b"aborted", b"no")?;
            txn2.abort()?;

            let mut txn3 = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn3.put(b"incomplete", b"no")?;
            drop(txn3);

            let mut txn4 = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn4.put(b"committed2", b"yes")?;
            txn4.commit()?;

            drop(tree);
            drop(wal);
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"committed1")?, Some(b"yes".to_vec()));
        assert_eq!(tree.get(b"aborted")?, None);
        assert_eq!(tree.get(b"incomplete")?, None);
        assert_eq!(tree.get(b"committed2")?, Some(b"yes".to_vec()));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("mixed-restart-wal"));
    let _ = fs::remove_file(temp_path("mixed-restart-storage"));
    assert!(
        result.is_ok(),
        "mixed transaction restart test failed: {result:?}"
    );
}

#[test]
fn reopen_continues_allocating_unique_txn_ids() {
    let wal_path = temp_path("reopen-txn-wal");
    let storage_path = temp_path("reopen-txn-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.put(b"first", b"1")?;
            txn.commit()?;
            drop(tree);
            drop(wal);
        }

        let manager = TransactionManager::open(&wal_path)?;
        let mut tree = BTree::open(&storage_path, 32)?;
        let mut wal = WalWriter::open(&wal_path)?;
        let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
        assert!(
            txn.txn_id().get() > 1,
            "TxnId must not reuse prior IDs after reopen"
        );
        txn.put(b"second", b"2")?;
        txn.commit()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("reopen-txn-wal"));
    let _ = fs::remove_file(temp_path("reopen-txn-storage"));
    assert!(result.is_ok(), "reopen TxnId test failed: {result:?}");
}

#[test]
fn reopen_continues_allocating_unique_commit_timestamps() {
    let wal_path = temp_path("reopen-ts-wal");
    let storage_path = temp_path("reopen-ts-storage");
    let result = (|| {
        {
            let manager = TransactionManager::new();
            let mut tree = BTree::create(&storage_path, 32)?;
            let mut wal = WalWriter::create(&wal_path)?;
            let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
            txn.put(b"first", b"1")?;
            txn.commit()?;
            drop(tree);
            drop(wal);
        }

        let manager = TransactionManager::open(&wal_path)?;
        let mut tree = BTree::open(&storage_path, 32)?;
        let mut wal = WalWriter::open(&wal_path)?;
        let mut txn = Transaction::begin(&manager, &mut wal, &mut tree)?;
        txn.put(b"second", b"2")?;
        let result = txn.commit()?;
        assert!(
            result.commit_timestamp.get() > 1,
            "CommitTimestamp must not reuse prior values after reopen"
        );
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("reopen-ts-wal"));
    let _ = fs::remove_file(temp_path("reopen-ts-storage"));
    assert!(
        result.is_ok(),
        "reopen commit timestamp test failed: {result:?}"
    );
}

#[test]
fn commit_twice_is_rejected() {
    let manager = TransactionManager::new();
    let storage_path = temp_path("double-commit-storage");
    let wal_path = temp_path("double-commit-wal");
    let mut tree = BTree::create(&storage_path, 32).unwrap();
    let mut wal = WalWriter::create(&wal_path).unwrap();
    let mut txn = Transaction::begin(&manager, &mut wal, &mut tree).unwrap();
    txn.put(b"k", b"v").unwrap();
    txn.commit().unwrap();
    let error = txn.commit();
    assert!(error.is_err(), "double commit must fail");
    let _ = fs::remove_file(storage_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn abort_then_commit_is_rejected() {
    let manager = TransactionManager::new();
    let storage_path = temp_path("abort-commit-storage");
    let wal_path = temp_path("abort-commit-wal");
    let mut tree = BTree::create(&storage_path, 32).unwrap();
    let mut wal = WalWriter::create(&wal_path).unwrap();
    let mut txn = Transaction::begin(&manager, &mut wal, &mut tree).unwrap();
    txn.abort().unwrap();
    let error = txn.commit();
    assert!(error.is_err(), "commit after abort must fail");
    let _ = fs::remove_file(storage_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn write_after_commit_is_rejected() {
    let manager = TransactionManager::new();
    let storage_path = temp_path("post-commit-storage");
    let wal_path = temp_path("post-commit-wal");
    let mut tree = BTree::create(&storage_path, 32).unwrap();
    let mut wal = WalWriter::create(&wal_path).unwrap();
    let mut txn = Transaction::begin(&manager, &mut wal, &mut tree).unwrap();
    txn.commit().unwrap();
    let error = txn.put(b"k", b"v");
    assert!(error.is_err(), "put after commit must fail");
    let _ = fs::remove_file(storage_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn write_after_abort_is_rejected() {
    let manager = TransactionManager::new();
    let storage_path = temp_path("post-abort-storage");
    let wal_path = temp_path("post-abort-wal");
    let mut tree = BTree::create(&storage_path, 32).unwrap();
    let mut wal = WalWriter::create(&wal_path).unwrap();
    let mut txn = Transaction::begin(&manager, &mut wal, &mut tree).unwrap();
    txn.abort().unwrap();
    let error = txn.put(b"k", b"v");
    assert!(error.is_err(), "put after abort must fail");
    let _ = fs::remove_file(storage_path);
    let _ = fs::remove_file(wal_path);
}

#[test]
fn concurrent_transactions_serialized_via_mutex() {
    let wal_path = temp_path("concurrent-txn");
    let storage_path = temp_path("concurrent-txn-storage");
    let result = (|| {
        let manager = std::sync::Arc::new(TransactionManager::new());
        let wal = std::sync::Arc::new(Mutex::new(WalWriter::create(&wal_path)?));
        let tree = std::sync::Arc::new(Mutex::new(BTree::create(&storage_path, 32)?));
        let thread_count = 8;
        let per_thread = 10;
        let mut handles = Vec::with_capacity(thread_count);

        for t in 0..thread_count {
            let manager = manager.clone();
            let wal = wal.clone();
            let tree = tree.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    let mut wal_guard = wal.lock().unwrap();
                    let mut tree_guard = tree.lock().unwrap();
                    let mut txn =
                        Transaction::begin(&manager, &mut *wal_guard, &mut *tree_guard).unwrap();
                    let key = format!("t{t}-i{i}");
                    txn.put(key.as_bytes(), b"value").unwrap();
                    txn.commit().unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut tree = BTree::open(&storage_path, 32)?;
        for t in 0..thread_count {
            for i in 0..per_thread {
                let key = format!("t{t}-i{i}");
                assert_eq!(tree.get(key.as_bytes())?, Some(b"value".to_vec()));
            }
        }
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(temp_path("concurrent-txn-wal"));
    let _ = fs::remove_file(temp_path("concurrent-txn-storage"));
    assert!(
        result.is_ok(),
        "concurrent transaction test failed: {result:?}"
    );
}
