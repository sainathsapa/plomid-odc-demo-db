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
// Moved from `src/allocator.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{CommitTimestamp, TxnId};
use plomid_txn::allocator::*;
use plomid_wal::RecordType;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-txn-{label}-{}-{id}", std::process::id()))
}

#[test]
fn first_txn_id_is_one() {
    let manager = TransactionManager::new();
    assert_eq!(manager.begin().unwrap().get(), 1);
}

#[test]
fn txn_ids_are_sequential() {
    let manager = TransactionManager::new();
    assert_eq!(manager.begin().unwrap().get(), 1);
    assert_eq!(manager.begin().unwrap().get(), 2);
    assert_eq!(manager.begin().unwrap().get(), 3);
}

#[test]
fn first_commit_timestamp_is_one() {
    let manager = TransactionManager::new();
    assert_eq!(manager.commit().unwrap().get(), 1);
}

#[test]
fn commit_timestamps_are_sequential() {
    let manager = TransactionManager::new();
    assert_eq!(manager.commit().unwrap().get(), 1);
    assert_eq!(manager.commit().unwrap().get(), 2);
    assert_eq!(manager.commit().unwrap().get(), 3);
}

#[test]
fn concurrent_txn_id_allocation_is_unique_and_ordered() {
    let manager = std::sync::Arc::new(TransactionManager::new());
    let thread_count = 32;
    let per_thread = 100;
    let mut handles = Vec::with_capacity(thread_count);
    let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    for _ in 0..thread_count {
        let manager = manager.clone();
        let results = results.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..per_thread {
                let id = manager.begin().unwrap();
                results.lock().unwrap().push(id.get());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut ids = results.lock().unwrap();
    assert_eq!(ids.len(), thread_count * per_thread);
    ids.sort_unstable();
    for (i, &id) in ids.iter().enumerate() {
        assert_eq!(
            id,
            (i + 1) as u64,
            "TxnIds must be a contiguous range starting at 1"
        );
    }
}

#[test]
fn concurrent_commit_timestamp_allocation_is_unique_and_ordered() {
    let manager = std::sync::Arc::new(TransactionManager::new());
    let thread_count = 32;
    let per_thread = 100;
    let mut handles = Vec::with_capacity(thread_count);
    let results = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    for _ in 0..thread_count {
        let manager = manager.clone();
        let results = results.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..per_thread {
                let ts = manager.commit().unwrap();
                results.lock().unwrap().push(ts.get());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let mut tss = results.lock().unwrap();
    assert_eq!(tss.len(), thread_count * per_thread);
    tss.sort_unstable();
    for (i, &ts) in tss.iter().enumerate() {
        assert_eq!(
            ts,
            (i + 1) as u64,
            "CommitTimestamps must be a contiguous range starting at 1"
        );
    }
}

#[test]
fn open_scans_wal_and_prevents_txn_id_reuse() {
    let wal_path = temp_path("txn-reuse");
    let result = (|| {
        {
            let mut writer = plomid_wal::WalWriter::create(&wal_path)?;
            for i in 1..=5u64 {
                let txn_id = TxnId::new(i);
                let ts = CommitTimestamp::new(i);
                writer.append(RecordType::Begin, &plomid_wal::encode_begin(txn_id))?;
                writer.append(
                    RecordType::Data,
                    &plomid_wal::encode_data(
                        txn_id,
                        &plomid_wal::DataOperation::Put {
                            key: format!("key-{i}").into_bytes(),
                            value: format!("value-{i}").into_bytes(),
                        },
                    )?,
                )?;
                writer.append(
                    RecordType::Commit,
                    &plomid_wal::encode_commit_with_timestamp(txn_id, ts),
                )?;
            }
            writer.sync()?;
        }

        let manager = TransactionManager::open(&wal_path)?;
        assert_eq!(manager.begin().unwrap().get(), 6);
        assert_eq!(manager.commit().unwrap().get(), 6);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&wal_path);
    assert!(result.is_ok(), "WAL scan restart test failed: {result:?}");
}

#[test]
fn open_after_partial_wal_does_not_reuse_aborted_ids() {
    let wal_path = temp_path("aborted-reuse");
    let result = (|| {
        {
            let mut writer = plomid_wal::WalWriter::create(&wal_path)?;
            let txn1 = TxnId::new(1);
            let txn2 = TxnId::new(2);
            writer.append(RecordType::Begin, &plomid_wal::encode_begin(txn1))?;
            writer.append(
                RecordType::Data,
                &plomid_wal::encode_data(
                    txn1,
                    &plomid_wal::DataOperation::Put {
                        key: b"committed".to_vec(),
                        value: b"yes".to_vec(),
                    },
                )?,
            )?;
            writer.append(
                RecordType::Commit,
                &plomid_wal::encode_commit_with_timestamp(txn1, CommitTimestamp::new(1)),
            )?;
            writer.append(RecordType::Begin, &plomid_wal::encode_begin(txn2))?;
            writer.append(
                RecordType::Data,
                &plomid_wal::encode_data(
                    txn2,
                    &plomid_wal::DataOperation::Put {
                        key: b"aborted".to_vec(),
                        value: b"no".to_vec(),
                    },
                )?,
            )?;
            writer.append(RecordType::Abort, &plomid_wal::encode_abort(txn2))?;
            writer.sync()?;
        }

        let manager = TransactionManager::open(&wal_path)?;
        let next = manager.begin().unwrap();
        assert!(next.get() > 2, "next TxnId must not reuse aborted ID");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&wal_path);
    assert!(result.is_ok(), "aborted ID reuse test failed: {result:?}");
}

#[test]
fn recovery_remains_compatible_with_extended_commit_payload() {
    let wal_path = temp_path("compat-commit");
    let storage_path = temp_path("compat-storage");
    let result = (|| {
        let mut tree = plomid_storage::BTree::create(&storage_path, 32)?;
        tree.sync()?;
        drop(tree);

        {
            let mut writer = plomid_wal::WalWriter::create(&wal_path)?;
            let txn = TxnId::new(1);
            writer.append(RecordType::Begin, &plomid_wal::encode_begin(txn))?;
            writer.append(
                RecordType::Data,
                &plomid_wal::encode_data(
                    txn,
                    &plomid_wal::DataOperation::Put {
                        key: b"key".to_vec(),
                        value: b"value".to_vec(),
                    },
                )?,
            )?;
            writer.append(
                RecordType::Commit,
                &plomid_wal::encode_commit_with_timestamp(txn, CommitTimestamp::new(1)),
            )?;
            writer.sync()?;
        }

        let report = plomid_wal::recover(&wal_path, &storage_path, 32)?;
        assert_eq!(report.committed_transactions, 1);
        let mut tree = plomid_storage::BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"key")?, Some(b"value".to_vec()));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&wal_path);
    let _ = fs::remove_file(&storage_path);
    assert!(
        result.is_ok(),
        "recovery compatibility test failed: {result:?}"
    );
}
