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
// Moved from `src/recovery.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::TxnId;
use plomid_wal::recovery::{encode_begin, encode_commit, encode_data, recover, DataOperation};
use plomid_wal::{RecordType, WalWriter};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-recovery-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn recovery_replays_only_committed_transaction() {
    let wal_path = temp_path("committed-wal");
    let storage_path = temp_path("committed-storage");
    let result = (|| {
        let mut tree = plomid_storage::BTree::create(&storage_path, 32)?;
        tree.sync()?;
        drop(tree);
        let mut writer = WalWriter::create(&wal_path)?;
        let txn = TxnId::new(1);
        writer.append(RecordType::Begin, &encode_begin(txn))?;
        writer.append(
            RecordType::Data,
            &encode_data(
                txn,
                &DataOperation::Put {
                    key: b"committed".to_vec(),
                    value: b"yes".to_vec(),
                },
            )?,
        )?;
        writer.append(RecordType::Commit, &encode_commit(txn))?;
        writer.sync()?;
        let report = recover(&wal_path, &storage_path, 32)?;
        assert_eq!(report.committed_transactions, 1);
        let mut tree = plomid_storage::BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"committed")?, Some(b"yes".to_vec()));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&wal_path);
    let _ = fs::remove_file(&storage_path);
    assert!(result.is_ok(), "committed recovery failed: {result:?}");
}

#[test]
fn recovery_ignores_uncommitted_transaction() {
    let wal_path = temp_path("uncommitted-wal");
    let storage_path = temp_path("uncommitted-storage");
    let result = (|| {
        let tree = plomid_storage::BTree::create(&storage_path, 32)?;
        drop(tree);
        let mut writer = WalWriter::create(&wal_path)?;
        let txn = TxnId::new(2);
        writer.append(RecordType::Begin, &encode_begin(txn))?;
        writer.append(
            RecordType::Data,
            &encode_data(
                txn,
                &DataOperation::Put {
                    key: b"uncommitted".to_vec(),
                    value: b"no".to_vec(),
                },
            )?,
        )?;
        writer.sync()?;
        let report = recover(&wal_path, &storage_path, 32)?;
        assert_eq!(report.committed_transactions, 0);
        let mut tree = plomid_storage::BTree::open(&storage_path, 32)?;
        assert_eq!(tree.get(b"uncommitted")?, None);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&wal_path);
    let _ = fs::remove_file(&storage_path);
    assert!(result.is_ok(), "uncommitted recovery failed: {result:?}");
}
