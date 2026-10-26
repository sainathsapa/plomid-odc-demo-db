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
// Moved from `src/store.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::TxnId;
use plomid_mvcc::store::*;
use plomid_mvcc::{RowVersion, Snapshot};
use std::collections::BTreeSet;

fn paging_snapshot() -> Snapshot {
    Snapshot::new(TxnId::new(999), 999, BTreeSet::new())
}

fn paged_collect(store: &VersionStore, snap: &Snapshot, chunk: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut bound = std::ops::Bound::Unbounded;
    loop {
        let (page, resume, completed) = store.scan_step(bound, None, snap, chunk);
        out.extend(page);
        if completed {
            break;
        }
        match resume {
            Some(key) => {
                bound = std::ops::Bound::Excluded(key);
            }
            None => break,
        }
    }
    out
}

#[test]
fn paged_scan_matches_full_scan_at_any_chunk_size() {
    let mut store = VersionStore::new();
    for i in 0..500 {
        store.install(
            format!("k{i:04}").into_bytes(),
            RowVersion::committed(TxnId::new(1), 1, vec![i as u8; 8]),
        );
    }
    // Every 7th key deleted; every 11th has an invisible newer version.
    for i in (0..500).step_by(7) {
        store.install(
            format!("k{i:04}").into_bytes(),
            RowVersion::deleted(TxnId::new(2), 2),
        );
    }
    let snap = paging_snapshot();
    let full = store.scan_visible(None, None, &snap);
    for chunk in [1, 7, 64, 1024, 10000] {
        assert_eq!(paged_collect(&store, &snap, chunk), full, "chunk={chunk}");
    }
    // Count agrees too.
    let total: u64 = {
        let mut total = 0;
        let mut bound = std::ops::Bound::Unbounded;
        loop {
            let (n, resume, completed) = store.count_step(bound, None, &snap, 64);
            total += n;
            if completed {
                break;
            }
            match resume {
                Some(key) => {
                    bound = std::ops::Bound::Excluded(key);
                }
                None => break,
            }
        }
        total
    };
    assert_eq!(total as usize, full.len());
}

#[test]
fn paged_scan_empty_and_singleton_ranges() {
    let store = VersionStore::new();
    let snap = paging_snapshot();
    let (rows, _, completed) = store.scan_step(std::ops::Bound::Unbounded, None, &snap, 10);
    assert!(rows.is_empty() && completed);
    let (n, _, completed) = store.count_step(std::ops::Bound::Unbounded, None, &snap, 10);
    assert_eq!((n, completed), (0, true));
}

#[test]
fn bounded_scan_preserves_key_order_and_excludes_tombstones() {
    let mut store = VersionStore::new();
    store.install(
        b"a".to_vec(),
        RowVersion::committed(TxnId::new(1), 1, b"a".to_vec()),
    );
    store.install(
        b"b".to_vec(),
        RowVersion::committed(TxnId::new(2), 2, b"b".to_vec()),
    );
    store.install(b"c".to_vec(), RowVersion::deleted(TxnId::new(3), 3));

    let snapshot = Snapshot::new(TxnId::new(99), 3, BTreeSet::new());
    assert_eq!(
        store.scan_visible(Some(b"a"), Some(b"c"), &snapshot),
        vec![
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"b".to_vec())
        ]
    );
}

#[test]
fn active_transaction_versions_remain_invisible_to_other_snapshots() {
    let mut store = VersionStore::new();
    store.install(
        b"key".to_vec(),
        RowVersion::committed(TxnId::new(1), 1, b"old".to_vec()),
    );
    store.install(
        b"key".to_vec(),
        RowVersion::committed(TxnId::new(2), 2, b"new".to_vec()),
    );
    let mut active = BTreeSet::new();
    active.insert(2);
    let snapshot = Snapshot::new(TxnId::new(3), 2, active);
    assert_eq!(
        store.visible(b"key", &snapshot).unwrap().payload,
        Some(b"old".to_vec())
    );
}
