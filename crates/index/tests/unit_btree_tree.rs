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
// Moved from `src/btree/tree.rs` inline `#[cfg(test)]` — public-API only.
mod tests {
    use plomid_core::{GenerationId, IndexId, ObjectId, RowId};
    use plomid_index::btree::entry::{Bound, IndexEntry};
    use plomid_index::btree::tree::*;

    const POOL: usize = 64;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("plomid-btree-tests");
        std::fs::create_dir_all(&dir).expect("create test dir");
        dir.join(name)
    }

    struct Scratch {
        path: std::path::PathBuf,
    }

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = temp_path(name);
            let _ = std::fs::remove_file(&path);
            Self { path }
        }

        fn create(&self, unique: bool) -> BTreeIndex {
            BTreeIndex::create(
                &self.path,
                POOL,
                IndexId::new(1),
                ObjectId::new(1),
                GenerationId::new(1),
                unique,
            )
            .expect("create index")
        }

        fn open(&self) -> BTreeIndex {
            BTreeIndex::open(&self.path, POOL).expect("open index")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn rid(n: u64) -> RowId {
        RowId::new(n)
    }

    fn keys_of(entries: &[IndexEntry]) -> Vec<Vec<u8>> {
        entries.iter().map(|entry| entry.key.clone()).collect()
    }

    // -- basic ---------------------------------------------------------------

    #[test]
    fn empty_tree_lookups_and_scans() {
        let mut index = Scratch::new("empty").create(false);
        assert_eq!(index.entry_count(), 0);
        assert!(index.root_page().is_none());
        assert_eq!(index.lookup(b"missing").unwrap(), None);
        assert!(!index.contains(b"missing").unwrap());
        assert!(index.scan_all().unwrap().is_empty());
        assert!(index.first().unwrap().is_none());
        assert!(index.last().unwrap().is_none());
        assert!(index.scan_all_reverse().unwrap().is_empty());
        let stats = index.stats().unwrap();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.node_pages, 0);
    }

    /// Regression: the offset-based read path (used by `contains`/`lookup`)
    /// must agree with the full decode path, including across reopen, with
    /// duplicate-heavy keys, variable key sizes, and unchanged-outcome inserts
    /// that no longer rewrite pages.
    #[test]
    fn offset_lookup_matches_decode_across_reopen() {
        let path = temp_path("offset-regression");
        let _ = std::fs::remove_file(&path);
        let expected: Vec<(Vec<u8>, Vec<RowId>)> = (0..500_u64)
            .map(|n| {
                let key = {
                    let mut k = vec![b'x'; (n % 7) as usize];
                    k.extend_from_slice(&n.to_be_bytes());
                    k
                };
                let rows: Vec<RowId> = (0..(n % 4) + 1).map(|d| rid(n * 10 + d)).collect();
                (key, rows)
            })
            .collect();
        {
            let mut index = BTreeIndex::create(
                &path,
                64,
                IndexId::new(1),
                ObjectId::new(1),
                GenerationId::new(1),
                false,
            )
            .expect("create");
            for (key, rows) in &expected {
                for row in rows {
                    index.insert(key, *row).expect("insert");
                    // Re-inserting the same (key, row) is Unchanged and must
                    // leave the index readable and uncorrupted.
                    assert!(index.contains(key).expect("contains"));
                }
            }
            for (key, rows) in &expected {
                assert_eq!(index.lookup(key).expect("lookup"), Some(rows.clone()));
                assert!(index.contains(key).expect("contains"));
            }
            assert_eq!(index.entry_count(), expected.len() as u64);
            index.sync().expect("sync");
        }
        let mut reopened = BTreeIndex::open(&path, 64).expect("reopen");
        assert_eq!(reopened.entry_count(), expected.len() as u64);
        for (key, rows) in &expected {
            assert_eq!(reopened.lookup(key).expect("lookup"), Some(rows.clone()));
        }
        // The decode path must see exactly the same entries (key order).
        let mut expected_sorted = expected.clone();
        expected_sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let decoded: Vec<(Vec<u8>, Vec<RowId>)> = reopened
            .scan_all()
            .expect("scan")
            .into_iter()
            .map(|entry| (entry.key, entry.row_ids))
            .collect();
        assert_eq!(decoded, expected_sorted);
        reopened.close().expect("close");
        let _ = std::fs::remove_file(&path);
    }

    /// Regression: a sequential build must not degenerate into a linked list.
    /// A previous optimization double-popped the insert path on split,
    /// promoting separators one level too high and producing a depth-245 tree
    /// for 100K entries. The shape is asserted here via `stats()`.
    #[test]
    fn sequential_build_keeps_balanced_shape() {
        let path = temp_path("shape");
        let _ = std::fs::remove_file(&path);
        let mut index = BTreeIndex::create(
            &path,
            64,
            IndexId::new(1),
            ObjectId::new(1),
            GenerationId::new(1),
            false,
        )
        .expect("create");
        let entries = 20_000_u64;
        for n in 0..entries {
            index.insert(&n.to_be_bytes(), rid(n)).expect("insert");
        }
        let stats = index.stats().expect("stats");
        assert_eq!(stats.entry_count, entries);
        // ~819 8-byte entries per leaf: depth 1..=3 for this size. A degenerate
        // build reaches depth in the hundreds.
        assert!(stats.depth <= 4, "degenerate tree: {stats:?}");
        assert!(stats.node_pages < entries / 50, "sparse tree: {stats:?}");
        index.close().expect("close");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn single_key_round_trip() {
        let mut index = Scratch::new("single").create(false);
        assert_eq!(index.insert(b"k", rid(1)).unwrap(), InsertOutcome::Inserted);
        assert_eq!(index.lookup(b"k").unwrap(), Some(vec![rid(1)]));
        assert_eq!(
            index.insert(b"k", rid(1)).unwrap(),
            InsertOutcome::Unchanged
        );
        assert!(index.contains(b"k").unwrap());
        assert!(index.delete(b"k", None, true).unwrap());
        assert!(!index.contains(b"k").unwrap());
        assert_eq!(index.entry_count(), 0);
    }

    #[test]
    fn unique_index_rejects_duplicate_key() {
        let mut index = Scratch::new("unique-dup").create(true);
        index.insert(b"k", rid(1)).unwrap();
        let error = index.insert(b"k", rid(2)).unwrap_err();
        assert_eq!(error.kind(), plomid_core::ErrorKind::Conflict);
        assert_eq!(
            index.insert(b"k", rid(1)).unwrap(),
            InsertOutcome::Unchanged
        );
    }

    #[test]
    fn non_unique_index_appends_row_ids_in_order() {
        let mut index = Scratch::new("nonunique").create(false);
        for row in [rid(5), rid(1), rid(3)] {
            index.insert(b"k", row).unwrap();
        }
        assert_eq!(
            index.lookup(b"k").unwrap(),
            Some(vec![rid(1), rid(3), rid(5)])
        );
        assert!(index.delete(b"k", Some(rid(3)), false).unwrap());
        assert_eq!(index.lookup(b"k").unwrap(), Some(vec![rid(1), rid(5)]));
        assert!(index.delete(b"k", None, true).unwrap());
        assert_eq!(index.lookup(b"k").unwrap(), None);
    }

    #[test]
    fn delete_missing_key_is_a_no_op() {
        let mut index = Scratch::new("delete-missing").create(false);
        index.insert(b"present", rid(1)).unwrap();
        assert!(!index.delete(b"missing", None, true).unwrap());
        assert!(!index.delete(b"present", Some(rid(9)), false).unwrap());
        assert!(index.contains(b"present").unwrap());
    }

    // -- ordering ------------------------------------------------------------

    #[test]
    fn sequential_reverse_and_random_inserts_stay_ordered() {
        let mut index = Scratch::new("ordering").create(false);
        let mut expected = std::collections::BTreeSet::new();
        for n in 0..200_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
            expected.insert(n.to_be_bytes().to_vec());
        }
        for n in (0..200_u64).rev() {
            index.insert(&(1_000 + n).to_be_bytes(), rid(n)).unwrap();
            expected.insert((1_000 + n).to_be_bytes().to_vec());
        }
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        for _ in 0..300 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let n = (state >> 33) % 2_000;
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
            expected.insert(n.to_be_bytes().to_vec());
        }
        let scanned = index.scan_all().unwrap();
        assert_eq!(scanned.len(), expected.len());
        let keys = keys_of(&scanned);
        let expected: Vec<Vec<u8>> = expected.into_iter().collect();
        assert_eq!(keys, expected);
        let mut reversed = index.scan_all_reverse().unwrap();
        reversed.reverse();
        assert_eq!(keys_of(&reversed), keys);
        assert_eq!(index.entry_count(), expected.len() as u64);
    }

    #[test]
    fn binary_keys_and_mixed_key_sizes_order_lexicographically() {
        let mut index = Scratch::new("binary").create(false);
        let cases: Vec<Vec<u8>> = vec![
            vec![0x00],
            vec![0x00, 0x00],
            vec![0x00, 0x01],
            vec![0x7F],
            vec![0x80],
            vec![0xFF],
            vec![0xFF, 0xFF, 0xFF],
            vec![],
        ];
        for key in &cases {
            index.insert(key, rid(1)).unwrap();
        }
        let scanned = index.scan_all().unwrap();
        let keys = keys_of(&scanned);
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
        assert!(index.contains(&[]).unwrap());
    }

    // -- splits ---------------------------------------------------------------

    #[test]
    fn large_keys_force_leaf_and_cascading_splits() {
        let mut index = Scratch::new("splits").create(false);
        let value = 2_048_usize;
        for n in 0..200_u64 {
            let mut key = format!("key-{n:012}").into_bytes();
            key.resize(value, b'x');
            index.insert(&key, rid(n)).unwrap();
        }
        assert_eq!(index.entry_count(), 200);
        let stats = index.stats().unwrap();
        assert!(stats.depth > 1, "large keys must split the root");
        // Every key remains reachable.
        for n in 0..200_u64 {
            let mut key = format!("key-{n:012}").into_bytes();
            key.resize(value, b'x');
            assert!(index.contains(&key).unwrap(), "key {n} missing");
        }
    }

    #[test]
    fn root_split_creates_internal_root_with_two_leaves() {
        let mut index = Scratch::new("root-split").create(false);
        // Small keys still overflow one 16 KiB leaf after enough entries.
        for n in 0..1_000_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        let stats = index.stats().unwrap();
        assert!(stats.depth >= 2);
        assert!(index.contains(&499_u64.to_be_bytes()).unwrap());
        let scanned = index.scan_all().unwrap();
        assert_eq!(scanned.len(), 1_000);
    }

    #[test]
    fn leaf_chain_is_fully_linked_after_splits() {
        let mut index = Scratch::new("chain").create(false);
        for n in 0..800_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        // Forward chain walk must visit every key exactly once.
        let forward = index.scan_all().unwrap();
        assert_eq!(forward.len(), 800);
        assert_eq!(forward.first().unwrap().key, 0_u64.to_be_bytes().to_vec());
        assert_eq!(forward.last().unwrap().key, 799_u64.to_be_bytes().to_vec());
        let backward = index.scan_all_reverse().unwrap();
        assert_eq!(
            keys_of(&forward),
            keys_of(&backward).into_iter().rev().collect::<Vec<_>>()
        );
    }

    // -- deletes and rebalance ----------------------------------------------

    #[test]
    fn deleting_all_keys_leaves_a_valid_empty_tree() {
        let mut index = Scratch::new("drain").create(false);
        for n in 0..500_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        for n in 0..500_u64 {
            assert!(index.delete(&n.to_be_bytes(), None, true).unwrap());
        }
        assert_eq!(index.entry_count(), 0);
        assert!(index.root_page().is_none());
        assert!(index.scan_all().unwrap().is_empty());
        // The drained tree accepts inserts again.
        index.insert(&7_u64.to_be_bytes(), rid(7)).unwrap();
        assert!(index.contains(&7_u64.to_be_bytes()).unwrap());
    }

    #[test]
    fn deletion_collapses_to_sibling_and_contracts_root() {
        let mut index = Scratch::new("contract").create(false);
        for n in 0..600_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        // Delete the entire upper half: many leaves empty and merge upward.
        for n in 300..600_u64 {
            assert!(index.delete(&n.to_be_bytes(), None, true).unwrap());
        }
        assert_eq!(index.entry_count(), 300);
        let scanned = index.scan_all().unwrap();
        assert_eq!(scanned.len(), 300);
        assert_eq!(
            keys_of(&scanned),
            (0..300_u64)
                .map(|n| n.to_be_bytes().to_vec())
                .collect::<Vec<_>>()
        );
        // Continue draining: the root must eventually contract to nothing.
        for n in 0..300_u64 {
            assert!(index.delete(&n.to_be_bytes(), None, true).unwrap());
        }
        assert!(index.root_page().is_none());
    }

    // -- range scans ----------------------------------------------------------

    #[test]
    fn range_bound_algebra() {
        let mut index = Scratch::new("bounds").create(false);
        for n in 0..10_u64 {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        let key = |n: u64| n.to_be_bytes().to_vec();
        let closed = index
            .range_scan(Bound::Included(key(2)), Bound::Included(key(5)))
            .unwrap();
        assert_eq!(keys_of(&closed), vec![key(2), key(3), key(4), key(5)]);
        let half_open = index
            .range_scan(Bound::Included(key(2)), Bound::Excluded(key(5)))
            .unwrap();
        assert_eq!(keys_of(&half_open), vec![key(2), key(3), key(4)]);
        let open_open = index
            .range_scan(Bound::Excluded(key(2)), Bound::Excluded(key(5)))
            .unwrap();
        assert_eq!(keys_of(&open_open), vec![key(3), key(4)]);
        let open_closed = index
            .range_scan(Bound::Excluded(key(2)), Bound::Included(key(5)))
            .unwrap();
        assert_eq!(keys_of(&open_closed), vec![key(3), key(4), key(5)]);
        let lower_only = index
            .range_scan(Bound::Included(key(8)), Bound::Unbounded)
            .unwrap();
        assert_eq!(keys_of(&lower_only), vec![key(8), key(9)]);
        let upper_only = index
            .range_scan(Bound::Unbounded, Bound::Excluded(key(2)))
            .unwrap();
        assert_eq!(keys_of(&upper_only), vec![key(0), key(1)]);
        let empty = index
            .range_scan(Bound::Included(key(7)), Bound::Excluded(key(7)))
            .unwrap();
        assert!(empty.is_empty());
        let inverted = index
            .range_scan(Bound::Included(key(5)), Bound::Included(key(2)))
            .unwrap();
        assert!(inverted.is_empty());
        let below = index
            .range_scan(Bound::Included(vec![0u8; 1]), Bound::Excluded(key(2)))
            .unwrap();
        assert_eq!(keys_of(&below), vec![key(0), key(1)]);
        let above = index
            .range_scan(Bound::Included(key(9)), Bound::Unbounded)
            .unwrap();
        assert_eq!(keys_of(&above), vec![key(9)]);
    }

    #[test]
    fn bounds_and_first_last() {
        let mut index = Scratch::new("bound-points").create(false);
        for n in [10_u64, 20, 30, 40] {
            index.insert(&n.to_be_bytes(), rid(n)).unwrap();
        }
        let entry = index.lower_bound(&25_u64.to_be_bytes()).unwrap().unwrap();
        assert_eq!(entry.key, 30_u64.to_be_bytes().to_vec());
        let entry = index.lower_bound(&20_u64.to_be_bytes()).unwrap().unwrap();
        assert_eq!(entry.key, 20_u64.to_be_bytes().to_vec());
        let entry = index.upper_bound(&20_u64.to_be_bytes()).unwrap().unwrap();
        assert_eq!(entry.key, 30_u64.to_be_bytes().to_vec());
        assert!(index.upper_bound(&40_u64.to_be_bytes()).unwrap().is_none());
        assert_eq!(
            index.first().unwrap().unwrap().key,
            10_u64.to_be_bytes().to_vec()
        );
        assert_eq!(
            index.last().unwrap().unwrap().key,
            40_u64.to_be_bytes().to_vec()
        );
    }

    // -- persistence -----------------------------------------------------------

    #[test]
    fn state_survives_close_and_reopen() {
        let scratch = Scratch::new("reopen");
        {
            let mut index = scratch.create(false);
            for n in 0..300_u64 {
                index.insert(&n.to_be_bytes(), rid(n)).unwrap();
            }
            for n in 0..50_u64 {
                index.delete(&n.to_be_bytes(), None, true).unwrap();
            }
            index.sync().unwrap();
        }
        {
            let mut index = scratch.open();
            assert_eq!(index.entry_count(), 250);
            assert!(!index.contains(&10_u64.to_be_bytes()).unwrap());
            assert!(index.contains(&100_u64.to_be_bytes()).unwrap());
            index.insert(&1_000_u64.to_be_bytes(), rid(1_000)).unwrap();
            assert!(index.contains(&1_000_u64.to_be_bytes()).unwrap());
            assert!(index.delete(&100_u64.to_be_bytes(), None, true).unwrap());
            assert!(!index.contains(&100_u64.to_be_bytes()).unwrap());
            index.sync().unwrap();
        }
        {
            let mut index = scratch.open();
            assert_eq!(index.entry_count(), 250);
            let scanned = index.scan_all().unwrap();
            assert_eq!(scanned.len(), 250);
            assert!(scanned
                .iter()
                .zip(scanned.iter().skip(1))
                .all(|(a, b)| a.key < b.key));
        }
    }

    #[test]
    fn reopening_a_foreign_file_reports_corruption() {
        let scratch = Scratch::new("bad-magic");
        std::fs::write(&scratch.path, vec![0_u8; 16_384]).unwrap();
        let error = match BTreeIndex::open(&scratch.path, POOL) {
            Err(error) => error,
            Ok(_) => panic!("opening a file without root metadata must fail"),
        };
        assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    }

    // -- WAL recovery ------------------------------------------------------------

    fn append_txn(
        wal: &mut plomid_wal::SharedWal,
        txn: plomid_core::TxnId,
        timestamp: u64,
        puts: &[(&[u8], u64)],
        deletes: &[&[u8]],
    ) {
        wal.append(
            plomid_wal::RecordType::Begin,
            &plomid_wal::encode_begin(txn),
        )
        .unwrap();
        for (key, row) in puts {
            wal.append(
                plomid_wal::RecordType::Data,
                &plomid_wal::encode_data(
                    txn,
                    &plomid_wal::DataOperation::Put {
                        key: key.to_vec(),
                        value: row.to_le_bytes().to_vec(),
                    },
                )
                .unwrap(),
            )
            .unwrap();
        }
        for key in deletes {
            wal.append(
                plomid_wal::RecordType::Data,
                &plomid_wal::encode_data(
                    txn,
                    &plomid_wal::DataOperation::Delete { key: key.to_vec() },
                )
                .unwrap(),
            )
            .unwrap();
        }
        let lsn = wal
            .append(
                plomid_wal::RecordType::Commit,
                &plomid_wal::encode_commit_with_timestamp(
                    txn,
                    plomid_core::CommitTimestamp::new(timestamp),
                ),
            )
            .unwrap();
        wal.commit(lsn).unwrap();
    }

    #[test]
    fn committed_wal_transactions_replay_into_the_index() {
        let scratch = Scratch::new("wal-recovery");
        let wal_path = temp_path("wal-recovery.log");
        let _ = std::fs::remove_file(&wal_path);
        {
            let mut index = scratch.create(false);
            let mut wal = plomid_wal::SharedWal::create(&wal_path).unwrap();
            let txn1 = plomid_core::TxnId::new(1);
            let txn2 = plomid_core::TxnId::new(2);
            let txn3 = plomid_core::TxnId::new(3);
            append_txn(&mut wal, txn1, 1, &[(b"alpha", 11)], &[]);
            // Aborted transaction: must never reach the index.
            wal.append(
                plomid_wal::RecordType::Begin,
                &plomid_wal::encode_begin(txn3),
            )
            .unwrap();
            wal.append(
                plomid_wal::RecordType::Data,
                &plomid_wal::encode_data(
                    txn3,
                    &plomid_wal::DataOperation::Put {
                        key: b"aborted".to_vec(),
                        value: 99_u64.to_le_bytes().to_vec(),
                    },
                )
                .unwrap(),
            )
            .unwrap();
            let lsn = wal
                .append(
                    plomid_wal::RecordType::Abort,
                    &plomid_wal::encode_abort(txn3),
                )
                .unwrap();
            wal.commit(lsn).unwrap();
            append_txn(&mut wal, txn2, 2, &[(b"beta", 22)], &[]);
            drop(wal);

            // Replay through the existing plomid_wal recovery path.
            let report = plomid_wal::recover_target(&wal_path, &mut index).unwrap();
            assert_eq!(report.committed_transactions, 2);
            assert_eq!(report.applied_operations, 2);
            assert!(index.contains(b"alpha").unwrap());
            assert!(index.contains(b"beta").unwrap());
            assert!(!index.contains(b"aborted").unwrap());
            assert_eq!(index.lookup(b"alpha").unwrap(), Some(vec![rid(11)]));
        }
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn recovery_replays_splits_deterministically() {
        let scratch = Scratch::new("wal-splits");
        let wal_path = temp_path("wal-splits.log");
        let _ = std::fs::remove_file(&wal_path);
        {
            let mut index = scratch.create(false);
            let mut wal = plomid_wal::SharedWal::create(&wal_path).unwrap();
            for round in 0..3_u64 {
                let txn = plomid_core::TxnId::new(round + 1);
                let puts: Vec<(&[u8], u64)> = (0..300_u64)
                    .map(|n| {
                        let key = (round * 300 + n).to_be_bytes().to_vec();
                        (Box::leak(key.into_boxed_slice()) as &[u8], round * 300 + n)
                    })
                    .collect();
                append_txn(&mut wal, txn, round + 1, &puts, &[]);
            }
            drop(wal);

            let report = plomid_wal::recover_target(&wal_path, &mut index).unwrap();
            assert_eq!(report.applied_operations, 900);
            assert_eq!(index.entry_count(), 900);
            let stats = index.stats().unwrap();
            assert!(stats.depth >= 2, "900 entries must split the root");
            let scanned = index.scan_all().unwrap();
            assert_eq!(scanned.len(), 900);
        }
        let _ = std::fs::remove_file(&wal_path);
    }

    // -- transaction adapter -----------------------------------------------------

    #[test]
    fn datastore_adapter_round_trips_row_ids() {
        use plomid_txn::DataStore;
        let mut index = Scratch::new("datastore").create(false);
        // Trait calls are qualified because the inherent insert/delete take
        // RowIds while the adapter takes encoded values.
        DataStore::insert(&mut index, b"k1", &1_u64.to_le_bytes()).unwrap();
        DataStore::insert(&mut index, b"k2", &2_u64.to_le_bytes()).unwrap();
        assert_eq!(
            DataStore::get(&mut index, b"k1").unwrap(),
            Some(1_u64.to_le_bytes().to_vec())
        );
        assert_eq!(DataStore::get(&mut index, b"missing").unwrap(), None);
        let rows = DataStore::range(&mut index, Some(b"k1"), Some(b"k2")).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"k1".to_vec());
        assert!(DataStore::delete(&mut index, b"k1").unwrap());
        assert_eq!(DataStore::get(&mut index, b"k1").unwrap(), None);
        DataStore::sync(&mut index).unwrap();
    }
}

mod chunk_tests {
    use plomid_index::btree::tree::*;

    use plomid_core::{GenerationId, IndexId, ObjectId, RowId};
    use plomid_index::btree::entry::Bound;

    const POOL: usize = 32;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("plomid-chunk-{name}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn open_fresh(name: &str, unique: bool) -> (BTreeIndex, std::path::PathBuf) {
        let path = temp_path(name);
        let index = BTreeIndex::create(
            &path,
            POOL,
            IndexId::new(1),
            ObjectId::new(1),
            GenerationId::new(1),
            unique,
        )
        .expect("create");
        (index, path)
    }

    fn ascending(n: u64) -> Vec<RowId> {
        (0..n).map(RowId::new).collect()
    }

    #[test]
    fn oversized_posting_round_trips() {
        let (mut index, path) = open_fresh("roundtrip", false);
        for id in ascending(5000) {
            index.insert(b"big", id).unwrap();
        }
        let found = index.lookup(b"big").unwrap().expect("present");
        assert_eq!(found, ascending(5000));
        assert!(index.contains(b"big").unwrap());
        // Single delete keeps order and completeness.
        assert!(index.delete(b"big", Some(RowId::new(2500)), false).unwrap());
        let found = index.lookup(b"big").unwrap().expect("present");
        assert_eq!(found.len(), 4999);
        assert!(!found.contains(&RowId::new(2500)));
        assert!(found.windows(2).all(|w| w[0] < w[1]), "ids stay ordered");
        // Delete-all removes every chunk.
        assert!(index.delete(b"big", None, true).unwrap());
        assert_eq!(index.lookup(b"big").unwrap(), None);
        assert!(!index.contains(b"big").unwrap());
        assert_eq!(index.entry_count(), 0);
        index.close().expect("close");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn posting_chunks_span_nodes_and_merge() {
        let (mut index, path) = open_fresh("span", false);
        for id in ascending(20_000) {
            index.insert(b"hot", id).unwrap();
        }
        // Surrounding keys keep the run bracketed across splits.
        for suffix in [b"a".as_slice(), b"z".as_slice()] {
            index.insert(suffix, RowId::new(999_999)).unwrap();
        }
        let found = index.lookup(b"hot").unwrap().expect("present");
        assert_eq!(found, ascending(20_000));
        // Range scans merge chunks into one entry per key.
        let range = index
            .range_scan(
                Bound::Included(b"hot".to_vec()),
                Bound::Included(b"hot".to_vec()),
            )
            .unwrap();
        assert_eq!(range.len(), 1, "one merged entry per key");
        assert_eq!(range[0].row_ids, ascending(20_000));
        // Full scans (both directions) agree.
        let all = index.scan_all().unwrap();
        assert_eq!(all.len(), 3);
        let hot = all.iter().find(|entry| entry.key == b"hot").expect("hot");
        assert_eq!(hot.row_ids, ascending(20_000));
        let reversed = index.scan_all_reverse().unwrap();
        assert_eq!(reversed.len(), 3);
        let hot = reversed
            .iter()
            .find(|entry| entry.key == b"hot")
            .expect("hot");
        assert_eq!(hot.row_ids, ascending(20_000));
        // Bounds see the merged run.
        assert_eq!(
            index
                .lower_bound(b"hot")
                .unwrap()
                .expect("lower")
                .row_ids
                .len(),
            20_000
        );
        assert!(index.upper_bound(b"hot").unwrap().expect("upper").key != b"hot");
        index.close().expect("close");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn chunked_delete_keeps_order_and_counts() {
        let (mut index, path) = open_fresh("delorder", false);
        for id in ascending(6000) {
            index.insert(b"k", id).unwrap();
        }
        // Delete every third id.
        for id in (0..6000).step_by(3) {
            assert!(index.delete(b"k", Some(RowId::new(id)), false).unwrap());
        }
        let found = index.lookup(b"k").unwrap().expect("present");
        assert_eq!(found.len(), 4000);
        assert!(found.windows(2).all(|w| w[0] < w[1]));
        assert!(found.iter().all(|id| id.get() % 3 != 0));
        // Reopen: counts and content survive (entry_count verified).
        index.sync().expect("sync");
        index.close().expect("close");
        let mut reopened = BTreeIndex::open(&path, POOL).expect("reopen");
        let found = reopened.lookup(b"k").unwrap().expect("present");
        assert_eq!(found.len(), 4000);
        let stats = reopened.stats().expect("stats");
        assert!(
            stats.entry_count >= 1,
            "chunks counted, got {}",
            stats.entry_count
        );
        reopened.close().expect("close");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn oversized_key_still_errors() {
        let (mut index, path) = open_fresh("bigkey", false);
        let big_key = vec![7u8; 20_000];
        let error = index.insert(&big_key, RowId::new(1)).unwrap_err();
        assert_eq!(error.kind(), plomid_core::ErrorKind::InvalidArgument);
        let _ = index.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unique_indexes_never_chunk() {
        let (mut index, path) = open_fresh("unique", true);
        index.insert(b"k", RowId::new(1)).unwrap();
        assert_eq!(
            index.insert(b"k", RowId::new(1)).unwrap(),
            InsertOutcome::Unchanged
        );
        let error = index.insert(b"k", RowId::new(2)).unwrap_err();
        assert_eq!(error.kind(), plomid_core::ErrorKind::Conflict);
        assert_eq!(index.lookup(b"k").unwrap(), Some(vec![RowId::new(1)]));
        let _ = index.close();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn duplicate_insert_stays_idempotent_across_chunks() {
        let (mut index, path) = open_fresh("dupid", false);
        for id in ascending(3000) {
            index.insert(b"k", id).unwrap();
        }
        // Re-inserting every id must be a no-op, including ids in head chunks.
        for id in ascending(3000) {
            assert_eq!(index.insert(b"k", id).unwrap(), InsertOutcome::Unchanged);
        }
        assert_eq!(index.lookup(b"k").unwrap().expect("present").len(), 3000);
        let _ = index.close();
        let _ = std::fs::remove_file(&path);
    }
}

mod bulk_tests {
    use plomid_index::btree::tree::*;

    use plomid_core::{GenerationId, IndexId, ObjectId, RowId};

    const POOL: usize = 32;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("plomid-bulk-{name}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn open_fresh(name: &str) -> (BTreeIndex, std::path::PathBuf) {
        let path = temp_path(name);
        let index = BTreeIndex::create(
            &path,
            POOL,
            IndexId::new(1),
            ObjectId::new(1),
            GenerationId::new(1),
            false,
        )
        .expect("create");
        (index, path)
    }

    /// 25-key, 800-rows-each build: bulk construction must resolve
    /// identically to per-row insertion across every read path.
    fn bulk_rows() -> Vec<(Vec<u8>, RowId)> {
        let mut rows = Vec::with_capacity(20_000);
        for id in 0..20_000_u64 {
            rows.push((format!("k-{:02}", id % 25).into_bytes(), RowId::new(id)));
        }
        rows
    }

    #[test]
    fn bulk_build_matches_incremental_insert() {
        let (mut incremental, incremental_path) = open_fresh("inc");
        let (mut bulk, bulk_path) = open_fresh("blk");
        let rows = bulk_rows();
        for (key, id) in &rows {
            incremental.insert(key, *id).unwrap();
        }
        bulk.build_bulk(rows).unwrap();
        for suffix in 0..25_u32 {
            let key = format!("k-{suffix:02}");
            assert_eq!(
                incremental.lookup(key.as_bytes()).unwrap(),
                bulk.lookup(key.as_bytes()).unwrap(),
                "lookup must match for {key}"
            );
        }
        assert_eq!(
            incremental.scan_all().unwrap(),
            bulk.scan_all().unwrap(),
            "forward scans must match"
        );
        assert_eq!(
            incremental.scan_all_reverse().unwrap(),
            bulk.scan_all_reverse().unwrap(),
            "reverse scans must match"
        );
        assert_eq!(incremental.entry_count(), bulk.entry_count());
        let _ = incremental.close();
        let _ = bulk.close();
        let _ = std::fs::remove_file(&incremental_path);
        let _ = std::fs::remove_file(&bulk_path);
    }

    /// Bulk-built trees survive close/reopen with identical content, and
    /// support deletes (including middle-node partial deletes) afterwards.
    #[test]
    fn bulk_build_reopens_and_deletes() {
        let (mut bulk, path) = open_fresh("reopen");
        bulk.build_bulk(bulk_rows()).unwrap();
        bulk.sync().unwrap();
        bulk.close().unwrap();
        let mut reopened = BTreeIndex::open(&path, POOL).unwrap();
        let found = reopened.lookup(b"k-07").unwrap().expect("present");
        assert_eq!(found.len(), 800);
        // Delete every third id of one key across chunks and nodes.
        for id in (7..20_000_u64).step_by(25 * 3) {
            assert!(reopened
                .delete(b"k-07", Some(RowId::new(id)), false)
                .unwrap());
        }
        let found = reopened.lookup(b"k-07").unwrap().expect("present");
        assert!(found.windows(2).all(|w| w[0] < w[1]), "order kept");
        assert!(found.iter().all(|id| (id.get() - 7) % 75 != 0));
        reopened.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    /// Unique keys and single-key trees pack correctly, including the
    /// single-leaf root case and out-of-order input.
    #[test]
    fn bulk_build_unique_and_single_key() {
        let (mut bulk, path) = open_fresh("uniq");
        let mut rows: Vec<(Vec<u8>, RowId)> = (0..5000_u64)
            .map(|id| (format!("u-{id:05}").into_bytes(), RowId::new(5000 - id)))
            .collect();
        rows.reverse();
        bulk.build_bulk(rows).unwrap();
        assert_eq!(bulk.entry_count(), 5000);
        assert_eq!(
            bulk.lookup(b"u-01234").unwrap(),
            Some(vec![RowId::new(5000 - 1234)])
        );
        assert_eq!(
            bulk.lower_bound(b"u-01234").unwrap().expect("lower").key,
            b"u-01234".to_vec()
        );
        bulk.close().unwrap();
        let _ = std::fs::remove_file(&path);

        let (mut single, single_path) = open_fresh("single");
        single
            .build_bulk(vec![(b"only".to_vec(), RowId::new(9))])
            .unwrap();
        assert_eq!(single.lookup(b"only").unwrap(), Some(vec![RowId::new(9)]));
        single.close().unwrap();
        let _ = std::fs::remove_file(&single_path);
    }

    /// Empty input yields an empty tree, not an error.
    #[test]
    fn bulk_build_empty_is_empty() {
        let (mut bulk, path) = open_fresh("empty");
        bulk.build_bulk(Vec::new()).unwrap();
        assert_eq!(bulk.entry_count(), 0);
        assert_eq!(bulk.lookup(b"anything").unwrap(), None);
        bulk.close().unwrap();
        let _ = std::fs::remove_file(&path);
    }
}
