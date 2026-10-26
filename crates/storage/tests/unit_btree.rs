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
// Moved from `src/btree.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::PageId;
use plomid_storage::btree::choose_internal_split;
use plomid_storage::btree::leaf_payload_end;
use plomid_storage::btree::BTree;
use plomid_storage::PAGE_DATA_SIZE;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

#[test]
fn leaf_payload_end_matches_hand_bytes() {
    let mut data = vec![0u8; 16332];
    data[0..4].copy_from_slice(b"PLBT");
    data[4] = 1;
    data[6..8].copy_from_slice(&1u16.to_le_bytes());
    data[8..16].copy_from_slice(&[0xff; 8]);
    data[16..18].copy_from_slice(&4u16.to_le_bytes());
    data[18..22].copy_from_slice(b"beta");
    data[22..26].copy_from_slice(&3u32.to_le_bytes());
    data[26..29].copy_from_slice(b"two");
    // Bisect: guard components first.
    assert!(data.len() >= 16, "len guard");
    assert_eq!(&data[..4], b"PLBT", "magic guard");
    assert_eq!(data[4], 1, "kind guard");
    assert_eq!(leaf_payload_end(&data), Some(29));
}

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-btree-{label}-{}-{id}", std::process::id()))
}

#[test]
fn replace_with_larger_value_splits_instead_of_overflowing() {
    // `replace_if_present` on a full page with a larger value used to hit
    // "B+Tree node exceeds page payload": the general path rewrote the
    // value without a fit check. It must delete+re-insert through the
    // splitting insert path instead, preserving every key.
    let path = temp_path("replace-overflow");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        // ~212-byte entries pack a 16KiB page nearly full.
        let value = vec![0x11_u8; 200];
        for index in 0..200_u32 {
            tree.insert(&format!("key-{index:05}").into_bytes(), &value)?;
        }
        // Grow one entry 10x on a full page: must split, not error.
        let big = vec![0x22_u8; 2000];
        assert!(tree.replace_if_present(b"key-00042", &big)?);
        assert_eq!(tree.get(b"key-00042")?, Some(big.clone()));
        // Grow every entry: sustained overflow pressure, no loss.
        for index in 0..200_u32 {
            let key = format!("key-{index:05}").into_bytes();
            assert!(tree.replace_if_present(&key, &big)?);
        }
        for index in 0..200_u32 {
            let key = format!("key-{index:05}").into_bytes();
            assert_eq!(tree.get(&key)?, Some(big.clone()));
        }
        // Absent keys still report false, and fresh inserts still work.
        assert!(!tree.replace_if_present(b"key-missing", &big)?);
        tree.insert(b"key-new", &value)?;
        assert_eq!(tree.get(b"key-new")?, Some(value));
        tree.sync()
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "replace-overflow test failed: {result:?}");
}

#[test]
fn splits_leaves_with_mixed_entry_sizes() {
    // Small entries fill leaves, then interleaved multi-kilobyte values
    // force splits. A count-based midpoint would produce halves that exceed
    // the page payload; the byte-aware split must keep every half writable.
    let path = temp_path("mixed-sizes");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        let small = vec![0x11_u8; 64];
        let large = vec![0x22_u8; 4 * 1024];
        for round in 0..40_u32 {
            for step in 0..32_u32 {
                let mut key = format!("row-{round:04}-{step:04}").into_bytes();
                tree.insert(&key, &small)?;
                key[0] = b'z';
                tree.insert(&key, &large)?;
            }
        }
        let mut key = b"row-0000-0000".to_vec();
        assert_eq!(tree.get(&key)?, Some(small.clone()));
        key[0] = b'z';
        assert_eq!(tree.get(&key)?, Some(large.clone()));
        tree.sync()
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "mixed-size split test failed: {result:?}");
}

#[test]
fn internal_split_point_keeps_both_halves_writable() {
    // Mirrors the production distribution that failed under load: thirteen
    // multi-kilobyte separators (vector-index keys) sorted ahead of fifteen
    // short separators (PK/staging keys). The count-based midpoint (14) puts
    // thirteen large separators on the left — 17,099 encoded bytes, beyond
    // the 16,332-byte payload — and `write_node` rejects that half. The
    // byte-aware choice must pick a boundary that keeps both halves writable.
    let mut separators: Vec<(Vec<u8>, PageId)> = Vec::new();
    for index in 0..13_u64 {
        let mut key = vec![b'l'; 1303];
        key[1] = index as u8;
        separators.push((key, PageId::new(100 + index)));
    }
    for index in 0..15_u64 {
        let mut key = vec![b's'; 4];
        key[1] = index as u8;
        separators.push((key, PageId::new(200 + index)));
    }
    let encoded = |slice: &[(Vec<u8>, PageId)]| {
        16 + slice
            .iter()
            .map(|(key, _)| 2 + key.len() + 8)
            .sum::<usize>()
    };
    let count_midpoint = separators.len() / 2;
    assert!(
        encoded(&separators[..count_midpoint]) > PAGE_DATA_SIZE,
        "the regression lens must overflow the count-based midpoint"
    );
    let middle = choose_internal_split(&separators);
    assert!(middle >= 1 && middle < separators.len());
    assert!(
        encoded(&separators[..middle]) <= PAGE_DATA_SIZE,
        "left half must fit one page"
    );
    assert!(
        encoded(&separators[middle + 1..]) <= PAGE_DATA_SIZE,
        "right half must fit one page"
    );
    // Uniform separators reduce to the count rule: both halves balanced.
    let uniform: Vec<(Vec<u8>, PageId)> = (0..21_u64)
        .map(|index| (vec![b'u'; 500], PageId::new(index)))
        .collect();
    let middle = choose_internal_split(&uniform);
    let left = middle;
    let right = uniform.len() - middle - 1;
    assert!(
        left.abs_diff(right) <= 1,
        "uniform split balanced: {left} vs {right}"
    );
}

#[test]
fn internal_splits_stay_writable_with_mixed_separator_sizes() {
    // Vector-index keys are multi-kilobyte while PK keys are a dozen bytes,
    // and both separator sizes meet in one internal node. Deferred insertion
    // recreates the production mix: twelve large keys fill the first leaf;
    // ten thousand short keys fill leaves behind them until a young root
    // node holds many short separators; large keys sorting between the two
    // families then land back in the left region, promoting large separators
    // into the same root node until it overflows while mixing both sizes. A
    // count-based internal midpoint leaves thirteen large separators on the
    // left and refuses the write.
    let path = temp_path("internal-mixed-sizes");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        let short_value = vec![0x11_u8; 2];
        let large_value = vec![0x22_u8; 4];
        let large_key = |prefix: u8, index: u32| {
            let mut key = format!("{}-{index:05}-", prefix as char).into_bytes();
            key.resize(1311, b'x');
            key
        };
        for index in 0..12_u32 {
            tree.insert(&large_key(b'l', index), &large_value)?;
        }
        for index in 0..10_500_u32 {
            tree.insert(format!("s-{index:05}").as_bytes(), &short_value)?;
        }
        for index in 0..180_u32 {
            tree.insert(&large_key(b'm', index), &large_value)?;
        }
        assert_eq!(tree.get(&large_key(b'l', 0))?, Some(large_value.clone()));
        assert_eq!(tree.get(&large_key(b'l', 11))?, Some(large_value.clone()));
        assert_eq!(tree.get(&large_key(b'm', 179))?, Some(large_value.clone()));
        assert_eq!(tree.get(b"s-00000")?, Some(short_value.clone()));
        assert_eq!(tree.get(b"s-05250")?, Some(short_value.clone()));
        assert_eq!(tree.get(b"s-10499")?, Some(short_value.clone()));
        tree.sync()
    })();
    let _ = fs::remove_file(&path);
    assert!(
        result.is_ok(),
        "internal mixed-size split test failed: {result:?}"
    );
}

#[test]
fn inserts_and_reads_on_disk() {
    let path = temp_path("get");
    let result = (|| {
        let mut tree = BTree::create(&path, 32)?;
        tree.insert(b"beta", b"two")?;
        tree.insert(b"alpha", b"one")?;
        assert_eq!(tree.get(b"alpha")?, Some(b"one".to_vec()));
        assert_eq!(tree.get(b"missing")?, None);
        tree.sync()
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "B+Tree get test failed: {result:?}");
}

/// Model-equivalence fuzz over the in-place fast path: deterministic
/// pseudorandom inserts (forward, reverse, interleaved), same-length and
/// different-length replacements, and deletes, verified against a
/// `BTreeMap` after every batch — plus a reopen that proves the bytes on
/// disk decode identically. This exercises fast-path inserts, in-place
/// replaces, fallback splits, and delete interplay together.
#[test]
fn randomized_ops_match_btreemap_model() {
    let path = temp_path("model");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        let mut model = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
        // Deterministic xorshift: no dev-dependency needed. The seed
        // is fixed and reported in every assertion so a failure
        // replays by reading it here (no seed search needed).
        const SEED: u64 = 0x9E3779B97F4A7C15;
        let mut state = SEED;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let key = |n: u64| format!("key-{n:05}").into_bytes();
        for round in 0..30 {
            for _ in 0..200 {
                let n = next() % 400;
                let op = next() % 10;
                if op < 7 {
                    // Varying lengths so replaces hit same-length,
                    // grow, and shrink cases.
                    let len = (next() % 60) as usize;
                    let value = vec![(n & 0xff) as u8; len];
                    tree.insert(&key(n), &value)?;
                    model.insert(key(n), value);
                } else if op < 8 {
                    let k = key(n);
                    let deleted_tree = tree.delete(&k)?;
                    let deleted_model = model.remove(&k).is_some();
                    assert_eq!(
                        deleted_tree, deleted_model,
                        "seed={SEED:#x} round={round} delete mismatch for {k:?}"
                    );
                } else {
                    let k = key(n);
                    assert_eq!(
                        tree.get(&k)?,
                        model.get(&k).cloned(),
                        "seed={SEED:#x} round={round} get mismatch for {k:?}"
                    );
                }
            }
            // Full ordered comparison every round.
            let scanned = tree.range(None, None)?;
            let expected: Vec<(Vec<u8>, Vec<u8>)> =
                model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            assert_eq!(
                scanned, expected,
                "seed={SEED:#x} scan mismatch at round {round}"
            );
            // Same-length and different-length replaces on live keys.
            if let Some(k) = model.keys().next().cloned() {
                tree.insert(&k, b"same")?;
                model.insert(k.clone(), b"same".to_vec());
                let long = vec![9u8; 300];
                tree.insert(&k, &long)?;
                model.insert(k.clone(), long);
                assert_eq!(tree.get(&k)?, model.get(&k).cloned());
            }
        }
        tree.sync()?;
        drop(tree);
        let mut reopened = BTree::open(&path, 64)?;
        let scanned = reopened.range(None, None)?;
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(scanned, expected, "seed={SEED:#x} post-reopen mismatch");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "model test failed: {result:?}");
}

#[test]
fn range_is_ordered() {
    let path = temp_path("range");
    let result = (|| {
        let mut tree = BTree::create(&path, 32)?;
        for key in [b"delta".as_slice(), b"alpha", b"charlie", b"bravo"] {
            tree.insert(key, key)?;
        }
        let values = tree.range(Some(b"bravo"), Some(b"delta"))?;
        let keys: Vec<Vec<u8>> = values.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, vec![b"bravo".to_vec(), b"charlie".to_vec()]);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "B+Tree range test failed: {result:?}");
}

#[test]
fn bounded_range_prunes_to_the_requested_window() {
    // Regression: `collect_range` used to descend into every internal
    // child, so a bounded range touched the whole tree. Insert enough keys
    // to force several levels of internal nodes, then check that bounded
    // ranges return exactly the keys in `[start, end)` for windows placed
    // at the beginning, middle, and end of the tree.
    let path = temp_path("bounded-range");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        for number in 0..600_u32 {
            let key = number.to_be_bytes();
            tree.insert(&key, &number.to_le_bytes())?;
        }
        let window = |tree: &mut BTree, start: u32, end: u32| -> plomid_core::Result<Vec<u32>> {
            let entries = tree.range(Some(&start.to_be_bytes()), Some(&end.to_be_bytes()))?;
            Ok(entries
                .into_iter()
                .map(|(key, _)| u32::from_be_bytes(key.try_into().expect("key width")))
                .collect())
        };
        assert_eq!(window(&mut tree, 0, 5)?, vec![0, 1, 2, 3, 4]);
        assert_eq!(window(&mut tree, 250, 260)?, (250..260).collect::<Vec<_>>());
        assert_eq!(window(&mut tree, 595, 700)?, (595..600).collect::<Vec<_>>());
        assert_eq!(window(&mut tree, 100_000, 100_010)?, Vec::<u32>::new());
        assert_eq!(window(&mut tree, 0, 600)?.len(), 600);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(
        result.is_ok(),
        "bounded range prune test failed: {result:?}"
    );
}

#[test]
fn values_survive_reopen() {
    let path = temp_path("reopen");
    let result = (|| {
        let mut tree = BTree::create(&path, 32)?;
        for number in 0..100_u32 {
            let key = number.to_be_bytes();
            tree.insert(&key, &number.to_le_bytes())?;
        }
        tree.sync()?;
        drop(tree);
        let mut reopened = BTree::open(&path, 32)?;
        assert_eq!(
            reopened.get(&42_u32.to_be_bytes())?,
            Some(42_u32.to_le_bytes().to_vec())
        );
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "B+Tree reopen test failed: {result:?}");
}

#[test]
fn larger_values_force_leaf_splits_and_ordered_scan() {
    let path = temp_path("splits");
    let result = (|| {
        let mut tree = BTree::create(&path, 64)?;
        for number in 0..40_u16 {
            let key = number.to_be_bytes();
            let value = vec![number as u8; 400];
            tree.insert(&key, &value)?;
        }
        let entries = tree.range(None, None)?;
        assert_eq!(entries.len(), 40);
        for (number, (key, value)) in entries.into_iter().enumerate() {
            assert_eq!(key, (number as u16).to_be_bytes());
            assert_eq!(value, vec![number as u8; 400]);
        }
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "B+Tree split test failed: {result:?}");
}
