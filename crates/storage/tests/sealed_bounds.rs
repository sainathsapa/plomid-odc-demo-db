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

//! Sealed-segment key-bounds routing regression tests.
//!
//! The routing contract under test:
//! * out-of-range keys skip sealed segments WITHOUT exact evaluation and
//!   WITHOUT triggering the lazy full-segment filter build (no false
//!   negatives: sealed key sets only shrink, so bounds only go stale-wide);
//! * in-range keys behave exactly as before (filter → exact descent);
//! * updates/deletes on sealed keys (including min/max) stay exact;
//! * newest-wins holds across sealed + active;
//! * restart preserves exactness (bounds recompute lazily).
//!
//! MVCC and maintenance interaction are covered by the existing
//! `mvcc`/`txn`/executor suites (storage layer is MVCC-agnostic).

use plomid_storage::{BTree, SegmentState, StorageManager};
use std::path::{Path, PathBuf};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ph03bounds-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

fn key(i: u64) -> Vec<u8> {
    format!("k-{i:08}").into_bytes()
}

/// Load sequential keys until at least `sealed` sealed segments exist.
/// Returns (total_keys, sealed_count).
fn load_sequential(root: &Path, seg_bytes: u64, sealed: usize) -> (u64, usize) {
    let mut m = StorageManager::create(root, 64, seg_bytes).expect("create");
    let mut n = 0u64;
    loop {
        let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..200)
            .map(|j| {
                let i = n + j;
                (key(i), Some(i.to_le_bytes().to_vec()))
            })
            .collect();
        n += 200;
        m.apply_batch(&ops).expect("apply");
        let sealed_now = m
            .segments()
            .filter(|s| s.state == SegmentState::Sealed)
            .count();
        if sealed_now >= sealed {
            m.sync().expect("sync");
            return (n, sealed_now);
        }
        assert!(n < 1_000_000, "segment target unreachable");
    }
}

fn stats(m: &StorageManager) -> (u64, u64) {
    m.filter_stats()
}

#[test]
fn out_of_range_keys_skip_without_exact_probes() {
    // NOTE: sealed filters are built eagerly at seal time, so `.xor`
    // sidecars always exist; the bounds prune skips the per-segment hashing
    // AND the false-positive exact descents (Xor FPR ~0.4% would otherwise
    // force ~0.004 * segments wasted descents per out-of-range key).
    let root = scratch("oor");
    let (nkeys, sealed) = load_sequential(&root, 256 * 1024, 4);
    assert!(sealed >= 4);
    let mut m = StorageManager::open(&root, 64, 256 * 1024).expect("open");
    // Far above max: thousands of trials so that even a 0.4% filter pass
    // rate would produce hundreds of exact probes without the prune.
    // P(zero probes | no prune) ~= (1-0.004)^3000 < 1e-5: deterministic.
    let (p0, s0) = stats(&m);
    for j in 0..3000u64 {
        assert!(
            m.get(&key(nkeys + j)).expect("get").is_none(),
            "out-of-range key unexpectedly present"
        );
    }
    let (p1, s1) = stats(&m);
    assert_eq!(p1, p0, "out-of-range keys caused exact probes");
    assert_eq!(
        s1 - s0,
        3000 * sealed as u64,
        "every sealed segment must skip"
    );
    // Far below min: same (fewer trials; same mechanism).
    let (p2, s2) = stats(&m);
    for j in 0..500u64 {
        let mut k = key(j);
        k[0] = b'!';
        assert!(m.get(&k).expect("get").is_none());
    }
    let (p3, s3) = stats(&m);
    assert_eq!(p3, p2, "low out-of-range keys caused exact probes");
    assert_eq!(s3 - s2, 500 * sealed as u64);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn in_range_and_positive_keys_stay_exact() {
    let root = scratch("exact");
    let (nkeys, _) = load_sequential(&root, 256 * 1024, 4);
    let mut m = StorageManager::open(&root, 64, 256 * 1024).expect("open");
    // Positives across the whole range (sparse + adjacent sampling).
    let mut i = 0u64;
    while i < nkeys {
        let got = m.get(&key(i)).expect("get").expect("present");
        assert_eq!(&got[..8], &i.to_le_bytes(), "wrong value for {i}");
        i += if i.is_multiple_of(7) { 1 } else { 13 };
    }
    // In-range absent keys (odd gaps via untouched prefix).
    for j in 0..50u64 {
        let mut k = key(j * 3);
        k[0] = b'Z';
        assert!(m.get(&k).expect("get").is_none());
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn sealed_update_delete_and_stale_bounds_stay_exact() {
    let root = scratch("mut");
    let (nkeys, _) = load_sequential(&root, 256 * 1024, 4);
    assert!(nkeys > 100);
    let mut m = StorageManager::open(&root, 64, 256 * 1024).expect("open");
    // Update a sealed key: newest copy wins.
    let victim = key(5);
    m.apply_batch(&[(victim.clone(), Some(424242u64.to_le_bytes().to_vec()))])
        .expect("update");
    assert_eq!(
        &m.get(&victim).expect("get").expect("present")[..8],
        &424242u64.to_le_bytes()
    );
    // Delete the global min and max: stored bounds go stale-wide, answers
    // must stay exact (extra work allowed, wrong skips forbidden).
    m.apply_batch(&[(key(0), None)]).expect("del min");
    m.apply_batch(&[(key(nkeys - 1), None)]).expect("del max");
    assert!(m.get(&key(0)).expect("get").is_none());
    assert!(m.get(&key(nkeys - 1)).expect("get").is_none());
    // Neighbors still resolve.
    assert!(m.get(&key(1)).expect("get").is_some());
    assert!(m.get(&key(nkeys - 2)).expect("get").is_some());
    // Duplicate across sealed + active resolves newest-first.
    let dup = key(9);
    m.apply_batch(&[(dup.clone(), Some(777u64.to_le_bytes().to_vec()))])
        .expect("dup");
    assert_eq!(
        &m.get(&dup).expect("get").expect("present")[..8],
        &777u64.to_le_bytes()
    );
    m.sync().expect("sync");
    drop(m);
    // Restart: bounds recompute lazily, exactness preserved.
    let mut m = StorageManager::open(&root, 64, 256 * 1024).expect("reopen");
    assert!(m.get(&key(0)).expect("get").is_none());
    assert!(m.get(&key(nkeys - 1)).expect("get").is_none());
    assert_eq!(
        &m.get(&victim).expect("get").expect("present")[..8],
        &424242u64.to_le_bytes()
    );
    assert!(m.get(&key(nkeys + 9000)).expect("get").is_none());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn edge_keys_unit() {
    let dir = scratch("edge");
    let path = dir.join("t.dat");
    let mut tree = BTree::create(&path, 16).expect("create");
    assert_eq!(tree.edge_keys().expect("empty"), (None, None));
    for k in [
        b"m".as_slice(),
        b"a".as_slice(),
        b"z".as_slice(),
        b"c".as_slice(),
    ] {
        tree.insert(k, b"v").expect("insert");
    }
    let (min, max) = tree.edge_keys().expect("edges");
    assert_eq!(min.as_deref(), Some(b"a".as_slice()));
    assert_eq!(max.as_deref(), Some(b"z".as_slice()));
    tree.delete(b"a").expect("del");
    tree.delete(b"z").expect("del");
    let (min, max) = tree.edge_keys().expect("edges2");
    assert_eq!(min.as_deref(), Some(b"c".as_slice()));
    assert_eq!(max.as_deref(), Some(b"m".as_slice()));
    std::fs::remove_dir_all(&dir).ok();
}
