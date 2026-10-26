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

//! Workstream C acceptance tests: spill behavior, correctness, cleanup.

use plomid_spill::{
    ExternalSort, SetOpKind, SpillConfig, SpillDistinct, SpillError, SpillGroupedAgg,
    SpillHashJoin, SpillManager, SpillSetOp,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn test_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("plomid-spill-test-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test temp dir");
    dir
}

fn tmp_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read test temp dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
        .count()
}

#[test]
fn sort_500k_rows_with_10k_limit_is_correct_and_bounded() {
    let dir = test_dir("sort500k");
    let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");
    let mut sort = ExternalSort::<u64>::new(mgr.clone(), 10_000).expect("sort");
    const N: u64 = 500_000;
    // Deterministic permutation: 2654435761 is coprime with 500_000.
    for i in 0..N {
        let v = (i * 2_654_435_761) % N;
        sort.push(v).expect("push");
    }
    assert!(sort.fell_back_to_disk(), "expected spill to disk");
    assert!(sort.spill_bytes() > 0, "spill_bytes must be > 0");
    // Temp bounded by the 1 GiB default (trivially true at ~6 MiB, but assert).
    assert!(mgr.current_bytes() <= mgr.temp_limit_bytes());
    assert!(mgr.files() > 0);

    let got = sort.finish_vec().expect("finish");
    assert_eq!(got.len(), N as usize);
    for (i, v) in got.iter().enumerate() {
        assert_eq!(*v, i as u64, "sorted mismatch at {i}");
    }
    drop(mgr);
    assert_eq!(tmp_count(&dir), 0, "temp files must be cleaned after drop");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hash_agg_over_limit_spills_and_aggregates() {
    let dir = test_dir("agg");
    let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");
    let mut agg = SpillGroupedAgg::<u64>::new(mgr.clone(), 1_000).expect("agg");
    const KEYS: u64 = 20_000;
    const REPS: u64 = 5;
    for r in 0..REPS {
        for k in 0..KEYS {
            agg.add(k, 1 + r as i64).expect("add");
        }
    }
    assert!(agg.fell_back_to_disk());
    assert!(agg.spill_bytes() > 0);
    let mut got = agg.finish().expect("finish");
    got.sort_by_key(|(k, _, _)| *k);
    assert_eq!(got.len(), KEYS as usize);
    let expect_sum: i64 = (0..REPS).map(|r| 1 + r as i64).sum();
    for (k, c, s) in got {
        assert_eq!(c, REPS, "count for key {k}");
        assert_eq!(s, expect_sum, "sum for key {k}");
    }
    drop(mgr);
    assert_eq!(tmp_count(&dir), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hash_join_over_limit_is_correct() {
    let dir = test_dir("join");
    let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");
    let mut join = SpillHashJoin::<u64>::new(mgr.clone(), 500).expect("join");
    const N: u64 = 5_000;
    for k in 0..N {
        join.insert_build(k, format!("b{k}").into_bytes())
            .expect("build");
    }
    assert!(join.fell_back_to_disk(), "build should have spilled");
    // Probe every key twice + some non-matching keys.
    for k in 0..N {
        join.insert_probe(k, format!("p{k}-1").into_bytes())
            .expect("probe");
        join.insert_probe(k, format!("p{k}-2").into_bytes())
            .expect("probe");
    }
    for k in N..N + 100 {
        join.insert_probe(k, b"nomatch".to_vec()).expect("probe");
    }
    let mut got = join.finish().expect("finish");
    got.sort();
    assert_eq!(got.len(), (2 * N) as usize);
    // Spot-check first key's cross product.
    let k0: Vec<_> = got.iter().filter(|(k, _, _)| *k == 0).collect();
    assert_eq!(k0.len(), 2);
    assert!(k0.iter().all(|(_, b, _)| b == b"b0"));
    drop(mgr);
    assert_eq!(tmp_count(&dir), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn temp_files_cleaned_after_drop() {
    let dir = test_dir("cleanup");
    {
        let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");
        let mut sort = ExternalSort::<u64>::new(mgr.clone(), 100).expect("sort");
        for i in 0..1_000u64 {
            sort.push((i * 7) % 1_000).expect("push");
        }
        assert!(sort.fell_back_to_disk());
        assert!(tmp_count(&dir) > 0, "spill files should exist while live");
        let _ = sort.finish_vec().expect("finish");
        // `sort` (owns runs) still alive here; files may still exist.
    }
    // Both manager and all runs dropped -> dir must hold no .tmp files.
    assert_eq!(tmp_count(&dir), 0, "dir must be empty after drop");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn storage_exhausted_when_temp_limit_tiny() {
    let dir = test_dir("exhausted");
    let mut cfg = SpillConfig::new(dir.clone());
    cfg.temp_limit_bytes = 1024; // 1 KiB: spills cannot fit.
    let mgr = SpillManager::new(cfg).expect("manager");
    let mut sort = ExternalSort::<u64>::new(mgr.clone(), 100).expect("sort");
    let mut saw_exhausted = false;
    for i in 0..10_000u64 {
        match sort.push((i * 13) % 999_983) {
            Ok(()) => {}
            Err(SpillError::StorageExhausted(_)) => {
                saw_exhausted = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    assert!(saw_exhausted, "expected StorageExhausted with 1 KiB limit");
    drop(sort);
    drop(mgr);
    assert_eq!(tmp_count(&dir), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cancel_mid_sort_returns_cancelled() {
    let dir = test_dir("cancel");
    let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");
    let cancel = Arc::new(AtomicBool::new(false));
    let mut sort = ExternalSort::<u64>::new(mgr.clone(), 10_000)
        .expect("sort")
        .with_cancel(Arc::clone(&cancel));
    for i in 0..50_000u64 {
        sort.push((i * 31) % 50_000).expect("push");
    }
    // Cancel before the merge phase.
    cancel.store(true, Ordering::Relaxed);
    match sort.finish_vec() {
        Err(SpillError::Cancelled) => {}
        Ok(_) => panic!("expected Cancelled"),
        Err(e) => panic!("expected Cancelled, got {e}"),
    }
    // Cancellation during ingest also surfaces Cancelled, never a panic.
    let cancel2 = Arc::new(AtomicBool::new(true));
    let mut sort2 = ExternalSort::<u64>::new(mgr.clone(), 10_000)
        .expect("sort")
        .with_cancel(cancel2);
    assert_eq!(
        sort2.push(1).unwrap_err(),
        SpillError::Cancelled,
        "push under set flag must return Cancelled"
    );
    drop(mgr);
    assert_eq!(tmp_count(&dir), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn distinct_and_setops_spill_correctly() {
    let dir = test_dir("distinct-setop");
    let mgr = SpillManager::with_temp_dir(dir.clone()).expect("manager");

    // Distinct over limit.
    let mut d = SpillDistinct::<u64>::new(mgr.clone(), 500).expect("distinct");
    for i in 0..10_000u64 {
        d.insert(i % 2_000).expect("insert");
    }
    assert!(d.fell_back_to_disk());
    let mut got = d.finish().expect("distinct finish");
    got.sort();
    assert_eq!(got, (0..2_000u64).collect::<Vec<_>>());

    // Set ops over limit.
    for kind in [SetOpKind::Union, SetOpKind::Intersect, SetOpKind::Except] {
        let mut op = SpillSetOp::<u64>::new(mgr.clone(), kind, 200).expect("setop");
        for i in 0..5_000u64 {
            op.insert_left(i % 3_000).expect("left");
            op.insert_right((i + 1_500) % 4_000).expect("right");
        }
        assert!(op.fell_back_to_disk(), "setop {kind:?} should spill");
        let got: HashSet<u64> = op.finish().expect("setop finish").into_iter().collect();
        let left: HashSet<u64> = (0..5_000u64).map(|i| i % 3_000).collect();
        let right: HashSet<u64> = (0..5_000u64).map(|i| (i + 1_500) % 4_000).collect();
        let expect: HashSet<u64> = match kind {
            SetOpKind::Union => left.union(&right).cloned().collect(),
            SetOpKind::Intersect => left.intersection(&right).cloned().collect(),
            SetOpKind::Except => left.difference(&right).cloned().collect(),
        };
        assert_eq!(got, expect, "setop {kind:?} mismatch");
    }

    // Charge hook observes alloc/free traffic (count invocations; net may be
    // zero after a full spill frees the buffer).
    let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let c = Arc::clone(&calls);
    let hook: Arc<dyn Fn(i64) + Send + Sync> = Arc::new(move |_d: i64| {
        c.fetch_add(1, Ordering::Relaxed);
    });
    let mut agg = SpillGroupedAgg::<u64>::new(mgr.clone(), 100)
        .expect("agg")
        .with_charge(hook);
    for k in 0..500u64 {
        agg.add(k, 1).expect("add");
    }
    assert_ne!(calls.load(Ordering::Relaxed), 0, "charge hook must fire");
    let rows = agg.finish().expect("finish");
    assert_eq!(rows.len(), 500);
    let map: HashMap<u64, (u64, i64)> = rows.into_iter().map(|(k, c, s)| (k, (c, s))).collect();
    assert!(map.values().all(|(c, s)| *c == 1 && *s == 1));

    drop(mgr);
    assert_eq!(tmp_count(&dir), 0);
    let _ = std::fs::remove_dir_all(&dir);
}
