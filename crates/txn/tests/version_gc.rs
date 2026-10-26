//
// © 2026 PLOMID Technologies INDIA
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0 (the "License");
// Opportunistic MVCC version-store GC regression tests.
//
// Background: `VersionStore::gc` existed but no production path ever called
// `gc_below`, so hot-key UPDATE chains grew one version per commit without
// bound. The engine now runs a throttled GC pass inside `commit_publish`
// when no active transaction pins a snapshot.
use plomid_txn::PlomidStorageEngine;
use plomid_wal::DataOperation;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-version-gc-{label}-{}-{id}",
        std::process::id()
    ))
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

const KEY: &[u8] = b"phase13-gc-hot-key";

fn commit_put(engine: &mut PlomidStorageEngine, value: Vec<u8>) {
    let (txn, _) = engine.begin_concurrent().expect("begin");
    engine
        .commit_concurrent(
            txn,
            vec![DataOperation::Put {
                key: KEY.to_vec(),
                value,
            }],
        )
        .expect("commit");
}

fn latest(engine: &mut PlomidStorageEngine) -> Vec<u8> {
    let snapshot = engine.committed_snapshot().expect("snapshot");
    engine
        .get_snapshot(KEY, &snapshot)
        .expect("get")
        .expect("key must exist")
}

#[test]
fn hot_key_chains_stay_bounded_under_autocommit() {
    let dir = scratch("bounded");
    let mut engine =
        PlomidStorageEngine::create(&dir, &dir.join("wal-alt"), 32).expect("engine create");
    commit_put(&mut engine, b"v0".to_vec());
    // 600 overwrites of the same key: crosses two GC intervals (256).
    for i in 0..600 {
        commit_put(&mut engine, format!("v{i}").into_bytes());
    }
    let count = engine.version_count(KEY).expect("version_count");
    let (_, reclaimed) = engine.version_gc_stats();
    // Phase-tolerant bound: GC collapses the chain every 256 commits, so at
    // any point the chain holds at most one interval's worth of versions.
    // Without GC this would be 601.
    assert!(
        count <= 100,
        "hot-key chain must be reclaimed by opportunistic GC, got {count} versions"
    );
    assert!(
        reclaimed > 0,
        "GC must have reclaimed versions across 600 overwrites"
    );
    assert_eq!(latest(&mut engine), b"v599".to_vec());
    drop(engine);
    cleanup(&dir);
}

#[test]
fn gc_defers_while_a_transaction_is_active() {
    let dir = scratch("pinned");
    let mut engine =
        PlomidStorageEngine::create(&dir, &dir.join("wal-alt"), 32).expect("engine create");
    commit_put(&mut engine, b"v0".to_vec());
    // Hold one transaction open: every GC interval must skip while pinned.
    let (pinned, _) = engine.begin_concurrent().expect("begin pinned");
    for i in 0..300 {
        commit_put(&mut engine, format!("p{i}").into_bytes());
    }
    let pinned_count = engine.version_count(KEY).expect("version_count");
    assert!(
        pinned_count > 200,
        "GC must defer while a transaction is active, got {pinned_count}"
    );
    engine.abort_concurrent(pinned).expect("abort pinned");
    // Drive past more intervals with nobody active: reclaim must happen.
    for i in 0..300 {
        commit_put(&mut engine, format!("q{i}").into_bytes());
    }
    let after = engine.version_count(KEY).expect("version_count");
    assert!(
        after <= 100,
        "chain must collapse after the pin releases, got {after}"
    );
    assert_eq!(latest(&mut engine), b"q299".to_vec());
    drop(engine);
    cleanup(&dir);
}
