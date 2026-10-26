//
// © 2026 PLOMID Technologies INDIA
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0 (the "License");
// Pinned repeatable-read snapshot regression tests on the
// concurrent storage facade.
//
// Background: every facade read took a fresh `manager.snapshot(0)`, so an
// explicit REPEATABLE READ transaction observed concurrently committed
// writes (silent downgrade to read-committed). Reads must resolve under
// the snapshot pinned at BEGIN while it is installed.
use plomid_storage::StorageEngine;
use plomid_txn::ConcurrentPlomidStorageEngine;
use plomid_wal::DataOperation;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-pinned-read-{label}-{}-{id}",
        std::process::id()
    ))
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn commit_put(engine: &mut plomid_txn::PlomidStorageEngine, key: &[u8], value: &[u8]) {
    let (txn, _) = engine.begin_concurrent().expect("begin");
    engine
        .commit_concurrent(
            txn,
            vec![DataOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        )
        .expect("commit");
}

fn commit_del(engine: &mut plomid_txn::PlomidStorageEngine, key: &[u8]) {
    let (txn, _) = engine.begin_concurrent().expect("begin");
    engine
        .commit_concurrent(txn, vec![DataOperation::Delete { key: key.to_vec() }])
        .expect("commit");
}

const K1: &[u8] = b"pin-k1";
const K2: &[u8] = b"pin-k2";
const K3: &[u8] = b"pin-k3";

#[test]
fn pinned_reads_hide_later_commits_and_deletes() {
    let dir = scratch("snapshot");
    let engine = plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal-alt"), 32)
        .expect("engine create");
    let shared = Arc::new(Mutex::new(engine));
    {
        let mut guard = shared.lock().expect("lock");
        commit_put(&mut guard, K1, b"v1");
        commit_put(&mut guard, K2, b"w1");
    }
    let mut facade =
        ConcurrentPlomidStorageEngine::from_shared(Arc::clone(&shared)).expect("facade");
    assert!(!facade.is_read_pinned());
    assert_eq!(facade.get(K1).expect("get"), Some(b"v1".to_vec()));

    let (txn_id, snapshot) = facade.begin_pinned_read().expect("begin pinned");
    facade.pin_read_snapshot(snapshot).expect("pin");
    assert!(facade.is_read_pinned());

    {
        let mut guard = shared.lock().expect("lock");
        commit_put(&mut guard, K1, b"v2");
        commit_del(&mut guard, K2);
        commit_put(&mut guard, K3, b"n3");
    }

    // Every read shape resolves under the pinned snapshot.
    assert_eq!(facade.get(K1).expect("get"), Some(b"v1".to_vec()));
    assert_eq!(
        facade.scan(None, None).expect("scan"),
        vec![(K1.to_vec(), b"v1".to_vec()), (K2.to_vec(), b"w1".to_vec())]
    );
    assert_eq!(
        facade
            .get_many(&[K1.to_vec(), K2.to_vec(), K3.to_vec()])
            .expect("get_many"),
        vec![Some(b"v1".to_vec()), Some(b"w1".to_vec()), None]
    );
    assert_eq!(facade.count_range(None, None).expect("count"), 2);
    assert_eq!(
        facade.scan_limit(None, None, 10).expect("scan_limit").len(),
        2
    );
    let mut walked = Vec::new();
    facade
        .scan_for_each(None, None, 1, &mut |chunk| {
            walked.extend(chunk.to_vec());
            Ok(true)
        })
        .expect("scan_for_each");
    assert_eq!(walked.len(), 2);

    // Release returns to fresh reads; the held id aborts cleanly.
    facade.unpin_read_snapshot().expect("unpin");
    assert!(!facade.is_read_pinned());
    facade.abort_pinned_read(txn_id).expect("abort");
    assert_eq!(facade.get(K1).expect("get"), Some(b"v2".to_vec()));
    assert_eq!(
        facade.scan(None, None).expect("scan"),
        vec![(K1.to_vec(), b"v2".to_vec()), (K3.to_vec(), b"n3".to_vec())]
    );
    drop(facade);
    drop(shared);
    cleanup(&dir);
}
