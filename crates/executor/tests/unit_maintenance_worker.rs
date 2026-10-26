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
// Moved from src/maintenance_worker.rs inline #[cfg(test)].
use plomid_executor::columnar_freshness;
use plomid_executor::maintenance_worker::*;
use plomid_txn::PlomidStorageEngine;
use std::collections::HashSet;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::sync_channel,
    Arc, Mutex,
};

const TEST_ROOT: &str = "/test-root";
fn test_root() -> std::path::PathBuf {
    std::path::PathBuf::from(TEST_ROOT)
}
fn test_link(bound: usize) -> (WorkerLink, std::sync::mpsc::Receiver<WorkItem>) {
    let (tx, rx) = sync_channel::<WorkItem>(bound.max(1));
    let link = WorkerLink {
        tx,
        root: Arc::new(test_root()),
        inflight: Arc::new(Mutex::new(HashSet::new())),
        stopped: Arc::new(AtomicBool::new(false)),
        submitted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
        inline_fallbacks: Arc::new(AtomicU64::new(0)),
    };
    (link, rx)
}

#[test]
fn submit_dedupes_inflight_tables() {
    // No worker thread drains here, so submissions accumulate
    // deterministically: five submits for one table queue one item.
    let (link, _rx) = test_link(64);
    for _ in 0..5 {
        assert!(link.submit(&test_root(), "db", "t"), "submit accepts");
    }
    assert_eq!(link.queue_depth(), 1, "one in-flight entry max per table");
    assert!(
        link.submit(&test_root(), "db", "other"),
        "distinct table queues too"
    );
    assert_eq!(link.queue_depth(), 2);
    let (submitted, _, _) = link.stats();
    assert_eq!(submitted, 2, "only first submissions queue work");
}

#[test]
fn full_queue_falls_back_to_inline() {
    let (link, _rx) = test_link(1);
    assert!(link.submit(&test_root(), "db", "t1"), "first item fits");
    // Same table coalesces even when full (no new queue slot needed).
    assert!(
        link.submit(&test_root(), "db", "t1"),
        "coalesced submit succeeds"
    );
    // Distinct table with a full queue declines to inline execution.
    assert!(
        !link.submit(&test_root(), "db", "t2"),
        "full queue declines"
    );
    let (_, _, inline_fallbacks) = link.stats();
    assert_eq!(inline_fallbacks, 1);
    // The declined table was released from the set: a later submit (once
    // a worker drains) may proceed.
    assert_eq!(link.queue_depth(), 1);
}

#[test]
fn stopped_link_declines() {
    let (link, _rx) = test_link(64);
    link.stopped.store(true, Ordering::Release);
    assert!(
        !link.submit(&test_root(), "db", "t"),
        "submissions after shutdown decline to inline"
    );
}

#[test]
fn foreign_root_declines_to_inline() {
    let (link, _rx) = test_link(64);
    assert!(
        !link.submit(&std::path::PathBuf::from("/other-root"), "db", "t"),
        "foreign roots run inline"
    );
    assert_eq!(link.queue_depth(), 0);
}

#[test]
fn worker_executes_pass_and_marks_fresh() {
    // End-to-end through a real thread: the worker runs the pass for a
    // submitted table and the shared freshness map records it, all
    // without any session running inline maintenance.
    let root = std::env::temp_dir().join(format!("plomid-worker-e2e-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
    let shared = Arc::new(Mutex::new(engine));
    {
        let mut setup =
            plomid_executor::executor::Executor::new_shared(Arc::clone(&shared)).expect("setup");
        setup
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")
            .unwrap();
        setup
            .execute("INSERT INTO t VALUES (1, 10), (2, 20);")
            .unwrap();
    }
    let key = columnar_freshness::freshness_key(&root, "plomid", "public.t");
    columnar_freshness::forget_table(&key);
    let worker = MaintenanceWorker::spawn(Arc::clone(&shared));
    let link = worker.link();
    assert!(link.submit(&root, "plomid", "public.t"));
    // Poll for completion (generous bound; the pass itself is fast).
    let mut completed = false;
    for _ in 0..300 {
        if link.stats().1 >= 1 {
            completed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(completed, "worker completed the pass");
    assert!(
        columnar_freshness::clean_generation(&key).is_some(),
        "pass marked the table fresh"
    );
    worker.shutdown();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(wal);
}
