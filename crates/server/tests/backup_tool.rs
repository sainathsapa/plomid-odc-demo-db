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
//! Offline backup tool roundtrip (P1-004): the `backup` / `restore` /
//! `backup-inspect` entry points perform exactly the tested cold sequence
//! (checkpoint + close, copy with manifest last, restore-to-clean-only)
//! and refuse live directories instead of copying under a writer.

use plomid_executor::Executor;
use plomid_server::backup_tool;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn scratch(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "plomid-srvbackup-{tag}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn argv(parts: &[&str]) -> Vec<String> {
    std::iter::once("plomid-server")
        .chain(parts.iter().copied())
        .map(str::to_string)
        .collect()
}

fn seed_db(data_dir: &std::path::Path) {
    let engine =
        PlomidStorageEngine::create(data_dir, &data_dir.join("wal"), 32).expect("create engine");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create");
    executor
        .execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .expect("insert");
}

fn row_count(data_dir: &std::path::Path) -> i64 {
    let engine = PlomidStorageEngine::open(data_dir, &data_dir.join("wal"), 32).expect("reopen");
    let mut executor = Executor::new(engine).expect("executor");
    count_rows(&mut executor)
}

fn count_rows(executor: &mut Executor<PlomidStorageEngine>) -> i64 {
    match executor.execute("SELECT count(*) FROM t").expect("count") {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int8(n) => *n,
            Value::Int4(n) => *n as i64,
            other => panic!("unexpected count {other:?}"),
        },
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn backup_restore_roundtrip_through_tool() {
    let data = scratch("data");
    let backup = scratch("backup");
    let restored = scratch("restored");
    seed_db(&data);
    // backup: checkpoint + close + copy + manifest.
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "backup",
        "--data",
        data.to_str().unwrap(),
        "--to",
        backup.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OK);
    assert!(backup.join("PLOMID_BACKUP_MANIFEST").exists());
    // inspect reads the manifest back.
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "backup-inspect",
        "--backup",
        backup.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OK);
    // restore to a clean directory, then verify rows + writability.
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "restore",
        "--from",
        backup.to_str().unwrap(),
        "--to",
        restored.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OK);
    assert_eq!(row_count(&restored), 2);
    let engine = PlomidStorageEngine::open(&restored, &restored.join("wal"), 32).expect("reopen");
    let mut executor = Executor::new(engine).expect("executor");
    executor
        .execute("INSERT INTO t VALUES (3, 'c')")
        .expect("write");
    drop(executor);
    assert_eq!(count_rows_reopen(&restored), 3);
}

fn count_rows_reopen(data_dir: &std::path::Path) -> i64 {
    let engine = PlomidStorageEngine::open(data_dir, &data_dir.join("wal"), 32).expect("reopen");
    let mut executor = Executor::new(engine).expect("executor");
    count_rows(&mut executor)
}

#[test]
fn backup_refuses_live_directory() {
    let data = scratch("live");
    seed_db(&data);
    // Simulate a live server by holding the data-directory lock ourselves.
    let _live = backup_tool::hold_data_dir_lock(&data).expect("lock");
    let backup = scratch("live-backup");
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "backup",
        "--data",
        data.to_str().unwrap(),
        "--to",
        backup.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_LIVE_OR_REFUSED);
    assert!(!backup.join("PLOMID_BACKUP_MANIFEST").exists());
}

#[test]
fn restore_refuses_live_destination_and_missing_manifest() {
    let data = scratch("src");
    seed_db(&data);
    let backup = scratch("bak");
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "backup",
        "--data",
        data.to_str().unwrap(),
        "--to",
        backup.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OK);
    // Live destination: locked beforehand.
    let live_dst = scratch("livedst");
    std::fs::create_dir_all(&live_dst).expect("mkdir");
    let _live = backup_tool::hold_data_dir_lock(&live_dst).expect("lock");
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "restore",
        "--from",
        backup.to_str().unwrap(),
        "--to",
        live_dst.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_LIVE_OR_REFUSED);
    drop(_live);
    // Manifest-less source is corruption, not a backup.
    let empty = scratch("empty");
    std::fs::create_dir_all(&empty).expect("mkdir");
    let dst = scratch("dst");
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "restore",
        "--from",
        empty.to_str().unwrap(),
        "--to",
        dst.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OPERATION_FAILED);
    // Restoring over an existing database is refused.
    let code = backup_tool::dispatch_offline_tool(&argv(&[
        "restore",
        "--from",
        backup.to_str().unwrap(),
        "--to",
        data.to_str().unwrap(),
    ]))
    .expect("dispatch");
    assert_eq!(code, backup_tool::EXIT_OPERATION_FAILED);
}

#[test]
fn unknown_subcommand_keeps_serve_path() {
    assert!(backup_tool::dispatch_offline_tool(&argv(&["--help"])).is_none());
    assert!(backup_tool::dispatch_offline_tool(&argv(&["--version"])).is_none());
    assert!(backup_tool::dispatch_offline_tool(&argv(&[])).is_none());
}
