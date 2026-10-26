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
//! Phase 12 operator drill: full backup → clean restore → verify.
//!
//! Covers the plan's restore checklist at the SQL boundary: schema, row
//! count, checksums (`SUM`), indexes (`ORDER BY` + point lookup), plain
//! transactions (`BEGIN`/`COMMIT`), and new writes after restore. Upgrade
//! items (version refusal, interrupted backup/manifest-last, rollback =
//! restore-to-clean refusal to overwrite) live here too; WAL/vector/
//! columnar internals are covered by the engine-boundary test in
//! `vector_durability_convergence` plus unit tests in `txn::backup`.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::backup;
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("plomid-backup-{tag}-{id}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn open(storage: &Path) -> Executor<PlomidStorageEngine> {
    let engine = PlomidStorageEngine::open(storage, &storage.join("wal-legacy-arg"), 32)
        .expect("engine open");
    Executor::new(engine).expect("executor")
}

fn exec(ex: &mut Executor<PlomidStorageEngine>, sql: &str) {
    ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}"));
}

fn scalar_i64(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> i64 {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => match &rows[0][0] {
            Value::Int8(n) => *n,
            Value::Int4(n) => *n as i64,
            Value::Int2(n) => *n as i64,
            v => panic!("{sql} -> non-int {v:?}"),
        },
        other => panic!("{sql} -> {other:?}"),
    }
}

fn seed(ex: &mut Executor<PlomidStorageEngine>) {
    exec(ex, "CREATE TABLE drill (id INTEGER PRIMARY KEY, v INTEGER)");
    let mut values = Vec::new();
    for id in 1..=500i64 {
        values.push(format!("({id}, {})", id * 3 + 7));
    }
    exec(
        ex,
        &format!("INSERT INTO drill VALUES {};", values.join(",")),
    );
    exec(ex, "CREATE INDEX drill_v_idx ON drill (v)");
    // Committed row via autocommit (explicit BEGIN/COMMIT is exercised on
    // the restored image, where the session starts clean).
    exec(ex, "INSERT INTO drill VALUES (501, 1510)");
}

fn checksum(ex: &mut Executor<PlomidStorageEngine>) -> (i64, i64) {
    let count = scalar_i64(ex, "SELECT COUNT(*) FROM drill");
    let sum = scalar_i64(ex, "SELECT SUM(v) FROM drill");
    (count, sum)
}

#[test]
fn full_backup_restore_roundtrip() {
    let src = scratch("src");
    let backup_dir = scratch("backup");
    let restored = scratch("restored");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&restored);

    let (count_before, sum_before) = {
        let mut ex = open(&src);
        seed(&mut ex);
        // Point lookup via the index + ordered scan prove the index works.
        let point = scalar_i64(&mut ex, "SELECT v FROM drill WHERE id = 42");
        assert_eq!(point, 42 * 3 + 7);
        let ordered: Vec<i64> = match ex
            .execute("SELECT id FROM drill ORDER BY id LIMIT 3")
            .unwrap()
        {
            QueryResult::Rows { rows, .. } => rows
                .iter()
                .map(|r| match r[0] {
                    Value::Int4(n) => n as i64,
                    Value::Int8(n) => n,
                    ref v => panic!("id {v:?}"),
                })
                .collect(),
            other => panic!("order {other:?}"),
        };
        assert_eq!(ordered, vec![1, 2, 3]);
        let sums = checksum(&mut ex);
        ex.engine_mut()
            .checkpoint()
            .expect("checkpoint before backup");
        drop(ex);
        sums
    };
    assert_eq!((count_before, sum_before), (501, 380_760));

    let manifest = backup::backup_database(&src, &backup_dir).expect("backup");
    assert!(manifest.files > 0, "backup must contain files");
    let reread = backup::read_backup_manifest(&backup_dir).expect("manifest reads");
    assert_eq!(reread, manifest);

    backup::restore_database(&backup_dir, &restored).expect("restore");

    {
        let mut ex = open(&restored);
        // Schema survived (query the restored table, not information_schema).
        let (count, sum) = checksum(&mut ex);
        assert_eq!(
            (count, sum),
            (count_before, sum_before),
            "row count + checksum"
        );
        let point = scalar_i64(&mut ex, "SELECT v FROM drill WHERE v = 133");
        assert_eq!(point, 133, "index point lookup on restored image");
        // Transactions work on the restored image (embedded batch path).
        ex.execute_all("BEGIN; INSERT INTO drill VALUES (502, 1513); COMMIT;")
            .expect("txn batch");
        assert_eq!(scalar_i64(&mut ex, "SELECT COUNT(*) FROM drill"), 502);
        // New writes persist across a second reopen.
        ex.engine_mut().checkpoint().expect("checkpoint restored");
        drop(ex);
    }
    {
        let mut ex = open(&restored);
        assert_eq!(scalar_i64(&mut ex, "SELECT COUNT(*) FROM drill"), 502);
    }

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&restored);
}

#[test]
fn interrupted_backup_has_no_manifest_and_restore_refuses() {
    let src = scratch("src-partial");
    let partial = scratch("partial");
    let target = scratch("target-partial");
    let _ = std::fs::remove_dir_all(&partial);
    let _ = std::fs::remove_dir_all(&target);
    {
        let mut ex = open(&src);
        seed(&mut ex);
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    // Simulate a kill mid-copy: files copied, manifest never written.
    std::fs::create_dir_all(&partial).unwrap();
    std::fs::copy(src.join("CURRENT"), partial.join("CURRENT")).unwrap();
    let err = backup::restore_database(&partial, &target).expect_err("must refuse manifest-less");
    assert_eq!(
        err.kind(),
        plomid_core::ErrorKind::Corruption,
        "interrupted backup is corruption, got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&partial);
    let _ = std::fs::remove_dir_all(&target);
}

#[test]
fn incompatible_backup_version_is_refused_no_silent_downgrade() {
    let src = scratch("src-ver");
    let backup_dir = scratch("backup-ver");
    let target = scratch("target-ver");
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&target);
    {
        let mut ex = open(&src);
        seed(&mut ex);
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    backup::backup_database(&src, &backup_dir).expect("backup");
    // Tamper the manifest to a future version (old binary → new backup).
    let manifest_path = backup_dir.join(backup::BACKUP_MANIFEST_FILE);
    let text = std::fs::read_to_string(&manifest_path).unwrap();
    let tampered = text.replace("backup_format=1", "backup_format=99");
    std::fs::write(&manifest_path, tampered).unwrap();
    let err = backup::restore_database(&backup_dir, &target).expect_err("must refuse version");
    assert_eq!(
        err.kind(),
        plomid_core::ErrorKind::Unsupported,
        "got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&backup_dir);
    let _ = std::fs::remove_dir_all(&target);
}

#[test]
fn restore_refuses_to_overwrite_existing_database_rollback_policy() {
    let src = scratch("src-rollback");
    let backup_dir = scratch("backup-rollback");
    let _ = std::fs::remove_dir_all(&backup_dir);
    {
        let mut ex = open(&src);
        seed(&mut ex);
        ex.engine_mut().checkpoint().expect("checkpoint");
        drop(ex);
    }
    backup::backup_database(&src, &backup_dir).expect("backup");
    // Rollback policy: restore-to-clean only. Restoring over the live src
    // must fail instead of silently merging generations.
    let err = backup::restore_database(&backup_dir, &src).expect_err("must refuse overwrite");
    assert_eq!(
        err.kind(),
        plomid_core::ErrorKind::AlreadyExists,
        "got {err:?}"
    );

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&backup_dir);
}
