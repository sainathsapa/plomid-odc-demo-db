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
//! Offline cold backup / restore / inspect (P1-004).
//!
//! Authority rule (unchanged from `plomid-txn::backup`): a backup is a
//! filesystem copy of the engine data directory taken after a checkpoint
//! with the engine closed (cold), plus a manifest written last. Copying
//! under a live writer is unsupported (torn copy) — every mutating command
//! here refuses to run against a live server.
//!
//! Liveness fencing uses an advisory data-directory lock (`LOCK`, held for
//! the process lifetime and released by the OS on death, so it can never go
//! stale): the serving path holds it while running, and these tools require
//! it. A server predating the lock cannot be detected — stop it first; the
//! documented runbook says so.
//!
//! There is deliberately no hot `BACKUP` SQL: online backup needs
//! write-quiesce/fencing and a WAL boundary under concurrency, a new
//! correctness surface with no Beta need. The documented contract stays
//! cold full backup, restore-to-clean-only, no incremental, no PITR.

use std::path::{Path, PathBuf};

/// Data-directory lock filename. Advisory only; presence alone means
/// nothing (crashes leave the file behind) — only holding the flock does.
pub const LOCK_FILE_NAME: &str = "LOCK";

/// Process exit codes for the offline tools.
pub const EXIT_OK: i32 = 0;
pub const EXIT_USAGE: i32 = 1;
pub const EXIT_LIVE_OR_REFUSED: i32 = 2;
pub const EXIT_OPERATION_FAILED: i32 = 3;

/// If `argv` invokes an offline tool (`backup`, `restore`, `backup-inspect`
/// as the first argument), run it and return its exit code. Returns `None`
/// for the normal serve path (including `--help`/`--version`, which keep
/// the server's existing parsing untouched).
pub fn dispatch_offline_tool(argv: &[String]) -> Option<i32> {
    let subcommand = argv.get(1).map(String::as_str)?;
    if !matches!(
        subcommand,
        "backup" | "restore" | "backup-inspect" | "help-backup"
    ) {
        return None;
    }
    Some(match subcommand {
        "backup" => cmd_backup(&parse_key_values(&argv[2..])),
        "restore" => cmd_restore(&parse_key_values(&argv[2..])),
        "backup-inspect" => cmd_inspect(&parse_key_values(&argv[2..])),
        _ => {
            print_usage(&argv[0]);
            EXIT_OK
        }
    })
}

fn parse_key_values(args: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut index = 0usize;
    while index < args.len() {
        let arg = &args[index];
        if let Some(value) = arg.strip_prefix("--data=") {
            out.push(("data".to_string(), value.to_string()));
        } else if let Some(value) = arg.strip_prefix("--to=") {
            out.push(("to".to_string(), value.to_string()));
        } else if let Some(value) = arg.strip_prefix("--from=") {
            out.push(("from".to_string(), value.to_string()));
        } else if let Some(value) = arg.strip_prefix("--backup=") {
            out.push(("backup".to_string(), value.to_string()));
        } else if arg == "--data" || arg == "--to" || arg == "--from" || arg == "--backup" {
            let key = arg.trim_start_matches("--").to_string();
            if let Some(next) = args.get(index + 1) {
                out.push((key, next.clone()));
                index += 1;
            } else {
                out.push((key, String::new()));
            }
        } else {
            out.push(("unexpected".to_string(), arg.clone()));
        }
        index += 1;
    }
    out
}

fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn print_usage(prog: &str) {
    println!("PLOMID offline cold backup tools (engine stopped; never under a live writer)");
    println!();
    println!("Usage:");
    println!("  {prog} backup --data <DATA_DIR> --to <BACKUP_DIR>");
    println!("  {prog} restore --from <BACKUP_DIR> --to <CLEAN_DATA_DIR>");
    println!("  {prog} backup-inspect --backup <BACKUP_DIR>");
    println!();
    println!("Contract: cold full backup only (checkpoint + close, then copy,");
    println!("manifest written last), restore-to-clean-only, no incremental,");
    println!("no PITR. Mutating commands refuse live data directories (exit 2).");
}

fn usage_error(prog: &str, message: &str) -> i32 {
    eprintln!("error: {message}");
    eprintln!("run `{prog} help-backup` for usage");
    EXIT_USAGE
}

/// Acquires the advisory data-directory lock, failing closed when another
/// process holds it (a live server).
///
/// The returned file must stay open for as long as the exclusion is needed;
/// dropping it releases the lock. Implemented with the standard library's
/// advisory file lock (no `unsafe`, no new dependency); the OS releases it
/// on process death, so crashes can never wedge it. This works on both Unix
/// (`flock`) and Windows (`LockFileEx`); an unfenced copy under a live
/// writer would be torn, so any lock error propagates.
pub fn hold_data_dir_lock(data_dir: &Path) -> std::io::Result<std::fs::File> {
    let path = data_dir.join(LOCK_FILE_NAME);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            format!(
                "data directory is locked by a live process (stop the server on {} first)",
                data_dir.display()
            ),
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

fn cmd_backup(pairs: &[(String, String)]) -> i32 {
    let (Some(data), Some(dst)) = (get(pairs, "data"), get(pairs, "to")) else {
        return usage_error(
            "plomid-server",
            "backup needs --data <DATA_DIR> --to <BACKUP_DIR>",
        );
    };
    if pairs.iter().any(|(k, _)| k == "unexpected") || data.is_empty() || dst.is_empty() {
        return usage_error(
            "plomid-server",
            "backup needs --data <DATA_DIR> --to <BACKUP_DIR>",
        );
    }
    let data_dir = PathBuf::from(data);
    let dst_dir = PathBuf::from(dst);
    let _lock = match hold_data_dir_lock(&data_dir) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("error: refusing backup: {error}");
            return EXIT_LIVE_OR_REFUSED;
        }
    };
    // Quiesce exactly like the tested drill: open, checkpoint, close —
    // with the lock held no live writer can interleave.
    let wal_dir = data_dir.join("wal");
    let mut engine = match plomid_txn::PlomidStorageEngine::open(&data_dir, &wal_dir, 1024) {
        Ok(engine) => engine,
        Err(error) => {
            eprintln!(
                "error: cannot open engine at {}: {error}",
                data_dir.display()
            );
            return EXIT_OPERATION_FAILED;
        }
    };
    if let Err(error) = engine.checkpoint() {
        eprintln!("error: pre-backup checkpoint failed: {error}");
        return EXIT_OPERATION_FAILED;
    }
    drop(engine);
    match plomid_txn::backup::backup_database(&data_dir, &dst_dir) {
        Ok(manifest) => {
            println!(
                "backup complete: {} files, {} bytes -> {} (manifest format {})",
                manifest.files,
                manifest.bytes,
                dst_dir.display(),
                manifest.format_version
            );
            EXIT_OK
        }
        Err(error) => {
            eprintln!("error: backup failed: {error}");
            EXIT_OPERATION_FAILED
        }
    }
}

fn cmd_restore(pairs: &[(String, String)]) -> i32 {
    let (Some(src), Some(dst)) = (get(pairs, "from"), get(pairs, "to")) else {
        return usage_error(
            "plomid-server",
            "restore needs --from <BACKUP_DIR> --to <CLEAN_DATA_DIR>",
        );
    };
    if pairs.iter().any(|(k, _)| k == "unexpected") || src.is_empty() || dst.is_empty() {
        return usage_error(
            "plomid-server",
            "restore needs --from <BACKUP_DIR> --to <CLEAN_DATA_DIR>",
        );
    }
    let dst_dir = PathBuf::from(dst);
    if let Err(error) = std::fs::create_dir_all(&dst_dir) {
        eprintln!("error: cannot create {}: {error}", dst_dir.display());
        return EXIT_OPERATION_FAILED;
    }
    // Fence the destination before copying into it: a live server there
    // would race the copy and read a torn image.
    let _lock = match hold_data_dir_lock(&dst_dir) {
        Ok(lock) => lock,
        Err(error) => {
            eprintln!("error: refusing restore: {error}");
            return EXIT_LIVE_OR_REFUSED;
        }
    };
    match plomid_txn::backup::restore_database(Path::new(src), &dst_dir) {
        Ok(manifest) => {
            println!(
                "restore complete: {} files, {} bytes -> {} (backup format {})",
                manifest.files,
                manifest.bytes,
                dst_dir.display(),
                manifest.format_version
            );
            println!(
                "start the server on {} and verify (COUNT/SUM, one write + reopen)",
                dst
            );
            EXIT_OK
        }
        Err(error) => {
            eprintln!("error: restore failed: {error}");
            EXIT_OPERATION_FAILED
        }
    }
}

fn cmd_inspect(pairs: &[(String, String)]) -> i32 {
    let Some(dir) = get(pairs, "backup") else {
        return usage_error(
            "plomid-server",
            "backup-inspect needs --backup <BACKUP_DIR>",
        );
    };
    if pairs.iter().any(|(k, _)| k == "unexpected") || dir.is_empty() {
        return usage_error(
            "plomid-server",
            "backup-inspect needs --backup <BACKUP_DIR>",
        );
    }
    match plomid_txn::backup::read_backup_manifest(Path::new(dir)) {
        Ok(manifest) => {
            println!("manifest format: {}", manifest.format_version);
            println!(
                "versions: catalog={} checkpoint={} generation={} wal={} segment={}",
                manifest.catalog_version,
                manifest.checkpoint_version,
                manifest.generation_version,
                manifest.wal_record_version,
                manifest.segment_version
            );
            println!("created_unix: {}", manifest.created_unix);
            println!("files: {} bytes: {}", manifest.files, manifest.bytes);
            EXIT_OK
        }
        Err(error) => {
            eprintln!("error: unreadable backup: {error}");
            EXIT_OPERATION_FAILED
        }
    }
}
