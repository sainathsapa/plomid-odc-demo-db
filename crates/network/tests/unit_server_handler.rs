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
// Moved from `src/server/handler.rs` inline `#[cfg(test)]` — public-API only.
mod savepoint_name_tests {
    use plomid_network::server::handler::savepoint_name;

    #[test]
    fn extracts_names_for_all_shapes() {
        assert_eq!(
            savepoint_name("SAVEPOINT", "SAVEPOINT sp1;"),
            Some("sp1".to_string())
        );
        assert_eq!(
            savepoint_name("RELEASE", "RELEASE sp1;"),
            Some("sp1".to_string())
        );
        assert_eq!(
            savepoint_name("RELEASE", "RELEASE SAVEPOINT sp1;"),
            Some("sp1".to_string())
        );
        assert_eq!(
            savepoint_name("ROLLBACK_TO_SAVEPOINT", "ROLLBACK TO SAVEPOINT sp1;"),
            Some("sp1".to_string())
        );
        assert_eq!(
            savepoint_name("ROLLBACK_TO_SAVEPOINT", "ROLLBACK TO sp1;"),
            Some("sp1".to_string())
        );
    }

    #[test]
    fn missing_name_is_none_not_empty() {
        assert_eq!(savepoint_name("SAVEPOINT", "SAVEPOINT;"), None);
        assert_eq!(savepoint_name("RELEASE", "RELEASE;"), None);
        assert_eq!(savepoint_name("RELEASE", "RELEASE SAVEPOINT;"), None);
        assert_eq!(
            savepoint_name("ROLLBACK_TO_SAVEPOINT", "ROLLBACK TO SAVEPOINT;"),
            None
        );
    }
}

mod sqlstate_tests {
    use plomid_core::{ErrorKind, PlomidError};
    use plomid_executor::SqlError;
    use plomid_network::server::handler::*;

    fn state(kind: ErrorKind, message: &str) -> &'static str {
        sqlstate_for(kind, message)
    }

    #[test]
    fn object_errors_map_to_postgres_class_42() {
        assert_eq!(state(ErrorKind::Syntax, "syntax error"), "42601");
        assert_eq!(
            state(ErrorKind::NotFound, "table \"missing\" does not exist"),
            "42P01"
        );
        assert_eq!(
            state(ErrorKind::Catalog, "column \"x\" does not exist"),
            "42703"
        );
        assert_eq!(
            state(ErrorKind::Catalog, "function f() does not exist"),
            "42883"
        );
        assert_eq!(
            state(ErrorKind::AlreadyExists, "relation \"t\" already exists"),
            "42710"
        );
    }

    #[test]
    fn constraint_errors_map_to_postgres_class_23() {
        assert_eq!(
            state(
                ErrorKind::Conflict,
                "null value in column \"name\" violates not-null constraint"
            ),
            "23502"
        );
        assert_eq!(
            state(
                ErrorKind::Conflict,
                "duplicate key value violates unique index \"users_pkey\""
            ),
            "23505"
        );
        assert_eq!(
            state(
                ErrorKind::Conflict,
                "value in column \"age\" violates check constraint"
            ),
            "23514"
        );
    }

    #[test]
    fn aborted_transaction_maps_to_25p02() {
        assert_eq!(
            state(
                ErrorKind::Conflict,
                "current transaction is aborted, commands ignored until ROLLBACK"
            ),
            "25P02"
        );
    }

    #[test]
    fn data_and_transaction_errors_map_to_their_classes() {
        assert_eq!(
            state(ErrorKind::InvalidArgument, "division by zero"),
            "22012"
        );
        assert_eq!(state(ErrorKind::Transaction, "txn"), "25P02");
        assert_eq!(state(ErrorKind::Unsupported, "feature"), "0A000");
        assert_eq!(state(ErrorKind::Internal, "boom"), "XX000");
    }

    /// Exhaustive kind coverage: every `ErrorKind` maps to a documented
    /// primary SQLSTATE with a keyword-free message. Adding a kind without
    /// extending this table fails the build of this test's intent — update
    /// both the arm in `sqlstate_for` and this row.
    #[test]
    fn every_error_kind_maps_to_a_documented_sqlstate() {
        let cases = [
            (ErrorKind::Syntax, "syntax error at end", "42601"),
            (ErrorKind::Catalog, "database \"gone\" missing", "3D000"),
            (ErrorKind::Catalog, "column \"x\" missing", "42703"),
            (ErrorKind::Catalog, "function f() missing", "42883"),
            (ErrorKind::Catalog, "table \"t\" missing", "42P01"),
            (ErrorKind::NotFound, "table \"t\" missing", "42P01"),
            (
                ErrorKind::AlreadyExists,
                "relation \"t\" already exists",
                "42710",
            ),
            (ErrorKind::Conflict, "null value violates not-null", "23502"),
            (
                ErrorKind::Conflict,
                "duplicate key violates unique",
                "23505",
            ),
            (ErrorKind::Conflict, "violates check constraint", "23514"),
            (ErrorKind::Conflict, "violates foreign key", "23503"),
            (
                ErrorKind::Conflict,
                "current transaction is aborted, commands ignored",
                "25P02",
            ),
            (ErrorKind::Conflict, "serialization failure", "40001"),
            (ErrorKind::Transaction, "no transaction is active", "25P02"),
            (ErrorKind::Aborted, "statement was aborted", "25P02"),
            (ErrorKind::InvalidArgument, "division by zero", "22012"),
            (ErrorKind::InvalidArgument, "value out of range", "22003"),
            (ErrorKind::InvalidArgument, "bad argument", "22023"),
            (ErrorKind::RowEncoding, "row decode failed", "22000"),
            (ErrorKind::PermissionDenied, "permission denied", "42501"),
            (ErrorKind::Unsupported, "not supported here", "0A000"),
            (ErrorKind::Io, "no such file", "58P01"),
            (ErrorKind::Io, "disk error", "58000"),
            (ErrorKind::Wal, "wal replay failed", "58000"),
            (ErrorKind::Corruption, "checksum mismatch", "XX001"),
            (ErrorKind::Internal, "unexpected state", "XX000"),
        ];
        for (kind, message, code) in cases {
            assert_eq!(state(kind, message), code, "kind={kind:?}");
        }
    }

    /// No unexpected `INTERNAL`: with a keyword-free message, only
    /// `ErrorKind::Internal` may map to `XX000`. A new kind falling into
    /// the `_ => "XX000"` arm fails here until it gets a real mapping.
    #[test]
    fn no_unexpected_internal_sqlstate() {
        let neutral: &[ErrorKind] = &[
            ErrorKind::Syntax,
            ErrorKind::Catalog,
            ErrorKind::NotFound,
            ErrorKind::AlreadyExists,
            ErrorKind::Conflict,
            ErrorKind::Transaction,
            ErrorKind::Aborted,
            ErrorKind::InvalidArgument,
            ErrorKind::RowEncoding,
            ErrorKind::PermissionDenied,
            ErrorKind::Unsupported,
            ErrorKind::Io,
            ErrorKind::Wal,
            ErrorKind::Corruption,
        ];
        for kind in neutral {
            let code = state(*kind, "neutral probe without keywords");
            assert_ne!(code, "XX000", "kind={kind:?} must not escape as INTERNAL");
        }
        assert_eq!(state(ErrorKind::Internal, "neutral probe"), "XX000");
    }

    #[test]
    fn wire_detail_redacts_datadir_but_keeps_code() {
        use std::path::Path;
        let datadir = Path::new("/var/lib/plomid/data");
        let err = SqlError::Storage(PlomidError::with_detail(
            ErrorKind::Io,
            "open failed",
            "root=/var/lib/plomid/data/wal/WAL-0001.dat: io error",
        ));
        let response = executor_error_response_with_datadir(err, Some(datadir));
        assert_eq!(response.code, "58000");
        let detail = response.detail.expect("detail preserved");
        assert!(!detail.contains("/var/lib/plomid/data"), "got: {detail}");
        assert!(detail.contains("<datadir>"), "got: {detail}");
        // Unrelated absolute paths never leak either.
        let other = redact_detail("read /etc/passwd failed", Some(datadir));
        assert!(!other.contains("/etc/passwd"), "got: {other}");
        assert!(other.contains("<path>"), "got: {other}");
    }
}

mod isolation_tests {
    use plomid_network::server::handler::*;
    use plomid_network::session::IsolationLevel;
    #[test]
    fn begin_levels_parse() {
        use plomid_network::server::handler::ParsedIsolation::*;
        assert_eq!(parse_isolation_level("BEGIN"), None);
        assert_eq!(parse_isolation_level("BEGIN;"), None);
        assert_eq!(parse_isolation_level("START TRANSACTION"), None);
        assert_eq!(
            parse_isolation_level("BEGIN ISOLATION LEVEL REPEATABLE READ"),
            Some(RepeatableRead)
        );
        assert_eq!(
            parse_isolation_level("start transaction isolation level repeatable read;"),
            Some(RepeatableRead)
        );
        assert_eq!(
            parse_isolation_level("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Some(ReadCommitted)
        );
        assert_eq!(
            parse_isolation_level("BEGIN ISOLATION LEVEL SERIALIZABLE"),
            Some(Serializable)
        );
        assert_eq!(
            parse_isolation_level("BEGIN ISOLATION LEVEL READ UNCOMMITTED"),
            Some(ReadCommitted)
        );
        assert_eq!(
            parse_isolation_level("BEGIN ISOLATION LEVEL CHAOS"),
            Some(Invalid)
        );
        assert_eq!(
            parse_isolation_level(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            ),
            Some(RepeatableRead)
        );
        assert_eq!(
            parse_isolation_level("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            Some(Serializable)
        );
    }

    #[test]
    fn set_scopes_split_session_from_transaction() {
        use plomid_network::server::handler::IsolationSetScope::*;
        assert_eq!(
            isolation_set_scope(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            ),
            SessionDefault
        );
        assert_eq!(
            isolation_set_scope("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            CurrentTransaction
        );
        assert_eq!(
            isolation_set_scope("SET LOCAL TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            CurrentTransaction
        );
    }

    #[test]
    fn isolation_display_matches_postgres_spelling() {
        assert_eq!(
            isolation_display(IsolationLevel::ReadCommitted),
            "read committed"
        );
        assert_eq!(
            isolation_display(IsolationLevel::RepeatableRead),
            "repeatable read"
        );
    }
}

mod bind_tests {
    use plomid_network::server::handler::*;

    fn some(values: &[&str]) -> Vec<Option<String>> {
        values.iter().map(|v| Some(v.to_string())).collect()
    }

    #[test]
    fn multi_digit_placeholders_bind_independently() {
        let bound = bind_query(
            "SELECT $1, $2, $10",
            &some(&["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]),
        );
        assert_eq!(bound, "SELECT 'a', 'b', 'j'");
    }

    #[test]
    fn placeholders_inside_literals_are_preserved() {
        let bound = bind_query("SELECT 'a$1b' AS t, $1 AS v", &some(&["X"]));
        assert_eq!(bound, "SELECT 'a$1b' AS t, 'X' AS v");
    }

    #[test]
    fn placeholders_inside_comments_and_dollar_bodies_are_preserved() {
        let bound = bind_query("SELECT $1 -- $2\n", &some(&["7", "8"]));
        assert_eq!(bound, "SELECT 7 -- $2\n");
        let bound = bind_query("SELECT $$ $1 $$, $1", &some(&["7"]));
        assert_eq!(bound, "SELECT $$ $1 $$, 7");
    }

    #[test]
    fn missing_values_stay_placeholders_or_null() {
        // Supplied None renders NULL; a $N beyond the supplied slice is left
        // for the executor to reject rather than silently corrupted.
        assert_eq!(bind_query("SELECT $1", &[None]), "SELECT NULL");
        assert_eq!(bind_query("SELECT $2", &some(&["7"])), "SELECT $2");
    }

    #[test]
    fn max_placeholder_counts_references_outside_literals() {
        assert_eq!(max_placeholder("SELECT $1, $10"), 10);
        assert_eq!(max_placeholder("SELECT 'a$99b', $2"), 2);
        assert_eq!(max_placeholder("SELECT 1"), 0);
        assert_eq!(max_placeholder("SELECT -- $5\n$3"), 3);
    }

    #[test]
    fn text_null_stays_a_string() {
        assert_eq!(quote_bound_value("null"), "'null'");
        assert_eq!(quote_bound_value("NULL"), "'NULL'");
        assert_eq!(quote_bound_value("42"), "42");
        assert_eq!(quote_bound_value("o'brien"), "'o''brien'");
    }

    #[test]
    fn utf8_query_text_round_trips() {
        let bound = bind_query("SELECT 'héllo $1 wörld', $1", &some(&["✓"]));
        assert_eq!(bound, "SELECT 'héllo $1 wörld', '✓'");
    }
}

mod wire_stream_tests {
    use plomid_executor::result_stream::rows_to_stream;
    use plomid_network::server::handler::*;

    #[test]
    fn batched_wire_send_delivers_all_rows_in_order() {
        // Mirrors the wire-send loops (`finish_copy_to`, `send_query_result`):
        // the materialized `QueryResult::Rows` is consumed by the streaming
        // bridge and pulled in bounded batches, with the completion count
        // taken from delivered batches rather than `rows.len()`.
        let total = 2500usize;
        let rows: Vec<Vec<plomid_sql::Value>> = (0..total as i32)
            .map(|id| vec![plomid_sql::Value::Int4(id)])
            .collect();
        let expected = rows.clone();
        let result = plomid_sql::QueryResult::Rows {
            columns: vec!["id".to_string()],
            column_types: vec![None],
            rows,
        };
        // (`Default` here is the bridge `StreamConfig`, inferred from the
        // call signature — no new dependency.)
        let (_, mut stream) =
            rows_to_stream(result, Default::default()).expect("row results always stream");
        let mut delivered = Vec::new();
        let mut count = 0usize;
        while let Some(batch) = stream
            .next_batch(WIRE_SEND_BATCH_SIZE)
            .expect("infallible row source")
        {
            assert!(
                batch.len() <= WIRE_SEND_BATCH_SIZE,
                "batches stay bounded: {}",
                batch.len()
            );
            count += batch.len();
            delivered.extend(batch);
        }
        assert_eq!(count, total, "no silent truncation");
        assert_eq!(delivered, expected, "rows arrive in order");
    }
}

/// Symlink-pinned COPY jail opens: a planted link (file or parent) fails
/// closed instead of escaping, while normal read/write/overwrite keeps
/// working. Unix only: the pinned path is `O_NOFOLLOW` opens.
#[cfg(all(test, unix))]
mod pinned_open_tests {
    use plomid_network::server::handler::{
        check_copy_path_syntax, copy_display_name, is_copy_denial, read_pinned_file,
        resolve_copy_path, write_pinned_file, MAX_COPY_FILE_BYTES,
    };

    fn jail(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "plomid-copy-pinned-{tag}-{}-{id}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|t| t.as_nanos())
                .unwrap_or(0),
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("jail dir");
        dir
    }

    fn canonical(dir: &std::path::Path) -> std::path::PathBuf {
        std::fs::canonicalize(dir).expect("canonical jail")
    }

    #[test]
    fn normal_read_write_overwrite_roundtrip() {
        let jail = jail("roundtrip");
        let root = canonical(&jail);
        let validated =
            resolve_copy_path(&root, std::path::Path::new("data.csv")).expect("valid name");
        write_pinned_file(&root, &validated, b"a,b\n1,2\n").expect("write");
        assert_eq!(
            std::fs::read(&validated).expect("plain read"),
            b"a,b\n1,2\n"
        );
        // Overwrite preserves historical semantics (no O_EXCL failure).
        write_pinned_file(&root, &validated, b"x\n").expect("overwrite");
        assert_eq!(
            read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES).expect("read"),
            b"x\n"
        );
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn planted_file_symlink_is_refused() {
        let jail = jail("filelink");
        let root = canonical(&jail);
        let outside = jail.join("outside.txt");
        std::fs::write(&outside, "SECRET").expect("outside file");
        // Validate a benign name, then plant the link before the open —
        // the exact TOCTOU shape: validation passed, use must not follow.
        let validated =
            resolve_copy_path(&root, std::path::Path::new("real.csv")).expect("valid name");
        std::os::unix::fs::symlink(&outside, root.join("real.csv")).expect("plant link");
        let error =
            read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES).expect_err("link must fail");
        assert!(is_copy_denial(&error), "jail denial, got: {error}");
        // Nothing outside was touched, and nothing was created inside.
        assert_eq!(std::fs::read(&outside).expect("outside"), b"SECRET");
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn planted_parent_symlink_is_refused() {
        let jail = jail("parentlink");
        let root = canonical(&jail);
        let real = root.join("realdir");
        std::fs::create_dir_all(&real).expect("real dir");
        std::fs::write(real.join("data.csv"), b"ok").expect("seed file");
        std::os::unix::fs::symlink(&real, root.join("linkdir")).expect("plant dir link");
        // A validated path under the linked dir must not open through it.
        let validated = root.join("linkdir").join("data.csv");
        let error = read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES)
            .expect_err("dir link must fail");
        assert!(
            error.to_string().contains("not permitted"),
            "jail denial, got: {error}"
        );
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn write_never_follows_a_planted_link() {
        let jail = jail("writelink");
        let root = canonical(&jail);
        let outside = root.join("victim.txt");
        std::fs::write(&outside, "ORIGINAL").expect("victim file");
        // Validate a benign name, then plant the link before the open —
        // the exact TOCTOU shape: validation passed, use must not follow.
        let validated =
            resolve_copy_path(&root, std::path::Path::new("out.csv")).expect("valid name");
        std::os::unix::fs::symlink(&outside, root.join("out.csv")).expect("plant link");
        let error = write_pinned_file(&root, &validated, b"pwned").expect_err("link must fail");
        assert!(is_copy_denial(&error), "jail denial, got: {error}");
        assert_eq!(
            std::fs::read(&outside).expect("victim"),
            b"ORIGINAL",
            "planted link must never be truncated"
        );
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn dotdot_escape_is_rejected_before_open() {
        let jail = jail("dotdot");
        let root = canonical(&jail);
        assert!(resolve_copy_path(&root, std::path::Path::new("../escape.csv")).is_err());
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn traversal_variants_are_rejected_syntactically() {
        let jail = jail("syntax");
        let root = canonical(&jail);
        for bad in [
            "../escape.csv",
            "../../etc/passwd",
            "a/../b.csv",
            "a/../../escape.csv",
            "./b.csv",
            "a/./b.csv",
            ".",
            "..",
            "",
            "/etc/passwd",
            "/tmp/x.csv",
            "subdir/",
            "a/b/../../c",
        ] {
            assert!(
                check_copy_path_syntax(std::path::Path::new(bad)).is_err(),
                "syntax must deny {bad:?}"
            );
            assert!(
                resolve_copy_path(&root, std::path::Path::new(bad)).is_err(),
                "resolve must deny {bad:?}"
            );
        }
        // NUL and overlong inputs fail closed without touching the fs.
        assert!(check_copy_path_syntax(std::path::Path::new("a\0b.csv")).is_err());
        let long = "a".repeat(5000);
        assert!(check_copy_path_syntax(std::path::Path::new(&long)).is_err());
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn directories_are_never_copy_files() {
        let jail = jail("isdir");
        let root = canonical(&jail);
        let sub = root.join("sub");
        std::fs::create_dir_all(&sub).expect("sub dir");
        // Existing directory denied at resolve…
        assert!(resolve_copy_path(&root, std::path::Path::new("sub")).is_err());
        // …and at open even when handed a raw validated path.
        let raw = root.join("sub");
        let error =
            read_pinned_file(&root, &raw, MAX_COPY_FILE_BYTES).expect_err("dir read must fail");
        assert!(is_copy_denial(&error), "got: {error}");
        let error = write_pinned_file(&root, &raw, b"x").expect_err("dir write must fail");
        assert!(is_copy_denial(&error), "got: {error}");
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn fifo_never_blocks_and_is_refused() {
        let jail = jail("fifo");
        let root = canonical(&jail);
        let fifo = root.join("pipe.csv");
        // FIFO creation without `unsafe`: delegate to the platform
        // `mkfifo(1)` helper; skip when unavailable.
        let created = std::process::Command::new("mkfifo")
            .arg("-m")
            .arg("600")
            .arg(&fifo)
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !created {
            let _ = std::fs::remove_dir_all(&jail);
            return;
        }
        let validated = resolve_copy_path(&root, std::path::Path::new("pipe.csv"))
            .expect("fifo name resolves; open must refuse");
        let start = std::time::Instant::now();
        let error =
            read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES).expect_err("fifo must fail");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "fifo open blocked: {:?}",
            start.elapsed()
        );
        assert!(is_copy_denial(&error), "got: {error}");
        let error = write_pinned_file(&root, &validated, b"x").expect_err("fifo write must fail");
        assert!(is_copy_denial(&error), "got: {error}");
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn oversized_file_is_refused_not_oom() {
        let jail = jail("bigfile");
        let root = canonical(&jail);
        let validated =
            resolve_copy_path(&root, std::path::Path::new("big.csv")).expect("valid name");
        // Sparse over-limit file: no large allocation in the test itself.
        let file = std::fs::File::create(&validated).expect("create");
        file.set_len(MAX_COPY_FILE_BYTES as u64 + 1)
            .expect("sparse");
        drop(file);
        let error = read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES)
            .expect_err("oversize must fail");
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::FileTooLarge,
            "got: {error}"
        );
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn written_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let jail = jail("perms");
        let root = canonical(&jail);
        let validated =
            resolve_copy_path(&root, std::path::Path::new("secret.csv")).expect("valid name");
        write_pinned_file(&root, &validated, b"row\n").expect("write");
        let mode = std::fs::symlink_metadata(&validated)
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "COPY TO must be owner-only, got {mode:o}");
        // Overwrite of a lax file tightens it as well.
        std::fs::set_permissions(&validated, std::fs::Permissions::from_mode(0o644)).expect("lax");
        write_pinned_file(&root, &validated, b"row2\n").expect("overwrite");
        let mode = std::fs::symlink_metadata(&validated)
            .expect("stat2")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "overwrite must tighten, got {mode:o}");
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn swap_race_never_reads_outside() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let jail = jail("race");
        let root = canonical(&jail);
        let outside = root.join("outside.txt");
        std::fs::write(&outside, "OUTSIDE-SECRET").expect("outside");
        let inside = root.join("target.csv");
        std::fs::write(&inside, "INSIDE-OK").expect("inside");
        let validated =
            resolve_copy_path(&root, std::path::Path::new("target.csv")).expect("valid name");
        let stop = Arc::new(AtomicBool::new(false));
        let stopper = stop.clone();
        let swap_path = inside.clone();
        let swapper = std::thread::spawn(move || {
            // Hammer the leaf between real file and symlink-to-outside.
            // `open_pinned` must either read INSIDE-OK or deny — never
            // OUTSIDE-SECRET.
            for _ in 0..500 {
                if stopper.load(Ordering::Relaxed) {
                    break;
                }
                let _ = std::fs::remove_file(&swap_path);
                let _ = std::fs::write(&swap_path, "INSIDE-OK");
                let _ = std::fs::remove_file(&swap_path);
                let _ = std::os::unix::fs::symlink(&outside, &swap_path);
            }
        });
        let mut denials = 0;
        let mut oks = 0;
        for _ in 0..500 {
            match read_pinned_file(&root, &validated, MAX_COPY_FILE_BYTES) {
                Ok(data) => {
                    // The swapper's `write` is itself non-atomic (create +
                    // write): a transient empty/partial read is benign. What
                    // must never happen is exposing the link target.
                    assert!(
                        data != b"OUTSIDE-SECRET".as_slice(),
                        "race must never expose outside bytes"
                    );
                    // Empty reads are the create-vs-write window; anything
                    // non-empty must be a prefix of the inside content
                    // (partial write) or the full content.
                    assert!(
                        data.is_empty() || b"INSIDE-OK".starts_with(&data) || data == b"INSIDE-OK",
                        "unexpected bytes: {data:?}"
                    );
                    oks += 1;
                }
                Err(e) => {
                    assert!(
                        is_copy_denial(&e) || e.kind() == std::io::ErrorKind::NotFound,
                        "got: {e}"
                    );
                    denials += 1;
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        let _ = swapper.join();
        assert!(oks + denials == 500);
        let _ = std::fs::remove_dir_all(&jail);
    }

    #[test]
    fn error_display_never_echoes_paths() {
        let jail = jail("display");
        let root = canonical(&jail);
        let traversal = std::path::Path::new("../../etc/passwd");
        let err = resolve_copy_path(&root, traversal).expect_err("must deny");
        assert!(!err.contains(root.to_str().unwrap_or("")), "got: {err}");
        assert!(!err.contains("/etc/passwd"), "got: {err}");
        assert!(err.contains("passwd"), "leaf may show, got: {err}");
        // Control bytes are sanitized.
        let nasty = std::path::Path::new("a\nb\tc.csv");
        let shown = copy_display_name(nasty);
        assert!(
            !shown.contains('\n') && !shown.contains('\t'),
            "got: {shown:?}"
        );
        let _ = std::fs::remove_dir_all(&jail);
    }
}
