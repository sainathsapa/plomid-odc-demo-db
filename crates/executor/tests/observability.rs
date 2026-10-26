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
//! Production observability: in-process counters, the `plomid_stat_*` SQL
//! surface, and the freshness-age signal.
//!
//! One test function (not several) so the process-wide counters need no
//! inter-test synchronization: this binary runs a single flow with one reset
//! at the start.

use plomid_core::observability;
use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn scratch(label: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "plomid-observability-{label}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    path
}

fn cleanup(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(root.with_extension("wal"));
}

fn open(root: &std::path::Path) -> Executor<PlomidStorageEngine> {
    let wal = root.with_extension("wal");
    let engine = PlomidStorageEngine::create(root, &wal, 64)
        .or_else(|_| PlomidStorageEngine::open(root, &wal, 64))
        .expect("open engine");
    Executor::new(engine).expect("executor")
}

fn rows_of(result: QueryResult) -> Vec<Vec<Value>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn columns_of(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Rows { columns, .. } => columns.clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn int(value: &Value) -> i64 {
    match value {
        Value::Int8(n) => *n,
        Value::Int4(n) => *n as i64,
        Value::Int2(n) => *n as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

fn bool_of(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        other => panic!("expected bool, got {other:?}"),
    }
}

fn text_of(value: &Value) -> &str {
    match value {
        Value::Text(text) => text,
        other => panic!("expected text, got {other:?}"),
    }
}

/// Runs the counters query and returns `(columns, first row)`.
fn counters_query(session: &mut Executor<PlomidStorageEngine>) -> (Vec<String>, Vec<Value>) {
    let result = session
        .execute("SELECT * FROM plomid_stat_counters()")
        .expect("counters query");
    let columns = columns_of(&result);
    let mut rows = rows_of(result);
    assert_eq!(rows.len(), 1, "counters surface is exactly one row");
    (columns, rows.remove(0))
}

fn column<'a>(columns: &[String], row: &'a [Value], name: &str) -> &'a Value {
    let index = columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("missing counters column {name}"));
    &row[index]
}

#[test]
fn observability_counters_and_freshness() {
    observability::reset_for_tests();
    let root = scratch("flow");
    let mut session = open(&root);
    let baseline = observability::snapshot();

    // Four batches: three succeed, one fails.
    session
        .execute("CREATE TABLE obs_t (id INTEGER, v TEXT);")
        .expect("create");
    session
        .execute("INSERT INTO obs_t VALUES (1, 'a'), (2, 'b');")
        .expect("insert");
    let selected = rows_of(session.execute("SELECT * FROM obs_t;").expect("select"));
    assert_eq!(selected.len(), 2);
    assert!(session.execute("SELECT * FROM no_such_table;").is_err());

    let after_queries = observability::snapshot();
    assert_eq!(after_queries.queries_total - baseline.queries_total, 4);
    assert_eq!(after_queries.errors_total - baseline.errors_total, 1);
    // Debug builds are fast: nothing here should trip the 100ms slow gate.
    assert_eq!(
        after_queries.slow_queries_total - baseline.slow_queries_total,
        0
    );

    // The SQL surface reports the same snapshot. Note the counters row is
    // rendered before its own batch is recorded, so it matches the
    // pre-query snapshot exactly (`after_queries`), not a later one.
    let (columns, row) = counters_query(&mut session);
    assert_eq!(
        columns,
        vec![
            "queries_total",
            "errors_total",
            "connections_opened",
            "connections_closed",
            "slow_queries_total",
            "slow_max_ms",
            "maintenance_passes_total",
            "checkpoints_total",
            "analytical_total",
            "tables_stale",
            "tls_connections",
            "tls_handshake_failures",
            "tls_plaintext_refusals",
        ]
    );
    let live = after_queries;
    assert_eq!(
        int(column(&columns, &row, "queries_total")),
        live.queries_total as i64
    );
    assert_eq!(
        int(column(&columns, &row, "errors_total")),
        live.errors_total as i64
    );
    // The write dirtied the table before it was observable: one stale table.
    assert_eq!(int(column(&columns, &row, "tables_stale")), 1);

    // The bare-table spelling resolves through the same catalog arm.
    let bare = rows_of(
        session
            .execute("SELECT queries_total FROM plomid_stat_counters")
            .expect("bare counters"),
    );
    assert_eq!(bare.len(), 1);

    // Per-table freshness: dirty after the write above.
    let tables = rows_of(
        session
            .execute("SELECT * FROM plomid_stat_tables()")
            .expect("tables"),
    );
    assert!(
        tables.iter().any(|row| {
            row.len() == 3
                && text_of(&row[0]).ends_with("obs_t")
                && int(&row[1]) >= 1
                && bool_of(&row[2])
        }),
        "obs_t must be stale after the write, got {tables:?}"
    );

    // VACUUM publishes a generation covering the write: fresh again, and the
    // pass is counted (VACUUM converges on maintain_table like every pass).
    let maintenance_before = observability::snapshot().maintenance_passes_total;
    session.execute("VACUUM obs_t;").expect("vacuum");
    assert!(
        observability::snapshot().maintenance_passes_total > maintenance_before,
        "VACUUM must record a maintenance pass"
    );
    let tables = rows_of(
        session
            .execute("SELECT * FROM plomid_stat_tables()")
            .expect("tables after vacuum"),
    );
    assert!(
        tables.iter().any(|row| {
            row.len() == 3 && text_of(&row[0]).ends_with("obs_t") && !bool_of(&row[2])
        }),
        "obs_t must be fresh after VACUUM, got {tables:?}"
    );
    let (_, row) = counters_query(&mut session);
    assert_eq!(int(column(&columns, &row, "tables_stale")), 0);

    // Explicit engine checkpoint counts (any trigger counts exactly once).
    let checkpoints_before = observability::snapshot().checkpoints_total;
    session
        .engine_mut()
        .checkpoint()
        .expect("explicit checkpoint");
    assert_eq!(
        observability::snapshot().checkpoints_total,
        checkpoints_before + 1
    );

    // Per-path analytical surface: zero markers for the skipped query/* paths.
    let analytical = rows_of(
        session
            .execute("SELECT * FROM plomid_stat_analytical()")
            .expect("analytical"),
    );
    for skipped in observability::SKIPPED_ANALYTICAL_PATHS {
        assert!(
            analytical
                .iter()
                .any(|row| { row.len() == 2 && text_of(&row[0]) == *skipped && int(&row[1]) == 0 }),
            "missing zero marker for {skipped}: {analytical:?}"
        );
    }

    // A selective indexed INNER JOIN is served from indexes (not full scans)
    // and records the wired analytical path.
    session
        .execute("CREATE TABLE obs_l (id INTEGER, v INTEGER);")
        .expect("create l");
    session
        .execute("CREATE TABLE obs_r (id INTEGER, v INTEGER);")
        .expect("create r");
    session
        .execute("CREATE INDEX obs_l_id ON obs_l (id);")
        .expect("index l");
    session
        .execute("CREATE INDEX obs_r_id ON obs_r (id);")
        .expect("index r");
    session
        .execute("INSERT INTO obs_l VALUES (1, 10), (2, 20), (3, 30);")
        .expect("load l");
    session
        .execute("INSERT INTO obs_r VALUES (1, 100), (2, 200), (9, 900);")
        .expect("load r");
    let analytical_before = observability::snapshot().analytical_total;
    let joined = rows_of(
        session
            .execute(
                "SELECT l.id, r.v FROM obs_l AS l JOIN obs_r AS r ON l.id = r.id WHERE l.id = 1;",
            )
            .expect("indexed join"),
    );
    assert_eq!(joined.len(), 1);
    assert_eq!(int(&joined[0][0]), 1);
    assert_eq!(int(&joined[0][1]), 100);
    let analytical_after = observability::snapshot();
    assert!(
        analytical_after.analytical_total > analytical_before,
        "indexed join must record an analytical path"
    );
    assert!(
        analytical_after
            .analytical_by_path
            .iter()
            .any(|(path, count)| path == "indexed_join" && *count > 0),
        "indexed_join path must be counted: {:?}",
        analytical_after.analytical_by_path
    );

    // A large equi-INNER JOIN fans out to the parallel hash join (when the
    // machine has more than one core) and records that path too.
    if std::thread::available_parallelism()
        .map(|cores| cores.get() >= 2)
        .unwrap_or(false)
    {
        let mut load_l = String::from("INSERT INTO obs_l VALUES ");
        for id in 4..2104 {
            if id > 4 {
                load_l.push_str(", ");
            }
            load_l.push_str(&format!("({id}, {id})"));
        }
        load_l.push(';');
        session.execute(&load_l).expect("bulk load l");
        let mut load_r = String::from("INSERT INTO obs_r VALUES ");
        // Start at 10: ids 1, 2 and 9 already exist above, and duplicates
        // would multiply the join matches.
        for id in 10..510 {
            if id > 10 {
                load_r.push_str(", ");
            }
            load_r.push_str(&format!("({id}, {id})"));
        }
        load_r.push(';');
        session.execute(&load_r).expect("bulk load r");
        let parallel_before = observability::snapshot();
        let joined = rows_of(
            session
                .execute("SELECT l.id FROM obs_l AS l JOIN obs_r AS r ON l.id = r.id;")
                .expect("parallel join"),
        );
        // obs_r holds ids 1, 2, 9 plus 10..509; obs_l holds 1..2103 and every
        // right id is unique: 503 matches.
        assert_eq!(joined.len(), 503);
        let parallel_after = observability::snapshot();
        assert!(
            parallel_after
                .analytical_by_path
                .iter()
                .any(|(path, count)| path == "parallel_join" && *count > 0),
            "parallel join must record the parallel_join path on multi-core: {:?} (before {parallel_before:?})",
            parallel_after.analytical_by_path
        );
    }

    cleanup(&root);
}
