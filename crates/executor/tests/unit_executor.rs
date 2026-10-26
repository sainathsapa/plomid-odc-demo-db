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
// Moved from src/executor.rs inline #[cfg(test)].
mod tests {

    use plomid_core::ObjectId;
    use plomid_core::{ErrorKind, PlomidError};

    use plomid_executor::encoding::decode_row;
    use plomid_executor::error::{SqlError, SqlResult};
    use plomid_executor::executor::*;

    use plomid_sql::Catalog;
    use plomid_sql::{QueryResult, Value};

    use plomid_txn::PlomidStorageEngine;
    use plomid_txn::StorageEngine;

    #[test]
    fn end_to_end_sql_with_restart() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-e2e-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-e2e-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE users (id INTEGER, name TEXT);")?;
            executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
            executor.execute("BEGIN; INSERT INTO users (id, name) VALUES (2, 'Bob'); COMMIT;")?;
            executor.execute("UPDATE users SET name = 'Alice2' WHERE id = 1;")?;
            executor.execute("DELETE FROM users WHERE id = 2;")?;
            drop(executor);

            let engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM users;")? else {
                panic!("expected rows after restart");
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::Text("Alice2".to_string()));
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "end_to_end_sql_with_restart failed: {result:?}"
        );
    }

    #[test]
    fn delta_writes_confirm_on_commit_and_absorb_on_vacuum() {
        // Wired-loop proof for the columnar delta design: committed DML
        // rows land in the delta store (insert feed), and a successful
        // VACUUM absorbs them (publish feed). Reads stay exact throughout.
        let storage_path =
            std::env::temp_dir().join(format!("plomid-delta-e2e-{}", std::process::id()));
        let wal_path = storage_path.with_extension("wal");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")?;
            executor.execute("INSERT INTO t VALUES (1, 10), (2, 20);")?;
            let root = executor.engine.root().to_path_buf();
            let table_id = executor.catalog.get_table("public.t")?.table_id;
            let key = plomid_columnar::delta::delta_key(&root, ObjectId::new(table_id.get()));
            let lag_key = plomid_executor::columnar_freshness::freshness_key(
                &root,
                &executor.current_database,
                "public.t",
            );
            // Insert feed: both staged rows confirmed with real sequences.
            assert_eq!(
                plomid_columnar::delta::global_delta_store().row_count(&key),
                2
            );
            // Freshness SLA: the lag counter moved on the write (one
            // statement-level unit, matching the maintenance accounting)...
            assert_eq!(
                plomid_executor::columnar_freshness::delta_lag_writes(&lag_key),
                1
            );
            // ...and a successful VACUUM absorbs deltas and resets the lag.
            executor.execute("VACUUM t;")?;
            assert_eq!(
                plomid_columnar::delta::global_delta_store().row_count(&key),
                0
            );
            assert_eq!(
                plomid_executor::columnar_freshness::delta_lag_writes(&lag_key),
                0
            );
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM t;")? else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Corruption,
                    "expected rows",
                )));
            };
            assert_eq!(rows.len(), 2);
            plomid_executor::columnar_freshness::forget_table(&lag_key);
            plomid_columnar::delta::note_deltas_published(&key);
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_writes_confirm_on_commit_and_absorb_on_vacuum failed: {result:?}"
        );
    }

    /// Scratch engine roots unique to one HTAP-gate test (the freshness map
    /// and the delta store are process-wide, so parallel tests must never
    /// share a storage root or a table name).
    fn gate_scratch(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let storage_path =
            std::env::temp_dir().join(format!("plomid-htap-gate-{name}-{}", std::process::id()));
        let wal_path = storage_path.with_extension("wal");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        (storage_path, wal_path)
    }

    /// Runs `sql` and unwraps a `Rows` result.
    fn gate_rows(executor: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
        match executor.execute(sql).expect("query executes") {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected rows for {sql}, got {other:?}"),
        }
    }

    /// Authoritative row-store image of `table`, decoded and ordered by its
    /// leading integer column: the row-path truth a delta-admitted columnar
    /// read must reproduce row-for-row.
    fn row_store_truth(
        executor: &mut Executor<PlomidStorageEngine>,
        table: &str,
    ) -> Vec<Vec<Value>> {
        let start = format!("{table}:").into_bytes();
        let end = format!("{table}:\u{10FFFF}").into_bytes();
        let mut rows: Vec<Vec<Value>> = executor
            .engine
            .scan(Some(&start), Some(&end))
            .expect("row-store scan executes")
            .into_iter()
            .map(|(_, bytes)| decode_row(&bytes).expect("row decodes"))
            .collect();
        rows.sort_by_key(|row| match row.first() {
            Some(Value::Int2(n)) => *n as i64,
            Some(Value::Int4(n)) => *n as i64,
            Some(Value::Int8(n)) => *n,
            other => panic!("unexpected id value {other:?}"),
        });
        rows
    }

    /// `(fresh, delta_covered)` gate state for `table` (qualified name).
    fn gate_state(executor: &Executor<PlomidStorageEngine>, table: &str) -> (bool, bool) {
        let schema = executor.catalog.get_table(table).expect("table exists");
        let table_id = schema.table_id.get();
        let root = executor.engine.root();
        let fresh = plomid_executor::columnar_freshness::is_generation_fresh(
            &executor.columnar_store,
            root,
            &executor.current_database,
            table,
            table_id,
        );
        let covered = plomid_executor::columnar_freshness::is_generation_delta_covered(
            &executor.columnar_store,
            root,
            &executor.current_database,
            table,
            table_id,
        );
        (fresh, covered)
    }

    /// Confirmed delta row count for `table` (qualified name).
    fn gate_deltas(executor: &Executor<PlomidStorageEngine>, table: &str) -> usize {
        let schema = executor.catalog.get_table(table).expect("table exists");
        let key = plomid_columnar::delta::delta_key(
            executor.engine.root(),
            ObjectId::new(schema.table_id.get()),
        );
        plomid_columnar::delta::global_delta_store().row_count(&key)
    }

    /// Drops gate state for `table` so later tests start fail-closed.
    fn gate_cleanup(executor: &Executor<PlomidStorageEngine>, table: &str) {
        let root = executor.engine.root().to_path_buf();
        let lag_key = plomid_executor::columnar_freshness::freshness_key(
            &root,
            &executor.current_database,
            table,
        );
        plomid_executor::columnar_freshness::forget_table(&lag_key);
        if let Ok(schema) = executor.catalog.get_table(table) {
            plomid_columnar::delta::note_deltas_published(&plomid_columnar::delta::delta_key(
                &root,
                ObjectId::new(schema.table_id.get()),
            ));
        }
    }

    #[test]
    fn delta_gate_admits_stale_covered_table_with_exact_rows() {
        // Write-heavy interleaving: after publication, inserts, updates, and
        // deletes land as confirmed deltas; the relaxed gate admits the
        // stale table to the merging scalar scan, which must reproduce the
        // authoritative row-store image row-for-row.
        let (storage_path, wal_path) = gate_scratch("interleave");
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE htap (id INTEGER PRIMARY KEY, v INTEGER);")?;
            executor.execute("INSERT INTO htap VALUES (1, 10), (2, 20), (3, 30);")?;
            executor.execute("VACUUM htap;")?;
            assert_eq!(gate_state(&executor, "public.htap"), (true, true));
            executor.execute("INSERT INTO htap VALUES (4, 40);")?;
            executor.execute("UPDATE htap SET v = 25 WHERE id = 2;")?;
            executor.execute("DELETE FROM htap WHERE id = 1;")?;
            // Stale generation, but every post-publication write staged: the
            // merge path is engaged.
            assert_eq!(gate_state(&executor, "public.htap"), (false, true));
            assert!(gate_deltas(&executor, "public.htap") > 0);
            // Full scan, filtered scan, and scalar aggregation all reproduce
            // row-store truth exactly.
            assert_eq!(
                gate_rows(&mut executor, "SELECT * FROM htap ORDER BY id;"),
                row_store_truth(&mut executor, "public.htap"),
            );
            assert_eq!(
                gate_rows(
                    &mut executor,
                    "SELECT v FROM htap WHERE id >= 2 ORDER BY id;"
                ),
                vec![
                    vec![Value::Int4(25)],
                    vec![Value::Int4(30)],
                    vec![Value::Int4(40)]
                ],
            );
            assert_eq!(
                gate_rows(
                    &mut executor,
                    "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM htap;"
                ),
                vec![vec![
                    Value::Int8(3),
                    Value::Int8(95),
                    Value::Int4(25),
                    Value::Int4(40)
                ]],
            );
            gate_cleanup(&executor, "public.htap");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_gate_admits_stale_covered_table_with_exact_rows failed: {result:?}"
        );
    }

    #[test]
    fn delta_gate_rollback_discards_staged_deltas() {
        // A rolled-back explicit transaction must leave no confirmed delta:
        // its staged rows describe writes that never committed, and
        // confirming them would resurrect ghost rows in later merges.
        let (storage_path, wal_path) = gate_scratch("rollback");
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE rb (id INTEGER PRIMARY KEY, v INTEGER);")?;
            executor.execute("INSERT INTO rb VALUES (1, 10);")?;
            executor.execute("VACUUM rb;")?;
            assert_eq!(gate_state(&executor, "public.rb"), (true, true));
            executor.execute("BEGIN; INSERT INTO rb VALUES (2, 20); ROLLBACK;")?;
            // No ghost delta, no freshness disturbance, exact rows.
            assert_eq!(gate_deltas(&executor, "public.rb"), 0);
            assert_eq!(gate_state(&executor, "public.rb"), (true, true));
            assert_eq!(
                gate_rows(&mut executor, "SELECT * FROM rb ORDER BY id;"),
                row_store_truth(&mut executor, "public.rb"),
            );
            gate_cleanup(&executor, "public.rb");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_gate_rollback_discards_staged_deltas failed: {result:?}"
        );
    }

    #[test]
    fn delta_gate_truncate_clears_coverage_and_reads_empty() {
        // TRUNCATE bulk-clears confirmed deltas, which the per-row merge
        // cannot reconstruct: coverage ends at the truncate mark and the
        // table reads from the row path until the next publication.
        let (storage_path, wal_path) = gate_scratch("truncate");
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE tr (id INTEGER PRIMARY KEY, v INTEGER);")?;
            executor.execute("INSERT INTO tr VALUES (1, 10), (2, 20);")?;
            executor.execute("VACUUM tr;")?;
            assert_eq!(gate_state(&executor, "public.tr"), (true, true));
            executor.execute("TRUNCATE tr;")?;
            assert_eq!(gate_deltas(&executor, "public.tr"), 0);
            assert_eq!(gate_state(&executor, "public.tr"), (false, false));
            assert!(gate_rows(&mut executor, "SELECT * FROM tr;").is_empty());
            // A later publication re-establishes both freshness and coverage.
            executor.execute("INSERT INTO tr VALUES (7, 70);")?;
            executor.execute("VACUUM tr;")?;
            assert_eq!(gate_state(&executor, "public.tr"), (true, true));
            assert_eq!(
                gate_rows(&mut executor, "SELECT * FROM tr;"),
                vec![vec![Value::Int4(7), Value::Int4(70)]],
            );
            gate_cleanup(&executor, "public.tr");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_gate_truncate_clears_coverage_and_reads_empty failed: {result:?}"
        );
    }

    #[test]
    fn delta_gate_publish_returns_stale_covered_table_to_fresh() {
        // Stale-but-covered reads stay exact, and the next successful VACUUM
        // absorbs the deltas and restores the fresh gate (merging the empty
        // set from then on).
        let (storage_path, wal_path) = gate_scratch("republish");
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE pf (id INTEGER PRIMARY KEY, v INTEGER);")?;
            executor.execute("INSERT INTO pf VALUES (1, 10);")?;
            executor.execute("VACUUM pf;")?;
            executor.execute("INSERT INTO pf VALUES (2, 20);")?;
            assert_eq!(gate_state(&executor, "public.pf"), (false, true));
            assert_eq!(
                gate_rows(&mut executor, "SELECT * FROM pf ORDER BY id;"),
                row_store_truth(&mut executor, "public.pf"),
            );
            executor.execute("VACUUM pf;")?;
            assert_eq!(gate_state(&executor, "public.pf"), (true, true));
            assert_eq!(gate_deltas(&executor, "public.pf"), 0);
            assert_eq!(
                gate_rows(&mut executor, "SELECT * FROM pf ORDER BY id;"),
                row_store_truth(&mut executor, "public.pf"),
            );
            gate_cleanup(&executor, "public.pf");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_gate_publish_returns_stale_covered_table_to_fresh failed: {result:?}"
        );
    }

    #[test]
    fn delta_gate_json_modality_unchanged_over_covered_table() {
        // Document/JSON modality safety: JSON evaluation runs over row
        // values downstream of storage-path selection, so delta-admitted
        // columnar row production plus JSON operators, and JSON_TABLE over
        // the covered table, must both match row-path truth exactly.
        let (storage_path, wal_path) = gate_scratch("json");
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, doc JSONB);")?;
            executor.execute(
                "INSERT INTO docs VALUES (1, '{\"name\":\"amy\"}'), (2, '{\"name\":\"bo\"}');",
            )?;
            executor.execute("VACUUM docs;")?;
            executor.execute("INSERT INTO docs VALUES (3, '{\"name\":\"cy\"}');")?;
            executor.execute("UPDATE docs SET doc = '{\"name\":\"bo2\"}' WHERE id = 2;")?;
            // The table is stale but delta-covered: JSON projection below
            // reads through the merging scan.
            assert_eq!(gate_state(&executor, "public.docs"), (false, true));
            assert_eq!(
                gate_rows(
                    &mut executor,
                    "SELECT id, doc ->> 'name' FROM docs ORDER BY id;"
                ),
                vec![
                    vec![Value::Int4(1), Value::Text("amy".to_string())],
                    vec![Value::Int4(2), Value::Text("bo2".to_string())],
                    vec![Value::Int4(3), Value::Text("cy".to_string())]
                ],
            );
            // JSON_TABLE keeps its existing table-function path over the
            // covered table's rows (lateral over row-store state).
            assert_eq!(
                gate_rows(
                    &mut executor,
                    "SELECT d.id, jt.v FROM docs AS d, JSON_TABLE('[10,20]', '$[*]' COLUMNS (v INTEGER PATH '$')) AS jt ORDER BY d.id, jt.v;"
                ),
                vec![
                    vec![Value::Int4(1), Value::Int4(10)],
                    vec![Value::Int4(1), Value::Int4(20)],
                    vec![Value::Int4(2), Value::Int4(10)],
                    vec![Value::Int4(2), Value::Int4(20)],
                    vec![Value::Int4(3), Value::Int4(10)],
                    vec![Value::Int4(3), Value::Int4(20)]
                ],
            );
            gate_cleanup(&executor, "public.docs");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "delta_gate_json_modality_unchanged_over_covered_table failed: {result:?}"
        );
    }

    #[test]
    fn secondary_index_builds_persists_and_filters_reads() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-index-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-index-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE users (id INTEGER, name TEXT);")?;
            executor.execute("INSERT INTO users VALUES (1, 'Alice');")?;
            executor.execute("INSERT INTO users VALUES (2, 'Bob');")?;
            executor.execute("CREATE INDEX users_name_idx ON users(name);")?;
            executor.execute("CREATE UNIQUE INDEX users_id_idx ON users(id);")?;
            assert!(executor
                .execute("INSERT INTO users VALUES (1, 'Duplicate');")
                .is_err());
            let QueryResult::Rows { rows, .. } =
                executor.execute("SELECT * FROM users WHERE name = 'Bob';")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Corruption,
                    "expected rows",
                )));
            };
            assert_eq!(
                rows,
                vec![vec![Value::Int4(2), Value::Text("Bob".to_string())]]
            );
            executor.execute("UPDATE users SET name = 'Robert' WHERE id = 2;")?;
            let QueryResult::Rows { rows, .. } =
                executor.execute("SELECT * FROM users WHERE name = 'Robert';")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Corruption,
                    "expected updated rows",
                )));
            };
            assert_eq!(rows.len(), 1);
            executor.execute("DELETE FROM users WHERE id = 1;")?;
            drop(executor);
            let engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            let QueryResult::Rows { rows, .. } =
                executor.execute("SELECT * FROM users WHERE name = 'Robert';")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Corruption,
                    "expected persisted rows",
                )));
            };
            assert_eq!(rows.len(), 1);
            executor.execute("DROP INDEX users_name_idx;")?;
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn schema_qualified_tables_use_isolated_key_namespaces() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-schema-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-schema-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE SCHEMA sales;")?;
            executor.execute("CREATE TABLE sales.orders (id INTEGER, customer TEXT);")?;
            executor.execute("CREATE TABLE orders (id INTEGER, customer TEXT);")?;
            executor.execute("INSERT INTO sales.orders VALUES (1, 'sales');")?;
            executor.execute("INSERT INTO orders VALUES (1, 'public');")?;
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM sales.orders;")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected schema-qualified rows",
                )));
            };
            assert_eq!(rows[0][1], Value::Text("sales".to_string()));
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM orders;")? else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected public rows",
                )));
            };
            assert_eq!(rows[0][1], Value::Text("public".to_string()));
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(result.is_ok(), "schema isolation test failed: {result:?}");
    }

    #[test]
    fn alter_table_columns_rewrite_persisted_rows() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-alter-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-alter-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE users (id INTEGER, name TEXT);")?;
            executor.execute("INSERT INTO users VALUES (1, 'Alice');")?;
            executor.execute("ALTER TABLE users ADD COLUMN age INTEGER;")?;
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM users;")? else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected rows",
                )));
            };
            assert_eq!(rows[0].len(), 3);
            assert_eq!(rows[0][2], Value::Null);
            executor.execute("ALTER TABLE users DROP COLUMN age;")?;
            executor.execute("ALTER TABLE users RENAME COLUMN name TO display_name;")?;
            let QueryResult::Rows { columns, rows, .. } =
                executor.execute("SELECT * FROM users;")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected rows",
                )));
            };
            assert_eq!(columns, vec!["id", "display_name"]);
            assert_eq!(rows[0][1], Value::Text("Alice".into()));
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(result.is_ok(), "alter test failed: {result:?}");
    }

    #[test]
    fn constraints_defaults_and_unique_values_are_enforced() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-constraints-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-constraints-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'unknown');")?;
            assert!(executor.catalog.column_rules("users")?[1].not_null);
            executor.execute("INSERT INTO users (id) VALUES (1);")?;
            let duplicate = executor.execute("INSERT INTO users (id) VALUES (1);");
            assert!(
                matches!(duplicate, Err(SqlError::Storage(error)) if error.kind() == ErrorKind::Conflict)
            );
            let null_name = executor.execute("INSERT INTO users VALUES (2, NULL);");
            assert!(
                null_name.is_err(),
                "expected NOT NULL failure: {null_name:?}"
            );
            let QueryResult::Rows { rows, .. } = executor.execute("SELECT * FROM users;")? else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected rows",
                )));
            };
            assert_eq!(rows[0][1], Value::Text("unknown".into()));
            executor.execute("CREATE TABLE ages (id INTEGER, age INTEGER CHECK (age >= 0));")?;
            let invalid = executor.execute("INSERT INTO ages VALUES (1, -1);");
            assert!(invalid.is_err(), "negative CHECK value must be rejected");
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(result.is_ok(), "constraint test failed: {result:?}");
    }

    #[test]
    fn sequence_state_is_transactionally_persistent() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-sequence-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-sequence-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            executor.execute("CREATE SEQUENCE users_id_seq;")?;
            let QueryResult::Rows { rows, .. } =
                executor.execute("SELECT nextval('users_id_seq');")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected nextval row",
                )));
            };
            assert_eq!(rows[0][0], Value::Int8(1));
            drop(executor);
            let engine = PlomidStorageEngine::open(&storage_path, &wal_path, 32)?;
            let mut executor = Executor::new(engine)?;
            let QueryResult::Rows { rows, .. } =
                executor.execute("SELECT nextval('users_id_seq');")?
            else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    "expected nextval row",
                )));
            };
            assert_eq!(rows[0][0], Value::Int8(2));
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(result.is_ok(), "sequence test failed: {result:?}");
    }
}

mod vacuum_claim_tests {

    use plomid_executor::error::SqlResult;
    use plomid_executor::executor::*;
    use plomid_executor::maintenance::{maintenance_claim_key, MaintenanceClaim};

    use plomid_txn::PlomidStorageEngine;
    use plomid_txn::StorageEngine;
    use std::sync::{Arc, Mutex};

    #[test]
    fn explicit_vacuum_waits_for_running_pass_instead_of_racing_it() {
        let storage_path = std::path::PathBuf::from("/tmp/plomid-vacuum-claim-test");
        let wal_path = std::path::PathBuf::from("/tmp/plomid-vacuum-claim-wal-test");
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        let result: SqlResult<()> = (|| {
            let engine = PlomidStorageEngine::create(&storage_path, &wal_path, 32)?;
            let shared = Arc::new(Mutex::new(engine));
            let mut setup = Executor::new_shared(Arc::clone(&shared))?;
            setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER);")?;
            setup.execute("INSERT INTO t VALUES (1, 10), (2, 20);")?;
            let root = shared.lock().expect("engine").root().to_path_buf();
            let key = maintenance_claim_key(&root, "plomid", "public.t");
            // Hold the single-flight claim as a running automatic pass would.
            let claim = MaintenanceClaim::try_acquire(key).expect("claim free");
            let vacuumed = Arc::new(Mutex::new(None));
            let vacuumed_thread = Arc::clone(&vacuumed);
            let shared_thread = Arc::clone(&shared);
            let started = std::time::Instant::now();
            let handle = std::thread::spawn(move || {
                let mut session = Executor::new_shared(shared_thread).expect("session");
                let outcome = session.execute("VACUUM t;");
                *vacuumed_thread.lock().expect("flag") = Some((outcome.is_ok(), started.elapsed()));
            });
            // Let the VACUUM arrive while the claim is held, then release:
            // it must have waited (not skipped, not conflicted) and then
            // succeeded on the freed slot.
            std::thread::sleep(std::time::Duration::from_millis(500));
            drop(claim);
            handle.join().expect("vacuum thread");
            let (ok, waited) = vacuumed.lock().expect("flag").expect("vacuum ran");
            assert!(ok, "VACUUM after claim release must succeed");
            assert!(
                waited >= std::time::Duration::from_millis(400),
                "VACUUM must wait for the running pass, waited={waited:?}"
            );
            Ok(())
        })();
        let _ = std::fs::remove_dir_all(&storage_path);
        let _ = std::fs::remove_dir_all(&wal_path);
        assert!(
            result.is_ok(),
            "explicit_vacuum_waits_for_running_pass failed: {result:?}"
        );
    }
}
