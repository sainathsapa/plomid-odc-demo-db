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
//! Session-held explicit transactions (Beta 2 Phase 4).
//!
//! The wire protocol delivers one message per statement, but a transaction
//! must stage effects — not text. [`SessionTxn`] holds a REAL open
//! [`ConcurrentTransaction`][plomid_txn::ConcurrentTransaction] across
//! awaits: every DML statement executes into its write overlay immediately
//! (constraints, defaults, sequences, RETURNING resolved at statement
//! time), every SELECT reads snapshot + overlay (read-your-writes), and
//! COMMIT only validates + publishes (WAL + MVCC) without re-executing
//! anything.
//!
//! The transaction handle is fully owned (Arc clones + owned overlay —
//! see `begin_owned`), so no self-referential borrowing is needed: the
//! executor owns it like any other session state.
//!
//! Isolation contract (documented narrowing, see the txn runbook):
//!
//! * READ COMMITTED: each statement refreshes to the latest committed
//!   snapshot (plus own writes). Concurrent writers serialize on row/table
//!   lanes; the waiter re-reads fresh state before writing (lost-update
//!   protection, PostgreSQL READ COMMITTED behavior). Unique conflicts
//!   error at statement time.
//! * REPEATABLE READ: the BEGIN snapshot is pinned for reads; writers still
//!   re-read at lane acquisition (no serialization failures — weaker than
//!   PostgreSQL REPEATABLE READ, which errors; documented).
//! * SERIALIZABLE: rejected at BEGIN (pre-existing).
//!
//! Error narrowing: ANY statement error aborts the whole session
//! transaction immediately (overlay dropped, catalog restored); only
//! ROLLBACK is accepted afterwards. Savepoints support user-directed
//! partial rollback of SUCCESSFUL statements only — there is no
//! error-recovery via ROLLBACK TO SAVEPOINT in Beta 2 (documented).

use crate::error::{SqlError, SqlResult};
use crate::executor::Executor;
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::{Catalog, InMemoryCatalog, QueryResult, Statement, Value};
use plomid_txn::ConcurrentTransaction;
use std::collections::BTreeMap;

/// Transaction isolation for an explicitly opened session transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnIsolation {
    ReadCommitted,
    RepeatableRead,
}

/// A named savepoint: overlay + catalog + creation-tracking images to
/// restore on ROLLBACK TO.
#[derive(Clone)]
struct TxnSavepoint {
    name: String,
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    catalog: InMemoryCatalog,
    created_len: usize,
    cleanups_len: usize,
}

/// An open explicit transaction held across wire messages.
pub struct SessionTxn {
    txn: ConcurrentTransaction,
    isolation: TxnIsolation,
    /// Catalog image at BEGIN (DDL mutates the live session catalog;
    /// abort restores this).
    catalog_snapshot: InMemoryCatalog,
    savepoints: Vec<TxnSavepoint>,
    /// Tables created inside the transaction (directory cleanup on abort).
    pub(crate) created_tables: Vec<String>,
    /// Deferred drop cleanups (columnar/stats/directory work) executed at
    /// successful commit, after the data publish.
    pub(crate) pending_cleanups: Vec<PendingCleanup>,
}

impl SessionTxn {
    fn new(txn: ConcurrentTransaction, isolation: TxnIsolation, catalog: &InMemoryCatalog) -> Self {
        Self {
            txn,
            isolation,
            catalog_snapshot: catalog.clone(),
            savepoints: Vec::new(),
            created_tables: Vec::new(),
            pending_cleanups: Vec::new(),
        }
    }
}

/// Deferred drop cleanup executed at commit (never on abort — there is
/// nothing durable to clean when effects never published).
#[derive(Debug, Clone)]
pub(crate) enum PendingCleanup {
    DropTable {
        name: String,
        table_id: plomid_core::TableId,
        database_id: plomid_core::DatabaseId,
        schema_id: plomid_core::SchemaId,
    },
    DropSchema {
        database_id: plomid_core::DatabaseId,
        schema_id: plomid_core::SchemaId,
    },
}

use plomid_storage::DatabaseLayout;
use plomid_txn::{ConcurrentPlomidStorageEngine, StorageEngine, StorageEngineTransaction};

impl Executor<ConcurrentPlomidStorageEngine> {
    /// True while an explicit session transaction is open.
    pub fn session_txn_active(&self) -> bool {
        self.session_txn.is_some()
    }

    /// Executes generated single-statement SQL (COPY paths) in the open
    /// session transaction when one exists, else autocommit. Authorization
    /// and error-abort rules match `execute_statement_in_session_txn`.
    pub fn execute_session_aware(&mut self, sql: &str) -> SqlResult<QueryResult> {
        if self.session_txn_active() {
            self.execute_one_in_session_txn(sql)
        } else {
            self.execute(sql)
        }
    }

    /// Parses exactly one statement and executes it inside the open session
    /// transaction (wire path for multi-message explicit transactions).
    /// Multi-statement input is rejected (the caller splits first).
    pub fn execute_one_in_session_txn(&mut self, sql: &str) -> SqlResult<QueryResult> {
        use plomid_sql::{Lexer, Parser};
        let mut tokens = Lexer::new(sql).lex()?;
        if !tokens
            .iter()
            .any(|token| matches!(token.kind, plomid_sql::TokenKind::Eof))
        {
            tokens.push(plomid_sql::Token::new(plomid_sql::TokenKind::Eof, "", 0, 0));
        }
        let parser = Parser::new(tokens, &mut self.catalog);
        let stmts = parser.parse_statements().map_err(SqlError::from)?;
        let mut stmts = stmts.into_iter();
        let Some(stmt) = stmts.next() else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                "empty query",
            )));
        };
        if stmts.next().is_some() {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                "expected a single statement",
            )));
        }
        self.execute_statement_in_session_txn(stmt)
    }

    /// Executes one statement inside the open session transaction (true
    /// staging: effects land in the write overlay at statement time,
    /// reads merge overlay over a fresh (RC) or pinned (RR) snapshot).
    /// On error the session transaction aborts immediately and the caller
    /// must ROLLBACK-reset (only ROLLBACK accepted afterwards).
    pub fn execute_statement_in_session_txn(&mut self, stmt: Statement) -> SqlResult<QueryResult> {
        if self.session_txn.is_none() {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "no active transaction",
            )));
        }
        // Authorization runs at statement time against the live session
        // catalog ( strictly earlier and more precise than COMMIT replay).
        let _auth = crate::authz::authorize_with_snapshot(
            &self.catalog,
            &self.current_user,
            &self.current_database,
            &stmt,
        )?;
        // Freshness + truncation marks mirror the autocommit pre-steps so
        // delta coverage never observes a generation the statement
        // invalidates.
        let write_targets = self.committed_write_targets(std::slice::from_ref(&stmt));
        self.mark_write_targets_dirty(&write_targets);
        if matches!(stmt, Statement::Truncate { .. }) {
            self.clear_truncate_coverage(&write_targets);
        }
        // Read-committed statements observe the latest committed state;
        // repeatable-read statements keep the BEGIN snapshot. The overlay
        // (own writes) is untouched either way.
        let refresh = self
            .session_txn
            .as_ref()
            .is_some_and(|session_txn| session_txn.isolation == TxnIsolation::ReadCommitted);
        if refresh {
            if let Some(session_txn) = self.session_txn.as_mut() {
                session_txn
                    .txn
                    .refresh_snapshot()
                    .map_err(SqlError::Storage)?;
            }
        }
        // SHOW/DESCRIBE read session state (never stage): SHOW answers
        // from parameters, DESCRIBE from the session catalog (which sees
        // in-transaction DDL). Probes underneath run LIMIT 0 over committed
        // state — schema only, no row content, so overlay visibility is moot.
        if let Statement::Show { name } = &stmt {
            return self.execute_show(name);
        }
        if let Statement::Describe { name } = &stmt {
            return self.execute_describe(name);
        }
        let outcome = {
            let session_txn = self.session_txn.as_mut().expect("checked above");
            dispatch_in_session_txn(
                &mut self.engine,
                session_txn,
                &mut self.catalog,
                &self.layout,
                &self.current_database,
                &self.current_user,
                &self.database_names,
                &mut self.statistics,
                stmt,
            )
        };
        match outcome {
            Ok(result) => Ok(result),
            Err(error) => {
                self.abort_session_txn_on_error();
                Err(error)
            }
        }
    }

    /// Opens a session transaction (wire BEGIN).
    ///
    /// Captures the catalog image for abort-revert. The data snapshot is
    /// the transaction's BEGIN snapshot; READ COMMITTED statements refresh
    /// it per statement, REPEATABLE READ pins it.
    pub fn begin_session_txn(&mut self, isolation: TxnIsolation) -> SqlResult<()> {
        if self.session_txn.is_some() {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "already in an explicit transaction",
            )));
        }
        let txn = self.engine.begin_owned().map_err(SqlError::Storage)?;
        self.session_txn = Some(SessionTxn::new(txn, isolation, &self.catalog));
        Ok(())
    }

    /// Aborts the session transaction (wire ROLLBACK or statement-error
    /// recovery): drops staged effects, restores the BEGIN catalog image.
    /// Idempotent (recovery ROLLBACK after a failed COMMIT is a no-op Ok).
    pub fn abort_session_txn(&mut self) -> SqlResult<()> {
        let Some(session_txn) = self.session_txn.take() else {
            return Ok(());
        };
        // Directories for created tables go first (catalog still knows
        // them), then overlay abort + catalog restore.
        self.remove_created_table_dirs(&session_txn.created_tables);
        let mut txn = session_txn.txn;
        // Best-effort abort of the storage transaction; the session state
        // is discarded regardless so no half-open txn can strand lanes.
        let _ = txn.abort();
        self.catalog = session_txn.catalog_snapshot;
        Ok(())
    }

    /// Commits the session transaction (wire COMMIT): stages the catalog
    /// delta into the session transaction and publishes data + catalog
    /// atomically (WAL + MVCC, conflict-checked), then runs post-commit
    /// maintenance accounting. No re-execution. A crash leaves either
    /// everything or nothing — never unreachable rows.
    pub fn commit_session_txn(&mut self) -> SqlResult<QueryResult> {
        let Some(mut session_txn) = self.session_txn.take() else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "no active transaction",
            )));
        };
        // F-004: a table create staged in this transaction claims a name that
        // a concurrent creator may have claimed since this image was loaded
        // (point-in-time images cannot see each other). Re-probe durable
        // state under the create gate and hold the gate across the commit
        // below: a concurrent creator either landed before the probe (this
        // COMMIT aborts with "already exists") or blocks on the gate until
        // this commit publishes (then its own probe observes our table).
        // Data-only transactions skip the gate entirely.
        let create_gate =
            (!session_txn.created_tables.is_empty()).then(plomid_sql::catalog::CreateGate::acquire);
        if let Some(gate) = &create_gate {
            for name in &session_txn.created_tables {
                // A create is a concurrent claim only when the BEGIN image
                // did not already know the name: a DROP + CREATE inside one
                // transaction intentionally replaces a table its own
                // snapshot could see, and that replacement must not read as
                // a conflict.
                if session_txn.catalog_snapshot.has_table(name) {
                    continue;
                }
                if gate
                    .table_exists(&mut self.engine, name)
                    .map_err(SqlError::Storage)?
                {
                    // The loser's local effects never existed durably:
                    // discard them exactly like a rollback (materialized
                    // directory first — the catalog still knows it).
                    self.remove_created_table_dirs(&session_txn.created_tables);
                    let _ = session_txn.txn.abort();
                    self.catalog = session_txn.catalog_snapshot;
                    self.session_txn = None;
                    return Err(SqlError::Storage(PlomidError::with_detail(
                        ErrorKind::AlreadyExists,
                        format!("table \"{name}\" already exists"),
                        format!("table={name}"),
                    )));
                }
            }
        }
        // Row counts per table for maintenance debt, derived from the
        // overlay before commit drains it.
        let overlay = session_txn.txn.overlay_checkpoint();
        let mut counts: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for (key, staged) in overlay.iter() {
            if staged.is_none() {
                continue;
            }
            if let Some(table) = table_of_key(key) {
                *counts.entry(table).or_insert(0) += 1;
            }
        }
        // Catalog delta rides the session transaction itself: the delta is
        // staged into the already-open txn (whose write lanes are held),
        // then published by the single commit below. A fresh engine
        // transaction here would block acquiring the lane gate this session
        // already holds — self-deadlock. One commit also makes catalog and
        // data atomic: a crash leaves either everything or nothing, never
        // unreachable rows.
        let (puts, deletes) = plomid_sql::staged_catalog_delta(&mut self.catalog);
        {
            use plomid_txn::StorageEngineTransaction as _;
            let stage = (|| -> SqlResult<()> {
                for (key, value) in &puts {
                    session_txn.txn.put(key, value).map_err(SqlError::Storage)?;
                }
                for key in &deletes {
                    session_txn.txn.delete(key).map_err(SqlError::Storage)?;
                }
                Ok(())
            })();
            if let Err(error) = stage {
                let _ = session_txn.txn.abort();
                self.catalog = session_txn.catalog_snapshot;
                self.session_txn = None;
                return Err(error);
            }
        }
        if let Err(error) = session_txn.txn.commit().map_err(SqlError::Storage) {
            // Commit failure publishes nothing (catalog delta dies with the
            // overlay); the in-memory catalog keeps its dirty marks so a
            // later save retries them. The session transaction is over
            // either way — the caller must ROLLBACK-reset like PostgreSQL
            // after a failed COMMIT.
            self.session_txn = None;
            return Err(error);
        }
        // The name claims are published; release the F-004 gate before the
        // post-commit bookkeeping so other creators only wait on the commit
        // itself.
        drop(create_gate);
        self.catalog.clear_dirty();
        let targets: Vec<(String, u64)> = counts.into_iter().collect();
        let tables: Vec<String> = targets.iter().map(|(t, _)| t.clone()).collect();
        self.after_commit(targets);
        self.refresh_art_indexes_for(&tables);
        // Deferred drop cleanups (columnar/stats/directory work for tables
        // dropped inside the transaction): the data publish is durable, so
        // derived state can now be reclaimed. Best-effort per item (a
        // failed cleanup only leaks, like the autocommit path).
        let cleanups = std::mem::take(&mut session_txn.pending_cleanups);
        for cleanup in cleanups {
            self.run_pending_cleanup(&cleanup);
        }
        self.session_txn = None;
        Ok(QueryResult::Committed)
    }

    /// Executes one deferred drop cleanup (see `PendingCleanup`).
    /// Only filesystem/columnar/stats effects defer here: catalog entries
    /// and storage keys were already handled transactionally at statement
    /// time. Best-effort (a failed cleanup only leaks, like autocommit).
    fn run_pending_cleanup(&mut self, cleanup: &PendingCleanup) {
        match cleanup {
            PendingCleanup::DropTable {
                name,
                table_id,
                database_id,
                schema_id,
            } => {
                if crate::ddl::remove_columnar_object(&mut self.engine, *table_id) {
                    crate::ddl::remove_relation_directory_by_ids(
                        &self.layout,
                        *database_id,
                        *schema_id,
                        *table_id,
                    );
                }
                crate::columnar_freshness::forget_table(&crate::columnar_freshness::freshness_key(
                    self.engine.root(),
                    &self.current_database.clone(),
                    name,
                ));
                plomid_columnar::delta::note_deltas_published(&plomid_columnar::delta::delta_key(
                    self.engine.root(),
                    plomid_core::ObjectId::new(table_id.get()),
                ));
                if let Ok(mut stats_txn) = self.engine.begin() {
                    use plomid_txn::StorageEngineTransaction as _;
                    let _ = stats_txn.delete(&crate::statistics::key(name));
                    let _ = stats_txn.commit();
                }
            }
            PendingCleanup::DropSchema {
                database_id,
                schema_id,
            } => {
                crate::ddl::remove_schema_directory_by_ids(&self.layout, *database_id, *schema_id);
            }
        }
    }

    /// Establishes a savepoint (wire SAVEPOINT): overlay + catalog images.
    /// Duplicate names replace the earlier marker (PostgreSQL semantics).
    pub fn savepoint_session_txn(&mut self, name: &str) -> SqlResult<()> {
        let Some(session_txn) = self.session_txn.as_mut() else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "no active transaction",
            )));
        };
        session_txn
            .savepoints
            .retain(|marker| !marker.name.eq_ignore_ascii_case(name));
        session_txn.savepoints.push(TxnSavepoint {
            name: name.to_string(),
            overlay: session_txn.txn.overlay_checkpoint(),
            catalog: self.catalog.clone(),
            created_len: session_txn.created_tables.len(),
            cleanups_len: session_txn.pending_cleanups.len(),
        });
        Ok(())
    }

    /// Releases a savepoint (wire RELEASE): forgets the marker, keeps effects.
    pub fn release_session_savepoint(&mut self, name: &str) -> SqlResult<()> {
        let Some(session_txn) = self.session_txn.as_mut() else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "no active transaction",
            )));
        };
        let before = session_txn.savepoints.len();
        session_txn
            .savepoints
            .retain(|marker| !marker.name.eq_ignore_ascii_case(name));
        if session_txn.savepoints.len() == before {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!("no such savepoint: \"{name}\""),
            )));
        }
        Ok(())
    }

    /// Rolls back to a savepoint (wire ROLLBACK TO): restores overlay +
    /// catalog images, drops the named savepoint and every savepoint
    /// established after it (PostgreSQL semantics). Lanes stay held
    /// (conservative).
    pub fn rollback_to_session_savepoint(&mut self, name: &str) -> SqlResult<()> {
        // Clone the marker out first so later borrows stay disjoint.
        let marker = self.session_txn.as_ref().and_then(|session_txn| {
            session_txn
                .savepoints
                .iter()
                .rposition(|marker| marker.name.eq_ignore_ascii_case(name))
                .and_then(|index| session_txn.savepoints.get(index).cloned())
        });
        let Some(marker) = marker else {
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!("no such savepoint: \"{name}\""),
            )));
        };
        let dropped: Vec<String>;
        {
            let Some(session_txn) = self.session_txn.as_mut() else {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Conflict,
                    "no active transaction",
                )));
            };
            session_txn
                .txn
                .overlay_restore(marker.overlay)
                .map_err(SqlError::Storage)?;
            self.catalog = marker.catalog;
            dropped = session_txn.created_tables.split_off(marker.created_len);
            session_txn.pending_cleanups.truncate(marker.cleanups_len);
            if let Some(index) = session_txn
                .savepoints
                .iter()
                .rposition(|marker| marker.name.eq_ignore_ascii_case(name))
            {
                session_txn.savepoints.truncate(index);
            }
        }
        // Tables created after the savepoint vanish with the catalog
        // restore: remove their eagerly-materialized directories.
        self.remove_created_table_dirs(&dropped);
        Ok(())
    }

    /// Aborts after a statement error inside the session transaction:
    /// effects and catalog revert immediately; the caller marks the session
    /// failed (only ROLLBACK accepted afterwards). See the module docs for
    /// the narrowing rationale. Public for the wire layer's savepoint-error
    /// path (same abort-on-error rule as statement errors).
    pub fn abort_session_txn_on_error(&mut self) {
        if let Some(session_txn) = self.session_txn.take() {
            // Directories materialized for created tables are removed while
            // the catalog still knows them; then the snapshot restore drops
            // the entries themselves.
            self.remove_created_table_dirs(&session_txn.created_tables);
            let mut txn = session_txn.txn;
            let _ = txn.abort();
            self.catalog = session_txn.catalog_snapshot;
        }
    }

    /// Best-effort removal of relation directories for tables created by
    /// an aborted transaction (their catalog entries vanish with the
    /// snapshot restore, so the directories would otherwise orphan).
    /// Runs against the live (pre-restore) catalog.
    fn remove_created_table_dirs(&mut self, created: &[String]) {
        for name in created {
            let table_id = self
                .catalog
                .get_table(name)
                .ok()
                .map(|schema| schema.table_id);
            if let Some(table_id) = table_id {
                crate::ddl::remove_relation_directory(
                    &self.catalog,
                    &self.layout,
                    &self.current_database.clone(),
                    name,
                    table_id,
                );
            }
        }
    }
}

/// Table name for a storage row key (`{table}:...`), if shaped like one.
fn table_of_key(key: &[u8]) -> Option<String> {
    let key = std::str::from_utf8(key).ok()?;
    let (table, _) = key.split_once(':')?;
    if table.is_empty() {
        None
    } else {
        Some(table.to_string())
    }
}

/// Statement dispatch inside an open session transaction. Every arm
/// executes effects immediately into the overlay/session catalog (true
/// staging); COMMIT later only publishes. On any error the caller aborts
/// the whole session transaction (see module docs).
#[allow(clippy::too_many_arguments)]
fn dispatch_in_session_txn(
    engine: &mut ConcurrentPlomidStorageEngine,
    session_txn: &mut SessionTxn,
    catalog: &mut InMemoryCatalog,
    layout: &DatabaseLayout,
    database: &str,
    user: &str,
    database_names: &[String],
    statistics: &mut std::collections::BTreeMap<String, crate::statistics::TableStatistics>,
    stmt: Statement,
) -> SqlResult<QueryResult> {
    use plomid_sql::Statement as S;
    match stmt {
        S::Insert {
            table,
            columns,
            source,
            returning,
            on_conflict,
        } => {
            // INSERT ... SELECT sources materialize through the
            // overlay-aware SELECT (read-your-writes, same snapshot).
            let source = match source {
                plomid_sql::InsertSource::Select(query) => {
                    let (_, rows) = materialize_select_source(
                        engine,
                        &mut session_txn.txn,
                        catalog,
                        database,
                        user,
                        database_names,
                        &query,
                    )?;
                    plomid_sql::InsertSource::Values(
                        rows.into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(plomid_sql::InsertValue::Literal)
                                    .collect()
                            })
                            .collect(),
                    )
                }
                source => source,
            };
            crate::dml::execute_insert_rows_txn(
                &mut session_txn.txn,
                catalog,
                table,
                columns,
                source,
                returning,
                on_conflict,
            )
        }
        S::Update {
            table,
            alias,
            from,
            assignments,
            where_expr,
            returning,
        } => {
            let from_source = match from {
                None => None,
                Some(clause) => Some(materialize_from_in_session_txn(
                    engine,
                    &mut session_txn.txn,
                    catalog,
                    database,
                    user,
                    database_names,
                    &clause,
                )?),
            };
            crate::dml::execute_update_txn(
                &mut session_txn.txn,
                catalog,
                table,
                alias,
                assignments,
                from_source,
                where_expr,
                returning,
            )
        }
        S::Delete {
            table,
            alias,
            using,
            where_expr,
            returning,
        } => {
            let using_source = match using {
                None => None,
                Some(clause) => Some(materialize_from_in_session_txn(
                    engine,
                    &mut session_txn.txn,
                    catalog,
                    database,
                    user,
                    database_names,
                    &clause,
                )?),
            };
            crate::dml::execute_delete_txn(
                &mut session_txn.txn,
                catalog,
                table,
                alias,
                using_source,
                where_expr,
                returning,
            )
        }
        S::Select {
            targets,
            distinct,
            distinct_on,
            from,
            where_expr,
            group_by,
            having,
            order_by,
            limit,
            offset,
        } => crate::query::txn_select::select_in_txn(
            &mut session_txn.txn,
            engine,
            catalog,
            &plomid_sql::Statement::Select {
                targets,
                distinct,
                distinct_on,
                from,
                where_expr,
                group_by,
                having,
                order_by,
                limit,
                offset,
            },
            database,
            user,
            database_names,
            None,
        ),
        S::With {
            recursive,
            ctes,
            body,
        } => crate::query::txn_select::select_in_txn(
            &mut session_txn.txn,
            engine,
            catalog,
            &plomid_sql::Statement::With {
                recursive,
                ctes,
                body,
            },
            database,
            user,
            database_names,
            None,
        ),
        S::SetOperation {
            op,
            all,
            left,
            right,
        } => crate::query::txn_select::select_in_txn(
            &mut session_txn.txn,
            engine,
            catalog,
            &plomid_sql::Statement::SetOperation {
                op,
                all,
                left,
                right,
            },
            database,
            user,
            database_names,
            None,
        ),
        S::Values(rows) => crate::query::txn_select::select_in_txn(
            &mut session_txn.txn,
            engine,
            catalog,
            &plomid_sql::Statement::Values(rows),
            database,
            user,
            database_names,
            None,
        ),
        S::Explain {
            statement,
            analyze,
            format,
        } => {
            if analyze {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Unsupported,
                    "EXPLAIN ANALYZE inside explicit transactions is not yet supported",
                )));
            }
            crate::query::explain_statement(
                engine,
                catalog,
                statement.as_ref(),
                false,
                format,
                database,
                user,
            )
        }
        S::Copy { .. } => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Unsupported,
            "COPY inside explicit transactions requires the PostgreSQL wire-level \
             CopyData/CopyDone/CopyFail frame protocol",
        ))),
        S::Truncate { tables } => {
            for table in tables {
                let resolved = catalog
                    .resolve_table_name(&table)
                    .map_err(SqlError::Storage)?;
                let (start, end) = txn_table_range(&resolved);
                let keys: Vec<Vec<u8>> = session_txn
                    .txn
                    .scan(Some(&start), Some(&end))
                    .map_err(|e| {
                        SqlError::Storage(PlomidError::new(
                            ErrorKind::Internal,
                            format!("transactional truncate scan failed: {}", e.message()),
                        ))
                    })?
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect();
                for key in keys {
                    session_txn.txn.delete(&key).map_err(SqlError::Storage)?;
                }
            }
            Ok(QueryResult::Created("TRUNCATE".into()))
        }
        // Transaction control never reaches here (wire routes it to the
        // session ops); nested control is rejected like the group path.
        S::Begin | S::Commit | S::Rollback => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Conflict,
            "nested transaction control inside an explicit transaction",
        ))),
        S::DropTable {
            name,
            if_exists,
            cascade,
        } => {
            match drop_table_in_session_txn(
                session_txn,
                catalog,
                database,
                statistics,
                name,
                if_exists,
                cascade,
            ) {
                Ok(cleanup) => {
                    session_txn.pending_cleanups.push(cleanup);
                    Ok(QueryResult::Created("DROP TABLE".into()))
                }
                Err(error) => {
                    // IF EXISTS on a missing table is a no-op success.
                    if if_exists && is_not_found(&error) {
                        Ok(QueryResult::Created("DROP TABLE".into()))
                    } else {
                        Err(error)
                    }
                }
            }
        }
        S::DropSchema {
            name,
            if_exists,
            cascade,
        } => drop_schema_in_session_txn(
            engine,
            session_txn,
            catalog,
            layout,
            database,
            statistics,
            name,
            if_exists,
            cascade,
        ),
        // Admin-free compatibility statements and REINDEX share the
        // autocommit engine semantics inside a transaction block:
        // `REINDEX` drops derived ANN graphs (safe at any time — nothing
        // transactional is touched) and the rest are pure
        // acknowledgments. `ANALYZE`/`CLUSTER` stay refused below:
        // statistics collection reads committed rows (would silently
        // ignore the overlay) and CLUSTER cannot run in a block.
        S::Reindex => crate::ddl::execute_reindex(catalog, user),
        S::CommentOn { .. } => Ok(QueryResult::Created("COMMENT".into())),
        S::Lock => Ok(QueryResult::Created("LOCK TABLE".into())),
        S::RefreshMaterializedView { .. } => {
            Ok(QueryResult::Created("REFRESH MATERIALIZED VIEW".into()))
        }
        // Maintenance and cross-database control cannot run inside a
        // transaction block (PostgreSQL parity).
        S::Vacuum { .. }
        | S::Analyze { .. }
        | S::Cluster
        | S::Create(plomid_sql::CreateStatement::Database { .. }) => {
            Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Unsupported,
                "this statement cannot be executed inside a transaction block",
            )))
        }
        S::Use { .. } => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Conflict,
            "cannot change database during a transaction",
        ))),
        // DDL with transactional catalog semantics (persist deferred to
        // COMMIT, reverted on abort).
        ddl => ddl_in_session_txn(
            engine,
            session_txn,
            catalog,
            layout,
            database,
            user,
            database_names,
            ddl,
        ),
    }
}

/// Storage key range for one table's rows (same layout as the row path).
fn txn_table_range(resolved: &str) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::with_capacity(resolved.len() + 1);
    start.extend_from_slice(resolved.as_bytes());
    start.push(b':');
    let mut end = Vec::with_capacity(resolved.len() + 5);
    end.extend_from_slice(resolved.as_bytes());
    end.push(b':');
    end.extend_from_slice("￿".as_bytes());
    (start, end)
}

/// Materializes an INSERT ... SELECT source through the overlay-aware
/// SELECT, returning display columns plus rows.
#[allow(clippy::too_many_arguments)]
fn materialize_select_source(
    engine: &mut ConcurrentPlomidStorageEngine,
    txn: &mut ConcurrentTransaction,
    catalog: &InMemoryCatalog,
    database: &str,
    user: &str,
    database_names: &[String],
    query: &Statement,
) -> SqlResult<(Vec<String>, Vec<Vec<Value>>)> {
    match crate::query::txn_select::select_in_txn(
        txn,
        engine,
        catalog,
        query,
        database,
        user,
        database_names,
        None,
    )? {
        QueryResult::Rows { columns, rows, .. } => Ok((columns, rows)),
        _ => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            "INSERT ... SELECT source did not produce rows",
        ))),
    }
}

/// Materializes an UPDATE ... FROM / DELETE ... USING source through the
/// overlay-aware SELECT. Any FROM shape works (plain tables take the same
/// code path as the committed machinery, joins/subqueries recurse).
#[allow(clippy::too_many_arguments)]
fn materialize_from_in_session_txn(
    engine: &mut ConcurrentPlomidStorageEngine,
    txn: &mut ConcurrentTransaction,
    catalog: &InMemoryCatalog,
    database: &str,
    user: &str,
    database_names: &[String],
    clause: &plomid_sql::FromClause,
) -> SqlResult<crate::update_from::UpdateFromSource> {
    let stmt = plomid_sql::Statement::Select {
        targets: vec![plomid_sql::SelectTarget::All],
        distinct: false,
        distinct_on: None,
        from: Some(clause.clone()),
        where_expr: None,
        group_by: None,
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    match crate::query::txn_select::select_in_txn(
        txn,
        engine,
        catalog,
        &stmt,
        database,
        user,
        database_names,
        None,
    )? {
        QueryResult::Rows { columns, rows, .. } => Ok(crate::update_from::UpdateFromSource {
            qualifiers: from_qualifiers(clause),
            columns,
            rows,
        }),
        _ => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            "FROM source did not produce rows",
        ))),
    }
}

/// Qualifiers that may legally prefix a column of a materialized relation:
/// explicit alias, trailing name segment, full name as written.
fn from_qualifiers(from: &plomid_sql::FromClause) -> Vec<String> {
    use plomid_sql::FromClause as F;
    match from {
        F::Table { name, alias } => {
            let mut out = Vec::new();
            if let Some(alias) = alias {
                out.push(alias.clone());
            } else if let Some(trailing) = name.rsplit('.').next() {
                out.push(trailing.to_string());
            }
            out.push(name.clone());
            out
        }
        F::TableFunction { name, alias, .. } => {
            vec![alias.clone().unwrap_or_else(|| name.clone()), name.clone()]
        }
        F::Subquery { alias, .. } => vec![alias.clone()],
        F::Join { left, right, .. } => {
            let mut out = from_qualifiers(left);
            out.extend(from_qualifiers(right));
            out
        }
    }
}

/// DDL inside an open session transaction. Catalog mutations apply to the
/// live session catalog immediately (later statements in the transaction
/// see them); NOTHING persists until COMMIT (which saves once); abort
/// restores the BEGIN image. Storage-row effects stage into the overlay;
/// filesystem/columnar/stats effects defer to commit cleanups.
#[allow(clippy::too_many_arguments)]
fn ddl_in_session_txn(
    engine: &mut ConcurrentPlomidStorageEngine,
    session_txn: &mut SessionTxn,
    catalog: &mut InMemoryCatalog,
    layout: &plomid_storage::DatabaseLayout,
    database: &str,
    user: &str,
    _database_names: &[String],
    stmt: Statement,
) -> SqlResult<QueryResult> {
    use plomid_sql::Statement as S;
    match stmt {
        S::CreateTable {
            name,
            if_not_exists,
            temporary: _,
            columns,
            constraints,
        } => {
            let name = catalog
                .resolve_create_name(&name)
                .map_err(SqlError::Storage)?;
            if if_not_exists && catalog.has_table(&name) {
                return Ok(QueryResult::Created("CREATE TABLE".into()));
            }
            let base_name = name.rsplit('.').next().unwrap_or(&name).to_string();
            for col in &columns {
                if col.col_type.serial {
                    let seq_name = format!("{base_name}_{}_seq", col.name);
                    if !catalog.has_sequence(&seq_name) {
                        catalog
                            .create_sequence(&seq_name)
                            .map_err(SqlError::Storage)?;
                        // Sequences advance outside the transaction
                        // (PostgreSQL: sequence gaps on rollback are correct).
                        let key = crate::catalog_fn::sequence_key(&seq_name);
                        let mut seq_txn = engine.begin().map_err(SqlError::Storage)?;
                        seq_txn
                            .put(&key, &0i64.to_le_bytes())
                            .map_err(SqlError::Storage)?;
                        seq_txn.commit().map_err(SqlError::Storage)?;
                    }
                }
            }
            catalog
                .create_table(name.clone(), columns, constraints)
                .map_err(SqlError::Storage)?;
            crate::index::register_constraint_indexes(catalog, &name).map_err(SqlError::Storage)?;
            if let Ok(created) = catalog.get_table(&name) {
                let vector_cols: Vec<plomid_sql::ColumnDef> = created
                    .columns
                    .iter()
                    .filter(|c| c.col_type.type_oid == plomid_types::TypeOid::VECTOR)
                    .cloned()
                    .collect();
                crate::ddl::register_auto_vector_indexes(catalog, &name, &vector_cols)?;
            }
            crate::ddl::materialize_relation(catalog, layout, database, &name)
                .map_err(SqlError::Storage)?;
            catalog.set_table_owner(&name, user);
            session_txn.created_tables.push(name);
            Ok(QueryResult::Created("CREATE TABLE".into()))
        }
        S::CreateIndex {
            name,
            table,
            column,
            columns,
            expression,
            unique,
            if_not_exists,
            using,
            operator_class,
        } => {
            // Index creation resolves against the session catalog (sees
            // in-transaction tables) and persists deferred like all DDL.
            let resolved_table = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            if catalog.index(&name).is_some() {
                if if_not_exists {
                    return Ok(QueryResult::Created("CREATE INDEX".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::AlreadyExists,
                    format!("index \"{name}\" already exists"),
                )));
            }
            // Vector-method indexes build ANN files outside the MVCC
            // transaction (separate persistence system): creating one
            // inside an explicit transaction cannot roll back. Create
            // vector indexes in autocommit instead.
            let schema = catalog
                .get_table(&resolved_table)
                .map_err(SqlError::Storage)?
                .clone();
            let ordered: Vec<String> = if columns.is_empty() {
                vec![column.clone()]
            } else {
                columns.clone()
            };
            if expression.is_none()
                && ordered.len() == 1
                && schema
                    .column_index(&ordered[0])
                    .ok()
                    .and_then(|pos| schema.columns.get(pos))
                    .is_some_and(|col| col.col_type.type_oid == plomid_types::TypeOid::VECTOR)
            {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::Unsupported,
                    "CREATE VECTOR index inside explicit transactions is not yet supported; create it in autocommit",
                )));
            }
            let _ = using;
            // Backfill from overlay-merged rows so the index covers staged
            // writes from earlier in the transaction (same entry encoding
            // and uniqueness checks as the autocommit build).
            let column_positions = if expression.is_none() {
                Some(
                    ordered
                        .iter()
                        .map(|indexed| schema.column_index(indexed))
                        .collect::<plomid_core::Result<Vec<_>>>()
                        .map_err(SqlError::Storage)?,
                )
            } else {
                None
            };
            let (start, end) = txn_table_range(&resolved_table);
            let pairs = session_txn
                .txn
                .scan(Some(&start), Some(&end))
                .map_err(|e| {
                    SqlError::Storage(PlomidError::new(
                        ErrorKind::Internal,
                        format!("transactional index backfill scan failed: {}", e.message()),
                    ))
                })?;
            let mut seen: Vec<Vec<Value>> = Vec::new();
            for (row_key, bytes) in pairs {
                let row = crate::encoding::decode_row(&bytes).map_err(SqlError::Storage)?;
                let values = if let Some(expr) = &expression {
                    vec![crate::query::evaluate_expression(&row, &schema, expr)?]
                } else {
                    column_positions
                        .as_ref()
                        .expect("plain column index")
                        .iter()
                        .map(|position| {
                            row.get(*position).cloned().ok_or_else(|| {
                                SqlError::Storage(PlomidError::new(
                                    ErrorKind::Corruption,
                                    "row has fewer columns than catalog",
                                ))
                            })
                        })
                        .collect::<SqlResult<Vec<_>>>()?
                };
                if unique
                    && crate::index::tuple_conflicts_under_unique(&values)
                    && seen
                        .iter()
                        .any(|existing| crate::index::tuples_equal(existing, &values))
                {
                    return Err(SqlError::Storage(PlomidError::new(
                        ErrorKind::Conflict,
                        "duplicate key violates unique index",
                    )));
                }
                if unique && crate::index::tuple_conflicts_under_unique(&values) {
                    seen.push(values.clone());
                }
                session_txn
                    .txn
                    .put(
                        &crate::index::index_entry_key_multi(&name, &values, &row_key),
                        &row_key,
                    )
                    .map_err(SqlError::Storage)?;
            }
            catalog
                .create_index(
                    name.clone(),
                    resolved_table.clone(),
                    column,
                    columns,
                    expression,
                    unique,
                    operator_class,
                )
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("CREATE INDEX".into()))
        }
        S::CreateView {
            name,
            columns,
            query,
            or_replace,
        } => {
            let name = catalog
                .resolve_create_name(&name)
                .map_err(SqlError::Storage)?;
            if or_replace && catalog.has_view(&name) {
                catalog.drop_view(&name).map_err(SqlError::Storage)?;
            }
            catalog
                .create_view(name.clone(), columns, query)
                .map_err(SqlError::Storage)?;
            catalog.set_table_owner(&name, user);
            Ok(QueryResult::Created("CREATE VIEW".into()))
        }
        S::DropView {
            name,
            if_exists,
            cascade: _,
        } => {
            if !catalog.has_view(&name) {
                if if_exists {
                    return Ok(QueryResult::Created("DROP VIEW".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("view \"{name}\" does not exist"),
                )));
            }
            catalog.drop_view(&name).map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP VIEW".into()))
        }
        S::DropIndex { name, if_exists } => {
            let definition = catalog
                .indexes()
                .into_iter()
                .find(|index| index.name == name);
            let Some(definition) = definition else {
                if if_exists {
                    return Ok(QueryResult::Created("DROP INDEX".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("index \"{name}\" does not exist"),
                )));
            };
            // Stage index-entry deletions into the overlay.
            let prefix = crate::index::index_value_prefix(&definition.name, None);
            let entries = session_txn
                .txn
                .scan(Some(&prefix), Some(&crate::index::prefix_end(&prefix)))
                .map_err(|e| {
                    SqlError::Storage(PlomidError::new(
                        ErrorKind::Internal,
                        format!("transactional index drop scan failed: {}", e.message()),
                    ))
                })?;
            for (key, _) in entries {
                session_txn.txn.delete(&key).map_err(SqlError::Storage)?;
            }
            catalog
                .drop_index(&definition.name)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP INDEX".into()))
        }
        S::Create(create) => {
            create_in_session_txn(engine, session_txn, catalog, layout, database, user, create)
        }
        S::DropSequence { name, if_exists } => {
            if !catalog.has_sequence(&name) {
                if if_exists {
                    return Ok(QueryResult::Created("DROP SEQUENCE".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("sequence \"{name}\" does not exist"),
                )));
            }
            catalog.drop_sequence(&name).map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP SEQUENCE".into()))
        }
        S::DropType {
            name, if_exists, ..
        } => {
            if !catalog.has_type(&name) {
                if if_exists {
                    return Ok(QueryResult::Created("DROP TYPE".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("type \"{name}\" does not exist"),
                )));
            }
            catalog.drop_type(&name).map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP TYPE".into()))
        }
        S::DropDomain {
            name, if_exists, ..
        } => {
            if !catalog.has_domain(&name) {
                if if_exists {
                    return Ok(QueryResult::Created("DROP DOMAIN".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("domain \"{name}\" does not exist"),
                )));
            }
            catalog.drop_domain(&name).map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP DOMAIN".into()))
        }
        S::DropFunction { name, args, .. } => {
            catalog
                .drop_function(&name, &args)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP FUNCTION".into()))
        }
        S::AlterTableRename { table, new_name } => {
            let resolved = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            catalog
                .rename_table(&resolved, new_name)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER TABLE".into()))
        }
        S::AlterTableRenameColumn {
            table,
            old_name,
            new_name,
        } => {
            let resolved = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            catalog
                .rename_column(&resolved, &old_name, new_name)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER TABLE".into()))
        }
        S::AlterTableAddColumn { table, column, .. } => {
            let resolved = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            catalog
                .add_column(&resolved, column)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER TABLE".into()))
        }
        S::AlterTableAddConstraint { table, constraint } => {
            let resolved = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            let mut constraints = catalog
                .constraints(&resolved)
                .map_err(SqlError::Storage)?
                .to_vec();
            for column in &constraint.columns {
                catalog
                    .get_table(&resolved)
                    .map_err(SqlError::Storage)?
                    .column_index(column)
                    .map_err(SqlError::Storage)?;
            }
            constraints.push(constraint);
            catalog
                .set_constraints(&resolved, constraints)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER TABLE".into()))
        }
        S::AlterTableDropColumn { table, column, .. } => {
            let resolved = catalog
                .resolve_table_name(&table)
                .map_err(SqlError::Storage)?;
            catalog
                .drop_column(&resolved, &column)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER TABLE".into()))
        }
        S::GrantRole { role, member } => {
            catalog
                .grant_role(&role, &member)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("GRANT ROLE".into()))
        }
        S::RevokeRole { role, member } => {
            catalog
                .revoke_role(&role, &member)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("REVOKE ROLE".into()))
        }
        S::GrantPrivileges {
            privileges,
            object,
            grantee,
        } => {
            crate::ddl::grant_privileges_op(catalog, &privileges, &object, &grantee)?;
            Ok(QueryResult::Created("GRANT".into()))
        }
        S::RevokePrivileges {
            privileges,
            object,
            grantee,
        } => {
            crate::ddl::revoke_privileges_op(catalog, &privileges, &object, &grantee)?;
            Ok(QueryResult::Created("REVOKE".into()))
        }
        S::AlterRole {
            name,
            password,
            login,
            superuser,
            create_database,
            create_role,
        } => {
            if !catalog.has_role(&name) {
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("role \"{name}\" does not exist"),
                )));
            }
            if let Some(password) = password {
                let credential = plomid_security::RoleCredential::derive(
                    password.as_bytes(),
                    plomid_security::random_salt().map_err(|e| {
                        SqlError::Storage(PlomidError::new(ErrorKind::Internal, e.to_string()))
                    })?,
                );
                catalog.set_role_credential(&name, &credential);
            }
            catalog
                .alter_role_flags(&name, login, superuser, create_database, create_role)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("ALTER ROLE".into()))
        }
        S::DropRole { name, if_exists } => {
            if !catalog.has_role(&name) {
                if if_exists {
                    return Ok(QueryResult::Created("DROP ROLE".into()));
                }
                return Err(SqlError::Storage(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("role \"{name}\" does not exist"),
                )));
            }
            catalog.drop_role(&name).map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("DROP ROLE".into()))
        }
        S::Set { .. } | S::Show { .. } | S::Describe { .. } => {
            Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                "session statements must not reach DDL dispatch",
            )))
        }
        _ => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Unsupported,
            "statement inside explicit transaction not supported",
        ))),
    }
}

/// CREATE variants inside a session transaction (catalog-only + eager
/// non-transactional side effects; persist deferred to COMMIT).
#[allow(clippy::too_many_arguments)]
fn create_in_session_txn(
    _engine: &mut ConcurrentPlomidStorageEngine,
    _session_txn: &mut SessionTxn,
    catalog: &mut InMemoryCatalog,
    layout: &plomid_storage::DatabaseLayout,
    database: &str,
    user: &str,
    create: plomid_sql::CreateStatement,
) -> SqlResult<QueryResult> {
    use plomid_sql::CreateStatement as C;
    match create {
        C::Database { .. } => Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Unsupported,
            "CREATE DATABASE cannot be executed inside a transaction block",
        ))),
        C::Schema {
            name,
            if_not_exists,
        } => {
            if if_not_exists && catalog.has_schema(&name) {
                return Ok(QueryResult::Created("CREATE SCHEMA".into()));
            }
            catalog
                .create_schema(name.clone())
                .map_err(SqlError::Storage)?;
            catalog.set_schema_owner(&name, user);
            crate::ddl::materialize_schema(catalog, layout, database, &name)
                .map_err(SqlError::Storage)?;
            Ok(QueryResult::Created("CREATE SCHEMA".into()))
        }
        C::Role {
            name,
            login,
            password,
            superuser,
            create_database,
            create_role,
        } => {
            catalog
                .create_role(plomid_sql::RoleDefinition {
                    name: name.clone(),
                    superuser,
                    inherit: true,
                    create_role,
                    create_database,
                    can_login: login,
                    replication: false,
                    bypass_rls: false,
                    connection_limit: -1,
                    password: None,
                    members: Vec::new(),
                })
                .map_err(SqlError::Storage)?;
            if let Some(password) = password {
                let credential = plomid_security::RoleCredential::derive(
                    password.as_bytes(),
                    plomid_security::random_salt().map_err(|e| {
                        SqlError::Storage(PlomidError::new(ErrorKind::Internal, e.to_string()))
                    })?,
                );
                catalog.set_role_credential(&name, &credential);
            }
            Ok(QueryResult::Created("CREATE ROLE".into()))
        }
        // Table-valued creation flows through dedicated arms; every other
        // object class follows the autocommit policy (superuser-gated by
        // the authorizer; unimplemented shapes error here like there).
        _ => Err(SqlError::Storage(PlomidError::with_detail(
            ErrorKind::Unsupported,
            "CREATE object is not implemented yet",
            "in-transaction".to_string(),
        ))),
    }
}

/// Drops a table inside a session transaction: row + index keys stage
/// into the overlay, catalog entries drop immediately (reverted on abort),
/// filesystem/columnar/stats effects defer to a commit cleanup record.
#[allow(clippy::too_many_arguments)]
fn drop_table_in_session_txn(
    session_txn: &mut SessionTxn,
    catalog: &mut InMemoryCatalog,
    database: &str,
    statistics: &mut std::collections::BTreeMap<String, crate::statistics::TableStatistics>,
    name: String,
    if_exists: bool,
    cascade: bool,
) -> SqlResult<PendingCleanup> {
    use plomid_sql::Catalog as _;
    let resolved = catalog
        .resolve_table_name(&name)
        .unwrap_or_else(|_| catalog.resolve_table_namespace(&name));
    if !catalog.has_table(&resolved) {
        if if_exists {
            // No-op drops still need a cleanup record shape; the caller
            // skips recording when nothing dropped (see below).
            return Err(SqlError::Storage(PlomidError::new(
                ErrorKind::NotFound,
                format!("table \"{resolved}\" does not exist"),
            )));
        }
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::NotFound,
            format!("table \"{resolved}\" does not exist"),
        )));
    }
    if !cascade {
        if let Some(dependent) = catalog.find_table_dependency(&resolved) {
            return Err(SqlError::Storage(PlomidError::with_detail(
                ErrorKind::Catalog,
                format!("cannot drop table \"{resolved}\" because other objects depend on it"),
                format!("dependent={dependent}"),
            )));
        }
    }
    if cascade {
        let dependent_views: Vec<String> = catalog
            .view_names()
            .into_iter()
            .filter(|v| catalog.view_depends_on_table(v, &resolved))
            .collect();
        for view in &dependent_views {
            catalog.drop_view(view).map_err(SqlError::Storage)?;
        }
    }
    let indexes = catalog.all_indexes_for_table(&resolved);
    // Stage every row key + index entry key into the overlay.
    let (start, end) = txn_table_range(&resolved);
    let row_keys: Vec<Vec<u8>> = session_txn
        .txn
        .scan(Some(&start), Some(&end))
        .map_err(|e| {
            SqlError::Storage(PlomidError::new(
                ErrorKind::Internal,
                format!("transactional drop scan failed: {}", e.message()),
            ))
        })?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    let mut index_keys = Vec::new();
    for index in &indexes {
        let prefix = crate::index::index_value_prefix(&index.name, None);
        let entries = session_txn
            .txn
            .scan(Some(&prefix), Some(&crate::index::prefix_end(&prefix)))
            .map_err(|e| {
                SqlError::Storage(PlomidError::new(
                    ErrorKind::Internal,
                    format!("transactional drop scan failed: {}", e.message()),
                ))
            })?;
        index_keys.extend(entries.into_iter().map(|(key, _)| key));
    }
    for key in row_keys.into_iter().chain(index_keys) {
        session_txn.txn.delete(&key).map_err(SqlError::Storage)?;
    }
    let table_id = catalog
        .get_table(&resolved)
        .map_err(SqlError::Storage)?
        .table_id;
    let database_id = crate::ddl::current_database_id(catalog, database).ok_or_else(|| {
        SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            "current database has no identity",
        ))
    })?;
    let schema_name = resolved.split_once('.').map(|(s, _)| s).unwrap_or("public");
    let schema_id = catalog.schema_id(schema_name).ok_or_else(|| {
        SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            format!("schema \"{schema_name}\" has no identity"),
        ))
    })?;
    catalog.drop_table(&resolved).map_err(SqlError::Storage)?;
    for index in &indexes {
        let _ = catalog.drop_index(&index.name);
    }
    statistics.retain(|key, _| key != &resolved && !key.starts_with(&format!("{resolved}.")));
    Ok(PendingCleanup::DropTable {
        name: resolved,
        table_id,
        database_id,
        schema_id,
    })
}

/// True for object-not-found errors (IF EXISTS no-op detection).
fn is_not_found(error: &SqlError) -> bool {
    matches!(error, SqlError::Storage(e) if e.kind() == ErrorKind::NotFound)
}

/// Drops a schema inside a session transaction: contained tables route
/// through the transactional drop core (overlay + deferred cleanups),
/// other objects drop from the session catalog, and the schema directory
/// removal defers to a commit cleanup.
#[allow(clippy::too_many_arguments)]
fn drop_schema_in_session_txn(
    engine: &mut ConcurrentPlomidStorageEngine,
    session_txn: &mut SessionTxn,
    catalog: &mut InMemoryCatalog,
    layout: &plomid_storage::DatabaseLayout,
    database: &str,
    statistics: &mut std::collections::BTreeMap<String, crate::statistics::TableStatistics>,
    name: String,
    if_exists: bool,
    cascade: bool,
) -> SqlResult<QueryResult> {
    use plomid_sql::Catalog as _;
    if !catalog.has_schema(&name) {
        if if_exists {
            return Ok(QueryResult::Created("DROP SCHEMA".into()));
        }
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::NotFound,
            format!("schema \"{name}\" does not exist"),
        )));
    }
    let prefix = format!("{name}.");
    let tables: Vec<String> = catalog
        .table_names()
        .into_iter()
        .filter(|table| table.starts_with(&prefix))
        .collect();
    let views: Vec<String> = catalog
        .view_names()
        .into_iter()
        .filter(|view| view.starts_with(&prefix))
        .collect();
    let sequences: Vec<String> = catalog
        .sequence_names()
        .into_iter()
        .filter(|sequence| sequence.starts_with(&prefix))
        .collect();
    let indexes: Vec<String> = catalog
        .indexes()
        .into_iter()
        .filter(|index| index.table.starts_with(&prefix))
        .map(|index| index.name)
        .collect();
    let has_objects =
        !tables.is_empty() || !views.is_empty() || !sequences.is_empty() || !indexes.is_empty();
    if has_objects && !cascade {
        return Err(SqlError::Storage(PlomidError::with_detail(
            ErrorKind::Catalog,
            format!("cannot drop schema \"{name}\" because other objects depend on it"),
            format!("schema={name}"),
        )));
    }
    for table in tables {
        match drop_table_in_session_txn(
            session_txn,
            catalog,
            database,
            statistics,
            table,
            false,
            true,
        ) {
            Ok(cleanup) => session_txn.pending_cleanups.push(cleanup),
            Err(error) => {
                if !is_not_found(&error) {
                    return Err(error);
                }
            }
        }
    }
    for view in &views {
        catalog.drop_view(view).map_err(SqlError::Storage)?;
    }
    for sequence in &sequences {
        catalog.drop_sequence(sequence).map_err(SqlError::Storage)?;
    }
    for index in &indexes {
        let _ = catalog.drop_index(index);
    }
    for ty in catalog
        .type_names()
        .into_iter()
        .filter(|t| t.starts_with(&prefix))
        .collect::<Vec<_>>()
    {
        let _ = catalog.drop_type(&ty);
    }
    for domain in catalog
        .domain_names()
        .into_iter()
        .filter(|d| d.starts_with(&prefix))
        .collect::<Vec<_>>()
    {
        let _ = catalog.drop_domain(&domain);
    }
    let database_id = crate::ddl::current_database_id(catalog, database).ok_or_else(|| {
        SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            "current database has no identity",
        ))
    })?;
    let schema_id = catalog.schema_id(&name).ok_or_else(|| {
        SqlError::Storage(PlomidError::new(
            ErrorKind::Internal,
            format!("schema \"{name}\" has no identity"),
        ))
    })?;
    catalog.drop_schema(&name).map_err(SqlError::Storage)?;
    session_txn
        .pending_cleanups
        .push(PendingCleanup::DropSchema {
            database_id,
            schema_id,
        });
    let _ = (engine, layout);
    Ok(QueryResult::Created("DROP SCHEMA".into()))
}
