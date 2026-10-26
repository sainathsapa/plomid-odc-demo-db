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
//! Statement-level authorization enforcement (Beta 2 Phase 2).
//!
//! [`authorize_statement`] is the single enforcement gate: every top-level
//! statement passes through it before execution, in both the autocommit
//! path and the explicit-transaction group path (including COMMIT-time
//! replay). It answers ONE question — "may `user` run this statement?" —
//! against the live catalog, and on denial emits a security audit event
//! (tracing + durable sink via `plomid_security::sink::emit` +
//! `authz_denials` counter) and fails with `42501`. Allowed statements emit
//! an allow record through the same sink (tracing at `info`).
//!
//! Rules (PostgreSQL-aligned subset, fail closed):
//!
//! * superusers bypass everything; table/schema owners hold full rights on
//!   their objects; everyone else needs explicit grants;
//! * reads need `SELECT` (+ schema `USAGE`); writes need their matching
//!   privilege; DDL needs ownership or the matching role attribute;
//! * `USAGE` on the `public` schema is default-allow (PostgreSQL default);
//! * `CONNECT` on databases is default-allow unless withdrawn;
//! * file `COPY` is superuser-only (PostgreSQL default behavior);
//! * unknown users, unknown objects, and missing grants deny.
//!
//! Expression-level `has_*_privilege` answers come from [`AuthSnapshot`],
//! a per-statement owned privilege view published here (scalar evaluation
//! has no catalog handle). Enforcement NEVER consults `has_*`: the
//! functions are informational tooling answers, never a decision input.

use crate::error::{SqlError, SqlResult};
use plomid_security::{
    denial_error, AuditAction, AuditEvent, DatabasePrivilege, GrantObject, RoleAttribute,
    SchemaPrivilege, TablePrivilege,
};
use plomid_sql::{Catalog, FromClause, InMemoryCatalog, Statement};
use std::collections::{BTreeSet, HashMap, HashSet};

/// Owned per-statement privilege view for expression-level answers.
///
/// Established by [`authorize_statement`] before execution (and therefore
/// reflecting exactly the catalog the statement runs against). Scalar
/// `has_*_privilege` calls read the thread-local current snapshot; nothing
/// in enforcement reads it.
#[derive(Debug, Clone)]
pub struct AuthSnapshot {
    user: String,
    superuser: bool,
    closure: HashSet<String>,
    search_path: Vec<String>,
    table_owners: HashMap<String, String>,
    table_grants: HashMap<(String, String), u16>,
    schema_owners: HashMap<String, String>,
    schema_grants: HashMap<(String, String), u16>,
    db_denied: BTreeSet<(String, String)>,
    db_grants: HashMap<(String, String), u16>,
}

impl AuthSnapshot {
    pub fn capture(catalog: &InMemoryCatalog, user: &str) -> Self {
        let (table_owners, table_grants, schema_owners, schema_grants, db_denied, db_grants) =
            catalog.auth_snapshot_maps();
        Self {
            user: user.to_string(),
            superuser: catalog.is_superuser(user),
            closure: catalog.role_closure(user).into_iter().collect(),
            search_path: catalog.search_path().to_vec(),
            table_owners,
            table_grants,
            schema_owners,
            schema_grants,
            db_denied,
            db_grants,
        }
    }

    /// Resolves a possibly-unqualified relation name the way the catalog
    /// would: exact hit, then first search_path hit, then any same-named
    /// relation. `None` when nothing matches (answer: false, not an error).
    fn resolve_table(&self, name: &str) -> Option<String> {
        if self.table_owners.contains_key(name) || self.table_grants.keys().any(|(_, t)| t == name)
        {
            return Some(name.to_string());
        }
        for schema in &self.search_path {
            if schema.is_empty() || schema == "$user" {
                continue;
            }
            let qualified = format!("{schema}.{name}");
            if self.table_owners.contains_key(&qualified)
                || self.table_grants.keys().any(|(_, t)| t == &qualified)
            {
                return Some(qualified);
            }
        }
        // Last resort: any relation with this bare name (ambiguous across
        // schemas — documented; enforcement resolves precisely).
        self.table_owners
            .keys()
            .chain(self.table_grants.keys().map(|(_, t)| t))
            .find(|k| k.split('.').next_back().is_some_and(|s| s == name))
            .cloned()
    }

    fn table_ok(&self, who: &str, table: &str, privilege: TablePrivilege) -> bool {
        if self.superuser && who == self.user {
            return true;
        }
        // Explicit-user form: superuser-ness is the *queried* user's own row,
        // which the snapshot does not carry — owners and grants still answer.
        if self.table_owners.get(table).is_some_and(|o| o == who) {
            return true;
        }
        closure_for(&self.closure, who).iter().any(|role| {
            self.table_grants
                .get(&(role.clone(), table.to_string()))
                .is_some_and(|bits| privilege.held_by(*bits))
        })
    }

    /// `has_table_privilege` answer for the snapshot user (or an explicit
    /// user). Unknown privilege names return false (fail closed).
    pub fn has_table_privilege(&self, who: &str, table: &str, priv_name: &str) -> bool {
        let Some(privilege) = parse_table_priv(priv_name) else {
            return false;
        };
        self.table_ok(who, table, privilege)
    }

    pub fn has_schema_privilege(&self, who: &str, schema: &str, priv_name: &str) -> bool {
        let privilege = match priv_name.to_ascii_uppercase().as_str() {
            "USAGE" => SchemaPrivilege::Usage,
            "CREATE" => SchemaPrivilege::Create,
            _ => return false,
        };
        if self.superuser && who == self.user {
            return true;
        }
        if self.schema_owners.get(schema).is_some_and(|o| o == who) {
            return true;
        }
        if schema == "public" && privilege == SchemaPrivilege::Usage {
            return true;
        }
        closure_for(&self.closure, who).iter().any(|role| {
            self.schema_grants
                .get(&(role.clone(), schema.to_string()))
                .is_some_and(|bits| privilege.held_by(*bits))
        })
    }

    pub fn has_database_privilege(&self, who: &str, database: &str, priv_name: &str) -> bool {
        let privilege = match priv_name.to_ascii_uppercase().as_str() {
            "CONNECT" => DatabasePrivilege::Connect,
            "CREATE" => DatabasePrivilege::Create,
            _ => return false,
        };
        if self.superuser && who == self.user {
            return true;
        }
        match privilege {
            DatabasePrivilege::Connect => !closure_for(&self.closure, who).iter().any(|role| {
                self.db_denied
                    .contains(&(role.clone(), database.to_string()))
            }),
            DatabasePrivilege::Create => closure_for(&self.closure, who).iter().any(|role| {
                self.db_grants
                    .get(&(role.clone(), database.to_string()))
                    .is_some_and(|bits| privilege.held_by(*bits))
            }),
        }
    }
}

/// Membership expansion for an explicitly named user inside a snapshot.
/// The snapshot closure belongs to the session user; for any other name
/// only `PUBLIC` grants apply (membership graphs live in the catalog,
/// which scalar evaluation cannot reach — a documented Beta 2 limit).
fn closure_for(snapshot_closure: &HashSet<String>, who: &str) -> Vec<String> {
    if snapshot_closure.contains(who) {
        snapshot_closure.iter().cloned().collect()
    } else {
        vec!["PUBLIC".to_string(), who.to_string()]
    }
}

fn parse_table_priv(name: &str) -> Option<TablePrivilege> {
    match name.to_ascii_uppercase().as_str() {
        "SELECT" => Some(TablePrivilege::Select),
        "INSERT" => Some(TablePrivilege::Insert),
        "UPDATE" => Some(TablePrivilege::Update),
        "DELETE" => Some(TablePrivilege::Delete),
        "TRUNCATE" => Some(TablePrivilege::Truncate),
        _ => None,
    }
}

thread_local! {
    static AUTH_SNAPSHOT: std::cell::RefCell<Option<std::rc::Rc<AuthSnapshot>>> =
        const { std::cell::RefCell::new(None) };
}

/// Publishes the statement's privilege view for expression-level answers.
/// Restores the previous snapshot on drop (nested statements reuse outer).
pub struct SnapshotGuard {
    prev: Option<std::rc::Rc<AuthSnapshot>>,
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        AUTH_SNAPSHOT.with(|slot| {
            *slot.borrow_mut() = self.prev.take();
        });
    }
}

pub fn establish_snapshot(snapshot: AuthSnapshot) -> SnapshotGuard {
    let prev = AUTH_SNAPSHOT.with(|slot| slot.borrow().clone());
    // Nested statements (e.g. EXPLAIN inner checks) keep the outer view:
    // one statement, one privilege view.
    if prev.is_none() {
        AUTH_SNAPSHOT.with(|slot| {
            *slot.borrow_mut() = Some(std::rc::Rc::new(snapshot));
        });
    }
    SnapshotGuard { prev }
}

/// Current statement snapshot, if one was established.
pub fn current_snapshot() -> Option<std::rc::Rc<AuthSnapshot>> {
    AUTH_SNAPSHOT.with(|slot| slot.borrow().clone())
}

/// Expression-level `has_*_privilege` answers from the statement snapshot.
///
/// Forms: `(object, priv)` (current user) and `(user, object, priv)`.
/// Non-text (e.g. OID) arguments yield NULL — OIDs are not resolved in
/// Beta 2. Unknown objects yield false; unknown privilege names yield
/// false. Outside a statement (no snapshot) yields false: fail closed.
pub fn has_privilege_answer(
    lname: &str,
    args: &[plomid_sql::Value],
) -> SqlResult<plomid_sql::Value> {
    use plomid_sql::Value;
    let Some(snapshot) = current_snapshot() else {
        return Ok(Value::Bool(false));
    };
    fn text(value: &Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::Text(s) | Value::Name(s) | Value::VarChar(s) | Value::BpChar(s) => {
                Some(s.clone())
            }
            _ => None,
        }
    }
    // NULL inputs propagate NULL (PostgreSQL semantics).
    if args.iter().any(|a| matches!(a, Value::Null)) {
        return Ok(Value::Null);
    }
    let (who, object, priv_name) = match args {
        [object, priv_name] => (snapshot.user.clone(), object, priv_name),
        [who, object, priv_name] => {
            let Some(who) = text(who) else {
                return Ok(Value::Null);
            };
            let who = if who.eq_ignore_ascii_case("current_user")
                || who.eq_ignore_ascii_case("session_user")
            {
                snapshot.user.clone()
            } else {
                who
            };
            (who, object, priv_name)
        }
        _ => {
            return Err(SqlError::Storage(plomid_core::PlomidError::new(
                plomid_core::ErrorKind::InvalidArgument,
                format!("{lname} requires 2 or 3 arguments"),
            )));
        }
    };
    let (Some(object), Some(priv_name)) = (text(object), text(priv_name)) else {
        return Ok(Value::Null);
    };
    let answer = match lname {
        "has_table_privilege" => snapshot
            .resolve_table(&object)
            .is_some_and(|resolved| snapshot.has_table_privilege(&who, &resolved, &priv_name)),
        "has_schema_privilege" => snapshot.has_schema_privilege(&who, &object, &priv_name),
        _ => snapshot.has_database_privilege(&who, &object, &priv_name),
    };
    Ok(Value::Bool(answer))
}

/// Canonical database spelling (case-insensitive), if registered.
fn canonical_database(catalog: &InMemoryCatalog, name: &str) -> Option<String> {
    catalog
        .database_names()
        .iter()
        .find(|n| n.eq_ignore_ascii_case(name))
        .cloned()
}

/// Schema qualifier of a resolved table name (`s.t` → `s`, bare → public).
fn schema_of(resolved: &str) -> &str {
    resolved.split_once('.').map(|(s, _)| s).unwrap_or("public")
}

/// Authorize + hold the snapshot guard for the statement's evaluation.
/// Callers (autocommit + group pre-check) use this so `has_*` answers
/// inside the statement see the checked view. Existence is checked before
/// privilege (PostgreSQL behavior): missing objects report `NotFound`,
/// existing-but-forbidden objects report `42501`.
pub fn authorize_with_snapshot(
    catalog: &InMemoryCatalog,
    user: &str,
    database: &str,
    stmt: &Statement,
) -> SqlResult<SnapshotGuard> {
    let guard = establish_snapshot(AuthSnapshot::capture(catalog, user));
    authorize_one(catalog, user, database, stmt).map(|()| guard)
}

/// Single-statement check with denial audit. Used by
/// [`authorize_with_snapshot`] and by the transaction-group pre-check
/// (which holds one snapshot across the whole group).
pub(crate) fn authorize_one(
    catalog: &InMemoryCatalog,
    user: &str,
    database: &str,
    stmt: &Statement,
) -> SqlResult<()> {
    let outcome = authorize_inner(catalog, user, database, stmt);
    let (action, object) = describe(stmt);
    match &outcome {
        Ok(()) => {
            // Allowed decisions are audited too: the sink must show both
            // sides of every gate, not just denials.
            let event = AuditEvent::allow(user, action, Some(object));
            tracing::info!(target: "security", "{}", event.log_line());
            let _ = plomid_security::sink::emit(&event);
        }
        Err(SqlError::Storage(e)) if e.kind() == plomid_core::ErrorKind::PermissionDenied => {
            let event = AuditEvent::deny(user, action, Some(object), "missing_privilege");
            // Durable sink (additive; the tracing line below is unchanged).
            let _ = plomid_security::sink::emit(&event);
            tracing::warn!(target: "security", "{}", event.log_line());
            plomid_core::observability::record_authz_denial();
        }
        Err(_) => {}
    }
    outcome
}

fn deny(user: &str, privilege: &str, object: &str) -> SqlResult<()> {
    Err(SqlError::Storage(denial_error(user, privilege, object)))
}

fn describe(stmt: &Statement) -> (AuditAction, String) {
    let name = statement_type_name(stmt);
    (
        AuditAction::Authorize {
            statement: name.clone(),
        },
        name,
    )
}

fn statement_type_name(stmt: &Statement) -> String {
    match stmt {
        Statement::Select { .. } => "SELECT",
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete { .. } => "DELETE",
        Statement::Copy { .. } => "COPY",
        Statement::Truncate { .. } => "TRUNCATE",
        _ => "OTHER",
    }
    .to_string()
}

/// Resolves a read/write table reference: existence first (NotFound),
/// returning the resolved (qualified) name.
fn resolve_table(catalog: &InMemoryCatalog, name: &str) -> SqlResult<String> {
    catalog.resolve_table_name(name).map_err(SqlError::Storage)
}

/// Requires `SELECT` (+ schema `USAGE`) on every table a read touches.
fn require_reads(catalog: &InMemoryCatalog, user: &str, tables: &[String]) -> SqlResult<()> {
    for table in tables {
        let resolved = resolve_table(catalog, table)?;
        let schema = schema_of(&resolved);
        if !catalog.check_schema_privilege(user, schema, SchemaPrivilege::Usage) {
            return deny(user, "USAGE", &format!("SCHEMA {schema}"));
        }
        if !catalog.check_table_privilege(user, &resolved, TablePrivilege::Select) {
            return deny(user, "SELECT", &format!("TABLE {resolved}"));
        }
    }
    Ok(())
}

/// Requires a write privilege (+ schema `USAGE`) on a single target table.
fn require_write(
    catalog: &InMemoryCatalog,
    user: &str,
    table: &str,
    privilege: TablePrivilege,
) -> SqlResult<()> {
    let resolved = resolve_table(catalog, table)?;
    let schema = schema_of(&resolved);
    if !catalog.check_schema_privilege(user, schema, SchemaPrivilege::Usage) {
        return deny(user, "USAGE", &format!("SCHEMA {schema}"));
    }
    if !catalog.check_table_privilege(user, &resolved, privilege) {
        return deny(user, privilege.as_str(), &format!("TABLE {resolved}"));
    }
    Ok(())
}

/// Collects every table a `FROM` clause reads (joins, subqueries included).
fn from_tables(from: &FromClause, out: &mut Vec<String>) {
    match from {
        FromClause::Table { name, .. } => out.push(name.clone()),
        FromClause::TableFunction { .. } => {}
        FromClause::Join { left, right, .. } => {
            from_tables(left, out);
            from_tables(right, out);
        }
        FromClause::Subquery { statement, .. } => statement_tables(statement, out),
    }
}

/// Collects every table a query statement reads (SELECT/With/SetOperation
/// bodies, including CTE definitions).
fn statement_tables(stmt: &Statement, out: &mut Vec<String>) {
    match stmt {
        Statement::Select {
            from: Some(from), ..
        } => from_tables(from, out),
        Statement::Select { .. } => {}
        Statement::With { ctes, body, .. } => {
            for cte in ctes {
                statement_tables(&cte.query, out);
            }
            statement_tables(body, out);
        }
        Statement::SetOperation { left, right, .. } => {
            statement_tables(left, out);
            statement_tables(right, out);
        }
        Statement::Insert {
            source: plomid_sql::InsertSource::Select(query),
            ..
        } => statement_tables(query, out),
        Statement::Insert { .. } => {}
        Statement::Update {
            from: Some(from), ..
        } => from_tables(from, out),
        Statement::Update { .. } => {}
        Statement::Delete {
            using: Some(using), ..
        } => from_tables(using, out),
        Statement::Delete { .. } => {}
        _ => {}
    }
}

fn authorize_inner(
    catalog: &InMemoryCatalog,
    user: &str,
    database: &str,
    stmt: &Statement,
) -> SqlResult<()> {
    // Cheap bypass first: superusers skip every check below.
    let admin = catalog.is_superuser(user);
    match stmt {
        Statement::Select { .. } | Statement::With { .. } | Statement::SetOperation { .. } => {
            if admin {
                return Ok(());
            }
            let mut tables = Vec::new();
            statement_tables(stmt, &mut tables);
            // System-catalog and stats relations are metadata, not user
            // data: readable by every authenticated role (PostgreSQL
            // information_schema behavior). Anything else needs SELECT.
            tables.retain(|t| !is_system_relation(catalog, t));
            require_reads(catalog, user, &tables)
        }
        Statement::Values(_) => Ok(()),
        Statement::Insert { table, source, .. } => {
            if !admin {
                require_write(catalog, user, table, TablePrivilege::Insert)?;
                if let plomid_sql::InsertSource::Select(query) = source {
                    let mut tables = Vec::new();
                    statement_tables(query, &mut tables);
                    tables.retain(|t| !is_system_relation(catalog, t));
                    require_reads(catalog, user, &tables)?;
                }
            }
            Ok(())
        }
        Statement::Update { table, from, .. } => {
            if !admin {
                require_write(catalog, user, table, TablePrivilege::Update)?;
                if let Some(from) = from {
                    let mut tables = Vec::new();
                    from_tables(from, &mut tables);
                    tables.retain(|t| !is_system_relation(catalog, t));
                    require_reads(catalog, user, &tables)?;
                }
            }
            Ok(())
        }
        Statement::Delete { table, using, .. } => {
            if !admin {
                require_write(catalog, user, table, TablePrivilege::Delete)?;
                if let Some(using) = using {
                    let mut tables = Vec::new();
                    from_tables(using, &mut tables);
                    tables.retain(|t| !is_system_relation(catalog, t));
                    require_reads(catalog, user, &tables)?;
                }
            }
            Ok(())
        }
        Statement::Copy {
            table, direction, ..
        } => {
            if admin {
                return Ok(());
            }
            // Executor-level COPY is the stream form (STDIN/STDOUT): table
            // privileges. File COPY is gated separately at the wire layer
            // (superuser-only).
            let privilege = match direction {
                plomid_sql::CopyDirection::From => TablePrivilege::Insert,
                plomid_sql::CopyDirection::To => TablePrivilege::Select,
            };
            require_write(catalog, user, table, privilege)
        }
        Statement::Truncate { tables } => {
            if admin {
                return Ok(());
            }
            for table in tables {
                require_write(catalog, user, table, TablePrivilege::Truncate)?;
            }
            Ok(())
        }
        Statement::Explain { statement, .. } => {
            // EXPLAIN reveals the plan but executes nothing: require the
            // same rights as running the statement (PostgreSQL behavior).
            authorize_inner(catalog, user, database, statement)
        }
        Statement::CreateTable { name, .. } => {
            if admin {
                return Ok(());
            }
            let schema = create_schema_of(catalog, name)?;
            require_schema_create(catalog, user, &schema)
        }
        Statement::Create(create) => authorize_create(catalog, user, database, create, admin),
        Statement::CreateIndex { table, .. } => {
            if admin {
                return Ok(());
            }
            // Index creation needs ownership of the indexed table.
            let resolved = resolve_table(catalog, table)?;
            if catalog.table_owner(&resolved).is_some_and(|o| o == user) {
                Ok(())
            } else {
                deny(user, "OWNERSHIP", &format!("TABLE {resolved}"))
            }
        }
        Statement::CreateView { name, .. } => {
            if admin {
                return Ok(());
            }
            let schema = create_schema_of(catalog, name)?;
            require_schema_create(catalog, user, &schema)
        }
        Statement::DropTable { name, .. } => require_owner(catalog, user, name, admin),
        Statement::DropView { name, .. } => require_owner(catalog, user, name, admin),
        Statement::DropIndex { name, .. } => {
            if admin {
                return Ok(());
            }
            // Indexes hang off their table: the table owner (or superuser)
            // may drop them.
            let Some(index) = catalog.index(name) else {
                return Err(SqlError::Storage(plomid_core::PlomidError::new(
                    plomid_core::ErrorKind::NotFound,
                    format!("index \"{name}\" does not exist"),
                )));
            };
            let resolved = resolve_table(catalog, &index.table)?;
            if catalog.table_owner(&resolved).is_some_and(|o| o == user) {
                Ok(())
            } else {
                deny(user, "OWNERSHIP", &format!("TABLE {resolved}"))
            }
        }
        Statement::DropSchema { name, .. } => {
            if admin {
                return Ok(());
            }
            if catalog.schema_owner(name).is_some_and(|o| o == user) {
                Ok(())
            } else {
                deny(user, "OWNERSHIP", &format!("SCHEMA {name}"))
            }
        }
        Statement::AlterTableRename { table, .. }
        | Statement::AlterTableRenameColumn { table, .. }
        | Statement::AlterTableAddColumn { table, .. }
        | Statement::AlterTableAddConstraint { table, .. }
        | Statement::AlterTableDropColumn { table, .. } => {
            require_owner(catalog, user, table, admin)
        }
        Statement::GrantRole { .. } | Statement::RevokeRole { .. } => {
            if admin || catalog.has_role_attribute(user, RoleAttribute::CreateRole) {
                Ok(())
            } else {
                deny(user, "CREATEROLE", "ROLE MEMBERSHIP")
            }
        }
        Statement::DropRole { name, .. } => {
            // PostgreSQL: CREATEROLE (or superuser) may drop roles, but only
            // a superuser may drop a superuser. Self-drop of the bootstrap
            // `plomid` role is refused at the catalog layer as well.
            if admin {
                return Ok(());
            }
            if catalog.has_role_attribute(name, RoleAttribute::Superuser) {
                return deny(user, "SUPERUSER", &format!("ROLE {name}"));
            }
            if catalog.has_role_attribute(user, RoleAttribute::CreateRole) {
                Ok(())
            } else {
                deny(user, "CREATEROLE", &format!("ROLE {name}"))
            }
        }
        Statement::GrantPrivileges { object, .. } | Statement::RevokePrivileges { object, .. } => {
            if admin {
                return Ok(());
            }
            match object {
                GrantObject::Table(table) => {
                    let resolved = resolve_table(catalog, table)?;
                    if catalog.table_owner(&resolved).is_some_and(|o| o == user) {
                        Ok(())
                    } else {
                        deny(user, "OWNERSHIP", &format!("TABLE {resolved}"))
                    }
                }
                GrantObject::Schema(schema) => {
                    if catalog.schema_owner(schema).is_some_and(|o| o == user) {
                        Ok(())
                    } else {
                        deny(user, "OWNERSHIP", &format!("SCHEMA {schema}"))
                    }
                }
                GrantObject::AllTablesInSchema(schema) => {
                    let owned = catalog
                        .tables()
                        .into_iter()
                        .filter(|table| {
                            table
                                .name
                                .split_once('.')
                                .is_some_and(|(owner, _)| owner == schema)
                        })
                        .all(|table| {
                            catalog
                                .table_owner(&table.name)
                                .is_some_and(|owner| owner == user)
                        });
                    if owned {
                        Ok(())
                    } else {
                        deny(user, "OWNERSHIP", &format!("ALL TABLES IN SCHEMA {schema}"))
                    }
                }
                // No database ownership in Beta 2: database grants are an
                // administrative operation.
                GrantObject::Database(_) => deny(user, "ADMIN", "DATABASE GRANT"),
            }
        }
        Statement::AlterRole {
            name,
            password,
            superuser,
            create_database,
            create_role,
            ..
        } => {
            if admin {
                return Ok(());
            }
            // Attribute changes (privilege escalation surface) are
            // superuser-only, enforced by the admin bypass above: any
            // non-superuser reaching here with attribute changes denies.
            if superuser.is_some() || create_database.is_some() || create_role.is_some() {
                return deny(user, "SUPERUSER", &format!("ROLE {name}"));
            }
            // Password rotation: self-service, or CREATEROLE holders.
            if password.is_some()
                && (name == user || catalog.has_role_attribute(user, RoleAttribute::CreateRole))
            {
                return Ok(());
            }
            deny(user, "ALTER", &format!("ROLE {name}"))
        }
        Statement::Vacuum { table } => {
            if admin {
                return Ok(());
            }
            match table {
                Some(name) => require_owner(catalog, user, name, false),
                // Bare VACUUM touches every table: administrative.
                None => deny(user, "VACUUM", "DATABASE"),
            }
        }
        Statement::Analyze { .. } => {
            // ANALYZE refreshes planner statistics without touching rows:
            // allowed for any authenticated role (statistics are metadata).
            Ok(())
        }
        Statement::Reindex => {
            if admin {
                Ok(())
            } else {
                deny(user, "REINDEX", "DATABASE")
            }
        }
        Statement::Use { database: target } => {
            // Canonicalize spelling (case-insensitive) so the CONNECT check
            // matches stored grants; unknown names pass through to
            // execution, which reports 3D000 (existence before privilege).
            let canonical = canonical_database(catalog, target);
            match canonical {
                Some(name) => {
                    if catalog.check_database_connect(user, &name) {
                        Ok(())
                    } else {
                        deny(user, "CONNECT", &format!("DATABASE {name}"))
                    }
                }
                None => Ok(()),
            }
        }
        // Session/transaction plumbing and no-op compatibility stubs carry
        // no privilege: they neither read user data nor change it.
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::Set { .. }
        | Statement::Show { .. }
        | Statement::Describe { .. }
        | Statement::Lock
        | Statement::CommentOn { .. }
        | Statement::RefreshMaterializedView { .. }
        | Statement::Do { .. } => Ok(()),
        // Everything not explicitly modeled above is administrative until
        // proven otherwise (fail closed): only superusers may run it.
        _ => {
            if admin {
                Ok(())
            } else {
                deny(user, "ADMIN", "STATEMENT")
            }
        }
    }
}

/// `CREATE <object>` gating (table/view handled by dedicated arms above).
fn authorize_create(
    catalog: &InMemoryCatalog,
    user: &str,
    database: &str,
    create: &plomid_sql::CreateStatement,
    admin: bool,
) -> SqlResult<()> {
    use plomid_sql::CreateStatement as C;
    if admin {
        return Ok(());
    }
    match create {
        C::Database { .. } => {
            if catalog.has_role_attribute(user, RoleAttribute::CreateDatabase) {
                Ok(())
            } else {
                deny(user, "CREATEDB", "DATABASE")
            }
        }
        C::Schema { name, .. } => {
            // Schema creation needs CREATE on the database (stored grant)
            // or the CREATEDB attribute. Ownership is recorded by DDL
            // execution on success.
            if catalog.check_database_create(user, database)
                || catalog.has_role_attribute(user, RoleAttribute::CreateDatabase)
            {
                Ok(())
            } else {
                deny(
                    user,
                    "CREATE",
                    &format!("DATABASE {database} for SCHEMA {name}"),
                )
            }
        }
        C::Role {
            superuser,
            create_database,
            create_role,
            ..
        } => {
            // Creating an elevated role is itself a superuser operation
            // (no creation-time self-escalation); plain roles need CREATEROLE.
            if *superuser || *create_database || *create_role {
                if catalog.is_superuser(user) {
                    Ok(())
                } else {
                    deny(user, "SUPERUSER", "ROLE CREATION")
                }
            } else if catalog.has_role_attribute(user, RoleAttribute::CreateRole) {
                Ok(())
            } else {
                deny(user, "CREATEROLE", "ROLE")
            }
        }
        // Table-valued creation flows through CreateTable/CreateView arms;
        // every other object class is administrative in Beta 2.
        _ => deny(user, "ADMIN", "CREATE OBJECT"),
    }
}

fn require_owner(catalog: &InMemoryCatalog, user: &str, table: &str, admin: bool) -> SqlResult<()> {
    if admin {
        return Ok(());
    }
    let resolved = resolve_table(catalog, table)?;
    if catalog.table_owner(&resolved).is_some_and(|o| o == user) {
        Ok(())
    } else {
        deny(user, "OWNERSHIP", &format!("TABLE {resolved}"))
    }
}

fn require_schema_create(catalog: &InMemoryCatalog, user: &str, schema: &str) -> SqlResult<()> {
    if catalog.check_schema_privilege(user, schema, SchemaPrivilege::Create) {
        Ok(())
    } else {
        deny(user, "CREATE", &format!("SCHEMA {schema}"))
    }
}

/// Schema a `CREATE TABLE/VIEW <name>` would land in (search_path rules),
/// without creating anything.
fn create_schema_of(catalog: &InMemoryCatalog, name: &str) -> SqlResult<String> {
    if let Some((schema, _)) = name.split_once('.') {
        if catalog.has_schema(schema) {
            return Ok(schema.to_string());
        }
        return Err(SqlError::Storage(plomid_core::PlomidError::new(
            plomid_core::ErrorKind::NotFound,
            format!("schema \"{schema}\" does not exist"),
        )));
    }
    Ok(catalog
        .search_path()
        .iter()
        .find(|s| !s.is_empty() && *s != "$user" && catalog.has_schema(s))
        .cloned()
        .unwrap_or_else(|| "public".to_string()))
}

/// True for relations that are server metadata rather than user data.
/// Readable by every authenticated role (information_schema-equivalent).
fn is_system_relation(catalog: &InMemoryCatalog, name: &str) -> bool {
    let resolved = catalog
        .resolve_table_name(name)
        .unwrap_or_else(|_| name.to_string());
    let lower = resolved.to_ascii_lowercase();
    lower.starts_with("pg_catalog.")
        || lower.starts_with("information_schema.")
        || lower == "pg_catalog"
        || lower == "information_schema"
        || matches!(
            lower.as_str(),
            _ if {
                let short = lower.split('.').next_back().unwrap_or(&lower);
                matches!(
                    short,
                    "plomid_stat_counters"
                        | "plomid_stat_analytical"
                        | "plomid_stat_tables"
                        | "plomid_htap_stats"
                        | "plomid_stat_security"
                        | "plomid_vector_stats"
                        | "plomid_vector_indexes"
                )
            }
        )
}
