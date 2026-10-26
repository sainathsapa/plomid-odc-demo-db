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
use crate::{
    value_pg_type, ColumnDef, ColumnType, Constraint, ConstraintKind, Expression, Statement, Value,
};
use plomid_core::{ColumnId, ErrorKind, IndexId, PlomidError, Result, SchemaId, TableId};
use plomid_security::RoleAttribute;

// Deterministic name->OID and qualification helpers live in the type registry.
pub use plomid_types::{bare_type_name, custom_type_oid};
pub mod encoding;
mod view_types;

pub const CATALOG_KEY: &str = "__plomid_catalog_meta";

/// Process-wide catalog ID allocators (§P1 concurrent-DDL ID race).
///
/// Session executors hold point-in-time `InMemoryCatalog` images: two
/// sessions that load the same generation start with identical `next_*`
/// counters, so plain `next_* += 1` mints duplicate `TableId`/`SchemaId`/
/// `ColumnId`/`IndexId` values. Allocation therefore goes through these
/// process-wide atomics (CAS-synced to each image's high-water before use),
/// so concurrent `CREATE TABLE/SCHEMA/INDEX` sessions in one process mint
/// distinct IDs even from the same base image. Durability comes from the
/// persisted high-water in the meta value (`encoding::ID_ALLOC_MAGIC`) plus
/// `max(id+1)` reconstruction on load; `sync_id_allocators` re-syncs the
/// atomics after every load.
static NEXT_SCHEMA_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(2);
static NEXT_TABLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static NEXT_COLUMN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
static NEXT_INDEX_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Serializes catalog save read-modify-write (§P1): concurrent savers from
/// the same base image must rebase against durable state atomically, or the
/// second meta rewrite loses the first's schemas/databases (last-writer-wins).
pub(crate) fn ddl_save_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// Process-wide gate making a table-name claim atomic with name resolution
/// (F-004: concurrent same-name `CREATE TABLE` used to double-win).
///
/// Sessions hold point-in-time catalog images: an in-memory existence check
/// cannot see a name another session claimed after this image was loaded, so
/// two concurrent `CREATE TABLE` statements both passed their checks, both
/// saved, and the second silently overwrote the first's shape while both
/// clients were told they succeeded. A creator acquires this gate, re-probes
/// the durable entry for the resolved name with [`Self::table_exists`], then
/// persists with [`Self::save`] while the gate is still held (the executor's
/// CREATE TABLE does exactly this). Creators serialize on the gate, so
/// exactly one observes "absent" and wins; every other creator observes the
/// winner and fails its statement before mutating any state.
///
/// The gate is the same lock [`save_catalog`] takes, so it also keeps
/// ordinary saves from overtaking the probe. It is NOT reentrant: never call
/// [`save_catalog`], a `DDL` path that saves, or acquire another
/// `CreateGate` while holding one.
pub struct CreateGate {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl CreateGate {
    /// Acquires the gate; blocks while another creator or save holds it.
    pub fn acquire() -> Self {
        let guard = ddl_save_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self { _guard: guard }
    }

    /// True when the durable catalog already holds a table entry for `name`.
    ///
    /// Authoritative for the critical section: no save can land between this
    /// answer and a paired [`Self::save`] unless the gate is dropped first.
    pub fn table_exists<E: plomid_txn::StorageEngine>(
        &self,
        engine: &mut E,
        name: &str,
    ) -> Result<bool> {
        encoding::table_entry_exists(engine, name)
    }

    /// Persists the catalog image while the gate is held (the locked form of
    /// [`save_catalog`]). Call before the gate is dropped so the claim made
    /// after [`Self::table_exists`] commits in the same critical section.
    pub fn save<E: plomid_txn::StorageEngine>(
        &self,
        catalog: &mut InMemoryCatalog,
        engine: &mut E,
    ) -> Result<()> {
        encoding::save_sharded_locked(catalog, engine)
    }
}

fn alloc_id(global: &std::sync::atomic::AtomicU64, local: &mut u64) -> u64 {
    use std::sync::atomic::Ordering;
    // Ensure the global is at least the image's high-water (handles images
    // loaded before another session bumped the global, and fresh images
    // after a restart whose globals were just re-synced from durable max).
    let mut cur = global.load(Ordering::SeqCst);
    loop {
        if cur >= *local {
            break;
        }
        match global.compare_exchange(cur, *local, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
    let id = global.fetch_add(1, Ordering::SeqCst);
    // `fetch_add` returns the pre-increment value, which is >= local by the
    // CAS above. Keep the image's high-water in lockstep for persistence.
    *local = (*local).max(id.saturating_add(1));
    id
}

/// Re-syncs process-wide allocators from an image's high-water (called after
/// every load and after ID persistence decode). Monotonic: never rewinds.
pub fn sync_id_allocators(catalog: &InMemoryCatalog) {
    use std::sync::atomic::Ordering;
    NEXT_SCHEMA_ID.fetch_max(catalog.next_schema_id, Ordering::SeqCst);
    NEXT_TABLE_ID.fetch_max(catalog.next_table_id, Ordering::SeqCst);
    NEXT_COLUMN_ID.fetch_max(catalog.next_column_id, Ordering::SeqCst);
    NEXT_INDEX_ID.fetch_max(catalog.next_index_id, Ordering::SeqCst);
}

pub trait Catalog {
    fn create_schema(&mut self, name: String) -> Result<()>;
    fn has_schema(&self, name: &str) -> bool;
    /// Session `search_path` used for unqualified name resolution. The default
    /// implementation is stateless (`public`) for catalogs without session
    /// state; `InMemoryCatalog` overrides it with per-session storage.
    /// Named `session_search_path` to avoid clashing with the inherent
    /// `InMemoryCatalog::search_path()` accessor.
    fn session_search_path(&self) -> Vec<String> {
        vec!["public".to_string()]
    }
    fn set_session_search_path(&mut self, _path: Vec<String>) {}
    fn schema_names(&self) -> Vec<String>;
    fn create_table(
        &mut self,
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
    ) -> Result<()>;
    fn set_constraints(&mut self, table: &str, constraints: Vec<Constraint>) -> Result<()>;
    fn constraints(&self, table: &str) -> Result<&[Constraint]>;
    fn create_sequence(&mut self, name: &str) -> Result<()>;
    fn has_sequence(&self, name: &str) -> bool;
    fn sequence_names(&self) -> Vec<String>;
    fn drop_sequence(&mut self, name: &str) -> Result<()>;
    // Index identity travels as one unit (name/table/columns/expression/
    // uniqueness/operator class); bundling would churn every catalog impl.
    #[allow(clippy::too_many_arguments)]
    fn create_index(
        &mut self,
        name: String,
        table: String,
        column: String,
        // Ordered indexed column list (`[column]` for single-column,
        // the full list for composite, empty for expression indexes).
        columns: Vec<String>,
        expression: Option<crate::Expression>,
        unique: bool,
        operator_class: Option<String>,
    ) -> Result<()>;
    /// Registers the internal backing index for a single-column `PRIMARY KEY`
    /// / `UNIQUE` constraint. Such an index enforces uniqueness but is not a
    /// user-created index object, so it is kept out of
    /// [`Catalog::indexes_for_table`].
    fn create_constraint_index(
        &mut self,
        name: String,
        table: String,
        column: String,
        unique: bool,
    ) -> Result<()>;
    /// Registers a composite (multi-column) `UNIQUE`/`PRIMARY KEY` backing
    /// index. A composite constraint is one tuple index, never one index per
    /// participating column.
    ///
    /// The default implementation refuses: only a catalog that can represent
    /// an ordered column list may back a composite constraint.
    fn create_composite_unique_index(
        &mut self,
        name: String,
        table: String,
        columns: Vec<String>,
    ) -> Result<()> {
        let _ = (name, table, columns);
        Err(PlomidError::new(
            ErrorKind::Unsupported,
            "this catalog cannot represent composite unique indexes",
        ))
    }
    fn drop_index(&mut self, name: &str) -> Result<IndexDefinition>;
    /// User-created indexes of `table`. Constraint backing indexes are not
    /// included; use [`Catalog::all_indexes_for_table`] for those.
    fn indexes_for_table(&self, table: &str) -> Vec<IndexDefinition>;
    /// Every index of `table`, including the internal indexes that back
    /// `PRIMARY KEY` / `UNIQUE` constraints.
    fn all_indexes_for_table(&self, table: &str) -> Vec<IndexDefinition>;
    fn get_table(&self, name: &str) -> Result<&TableSchema>;
    fn has_table(&self, name: &str) -> bool;
    fn table_names(&self) -> Vec<String>;
    fn drop_table(&mut self, name: &str) -> Result<TableSchema>;
    fn rename_table(&mut self, name: &str, new_name: String) -> Result<()>;
    fn rename_column(&mut self, table: &str, old_name: &str, new_name: String) -> Result<()>;
    fn add_column(&mut self, table: &str, column: ColumnDef) -> Result<()>;
    fn drop_column(&mut self, table: &str, column: &str) -> Result<ColumnId>;
    fn drop_schema(&mut self, name: &str) -> Result<()>;
    // Views.
    fn create_view(
        &mut self,
        name: String,
        columns: Vec<String>,
        query: Box<Statement>,
    ) -> Result<()>;
    fn drop_view(&mut self, name: &str) -> Result<()>;
    fn get_view(&self, name: &str) -> Option<&StoredView>;
    fn has_view(&self, name: &str) -> bool;
    fn view_names(&self) -> Vec<String>;
    // Types.
    fn create_type(&mut self, def: TypeDefinition) -> Result<()>;
    fn has_type(&self, name: &str) -> bool;
    fn drop_type(&mut self, name: &str) -> Result<()>;
    /// Names of all user-defined types stored in this catalog.
    fn type_names(&self) -> Vec<String>;
    /// Returns the stored definition for a user-defined type, tolerating
    /// schema qualification and case differences like `user_type_oid` does.
    fn get_type(&self, name: &str) -> Option<StoredType> {
        let _ = name;
        None
    }
    /// Names of all user-defined domains stored in this catalog.
    fn domain_names(&self) -> Vec<String>;
    /// Resolves a user-defined type or domain name to its synthetic type OID
    /// so column definitions can reference `CREATE TYPE`/`CREATE DOMAIN`
    /// objects, matching PostgreSQL's type resolution for columns.
    fn user_type_oid(&self, name: &str) -> Option<u32> {
        let _ = name;
        None
    }
    // Domains.
    fn create_domain(&mut self, def: DomainDefinition) -> Result<()>;
    fn has_domain(&self, name: &str) -> bool;
    fn drop_domain(&mut self, name: &str) -> Result<()>;
    // Functions.
    fn create_function(&mut self, def: FunctionDefinition) -> Result<()>;
    fn has_function(&self, name: &str, args: &[String]) -> bool;
    fn drop_function(&mut self, name: &str, args: &[String]) -> Result<()>;
    /// Finds a user-defined SQL function by (case-insensitive) name and
    /// argument count, used for call-site resolution.
    fn find_function_by_call(&self, name: &str, arg_count: usize) -> Option<StoredFunction> {
        let _ = (name, arg_count);
        None
    }
    // Dependency tracking.
    fn find_table_dependency(&self, name: &str) -> Option<String>;
    fn find_view_dependency(&self, name: &str) -> Option<String>;
    fn find_type_dependency(&self, name: &str) -> Option<String>;
    fn find_domain_dependency(&self, name: &str) -> Option<String>;
    fn find_function_dependency(&self, name: &str, args: &[String]) -> Option<String>;
    fn find_column_dependency(&self, table: &str, column: &str) -> Option<String>;
    fn view_depends_on_table(&self, view: &str, table: &str) -> bool;
    fn find_views_referencing_column(&self, table: &str, column: &str) -> Vec<String>;
}

/// Per-column constraint view used by the executor. The catalog stores the
/// authoritative table-level representation; this is only a derived view.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnRule {
    pub not_null: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub default_value: Option<Value>,
    /// Non-literal DEFAULT expression (e.g. `DEFAULT CURRENT_TIMESTAMP`).
    /// Evaluated per inserted row against the statement execution context.
    pub default_expr: Option<crate::Expression>,
    /// `GENERATED ALWAYS AS (expr) STORED` generation expression. Evaluated
    /// against the fully-built row on every INSERT/UPDATE, overriding any
    /// supplied value.
    pub generated_expr: Option<crate::Expression>,
    pub min_integer: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub table_id: TableId,
    pub column_ids: Vec<ColumnId>,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexDefinition {
    pub name: String,
    pub table: String,
    /// The index's first indexed column. Kept as the canonical single-column
    /// name so every single-column access path (`WHERE col = literal`
    /// discovery, statistics, system catalogs) stays unchanged. For a
    /// composite index this is `columns[0]`; empty for expression indexes.
    pub column: String,
    /// The ordered indexed column list. Exactly one entry for a single-column
    /// index, the constraint's column order for a composite
    /// `UNIQUE (a, b)` / `PRIMARY KEY (a, b)`, and empty for an expression
    /// index. The tuple is the conflict domain: `(a, b)` is unique as a
    /// whole, never `a` and `b` independently.
    pub columns: Vec<String>,
    /// For expression indexes, the index key expression as a parsed AST.
    /// `None` for plain column indexes.
    pub expression: Option<crate::Expression>,
    pub unique: bool,
    pub index_id: IndexId,
    /// PostgreSQL GIN operator class (e.g. `jsonb_path_ops`). This metadata
    /// is distinct from the indexed column/expression: it tells the index
    /// infrastructure how to build the key, not what to index.
    ///
    /// We preserve it as optional metadata rather than silently dropping it
    /// so that future GIN-aware planning can honor the operator class.
    /// For now, PLOMID treats all indexes as B-tree-style unless the
    /// executor explicitly supports GIN.
    pub operator_class: Option<String>,
    /// True when this index exists only to enforce a `PRIMARY KEY` / `UNIQUE`
    /// constraint (`register_constraint_indexes`). It is the durable
    /// uniqueness authority, but it is *not* a user-created index object: the
    /// planner, statistics, derived ART, and columnar index generations only
    /// consider user indexes, and the system catalog presents the constraint's
    /// index from the table's constraint metadata instead.
    pub constraint: bool,
}

/// A stored view: its optional column alias list and defining query.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredView {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<Statement>,
    /// Canonical SQL used to restore the defining query after restart.
    pub definition: String,
    /// Resolved output type of each SELECT target, aligned with `columns`.
    ///
    /// Computed once at `CREATE VIEW` time from the defining query so that
    /// `information_schema.columns` / `pg_attribute` report the real view
    /// column types (not `TEXT`). Entries may be `None` when the type could
    /// not be statically resolved; consumers must fall back to `TEXT`.
    /// Empty for views persisted before this metadata existed (or when the
    /// definition has more/fewer columns than types due to `SELECT *`).
    pub column_types: Vec<Option<ColumnType>>,
}

impl StoredView {
    /// Resolved output type for the view column at `index`, if known.
    #[must_use]
    pub fn column_type(&self, index: usize) -> Option<ColumnType> {
        self.column_types.get(index).copied().flatten()
    }
}

/// Bundled RBAC maps for the executor's statement privilege snapshot:
/// table owners, table grants, schema owners, schema grants, withdrawn
/// database CONNECT entries, database grants.
pub type AuthSnapshotMaps = (
    std::collections::HashMap<String, String>,
    std::collections::HashMap<(String, String), u16>,
    std::collections::HashMap<String, String>,
    std::collections::HashMap<(String, String), u16>,
    std::collections::BTreeSet<(String, String)>,
    std::collections::HashMap<(String, String), u16>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleDefinition {
    pub name: String,
    pub superuser: bool,
    pub inherit: bool,
    pub create_role: bool,
    pub create_database: bool,
    pub can_login: bool,
    pub replication: bool,
    pub bypass_rls: bool,
    pub connection_limit: i32,
    pub password: Option<String>,
    pub members: Vec<String>,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!(
                        "column \"{name}\" does not exist in table \"{}\"",
                        self.name
                    ),
                    format!("table={} column={name}", self.name),
                )
            })
    }

    /// True when the column at `index` may not hold NULL.
    pub fn is_not_null(&self, index: usize) -> bool {
        self.constraints
            .iter()
            .chain(
                self.columns
                    .iter()
                    .flat_map(|column| column.constraints.iter()),
            )
            .any(|constraint| {
                matches!(
                    constraint.kind,
                    ConstraintKind::NotNull | ConstraintKind::PrimaryKey
                ) && constraint.columns.contains(&self.columns[index].name)
            })
    }

    /// True when the column at `index` participates in a UNIQUE or PRIMARY KEY
    /// constraint (either column-level or table-level).
    pub fn column_is_unique(&self, index: usize) -> bool {
        self.constraints
            .iter()
            .chain(
                self.columns
                    .iter()
                    .flat_map(|column| column.constraints.iter()),
            )
            .any(|constraint| {
                // Only a *single-column* UNIQUE/PK makes its column
                // individually unique. A composite constraint's conflict
                // domain is the whole tuple, so none of its columns is
                // individually unique here.
                matches!(
                    constraint.kind,
                    ConstraintKind::Unique | ConstraintKind::PrimaryKey
                ) && constraint.columns == [self.columns[index].name.clone()]
            })
    }

    /// The CHECK constraints that apply to this table.
    pub fn check_constraints(&self) -> Vec<&Constraint> {
        self.constraints
            .iter()
            .filter(|constraint| constraint.kind == ConstraintKind::Check)
            .collect()
    }

    /// The FOREIGN KEY constraints declared on this table.
    pub fn foreign_keys(&self) -> Vec<&Constraint> {
        self.constraints
            .iter()
            .filter(|c| matches!(c.kind, ConstraintKind::ForeignKey { .. }))
            .collect()
    }

    /// The DEFAULT expression for the column at `index`, if declared.
    pub fn default_expr(&self, index: usize) -> Option<&crate::Expression> {
        self.constraints
            .iter()
            .chain(
                self.columns
                    .iter()
                    .flat_map(|column| column.constraints.iter()),
            )
            .find_map(|constraint| {
                if constraint.kind == ConstraintKind::Default
                    && constraint.columns.contains(&self.columns[index].name)
                {
                    constraint.expr.as_ref()
                } else {
                    None
                }
            })
    }

    /// Returns the tuples of column indexes that form each UNIQUE/PRIMARY KEY
    /// constraint.
    pub fn unique_column_sets(&self) -> Vec<Vec<usize>> {
        self.constraints
            .iter()
            .filter(|constraint| {
                matches!(
                    constraint.kind,
                    ConstraintKind::Unique | ConstraintKind::PrimaryKey
                )
            })
            .map(|constraint| {
                constraint
                    .columns
                    .iter()
                    .filter_map(|name| self.columns.iter().position(|col| col.name == *name))
                    .collect()
            })
            .filter(|set: &Vec<usize>| !set.is_empty())
            .collect()
    }

    pub fn validate_row(&self, values: &[crate::Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!(
                    "column count mismatch: expected {}, got {}",
                    self.columns.len(),
                    values.len()
                ),
            ));
        }
        for (col, value) in self.columns.iter().zip(values.iter()) {
            if !value_matches_column(&col.col_type, value) {
                let expected = col
                    .col_type
                    .pg_type()
                    .map(|ty| ty.name().to_string())
                    .unwrap_or_else(|| format!("oid {}", col.col_type.type_oid));
                return Err(PlomidError::with_detail(
                    ErrorKind::InvalidArgument,
                    format!(
                        "column \"{}\" expects {} but received {}",
                        col.name,
                        expected,
                        plomid_types::format_text(value),
                    ),
                    format!(
                        "table={} column={} expected_type={} actual_type={}",
                        self.name,
                        col.name,
                        expected,
                        crate::value_pg_type(value)
                            .map(|ty| ty.name().to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// A stored user-defined type (e.g. enum, composite).
#[derive(Debug, Clone, PartialEq)]
pub struct StoredType {
    pub name: String,
    pub labels: Vec<String>,
    /// Composite type attributes: [(column_name, type_name), ...].
    /// Empty for enums; for composites, the list of (column_name, type_name) pairs.
    pub attributes: Vec<(String, String)>,
}

/// A stored domain definition.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredDomain {
    pub name: String,
    pub base_type: String,
    pub constraints: Vec<StoredDomainConstraint>,
}

/// A domain constraint.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredDomainConstraint {
    pub name: Option<String>,
    pub check: String,
}

/// A stored function definition.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredFunction {
    pub name: String,
    pub args: Vec<StoredFunctionArg>,
    pub returns: String,
    pub language: String,
    pub body: String,
}

/// A function argument.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredFunctionArg {
    pub name: String,
    pub data_type: String,
}

/// Public type definition for `create_type`.
#[derive(Debug, Clone, PartialEq)]
pub struct TypeDefinition {
    pub name: String,
    pub labels: Vec<String>,
    /// Composite type attributes: [(column_name, type_name), ...].
    /// Empty for enums; `Some([])` for composites without columns.
    pub attributes: Vec<(String, String)>,
}

/// Public domain definition for `create_domain`.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainDefinition {
    pub name: String,
    pub base_type: String,
    pub constraints: Vec<DomainConstraintDefinition>,
}

/// Public domain constraint definition.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainConstraintDefinition {
    pub name: Option<String>,
    pub check: String,
}

/// Public function definition for `create_function`.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionDefinition {
    pub name: String,
    pub args: Vec<FunctionArgDefinition>,
    pub returns: String,
    pub language: String,
    pub body: String,
}

/// Public function argument definition.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionArgDefinition {
    pub name: String,
    pub data_type: String,
}

/// One committed-write candidate for the columnar delta store.
///
/// DML stages these in statement order while a write transaction runs; the
/// executor confirms them into `plomid_columnar::delta` only after the commit
/// succeeds (stamped with the post-commit watermark), or discards them when
/// the statement fails. Staging is session-statement-local: the catalog is
/// never shared across sessions, so staged rows can never leak into another
/// session's confirmation. Values are SQL-native; field conversion happens at
/// confirmation time.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedDelta {
    /// Qualified table name (`"public.t"`), resolved to an object at confirm.
    pub table: String,
    /// Storage row id (`{table}:{row_id}` key suffix); negative for markers.
    pub row_id: i64,
    /// Post-image in schema column order (empty for marker/tombstone rows).
    pub values: Vec<Value>,
    /// Pre-image in schema column order, when the writer observed one
    /// (updates and deletes; `None` for inserts and markers).
    pub prev: Option<Vec<Value>>,
    /// True for a delete tombstone (matched by pre-image at merge time).
    pub deleted: bool,
    /// True for a whole-table marker (`TRUNCATE`): confirmation clears the
    /// table's deltas instead of inserting a row.
    pub truncate: bool,
}

impl StagedDelta {
    /// Stages an upsert (insert or update post-image) for `row_id`.
    #[must_use]
    pub fn upsert(table: &str, row_id: i64, values: &[Value], prev: Option<&[Value]>) -> Self {
        Self {
            table: table.to_string(),
            row_id,
            values: values.to_vec(),
            prev: prev.map(<[Value]>::to_vec),
            deleted: false,
            truncate: false,
        }
    }

    /// Stages a delete tombstone for `row_id` with its pre-image.
    #[must_use]
    pub fn delete(table: &str, row_id: i64, prev: &[Value]) -> Self {
        Self {
            table: table.to_string(),
            row_id,
            values: Vec::new(),
            prev: Some(prev.to_vec()),
            deleted: true,
            truncate: false,
        }
    }

    /// Stages a whole-table clear marker (`TRUNCATE`).
    #[must_use]
    pub fn truncate(table: &str) -> Self {
        Self {
            table: table.to_string(),
            row_id: -1,
            values: Vec::new(),
            prev: None,
            deleted: false,
            truncate: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct InMemoryCatalog {
    pub(crate) tables: std::collections::HashMap<String, TableSchema>,
    pub(crate) schemas: std::collections::HashMap<String, SchemaId>,
    pub(crate) next_schema_id: u64,
    pub(crate) next_table_id: u64,
    pub(crate) next_column_id: u64,
    pub(crate) sequences: std::collections::BTreeMap<String, u64>,
    pub(crate) indexes: std::collections::HashMap<String, IndexDefinition>,
    pub(crate) next_index_id: u64,
    pub(crate) views: std::collections::HashMap<String, StoredView>,
    pub(crate) roles: std::collections::BTreeMap<String, RoleDefinition>,
    pub(crate) database_names: Vec<String>,
    /// Table ownership: resolved table name → owning role. The owner holds
    /// every table privilege implicitly (never stored as grants).
    pub(crate) table_owners: std::collections::HashMap<String, String>,
    /// Explicit table grants: (grantee role, resolved table) → privilege bits
    /// (`plomid-security` taxonomy). No entry means no grant (fail closed).
    pub(crate) table_grants: std::collections::HashMap<(String, String), u16>,
    /// Schema ownership: schema name → owning role.
    pub(crate) schema_owners: std::collections::HashMap<String, String>,
    /// Explicit schema grants: (grantee role, schema) → privilege bits.
    /// `public` USAGE is default-allow and is NOT stored here.
    pub(crate) schema_grants: std::collections::HashMap<(String, String), u16>,
    /// Explicit database grants: (grantee role, database) → privilege bits
    /// (`CREATE`; `CONNECT` stays default-allow via `db_connect_denied`).
    pub(crate) db_grants: std::collections::HashMap<(String, String), u16>,
    /// Explicit CONNECT withdrawals: (grantee role, database). CONNECT is
    /// default-allow (PostgreSQL `GRANT CONNECT TO PUBLIC` default); an
    /// entry here revokes it for that role.
    pub(crate) db_connect_denied: std::collections::BTreeSet<(String, String)>,
    /// Password verifiers: role → `iterations:salt_b64:verifier_b64`
    /// (`plomid-security::RoleCredential` encoding). Passwords themselves
    /// never persist. Replaces the legacy plaintext `RoleDefinition.password`
    /// (kept only as a read-migration source; never written by new code).
    pub(crate) role_credentials: std::collections::HashMap<String, String>,
    /// Connection-local relation lookup path. This is deliberately not
    /// persisted: the catalog is shared database metadata, while search_path
    /// belongs to the PostgreSQL session.
    pub(crate) search_path: Vec<String>,
    pub(crate) types: std::collections::HashMap<String, StoredType>,
    pub(crate) domains: std::collections::HashMap<String, StoredDomain>,
    pub(crate) functions: std::collections::HashMap<String, StoredFunction>,
    /// Objects changed since load (or since process start for fresh
    /// catalogs). Persistence writes ONLY these entries: sessions hold
    /// point-in-time images, so a full rewrite would delete objects created
    /// by other sessions after this image was loaded. The entry is decided
    /// at save time — present in the maps means write it, absent means
    /// delete its keys — so drops and creates share one mechanism.
    dirty_tables: std::collections::BTreeSet<String>,
    dirty_indexes: std::collections::BTreeSet<String>,
    dirty_views: std::collections::BTreeSet<String>,
    dirty_types: std::collections::BTreeSet<String>,
    dirty_domains: std::collections::BTreeSet<String>,
    dirty_functions: std::collections::BTreeSet<String>,
    dirty_sequences: std::collections::BTreeSet<String>,
    /// Schemas, roles, and database names live in the shared meta value.
    dirty_meta: bool,
    /// Explicitly dropped meta names (§P1 merge): `merge_durable_meta` unions
    /// durable schemas/roles/databases missing locally (concurrent creates),
    /// but must NOT resurrect names this image explicitly dropped (including
    /// its own single-session drops, where durable still holds the old row
    /// until this save commits). Populated by `drop_schema`/`drop_role` and
    /// database-name removal; cleared by the corresponding `create_*` and by
    /// `clear_dirty` after a successful save/load.
    dropped_schemas: std::collections::BTreeSet<String>,
    dropped_roles: std::collections::BTreeSet<String>,
    dropped_databases: std::collections::BTreeSet<String>,
    /// Durable catalog generation: a unique token per persisted change,
    /// stored in the meta value's trailing section. Live sessions compare it
    /// against their image to notice commits from other sessions (P1-003)
    /// without rescanning. Uniqueness (not monotonicity) is the contract:
    /// every persisted delta mints a fresh process-unique value, so even
    /// simultaneous writers from the same base image land distinct
    /// detectors and no change goes unnoticed.
    pub(crate) generation: u64,
    /// Unconfirmed columnar-delta candidates staged by the current
    /// statement's DML writes. Runtime-only: never persisted, never loaded,
    /// never cloned with content (a clone starts unstaged; staging belongs
    /// to the statement running on the original). Guarded by a mutex so
    /// staging works through shared catalog references without signature
    /// churn; lock poisoning fails open (a skipped stage only costs delta
    /// acceleration — the row path stays authoritative).
    staged_deltas: std::sync::Mutex<Vec<StagedDelta>>,
}

/// Session-local catalog images share metadata but never share unconfirmed
/// delta staging: a clone starts with an empty staging buffer, because
/// staged rows belong to the statement running on the original catalog.
impl Clone for InMemoryCatalog {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            schemas: self.schemas.clone(),
            next_schema_id: self.next_schema_id,
            next_table_id: self.next_table_id,
            next_column_id: self.next_column_id,
            sequences: self.sequences.clone(),
            indexes: self.indexes.clone(),
            next_index_id: self.next_index_id,
            views: self.views.clone(),
            roles: self.roles.clone(),
            database_names: self.database_names.clone(),
            table_owners: self.table_owners.clone(),
            table_grants: self.table_grants.clone(),
            schema_owners: self.schema_owners.clone(),
            schema_grants: self.schema_grants.clone(),
            db_connect_denied: self.db_connect_denied.clone(),
            db_grants: self.db_grants.clone(),
            role_credentials: self.role_credentials.clone(),
            search_path: self.search_path.clone(),
            types: self.types.clone(),
            domains: self.domains.clone(),
            functions: self.functions.clone(),
            dirty_tables: self.dirty_tables.clone(),
            dirty_indexes: self.dirty_indexes.clone(),
            dirty_views: self.dirty_views.clone(),
            dirty_types: self.dirty_types.clone(),
            dirty_domains: self.dirty_domains.clone(),
            dirty_functions: self.dirty_functions.clone(),
            dirty_sequences: self.dirty_sequences.clone(),
            dirty_meta: self.dirty_meta,
            dropped_schemas: self.dropped_schemas.clone(),
            dropped_roles: self.dropped_roles.clone(),
            dropped_databases: self.dropped_databases.clone(),
            generation: self.generation,
            staged_deltas: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        let mut catalog = Self::default();
        catalog
            .schemas
            .insert("public".to_string(), SchemaId::new(1));
        catalog.next_schema_id = 2;
        catalog.next_table_id = 1;
        catalog.next_column_id = 1;
        catalog.next_index_id = 1;
        catalog.views = std::collections::HashMap::new();
        catalog.search_path = vec!["public".to_string()];
        catalog.ensure_superuser("plomid");
        catalog.database_names.push("plomid".to_string());
        // A fresh catalog has no durable image at all: everything must be
        // written by the first save. (Loads clear the set below.)
        catalog.mark_all_dirty();
        catalog
    }

    /// Marks one object for persistence on the next save.
    pub(crate) fn mark_table_dirty(&mut self, name: &str) {
        self.dirty_tables.insert(name.to_string());
    }

    /// Marks one index for persistence on the next save.
    pub(crate) fn mark_index_dirty(&mut self, name: &str) {
        self.dirty_indexes.insert(name.to_string());
    }

    /// Marks one view for persistence on the next save.
    pub(crate) fn mark_view_dirty(&mut self, name: &str) {
        self.dirty_views.insert(name.to_string());
    }

    /// Marks one type for persistence on the next save.
    pub(crate) fn mark_type_dirty(&mut self, name: &str) {
        self.dirty_types.insert(name.to_string());
    }

    /// Marks one domain for persistence on the next save.
    pub(crate) fn mark_domain_dirty(&mut self, name: &str) {
        self.dirty_domains.insert(name.to_string());
    }

    /// Marks one function for persistence on the next save.
    pub(crate) fn mark_function_dirty(&mut self, key: &str) {
        self.dirty_functions.insert(key.to_string());
    }

    /// Marks one sequence for persistence on the next save.
    pub(crate) fn mark_sequence_dirty(&mut self, name: &str) {
        self.dirty_sequences.insert(name.to_string());
    }

    /// Marks the shared meta value (schemas, roles, database names).
    pub(crate) fn mark_meta_dirty(&mut self) {
        self.dirty_meta = true;
    }

    /// Marks the whole image dirty (fresh catalogs, legacy migration).
    pub(crate) fn mark_all_dirty(&mut self) {
        self.dirty_tables.extend(self.tables.keys().cloned());
        self.dirty_indexes.extend(self.indexes.keys().cloned());
        self.dirty_views.extend(self.views.keys().cloned());
        self.dirty_types.extend(self.types.keys().cloned());
        self.dirty_domains.extend(self.domains.keys().cloned());
        self.dirty_functions.extend(self.functions.keys().cloned());
        self.dirty_sequences.extend(self.sequences.keys().cloned());
        self.dirty_meta = true;
    }

    /// Clears persistence marks (after a successful save or a fresh load).
    /// Public so session-transaction commit can clear the marks after
    /// staging the delta into its own transaction (see
    /// [`staged_catalog_delta`]).
    pub fn clear_dirty(&mut self) {
        self.dirty_tables.clear();
        self.dirty_indexes.clear();
        self.dirty_views.clear();
        self.dirty_types.clear();
        self.dirty_domains.clear();
        self.dirty_functions.clear();
        self.dirty_sequences.clear();
        self.dirty_meta = false;
        self.dropped_schemas.clear();
        self.dropped_roles.clear();
        self.dropped_databases.clear();
    }

    /// Durable generation of this image (see the `generation` field). Live
    /// sessions compare it against the stored generation to notice other
    /// sessions' commits.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// True when this image holds uncommitted local mutations. A dirty image
    /// must never be replaced by a reload: the marks are the only record of
    /// what still needs persisting.
    pub fn has_pending_marks(&self) -> bool {
        self.dirty_meta
            || !self.dirty_tables.is_empty()
            || !self.dirty_indexes.is_empty()
            || !self.dirty_views.is_empty()
            || !self.dirty_types.is_empty()
            || !self.dirty_domains.is_empty()
            || !self.dirty_functions.is_empty()
            || !self.dirty_sequences.is_empty()
    }

    /// Stages one columnar-delta candidate from a DML row write.
    ///
    /// Infallible and callable through a shared reference: staging must
    /// never fail a write (a skipped stage only costs delta acceleration).
    /// Lock poisoning drops the stage rather than the statement.
    pub fn stage_delta(&self, delta: StagedDelta) {
        if let Ok(mut staged) = self.staged_deltas.lock() {
            staged.push(delta);
        }
    }

    /// Drains staged delta candidates for post-commit confirmation.
    ///
    /// The executor calls this exactly once per committed statement and
    /// confirms each entry into the columnar delta store. Lock poisoning
    /// yields whatever was staged (fail-open: confirmation re-resolves
    /// every entry and skips the unresolvable).
    pub fn take_staged_deltas(&self) -> Vec<StagedDelta> {
        self.staged_deltas
            .lock()
            .map(|mut staged| std::mem::take(&mut *staged))
            .unwrap_or_default()
    }

    /// Discards staged delta candidates (statement failure or rollback).
    ///
    /// Staged rows were never confirmed, so dropping them is exact: no
    /// delta outlives the transaction that staged it.
    pub fn discard_staged_deltas(&self) {
        if let Ok(mut staged) = self.staged_deltas.lock() {
            staged.clear();
        }
    }

    pub fn set_database_names(&mut self, names: Vec<String>) {
        // Mark only on actual change: the wire layer re-syncs the registry
        // list on every connection, and an unconditional mark would leave
        // every virgin session dirty (disabling live-session refresh and
        // letting a later save persist a stale registry view).
        if self.database_names != names {
            // Track explicitly removed databases so the save-time meta merge
            // (§P1) does not resurrect them from durable state.
            for removed in self
                .database_names
                .iter()
                .filter(|old| !names.iter().any(|n| n.eq_ignore_ascii_case(old)))
            {
                self.dropped_databases.insert(removed.clone());
            }
            // Re-created names clear their tombstone.
            for added in &names {
                self.dropped_databases
                    .retain(|d| !d.eq_ignore_ascii_case(added));
            }
            self.database_names = names;
            self.mark_meta_dirty();
        }
    }

    pub fn database_names(&self) -> &[String] {
        &self.database_names
    }

    pub fn set_search_path(&mut self, path: Vec<String>) {
        self.search_path = if path.is_empty() {
            vec!["public".to_string()]
        } else {
            path
        };
    }

    pub fn search_path(&self) -> &[String] {
        &self.search_path
    }

    fn relation_candidates(&self, name: &str) -> Vec<String> {
        if name.contains('.') {
            return vec![name.to_string()];
        }
        self.search_path
            .iter()
            .filter(|schema| !schema.is_empty() && *schema != "$user")
            .map(|schema| format!("{schema}.{name}"))
            .collect()
    }

    /// Resolves a relation reference (qualified or unqualified) to its stored
    /// catalog identity, honoring the session `search_path` for unqualified
    /// names. Errors when the table does not exist.
    pub fn resolve_table_name(&self, name: &str) -> Result<String> {
        self.get_table(name).map(|schema| schema.name.clone())
    }

    /// Resolves a relation reference for lookup/mutation. Unlike
    /// [`resolve_table_name`], existence is not required: the resolved key is
    /// returned even when the target does not exist yet (e.g. to build the
    /// namespace for a `RENAME`). Unqualified names are mapped through the
    /// first existing schema in the search path.
    pub fn resolve_table_namespace(&self, name: &str) -> String {
        if name.contains('.') {
            let (schema, rest) = name.split_once('.').unwrap_or(("public", name));
            if self.schemas.contains_key(schema) {
                return name.to_string();
            }
            return format!("{schema}.{rest}");
        }
        match self.resolve_create_schema() {
            Some(schema) => format!("{schema}.{name}"),
            None => name.to_string(),
        }
    }

    /// Resolves the schema in which an unqualified `CREATE TABLE`/relation
    /// should be placed. PostgreSQL creates the object in the first existing
    /// schema of the session `search_path`; it never auto-creates a schema.
    fn resolve_create_schema(&self) -> Option<String> {
        self.search_path
            .iter()
            .find(|schema| {
                !schema.is_empty() && *schema != "$user" && self.schemas.contains_key(*schema)
            })
            .cloned()
    }

    /// Qualifies a relation name for creation. Explicitly schema-qualified
    /// names are validated and returned unchanged; unqualified names are
    /// prefixed with the first existing schema in the session search path
    /// (PostgreSQL `CREATE TABLE` semantics).
    pub fn resolve_create_name(&self, name: &str) -> Result<String> {
        if name.contains('.') {
            let schema = name.split('.').next().unwrap_or("");
            if !self.schemas.contains_key(schema) {
                return Err(PlomidError::new(
                    ErrorKind::NotFound,
                    format!("schema \"{schema}\" does not exist"),
                ));
            }
            return Ok(name.to_string());
        }
        match self.resolve_create_schema() {
            Some(schema) => Ok(format!("{schema}.{name}")),
            None => Err(PlomidError::new(
                ErrorKind::NotFound,
                "no schema has been selected to create in",
            )),
        }
    }

    pub fn roles(&self) -> Vec<&RoleDefinition> {
        self.roles.values().collect()
    }

    /// True when the role exists (includes `PUBLIC`-addressed checks by the
    /// caller: `PUBLIC` is a pseudo-role, never a row).
    pub fn has_role(&self, name: &str) -> bool {
        self.roles.contains_key(name)
    }

    /// Registers a role that authenticated externally (trust mode) or that
    /// needs a placeholder row. Least privilege by construction: a
    /// first-seen role can log in but holds NO privileges until granted.
    /// Superuser bootstrapping uses [`Self::ensure_superuser`]; the shared
    /// `plomid` seed in [`Self::new`] is the only other superuser source.
    pub fn ensure_role(&mut self, name: &str) {
        if self.roles.contains_key(name) {
            return;
        }
        self.roles
            .entry(name.to_string())
            .or_insert_with(|| RoleDefinition {
                name: name.to_string(),
                superuser: false,
                inherit: true,
                create_role: false,
                create_database: false,
                can_login: true,
                replication: false,
                bypass_rls: false,
                connection_limit: -1,
                password: None,
                members: Vec::new(),
            });
        self.mark_meta_dirty();
    }

    /// Registers the bootstrap superuser (first-run credential binding).
    ///
    /// Upgrades an existing placeholder row (auto-created least-privilege
    /// by `ensure_role`) to full superuser; idempotent afterwards. This,
    /// the catalog seed, and `ALTER ROLE ... SUPERUSER` by an existing
    /// superuser are the only superuser-creating paths: privilege can
    /// never self-escalate from below (attribute changes are
    /// superuser-gated at execution).
    pub fn ensure_superuser(&mut self, name: &str) {
        if let Some(role) = self.roles.get_mut(name) {
            role.superuser = true;
            role.inherit = true;
            role.create_role = true;
            role.create_database = true;
            role.can_login = true;
            role.bypass_rls = true;
            self.mark_meta_dirty();
            return;
        }
        self.roles.insert(
            name.to_string(),
            RoleDefinition {
                name: name.to_string(),
                superuser: true,
                inherit: true,
                create_role: true,
                create_database: true,
                can_login: true,
                replication: false,
                bypass_rls: true,
                connection_limit: -1,
                password: None,
                members: Vec::new(),
            },
        );
        self.mark_meta_dirty();
    }

    pub fn create_role(&mut self, role: RoleDefinition) -> Result<()> {
        if self.roles.contains_key(&role.name) {
            return Err(PlomidError::new(
                ErrorKind::AlreadyExists,
                format!("role \"{}\" already exists", role.name),
            ));
        }
        self.dropped_roles.remove(&role.name);
        self.roles.insert(role.name.clone(), role);
        self.mark_meta_dirty();
        Ok(())
    }

    /// Drops a role (§P2 DROP ROLE).
    ///
    /// PostgreSQL semantics: the role must exist (unless `if_exists`), must
    /// not own objects, and must not hold privileges. `REASSIGN OWNED` /
    /// `DROP OWNED` are not supported — ownership must be transferred or
    /// objects dropped first (documented in `compatibility.mdx`). On success
    /// the role is removed plus its credential, membership edges (both as
    /// member and as granted role), and any grant rows addressing it.
    pub fn drop_role(&mut self, name: &str) -> Result<()> {
        if !self.roles.contains_key(name) {
            return Err(PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{name}\" does not exist"),
            ));
        }
        // Bootstrap superuser is never droppable (protects single-user
        // recovery; PostgreSQL similarly protects the bootstrap superuser).
        if name == "plomid" {
            return Err(PlomidError::new(
                ErrorKind::PermissionDenied,
                "role \"plomid\" cannot be dropped".to_string(),
            ));
        }
        // Ownership check: tables, schemas, and database grants.
        if self.table_owners.values().any(|o| o == name) {
            return Err(PlomidError::with_detail(
                ErrorKind::PermissionDenied,
                format!("role \"{name}\" cannot be dropped because it owns objects"),
                "hint=reassign owned objects or drop them first".to_string(),
            ));
        }
        if self.schema_owners.values().any(|o| o == name) {
            return Err(PlomidError::with_detail(
                ErrorKind::PermissionDenied,
                format!("role \"{name}\" cannot be dropped because it owns objects"),
                "hint=reassign owned objects or drop them first".to_string(),
            ));
        }
        let holds_grants = self.table_grants.keys().any(|(grantee, _)| grantee == name)
            || self
                .schema_grants
                .keys()
                .any(|(grantee, _)| grantee == name)
            || self.db_grants.keys().any(|(grantee, _)| grantee == name)
            || self
                .db_connect_denied
                .iter()
                .any(|(grantee, _)| grantee == name);
        if holds_grants {
            return Err(PlomidError::with_detail(
                ErrorKind::PermissionDenied,
                format!("role \"{name}\" cannot be dropped because privileges depend on it"),
                "hint=revoke privileges first".to_string(),
            ));
        }
        self.roles.remove(name);
        self.role_credentials.remove(name);
        // Remove membership edges in both directions.
        for role in self.roles.values_mut() {
            role.members.retain(|m| m != name);
        }
        // §P1: tombstone so the save-time meta merge does not resurrect it.
        self.dropped_roles.insert(name.to_string());
        self.mark_meta_dirty();
        Ok(())
    }

    pub fn grant_role(&mut self, role: &str, member: &str) -> Result<()> {
        if !self.roles.contains_key(role) {
            return Err(PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{role}\" does not exist"),
            ));
        }
        let member_role = self.roles.get_mut(member).ok_or_else(|| {
            PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{member}\" does not exist"),
            )
        })?;
        if !member_role.members.iter().any(|name| name == role) {
            member_role.members.push(role.to_string());
            self.mark_meta_dirty();
        }
        Ok(())
    }

    /// Withdraws role `role` from `member`. Returns true when membership
    /// changed. Unknown roles error (fail closed, like the grant path).
    pub fn revoke_role(&mut self, role: &str, member: &str) -> Result<bool> {
        if !self.roles.contains_key(role) {
            return Err(PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{role}\" does not exist"),
            ));
        }
        let member_role = self.roles.get_mut(member).ok_or_else(|| {
            PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{member}\" does not exist"),
            )
        })?;
        let before = member_role.members.len();
        member_role.members.retain(|name| name != role);
        if member_role.members.len() != before {
            self.mark_meta_dirty();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Sets login/attribute flags on `role`. Each `None` leaves the flag
    /// untouched. Attribute escalation is gated at execution (superuser
    /// only); this only writes.
    pub fn alter_role_flags(
        &mut self,
        name: &str,
        login: Option<bool>,
        superuser: Option<bool>,
        create_database: Option<bool>,
        create_role: Option<bool>,
    ) -> Result<()> {
        let role = self.roles.get_mut(name).ok_or_else(|| {
            PlomidError::new(
                ErrorKind::NotFound,
                format!("role \"{name}\" does not exist"),
            )
        })?;
        if let Some(login) = login {
            role.can_login = login;
        }
        if let Some(superuser) = superuser {
            role.superuser = superuser;
        }
        if let Some(create_database) = create_database {
            role.create_database = create_database;
        }
        if let Some(create_role) = create_role {
            role.create_role = create_role;
        }
        self.mark_meta_dirty();
        Ok(())
    }

    /// Bundled privilege maps for the executor's per-statement
    /// [`AuthSnapshot`](https://docs.rs/plomid-executor): ownership and
    /// grant tables cloned for expression-level `has_*` answers. The maps
    /// are small (one entry per owned/granted object); the clone cost is
    /// bounded and keeps scalar evaluation catalog-free.
    pub fn auth_snapshot_maps(&self) -> AuthSnapshotMaps {
        (
            self.table_owners.clone(),
            self.table_grants.clone(),
            self.schema_owners.clone(),
            self.schema_grants.clone(),
            self.db_connect_denied.clone(),
            self.db_grants.clone(),
        )
    }

    pub fn schema_id(&self, name: &str) -> Option<SchemaId> {
        self.schemas.get(name).copied()
    }

    // ---- RBAC: ownership, grants, and privilege checks (Beta 2 Phase 2) ----
    //
    // Fail-closed throughout: unknown roles/tables hold nothing, missing
    // entries mean no grant, and only the owner's own row confers
    // superuser-class attributes (attributes never flow through membership;
    // only object privileges do — PostgreSQL semantics).
    //
    // `PUBLIC` is a pseudo-member of every closure: grants addressed to the
    // literal role name `PUBLIC` apply to all roles.

    /// Transitive membership closure for `user`, always including `user`
    /// itself plus `PUBLIC`. Follows `members` edges transitively while the
    /// session role inherits (PostgreSQL `INHERIT`; a `NOINHERIT` login
    /// role keeps only its own grants — there is no `SET ROLE` in Beta 2).
    /// Cycles are guarded by the visited set.
    pub fn role_closure(&self, user: &str) -> Vec<String> {
        let mut closure = vec!["PUBLIC".to_string()];
        let session_inherits = self
            .roles
            .get(user)
            .map(|role| role.inherit)
            .unwrap_or(true);
        if !session_inherits {
            closure.push(user.to_string());
            return closure;
        }
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![user.to_string()];
        while let Some(name) = stack.pop() {
            if !visited.insert(name.clone()) {
                continue;
            }
            closure.push(name.clone());
            if let Some(role) = self.roles.get(&name) {
                for member_of in &role.members {
                    stack.push(member_of.clone());
                }
            }
        }
        closure
    }

    /// True when `user`'s own role row carries superuser. Never inherited.
    pub fn is_superuser(&self, user: &str) -> bool {
        self.roles.get(user).is_some_and(|role| role.superuser)
    }

    /// True when `user`'s own role row carries the attribute. Never inherited.
    pub fn has_role_attribute(&self, user: &str, attribute: RoleAttribute) -> bool {
        let Some(role) = self.roles.get(user) else {
            return false;
        };
        match attribute {
            RoleAttribute::Superuser => role.superuser,
            RoleAttribute::CreateRole => role.superuser || role.create_role,
            RoleAttribute::CreateDatabase => role.superuser || role.create_database,
        }
    }

    /// Records table ownership (called on successful `CREATE TABLE` and
    /// `CREATE VIEW`). Re-creation by the same owner is idempotent; a
    /// different owner never overwrites silently — drops clear ownership.
    pub fn set_table_owner(&mut self, table: &str, owner: &str) {
        if self.table_owners.get(table).is_some_and(|o| o == owner) {
            return;
        }
        self.table_owners
            .insert(table.to_string(), owner.to_string());
        self.mark_table_dirty(table);
        self.mark_meta_dirty();
    }

    pub fn table_owner(&self, table: &str) -> Option<&str> {
        self.table_owners.get(table).map(String::as_str)
    }

    pub fn clear_table_owner(&mut self, table: &str) {
        if self.table_owners.remove(table).is_some() {
            self.mark_table_dirty(table);
            self.mark_meta_dirty();
        }
        let before = self.table_grants.len();
        self.table_grants.retain(|(_, t), _| t != table);
        if self.table_grants.len() != before {
            self.mark_table_dirty(table);
            self.mark_meta_dirty();
        }
    }

    /// Adds privilege `bits` for `grantee` on `table`. Returns true when the
    /// stored set changed.
    pub fn grant_table(&mut self, grantee: &str, table: &str, bits: u16) -> bool {
        let entry = self
            .table_grants
            .entry((grantee.to_string(), table.to_string()))
            .or_insert(0);
        let before = *entry;
        *entry |= bits;
        if *entry != before {
            self.mark_table_dirty(table);
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Removes privilege `bits` for `grantee` on `table`. Returns true when
    /// the stored set changed.
    pub fn revoke_table(&mut self, grantee: &str, table: &str, bits: u16) -> bool {
        let key = (grantee.to_string(), table.to_string());
        let Some(entry) = self.table_grants.get_mut(&key) else {
            return false;
        };
        let before = *entry;
        *entry &= !bits;
        if *entry != before {
            if *entry == 0 {
                self.table_grants.remove(&key);
            }
            self.mark_table_dirty(table);
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// True when `user` (or any inherited role) holds `privilege` on the
    /// resolved table. Superusers and the table owner always pass. Unknown
    /// users and unowned/unknown tables fail closed.
    pub fn check_table_privilege(
        &self,
        user: &str,
        table: &str,
        privilege: plomid_security::TablePrivilege,
    ) -> bool {
        if self.is_superuser(user) {
            return true;
        }
        if self.table_owners.get(table).is_some_and(|o| o == user) {
            return true;
        }
        self.role_closure(user).iter().any(|role| {
            self.table_grants
                .get(&(role.clone(), table.to_string()))
                .is_some_and(|bits| privilege.held_by(*bits))
        })
    }

    pub fn set_schema_owner(&mut self, schema: &str, owner: &str) {
        if self.schema_owners.get(schema).is_some_and(|o| o == owner) {
            return;
        }
        self.schema_owners
            .insert(schema.to_string(), owner.to_string());
        self.mark_meta_dirty();
    }

    /// Schema owner lookup (for GRANT/ownership checks).
    pub fn schema_owner(&self, schema: &str) -> Option<&str> {
        self.schema_owners.get(schema).map(String::as_str)
    }

    pub fn grant_schema(&mut self, grantee: &str, schema: &str, bits: u16) -> bool {
        let entry = self
            .schema_grants
            .entry((grantee.to_string(), schema.to_string()))
            .or_insert(0);
        let before = *entry;
        *entry |= bits;
        if *entry != before {
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    pub fn revoke_schema(&mut self, grantee: &str, schema: &str, bits: u16) -> bool {
        let key = (grantee.to_string(), schema.to_string());
        let Some(entry) = self.schema_grants.get_mut(&key) else {
            return false;
        };
        let before = *entry;
        *entry &= !bits;
        if *entry != before {
            if *entry == 0 {
                self.schema_grants.remove(&key);
            }
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Schema privilege check. Superusers and schema owners pass; `USAGE`
    /// on the `public` schema is default-allow (PostgreSQL default) unless
    /// the schema was explicitly created without it — Beta 2 keeps the
    /// default and documents it.
    pub fn check_schema_privilege(
        &self,
        user: &str,
        schema: &str,
        privilege: plomid_security::SchemaPrivilege,
    ) -> bool {
        if self.is_superuser(user) {
            return true;
        }
        if self.schema_owners.get(schema).is_some_and(|o| o == user) {
            return true;
        }
        if schema == "public" && privilege == plomid_security::SchemaPrivilege::Usage {
            return true;
        }
        self.role_closure(user).iter().any(|role| {
            self.schema_grants
                .get(&(role.clone(), schema.to_string()))
                .is_some_and(|bits| privilege.held_by(*bits))
        })
    }

    /// CONNECT check for `database`. Default-allow (PostgreSQL `GRANT
    /// CONNECT TO PUBLIC` default) unless explicitly withdrawn for a role
    /// in the user's closure. Superusers always pass.
    pub fn check_database_connect(&self, user: &str, database: &str) -> bool {
        if self.is_superuser(user) {
            return true;
        }
        !self.role_closure(user).iter().any(|role| {
            self.db_connect_denied
                .contains(&(role.clone(), database.to_string()))
        })
    }

    /// CREATE check for `database` (schema creation). Superusers pass;
    /// otherwise an explicit stored `CREATE` grant on the database is
    /// required (see `GRANT CREATE ON DATABASE`). No default-allow.
    pub fn check_database_create(&self, user: &str, database: &str) -> bool {
        if self.is_superuser(user) {
            return true;
        }
        self.role_closure(user).iter().any(|role| {
            self.db_grants
                .get(&(role.clone(), database.to_string()))
                .is_some_and(|bits| plomid_security::DatabasePrivilege::Create.held_by(*bits))
        })
    }

    /// Adds database privilege `bits` for `grantee` on `database`.
    /// Returns true when the stored set changed.
    pub fn grant_database(&mut self, grantee: &str, database: &str, bits: u16) -> bool {
        let entry = self
            .db_grants
            .entry((grantee.to_string(), database.to_string()))
            .or_insert(0);
        let before = *entry;
        *entry |= bits;
        if *entry != before {
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Removes database privilege `bits` for `grantee` on `database`.
    /// `CONNECT` revocation routes through [`Self::deny_database_connect`]
    /// instead (default-allow model); this removes stored `CREATE` bits.
    pub fn revoke_database(&mut self, grantee: &str, database: &str, bits: u16) -> bool {
        let key = (grantee.to_string(), database.to_string());
        let Some(entry) = self.db_grants.get_mut(&key) else {
            return false;
        };
        let before = *entry;
        *entry &= !bits;
        if *entry != before {
            if *entry == 0 {
                self.db_grants.remove(&key);
            }
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Withdraws CONNECT on `database` for `grantee`. Returns true on change.
    pub fn deny_database_connect(&mut self, grantee: &str, database: &str) -> bool {
        if self
            .db_connect_denied
            .insert((grantee.to_string(), database.to_string()))
        {
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Restores CONNECT on `database` for `grantee`. Returns true on change.
    pub fn allow_database_connect(&mut self, grantee: &str, database: &str) -> bool {
        if self
            .db_connect_denied
            .remove(&(grantee.to_string(), database.to_string()))
        {
            self.mark_meta_dirty();
            true
        } else {
            false
        }
    }

    /// Stores a password verifier for `role`, clearing any legacy
    /// plaintext. The only writer of new credentials (creation, rotation,
    /// legacy upgrade); passwords never persist.
    pub fn set_role_credential(
        &mut self,
        role: &str,
        credential: &plomid_security::RoleCredential,
    ) {
        self.role_credentials
            .insert(role.to_string(), credential.encode());
        if let Some(def) = self.roles.get_mut(role) {
            def.password = None;
        }
        self.mark_meta_dirty();
    }

    /// Decodes the stored verifier for `role`, if any.
    pub fn role_credential(&self, role: &str) -> Option<plomid_security::RoleCredential> {
        self.role_credentials
            .get(role)
            .and_then(|text| plomid_security::RoleCredential::decode(text))
    }

    /// Legacy plaintext password (pre-Phase-2 images). Read source for the
    /// one-shot upgrade at next successful authentication only; new code
    /// must never write this field.
    pub fn legacy_role_password(&self, role: &str) -> Option<String> {
        self.roles.get(role).and_then(|def| def.password.clone())
    }

    /// Drops all ownership/grant state for a table or view. Called by
    /// `drop_table`/`drop_view` so a recreated relation never inherits
    /// the old one's rights.
    pub(crate) fn clear_table_auth(&mut self, table: &str) {
        let mut changed = self.table_owners.remove(table).is_some();
        let before = self.table_grants.len();
        self.table_grants.retain(|(_, t), _| t != table);
        changed |= self.table_grants.len() != before;
        if changed {
            self.mark_table_dirty(table);
            self.mark_meta_dirty();
        }
    }

    /// Moves ownership/grant state across a table rename.
    pub(crate) fn rename_table_auth(&mut self, old: &str, new: &str) {
        let mut changed = false;
        if let Some(owner) = self.table_owners.remove(old) {
            self.table_owners.insert(new.to_string(), owner);
            changed = true;
        }
        let moves: Vec<((String, String), u16)> = self
            .table_grants
            .iter()
            .filter(|((_, t), _)| t == old)
            .map(|((g, _), bits)| ((g.clone(), new.to_string()), *bits))
            .collect();
        if !moves.is_empty() {
            self.table_grants.retain(|(_, t), _| t != old);
            self.table_grants.extend(moves);
            changed = true;
        }
        if changed {
            self.mark_table_dirty(old);
            self.mark_table_dirty(new);
            self.mark_meta_dirty();
        }
    }

    /// Drops schema ownership/grant state. Contained tables/views clean
    /// themselves through their own drop paths.
    pub(crate) fn clear_schema_auth(&mut self, schema: &str) {
        let mut changed = self.schema_owners.remove(schema).is_some();
        let before = self.schema_grants.len();
        self.schema_grants.retain(|(_, s), _| s != schema);
        changed |= self.schema_grants.len() != before;
        if changed {
            self.mark_meta_dirty();
        }
    }

    pub fn tables(&self) -> Vec<&TableSchema> {
        let mut tables: Vec<&TableSchema> = self.tables.values().collect();
        tables.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        tables
    }

    pub fn indexes(&self) -> Vec<IndexDefinition> {
        let mut indexes: Vec<IndexDefinition> = self.indexes.values().cloned().collect();
        indexes.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        indexes
    }

    pub fn index(&self, name: &str) -> Option<IndexDefinition> {
        self.indexes.get(name).cloned()
    }

    pub fn encode(&self) -> Vec<u8> {
        encoding::encode(self)
    }

    pub fn decode(&mut self, bytes: &[u8]) -> Result<()> {
        encoding::decode(self, bytes)
    }

    pub fn column_rules(&self, table: &str) -> Result<Vec<ColumnRule>> {
        let schema = self.get_table(table)?;
        let mut rules = vec![ColumnRule::default(); schema.columns.len()];
        let all_constraints = schema
            .constraints
            .iter()
            .chain(schema.columns.iter().flat_map(|col| col.constraints.iter()))
            .collect::<Vec<_>>();

        for constraint in all_constraints {
            for name in &constraint.columns {
                if let Some(index) = schema.columns.iter().position(|c| c.name == *name) {
                    match constraint.kind {
                        ConstraintKind::NotNull => {
                            rules[index].not_null = true;
                        }
                        ConstraintKind::PrimaryKey => {
                            // The tuple is the key. A composite PRIMARY KEY
                            // keys on `(a, b)` as a whole, so no participating
                            // column is individually unique; every member is
                            // still NOT NULL.
                            rules[index].not_null = true;
                            if constraint.columns.len() == 1 {
                                rules[index].primary_key = true;
                            }
                        }
                        ConstraintKind::Unique => {
                            // Same rule for UNIQUE: `UNIQUE (a, b)` must not be
                            // degraded into `UNIQUE (a)` + `UNIQUE (b)`.
                            if constraint.columns.len() == 1 {
                                rules[index].unique = true;
                            }
                        }
                        ConstraintKind::ForeignKey { .. } => {}
                        ConstraintKind::Default => match constraint.expr.as_ref() {
                            Some(Expression::Literal(value)) => {
                                rules[index].default_value = Some(value.clone());
                            }
                            Some(expr) => {
                                rules[index].default_expr = Some(expr.clone());
                            }
                            None => {}
                        },
                        ConstraintKind::Check => {
                            if let Some(expr) = constraint.expr.as_ref() {
                                if let Some(min) = extract_min_integer(name, expr) {
                                    rules[index].min_integer = Some(min);
                                }
                            }
                        }
                        ConstraintKind::GeneratedAlways => {
                            rules[index].generated_expr = constraint.expr.clone();
                        }
                    }
                }
            }
        }
        Ok(rules)
    }

    /// Finds the first table column whose type is the user-defined type or
    /// domain `name` (OID-based, matching how columns resolve custom types).
    fn find_custom_type_dependency(&self, name: &str, domains: bool) -> Option<String> {
        let registered = if domains {
            self.domains
                .keys()
                .any(|stored| type_name_matches(stored, name))
        } else {
            self.types
                .keys()
                .any(|stored| type_name_matches(stored, name))
        };
        if !registered {
            return None;
        }
        let oid = custom_type_oid(name);
        for (table_name, table) in &self.tables {
            for col in &table.columns {
                if col.col_type.type_oid.raw() == oid {
                    return Some(format!("table \"{table_name}\" column \"{}\"", col.name));
                }
            }
        }
        None
    }
}

impl Catalog for InMemoryCatalog {
    fn session_search_path(&self) -> Vec<String> {
        self.search_path.clone()
    }

    fn set_session_search_path(&mut self, path: Vec<String>) {
        self.set_search_path(path);
    }

    fn create_schema(&mut self, name: String) -> Result<()> {
        if self.schemas.contains_key(&name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("schema \"{}\" already exists", name),
                format!("schema={}", name),
            ));
        }
        // §P1: allocate through the process-wide atomic so concurrent
        // sessions from the same base image mint distinct SchemaIds.
        let id = SchemaId::new(alloc_id(&NEXT_SCHEMA_ID, &mut self.next_schema_id));
        self.dropped_schemas.remove(&name);
        self.schemas.insert(name, id);
        self.mark_meta_dirty();
        Ok(())
    }

    fn has_schema(&self, name: &str) -> bool {
        self.schemas.contains_key(name)
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    fn create_table(
        &mut self,
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<Constraint>,
    ) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("table \"{}\" already exists", name),
                format!("table={}", name),
            ));
        }
        // §P1: atomic allocation — concurrent sessions must not reuse IDs.
        let table_id = TableId::new(alloc_id(&NEXT_TABLE_ID, &mut self.next_table_id));

        let mut column_ids = Vec::with_capacity(columns.len());
        for _ in 0..columns.len() {
            column_ids.push(ColumnId::new(alloc_id(
                &NEXT_COLUMN_ID,
                &mut self.next_column_id,
            )));
        }

        let table = TableSchema {
            name: name.clone(),
            table_id,
            column_ids,
            columns,
            constraints,
        };
        self.mark_table_dirty(&name);
        self.tables.insert(name, table);
        Ok(())
    }

    fn set_constraints(&mut self, table: &str, constraints: Vec<Constraint>) -> Result<()> {
        let table = self.resolve_table_name(table)?;
        let table_schema = self.tables.get_mut(&table).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", table),
                format!("table={}", table),
            )
        })?;
        table_schema.constraints = constraints;
        self.mark_table_dirty(&table);
        Ok(())
    }

    fn constraints(&self, table: &str) -> Result<&[Constraint]> {
        let table = self.resolve_table_name(table)?;
        self.tables
            .get(&table)
            .map(|ts| ts.constraints.as_slice())
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!("table \"{}\" not found", table),
                    format!("table={}", table),
                )
            })
    }

    fn create_sequence(&mut self, name: &str) -> Result<()> {
        if self.sequences.contains_key(name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("sequence \"{}\" already exists", name),
                format!("sequence={}", name),
            ));
        }
        self.sequences.insert(name.to_string(), 0);
        self.mark_sequence_dirty(name);
        Ok(())
    }

    fn has_sequence(&self, name: &str) -> bool {
        self.sequences.contains_key(name)
    }

    fn sequence_names(&self) -> Vec<String> {
        self.sequences.keys().cloned().collect()
    }

    fn drop_sequence(&mut self, name: &str) -> Result<()> {
        if !self.sequences.contains_key(name) {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("sequence \"{}\" not found", name),
                format!("sequence={}", name),
            ));
        }
        self.sequences.remove(name);
        self.mark_sequence_dirty(name);
        Ok(())
    }

    fn create_index(
        &mut self,
        name: String,
        table: String,
        column: String,
        columns: Vec<String>,
        expression: Option<crate::Expression>,
        unique: bool,
        operator_class: Option<String>,
    ) -> Result<()> {
        if self.indexes.contains_key(&name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("index \"{}\" already exists", name),
                format!("index={}", name),
            ));
        }
        // Normalize the ordered column list: `[column]` for single-column,
        // the full list for composite, empty for expression indexes. Reject
        // unknown columns up front rather than registering an index that can
        // never be maintained.
        let schema = self.get_table(&table)?.clone();
        let columns = if expression.is_none() {
            let columns = if columns.is_empty() {
                vec![column.clone()]
            } else {
                columns
            };
            if columns.is_empty() {
                return Err(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    "an index needs at least one column",
                ));
            }
            for indexed in &columns {
                schema.column_index(indexed)?;
            }
            columns
        } else {
            Vec::new()
        };
        let column = columns.first().cloned().unwrap_or(column);
        // §P1: atomic index allocation.
        let index_id = IndexId::new(alloc_id(&NEXT_INDEX_ID, &mut self.next_index_id));

        let index = IndexDefinition {
            name: name.clone(),
            table,
            columns,
            column,
            expression,
            unique,
            index_id,
            operator_class,
            constraint: false,
        };
        self.mark_index_dirty(&name);
        self.indexes.insert(name, index);
        Ok(())
    }

    /// Registers the internal backing index for a single-column `PRIMARY KEY`
    /// / `UNIQUE` constraint. Unlike [`Catalog::create_index`] this marks the
    /// index as constraint-owned so it stays out of the derived index views.
    fn create_constraint_index(
        &mut self,
        name: String,
        table: String,
        column: String,
        unique: bool,
    ) -> Result<()> {
        if self.indexes.contains_key(&name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("index \"{}\" already exists", name),
                format!("index={}", name),
            ));
        }
        // §P1: atomic index allocation.
        let index_id = IndexId::new(alloc_id(&NEXT_INDEX_ID, &mut self.next_index_id));
        let index = IndexDefinition {
            name: name.clone(),
            table,
            column: column.clone(),
            columns: vec![column],
            expression: None,
            unique,
            index_id,
            operator_class: None,
            constraint: true,
        };
        self.mark_index_dirty(&name);
        self.indexes.insert(name, index);
        Ok(())
    }

    fn create_composite_unique_index(
        &mut self,
        name: String,
        table: String,
        columns: Vec<String>,
    ) -> Result<()> {
        if columns.len() < 2 {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "a composite index needs at least two columns",
            ));
        }
        if self.indexes.contains_key(&name) {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("index \"{}\" already exists", name),
                format!("index={}", name),
            ));
        }
        // Reject an unknown column rather than registering an index that can
        // never be maintained (its key values would not be computable).
        let schema = self.get_table(&table)?;
        for column in &columns {
            schema.column_index(column)?;
        }
        // §P1: atomic index allocation.
        let index_id = IndexId::new(alloc_id(&NEXT_INDEX_ID, &mut self.next_index_id));
        let index = IndexDefinition {
            name: name.clone(),
            table,
            column: columns[0].clone(),
            columns,
            expression: None,
            unique: true,
            index_id,
            operator_class: None,
            constraint: true,
        };
        self.mark_index_dirty(&name);
        self.indexes.insert(name, index);
        Ok(())
    }

    fn drop_index(&mut self, name: &str) -> Result<IndexDefinition> {
        let index = self.indexes.remove(name).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("index \"{}\" not found", name),
                format!("index={}", name),
            )
        })?;
        self.mark_index_dirty(name);
        Ok(index)
    }

    fn indexes_for_table(&self, table: &str) -> Vec<IndexDefinition> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table && !idx.constraint)
            .cloned()
            .collect()
    }

    fn all_indexes_for_table(&self, table: &str) -> Vec<IndexDefinition> {
        self.indexes
            .values()
            .filter(|idx| idx.table == table)
            .cloned()
            .collect()
    }

    fn create_view(
        &mut self,
        name: String,
        mut columns: Vec<String>,
        query: Box<Statement>,
    ) -> Result<()> {
        if self.views.contains_key(&name)
            || self.tables.contains_key(&name)
            || self.get_table(&name).is_ok()
        {
            return Err(PlomidError::with_detail(
                ErrorKind::AlreadyExists,
                format!("relation \"{name}\" already exists"),
                format!("view={name}"),
            ));
        }
        if columns.is_empty() {
            columns = infer_view_columns(&query);
        }
        // `SELECT *` views: the legacy name inference yields no names.
        // Expand to the real output names so stored columns/types stay aligned.
        if columns.is_empty() {
            let names = view_types::infer_query_output_names(self, &query);
            if !names.is_empty() {
                columns = names;
            }
        }
        let mut column_types = view_types::infer_query_output_types(self, &query);
        if column_types.len() != columns.len() {
            if column_types.len() > columns.len() {
                column_types.truncate(columns.len());
            } else {
                while column_types.len() < columns.len() {
                    column_types.push(None);
                }
            }
        }
        let definition = render_view_definition(&query);
        self.mark_view_dirty(&name);
        self.views.insert(
            name.clone(),
            StoredView {
                name,
                columns,
                query,
                definition,
                column_types,
            },
        );
        Ok(())
    }

    fn drop_view(&mut self, name: &str) -> Result<()> {
        // Resolve the view name through the session search_path so that
        // unqualified names match the stored schema-qualified key, matching
        // the lookup behavior of `get_view` and `has_view`.
        let name = match self.get_view(name) {
            Some(_) => self
                .relation_candidates(name)
                .iter()
                .find(|candidate| self.views.contains_key(candidate.as_str()))
                .cloned()
                .unwrap_or_else(|| name.to_string()),
            None => name.to_string(),
        };
        self.views
            .remove(&name)
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!("view \"{name}\" does not exist"),
                    format!("view={name}"),
                )
            })
            .map(|_| self.mark_view_dirty(&name))?;
        // A recreated view must never inherit the old one's rights.
        self.clear_table_auth(&name);
        Ok(())
    }

    fn get_view(&self, name: &str) -> Option<&StoredView> {
        self.views.get(name).or_else(|| {
            self.relation_candidates(name)
                .iter()
                .find_map(|candidate| self.views.get(candidate))
        })
    }

    fn has_view(&self, name: &str) -> bool {
        self.get_view(name).is_some()
    }

    fn view_names(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    fn get_table(&self, name: &str) -> Result<&TableSchema> {
        self.tables
            .get(name)
            .or_else(|| {
                self.relation_candidates(name)
                    .iter()
                    .find_map(|candidate| self.tables.get(candidate))
            })
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!("table \"{}\" not found", name),
                    format!("table={}", name),
                )
            })
    }

    fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name) || self.get_table(name).is_ok()
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn drop_table(&mut self, name: &str) -> Result<TableSchema> {
        let name = self.resolve_table_name(name)?;
        let schema = self.tables.remove(&name).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", name),
                format!("table={}", name),
            )
        })?;
        self.mark_table_dirty(&name);
        // A recreated table must never inherit the old one's rights.
        self.clear_table_auth(&name);
        Ok(schema)
    }

    fn rename_table(&mut self, name: &str, new_name: String) -> Result<()> {
        // Resolve the source table and keep the renamed table in the same schema.
        let name = self.resolve_table_name(name)?;
        let (schema, _) = name.split_once('.').unwrap_or(("public", &name));
        let resolved_new = if new_name.contains('.') {
            new_name.clone()
        } else {
            format!("{schema}.{new_name}")
        };
        let mut table_schema = self.tables.remove(&name).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", name),
                format!("table={}", name),
            )
        })?;
        table_schema.name = resolved_new.clone();
        self.tables.insert(resolved_new.clone(), table_schema);
        self.mark_table_dirty(&name);
        self.mark_table_dirty(&resolved_new);
        // Ownership and grants follow the rename (same object, new name).
        self.rename_table_auth(&name, &resolved_new);
        Ok(())
    }

    fn rename_column(&mut self, table: &str, old_name: &str, new_name: String) -> Result<()> {
        let table = self.resolve_table_name(table)?;
        let table_schema = self.tables.get_mut(&table).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", table),
                format!("table={}", table),
            )
        })?;
        let col_index = table_schema
            .columns
            .iter()
            .position(|c| c.name == old_name)
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!("column \"{}\" not found in table \"{}\"", old_name, table),
                    format!("table={} column={}", table, old_name),
                )
            })?;
        table_schema.columns[col_index].name = new_name.clone();
        for constraint in &mut table_schema.constraints {
            if let Some(pos) = constraint.columns.iter().position(|c| c == old_name) {
                constraint.columns[pos] = new_name.clone();
            }
        }
        self.mark_table_dirty(&table);
        Ok(())
    }

    fn add_column(&mut self, table: &str, column: ColumnDef) -> Result<()> {
        let table = self.resolve_table_name(table)?;
        // §P1: allocate before borrowing the table mutably.
        let column_id = ColumnId::new(alloc_id(&NEXT_COLUMN_ID, &mut self.next_column_id));
        let table_schema = self.tables.get_mut(&table).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", table),
                format!("table={}", table),
            )
        })?;
        table_schema.columns.push(column);
        table_schema.column_ids.push(column_id);
        self.mark_table_dirty(&table);
        Ok(())
    }

    fn drop_column(&mut self, table: &str, column: &str) -> Result<ColumnId> {
        let table = self.resolve_table_name(table)?;
        let table_schema = self.tables.get_mut(&table).ok_or_else(|| {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("table \"{}\" not found", table),
                format!("table={}", table),
            )
        })?;
        let col_index = table_schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| {
                PlomidError::with_detail(
                    ErrorKind::NotFound,
                    format!("column \"{}\" not found in table \"{}\"", column, table),
                    format!("table={} column={}", table, column),
                )
            })?;
        let column_id = table_schema.column_ids.remove(col_index);
        table_schema.columns.remove(col_index);
        for constraint in &mut table_schema.constraints {
            constraint.columns.retain(|c| c != column);
        }
        self.mark_table_dirty(&table);
        Ok(column_id)
    }

    fn drop_schema(&mut self, name: &str) -> Result<()> {
        if self.schemas.remove(name).is_none() {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("schema \"{}\" not found", name),
                format!("schema={}", name),
            ));
        }
        self.clear_schema_auth(name);
        // §P1: tombstone so the save-time meta merge does not resurrect it.
        self.dropped_schemas.insert(name.to_string());
        self.mark_meta_dirty();
        Ok(())
    }

    fn create_type(&mut self, def: TypeDefinition) -> Result<()> {
        if self.types.contains_key(&def.name) {
            return Err(PlomidError::new(
                ErrorKind::AlreadyExists,
                format!("type \"{}\" already exists", def.name),
            ));
        }
        self.mark_type_dirty(&def.name);
        self.types.insert(
            def.name.clone(),
            StoredType {
                name: def.name,
                labels: def.labels,
                attributes: def.attributes,
            },
        );
        Ok(())
    }

    fn has_type(&self, name: &str) -> bool {
        self.get_type(name).is_some()
    }

    fn get_type(&self, name: &str) -> Option<StoredType> {
        if let Some(found) = self.types.get(name) {
            return Some(found.clone());
        }
        self.types
            .iter()
            .find(|(stored, _)| type_name_matches(stored, name))
            .map(|(_, stored)| stored.clone())
    }

    fn type_names(&self) -> Vec<String> {
        self.types.keys().cloned().collect()
    }

    fn domain_names(&self) -> Vec<String> {
        self.domains.keys().cloned().collect()
    }

    fn user_type_oid(&self, name: &str) -> Option<u32> {
        let matches = |stored: &String| type_name_matches(stored, name);
        if self.types.keys().any(matches) || self.domains.keys().any(matches) {
            Some(custom_type_oid(name))
        } else {
            None
        }
    }

    fn find_function_by_call(&self, name: &str, arg_count: usize) -> Option<StoredFunction> {
        self.functions
            .values()
            .find(|func| func.args.len() == arg_count && type_name_matches(&func.name, name))
            .cloned()
    }

    fn drop_type(&mut self, name: &str) -> Result<()> {
        self.mark_type_dirty(name);
        if self.types.remove(name).is_none() {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("type \"{}\" not found", name),
                format!("type={}", name),
            ));
        }
        Ok(())
    }

    fn create_domain(&mut self, def: DomainDefinition) -> Result<()> {
        if self.domains.contains_key(&def.name) {
            return Err(PlomidError::new(
                ErrorKind::AlreadyExists,
                format!("domain \"{}\" already exists", def.name),
            ));
        }
        self.mark_domain_dirty(&def.name);
        self.domains.insert(
            def.name.clone(),
            StoredDomain {
                name: def.name,
                base_type: def.base_type,
                constraints: def
                    .constraints
                    .into_iter()
                    .map(|c| StoredDomainConstraint {
                        name: c.name,
                        check: c.check,
                    })
                    .collect(),
            },
        );
        Ok(())
    }

    fn has_domain(&self, name: &str) -> bool {
        self.domains.contains_key(name)
    }

    fn drop_domain(&mut self, name: &str) -> Result<()> {
        self.mark_domain_dirty(name);
        if self.domains.remove(name).is_none() {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("domain \"{}\" not found", name),
                format!("domain={}", name),
            ));
        }
        Ok(())
    }

    fn create_function(&mut self, def: FunctionDefinition) -> Result<()> {
        let key = function_key_from_definition(&def.name, &def.args);
        if self.functions.contains_key(&key) {
            return Err(PlomidError::new(
                ErrorKind::AlreadyExists,
                format!("function \"{}\" already exists", def.name),
            ));
        }
        self.mark_function_dirty(&key);
        self.functions.insert(
            key,
            StoredFunction {
                name: def.name,
                args: def
                    .args
                    .into_iter()
                    .map(|a| StoredFunctionArg {
                        name: a.name,
                        data_type: a.data_type,
                    })
                    .collect(),
                returns: def.returns,
                language: def.language,
                body: def.body,
            },
        );
        Ok(())
    }

    fn has_function(&self, name: &str, args: &[String]) -> bool {
        self.functions
            .contains_key(&function_key_from_args(name, args))
    }

    fn drop_function(&mut self, name: &str, args: &[String]) -> Result<()> {
        let key = function_key_from_args(name, args);
        self.mark_function_dirty(&key);
        if self.functions.remove(&key).is_none() {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("function \"{}\" not found", name),
                format!("function={}", name),
            ));
        }
        Ok(())
    }

    fn find_table_dependency(&self, name: &str) -> Option<String> {
        for (view_name, view) in &self.views {
            if view_refers_to_table(&view.query, name) {
                return Some(format!("view \"{view_name}\""));
            }
        }
        None
    }

    fn find_view_dependency(&self, _name: &str) -> Option<String> {
        None
    }

    fn find_type_dependency(&self, name: &str) -> Option<String> {
        self.find_custom_type_dependency(name, /*domains*/ false)
    }

    fn find_domain_dependency(&self, name: &str) -> Option<String> {
        self.find_custom_type_dependency(name, /*domains*/ true)
    }

    fn find_function_dependency(&self, _name: &str, _args: &[String]) -> Option<String> {
        None
    }

    fn find_column_dependency(&self, table: &str, column: &str) -> Option<String> {
        for (view_name, view) in &self.views {
            if view_refers_to_column(&view.query, table, column) {
                return Some(format!("view \"{view_name}\""));
            }
        }
        None
    }

    fn view_depends_on_table(&self, view: &str, table: &str) -> bool {
        if let Some(v) = self.views.get(view) {
            view_refers_to_table(&v.query, table)
        } else {
            false
        }
    }

    fn find_views_referencing_column(&self, table: &str, column: &str) -> Vec<String> {
        self.views
            .iter()
            .filter(|(_, view)| view_refers_to_column(&view.query, table, column))
            .map(|(name, _)| name.clone())
            .collect()
    }
}

fn render_view_definition(statement: &Statement) -> String {
    match statement {
        Statement::Select {
            targets,
            from,
            where_expr,
            ..
        } => {
            let mut sql = String::from("SELECT ");
            sql.push_str(
                &targets
                    .iter()
                    .map(render_view_target)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            if let Some(from) = from {
                sql.push_str(" FROM ");
                sql.push_str(&render_view_from(from));
            }
            if let Some(predicate) = where_expr {
                sql.push_str(" WHERE ");
                sql.push_str(&render_view_expression(predicate));
            }
            sql
        }
        _ => String::new(),
    }
}

fn render_view_target(target: &crate::SelectTarget) -> String {
    match target {
        crate::SelectTarget::All => "*".into(),
        crate::SelectTarget::QualifiedStar { qualifier } => format!("{qualifier}.*"),
        crate::SelectTarget::Expr { expr, alias } => {
            let mut text = render_view_expression(expr);
            if let Some(alias) = alias {
                text.push_str(" AS ");
                text.push_str(alias);
            }
            text
        }
        crate::SelectTarget::Aliased { target, alias } => {
            format!("{} AS {alias}", render_view_target(target))
        }
        crate::SelectTarget::Function(name) => format!("{name}()"),
        crate::SelectTarget::FunctionCall { name, args } => format!(
            "{}({})",
            name,
            args.iter()
                .map(render_view_expression)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        crate::SelectTarget::WindowFunction { name, .. } => name.clone(),
    }
}

fn render_view_from(from: &crate::FromClause) -> String {
    match from {
        crate::FromClause::Table { name, alias } => alias
            .as_ref()
            .map_or_else(|| name.clone(), |alias| format!("{name} AS {alias}")),
        crate::FromClause::TableFunction {
            name, args, alias, ..
        } => {
            let text = format!(
                "{}({})",
                name,
                args.iter()
                    .map(render_view_expression)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            alias
                .as_ref()
                .map_or(text.clone(), |alias| format!("{text} AS {alias}"))
        }
        crate::FromClause::Subquery { .. } => "(SELECT 1)".into(),
        crate::FromClause::Join {
            left,
            right,
            kind,
            on,
        } => {
            let join = match kind {
                crate::JoinKind::Inner => " JOIN ",
                crate::JoinKind::Left => " LEFT JOIN ",
                crate::JoinKind::Right => " RIGHT JOIN ",
                crate::JoinKind::Full => " FULL JOIN ",
                crate::JoinKind::Cross => " CROSS JOIN ",
            };
            let mut text = format!(
                "{}{}{}",
                render_view_from(left),
                join,
                render_view_from(right)
            );
            if let Some(on) = on {
                text.push_str(" ON ");
                text.push_str(&render_view_expression(on));
            }
            text
        }
    }
}

fn render_view_expression(expression: &crate::Expression) -> String {
    match expression {
        crate::Expression::ColumnRef(name) => name.clone(),
        crate::Expression::Literal(value) => render_view_literal(value),
        crate::Expression::Star => "*".into(),
        crate::Expression::Equal(left, right) => format!(
            "{} = {}",
            render_view_expression(left),
            render_view_expression(right)
        ),
        crate::Expression::Greater(left, right) => format!(
            "{} > {}",
            render_view_expression(left),
            render_view_expression(right)
        ),
        crate::Expression::Less(left, right) => format!(
            "{} < {}",
            render_view_expression(left),
            render_view_expression(right)
        ),
        crate::Expression::And(left, right) => format!(
            "{} AND {}",
            render_view_expression(left),
            render_view_expression(right)
        ),
        crate::Expression::FunctionCall { name, args, .. } => format!(
            "{}({})",
            name,
            args.iter()
                .map(render_view_expression)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => "NULL".into(),
    }
}

fn render_view_literal(value: &Value) -> String {
    match value {
        Value::Text(text)
        | Value::VarChar(text)
        | Value::BpChar(text)
        | Value::Name(text)
        | Value::Json(text) => format!("'{}'", text.replace('\'', "''")),
        _ => value.to_sql_text(),
    }
}

fn infer_view_columns(statement: &Statement) -> Vec<String> {
    let Statement::Select { targets, .. } = statement else {
        return Vec::new();
    };
    targets
        .iter()
        .filter_map(|target| match target {
            crate::ast::SelectTarget::Expr { expr, alias } => {
                alias.clone().or_else(|| match expr {
                    Expression::ColumnRef(name) => name.rsplit('.').next().map(str::to_string),
                    _ => None,
                })
            }
            crate::ast::SelectTarget::Aliased { alias, .. } => Some(alias.clone()),
            crate::ast::SelectTarget::Function(name)
            | crate::ast::SelectTarget::FunctionCall { name, .. }
            | crate::ast::SelectTarget::WindowFunction { name, .. } => Some(name.clone()),
            crate::ast::SelectTarget::All | crate::ast::SelectTarget::QualifiedStar { .. } => None,
        })
        .collect()
}

fn extract_min_integer(column: &str, expr: &Expression) -> Option<i64> {
    match expr {
        Expression::Greater(left, right) => {
            let (column_expr, value_expr) = (left, right);
            extract_min_integer_from_comparison(column, column_expr, value_expr, true)
        }
        Expression::GreaterOrEqual(left, right) => {
            let (column_expr, value_expr) = (left, right);
            extract_min_integer_from_comparison(column, column_expr, value_expr, false)
        }
        Expression::Less(left, right) => {
            let (column_expr, value_expr) = (right, left);
            extract_min_integer_from_comparison(column, column_expr, value_expr, true)
        }
        Expression::LessOrEqual(left, right) => {
            let (column_expr, value_expr) = (right, left);
            extract_min_integer_from_comparison(column, column_expr, value_expr, false)
        }
        _ => None,
    }
}

fn extract_min_integer_from_comparison(
    column: &str,
    column_expr: &Expression,
    value_expr: &Expression,
    is_strict: bool,
) -> Option<i64> {
    // Check if column_expr is a reference to our column
    if !matches!(column_expr, Expression::ColumnRef(name) if name == column) {
        return None;
    }

    // Extract the integer value from value_expr
    let value = match value_expr {
        Expression::Literal(Value::Int8(v)) => *v,
        Expression::Literal(Value::Int4(v)) => *v as i64,
        Expression::Literal(Value::Int2(v)) => *v as i64,
        Expression::Negate(inner) => match &**inner {
            Expression::Literal(Value::Int8(v)) => -*v,
            Expression::Literal(Value::Int4(v)) => -(*v as i64),
            Expression::Literal(Value::Int2(v)) => -(*v as i64),
            _ => return None,
        },
        _ => return None,
    };

    // For strict inequalities (> or <), we need value + 1
    // For non-strict (>= or <=), we use value as-is
    if is_strict {
        value.checked_add(1)
    } else {
        Some(value)
    }
}

fn value_matches_column(col_type: &ColumnType, value: &Value) -> bool {
    if value.is_null() || matches!(value, Value::Unknown(_)) {
        return true;
    }
    let Some(column_pg) = col_type.pg_type() else {
        return true;
    };
    value_pg_type(value) == Some(column_pg)
}

// OIDs for user-defined types/domains start above the builtin range and are
// derived deterministically from the (lowercased, unqualified) type name so
// they are stable across restarts without persisting an OID mapping.

/// Case-insensitive match that also tolerates one side being schema-qualified.
fn type_name_matches(stored: &str, asked: &str) -> bool {
    stored.eq_ignore_ascii_case(asked)
        || bare_type_name(stored).eq_ignore_ascii_case(bare_type_name(asked))
}

pub fn load_catalog<E: plomid_txn::StorageEngine>(engine: &mut E) -> Result<InMemoryCatalog> {
    tracing::trace!(target: "catalog", "load_start");
    let mut catalog = InMemoryCatalog::new();
    if let Some(legacy_relation_auth) = encoding::load_sharded(engine, &mut catalog)? {
        catalog.clear_dirty();
        if legacy_relation_auth {
            // Older sharded images kept table owners/grants in the global
            // meta value. Rewrite every relation shard once so the next DDL
            // never recreates that page-sized global record.
            catalog.mark_all_dirty();
            encoding::save_sharded(&mut catalog, engine)?;
            catalog.clear_dirty();
        }
        tracing::trace!(target: "catalog", "load_complete table_count={}", catalog.tables.len());
        return Ok(catalog);
    }
    match engine.get(CATALOG_KEY.as_bytes()) {
        Ok(Some(bytes)) => {
            tracing::trace!(target: "catalog", "load_decode bytes={}", bytes.len());
            catalog.decode(&bytes)?;
            // Migrate legacy single-blob images forward so the next save is
            // already sharded (idempotent: a concurrent loader sees either
            // layout and converges to the same image). The whole decoded
            // image is marked dirty so the migration persists every entry.
            catalog.clear_dirty();
            catalog.mark_all_dirty();
            encoding::save_sharded(&mut catalog, engine)?;
            catalog.clear_dirty();
        }
        Ok(None) => {
            tracing::trace!(target: "catalog", "load_empty");
            // A fresh database has nothing durable to preserve: clear the
            // constructor's dirty marks so live-session refresh (§P1-003)
            // treats this image as clean. The first real change forces a
            // full meta rewrite anyway (generation bump), so schemas, roles
            // and database names still persist on first save.
            catalog.clear_dirty();
        }
        Err(err) => {
            tracing::error!(target: "catalog", "load_failed error={}", err);
            return Err(err);
        }
    }
    tracing::trace!(target: "catalog", "load_complete table_count={}", catalog.tables.len());
    Ok(catalog)
}

pub fn save_catalog<E: plomid_txn::StorageEngine>(
    catalog: &mut InMemoryCatalog,
    engine: &mut E,
) -> Result<()> {
    tracing::trace!(target: "catalog", "persist_start table_count={}", catalog.tables.len());
    encoding::save_sharded(catalog, engine)?;
    tracing::trace!(target: "catalog", "persist_complete table_count={}", catalog.tables.len());
    Ok(())
}

/// Computes the pending catalog delta without writing it.
///
/// For session-transaction commit, which stages the delta into its own
/// already-open transaction: opening a fresh engine transaction there
/// would block acquiring the write-lane gate the session already holds
/// (self-deadlock). The caller applies the puts/deletes to its own
/// transaction and clears the marks after a successful commit.
pub fn staged_catalog_delta(catalog: &mut InMemoryCatalog) -> encoding::CatalogDelta {
    encoding::catalog_delta(catalog)
}

/// Durable catalog generation (§P1-003): the change detector live sessions
/// compare their image against. Returns `None` when no versioned image is
/// stored yet (fresh database) or the stored image predates generations;
/// read errors also report `None` so callers fail open to a reload check,
/// never to a panic.
pub fn catalog_generation<E: plomid_txn::StorageEngine>(engine: &mut E) -> Option<u64> {
    encoding::peek_generation(engine).unwrap_or(None)
}

fn function_key_from_definition(name: &str, args: &[FunctionArgDefinition]) -> String {
    let types: Vec<String> = args.iter().map(|a| a.data_type.clone()).collect();
    format!("{}[{}]", name, types.join(","))
}

fn function_key_from_args(name: &str, args: &[String]) -> String {
    format!("{}[{}]", name, args.join(","))
}

fn view_refers_to_table(query: &Statement, table: &str) -> bool {
    match query {
        Statement::Select {
            from, where_expr, ..
        } => {
            if let Some(from) = from {
                if from_refers_to_table(from, table) {
                    return true;
                }
            }
            if let Some(where_expr) = where_expr {
                if expr_refers_to_table(where_expr, table) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn from_refers_to_table(from: &crate::FromClause, table: &str) -> bool {
    match from {
        crate::FromClause::Table { name, .. } => name == table,
        crate::FromClause::Subquery { statement, .. } => view_refers_to_table(statement, table),
        crate::FromClause::Join { left, right, .. } => {
            from_refers_to_table(left, table) || from_refers_to_table(right, table)
        }
        crate::FromClause::TableFunction { .. } => false,
    }
}

fn expr_refers_to_table(expr: &Expression, table: &str) -> bool {
    match expr {
        Expression::ColumnRef(name) => {
            if let Some((qualifier, _)) = name.rsplit_once('.') {
                qualifier == table
            } else {
                false
            }
        }
        Expression::FunctionCall { args, .. } => {
            args.iter().any(|a| expr_refers_to_table(a, table))
        }
        Expression::And(left, right) | Expression::Or(left, right) => {
            expr_refers_to_table(left, table) || expr_refers_to_table(right, table)
        }
        Expression::Equal(left, right)
        | Expression::Greater(left, right)
        | Expression::Less(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::NotEqual(left, right) => {
            expr_refers_to_table(left, table) || expr_refers_to_table(right, table)
        }
        Expression::Cast { expr, .. } | Expression::TypeCast { expr, .. } => {
            expr_refers_to_table(expr, table)
        }
        Expression::ScalarSubquery(query) => view_refers_to_table(query, table),
        _ => false,
    }
}

fn view_refers_to_column(query: &Statement, table: &str, column: &str) -> bool {
    match query {
        Statement::Select {
            targets,
            from,
            where_expr,
            ..
        } => {
            for target in targets {
                if target_refers_to_column(target, table, column) {
                    return true;
                }
            }
            if let Some(from) = from {
                if from_refers_to_table(from, table) {
                    for target in targets {
                        if target_refers_to_column(target, table, column) {
                            return true;
                        }
                    }
                    if let Some(where_expr) = where_expr {
                        if expr_refers_to_column(where_expr, table, column) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn target_refers_to_column(target: &crate::SelectTarget, table: &str, column: &str) -> bool {
    match target {
        crate::SelectTarget::All => false,
        crate::SelectTarget::QualifiedStar { qualifier } => qualifier == table,
        crate::SelectTarget::Expr { expr, .. } => expr_refers_to_column(expr, table, column),
        crate::SelectTarget::Function(_) => false,
        crate::SelectTarget::FunctionCall { name: _, args } => {
            args.iter().any(|a| expr_refers_to_column(a, table, column))
        }
        crate::SelectTarget::WindowFunction { args, .. } => {
            args.iter().any(|a| expr_refers_to_column(a, table, column))
        }
        crate::SelectTarget::Aliased { target, .. } => {
            target_refers_to_column(target, table, column)
        }
    }
}

fn expr_refers_to_column(expr: &Expression, table: &str, column: &str) -> bool {
    match expr {
        Expression::ColumnRef(name) => {
            if name == column {
                return true;
            }
            if let Some((qualifier, col)) = name.rsplit_once('.') {
                return qualifier == table && col == column;
            }
            false
        }
        Expression::FunctionCall { args, .. } => {
            args.iter().any(|a| expr_refers_to_column(a, table, column))
        }
        Expression::And(left, right) | Expression::Or(left, right) => {
            expr_refers_to_column(left, table, column)
                || expr_refers_to_column(right, table, column)
        }
        Expression::Equal(left, right)
        | Expression::Greater(left, right)
        | Expression::Less(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::NotEqual(left, right) => {
            expr_refers_to_column(left, table, column)
                || expr_refers_to_column(right, table, column)
        }
        Expression::Cast { expr, .. } | Expression::TypeCast { expr, .. } => {
            expr_refers_to_column(expr, table, column)
        }
        Expression::ScalarSubquery(query) => view_refers_to_column(query, table, column),
        _ => false,
    }
}
