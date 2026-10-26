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
// Moved from `src/catalog/mod.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_sql::catalog::*;
use plomid_sql::{ColumnDef, ColumnType};
use plomid_txn::{PlomidStorageEngine, StorageEngine, StorageEngineTransaction};

#[test]
fn create_and_lookup_table() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::text(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let table = catalog.get_table("users").unwrap();
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[1].name, "name");
}

#[test]
fn unqualified_lookup_uses_session_search_path() {
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("plomid_compat".to_string()).unwrap();
    catalog
        .create_table(
            "plomid_compat.users".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::int4(),
                constraints: Vec::new(),
            }],
            Vec::new(),
        )
        .unwrap();
    catalog.set_search_path(vec!["plomid_compat".to_string(), "public".to_string()]);
    assert_eq!(
        catalog.get_table("users").unwrap().name,
        "plomid_compat.users"
    );
}

#[test]
fn same_name_tables_coexist_across_schemas() {
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("plomid_test".to_string()).unwrap();
    // `public.customers` exists; an unqualified create must still be
    // allowed in `plomid_test` (the first schema on the search_path).
    catalog
        .create_table("public.customers".to_string(), vec![], Vec::new())
        .unwrap();
    catalog.set_search_path(vec!["plomid_test".to_string(), "public".to_string()]);
    let resolved = catalog.resolve_create_name("customers").unwrap();
    assert_eq!(resolved, "plomid_test.customers");
    catalog.create_table(resolved, vec![], Vec::new()).unwrap();
    // Both coexist and unqualified lookup prefers the first schema.
    assert!(catalog.has_table("public.customers"));
    assert!(catalog.has_table("plomid_test.customers"));
    assert_eq!(
        catalog.resolve_table_name("customers").unwrap(),
        "plomid_test.customers"
    );
    assert_eq!(
        catalog.get_table("plomid_test.customers").unwrap().name,
        "plomid_test.customers"
    );
}

#[test]
fn create_table_does_not_reject_due_to_other_schema() {
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("plomid_test".to_string()).unwrap();
    catalog
        .create_table("public.customers".to_string(), vec![], Vec::new())
        .unwrap();
    catalog.set_search_path(vec!["plomid_test".to_string(), "public".to_string()]);
    let resolved = catalog.resolve_create_name("customers").unwrap();
    catalog.create_table(resolved, vec![], Vec::new()).unwrap();
    // Recreating the same qualified table must still be rejected.
    assert!(catalog
        .create_table("plomid_test.customers".to_string(), vec![], Vec::new())
        .is_err());
}

#[test]
fn duplicate_table_rejected() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table("users".to_string(), vec![], Vec::new())
        .unwrap();
    let err = catalog
        .create_table("users".to_string(), vec![], Vec::new())
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn schema_is_persistent_in_catalog_encoding() {
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("analytics".to_string()).unwrap();
    let bytes = catalog.encode();
    let mut decoded = InMemoryCatalog::new();
    decoded.decode(&bytes).unwrap();
    assert!(decoded.has_schema("public"));
    assert!(decoded.has_schema("analytics"));
    assert_eq!(decoded.schema_names(), vec!["analytics", "public"]);
}

#[test]
fn stable_object_ids_survive_catalog_round_trip() {
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("analytics".to_string()).unwrap();
    catalog
        .create_table(
            "analytics.events".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::int4(),
                constraints: Vec::new(),
            }],
            Vec::new(),
        )
        .unwrap();
    let schema_id = catalog.schema_id("analytics").unwrap();
    let table = catalog.get_table("analytics.events").unwrap();
    let table_id = table.table_id;
    let column_id = table.column_ids[0];

    let mut decoded = InMemoryCatalog::new();
    decoded.decode(&catalog.encode()).unwrap();
    assert_eq!(decoded.schema_id("analytics"), Some(schema_id));
    let decoded_table = decoded.get_table("analytics.events").unwrap();
    assert_eq!(decoded_table.table_id, table_id);
    assert_eq!(decoded_table.column_ids, vec![column_id]);
}

#[test]
fn index_metadata_round_trips_with_stable_identity() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "email".to_string(),
                    col_type: ColumnType::text(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    catalog
        .create_index(
            "users_email_idx".to_string(),
            "users".to_string(),
            "email".to_string(),
            vec!["email".to_string()],
            None,
            true,
            None,
        )
        .unwrap();
    let expected = catalog.index("users_email_idx").unwrap();
    let mut restored = InMemoryCatalog::new();
    restored.decode(&catalog.encode()).unwrap();
    assert_eq!(restored.index("users_email_idx"), Some(expected));
}

#[test]
fn catalog_round_trips() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::text(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();

    let bytes = catalog.encode();
    let mut decoded = InMemoryCatalog::new();
    decoded.decode(&bytes).unwrap();

    assert_eq!(decoded.table_names(), vec!["users"]);
    let table = decoded.get_table("users").unwrap();
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.columns[0].name, "id");
    assert_eq!(table.columns[0].col_type, ColumnType::int4());
    assert_eq!(table.columns[1].name, "name");
    assert_eq!(table.columns[1].col_type, ColumnType::text());
}

#[test]
fn unknown_column_error_has_context() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::int4(),
                constraints: Vec::new(),
            }],
            Vec::new(),
        )
        .unwrap();
    let table = catalog.get_table("users").unwrap();
    let err = table.column_index("missing").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.to_string().contains("missing"));
    assert!(err.to_string().contains("users"));
    assert_eq!(err.detail(), Some("table=users column=missing"));
}

#[test]
fn type_mismatch_error_has_context() {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::text(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let table = catalog.get_table("users").unwrap();
    let err = table
        .validate_row(&[
            plomid_sql::Value::Text("bad".to_string()),
            plomid_sql::Value::Null,
        ])
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidArgument);
    assert!(err.to_string().contains("expects int4"));
    assert!(err.detail().unwrap().contains("table=users"));
}

#[test]
fn corrupted_catalog_metadata_returns_catalog_error() {
    let mut catalog = InMemoryCatalog::new();
    let err = catalog.decode(&[0xFF]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    assert!(err.to_string().contains("unsupported catalog version"));
    assert!(err.to_string().contains("expected"));
}

fn scratch_engine(tag: &str) -> (PlomidStorageEngine, std::path::PathBuf, std::path::PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-cat-shard-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-cat-shard-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::create(&storage, &wal, 64).unwrap();
    (engine, storage, wal)
}

#[test]
fn sharded_save_load_roundtrips_many_tables() {
    let (mut engine, storage, wal) = scratch_engine("roundtrip");
    let mut catalog = InMemoryCatalog::new();
    for i in 0..500 {
        let name = format!("public.t{i:03}");
        catalog
            .create_table(
                name.clone(),
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        col_type: ColumnType::bigint(),
                        constraints: Vec::new(),
                    },
                    ColumnDef {
                        name: "v".to_string(),
                        col_type: ColumnType::text(),
                        constraints: Vec::new(),
                    },
                ],
                vec![plomid_sql::ast::Constraint {
                    name: None,
                    kind: plomid_sql::ast::ConstraintKind::PrimaryKey,
                    columns: vec!["id".to_string()],
                    expr: None,
                }],
            )
            .unwrap();
        catalog.set_table_owner(&name, "plomid");
        assert!(catalog.grant_table("analyst", &name, 1));
    }
    save_catalog(&mut catalog, &mut engine).unwrap();
    let restored = load_catalog(&mut engine).unwrap();
    assert_eq!(restored.tables().len(), 500);
    for i in (0..500).step_by(37) {
        let t = restored.get_table(&format!("t{i:03}")).unwrap();
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.constraints.len(), 1);
        assert_eq!(
            restored.table_owner(&format!("public.t{i:03}")),
            Some("plomid")
        );
    }
    // Every entry is a small key: nothing near the page ceiling.
    for (key, value) in engine
        .scan(Some(b"__plomid_catalog_"), Some(b"__plomid_catalog_\xFF"))
        .unwrap()
    {
        assert!(
            value.len() < 4096,
            "catalog entry too large: {} bytes for {key:?}",
            value.len()
        );
    }
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn sharded_save_deletes_dropped_table_keys() {
    let (mut engine, storage, wal) = scratch_engine("dropkeys");
    let mut catalog = InMemoryCatalog::new();
    for name in ["alpha", "beta", "gamma"] {
        catalog
            .create_table(
                name.to_string(),
                vec![ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::bigint(),
                    constraints: Vec::new(),
                }],
                Vec::new(),
            )
            .unwrap();
    }
    save_catalog(&mut catalog, &mut engine).unwrap();
    catalog.drop_table("beta").unwrap();
    save_catalog(&mut catalog, &mut engine).unwrap();
    let keys: Vec<Vec<u8>> = engine
        .scan(Some(b"__plomid_catalog_"), Some(b"__plomid_catalog_\xFF"))
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(
        keys.iter().all(|k| !k.windows(4).any(|w| w == b"beta")),
        "dropped table key survived: {keys:?}"
    );
    let restored = load_catalog(&mut engine).unwrap();
    assert_eq!(restored.tables().len(), 2);
    assert!(restored.get_table("beta").is_err());
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn single_blob_migrates_to_sharded_on_load() {
    let (mut engine, storage, wal) = scratch_engine("migrate");
    let mut catalog = InMemoryCatalog::new();
    for name in ["one", "two"] {
        catalog
            .create_table(
                name.to_string(),
                vec![ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::bigint(),
                    constraints: Vec::new(),
                }],
                Vec::new(),
            )
            .unwrap();
    }
    // Write a single-blob image directly (first byte 0), bypassing save.
    let blob = catalog.encode();
    assert_eq!(blob[0], 0);
    let mut txn = engine.begin().unwrap();
    txn.put(CATALOG_KEY.as_bytes(), &blob).unwrap();
    txn.commit().unwrap();
    drop(txn);
    let restored = load_catalog(&mut engine).unwrap();
    assert_eq!(restored.tables().len(), 2);
    assert!(restored.get_table("one").is_ok());
    // Beta baseline: columns decode with the multi-vector cap present.
    let col = &restored.get_table("one").unwrap().columns[0];
    assert_eq!(col.col_type.vector_max_vectors, 1);
    // Migration replaced the blob with the sharded image in the same key.
    let meta = engine.get(CATALOG_KEY.as_bytes()).unwrap().unwrap();
    assert_eq!(meta[0], plomid_sql::catalog::encoding::SHARD_VERSION);
    let reloaded = load_catalog(&mut engine).unwrap();
    assert_eq!(reloaded.tables().len(), 2);
    let mut reloaded = reloaded;
    reloaded
        .create_table(
            "three".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                col_type: ColumnType::bigint(),
                constraints: Vec::new(),
            }],
            Vec::new(),
        )
        .unwrap();
    save_catalog(&mut reloaded, &mut engine).unwrap();
    assert_eq!(load_catalog(&mut engine).unwrap().tables().len(), 3);
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
