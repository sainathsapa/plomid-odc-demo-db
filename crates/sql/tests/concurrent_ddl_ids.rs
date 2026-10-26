//
// © 2026 PLOMID Technologies INDIA
//
// Licensed under the Apache License, Version 2.0;
// Regression test for P1 — Concurrent-DDL ID race.
//
// Two sessions loading the same catalog generation used to copy identical
// `next_*` counters, so concurrent `CREATE TABLE/SCHEMA/INDEX` minted
// duplicate IDs (same TableId for different names, same SchemaId(2) after
// restart). Allocation now goes through process-wide atomics plus durable
// high-water, and saves merge durable meta under a process-wide lock.

use plomid_sql::catalog::*;
use plomid_sql::{ColumnDef, ColumnType};
use plomid_txn::PlomidStorageEngine;
use std::collections::HashSet;

fn test_columns() -> Vec<ColumnDef> {
    vec![ColumnDef {
        name: "id".to_string(),
        col_type: ColumnType::bigint(),
        constraints: Vec::new(),
    }]
}

/// Mirrors the executor's `CREATE TABLE` claim sequence step for step
/// (F-004): acquire the create gate, re-probe the durable entry for the
/// resolved name, create in the image, and save — all while still holding
/// the gate, so the probe answer stays authoritative until the claim lands.
fn atomic_create(
    catalog: &mut InMemoryCatalog,
    engine: &mut PlomidStorageEngine,
    name: &str,
    columns: Vec<ColumnDef>,
) -> plomid_core::Result<()> {
    let gate = CreateGate::acquire();
    if gate.table_exists(engine, name)? {
        return Err(plomid_core::PlomidError::with_detail(
            plomid_core::ErrorKind::AlreadyExists,
            format!("table \"{name}\" already exists"),
            format!("table={name}"),
        ));
    }
    catalog.create_table(name.to_string(), columns, Vec::new())?;
    gate.save(catalog, engine)
}

#[test]
fn concurrent_table_ids_are_distinct() {
    // Two sessions from the same base image (identical counters pre-fix).
    let base = InMemoryCatalog::new();
    let mut a = base.clone();
    let mut b = base.clone();
    a.create_table("public.a".to_string(), test_columns(), Vec::new())
        .unwrap();
    b.create_table("public.b".to_string(), test_columns(), Vec::new())
        .unwrap();
    let id_a = a.get_table("public.a").unwrap().table_id;
    let id_b = b.get_table("public.b").unwrap().table_id;
    assert_ne!(
        id_a,
        id_b,
        "concurrent CREATE TABLE minted duplicate TableId({})",
        id_a.get()
    );
    // Column IDs must also be disjoint (global allocation).
    let cols_a: HashSet<u64> = a
        .get_table("public.a")
        .unwrap()
        .column_ids
        .iter()
        .map(|id| id.get())
        .collect();
    let cols_b: HashSet<u64> = b
        .get_table("public.b")
        .unwrap()
        .column_ids
        .iter()
        .map(|id| id.get())
        .collect();
    assert!(
        cols_a.is_disjoint(&cols_b),
        "concurrent CREATE TABLE reused column IDs: {cols_a:?} vs {cols_b:?}"
    );
}

#[test]
fn concurrent_schema_ids_are_distinct() {
    let base = InMemoryCatalog::new();
    let mut a = base.clone();
    let mut b = base.clone();
    a.create_schema("s_a".to_string()).unwrap();
    b.create_schema("s_b".to_string()).unwrap();
    let id_a = a.schema_id("s_a").unwrap();
    let id_b = b.schema_id("s_b").unwrap();
    assert_ne!(id_a, id_b, "concurrent CREATE SCHEMA reused SchemaId");
}

#[test]
fn concurrent_threads_mint_distinct_table_ids() {
    // Deterministic thread rendezvous: all threads clone the same base, then
    // allocate concurrently. Distinctness must hold regardless of interleaving.
    let base = InMemoryCatalog::new();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let mut handles = Vec::new();
    for i in 0..8 {
        let base = base.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let mut cat = base;
            barrier.wait();
            cat.create_table(format!("public.t{i}"), test_columns(), Vec::new())
                .unwrap();
            cat.get_table(&format!("public.t{i}"))
                .unwrap()
                .table_id
                .get()
        }));
    }
    let mut ids = HashSet::new();
    for h in handles {
        let id = h.join().unwrap();
        assert!(ids.insert(id), "duplicate TableId({id}) under concurrency");
    }
    assert_eq!(ids.len(), 8);
}

#[test]
fn concurrent_add_column_ids_are_distinct() {
    let mut base = InMemoryCatalog::new();
    base.create_table("public.t".to_string(), test_columns(), Vec::new())
        .unwrap();
    let mut a = base.clone();
    let mut b = base.clone();
    a.add_column(
        "public.t",
        ColumnDef {
            name: "a".to_string(),
            col_type: ColumnType::int4(),
            constraints: Vec::new(),
        },
    )
    .unwrap();
    b.add_column(
        "public.t",
        ColumnDef {
            name: "b".to_string(),
            col_type: ColumnType::int4(),
            constraints: Vec::new(),
        },
    )
    .unwrap();
    let id_a = *a.get_table("public.t").unwrap().column_ids.last().unwrap();
    let id_b = *b.get_table("public.t").unwrap().column_ids.last().unwrap();
    assert_ne!(id_a, id_b, "concurrent ADD COLUMN reused ColumnId");
}

#[test]
fn concurrent_index_ids_are_distinct() {
    let mut base = InMemoryCatalog::new();
    for t in ["public.a", "public.b"] {
        base.create_table(t.to_string(), test_columns(), Vec::new())
            .unwrap();
    }
    let mut a = base.clone();
    let mut b = base.clone();
    a.create_index(
        "idx_a".to_string(),
        "public.a".to_string(),
        "id".to_string(),
        vec!["id".to_string()],
        None,
        false,
        None,
    )
    .unwrap();
    b.create_index(
        "idx_b".to_string(),
        "public.b".to_string(),
        "id".to_string(),
        vec!["id".to_string()],
        None,
        false,
        None,
    )
    .unwrap();
    assert_ne!(
        a.index("idx_a").unwrap().index_id,
        b.index("idx_b").unwrap().index_id,
        "concurrent CREATE INDEX reused IndexId"
    );
}

fn scratch_engine(tag: &str) -> (PlomidStorageEngine, std::path::PathBuf, std::path::PathBuf) {
    let storage =
        std::env::temp_dir().join(format!("plomid-ddl-race-{tag}-{}", std::process::id()));
    let wal =
        std::env::temp_dir().join(format!("plomid-ddl-race-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::create(&storage, &wal, 64).unwrap();
    (engine, storage, wal)
}

#[test]
fn sharded_concurrent_saves_preserve_both_tables_and_ids() {
    // Simulates two sessions that loaded the same generation, created
    // different tables, and saved "concurrently" (serialized by the DDL save
    // lock). Both tables must survive with distinct IDs.
    let (mut engine, storage, wal) = scratch_engine("tables");
    let mut base = InMemoryCatalog::new();
    save_catalog(&mut base, &mut engine).unwrap();

    let mut a = load_catalog(&mut engine).unwrap();
    let mut b = load_catalog(&mut engine).unwrap();
    a.create_table("public.a".to_string(), test_columns(), Vec::new())
        .unwrap();
    b.create_table("public.b".to_string(), test_columns(), Vec::new())
        .unwrap();
    save_catalog(&mut a, &mut engine).unwrap();
    save_catalog(&mut b, &mut engine).unwrap();

    let restored = load_catalog(&mut engine).unwrap();
    let id_a = restored.get_table("public.a").unwrap().table_id;
    let id_b = restored.get_table("public.b").unwrap().table_id;
    assert_ne!(id_a, id_b, "durable tables share TableId({})", id_a.get());
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn sharded_concurrent_schema_saves_preserve_both() {
    // Same as above for the shared meta value (schemas): last-writer-wins
    // used to drop the first schema entirely.
    let (mut engine, storage, wal) = scratch_engine("schemas");
    let mut base = InMemoryCatalog::new();
    save_catalog(&mut base, &mut engine).unwrap();

    let mut a = load_catalog(&mut engine).unwrap();
    let mut b = load_catalog(&mut engine).unwrap();
    a.create_schema("s_a".to_string()).unwrap();
    b.create_schema("s_b".to_string()).unwrap();
    save_catalog(&mut a, &mut engine).unwrap();
    save_catalog(&mut b, &mut engine).unwrap();

    let restored = load_catalog(&mut engine).unwrap();
    assert!(
        restored.has_schema("s_a"),
        "concurrent CREATE SCHEMA s_a lost"
    );
    assert!(
        restored.has_schema("s_b"),
        "concurrent CREATE SCHEMA s_b lost"
    );
    assert_ne!(
        restored.schema_id("s_a"),
        restored.schema_id("s_b"),
        "durable schemas share SchemaId"
    );
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn schema_highwater_survives_reload() {
    // Pre-fix `next_schema_id` was never persisted/decoded (always 2 after
    // restart), so every post-restart CREATE SCHEMA re-minted SchemaId(2).
    let (mut engine, storage, wal) = scratch_engine("highwater");
    let mut catalog = InMemoryCatalog::new();
    catalog.create_schema("s1".to_string()).unwrap();
    catalog.create_schema("s2".to_string()).unwrap();
    let id1 = catalog.schema_id("s1").unwrap();
    let id2 = catalog.schema_id("s2").unwrap();
    assert_ne!(id1, id2);
    save_catalog(&mut catalog, &mut engine).unwrap();

    let mut restored = load_catalog(&mut engine).unwrap();
    restored.create_schema("s3".to_string()).unwrap();
    let id3 = restored.schema_id("s3").unwrap();
    assert_ne!(id3, id1, "schema ID reused after reload");
    assert_ne!(id3, id2, "schema ID reused after reload");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}

#[test]
fn concurrent_same_name_create_table_has_exactly_one_winner() {
    // F-004: two sessions holding the same point-in-time image used to both
    // pass their in-memory existence checks and both save; the later save
    // silently overwrote the earlier shape while both clients were told
    // they succeeded. The create gate re-probes durable state under the
    // save lock, so the second claim must observe the first before mutating
    // anything.
    let (mut engine, storage, wal) = scratch_engine("namesake");
    let mut base = InMemoryCatalog::new();
    save_catalog(&mut base, &mut engine).unwrap();

    let mut a = load_catalog(&mut engine).unwrap();
    let mut b = load_catalog(&mut engine).unwrap();

    let winner = atomic_create(&mut a, &mut engine, "public.namesake", test_columns());
    assert!(winner.is_ok(), "first claim must win: {winner:?}");
    let loser = atomic_create(
        &mut b,
        &mut engine,
        "public.namesake",
        vec![ColumnDef {
            name: "s".to_string(),
            col_type: ColumnType::text(),
            constraints: Vec::new(),
        }],
    );
    let loser_error = loser.expect_err("second claim must observe the first");
    assert!(
        matches!(loser_error.kind(), plomid_core::ErrorKind::AlreadyExists),
        "loser must fail with AlreadyExists: {loser_error:?}"
    );
    assert!(
        !b.has_table("public.namesake"),
        "loser's image must stay clean so its next refresh adopts the winner"
    );

    // The durable image holds exactly the winner's table — the winner's
    // allocation and the winner's shape; the loser's shape never reached
    // the store.
    let restored = load_catalog(&mut engine).unwrap();
    let table = restored.get_table("public.namesake").unwrap();
    assert_eq!(
        table.table_id,
        a.get_table("public.namesake").unwrap().table_id,
        "durable entry is not the winner's allocation"
    );
    assert_eq!(table.columns.len(), 1);
    assert_eq!(table.columns[0].name, "id");

    // Control: after a real drop is durable, a fresh claim succeeds — the
    // probe reads durable state, not an "ever created" memory.
    a.drop_table("public.namesake").unwrap();
    save_catalog(&mut a, &mut engine).unwrap();
    let mut c = load_catalog(&mut engine).unwrap();
    atomic_create(&mut c, &mut engine, "public.namesake", test_columns())
        .expect("re-creation after a durable drop must succeed");

    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&wal);
}
