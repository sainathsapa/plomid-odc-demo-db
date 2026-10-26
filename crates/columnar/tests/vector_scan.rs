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
//! Vector filter-scan integration tests: store-level end-to-end.
//!
//! Each test publishes a real generation, runs `vector_filter_scan`, and
//! compares against an independent scalar accumulation over
//! `read_current_generation_rows` plus a manual match check. Decline paths
//! (unsupported shapes) must return `None` so the caller keeps the scalar
//! path. Timings are in-process figures via `eprintln` (read with
//! `--nocapture`); ratios, not absolutes, are the signal.

use plomid_columnar::vector::{VectorScanRequest, F64_EXACT_INT_RANGE};
use plomid_columnar::{
    read_current_generation_rows, ColumnType, ColumnarPublish, ColumnarStore, FlushConfig,
    PruneOperator, PrunePredicate,
};
use plomid_core::{CatalogVersion, ColumnId, GenerationId, Lsn, ObjectId, SchemaId};
use plomid_storage::{Field, Row};
use plomid_txn::PlomidStorageEngine;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-vscan-it-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_dir_all(path.with_extension("wal"));
}

fn engine_at(dir: &Path) -> PlomidStorageEngine {
    PlomidStorageEngine::create(dir, &dir.join("wal"), 32).expect("create engine")
}

/// Publishes `rows` as generation 1 of object 7 with the given types.
fn publish(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    rows: &[Row],
    types: &[ColumnType],
) {
    let publish = ColumnarPublish::new(
        ObjectId::new(7),
        ColumnarStore::columnar_schema(SchemaId::new(4), CatalogVersion::new(1), types)
            .expect("schema"),
        GenerationId::new(1),
        GenerationId::new(1),
        Lsn::new(1),
    );
    let segment_id = store.allocate_segment_id();
    store
        .flush_rows_with_id(
            engine,
            rows,
            types,
            segment_id,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish");
}

/// Builds the int/int/string fixture: col0 = i*3-500 (NULL every 7th),
/// col1 = 1000-2i (NULL every 11th), col2 = tag text.
fn fixture_rows(n: usize) -> Vec<Row> {
    (0..n)
        .map(|i| {
            Row::new(vec![
                if i % 7 == 0 {
                    Field::Null
                } else {
                    Field::Integer(i as i64 * 3 - 500)
                },
                if i % 11 == 0 {
                    Field::Null
                } else {
                    Field::Integer(1000 - i as i64 * 2)
                },
                Field::String(["a", "b", "c"][i % 3].to_owned()),
            ])
        })
        .collect()
}

fn types() -> Vec<ColumnType> {
    vec![ColumnType::Integer, ColumnType::Integer, ColumnType::String]
}

fn scan_request(
    filter: PrunePredicate,
    columns: &[u64],
    limit_rows: Option<usize>,
) -> VectorScanRequest {
    VectorScanRequest {
        filter,
        columns: columns.iter().map(|c| ColumnId::new(*c)).collect(),
        batch_rows: 0,
        text_ordering_exact: true,
        limit_rows,
    }
}

/// Scalar expectation: read all published rows, keep those matching `keep`
/// over the requested column positions, projecting to those positions.
fn scalar_filtered(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    columns: &[u64],
    keep: &dyn Fn(&[Field]) -> bool,
) -> Vec<Vec<Field>> {
    let scan = read_current_generation_rows(store, engine, ObjectId::new(7), None, None)
        .expect("read")
        .expect("generation");
    // The published column order is positional: requested positions index
    // directly into each row's fields.
    scan.rows
        .iter()
        .filter(|row| keep(row.fields()))
        .map(|row| {
            columns
                .iter()
                .map(|c| row.fields()[*c as usize].clone())
                .collect()
        })
        .collect()
}

fn scan_fields(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    request: &VectorScanRequest,
) -> Vec<Vec<Field>> {
    let scan =
        plomid_columnar::vector::vector_filter_scan(store, engine, ObjectId::new(7), request)
            .expect("scan")
            .expect("supported shape must engage");
    assert_eq!(
        scan.column_ids, request.columns,
        "scan must report the requested column shape"
    );
    scan.rows.iter().map(|row| row.fields().to_vec()).collect()
}

#[test]
fn vector_scan_selective_int_matches_scalar() {
    let root = scratch("int");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    let rows = fixture_rows(5000);
    publish(&store, &mut engine, &rows, &types());
    // WHERE col0 > 100 AND col1 <= 0: selective (~2.4k of 5k), NULLs excluded.
    let filter = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::Greater,
        Field::Integer(100),
    )
    .and(PrunePredicate::compare(
        ColumnId::new(1),
        PruneOperator::LessOrEqual,
        Field::Integer(0),
    ));
    let request = scan_request(filter, &[0, 2], None);
    let got = scan_fields(&store, &mut engine, &request);
    let expect = scalar_filtered(&store, &mut engine, &[0, 2], &|fields| {
        matches!(fields[0], Field::Integer(v) if v > 100)
            && matches!(fields[1], Field::Integer(v) if v <= 0)
    });
    assert_eq!(got, expect);
    assert!(!got.is_empty());
    eprintln!("vector_scan selective int: {} of 5000 rows", got.len());
    cleanup(&root);
}

#[test]
fn vector_scan_text_null_and_compound_shapes() {
    let root = scratch("shapes");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    publish(&store, &mut engine, &fixture_rows(5000), &types());
    // Text equality (byte-ordered, caller-verified text family).
    let filter = PrunePredicate::compare(
        ColumnId::new(2),
        PruneOperator::Equal,
        Field::String("b".into()),
    );
    let got = scan_fields(&store, &mut engine, &scan_request(filter, &[0, 1, 2], None));
    let expect = scalar_filtered(
        &store,
        &mut engine,
        &[0, 1, 2],
        &|fields| matches!(&fields[2], Field::String(s) if s == "b"),
    );
    assert_eq!(got, expect);
    // IS NULL over the integer column.
    let filter = PrunePredicate::is_null(ColumnId::new(0));
    let got = scan_fields(&store, &mut engine, &scan_request(filter, &[1], None));
    let expect = scalar_filtered(&store, &mut engine, &[1], &|fields| {
        matches!(fields[0], Field::Null)
    });
    assert_eq!(got, expect);
    // OR of disjoint ranges plus NOT: (col0 < -400 OR col0 >= 14000) AND col0 IS NOT NULL.
    // (NOT over IS NULL normalizes through duals; NULLs never match range leaves anyway.)
    let filter =
        PrunePredicate::compare(ColumnId::new(0), PruneOperator::Less, Field::Integer(-400))
            .or(PrunePredicate::compare(
                ColumnId::new(0),
                PruneOperator::GreaterOrEqual,
                Field::Integer(14000),
            ))
            .and(PrunePredicate::is_not_null(ColumnId::new(0)));
    let got = scan_fields(&store, &mut engine, &scan_request(filter, &[0], None));
    let expect = scalar_filtered(
        &store,
        &mut engine,
        &[0],
        &|fields| matches!(fields[0], Field::Integer(v) if !(-400..14000).contains(&v)),
    );
    assert_eq!(got, expect);
    cleanup(&root);
}

#[test]
fn vector_scan_limit_truncates_in_scan_order() {
    let root = scratch("limit");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    publish(&store, &mut engine, &fixture_rows(5000), &types());
    let filter = PrunePredicate::compare(
        ColumnId::new(1),
        PruneOperator::LessOrEqual,
        Field::Integer(0),
    );
    let full = scan_fields(
        &store,
        &mut engine,
        &scan_request(filter.clone(), &[0, 1], None),
    );
    assert!(full.len() > 20);
    let capped = scan_fields(
        &store,
        &mut engine,
        &scan_request(filter, &[0, 1], Some(20)),
    );
    assert_eq!(capped, full[..20].to_vec());
    cleanup(&root);
}

#[test]
fn vector_scan_decline_shapes_return_none() {
    let root = scratch("decline");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    publish(&store, &mut engine, &fixture_rows(1000), &types());
    // Decline is `Ok(None)`, never `Err`.
    let scan =
        |store: &ColumnarStore, engine: &mut PlomidStorageEngine, request: &VectorScanRequest| {
            plomid_columnar::vector::vector_filter_scan(store, engine, ObjectId::new(7), request)
                .expect("decline is Ok(None), never Err")
        };
    // Integer literal outside the f64-exact range: the scalar f64 comparison
    // could disagree, so the whole request declines.
    let huge = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::Greater,
        Field::Integer(F64_EXACT_INT_RANGE + 1),
    );
    assert!(scan(&store, &mut engine, &scan_request(huge, &[0], None)).is_none());
    // Unknown predicate column.
    let missing =
        PrunePredicate::compare(ColumnId::new(99), PruneOperator::Greater, Field::Integer(0));
    assert!(scan(&store, &mut engine, &scan_request(missing, &[0], None)).is_none());
    // Unknown output column: the scalar path owns the error.
    let ok_filter = PrunePredicate::is_not_null(ColumnId::new(0));
    assert!(scan(
        &store,
        &mut engine,
        &scan_request(ok_filter.clone(), &[99], None)
    )
    .is_none());
    // Empty output width.
    assert!(scan(&store, &mut engine, &scan_request(ok_filter, &[], None)).is_none());
    // String leaf without the caller-verified text flag.
    let text = PrunePredicate::compare(
        ColumnId::new(2),
        PruneOperator::Equal,
        Field::String("a".into()),
    );
    let mut request = scan_request(text, &[2], None);
    request.text_ordering_exact = false;
    assert!(scan(&store, &mut engine, &request).is_none());
    // Cross-type leaf (integer column against a text literal).
    let cross = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::Equal,
        Field::String("7".into()),
    );
    assert!(scan(&store, &mut engine, &scan_request(cross, &[0], None)).is_none());
    // Unpublished object.
    let filter = PrunePredicate::is_not_null(ColumnId::new(0));
    let request = scan_request(filter, &[0], None);
    assert!(plomid_columnar::vector::vector_filter_scan(
        &store,
        &mut engine,
        ObjectId::new(4242),
        &request
    )
    .expect("read")
    .is_none());
    cleanup(&root);
}

#[test]
fn vector_scan_empty_match_and_zero_limit() {
    let root = scratch("empty");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    publish(&store, &mut engine, &fixture_rows(1000), &types());
    // Impossible predicate: empty match set, still the requested shape.
    // (The bound stays inside the f64-exact range so the shape engages;
    // no fixture value reaches it.)
    let filter = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::Greater,
        Field::Integer(1_000_000),
    );
    let scan = plomid_columnar::vector::vector_filter_scan(
        &store,
        &mut engine,
        ObjectId::new(7),
        &scan_request(filter, &[0, 1], None),
    )
    .expect("read")
    .expect("supported");
    assert!(scan.rows.is_empty());
    assert_eq!(scan.column_ids, vec![ColumnId::new(0), ColumnId::new(1)]);
    // Zero limit: empty without reading.
    let filter = PrunePredicate::is_not_null(ColumnId::new(0));
    let scan = plomid_columnar::vector::vector_filter_scan(
        &store,
        &mut engine,
        ObjectId::new(7),
        &scan_request(filter, &[0], Some(0)),
    )
    .expect("read")
    .expect("supported");
    assert!(scan.rows.is_empty());
    cleanup(&root);
}

/// Scale figure: selective vector scan vs scalar read + manual filter at
/// 100k rows. Reports best-of-3 via eprintln; asserts identical results.
/// Release profile preferred (`cargo test --release -p plomid-columnar
/// --test vector_scan timing -- --nocapture`).
#[test]
#[ignore = "scale: runs in CI via make test-scale, skipped locally to keep MacBook cool"]
fn vector_scan_timing() {
    let root = scratch("timing");
    let mut engine = engine_at(&root);
    let store = ColumnarStore::open(&root).expect("store");
    let rows = fixture_rows(100_000);
    publish(&store, &mut engine, &rows, &types());
    // ~0.14% selectivity: col0 in [14000, 14500) (~142 of 100k).
    let filter = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::GreaterOrEqual,
        Field::Integer(14000),
    )
    .and(PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::Less,
        Field::Integer(14500),
    ));
    let request = scan_request(filter.clone(), &[0, 1, 2], None);
    // Warmup + correctness against the scalar path.
    let expect = scalar_filtered(
        &store,
        &mut engine,
        &[0, 1, 2],
        &|fields| matches!(fields[0], Field::Integer(v) if (14000..14500).contains(&v)),
    );
    let mut best_vector = f64::INFINITY;
    for _ in 0..3 {
        let started = Instant::now();
        let scan = plomid_columnar::vector::vector_filter_scan(
            &store,
            &mut engine,
            ObjectId::new(7),
            &request,
        )
        .expect("read")
        .expect("supported");
        best_vector = best_vector.min(started.elapsed().as_secs_f64() * 1000.0);
        let got: Vec<Vec<Field>> = scan.rows.iter().map(|row| row.fields().to_vec()).collect();
        assert_eq!(got, expect);
    }
    let mut best_scalar = f64::INFINITY;
    for _ in 0..3 {
        let started = Instant::now();
        let scan = read_current_generation_rows(&store, &mut engine, ObjectId::new(7), None, None)
            .expect("read")
            .expect("generation");
        let kept: Vec<&Row> = scan
            .rows
            .iter()
            .filter(
                |row| matches!(row.fields()[0], Field::Integer(v) if (14000..14500).contains(&v)),
            )
            .collect();
        std::hint::black_box(kept.len());
        best_scalar = best_scalar.min(started.elapsed().as_secs_f64() * 1000.0);
    }
    eprintln!(
        "vector_scan 100k selective (~{} matches): vector best-of-3 {best_vector:.1}ms vs scalar best-of-3 {best_scalar:.1}ms ({:.2}x)",
        expect.len(),
        best_scalar / best_vector.max(0.001),
    );
    // LIMIT pushdown: the vector scan stops after 10 matches (early
    // segments only) while the scalar read materializes everything.
    let capped = scan_request(filter, &[0, 1, 2], Some(10));
    let mut best_capped = f64::INFINITY;
    for _ in 0..3 {
        let started = Instant::now();
        let scan = plomid_columnar::vector::vector_filter_scan(
            &store,
            &mut engine,
            ObjectId::new(7),
            &capped,
        )
        .expect("read")
        .expect("supported");
        best_capped = best_capped.min(started.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(scan.rows.len(), 10);
        let got: Vec<Vec<Field>> = scan.rows.iter().map(|row| row.fields().to_vec()).collect();
        assert_eq!(got, expect[..10].to_vec());
    }
    eprintln!(
        "vector_scan 100k selective LIMIT 10: vector best-of-3 {best_capped:.1}ms vs scalar-full best-of-3 {best_scalar:.1}ms ({:.2}x)",
        best_scalar / best_capped.max(0.001),
    );
    cleanup(&root);
}
