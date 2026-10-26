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
// Throttling + cancellation — compaction never starves OLTP.

use plomid_columnar::{
    compact, compact_gated, mvcc_safety, read_generation_rows_gated, ColumnType, ColumnarPublish,
    ColumnarStore, CompactionFailPoint, CompactionRequest, FlushConfig,
};
use plomid_core::{
    CatalogVersion, DatabaseId, ErrorKind, GenerationId, Lsn, ObjectId, SchemaId, TableId,
    TableIdentity,
};
use plomid_htap::{CancelToken, CompactionGate, CompactionThrottle, Deadline};
use plomid_storage::{Field, Row};
use plomid_txn::PlomidStorageEngine;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(3), SchemaId::new(4), TableId::new(1))
}
fn object() -> ObjectId {
    ObjectId::new(1)
}
fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-compaction-gated-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}
fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_dir_all(path);
}
fn int_rows(values: &[i64]) -> Vec<Row> {
    values
        .iter()
        .map(|v| Row::new(vec![Field::Integer(*v)]))
        .collect()
}
fn int_range(n: i64) -> Vec<Row> {
    (0..n).map(|v| Row::new(vec![Field::Integer(v)])).collect()
}
fn publish_generation(
    store: &ColumnarStore,
    engine: &mut PlomidStorageEngine,
    generation: u64,
    rows: &[Row],
) {
    let publish = ColumnarPublish::new(
        object(),
        ColumnarStore::columnar_schema(
            SchemaId::new(4),
            CatalogVersion::new(1),
            &[ColumnType::Integer],
        )
        .expect("schema"),
        GenerationId::new(generation),
        GenerationId::new(1),
        Lsn::new(1),
    )
    .with_identity(identity());
    let segment_id = store.allocate_segment_id();
    store
        .flush_rows_with_id(
            engine,
            rows,
            &[ColumnType::Integer],
            segment_id,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish generation");
}
fn request(generation: u64) -> CompactionRequest {
    CompactionRequest {
        identity: identity(),
        object_id: object(),
        column_types: vec![ColumnType::Integer],
        config: FlushConfig::default(),
        generation: GenerationId::new(generation),
        storage_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
    }
}
fn setup(label: &str) -> (PathBuf, PlomidStorageEngine, ColumnarStore) {
    let dir = scratch(label);
    let engine = PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("engine");
    let store = ColumnarStore::open(&dir).expect("store");
    (dir, engine, store)
}
fn current_generation(store: &ColumnarStore) -> GenerationId {
    store
        .generations()
        .reader()
        .expect("reader")
        .object(object())
        .expect("record")
        .current_generation
}

#[test]
fn paused_gate_refuses_at_entry_with_no_visible_change() {
    let (dir, mut engine, store) = setup("paused");
    let result = (|| {
        publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
        publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
        let before = current_generation(&store);
        assert_eq!(before, GenerationId::new(6));
        let (active, last) = mvcc_safety(&engine)?;
        let gate = CompactionGate::paused();
        let err = compact_gated(
            &store,
            &mut engine,
            &request(7),
            &int_rows(&[1, 2]),
            &active,
            last,
            CompactionFailPoint::None,
            true,
            &gate,
        )
        .expect_err("paused must abort");
        assert_eq!(err.kind(), ErrorKind::Aborted);
        // No visible change: same current generation, no generation 7.
        assert_eq!(current_generation(&store), GenerationId::new(6));
        assert!(store
            .generations()
            .reader()?
            .generation(GenerationId::new(7))
            .is_none());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn cancelled_gate_aborts_before_publish_with_no_visible_change() {
    let (dir, mut engine, store) = setup("cancel");
    let result = (|| {
        publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
        publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
        let cancel = CancelToken::new();
        cancel.cancel();
        let gate = CompactionGate::new(CompactionThrottle::full(), cancel, Deadline::never());
        let (active, last) = mvcc_safety(&engine)?;
        let err = compact_gated(
            &store,
            &mut engine,
            &request(7),
            &int_rows(&[1, 2]),
            &active,
            last,
            CompactionFailPoint::None,
            true,
            &gate,
        )
        .expect_err("cancelled must abort");
        assert_eq!(err.kind(), ErrorKind::Aborted);
        assert_eq!(current_generation(&store), GenerationId::new(6));
        assert!(store
            .generations()
            .reader()?
            .generation(GenerationId::new(7))
            .is_none());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn expired_deadline_aborts_before_publish() {
    let (dir, mut engine, store) = setup("deadline");
    let result = (|| {
        publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
        publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
        let gate = CompactionGate::new(
            CompactionThrottle::full(),
            CancelToken::new(),
            Deadline::expired(),
        );
        let (active, last) = mvcc_safety(&engine)?;
        let err = compact_gated(
            &store,
            &mut engine,
            &request(7),
            &int_rows(&[1, 2]),
            &active,
            last,
            CompactionFailPoint::None,
            true,
            &gate,
        )
        .expect_err("expired must abort");
        assert_eq!(err.kind(), ErrorKind::Aborted);
        assert_eq!(current_generation(&store), GenerationId::new(6));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn degraded_gate_yields_at_least_once_and_still_compacts() {
    let (dir, mut engine, store) = setup("degraded");
    let result = (|| {
        // 600 rows: chunk loop (256-row slices) hits yield points at 256, 512.
        let rows = int_range(600);
        publish_generation(&store, &mut engine, 5, &rows[..300]);
        publish_generation(&store, &mut engine, 6, &rows);
        let gate = CompactionGate::new(
            CompactionThrottle::degraded(),
            CancelToken::new(),
            Deadline::never(),
        );
        let (active, last) = mvcc_safety(&engine)?;
        let outcome = compact_gated(
            &store,
            &mut engine,
            &request(7),
            &rows,
            &active,
            last,
            CompactionFailPoint::None,
            true,
            &gate,
        )?;
        assert_eq!(outcome.output_rows, 600);
        assert!(
            gate.yield_count() >= 1,
            "degraded must yield at least once, got {}",
            gate.yield_count()
        );
        assert_eq!(current_generation(&store), GenerationId::new(7));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn open_gate_matches_ungated_behavior() {
    // Twin fixtures: identical setups, one ungated, one open-gated.
    let (dir_a, mut engine_a, store_a) = setup("twin-a");
    let (dir_b, mut engine_b, store_b) = setup("twin-b");
    let result = (|| {
        for (store, engine) in [(&store_a, &mut engine_a), (&store_b, &mut engine_b)] {
            publish_generation(store, engine, 5, &int_rows(&[1]));
            publish_generation(store, engine, 5 + 1, &int_rows(&[1, 2]));
        }
        let (active_a, last_a) = mvcc_safety(&engine_a)?;
        let outcome_a = compact(
            &store_a,
            &mut engine_a,
            &request(7),
            &int_rows(&[1, 2]),
            &active_a,
            last_a,
            CompactionFailPoint::None,
            true,
        )?;
        let (active_b, last_b) = mvcc_safety(&engine_b)?;
        let gate = CompactionGate::open();
        let outcome_b = compact_gated(
            &store_b,
            &mut engine_b,
            &request(7),
            &int_rows(&[1, 2]),
            &active_b,
            last_b,
            CompactionFailPoint::None,
            true,
            &gate,
        )?;
        assert_eq!(outcome_a.visible_rows, outcome_b.visible_rows);
        assert_eq!(outcome_a.output_rows, outcome_b.output_rows);
        assert_eq!(outcome_a.input_rows, outcome_b.input_rows);
        assert_eq!(outcome_a.released, outcome_b.released);
        assert_eq!(outcome_a.retained_inputs, outcome_b.retained_inputs);
        assert_eq!(gate.yield_count(), 0, "open gate never yields");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir_a);
    cleanup(&dir_b);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn gated_read_aborts_when_paused_and_reads_when_open() {
    let (dir, mut engine, store) = setup("read-gated");
    let result = (|| {
        publish_generation(&store, &mut engine, 5, &int_rows(&[1, 2, 3]));
        // Paused refuses before any I/O.
        let paused = CompactionGate::paused();
        let err = read_generation_rows_gated(
            &store,
            &mut engine,
            store.generations(),
            object(),
            GenerationId::new(5),
            &paused,
        )
        .expect_err("paused read must abort");
        assert_eq!(err.kind(), ErrorKind::Aborted);
        // Open reads exactly the published rows.
        let open = CompactionGate::open();
        let rows = read_generation_rows_gated(
            &store,
            &mut engine,
            store.generations(),
            object(),
            GenerationId::new(5),
            &open,
        )?;
        assert_eq!(rows, int_rows(&[1, 2, 3]));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn gc_gated_aborts_when_paused_and_collects_when_open() {
    let (dir, mut engine, store) = setup("gc-gated");
    let result = (|| {
        publish_generation(&store, &mut engine, 5, &int_rows(&[1]));
        publish_generation(&store, &mut engine, 6, &int_rows(&[1, 2]));
        // Paused aborts before deleting anything.
        let paused = CompactionGate::paused();
        let err = store
            .generations()
            .gc_gated(&paused)
            .expect_err("paused gc must abort");
        assert_eq!(err.kind(), ErrorKind::Aborted);
        store.generations().validate()?;
        // Open collects normally (may reclaim nothing while retained; must not error).
        let open = CompactionGate::open();
        let _ = store.generations().gc_gated(&open)?;
        store.generations().validate()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
#[ignore = "benchmark: wall-clock overhead assert over compaction; runs in CI via make test-scale"]
fn gated_vs_ungated_compact_overhead_is_small() {
    // Same 2000-row fixture, twin roots, timed back-to-back.
    let rows = int_range(2000);
    let (dir_a, mut engine_a, store_a) = setup("bench-ungated");
    publish_generation(&store_a, &mut engine_a, 5, &rows[..1000]);
    publish_generation(&store_a, &mut engine_a, 6, &rows);
    let (active_a, last_a) = mvcc_safety(&engine_a).expect("safety");
    let start = std::time::Instant::now();
    compact(
        &store_a,
        &mut engine_a,
        &request(7),
        &rows,
        &active_a,
        last_a,
        CompactionFailPoint::None,
        false,
    )
    .expect("ungated");
    let ungated = start.elapsed();

    let (dir_b, mut engine_b, store_b) = setup("bench-gated");
    publish_generation(&store_b, &mut engine_b, 5, &rows[..1000]);
    publish_generation(&store_b, &mut engine_b, 6, &rows);
    let (active_b, last_b) = mvcc_safety(&engine_b).expect("safety");
    let gate = CompactionGate::open();
    let start = std::time::Instant::now();
    compact_gated(
        &store_b,
        &mut engine_b,
        &request(7),
        &rows,
        &active_b,
        last_b,
        CompactionFailPoint::None,
        false,
        &gate,
    )
    .expect("gated");
    let gated = start.elapsed();

    let overhead = if ungated.as_nanos() == 0 {
        0.0
    } else {
        (gated.as_secs_f64() - ungated.as_secs_f64()) / ungated.as_secs_f64() * 100.0
    };
    eprintln!(
        "37C BENCHMARK gated-vs-ungated compact (2000 rows): ungated={:?} gated={:?} overhead={:.2}%",
        ungated, gated, overhead
    );
    // The open gate adds only a handful of atomic/deadline checkpoints; it
    // must not double the cost. 100% is a generous ceiling for CI noise.
    assert!(
        overhead < 100.0,
        "overhead {overhead:.2}% exceeds 100% (ungated={ungated:?} gated={gated:?})"
    );
    cleanup(&dir_a);
    cleanup(&dir_b);
}
