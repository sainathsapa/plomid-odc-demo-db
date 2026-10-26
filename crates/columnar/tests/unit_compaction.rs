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
// Moved from src/compaction.rs inline #[cfg(test)].
mod release_rule_tests {

    use plomid_columnar::compaction::release_provable;

    #[test]
    fn empty_active_set_releases_vacuously() {
        assert!(release_provable(&[], 100));
    }

    #[test]
    fn transactions_below_the_seal_pin() {
        assert!(!release_provable(&[50], 100));
        assert!(!release_provable(&[99, 100, 101], 100));
    }

    #[test]
    fn transactions_at_or_above_the_seal_release() {
        assert!(release_provable(&[100], 100));
        assert!(release_provable(&[100, 101, 500], 100));
    }
}

mod delta_scan_tests {
    use plomid_columnar::column::ColumnType;
    use plomid_columnar::compaction::*;
    use plomid_columnar::delta::{
        commit_watermark, delta_key, global_delta_store, note_deltas_published, DeltaRow,
    };
    use plomid_columnar::flush::FlushConfig;

    use plomid_columnar::store::ColumnarFailPoint;
    use plomid_columnar::store::{ColumnarPublish, ColumnarStore};
    use plomid_core::{
        CatalogVersion, ColumnId, DatabaseId, Lsn, SchemaId, TableId, TableIdentity,
    };
    use plomid_core::{GenerationId, ObjectId, Result};

    use plomid_storage::{Field, Row};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch(label: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "plomid-delta-scan-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("scratch");
        path
    }

    fn identity() -> TableIdentity {
        TableIdentity::new(DatabaseId::new(1), SchemaId::new(1), TableId::new(1))
    }

    fn publish(
        store: &ColumnarStore,
        engine: &mut plomid_txn::PlomidStorageEngine,
        values: &[i64],
    ) {
        let rows: Vec<Row> = values
            .iter()
            .map(|value| Row::new(vec![Field::Integer(*value)]))
            .collect();
        let types = vec![ColumnType::Integer];
        let coordinates = ColumnarPublish::new(
            ObjectId::new(1),
            ColumnarStore::columnar_schema(SchemaId::new(1), CatalogVersion::new(1), &types)
                .expect("schema"),
            GenerationId::new(1),
            GenerationId::new(1),
            Lsn::new(1),
        )
        .with_identity(identity());
        let segment = store.allocate_segment_id();
        store
            .flush_rows_with_id(
                engine,
                &rows,
                &types,
                segment,
                &FlushConfig::default(),
                &coordinates,
                ColumnarFailPoint::None,
            )
            .expect("publish");
    }

    fn scan_values(scan: Result<Option<ColumnarScan>>) -> Vec<i64> {
        scan.expect("scan")
            .expect("published")
            .rows
            .into_iter()
            .map(|row| match row.fields() {
                [Field::Integer(value)] => *value,
                fields => panic!("single-column scan, got {fields:?}"),
            })
            .collect()
    }

    #[test]
    fn variant_merges_visible_deltas_over_published_generation() {
        let dir = scratch("merge");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        publish(&store, &mut engine, &[10, 20, 30]);

        let key = delta_key(&dir, ObjectId::new(1));
        let watermark = commit_watermark(&engine);
        let deltas = global_delta_store();
        // Insert (no pre-image: appends), delete with pre-image (hides the
        // main row), and a future commit invisible to current snapshots.
        deltas.insert(
            &key,
            DeltaRow::upsert(101, vec![Field::Integer(40)], watermark),
        );
        deltas.insert(
            &key,
            DeltaRow::delete_row(102, vec![Field::Integer(20)], watermark),
        );
        deltas.insert(
            &key,
            DeltaRow::upsert(
                103,
                vec![Field::Integer(50)],
                watermark.saturating_add(1000),
            ),
        );

        // At the watermark both confirmed deltas apply: main order kept,
        // the insert appended, the tombstoned row gone, the future row
        // absent.
        let merged = read_current_generation_rows_with_deltas(
            &store,
            &mut engine,
            ObjectId::new(1),
            &key,
            watermark,
            None,
            None,
        );
        assert_eq!(scan_values(merged), vec![10, 30, 40]);

        // The original entry point delegates with the same key and the live
        // watermark, so it observes exactly the same merged image.
        let direct =
            read_current_generation_rows(&store, &mut engine, ObjectId::new(1), None, None);
        assert_eq!(scan_values(direct), vec![10, 30, 40]);

        // Publishing absorbs the deltas: the scan is the main image again.
        note_deltas_published(&key);
        let absorbed = read_current_generation_rows_with_deltas(
            &store,
            &mut engine,
            ObjectId::new(1),
            &key,
            watermark.saturating_add(1000),
            None,
            None,
        );
        assert_eq!(scan_values(absorbed), vec![10, 20, 30]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn variant_projects_deltas_to_requested_columns() {
        let dir = scratch("project");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        let rows: Vec<Row> = vec![(1, 10), (2, 20)]
            .into_iter()
            .map(|(left, right)| Row::new(vec![Field::Integer(left), Field::Integer(right)]))
            .collect();
        let types = vec![ColumnType::Integer, ColumnType::Integer];
        let coordinates = ColumnarPublish::new(
            ObjectId::new(1),
            ColumnarStore::columnar_schema(SchemaId::new(1), CatalogVersion::new(1), &types)
                .expect("schema"),
            GenerationId::new(1),
            GenerationId::new(1),
            Lsn::new(1),
        )
        .with_identity(identity());
        let segment = store.allocate_segment_id();
        store
            .flush_rows_with_id(
                &mut engine,
                &rows,
                &types,
                segment,
                &FlushConfig::default(),
                &coordinates,
                ColumnarFailPoint::None,
            )
            .expect("publish");

        let key = delta_key(&dir, ObjectId::new(1));
        let watermark = commit_watermark(&engine);
        let deltas = global_delta_store();
        // Full-width staged images; the scan below requests only column 1.
        deltas.insert(
            &key,
            DeltaRow::upsert(201, vec![Field::Integer(3), Field::Integer(30)], watermark),
        );
        deltas.insert(
            &key,
            DeltaRow::upsert(202, vec![Field::Integer(2), Field::Integer(25)], watermark)
                .with_prev(vec![Field::Integer(2), Field::Integer(20)]),
        );

        let requested = vec![ColumnId::new(1)];
        let merged = read_current_generation_rows_with_deltas(
            &store,
            &mut engine,
            ObjectId::new(1),
            &key,
            watermark,
            None,
            Some(&requested),
        )
        .expect("scan")
        .expect("published");
        assert_eq!(merged.column_ids, requested);
        let values: Vec<i64> = merged
            .rows
            .into_iter()
            .map(|row| match row.fields() {
                [Field::Integer(value)] => *value,
                fields => panic!("projected scan, got {fields:?}"),
            })
            .collect();
        // The update replaced its pre-image in place; the insert appended.
        assert_eq!(values, vec![10, 25, 30]);
        note_deltas_published(&key);
        std::fs::remove_dir_all(&dir).ok();
    }
}
