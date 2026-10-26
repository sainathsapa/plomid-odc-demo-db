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
// Moved from src/store.rs inline #[cfg(test)].
mod space_wiring_tests {
    use plomid_columnar::column::ColumnType;
    use plomid_columnar::compaction::{compact, CompactionRequest};

    use plomid_columnar::flush::FlushConfig;

    use plomid_columnar::store::*;
    use plomid_core::{CatalogVersion, DatabaseId, GenerationId, Lsn, SchemaId, TableId};
    use plomid_core::{ObjectId, TableIdentity};
    use plomid_storage::StorageEngine;
    use plomid_storage::{Field, Row};

    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch(label: &str) -> std::path::PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "plomid-space-wiring-{label}-{}-{id}",
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
        generation: u64,
        base: i64,
    ) -> Vec<Row> {
        let rows: Vec<Row> = (0..50)
            .map(|i| Row::new(vec![Field::Integer(base + i)]))
            .collect();
        let types = vec![ColumnType::Integer];
        let coordinates = ColumnarPublish::new(
            ObjectId::new(1),
            ColumnarStore::columnar_schema(SchemaId::new(1), CatalogVersion::new(1), &types)
                .expect("schema"),
            GenerationId::new(generation),
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
        rows
    }

    #[test]
    fn flush_feeds_the_space_ledger() {
        let dir = scratch("flush");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        let table = ColumnarStore::space_table_key(identity());
        assert_eq!(table, "db1/schema1/table1");
        assert_eq!(store.space_account(&table), None);

        publish(&store, &mut engine, 1, 0);
        let account = store.space_account(&table).expect("tracked after flush");
        assert_eq!(account.live_rows, 50);
        assert!(account.live_bytes > 0);
        assert_eq!(account.dead_bytes, 0);
        assert_eq!(store.space_debt_order(), vec![table.clone()]);
        // No debt yet: the advisory scheduler must not request maintenance.
        assert!(!store.space_maintenance_due(&table));
        assert!(!store.space_maintenance_due("db1/schema1/table-missing"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn compact_folds_released_and_reclaimed_into_the_ledger() {
        let dir = scratch("compact");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        let table = ColumnarStore::space_table_key(identity());
        let mut rows = publish(&store, &mut engine, 1, 0);
        rows.extend(publish(&store, &mut engine, 2, 1000));

        let (active, last_committed) = engine.mvcc_safety().expect("safety");
        let request = CompactionRequest {
            identity: identity(),
            object_id: ObjectId::new(1),
            column_types: vec![ColumnType::Integer],
            config: FlushConfig::default(),
            generation: GenerationId::new(3),
            storage_generation: GenerationId::new(1),
            checkpoint_lsn: Lsn::new(1),
        };
        let outcome = compact(
            &store,
            &mut engine,
            &request,
            &rows,
            &active,
            last_committed,
            plomid_columnar::compaction::CompactionFailPoint::None,
            true,
        )
        .expect("compact");
        assert!(outcome.published.is_some());

        // The merged generation is the live set; superseded inputs were
        // released and their slices reclaimed, so no debt remains.
        let account = store.space_account(&table).expect("tracked after compact");
        assert_eq!(account.live_rows, 100);
        assert_eq!(account.dead_rows, 0);
        assert_eq!(account.dead_bytes, 0);
        assert!(account.live_bytes > 0);
        assert!(!store.space_maintenance_due(&table));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn orphan_reclaim_helper_is_callable_without_signature_breaks() {
        let dir = scratch("orphan");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        let table = ColumnarStore::space_table_key(identity());
        // Nothing published: the scan finds nothing and the ledger stays
        // untracked. `vacuum_table` calls this with the store and engine it
        // already holds, so no caller signature changes.
        assert_eq!(store.reclaim_orphan_slices(&mut engine, &table), (0, 0));
        assert_eq!(store.space_account(&table), None);

        publish(&store, &mut engine, 1, 0);
        // No generation was released, so nothing is orphaned either.
        assert_eq!(store.reclaim_orphan_slices(&mut engine, &table), (0, 0));
        let account = store.space_account(&table).expect("tracked after flush");
        assert_eq!(account.orphan_bytes, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn olap_scan_consults_the_process_wide_buffer_manager() {
        use plomid_columnar::compaction::read_current_generation_rows;

        let dir = scratch("buffer");
        let mut engine =
            plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
        let store = ColumnarStore::open(&dir).expect("store");
        publish(&store, &mut engine, 1, 0);
        let scan = read_current_generation_rows(&store, &mut engine, ObjectId::new(1), None, None)
            .expect("scan")
            .expect("published");
        assert_eq!(scan.rows.len(), 50);

        // Every segment the OLAP scan read is now known to the process-wide
        // manager (admitted as Streaming on first touch; reuse promotes via
        // record_hit). The key helper is deterministic per segment.
        let buffer = plomid_htap::global_buffer_manager();
        assert_eq!(
            plomid_columnar::compaction::olap_buffer_key(plomid_core::SegmentId::new(1)),
            "columnar-segment-1"
        );
        assert!(buffer.contains("columnar-segment-1"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
