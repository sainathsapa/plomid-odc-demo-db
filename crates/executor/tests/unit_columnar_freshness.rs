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
// Moved from src/columnar_freshness.rs inline #[cfg(test)].
mod tests {
    use plomid_core::GenerationId;
    use plomid_executor::columnar_freshness::*;

    fn key(table: &str) -> String {
        freshness_key(std::path::Path::new("/root"), "db", table)
    }

    #[test]
    fn absent_table_is_not_fresh() {
        let key = key("never-written");
        forget_table(&key);
        assert_eq!(clean_generation(&key), None);
        assert_eq!(writes_since_boot(&key), 0);
    }

    #[test]
    fn dirty_write_clears_freshness() {
        let key = key("dirty-write");
        forget_table(&key);
        assert!(try_mark_published(&key, GenerationId::new(7), 0, None));
        assert_eq!(clean_generation(&key), Some(GenerationId::new(7)));
        mark_table_dirty(&key);
        assert_eq!(clean_generation(&key), None);
        assert_eq!(writes_since_boot(&key), 1);
        forget_table(&key);
    }

    #[test]
    fn concurrent_write_inside_build_window_blocks_marking() {
        let key = key("build-race");
        forget_table(&key);
        mark_table_dirty(&key);
        mark_table_dirty(&key);
        let writes_before = writes_since_boot(&key);
        assert_eq!(writes_before, 2);
        // A commit landing now must defeat the mark even though the check
        // runs after it.
        mark_table_dirty(&key);
        assert!(!try_mark_published(
            &key,
            GenerationId::new(9),
            writes_before,
            None
        ));
        assert_eq!(clean_generation(&key), None);
        // A clean build window marks exactly the published generation.
        let writes_before = writes_since_boot(&key);
        assert!(try_mark_published(
            &key,
            GenerationId::new(10),
            writes_before,
            None
        ));
        assert_eq!(clean_generation(&key), Some(GenerationId::new(10)));
        forget_table(&key);
    }

    #[test]
    fn forget_drops_claims() {
        let key = key("dropped");
        forget_table(&key);
        assert!(try_mark_published(&key, GenerationId::new(3), 0, None));
        forget_table(&key);
        assert_eq!(clean_generation(&key), None);
    }

    #[test]
    fn publish_absorbs_delta_store_only_on_success() {
        use plomid_columnar::delta::{global_delta_store, DeltaRow};
        use plomid_storage::Field;
        let key = key("delta-absorb");
        let dtable = "htap-gate-absorb-scratch";
        forget_table(&key);
        global_delta_store().clear(dtable);
        // A clean-window publish absorbs the seeded delta and certifies
        // coverage in the same critical section.
        global_delta_store().insert(dtable, DeltaRow::upsert(1, vec![Field::Integer(1)], 1));
        assert_eq!(global_delta_store().row_count(dtable), 1);
        assert!(try_mark_published(
            &key,
            GenerationId::new(30),
            0,
            Some(dtable)
        ));
        assert_eq!(global_delta_store().row_count(dtable), 0);
        assert!(delta_covered(&key));
        // A commit inside the next window defeats the mark: coverage drops
        // and the seeded delta survives for the next pass to absorb.
        global_delta_store().insert(dtable, DeltaRow::upsert(2, vec![Field::Integer(2)], 2));
        let writes_before = writes_since_boot(&key);
        mark_table_dirty(&key);
        assert!(!try_mark_published(
            &key,
            GenerationId::new(31),
            writes_before,
            Some(dtable)
        ));
        assert!(!delta_covered(&key));
        assert_eq!(global_delta_store().row_count(dtable), 1);
        global_delta_store().clear(dtable);
        forget_table(&key);
    }

    #[test]
    fn bootstrap_mark_with_nonzero_writes_fails_closed() {
        // No entry plus a nonzero baseline is incoherent (the counter reads
        // zero when absent): the mark must fail rather than certify coverage.
        let key = key("bootstrap-incoherent");
        forget_table(&key);
        assert!(!try_mark_published(&key, GenerationId::new(11), 3, None));
        assert_eq!(clean_generation(&key), None);
        assert!(!delta_covered(&key));
        forget_table(&key);
    }

    #[test]
    fn coverage_lifecycle_publish_dirty_fail_forget() {
        let key = key("coverage-lifecycle");
        forget_table(&key);
        // Absence fails closed.
        assert!(!delta_covered(&key));
        // A successful publication certifies coverage.
        assert!(try_mark_published(&key, GenerationId::new(12), 0, None));
        assert!(delta_covered(&key));
        // Ordinary DML dirtiness preserves coverage (writes stage deltas).
        mark_table_dirty(&key);
        assert_eq!(clean_generation(&key), None);
        assert!(delta_covered(&key));
        // A commit inside the next build window breaks coverage.
        let writes_before = writes_since_boot(&key);
        mark_table_dirty(&key);
        assert!(!try_mark_published(
            &key,
            GenerationId::new(13),
            writes_before,
            None
        ));
        assert!(!delta_covered(&key));
        // A clean window re-establishes it.
        let writes_before = writes_since_boot(&key);
        assert!(try_mark_published(
            &key,
            GenerationId::new(14),
            writes_before,
            None
        ));
        assert!(delta_covered(&key));
        // Truncation (bulk delta clear) drops coverage without dropping the
        // entry; drops remove it entirely.
        clear_delta_coverage(&key);
        assert!(!delta_covered(&key));
        assert_eq!(writes_since_boot(&key), 2);
        forget_table(&key);
        assert!(!delta_covered(&key));
    }
}

mod staging_tests {

    use plomid_executor::columnar_freshness::*;
    use plomid_sql::{InMemoryCatalog, Value};

    fn catalog() -> InMemoryCatalog {
        InMemoryCatalog::new()
    }

    #[test]
    fn row_id_parses_from_row_keys_only() {
        assert_eq!(parse_row_id("public.t", b"public.t:42"), Some(42));
        assert_eq!(parse_row_id("public.t", b"public.t:abc"), None);
        assert_eq!(parse_row_id("public.t", b"other:42"), None);
        assert_eq!(parse_row_id("public.t", b"public.t:"), None);
    }

    #[test]
    fn stage_take_discard_round_trip() {
        let catalog = catalog();
        stage_delta_row(&catalog, "public.t", 7, &[Value::Int8(1)], None, false);
        // Non-positive row ids never stage.
        stage_delta_row(&catalog, "public.t", 0, &[Value::Int8(2)], None, false);
        stage_truncate(&catalog, "public.t");
        let staged = catalog.take_staged_deltas();
        assert_eq!(staged.len(), 2);
        assert!(!staged[0].deleted && !staged[0].truncate);
        assert_eq!(staged[0].row_id, 7);
        assert!(staged[1].truncate);
        // Taking drains; discarding an empty buffer is a no-op.
        assert!(catalog.take_staged_deltas().is_empty());
        catalog.discard_staged_deltas();
    }
}

mod delta_lag_tests {
    use plomid_core::GenerationId;
    use plomid_executor::columnar_freshness::*;

    fn delta_key(table: &str) -> String {
        freshness_key(std::path::Path::new("/root"), "db", table)
    }

    /// Resets both maps through the real publication path: a successful
    /// `try_mark_published` clears delta lag via the internal reset, then
    /// `forget_table` drops the freshness entry. No direct delta-map reset.
    fn reset_via_publish(key: &str) {
        let writes_before = writes_since_boot(key);
        assert!(try_mark_published(
            key,
            GenerationId::new(999),
            writes_before,
            None
        ));
        assert_eq!(delta_lag_writes(key), 0);
        forget_table(key);
    }

    #[test]
    fn absent_table_has_no_delta_lag() {
        let key = delta_key("delta-never-written");
        reset_via_publish(&key);
        assert_eq!(delta_lag_writes(&key), 0);
        forget_table(&key);
    }

    #[test]
    fn delta_writes_accumulate_until_published() {
        let key = delta_key("delta-lag");
        reset_via_publish(&key);
        note_delta_write(&key);
        note_delta_write(&key);
        assert_eq!(delta_lag_writes(&key), 2);
        // Publishing through the real path clears the lag.
        let writes_before = writes_since_boot(&key);
        assert!(try_mark_published(
            &key,
            GenerationId::new(20),
            writes_before,
            None
        ));
        assert_eq!(delta_lag_writes(&key), 0);
        forget_table(&key);
    }

    #[test]
    fn successful_publish_clears_delta_lag() {
        let key = delta_key("delta-publish-reset");
        forget_table(&key);
        reset_via_publish(&key);
        note_delta_write(&key);
        note_delta_write(&key);
        assert_eq!(delta_lag_writes(&key), 2);
        let writes_before = writes_since_boot(&key);
        assert!(try_mark_published(
            &key,
            GenerationId::new(21),
            writes_before,
            None
        ));
        assert_eq!(delta_lag_writes(&key), 0);
        forget_table(&key);
    }

    #[test]
    fn delta_lag_is_independent_of_freshness_writes() {
        let key = delta_key("delta-independent");
        forget_table(&key);
        reset_via_publish(&key);
        note_delta_write(&key);
        // The freshness write counter is untouched by delta accounting.
        assert_eq!(writes_since_boot(&key), 0);
        assert_eq!(delta_lag_writes(&key), 1);
        mark_table_dirty(&key);
        assert_eq!(writes_since_boot(&key), 1);
        assert_eq!(delta_lag_writes(&key), 1);
        reset_via_publish(&key);
    }
}
