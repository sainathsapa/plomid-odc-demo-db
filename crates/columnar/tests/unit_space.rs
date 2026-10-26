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
// Moved from src/space.rs inline #[cfg(test)].
use plomid_columnar::compaction::CompactionOutcome;
use plomid_columnar::compaction::{CompactionInput, CompactionPlan};
use plomid_columnar::space::*;
use plomid_columnar::store::ColumnarStore;
use plomid_columnar::{ColumnType, ColumnarPublish, FlushConfig};
use plomid_core::SegmentId;
use plomid_core::{
    CatalogVersion, DatabaseId, GenerationId, Lsn, ObjectId, SchemaId, TableId, TableIdentity,
};
use plomid_storage::{Field, PublicationRequest, Row};

fn segment(value: u64) -> SegmentId {
    SegmentId::new(value)
}

#[test]
fn accounting_math_and_ratios_are_exact() {
    let mut engine = SpaceEngine::new();
    // Supersede is whole-segment: the ledger's recorded size is
    // authoritative, so dead(seg1) moves all 600 bytes live -> dead.
    engine.record_published("orders", segment(1), 600, 60);
    engine.record_published("orders", segment(2), 400, 40);
    engine.record_dead("orders", segment(1), 600);
    engine.record_orphan("orders", segment(9), 100);
    // Untracked segments count their reported bytes as dead directly.
    engine.record_dead("orders", segment(10), 50);
    let account = engine.account("orders");
    assert_eq!(account.live_bytes, 400);
    assert_eq!(account.dead_bytes, 650);
    assert_eq!(account.orphan_bytes, 100);
    assert_eq!(account.live_rows, 40);
    assert_eq!(account.total_bytes(), 1150);
    assert!((account.live_ratio() - 400.0 / 1150.0).abs() < 1e-12);
    assert!((account.dead_ratio() - 750.0 / 1150.0).abs() < 1e-12);
    assert!((account.live_ratio() + account.dead_ratio() - 1.0).abs() < 1e-12);
}

#[test]
fn empty_account_reports_zero_ratios_not_nan() {
    let account = SpaceAccount::empty("missing");
    assert!(account.is_empty());
    assert_eq!(account.live_ratio(), 0.0);
    assert_eq!(account.dead_ratio(), 0.0);
    let engine = SpaceEngine::new();
    assert!(!engine.needs_compaction("missing", 0.0));
}

#[test]
fn counters_saturate_instead_of_wrapping() {
    let mut engine = SpaceEngine::new();
    engine.record_published("t", segment(1), u64::MAX, u64::MAX);
    engine.record_published("t", segment(2), u64::MAX, u64::MAX);
    let account = engine.account("t");
    assert_eq!(account.live_bytes, u64::MAX);
    assert_eq!(account.live_rows, u64::MAX);
    // Reclaiming far more than tracked floors at zero, never underflows.
    engine.record_reclaimed("t", segment(77), u64::MAX);
    engine.record_reclaimed("t", segment(78), u64::MAX);
    let account = engine.account("t");
    assert_eq!(account.live_bytes, u64::MAX);
    assert_eq!(account.dead_bytes, 0);
    assert_eq!(account.orphan_bytes, 0);
    // Double reclaim of a tracked segment is a no-op the second time.
    engine.record_dead("t", segment(1), u64::MAX);
    engine.record_reclaimed("t", segment(1), 0);
    engine.record_reclaimed("t", segment(1), 0);
    assert_eq!(engine.account("t").dead_bytes, 0);
}

#[test]
fn debt_ordering_is_dead_ratio_descending() {
    let mut engine = SpaceEngine::new();
    // low: 900 live + 100 dead = 0.1 debt.
    engine.record_published("low", segment(1), 900, 90);
    engine.record_published("low", segment(4), 100, 10);
    engine.record_dead("low", segment(4), 100);
    // high: 100 live + 900 dead = 0.9 debt.
    engine.record_published("high", segment(2), 100, 10);
    engine.record_published("high", segment(5), 900, 90);
    engine.record_dead("high", segment(5), 900);
    engine.record_published("clean", segment(3), 500, 50);
    let order: Vec<String> = engine
        .tables_by_debt()
        .iter()
        .map(|account| account.table.clone())
        .collect();
    assert_eq!(order, vec!["high", "low", "clean"]);
    assert!(engine.needs_compaction("high", 0.5));
    assert!(!engine.needs_compaction("low", 0.5));
    assert!(!engine.needs_compaction("clean", 0.0));
}

#[test]
fn observe_outcome_folds_rows_and_reclaims() {
    let mut engine = SpaceEngine::new();
    engine.record_published("t", segment(1), 600, 60);
    engine.record_published("t", segment(2), 400, 40);
    engine.record_dead("t", segment(1), 600);
    let outcome = CompactionOutcome {
        plan: CompactionPlan {
            identity: TableIdentity::new(DatabaseId::new(1), SchemaId::new(1), TableId::new(1)),
            object_id: ObjectId::new(1),
            inputs: vec![
                CompactionInput {
                    generation: GenerationId::new(1),
                    current: false,
                },
                CompactionInput {
                    generation: GenerationId::new(2),
                    current: true,
                },
            ],
            horizon: 7,
            inputs_releasable: true,
        },
        published: None,
        released: Vec::new(),
        reclaimed: None,
        visible_rows: 70,
        output_rows: 70,
        retained_inputs: Vec::new(),
        input_rows: vec![(GenerationId::new(1), 60), (GenerationId::new(2), 40)],
        reclaimed_segments: vec![segment(1)],
        reclaimed_segment_bytes: 600,
    };
    engine.observe_outcome("t", &outcome);
    let account = engine.account("t");
    // 100 input rows compacted to 70: 30 dead rows, live set is 70.
    assert_eq!(account.dead_rows, 30);
    assert_eq!(account.live_rows, 70);
    // The dead segment was reclaimed: debt cleared, live untouched.
    assert_eq!(account.dead_bytes, 0);
    assert_eq!(account.live_bytes, 400);
}

#[test]
fn memory_bound_evicts_zero_debt_first_and_counts_drops() {
    let mut engine = SpaceEngine::with_limits(2, 8);
    engine.record_published("idle-a", segment(1), 100, 10);
    engine.record_published("idle-b", segment(2), 100, 10);
    // Full of zero-debt tables: a debt-carrying table evicts one idle.
    engine.record_published("hot", segment(3), 100, 10);
    engine.record_dead("hot", segment(3), 100);
    assert!(engine.contains("hot"));
    assert_eq!(engine.table_count(), 2);
    assert_eq!(engine.dropped_events(), 0);
    // Now every slot holds debt: the next new table is dropped, counted.
    engine.record_dead("idle-b", segment(2), 100);
    engine.record_published("lost", segment(4), 100, 10);
    assert!(!engine.contains("lost"));
    assert_eq!(engine.dropped_events(), 1);
    // Explicit idle eviction removes only zero-debt tables (none left).
    assert!(engine.evict_idle().is_empty());
}

#[test]
fn segment_ledger_cap_folds_overflow_into_aggregates() {
    let mut engine = SpaceEngine::with_limits(16, 2);
    engine.record_published("t", segment(1), 100, 10);
    engine.record_published("t", segment(2), 100, 10);
    engine.record_published("t", segment(3), 100, 10);
    // Aggregates count every byte even though the ledger holds two.
    assert_eq!(engine.account("t").live_bytes, 300);
    assert_eq!(engine.account("t").live_rows, 30);
}

// -- Fixture helpers (real engine + store + generations) ----------------

fn fixture_identity() -> TableIdentity {
    TableIdentity::new(DatabaseId::new(9), SchemaId::new(9), TableId::new(9))
}

fn fixture_object() -> ObjectId {
    ObjectId::new(9)
}

fn fixture_dir(label: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-space-it-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

fn publish_fixture_generation(
    store: &ColumnarStore,
    engine: &mut plomid_txn::PlomidStorageEngine,
    generation: u64,
    base: i64,
) -> SegmentId {
    let rows: Vec<Row> = (0..50)
        .map(|i| Row::new(vec![Field::Integer(base + i)]))
        .collect();
    let types = vec![ColumnType::Integer];
    let publish = ColumnarPublish::new(
        fixture_object(),
        ColumnarStore::columnar_schema(SchemaId::new(9), CatalogVersion::new(1), &types)
            .expect("schema"),
        GenerationId::new(generation),
        GenerationId::new(1),
        Lsn::new(1),
    )
    .with_identity(fixture_identity());
    let segment = store.allocate_segment_id();
    store
        .flush_rows_with_id(
            engine,
            &rows,
            &types,
            segment,
            &FlushConfig::default(),
            &publish,
            plomid_columnar::ColumnarFailPoint::None,
        )
        .expect("publish");
    segment
}

#[test]
fn orphan_scan_finds_and_reclaims_superseded_slices() {
    let dir = fixture_dir("orphan");
    let mut engine =
        plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    let store = ColumnarStore::open(&dir).expect("store");
    let first = publish_fixture_generation(&store, &mut engine, 1, 0);
    let second = publish_fixture_generation(&store, &mut engine, 2, 1000);
    let _third = publish_fixture_generation(&store, &mut engine, 3, 2000);

    // Nothing is orphaned while every generation is still owned.
    assert!(OrphanScan::find_orphan_slices(&store, &mut engine, store.generations()).is_empty());

    // Release the first two generations and reclaim their files; their
    // slices stay on disk, referenced by nothing: orphans.
    let release = PublicationRequest::release(
        GenerationId::new(1),
        Lsn::new(1),
        vec![GenerationId::new(1), GenerationId::new(2)],
    )
    .expect("release request");
    store.generations().publish(release).expect("release");
    let outcome = store.generations().gc().expect("gc");
    assert_eq!(outcome.reclaimed.len(), 2);

    let orphans = OrphanScan::find_orphan_slices(&store, &mut engine, store.generations());
    let found: Vec<SegmentId> = orphans.iter().map(|(segment, _)| *segment).collect();
    assert!(
        found.contains(&first),
        "first segment must orphan, got {found:?}"
    );
    assert!(
        found.contains(&second),
        "second segment must orphan, got {found:?}"
    );
    assert!(orphans.iter().all(|(_, bytes)| *bytes > 0));

    let mut space = SpaceEngine::new();
    for (segment, bytes) in &orphans {
        space.record_orphan("t", *segment, *bytes);
    }
    assert!(space.account("t").orphan_bytes > 0);

    let bytes = OrphanScan::reclaim_orphans(&store, &mut engine, &orphans);
    assert!(bytes > 0);
    for (segment, bytes) in &orphans {
        space.record_reclaimed("t", *segment, *bytes);
    }
    assert_eq!(space.account("t").orphan_bytes, 0);

    // Reclaim is idempotent: nothing left to find, re-delete frees zero.
    assert!(OrphanScan::find_orphan_slices(&store, &mut engine, store.generations()).is_empty());
    assert_eq!(
        OrphanScan::reclaim_orphans(&store, &mut engine, &orphans),
        0
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn orphan_scan_is_fail_closed_without_publication() {
    let dir = fixture_dir("failclosed");
    let mut engine =
        plomid_txn::PlomidStorageEngine::create(&dir, &dir.join("wal"), 32).expect("create");
    let store = ColumnarStore::open(&dir).expect("store");
    // No catalog state at all: the scan reports nothing, deletes nothing.
    assert!(OrphanScan::find_orphan_slices(&store, &mut engine, store.generations()).is_empty());
    assert_eq!(
        OrphanScan::reclaim_orphans(&store, &mut engine, &[(segment(999), 10)]),
        0
    );
    assert_eq!(
        OrphanScan::scan_and_reclaim(&store, &mut engine, store.generations()),
        (0, 0)
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn scheduler_prefers_debt_then_guarantees_fairness() {
    let mut engine = SpaceEngine::new();
    // hot: 100 live + 900 dead = 0.9 debt.
    engine.record_published("hot", segment(1), 100, 10);
    engine.record_published("hot", segment(3), 900, 90);
    engine.record_dead("hot", segment(3), 900);
    // cold: 990 live + 10 dead = 0.01 debt.
    engine.record_published("cold", segment(2), 990, 99);
    engine.record_published("cold", segment(4), 10, 1);
    engine.record_dead("cold", segment(4), 10);
    let mut scheduler = SpaceScheduler::new(0.5);
    assert_eq!(scheduler.max_dead_ratio(), 0.5);
    // Debt order wins while both tables are young.
    assert_eq!(scheduler.next_candidate(&engine, 1).as_deref(), Some("hot"));
    // The concurrency budget blocks dispatch when full.
    scheduler.begin();
    assert_eq!(scheduler.next_candidate(&engine, 1), None);
    scheduler.finish();
    assert_eq!(scheduler.in_flight(), 0);
    // Starvation-freedom: the cold table misses every round until its
    // banked waits force eligibility, then it is picked at least once.
    let mut picked_cold = false;
    for _ in 0..(SCHEDULER_STARVATION_WAITS + 5) {
        if scheduler.next_candidate(&engine, 8).as_deref() == Some("cold") {
            picked_cold = true;
            break;
        }
    }
    assert!(picked_cold, "lowest-debt table must eventually run");
    // Advisory single-table answers agree with the debt threshold.
    assert!(scheduler.should_run_maintenance(&engine, "hot"));
    assert!(!scheduler.should_run_maintenance(&engine, "cold"));
}
