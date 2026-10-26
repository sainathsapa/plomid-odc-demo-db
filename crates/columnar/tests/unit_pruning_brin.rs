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
// Moved from src/pruning/brin.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::pruning::brin::*;
use plomid_columnar::pruning::zonemap::{NullState, ZoneMapBuilder};
use plomid_columnar::pruning::{PruneOperator, PrunePredicate, PruneVerdict};
use plomid_core::ColumnId;
use plomid_storage::Field;

fn c0() -> ColumnId {
    ColumnId::new(0)
}

fn c1() -> ColumnId {
    ColumnId::new(1)
}

/// Builds an index over `values` with `rows_per_range` rows per range.
fn index(values: &[Option<i64>], rows_per_range: u64) -> BrinIndex {
    let mut builder = BrinBuilder::new(values.len() as u64)
        .with_rows_per_range(rows_per_range)
        .add_column(c0(), ColumnType::Integer);
    for (row, value) in values.iter().enumerate() {
        builder.observe(row as u64, c0(), value.map(Field::Integer));
    }
    builder.finish()
}

#[test]
fn empty_index_has_no_ranges_and_validates() {
    let index = BrinBuilder::new(0)
        .with_rows_per_range(4)
        .add_column(c0(), ColumnType::Integer)
        .finish();
    assert_eq!(index.row_count, 0);
    assert!(index.ranges.is_empty());
    index.validate().expect("empty index is valid");
}

#[test]
fn ranges_are_deterministic_contiguous_and_cover_the_segment() {
    let index = index(&(0..10).map(Some).collect::<Vec<_>>(), 4);
    assert_eq!(index.rows_per_range, 4);
    let extents: Vec<(u64, u64)> = index
        .ranges
        .iter()
        .map(|range| (range.start_row, range.end_row))
        .collect();
    assert_eq!(extents, vec![(0, 4), (4, 8), (8, 10)]);
    index.validate().expect("tiling");
    // A short trailing range is expected, never an empty one.
    assert!(index.ranges.iter().all(|range| range.row_count() > 0));
}

#[test]
fn each_range_summarizes_exactly_its_own_rows() {
    // Deliberately clustered-then-unclustered so a misaligned range shows.
    let values: Vec<Option<i64>> = vec![
        Some(100),
        Some(101),
        Some(102),
        Some(103), // [0,4)
        Some(-5),
        Some(-4),
        Some(-3),
        Some(-2), // [4,8)
        Some(0),
        Some(0), // [8,10)
    ];
    let index = index(&values, 4);
    let expected = [
        (0_u64, 4_u64, 100_i64, 103_i64),
        (4, 8, -5, -2),
        (8, 10, 0, 0),
    ];
    for (range, (start, end, min, max)) in index.ranges.iter().zip(expected) {
        assert_eq!((range.start_row, range.end_row), (start, end));
        let zone = range.zone_for(c0()).expect("zone");
        assert_eq!(zone.min, Some(Field::Integer(min)));
        assert_eq!(zone.max, Some(Field::Integer(max)));
        assert_eq!(zone.start_row, start);
        assert_eq!(zone.row_count, end - start);
    }
}

#[test]
fn one_row_per_range_produces_one_range_per_row() {
    let index = index(&[Some(5), Some(5), Some(5)], 1);
    assert_eq!(index.ranges.len(), 3);
    for range in &index.ranges {
        let zone = range.zone_for(c0()).expect("zone");
        assert_eq!(zone.min, zone.max);
    }
    index.validate().expect("valid");
}

#[test]
fn zero_rows_per_range_degrades_to_one_rather_than_failing() {
    let index = index(&[Some(1), Some(2)], 0);
    assert_eq!(index.rows_per_range, 1);
    assert_eq!(index.ranges.len(), 2);
}

#[test]
fn rows_per_range_larger_than_the_segment_yields_a_single_range() {
    let index = index(&[Some(1), Some(2), Some(3)], 4096);
    assert_eq!(index.ranges.len(), 1);
    assert_eq!(index.ranges[0].end_row, 3);
}

#[test]
fn unobserved_rows_are_summarized_as_null_not_dropped() {
    // Only row 1 is observed; rows 0 and 2 must still exist in the summary.
    let index = {
        let mut builder = BrinBuilder::new(3)
            .with_rows_per_range(3)
            .add_column(c0(), ColumnType::Integer);
        builder.observe(1, c0(), Some(Field::Integer(7)));
        builder.finish()
    };
    let zone = index.ranges[0].zone_for(c0()).expect("zone");
    assert_eq!(zone.row_count, 3);
    assert_eq!(zone.null_state, NullState::HasNulls);
    assert_eq!(zone.min, Some(Field::Integer(7)));
    assert_eq!(zone.max, Some(Field::Integer(7)));
    index.validate().expect("valid");
}

#[test]
fn unobserved_column_is_summarized_as_all_nulls() {
    let mut builder = BrinBuilder::new(2)
        .with_rows_per_range(2)
        .add_column(c0(), ColumnType::Integer)
        .add_column(c1(), ColumnType::String);
    builder.observe(0, c0(), Some(Field::Integer(1)));
    builder.observe(1, c0(), Some(Field::Integer(2)));
    let index = builder.finish();
    assert_eq!(index.ranges[0].zone_maps.len(), 2);
    let zone = index.ranges[0].zone_for(c1()).expect("zone");
    assert_eq!(zone.null_state, NullState::AllNulls);
    assert!(!zone.has_bounds());
}

#[test]
fn duplicate_column_declarations_keep_the_first_type() {
    let builder = BrinBuilder::new(1)
        .with_rows_per_range(1)
        .add_column(c0(), ColumnType::Integer)
        .add_column(c0(), ColumnType::String);
    assert_eq!(builder.finish().ranges[0].zone_maps.len(), 1);
}

#[test]
fn range_evaluation_prunes_only_provably_empty_intervals() {
    // Clustered: [0,4) holds 1..4, [4,8) holds 100..103.
    let values = [
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(100),
        Some(101),
        Some(102),
        Some(103),
    ];
    let index = index(&values, 4);
    // A predicate only the low range can satisfy prunes the high range.
    let low = PrunePredicate::compare(c0(), PruneOperator::LessOrEqual, Field::Integer(4));
    assert_eq!(index.ranges[0].evaluate(&low), PruneVerdict::Keep);
    assert_eq!(index.ranges[1].evaluate(&low), PruneVerdict::Prune);
    // Selective lookup inside the high range keeps exactly that range.
    let hit = PrunePredicate::eq(c0(), Field::Integer(102));
    assert_eq!(index.ranges[0].evaluate(&hit), PruneVerdict::Prune);
    assert_eq!(index.ranges[1].evaluate(&hit), PruneVerdict::Keep);
    // `!=` can never prune a non-empty range on bounds alone.
    let ne = PrunePredicate::ne(c0(), Field::Integer(2));
    assert_eq!(index.ranges[0].evaluate(&ne), PruneVerdict::Keep);
    assert_eq!(index.ranges[1].evaluate(&ne), PruneVerdict::Keep);
}

#[test]
fn evaluate_extent_requires_full_range_coverage() {
    let values: Vec<Option<i64>> = (0..8).map(Some).collect();
    let index = index(&values, 4);
    // The whole segment: the high range matches `>= 0`, so no prune.
    assert_eq!(
        index.evaluate_extent(&PrunePredicate::eq(c0(), Field::Integer(9)), 0, 8),
        PruneVerdict::Prune
    );
    // Both ranges prove impossibility, so the covered extent prunes.
    assert_eq!(
        index.evaluate_extent(
            &PrunePredicate::compare(c0(), PruneOperator::GreaterOrEqual, Field::Integer(100)),
            0,
            8
        ),
        PruneVerdict::Prune
    );
    // Half the extent: only the low range is involved.
    assert_eq!(
        index.evaluate_extent(&PrunePredicate::eq(c0(), Field::Integer(1)), 0, 4),
        PruneVerdict::Keep
    );
    assert_eq!(
        index.evaluate_extent(&PrunePredicate::eq(c0(), Field::Integer(1)), 4, 8),
        PruneVerdict::Prune
    );
    // An empty extent holds no rows.
    assert_eq!(
        index.evaluate_extent(&PrunePredicate::eq(c0(), Field::Integer(1)), 3, 3),
        PruneVerdict::Prune
    );
}

#[test]
fn evaluation_of_a_foreign_column_is_unknown_never_prune() {
    let index = index(&[Some(1), Some(2)], 2);
    let foreign = PrunePredicate::eq(c1(), Field::Integer(999));
    assert_eq!(index.ranges[0].evaluate(&foreign), PruneVerdict::Unknown);
    assert_eq!(index.evaluate_extent(&foreign, 0, 2), PruneVerdict::Unknown);
}

#[test]
fn validate_rejects_gaps_overlap_and_zones_escaping_their_range() {
    let make = |ranges: Vec<BrinRange>, row_count: u64| BrinIndex {
        row_count,
        rows_per_range: 4,
        ranges,
    };
    let zone = |start_row: u64, row_count: u64| {
        let mut builder = ZoneMapBuilder::new(c0(), ColumnType::Integer);
        for row in start_row..start_row + row_count {
            builder.observe(row, Some(Field::Integer(row as i64)));
        }
        builder.finish()
    };
    // A gap would make rows 2..4 unreachable to both prune and scan.
    let error = make(
        vec![
            BrinRange::new(0, 2, vec![zone(0, 2)]),
            BrinRange::new(4, 6, vec![zone(4, 2)]),
        ],
        6,
    )
    .validate()
    .expect_err("gap");
    assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    // Rows claimed but not covered.
    let error = make(vec![BrinRange::new(0, 2, vec![zone(0, 2)])], 5)
        .validate()
        .expect_err("short covering");
    assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    // A range past the segment.
    let error = make(vec![BrinRange::new(0, 9, vec![zone(0, 9)])], 6)
        .validate()
        .expect_err("past end");
    assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    // A zone map escaping its parent range.
    let error = make(vec![BrinRange::new(0, 2, vec![zone(0, 4)])], 2)
        .validate()
        .expect_err("escaping zone");
    assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    // Zero rows with ranges, and rows without ranges, are both corrupt.
    assert!(make(vec![BrinRange::new(0, 1, Vec::new())], 0)
        .validate()
        .is_err());
    assert!(make(Vec::new(), 3).validate().is_err());
    // Ending before starting is corrupt too.
    assert!(make(vec![BrinRange::new(4, 2, Vec::new())], 4)
        .validate()
        .is_err());
}

#[test]
fn from_ranges_sorts_but_validate_still_proves_tiling() {
    let zone_over = |start: u64, count: u64| {
        let mut builder = ZoneMapBuilder::new(c0(), ColumnType::Integer);
        for row in start..start + count {
            builder.observe(row, Some(Field::Integer(row as i64)));
        }
        builder.finish()
    };
    let high = BrinRange::new(2, 4, vec![zone_over(2, 2)]);
    let low = BrinRange::new(0, 2, vec![zone_over(0, 2)]);
    let index = BrinIndex::from_ranges(4, 2, vec![high, low]);
    assert_eq!(index.ranges[0].start_row, 0);
    assert_eq!(index.ranges[0].zone_maps[0].start_row, 0);
    index.validate().expect("sorted tiling");
    assert_eq!(
        index.evaluate_extent(&PrunePredicate::eq(c0(), Field::Integer(0)), 0, 4),
        PruneVerdict::Keep
    );
}
