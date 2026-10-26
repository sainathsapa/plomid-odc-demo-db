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
// Moved from src/pruning/zonemap.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::pruning::zonemap::*;
use plomid_columnar::pruning::{PruneOperator, PrunePredicate, PruneVerdict};
use plomid_core::ColumnId;
use plomid_storage::Field;

fn col() -> ColumnId {
    ColumnId::new(0)
}

fn int(value: i64) -> Option<Field> {
    Some(Field::Integer(value))
}

/// Builds one zone map over `values`, where element `i` is physical row `i`.
fn build(values: &[Option<Field>], column_type: ColumnType) -> ZoneMap {
    let mut builder = ZoneMapBuilder::new(col(), column_type);
    for (row, value) in values.iter().enumerate() {
        builder.observe(row as u64, value.clone());
    }
    builder.finish()
}

fn ints(values: &[Option<i64>]) -> ZoneMap {
    let values: Vec<Option<Field>> = values.iter().map(|v| v.map(Field::Integer)).collect();
    build(&values, ColumnType::Integer)
}

#[test]
fn negative_and_extreme_values_are_bounded_exactly() {
    let zone = ints(&[Some(i64::MIN), Some(-1), Some(0), Some(i64::MAX)]);
    assert_eq!(zone.min, Some(Field::Integer(i64::MIN)));
    assert_eq!(zone.max, Some(Field::Integer(i64::MAX)));
    // Evaluation compares; it never computes, so extremes cannot overflow.
    assert_eq!(
        zone.evaluate(&PrunePredicate::compare(
            col(),
            PruneOperator::GreaterOrEqual,
            Field::Integer(i64::MAX)
        )),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::compare(
            col(),
            PruneOperator::Less,
            Field::Integer(i64::MIN)
        )),
        PruneVerdict::Prune
    );
}

#[test]
fn null_only_range_reports_all_nulls_without_bounds() {
    let zone = ints(&[None, None, None]);
    assert_eq!(zone.row_count, 3);
    assert_eq!(zone.null_state, NullState::AllNulls);
    assert!(!zone.has_bounds());
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_null(col())),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_not_null(col())),
        PruneVerdict::Prune
    );
}

#[test]
fn mixed_range_bounds_cover_non_nulls_only() {
    let zone = ints(&[None, Some(3), None, Some(-2)]);
    assert_eq!(zone.null_state, NullState::HasNulls);
    assert_eq!(zone.min, Some(Field::Integer(-2)));
    assert_eq!(zone.max, Some(Field::Integer(3)));
    // NULL never becomes a bound, so both NULL predicates keep the range.
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_not_null(col())),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_null(col())),
        PruneVerdict::Keep
    );
}

#[test]
fn field_null_is_treated_as_null_not_as_a_value() {
    let zone = build(
        &[Some(Field::Null), Some(Field::Integer(1))],
        ColumnType::Integer,
    );
    assert_eq!(zone.null_state, NullState::HasNulls);
    assert_eq!(zone.min, Some(Field::Integer(1)));
    assert_eq!(zone.max, Some(Field::Integer(1)));
}

#[test]
fn unsupported_type_yields_null_state_but_no_bounds() {
    for column_type in [
        ColumnType::Bool,
        ColumnType::I16,
        ColumnType::I32,
        ColumnType::F32,
        ColumnType::F64,
        ColumnType::Null,
    ] {
        assert!(!supports_bounds(column_type), "{column_type:?}");
        let zone = build(&[int(1), int(2)], column_type);
        assert_eq!(zone.row_count, 2);
        assert!(!zone.has_bounds(), "{column_type:?}");
        // No bounds: the range must be scanned, never pruned. `Unknown`
        // (no evidence) is exactly as conservative as `Keep`.
        let verdict = zone.evaluate(&PrunePredicate::eq(col(), Field::Integer(99)));
        assert_eq!(verdict, PruneVerdict::Unknown, "{column_type:?}");
        assert_ne!(verdict, PruneVerdict::Prune, "{column_type:?}");
    }
}

#[test]
fn incomparable_values_clear_bounds_permanently() {
    // A well-formed segment never mixes these, but a corrupt in-memory
    // summary must degrade to "no bounds", never to a wrong bound.
    let zone = build(
        &[
            Some(Field::Integer(1)),
            Some(Field::String("a".to_owned())),
            Some(Field::Integer(9)),
        ],
        ColumnType::Integer,
    );
    assert!(!zone.has_bounds());
    assert_eq!(zone.null_state, NullState::NoNulls);
    assert_eq!(zone.row_count, 3);
    assert_eq!(
        zone.evaluate(&PrunePredicate::eq(col(), Field::Integer(-50))),
        PruneVerdict::Unknown
    );
    // Bounds stay cleared even after a later comparable value arrives.
    let mut builder = ZoneMapBuilder::new(col(), ColumnType::Integer);
    builder.observe(0, int(1));
    builder.observe(1, Some(Field::String("a".to_owned())));
    builder.observe(2, int(100));
    assert!(!builder.finish().has_bounds());
}

#[test]
fn boundless_all_null_ranges_prune_value_comparisons_only() {
    // Every row is NULL, so no comparison can be TRUE: pruning is sound
    // even without bounds. NULL predicates still keep the range.
    let zone = ints(&[None, None]);
    assert_eq!(zone.null_state, NullState::AllNulls);
    for predicate in [
        PrunePredicate::eq(col(), Field::Integer(1)),
        PrunePredicate::compare(col(), PruneOperator::Less, Field::Integer(1)),
        PrunePredicate::compare(col(), PruneOperator::GreaterOrEqual, Field::Integer(1)),
        PrunePredicate::ne(col(), Field::Integer(1)),
    ] {
        assert_eq!(
            zone.evaluate(&predicate),
            PruneVerdict::Prune,
            "{predicate:?}"
        );
    }
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_null(col())),
        PruneVerdict::Keep
    );
}

#[test]
fn bounds_match_a_brute_force_scan_over_deterministic_shapes() {
    // Fixed-seed xorshift: many deterministic shapes, each compared against
    // a direct scan of the very same values.
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for shape in 0..128 {
        let len = (next() % 37) as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            if next() % 5 == 0 {
                values.push(None);
            } else {
                values.push(Some(next() as i64));
            }
        }
        let zone = ints(&values);
        let non_null: Vec<i64> = values.iter().flatten().copied().collect();
        assert_eq!(zone.row_count, values.len() as u64, "shape {shape}");
        assert_eq!(
            zone.min,
            non_null.iter().min().copied().map(Field::Integer),
            "shape {shape}"
        );
        assert_eq!(
            zone.max,
            non_null.iter().max().copied().map(Field::Integer),
            "shape {shape}"
        );
        let expected = if non_null.is_empty() {
            NullState::AllNulls
        } else if non_null.len() == values.len() {
            NullState::NoNulls
        } else {
            NullState::HasNulls
        };
        assert_eq!(zone.null_state, expected, "shape {shape}");
    }
}

#[test]
fn string_and_bytes_use_storage_byte_order() {
    assert!(supports_bounds(ColumnType::String));
    assert!(supports_bounds(ColumnType::Bytes));
    let zone = build(
        &[
            Some(Field::String("delta".to_owned())),
            Some(Field::String("alpha".to_owned())),
            Some(Field::String("charlie".to_owned())),
        ],
        ColumnType::String,
    );
    assert_eq!(zone.min, Some(Field::String("alpha".to_owned())));
    assert_eq!(zone.max, Some(Field::String("delta".to_owned())));
    let zone = build(
        &[Some(Field::Bytes(vec![9])), Some(Field::Bytes(vec![1, 2]))],
        ColumnType::Bytes,
    );
    assert_eq!(zone.min, Some(Field::Bytes(vec![1, 2])));
    assert_eq!(zone.max, Some(Field::Bytes(vec![9])));
}

#[test]
fn overlaps_tracks_half_open_intervals_and_ignores_empty_zones() {
    let zone = ints(&[Some(1), Some(2), Some(3)]); // rows [0, 3)
    assert!(zone.overlaps(0, 1));
    assert!(zone.overlaps(2, 5));
    assert!(zone.overlaps(1, 2));
    assert!(!zone.overlaps(3, 4));
    assert!(!zone.overlaps(0, 0));
    let empty = build(&[], ColumnType::Integer);
    assert!(!empty.overlaps(0, 100));
}

#[test]
fn null_state_combines_as_a_lattice() {
    use NullState::{AllNulls, HasNulls, NoNulls};
    assert_eq!(NoNulls.combine(NoNulls), NoNulls);
    assert_eq!(AllNulls.combine(AllNulls), AllNulls);
    assert_eq!(NoNulls.combine(AllNulls), HasNulls);
    assert_eq!(HasNulls.combine(HasNulls), HasNulls);
    assert_eq!(NullState::observe(None, true), AllNulls);
    assert_eq!(NullState::observe(None, false), NoNulls);
    assert_eq!(NullState::observe(Some(AllNulls), false), HasNulls);
    assert_eq!(NullState::observe(Some(HasNulls), true), HasNulls);
}

#[test]
fn null_state_tags_round_trip_and_reject_unknown_values() {
    for state in [NullState::NoNulls, NullState::HasNulls, NullState::AllNulls] {
        assert_eq!(NullState::from_tag(state.tag()).expect("tag"), state);
    }
    for bad in [3_u8, 7, 255] {
        let error = NullState::from_tag(bad).expect_err("unknown null state");
        assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
    }
}

#[test]
fn constructor_never_keeps_a_null_bound() {
    let zone = ZoneMap::new(
        col(),
        ColumnType::Integer,
        0,
        1,
        NullState::AllNulls,
        Some(Field::Null),
        Some(Field::Null),
    );
    assert!(!zone.has_bounds());
    assert_eq!(zone.null_state, NullState::AllNulls);
}

#[test]
fn zone_map_encoding_round_trips_without_loss() {
    for zone in [
        ints(&[Some(-3), Some(8)]),
        ints(&[None, None]),
        build(&[Some(Field::String("x".to_owned()))], ColumnType::String),
        build(&[Some(Field::Bytes(vec![0, 255]))], ColumnType::Bytes),
        build(&[], ColumnType::Integer),
    ] {
        let bytes = zone.encode();
        assert_eq!(bytes.len(), zone.encoded_len());
        let decoded = ZoneMap::decode(&bytes).expect("decode");
        assert_eq!(decoded, zone);
        // Trailing bytes are corruption, not a longer zone map.
        let mut padded = bytes.clone();
        padded.push(0);
        assert!(ZoneMap::decode(&padded).is_err());
    }
}

#[test]
fn empty_range_carries_no_rows_bounds_or_null_evidence() {
    let zone = build(&[], ColumnType::Integer);
    assert_eq!(zone.row_count, 0);
    assert_eq!(zone.end_row(), zone.start_row);
    assert!(!zone.has_bounds());
    assert_eq!(zone.null_state, NullState::AllNulls);
    // An empty range provably holds no matching row, whatever the shape.
    assert_eq!(
        zone.evaluate(&PrunePredicate::eq(col(), Field::Integer(1))),
        PruneVerdict::Prune
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_null(col())),
        PruneVerdict::Prune
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::is_not_null(col())),
        PruneVerdict::Prune
    );
}

#[test]
fn single_row_range_bounds_equal_that_row() {
    let zone = ints(&[Some(-12)]);
    assert_eq!(zone.row_count, 1);
    assert_eq!(zone.min, Some(Field::Integer(-12)));
    assert_eq!(zone.max, Some(Field::Integer(-12)));
    assert_eq!(zone.null_state, NullState::NoNulls);
}

#[test]
fn identical_values_collapse_to_one_bound_pair() {
    let zone = ints(&[Some(5), Some(5), Some(5), Some(5)]);
    assert_eq!(zone.min, Some(Field::Integer(5)));
    assert_eq!(zone.max, Some(Field::Integer(5)));
    assert_eq!(
        zone.evaluate(&PrunePredicate::eq(col(), Field::Integer(5))),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&PrunePredicate::eq(col(), Field::Integer(6))),
        PruneVerdict::Prune
    );
}

#[test]
fn ascending_and_descending_ranges_agree_on_bounds() {
    let ascending = ints(&[Some(-9), Some(0), Some(4), Some(100)]);
    let descending = ints(&[Some(100), Some(4), Some(0), Some(-9)]);
    assert_eq!(ascending.min, Some(Field::Integer(-9)));
    assert_eq!(ascending.max, Some(Field::Integer(100)));
    assert_eq!(descending.min, ascending.min);
    assert_eq!(descending.max, ascending.max);
    assert_eq!(ascending.null_state, descending.null_state);
}
