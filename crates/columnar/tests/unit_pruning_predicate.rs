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
// Moved from src/pruning/predicate.rs inline #[cfg(test)].
use plomid_columnar::pruning::predicate::*;
use plomid_columnar::pruning::verdict::PruneVerdict;
use plomid_columnar::pruning::zonemap::{ZoneMap, ZoneMapBuilder};
use plomid_core::ColumnId;
use plomid_storage::Field;

fn col() -> ColumnId {
    ColumnId::new(3)
}

/// Builds one zone map over `values` (row `i` is physical row `i`).
fn zone_of(values: &[Option<i64>]) -> ZoneMap {
    let mut builder = ZoneMapBuilder::new(col(), plomid_columnar::column::ColumnType::Integer);
    for (row, value) in values.iter().enumerate() {
        builder.observe(row as u64, value.map(Field::Integer));
    }
    builder.finish()
}

fn verdict(values: &[Option<i64>], predicate: &PrunePredicate) -> PruneVerdict {
    zone_of(values).evaluate(predicate)
}

fn cmp(operator: PruneOperator, literal: i64) -> PrunePredicate {
    PrunePredicate::compare(col(), operator, Field::Integer(literal))
}

/// Ground truth: does any row of `values` satisfy `predicate`?
fn any_row_matches(values: &[Option<i64>], predicate: &PrunePredicate) -> bool {
    values
        .iter()
        .any(|value| row_matches(predicate, &|_| value.map(Field::Integer)) == Tri::True)
}

/// The invariant every prune must satisfy, checked against a full scan.
fn assert_no_false_negative(values: &[Option<i64>], predicate: &PrunePredicate, label: &str) {
    let verdict = verdict(values, predicate);
    if verdict == PruneVerdict::Prune {
        assert!(
            !any_row_matches(values, predicate),
            "false negative ({label}): pruned {values:?} for {predicate:?} but a row matches"
        );
    }
}

#[test]
fn equal_prunes_only_outside_the_closed_bounds() {
    let values = [Some(10), Some(20), Some(30)];
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 9)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 10)),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 20)),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 30)),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 31)),
        PruneVerdict::Prune
    );
}

#[test]
fn less_and_less_or_equal_respect_the_min_boundary() {
    let values = [Some(10), Some(20), Some(30)];
    // min == literal: `< 10` cannot match, `<= 10` can.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Less, 10)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::LessOrEqual, 10)),
        PruneVerdict::Keep
    );
    // just below the minimum prunes both.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Less, 9)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::LessOrEqual, 9)),
        PruneVerdict::Prune
    );
    // just above the minimum keeps.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Less, 11)),
        PruneVerdict::Keep
    );
}

#[test]
fn greater_and_greater_or_equal_respect_the_max_boundary() {
    let values = [Some(10), Some(20), Some(30)];
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Greater, 30)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::GreaterOrEqual, 30)),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Greater, 31)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::GreaterOrEqual, 31)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Greater, 29)),
        PruneVerdict::Keep
    );
}

#[test]
fn not_equal_never_prunes_on_bounds() {
    // min = 1, max = 10 says nothing about whether 5 occurs.
    let values = [Some(1), Some(10)];
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::NotEqual, 5)),
        PruneVerdict::Keep
    );
    // Even a single-valued range keeps: `col != v` may hold for no row,
    // but min/max alone cannot prove it.
    let single = [Some(7)];
    assert_eq!(
        verdict(&single, &cmp(PruneOperator::NotEqual, 7)),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&single, &cmp(PruneOperator::NotEqual, 8)),
        PruneVerdict::Keep
    );
}

#[test]
fn value_comparisons_prune_all_null_and_empty_ranges() {
    assert_eq!(
        verdict(&[None, None], &cmp(PruneOperator::Equal, 1)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&[None, None], &cmp(PruneOperator::Less, 1)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&[None, None], &cmp(PruneOperator::GreaterOrEqual, 1)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&[], &cmp(PruneOperator::Equal, 1)),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&[], &PrunePredicate::is_null(col())),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&[], &PrunePredicate::is_not_null(col())),
        PruneVerdict::Prune
    );
}

#[test]
fn null_predicates_follow_three_valued_logic() {
    let no_nulls = [Some(1), Some(2)];
    let all_nulls = [None, None];
    let mixed = [None, Some(1)];
    assert_eq!(
        verdict(&no_nulls, &PrunePredicate::is_null(col())),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&no_nulls, &PrunePredicate::is_not_null(col())),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&all_nulls, &PrunePredicate::is_null(col())),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&all_nulls, &PrunePredicate::is_not_null(col())),
        PruneVerdict::Prune
    );
    assert_eq!(
        verdict(&mixed, &PrunePredicate::is_null(col())),
        PruneVerdict::Keep
    );
    assert_eq!(
        verdict(&mixed, &PrunePredicate::is_not_null(col())),
        PruneVerdict::Keep
    );
}

#[test]
fn null_literal_comparisons_are_never_true() {
    let values = [Some(1), Some(2), None];
    for operator in [
        PruneOperator::Equal,
        PruneOperator::Less,
        PruneOperator::LessOrEqual,
        PruneOperator::Greater,
        PruneOperator::GreaterOrEqual,
    ] {
        let predicate = PrunePredicate::compare(col(), operator, Field::Null);
        assert_eq!(
            verdict(&values, &predicate),
            PruneVerdict::Prune,
            "{operator}"
        );
        assert_eq!(
            row_matches(&predicate, &|_| Some(Field::Integer(1))),
            Tri::Null,
            "{operator}"
        );
    }
    // `!=` never prunes on bounds; the NULL literal case keeps the range
    // too (conservative, and still never a false negative).
    let not_equal = PrunePredicate::compare(col(), PruneOperator::NotEqual, Field::Null);
    assert_ne!(verdict(&values, &not_equal), PruneVerdict::Prune);
    assert_eq!(
        row_matches(&not_equal, &|_| Some(Field::Integer(1))),
        Tri::Null
    );
    // An all-NULL range has nothing `!=` could be TRUE for.
    assert_eq!(verdict(&[None], &not_equal), PruneVerdict::Prune);
}

#[test]
fn foreign_columns_and_untyped_literals_are_unknown() {
    let zone = zone_of(&[Some(1)]);
    let foreign = PrunePredicate::eq(ColumnId::new(99), Field::Integer(5));
    assert_eq!(zone.evaluate(&foreign), PruneVerdict::Unknown);
    let incomparable = PrunePredicate::eq(col(), Field::String("x".to_owned()));
    assert_eq!(zone.evaluate(&incomparable), PruneVerdict::Unknown);
    // A missing literal (only legal for IS NULL / IS NOT NULL) is unknown.
    let missing = PrunePredicate::Compare {
        column_id: col(),
        operator: PruneOperator::Equal,
        literal: None,
    };
    assert_eq!(zone.evaluate(&missing), PruneVerdict::Unknown);
}

#[test]
fn extreme_boundaries_never_overflow() {
    let zone = zone_of(&[Some(i64::MIN), Some(i64::MAX)]);
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::Less, i64::MIN)),
        PruneVerdict::Prune
    );
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::LessOrEqual, i64::MIN)),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::Greater, i64::MAX)),
        PruneVerdict::Prune
    );
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::GreaterOrEqual, i64::MAX)),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::Equal, i64::MIN)),
        PruneVerdict::Keep
    );
    assert_eq!(
        zone.evaluate(&cmp(PruneOperator::Equal, i64::MAX)),
        PruneVerdict::Keep
    );
    let zero = zone_of(&[Some(0)]);
    assert_eq!(
        zero.evaluate(&cmp(PruneOperator::Equal, 0)),
        PruneVerdict::Keep
    );
    assert_eq!(
        zero.evaluate(&cmp(PruneOperator::Less, -1)),
        PruneVerdict::Prune
    );
    assert_eq!(
        zero.evaluate(&cmp(PruneOperator::Greater, 0)),
        PruneVerdict::Prune
    );
}

#[test]
fn and_prunes_when_either_branch_proves_impossibility() {
    let values = [Some(10), Some(20)];
    // Impossible left, possible right: AND can never be TRUE.
    let predicate = cmp(PruneOperator::Equal, 99).and(cmp(PruneOperator::Greater, 5));
    assert_eq!(verdict(&values, &predicate), PruneVerdict::Prune);
    // Possible left, impossible right.
    let predicate = cmp(PruneOperator::Greater, 5).and(cmp(PruneOperator::Equal, 99));
    assert_eq!(verdict(&values, &predicate), PruneVerdict::Prune);
    // Both possible: keep.
    let predicate = cmp(PruneOperator::Greater, 5).and(cmp(PruneOperator::Less, 99));
    assert_eq!(verdict(&values, &predicate), PruneVerdict::Keep);
    // Unknown branch (foreign column) cannot upgrade to Prune.
    let predicate = cmp(PruneOperator::Greater, 5)
        .and(PrunePredicate::eq(ColumnId::new(99), Field::Integer(1)));
    assert_ne!(verdict(&values, &predicate), PruneVerdict::Prune);
}

#[test]
fn or_prunes_only_when_every_branch_proves_impossibility() {
    let values = [Some(10), Some(20)];
    let both_impossible = cmp(PruneOperator::Equal, 99).or(cmp(PruneOperator::Less, 1));
    assert_eq!(verdict(&values, &both_impossible), PruneVerdict::Prune);
    let one_possible = cmp(PruneOperator::Equal, 99).or(cmp(PruneOperator::Greater, 5));
    assert_eq!(verdict(&values, &one_possible), PruneVerdict::Keep);
    // An unknown branch keeps the range: it might be TRUE somewhere.
    let with_unknown =
        cmp(PruneOperator::Equal, 99).or(PrunePredicate::eq(ColumnId::new(99), Field::Integer(1)));
    assert_ne!(verdict(&values, &with_unknown), PruneVerdict::Prune);
}

#[test]
fn not_pushes_down_through_safe_duals() {
    let values = [Some(10), Some(20)];
    // NOT (col < 10) == col >= 10: possible.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Less, 10).not()),
        PruneVerdict::Keep
    );
    // NOT (col < 5) == col >= 5: possible.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Less, 5).not()),
        PruneVerdict::Keep
    );
    // NOT (col > 20) == col <= 20: possible.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Greater, 20).not()),
        PruneVerdict::Keep
    );
    // NOT (col IS NOT NULL) == col IS NULL: no NULLs, impossible.
    assert_eq!(
        verdict(&values, &PrunePredicate::is_not_null(col()).not()),
        PruneVerdict::Prune
    );
    // NOT (col = v) == col != v: never prunes on bounds.
    assert_ne!(
        verdict(&values, &cmp(PruneOperator::Equal, 10).not()),
        PruneVerdict::Prune
    );
    // Double negation collapses.
    assert_eq!(
        verdict(&values, &cmp(PruneOperator::Equal, 99).not().not()),
        PruneVerdict::Prune
    );
    // De Morgan: NOT (A OR B) == NOT A AND NOT B.
    let inner = cmp(PruneOperator::Equal, 10).or(cmp(PruneOperator::Equal, 20));
    assert_ne!(verdict(&values, &inner.not()), PruneVerdict::Prune);
    // NOT (A AND B) == NOT A OR NOT B: `col != 10 OR col != 20` is TRUE for
    // row 10, so it must never be pruned.
    let inner = cmp(PruneOperator::Equal, 10).and(cmp(PruneOperator::Equal, 20));
    assert_eq!(verdict(&values, &inner.not()), PruneVerdict::Keep);
}

#[test]
fn negate_predicate_is_an_involution_where_defined() {
    for predicate in [
        cmp(PruneOperator::Equal, 1),
        cmp(PruneOperator::NotEqual, 1),
        cmp(PruneOperator::Less, 1),
        cmp(PruneOperator::LessOrEqual, 1),
        cmp(PruneOperator::Greater, 1),
        cmp(PruneOperator::GreaterOrEqual, 1),
        PrunePredicate::is_null(col()),
        PrunePredicate::is_not_null(col()),
    ] {
        let dual = negate_predicate(&predicate).expect("dual");
        let round_trip = negate_predicate(&dual).expect("dual");
        assert_eq!(round_trip, predicate, "{predicate:?}");
    }
}

#[test]
fn predicate_columns_are_collected_once_in_order() {
    let other = ColumnId::new(9);
    let predicate = cmp(PruneOperator::Equal, 1)
        .and(PrunePredicate::eq(other, Field::Integer(2)))
        .or(cmp(PruneOperator::Less, 3));
    assert_eq!(predicate_columns(&predicate), vec![col(), other]);
}

/// Deterministic randomized property test over generated datasets and
/// predicates: whenever the metadata says PRUNE, a full scan of the very
/// same values must find no matching row.
///
/// Seeds are fixed so any failure is replayable byte for byte.
#[test]
fn pruned_ranges_never_hide_a_matching_row() {
    let operators = [
        PruneOperator::Equal,
        PruneOperator::NotEqual,
        PruneOperator::Less,
        PruneOperator::LessOrEqual,
        PruneOperator::Greater,
        PruneOperator::GreaterOrEqual,
    ];
    for seed in [
        1_u64, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987,
    ] {
        let mut state = seed | 1;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for shape in 0..24 {
            let len = (next() % 40) as usize;
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            for _ in 0..len {
                match next() % 6 {
                    0 | 1 => values.push(None),
                    2 => values.push(Some((next() % 3) as i64)), // clustered
                    3 => values.push(Some(next() as i64 / 1_000_000)), // wide
                    _ => values.push(Some((next() % 201) as i64 - 100)),
                }
            }

            // Literals drawn from the data (so hits are common), from just
            // outside the data, and from the representable extremes.
            let non_null: Vec<i64> = values.iter().flatten().copied().collect();
            let mut literals: Vec<i64> = Vec::new();
            if let (Some(min), Some(max)) = (
                non_null.iter().copied().min(),
                non_null.iter().copied().max(),
            ) {
                literals.extend([
                    min,
                    max,
                    min.saturating_sub(1),
                    max.saturating_add(1),
                    min.saturating_add(1),
                    max.saturating_sub(1),
                ]);
            }
            literals.extend([0, -1, 1, i64::MIN, i64::MAX, 42, -42]);
            for _ in 0..4 {
                literals.push((next() % 201) as i64 - 100);
            }

            for literal in literals {
                for operator in operators {
                    let leaf = PrunePredicate::compare(col(), operator, Field::Integer(literal));
                    let label = format!("seed={seed} shape={shape} {operator} {literal}");
                    assert_no_false_negative(&values, &leaf, &label);
                    assert_no_false_negative(
                        &values,
                        &leaf.clone().and(PrunePredicate::is_not_null(col())),
                        &label,
                    );
                    assert_no_false_negative(
                        &values,
                        &leaf.clone().or(PrunePredicate::is_null(col())),
                        &label,
                    );
                    assert_no_false_negative(&values, &leaf.not(), &label);
                }
            }
            for predicate in [
                PrunePredicate::is_null(col()),
                PrunePredicate::is_not_null(col()),
                PrunePredicate::is_null(col()).not(),
            ] {
                assert_no_false_negative(
                    &values,
                    &predicate,
                    &format!("seed={seed} shape={shape}"),
                );
            }
        }
    }
}
