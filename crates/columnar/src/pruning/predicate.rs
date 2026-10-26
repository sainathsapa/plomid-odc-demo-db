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
//! Safe metadata evaluation of predicates against zone maps.
//!
//! # The correctness rule
//!
//! Every function in this module answers one question: *can the metadata
//! prove that no row in a range satisfies the predicate?* A `PRUNE` answer
//! must be backed by a proof; anything less — missing bounds, an unsupported
//! literal, incomparable types, a predicate over a column the metadata does
//! not describe — answers `KEEP`/`UNKNOWN`, and the caller scans.
//!
//! # SQL three-valued logic
//!
//! Range comparisons (`= < <= > >= !=`) are never TRUE for NULL rows, so a
//! NULL row can never be the reason a `PRUNE` was wrong: pruning reasons
//! only ever talk about non-NULL rows. `IS NULL` / `IS NOT NULL` reason
//! directly about the NULL state.
//!
//! # Operator proofs (bounds `min`/`max` over non-NULL rows, `v` literal)
//!
//! With rows confined to `[min, max]`:
//!
//! * `col = v`  prunes iff `v < min` or `v > max`
//! * `col < v`  prunes iff `min >= v`
//! * `col <= v` prunes iff `min >  v`
//! * `col > v`  prunes iff `max <= v`
//! * `col >= v` prunes iff `max <  v`
//! * `col != v` **never** prunes on bounds (min=1, max=10 says nothing about
//!   whether 5 occurs); only an all-NULL range prunes
//! * `IS NULL`     prunes iff the range holds no NULLs
//! * `IS NOT NULL` prunes iff every row is NULL
//!
//! Comparisons never do arithmetic on values, so there is no overflow: the
//! `i64::MIN`/`i64::MAX` boundaries fall out of plain `Ord` comparisons.
//!
//! # Compound predicates
//!
//! `A AND B` prunes when *either* branch proves impossibility; `A OR B`
//! prunes only when *both* branches do; `NOT` is handled by pushing it down
//! through safe operator duals (`NOT (col < v)` becomes `col >= v`, and so
//! on) — no Boolean simplification beyond De Morgan's law on the proof
//! structure.

use super::verdict::PruneVerdict;
use super::zonemap::{NullState, ZoneMap};
use crate::statistics::compare_fields;
use plomid_core::ColumnId;
use plomid_storage::Field;
use std::cmp::Ordering;
use std::fmt;

/// Range comparison operators the pruning layer can evaluate safely.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PruneOperator {
    /// `column = value`.
    Equal,
    /// `column != value` (never prunes on min/max alone).
    NotEqual,
    /// `column < value`.
    Less,
    /// `column <= value`.
    LessOrEqual,
    /// `column > value`.
    Greater,
    /// `column >= value`.
    GreaterOrEqual,
    /// `column IS NULL`.
    IsNull,
    /// `column IS NOT NULL`.
    IsNotNull,
}

impl PruneOperator {
    /// Returns the operator dual under `NOT`, when a safe dual exists.
    ///
    /// Every supported operator has a dual that evaluates `NOT (col OP v)`
    /// soundly. The duals inherit three-valued logic: negating a comparison
    /// is still NULL (not TRUE) for NULL rows, exactly like the dual
    /// operator.
    #[must_use]
    pub fn negate(self) -> Self {
        match self {
            Self::Equal => Self::NotEqual,
            Self::NotEqual => Self::Equal,
            Self::Less => Self::GreaterOrEqual,
            Self::LessOrEqual => Self::Greater,
            Self::Greater => Self::LessOrEqual,
            Self::GreaterOrEqual => Self::Less,
            Self::IsNull => Self::IsNotNull,
            Self::IsNotNull => Self::IsNull,
        }
    }
}

impl fmt::Display for PruneOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Less => "<",
            Self::LessOrEqual => "<=",
            Self::Greater => ">",
            Self::GreaterOrEqual => ">=",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        };
        write!(f, "{text}")
    }
}

/// A pruning predicate over `(column, operator, literal)` leaves plus
/// `AND`/`OR`/`NOT`.
///
/// This is deliberately *not* a second SQL AST: it is the small model the
/// pruning layer can reason about. Anything the SQL layer expresses beyond
/// this shape simply does not bridge, and the caller keeps the range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrunePredicate {
    /// `column OP literal`; `literal` is `None` only for
    /// `IS NULL`/`IS NOT NULL`.
    Compare {
        /// Column the predicate filters.
        column_id: ColumnId,
        /// Comparison to evaluate.
        operator: PruneOperator,
        /// Literal in the column's own [`Field`] representation.
        literal: Option<Field>,
    },
    /// `left AND right`.
    And(Box<PrunePredicate>, Box<PrunePredicate>),
    /// `left OR right`.
    Or(Box<PrunePredicate>, Box<PrunePredicate>),
    /// `NOT inner` (evaluated through safe operator duals).
    Not(Box<PrunePredicate>),
}

impl PrunePredicate {
    /// Builds `column = value`.
    #[must_use]
    pub fn eq(column_id: ColumnId, literal: Field) -> Self {
        Self::Compare {
            column_id,
            operator: PruneOperator::Equal,
            literal: Some(literal),
        }
    }

    /// Builds `column != value`.
    #[must_use]
    pub fn ne(column_id: ColumnId, literal: Field) -> Self {
        Self::Compare {
            column_id,
            operator: PruneOperator::NotEqual,
            literal: Some(literal),
        }
    }

    /// Builds `column IS NULL`.
    #[must_use]
    pub fn is_null(column_id: ColumnId) -> Self {
        Self::Compare {
            column_id,
            operator: PruneOperator::IsNull,
            literal: None,
        }
    }

    /// Builds `column IS NOT NULL`.
    #[must_use]
    pub fn is_not_null(column_id: ColumnId) -> Self {
        Self::Compare {
            column_id,
            operator: PruneOperator::IsNotNull,
            literal: None,
        }
    }

    /// Builds `column OP value` for an arbitrary operator.
    #[must_use]
    pub fn compare(column_id: ColumnId, operator: PruneOperator, literal: Field) -> Self {
        Self::Compare {
            column_id,
            operator,
            literal: Some(literal),
        }
    }

    /// Conjoins two predicates.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self::And(Box::new(self), Box::new(other))
    }

    /// Disjoins two predicates.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self::Or(Box::new(self), Box::new(other))
    }

    /// Negates a predicate (safe duals only).
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn not(self) -> Self {
        Self::Not(Box::new(self))
    }
}

/// Collects the column identities a predicate constrains, in order of first
/// appearance. Used to drive per-column pruning decisions.
#[must_use]
pub fn predicate_columns(predicate: &PrunePredicate) -> Vec<ColumnId> {
    fn collect(predicate: &PrunePredicate, out: &mut Vec<ColumnId>) {
        match predicate {
            PrunePredicate::Compare { column_id, .. } => {
                if !out.contains(column_id) {
                    out.push(*column_id);
                }
            }
            PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
                collect(left, out);
                collect(right, out);
            }
            PrunePredicate::Not(inner) => collect(inner, out),
        }
    }
    let mut out = Vec::new();
    collect(predicate, &mut out);
    out
}

/// Pushes `NOT` inside a predicate using safe operator duals; returns `None`
/// only when the shape has no dual (the caller must then keep the range).
#[must_use]
pub fn negate_predicate(predicate: &PrunePredicate) -> Option<PrunePredicate> {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => Some(PrunePredicate::Compare {
            column_id: *column_id,
            operator: operator.negate(),
            literal: literal.clone(),
        }),
        // De Morgan, applied to the proof structure only.
        PrunePredicate::And(left, right) => Some(PrunePredicate::Or(
            Box::new(negate_predicate(left)?),
            Box::new(negate_predicate(right)?),
        )),
        PrunePredicate::Or(left, right) => Some(PrunePredicate::And(
            Box::new(negate_predicate(left)?),
            Box::new(negate_predicate(right)?),
        )),
        PrunePredicate::Not(inner) => Some((**inner).clone()),
    }
}

/// Returns true when the zone maps of one column *cover* `[start, end)`.
///
/// Coverage is the guard that makes per-column pruning sound: a column may
/// only prove anything about an extent when every row of the extent lies in
/// some zone map of that column. Uncovered rows have no evidence, so an
/// uncovered extent must never be pruned (this defends against metadata
/// whose zones do not align with the extent being pruned, including
/// hand-crafted trailer bytes).
pub fn zones_cover_extent(zones: &[&ZoneMap], start: u64, end: u64) -> bool {
    if start >= end {
        return true;
    }
    let mut sorted: Vec<&ZoneMap> = zones
        .iter()
        .copied()
        .filter(|zone| zone.row_count > 0)
        .collect();
    sorted.sort_by_key(|zone| zone.start_row);
    let mut expected = start;
    for zone in sorted {
        if zone.start_row > expected {
            return false; // gap: rows [expected, zone.start_row) uncovered
        }
        expected = expected.max(zone.end_row());
        if expected >= end {
            return true;
        }
    }
    expected >= end
}

/// Prunes `[start_row, end_row)` using the zone maps of a set of columns.
///
/// Per-column rule (the soundness core at extent granularity):
///
/// 1. For each column the predicate constrains, gather that column's zone
///    maps overlapping the extent.
/// 2. A column can prove impossibility only when its overlapping zones
///    *cover* the whole extent (see [`zones_cover_extent`]) **and** every
///    one of them evaluates to `Prune`.
/// 3. The extent prunes when at least one column proves impossibility.
///    Otherwise it keeps, and the verdict distinguishes Keep (there was
///    applicable metadata) from Unknown (no evidence at all) — both mean
///    "scan".
pub fn prune_extent_with_zones(
    zones: &[ZoneMap],
    predicate: &PrunePredicate,
    start_row: u64,
    end_row: u64,
) -> PruneVerdict {
    if start_row >= end_row {
        // An empty extent holds no rows; nothing can match it.
        return PruneVerdict::Prune;
    }
    let mut saw_evidence = false;
    for column_id in predicate_columns(predicate) {
        let overlapping: Vec<&ZoneMap> = zones
            .iter()
            .filter(|zone| zone.column_id == column_id && zone.overlaps(start_row, end_row))
            .collect();
        if overlapping.is_empty() {
            continue; // no evidence for this column
        }
        saw_evidence = true;
        if zones_cover_extent(&overlapping, start_row, end_row)
            && overlapping
                .iter()
                .all(|zone| zone.evaluate(predicate) == PruneVerdict::Prune)
        {
            return PruneVerdict::Prune;
        }
    }
    if saw_evidence {
        PruneVerdict::Keep
    } else {
        PruneVerdict::Unknown
    }
}

/// SQL three-valued result of evaluating a predicate against one row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Tri {
    /// The predicate is TRUE for this row.
    True,
    /// The predicate is FALSE for this row.
    False,
    /// The predicate is NULL (a required value is NULL, or the operands are
    /// incomparable): SQL treats this as "not in the result set".
    Null,
}

/// Evaluates a predicate against one row's field lookup with SQL
/// three-valued logic.
///
/// `field_of(column_id)` returns the row's value for a column (`None` when
/// the row does not carry that column, treated as NULL). This is the
/// ground-truth semantics the property tests compare pruning against: a
/// range may be pruned only when no row in it evaluates [`Tri::True`].
pub fn row_matches<F>(predicate: &PrunePredicate, field_of: &F) -> Tri
where
    F: Fn(ColumnId) -> Option<Field>,
{
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => {
            let value = field_of(*column_id);
            match operator {
                PruneOperator::IsNull => match value {
                    None => Tri::Null,
                    Some(Field::Null) => Tri::True,
                    Some(_) => Tri::False,
                },
                PruneOperator::IsNotNull => match value {
                    None => Tri::Null,
                    Some(Field::Null) => Tri::False,
                    Some(_) => Tri::True,
                },
                _ => {
                    let Some(literal) = literal else {
                        return Tri::Null;
                    };
                    if matches!(literal, Field::Null) {
                        // `col = NULL` is NULL, never TRUE.
                        return Tri::Null;
                    }
                    match value {
                        None | Some(Field::Null) => Tri::Null,
                        Some(value) => match compare_fields(&value, literal) {
                            None => Tri::Null,
                            Some(ordering) => {
                                let is_true = match operator {
                                    PruneOperator::Equal => ordering == Ordering::Equal,
                                    PruneOperator::NotEqual => ordering != Ordering::Equal,
                                    PruneOperator::Less => ordering == Ordering::Less,
                                    PruneOperator::LessOrEqual => ordering != Ordering::Greater,
                                    PruneOperator::Greater => ordering == Ordering::Greater,
                                    PruneOperator::GreaterOrEqual => ordering != Ordering::Less,
                                    PruneOperator::IsNull | PruneOperator::IsNotNull => false,
                                };
                                if is_true {
                                    Tri::True
                                } else {
                                    Tri::False
                                }
                            }
                        },
                    }
                }
            }
        }
        // AND: any FALSE wins (even over NULLs); TRUE requires both TRUE.
        PrunePredicate::And(left, right) => {
            let l = row_matches(left, field_of);
            let r = row_matches(right, field_of);
            if matches!(l, Tri::False) || matches!(r, Tri::False) {
                Tri::False
            } else if matches!(l, Tri::True) && matches!(r, Tri::True) {
                Tri::True
            } else {
                Tri::Null
            }
        }
        // OR: any TRUE wins; FALSE requires both FALSE; else NULL.
        PrunePredicate::Or(left, right) => {
            let l = row_matches(left, field_of);
            let r = row_matches(right, field_of);
            if matches!(l, Tri::True) || matches!(r, Tri::True) {
                Tri::True
            } else if matches!(l, Tri::False) && matches!(r, Tri::False) {
                Tri::False
            } else {
                Tri::Null
            }
        }
        // NOT TRUE → FALSE, NOT FALSE → TRUE, NOT NULL → NULL.
        PrunePredicate::Not(inner) => match row_matches(inner, field_of) {
            Tri::True => Tri::False,
            Tri::False => Tri::True,
            Tri::Null => Tri::Null,
        },
    }
}

/// Evaluates one leaf predicate against one zone map.
///
/// Soundness per operator follows the module-level proof table. The verdict
/// for a foreign column is `Unknown`: this zone map simply says nothing
/// about a predicate over another column.
fn evaluate_leaf(
    zone: &ZoneMap,
    column_id: ColumnId,
    operator: PruneOperator,
    literal: Option<&Field>,
) -> PruneVerdict {
    if zone.column_id != column_id {
        return PruneVerdict::Unknown;
    }
    match operator {
        PruneOperator::IsNull => {
            if zone.row_count == 0 {
                return PruneVerdict::Prune;
            }
            match zone.null_state {
                NullState::NoNulls => PruneVerdict::Prune,
                NullState::HasNulls | NullState::AllNulls => PruneVerdict::Keep,
            }
        }
        PruneOperator::IsNotNull => {
            if zone.row_count == 0 {
                return PruneVerdict::Prune;
            }
            match zone.null_state {
                NullState::AllNulls => PruneVerdict::Prune,
                NullState::NoNulls | NullState::HasNulls => PruneVerdict::Keep,
            }
        }
        PruneOperator::NotEqual => {
            // min/max can never prove absence for `!=` (min=1, max=10 says
            // nothing about whether 5 occurs). Only ranges with no non-NULL
            // row at all are provably empty.
            if zone.row_count == 0 {
                return PruneVerdict::Prune;
            }
            match zone.null_state {
                NullState::AllNulls => PruneVerdict::Prune,
                NullState::NoNulls | NullState::HasNulls => PruneVerdict::Keep,
            }
        }
        PruneOperator::Equal
        | PruneOperator::Less
        | PruneOperator::LessOrEqual
        | PruneOperator::Greater
        | PruneOperator::GreaterOrEqual => {
            let Some(literal) = literal else {
                return PruneVerdict::Unknown;
            };
            // `col = NULL` (and friends) is never TRUE under SQL semantics.
            if matches!(literal, Field::Null) {
                return PruneVerdict::Prune;
            }
            let (Some(min), Some(max)) = (&zone.min, &zone.max) else {
                // Without bounds only a value-free range can be proven.
                if zone.row_count == 0 {
                    return PruneVerdict::Prune;
                }
                return match zone.null_state {
                    NullState::AllNulls => PruneVerdict::Prune,
                    _ => PruneVerdict::Unknown,
                };
            };
            // `cmp_*` compares the literal against the stored bound. An
            // incomparable literal type cannot reason: keep.
            let (Some(cmp_min), Some(cmp_max)) =
                (compare_fields(literal, min), compare_fields(literal, max))
            else {
                return PruneVerdict::Unknown;
            };
            // The proof table from the module docs. Comparisons only — no
            // arithmetic, so extreme values are safe.
            let proven_empty = match operator {
                PruneOperator::Equal => cmp_min == Ordering::Less || cmp_max == Ordering::Greater,
                PruneOperator::Less => cmp_min != Ordering::Greater,
                PruneOperator::LessOrEqual => cmp_min == Ordering::Less,
                PruneOperator::Greater => cmp_max != Ordering::Less,
                PruneOperator::GreaterOrEqual => cmp_max == Ordering::Greater,
                _ => return PruneVerdict::Unknown,
            };
            if proven_empty {
                PruneVerdict::Prune
            } else {
                PruneVerdict::Keep
            }
        }
    }
}

/// Evaluates a full predicate against one zone map.
///
/// `AND` prunes when either branch proves impossibility; `OR` prunes only
/// when both do; `NOT` is pushed down through safe duals. Unknown branches
/// degrade the verdict to Unknown/Keep — they never upgrade it to Prune.
pub fn evaluate_on_zone(zone: &ZoneMap, predicate: &PrunePredicate) -> PruneVerdict {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => evaluate_leaf(zone, *column_id, *operator, literal.as_ref()),
        PrunePredicate::And(left, right) => {
            match (evaluate_on_zone(zone, left), evaluate_on_zone(zone, right)) {
                (PruneVerdict::Prune, _) | (_, PruneVerdict::Prune) => PruneVerdict::Prune,
                (PruneVerdict::Keep, PruneVerdict::Keep) => PruneVerdict::Keep,
                _ => PruneVerdict::Unknown,
            }
        }
        PrunePredicate::Or(left, right) => {
            match (evaluate_on_zone(zone, left), evaluate_on_zone(zone, right)) {
                (PruneVerdict::Prune, PruneVerdict::Prune) => PruneVerdict::Prune,
                (PruneVerdict::Keep, _) | (_, PruneVerdict::Keep) => PruneVerdict::Keep,
                _ => PruneVerdict::Unknown,
            }
        }
        PrunePredicate::Not(inner) => evaluate_not_on_zone(zone, inner),
    }
}

/// Evaluates `NOT inner` against one zone map.
///
/// Pushes the negation inside through safe duals (see
/// [`PruneOperator::negate`] and `negate_predicate`) and reuses the plain
/// evaluator on the pushed-down shape. `NOT NOT x` collapses to `x`.
pub fn evaluate_not_on_zone(zone: &ZoneMap, inner: &PrunePredicate) -> PruneVerdict {
    match inner {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => evaluate_leaf(zone, *column_id, operator.negate(), literal.as_ref()),
        // NOT (A AND B) == NOT A OR NOT B: prune only when both duals prune.
        PrunePredicate::And(left, right) => match (
            evaluate_not_on_zone(zone, left),
            evaluate_not_on_zone(zone, right),
        ) {
            (PruneVerdict::Prune, PruneVerdict::Prune) => PruneVerdict::Prune,
            (PruneVerdict::Keep, _) | (_, PruneVerdict::Keep) => PruneVerdict::Keep,
            _ => PruneVerdict::Unknown,
        },
        // NOT (A OR B) == NOT A AND NOT B: prune when either dual prunes.
        PrunePredicate::Or(left, right) => match (
            evaluate_not_on_zone(zone, left),
            evaluate_not_on_zone(zone, right),
        ) {
            (PruneVerdict::Prune, _) | (_, PruneVerdict::Prune) => PruneVerdict::Prune,
            (PruneVerdict::Keep, PruneVerdict::Keep) => PruneVerdict::Keep,
            _ => PruneVerdict::Unknown,
        },
        PrunePredicate::Not(inner) => evaluate_on_zone(zone, inner),
    }
}
