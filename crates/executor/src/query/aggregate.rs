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
//! Aggregate accumulators and grouped-expression evaluation.
//!
//! [`AggregateAccumulator`] is the state machine behind every aggregate
//! (`COUNT`, `SUM`, `ARRAY_AGG`, `JSON_AGG`, ...); the join engine reuses it
//! through `crate::query`, so it stays `pub`. This module also collects
//! the aggregate slots a query needs and evaluates the expressions of one
//! finished group.

use super::expr::evaluate_expression;
use super::scan::{number, value_cmp};
use super::select::expression_has_aggregate;
use crate::catalog_fn::is_aggregate_function;
use crate::coerce::number_f64;
use crate::error::{SqlError, SqlResult};
use crate::row::values_equal;
use plomid_core::{ErrorKind, PlomidError};
use plomid_sql::Expression;
use plomid_sql::SelectTarget;
use plomid_sql::TableSchema;
use plomid_sql::Value;
use std::collections::HashSet;

#[derive(Clone)]
pub enum AggregateAccumulator {
    CountStar(i64),
    Count(i64),
    SumN(i64, bool),
    SumF(f64, bool),
    SumNumeric(Option<plomid_types::Numeric>, bool),
    AvgN {
        sum: i64,
        count: i64,
    },
    AvgF {
        sum: f64,
        count: i64,
    },
    AvgNumeric {
        sum: plomid_types::Numeric,
        count: i64,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    // JSON aggregates accumulate the full per-row value list (not just
    // numeric state) so json_agg/jsonb_agg and the object aggregates can
    // finalize through the shared json_aggregate_result helpers. The
    // single-table fast path previously fell back to CountStar state for
    // these names, which produced wrong results whenever the query reached
    // grouped execution here instead of the general join engine.
    JsonValues(Vec<Value>),
    JsonPairs(Vec<(Value, Value)>),
    /// `COUNT(DISTINCT expr)`: the canonical SQL texts of the non-`NULL`
    /// values seen, mirroring the join engine's `seen` set and the
    /// single-target `COUNT(DISTINCT ...)` fast path. Textual (not `Value`)
    /// keys keep `Int8(1)`, `Float8(1.0)`, and `Numeric(1.00)` unified, as
    /// every other deduplication path in this engine does.
    CountDistinct(HashSet<String>),
}

impl AggregateAccumulator {
    pub fn new(name: &str, arg: Option<&Expression>, distinct: bool) -> Self {
        // JSON aggregates keep every input value (SQL NULL included for the
        // array forms) so finalization can reuse the shared JSON helpers.
        // Object aggregates store key/value pairs separately because the
        // two-argument form does not fit the single-value accumulator.
        if crate::json::is_json_aggregate(name) {
            if crate::json::is_json_object_aggregate(name) {
                return Self::JsonPairs(Vec::new());
            }
            return Self::JsonValues(Vec::new());
        }
        // `COUNT(DISTINCT expr)` (but never `COUNT(DISTINCT *)`, which keeps
        // the historical `COUNT(*)` behavior) deduplicates through the set
        // above; every other `DISTINCT` combination keeps its historical
        // accumulator exactly as before.
        if distinct
            && name.eq_ignore_ascii_case("count")
            && !matches!(arg, Some(Expression::Star) | None)
        {
            return Self::CountDistinct(HashSet::new());
        }
        match name.to_uppercase().as_str() {
            "COUNT" if matches!(arg, Some(Expression::Star)) => Self::CountStar(0),
            "COUNT" => Self::Count(0),
            "SUM" => Self::SumN(0, false),
            "AVG" => Self::AvgN { sum: 0, count: 0 },
            "MIN" => Self::Min(None),
            "MAX" => Self::Max(None),
            _ => Self::CountStar(0),
        }
    }

    /// Accumulates one row by evaluating `arg` in the single-table fast path
    /// context, or COUNT(*) semantics when `arg` is None/Star.
    pub fn step(
        &mut self,
        arg: Option<&Expression>,
        row: &[Value],
        schema: &TableSchema,
    ) -> SqlResult<()> {
        let value = if let Some(expr) = arg {
            if matches!(expr, Expression::Star) {
                None
            } else {
                Some(evaluate_expression(row, schema, expr)?)
            }
        } else {
            None
        };
        if let Self::CountDistinct(seen) = self {
            // Distinct counting skips NULLs like COUNT and unifies values by
            // canonical text, exactly like the single-target fast path.
            if let Some(value) = value.as_ref() {
                if !value.is_null() {
                    seen.insert(value.to_sql_text());
                }
            }
            return Ok(());
        }
        self.step_value(value.as_ref());
        Ok(())
    }

    /// Accumulates a pre-evaluated value into aggregate state. Used by the
    /// join-aware engine where expression evaluation uses a different row
    /// context than the single-table fast path.
    pub fn step_value(&mut self, value: Option<&Value>) {
        match self {
            Self::CountStar(n) => *n += 1,
            Self::Count(n) => {
                if value.is_some_and(|v| !v.is_null()) {
                    *n += 1;
                }
            }
            Self::CountDistinct(seen) => {
                if let Some(value) = value {
                    if !value.is_null() {
                        seen.insert(value.to_sql_text());
                    }
                }
            }
            Self::SumN(acc, has_any) => {
                if let Some(v) = value {
                    if let Some(n) = number(v) {
                        *acc += n;
                        *has_any = true;
                    } else if let Value::Numeric(n) = v {
                        *self = Self::SumNumeric(
                            Some(
                                n.clone()
                                    .add(plomid_types::Numeric::from_i64(*acc))
                                    .unwrap_or_else(|_| n.clone()),
                            ),
                            true,
                        );
                    } else if let Some(n) = number_f64(v) {
                        *self = Self::SumF(n + *acc as f64, true);
                    }
                }
            }
            Self::SumF(acc, has_any) => {
                if let Some(n) = value.and_then(number_f64) {
                    *acc += n;
                    *has_any = true;
                }
            }
            Self::SumNumeric(acc, has_any) => {
                // §P2 (AVG-Decimal): the feeder used to consume ONLY `Numeric`
                // after promotion, silently dropping later integers/floats
                // (order-dependent loss: `[numeric, int]` diverged from
                // `[int, numeric]`). Convert integers exactly and floats via
                // `Numeric::from_f64` so every non-NULL number contributes.
                if let Some(v) = value {
                    if v.is_null() {
                        return;
                    }
                    let as_numeric = match v {
                        Value::Numeric(n) => Some(n.clone()),
                        Value::Int2(n) => Some(plomid_types::Numeric::from_i64(i64::from(*n))),
                        Value::Int4(n) => Some(plomid_types::Numeric::from_i64(i64::from(*n))),
                        Value::Int8(n) => Some(plomid_types::Numeric::from_i64(*n)),
                        Value::Float4(n) => Some(plomid_types::Numeric::from_f64(f64::from(*n))),
                        Value::Float8(n) => Some(plomid_types::Numeric::from_f64(*n)),
                        _ => None,
                    };
                    if let Some(n) = as_numeric {
                        *acc = acc
                            .clone()
                            .map(|existing| existing.add(n.clone()).unwrap_or_else(|_| n.clone()))
                            .or_else(|| Some(n.clone()));
                        *has_any = true;
                    }
                }
            }
            Self::AvgN { sum, count } => {
                if let Some(v) = value {
                    if let Some(n) = number(v) {
                        *sum += n;
                        *count += 1;
                    } else if let Value::Numeric(n) = v {
                        *self = Self::AvgNumeric {
                            sum: n
                                .clone()
                                .add(plomid_types::Numeric::from_i64(*sum))
                                .unwrap_or_else(|_| n.clone()),
                            count: *count + 1,
                        };
                    } else if let Some(n) = number_f64(v) {
                        *self = Self::AvgF {
                            sum: n + *sum as f64,
                            count: *count + 1,
                        };
                    }
                }
            }
            Self::AvgF { sum, count } => {
                if let Some(n) = value.and_then(number_f64) {
                    *sum += n;
                    *count += 1
                }
            }
            Self::AvgNumeric { sum, count } => {
                // §P2 (AVG-Decimal): same order-dependent loss as `SumNumeric`
                // — later integers/floats were ignored after promotion.
                // Convert so every non-NULL number contributes to sum+count.
                if let Some(v) = value {
                    if v.is_null() {
                        return;
                    }
                    let as_numeric = match v {
                        Value::Numeric(n) => Some(n.clone()),
                        Value::Int2(n) => Some(plomid_types::Numeric::from_i64(i64::from(*n))),
                        Value::Int4(n) => Some(plomid_types::Numeric::from_i64(i64::from(*n))),
                        Value::Int8(n) => Some(plomid_types::Numeric::from_i64(*n)),
                        Value::Float4(n) => Some(plomid_types::Numeric::from_f64(f64::from(*n))),
                        Value::Float8(n) => Some(plomid_types::Numeric::from_f64(*n)),
                        _ => None,
                    };
                    if let Some(n) = as_numeric {
                        *sum = sum.clone().add(n).unwrap_or_else(|_| sum.clone());
                        *count += 1;
                    }
                }
            }
            Self::Min(acc) => {
                if let Some(v) = value {
                    if !v.is_null() {
                        match acc {
                            None => *acc = Some(v.clone()),
                            Some(cur) => {
                                if let Some(ord) = value_cmp(v, cur) {
                                    if ord == std::cmp::Ordering::Less {
                                        *acc = Some(v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Self::Max(acc) => {
                if let Some(v) = value {
                    if !v.is_null() {
                        match acc {
                            None => *acc = Some(v.clone()),
                            Some(cur) => {
                                if let Some(ord) = value_cmp(v, cur) {
                                    if ord == std::cmp::Ordering::Greater {
                                        *acc = Some(v.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // json_agg/jsonb_agg keep SQL NULL rows (PostgreSQL aggregates a
            // JSON null for each NULL input); object aggregates skip NULL
            // keys at finalization time via the shared helper.
            Self::JsonValues(values) => {
                values.push(value.cloned().unwrap_or(Value::Null));
            }
            Self::JsonPairs(_) => {}
        }
    }

    /// Accumulates the (key, value) pair of an object aggregate. The
    /// single-table fast path evaluates both argument expressions per row,
    /// mirroring the join engine's step_pair handling.
    pub fn step_pair(&mut self, key: Value, value: Value) {
        if let Self::JsonPairs(pairs) = self {
            if !key.is_null() {
                pairs.push((key, value));
            }
        }
    }

    /// Merges another partial aggregate state into `self` (parallel reduction).
    ///
    /// Both sides must be partial states of the SAME aggregate slot (built by
    /// [`AggregateAccumulator::new`] with identical arguments and fed
    /// disjoint row partitions); merging in partition order must equal
    /// feeding the concatenated rows. No threading or execution wiring
    /// happens here: this is pure merge algebra for future parallel/grouped
    /// reduction.
    ///
    /// Same-variant merges reuse the exact feed operators (`+=` for
    /// counts/integer sums, `Numeric::add` with the feed's overflow fallback,
    /// `HashSet::extend` for distinct sets, `Vec::extend` for JSON buffers,
    /// [`value_cmp`](super::scan::value_cmp) folds for `MIN`/`MAX`), so
    /// overflow behavior matches feeding bit-for-bit.
    ///
    /// # Cross-variant `SUM`/`AVG` partials (same slot, different state)
    ///
    /// `SUM` always starts as `SumN` and `AVG` as `AvgN`; feeding the first
    /// non-integer value promotes the whole state (`SumN` -> `SumF` on float,
    /// `SumN` -> `SumNumeric` on `NUMERIC`, likewise for `AVG`). Two
    /// same-slot partitions can therefore legitimately hold different
    /// variants (e.g. an ints-only partition stays `SumN` while a partition
    /// that saw a float became `SumF`, and empty partitions never promote).
    /// These intra-family pairs merge by promoting to the common
    /// representation, mirroring the feed promotion expressions exactly.
    /// Left-wins on type: a `SumNumeric`/`AvgNumeric` left side converts an
    /// integer/float tail to `Numeric` (exact for ints, `from_f64` for
    /// floats); a `SumF`/`AvgF` left side converts a `NUMERIC` tail via
    /// `to_f64`. This reproduces the single-pass fold where the first
    /// non-integer decides the representation (§P2 AVG-Decimal fix: the
    /// feeder now consumes every number after promotion instead of
    /// silently dropping later ints/floats).
    ///
    /// # Mismatched variants (different families)
    ///
    /// Choice: `merge_into` stays infallible like [`step_value`](Self::step_value)
    /// (no `Result`, no `panic!`), asserts the invariant with `debug_assert!`
    /// in debug/test builds, and is a documented no-op (keep left) in
    /// release. Rationale: the slot constructor maps each aggregate to
    /// exactly one variant family, so cross-family pairs are unreachable by
    /// construction; a `Result` would force every future parallel caller to
    /// handle an impossible error, `panic!` would turn a logic bug into a
    /// crash, and silently combining (e.g. adding a count into a sum) would
    /// corrupt results. Failing closed here means debug builds catch the bug
    /// at the merge site while release builds keep serving the left partial
    /// instead of crashing.
    ///
    /// # Float / `NUMERIC` ordering
    ///
    /// Integer, count, `MIN`/`MAX`, distinct-set, and JSON merges are
    /// bit-exact versus single-pass feeding. `f64` sums (`SumF`/`AvgF`)
    /// depend on reduction order: merging partitions associates the additions
    /// differently than one sequential fold, so results can differ in the
    /// last ulp (NaN/inf propagate per IEEE-754 in both paths; finite inputs
    /// of sane magnitude stay finite). `NUMERIC` addition itself is decimal
    /// exact, so `SumNumeric`/`AvgNumeric` merges are bit-exact barring the
    /// (practically unreachable) overflow fallback, which keeps one side
    /// exactly like the feeder does. No attempt is made to force float
    /// merges bit-identical to sequential feeding.
    ///
    /// Staged landing: parallel reduction is not wired yet, so non-test
    /// builds see no caller; the `allow` below lapses once callers land.
    #[allow(dead_code)]
    pub fn merge_into(&mut self, other: Self) {
        // Take ownership of both sides so promotion arms can replace the
        // whole state (e.g. `SumN` -> `SumF`) without fighting the borrow
        // checker. The placeholder is overwritten on every path below and is
        // never observed.
        let owned = std::mem::replace(self, Self::CountStar(0));
        *self = Self::merge_owned(owned, other);
    }

    /// Owned merge worker behind [`merge_into`](Self::merge_into).
    /// Reachable only via `merge_into` (allowed dead until wired).
    #[allow(dead_code)]
    fn merge_owned(left: Self, right: Self) -> Self {
        // SUM/AVG families route to promotion-aware helpers (both sides are
        // guaranteed in-family here because the slot constructor assigns
        // exactly one family per slot).
        if matches!(
            &left,
            Self::SumN(..) | Self::SumF(..) | Self::SumNumeric(..)
        ) && matches!(
            &right,
            Self::SumN(..) | Self::SumF(..) | Self::SumNumeric(..)
        ) {
            return Self::merge_sum(left, right);
        }
        if matches!(
            &left,
            Self::AvgN { .. } | Self::AvgF { .. } | Self::AvgNumeric { .. }
        ) && matches!(
            &right,
            Self::AvgN { .. } | Self::AvgF { .. } | Self::AvgNumeric { .. }
        ) {
            return Self::merge_avg(left, right);
        }
        match (left, right) {
            (Self::CountStar(mut total), Self::CountStar(extra)) => {
                total += extra;
                Self::CountStar(total)
            }
            (Self::Count(mut total), Self::Count(extra)) => {
                total += extra;
                Self::Count(total)
            }
            (Self::CountDistinct(mut seen), Self::CountDistinct(extra)) => {
                seen.extend(extra);
                Self::CountDistinct(seen)
            }
            (Self::Min(current), Self::Min(incoming)) => {
                Self::Min(Self::merge_extreme(current, incoming, true))
            }
            (Self::Max(current), Self::Max(incoming)) => {
                Self::Max(Self::merge_extreme(current, incoming, false))
            }
            (Self::JsonValues(mut values), Self::JsonValues(extra)) => {
                // Concatenation in partition order reproduces the single-pass
                // row order exactly; finalization (`finalize_json`) is a pure
                // function of that order.
                values.extend(extra);
                Self::JsonValues(values)
            }
            (Self::JsonPairs(mut pairs), Self::JsonPairs(extra)) => {
                // Pairs are already NULL-key filtered by `step_pair`, so plain
                // concatenation preserves single-pass semantics (duplicate
                // keys resolve identically at finalization).
                pairs.extend(extra);
                Self::JsonPairs(pairs)
            }
            // Cross-family pairs are unreachable by construction (see
            // `merge_into`): keep the left partial unchanged.
            (left, right) => {
                debug_assert!(
                    std::mem::discriminant(&left) == std::mem::discriminant(&right),
                    "AggregateAccumulator::merge_into: mismatched variants are \
                     unreachable by construction (same-slot partials)"
                );
                left
            }
        }
    }

    /// Promotion-aware merge for two `SUM`-family partials (see `merge_into`).
    /// Callers guarantee both sides are `SumN`/`SumF`/`SumNumeric`.
    /// Reachable only via `merge_into` (allowed dead until wired).
    #[allow(dead_code)]
    fn merge_sum(left: Self, right: Self) -> Self {
        // An empty side fed zero rows: adopting/keeping the other side is
        // bitwise exact (no `+ 0.0` that could flip signed-zero/NaN bits).
        if !matches!(
            &left,
            Self::SumN(_, true) | Self::SumF(_, true) | Self::SumNumeric(_, true)
        ) {
            return right;
        }
        if !matches!(
            &right,
            Self::SumN(_, true) | Self::SumF(_, true) | Self::SumNumeric(_, true)
        ) {
            return left;
        }
        match (left, right) {
            (Self::SumN(mut total, _), Self::SumN(extra, _)) => {
                total += extra;
                Self::SumN(total, true)
            }
            (Self::SumF(mut total, _), Self::SumF(extra, _)) => {
                total += extra;
                Self::SumF(total, true)
            }
            (Self::SumNumeric(current, _), Self::SumNumeric(incoming, _)) => {
                let sum = match (current, incoming) {
                    (Some(existing), Some(new)) => Some(existing.add(new.clone()).unwrap_or(new)),
                    (existing, None) => existing,
                    (None, incoming) => incoming,
                };
                Self::SumNumeric(sum, true)
            }
            (Self::SumN(total, _), Self::SumF(extra, _)) => {
                // Mirrors the feed promotion `n + acc as f64`.
                Self::SumF(total as f64 + extra, true)
            }
            (Self::SumN(total, _), Self::SumNumeric(incoming, _)) => {
                // Mirrors the feed promotion `n.add(from_i64(acc))`.
                let sum = incoming.map(|new| {
                    new.clone()
                        .add(plomid_types::Numeric::from_i64(total))
                        .unwrap_or(new)
                });
                Self::SumNumeric(sum, true)
            }
            (Self::SumF(mut total, _), Self::SumN(extra, _)) => {
                // The `SumF` feeder consumes integers through `number_f64`.
                total += extra as f64;
                Self::SumF(total, true)
            }
            (Self::SumF(mut total, _), Self::SumNumeric(incoming, _)) => {
                // Likewise for `NUMERIC` values (`to_f64` is `number_f64`).
                if let Some(new) = incoming {
                    total += new.to_f64();
                }
                Self::SumF(total, true)
            }
            // §P2: left-numeric wins by converting the right tail to Numeric
            // (mirrors the fixed feeder, which consumes ints/floats after
            // promotion). Reproduces the single-pass fold where the first
            // non-integer was numeric.
            (Self::SumNumeric(current, _), Self::SumN(extra, _)) => {
                let addend = plomid_types::Numeric::from_i64(extra);
                let sum = match current {
                    Some(existing) => Some(existing.add(addend.clone()).unwrap_or(addend)),
                    None => Some(addend),
                };
                Self::SumNumeric(sum, true)
            }
            (Self::SumNumeric(current, _), Self::SumF(extra, _)) => {
                let addend = plomid_types::Numeric::from_f64(extra);
                let sum = match current {
                    Some(existing) => Some(existing.add(addend.clone()).unwrap_or(addend)),
                    None => Some(addend),
                };
                Self::SumNumeric(sum, true)
            }
            // Defensive: both sides are non-empty `SUM`-family, and every
            // same-family pair is covered above.
            (left, right) => {
                debug_assert!(
                    std::mem::discriminant(&left) == std::mem::discriminant(&right),
                    "AggregateAccumulator::merge_sum: unreachable pair"
                );
                left
            }
        }
    }

    /// Promotion-aware merge for two `AVG`-family partials (see `merge_into`).
    /// Callers guarantee both sides are `AvgN`/`AvgF`/`AvgNumeric`.
    /// Reachable only via `merge_into` (allowed dead until wired).
    #[allow(dead_code)]
    fn merge_avg(left: Self, right: Self) -> Self {
        // A zero-count side fed zero rows: adopting/keeping the other side is
        // bitwise exact.
        if !matches!(
            &left,
            Self::AvgN { count: 1.., .. }
                | Self::AvgF { count: 1.., .. }
                | Self::AvgNumeric { count: 1.., .. }
        ) {
            return right;
        }
        if !matches!(
            &right,
            Self::AvgN { count: 1.., .. }
                | Self::AvgF { count: 1.., .. }
                | Self::AvgNumeric { count: 1.., .. }
        ) {
            return left;
        }
        match (left, right) {
            (
                Self::AvgN {
                    sum: mut total,
                    count: mut n,
                },
                Self::AvgN { sum, count },
            ) => {
                total += sum;
                n += count;
                Self::AvgN {
                    sum: total,
                    count: n,
                }
            }
            (
                Self::AvgF {
                    sum: mut total,
                    count: mut n,
                },
                Self::AvgF { sum, count },
            ) => {
                total += sum;
                n += count;
                Self::AvgF {
                    sum: total,
                    count: n,
                }
            }
            (
                Self::AvgNumeric {
                    sum: existing,
                    count: mut n,
                },
                Self::AvgNumeric { sum, count },
            ) => {
                n += count;
                Self::AvgNumeric {
                    // Mirrors the feeder, which keeps the accumulated sum when
                    // `Numeric::add` reports overflow.
                    sum: existing.clone().add(sum).unwrap_or(existing),
                    count: n,
                }
            }
            (
                Self::AvgN {
                    sum: total,
                    count: n,
                },
                Self::AvgF { sum, count },
            ) => Self::AvgF {
                sum: total as f64 + sum,
                count: n + count,
            },
            (
                Self::AvgN {
                    sum: total,
                    count: n,
                },
                Self::AvgNumeric { sum, count },
            ) => Self::AvgNumeric {
                // Mirrors the feed promotion `n.add(from_i64(sum))`.
                sum: sum
                    .clone()
                    .add(plomid_types::Numeric::from_i64(total))
                    .unwrap_or(sum),
                count: n + count,
            },
            (
                Self::AvgF {
                    sum: mut total,
                    count: mut n,
                },
                Self::AvgN { sum, count },
            ) => {
                total += sum as f64;
                n += count;
                Self::AvgF {
                    sum: total,
                    count: n,
                }
            }
            (
                Self::AvgF {
                    sum: mut total,
                    count: mut n,
                },
                Self::AvgNumeric { sum, count },
            ) => {
                total += sum.to_f64();
                n += count;
                Self::AvgF {
                    sum: total,
                    count: n,
                }
            }
            // §P2: left-numeric wins by converting the right tail to Numeric
            // (mirrors the fixed feeder). Reproduces the single-pass fold
            // where the first non-integer was numeric.
            (Self::AvgNumeric { sum, count: n }, Self::AvgN { sum: extra, count }) => {
                let addend = plomid_types::Numeric::from_i64(extra);
                Self::AvgNumeric {
                    sum: sum.add(addend.clone()).unwrap_or(addend),
                    count: n + count,
                }
            }
            (Self::AvgNumeric { sum, count: n }, Self::AvgF { sum: extra, count }) => {
                let addend = plomid_types::Numeric::from_f64(extra);
                Self::AvgNumeric {
                    sum: sum.add(addend.clone()).unwrap_or(addend),
                    count: n + count,
                }
            }
            // Defensive: both sides are non-empty `AVG`-family, and every
            // same-family pair is covered above.
            (left, right) => {
                debug_assert!(
                    std::mem::discriminant(&left) == std::mem::discriminant(&right),
                    "AggregateAccumulator::merge_avg: unreachable pair"
                );
                left
            }
        }
    }

    /// Folds one `MIN`/`MAX` candidate into the current extreme, mirroring
    /// `step_value` exactly (including ignoring values `value_cmp` cannot
    /// order, such as NaN against a number). Left-biased like the sequential
    /// fold, so partition-order merging is bit-exact.
    /// Reachable only via `merge_into` (allowed dead until wired).
    #[allow(dead_code)]
    fn merge_extreme(
        current: Option<Value>,
        incoming: Option<Value>,
        want_min: bool,
    ) -> Option<Value> {
        match (current, incoming) {
            (None, incoming) => incoming,
            (current @ Some(_), None) => current,
            (Some(cur), Some(val)) => {
                let take = match value_cmp(&val, &cur) {
                    Some(std::cmp::Ordering::Less) => want_min,
                    Some(std::cmp::Ordering::Greater) => !want_min,
                    _ => false,
                };
                if take {
                    Some(val)
                } else {
                    Some(cur)
                }
            }
        }
    }

    pub fn finalize(&self) -> Value {
        match self {
            Self::CountStar(n) => Value::Int8(*n),
            Self::Count(n) => Value::Int8(*n),
            Self::CountDistinct(seen) => Value::Int8(seen.len() as i64),
            // PostgreSQL: SUM(empty) = NULL, not 0. Track whether any non-NULL
            // value was processed so we can distinguish "no input" from "sum is 0".
            Self::SumN(acc, has_any) => {
                if *has_any {
                    Value::Int8(*acc)
                } else {
                    Value::Null
                }
            }
            Self::SumF(acc, has_any) => {
                // PostgreSQL `SUM(float8)` returns `float8` (OID 701), not
                // `numeric`. Keep the f64 accumulator exact through finalize
                // so wire type (FLOAT8) and value agree (§P2 AVG-Decimal).
                if *has_any {
                    Value::Float8(*acc)
                } else {
                    Value::Null
                }
            }
            Self::SumNumeric(acc, has_any) => {
                if *has_any {
                    Value::Numeric(
                        acc.clone()
                            .unwrap_or_else(|| plomid_types::Numeric::from_i64(0)),
                    )
                } else {
                    Value::Null
                }
            }
            Self::AvgN { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    // PostgreSQL AVG(integer) returns NUMERIC, not integer.
                    // Perform division in Numeric space to avoid integer truncation,
                    // then normalize so integral means render as "21" not "21.0000".
                    let sum_numeric = plomid_types::Numeric::from_i64(*sum);
                    let count_numeric = plomid_types::Numeric::from_i64(*count);
                    sum_numeric
                        .div(count_numeric)
                        .map(|v| Value::Numeric(v.normalize()))
                        .unwrap_or_else(|_| Value::Numeric(plomid_types::Numeric::from_i64(*sum)))
                }
            }
            Self::AvgF { sum, count } => {
                // PostgreSQL `AVG(float8)` returns `float8` (OID 701), not
                // `numeric`. Return Float8 so wire type and value agree
                // (§P2 AVG-Decimal; previously returned Numeric).
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Float8(*sum / *count as f64)
                }
            }
            Self::AvgNumeric { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Numeric(
                        sum.clone()
                            .div(plomid_types::Numeric::from_i64(*count))
                            .map(|v| v.normalize())
                            .unwrap_or_else(|_| sum.clone()),
                    )
                }
            }
            Self::Min(v) => v.clone().unwrap_or(Value::Null),
            Self::Max(v) => v.clone().unwrap_or(Value::Null),
            Self::JsonValues(_) | Self::JsonPairs(_) => Value::Null,
        }
    }

    /// Finalizes a JSON aggregate with its function name so the shared
    /// json_aggregate_result / json_object_aggregate_result helpers pick the
    /// json vs jsonb output encoding. Separated from finalize() because the
    /// accumulator alone does not retain the original function name.
    pub fn finalize_json(&self, name: &str) -> Value {
        match self {
            Self::JsonValues(values) => {
                crate::json::json_aggregate_result(name, values).unwrap_or(Value::Null)
            }
            Self::JsonPairs(pairs) => {
                crate::json::json_object_aggregate_result(name, pairs).unwrap_or(Value::Null)
            }
            _ => self.finalize(),
        }
    }
}

#[derive(Clone)]
pub struct AggregateSlot {
    pub name: String,
    pub arg: Option<Expression>,
    /// Second argument of two-argument aggregates
    /// (`json_object_agg(key, value)` / `jsonb_object_agg(key, value)`).
    /// Stored separately because the join engine's accumulator consumes the
    /// pair together; without it the value half would be dropped.
    pub second_arg: Option<Expression>,
    /// FILTER predicate: only rows where this evaluates to TRUE are passed
    /// to the aggregate. NULL or FALSE results exclude the row.
    pub filter: Option<Expression>,
    /// `DISTINCT` modifier of the aggregate call (`COUNT(DISTINCT expr)`).
    /// Carried so grouped lookup and accumulator construction can tell
    /// `COUNT(x)` apart from `COUNT(DISTINCT x)`, which previously degraded
    /// silently into the same slot.
    pub distinct: bool,
}

impl AggregateSlot {
    pub fn arg(&self) -> Option<&Expression> {
        self.arg.as_ref()
    }

    pub fn second_arg(&self) -> Option<&Expression> {
        self.second_arg.as_ref()
    }

    pub fn filter(&self) -> Option<&Expression> {
        self.filter.as_ref()
    }

    pub fn distinct(&self) -> bool {
        self.distinct
    }
}

pub fn collect_aggregate_slots(
    targets: &[SelectTarget],
    having: Option<&Expression>,
) -> Vec<AggregateSlot> {
    let mut slots = Vec::new();
    for target in targets {
        visit_target(target, &mut slots);
    }
    if let Some(expr) = having {
        visit_expr(expr, &mut slots);
    }
    slots
}

pub fn visit_target(target: &SelectTarget, slots: &mut Vec<AggregateSlot>) {
    match target {
        SelectTarget::All | SelectTarget::Function(_) => {}
        SelectTarget::FunctionCall { name, args } if is_aggregate_function(name) => {
            slots.push(AggregateSlot {
                name: name.clone(),
                arg: args.first().cloned(),
                second_arg: args.get(1).cloned(),
                filter: None,
                // The bare-call shape carries no DISTINCT modifier.
                distinct: false,
            });
        }
        SelectTarget::Expr { expr, .. } => visit_expr(expr, slots),
        SelectTarget::Aliased { target, .. } => visit_target(target, slots),
        _ => {}
    }
}

pub fn visit_expr(expr: &Expression, slots: &mut Vec<AggregateSlot>) {
    match expr {
        Expression::FunctionCall {
            name,
            args,
            distinct,
            filter,
            ..
        } if is_aggregate_function(name) => {
            slots.push(AggregateSlot {
                name: name.clone(),
                arg: args.first().cloned(),
                second_arg: args.get(1).cloned(),
                filter: filter.as_deref().cloned(),
                distinct: *distinct,
            });
            // An aggregate nested inside another aggregate's argument (for
            // example `jsonb_path_query_array(jsonb_agg(payload), ...)`) is
            // still evaluated per input row and accumulated through the same
            // aggregate state. Descend into the arguments so the inner call
            // is registered; the projection lookup matches on the full
            // expression and therefore keeps working.
            for arg in args {
                visit_expr(arg, slots);
            }
        }
        // Aggregates nested inside ordinary scalar calls (for example
        // `jsonb_path_query_array(jsonb_agg(payload), '...')`) are evaluated
        // per input row and accumulated through the query's aggregate state.
        // Without descending here the outer scalar call reaches projection
        // with no registered slot and falls back to "cannot be evaluated
        // here". Recursing preserves the aggregate framework instead of
        // special-casing particular function names.
        Expression::FunctionCall { args, .. } => {
            for arg in args {
                visit_expr(arg, slots);
            }
        }
        Expression::ColumnRef(_) | Expression::Literal(_) | Expression::Star => {}
        Expression::Equal(l, r)
        | Expression::NotEqual(l, r)
        | Expression::Less(l, r)
        | Expression::LessOrEqual(l, r)
        | Expression::Greater(l, r)
        | Expression::GreaterOrEqual(l, r)
        | Expression::And(l, r)
        | Expression::Or(l, r)
        | Expression::Add(l, r)
        | Expression::Subtract(l, r)
        | Expression::Multiply(l, r)
        | Expression::Divide(l, r)
        | Expression::Modulo(l, r)
        | Expression::Concat(l, r) => {
            visit_expr(l, slots);
            visit_expr(r, slots);
        }
        Expression::IsNull(inner)
        | Expression::IsNotNull(inner)
        | Expression::Not(inner)
        | Expression::Negate(inner) => visit_expr(inner, slots),
        // Conditional wrappers can hide aggregates (`CASE WHEN COUNT(*) =
        // ...`, `COALESCE(SUM(x), 0)`, `NULLIF(COUNT(*), 0)`): descend like
        // the join engine's collector so slots register and projection
        // finds them instead of falling back to "cannot be evaluated here".
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            if let Some(op) = operand {
                visit_expr(op, slots);
            }
            for (cond, value) in whens {
                visit_expr(cond, slots);
                visit_expr(value, slots);
            }
            if let Some(def) = default {
                visit_expr(def, slots);
            }
        }
        Expression::Coalesce(args) => {
            for arg in args {
                visit_expr(arg, slots);
            }
        }
        Expression::NullIf(left, right) => {
            visit_expr(left, slots);
            visit_expr(right, slots);
        }
        _ => {}
    }
}

pub fn lookup_aggregate(
    slots: &[AggregateSlot],
    accs: &[AggregateAccumulator],
    name: &str,
    arg: Option<&Expression>,
    filter: Option<&Expression>,
    distinct: bool,
) -> Option<Value> {
    let target_name = name.to_uppercase();
    for (i, slot) in slots.iter().enumerate() {
        if slot.name.to_uppercase() != target_name {
            continue;
        }
        if slot.filter().as_ref().map(|e| e as &Expression) != filter {
            continue;
        }
        // DISTINCT is part of a slot's identity: without it
        // `COUNT(x)` and `COUNT(DISTINCT x)` in one query resolve to the
        // same accumulator and one of them answers wrongly.
        if slot.distinct() != distinct {
            continue;
        }
        let same_arg = match (slot.arg.as_ref(), arg) {
            (None, None) => true,
            (Some(Expression::Star), Some(Expression::Star)) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
        if same_arg {
            // JSON aggregates need their function name to pick the json vs
            // jsonb encoding; other aggregates finalize from state alone.
            // Routing through finalize_json keeps one shared JSON result path
            // for both the single-table and join engines.
            return Some(accs[i].finalize_json(&slot.name));
        }
    }
    None
}

pub fn evaluate_grouped_expression(
    row: &[Value],
    schema: &TableSchema,
    expr: &Expression,
    slots: &[AggregateSlot],
    accs: &[AggregateAccumulator],
) -> SqlResult<Value> {
    match expr {
        Expression::FunctionCall {
            name,
            args,
            distinct,
            filter,
            ..
        } if is_aggregate_function(name) => Ok(lookup_aggregate(
            slots,
            accs,
            name,
            args.first(),
            filter.as_deref(),
            *distinct,
        )
        .unwrap_or(Value::Null)),
        // Scalar calls wrapping aggregates (for example
        // `jsonb_path_query_array(jsonb_agg(payload), '...')`) are evaluated
        // after aggregation: each nested aggregate resolves through the
        // accumulated slots, then the outer scalar function runs over those
        // grouped values. This keeps jsonb_agg on the aggregate path instead
        // of reaching the scalar evaluator's "cannot be evaluated here"
        // fallback, without special-casing individual function names.
        Expression::FunctionCall {
            name,
            args,
            returning,
            null_handling,
            unique_keys,
            ..
        } if !crate::catalog_fn::is_session_function(name)
            && args.iter().any(expression_has_aggregate) =>
        {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                if expression_has_aggregate(arg) {
                    values.push(evaluate_grouped_expression(row, schema, arg, slots, accs)?);
                } else {
                    values.push(evaluate_expression(row, schema, arg)?);
                }
            }
            if crate::scalar::is_scalar_function(name) {
                return crate::scalar::scalar_function_value_extended(
                    name,
                    &values,
                    returning.clone(),
                    *null_handling,
                    *unique_keys,
                );
            }
            Err(SqlError::Storage(PlomidError::new(
                ErrorKind::Unsupported,
                format!("function \"{name}\" is not supported in grouped projection"),
            )))
        }
        Expression::Equal(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(values_equal(&left, &right)))
            }
        }
        Expression::NotEqual(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Bool(!values_equal(&left, &right)))
            }
        }
        Expression::Less(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(
                value_cmp(&left, &right) == Some(std::cmp::Ordering::Less),
            ))
        }
        Expression::LessOrEqual(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(
                value_cmp(&left, &right) != Some(std::cmp::Ordering::Greater),
            ))
        }
        Expression::Greater(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(
                value_cmp(&left, &right) == Some(std::cmp::Ordering::Greater),
            ))
        }
        Expression::GreaterOrEqual(l, r) => {
            let left = evaluate_grouped_expression(row, schema, l, slots, accs)?;
            let right = evaluate_grouped_expression(row, schema, r, slots, accs)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(
                value_cmp(&left, &right) != Some(std::cmp::Ordering::Less),
            ))
        }
        // Conditional over aggregates (`CASE WHEN COUNT(*) = 2 THEN ...`):
        // same searched/simple semantics as the row evaluator, with every
        // branch resolved through the grouped slots.
        Expression::Case {
            operand,
            whens,
            default,
        } => {
            for (cond, val) in whens {
                let test = if let Some(op) = operand {
                    let op_v = evaluate_grouped_expression(row, schema, op, slots, accs)?;
                    let cv = evaluate_grouped_expression(row, schema, cond, slots, accs)?;
                    if op_v.is_null() || cv.is_null() {
                        Value::Null
                    } else {
                        Value::Bool(values_equal(&op_v, &cv))
                    }
                } else {
                    evaluate_grouped_expression(row, schema, cond, slots, accs)?
                };
                if matches!(test, Value::Bool(true)) {
                    return evaluate_grouped_expression(row, schema, val, slots, accs);
                }
            }
            if let Some(def) = default {
                evaluate_grouped_expression(row, schema, def, slots, accs)
            } else {
                Ok(Value::Null)
            }
        }
        _ => evaluate_expression(row, schema, expr),
    }
}
