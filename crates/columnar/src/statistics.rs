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
//! Column statistics for analytical pruning.
//!
//! A segment records, for each column, the number of rows, the number of NULL
//! rows, and the minimum and maximum non-NULL values. The purpose is pruning: a
//! scan that needs `price > 500` can discard a segment whose maximum price is
//! 100 without reading a single value.
//!
//! # Conservative by construction
//!
//! A statistic that is reported is a statistic that is held. [`ColumnStatistics`]
//! tracks bound availability explicitly, so pruning never discards a segment on
//! the strength of a bound the writer could not compute. Bounds are unavailable
//! when the column type is not ordered, when every row is NULL, and when the
//! values are not mutually comparable — a mixed-type payload makes the bounds
//! unavailable rather than reporting an unsound minimum.
//!
//! # Persisted form
//!
//! The payload is a two-field [`Row`] holding the minimum and maximum, reusing
//! the checksum-validated row codec instead of defining a second value format.
//! Availability lives in the metadata flag byte, so a payload whose bound slot is
//! NULL while its flag is set is rejected as corruption rather than read as
//! "no bound".
//!
//! # Memory-only estimators (not in the v1 payload)
//!
//! [`ColumnStatistics`] additionally carries a probabilistic distinct-value
//! sketch ([`HllEstimator`]), a materialized distinct estimate, and an optional
//! equi-depth [`Histogram`]. None of these are persisted by [`encode`][ColumnStatistics::encode]:
//! the v1 payload stays byte-identical so existing segments keep decoding.
//! They are rebuilt at flush/scan time and travel with the in-memory
//! statistics. A future trailer field may persist them; until then a decoded
//! copy simply reports `None` for the estimate and histogram while keeping
//! the persisted min/max contract intact.

use crate::column::ColumnType;
use crate::format;
use crate::pruning::verdict::PruneVerdict;
use crate::pruning::{PruneOperator, PrunePredicate};
use plomid_core::{ColumnId, ErrorKind, PlomidError, Result};
use plomid_storage::{Field, Row};
use std::cmp::Ordering;
use std::fmt;

/// Number of fields in a persisted statistics payload: min and max.
const STATISTICS_FIELD_COUNT: usize = 2;

/// Orders two non-NULL fields, returning `None` when they are not comparable.
///
/// `String` is ordered by its UTF-8 bytes, which preserves code-point order.
/// `String` and `Bytes` are ordered against each other because both are byte
/// sequences. Mixed variants involving `Integer` are not comparable: the format
/// does not define whether an integer sorts before arbitrary bytes.
#[must_use]
pub fn compare_fields(left: &Field, right: &Field) -> Option<Ordering> {
    match (left, right) {
        (Field::Integer(a), Field::Integer(b)) => Some(a.cmp(b)),
        (Field::Bytes(a), Field::Bytes(b)) => Some(a.cmp(b)),
        (Field::String(a), Field::String(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (Field::Bytes(a), Field::String(b)) => Some(a.as_slice().cmp(b.as_bytes())),
        (Field::String(a), Field::Bytes(b)) => Some(a.as_bytes().cmp(b.as_slice())),
        _ => None,
    }
}
/// A borrowed row value together with the logical type of its column.
#[derive(Clone, Copy, Debug)]
pub struct ValueRef<'a> {
    /// Logical type the bytes are interpreted through.
    pub column_type: ColumnType,
    /// Raw persisted bytes of the value.
    pub bytes: &'a [u8],
}

impl<'a> ValueRef<'a> {
    /// Creates a borrowed value of the given column type.
    #[must_use]
    pub fn new(column_type: ColumnType, bytes: &'a [u8]) -> Self {
        Self { column_type, bytes }
    }

    /// Returns true when the bytes are a valid encoding of `column_type`.
    #[must_use]
    pub fn is_well_typed(&self) -> bool {
        match self.column_type {
            ColumnType::Integer => self.bytes.len() == size_of::<i64>(),
            ColumnType::Bytes => true,
            ColumnType::String => std::str::from_utf8(self.bytes).is_ok(),
            ColumnType::Null
            | ColumnType::Bool
            | ColumnType::I16
            | ColumnType::I32
            | ColumnType::F32
            | ColumnType::F64 => false,
        }
    }

    /// Rebuilds the logical field this value denotes.
    #[must_use]
    pub fn materialize(&self) -> Field {
        match self.column_type {
            ColumnType::Integer => match <[u8; 8]>::try_from(self.bytes) {
                Ok(array) => Field::Integer(i64::from_le_bytes(array)),
                Err(_) => Field::Null,
            },
            ColumnType::String => match std::str::from_utf8(self.bytes) {
                Ok(text) => Field::String(text.to_owned()),
                Err(_) => Field::Null,
            },
            ColumnType::Bytes => Field::Bytes(self.bytes.to_vec()),
            ColumnType::Null
            | ColumnType::Bool
            | ColumnType::I16
            | ColumnType::I32
            | ColumnType::F32
            | ColumnType::F64 => Field::Null,
        }
    }
}

/// Orders a borrowed value against a stored bound.
///
/// Returns `None` when the two are not comparable, which the caller treats as
/// "this bound can no longer be proven" rather than as a silent ordering.
#[must_use]
pub fn compare_value_to_field(value: ValueRef<'_>, stored: &Field) -> Option<Ordering> {
    match (value.column_type, stored) {
        (ColumnType::Integer, Field::Integer(rhs)) => {
            let array: [u8; 8] = value.bytes.try_into().ok()?;
            Some(i64::from_le_bytes(array).cmp(rhs))
        }
        (ColumnType::Bytes, Field::Bytes(rhs)) => Some(value.bytes.cmp(rhs.as_slice())),
        (ColumnType::String, Field::String(rhs)) => Some(value.bytes.cmp(rhs.as_bytes())),
        (ColumnType::String, Field::Bytes(rhs)) => Some(value.bytes.cmp(rhs.as_slice())),
        (ColumnType::Bytes, Field::String(rhs)) => Some(value.bytes.cmp(rhs.as_bytes())),
        _ => None,
    }
}

/// Statistics for one column of a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnStatistics {
    /// Column these statistics describe.
    pub column_id: ColumnId,
    /// Number of logical rows in the column.
    pub row_count: u64,
    /// Number of rows whose null bit is set.
    pub null_count: u64,
    /// Minimum non-NULL value, when one is held.
    pub min: Option<Field>,
    /// Maximum non-NULL value, when one is held.
    pub max: Option<Field>,
    /// Whether a minimum is meaningful for this column.
    pub min_available: bool,
    /// Whether a maximum is meaningful for this column.
    pub max_available: bool,
    /// Live probabilistic distinct-value sketch, created on the first
    /// observed non-NULL value. Memory-only: never persisted in v1.
    pub distinct_estimator: Option<HllEstimator>,
    /// Materialized distinct estimate used when no live sketch exists
    /// (for example an exact count supplied by a builder, or a value kept
    /// across a merge that had no sketch). Memory-only: never persisted.
    pub distinct_estimate: Option<u64>,
    /// Optional equi-depth histogram over non-NULL values, used for
    /// selectivity estimation. Memory-only: never persisted in v1.
    pub histogram: Option<Histogram>,
}
impl ColumnStatistics {
    /// Creates statistics that track no bounds.
    #[must_use]
    pub fn new(column_id: ColumnId, row_count: u64, null_count: u64) -> Self {
        Self {
            column_id,
            row_count,
            null_count,
            min: None,
            max: None,
            min_available: false,
            max_available: false,
            distinct_estimator: None,
            distinct_estimate: None,
            histogram: None,
        }
    }

    /// Creates statistics that track bounds when the column type permits it.
    ///
    /// Bounds are tracked only for ordered types, and only when at least one row
    /// is non-NULL, because a column of nothing but NULLs has no minimum.
    #[must_use]
    pub fn with_type(
        column_id: ColumnId,
        row_count: u64,
        null_count: u64,
        column_type: ColumnType,
    ) -> Self {
        let ordered = matches!(
            column_type,
            ColumnType::Integer | ColumnType::Bytes | ColumnType::String
        );
        let track = ordered && null_count < row_count;
        Self {
            column_id,
            row_count,
            null_count,
            min: None,
            max: None,
            min_available: track,
            max_available: track,
            distinct_estimator: None,
            distinct_estimate: None,
            histogram: None,
        }
    }

    /// Folds one borrowed row value into the tracked bounds.
    ///
    /// This is the flush-path entry point: it compares the raw persisted bytes
    /// of a value against the tracked bounds, so a scan over millions of rows
    /// allocates only when a bound actually has to be replaced. `None` denotes
    /// a NULL row. A value whose encoding does not match its column's type
    /// makes both bounds permanently unavailable.
    pub fn observe_value(&mut self, value: Option<ValueRef<'_>>) {
        let Some(value) = value else {
            return;
        };
        if !value.is_well_typed() {
            if self.min_available && self.max_available {
                self.invalidate_bounds();
            }
            return;
        }
        self.observe_distinct(value_bytes_hash(value.column_type, value.bytes));
        if !self.min_available || !self.max_available {
            return;
        }
        if let Some(current) = &self.min {
            match compare_value_to_field(value, current) {
                Some(Ordering::Less) => self.min = Some(value.materialize()),
                Some(_) => {}
                None => return self.invalidate_bounds(),
            }
        } else {
            self.min = Some(value.materialize());
        }
        if let Some(current) = &self.max {
            match compare_value_to_field(value, current) {
                Some(Ordering::Greater) => self.max = Some(value.materialize()),
                Some(_) => {}
                None => self.invalidate_bounds(),
            }
        } else {
            self.max = Some(value.materialize());
        }
    }

    /// Folds one value into the tracked bounds.
    ///
    /// NULL values are ignored. A value that cannot be ordered against a bound
    /// makes both bounds permanently unavailable, so the segment stops claiming
    /// a minimum it can no longer prove.
    pub fn observe(&mut self, value: &Field) {
        if matches!(value, Field::Null) {
            return;
        }
        self.observe_distinct(field_hash(value));
        if !self.min_available || !self.max_available {
            return;
        }
        match &self.min {
            Some(current) => match compare_fields(value, current) {
                Some(Ordering::Less) => self.min = Some(value.clone()),
                Some(_) => {}
                None => return self.invalidate_bounds(),
            },
            None => self.min = Some(value.clone()),
        }
        match &self.max {
            Some(current) => match compare_fields(value, current) {
                Some(Ordering::Greater) => self.max = Some(value.clone()),
                Some(_) => {}
                None => self.invalidate_bounds(),
            },
            None => self.max = Some(value.clone()),
        }
    }

    /// Drops both bounds and marks them unavailable.
    fn invalidate_bounds(&mut self) {
        self.min = None;
        self.max = None;
        self.min_available = false;
        self.max_available = false;
    }

    /// Returns the statistics flag bitmask describing what is held.
    ///
    /// A bound is only advertised when the column both tracks it and actually
    /// holds a value for it, so the flag byte and the payload cannot disagree.
    #[must_use]
    pub fn flags(&self) -> u8 {
        let mut flags = format::STATS_ROW_COUNT_AVAILABLE | format::STATS_NULL_COUNT_AVAILABLE;
        if self.min_available && self.min.is_some() {
            flags |= format::STATS_MIN_AVAILABLE;
        }
        if self.max_available && self.max.is_some() {
            flags |= format::STATS_MAX_AVAILABLE;
        }
        flags
    }

    /// Returns true when a minimum is held.
    #[must_use]
    pub fn min_is_available(&self) -> bool {
        self.min_available && self.min.is_some()
    }

    /// Returns true when a maximum is held.
    #[must_use]
    pub fn max_is_available(&self) -> bool {
        self.max_available && self.max.is_some()
    }

    /// Returns the fraction of rows that are NULL.
    #[must_use]
    pub fn null_fraction(&self) -> f64 {
        if self.row_count == 0 {
            0.0
        } else {
            self.null_count as f64 / self.row_count as f64
        }
    }

    /// Returns the fraction of rows that are non-NULL.
    #[must_use]
    pub fn non_null_fraction(&self) -> f64 {
        1.0 - self.null_fraction()
    }

    /// Returns true when `value` could occur in this column.
    ///
    /// Returns `true` whenever a bound is not held or not comparable, because
    /// pruning may only discard a segment when the statistics prove it cannot
    /// match.
    #[must_use]
    pub fn may_contain(&self, value: &Field) -> bool {
        if let Some(min) = &self.min {
            if self.min_available {
                if let Some(Ordering::Less) = compare_fields(value, min) {
                    return false;
                }
            }
        }
        if let Some(max) = &self.max {
            if self.max_available {
                if let Some(Ordering::Greater) = compare_fields(value, max) {
                    return false;
                }
            }
        }
        true
    }

    /// Folds a pre-hashed non-NULL value into the distinct-value sketch,
    /// creating the sketch on first use.
    pub fn observe_distinct(&mut self, hash: u64) {
        self.distinct_estimator
            .get_or_insert_with(HllEstimator::new)
            .observe(hash);
    }

    /// Returns the distinct-value estimate: the live sketch estimate when a
    /// sketch exists, otherwise the stored materialized estimate.
    #[must_use]
    pub fn distinct_estimate_value(&self) -> Option<u64> {
        if let Some(estimator) = &self.distinct_estimator {
            Some(estimator.estimate())
        } else {
            self.distinct_estimate
        }
    }

    /// Records an exact (or externally computed) distinct count, used when
    /// no live sketch exists. A live sketch, when present, takes precedence
    /// in [`distinct_estimate_value`](Self::distinct_estimate_value).
    pub fn set_distinct_estimate(&mut self, distinct: u64) {
        self.distinct_estimate = Some(distinct);
    }

    /// Attaches an equi-depth histogram (see [`Histogram::build_equidepth`]).
    pub fn set_histogram(&mut self, histogram: Histogram) {
        self.histogram = Some(histogram);
    }

    /// Merges `other` into `self`.
    ///
    /// The caller must ensure both describe the same column. Row and NULL
    /// counts are summed (saturating); bounds are widened, and become
    /// unavailable when the two sides are not mutually comparable —
    /// mirroring [`observe`](Self::observe). Sketches are merged by register
    /// maxima; stored estimates keep their maximum as a fallback. Histograms
    /// built from disjoint samples cannot be combined without the raw data,
    /// so a histogram survives only when exactly one side holds one.
    pub fn merge_from(&mut self, other: &Self) {
        debug_assert_eq!(self.column_id, other.column_id);
        self.row_count = self.row_count.saturating_add(other.row_count);
        self.null_count = self.null_count.saturating_add(other.null_count);
        let (min, min_ok) = widen_min(self.min.take(), other.min.clone());
        let (max, max_ok) = widen_max(self.max.take(), other.max.clone());
        self.min = min;
        self.max = max;
        self.min_available = self.min_available && other.min_available && min_ok;
        self.max_available = self.max_available && other.max_available && max_ok;
        if !self.min_available {
            self.min = None;
        }
        if !self.max_available {
            self.max = None;
        }
        match (&mut self.distinct_estimator, &other.distinct_estimator) {
            (Some(mine), Some(theirs)) => mine.merge_from(theirs),
            (None, Some(theirs)) => self.distinct_estimator = Some(theirs.clone()),
            _ => {}
        }
        self.distinct_estimate = match (self.distinct_estimate, other.distinct_estimate) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        };
        match (&self.histogram, &other.histogram) {
            (None, Some(theirs)) => self.histogram = Some(theirs.clone()),
            (Some(_), Some(_)) => self.histogram = None,
            _ => {}
        }
    }

    /// Estimates the fraction of all rows satisfying `column = value`.
    ///
    /// NULL can never satisfy a comparison (SQL three-valued logic), so a
    /// NULL literal estimates `0.0`; a value the bounds rule out also
    /// estimates `0.0`. Otherwise the histogram is consulted when present,
    /// then a uniform-over-distinct estimate (`non_null / distinct`), then
    /// the non-NULL fraction when only bounds are held. With no usable
    /// evidence at all the conservative `1.0` is returned. This is a costing
    /// aid, never a pruning proof.
    #[must_use]
    pub fn selectivity_equals(&self, value: &Field) -> f64 {
        if self.row_count == 0 || matches!(value, Field::Null) {
            return 0.0;
        }
        if !self.may_contain(value) {
            return 0.0;
        }
        if let Some(histogram) = &self.histogram {
            if histogram.total_count() > 0 {
                let predicate = PrunePredicate::eq(self.column_id, value.clone());
                return clamp01(histogram.selectivity(&predicate) * self.non_null_fraction());
            }
        }
        if let Some(distinct) = self.distinct_estimate_value().filter(|d| *d > 0) {
            return clamp01(self.non_null_fraction() / distinct as f64);
        }
        if self.min_is_available() || self.max_is_available() {
            return self.non_null_fraction();
        }
        1.0
    }

    /// Estimates the fraction of all rows with `low <= column <= high`
    /// (inclusive ends; `None` leaves that side unbounded).
    ///
    /// A range the bounds prove empty estimates `0.0` — the same soundness
    /// argument as pruning. Otherwise the histogram is consulted when
    /// present, then a uniform-over-width estimate for integer bounds, then
    /// the non-NULL fraction when bounds overlap but nothing finer is
    /// known. With no usable evidence the conservative `1.0` is returned.
    #[must_use]
    pub fn selectivity_range(&self, low: Option<&Field>, high: Option<&Field>) -> f64 {
        if self.row_count == 0 {
            return 0.0;
        }
        if low.is_some_and(|v| matches!(v, Field::Null))
            || high.is_some_and(|v| matches!(v, Field::Null))
        {
            // `col >= NULL` (and friends) is never TRUE.
            return 0.0;
        }
        if let (Some(bound), Some(held)) = (low, self.max.as_ref()) {
            if self.max_available && compare_fields(bound, held) == Some(Ordering::Greater) {
                return 0.0;
            }
        }
        if let (Some(bound), Some(held)) = (high, self.min.as_ref()) {
            if self.min_available && compare_fields(bound, held) == Some(Ordering::Less) {
                return 0.0;
            }
        }
        if let Some(histogram) = &self.histogram {
            if histogram.total_count() > 0 {
                let mut predicate: Option<PrunePredicate> = None;
                if let Some(bound) = low {
                    let leaf = PrunePredicate::compare(
                        self.column_id,
                        PruneOperator::GreaterOrEqual,
                        bound.clone(),
                    );
                    predicate = Some(match predicate {
                        Some(current) => current.and(leaf),
                        None => leaf,
                    });
                }
                if let Some(bound) = high {
                    let leaf = PrunePredicate::compare(
                        self.column_id,
                        PruneOperator::LessOrEqual,
                        bound.clone(),
                    );
                    predicate = Some(match predicate {
                        Some(current) => current.and(leaf),
                        None => leaf,
                    });
                }
                if let Some(predicate) = predicate {
                    return clamp01(histogram.selectivity(&predicate) * self.non_null_fraction());
                }
            }
        }
        if let (Some(min), Some(max)) = (self.min.as_ref(), self.max.as_ref()) {
            if self.min_available && self.max_available {
                if let Some(fraction) = range_width_fraction(min, max, low, high) {
                    return clamp01(fraction * self.non_null_fraction());
                }
                return self.non_null_fraction();
            }
        }
        if self.min_is_available() || self.max_is_available() {
            return self.non_null_fraction();
        }
        1.0
    }
}
impl ColumnStatistics {
    /// Encodes the persisted statistics payload.
    ///
    /// An unheld bound is persisted as [`Field::Null`] and its flag bit is
    /// omitted, which is what makes the flag byte authoritative on read.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let min = self.min.clone().unwrap_or(Field::Null);
        let max = self.max.clone().unwrap_or(Field::Null);
        Row::new(vec![min, max]).encode()
    }

    /// Encodes the payload together with the flag byte that describes it.
    pub fn encode_with_flags(&self) -> Result<(Vec<u8>, u8)> {
        Ok((self.encode()?, self.flags()))
    }

    /// Decodes a persisted statistics payload into a copy of `self`.
    ///
    /// `self` supplies the row and null counts that the payload does not carry;
    /// `flags` states which bounds the segment claims to hold.
    pub fn decode_with(&self, bytes: &[u8], flags: u8) -> Result<Self> {
        let row = Row::decode(bytes)?;
        let fields = row.fields();
        if fields.len() != STATISTICS_FIELD_COUNT {
            return Err(PlomidError::new(
                ErrorKind::Corruption,
                "column statistics payload has an unexpected field count",
            ));
        }
        let mut stats = self.clone();
        stats.min_available = flags & format::STATS_MIN_AVAILABLE != 0;
        stats.max_available = flags & format::STATS_MAX_AVAILABLE != 0;
        stats.min = Self::decode_bound(&fields[0], stats.min_available, "minimum")?;
        stats.max = Self::decode_bound(&fields[1], stats.max_available, "maximum")?;
        Ok(stats)
    }

    /// Validates and decodes one persisted bound slot.
    ///
    /// A bound whose flag is set but whose payload is NULL is corruption: the
    /// segment promised a bound it did not write.
    fn decode_bound(field: &Field, available: bool, what: &str) -> Result<Option<Field>> {
        match (available, field) {
            (false, _) => Ok(None),
            (true, Field::Null) => Err(PlomidError::new(
                ErrorKind::Corruption,
                format!("column statistics advertise a {what} whose payload is NULL"),
            )),
            (true, value) => Ok(Some(value.clone())),
        }
    }
}

impl fmt::Display for ColumnStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ColumnStatistics {{ column: {}, rows: {}, nulls: {} ({:.2}%), min: {:?}, max: {:?} }}",
            self.column_id.get(),
            self.row_count,
            self.null_count,
            self.null_fraction() * 100.0,
            self.min,
            self.max
        )
    }
}

/// Precision bits of the distinct-value sketch: `2^6 = 64` registers.
const HLL_PRECISION: u32 = 6;
/// Number of HyperLogLog registers.
const HLL_REGISTERS: usize = 1 << HLL_PRECISION;
/// Bias-correction constant for a 64-register sketch.
const HLL_ALPHA_64: f64 = 0.673;
/// Upper end of the small-range regime (`2.5 * m`) where linear counting
/// replaces the raw harmonic-mean estimator.
const HLL_SMALL_RANGE: f64 = 2.5 * HLL_REGISTERS as f64;

/// A 64-register HyperLogLog distinct-value sketch.
///
/// Standard deviation is `1.04 / sqrt(64) ≈ 13%`, which is ample for
/// selectivity costing while costing 64 bytes per column. Register maxima
/// merge losslessly, so segment sketches combine into table sketches.
/// Implemented inline with no new dependencies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HllEstimator {
    /// Per-register maxima of observed ranks.
    registers: [u8; HLL_REGISTERS],
}

impl Default for HllEstimator {
    fn default() -> Self {
        Self {
            registers: [0; HLL_REGISTERS],
        }
    }
}

impl HllEstimator {
    /// Creates an empty sketch that has observed nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one 64-bit hash into the sketch.
    ///
    /// The low [`HLL_PRECISION`] bits select the register; the rank is one
    /// plus the number of leading zeros of the remaining bits.
    pub fn observe(&mut self, hash: u64) {
        let index = (hash & (HLL_REGISTERS as u64 - 1)) as usize;
        let rest = hash >> HLL_PRECISION;
        let rank = rest.leading_zeros() - (u64::BITS - (64 - HLL_PRECISION)) + 1;
        let rank = rank.min(u8::MAX as u32) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }

    /// Estimates the number of distinct hashed values.
    ///
    /// Uses linear counting below [`HLL_SMALL_RANGE`] (exact for tiny
    /// cardinalities) and the raw HyperLogLog estimator above it. No
    /// large-range correction is applied: 64-bit hashes never saturate at
    /// the cardinalities this sketch is used for.
    #[must_use]
    pub fn estimate(&self) -> u64 {
        let width = HLL_REGISTERS as f64;
        let mut harmonic = 0.0_f64;
        let mut zeros = 0_u32;
        for register in self.registers {
            harmonic += 2_f64.powi(-(i32::from(register)));
            zeros += u32::from(register == 0);
        }
        let raw = HLL_ALPHA_64 * width * width / harmonic;
        let estimate = if raw <= HLL_SMALL_RANGE && zeros > 0 {
            width * (width / f64::from(zeros)).ln()
        } else {
            raw
        };
        estimate.round().max(0.0) as u64
    }

    /// Merges `other` into `self` by taking per-register maxima.
    pub fn merge_from(&mut self, other: &Self) {
        for (mine, theirs) in self.registers.iter_mut().zip(other.registers.iter()) {
            *mine = (*mine).max(*theirs);
        }
    }
}

/// FNV-1a (64-bit) over one tag byte plus `bytes`, finished with a
/// SplitMix64 avalanche so sequential inputs (such as consecutive integer
/// keys, whose encodings differ only in low bytes) still spread uniformly
/// over the sketch registers.
fn hash_tagged_bytes(tag: u8, bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    hash ^= u64::from(tag);
    hash = hash.wrapping_mul(PRIME);
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    avalanche(hash)
}

/// Final avalanche (SplitMix64) that gives FNV output full-bit diffusion.
fn avalanche(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

/// Hashes a logical [`Field`] for distinct-value sketching.
fn field_hash(field: &Field) -> u64 {
    match field {
        Field::Null => avalanche(0x0711_0238_9e76_5c4d),
        Field::Integer(value) => hash_tagged_bytes(1, &value.to_le_bytes()),
        Field::Bytes(bytes) => {
            let len = (bytes.len() as u64).to_le_bytes();
            let mut hash = hash_tagged_bytes(2, &len);
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
            avalanche(hash)
        }
        Field::String(text) => hash_tagged_bytes(3, text.as_bytes()),
    }
}

/// Hashes raw persisted value bytes in the encoding of `column_type`.
fn value_bytes_hash(column_type: ColumnType, bytes: &[u8]) -> u64 {
    let tag = match column_type {
        ColumnType::Integer => 1,
        ColumnType::Bytes => 2,
        ColumnType::String => 3,
        _ => 0,
    };
    hash_tagged_bytes(tag, bytes)
}

/// Widens two optional minima into their union.
///
/// Returns the widened minimum and whether the two sides were mutually
/// comparable. An incomparable pair poisons the bound (`None, false`), which
/// the caller turns into permanent unavailability.
fn widen_min(first: Option<Field>, second: Option<Field>) -> (Option<Field>, bool) {
    widen_bound(first, second, Ordering::Less)
}

/// Widens two optional maxima into their union. See [`widen_min`].
fn widen_max(first: Option<Field>, second: Option<Field>) -> (Option<Field>, bool) {
    widen_bound(first, second, Ordering::Greater)
}

/// Shared union logic: keeps the extreme in the `keep` direction on
/// comparable inputs.
fn widen_bound(
    first: Option<Field>,
    second: Option<Field>,
    keep: Ordering,
) -> (Option<Field>, bool) {
    match (first, second) {
        (Some(a), Some(b)) => match compare_fields(&a, &b) {
            Some(ordering) if ordering == keep || ordering == Ordering::Equal => (Some(a), true),
            Some(_) => (Some(b), true),
            None => (None, false),
        },
        (Some(a), None) => (Some(a), true),
        (None, b) => (b, true),
    }
}

/// Clamps a selectivity estimate into the probability range.
fn clamp01(value: f64) -> f64 {
    if value.is_nan() {
        1.0
    } else {
        value.clamp(0.0, 1.0)
    }
}

/// Reads an integer bound as `i64`.
fn field_as_i64(field: &Field) -> Option<i64> {
    match field {
        Field::Integer(value) => Some(*value),
        _ => None,
    }
}

/// Uniform-over-width overlap of `[low, high]` (inclusive, unbounded sides
/// extend to the held bounds) with the held `[min, max]`.
///
/// Returns `None` unless every involved bound is an integer, so callers fall
/// back to a coarser estimate for byte-ordered types.
fn range_width_fraction(
    min: &Field,
    max: &Field,
    low: Option<&Field>,
    high: Option<&Field>,
) -> Option<f64> {
    let min = field_as_i64(min)? as i128;
    let max = field_as_i64(max)? as i128;
    if min > max {
        return Some(0.0);
    }
    let total = (max - min + 1).max(1) as f64;
    let mut lo = min;
    let mut hi = max;
    if let Some(bound) = low {
        lo = lo.max(field_as_i64(bound)? as i128);
    }
    if let Some(bound) = high {
        hi = hi.min(field_as_i64(bound)? as i128);
    }
    if lo > hi {
        return Some(0.0);
    }
    Some(((hi - lo + 1) as f64 / total).min(1.0))
}

/// One equi-depth histogram bucket: every non-NULL value in
/// `[low, high]` (inclusive), holding `count` rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistogramBucket {
    /// Smallest value in the bucket.
    pub low: Field,
    /// Largest value in the bucket.
    pub high: Field,
    /// Number of non-NULL rows in the bucket.
    pub count: u64,
}

/// An equi-depth histogram over non-NULL column values.
///
/// Each bucket holds (approximately) the same number of rows, so skewed data
/// still yields useful per-bucket resolution. Selectivity estimation assumes
/// uniformity *within* a bucket (width-proportional for integers, half-open
/// for byte-ordered types). Memory-only: never persisted in v1 (see the
/// module docs); rebuild from a sample at flush or planning time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Histogram {
    /// Buckets in ascending order.
    pub buckets: Vec<HistogramBucket>,
    /// Number of buckets (always equal to `buckets.len()`).
    pub bucket_count: usize,
}

impl Histogram {
    /// Builds an equi-depth histogram from an ascending `sorted` sample.
    ///
    /// NULLs are skipped (they are tracked by null counts, never by value
    /// histograms). Returns `None` when there is nothing to bucket or when
    /// `bucket_count` is zero. Fewer values than buckets yields one bucket
    /// per value. The caller must supply ascending input; unsorted input
    /// still terminates but its buckets misdescribe the data.
    #[must_use]
    pub fn build_equidepth(sorted: &[Field], bucket_count: usize) -> Option<Self> {
        let values: Vec<&Field> = sorted
            .iter()
            .filter(|value| !matches!(value, Field::Null))
            .collect();
        if values.is_empty() || bucket_count == 0 {
            return None;
        }
        let total = values.len();
        let buckets_to_fill = bucket_count.min(total);
        let base = total / buckets_to_fill;
        let remainder = total % buckets_to_fill;
        let mut buckets = Vec::with_capacity(buckets_to_fill);
        let mut start = 0;
        for index in 0..buckets_to_fill {
            let len = base + usize::from(index < remainder);
            let slice = &values[start..start + len];
            buckets.push(HistogramBucket {
                low: (*slice[0]).clone(),
                high: (*slice[len - 1]).clone(),
                count: len as u64,
            });
            start += len;
        }
        Some(Self {
            bucket_count: buckets.len(),
            buckets,
        })
    }

    /// Total non-NULL rows covered by the histogram.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.buckets.iter().map(|bucket| bucket.count).sum()
    }

    /// Estimates the fraction of covered (non-NULL) rows satisfying
    /// `predicate`.
    ///
    /// Comparison leaves are evaluated per bucket under a
    /// uniform-within-bucket assumption; `AND` multiplies (independence),
    /// `OR` adds (capped at one), `NOT` complements. Leaves the histogram
    /// cannot reason about — NULL checks, missing or NULL literals,
    /// incomparable literals — estimate `1.0` (conservative for costing).
    /// An empty histogram also estimates `1.0`: no evidence must never look
    /// selective.
    #[must_use]
    pub fn selectivity(&self, predicate: &PrunePredicate) -> f64 {
        let total = self.total_count();
        if total == 0 {
            return 1.0;
        }
        clamp01(self.fraction(predicate, total as f64))
    }

    /// Recursive selectivity fraction in `[0, 1]`.
    fn fraction(&self, predicate: &PrunePredicate, total: f64) -> f64 {
        match predicate {
            PrunePredicate::Compare {
                operator, literal, ..
            } => {
                let matched: f64 = self
                    .buckets
                    .iter()
                    .map(|bucket| {
                        bucket.count as f64 * bucket_fraction(bucket, *operator, literal.as_ref())
                    })
                    .sum();
                clamp01(matched / total)
            }
            PrunePredicate::And(left, right) => {
                self.fraction(left, total) * self.fraction(right, total)
            }
            PrunePredicate::Or(left, right) => {
                (self.fraction(left, total) + self.fraction(right, total)).min(1.0)
            }
            PrunePredicate::Not(inner) => 1.0 - self.fraction(inner, total),
        }
    }
}

/// Fraction of one bucket's rows matching a comparison leaf.
///
/// Integer buckets use width-proportional shares; byte-ordered buckets use a
/// half share for partial overlap and an all-distinct assumption for
/// equality. Incomparable literals yield `1.0`: the histogram says nothing,
/// so costing must not look selective.
fn bucket_fraction(
    bucket: &HistogramBucket,
    operator: PruneOperator,
    literal: Option<&Field>,
) -> f64 {
    if bucket.count == 0 {
        return 0.0;
    }
    match operator {
        PruneOperator::IsNull | PruneOperator::IsNotNull => 1.0,
        PruneOperator::NotEqual => {
            let equal = bucket_fraction(bucket, PruneOperator::Equal, literal);
            clamp01(1.0 - equal)
        }
        PruneOperator::Equal
        | PruneOperator::Less
        | PruneOperator::LessOrEqual
        | PruneOperator::Greater
        | PruneOperator::GreaterOrEqual => {
            let Some(value) = literal else {
                return 1.0;
            };
            if matches!(value, Field::Null) {
                // `col = NULL` (and friends) is never TRUE.
                return 0.0;
            }
            if !bounds_comparable(value, &bucket.low, &bucket.high) {
                return 1.0;
            }
            let cmp_low = compare_fields(value, &bucket.low);
            let cmp_high = compare_fields(value, &bucket.high);
            match operator {
                PruneOperator::Equal => equality_share(bucket, cmp_low, cmp_high),
                PruneOperator::Less => upper_share(bucket, value, cmp_low, cmp_high, false),
                PruneOperator::LessOrEqual => upper_share(bucket, value, cmp_low, cmp_high, true),
                PruneOperator::Greater => lower_share(bucket, value, cmp_low, cmp_high, false),
                PruneOperator::GreaterOrEqual => {
                    lower_share(bucket, value, cmp_low, cmp_high, true)
                }
                _ => 1.0,
            }
        }
    }
}

/// Returns false when the literal cannot be ordered against the bucket.
fn bounds_comparable(value: &Field, low: &Field, high: &Field) -> bool {
    compare_fields(value, low).is_some() && compare_fields(value, high).is_some()
}

/// Equality share: the fraction of a bucket equal to a contained literal.
fn equality_share(
    bucket: &HistogramBucket,
    cmp_low: Option<Ordering>,
    cmp_high: Option<Ordering>,
) -> f64 {
    match (cmp_low, cmp_high) {
        (Some(Ordering::Less), _) | (_, Some(Ordering::Greater)) => 0.0,
        _ => {
            if bucket.low == bucket.high {
                return 1.0;
            }
            match (field_as_i64(&bucket.low), field_as_i64(&bucket.high)) {
                (Some(lo), Some(hi)) => {
                    let width = (hi as i128 - lo as i128 + 1).max(1) as f64;
                    1.0 / width
                }
                // Without a measurable width, assume every row is distinct.
                _ => 1.0 / bucket.count.max(1) as f64,
            }
        }
    }
}

/// Share of a bucket strictly below (`inclusive == false`) or at-or-below
/// (`inclusive == true`) the literal.
fn upper_share(
    bucket: &HistogramBucket,
    value: &Field,
    cmp_low: Option<Ordering>,
    cmp_high: Option<Ordering>,
    inclusive: bool,
) -> f64 {
    // The literal is at or below the whole bucket: nothing is below it.
    if cmp_low == Some(Ordering::Less) {
        return 0.0;
    }
    if cmp_low == Some(Ordering::Equal) && !inclusive {
        return 0.0;
    }
    // The literal is above the whole bucket (or on its top edge, inclusive):
    // everything is below it.
    if cmp_high == Some(Ordering::Greater) {
        return 1.0;
    }
    if cmp_high == Some(Ordering::Equal) && inclusive {
        return 1.0;
    }
    partial_below(bucket, value, inclusive)
}

/// Share of a bucket strictly above (`inclusive == false`) or at-or-above
/// (`inclusive == true`) the literal.
fn lower_share(
    bucket: &HistogramBucket,
    value: &Field,
    cmp_low: Option<Ordering>,
    cmp_high: Option<Ordering>,
    inclusive: bool,
) -> f64 {
    // The literal is at or above the whole bucket: nothing is above it.
    if cmp_high == Some(Ordering::Greater) {
        return 0.0;
    }
    if cmp_high == Some(Ordering::Equal) && !inclusive {
        return 0.0;
    }
    // The literal is below the whole bucket (or on its bottom edge,
    // inclusive): everything is above it.
    if cmp_low == Some(Ordering::Less) {
        return 1.0;
    }
    if cmp_low == Some(Ordering::Equal) && inclusive {
        return 1.0;
    }
    partial_above(bucket, value, inclusive)
}

/// Share of a straddling bucket below the literal: width-proportional for
/// integers, one half for byte-ordered types.
fn partial_below(bucket: &HistogramBucket, value: &Field, inclusive: bool) -> f64 {
    match (
        field_as_i64(&bucket.low),
        field_as_i64(&bucket.high),
        field_as_i64(value),
    ) {
        (Some(lo), Some(hi), Some(v)) => {
            let width = (hi as i128 - lo as i128 + 1).max(1) as f64;
            let below = v as i128 - lo as i128 + i128::from(inclusive);
            clamp01(below as f64 / width)
        }
        _ => 0.5,
    }
}

/// Share of a straddling bucket above the literal: width-proportional for
/// integers, one half for byte-ordered types.
fn partial_above(bucket: &HistogramBucket, value: &Field, inclusive: bool) -> f64 {
    match (
        field_as_i64(&bucket.low),
        field_as_i64(&bucket.high),
        field_as_i64(value),
    ) {
        (Some(lo), Some(hi), Some(v)) => {
            let width = (hi as i128 - lo as i128 + 1).max(1) as f64;
            let above = hi as i128 - v as i128 + i128::from(inclusive);
            clamp01(above as f64 / width)
        }
        _ => 0.5,
    }
}
/// Statistics for one chunk (a sub-segment row range) of one column.
///
/// A finer pruning grain than [`SegmentStatistics`]: a scan with
/// `WHERE timestamp >= X` can skip the chunks whose maximum precedes `X`
/// without decoding them. Bounds follow the same conservative contract as
/// [`ColumnStatistics`] — `None` (or an unavailable flag) means "unknown",
/// never "empty" — and the same proof rules as the zone-map layer decide
/// pruning (see [`ChunkStatistics::evaluate`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkStatistics {
    /// Chunk these statistics describe (index into the chunk table).
    pub chunk_id: u32,
    /// Column these statistics describe.
    pub column_id: ColumnId,
    /// Number of logical rows in the chunk.
    pub row_count: u64,
    /// Number of rows whose null bit is set.
    pub null_count: u64,
    /// Minimum non-NULL value, when one is held.
    pub min: Option<Field>,
    /// Maximum non-NULL value, when one is held.
    pub max: Option<Field>,
    /// Live probabilistic distinct-value sketch. Memory-only.
    pub distinct_estimator: Option<HllEstimator>,
    /// Materialized distinct estimate fallback. Memory-only.
    pub distinct_estimate: Option<u64>,
    /// Whether the held bounds are meaningful. Once poisoned by an
    /// incomparable value the chunk stops claiming bounds.
    bounds_available: bool,
}

impl ChunkStatistics {
    /// Creates empty statistics for one chunk of one column.
    #[must_use]
    pub fn new(chunk_id: u32, column_id: ColumnId) -> Self {
        Self {
            chunk_id,
            column_id,
            row_count: 0,
            null_count: 0,
            min: None,
            max: None,
            distinct_estimator: None,
            distinct_estimate: None,
            bounds_available: true,
        }
    }

    /// Returns true when the held bounds may be used for pruning.
    #[must_use]
    pub fn bounds_available(&self) -> bool {
        self.bounds_available && (self.min.is_some() || self.max.is_some())
    }

    /// Folds one value into the chunk statistics.
    ///
    /// Counts the row (NULLs via `null_count`), sketches non-NULL values
    /// for distinct estimation, and widens the bounds. An incomparable
    /// value poisons the bounds permanently, exactly like
    /// [`ColumnStatistics::observe`].
    pub fn observe(&mut self, value: &Field) {
        self.row_count = self.row_count.saturating_add(1);
        if matches!(value, Field::Null) {
            self.null_count = self.null_count.saturating_add(1);
            return;
        }
        self.observe_distinct(field_hash(value));
        if !self.bounds_available {
            return;
        }
        match &self.min {
            Some(current) => match compare_fields(value, current) {
                Some(Ordering::Less) => self.min = Some(value.clone()),
                Some(_) => {}
                None => return self.invalidate_bounds(),
            },
            None => self.min = Some(value.clone()),
        }
        match &self.max {
            Some(current) => match compare_fields(value, current) {
                Some(Ordering::Greater) => self.max = Some(value.clone()),
                Some(_) => {}
                None => self.invalidate_bounds(),
            },
            None => self.max = Some(value.clone()),
        }
    }

    /// Folds a pre-hashed non-NULL value into the distinct-value sketch.
    pub fn observe_distinct(&mut self, hash: u64) {
        self.distinct_estimator
            .get_or_insert_with(HllEstimator::new)
            .observe(hash);
    }

    /// Returns the distinct-value estimate, preferring the live sketch.
    #[must_use]
    pub fn distinct_estimate_value(&self) -> Option<u64> {
        if let Some(estimator) = &self.distinct_estimator {
            Some(estimator.estimate())
        } else {
            self.distinct_estimate
        }
    }

    /// Drops both bounds and marks them unavailable.
    fn invalidate_bounds(&mut self) {
        self.min = None;
        self.max = None;
        self.bounds_available = false;
    }

    /// Returns true when the chunk holds no NULL rows.
    fn has_no_nulls(&self) -> bool {
        self.null_count == 0
    }

    /// Returns true when every row of a non-empty chunk is NULL.
    fn all_null(&self) -> bool {
        self.row_count > 0 && self.null_count >= self.row_count
    }

    /// Evaluates a predicate against this chunk's metadata.
    ///
    /// Reuses the [`PrunePredicate`]/[`PruneOperator`] model with the same
    /// proof table as the zone-map layer: a range comparison prunes only
    /// when the held `[min, max]` proves no non-NULL row can match;
    /// `!=` never prunes on bounds; missing bounds, foreign columns, and
    /// incomparable literals keep (or report unknown) so the caller scans.
    /// An empty chunk prunes: it holds no row that could match.
    #[must_use]
    pub fn evaluate(&self, predicate: &PrunePredicate) -> PruneVerdict {
        match predicate {
            PrunePredicate::Compare {
                column_id,
                operator,
                literal,
            } => self.evaluate_leaf(*column_id, *operator, literal.as_ref()),
            PrunePredicate::And(left, right) => match (self.evaluate(left), self.evaluate(right)) {
                (PruneVerdict::Prune, _) | (_, PruneVerdict::Prune) => PruneVerdict::Prune,
                (PruneVerdict::Keep, PruneVerdict::Keep) => PruneVerdict::Keep,
                _ => PruneVerdict::Unknown,
            },
            PrunePredicate::Or(left, right) => match (self.evaluate(left), self.evaluate(right)) {
                (PruneVerdict::Prune, PruneVerdict::Prune) => PruneVerdict::Prune,
                (PruneVerdict::Keep, _) | (_, PruneVerdict::Keep) => PruneVerdict::Keep,
                _ => PruneVerdict::Unknown,
            },
            PrunePredicate::Not(inner) => self.evaluate_not(inner),
        }
    }

    /// Evaluates `NOT inner` by pushing the negation through safe duals.
    fn evaluate_not(&self, inner: &PrunePredicate) -> PruneVerdict {
        match inner {
            PrunePredicate::Compare {
                column_id,
                operator,
                literal,
            } => self.evaluate_leaf(*column_id, operator.negate(), literal.as_ref()),
            PrunePredicate::And(left, right) => {
                match (self.evaluate_not(left), self.evaluate_not(right)) {
                    (PruneVerdict::Prune, PruneVerdict::Prune) => PruneVerdict::Prune,
                    (PruneVerdict::Keep, _) | (_, PruneVerdict::Keep) => PruneVerdict::Keep,
                    _ => PruneVerdict::Unknown,
                }
            }
            PrunePredicate::Or(left, right) => {
                match (self.evaluate_not(left), self.evaluate_not(right)) {
                    (PruneVerdict::Prune, _) | (_, PruneVerdict::Prune) => PruneVerdict::Prune,
                    (PruneVerdict::Keep, PruneVerdict::Keep) => PruneVerdict::Keep,
                    _ => PruneVerdict::Unknown,
                }
            }
            PrunePredicate::Not(inner) => self.evaluate(inner),
        }
    }

    /// Evaluates one predicate leaf against this chunk.
    fn evaluate_leaf(
        &self,
        column_id: ColumnId,
        operator: PruneOperator,
        literal: Option<&Field>,
    ) -> PruneVerdict {
        if self.column_id != column_id {
            return PruneVerdict::Unknown;
        }
        if self.row_count == 0 {
            return PruneVerdict::Prune;
        }
        match operator {
            PruneOperator::IsNull => {
                if self.has_no_nulls() {
                    PruneVerdict::Prune
                } else {
                    PruneVerdict::Keep
                }
            }
            PruneOperator::IsNotNull => {
                if self.all_null() {
                    PruneVerdict::Prune
                } else {
                    PruneVerdict::Keep
                }
            }
            PruneOperator::NotEqual => {
                if self.all_null() {
                    PruneVerdict::Prune
                } else {
                    PruneVerdict::Keep
                }
            }
            PruneOperator::Equal
            | PruneOperator::Less
            | PruneOperator::LessOrEqual
            | PruneOperator::Greater
            | PruneOperator::GreaterOrEqual => {
                let Some(value) = literal else {
                    return PruneVerdict::Unknown;
                };
                if matches!(value, Field::Null) {
                    return PruneVerdict::Prune;
                }
                let (Some(min), Some(max)) = (self.min.as_ref(), self.max.as_ref()) else {
                    if self.all_null() {
                        return PruneVerdict::Prune;
                    }
                    return PruneVerdict::Unknown;
                };
                if !self.bounds_available {
                    if self.all_null() {
                        return PruneVerdict::Prune;
                    }
                    return PruneVerdict::Unknown;
                }
                let (Some(cmp_min), Some(cmp_max)) =
                    (compare_fields(value, min), compare_fields(value, max))
                else {
                    return PruneVerdict::Unknown;
                };
                let proven_empty = match operator {
                    PruneOperator::Equal => {
                        cmp_min == Ordering::Less || cmp_max == Ordering::Greater
                    }
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

    /// Merges `other` (same chunk and column) into `self`.
    pub fn merge_from(&mut self, other: &Self) {
        debug_assert_eq!(self.chunk_id, other.chunk_id);
        debug_assert_eq!(self.column_id, other.column_id);
        self.row_count = self.row_count.saturating_add(other.row_count);
        self.null_count = self.null_count.saturating_add(other.null_count);
        let (min, min_ok) = widen_min(self.min.take(), other.min.clone());
        let (max, max_ok) = widen_max(self.max.take(), other.max.clone());
        self.min = min;
        self.max = max;
        self.bounds_available = self.bounds_available && other.bounds_available && min_ok && max_ok;
        if !self.bounds_available {
            self.min = None;
            self.max = None;
        }
        match (&mut self.distinct_estimator, &other.distinct_estimator) {
            (Some(mine), Some(theirs)) => mine.merge_from(theirs),
            (None, Some(theirs)) => self.distinct_estimator = Some(theirs.clone()),
            _ => {}
        }
        self.distinct_estimate = match (self.distinct_estimate, other.distinct_estimate) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (Some(a), None) => Some(a),
            (None, b) => b,
        };
    }
}

/// Per-chunk statistics of (typically) one column, driving chunk-granularity
/// pruning: whole chunks whose metadata proves them empty are skipped before
/// any value is decoded.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChunkSetStatistics {
    /// Per-chunk statistics in chunk order.
    pub chunks: Vec<ChunkStatistics>,
}

impl ChunkSetStatistics {
    /// Creates an empty chunk set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one chunk's statistics.
    pub fn add_chunk(&mut self, chunk: ChunkStatistics) {
        self.chunks.push(chunk);
    }

    /// Returns the chunk statistics in chunk order.
    #[must_use]
    pub fn chunks(&self) -> &[ChunkStatistics] {
        &self.chunks
    }

    /// Returns the number of chunks described.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Returns true when no chunk is described.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Prunes chunks with `predicate`, returning one keep flag per chunk in
    /// chunk order (`true` means "scan this chunk").
    ///
    /// A chunk is dropped only when its metadata proves no row in it can
    /// satisfy the predicate; every other chunk — including chunks with no
    /// usable metadata — is kept. With no chunks described the result is
    /// empty.
    #[must_use]
    pub fn prune(&self, predicate: &PrunePredicate) -> Vec<bool> {
        self.chunks
            .iter()
            .map(|chunk| chunk.evaluate(predicate) != PruneVerdict::Prune)
            .collect()
    }

    /// Returns the indexes of the chunks that survive pruning.
    #[must_use]
    pub fn kept_indexes(&self, predicate: &PrunePredicate) -> Vec<usize> {
        self.prune(predicate)
            .into_iter()
            .enumerate()
            .filter_map(|(index, keep)| keep.then_some(index))
            .collect()
    }
}
/// Statistics for every column of a segment.
///
/// Columns are indexed by identity so that a reader can look up the statistics
/// of a column without scanning the segment's column list.
#[derive(Clone, Debug, Default)]
pub struct SegmentStatistics {
    /// Per-column statistics indexed by column identity.
    columns: Vec<ColumnStatistics>,
}

impl SegmentStatistics {
    /// Creates an empty statistics set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts the statistics of one column.
    pub fn add_column(&mut self, stats: ColumnStatistics) {
        let idx = stats.column_id.get() as usize;
        if idx >= self.columns.len() {
            self.columns
                .resize(idx + 1, ColumnStatistics::new(ColumnId::new(0), 0, 0));
        }
        self.columns[idx] = stats;
    }

    /// Returns the statistics recorded for `column_id`.
    #[must_use]
    pub fn get(&self, column_id: ColumnId) -> Option<&ColumnStatistics> {
        self.columns
            .get(column_id.get() as usize)
            .filter(|stats| stats.column_id == column_id)
    }

    /// Returns every recorded column's statistics, ordered by identity.
    #[must_use]
    pub fn all(&self) -> &[ColumnStatistics] {
        &self.columns
    }

    /// Returns the number of recorded columns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns true when no statistics have been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}
