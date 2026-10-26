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
//! Zone maps: min/max/NULL summaries of immutable row ranges.
//!
//! A zone map summarizes one row range of one column with four facts:
//! row count, NULL state, and — when the column type has a storage-defined
//! ordering and the range holds at least one non-NULL value — the minimum
//! and maximum non-NULL values. These are the only facts min/max pruning is
//! allowed to reason from; everything else about the data is invisible to
//! the pruner and therefore always results in a scan.
//!
//! # NULL semantics
//!
//! NULL is never a minimum or maximum. [`NullState`] tracks NULL occupancy
//! separately (`NO_NULLS` / `HAS_NULLS` / `ALL_NULLS`), and range
//! comparisons never match NULL rows (SQL three-valued logic), so a NULL
//! row can never make a `PRUNE` wrong.
//!
//! # Supported types
//!
//! Ordering — and therefore min/max pruning — is supported exactly for the
//! columnar types whose ordering the storage layer already defines in
//! [`crate::statistics::compare_fields`]:
//!
//! * `Integer` — signed 64-bit numeric order;
//! * `Bytes` — unsigned lexicographic byte order;
//! * `String` — UTF-8 byte order (code-point preserving), cross-comparable
//!   with `Bytes` because both are byte sequences.
//!
//! Every other type (including the `Null` and reserved `Bool`/`I16`/
//! `I32`/`F32`/`F64` tags) reports no bounds: its zone maps still carry row
//! count and NULL state, but range predicates always evaluate to KEEP.

use crate::column::ColumnType;
use crate::layout::corruption;
use crate::statistics::compare_fields;
use plomid_core::{ColumnId, Result};
use plomid_storage::Field;
use std::cmp::Ordering;

use super::predicate::PrunePredicate;
use super::verdict::PruneVerdict;

/// Persisted tag: the summarized range holds no NULL rows.
pub const NULL_STATE_NO_NULLS: u8 = 0;
/// Persisted tag: the range holds at least one NULL and one non-NULL row.
pub const NULL_STATE_HAS_NULLS: u8 = 1;
/// Persisted tag: every row in the range is NULL.
pub const NULL_STATE_ALL_NULLS: u8 = 2;
/// NULL occupancy of one summarized row range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NullState {
    NoNulls,
    HasNulls,
    AllNulls,
}
impl NullState {
    #[must_use]
    pub fn observe(current: Option<Self>, is_null: bool) -> Self {
        match (current, is_null) {
            (None, true) => Self::AllNulls,
            (None, false) => Self::NoNulls,
            (Some(Self::AllNulls), true) => Self::AllNulls,
            (Some(Self::NoNulls), false) => Self::NoNulls,
            _ => Self::HasNulls,
        }
    }
    #[must_use]
    pub fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::NoNulls, Self::NoNulls) => Self::NoNulls,
            (Self::AllNulls, Self::AllNulls) => Self::AllNulls,
            _ => Self::HasNulls,
        }
    }
    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            NULL_STATE_NO_NULLS => Ok(Self::NoNulls),
            NULL_STATE_HAS_NULLS => Ok(Self::HasNulls),
            NULL_STATE_ALL_NULLS => Ok(Self::AllNulls),
            other => Err(corruption(format!(
                "pruning zone has invalid null state {other}"
            ))),
        }
    }
    #[must_use]
    pub fn tag(self) -> u8 {
        match self {
            Self::NoNulls => NULL_STATE_NO_NULLS,
            Self::HasNulls => NULL_STATE_HAS_NULLS,
            Self::AllNulls => NULL_STATE_ALL_NULLS,
        }
    }
}
impl std::fmt::Display for NullState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoNulls => write!(f, "NO_NULLS"),
            Self::HasNulls => write!(f, "HAS_NULLS"),
            Self::AllNulls => write!(f, "ALL_NULLS"),
        }
    }
}

/// Builds one [`ZoneMap`] incrementally from observed rows.
///
/// Each call to `observe` adds one row with its NULL flag and (optionally) its
/// value. The builder computes row_count, null state, min, and max in one pass.
/// Supported types (`Integer`, `Bytes`, `String`) produce bounds when at least
/// one non-NULL value exists and all non-NULL values are mutually comparable;
/// unsupported types and incomparable values leave bounds empty (the range
/// keeps on everything).
#[derive(Clone, Debug)]
pub struct ZoneMapBuilder {
    column_id: ColumnId,
    column_type: ColumnType,
    start_row: Option<u64>,
    end_row: u64,
    row_count: u64,
    null_state: Option<NullState>,
    min: Option<Field>,
    max: Option<Field>,
    /// Set when non-NULL values are mutually incomparable; bounds cleared.
    incomparable: bool,
}

impl ZoneMapBuilder {
    #[must_use]
    pub fn new(column_id: ColumnId, column_type: ColumnType) -> Self {
        Self {
            column_id,
            column_type,
            start_row: None,
            end_row: 0,
            row_count: 0,
            null_state: None,
            min: None,
            max: None,
            incomparable: false,
        }
    }

    /// Observes one row: `value` is the stored [`Field`] or `None` for NULL.
    pub fn observe(&mut self, row: u64, value: Option<Field>) {
        self.start_row = Some(self.start_row.map_or(row, |s| s.min(row)));
        self.end_row = self.end_row.max(row.saturating_add(1));
        self.row_count = self.row_count.saturating_add(1);

        match value {
            None => {
                self.null_state = Some(NullState::observe(self.null_state, true));
            }
            Some(Field::Null) => {
                self.null_state = Some(NullState::observe(self.null_state, true));
            }
            Some(field) => {
                self.null_state = Some(NullState::observe(self.null_state, false));

                // Only supported types carry bounds.
                if !supports_bounds(self.column_type) || self.incomparable {
                    return;
                }

                if self.min.is_none() && self.max.is_none() {
                    self.min = Some(field.clone());
                    self.max = Some(field);
                } else {
                    // Compare against current bounds; mutually incomparable
                    // values clear bounds and keep the range conservative.
                    match (
                        compare_fields(&field, self.min.as_ref().unwrap()),
                        compare_fields(&field, self.max.as_ref().unwrap()),
                    ) {
                        (Some(Ordering::Less), _) => self.min = Some(field.clone()),
                        (_, Some(Ordering::Greater)) => self.max = Some(field.clone()),
                        (Some(_), Some(_)) => {}
                        // Incomparable: clear bounds permanently.
                        _ => {
                            self.incomparable = true;
                            self.min = None;
                            self.max = None;
                        }
                    }
                }
            }
        }
    }

    /// Finishes this builder, returning the completed [`ZoneMap`].
    #[must_use]
    pub fn finish(self) -> ZoneMap {
        let (min, max) = if self.incomparable || !supports_bounds(self.column_type) {
            (None, None)
        } else if let (Some(min), Some(max)) = (self.min, self.max) {
            // Defensive: never emit min > max (builder invariant).
            match compare_fields(&min, &max) {
                Some(Ordering::Greater) => (None, None),
                _ => (Some(min), Some(max)),
            }
        } else {
            (None, None)
        };

        ZoneMap {
            column_id: self.column_id,
            column_type: self.column_type,
            start_row: self.start_row.unwrap_or(0),
            row_count: self.row_count,
            null_state: self.null_state.unwrap_or(NullState::AllNulls),
            min,
            max,
        }
    }
}

/// Returns true when a column type has a storage-defined ordering and can
/// produce min/max bounds for pruning.
#[must_use]
pub fn supports_bounds(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Integer | ColumnType::Bytes | ColumnType::String
    )
}

/// Min/max + NULL summary of one immutable row range of one column.
///
/// `column_id`/`column_type` name what is summarized; `[start_row, row_count`
/// rows) names the physical rows. `min`/`max` are non-NULL bounds in the
/// column's own ordering; they are always either both present or both
/// absent, and never `Field::Null`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZoneMap {
    pub column_id: ColumnId,
    pub column_type: ColumnType,
    pub start_row: u64,
    pub row_count: u64,
    pub null_state: NullState,
    pub min: Option<Field>,
    pub max: Option<Field>,
}
impl ZoneMap {
    #[must_use]
    pub fn new(
        column_id: ColumnId,
        column_type: ColumnType,
        start_row: u64,
        row_count: u64,
        null_state: NullState,
        min: Option<Field>,
        max: Option<Field>,
    ) -> Self {
        let min = match min {
            Some(Field::Null) => None,
            other => other,
        };
        let max = match max {
            Some(Field::Null) => None,
            other => other,
        };
        Self {
            column_id,
            column_type,
            start_row,
            row_count,
            null_state,
            min,
            max,
        }
    }
    #[must_use]
    pub fn end_row(&self) -> u64 {
        self.start_row.saturating_add(self.row_count)
    }
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.row_count
    }
    #[must_use]
    pub fn has_bounds(&self) -> bool {
        self.min.is_some() && self.max.is_some()
    }
    /// Returns true when this zone map's row range overlaps `[start_row, end_row)`.
    #[must_use]
    pub fn overlaps(&self, start_row: u64, end_row: u64) -> bool {
        // An empty zone map describes no rows and is irrelevant.
        if self.row_count == 0 {
            return false;
        }
        let self_end = self.end_row();
        self.start_row < end_row && self_end > start_row
    }
    /// Evaluates one predicate against this zone map.
    ///
    /// Returns `PruneVerdict::Prune` only when the metadata *proves* no row in this
    /// range can satisfy `predicate`; otherwise returns `Keep`/`Unknown` (both mean
    /// "scan this range"). This is the soundness core for leaf predicates.
    #[must_use]
    pub fn evaluate(&self, predicate: &PrunePredicate) -> PruneVerdict {
        super::predicate::evaluate_on_zone(self, predicate)
    }
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        super::trailer::encode_zone(self)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (zone, consumed) = super::trailer::decode_zone(bytes)?;
        if consumed != bytes.len() {
            return Err(corruption("pruning zone map has trailing bytes"));
        }
        Ok(zone)
    }
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.encode().len()
    }
}
impl std::fmt::Display for ZoneMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ZoneMap {{ column: {}, rows: [{}..{}), nulls: {}, min: {:?}, max: {:?} }}",
            self.column_id.get(),
            self.start_row,
            self.start_row.saturating_add(self.row_count),
            self.null_state,
            self.min,
            self.max
        )
    }
}
