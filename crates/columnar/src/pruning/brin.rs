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
//! BRIN-style block-range summaries over segment row intervals.
//!
//! # Range granularity (explicit)
//!
//! A BRIN range is a **row interval** `[start_row, end_row)` of one immutable
//! columnar segment. Row intervals — not byte offsets and not chunk indexes —
//! are the physical scan unit the reader already understands
//! (`SegmentReader::read_rows(start, end, …)`), so a pruned range is skipped
//! by simply not requesting its rows. Ranges are contiguous, non-overlapping,
//! and tile `[0, row_count)` exactly; [`BrinIndex::validate`] enforces this,
//! because a gap in the covering would make the *uncovered* rows
//! unprunable-but-unscanned, which is a correctness hazard, not a tuning
//! detail.
//!
//! Each range carries one [`ZoneMap`](super::zonemap::ZoneMap) per column,
//! summarizing exactly the rows inside the interval. Zone maps inside a range
//! may cover strict sub-intervals (chunk boundaries do not have to align with
//! range boundaries); the pruning rule requires coverage before any proof is
//! accepted.

use super::predicate::PrunePredicate;
use super::verdict::PruneVerdict;
use super::zonemap::{NullState, ZoneMap, ZoneMapBuilder};
use crate::column::ColumnType;
use crate::layout::corruption;
use plomid_core::{ColumnId, Result};
use plomid_storage::Field;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Rows per BRIN range when the caller does not configure one.
///
/// 1024 rows keeps metadata overhead small (a range is roughly the size of a
/// chunk for typical `CHUNK_TARGET_SIZE`) while still letting selective
/// predicates skip most of a segment.
pub const DEFAULT_BRIN_ROWS_PER_RANGE: u64 = 1024;

/// One BRIN range: a physical row interval plus one zone map per column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrinRange {
    /// First physical row covered (inclusive).
    pub start_row: u64,
    /// First physical row not covered (exclusive); always `>= start_row`.
    pub end_row: u64,
    /// Per-column summaries for rows inside the interval, ordered by column
    /// identity. Every map's extent lies inside `[start_row, end_row)`.
    pub zone_maps: Vec<ZoneMap>,
}

impl BrinRange {
    /// Creates a range; zone maps are sorted by column identity.
    #[must_use]
    pub fn new(start_row: u64, end_row: u64, mut zone_maps: Vec<ZoneMap>) -> Self {
        zone_maps.sort_by_key(|zone| zone.column_id.get());
        Self {
            start_row,
            end_row,
            zone_maps,
        }
    }

    /// Returns the zone map for `column_id`, if this range summarizes it.
    #[must_use]
    pub fn zone_for(&self, column_id: ColumnId) -> Option<&ZoneMap> {
        self.zone_maps
            .iter()
            .find(|zone| zone.column_id == column_id)
    }

    /// Number of rows covered by this range.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.end_row.saturating_sub(self.start_row)
    }

    /// Returns true when this range overlaps `[start_row, end_row)`.
    #[must_use]
    pub fn overlaps(&self, start_row: u64, end_row: u64) -> bool {
        self.start_row < end_row && self.end_row > start_row
    }

    /// Evaluates one predicate against this range's own zone maps.
    ///
    /// A range is pruned only when some constrained column's zone maps cover
    /// the whole range *and* all of them prove impossibility. Anything less
    /// keeps the range.
    #[must_use]
    pub fn evaluate(&self, predicate: &PrunePredicate) -> PruneVerdict {
        super::predicate::prune_extent_with_zones(
            &self.zone_maps,
            predicate,
            self.start_row,
            self.end_row,
        )
    }
}

impl fmt::Display for BrinRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BrinRange {{ rows: [{}..{}), zones: {} }}",
            self.start_row,
            self.end_row,
            self.zone_maps.len()
        )
    }
}

/// Builder for a segment-wide BRIN index over fixed-size row intervals.
///
/// Rows are observed one by one (`observe`) and folded into the range their
/// ordinal falls in. A row that never receives a value for a declared column
/// is treated as NULL for that column: BRIN summarizes the *rows* of the
/// segment, and a row without a value cannot silently vanish from the
/// summary (that would desynchronize the NULL state from the data).
#[derive(Clone, Debug)]
pub struct BrinBuilder {
    rows_per_range: u64,
    columns: Vec<(ColumnId, ColumnType)>,
    observed: BTreeMap<u64, Vec<(ColumnId, Option<Field>)>>,
    row_count: u64,
}

impl BrinBuilder {
    /// Creates a builder covering `row_count` rows with default range sizes.
    #[must_use]
    pub fn new(row_count: u64) -> Self {
        Self {
            rows_per_range: DEFAULT_BRIN_ROWS_PER_RANGE,
            columns: Vec::new(),
            observed: BTreeMap::new(),
            row_count,
        }
    }

    /// Configures how many consecutive rows each BRIN range covers.
    ///
    /// Values below one degrade to one row per range rather than failing or
    /// emitting empty ranges.
    #[must_use]
    pub fn with_rows_per_range(mut self, rows_per_range: u64) -> Self {
        self.rows_per_range = rows_per_range.max(1);
        self
    }

    /// Declares a column summarized by every range. Duplicate declarations
    /// are ignored (first type wins).
    #[must_use]
    pub fn add_column(mut self, column_id: ColumnId, column_type: ColumnType) -> Self {
        if !self.columns.iter().any(|(id, _)| *id == column_id) {
            self.columns.push((column_id, column_type));
        }
        self
    }

    /// Observes one row's value for one column (`None` marks NULL).
    pub fn observe(&mut self, row: u64, column_id: ColumnId, value: Option<Field>) {
        self.observed
            .entry(row)
            .or_default()
            .push((column_id, value));
        self.row_count = self.row_count.max(row.saturating_add(1));
    }

    /// Finishes the ordered, non-overlapping covering of `[0, row_count)`.
    #[must_use]
    pub fn finish(self) -> BrinIndex {
        let mut types: BTreeMap<ColumnId, ColumnType> = BTreeMap::new();
        for (id, ty) in &self.columns {
            types.insert(*id, *ty);
        }
        let mut ranges = Vec::new();
        if self.row_count == 0 {
            return BrinIndex {
                row_count: 0,
                rows_per_range: self.rows_per_range,
                ranges,
            };
        }
        let mut start = 0_u64;
        while start < self.row_count {
            let end = start
                .saturating_add(self.rows_per_range)
                .min(self.row_count);
            let mut builders: BTreeMap<ColumnId, ZoneMapBuilder> = BTreeMap::new();
            for (id, ty) in &types {
                builders.insert(*id, ZoneMapBuilder::new(*id, *ty));
            }
            // Every row of the interval is observed exactly once per column:
            // recorded values contribute their field, unrecorded columns
            // observe NULL.
            for row in start..end {
                match self.observed.get(&row) {
                    Some(entries) => {
                        let mut seen = BTreeSet::new();
                        for (column_id, value) in entries {
                            if let Some(builder) = builders.get_mut(column_id) {
                                builder.observe(row, value.clone());
                                seen.insert(*column_id);
                            }
                        }
                        for (column_id, builder) in builders.iter_mut() {
                            if !seen.contains(column_id) {
                                builder.observe(row, None);
                            }
                        }
                    }
                    None => {
                        for builder in builders.values_mut() {
                            builder.observe(row, None);
                        }
                    }
                }
            }
            let zone_maps = builders.into_values().map(|b| b.finish()).collect();
            ranges.push(BrinRange::new(start, end, zone_maps));
            start = end;
        }
        BrinIndex {
            row_count: self.row_count,
            rows_per_range: self.rows_per_range,
            ranges,
        }
    }
}

/// A segment-wide BRIN index: an ordered, non-overlapping covering of
/// `[0, row_count)` by row ranges.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BrinIndex {
    /// Total rows covered; when `ranges` is non-empty they tile
    /// `[0, row_count)` exactly.
    pub row_count: u64,
    /// Rows per range used at build time (the last range may be short).
    pub rows_per_range: u64,
    /// Ranges in ascending row order.
    pub ranges: Vec<BrinRange>,
}

impl BrinIndex {
    /// Creates an index from pre-built ranges, sorted by start row.
    ///
    /// Call [`validate`](Self::validate) before trusting a decoded index:
    /// sorting repairs order, not content.
    #[must_use]
    pub fn from_ranges(row_count: u64, rows_per_range: u64, mut ranges: Vec<BrinRange>) -> Self {
        ranges.sort_by_key(|range| range.start_row);
        Self {
            row_count,
            rows_per_range: rows_per_range.max(1),
            ranges,
        }
    }

    /// Validates covering, ordering, and zone containment.
    ///
    /// A non-empty index must tile `[0, row_count)` exactly: contiguity from
    /// row 0 and total coverage. Ranges with gaps, with a start past the
    /// segment, or with zone maps escaping their range are corruption —
    /// never "approximately right" metadata.
    pub fn validate(&self) -> Result<()> {
        if self.ranges.is_empty() {
            if self.row_count != 0 {
                return Err(corruption("BRIN index claims rows but carries no ranges"));
            }
            return Ok(());
        }
        let mut expected = 0_u64;
        for range in &self.ranges {
            if range.start_row != expected {
                return Err(corruption(format!(
                    "BRIN ranges are not contiguous: expected start {expected} but found {}",
                    range.start_row
                )));
            }
            if range.end_row < range.start_row {
                return Err(corruption("BRIN range ends before it starts"));
            }
            if range.end_row > self.row_count {
                return Err(corruption("BRIN range extends past its segment"));
            }
            for zone in &range.zone_maps {
                if zone.row_count == 0 {
                    continue;
                }
                if zone.start_row < range.start_row || zone.end_row() > range.end_row {
                    return Err(corruption(format!(
                        "BRIN zone for column {} escapes its range",
                        zone.column_id.get()
                    )));
                }
            }
            expected = range.end_row;
        }
        if expected != self.row_count {
            return Err(corruption(format!(
                "BRIN ranges cover [0..{expected}) but the index claims {} rows; \
                 uncovered rows would be unreachable to both prune and scan",
                self.row_count
            )));
        }
        Ok(())
    }

    /// Evaluates one predicate over `[start_row, end_row)`.
    ///
    /// The extent prunes only when the ranges overlapping it cover it
    /// completely *and* every one of them proves impossibility.
    #[must_use]
    pub fn evaluate_extent(
        &self,
        predicate: &PrunePredicate,
        start_row: u64,
        end_row: u64,
    ) -> PruneVerdict {
        if start_row >= end_row {
            return PruneVerdict::Prune;
        }
        let overlapping: Vec<&BrinRange> = self
            .ranges
            .iter()
            .filter(|range| range.overlaps(start_row, end_row))
            .collect();
        if overlapping.is_empty() {
            return PruneVerdict::Unknown;
        }
        // Coverage of the extent by ranges (defensive even though `validate`
        // guarantees tiling of [0, row_count)).
        //
        // The verdict keeps `UNKNOWN` distinct from `KEEP`: when no range had
        // any evidence for the predicate's columns the answer stays `UNKNOWN`
        // rather than pretending the metadata said something. Both mean
        // "scan", so this is a reporting fidelity improvement only.
        let mut expected = start_row;
        let mut all_prune = true;
        let mut saw_evidence = false;
        for range in &overlapping {
            if range.start_row > expected {
                return PruneVerdict::Keep; // uncovered rows: never prune
            }
            match range.evaluate(predicate) {
                PruneVerdict::Prune => saw_evidence = true,
                PruneVerdict::Keep => {
                    all_prune = false;
                    saw_evidence = true;
                }
                // No proof for this range: the extent can never be pruned on
                // the strength of the other ranges alone.
                PruneVerdict::Unknown => all_prune = false,
            }
            expected = expected.max(range.end_row);
        }
        if expected < end_row {
            // Uncovered rows exist: no proof is possible for the extent.
            return PruneVerdict::Keep;
        }
        // `Prune` requires a proof from *every* range covering the extent:
        // `all_prune` alone is not enough, because a range with no evidence
        // leaves it true without proving anything.
        if all_prune && saw_evidence {
            PruneVerdict::Prune
        } else if saw_evidence {
            PruneVerdict::Keep
        } else {
            PruneVerdict::Unknown
        }
    }
}

/// Helper used by the trailer encoder: folds two sub-range NULL states.
#[must_use]
pub fn combine_null_states(left: NullState, right: NullState) -> NullState {
    left.combine(right)
}
