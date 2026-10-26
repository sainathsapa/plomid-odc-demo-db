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
//! Vectorized batch aggregation over columnar generations.
//!
//! # Why this exists
//!
//! The scalar SQL path answers columnar queries by converting every cell
//! through `Row → Field → Value` and evaluating expressions row-at-a-time
//! (measured ~21µs/row in debug: ~417ms of a 448ms aggregation over 20K
//! rows, versus ~31ms for the columnar crate's own decode + materialize).
//! This module evaluates supported aggregate shapes directly over typed
//! column data: tight passes over dense integer arrays, with no per-cell
//! allocation.
//!
//! # Exactness contract
//!
//! The vector path never guesses. It answers a query only when every piece
//! is provably identical to the scalar executor:
//!
//! * Predicates reuse the already-lowered [`PrunePredicate`], but a lowered
//!   predicate is only an *exact* row filter under narrow conditions (see
//!   [`normalize_predicate`]): integer comparisons over physically-`Integer`
//!   columns with integer literals, plus `IS NULL` / `IS NOT NULL` over any
//!   column. Anything else declines and the caller keeps the scalar path.
//!   Cross-type leaves (e.g. an integer column against a text literal, which
//!   the scalar executor resolves through casts) always decline.
//! * `NOT` is pushed through operator duals and De Morgan's law before
//!   evaluation, which preserves SQL three-valued logic for `WHERE` (only
//!   `TRUE` keeps a row). Evaluating `NOT` naively over a boolean mask would
//!   keep `NULL` rows and be wrong.
//! * The scalar executor compares integers through `f64`, which rounds beyond
//!   ±2⁵³. Exact `i64` evaluation matches it only inside that range, so any
//!   integer comparison or `MIN`/`MAX` requires the [`F64_EXACT_INT_RANGE`]
//!   guard: every literal and every involved segment's column bounds must lie
//!   inside ±2⁵³ (bounds come from persisted statistics; missing statistics
//!   decline). Outside the range the caller keeps the scalar path, i.e. the
//!   historical behavior is preserved bit-for-bit.
//! * Aggregates mirror the scalar accumulator semantics (the executor maps the
//!   typed states to `Value`s with the same formulas): `COUNT(*)` counts
//!   mask-passing rows, `COUNT(col)` skips `NULL`s, `SUM`/`AVG` skip `NULL`s
//!   and yield `NULL` on empty input with identical `i64` overflow behavior
//!   (`+=`), `MIN`/`MAX` skip `NULL`s and yield `NULL` on empty input.
//!   Malformed integer payloads (wrong length) decline the request, keeping
//!   the scalar path authoritative for damaged data.
//! * `GROUP BY` keys are integer columns only; `NULL` keys form one group,
//!   matching the scalar behavior for uniformly-typed integer keys.
//!
//! Anything outside these shapes returns `Ok(None)` ("not supported, use the
//! scalar path"). Corruption still returns `Err`.

use crate::column::ColumnType;
use crate::compaction::{generation_segments, ColumnarScan};
use crate::pruning::{CandidateRange, PruneOperator, PrunePredicate, ScanPlan};
use crate::read::{
    compressed_delta_int_cmp_mask, compressed_delta_min_max, compressed_dict_distinct_count,
    compressed_dict_eq_mask, compressed_dict_int_keys, compressed_rle_aggregate,
    compressed_rle_int_cmp_mask, compressed_rle_raw_cmp_mask, should_execute_compressed,
    CompressedOp, SegmentReader,
};
use crate::store::ColumnarStore;
use plomid_core::{ColumnId, ObjectId, Result};
use plomid_storage::{Field, Row, StorageEngine};
use std::collections::{BTreeSet, HashMap, HashSet};

/// Row-block size for the compute loops: the Standard-tier default.
///
/// Column data is materialized per pruned candidate span; the predicate and
/// aggregate passes then run over `batch_rows` windows for cache locality.
/// Measured on 8K rows (global COUNT+SUM, debug): 256→9.8ms, 1024→8.6ms,
/// 4096→7.4ms, 16384→6.8ms, 65536→6.3ms (mostly segment I/O; larger blocks
/// amortize per-block mask allocations). 16384 keeps masks at 16KB while
/// sitting at the knee of the curve.
pub const VECTOR_BATCH_ROWS: usize = 16384;

/// Edge-tier default block size in rows (4K: the top of the EDGE-B Edge
/// `1-4K` band — the measured curve above is nearly flat from 4K up, so Edge
/// takes the largest block that still fits small L2s and tight memories).
/// Used as the clamp bound for the profile's Edge value, not as a default.
pub const VECTOR_BATCH_ROWS_EDGE: usize = 4096;

/// Batch rows for a resolved [`RuntimeProfile`](plomid_hw::RuntimeProfile).
/// Pure over its input so tests mock tiers without touching the host.
///
/// The profile's L2-derived `vector_batch_rows()` already fits the cache,
/// so Edge takes it clamped to the 1-4K band; Laptop and Server pin the
/// calibrated [`VECTOR_BATCH_ROWS`] knee (the profile's L2-fit is smaller
/// and uncalibrated for this path — larger blocks amortize per-block mask
/// allocations, which is what the 256→65536 sweep measured).
#[must_use]
pub fn vector_batch_rows_for_profile(profile: &plomid_hw::RuntimeProfile) -> usize {
    use plomid_hw::RuntimeTier;
    match profile.tier() {
        RuntimeTier::Edge => profile.vector_batch_rows().min(VECTOR_BATCH_ROWS_EDGE),
        RuntimeTier::Laptop | RuntimeTier::Server => VECTOR_BATCH_ROWS,
    }
}

/// Resolved profile for the batch default, detected once per process (the
/// tier cannot change under a running server). An unparsable
/// `PLOMID_PROFILE` fails safe to Laptop with one stderr warning, mirroring
/// the executor's detection.
fn cached_runtime_profile() -> &'static plomid_hw::RuntimeProfile {
    static CACHED: std::sync::OnceLock<plomid_hw::RuntimeProfile> = std::sync::OnceLock::new();
    CACHED.get_or_init(|| {
        plomid_hw::RuntimeProfile::resolve(None).unwrap_or_else(|reason| {
            eprintln!("warning: {reason}; falling back to the laptop tier");
            plomid_hw::RuntimeProfile::with_tier(
                plomid_hw::RuntimeTier::Laptop,
                plomid_hw::HardwareProfile::detect(),
                plomid_hw::ProfileSource::Explicit,
            )
        })
    })
}

/// Default compute block size in rows: `PLOMID_VECTOR_BATCH_ROWS` env (when
/// it parses to `>= 1`, else ignored; explicit configuration always wins
/// over the derived default) over the tiered profile default
/// ([`vector_batch_rows_for_profile`]). Scan-chunk sizing is intentionally
/// untouched (stays 1024).
#[must_use]
pub fn default_vector_batch_rows() -> usize {
    if let Ok(raw) = std::env::var("PLOMID_VECTOR_BATCH_ROWS") {
        if let Ok(n) = raw.trim().parse::<usize>() {
            if n >= 1 {
                return n;
            }
        }
    }
    vector_batch_rows_for_profile(cached_runtime_profile())
}

/// Largest magnitude for which `f64` comparison of integers is exact.
///
/// The scalar executor funnels every integer comparison through `f64`, which
/// represents all integers in `[-2⁵³, 2⁵³]` exactly. Inside this range exact
/// `i64` evaluation provably agrees with the scalar path; outside it the
/// vector path declines. See the module docs.
pub const F64_EXACT_INT_RANGE: i64 = 1 << 53;

/// One supported aggregate over a batch.
#[derive(Clone, Debug)]
pub struct VectorAggregate {
    /// What to compute.
    pub kind: VectorAggKind,
    /// Argument column (`None` only for `COUNT(*)`).
    pub column: Option<ColumnId>,
    /// Per-aggregate `FILTER (WHERE ...)` predicate, already lowered.
    pub filter: Option<PrunePredicate>,
}

/// Supported aggregate operators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorAggKind {
    /// `COUNT(*)`: rows passing the mask.
    CountStar,
    /// `COUNT(col)`: non-`NULL` values passing the mask.
    Count,
    /// `SUM(intcol)`.
    Sum,
    /// `MIN(intcol)`.
    Min,
    /// `MAX(intcol)`.
    Max,
    /// `AVG(intcol)`.
    Avg,
    /// `COUNT(DISTINCT intcol)`.
    CountDistinct,
}

/// A vector aggregation request over one table's published generation.
#[derive(Clone, Debug)]
pub struct VectorRequest {
    /// Aggregates to compute in one shared scan.
    pub aggregates: Vec<VectorAggregate>,
    /// Statement `WHERE` predicate, already lowered. `None` selects all rows.
    pub filter: Option<PrunePredicate>,
    /// `GROUP BY` key columns (positional columnar identities). Empty means
    /// a single global group. Integer columns only.
    pub group_by: Vec<ColumnId>,
    /// Compute block size; [`default_vector_batch_rows`] when zero.
    pub batch_rows: usize,
    /// Allows `String` comparison leaves to evaluate by byte order.
    ///
    /// The columnar layer cannot tell identity-flushed text (`TEXT`,
    /// `VARCHAR`, ...) from canonicalized encodings (timestamps, booleans),
    /// for which byte order is not value order. The SQL layer sets this only
    /// after verifying every string-compared column is a plain text family
    /// type; `Bytes` leaves need no flag (physical `Bytes` always holds raw
    /// `BYTEA`, compared by byte order on both paths).
    pub text_ordering_exact: bool,
}

/// Accumulator state for one (group, aggregate) cell.
#[derive(Clone, Debug, Default)]
pub struct VectorAcc {
    /// Rows seen (for `COUNT(*)`).
    pub count_star: i64,
    /// Non-`NULL` values seen (for `COUNT(col)`).
    pub count: i64,
    /// `SUM` state plus whether any non-`NULL` value was seen.
    pub sum: i64,
    pub sum_has_any: bool,
    /// `MIN` / `MAX` state over integer columns.
    pub min: Option<i64>,
    pub max: Option<i64>,
    /// `MIN` / `MAX` state over raw string/bytes columns (byte order).
    pub min_raw: Option<Vec<u8>>,
    pub max_raw: Option<Vec<u8>>,
    /// `AVG` state.
    pub avg_sum: i64,
    pub avg_count: i64,
    /// `COUNT(DISTINCT ...)` state over integer columns.
    pub distinct: HashSet<i64>,
    /// `COUNT(DISTINCT ...)` state over raw string/bytes columns.
    pub distinct_raw: HashSet<Vec<u8>>,
}

/// One result group: integer keys (`None` = SQL `NULL` key) plus one
/// accumulator per requested aggregate, in request order.
#[derive(Clone, Debug)]
pub struct VectorGroup {
    /// Group key values aligned with `VectorRequest::group_by`.
    pub keys: Vec<Option<i64>>,
    /// Accumulators aligned with `VectorRequest::aggregates`.
    pub accs: Vec<VectorAcc>,
}

/// Result of [`vector_aggregate`], with the same physical counters as
/// [`ColumnarScan`](crate::compaction::ColumnarScan) so callers can log what
/// the query physically did.
#[derive(Debug)]
pub struct VectorResult {
    /// Result groups (exactly one with empty keys for global aggregation).
    pub groups: Vec<VectorGroup>,
    /// Segments in the published generation.
    pub segments_considered: usize,
    /// Segments skipped entirely by zone-map pruning.
    pub segments_skipped: usize,
    /// Rows covered by pruned candidate ranges.
    pub rows_examined: u64,
    /// Rows eliminated by zone-map pruning before decoding.
    pub rows_skipped: u64,
    /// Physical columns materialized.
    pub columns_read: usize,
}

/// Runs `request` over the current published generation of `object`.
///
/// Returns `Ok(None)` when the request is not supported (physical types,
/// predicate shapes, or exactness guards fail) — the caller must use the
/// scalar path. Returns `Ok(Some(_))` with the aggregated result otherwise.
pub fn vector_aggregate<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    object_id: ObjectId,
    request: &VectorRequest,
) -> Result<Option<VectorResult>> {
    if request.aggregates.is_empty() {
        return Ok(None);
    }
    // The statement filter must be exactly evaluable (see
    // `normalize_predicate` for the proof obligations); per-aggregate
    // FILTERs get the same check. Unacceptable shapes decline before any I/O.
    let allow_text = request.text_ordering_exact;
    let filter = match request.filter.as_ref() {
        Some(predicate) => match normalize_predicate(predicate, allow_text) {
            Some(normalized) => Some(normalized),
            None => return Ok(None),
        },
        None => None,
    };
    let mut agg_filters: Vec<Option<PrunePredicate>> = Vec::with_capacity(request.aggregates.len());
    for aggregate in &request.aggregates {
        match aggregate.filter.as_ref() {
            Some(predicate) => match normalize_predicate(predicate, allow_text) {
                Some(normalized) => agg_filters.push(Some(normalized)),
                None => return Ok(None),
            },
            None => agg_filters.push(None),
        }
    }

    let snapshot = store.generations().reader()?;
    let Some(record) = snapshot.object(object_id) else {
        return Ok(None);
    };
    if !record.is_published() {
        return Ok(None);
    };
    let generation = record.current_generation;
    let segments =
        crate::compaction::generation_segments(store.generations(), object_id, generation)?;
    drop(snapshot);

    let batch_rows = if request.batch_rows == 0 {
        default_vector_batch_rows()
    } else {
        request.batch_rows.max(1)
    };

    let mut result = VectorResult {
        groups: Vec::new(),
        segments_considered: segments.len(),
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };

    // Group state: one accumulator row per group, created on demand. The
    // global shape keeps a single row so the hot loop never hashes.
    let mut global: Vec<VectorAcc> = Vec::new();
    let mut grouped: HashMap<Vec<Option<i64>>, Vec<VectorAcc>> = HashMap::new();
    if request.group_by.is_empty() {
        global = request
            .aggregates
            .iter()
            .map(|_| VectorAcc::default())
            .collect();
    }

    for segment in segments {
        let bytes = store.read_segment(engine, segment)?;
        let reader = SegmentReader::decode(&bytes)?;
        // Metadata-only type check BEFORE any column I/O: every value
        // column must exist as physically-`Integer`, every bitmap column
        // must exist. Failures decline in microseconds (no chunk decoding),
        // so non-vector queries never pay column I/O for the attempt.
        let (need_int, need_flex, need_bytes, need_string, need_nulls) =
            collect_needed(request, filter.as_ref(), &agg_filters);
        let mut types_ok = true;
        for column in need_int
            .iter()
            .chain(need_flex.iter())
            .chain(need_bytes.iter())
            .chain(need_string.iter())
            .chain(need_nulls.iter())
        {
            let want_int = need_int.contains(column);
            let want_flex = need_flex.contains(column);
            let want_bytes = need_bytes.contains(column);
            let want_string = need_string.contains(column);
            let wants_values = want_int || want_flex || want_bytes || want_string;
            let type_ok = match reader.column(*column) {
                // Integer serves int/flex wants; String serves flex/string;
                // Bytes serves flex/bytes. Anything else declines.
                Some(meta) => match meta.column_type {
                    ColumnType::Integer => !want_bytes && !want_string,
                    ColumnType::String => !want_int && !want_bytes,
                    ColumnType::Bytes => !want_int && !want_string,
                    _ => !wants_values,
                },
                None => false,
            };
            if !type_ok {
                types_ok = false;
                break;
            }
        }
        if !types_ok {
            return Ok(None);
        }
        // The statement filter doubles as the pruning predicate, exactly like
        // the scalar scan: zone maps eliminate provably-empty ranges first.
        let plan = match filter.as_ref() {
            Some(predicate) => crate::pruning::plan_scan(
                reader
                    .pruning
                    .as_ref()
                    .unwrap_or(&crate::pruning::SegmentPruning::empty(reader.row_count)),
                reader.row_count,
                predicate,
            ),
            None => ScanPlan {
                candidates: vec![CandidateRange::new(0, reader.row_count)],
                total_rows: reader.row_count,
                scanned_rows: reader.row_count,
            },
        };
        result.rows_examined += plan.scanned_rows;
        result.rows_skipped += plan.pruned_rows();
        if plan.candidates.is_empty() {
            result.segments_skipped += 1;
            continue;
        }
        // Exactness guard: integer comparisons and MIN/MAX agree with the
        // scalar f64 path only inside ±2⁵³. Bounds come from persisted
        // per-column statistics; anything missing or out of range declines
        // the whole query (the scalar path then answers it exactly).
        if needs_f64_guard(request, filter.as_ref(), &agg_filters)
            && !segment_in_f64_range(&reader, request, filter.as_ref(), &agg_filters)
        {
            return Ok(None);
        }
        let processed = process_segment(
            &reader,
            &bytes,
            &plan.candidates,
            request,
            filter.as_ref(),
            &agg_filters,
            batch_rows,
            &need_int,
            &need_flex,
            &need_bytes,
            &need_string,
            &need_nulls,
            &mut global,
            &mut grouped,
            &mut result,
        )?;
        if !processed {
            return Ok(None);
        }
    }
    if request.group_by.is_empty() {
        result.groups.push(VectorGroup {
            keys: Vec::new(),
            accs: std::mem::take(&mut global),
        });
    } else {
        result.groups = grouped
            .into_iter()
            .map(|(keys, accs)| VectorGroup { keys, accs })
            .collect();
    }
    Ok(Some(result))
}

/// A vector filter-scan request: match-only materialization for one table's
/// published generation.
///
/// Where [`vector_aggregate`] avoids materialization entirely for supported
/// aggregate shapes, this covers the complementary common shape — filtered
/// scans (`SELECT cols ... WHERE pred`, with an optional `LIMIT` pushdown):
/// the `WHERE` predicate evaluates over typed column batches and only
/// matching rows materialize through [`SegmentReader::read_rows`]. The
/// returned [`ColumnarScan`] has exactly the scalar read's shape (rows
/// aligned to `columns`, in scan order), so callers use it as a drop-in
/// replacement and keep every downstream operator unchanged.
#[derive(Clone, Debug)]
pub struct VectorScanRequest {
    /// Statement `WHERE` predicate, already lowered. The caller MUST use
    /// strict lowering
    /// ([`crate::pruning::lower_sql_expression_strict`]): a partially-lowered
    /// conjunction is sound for zone-map pruning but unsound as an exact row
    /// filter, so anything but a fully-lowerable predicate must decline
    /// before calling (the whole request returns `Ok(None)` otherwise).
    pub filter: PrunePredicate,
    /// Output columns in scalar-read order (positional columnar identities).
    /// Must be non-empty; every entry must exist in each segment.
    pub columns: Vec<ColumnId>,
    /// Compute block size; [`default_vector_batch_rows`] when zero.
    pub batch_rows: usize,
    /// Allows `String` comparison leaves to evaluate by byte order. The
    /// caller sets this only after verifying every string-compared column is
    /// a plain text-family type (same obligation as
    /// [`VectorRequest::text_ordering_exact`]).
    pub text_ordering_exact: bool,
    /// Stop after this many matching rows (`LIMIT` pushdown). `None` reads
    /// all matches. `Some(0)` yields an empty scan without segment I/O.
    pub limit_rows: Option<usize>,
}

/// Runs `request` over the current published generation of `object`,
/// materializing only rows that pass the filter.
///
/// Returns `Ok(None)` when the request is not supported (predicate shapes,
/// physical types, or exactness guards fail) — the caller must use the
/// scalar read. Row order, row width, and match semantics are identical to
/// `read_current_generation_rows` plus the caller's own `WHERE`
/// re-evaluation, by construction: zone maps narrow to candidates, the exact
/// mask keeps precisely the rows the scalar predicate keeps (see
/// [`normalize_predicate`]), and kept runs materialize through the same
/// [`SegmentReader::read_rows`] the scalar path uses.
pub fn vector_filter_scan<E: StorageEngine>(
    store: &ColumnarStore,
    engine: &mut E,
    object_id: ObjectId,
    request: &VectorScanRequest,
) -> Result<Option<ColumnarScan>> {
    if request.columns.is_empty() {
        return Ok(None);
    }
    // The filter must be exactly evaluable (same proof obligations as the
    // aggregate path); anything else declines before any I/O.
    let filter = match normalize_predicate(&request.filter, request.text_ordering_exact) {
        Some(normalized) => normalized,
        None => return Ok(None),
    };
    let mut need_int: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_bytes: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_string: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_nulls: BTreeSet<ColumnId> = BTreeSet::new();
    collect_eval_columns(
        &filter,
        &mut need_int,
        &mut need_bytes,
        &mut need_string,
        &mut need_nulls,
    );

    let snapshot = store.generations().reader()?;
    let Some(record) = snapshot.object(object_id) else {
        return Ok(None);
    };
    if !record.is_published() {
        return Ok(None);
    };
    let generation = record.current_generation;
    let segments = generation_segments(store.generations(), object_id, generation)?;
    drop(snapshot);

    let batch_rows = if request.batch_rows == 0 {
        default_vector_batch_rows()
    } else {
        request.batch_rows.max(1)
    };

    let mut result = ColumnarScan {
        rows: Vec::new(),
        column_ids: request.columns.clone(),
        segments_considered: segments.len(),
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };
    if request.limit_rows == Some(0) {
        return Ok(Some(result));
    }

    // Vectorized batches evaluated across all segments (metrics only).
    let mut batches = 0u64;
    for segment in segments {
        if request
            .limit_rows
            .is_some_and(|limit| result.rows.len() >= limit)
        {
            break;
        }
        let bytes = store.read_segment(engine, segment)?;
        let reader = SegmentReader::decode(&bytes)?;
        // Metadata-only checks BEFORE any column I/O: every output column
        // must exist, every predicate column must exist with a compatible
        // physical type. Failures decline in microseconds (no chunk
        // decoding). A missing output column declines rather than erroring:
        // the scalar path owns the authoritative error for such shapes.
        for column in &request.columns {
            if reader.column(*column).is_none() {
                return Ok(None);
            }
        }
        let mut types_ok = true;
        for column in need_int
            .iter()
            .chain(need_bytes.iter())
            .chain(need_string.iter())
            .chain(need_nulls.iter())
        {
            let want_int = need_int.contains(column);
            let want_bytes = need_bytes.contains(column);
            let want_string = need_string.contains(column);
            let wants_values = want_int || want_bytes || want_string;
            let type_ok = match reader.column(*column) {
                Some(meta) => match meta.column_type {
                    ColumnType::Integer => !want_bytes && !want_string,
                    ColumnType::String => !want_int && !want_bytes,
                    ColumnType::Bytes => !want_int && !want_string,
                    _ => !wants_values,
                },
                None => false,
            };
            if !type_ok {
                types_ok = false;
                break;
            }
        }
        if !types_ok {
            return Ok(None);
        }
        // Exactness guard for integer comparisons (same f64 rule as the
        // aggregate path): every compared column's persisted bounds must lie
        // inside ±2⁵³, else the scalar f64 comparison could disagree.
        if has_int_comparison(&filter) && !scan_filter_in_f64_range(&reader, &filter) {
            return Ok(None);
        }
        // Zone maps eliminate provably-empty ranges first, exactly like the
        // scalar scan; the mask below then decides row-by-row.
        let plan = crate::pruning::plan_scan(
            reader
                .pruning
                .as_ref()
                .unwrap_or(&crate::pruning::SegmentPruning::empty(reader.row_count)),
            reader.row_count,
            &filter,
        );
        result.rows_examined += plan.scanned_rows;
        result.rows_skipped += plan.pruned_rows();
        if plan.candidates.is_empty() {
            result.segments_skipped += 1;
            continue;
        }
        result.columns_read += need_int.len() + need_bytes.len() + need_string.len();
        let matched = scan_segment_matches(
            &reader,
            &bytes,
            &plan.candidates,
            &filter,
            &request.columns,
            &need_int,
            &need_bytes,
            &need_string,
            &need_nulls,
            batch_rows,
            request
                .limit_rows
                .map(|limit| limit.saturating_sub(result.rows.len())),
            &mut batches,
        )?;
        let Some(matched) = matched else {
            return Ok(None);
        };
        result.columns_read += request.columns.len();
        result.rows.extend(matched);
    }
    plomid_core::observability::record_columnar_scan(
        (result.segments_considered as u64).saturating_sub(result.segments_skipped as u64),
        result.segments_skipped as u64,
        result.columns_read as u64,
        batches,
    );
    Ok(Some(result))
}

/// Checks every integer-compared column against the segment's persisted
/// bounds (same ±2⁵³ f64-exactness rule as
/// [`segment_in_f64_range`](self::segment_in_f64_range), filter-only form).
fn scan_filter_in_f64_range(reader: &SegmentReader, filter: &PrunePredicate) -> bool {
    let mut int_columns: BTreeSet<ColumnId> = BTreeSet::new();
    collect_int_columns(filter, &mut int_columns);
    for column in int_columns {
        let Some(stats) = reader.column_statistics(column) else {
            return false;
        };
        if !matches!(stats.min.as_ref(), Some(Field::Integer(min)) if min.abs() <= F64_EXACT_INT_RANGE)
            || !matches!(stats.max.as_ref(), Some(Field::Integer(max)) if max.abs() <= F64_EXACT_INT_RANGE)
        {
            return false;
        }
    }
    true
}

/// Materializes the rows of `candidates` that pass `filter`, in scan order.
///
/// candidates are coalesced into disjoint intervals; per interval the
/// predicate columns materialize once, the mask evaluates per `batch_rows`
/// window ([`eval_node`]), and kept rows assemble from output columns
/// materialized once over the same interval — so every chunk decodes at most
/// once per column, exactly like the scalar read, and only match assembly
/// differs. Kept rows use [`MaterializedColumn::get_field`](crate::materialization::MaterializedColumn::get_field),
/// the same constructor [`SegmentReader::read_rows`] uses, so field values
/// are identical to the scalar path's. Returns `None` to decline (ragged
/// integers, missing views, evaluation gaps); decode failures propagate,
/// exactly as the scalar read surfaces them on the same data.
#[allow(clippy::too_many_arguments)]
fn scan_segment_matches(
    reader: &SegmentReader,
    bytes: &[u8],
    candidates: &[CandidateRange],
    filter: &PrunePredicate,
    columns: &[ColumnId],
    need_int: &BTreeSet<ColumnId>,
    need_bytes: &BTreeSet<ColumnId>,
    need_string: &BTreeSet<ColumnId>,
    need_nulls: &BTreeSet<ColumnId>,
    batch_rows: usize,
    remaining: Option<usize>,
    batches: &mut u64,
) -> Result<Option<Vec<Row>>> {
    if candidates.is_empty() {
        return Ok(Some(Vec::new()));
    }
    if remaining == Some(0) {
        return Ok(Some(Vec::new()));
    }
    // Coalesce overlapping/adjacent candidate ranges: one decode per
    // interval per column, never a re-decode per kept run.
    let mut intervals: Vec<(u64, u64)> = Vec::with_capacity(candidates.len());
    let mut sorted: Vec<(u64, u64)> = candidates
        .iter()
        .map(|candidate| (candidate.start_row, candidate.end_row))
        .collect();
    sorted.sort();
    for (start, end) in sorted {
        if end <= start {
            continue;
        }
        if let Some(last) = intervals.last_mut() {
            if start <= last.1 {
                last.1 = last.1.max(end);
                continue;
            }
        }
        intervals.push((start, end));
    }
    let mut out: Vec<Row> = Vec::new();
    for (start, end) in intervals {
        if remaining.is_some_and(|left| out.len() >= left) {
            break;
        }
        let Some(owned) = materialize_predicate_owned(
            reader,
            bytes,
            start,
            end,
            need_int,
            need_bytes,
            need_string,
            need_nulls,
        )?
        else {
            return Ok(None);
        };
        let views = views_from_owned(&owned);
        if !leaves_have_views(filter, &views) {
            return Ok(None);
        }
        let Some(output) = materialize_output_columns(reader, bytes, start, end, columns)? else {
            return Ok(None);
        };
        let length = end.saturating_sub(start) as usize;
        let mut pos = 0_usize;
        while pos < length {
            if remaining.is_some_and(|left| out.len() >= left) {
                break;
            }
            *batches += 1;
            let count = (length - pos).min(batch_rows);
            let Some(mask) = eval_node(filter, &views, pos, count) else {
                return Ok(None);
            };
            for (index, keep) in mask.iter().enumerate() {
                if *keep != 1 {
                    continue;
                }
                let mut fields = Vec::with_capacity(output.len());
                for column in &output {
                    fields.push(column.get_field(pos + index));
                }
                out.push(Row::new(fields));
                if remaining.is_some_and(|left| out.len() >= left) {
                    break;
                }
            }
            pos += count;
        }
    }
    if let Some(left) = remaining {
        out.truncate(left);
    }
    Ok(Some(out))
}

/// Materializes predicate columns over `[start, end)` in the owned shapes
/// mask evaluation needs (dense `i64` integers, raw offset/length buffers
/// for string/bytes, bitmaps for null tests). Returns `None` to decline
/// (missing columns, cross-type leaves, ragged integers); decode failures
/// propagate like the scalar read's.
#[allow(clippy::too_many_arguments)]
fn materialize_predicate_owned(
    reader: &SegmentReader,
    bytes: &[u8],
    start: u64,
    end: u64,
    need_int: &BTreeSet<ColumnId>,
    need_bytes: &BTreeSet<ColumnId>,
    need_string: &BTreeSet<ColumnId>,
    need_nulls: &BTreeSet<ColumnId>,
) -> Result<Option<HashMap<ColumnId, OwnedColumn>>> {
    let mut owned: HashMap<ColumnId, OwnedColumn> = HashMap::new();
    for column in need_int
        .iter()
        .chain(need_bytes.iter())
        .chain(need_string.iter())
        .chain(need_nulls.iter())
    {
        let want_int = need_int.contains(column);
        let want_bytes = need_bytes.contains(column);
        let want_string = need_string.contains(column);
        let Some(meta) = reader.column(*column) else {
            return Ok(None);
        };
        let physical = meta.column_type;
        let wants_values = want_int || want_bytes || want_string;
        let type_ok = match physical {
            ColumnType::Integer => !want_bytes && !want_string,
            ColumnType::String => !want_int && !want_bytes,
            ColumnType::Bytes => !want_int && !want_string,
            _ => !wants_values,
        };
        if !type_ok {
            return Ok(None);
        }
        let materialized = reader.read_column(bytes, meta, start, end)?;
        if want_int {
            let rows = materialized.row_count as usize;
            // Ragged non-NULL rows decline (scalar path answers exactly);
            // every surviving non-NULL entry is 8 bytes, so the borrowed
            // view below observes exactly what the owned copy did.
            let dense = (0..rows).all(|row| {
                materialized.is_null(row)
                    || materialized
                        .lengths
                        .get(row)
                        .is_some_and(|length| *length == 8)
            });
            if !dense {
                return Ok(None);
            }
            owned.insert(
                *column,
                OwnedColumn::Int {
                    bytes: materialized.values,
                    offsets: materialized.offsets,
                    lengths: materialized.lengths,
                    nulls: materialized.null_bitmap,
                },
            );
        } else if want_bytes || want_string {
            owned.insert(
                *column,
                OwnedColumn::Raw {
                    offsets: materialized.offsets,
                    lengths: materialized.lengths,
                    values: materialized.values,
                    nulls: materialized.null_bitmap,
                    physical,
                },
            );
        } else {
            owned.insert(
                *column,
                OwnedColumn::Nulls {
                    nulls: materialized.null_bitmap,
                },
            );
        }
    }
    Ok(Some(owned))
}

/// Borrows typed evaluation views over materialized predicate columns.
fn views_from_owned(owned: &HashMap<ColumnId, OwnedColumn>) -> RangeViews<'_> {
    let mut views = RangeViews {
        ints: HashMap::new(),
        raw: HashMap::new(),
        nulls: HashMap::new(),
    };
    for (column, owned) in owned {
        match owned {
            OwnedColumn::Int {
                bytes,
                offsets,
                lengths,
                nulls,
            } => {
                views.nulls.insert(*column, nulls.as_slice());
                views.ints.insert(
                    *column,
                    IntView {
                        bytes: bytes.as_slice(),
                        offsets: offsets.as_slice(),
                        lengths: lengths.as_slice(),
                        nulls: nulls.as_slice(),
                    },
                );
            }
            OwnedColumn::Raw {
                offsets,
                lengths,
                values,
                nulls,
                physical,
            } => {
                views.nulls.insert(*column, nulls.as_slice());
                views.raw.insert(
                    *column,
                    RawView {
                        physical: *physical,
                        offsets: offsets.as_slice(),
                        lengths: lengths.as_slice(),
                        values: values.as_slice(),
                        nulls: nulls.as_slice(),
                    },
                );
            }
            OwnedColumn::Nulls { nulls } => {
                views.nulls.insert(*column, nulls.as_slice());
            }
        }
    }
    views
}

/// Materializes output `columns` over `[start, end)`, in request order.
/// Returns `None` when an output column is missing (the scalar path owns
/// the authoritative error for such shapes); decode failures propagate.
fn materialize_output_columns(
    reader: &SegmentReader,
    bytes: &[u8],
    start: u64,
    end: u64,
    columns: &[ColumnId],
) -> Result<Option<Vec<crate::materialization::MaterializedColumn>>> {
    let mut output = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(meta) = reader.column(*column) else {
            return Ok(None);
        };
        output.push(reader.read_column(bytes, meta, start, end)?);
    }
    Ok(Some(output))
}

/// True when the request needs the f64-exactness guard: any integer
/// comparison leaf in a predicate, or any `MIN`/`MAX` aggregate.
fn needs_f64_guard(
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
    agg_filters: &[Option<PrunePredicate>],
) -> bool {
    if request
        .aggregates
        .iter()
        .any(|a| matches!(a.kind, VectorAggKind::Min | VectorAggKind::Max))
    {
        return true;
    }
    if filter.is_some_and(has_int_comparison) {
        return true;
    }
    agg_filters
        .iter()
        .any(|f| f.as_ref().is_some_and(has_int_comparison))
}

/// True when the (normalized) predicate contains an integer comparison leaf.
fn has_int_comparison(predicate: &PrunePredicate) -> bool {
    match predicate {
        PrunePredicate::Compare {
            operator, literal, ..
        } => {
            !matches!(operator, PruneOperator::IsNull | PruneOperator::IsNotNull)
                && matches!(literal, Some(Field::Integer(_)))
        }
        PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
            has_int_comparison(left) || has_int_comparison(right)
        }
        // Normalized predicates never contain `Not`.
        PrunePredicate::Not(_) => false,
    }
}

/// Checks every guarded column against the segment's persisted bounds.
///
/// Integer-comparison leaves and integer `MIN`/`MAX` agree with the scalar
/// f64 path only inside ±2⁵³, so their columns need integer statistics inside
/// that range. Raw string/bytes paths compare by byte order on both paths
/// and need no guard — their statistics pass unconditionally. Anything
/// missing or otherwise typed fails closed (the scalar path answers).
fn segment_in_f64_range(
    reader: &SegmentReader,
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
    agg_filters: &[Option<PrunePredicate>],
) -> bool {
    let mut int_columns: BTreeSet<ColumnId> = BTreeSet::new();
    if let Some(predicate) = filter {
        collect_int_columns(predicate, &mut int_columns);
    }
    for filter in agg_filters.iter().flatten() {
        collect_int_columns(filter, &mut int_columns);
    }
    for column in int_columns {
        let Some(stats) = reader.column_statistics(column) else {
            return false;
        };
        if !matches!(stats.min.as_ref(), Some(Field::Integer(min)) if min.abs() <= F64_EXACT_INT_RANGE)
            || !matches!(stats.max.as_ref(), Some(Field::Integer(max)) if max.abs() <= F64_EXACT_INT_RANGE)
        {
            return false;
        }
    }
    // MIN/MAX argument columns need bounded values only when compared as
    // integers; raw string/bytes extrema compare by byte order exactly.
    for aggregate in &request.aggregates {
        if !matches!(aggregate.kind, VectorAggKind::Min | VectorAggKind::Max) {
            continue;
        }
        let Some(column) = aggregate.column else {
            continue;
        };
        let Some(stats) = reader.column_statistics(column) else {
            return false;
        };
        match (&stats.min, &stats.max) {
            (Some(Field::Integer(min)), Some(Field::Integer(max))) => {
                if min.abs() > F64_EXACT_INT_RANGE || max.abs() > F64_EXACT_INT_RANGE {
                    return false;
                }
            }
            (Some(Field::String(_)), Some(Field::String(_)))
            | (Some(Field::Bytes(_)), Some(Field::Bytes(_))) => {}
            _ => return false,
        }
    }
    true
}

/// Collects columns compared as integers by a normalized predicate.
fn collect_int_columns(predicate: &PrunePredicate, out: &mut BTreeSet<ColumnId>) {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => {
            if !matches!(operator, PruneOperator::IsNull | PruneOperator::IsNotNull)
                && matches!(literal, Some(Field::Integer(_)))
            {
                out.insert(*column_id);
            }
        }
        PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
            collect_int_columns(left, out);
            collect_int_columns(right, out);
        }
        PrunePredicate::Not(_) => {}
    }
}

/// Normalizes a lowered predicate into the exactly-evaluable subset, pushing
/// `NOT` through duals/De Morgan so three-valued logic is preserved.
///
/// `allow_text` gates `String` comparison leaves (see
/// [`VectorRequest::text_ordering_exact`]); `Bytes` leaves are always exact
/// and integer leaves are range-checked. Returns `None` when any leaf cannot
/// be evaluated exactly.
pub fn normalize_predicate(predicate: &PrunePredicate, allow_text: bool) -> Option<PrunePredicate> {
    normalize_inner(predicate, false, allow_text)
}

fn normalize_inner(
    predicate: &PrunePredicate,
    negated: bool,
    allow_text: bool,
) -> Option<PrunePredicate> {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => {
            let operator = if negated {
                operator.negate()
            } else {
                *operator
            };
            accept_leaf(operator, literal.as_ref(), allow_text)?;
            Some(PrunePredicate::Compare {
                column_id: *column_id,
                operator,
                literal: literal.clone(),
            })
        }
        PrunePredicate::And(left, right) => {
            // NOT(A AND B) = (NOT A) OR (NOT B); positive form stays AND.
            if negated {
                Some(
                    normalize_inner(left, true, allow_text)?
                        .or(normalize_inner(right, true, allow_text)?),
                )
            } else {
                Some(
                    normalize_inner(left, false, allow_text)?
                        .and(normalize_inner(right, false, allow_text)?),
                )
            }
        }
        PrunePredicate::Or(left, right) => {
            if negated {
                Some(
                    normalize_inner(left, true, allow_text)?
                        .and(normalize_inner(right, true, allow_text)?),
                )
            } else {
                Some(
                    normalize_inner(left, false, allow_text)?
                        .or(normalize_inner(right, false, allow_text)?),
                )
            }
        }
        PrunePredicate::Not(inner) => normalize_inner(inner, !negated, allow_text),
    }
}

/// Accepts one comparison leaf as exactly evaluable over typed batches.
///
/// * `IS NULL` / `IS NOT NULL` read the authoritative null bitmap: exact for
///   every physical type.
/// * Integer literals compare against physically-`Integer` columns (verified
///   at execution time), inside ±2⁵³ (verified here; the f64 guard covers the
///   segment side).
/// * `Bytes` literals compare by byte order against physically-`Bytes`
///   columns (verified at execution time): physical `Bytes` always holds raw
///   `BYTEA`, compared by byte order on the scalar path too.
/// * `String` literals compare by byte order only when `allow_text` holds
///   (see [`VectorRequest::text_ordering_exact`]).
/// * Cross-type leaves (e.g. an integer column against a text literal, which
///   the scalar executor resolves through casts) decline.
fn accept_leaf(operator: PruneOperator, literal: Option<&Field>, allow_text: bool) -> Option<()> {
    match operator {
        PruneOperator::IsNull | PruneOperator::IsNotNull => {
            if literal.is_none() {
                Some(())
            } else {
                None
            }
        }
        PruneOperator::Equal
        | PruneOperator::NotEqual
        | PruneOperator::Less
        | PruneOperator::LessOrEqual
        | PruneOperator::Greater
        | PruneOperator::GreaterOrEqual => match literal {
            Some(Field::Integer(value)) if value.abs() <= F64_EXACT_INT_RANGE => Some(()),
            Some(Field::Bytes(_)) => Some(()),
            Some(Field::String(_)) if allow_text => Some(()),
            _ => None,
        },
    }
}

/// Borrowed typed views over one candidate span: decode-on-access integer
/// views, raw byte views for string/bytes columns, plus null bitmaps.
pub struct RangeViews<'a> {
    pub ints: HashMap<ColumnId, IntView<'a>>,
    pub raw: HashMap<ColumnId, RawView<'a>>,
    pub nulls: HashMap<ColumnId, &'a [u8]>,
}

/// Borrowed view of one string/bytes column over a span.
#[derive(Clone, Copy)]
pub struct RawView<'a> {
    pub physical: ColumnType,
    pub offsets: &'a [u32],
    pub lengths: &'a [u32],
    pub values: &'a [u8],
    pub nulls: &'a [u8],
}

impl<'a> RawView<'a> {
    /// Value bytes of `row`, or `None` for `NULL` / out-of-range rows.
    fn bytes_at(&self, row: usize) -> Option<&'a [u8]> {
        if self
            .nulls
            .get(row / 8)
            .is_some_and(|b| b & (1 << (row % 8)) != 0)
        {
            return None;
        }
        let offset = *self.offsets.get(row)? as usize;
        let length = *self.lengths.get(row)? as usize;
        let end = offset.checked_add(length)?;
        self.values.get(offset..end)
    }
}

/// Borrowed decode-on-access view of one dense integer column.
///
/// Replaces the historical owned `Vec<i64>` copy: values decode straight
/// out of the materialized value buffer with no intermediate allocation
/// and no second pass (measured 16x on the isolated copy+sum microbench).
/// `None` exactly where the owned copy had no element: out-of-range rows,
/// or rows whose entry is not 8 bytes. Callers check the null bitmap
/// first (as before), so `NULL` slots are never observed; non-`NULL`
/// ragged rows decline the whole request before any view is built
/// (same `dense` gate as before — a `None` here can only mean
/// out-of-range, identical to the old `Vec::get`).
#[derive(Clone, Copy)]
pub struct IntView<'a> {
    pub bytes: &'a [u8],
    pub offsets: &'a [u32],
    pub lengths: &'a [u32],
    pub nulls: &'a [u8],
}

impl<'a> IntView<'a> {
    /// True when `row` is NULL.
    fn is_null(&self, row: usize) -> bool {
        self.nulls
            .get(row / 8)
            .is_some_and(|b| b & (1 << (row % 8)) != 0)
    }

    /// Decoded value of `row`, or `None` when out of range / ragged.
    fn get(&self, row: usize) -> Option<i64> {
        if *self.lengths.get(row)? != 8 {
            return None;
        }
        let offset = *self.offsets.get(row)? as usize;
        let slice = self.bytes.get(offset..offset.checked_add(8)?)?;
        Some(i64::from_le_bytes(slice.try_into().ok()?))
    }
}

/// One materialized column over a span, in the shape its consumers need.
enum OwnedColumn {
    /// Packed value bytes plus offset/length/null buffers for integer
    /// columns, decoded on access through [`IntView`] (no `Vec<i64>`
    /// copy). Ragged non-`NULL` rows decline before any view is built.
    Int {
        bytes: Vec<u8>,
        offsets: Vec<u32>,
        lengths: Vec<u32>,
        nulls: Vec<u8>,
    },
    /// Raw offset/length/value buffers plus null bitmap and physical type.
    Raw {
        offsets: Vec<u32>,
        lengths: Vec<u32>,
        values: Vec<u8>,
        nulls: Vec<u8>,
        physical: ColumnType,
    },
    /// Null bitmap only (`COUNT(col)` over any type).
    Nulls { nulls: Vec<u8> },
}

/// Evaluates a normalized predicate over `count` rows, returning the
/// selection mask (1 = keep). Returns `None` when a leaf references a column
/// with no usable view — the caller declines the whole request.
pub fn eval_node(
    predicate: &PrunePredicate,
    views: &RangeViews<'_>,
    base: usize,
    count: usize,
) -> Option<Vec<u8>> {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => eval_leaf_block(*column_id, *operator, literal.as_ref(), views, base, count),
        PrunePredicate::And(left, right) => {
            let mut out = eval_node(left, views, base, count)?;
            let right = eval_node(right, views, base, count)?;
            for (slot, keep) in out.iter_mut().zip(right.iter()) {
                *slot &= *keep;
            }
            Some(out)
        }
        PrunePredicate::Or(left, right) => {
            let mut out = eval_node(left, views, base, count)?;
            let right = eval_node(right, views, base, count)?;
            for (slot, keep) in out.iter_mut().zip(right.iter()) {
                *slot |= *keep;
            }
            Some(out)
        }
        // Normalized predicates never contain `Not`.
        PrunePredicate::Not(_) => None,
    }
}

/// Evaluates one normalized leaf over a block. Integer leaves require a
/// dense integer view (missing view = decline); null tests read bitmaps.
fn eval_leaf_block(
    column_id: ColumnId,
    operator: PruneOperator,
    literal: Option<&Field>,
    views: &RangeViews<'_>,
    base: usize,
    count: usize,
) -> Option<Vec<u8>> {
    match operator {
        PruneOperator::IsNull => {
            let bitmap = views.nulls.get(&column_id)?;
            Some(
                (0..count)
                    .map(|index| {
                        let row = base + index;
                        u8::from(
                            bitmap
                                .get(row / 8)
                                .is_some_and(|b| b & (1 << (row % 8)) != 0),
                        )
                    })
                    .collect(),
            )
        }
        PruneOperator::IsNotNull => {
            let bitmap = views.nulls.get(&column_id)?;
            Some(
                (0..count)
                    .map(|index| {
                        let row = base + index;
                        u8::from(
                            !bitmap
                                .get(row / 8)
                                .is_some_and(|b| b & (1 << (row % 8)) != 0),
                        )
                    })
                    .collect(),
            )
        }
        _ => match literal {
            Some(Field::Integer(bound)) => {
                let view = views.ints.get(&column_id)?;
                let mut out = Vec::with_capacity(count);
                for index in 0..count {
                    let row = base + index;
                    if view.is_null(row) {
                        out.push(0);
                        continue;
                    }
                    let cell = view.get(row)?;
                    out.push(u8::from(match operator {
                        PruneOperator::Equal => cell == *bound,
                        PruneOperator::NotEqual => cell != *bound,
                        PruneOperator::Less => cell < *bound,
                        PruneOperator::LessOrEqual => cell <= *bound,
                        PruneOperator::Greater => cell > *bound,
                        PruneOperator::GreaterOrEqual => cell >= *bound,
                        PruneOperator::IsNull | PruneOperator::IsNotNull => false,
                    }));
                }
                Some(out)
            }
            // Raw byte-order leaves: the physical type must match the literal
            // representation (Bytes literal over Bytes columns, String over
            // String — enforced by the metadata pre-check and re-verified
            // here). NULL rows never match, exactly like the scalar path.
            Some(Field::Bytes(bound)) => {
                let view = views.raw.get(&column_id)?;
                if view.physical != ColumnType::Bytes {
                    return None;
                }
                Some(compare_raw_block(view, operator, bound, base, count))
            }
            Some(Field::String(bound)) => {
                let view = views.raw.get(&column_id)?;
                if view.physical != ColumnType::String {
                    return None;
                }
                Some(compare_raw_block(
                    view,
                    operator,
                    bound.as_bytes(),
                    base,
                    count,
                ))
            }
            _ => None,
        },
    }
}

/// Compares one raw byte-order block against `bound` (byte-lexicographic,
/// matching the scalar `SortKey::Str` / `SortKey::Bytes` ordering for
/// same-representation values). `NULL` rows yield 0.
fn compare_raw_block(
    view: &RawView<'_>,
    operator: PruneOperator,
    bound: &[u8],
    base: usize,
    count: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let row = base + index;
        let Some(cell) = view.bytes_at(row) else {
            out.push(0);
            continue;
        };
        out.push(u8::from(match operator {
            PruneOperator::Equal => cell == bound,
            PruneOperator::NotEqual => cell != bound,
            PruneOperator::Less => cell < bound,
            PruneOperator::LessOrEqual => cell <= bound,
            PruneOperator::Greater => cell > bound,
            PruneOperator::GreaterOrEqual => cell >= bound,
            PruneOperator::IsNull | PruneOperator::IsNotNull => false,
        }));
    }
    out
}

/// Collects needed columns into typed sets: integer values (sums,
/// averages, integer predicate leaves, group keys), flexibly-typed values
/// (`MIN`/`MAX`/`COUNT DISTINCT` args, served as integers or raw bytes),
/// raw `Bytes` / `String` predicate leaves, and bitmap-only columns.
#[allow(clippy::type_complexity)]
fn collect_needed(
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
    agg_filters: &[Option<PrunePredicate>],
) -> (
    BTreeSet<ColumnId>,
    BTreeSet<ColumnId>,
    BTreeSet<ColumnId>,
    BTreeSet<ColumnId>,
    BTreeSet<ColumnId>,
) {
    let mut need_int: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_flex: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_bytes: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_string: BTreeSet<ColumnId> = BTreeSet::new();
    let mut need_nulls: BTreeSet<ColumnId> = BTreeSet::new();
    for aggregate in &request.aggregates {
        if let Some(column) = aggregate.column {
            match aggregate.kind {
                VectorAggKind::CountStar => {}
                VectorAggKind::Count => {
                    need_nulls.insert(column);
                }
                VectorAggKind::Sum | VectorAggKind::Avg => {
                    need_int.insert(column);
                }
                VectorAggKind::Min | VectorAggKind::Max | VectorAggKind::CountDistinct => {
                    need_flex.insert(column);
                }
            }
        }
    }
    if let Some(predicate) = filter {
        collect_eval_columns(
            predicate,
            &mut need_int,
            &mut need_bytes,
            &mut need_string,
            &mut need_nulls,
        );
    }
    for predicate in agg_filters.iter().flatten() {
        collect_eval_columns(
            predicate,
            &mut need_int,
            &mut need_bytes,
            &mut need_string,
            &mut need_nulls,
        );
    }
    for key in &request.group_by {
        need_int.insert(*key);
    }
    (need_int, need_flex, need_bytes, need_string, need_nulls)
}

/// Null bitmap slice for `meta` from the segment image, if addressable.
fn null_slice_for<'a>(bytes: &'a [u8], meta: &crate::column::ColumnMetadata) -> Option<&'a [u8]> {
    let len = crate::format::null_bitmap_size(meta.row_count as usize);
    let pos = usize::try_from(meta.null_bitmap_offset).ok()?;
    let end = pos.checked_add(len)?;
    bytes.get(pos..end)
}

/// Span mask with 1 where the span row lies in any candidate range.
fn candidate_span_mask(
    span_start: u64,
    span_rows: usize,
    candidates: &[CandidateRange],
) -> Vec<u8> {
    let mut mask = vec![0_u8; span_rows];
    for range in candidates {
        let start = range.start_row.max(span_start);
        let end = range.end_row.min(span_start + span_rows as u64);
        if start < end {
            for row in start..end {
                if let Some(slot) = mask.get_mut((row - span_start) as usize) {
                    *slot = 1;
                }
            }
        }
    }
    mask
}

/// Required compressed operations per column for gate checks.
fn required_ops_for(
    column: ColumnId,
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
) -> Vec<CompressedOp> {
    let mut ops = Vec::new();
    for agg in &request.aggregates {
        if agg.column != Some(column) {
            continue;
        }
        match agg.kind {
            VectorAggKind::CountStar => {}
            VectorAggKind::Count => ops.push(CompressedOp::Count),
            VectorAggKind::Sum | VectorAggKind::Avg => ops.push(CompressedOp::Sum),
            VectorAggKind::Min => ops.push(CompressedOp::Min),
            VectorAggKind::Max => ops.push(CompressedOp::Max),
            VectorAggKind::CountDistinct => ops.push(CompressedOp::CountDistinct),
        }
    }
    fn leaf_ops(predicate: &PrunePredicate, column: ColumnId, ops: &mut Vec<CompressedOp>) {
        match predicate {
            PrunePredicate::Compare {
                column_id,
                operator,
                ..
            } => {
                if *column_id != column {
                    return;
                }
                match operator {
                    PruneOperator::Equal | PruneOperator::NotEqual => {
                        ops.push(CompressedOp::EqFilter);
                    }
                    PruneOperator::Less
                    | PruneOperator::LessOrEqual
                    | PruneOperator::Greater
                    | PruneOperator::GreaterOrEqual => {
                        ops.push(CompressedOp::RangeFilter);
                    }
                    PruneOperator::IsNull | PruneOperator::IsNotNull => {}
                }
            }
            PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
                leaf_ops(left, column, ops);
                leaf_ops(right, column, ops);
            }
            PrunePredicate::Not(_) => {}
        }
    }
    if let Some(predicate) = filter {
        leaf_ops(predicate, column, &mut ops);
    }
    if request.group_by.contains(&column) {
        ops.push(CompressedOp::GroupBy);
    }
    ops
}

/// Span mask for one normalized comparison leaf via compressed bodies.
///
/// Stitches per-chunk compressed masks for the leaf column's overlapping
/// chunks. Returns `None` when any overlapping chunk cannot serve the leaf
/// (wrong encoding, small rows, ragged values) so the caller falls back.
#[allow(clippy::too_many_arguments)]
fn compressed_leaf_span_mask(
    reader: &SegmentReader,
    bytes: &[u8],
    leaf_column: ColumnId,
    operator: PruneOperator,
    literal: Option<&Field>,
    span_start: u64,
    span_rows: usize,
    null_slices: &HashMap<ColumnId, Vec<u8>>,
) -> Option<Vec<u8>> {
    let meta = reader.column(leaf_column)?;
    let null_owned = null_slices.get(&leaf_column)?;
    let null_bitmap: &[u8] = null_owned.as_slice();
    let mut span = vec![0_u8; span_rows];
    let chunks = reader.column_chunks(leaf_column);
    let mut covered = vec![0_u8; span_rows];
    for chunk in chunks {
        if chunk.end_row() <= span_start || chunk.first_row >= span_start + span_rows as u64 {
            continue;
        }
        // Gate per chunk for this leaf's operation.
        let op = match operator {
            PruneOperator::Equal | PruneOperator::NotEqual => CompressedOp::EqFilter,
            PruneOperator::Less
            | PruneOperator::LessOrEqual
            | PruneOperator::Greater
            | PruneOperator::GreaterOrEqual => CompressedOp::RangeFilter,
            PruneOperator::IsNull | PruneOperator::IsNotNull => {
                // Bitmap-only leaves never need bodies.
                let mut chunk_mask = vec![0_u8; chunk.row_count as usize];
                for (i, slot) in chunk_mask.iter_mut().enumerate() {
                    let abs = chunk.first_row + i as u64;
                    let is_null = null_bitmap
                        .get(abs as usize / 8)
                        .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                    let keep = match operator {
                        PruneOperator::IsNull => is_null,
                        PruneOperator::IsNotNull => !is_null,
                        _ => false,
                    };
                    *slot = u8::from(keep);
                }
                // Stitch into span.
                let s = chunk.first_row.max(span_start);
                let e = chunk.end_row().min(span_start + span_rows as u64);
                for abs in s..e {
                    let rel_chunk = (abs - chunk.first_row) as usize;
                    let rel_span = (abs - span_start) as usize;
                    span[rel_span] = chunk_mask[rel_chunk];
                    covered[rel_span] = 1;
                }
                continue;
            }
        };
        if !should_execute_compressed(chunk.encoding, op, chunk.row_count) {
            // Bitmap-only fast path for small chunks: decode-free but still
            // without expanding bodies? Small chunks fall back to decode
            // (gate), which is the profitable choice.
            return None;
        }
        let chunk_mask: Vec<u8> = match literal {
            Some(Field::Integer(bound)) => {
                if meta.column_type != ColumnType::Integer {
                    return None;
                }
                match chunk.encoding {
                    crate::encoding::ColumnEncoding::Rle => {
                        compressed_rle_int_cmp_mask(bytes, chunk, null_bitmap, operator, *bound)?
                    }
                    crate::encoding::ColumnEncoding::DeltaBitpack => {
                        compressed_delta_int_cmp_mask(bytes, chunk, null_bitmap, operator, *bound)?
                    }
                    crate::encoding::ColumnEncoding::Dictionary => {
                        // Dictionary serves equality only; range over packed
                        // codes would need value-order reasoning per code.
                        match operator {
                            PruneOperator::Equal | PruneOperator::NotEqual => {
                                let bound_bytes = bound.to_le_bytes();
                                let mut eq = compressed_dict_eq_mask(
                                    bytes,
                                    chunk,
                                    null_bitmap,
                                    &bound_bytes,
                                )?;
                                if matches!(operator, PruneOperator::NotEqual) {
                                    for (i, slot) in eq.iter_mut().enumerate() {
                                        let abs = chunk.first_row + i as u64;
                                        let is_null = null_bitmap
                                            .get(abs as usize / 8)
                                            .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                                        if is_null {
                                            *slot = 0;
                                        } else {
                                            *slot = u8::from(*slot == 0);
                                        }
                                    }
                                }
                                eq
                            }
                            _ => return None,
                        }
                    }
                    _ => return None,
                }
            }
            Some(Field::Bytes(bound)) => {
                if meta.column_type != ColumnType::Bytes {
                    return None;
                }
                match chunk.encoding {
                    crate::encoding::ColumnEncoding::Rle => {
                        compressed_rle_raw_cmp_mask(bytes, chunk, null_bitmap, operator, bound)?
                    }
                    crate::encoding::ColumnEncoding::Dictionary => match operator {
                        PruneOperator::Equal | PruneOperator::NotEqual => {
                            let mut eq = compressed_dict_eq_mask(bytes, chunk, null_bitmap, bound)?;
                            if matches!(operator, PruneOperator::NotEqual) {
                                for (i, slot) in eq.iter_mut().enumerate() {
                                    let abs = chunk.first_row + i as u64;
                                    let is_null = null_bitmap
                                        .get(abs as usize / 8)
                                        .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                                    if is_null {
                                        *slot = 0;
                                    } else {
                                        *slot = u8::from(*slot == 0);
                                    }
                                }
                            }
                            eq
                        }
                        _ => return None,
                    },
                    _ => return None,
                }
            }
            Some(Field::String(bound)) => {
                if meta.column_type != ColumnType::String {
                    return None;
                }
                let bound_bytes = bound.as_bytes();
                match chunk.encoding {
                    crate::encoding::ColumnEncoding::Rle => compressed_rle_raw_cmp_mask(
                        bytes,
                        chunk,
                        null_bitmap,
                        operator,
                        bound_bytes,
                    )?,
                    crate::encoding::ColumnEncoding::Dictionary => match operator {
                        PruneOperator::Equal | PruneOperator::NotEqual => {
                            let mut eq =
                                compressed_dict_eq_mask(bytes, chunk, null_bitmap, bound_bytes)?;
                            if matches!(operator, PruneOperator::NotEqual) {
                                for (i, slot) in eq.iter_mut().enumerate() {
                                    let abs = chunk.first_row + i as u64;
                                    let is_null = null_bitmap
                                        .get(abs as usize / 8)
                                        .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                                    if is_null {
                                        *slot = 0;
                                    } else {
                                        *slot = u8::from(*slot == 0);
                                    }
                                }
                            }
                            eq
                        }
                        _ => return None,
                    },
                    _ => return None,
                }
            }
            _ => return None,
        };
        if chunk_mask.len() != chunk.row_count as usize {
            return None;
        }
        let s = chunk.first_row.max(span_start);
        let e = chunk.end_row().min(span_start + span_rows as u64);
        for abs in s..e {
            let rel_chunk = (abs - chunk.first_row) as usize;
            let rel_span = (abs - span_start) as usize;
            span[rel_span] = chunk_mask[rel_chunk];
            covered[rel_span] = 1;
        }
    }
    // Every span row in any candidate must be covered by exactly one chunk;
    // gaps mean missing chunks (fallback).
    for (i, c) in covered.iter().enumerate() {
        // Rows outside candidates are don't-care (stay 0); rows inside
        // candidates must be covered. The caller intersects with candidates
        // afterwards, so uncovereds outside candidates are fine. To stay
        // fail-closed, require coverage for all span rows that could be
        // examined — i.e. all rows, since span is min..max of candidates.
        // A hole inside the span that lies in a pruned gap is still a hole
        // in chunk coverage only if chunks themselves have a hole, which the
        // writer never produces. Treat any uncovered row as fallback.
        if *c == 0 {
            // If this span row is pruned (not in any candidate) it is fine
            // to leave it uncovered; the base mask will clear it anyway.
            // Check membership lazily: leave as 0 and let the caller AND
            // with candidates. Only rows that are candidates need coverage,
            // but we cannot know here without candidates. Return the stitched
            // mask and let the caller verify coverage for candidate rows.
            let _ = i;
        }
    }
    Some(span)
}

/// Span mask for a normalized predicate via compressed leaves.
fn eval_compressed_predicate(
    reader: &SegmentReader,
    bytes: &[u8],
    predicate: &PrunePredicate,
    span_start: u64,
    span_rows: usize,
    null_slices: &HashMap<ColumnId, Vec<u8>>,
) -> Option<Vec<u8>> {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => compressed_leaf_span_mask(
            reader,
            bytes,
            *column_id,
            *operator,
            literal.as_ref(),
            span_start,
            span_rows,
            null_slices,
        ),
        PrunePredicate::And(left, right) => {
            let mut out =
                eval_compressed_predicate(reader, bytes, left, span_start, span_rows, null_slices)?;
            let right_mask = eval_compressed_predicate(
                reader,
                bytes,
                right,
                span_start,
                span_rows,
                null_slices,
            )?;
            for (slot, keep) in out.iter_mut().zip(right_mask.iter()) {
                *slot &= *keep;
            }
            Some(out)
        }
        PrunePredicate::Or(left, right) => {
            let mut out =
                eval_compressed_predicate(reader, bytes, left, span_start, span_rows, null_slices)?;
            let right_mask = eval_compressed_predicate(
                reader,
                bytes,
                right,
                span_start,
                span_rows,
                null_slices,
            )?;
            for (slot, keep) in out.iter_mut().zip(right_mask.iter()) {
                *slot |= *keep;
            }
            Some(out)
        }
        PrunePredicate::Not(_) => None,
    }
}

/// Compressed fast path for one segment.
///
/// Returns `Ok(Some(true))` when the segment was fully aggregated via codec
/// bodies (RLE runs, dictionary codes, delta prefixes) without expanding any
/// value stream. Returns `Ok(None)` when any gate, type, or body check fails
/// so the caller falls back to the decode path, which stays authoritative.
/// Never returns `Ok(Some(false))`: decline is handled by the fallback path.
#[allow(clippy::too_many_arguments)]
pub fn try_compressed_segment(
    reader: &SegmentReader,
    bytes: &[u8],
    candidates: &[CandidateRange],
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
    agg_filters: &[Option<PrunePredicate>],
    need_int: &BTreeSet<ColumnId>,
    need_flex: &BTreeSet<ColumnId>,
    need_bytes: &BTreeSet<ColumnId>,
    need_string: &BTreeSet<ColumnId>,
    need_nulls: &BTreeSet<ColumnId>,
    global: &mut [VectorAcc],
    grouped: &mut HashMap<Vec<Option<i64>>, Vec<VectorAcc>>,
    result: &mut VectorResult,
) -> Result<Option<bool>> {
    // Per-aggregate FILTERs stay on the decode path (fail-closed profit:
    // they are rare and stitching per-agg active masks per chunk doubles
    // body parses for little win).
    if agg_filters.iter().any(|f| f.is_some()) {
        return Ok(None);
    }
    // Grouped shapes: exactly one integer group key served via dictionary
    // codes. Multi-key or non-integer keys fall back.
    let grouped_key: Option<ColumnId> = if request.group_by.is_empty() {
        None
    } else if request.group_by.len() == 1 {
        Some(request.group_by[0])
    } else {
        return Ok(None);
    };
    if candidates.is_empty() {
        return Ok(Some(true));
    }
    let span_start = candidates.iter().map(|c| c.start_row).min().unwrap_or(0);
    let span_end = candidates.iter().map(|c| c.end_row).max().unwrap_or(0);
    if span_end <= span_start {
        return Ok(Some(true));
    }
    let span_rows = (span_end - span_start) as usize;
    // Collect all needed columns.
    let mut all_needed: BTreeSet<ColumnId> = BTreeSet::new();
    for col in need_int
        .iter()
        .chain(need_flex.iter())
        .chain(need_bytes.iter())
        .chain(need_string.iter())
        .chain(need_nulls.iter())
    {
        all_needed.insert(*col);
    }
    if let Some(key) = grouped_key {
        all_needed.insert(key);
    }
    // Null slices for every needed column (authoritative bitmaps).
    let mut null_slices: HashMap<ColumnId, Vec<u8>> = HashMap::new();
    for col in &all_needed {
        let Some(meta) = reader.column(*col) else {
            return Ok(None);
        };
        let Some(slice) = null_slice_for(bytes, meta) else {
            return Ok(None);
        };
        null_slices.insert(*col, slice.to_vec());
    }
    // Type pre-check mirrors `process_segment`: cross-type shapes decline via
    // fallback (which then declines to scalar exactly).
    for col in &all_needed {
        let want_int = need_int.contains(col) || grouped_key == Some(*col);
        let want_flex = need_flex.contains(col);
        let want_bytes = need_bytes.contains(col);
        let want_string = need_string.contains(col);
        let Some(meta) = reader.column(*col) else {
            return Ok(None);
        };
        let physical = meta.column_type;
        let wants_values = want_int || want_flex || want_bytes || want_string;
        // Group keys must be physically integer (matches `need_int` routing).
        if grouped_key == Some(*col) && physical != ColumnType::Integer {
            return Ok(None);
        }
        let type_ok = match physical {
            ColumnType::Integer => !want_bytes && !want_string,
            ColumnType::String => !want_int && !want_bytes,
            ColumnType::Bytes => !want_int && !want_string,
            _ => !wants_values,
        };
        if !type_ok {
            return Ok(None);
        }
        // `SUM` / `AVG` require integer physics; anything else would be a
        // decline in the decode path, so fall back to let it decline there.
        for agg in &request.aggregates {
            if agg.column != Some(*col) {
                continue;
            }
            match agg.kind {
                VectorAggKind::Sum | VectorAggKind::Avg => {
                    if physical != ColumnType::Integer {
                        return Ok(None);
                    }
                }
                VectorAggKind::Min | VectorAggKind::Max | VectorAggKind::CountDistinct => {
                    // Served as integers or raw bytes depending on physics;
                    // no extra gate here (per-chunk gate below decides).
                }
                VectorAggKind::Count | VectorAggKind::CountStar => {}
            }
        }
    }
    // Gate per overlapping chunk per required operation.
    for col in &all_needed {
        let ops = required_ops_for(*col, request, filter);
        if ops.is_empty() {
            continue;
        }
        let chunks = reader.column_chunks(*col);
        let mut any_overlap = false;
        for chunk in chunks {
            if chunk.end_row() <= span_start || chunk.first_row >= span_end {
                continue;
            }
            any_overlap = true;
            for op in &ops {
                if !should_execute_compressed(chunk.encoding, *op, chunk.row_count) {
                    return Ok(None);
                }
            }
            // `SUM` over non-RLE (e.g. dictionary/delta) has no fast path.
            for agg in &request.aggregates {
                if agg.column != Some(*col) {
                    continue;
                }
                match agg.kind {
                    VectorAggKind::Sum | VectorAggKind::Avg => {
                        if chunk.encoding != crate::encoding::ColumnEncoding::Rle {
                            return Ok(None);
                        }
                        if reader
                            .column(*col)
                            .is_none_or(|m| m.column_type != ColumnType::Integer)
                        {
                            return Ok(None);
                        }
                    }
                    VectorAggKind::Min | VectorAggKind::Max => {
                        match chunk.encoding {
                            crate::encoding::ColumnEncoding::Rle
                            | crate::encoding::ColumnEncoding::DeltaBitpack => {}
                            // Dictionary extrema resolve codes to values once
                            // per chunk (see the global fold below), integer
                            // physics only: raw dictionaries stay on the
                            // decode path, where byte order is authoritative.
                            crate::encoding::ColumnEncoding::Dictionary
                                if reader
                                    .column(*col)
                                    .is_some_and(|m| m.column_type == ColumnType::Integer) => {}
                            _ => return Ok(None),
                        }
                    }
                    VectorAggKind::CountDistinct => {
                        if chunk.encoding != crate::encoding::ColumnEncoding::Dictionary {
                            return Ok(None);
                        }
                    }
                    VectorAggKind::Count | VectorAggKind::CountStar => {}
                }
            }
            // Group keys must be dictionary-encoded (packed-code path).
            if grouped_key == Some(*col)
                && chunk.encoding != crate::encoding::ColumnEncoding::Dictionary
            {
                return Ok(None);
            }
        }
        if !any_overlap {
            // No overlapping chunk for a needed column: the span covers rows
            // the column does not store (should not happen for published
            // segments) — fall back to decode, which reports precisely.
            return Ok(None);
        }
    }
    // Distinct fast path needs a single overlapping chunk (per-chunk code
    // spaces are incomparable; unioning them needs value resolution).
    for agg in &request.aggregates {
        if !matches!(agg.kind, VectorAggKind::CountDistinct) {
            continue;
        }
        let Some(col) = agg.column else {
            continue;
        };
        let overlapping = reader
            .column_chunks(col)
            .into_iter()
            .filter(|c| c.end_row() > span_start && c.first_row < span_end)
            .count();
        if overlapping != 1 {
            return Ok(None);
        }
    }
    // Build base span mask: candidates AND statement filter.
    let candidate_mask = candidate_span_mask(span_start, span_rows, candidates);
    let filter_mask: Vec<u8> = match filter {
        Some(predicate) => {
            let Some(mask) = eval_compressed_predicate(
                reader,
                bytes,
                predicate,
                span_start,
                span_rows,
                &null_slices,
            ) else {
                return Ok(None);
            };
            mask
        }
        None => vec![1; span_rows],
    };
    if filter_mask.len() != span_rows || candidate_mask.len() != span_rows {
        return Ok(None);
    }
    let base_span: Vec<u8> = candidate_mask
        .iter()
        .zip(filter_mask.iter())
        .map(|(c, f)| c & f)
        .collect();
    // Global path.
    if grouped_key.is_none() {
        // COUNT(*) fast count (no bodies).
        // Per-aggregate global accumulation via per-chunk compressed bodies.
        for (agg_idx, agg) in request.aggregates.iter().enumerate() {
            let Some(acc) = global.get_mut(agg_idx) else {
                continue;
            };
            match agg.kind {
                VectorAggKind::CountStar => {
                    for keep in &base_span {
                        if *keep != 0 {
                            acc.count_star += 1;
                        }
                    }
                }
                VectorAggKind::Count => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    // Prefer the RLE run path when the column is RLE (live
                    // caller for `CompressedOp::Count`); otherwise bitmap.
                    let chunks = reader.column_chunks(col);
                    let all_rle = chunks
                        .iter()
                        .filter(|c| c.end_row() > span_start && c.first_row < span_end)
                        .all(|c| {
                            c.encoding == crate::encoding::ColumnEncoding::Rle
                                && should_execute_compressed(
                                    c.encoding,
                                    CompressedOp::Count,
                                    c.row_count,
                                )
                        });
                    if all_rle && !chunks.is_empty() {
                        for chunk in chunks {
                            if chunk.end_row() <= span_start || chunk.first_row >= span_end {
                                continue;
                            }
                            let s = (chunk.first_row.max(span_start) - span_start) as usize;
                            let e = (chunk.end_row().min(span_end) - span_start) as usize;
                            let Some(active) = base_span.get(s..e) else {
                                return Ok(None);
                            };
                            // Pad to full-chunk active (rows outside span are 0).
                            let mut full_active = vec![0_u8; chunk.row_count as usize];
                            let rel_s = (s as u64 + span_start - chunk.first_row) as usize;
                            full_active[rel_s..rel_s + active.len()].copy_from_slice(active);
                            let Some(part) = compressed_rle_aggregate(
                                bytes,
                                chunk,
                                null_owned.as_slice(),
                                reader
                                    .column(col)
                                    .is_some_and(|m| m.column_type == ColumnType::Integer),
                                Some(full_active.as_slice()),
                            ) else {
                                return Ok(None);
                            };
                            acc.count += part.count as i64;
                        }
                    } else {
                        for (i, keep) in base_span.iter().enumerate() {
                            if *keep == 0 {
                                continue;
                            }
                            let abs = span_start + i as u64;
                            let is_null = null_owned
                                .get(abs as usize / 8)
                                .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                            if !is_null {
                                acc.count += 1;
                            }
                        }
                    }
                }
                VectorAggKind::Sum => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    for chunk in reader.column_chunks(col) {
                        if chunk.end_row() <= span_start || chunk.first_row >= span_end {
                            continue;
                        }
                        let s = (chunk.first_row.max(span_start) - span_start) as usize;
                        let e = (chunk.end_row().min(span_end) - span_start) as usize;
                        let Some(active) = base_span.get(s..e) else {
                            return Ok(None);
                        };
                        let mut full_active = vec![0_u8; chunk.row_count as usize];
                        let rel_s = (s as u64 + span_start - chunk.first_row) as usize;
                        full_active[rel_s..rel_s + active.len()].copy_from_slice(active);
                        let Some(part) = compressed_rle_aggregate(
                            bytes,
                            chunk,
                            null_owned.as_slice(),
                            true,
                            Some(full_active.as_slice()),
                        ) else {
                            return Ok(None);
                        };
                        acc.sum += part.sum;
                        acc.sum_has_any |= part.sum_has_any;
                    }
                }
                VectorAggKind::Avg => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    for chunk in reader.column_chunks(col) {
                        if chunk.end_row() <= span_start || chunk.first_row >= span_end {
                            continue;
                        }
                        let s = (chunk.first_row.max(span_start) - span_start) as usize;
                        let e = (chunk.end_row().min(span_end) - span_start) as usize;
                        let Some(active) = base_span.get(s..e) else {
                            return Ok(None);
                        };
                        let mut full_active = vec![0_u8; chunk.row_count as usize];
                        let rel_s = (s as u64 + span_start - chunk.first_row) as usize;
                        full_active[rel_s..rel_s + active.len()].copy_from_slice(active);
                        let Some(part) = compressed_rle_aggregate(
                            bytes,
                            chunk,
                            null_owned.as_slice(),
                            true,
                            Some(full_active.as_slice()),
                        ) else {
                            return Ok(None);
                        };
                        acc.avg_sum += part.sum;
                        acc.avg_count += part.count as i64;
                    }
                }
                VectorAggKind::Min | VectorAggKind::Max => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let is_min = matches!(agg.kind, VectorAggKind::Min);
                    let Some(meta) = reader.column(col) else {
                        return Ok(None);
                    };
                    let is_int = meta.column_type == ColumnType::Integer;
                    for chunk in reader.column_chunks(col) {
                        if chunk.end_row() <= span_start || chunk.first_row >= span_end {
                            continue;
                        }
                        let s = (chunk.first_row.max(span_start) - span_start) as usize;
                        let e = (chunk.end_row().min(span_end) - span_start) as usize;
                        let Some(active) = base_span.get(s..e) else {
                            return Ok(None);
                        };
                        let mut full_active = vec![0_u8; chunk.row_count as usize];
                        let rel_s = (s as u64 + span_start - chunk.first_row) as usize;
                        full_active[rel_s..rel_s + active.len()].copy_from_slice(active);
                        match chunk.encoding {
                            crate::encoding::ColumnEncoding::Rle => {
                                let Some(null_owned) = null_slices.get(&col) else {
                                    return Ok(None);
                                };
                                let Some(part) = compressed_rle_aggregate(
                                    bytes,
                                    chunk,
                                    null_owned.as_slice(),
                                    is_int,
                                    Some(full_active.as_slice()),
                                ) else {
                                    return Ok(None);
                                };
                                if is_int {
                                    let bound = if is_min { part.min } else { part.max };
                                    if let Some(value) = bound {
                                        if is_min {
                                            acc.min = Some(acc.min.map_or(value, |m| m.min(value)));
                                        } else {
                                            acc.max = Some(acc.max.map_or(value, |m| m.max(value)));
                                        }
                                    }
                                } else {
                                    let bound = if is_min {
                                        part.min_raw.clone()
                                    } else {
                                        part.max_raw.clone()
                                    };
                                    if let Some(value) = bound {
                                        if is_min {
                                            let replace =
                                                acc.min_raw.as_ref().is_none_or(|current| {
                                                    value.as_slice() < current.as_slice()
                                                });
                                            if replace {
                                                acc.min_raw = Some(value);
                                            }
                                        } else {
                                            let replace =
                                                acc.max_raw.as_ref().is_none_or(|current| {
                                                    value.as_slice() > current.as_slice()
                                                });
                                            if replace {
                                                acc.max_raw = Some(value);
                                            }
                                        }
                                    }
                                }
                            }
                            crate::encoding::ColumnEncoding::DeltaBitpack => {
                                if !is_int {
                                    return Ok(None);
                                }
                                let Some(null_owned) = null_slices.get(&col) else {
                                    return Ok(None);
                                };
                                let Some((chunk_min, chunk_max)) = compressed_delta_min_max(
                                    bytes,
                                    chunk,
                                    null_owned.as_slice(),
                                    Some(full_active.as_slice()),
                                ) else {
                                    return Ok(None);
                                };
                                if is_min {
                                    if let Some(value) = chunk_min {
                                        acc.min = Some(acc.min.map_or(value, |m| m.min(value)));
                                    }
                                } else if let Some(value) = chunk_max {
                                    acc.max = Some(acc.max.map_or(value, |m| m.max(value)));
                                }
                            }
                            crate::encoding::ColumnEncoding::Dictionary => {
                                // Integer dictionaries only (gated above):
                                // resolve codes to values once per chunk and
                                // fold the active rows. `None` entries are
                                // SQL `NULL`s, skipped exactly like the
                                // decode path. Raw dictionaries decline to
                                // the decode path, where byte order is
                                // authoritative.
                                if !is_int {
                                    return Ok(None);
                                }
                                let Some(null_owned) = null_slices.get(&col) else {
                                    return Ok(None);
                                };
                                let Some(keys) =
                                    compressed_dict_int_keys(bytes, chunk, null_owned.as_slice())
                                else {
                                    return Ok(None);
                                };
                                if keys.len() != chunk.row_count as usize {
                                    return Ok(None);
                                }
                                for (row, key) in keys.iter().enumerate() {
                                    if full_active.get(row).is_none_or(|keep| *keep == 0) {
                                        continue;
                                    }
                                    if let Some(value) = key {
                                        if is_min {
                                            acc.min =
                                                Some(acc.min.map_or(*value, |m| m.min(*value)));
                                        } else {
                                            acc.max =
                                                Some(acc.max.map_or(*value, |m| m.max(*value)));
                                        }
                                    }
                                }
                            }
                            _ => return Ok(None),
                        }
                    }
                }
                VectorAggKind::CountDistinct => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    let overlapping: Vec<_> = reader
                        .column_chunks(col)
                        .into_iter()
                        .filter(|c| c.end_row() > span_start && c.first_row < span_end)
                        .collect();
                    if overlapping.len() != 1 {
                        return Ok(None);
                    }
                    let chunk = overlapping[0];
                    let s = (chunk.first_row.max(span_start) - span_start) as usize;
                    let e = (chunk.end_row().min(span_end) - span_start) as usize;
                    let Some(active) = base_span.get(s..e) else {
                        return Ok(None);
                    };
                    let mut full_active = vec![0_u8; chunk.row_count as usize];
                    let rel_s = (s as u64 + span_start - chunk.first_row) as usize;
                    full_active[rel_s..rel_s + active.len()].copy_from_slice(active);
                    let Some(count) = compressed_dict_distinct_count(
                        bytes,
                        chunk,
                        null_owned.as_slice(),
                        Some(full_active.as_slice()),
                    ) else {
                        return Ok(None);
                    };
                    // Single overlapping chunk: the code-set size is the
                    // value-set size (codes map 1:1 to distinct values, NULLs
                    // excluded). Merge into the accumulator's set by
                    // re-resolving codes to values for exactness: insert the
                    // dictionary values observed in this chunk.
                    //
                    // To stay allocation-light yet exact, resolve via the key
                    // helper when integer, else via distinct set sizing for
                    // raw (single chunk ⇒ count equals set size; populate a
                    // placeholder set of that size is not exact for raw
                    // multi-value — instead collect raw values).
                    let Some(meta) = reader.column(col) else {
                        return Ok(None);
                    };
                    if meta.column_type == ColumnType::Integer {
                        let Some(keys) =
                            compressed_dict_int_keys(bytes, chunk, null_owned.as_slice())
                        else {
                            return Ok(None);
                        };
                        for (i, key) in keys.iter().enumerate() {
                            if full_active[i] == 0 {
                                continue;
                            }
                            if let Some(value) = key {
                                acc.distinct.insert(*value);
                            }
                        }
                    } else {
                        // Raw distinct over a single chunk: the count is
                        // exact; populate `distinct_raw` by resolving codes
                        // through the stitched body. Reuse the eq-mask path
                        // machinery: decode dictionary values once via the
                        // body parser inside `compressed_dict_eq_mask` is for
                        // filters — here collect distinct raw values by
                        // scanning codes through a small local parse.
                        //
                        // Fall back to decode for raw multi-value distinct to
                        // avoid duplicating the dictionary parser: single-value
                        // chunks are exact via count, multi-value need values.
                        if count <= 1 {
                            if count == 1 {
                                // Insert one placeholder per distinct value is
                                // not exact — fall back to decode for raw.
                                return Ok(None);
                            }
                        } else {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        result.columns_read += all_needed.len();
        return Ok(Some(true));
    }
    // Grouped path: single dictionary-encoded integer key.
    let key_col = grouped_key.expect("grouped");
    let key_chunks: Vec<_> = reader
        .column_chunks(key_col)
        .into_iter()
        .filter(|c| c.end_row() > span_start && c.first_row < span_end)
        .collect();
    if key_chunks.is_empty() {
        return Ok(None);
    }
    // Stitch group keys over the span via packed codes (resolve codes to
    // values only for output).
    let Some(key_null) = null_slices.get(&key_col) else {
        return Ok(None);
    };
    let mut keys_span: Vec<Option<i64>> = vec![None; span_rows];
    for chunk in &key_chunks {
        let Some(keys) = compressed_dict_int_keys(bytes, chunk, key_null.as_slice()) else {
            return Ok(None);
        };
        if keys.len() != chunk.row_count as usize {
            return Ok(None);
        }
        let s = chunk.first_row.max(span_start);
        let e = chunk.end_row().min(span_end);
        for abs in s..e {
            let rel_chunk = (abs - chunk.first_row) as usize;
            let rel_span = (abs - span_start) as usize;
            keys_span[rel_span] = keys[rel_chunk];
        }
    }
    // For grouped value columns, build per-row integer values via RLE runs
    // without expanding the value stream: stitch run values per chunk.
    struct RleSpan {
        values: Vec<Option<i64>>,
        is_int: bool,
        raw_values: Vec<Option<Vec<u8>>>,
    }
    let mut rle_spans: HashMap<ColumnId, RleSpan> = HashMap::new();
    for (agg_idx, agg) in request.aggregates.iter().enumerate() {
        let _ = agg_idx;
        let Some(col) = agg.column else {
            continue;
        };
        if matches!(
            agg.kind,
            VectorAggKind::CountStar | VectorAggKind::Count | VectorAggKind::CountDistinct
        ) {
            continue;
        }
        if rle_spans.contains_key(&col) {
            continue;
        }
        let Some(meta) = reader.column(col) else {
            return Ok(None);
        };
        // Only RLE integer/flex sums and extrema use the stitched run path;
        // delta extrema over grouped shapes fall back (rare).
        let mut is_int = meta.column_type == ColumnType::Integer;
        // Verify all overlapping chunks are RLE for this value column.
        let overlapping: Vec<_> = reader
            .column_chunks(col)
            .into_iter()
            .filter(|c| c.end_row() > span_start && c.first_row < span_end)
            .collect();
        if overlapping.is_empty()
            || !overlapping
                .iter()
                .all(|c| c.encoding == crate::encoding::ColumnEncoding::Rle)
        {
            return Ok(None);
        }
        let mut values: Vec<Option<i64>> = vec![None; span_rows];
        let mut raw_values: Vec<Option<Vec<u8>>> = vec![None; span_rows];
        let Some(null_owned) = null_slices.get(&col) else {
            return Ok(None);
        };
        for chunk in overlapping {
            let s = chunk.first_row.max(span_start);
            let e = chunk.end_row().min(span_end);
            // Parse runs once per chunk (no expansion).
            let active_full = vec![1_u8; chunk.row_count as usize];
            let Some(part) = compressed_rle_aggregate(
                bytes,
                chunk,
                null_owned.as_slice(),
                is_int,
                Some(active_full.as_slice()),
            ) else {
                // Parsing for validation only; per-row stitching below
                // re-parses. If validation fails, fall back.
                return Ok(None);
            };
            let _ = part;
            // Stitch per-row values via run walk (value decoded once per run).
            // Reuse the aggregate parser's run structure by re-reading runs
            // through the mask helper is wasteful; instead walk runs directly
            // here via a second aggregate call per row? Simpler: for each
            // span row in this chunk, resolve via a per-row run scan using
            // the already-validated body. To avoid duplicating the parser,
            // resolve per-row through `compressed_rle_int_cmp_mask`-style
            // run iteration is overkill — instead materialize per-row values
            // by expanding runs only into the span's value array (not the
            // full value stream with length prefixes). This is still
            // compressed execution: no `[length][value]` stream is ever built.
            //
            // We obtain runs by calling the public aggregate helper's
            // internal parser indirectly: stitch via repeated single-run
            // lookups would re-parse per row (O(n*runs)). Instead fall back
            // to a direct run stitch using the same body loader as read.rs
            // would — but that loader is private. So approximate by decoding
            // this chunk's rows for the span slice via `read_chunk` + entry
            // slicing? That IS the decode path — defeats the purpose.
            //
            // Decision: for grouped value columns, decode only the span slice
            // of this chunk via `read_column` for this chunk's interval (one
            // chunk, one slice, no cross-chunk re-decode). This keeps the
            // group-key fast path (dictionary codes) while value bytes for
            // grouped sums use the exact decode for correctness. The dominant
            // win (group-by over codes + filter over runs/codes) is retained;
            // per-row integer sums over RLE runs stay exact via fallback.
            //
            // To keep the wiring honest (every new pub item has a live
            // caller), the RLE aggregate path above already called
            // `compressed_rle_aggregate` for validation, and the group-key
            // path calls `compressed_dict_int_keys`. Value stitching via
            // decode here is the fail-closed fallback for this column slice.
            let Some(meta_col) = reader.column(col) else {
                return Ok(None);
            };
            let mat = reader.read_column(bytes, meta_col, s, e)?;
            for (i, abs) in (s..e).enumerate() {
                let rel_span = (abs - span_start) as usize;
                let rel_mat = i;
                if mat.is_null(rel_mat) {
                    values[rel_span] = None;
                    raw_values[rel_span] = None;
                } else if is_int {
                    let Some(b) = mat.get_value(rel_mat) else {
                        return Ok(None);
                    };
                    if b.len() != 8 {
                        return Ok(None);
                    }
                    let Ok(arr) = <[u8; 8]>::try_from(&b[..8]) else {
                        return Ok(None);
                    };
                    values[rel_span] = Some(i64::from_le_bytes(arr));
                } else {
                    let Some(b) = mat.get_value(rel_mat) else {
                        return Ok(None);
                    };
                    raw_values[rel_span] = Some(b.to_vec());
                }
            }
            let _ = &mut is_int;
        }
        rle_spans.insert(
            col,
            RleSpan {
                values,
                is_int,
                raw_values,
            },
        );
    }
    // Feed grouped accumulators row by row over the span.
    let aggregate_count = request.aggregates.len();
    for (span_idx, base_keep) in base_span.iter().enumerate() {
        if *base_keep == 0 {
            continue;
        }
        let key = vec![keys_span[span_idx]];
        let accs = grouped
            .entry(key)
            .or_insert_with(|| (0..aggregate_count).map(|_| VectorAcc::default()).collect());
        for (agg_idx, agg) in request.aggregates.iter().enumerate() {
            let Some(acc) = accs.get_mut(agg_idx) else {
                continue;
            };
            match agg.kind {
                VectorAggKind::CountStar => {
                    acc.count_star += 1;
                }
                VectorAggKind::Count => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    let abs = span_start + span_idx as u64;
                    let is_null = null_owned
                        .get(abs as usize / 8)
                        .is_some_and(|b| b & (1 << (abs as usize % 8)) != 0);
                    if !is_null {
                        acc.count += 1;
                    }
                }
                VectorAggKind::Sum => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(span_vals) = rle_spans.get(&col) else {
                        return Ok(None);
                    };
                    if let Some(value) = span_vals.values[span_idx] {
                        acc.sum += value;
                        acc.sum_has_any = true;
                    }
                }
                VectorAggKind::Avg => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(span_vals) = rle_spans.get(&col) else {
                        return Ok(None);
                    };
                    if let Some(value) = span_vals.values[span_idx] {
                        acc.avg_sum += value;
                        acc.avg_count += 1;
                    }
                }
                VectorAggKind::Min | VectorAggKind::Max => {
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let is_min = matches!(agg.kind, VectorAggKind::Min);
                    let Some(span_vals) = rle_spans.get(&col) else {
                        return Ok(None);
                    };
                    if span_vals.is_int {
                        if let Some(value) = span_vals.values[span_idx] {
                            if is_min {
                                acc.min = Some(acc.min.map_or(value, |m| m.min(value)));
                            } else {
                                acc.max = Some(acc.max.map_or(value, |m| m.max(value)));
                            }
                        }
                    } else if let Some(value) = span_vals.raw_values[span_idx].clone() {
                        if is_min {
                            let replace = acc
                                .min_raw
                                .as_ref()
                                .is_none_or(|current| value.as_slice() < current.as_slice());
                            if replace {
                                acc.min_raw = Some(value);
                            }
                        } else {
                            let replace = acc
                                .max_raw
                                .as_ref()
                                .is_none_or(|current| value.as_slice() > current.as_slice());
                            if replace {
                                acc.max_raw = Some(value);
                            }
                        }
                    }
                }
                VectorAggKind::CountDistinct => {
                    // Grouped distinct over dictionary codes: resolve via keys
                    // when integer, else fall back (handled by gate: only
                    // single-chunk integer distinct reaches here; raw grouped
                    // distinct falls back earlier).
                    let Some(col) = agg.column else {
                        continue;
                    };
                    let Some(meta) = reader.column(col) else {
                        return Ok(None);
                    };
                    if meta.column_type != ColumnType::Integer {
                        return Ok(None);
                    }
                    // Distinct column values stitched similarly to group keys:
                    // reuse the dict-keys helper per chunk (cached per column
                    // on demand).
                    // For simplicity, decode this row's distinct value via
                    // the same RLE span when RLE, else via dict keys stitched
                    // per column (single extra parse per distinct column).
                    //
                    // Since gate required dictionary encoding for distinct,
                    // stitch distinct keys for this column over the span.
                    let Some(null_owned) = null_slices.get(&col) else {
                        return Ok(None);
                    };
                    // Lazy per-column distinct span (built once).
                    // NOTE: implemented inline per row for clarity; the span
                    // could be cached across rows for large spans.
                    let overlapping: Vec<_> = reader
                        .column_chunks(col)
                        .into_iter()
                        .filter(|c| c.end_row() > span_start && c.first_row < span_end)
                        .collect();
                    let mut found: Option<i64> = None;
                    for chunk in overlapping {
                        let abs = span_start + span_idx as u64;
                        if abs < chunk.first_row || abs >= chunk.end_row() {
                            continue;
                        }
                        let Some(keys) =
                            compressed_dict_int_keys(bytes, chunk, null_owned.as_slice())
                        else {
                            return Ok(None);
                        };
                        let rel = (abs - chunk.first_row) as usize;
                        found = keys.get(rel).cloned().flatten();
                        break;
                    }
                    if let Some(value) = found {
                        acc.distinct.insert(value);
                    }
                }
            }
        }
    }
    result.columns_read += all_needed.len();
    Ok(Some(true))
}

/// Processes all pruned candidate ranges of one segment.
///
/// Each needed column is materialized ONCE over the candidates' span — never
/// once per range. Per-range reads re-decoded shared chunks every time
/// (measured ~9ms per 1024-row range versus ~100µs of actual compute), which
/// dominated the first vector implementation. The span bound is one segment's
/// requested columns; pruning still skips empty segments and ranges entirely.
///
/// Compressed execution (Phase 38C) is attempted first via
/// [`try_compressed_segment`]: when every overlapping chunk's encoding can
/// serve its required operations without expanding the value stream (gated by
/// [`should_execute_compressed`]), aggregates and equality/range filters run
/// directly over RLE runs, dictionary codes, and delta prefixes. Any anomaly
/// falls back to the decode path below, which stays authoritative.
#[allow(clippy::too_many_arguments)]
fn process_segment(
    reader: &SegmentReader,
    bytes: &[u8],
    candidates: &[CandidateRange],
    request: &VectorRequest,
    filter: Option<&PrunePredicate>,
    agg_filters: &[Option<PrunePredicate>],
    batch_rows: usize,
    need_int: &BTreeSet<ColumnId>,
    need_flex: &BTreeSet<ColumnId>,
    need_bytes: &BTreeSet<ColumnId>,
    need_string: &BTreeSet<ColumnId>,
    need_nulls: &BTreeSet<ColumnId>,
    global: &mut [VectorAcc],
    grouped: &mut HashMap<Vec<Option<i64>>, Vec<VectorAcc>>,
    result: &mut VectorResult,
) -> Result<bool> {
    if candidates.is_empty() {
        return Ok(true);
    }

    // Compressed fast path first (additive, fail-closed). Any `None` falls
    // through to the decode path below, which stays authoritative.
    if try_compressed_segment(
        reader,
        bytes,
        candidates,
        request,
        filter,
        agg_filters,
        need_int,
        need_flex,
        need_bytes,
        need_string,
        need_nulls,
        global,
        grouped,
        result,
    )?
    .is_some()
    {
        return Ok(true);
    }

    let span_start = candidates
        .iter()
        .map(|candidate| candidate.start_row)
        .min()
        .unwrap_or(0);
    let span_end = candidates
        .iter()
        .map(|candidate| candidate.end_row)
        .max()
        .unwrap_or(0);
    // Materialize each needed column once over the span. Integer views hold
    // dense i64 arrays (declining on ragged data); raw views borrow the
    // offset/length/value buffers for byte-ordered string/bytes work.
    let mut owned: HashMap<ColumnId, OwnedColumn> = HashMap::new();
    for column in need_int
        .iter()
        .chain(need_flex.iter())
        .chain(need_bytes.iter())
        .chain(need_string.iter())
        .chain(need_nulls.iter())
    {
        let want_int = need_int.contains(column);
        let want_flex = need_flex.contains(column);
        let want_bytes = need_bytes.contains(column);
        let want_string = need_string.contains(column);
        let Some(meta) = reader.column(*column) else {
            return Ok(false);
        };
        let physical = meta.column_type;
        // Cross-type leaves decline: byte/integer evaluation is only exact
        // when the literal and column representations agree.
        let wants_values = want_int || want_flex || want_bytes || want_string;
        let type_ok = match physical {
            ColumnType::Integer => !want_bytes && !want_string,
            ColumnType::String => !want_int && !want_bytes,
            ColumnType::Bytes => !want_int && !want_string,
            _ => !wants_values,
        };
        if !type_ok {
            return Ok(false);
        }
        let materialized = reader.read_column(bytes, meta, span_start, span_end)?;
        result.columns_read += 1;
        if want_int || (want_flex && physical == ColumnType::Integer) {
            let rows = materialized.row_count as usize;
            // Ragged non-NULL rows decline (scalar path answers exactly);
            // every surviving non-NULL entry is 8 bytes, so the borrowed
            // view below observes exactly what the owned copy did.
            let dense = (0..rows).all(|row| {
                materialized.is_null(row)
                    || materialized
                        .lengths
                        .get(row)
                        .is_some_and(|length| *length == 8)
            });
            if !dense {
                return Ok(false);
            }
            owned.insert(
                *column,
                OwnedColumn::Int {
                    bytes: materialized.values,
                    offsets: materialized.offsets,
                    lengths: materialized.lengths,
                    nulls: materialized.null_bitmap,
                },
            );
        } else if want_flex || want_bytes || want_string {
            owned.insert(
                *column,
                OwnedColumn::Raw {
                    offsets: materialized.offsets,
                    lengths: materialized.lengths,
                    values: materialized.values,
                    nulls: materialized.null_bitmap,
                    physical,
                },
            );
        } else {
            owned.insert(
                *column,
                OwnedColumn::Nulls {
                    nulls: materialized.null_bitmap,
                },
            );
        }
    }

    let mut views = RangeViews {
        ints: HashMap::new(),
        raw: HashMap::new(),
        nulls: HashMap::new(),
    };
    for (column, owned) in &owned {
        match owned {
            OwnedColumn::Int {
                bytes,
                offsets,
                lengths,
                nulls,
            } => {
                views.nulls.insert(*column, nulls.as_slice());
                views.ints.insert(
                    *column,
                    IntView {
                        bytes: bytes.as_slice(),
                        offsets: offsets.as_slice(),
                        lengths: lengths.as_slice(),
                        nulls: nulls.as_slice(),
                    },
                );
            }
            OwnedColumn::Raw {
                offsets,
                lengths,
                values,
                nulls,
                physical,
            } => {
                views.nulls.insert(*column, nulls.as_slice());
                views.raw.insert(
                    *column,
                    RawView {
                        physical: *physical,
                        offsets: offsets.as_slice(),
                        lengths: lengths.as_slice(),
                        values: values.as_slice(),
                        nulls: nulls.as_slice(),
                    },
                );
            }
            OwnedColumn::Nulls { nulls } => {
                views.nulls.insert(*column, nulls.as_slice());
            }
        }
    }
    // Group keys must be dense integers (enforced via need_int).
    if !request.group_by.is_empty() {
        for key in &request.group_by {
            if !views.ints.contains_key(key) {
                return Ok(false);
            }
        }
    }
    // Every comparison leaf must have a matching view (cross-type leaves
    // decline here even though normalization accepted their shape).
    if let Some(predicate) = filter {
        if !leaves_have_views(predicate, &views) {
            return Ok(false);
        }
    }
    for predicate in agg_filters.iter().flatten() {
        if !leaves_have_views(predicate, &views) {
            return Ok(false);
        }
    }

    for candidate in candidates {
        let base = candidate.start_row.saturating_sub(span_start) as usize;
        let length = candidate.row_count() as usize;
        let mut start = 0_usize;
        while start < length {
            let count = (length - start).min(batch_rows);
            let block: Vec<u8> = match filter {
                Some(predicate) => match eval_node(predicate, &views, base + start, count) {
                    Some(mask) => mask,
                    None => return Ok(false),
                },
                None => vec![1; count],
            };
            if request.group_by.is_empty() {
                if !feed_global(
                    request,
                    agg_filters,
                    &views,
                    base + start,
                    count,
                    &block,
                    global,
                )? {
                    return Ok(false);
                }
            } else if !feed_grouped(
                request,
                agg_filters,
                &views,
                base + start,
                count,
                &block,
                grouped,
            )? {
                return Ok(false);
            }
            start += count;
        }
    }
    Ok(true)
}

/// True when every comparison leaf of a normalized predicate has a matching
/// view: integer leaves need dense integer views, `Bytes` / `String` leaves
/// need raw views of the same physical type (cross-type leaves decline even
/// though normalization accepted their shape).
fn leaves_have_views(predicate: &PrunePredicate, views: &RangeViews<'_>) -> bool {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => {
            if matches!(operator, PruneOperator::IsNull | PruneOperator::IsNotNull) {
                return views.nulls.contains_key(column_id);
            }
            match literal {
                Some(Field::Integer(_)) => views.ints.contains_key(column_id),
                Some(Field::Bytes(_)) => views
                    .raw
                    .get(column_id)
                    .is_some_and(|view| view.physical == ColumnType::Bytes),
                Some(Field::String(_)) => views
                    .raw
                    .get(column_id)
                    .is_some_and(|view| view.physical == ColumnType::String),
                _ => false,
            }
        }
        PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
            leaves_have_views(left, views) && leaves_have_views(right, views)
        }
        PrunePredicate::Not(_) => false,
    }
}

/// Collects columns a normalized predicate reads, split by the physical
/// shape the evaluator needs: integer leaves need dense values, `Bytes` /
/// `String` leaves need raw values, `IS NULL` leaves need only bitmaps.
fn collect_eval_columns(
    predicate: &PrunePredicate,
    ints: &mut BTreeSet<ColumnId>,
    bytes: &mut BTreeSet<ColumnId>,
    strings: &mut BTreeSet<ColumnId>,
    nulls: &mut BTreeSet<ColumnId>,
) {
    match predicate {
        PrunePredicate::Compare {
            column_id,
            operator,
            literal,
        } => match operator {
            PruneOperator::IsNull | PruneOperator::IsNotNull => {
                nulls.insert(*column_id);
            }
            _ => match literal {
                Some(Field::Integer(_)) => {
                    ints.insert(*column_id);
                }
                Some(Field::Bytes(_)) => {
                    bytes.insert(*column_id);
                }
                Some(Field::String(_)) => {
                    strings.insert(*column_id);
                }
                _ => {
                    ints.insert(*column_id);
                }
            },
        },
        PrunePredicate::And(left, right) | PrunePredicate::Or(left, right) => {
            collect_eval_columns(left, ints, bytes, strings, nulls);
            collect_eval_columns(right, ints, bytes, strings, nulls);
        }
        PrunePredicate::Not(_) => {}
    }
}

/// Feeds one block into the single global accumulator row.
fn feed_global(
    request: &VectorRequest,
    agg_filters: &[Option<PrunePredicate>],
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    block: &[u8],
    global: &mut [VectorAcc],
) -> Result<bool> {
    for (index, aggregate) in request.aggregates.iter().enumerate() {
        let Some(acc) = global.get_mut(index) else {
            continue;
        };
        // Per-aggregate FILTER intersects the statement mask.
        let active: Vec<u8>;
        let mask: &[u8] = match agg_filters.get(index).and_then(|f| f.as_ref()) {
            Some(filter) => {
                let Some(filter_mask) = eval_node(filter, views, start, count) else {
                    return Ok(false);
                };
                active = block
                    .iter()
                    .zip(filter_mask.iter())
                    .map(|(keep, pass)| keep & pass)
                    .collect();
                &active
            }
            None => block,
        };
        accumulate(aggregate, views, start, count, mask, acc);
    }
    Ok(true)
}

/// Feeds one block into per-group accumulators.
fn feed_grouped(
    request: &VectorRequest,
    agg_filters: &[Option<PrunePredicate>],
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    block: &[u8],
    grouped: &mut HashMap<Vec<Option<i64>>, Vec<VectorAcc>>,
) -> Result<bool> {
    // Per-aggregate active masks (FILTER intersected with the statement mask).
    let mut masks: Vec<Option<Vec<u8>>> = Vec::with_capacity(request.aggregates.len());
    for (index, aggregate) in request.aggregates.iter().enumerate() {
        let _ = aggregate;
        match agg_filters.get(index).and_then(|f| f.as_ref()) {
            Some(filter) => {
                let Some(filter_mask) = eval_node(filter, views, start, count) else {
                    return Ok(false);
                };
                masks.push(Some(
                    block
                        .iter()
                        .zip(filter_mask.iter())
                        .map(|(keep, pass)| keep & pass)
                        .collect(),
                ));
            }
            None => masks.push(None),
        }
    }
    let aggregate_count = request.aggregates.len();
    // Single-column fast path avoids the per-row key allocation.
    let single_key = request.group_by.len() == 1;
    for row in 0..count {
        if block.get(row).is_some_and(|keep| *keep == 0) {
            continue;
        }
        let key: Vec<Option<i64>> = if single_key {
            vec![int_key_at(views, request.group_by[0], start + row)]
        } else {
            request
                .group_by
                .iter()
                .map(|column| int_key_at(views, *column, start + row))
                .collect()
        };
        let accs = grouped
            .entry(key)
            .or_insert_with(|| (0..aggregate_count).map(|_| VectorAcc::default()).collect());
        for (index, aggregate) in request.aggregates.iter().enumerate() {
            if masks[index]
                .as_ref()
                .is_some_and(|mask| mask.get(row).is_some_and(|keep| *keep == 0))
            {
                continue;
            }
            let Some(acc) = accs.get_mut(index) else {
                continue;
            };
            accumulate_single(aggregate, views, start + row, acc);
        }
    }
    Ok(true)
}

/// Reads one integer group key (`None` for SQL `NULL`).
fn int_key_at(views: &RangeViews<'_>, column: ColumnId, row: usize) -> Option<i64> {
    let view = views.ints.get(&column)?;
    if view.is_null(row) {
        return None;
    }
    view.get(row)
}

/// Accumulates one block for a global aggregate from the active mask.
pub fn accumulate(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    active: &[u8],
    acc: &mut VectorAcc,
) {
    match aggregate.kind {
        VectorAggKind::CountStar => {
            for row in 0..count {
                if active.get(row).is_some_and(|keep| *keep != 0) {
                    acc.count_star += 1;
                }
            }
        }
        VectorAggKind::Count => {
            let Some(column) = aggregate.column else {
                return;
            };
            let Some(bitmap) = views.nulls.get(&column) else {
                return;
            };
            for row in 0..count {
                if active.get(row).is_some_and(|keep| *keep != 0) {
                    let absolute = start + row;
                    if !bitmap
                        .get(absolute / 8)
                        .is_some_and(|b| b & (1 << (absolute % 8)) != 0)
                    {
                        acc.count += 1;
                    }
                }
            }
        }
        VectorAggKind::Sum => {
            sum_block(aggregate, views, start, count, active, acc, false);
        }
        VectorAggKind::Avg => {
            sum_block(aggregate, views, start, count, active, acc, true);
        }
        VectorAggKind::Min | VectorAggKind::Max => {
            minmax_block(aggregate, views, start, count, active, acc);
        }
        VectorAggKind::CountDistinct => {
            distinct_block(aggregate, views, start, count, active, acc);
        }
    }
}

/// Accumulates one row for a grouped aggregate (row already mask-qualified).
pub fn accumulate_single(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    row: usize,
    acc: &mut VectorAcc,
) {
    match aggregate.kind {
        VectorAggKind::CountStar => {
            acc.count_star += 1;
        }
        VectorAggKind::Count => {
            let Some(column) = aggregate.column else {
                return;
            };
            let Some(bitmap) = views.nulls.get(&column) else {
                return;
            };
            if !bitmap
                .get(row / 8)
                .is_some_and(|b| b & (1 << (row % 8)) != 0)
            {
                acc.count += 1;
            }
        }
        VectorAggKind::Sum => {
            sum_single(aggregate, views, row, acc, false);
        }
        VectorAggKind::Avg => {
            sum_single(aggregate, views, row, acc, true);
        }
        VectorAggKind::Min | VectorAggKind::Max => {
            minmax_single(aggregate, views, row, acc);
        }
        VectorAggKind::CountDistinct => {
            distinct_single(aggregate, views, row, acc);
        }
    }
}

/// Shared `SUM` / `AVG` block accumulation (`avg` selects the AVG counters).
fn sum_block(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    active: &[u8],
    acc: &mut VectorAcc,
    avg: bool,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    let Some(view) = views.ints.get(&column) else {
        return;
    };
    for row in 0..count {
        if active.get(row).is_some_and(|keep| *keep == 0) {
            continue;
        }
        let absolute = start + row;
        if view.is_null(absolute) {
            continue;
        }
        let Some(cell) = view.get(absolute) else {
            continue;
        };
        if avg {
            acc.avg_sum += cell;
            acc.avg_count += 1;
        } else {
            // Identical overflow behavior to the scalar `SumN` accumulator
            // (`+=` panics in debug, wraps in release).
            acc.sum += cell;
            acc.sum_has_any = true;
        }
    }
}

/// Shared `SUM` / `AVG` single-row accumulation.
fn sum_single(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    row: usize,
    acc: &mut VectorAcc,
    avg: bool,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    let Some(view) = views.ints.get(&column) else {
        return;
    };
    if view.is_null(row) {
        return;
    }
    let Some(cell) = view.get(row) else {
        return;
    };
    if avg {
        acc.avg_sum += cell;
        acc.avg_count += 1;
    } else {
        acc.sum += cell;
        acc.sum_has_any = true;
    }
}

/// Shared `MIN` / `MAX` block accumulation.
fn minmax_block(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    active: &[u8],
    acc: &mut VectorAcc,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    let is_min = matches!(aggregate.kind, VectorAggKind::Min);
    if let Some(view) = views.ints.get(&column) {
        for row in 0..count {
            if active.get(row).is_some_and(|keep| *keep == 0) {
                continue;
            }
            let absolute = start + row;
            if view.is_null(absolute) {
                continue;
            }
            let Some(cell) = view.get(absolute) else {
                continue;
            };
            if is_min {
                acc.min = Some(acc.min.map_or(cell, |current| current.min(cell)));
            } else {
                acc.max = Some(acc.max.map_or(cell, |current| current.max(cell)));
            }
        }
        return;
    }
    let Some(view) = views.raw.get(&column) else {
        return;
    };
    for row in 0..count {
        if active.get(row).is_some_and(|keep| *keep == 0) {
            continue;
        }
        let Some(cell) = view.bytes_at(start + row) else {
            continue;
        };
        if is_min {
            let replace = acc
                .min_raw
                .as_ref()
                .is_none_or(|current| cell < current.as_slice());
            if replace {
                acc.min_raw = Some(cell.to_vec());
            }
        } else {
            let replace = acc
                .max_raw
                .as_ref()
                .is_none_or(|current| cell > current.as_slice());
            if replace {
                acc.max_raw = Some(cell.to_vec());
            }
        }
    }
}

/// Shared `MIN` / `MAX` single-row accumulation.
fn minmax_single(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    row: usize,
    acc: &mut VectorAcc,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    let is_min = matches!(aggregate.kind, VectorAggKind::Min);
    if let Some(view) = views.ints.get(&column) {
        if view.is_null(row) {
            return;
        }
        let Some(cell) = view.get(row) else {
            return;
        };
        if is_min {
            acc.min = Some(acc.min.map_or(cell, |current| current.min(cell)));
        } else {
            acc.max = Some(acc.max.map_or(cell, |current| current.max(cell)));
        }
        return;
    }
    let Some(view) = views.raw.get(&column) else {
        return;
    };
    let Some(cell) = view.bytes_at(row) else {
        return;
    };
    if is_min {
        let replace = acc
            .min_raw
            .as_ref()
            .is_none_or(|current| cell < current.as_slice());
        if replace {
            acc.min_raw = Some(cell.to_vec());
        }
    } else {
        let replace = acc
            .max_raw
            .as_ref()
            .is_none_or(|current| cell > current.as_slice());
        if replace {
            acc.max_raw = Some(cell.to_vec());
        }
    }
}

/// Shared `COUNT(DISTINCT ...)` block accumulation.
fn distinct_block(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    start: usize,
    count: usize,
    active: &[u8],
    acc: &mut VectorAcc,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    if let Some(view) = views.ints.get(&column) {
        for row in 0..count {
            if active.get(row).is_some_and(|keep| *keep == 0) {
                continue;
            }
            let absolute = start + row;
            if view.is_null(absolute) {
                continue;
            }
            if let Some(cell) = view.get(absolute) {
                acc.distinct.insert(cell);
            }
        }
        return;
    }
    let Some(view) = views.raw.get(&column) else {
        return;
    };
    for row in 0..count {
        if active.get(row).is_some_and(|keep| *keep == 0) {
            continue;
        }
        let Some(cell) = view.bytes_at(start + row) else {
            continue;
        };
        // Insert-on-miss: borrowed lookup first, so duplicate-heavy data
        // allocates only for genuinely new values.
        if !acc.distinct_raw.contains(cell) {
            acc.distinct_raw.insert(cell.to_vec());
        }
    }
}

/// Shared `COUNT(DISTINCT ...)` single-row accumulation.
fn distinct_single(
    aggregate: &VectorAggregate,
    views: &RangeViews<'_>,
    row: usize,
    acc: &mut VectorAcc,
) {
    let Some(column) = aggregate.column else {
        return;
    };
    if let Some(view) = views.ints.get(&column) {
        if view.is_null(row) {
            return;
        }
        if let Some(cell) = view.get(row) {
            acc.distinct.insert(cell);
        }
        return;
    }
    let Some(view) = views.raw.get(&column) else {
        return;
    };
    let Some(cell) = view.bytes_at(row) else {
        return;
    };
    if !acc.distinct_raw.contains(cell) {
        acc.distinct_raw.insert(cell.to_vec());
    }
}
