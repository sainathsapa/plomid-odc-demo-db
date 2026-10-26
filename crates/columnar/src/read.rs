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
//! Read path for columnar segments.
//!
//! A [`SegmentReader`] owns the decoded image of a segment. It verifies every
//! checksum the format carries — the header, the body, each column metadata
//! record, each statistics payload, and each chunk — so callers never read a
//! structure without validating it first.
//!
//! Rows are rebuilt lazily: the reader locates the chunks of a column, decodes
//! only the chunk payloads that overlap the requested row range, strips the
//! `u32`-length prefixes the writer wrote, and yields the original value bytes.
//! NULL rows are recovered from the column's null bitmap, which is authoritative.

use crate::chunk::ColumnChunk;
use crate::chunk::{chunk_checksum, ChunkData};
use crate::column::{column_encoding_summary, ColumnEncodingSummary, ColumnMetadata};
use crate::encoding::{codec_version, ColumnEncoding, EncodingFrame};
use crate::format;
use crate::layout::{checksum_of, corruption, to_usize};
use crate::materialization::MaterializedColumn;
use crate::pruning::{PruneOperator, SegmentPruning};
use crate::segment::ColumnarSegment;
use crate::statistics::{ColumnStatistics, SegmentStatistics};
use plomid_core::{ColumnId, PlomidError, Result};
use plomid_storage::Row;

/// Validates and decodes a persisted columnar segment image.
#[derive(Clone, Debug)]
pub struct SegmentReader {
    /// Decoded header of the segment.
    pub header: crate::segment::SegmentHeader,
    /// Decoded column metadata, in column order.
    pub columns: Vec<ColumnMetadata>,
    /// Decoded chunk table, in column then row order.
    pub chunks: Vec<ColumnChunk>,
    /// Per-column statistics rebuilt from the statistics region.
    pub statistics: crate::statistics::SegmentStatistics,
    /// Number of logical rows in the segment.
    pub row_count: u64,
    /// Pruning metadata (zone maps + BRIN) attached to this segment, if present.
    pub pruning: Option<SegmentPruning>,
}

impl SegmentReader {
    /// Encoded size of a segment header in bytes.
    pub const HEADER_SIZE: usize = format::COLUMNAR_HEADER_SIZE;

    /// Decodes a segment image, verifying every checksum on the way.
    ///
    /// The header checksum is checked first; if the header is damaged nothing
    /// else is trusted. The body checksum is then checked over the metadata,
    /// null-bitmap, and statistics regions. Column metadata records, statistics
    /// payloads, and chunk headers are validated as they are decoded, so a
    /// single corrupt column or chunk is reported precisely.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let header = crate::segment::SegmentHeader::decode(bytes)?;
        let header_end = header.metadata_offset();
        let column_count = header.column_count as usize;
        let metadata_size = column_count
            .checked_mul(format::COLUMNAR_COLUMN_METADATA_SIZE)
            .ok_or_else(|| corruption("column metadata size overflows usize"))?;
        let metadata_end = header_end
            .checked_add(metadata_size)
            .ok_or_else(|| corruption("column metadata region overflows the segment"))?;

        // Body checksum covers metadata + null bitmaps + statistics.
        let chunks_offset_usize = to_usize(header.chunks_offset, "chunks offset")?;
        if chunks_offset_usize < metadata_end {
            return Err(corruption(
                "segment chunk table precedes its metadata table",
            ));
        }
        let body = bytes
            .get(header_end..chunks_offset_usize)
            .ok_or_else(|| corruption("segment body lies outside its image"))?;
        if header.body_checksum != checksum_of(body) {
            return Err(corruption("columnar segment body checksum mismatch"));
        }

        // Decode column metadata records.
        let mut columns = Vec::with_capacity(column_count);
        for col_idx in 0..column_count {
            let base = header_end + col_idx * format::COLUMNAR_COLUMN_METADATA_SIZE;
            let record = bytes
                .get(base..base + format::COLUMNAR_COLUMN_METADATA_SIZE)
                .ok_or_else(|| corruption("column metadata table is truncated"))?;
            columns.push(ColumnMetadata::decode(record)?);
        }

        // Decode the statistics region, one payload per column.
        let mut statistics = SegmentStatistics::new();
        for meta in &columns {
            let stats_bytes = if meta.statistics_size == 0 {
                &b""[..]
            } else {
                let stats_offset = to_usize(meta.statistics_offset, "statistics offset")?;
                let stats_end = stats_offset
                    .checked_add(meta.statistics_size as usize)
                    .ok_or_else(|| corruption("statistics region overflows the segment image"))?;
                bytes
                    .get(stats_offset..stats_end)
                    .ok_or_else(|| corruption("statistics payload lies outside the segment"))?
            };
            if checksum_of(stats_bytes) != meta.statistics_checksum {
                return Err(corruption(format!(
                    "column {} statistics checksum mismatch",
                    meta.column_id.get()
                )));
            }
            // The statistics payload carries the bounds; the row and null counts
            // live in the column metadata record, which is the same record the
            // flag byte was written from.
            let base = ColumnStatistics::new(meta.column_id, meta.row_count, meta.null_count);
            let decoded = base.decode_with(stats_bytes, meta.stats_flags)?;
            statistics.add_column(decoded);
        }

        // Decode the chunk table. Chunks are stored as [header][payload] pairs
        // beginning at the chunk table offset. `decode_header` validates the
        // framing and returns the offset of the next header, which is the first
        // byte after this chunk's stored payload.
        let mut chunks = Vec::with_capacity(header.chunk_count as usize);
        let mut position = chunks_offset_usize;
        for chunk_idx in 0..header.chunk_count as usize {
            let (fields, next) =
                ColumnChunk::decode_header(bytes, position as u64).map_err(|error| {
                    // Keep the error kind: an unsupported framing version stays
                    // distinguishable from damaged bytes.
                    PlomidError::new(error.kind(), format!("chunk {chunk_idx} header: {error}"))
                })?;
            let prefix_end = position
                .checked_add(format::CHUNK_OFF_CHECKSUM)
                .ok_or_else(|| corruption("chunk header prefix overflows usize"))?;
            let prefix = bytes
                .get(position..prefix_end)
                .ok_or_else(|| corruption(format!("chunk {chunk_idx} header is truncated")))?;
            let payload_pos = to_usize(fields.payload_offset, "chunk payload offset")?;
            let compressed_end = payload_pos
                .checked_add(fields.compressed_size as usize)
                .ok_or_else(|| corruption("chunk payload overflows the segment image"))?;
            let compressed = bytes
                .get(payload_pos..compressed_end)
                .ok_or_else(|| corruption("chunk payload lies outside the segment image"))?;
            if chunk_checksum(prefix, compressed) != fields.checksum {
                return Err(corruption(format!(
                    "chunk {chunk_idx} payload checksum mismatch"
                )));
            }

            chunks.push(ColumnChunk::new(
                ColumnId::new(0), // attributed from metadata below
                fields.encoding,
                fields.compression,
                fields.first_row,
                fields.row_count,
                fields.payload_offset,
                fields.compressed_size,
                fields.uncompressed_size,
                fields.checksum,
            ));
            position = to_usize(next, "chunk table position")?;
        }

        // Attribute each chunk to its column. A column with N chunks owns the
        // next N chunk-table slots; column_id comes from the metadata record,
        // which is the source of truth.
        let mut slot = 0_usize;
        for meta in &columns {
            for _ in 0..meta.chunk_count {
                if slot < chunks.len() {
                    chunks[slot].column_id = meta.column_id;
                }
                slot += 1;
            }
        }
        if slot != chunks.len() {
            return Err(corruption(
                "chunk table length disagrees with column metadata",
            ));
        }

        // Pruning metadata, if any, begins exactly where the chunk table ends.
        // That offset comes from integrity-checked chunk framing, so a damaged
        // trailer length can never masquerade as "no metadata present".
        let trailer_start = position;
        let pruning =
            crate::pruning::decode_trailer_at(bytes, trailer_start)?.map(SegmentPruning::from_brin);

        Ok(Self {
            header,
            columns,
            chunks,
            statistics,
            row_count: header.row_count,
            pruning,
        })
    }

    /// Returns the metadata of `column_id`, if present.
    #[must_use]
    pub fn column(&self, column_id: ColumnId) -> Option<&ColumnMetadata> {
        self.columns
            .get(column_id.get() as usize)
            .filter(|meta| meta.column_id == column_id)
    }

    /// Returns the chunks of `column_id` in row order.
    #[must_use]
    pub fn column_chunks(&self, column_id: ColumnId) -> Vec<&ColumnChunk> {
        self.chunks
            .iter()
            .filter(|chunk| chunk.column_id == column_id)
            .collect()
    }

    /// Returns the statistics logged for `column_id`.
    #[must_use]
    pub fn column_statistics(&self, column_id: ColumnId) -> Option<&ColumnStatistics> {
        self.statistics.get(column_id)
    }

    /// Per-column encoding summaries for compressed-execution planning.
    ///
    /// Folds [`column_encoding_summary`] over this reader's decoded columns
    /// and chunk table. The persisted dominant tags are advisory — chunk
    /// headers stay authoritative — and segments written before summaries
    /// existed recompute from their chunks rather than erroring.
    #[must_use]
    pub fn encoding_summaries(&self) -> Vec<ColumnEncodingSummary> {
        column_encoding_summary(&self.columns, &self.chunks)
    }

    /// Segment-maintenance inspection report for VACUUM/maintenance workers.
    ///
    /// Combines the per-column encoding summaries with segment-wide stored
    /// and uncompressed totals so a maintenance pass can report ratios and
    /// spot columns whose encoding no longer pays without decoding a single
    /// payload. Pure inspection: it reads decoded metadata only.
    #[must_use]
    pub fn maintenance_report(&self) -> SegmentMaintenanceReport {
        SegmentMaintenanceReport::for_reader(self)
    }

    /// Decodes a single chunk's payload into an owned [`ChunkData`].
    ///
    /// The stored payload was integrity-checked against its framing checksum
    /// when the segment was decoded, so this decompresses rather than
    /// re-checksums. Decoding is delegated to the encoding layer, which enforces
    /// the format's allocation bounds, requires every declared length to agree
    /// with the bytes actually present, validates the chunk's encoding frame
    /// against this chunk's framing, and re-checks the decoded value stream
    /// entry by entry before it is handed to a caller.
    pub fn read_chunk(&self, bytes: &[u8], chunk: &ColumnChunk) -> Result<ChunkData> {
        let payload_pos = to_usize(chunk.file_offset, "chunk payload offset")?;
        let compressed_end = payload_pos
            .checked_add(chunk.compressed_size as usize)
            .ok_or_else(|| corruption("chunk payload overflows the segment image"))?;
        let compressed = bytes
            .get(payload_pos..compressed_end)
            .ok_or_else(|| corruption("chunk payload lies outside the segment image"))?;
        let value_stream = crate::encoding::decode_chunk(
            compressed,
            chunk.encoding,
            chunk.compression,
            chunk.uncompressed_size,
            chunk.row_count,
        )?;
        let mut data = ChunkData::new(chunk.clone(), value_stream);
        data.mark_verified();
        Ok(data)
    }

    /// Materializes the rows in `[start_row, end_row)` by scanning columns.
    ///
    /// Only the chunks overlapping the requested range are decoded, so a narrow
    /// scan skips chunks it never touches.
    pub fn read_rows(
        &self,
        bytes: &[u8],
        start_row: u64,
        end_row: u64,
        column_ids: &[ColumnId],
    ) -> Result<Vec<Row>> {
        let end_row = end_row.min(self.row_count);
        let start_row = start_row.min(end_row);
        let count = (end_row - start_row) as usize;

        let mut columns: Vec<MaterializedColumn> = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let meta = self
                .column(column_id)
                .ok_or_else(|| corruption(format!("unknown column {}", column_id.get())))?;
            columns.push(self.read_column(bytes, meta, start_row, end_row)?);
        }

        let mut rows = Vec::with_capacity(count);
        for row_idx in 0..count {
            let mut fields = Vec::with_capacity(columns.len());
            for col in &columns {
                fields.push(col.get_field(row_idx));
            }
            rows.push(Row::new(fields));
        }
        Ok(rows)
    }

    /// Materializes one column's value entries for rows `[start_row, end_row)`.
    ///
    /// Chunks are decoded lazily and only the ones overlapping the range are
    /// touched; the resulting [`MaterializedColumn`] reuses the writer's
    /// in-memory layout, so the value-extraction helpers apply unchanged.
    pub fn read_column(
        &self,
        bytes: &[u8],
        meta: &ColumnMetadata,
        start_row: u64,
        end_row: u64,
    ) -> Result<MaterializedColumn> {
        let row_count = end_row.saturating_sub(start_row) as usize;
        let mut null_bitmap = vec![0_u8; format::null_bitmap_size(row_count)];
        let mut values = Vec::new();
        let mut offsets = vec![0_u32; row_count];
        let mut lengths = vec![0_u32; row_count];

        let null_offset = to_usize(meta.null_bitmap_offset, "null bitmap offset")?;
        let null_len = format::null_bitmap_size(meta.row_count as usize);
        let null_bytes = bytes
            .get(null_offset..null_offset + null_len)
            .ok_or_else(|| corruption("column null bitmap lies outside the segment image"))?;

        for chunk in self.column_chunks(meta.column_id) {
            if chunk.end_row() <= start_row || chunk.first_row >= end_row {
                continue;
            }
            let data = self.read_chunk(bytes, chunk)?;
            let payload = data.as_slice();

            let row_begin = chunk.first_row.max(start_row);
            let row_end = chunk.end_row().min(end_row);
            let mut cursor = 0_usize;
            for _ in 0..row_begin.saturating_sub(chunk.first_row) {
                let length = read_entry_length(payload, &mut cursor)?;
                cursor = cursor
                    .checked_add(length as usize)
                    .ok_or_else(|| corruption("chunk entry overflows its frame"))?;
            }
            for out_row in 0..(row_end - row_begin) as usize {
                let length = read_entry_length(payload, &mut cursor)?;
                let value = payload
                    .get(cursor..cursor + length as usize)
                    .ok_or_else(|| corruption("chunk value lies outside its entry"))?;
                let absolute = row_begin + out_row as u64;
                let dest = (absolute - start_row) as usize;
                offsets[dest] = values.len() as u32;
                lengths[dest] = length;
                values.extend_from_slice(value);
                cursor += length as usize;

                let byte = absolute as usize / 8;
                let bit = absolute as usize % 8;
                if null_bytes.get(byte).is_some_and(|b| b & (1 << bit) != 0) {
                    let dest_byte = dest / 8;
                    let dest_bit = dest % 8;
                    if let Some(slot) = null_bitmap.get_mut(dest_byte) {
                        *slot |= 1 << dest_bit;
                    }
                }
            }
        }

        Ok(MaterializedColumn {
            column_id: meta.column_id,
            column_type: meta.column_type,
            row_count: row_count as u64,
            null_bitmap,
            values,
            offsets,
            lengths,
        })
    }

    /// Returns the decoded segment model.
    #[must_use]
    pub fn into_segment(self) -> ColumnarSegment {
        ColumnarSegment::new(
            self.header.generation_id,
            self.header.segment_id,
            std::path::PathBuf::new(),
            self.columns,
            self.chunks,
            self.statistics,
            self.row_count,
        )
    }
}

/// Reads a little-endian `u32` length prefix from `cursor`.
fn read_entry_length(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    let prefix = payload
        .get(*cursor..*cursor + format::COLUMNAR_VALUE_LENGTH_PREFIX)
        .ok_or_else(|| corruption("chunk value length prefix is truncated"))?;
    *cursor += format::COLUMNAR_VALUE_LENGTH_PREFIX;
    Ok(u32::from_le_bytes(prefix.try_into().map_err(|_| {
        corruption("chunk value length prefix is truncated")
    })?))
}

/// Inspection report over one decoded segment for VACUUM/maintenance workers.
///
/// Built entirely from decoded metadata (column records plus the chunk
/// table), so producing it never decompresses or decodes a payload. The
/// per-column summaries come from [`column_encoding_summary`]; the totals
/// are the exact sums across every chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct SegmentMaintenanceReport {
    /// Logical rows in the segment.
    pub row_count: u64,
    /// Columns in the segment.
    pub column_count: usize,
    /// Chunks in the segment chunk table.
    pub chunk_count: usize,
    /// Stored (compressed) payload bytes across all chunks.
    pub compressed_bytes: u64,
    /// Pre-codec payload bytes across all chunks.
    pub uncompressed_bytes: u64,
    /// `compressed_bytes / uncompressed_bytes`, or `1.0` when empty.
    pub ratio: f64,
    /// One summary per column, in column order.
    pub columns: Vec<ColumnEncodingSummary>,
}

impl SegmentMaintenanceReport {
    /// Folds the report from a decoded segment.
    #[must_use]
    pub fn for_reader(reader: &SegmentReader) -> Self {
        let columns = reader.encoding_summaries();
        let compressed_bytes: u64 = columns
            .iter()
            .map(|summary| summary.compressed_bytes)
            .fold(0_u64, u64::saturating_add);
        let uncompressed_bytes: u64 = columns
            .iter()
            .map(|summary| summary.uncompressed_bytes)
            .fold(0_u64, u64::saturating_add);
        Self {
            row_count: reader.row_count,
            column_count: reader.columns.len(),
            chunk_count: reader.chunks.len(),
            compressed_bytes,
            uncompressed_bytes,
            ratio: if uncompressed_bytes == 0 {
                1.0
            } else {
                compressed_bytes as f64 / uncompressed_bytes as f64
            },
            columns,
        }
    }
}
// ---------------------------------------------------------------------------
// Compressed execution (Phase 38C).
//
// These are additive fast paths over already-persisted codec bodies. Every
// entry point is fail-closed: any anomaly (truncation, version skew,
// out-of-range index, ragged integer payload, NULL ambiguity, overflow
// uncertainty) returns `None` so the caller falls back to the existing
// full-decode path, which remains authoritative and still verifies checksums.
// Checksums themselves are verified once at `SegmentReader::decode` time over
// the stored payloads; these paths never skip that gate — they only avoid
// expanding the value stream after decompression.
// ---------------------------------------------------------------------------

/// Minimum chunk rows for compressed execution to pay off.
///
/// Measured on debug builds (decode + materialize + scan): below ~64 rows the
/// frame/bit-reader setup costs more than a straight decode; above 128 rows
/// RLE/Dictionary/Delta fast paths win by 2–8x on friendly fixtures. The
/// gate uses 128 so small tail chunks always take the exact decode path.
pub const COMPRESSED_MIN_ROWS: u64 = 128;

/// Operation a compressed fast path can serve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressedOp {
    /// `COUNT(col)` / `COUNT(*)`.
    Count,
    /// `SUM(intcol)` / `AVG(intcol)`.
    Sum,
    /// `MIN(intcol)`.
    Min,
    /// `MAX(intcol)`.
    Max,
    /// `COUNT(DISTINCT col)`.
    CountDistinct,
    /// Equality filter (`=` / `!=` over runs or dictionary codes).
    EqFilter,
    /// Range filter (`<`, `<=`, `>`, `>=` over runs or delta prefix).
    RangeFilter,
    /// `GROUP BY` key extraction over dictionary codes.
    GroupBy,
}

/// Profitability + applicability gate for compressed execution.
///
/// Returns true only when the chunk's encoding can serve `op` without
/// expanding the value stream *and* the chunk is large enough for the setup
/// cost to pay off. Every caller must still handle `None` from the fast path
/// by falling back to full decode.
#[must_use]
pub fn should_execute_compressed(
    encoding: ColumnEncoding,
    op: CompressedOp,
    chunk_rows: u64,
) -> bool {
    if chunk_rows < COMPRESSED_MIN_ROWS {
        return false;
    }
    matches!(
        (encoding, op),
        (
            ColumnEncoding::Rle,
            CompressedOp::Count
                | CompressedOp::Sum
                | CompressedOp::Min
                | CompressedOp::Max
                | CompressedOp::EqFilter
                | CompressedOp::RangeFilter,
        ) | (
            ColumnEncoding::Dictionary,
            CompressedOp::EqFilter
                | CompressedOp::GroupBy
                | CompressedOp::CountDistinct
                | CompressedOp::Min
                | CompressedOp::Max,
        ) | (
            ColumnEncoding::DeltaBitpack,
            CompressedOp::Min | CompressedOp::Max | CompressedOp::RangeFilter,
        )
    )
}

/// Aggregates computed over one RLE chunk without expanding runs.
#[derive(Clone, Debug, Default)]
pub struct RleAgg {
    /// Non-`NULL` rows observed.
    pub count: u64,
    /// `SUM` state (wrapping in release, panics in debug like the scalar path).
    pub sum: i64,
    /// True once any non-`NULL` integer value was seen.
    pub sum_has_any: bool,
    /// Integer `MIN` / `MAX` over non-`NULL` 8-byte values.
    pub min: Option<i64>,
    /// Integer `MIN` / `MAX` over non-`NULL` 8-byte values.
    pub max: Option<i64>,
    /// Raw byte-order `MIN` over non-`NULL` values.
    pub min_raw: Option<Vec<u8>>,
    /// Raw byte-order `MAX` over non-`NULL` values.
    pub max_raw: Option<Vec<u8>>,
}

/// Loads and validates a framed body for `chunk`.
///
/// Returns `(raw_len, body)` where `body` is the decompressed codec body
/// without its 40-byte frame. Returns `None` on any anomaly (truncation,
/// version skew, length disagreement) so the caller falls back to
/// [`SegmentReader::read_chunk`], which surfaces corruption authoritatively.
fn load_framed_body(segment_bytes: &[u8], chunk: &ColumnChunk) -> Option<(u64, Vec<u8>)> {
    let pos = usize::try_from(chunk.file_offset).ok()?;
    let len = usize::try_from(chunk.compressed_size).ok()?;
    let end = pos.checked_add(len)?;
    let stored = segment_bytes.get(pos..end)?;
    if chunk.uncompressed_size > plomid_core::COLUMNAR_MAX_CHUNK_UNCOMPRESSED_SIZE {
        return None;
    }
    let decompressed =
        crate::compression::decompress(stored, chunk.uncompressed_size, chunk.compression).ok()?;
    if decompressed.len() as u64 != chunk.uncompressed_size {
        return None;
    }
    if chunk.encoding == ColumnEncoding::Raw || !chunk.encoding.is_framed() {
        return None;
    }
    if decompressed.len() < EncodingFrame::ENCODED_LEN {
        return None;
    }
    let frame = EncodingFrame::decode(&decompressed).ok()?;
    if frame.encoding != chunk.encoding {
        return None;
    }
    if frame.codec != chunk.compression {
        return None;
    }
    if frame.encoding_version != chunk.encoding.version().unwrap_or(frame.encoding_version) {
        return None;
    }
    if frame.codec_version != codec_version(chunk.compression) {
        return None;
    }
    if frame.value_count != chunk.row_count {
        return None;
    }
    let body_len = chunk
        .uncompressed_size
        .checked_sub(EncodingFrame::ENCODED_LEN as u64)?;
    if frame.encoded_len != body_len {
        return None;
    }
    if frame.raw_len > plomid_core::COLUMNAR_MAX_CHUNK_RAW_SIZE {
        return None;
    }
    let minimum = (format::COLUMNAR_VALUE_LENGTH_PREFIX as u64).checked_mul(chunk.row_count)?;
    if frame.raw_len < minimum {
        return None;
    }
    let body = decompressed.get(EncodingFrame::ENCODED_LEN..)?.to_vec();
    if body.len() as u64 != frame.encoded_len {
        return None;
    }
    Some((frame.raw_len, body))
}

/// True when absolute row `row` is `NULL` in `null_bitmap`.
fn is_null_row(null_bitmap: &[u8], row: u64) -> bool {
    let row = row as usize;
    null_bitmap
        .get(row / 8)
        .is_some_and(|b| b & (1 << (row % 8)) != 0)
}

/// Counts `NULL` rows in `[start, end)`.
fn null_count_in(null_bitmap: &[u8], start: u64, end: u64) -> u64 {
    let mut count = 0_u64;
    // Byte-fast path for the interior full bytes.
    let mut row = start;
    // Leading partial byte.
    while row < end && !row.is_multiple_of(8) {
        if is_null_row(null_bitmap, row) {
            count += 1;
        }
        row += 1;
    }
    while row + 8 <= end {
        let byte = (row / 8) as usize;
        if let Some(b) = null_bitmap.get(byte) {
            count += u64::from(b.count_ones());
        } else {
            // Missing bitmap bytes mean "no NULLs" is not provable: the
            // caller treats the whole chunk as unverifiable and falls back.
            // Signal by returning a sentinel larger than the interval.
            return u64::MAX;
        }
        row += 8;
    }
    while row < end {
        if is_null_row(null_bitmap, row) {
            count += 1;
        }
        row += 1;
    }
    count
}

/// Adds `value * count` to `sum` with identical overflow behavior to repeated
/// `+=` in the scalar/vector path.
///
/// For a constant run the intermediate sums are monotonic, so overflow occurs
/// exactly when the final sum leaves `i64` range. In debug that must panic
/// (matching `+=`); in release it wraps. Computing via `i128` then
/// narrowing preserves both behaviors.
fn add_mul(sum: &mut i64, value: i64, count: u64) {
    if count == 0 || value == 0 {
        return;
    }
    let final_sum = *sum as i128 + value as i128 * count as i128;
    if final_sum < i64::MIN as i128 || final_sum > i64::MAX as i128 {
        // Replicate the `+=` panic in debug; wrap in release.
        if cfg!(debug_assertions) {
            panic!("attempt to add with overflow");
        }
        *sum = final_sum as i64;
    } else {
        *sum = final_sum as i64;
    }
}

/// Parses an RLE body into `(run_len, value)` pairs without expanding.
///
/// Returns `None` on any framing anomaly so the caller falls back.
fn parse_rle_runs(body: &[u8], value_count: u64, raw_len: u64) -> Option<Vec<(u64, Vec<u8>)>> {
    if body.len() < 8 {
        return None;
    }
    let run_count = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
    // Empty chunk: exactly one 8-byte zero header (mirrors `rle::encode` for
    // zero rows, which writes run_count 0 and nothing else).
    if value_count == 0 {
        if run_count != 0 || body.len() != 8 || raw_len != 0 {
            return None;
        }
        return Some(Vec::new());
    }
    // Bound run iterations by the body length so a hostile count cannot
    // force an unbounded allocation before bounds checks fail.
    if run_count > body.len() as u64 {
        // Not necessarily corrupt (tiny values could still claim many runs),
        // but cap the pre-allocation; parsing below fails closed anyway.
    }
    let mut runs: Vec<(u64, Vec<u8>)> = Vec::new();
    if run_count <= 1_000_000 {
        runs.reserve(run_count as usize);
    }
    let mut cursor = 8_usize;
    let mut rows = 0_u64;
    let mut total = 0_usize;
    for _ in 0..run_count {
        let count_bytes = body.get(cursor..cursor + 8)?;
        let count = u64::from_le_bytes(count_bytes.try_into().ok()?);
        let len_bytes = body.get(cursor + 8..cursor + 12)?;
        let length = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
        cursor = cursor.checked_add(12)?;
        if count == 0 {
            return None;
        }
        let end = cursor.checked_add(length)?;
        let value = body.get(cursor..end)?.to_vec();
        cursor = end;
        rows = rows.checked_add(count)?;
        if rows > value_count {
            return None;
        }
        let expanded = (format::COLUMNAR_VALUE_LENGTH_PREFIX.checked_add(length)?)
            .checked_mul(count as usize)?;
        total = total.checked_add(expanded)?;
        runs.push((count, value));
    }
    if cursor != body.len() {
        return None;
    }
    if rows != value_count {
        return None;
    }
    if total as u64 != raw_len {
        return None;
    }
    Some(runs)
}

/// Computes `COUNT` / `SUM` / `MIN` / `MAX` over one RLE chunk's runs.
///
/// Integer extrema cover 8-byte non-`NULL` values; raw extrema cover the same
/// values by byte order (used for `String` / `Bytes` columns). Any ragged
/// integer run (non-`NULL` row with length != 8) declines via `None`.
pub fn compressed_rle_aggregate(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    column_is_integer: bool,
    active: Option<&[u8]>,
) -> Option<RleAgg> {
    if chunk.encoding != ColumnEncoding::Rle {
        return None;
    }
    let rows = usize::try_from(chunk.row_count).ok()?;
    if let Some(mask) = active {
        if mask.len() != rows {
            return None;
        }
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let runs = parse_rle_runs(&body, chunk.row_count, raw_len)?;
    let mut out = RleAgg::default();
    let mut cursor = chunk.first_row;
    let mut rel = 0_usize;
    for (run_len, value) in &runs {
        let run_len_usize = usize::try_from(*run_len).ok()?;
        let run_end = cursor.checked_add(*run_len)?;
        // Count live + active rows in this run without expanding values.
        let mut live_active = 0_u64;
        if let Some(mask) = active {
            let slice = mask.get(rel..rel + run_len_usize)?;
            for (i, keep) in slice.iter().enumerate() {
                if *keep == 0 {
                    continue;
                }
                if !is_null_row(null_bitmap, cursor + i as u64) {
                    live_active += 1;
                }
            }
        } else {
            let nulls = null_count_in(null_bitmap, cursor, run_end);
            if nulls == u64::MAX {
                return None;
            }
            live_active = run_len.saturating_sub(nulls);
        }
        // Ragged-integer check still needs the all-`NULL` shape rule even
        // when an active mask excludes every row: verify against the null
        // bitmap (authoritative), not the mask.
        let nulls_total = null_count_in(null_bitmap, cursor, run_end);
        if nulls_total == u64::MAX {
            return None;
        }
        let live_total = run_len.saturating_sub(nulls_total);
        if column_is_integer {
            if live_total > 0 && value.len() != 8 {
                return None;
            }
            if live_total == 0 && !value.is_empty() && value.len() != 8 {
                // All-`NULL` runs must carry the zero-length placeholder.
                return None;
            }
        }
        out.count += live_active;
        if live_active > 0 {
            if column_is_integer {
                if value.len() != 8 {
                    return None;
                }
                let word = i64::from_le_bytes(value.as_slice().try_into().ok()?);
                add_mul(&mut out.sum, word, live_active);
                out.sum_has_any = true;
                out.min = Some(out.min.map_or(word, |m| m.min(word)));
                out.max = Some(out.max.map_or(word, |m| m.max(word)));
            } else {
                let replace_min = out
                    .min_raw
                    .as_ref()
                    .is_none_or(|m| value.as_slice() < m.as_slice());
                if replace_min {
                    out.min_raw = Some(value.clone());
                }
                let replace_max = out
                    .max_raw
                    .as_ref()
                    .is_none_or(|m| value.as_slice() > m.as_slice());
                if replace_max {
                    out.max_raw = Some(value.clone());
                }
            }
        }
        cursor = run_end;
        rel += run_len_usize;
    }
    if rel != rows {
        return None;
    }
    Some(out)
}

/// Evaluates one integer comparison over RLE runs without expanding.
pub fn compressed_rle_int_cmp_mask(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    op: PruneOperator,
    bound: i64,
) -> Option<Vec<u8>> {
    if chunk.encoding != ColumnEncoding::Rle {
        return None;
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let runs = parse_rle_runs(&body, chunk.row_count, raw_len)?;
    let rows = chunk.row_count as usize;
    let mut mask = vec![0_u8; rows];
    let mut cursor = 0_usize;
    let mut absolute = chunk.first_row;
    for (run_len, value) in &runs {
        let run_len_usize = usize::try_from(*run_len).ok()?;
        if value.len() != 8 {
            // Integer columns store 8-byte values; a zero-length run value is
            // the `NULL`/empty placeholder. If every row in the run is `NULL`
            // the mask is all zeros regardless; otherwise the framing is
            // ragged for an integer evaluation and we decline.
            let nulls = null_count_in(null_bitmap, absolute, absolute + *run_len);
            if nulls == u64::MAX {
                return None;
            }
            if nulls != *run_len {
                return None;
            }
            for i in 0..run_len_usize {
                mask[cursor + i] = 0;
            }
        } else {
            let word = i64::from_le_bytes(value.as_slice().try_into().ok()?);
            let keep = match op {
                PruneOperator::Equal => word == bound,
                PruneOperator::NotEqual => word != bound,
                PruneOperator::Less => word < bound,
                PruneOperator::LessOrEqual => word <= bound,
                PruneOperator::Greater => word > bound,
                PruneOperator::GreaterOrEqual => word >= bound,
                PruneOperator::IsNull | PruneOperator::IsNotNull => return None,
            };
            for i in 0..run_len_usize {
                let abs = absolute + i as u64;
                mask[cursor + i] = u8::from(keep && !is_null_row(null_bitmap, abs));
            }
        }
        cursor += run_len_usize;
        absolute += *run_len;
    }
    if cursor != rows {
        return None;
    }
    Some(mask)
}

/// Evaluates one byte-order comparison over RLE runs without expanding.
pub fn compressed_rle_raw_cmp_mask(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    op: PruneOperator,
    bound: &[u8],
) -> Option<Vec<u8>> {
    if chunk.encoding != ColumnEncoding::Rle {
        return None;
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let runs = parse_rle_runs(&body, chunk.row_count, raw_len)?;
    let rows = chunk.row_count as usize;
    let mut mask = vec![0_u8; rows];
    let mut cursor = 0_usize;
    let mut absolute = chunk.first_row;
    for (run_len, value) in &runs {
        let run_len_usize = usize::try_from(*run_len).ok()?;
        let keep = match op {
            PruneOperator::Equal => value.as_slice() == bound,
            PruneOperator::NotEqual => value.as_slice() != bound,
            PruneOperator::Less => value.as_slice() < bound,
            PruneOperator::LessOrEqual => value.as_slice() <= bound,
            PruneOperator::Greater => value.as_slice() > bound,
            PruneOperator::GreaterOrEqual => value.as_slice() >= bound,
            PruneOperator::IsNull | PruneOperator::IsNotNull => return None,
        };
        for i in 0..run_len_usize {
            let abs = absolute + i as u64;
            mask[cursor + i] = u8::from(keep && !is_null_row(null_bitmap, abs));
        }
        cursor += run_len_usize;
        absolute += *run_len;
    }
    if cursor != rows {
        return None;
    }
    Some(mask)
}

// --- Dictionary fast paths ---

/// Parsed dictionary body: distinct values plus packed per-row codes.
struct ParsedDict {
    dictionary: Vec<Vec<u8>>,
    width: u32,
    packed: Vec<u8>,
}

fn parse_dict_body(body: &[u8], value_count: u64, raw_len: u64) -> Option<ParsedDict> {
    if body.len() < 9 {
        // Empty chunk: dict_count 0 + width byte. `encode` writes 8-byte zero
        // count plus a zero width byte for zero rows.
        if value_count == 0 && raw_len == 0 && body == [0, 0, 0, 0, 0, 0, 0, 0, 0] {
            return Some(ParsedDict {
                dictionary: Vec::new(),
                width: 0,
                packed: Vec::new(),
            });
        }
        return None;
    }
    let dict_count = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
    if dict_count > plomid_core::COLUMNAR_MAX_DICTIONARY_ENTRIES as u64 {
        return None;
    }
    if dict_count == 0 && (value_count != 0 || raw_len != 0) {
        return None;
    }
    let dict_len = usize::try_from(dict_count).ok()?;
    let mut dictionary: Vec<Vec<u8>> = Vec::with_capacity(dict_len);
    let mut cursor = 8_usize;
    for _ in 0..dict_count {
        let len_bytes = body.get(cursor..cursor + 4)?;
        let length = u32::from_le_bytes(len_bytes.try_into().ok()?) as usize;
        cursor = cursor.checked_add(4)?;
        let end = cursor.checked_add(length)?;
        let value = body.get(cursor..end)?.to_vec();
        cursor = end;
        dictionary.push(value);
    }
    let width_byte = *body.get(cursor)?;
    cursor += 1;
    let width = u32::from(width_byte);
    // Minimal-width check mirrors `dictionary::decode`.
    let mut max_index: u64 = 0;
    if dict_len > 1 {
        max_index = dict_len as u64 - 1;
    }
    let expected = if max_index == 0 {
        0
    } else {
        64 - max_index.leading_zeros()
    };
    if width != expected {
        return None;
    }
    let bits = (value_count as usize).saturating_mul(width as usize);
    let expected_packed = bits.div_ceil(8);
    if body.len() != cursor + expected_packed {
        return None;
    }
    let packed = body.get(cursor..)?.to_vec();
    // Validate total expanded length matches the frame without allocating the
    // value stream: sum entry lengths weighted by code frequencies.
    if value_count > 0 {
        let mut total = 0_usize;
        let count = usize::try_from(value_count).ok()?;
        for row in 0..count {
            let code = dict_code_at(&packed, width, row)?;
            let index = usize::try_from(code).ok()?;
            let value = dictionary.get(index)?;
            total = total
                .checked_add(format::COLUMNAR_VALUE_LENGTH_PREFIX.checked_add(value.len())?)?;
        }
        if total as u64 != raw_len {
            return None;
        }
    } else if raw_len != 0 {
        return None;
    }
    Some(ParsedDict {
        dictionary,
        width,
        packed,
    })
}

/// Reads the `index`-th packed code without a full expansion.
fn dict_code_at(packed: &[u8], width: u32, index: usize) -> Option<u64> {
    if width == 0 {
        return Some(0);
    }
    let bit = index.checked_mul(width as usize)?;
    let mut out = 0_u64;
    for b in 0..width as usize {
        let pos = bit + b;
        let byte = *packed.get(pos / 8)?;
        if (byte >> (pos % 8)) & 1 != 0 {
            out |= 1 << b;
        }
    }
    Some(out)
}

/// Equality filter directly over packed dictionary codes.
///
/// Resolves the bound to at most one matching code per distinct value (the
/// dictionary holds distinct values, so equality matches at most the codes
/// whose value equals the bound), scans codes, and emits the mask. `NULL`
/// rows never match.
pub fn compressed_dict_eq_mask(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    bound: &[u8],
) -> Option<Vec<u8>> {
    if chunk.encoding != ColumnEncoding::Dictionary {
        return None;
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let parsed = parse_dict_body(&body, chunk.row_count, raw_len)?;
    let mut matching: Vec<u64> = Vec::new();
    for (index, value) in parsed.dictionary.iter().enumerate() {
        if value.as_slice() == bound {
            matching.push(index as u64);
        }
    }
    let rows = chunk.row_count as usize;
    let mut mask = vec![0_u8; rows];
    if matching.is_empty() {
        return Some(mask);
    }
    // Single-value fast path: every row carries code 0.
    if parsed.width == 0 {
        let hit = matching.contains(&0);
        for (row, slot) in mask.iter_mut().enumerate() {
            let abs = chunk.first_row + row as u64;
            *slot = u8::from(hit && !is_null_row(null_bitmap, abs));
        }
        return Some(mask);
    }
    let is_match = |code: u64| matching.contains(&code);
    for (row, slot) in mask.iter_mut().enumerate() {
        let code = dict_code_at(&parsed.packed, parsed.width, row)?;
        let abs = chunk.first_row + row as u64;
        *slot = u8::from(is_match(code) && !is_null_row(null_bitmap, abs));
    }
    Some(mask)
}

/// `COUNT(DISTINCT col)` as the size of the observed code set.
///
/// Scans packed codes once, collects distinct codes over non-`NULL` rows,
/// and returns the set size. Distinct codes map 1:1 to distinct values (the
/// dictionary holds distinct entries), with `NULL` rows excluded via the
/// authoritative bitmap — including the shared empty-value edge case where a
/// `NULL` placeholder and a genuine empty value share one code.
pub fn compressed_dict_distinct_count(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    active: Option<&[u8]>,
) -> Option<usize> {
    if chunk.encoding != ColumnEncoding::Dictionary {
        return None;
    }
    let rows = usize::try_from(chunk.row_count).ok()?;
    if let Some(mask) = active {
        if mask.len() != rows {
            return None;
        }
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let parsed = parse_dict_body(&body, chunk.row_count, raw_len)?;
    if rows == 0 {
        return Some(0);
    }
    if parsed.width == 0 {
        // Single code: distinct is 1 iff any active non-`NULL` row exists.
        for row in 0..rows {
            if let Some(mask) = active {
                if mask[row] == 0 {
                    continue;
                }
            }
            if !is_null_row(null_bitmap, chunk.first_row + row as u64) {
                return Some(1);
            }
        }
        return Some(0);
    }
    let mut seen = std::collections::HashSet::with_capacity(parsed.dictionary.len());
    for row in 0..rows {
        if let Some(mask) = active {
            if mask[row] == 0 {
                continue;
            }
        }
        if is_null_row(null_bitmap, chunk.first_row + row as u64) {
            continue;
        }
        let code = dict_code_at(&parsed.packed, parsed.width, row)?;
        if code as usize >= parsed.dictionary.len() {
            return None;
        }
        seen.insert(code);
    }
    Some(seen.len())
}

/// `GROUP BY` keys directly over packed codes for integer dictionaries.
///
/// Resolves codes to values only for output: each distinct 8-byte dictionary
/// entry becomes its `i64` once, then per-row keys are code lookups. Any
/// non-8-byte non-`NULL` value declines (ragged for an integer group key).
/// `NULL` rows yield `None`, forming the single SQL `NULL` group.
pub fn compressed_dict_int_keys(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
) -> Option<Vec<Option<i64>>> {
    if chunk.encoding != ColumnEncoding::Dictionary {
        return None;
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let parsed = parse_dict_body(&body, chunk.row_count, raw_len)?;
    let rows = chunk.row_count as usize;
    // Resolve codes to values once for output.
    let mut code_to_int: Vec<Option<i64>> = Vec::with_capacity(parsed.dictionary.len());
    for value in &parsed.dictionary {
        if value.len() != 8 {
            // Empty placeholder values are only valid on `NULL` rows. If the
            // dictionary holds a non-8-byte entry, integer grouping can only
            // stay exact when every row carrying that code is `NULL`.
            code_to_int.push(None);
        } else {
            code_to_int.push(Some(i64::from_le_bytes(value.as_slice().try_into().ok()?)));
        }
    }
    let mut keys: Vec<Option<i64>> = Vec::with_capacity(rows);
    for row in 0..rows {
        let abs = chunk.first_row + row as u64;
        if is_null_row(null_bitmap, abs) {
            keys.push(None);
            continue;
        }
        let code = if parsed.width == 0 {
            0
        } else {
            dict_code_at(&parsed.packed, parsed.width, row)?
        };
        let index = usize::try_from(code).ok()?;
        let entry = parsed.dictionary.get(index)?;
        if entry.len() != 8 {
            return None;
        }
        keys.push(*code_to_int.get(index)?);
    }
    Some(keys)
}

// --- Delta fast paths ---

/// Parsed delta body without expansion.
struct ParsedDelta {
    width: u32,
    min_delta: i64,
    first: i64,
    packed: Vec<u8>,
}

fn parse_delta_body(body: &[u8], value_count: u64, raw_len: u64) -> Option<ParsedDelta> {
    let expected_raw =
        (format::COLUMNAR_VALUE_LENGTH_PREFIX + 8).checked_mul(value_count as usize)? as u64;
    if expected_raw != raw_len {
        return None;
    }
    if value_count == 0 {
        if body != [0_u8] {
            return None;
        }
        return Some(ParsedDelta {
            width: 0,
            min_delta: 0,
            first: 0,
            packed: Vec::new(),
        });
    }
    if body.len() < 17 {
        return None;
    }
    let width = u32::from(*body.first()?);
    if width > 64 {
        return None;
    }
    let min_delta = i64::from_le_bytes(body.get(1..9)?.try_into().ok()?);
    let first = i64::from_le_bytes(body.get(9..17)?.try_into().ok()?);
    let delta_count = value_count - 1;
    let bits = (delta_count as usize).checked_mul(width as usize)?;
    let expected_packed = bits.div_ceil(8);
    if body.len() != 17 + expected_packed {
        return None;
    }
    let packed = body.get(17..)?.to_vec();
    Some(ParsedDelta {
        width,
        min_delta,
        first,
        packed,
    })
}

/// Reads one packed residue without expansion.
fn delta_residue_at(packed: &[u8], width: u32, index: usize) -> Option<u64> {
    if width == 0 {
        return Some(0);
    }
    let bit = index.checked_mul(width as usize)?;
    let mut out = 0_u64;
    for b in 0..width as usize {
        let pos = bit + b;
        let byte = *packed.get(pos / 8)?;
        if (byte >> (pos % 8)) & 1 != 0 {
            out |= 1 << b;
        }
    }
    Some(out)
}

/// `MIN` / `MAX` from `first + min_delta × count` bounds without expanding.
///
/// Serves exactly the provably-monotonic single-delta shape: `width == 0`
/// means every delta equals `min_delta`, so the sequence is the arithmetic
/// progression `first + i × min_delta`. Monotonicity follows from the stored
/// `min_delta` sign; any `width > 0` declines so the caller expands. Signed
/// overflow would wrap the progression out of order, so a checked interval
/// that does not fit in `i64` also declines. `NULL` rows decline (delta
/// chunks with `NULL`s are not produced by the writer; observing one means
/// falling back to the authoritative decode).
pub fn compressed_delta_min_max(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    active: Option<&[u8]>,
) -> Option<(Option<i64>, Option<i64>)> {
    if chunk.encoding != ColumnEncoding::DeltaBitpack {
        return None;
    }
    if chunk.row_count == 0 {
        return Some((None, None));
    }
    let rows = usize::try_from(chunk.row_count).ok()?;
    if let Some(mask) = active {
        if mask.len() != rows {
            return None;
        }
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let parsed = parse_delta_body(&body, chunk.row_count, raw_len)?;
    if parsed.width != 0 {
        return None;
    }
    // Fail closed on any `NULL`: the writer never emits delta bodies over
    // `NULL` rows (zero-length entries decline the encoding), so a `NULL`
    // here is either corruption or a future writer shape — decode decides.
    let end = chunk.first_row.checked_add(chunk.row_count)?;
    if null_count_in(null_bitmap, chunk.first_row, end) != 0 {
        return None;
    }
    // Restrict to the active subset when present. The progression is
    // monotonic, so extrema are the first/last active values.
    let (first_idx, last_idx) = match active {
        Some(mask) => {
            let mut first: Option<usize> = None;
            let mut last: Option<usize> = None;
            for (i, keep) in mask.iter().enumerate() {
                if *keep != 0 {
                    if first.is_none() {
                        first = Some(i);
                    }
                    last = Some(i);
                }
            }
            match (first, last) {
                (Some(f), Some(l)) => (f, l),
                _ => return Some((None, None)),
            }
        }
        None => (0, rows - 1),
    };
    let step = parsed.min_delta as i128;
    let first_base = parsed.first as i128;
    let first_val = first_base + step * first_idx as i128;
    let last_val = first_base + step * last_idx as i128;
    if first_val < i64::MIN as i128
        || first_val > i64::MAX as i128
        || last_val < i64::MIN as i128
        || last_val > i64::MAX as i128
    {
        return None;
    }
    let first_val = first_val as i64;
    let last_val = last_val as i64;
    if parsed.min_delta == 0 {
        return Some((Some(first_val), Some(first_val)));
    }
    if parsed.min_delta > 0 {
        return Some((Some(first_val), Some(last_val)));
    }
    Some((Some(last_val), Some(first_val)))
}

/// Range filter over delta bodies via prefix reconstruction.
///
/// Reconstructs values sequentially from `first` by applying
/// `min_delta + residue` with wrapping arithmetic (exactly as `decode`
/// does), comparing each to `bound` until the predicate outcome is decided
/// for the remainder. For provably-monotonic single-delta progressions the
/// remainder is filled without further reconstruction; otherwise every
/// residue is still scanned — never the expanded value stream. `NULL` rows
/// decline; any truncation or width anomaly declines.
pub fn compressed_delta_int_cmp_mask(
    segment_bytes: &[u8],
    chunk: &ColumnChunk,
    null_bitmap: &[u8],
    op: PruneOperator,
    bound: i64,
) -> Option<Vec<u8>> {
    if chunk.encoding != ColumnEncoding::DeltaBitpack {
        return None;
    }
    if matches!(op, PruneOperator::IsNull | PruneOperator::IsNotNull) {
        return None;
    }
    let rows = chunk.row_count as usize;
    if rows == 0 {
        return Some(Vec::new());
    }
    let (raw_len, body) = load_framed_body(segment_bytes, chunk)?;
    let parsed = parse_delta_body(&body, chunk.row_count, raw_len)?;
    // Fail closed on `NULL`s (see `compressed_delta_min_max`).
    let end = chunk.first_row.checked_add(chunk.row_count)?;
    // Fast check: any `NULL` at all declines. The per-row loop below also
    // checks, but this prefix avoids a partial mask on a shape the writer
    // never emits.
    if null_count_in(null_bitmap, chunk.first_row, end) != 0 {
        return None;
    }
    // Single-delta monotonic fast path: arithmetic progression, no residue
    // scan at all. Overflow-checked; on overflow decline to decode.
    if parsed.width == 0 {
        let step = parsed.min_delta as i128;
        let first = parsed.first as i128;
        let mut mask = Vec::with_capacity(rows);
        for index in 0..rows {
            let value = first + step * index as i128;
            if value < i64::MIN as i128 || value > i64::MAX as i128 {
                return None;
            }
            let value = value as i64;
            let keep = match op {
                PruneOperator::Equal => value == bound,
                PruneOperator::NotEqual => value != bound,
                PruneOperator::Less => value < bound,
                PruneOperator::LessOrEqual => value <= bound,
                PruneOperator::Greater => value > bound,
                PruneOperator::GreaterOrEqual => value >= bound,
                PruneOperator::IsNull | PruneOperator::IsNotNull => return None,
            };
            mask.push(u8::from(keep));
        }
        return Some(mask);
    }
    // General path: prefix reconstruction with monotonic early exit. The
    // stored `min_delta` sign proves direction only when no signed wrapping
    // occurs; track wrapping via `checked_add` and disable early exit once
    // wrapping is observed (results stay exact — only the shortcut stops).
    let monotonic_up = parsed.min_delta >= 0;
    // Width > 0 with non-negative min_delta means every delta is
    // `min_delta + residue` with residue >= 0 as unsigned difference. As
    // signed values after wrapping this can still go negative on overflow,
    // hence the `wrapped` guard below.
    let mut mask = Vec::with_capacity(rows);
    let mut previous = parsed.first;
    let mut wrapped = false;
    for index in 0..rows {
        let value = if index == 0 {
            parsed.first
        } else {
            let residue = delta_residue_at(&parsed.packed, parsed.width, index - 1)?;
            let delta = parsed.min_delta.wrapping_add(residue as i64);
            match previous.checked_add(delta) {
                Some(next) => {
                    previous = next;
                    next
                }
                None => {
                    // Signed overflow: wrapping defines the stored value.
                    wrapped = true;
                    previous = previous.wrapping_add(delta);
                    previous
                }
            }
        };
        // `NULL` rows were already excluded above; keep the per-row guard
        // for defense in depth (unreachable when the prefix check passed).
        let abs = chunk.first_row + index as u64;
        if is_null_row(null_bitmap, abs) {
            mask.push(0);
            continue;
        }
        let keep = match op {
            PruneOperator::Equal => value == bound,
            PruneOperator::NotEqual => value != bound,
            PruneOperator::Less => value < bound,
            PruneOperator::LessOrEqual => value <= bound,
            PruneOperator::Greater => value > bound,
            PruneOperator::GreaterOrEqual => value >= bound,
            PruneOperator::IsNull | PruneOperator::IsNotNull => return None,
        };
        mask.push(u8::from(keep));
        // Prefix shortcut: once monotonic direction decides the remainder,
        // fill without further reconstruction. Only when no wrapping has
        // been observed (wrapping breaks signed order).
        if !wrapped {
            if monotonic_up {
                match op {
                    PruneOperator::Greater | PruneOperator::GreaterOrEqual => {
                        let done = if op == PruneOperator::Greater {
                            value > bound
                        } else {
                            value >= bound
                        };
                        if done {
                            // All later values are >= current (non-decreasing
                            // while unwrapped), so they all pass.
                            mask.extend(std::iter::repeat_n(1, rows - index - 1));
                            break;
                        }
                    }
                    PruneOperator::Less | PruneOperator::LessOrEqual => {
                        let failed = if op == PruneOperator::Less {
                            value >= bound
                        } else {
                            value > bound
                        };
                        if failed {
                            mask.extend(std::iter::repeat_n(0, rows - index - 1));
                            break;
                        }
                    }
                    _ => {}
                }
            } else {
                // Monotonic decreasing (all deltas negative while unwrapped).
                // Only valid as early exit when every delta is provably < 0:
                // min_delta < 0 alone does not prove it when width > 0
                // (residues can offset it positive), so no shortcut here —
                // scan to the end. This keeps the filter exact.
            }
        }
    }
    if mask.len() != rows {
        return None;
    }
    Some(mask)
}
