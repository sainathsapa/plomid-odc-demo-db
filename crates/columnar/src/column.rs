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
//! Column metadata for columnar segments.

use crate::chunk::{ChunkCompression, ColumnChunk};
use crate::encoding::ColumnEncoding;
use crate::format;
use plomid_core::{ColumnId, Result};
use plomid_storage::Field;
use std::fmt;

/// Logical type of a column in the columnar representation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ColumnType {
    /// Column holds only NULLs, or its type is not yet known.
    Null,
    /// A signed 64-bit integer.
    Integer,
    /// Arbitrary binary data.
    Bytes,
    /// UTF-8 variable-length text.
    String,
    /// Reserved for a future boolean encoding.
    Bool,
    /// Reserved for a future 16-bit integer encoding.
    I16,
    /// Reserved for a future 32-bit integer encoding.
    I32,
    /// Reserved for a future single-precision encoding.
    F32,
    /// Reserved for a future double-precision encoding.
    F64,
}

impl ColumnType {
    /// Maps a [`Field`] variant to its column type.
    #[must_use]
    pub fn from_field(field: &Field) -> Self {
        match field {
            Field::Null => ColumnType::Null,
            Field::Integer(_) => ColumnType::Integer,
            Field::Bytes(_) => ColumnType::Bytes,
            Field::String(_) => ColumnType::String,
        }
    }

    /// Returns the format tag byte for this column type.
    ///
    /// Types without a persisted encoding share the NULL tag, which the writer
    /// never emits for a column that carries data.
    #[must_use]
    pub fn tag(&self) -> u8 {
        match self {
            ColumnType::Integer => format::COLUMN_TYPE_INTEGER,
            ColumnType::Bytes => format::COLUMN_TYPE_BYTES,
            ColumnType::String => format::COLUMN_TYPE_STRING,
            ColumnType::Null
            | ColumnType::Bool
            | ColumnType::I16
            | ColumnType::I32
            | ColumnType::F32
            | ColumnType::F64 => format::COLUMN_TYPE_NULL,
        }
    }

    /// Decodes a column type from its format tag byte.
    #[must_use]
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            format::COLUMN_TYPE_NULL => Some(ColumnType::Null),
            format::COLUMN_TYPE_INTEGER => Some(ColumnType::Integer),
            format::COLUMN_TYPE_BYTES => Some(ColumnType::Bytes),
            format::COLUMN_TYPE_STRING => Some(ColumnType::String),
            _ => None,
        }
    }

    /// Returns the fixed width of a value in bytes, if the type is fixed width.
    #[must_use]
    pub fn fixed_width(&self) -> Option<usize> {
        match self {
            ColumnType::Integer => Some(8),
            _ => None,
        }
    }
}

/// Offset of the dominant-encoding advisory byte inside a column record.
///
/// Byte 11 is the alignment padding between `stats_flags` (offset 10) and
/// `first_chunk` (offset 12): the version-1 writer always left it zero and no
/// reader ever inspected it. It now carries the column's dominant value
/// encoding as an `ENCODING_*_TAG` (zero is [`ColumnEncoding::Raw`], which is
/// also what every pre-encoding segment holds there). The byte is covered by
/// the record checksum like every other record byte, so old readers — which
/// never read it — still verify and decode new segments unchanged.
pub const COLUMN_META_OFF_DOMINANT_ENCODING: usize = 11;

/// Metadata for one column in a columnar segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnMetadata {
    /// Stable identity of the column.
    pub column_id: ColumnId,
    /// Logical type of the stored values.
    pub column_type: ColumnType,
    /// Number of logical rows covered by this column.
    pub row_count: u64,
    /// Number of rows whose null bit is set.
    pub null_count: u64,
    /// Number of chunks that store this column's values.
    pub chunk_count: u32,
    /// Index of this column's first chunk within the segment chunk table.
    pub first_chunk: u32,
    /// Absolute offset of this column's null bitmap.
    pub null_bitmap_offset: u64,
    /// Absolute offset of this column's first chunk header.
    pub values_offset: u64,
    /// Total on-disk size of this column's chunks, headers included.
    pub values_size: u64,
    /// Absolute offset of this column's encoded statistics.
    pub statistics_offset: u64,
    /// Encoded statistics size in bytes.
    pub statistics_size: u32,
    /// CRC32C of the encoded statistics payload.
    pub statistics_checksum: u32,
    /// Compression encoding used by this column's chunks.
    ///
    /// Advisory summary only: the writer records the dominant codec (the one
    /// covering the most stored bytes) so planners can skip the chunk table,
    /// but the per-chunk headers stay authoritative and a reader never trusts
    /// this byte for decoding. Segments written before the writer set it read
    /// back as [`ChunkCompression::None`], exactly as they always have.
    pub compression: u8,
    /// Dominant value encoding of this column's chunks, as an `ENCODING_*_TAG`.
    ///
    /// Advisory summary persisted in the record's former padding byte (offset
    /// 11): the dominant encoding is the one covering the most uncompressed
    /// bytes. Unknown tags (a newer writer, or a resealed-but-tampered byte)
    /// are reported as [`None`] by [`Self::dominant_encoding`] and the caller
    /// recomputes from the chunk table instead of failing the read.
    pub encoding_tag: u8,
    /// Bitmask of the statistics that this column advertises.
    pub stats_flags: u8,
}

impl ColumnMetadata {
    /// Creates metadata for a column with no chunks and no statistics.
    #[must_use]
    pub fn new(column_id: ColumnId, column_type: ColumnType, row_count: u64) -> Self {
        Self {
            column_id,
            column_type,
            row_count,
            null_count: 0,
            chunk_count: 0,
            first_chunk: 0,
            null_bitmap_offset: 0,
            values_offset: 0,
            values_size: 0,
            statistics_offset: 0,
            statistics_size: 0,
            statistics_checksum: 0,
            compression: format::COMPRESSION_NONE_TAG,
            encoding_tag: format::ENCODING_RAW_TAG,
            stats_flags: 0,
        }
    }

    /// Returns true when both a minimum and a maximum are persisted.
    #[must_use]
    pub fn has_min_max(&self) -> bool {
        (self.stats_flags & format::STATS_MIN_AVAILABLE) != 0
            && (self.stats_flags & format::STATS_MAX_AVAILABLE) != 0
    }

    /// Returns true when a null count is persisted.
    #[must_use]
    pub fn has_null_count(&self) -> bool {
        (self.stats_flags & format::STATS_NULL_COUNT_AVAILABLE) != 0
    }

    /// Returns true when a row count is persisted.
    #[must_use]
    pub fn has_row_count(&self) -> bool {
        (self.stats_flags & format::STATS_ROW_COUNT_AVAILABLE) != 0
    }

    /// Returns the compression encoding used by this column's chunks.
    ///
    /// Advisory: the dominant codec across the column's chunks. Chunk headers
    /// remain the authority for decoding any single chunk.
    #[must_use]
    pub fn chunk_compression(&self) -> Option<crate::chunk::ChunkCompression> {
        crate::chunk::ChunkCompression::from_tag(self.compression)
    }

    /// Returns the dominant value encoding of this column's chunks.
    ///
    /// Advisory: `None` when the persisted tag names no known encoding (an
    /// old segment predates encodings and persists `RAW`; anything else
    /// unknown means recompute from the chunk table via
    /// [`dominant_chunk_encoding`]). Never an error: a damaged summary must
    /// not fail a segment whose chunks are intact.
    #[must_use]
    pub fn dominant_encoding(&self) -> Option<ColumnEncoding> {
        ColumnEncoding::from_tag(self.encoding_tag)
    }

    /// Encodes this record into its fixed-size little-endian image.
    ///
    /// The record checksum covers the record prefix, so a reader can validate
    /// a column's metadata before trusting any offset it carries.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use crate::layout::{checksum_of, put_u32, put_u64, put_u8};
        let mut bytes = vec![0_u8; format::COLUMNAR_COLUMN_METADATA_SIZE];
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_COLUMN_ID,
            self.column_id.get(),
        );
        put_u8(
            &mut bytes,
            format::COLUMN_META_OFF_COLUMN_TYPE,
            self.column_type.tag(),
        );
        put_u8(
            &mut bytes,
            format::COLUMN_META_OFF_COMPRESSION,
            self.compression,
        );
        put_u8(
            &mut bytes,
            format::COLUMN_META_OFF_STATS_FLAGS,
            self.stats_flags,
        );
        // Dominant-encoding advisory in the record's padding byte. Old
        // segments hold zero here, which decodes as `RAW` — the only encoding
        // they ever used.
        bytes[COLUMN_META_OFF_DOMINANT_ENCODING] = self.encoding_tag;
        put_u32(
            &mut bytes,
            format::COLUMN_META_OFF_FIRST_CHUNK,
            self.first_chunk,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_ROW_COUNT,
            self.row_count,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_NULL_COUNT,
            self.null_count,
        );
        put_u32(
            &mut bytes,
            format::COLUMN_META_OFF_CHUNK_COUNT,
            self.chunk_count,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_NULL_BITMAP,
            self.null_bitmap_offset,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_VALUES,
            self.values_offset,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_VALUES_SIZE,
            self.values_size,
        );
        put_u64(
            &mut bytes,
            format::COLUMN_META_OFF_STATISTICS,
            self.statistics_offset,
        );
        put_u32(
            &mut bytes,
            format::COLUMN_META_OFF_STATISTICS_SIZE,
            self.statistics_size,
        );
        put_u32(
            &mut bytes,
            format::COLUMN_META_OFF_STATISTICS_CHECKSUM,
            self.statistics_checksum,
        );
        let checksum = checksum_of(&bytes[..format::COLUMN_META_OFF_RECORD_CHECKSUM]);
        put_u32(
            &mut bytes,
            format::COLUMN_META_OFF_RECORD_CHECKSUM,
            checksum,
        );
        bytes
    }

    /// Decodes and validates a fixed-size column metadata record.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        use crate::layout::{checksum_of, corruption, get_u32, get_u64, get_u8};
        let record = bytes
            .get(..format::COLUMNAR_COLUMN_METADATA_SIZE)
            .ok_or_else(|| corruption("column metadata record is truncated"))?;
        let stored = get_u32(
            record,
            format::COLUMN_META_OFF_RECORD_CHECKSUM,
            "column metadata checksum",
        )?;
        if stored != checksum_of(&record[..format::COLUMN_META_OFF_RECORD_CHECKSUM]) {
            return Err(corruption("column metadata checksum mismatch"));
        }
        let type_tag = get_u8(record, format::COLUMN_META_OFF_COLUMN_TYPE, "column type")?;
        let column_type = ColumnType::from_tag(type_tag)
            .ok_or_else(|| corruption(format!("unknown column type tag {type_tag}")))?;
        let compression_tag = get_u8(
            record,
            format::COLUMN_META_OFF_COMPRESSION,
            "column compression",
        )?;
        if crate::chunk::ChunkCompression::from_tag(compression_tag).is_none() {
            return Err(corruption(format!(
                "unknown column compression tag {compression_tag}"
            )));
        }
        Ok(Self {
            column_id: ColumnId::new(get_u64(
                record,
                format::COLUMN_META_OFF_COLUMN_ID,
                "column id",
            )?),
            column_type,
            row_count: get_u64(record, format::COLUMN_META_OFF_ROW_COUNT, "row count")?,
            null_count: get_u64(record, format::COLUMN_META_OFF_NULL_COUNT, "null count")?,
            chunk_count: get_u32(record, format::COLUMN_META_OFF_CHUNK_COUNT, "chunk count")?,
            first_chunk: get_u32(record, format::COLUMN_META_OFF_FIRST_CHUNK, "first chunk")?,
            null_bitmap_offset: get_u64(
                record,
                format::COLUMN_META_OFF_NULL_BITMAP,
                "null bitmap offset",
            )?,
            values_offset: get_u64(record, format::COLUMN_META_OFF_VALUES, "values offset")?,
            values_size: get_u64(record, format::COLUMN_META_OFF_VALUES_SIZE, "values size")?,
            statistics_offset: get_u64(
                record,
                format::COLUMN_META_OFF_STATISTICS,
                "statistics offset",
            )?,
            statistics_size: get_u32(
                record,
                format::COLUMN_META_OFF_STATISTICS_SIZE,
                "statistics size",
            )?,
            statistics_checksum: get_u32(
                record,
                format::COLUMN_META_OFF_STATISTICS_CHECKSUM,
                "statistics checksum",
            )?,
            compression: compression_tag,
            // Advisory only: an unknown tag is preserved, not rejected.
            // `dominant_encoding()` reports `None` for it and summary
            // callers recompute from the chunk table instead.
            encoding_tag: record[COLUMN_META_OFF_DOMINANT_ENCODING],
            stats_flags: get_u8(
                record,
                format::COLUMN_META_OFF_STATS_FLAGS,
                "statistics flags",
            )?,
        })
    }
}

impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ColumnMetadata {{ column_id: {}, type: {:?}, rows: {}, nulls: {}, chunks: {}, compression: {}, encoding: {} }}",
            self.column_id.get(),
            self.column_type,
            self.row_count,
            self.null_count,
            self.chunk_count,
            self.compression,
            self.encoding_tag
        )
    }
}
/// Per-column encoding summary for compressed-execution planning and ratio
/// reporting.
///
/// The dominant tags are advisory (chunk headers stay authoritative); the byte
/// totals are exact, folded from the column's chunks. A column with no chunks
/// reports zero bytes, `RAW`/`None` dominants, and a ratio of `1.0`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ColumnEncodingSummary {
    /// Column this summary describes.
    pub column_id: ColumnId,
    /// Encoding covering the most uncompressed bytes of the column.
    pub dominant_encoding: ColumnEncoding,
    /// Codec covering the most stored bytes of the column, if any chunk exists.
    pub dominant_compression: Option<ChunkCompression>,
    /// Stored (compressed) payload bytes across the column's chunks.
    pub compressed_bytes: u64,
    /// Pre-codec payload bytes across the column's chunks.
    pub uncompressed_bytes: u64,
    /// Number of chunks storing the column.
    pub chunk_count: u32,
    /// `compressed_bytes / uncompressed_bytes`, or `1.0` when empty.
    pub ratio: f64,
}

impl ColumnEncodingSummary {
    /// Builds the summary of one column from its metadata and chunks.
    ///
    /// The persisted advisory tags are used only when they name a known
    /// encoding and agree with what the chunks hold; otherwise the dominant
    /// is recomputed from the chunks. Totals always come from the chunks, so
    /// a corrupt advisory byte can never skew a ratio — it is ignored and
    /// the data underneath stays intact.
    #[must_use]
    pub fn for_column(meta: &ColumnMetadata, chunks: &[ColumnChunk]) -> Self {
        let mut compressed_bytes = 0_u64;
        let mut uncompressed_bytes = 0_u64;
        for chunk in chunks {
            compressed_bytes = compressed_bytes.saturating_add(chunk.compressed_size);
            uncompressed_bytes = uncompressed_bytes.saturating_add(chunk.uncompressed_size);
        }
        let dominant_encoding = match meta.dominant_encoding() {
            // Trust the persisted advisory only when it agrees with the
            // chunks; a stale or unknown tag recomputes instead of failing.
            Some(advisory) if advisory == dominant_chunk_encoding(chunks) => advisory,
            _ => dominant_chunk_encoding(chunks),
        };
        ColumnEncodingSummary {
            column_id: meta.column_id,
            dominant_encoding,
            dominant_compression: dominant_chunk_compression(chunks),
            compressed_bytes,
            uncompressed_bytes,
            chunk_count: chunks.len() as u32,
            ratio: if uncompressed_bytes == 0 {
                1.0
            } else {
                compressed_bytes as f64 / uncompressed_bytes as f64
            },
        }
    }
}

/// Returns the codec covering the most stored bytes, or `None` when empty.
///
/// Ties keep the earliest chunk's codec, so the answer is deterministic for a
/// given chunk table.
#[must_use]
pub fn dominant_chunk_compression(chunks: &[ColumnChunk]) -> Option<ChunkCompression> {
    let mut totals: Vec<(ChunkCompression, u64)> = Vec::new();
    for chunk in chunks {
        match totals
            .iter_mut()
            .find(|(codec, _)| *codec == chunk.compression)
        {
            Some(entry) => entry.1 = entry.1.saturating_add(chunk.compressed_size),
            None => totals.push((chunk.compression, chunk.compressed_size)),
        }
    }
    let mut best: Option<(ChunkCompression, u64)> = None;
    for (codec, bytes) in totals {
        // Strictly greater wins, so the earliest codec keeps ties and the
        // answer is deterministic for a given chunk table.
        if best.is_none_or(|(_, held)| bytes > held) {
            best = Some((codec, bytes));
        }
    }
    best.map(|(codec, _)| codec)
}

/// Returns the encoding covering the most uncompressed bytes.
///
/// Empty input reports [`ColumnEncoding::Raw`]: there is no encoded byte to
/// dominate, and `Raw` is the version-1 layout. Ties keep the earliest chunk's
/// encoding, matching the encoder's deterministic preference order.
#[must_use]
pub fn dominant_chunk_encoding(chunks: &[ColumnChunk]) -> ColumnEncoding {
    let mut best = ColumnEncoding::Raw;
    let mut best_bytes = 0_u64;
    let mut seen: Vec<(ColumnEncoding, u64)> = Vec::new();
    for chunk in chunks {
        if let Some(entry) = seen.iter_mut().find(|(enc, _)| *enc == chunk.encoding) {
            entry.1 = entry.1.saturating_add(chunk.uncompressed_size);
        } else {
            seen.push((chunk.encoding, chunk.uncompressed_size));
        }
    }
    for (encoding, bytes) in seen {
        if bytes > best_bytes {
            best = encoding;
            best_bytes = bytes;
        }
    }
    best
}

/// Per-column encoding summaries for planning and ratio reporting.
///
/// Folds one [`ColumnEncodingSummary`] per column in `columns` order, using
/// `chunks` (matched by `column_id`) as the authoritative source for totals
/// and as the fallback when a persisted advisory tag is absent — which is the
/// ordinary state of segments written before summaries existed. Never errors:
/// missing or corrupt advisories recompute from the chunks.
///
/// This is the planning surface the compressed-execution gate and the
/// segment-maintenance report both read; see
/// [`crate::read::SegmentReader::encoding_summaries`].
#[must_use]
pub fn column_encoding_summary(
    columns: &[ColumnMetadata],
    chunks: &[ColumnChunk],
) -> Vec<ColumnEncodingSummary> {
    columns
        .iter()
        .map(|meta| {
            let owned: Vec<ColumnChunk> = chunks
                .iter()
                .filter(|chunk| chunk.column_id == meta.column_id)
                .cloned()
                .collect();
            ColumnEncodingSummary::for_column(meta, &owned)
        })
        .collect()
}
