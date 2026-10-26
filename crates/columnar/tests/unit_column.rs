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
// Moved from src/column.rs inline #[cfg(test)].
use plomid_columnar::chunk::{ChunkCompression, ColumnChunk};
use plomid_columnar::column::*;
use plomid_columnar::encoding::ColumnEncoding;
use plomid_columnar::format;
use plomid_columnar::layout::{checksum_of, put_u32};
use plomid_core::ColumnId;

fn test_chunk(
    column: u64,
    encoding: ColumnEncoding,
    compression: ChunkCompression,
    compressed: u64,
    uncompressed: u64,
) -> ColumnChunk {
    ColumnChunk::new(
        ColumnId::new(column),
        encoding,
        compression,
        0,
        1,
        0,
        compressed,
        uncompressed,
        0,
    )
}

fn resealed(mut bytes: Vec<u8>) -> Vec<u8> {
    let checksum = checksum_of(&bytes[..format::COLUMN_META_OFF_RECORD_CHECKSUM]);
    put_u32(
        &mut bytes,
        format::COLUMN_META_OFF_RECORD_CHECKSUM,
        checksum,
    );
    bytes
}

#[test]
fn record_round_trips_with_dominant_encoding() {
    let mut meta = ColumnMetadata::new(ColumnId::new(3), ColumnType::Integer, 9);
    meta.compression = format::COMPRESSION_ZSTD_TAG;
    meta.encoding_tag = format::ENCODING_RLE_TAG;
    let back = ColumnMetadata::decode(&meta.encode()).expect("decode");
    assert_eq!(back, meta);
    assert_eq!(back.dominant_encoding(), Some(ColumnEncoding::Rle));
    assert_eq!(back.chunk_compression(), Some(ChunkCompression::Zstd));
}

#[test]
fn old_record_padding_decodes_as_raw_advisory() {
    // A version-1 writer left byte 11 at zero; that tag is `RAW`, the only
    // encoding such segments ever used.
    let meta = ColumnMetadata::new(ColumnId::new(0), ColumnType::String, 4);
    let mut bytes = meta.encode();
    assert_eq!(bytes[COLUMN_META_OFF_DOMINANT_ENCODING], 0);
    bytes[COLUMN_META_OFF_DOMINANT_ENCODING] = 0;
    let back = ColumnMetadata::decode(&resealed(bytes)).expect("decode");
    assert_eq!(back.dominant_encoding(), Some(ColumnEncoding::Raw));
    assert_eq!(back.chunk_compression(), Some(ChunkCompression::None));
}

#[test]
fn unknown_encoding_tag_is_advisory_not_an_error() {
    let meta = ColumnMetadata::new(ColumnId::new(1), ColumnType::Bytes, 4);
    let mut bytes = meta.encode();
    bytes[COLUMN_META_OFF_DOMINANT_ENCODING] = 0x7F;
    let back = ColumnMetadata::decode(&resealed(bytes)).expect("decode");
    assert_eq!(back.dominant_encoding(), None);
    // The unknown tag round-trips losslessly for a newer reader.
    assert_eq!(back.encoding_tag, 0x7F);
}

#[test]
fn dominant_folds_cover_most_bytes() {
    let chunks = vec![
        test_chunk(0, ColumnEncoding::Raw, ChunkCompression::None, 100, 100),
        test_chunk(0, ColumnEncoding::Rle, ChunkCompression::None, 10, 400),
        test_chunk(0, ColumnEncoding::Rle, ChunkCompression::Zstd, 300, 50),
    ];
    // Encoding dominance is by uncompressed bytes: RLE's 450 beats Raw's 100.
    assert_eq!(dominant_chunk_encoding(&chunks), ColumnEncoding::Rle);
    // Codec dominance is by stored bytes: ZSTD's 300 beats None's 110.
    assert_eq!(
        dominant_chunk_compression(&chunks),
        Some(ChunkCompression::Zstd)
    );
    assert_eq!(dominant_chunk_encoding(&[]), ColumnEncoding::Raw);
    assert_eq!(dominant_chunk_compression(&[]), None);
}

#[test]
fn summary_totals_come_from_chunks_not_the_advisory() {
    let mut meta = ColumnMetadata::new(ColumnId::new(5), ColumnType::Integer, 3);
    // Stale advisory: claims RLE while every chunk is Raw.
    meta.encoding_tag = format::ENCODING_RLE_TAG;
    let chunks = vec![
        test_chunk(5, ColumnEncoding::Raw, ChunkCompression::None, 60, 60),
        test_chunk(5, ColumnEncoding::Raw, ChunkCompression::None, 40, 80),
    ];
    let summary = ColumnEncodingSummary::for_column(&meta, &chunks);
    assert_eq!(summary.column_id, ColumnId::new(5));
    assert_eq!(summary.dominant_encoding, ColumnEncoding::Raw);
    assert_eq!(summary.dominant_compression, Some(ChunkCompression::None));
    assert_eq!(summary.compressed_bytes, 100);
    assert_eq!(summary.uncompressed_bytes, 140);
    assert_eq!(summary.chunk_count, 2);
    assert!((summary.ratio - 100.0 / 140.0).abs() < 1e-12);
}

#[test]
fn empty_column_reports_identity_ratio() {
    let meta = ColumnMetadata::new(ColumnId::new(2), ColumnType::Null, 0);
    let summary = ColumnEncodingSummary::for_column(&meta, &[]);
    assert_eq!(summary.dominant_encoding, ColumnEncoding::Raw);
    assert_eq!(summary.dominant_compression, None);
    assert_eq!(summary.compressed_bytes, 0);
    assert_eq!(summary.uncompressed_bytes, 0);
    assert_eq!(summary.chunk_count, 0);
    assert_eq!(summary.ratio, 1.0);
}

#[test]
fn column_encoding_summary_groups_by_column() {
    let first = ColumnMetadata::new(ColumnId::new(0), ColumnType::Integer, 2);
    let second = ColumnMetadata::new(ColumnId::new(1), ColumnType::String, 1);
    let chunks = vec![
        test_chunk(0, ColumnEncoding::Raw, ChunkCompression::None, 8, 8),
        test_chunk(1, ColumnEncoding::Dictionary, ChunkCompression::None, 6, 60),
        test_chunk(0, ColumnEncoding::Raw, ChunkCompression::None, 8, 8),
    ];
    let summaries = column_encoding_summary(&[first, second], &chunks);
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].column_id, ColumnId::new(0));
    assert_eq!(summaries[0].chunk_count, 2);
    assert_eq!(summaries[0].compressed_bytes, 16);
    assert_eq!(summaries[1].column_id, ColumnId::new(1));
    assert_eq!(summaries[1].dominant_encoding, ColumnEncoding::Dictionary);
    assert_eq!(summaries[1].chunk_count, 1);
}
