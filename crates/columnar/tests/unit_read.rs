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
// Moved from src/read.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::encoding::ColumnEncoding;
use plomid_columnar::flush::{flush, FlushConfig};
use plomid_columnar::format;
use plomid_columnar::layout::{checksum_of, put_u32};
use plomid_columnar::read::*;
use plomid_core::ColumnId;
use plomid_core::{GenerationId, SegmentId};
use plomid_storage::{Field, Row};

fn constant_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|_| Row::new(vec![Field::Integer(7)]))
        .collect()
}

/// Rewrites one column record's advisory bytes and reseals every checksum
/// above them (record, body, header), so the image stays fully valid.
fn patch_advisory(bytes: &mut [u8], col_idx: usize, compression: u8, encoding: u8) {
    use plomid_columnar::column::COLUMN_META_OFF_DOMINANT_ENCODING;
    let record = format::COLUMNAR_HEADER_SIZE + col_idx * format::COLUMNAR_COLUMN_METADATA_SIZE;
    bytes[record + format::COLUMN_META_OFF_COMPRESSION] = compression;
    bytes[record + COLUMN_META_OFF_DOMINANT_ENCODING] = encoding;
    let checksum = checksum_of(&bytes[record..record + format::COLUMN_META_OFF_RECORD_CHECKSUM]);
    put_u32(
        bytes,
        record + format::COLUMN_META_OFF_RECORD_CHECKSUM,
        checksum,
    );
    let header = plomid_columnar::segment::SegmentHeader::decode(bytes).expect("header");
    let end = header.chunks_offset as usize;
    let body_checksum = checksum_of(&bytes[format::COLUMNAR_HEADER_SIZE..end]);
    put_u32(bytes, format::COLUMNAR_OFF_BODY_CHECKSUM, body_checksum);
    let header_checksum = checksum_of(&bytes[..format::COLUMNAR_CHECKSUMMED_PREFIX_LEN]);
    put_u32(bytes, format::COLUMNAR_OFF_CHECKSUM, header_checksum);
}

fn flushed_default(rows: &[Row]) -> Vec<u8> {
    flush(
        rows,
        &[ColumnType::Integer],
        GenerationId::new(9),
        SegmentId::new(3),
        &FlushConfig::default(),
    )
    .expect("flush")
    .bytes
}

#[test]
fn new_segments_expose_encoding_summaries() {
    let rows = constant_rows(200);
    let bytes = flushed_default(&rows);
    let reader = SegmentReader::decode(&bytes).expect("decode");
    let summaries = reader.encoding_summaries();
    assert_eq!(summaries.len(), 1);
    let summary = summaries[0];
    // Totals are exact folds over the chunk table.
    let (compressed, uncompressed) = reader.chunks.iter().fold((0_u64, 0_u64), |(c, u), chunk| {
        (c + chunk.compressed_size, u + chunk.uncompressed_size)
    });
    assert_eq!(summary.compressed_bytes, compressed);
    assert_eq!(summary.uncompressed_bytes, uncompressed);
    assert_eq!(summary.chunk_count, reader.chunks.len() as u32);
    assert_eq!(
        summary.dominant_compression,
        Some(plomid_columnar::chunk::ChunkCompression::None)
    );
    // The persisted advisory agrees with the chunk table.
    let meta = &reader.columns[0];
    assert_eq!(meta.dominant_encoding(), Some(summary.dominant_encoding));
    assert_eq!(meta.encoding_tag, summary.dominant_encoding.tag().unwrap());
    // Data still reads back exactly.
    let back = reader
        .read_rows(&bytes, 0, reader.row_count, &[ColumnId::new(0)])
        .expect("rows");
    assert_eq!(back, rows);
}

#[test]
fn explicit_encoding_is_persisted_in_the_column_record() {
    let rows = constant_rows(200);
    let flushed = flush(
        &rows,
        &[ColumnType::Integer],
        GenerationId::new(9),
        SegmentId::new(4),
        &FlushConfig::default().with_encoding(ColumnEncoding::Rle),
    )
    .expect("flush");
    let reader = SegmentReader::decode(&flushed.bytes).expect("decode");
    assert_eq!(
        reader.columns[0].dominant_encoding(),
        Some(ColumnEncoding::Rle)
    );
    assert_eq!(
        reader.encoding_summaries()[0].dominant_encoding,
        ColumnEncoding::Rle
    );
}

#[test]
fn old_segments_without_advisories_decode_identically() {
    let rows = constant_rows(200);
    let mut old_style = flushed_default(&rows);
    // Simulate the pre-summary writer: zero both advisory bytes.
    patch_advisory(
        &mut old_style,
        0,
        format::COMPRESSION_NONE_TAG,
        format::ENCODING_RAW_TAG,
    );
    let old_reader = SegmentReader::decode(&old_style).expect("old decode");
    let new_reader = SegmentReader::decode(&flushed_default(&rows)).expect("new decode");
    // Rows are byte-identical through both images.
    let ids = [ColumnId::new(0)];
    let old_rows = old_reader
        .read_rows(&old_style, 0, old_reader.row_count, &ids)
        .expect("old rows");
    assert_eq!(old_rows, rows);
    // Summaries recompute from the chunks: same totals as the new image.
    let old_summary = &old_reader.encoding_summaries()[0];
    let new_summary = &new_reader.encoding_summaries()[0];
    assert_eq!(old_summary.compressed_bytes, new_summary.compressed_bytes);
    assert_eq!(
        old_summary.uncompressed_bytes,
        new_summary.uncompressed_bytes
    );
    assert_eq!(old_summary.dominant_encoding, new_summary.dominant_encoding);
}

#[test]
fn stale_compression_byte_never_drives_decoding() {
    let rows = constant_rows(64);
    let mut bytes = flushed_default(&rows);
    // Every chunk is stored verbatim, but the column byte claims ZSTD:
    // readers must still decode from the chunk headers.
    patch_advisory(
        &mut bytes,
        0,
        format::COMPRESSION_ZSTD_TAG,
        format::ENCODING_RAW_TAG,
    );
    let reader = SegmentReader::decode(&bytes).expect("decode");
    let back = reader
        .read_rows(&bytes, 0, reader.row_count, &[ColumnId::new(0)])
        .expect("rows");
    assert_eq!(back, rows);
    // The summary follows the chunks (None), not the stale byte.
    assert_eq!(
        reader.encoding_summaries()[0].dominant_compression,
        Some(plomid_columnar::chunk::ChunkCompression::None)
    );
}

#[test]
fn corrupt_summary_is_ignored_and_data_stays_intact() {
    let rows = constant_rows(200);
    let mut bytes = flushed_default(&rows);
    // An advisory tag no reader knows, under a valid checksum: the
    // segment must decode and the summary must fall back to the chunks.
    patch_advisory(&mut bytes, 0, format::COMPRESSION_NONE_TAG, 0x7F);
    let reader = SegmentReader::decode(&bytes).expect("decode");
    assert_eq!(reader.columns[0].dominant_encoding(), None);
    let back = reader
        .read_rows(&bytes, 0, reader.row_count, &[ColumnId::new(0)])
        .expect("rows");
    assert_eq!(back, rows);
    let summary = &reader.encoding_summaries()[0];
    let expected = plomid_columnar::column::dominant_chunk_encoding(&reader.chunks);
    assert_eq!(summary.dominant_encoding, expected);
}

#[test]
fn maintenance_report_folds_exact_segment_totals() {
    let rows = constant_rows(200);
    let bytes = flushed_default(&rows);
    let reader = SegmentReader::decode(&bytes).expect("decode");
    let report = reader.maintenance_report();
    assert_eq!(report.row_count, 200);
    assert_eq!(report.column_count, 1);
    assert_eq!(report.chunk_count, reader.chunks.len());
    assert_eq!(report.columns.len(), 1);
    let (compressed, uncompressed) = reader.chunks.iter().fold((0_u64, 0_u64), |(c, u), chunk| {
        (
            c.saturating_add(chunk.compressed_size),
            u.saturating_add(chunk.uncompressed_size),
        )
    });
    assert_eq!(report.compressed_bytes, compressed);
    assert_eq!(report.uncompressed_bytes, uncompressed);
    assert!((report.ratio - report.columns[0].ratio).abs() < 1e-12);
}
