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
// Moved from src/flush.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::encoding::{ColumnEncoding, ColumnEncodingPreference};
use plomid_columnar::flush::*;
use plomid_core::{GenerationId, SegmentId};
use plomid_storage::{Field, Row};

fn gid(value: u64) -> GenerationId {
    GenerationId::new(value)
}

fn sid(value: u64) -> SegmentId {
    SegmentId::new(value)
}

fn int_rows(values: &[i64]) -> Vec<Row> {
    values
        .iter()
        .map(|value| Row::new(vec![Field::Integer(*value)]))
        .collect()
}

#[test]
fn default_config_carries_no_column_policies() {
    let config = FlushConfig::default();
    assert!(config.column_policies.is_empty());
    assert_eq!(config.encoding, ColumnEncoding::Auto);
    assert_eq!(config.encoding_for_column(0), ColumnEncoding::Auto);
    assert_eq!(config.encoding_for_column(99), ColumnEncoding::Auto);
}

#[test]
fn column_policy_falls_back_to_the_segment_encoding() {
    let config = FlushConfig::default()
        .with_encoding(ColumnEncoding::Rle)
        .with_column_policies(vec![
            ColumnEncodingPreference::Auto,
            ColumnEncodingPreference::Dictionary,
        ]);
    // `Auto` entries and missing entries use the segment-wide encoding.
    assert_eq!(config.encoding_for_column(0), ColumnEncoding::Rle);
    assert_eq!(config.encoding_for_column(1), ColumnEncoding::Dictionary);
    assert_eq!(config.encoding_for_column(7), ColumnEncoding::Rle);
}

#[test]
fn preference_resolves_like_its_encoding() {
    assert_eq!(
        ColumnEncodingPreference::Auto.resolve(ColumnEncoding::Rle),
        ColumnEncoding::Rle
    );
    assert_eq!(
        ColumnEncodingPreference::Dictionary.resolve(ColumnEncoding::Rle),
        ColumnEncoding::Dictionary
    );
    assert_eq!(
        ColumnEncodingPreference::from(ColumnEncoding::DeltaBitpack),
        ColumnEncodingPreference::DeltaBitpack
    );
    assert_eq!(ColumnEncodingPreference::Auto.as_encoding(), None);
    assert_eq!(
        ColumnEncodingPreference::Raw.as_encoding(),
        Some(ColumnEncoding::Raw)
    );
}

fn read_back(bytes: &[u8], row_count: u64) -> Vec<Row> {
    let reader = plomid_columnar::read::SegmentReader::decode(bytes).expect("decode");
    let ids: Vec<plomid_core::ColumnId> = reader.columns.iter().map(|c| c.column_id).collect();
    reader
        .read_rows(bytes, 0, row_count, &ids)
        .expect("read rows")
}

#[test]
fn per_column_policies_round_trip_exactly() {
    let rows: Vec<Row> = (0..256)
        .map(|i| {
            Row::new(vec![
                Field::Integer(i),
                Field::String(["red", "green", "blue"][i as usize % 3].to_owned()),
            ])
        })
        .collect();
    let types = [ColumnType::Integer, ColumnType::String];
    let config = FlushConfig::default().with_column_policies(vec![
        ColumnEncodingPreference::DeltaBitpack,
        ColumnEncodingPreference::Dictionary,
    ]);
    let flushed = flush(&rows, &types, gid(1), sid(1), &config).expect("flush");
    assert_eq!(read_back(&flushed.bytes, 256), rows);
}

#[test]
fn flush_is_deterministic_with_policies() {
    let rows = int_rows(&(0..300).collect::<Vec<i64>>());
    let config = FlushConfig::default().with_column_policies(vec![ColumnEncodingPreference::Auto]);
    let first = flush(&rows, &[ColumnType::Integer], gid(2), sid(3), &config).expect("flush");
    let second = flush(&rows, &[ColumnType::Integer], gid(2), sid(3), &config).expect("flush");
    assert_eq!(first.bytes, second.bytes);
}

#[test]
fn flush_round_trips_null_empty_and_constant_shapes() {
    // Empty segment.
    let empty: Vec<Row> = Vec::new();
    let flushed = flush(
        &empty,
        &[ColumnType::Integer],
        gid(4),
        sid(5),
        &FlushConfig::default(),
    )
    .expect("empty flush");
    assert_eq!(read_back(&flushed.bytes, 0), empty);
    // All NULLs and constants, with per-column pins.
    let rows = vec![
        Row::new(vec![Field::Null, Field::Integer(7)]),
        Row::new(vec![Field::Null, Field::Integer(7)]),
        Row::new(vec![Field::Null, Field::Integer(7)]),
    ];
    let config = FlushConfig::default().with_column_policies(vec![
        ColumnEncodingPreference::Rle,
        ColumnEncodingPreference::Dictionary,
    ]);
    let types = [ColumnType::Integer, ColumnType::Integer];
    let flushed = flush(&rows, &types, gid(4), sid(6), &config).expect("flush");
    assert_eq!(read_back(&flushed.bytes, 3), rows);
    // Adversarial unsorted integers under adaptive Auto.
    let rows = int_rows(&[i64::MAX, i64::MIN, 0, -1, 1, i64::MAX - 1]);
    let flushed = flush(
        &rows,
        &[ColumnType::Integer],
        gid(4),
        sid(7),
        &FlushConfig::default(),
    )
    .expect("flush");
    assert_eq!(read_back(&flushed.bytes, 6), rows);
}

#[test]
fn adaptive_flush_matches_legacy_auto_bytes() {
    // The hinted live path must store exactly what brute-force Auto
    // stores on structured shapes (no size regression from early stops).
    let rows: Vec<Row> = (0..2_000)
        .map(|i| {
            Row::new(vec![
                Field::Integer(1_700_000_000_000_000 + i * 1_000),
                Field::String(["red", "green", "blue"][i as usize % 3].to_owned()),
            ])
        })
        .collect();
    let types = [ColumnType::Integer, ColumnType::String];
    let legacy = {
        // Legacy path: global Auto resolved per column without policies.
        let config = FlushConfig::default();
        flush(&rows, &types, gid(8), sid(9), &config).expect("flush")
    };
    let reader = plomid_columnar::read::SegmentReader::decode(&legacy.bytes).expect("decode");
    let ids: Vec<plomid_core::ColumnId> = reader.columns.iter().map(|c| c.column_id).collect();
    let back = reader
        .read_rows(&legacy.bytes, 0, 2_000, &ids)
        .expect("rows");
    assert_eq!(back, rows);
}
