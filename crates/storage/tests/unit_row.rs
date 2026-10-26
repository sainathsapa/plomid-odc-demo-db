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
// Moved from `src/row.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_storage::row::{decode, encode, Field, Row};

#[test]
fn round_trips_scalars_strings_bytes_and_nulls() {
    let row = Row::new(vec![
        Field::Integer(-42),
        Field::Null,
        Field::Bytes(vec![0, 1, 2, 255]),
        Field::String("PLOMID storage".to_owned()),
    ]);
    let encoded = encode(&row).expect("row encoding must succeed");
    let decoded = decode(&encoded).expect("row decoding must succeed");
    assert_eq!(decoded, row);
}

#[test]
fn truncated_rows_fail_with_corruption() {
    let encoded = Row::new(vec![Field::Integer(7), Field::String("value".to_owned())])
        .encode()
        .expect("row encoding must succeed");
    for length in 0..encoded.len() {
        let error = decode(&encoded[..length]).expect_err("truncated row must fail");
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn invalid_tag_fails_with_corruption() {
    let mut encoded = Row::new(vec![Field::Integer(7)])
        .encode()
        .expect("row encoding must succeed");
    encoded[12] = 99;
    let error = decode(&encoded).expect_err("invalid tag must fail");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn empty_variable_width_fields_round_trip() {
    let row = Row::new(vec![
        Field::Bytes(Vec::new()),
        Field::String(String::new()),
        Field::Bytes(vec![0]),
    ]);
    let encoded = encode(&row).expect("row encoding must succeed");
    assert_eq!(decode(&encoded).expect("row decoding must succeed"), row);
    // Empty values are length-prefixed zero, not omitted.
    assert!(encoded.len() > 12 + 1);
}

#[test]
fn all_null_and_empty_rows_round_trip() {
    let all_null = Row::new(vec![
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
        Field::Null,
    ]);
    let encoded = encode(&all_null).expect("row encoding must succeed");
    assert_eq!(
        decode(&encoded).expect("row decoding must succeed"),
        all_null
    );

    let empty = Row::new(Vec::new());
    let encoded = encode(&empty).expect("row encoding must succeed");
    assert_eq!(encoded.len(), 12);
    assert_eq!(decode(&encoded).expect("row decoding must succeed"), empty);
}

#[test]
fn maximum_supported_values_round_trip() {
    let wide_text = "x".repeat(4_096);
    let wide_bytes = vec![0xAB; 8_192];
    let row = Row::new(vec![
        Field::Integer(i64::MIN),
        Field::Integer(i64::MAX),
        Field::Integer(0),
        Field::String(wide_text),
        Field::Bytes(wide_bytes),
    ]);
    let encoded = encode(&row).expect("row encoding must succeed");
    assert_eq!(decode(&encoded).expect("row decoding must succeed"), row);
}

#[test]
fn encoding_is_deterministic() {
    let row = Row::new(vec![
        Field::Integer(11),
        Field::Null,
        Field::String("deterministic".to_owned()),
        Field::Bytes(vec![1, 2, 3]),
    ]);
    let first = encode(&row).expect("row encoding must succeed");
    let second = encode(&row).expect("row encoding must succeed");
    assert_eq!(first, second);
    // Re-encoding a decoded row reproduces the identical byte string.
    let decoded = decode(&first).expect("row decoding must succeed");
    assert_eq!(encode(&decoded).expect("row encoding must succeed"), first);
}

#[test]
fn malformed_headers_fail_with_corruption() {
    let encoded = Row::new(vec![Field::Integer(7)])
        .encode()
        .expect("row encoding must succeed");

    let mut bad_magic = encoded.clone();
    bad_magic[0] = 0x00;
    assert_corruption(&bad_magic);

    let mut bad_version = encoded.clone();
    bad_version[2] = 0x7F;
    assert_corruption(&bad_version);

    let mut bad_flags = encoded.clone();
    bad_flags[3] = 0x01;
    assert_corruption(&bad_flags);

    let mut bad_bitmap = encoded.clone();
    bad_bitmap[6] = 0x02;
    assert_corruption(&bad_bitmap);

    let mut bad_length = encoded.clone();
    bad_length[8] = 0xFF;
    assert_corruption(&bad_length);
}

#[test]
fn trailing_and_overlong_payloads_fail_with_corruption() {
    let mut trailing = Row::new(vec![Field::Integer(7)])
        .encode()
        .expect("row encoding must succeed");
    trailing.push(0x00);
    let length = u32::try_from(trailing.len()).expect("length");
    trailing[8..12].copy_from_slice(&length.to_le_bytes());
    assert_corruption(&trailing);

    let mut overlong = Row::new(vec![Field::Bytes(vec![1, 2, 3])])
        .encode()
        .expect("row encoding must succeed");
    let inflated = u32::MAX.to_le_bytes();
    overlong[14..18].copy_from_slice(&inflated);
    assert_corruption(&overlong);
    let inflated = (u32::MAX / 2).to_le_bytes();
    overlong[14..18].copy_from_slice(&inflated);
    assert_corruption(&overlong);
}

#[test]
fn non_utf8_string_fields_fail_with_corruption() {
    let mut encoded = Row::new(vec![Field::String("value".to_owned())])
        .encode()
        .expect("row encoding must succeed");
    encoded[18] = 0xFF;
    assert_corruption(&encoded);
}

#[test]
fn excessive_field_counts_fail_with_invalid_argument() {
    let row = Row::new(vec![Field::Null; usize::from(u16::MAX) + 1]);
    let error = encode(&row).expect_err("too many fields must fail");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

fn assert_corruption(bytes: &[u8]) {
    let error = decode(bytes).expect_err("malformed row must fail");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}
