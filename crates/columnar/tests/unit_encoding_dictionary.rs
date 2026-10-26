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
// Moved from src/encoding/dictionary.rs inline #[cfg(test)].
use plomid_columnar::encoding::dictionary::{decode, encode};
use plomid_columnar::encoding::entries::{append_entry, validate_stream};
use plomid_core::ErrorKind;

fn stream(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, value).expect("append");
    }
    out
}

fn body_of(values: &[&[u8]]) -> (Vec<u8>, Vec<u8>) {
    let raw = stream(values);
    let body = encode(&raw, values.len() as u64)
        .expect("encode")
        .expect("applicable");
    (raw, body)
}

#[test]
fn round_trips_low_cardinality_columns() {
    for values in [
        vec![&b"red"[..], b"green", b"red", b"blue", b"red"],
        vec![&b""[..], b"", b"x"],
        vec![&b"only"[..]],
        vec![],
    ] {
        let (raw, body) = body_of(&values);
        let back = decode(&body, values.len() as u64, raw.len() as u64).expect("decode");
        assert_eq!(back, raw);
        validate_stream(&back, values.len() as u64).expect("well formed");
    }
}

#[test]
fn two_distinct_values_cost_one_bit_per_row() {
    let values: Vec<&[u8]> = (0..1_000)
        .map(|i| if i % 2 == 0 { &b"y"[..] } else { &b"n"[..] })
        .collect();
    let (raw, body) = body_of(&values);
    // dict_count(8) + two five-byte entries, then the width byte.
    assert_eq!(body[18], 1);
    assert!(body.len() < raw.len() / 2);
}

#[test]
fn rejects_a_non_minimal_index_width() {
    let (raw, body) = body_of(&[b"a", b"b"]);
    let mut wide = body.clone();
    wide[18] = 8;
    assert_eq!(
        decode(&wide, 2, raw.len() as u64)
            .expect_err("width")
            .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn rejects_an_index_outside_the_dictionary() {
    // A hand-built body for three values whose middle index is 3, which no
    // three-entry dictionary can hold.
    let mut body = Vec::new();
    body.extend_from_slice(&3_u64.to_le_bytes());
    for value in [&b"a"[..], b"b", b"c"] {
        body.extend_from_slice(&1_u32.to_le_bytes());
        body.extend_from_slice(value);
    }
    body.push(2); // width for three entries
    body.push(0x1C); // indices 0, 3, 1 at two bits each, LSB-first
    let error = decode(&body, 3, 15).expect_err("index");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(error.to_string().contains("out of range"));
}

#[test]
fn rejects_truncation_trailing_and_wrong_lengths() {
    let (raw, body) = body_of(&[b"a", b"b", b"a"]);
    assert!(decode(&body[..body.len() - 1], 3, raw.len() as u64).is_err());
    let mut padded = body.clone();
    padded.push(0);
    assert!(decode(&padded, 3, raw.len() as u64).is_err());
    assert!(decode(&body, 3, raw.len() as u64 + 4).is_err());
    assert!(decode(&body, 4, raw.len() as u64).is_err());
    assert!(decode(&[], 0, 0).is_err());
}

#[test]
fn declines_a_dictionary_beyond_the_configured_cap() {
    let mut raw = Vec::new();
    for index in 0..=plomid_core::COLUMNAR_MAX_DICTIONARY_ENTRIES {
        append_entry(&mut raw, format!("v{index}").as_bytes()).expect("append");
    }
    assert!(encode(
        &raw,
        plomid_core::COLUMNAR_MAX_DICTIONARY_ENTRIES as u64 + 1
    )
    .expect("encode")
    .is_none());
}
