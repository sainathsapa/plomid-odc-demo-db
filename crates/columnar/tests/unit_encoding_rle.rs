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
// Moved from src/encoding/rle.rs inline #[cfg(test)].
use plomid_columnar::encoding::entries::{append_entry, validate_stream};
use plomid_columnar::encoding::rle::{decode, encode};
use plomid_core::ErrorKind;

fn stream(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, value).expect("append");
    }
    out
}

fn round_trip(values: &[&[u8]]) {
    let raw = stream(values);
    let body = encode(&raw, values.len() as u64)
        .expect("encode")
        .expect("body");
    let back = decode(&body, values.len() as u64, raw.len() as u64).expect("decode");
    assert_eq!(back, raw);
    validate_stream(&back, values.len() as u64).expect("well formed");
}

#[test]
fn round_trips_runs() {
    round_trip(&[b"a", b"a", b"a", b"b", b"b", b"c"]);
    round_trip(&[b"", b"", b"x"]);
    round_trip(&[b"only"]);
    round_trip(&[]);
}

#[test]
fn collapses_a_constant_column_to_one_run() {
    let values: Vec<&[u8]> = vec![b"repeated"; 1_000];
    let raw = stream(&values);
    let body = encode(&raw, 1_000).expect("encode").expect("body");
    assert_eq!(body.len(), 8 + 12 + b"repeated".len());
    assert_eq!(decode(&body, 1_000, raw.len() as u64).expect("decode"), raw);
}

#[test]
fn rejects_zero_length_runs() {
    let mut body = Vec::new();
    body.extend_from_slice(&1_u64.to_le_bytes());
    body.extend_from_slice(&0_u64.to_le_bytes());
    body.extend_from_slice(&1_u32.to_le_bytes());
    body.push(b'a');
    assert_eq!(
        decode(&body, 1, 5).expect_err("zero run").kind(),
        ErrorKind::Corruption
    );
    assert!(decode(&body, 0, 0).is_err());
}

#[test]
fn rejects_a_wrong_declared_raw_length() {
    let raw = stream(&[b"aa", b"aa"]);
    let body = encode(&raw, 2).expect("encode").expect("body");
    assert!(decode(&body, 2, raw.len() as u64 + 1).is_err());
    assert!(decode(&body, 2, raw.len() as u64 - 1).is_err());
}

#[test]
fn rejects_trailing_bytes_truncation_and_empty_bodies() {
    let raw = stream(&[b"aa", b"bb"]);
    let body = encode(&raw, 2).expect("encode").expect("body");
    let mut padded = body.clone();
    padded.push(0);
    assert!(decode(&padded, 2, raw.len() as u64).is_err());
    let truncated = &body[..body.len() - 1];
    assert!(decode(truncated, 2, raw.len() as u64).is_err());
    assert!(decode(&[], 0, 0).is_err());
}
