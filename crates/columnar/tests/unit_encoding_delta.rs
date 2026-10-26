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
// Moved from src/encoding/delta.rs inline #[cfg(test)].
use plomid_columnar::encoding::delta::{decode, encode};
use plomid_columnar::encoding::entries::{append_entry, validate_stream};
use plomid_core::ErrorKind;

fn int_stream(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, &value.to_le_bytes()).expect("append");
    }
    out
}

fn body_of(values: &[i64]) -> (Vec<u8>, Vec<u8>) {
    let raw = int_stream(values);
    let body = encode(&raw, values.len() as u64)
        .expect("encode")
        .expect("applicable");
    (raw, body)
}

fn round_trip(values: &[i64]) {
    let (raw, body) = body_of(values);
    let back = decode(&body, values.len() as u64, raw.len() as u64).expect("decode");
    assert_eq!(back, raw);
    validate_stream(&back, values.len() as u64).expect("well formed");
}

#[test]
fn round_trips_monotonic_repeated_and_wide_values() {
    round_trip(&[0, 1, 2, 3, 4, 5]);
    round_trip(&[7; 32]);
    round_trip(&[i64::MIN, i64::MAX, 0, -1, 1]);
    round_trip(&[-5]);
    round_trip(&[]);
    let random: Vec<i64> = (0..256)
        .map(|i| (i as i64).wrapping_mul(0x9E37_79B9_7F4A_7C15_u64 as i64) ^ i64::MIN)
        .collect();
    round_trip(&random);
}

#[test]
fn constant_steps_need_no_delta_bits() {
    let values: Vec<i64> = (0..1_000).collect();
    let (raw, body) = body_of(&values);
    assert_eq!(body[0], 0);
    assert_eq!(body.len(), 17);
    assert!(body.len() * 50 < raw.len());
}

#[test]
fn irregular_steps_need_bits_only_for_their_spread() {
    // Deltas are 1, 2 and 3: min_delta is 1, so the widest adjusted delta is
    // 2, which fits in two bits.
    let mut values = vec![0_i64];
    for step in [1_i64, 2, 3].iter().cycle().take(255) {
        values.push(values[values.len() - 1] + *step);
    }
    let (raw, body) = body_of(&values);
    assert_eq!(body[0], 2);
    assert_eq!(body.len(), 17 + (255 * 2_usize).div_ceil(8));
    assert!(body.len() < raw.len() / 4);
}

#[test]
fn constant_columns_need_no_delta_bits() {
    let (_, body) = body_of(&vec![42_i64; 100]);
    assert_eq!(body[0], 0);
    assert_eq!(body.len(), 17);
}

#[test]
fn declines_variable_width_entries() {
    let mut stream = Vec::new();
    append_entry(&mut stream, b"abc").expect("append");
    assert!(encode(&stream, 1).expect("encode").is_none());
}

#[test]
fn rejects_wrong_widths_lengths_and_truncation() {
    let (raw, body) = body_of(&[1, 2, 3]);
    assert!(decode(&body, 3, raw.len() as u64 + 1).is_err());
    assert!(decode(&body, 2, raw.len() as u64).is_err());
    assert!(decode(&body[..body.len() - 1], 3, raw.len() as u64).is_err());
    let mut wide = body.clone();
    wide[0] = 65;
    assert_eq!(
        decode(&wide, 3, raw.len() as u64)
            .expect_err("width")
            .kind(),
        ErrorKind::Corruption
    );
    assert!(decode(&[], 1, 12).is_err());
    assert!(decode(&[0_u8], 1, 12).is_err());
}
