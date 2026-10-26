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
// Moved from src/layout.rs inline #[cfg(test)].
use plomid_columnar::layout::{
    checksum_of, checksum_of_pair, get_u32, get_u64, put_u32, put_u64, to_usize,
};
use plomid_core::ErrorKind;

#[test]
fn round_trips_little_endian_values() {
    let mut bytes = [0_u8; 12];
    put_u32(&mut bytes, 0, 0x1234_5678);
    put_u64(&mut bytes, 4, 0x0102_0304_0506_0708);
    assert_eq!(get_u32(&bytes, 0, "value").expect("u32"), 0x1234_5678);
    assert_eq!(
        get_u64(&bytes, 4, "value").expect("u64"),
        0x0102_0304_0506_0708
    );
}

#[test]
fn reads_outside_the_buffer_are_rejected() {
    let bytes = [0_u8; 4];
    let error = get_u64(&bytes, 0, "value").expect_err("truncated must fail");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert!(get_u32(&bytes, 1, "value").is_err());
    assert!(get_u32(&bytes, usize::MAX, "value").is_err());
}

#[test]
fn pair_checksum_matches_concatenation() {
    let first = [1_u8, 2, 3];
    let second = [4_u8, 5, 6, 7];
    let mut joined = Vec::new();
    joined.extend_from_slice(&first);
    joined.extend_from_slice(&second);
    assert_eq!(checksum_of_pair(&first, &second), checksum_of(&joined));
}

#[test]
fn oversized_offsets_are_rejected() {
    assert_eq!(to_usize(7, "offset").expect("fits"), 7);
    if usize::BITS < 64 {
        assert!(to_usize(u64::MAX, "offset").is_err());
    }
}
