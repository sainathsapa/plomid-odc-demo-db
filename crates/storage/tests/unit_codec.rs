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
// Moved from `src/codec.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_storage::codec::{checked_len, Cursor};

#[test]
fn reads_little_endian_values() {
    let mut cursor = Cursor::new(&[1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 9]);
    assert_eq!(cursor.u32("value").expect("u32"), 1);
    assert_eq!(cursor.u64("value").expect("u64"), 2);
    assert_eq!(cursor.u8("flag").expect("u8"), 9);
    assert!(cursor.is_empty());
    cursor.ensure_end("cursor").expect("end");
}

#[test]
fn truncated_images_are_rejected_without_panicking() {
    let mut cursor = Cursor::new(&[1, 2]);
    let error = cursor.u32("value").expect_err("truncated");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    let mut cursor = Cursor::new(&[]);
    assert!(cursor.u64("value").is_err());
    assert!(cursor.fixed::<4>("magic").is_err());
}

#[test]
fn trailing_data_is_detected() {
    let cursor = Cursor::new(&[1, 2, 3]);
    assert!(cursor.ensure_end("cursor").is_err());
}

#[test]
fn checked_lengths_never_overflow() {
    assert_eq!(checked_len(4, 8), Some(32));
    assert_eq!(checked_len(usize::MAX, 2), None);
}
