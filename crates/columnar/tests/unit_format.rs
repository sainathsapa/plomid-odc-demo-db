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
// Moved from src/format.rs inline #[cfg(test)].
use plomid_columnar::format::{null_bitmap_is_null, null_bitmap_size};

#[test]
fn null_bitmap_size_rounds_up_to_whole_bytes() {
    assert_eq!(null_bitmap_size(0), 0);
    assert_eq!(null_bitmap_size(1), 1);
    assert_eq!(null_bitmap_size(8), 1);
    assert_eq!(null_bitmap_size(9), 2);
}

#[test]
fn null_bits_are_little_endian_within_each_byte() {
    let bitmap = [0b0000_0101, 0b1000_0000];
    assert!(null_bitmap_is_null(&bitmap, 0));
    assert!(!null_bitmap_is_null(&bitmap, 1));
    assert!(null_bitmap_is_null(&bitmap, 2));
    assert!(null_bitmap_is_null(&bitmap, 15));
}

#[test]
fn indices_outside_the_bitmap_are_not_null() {
    let bitmap = [0xFF_u8];
    assert!(!null_bitmap_is_null(&bitmap, 8));
    assert!(!null_bitmap_is_null(&bitmap, usize::MAX));
    assert!(!null_bitmap_is_null(&[], 0));
}
