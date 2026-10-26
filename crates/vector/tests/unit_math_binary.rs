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
// Moved from `src/math/binary.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::binary::*;

#[test]
fn hamming_basic_and_padding_mask() {
    // 10 bits MSB-first: full byte 0 plus the top 2 bits of byte 1.
    let a = [0xffu8, 0xff];
    let b = [0x00u8, 0x00];
    assert_eq!(hamming_packed(&a, &b, 10).unwrap(), 10);
    assert_eq!(hamming_packed(&a, &b, 16).unwrap(), 16);
    assert_eq!(hamming_packed(&a, &a, 10).unwrap(), 0);
    // Padding differs but must be ignored: top 2 bits of byte 1 are
    // data (equal here), the low 6 bits are padding (differ).
    let c = [0x03u8, 0xff];
    let d = [0x03u8, 0xc0];
    assert_eq!(hamming_packed(&c, &d, 10).unwrap(), 0);
    // Same, but with differing data bits: 6 (byte 0: 0xff^0x03) + 0.
    let e = [0xffu8, 0xc0];
    assert_eq!(hamming_packed(&e, &d, 10).unwrap(), 6);
    // Byte-length mismatch is an error (9 bits need 2 bytes).
    assert!(hamming_packed(&a, &b[..1], 10).is_err());
    assert!(hamming_packed(&a[..1], &b[..1], 9).is_err());
    assert_eq!(hamming_packed(&[], &[], 0).unwrap(), 0);
    assert_eq!(hamming_u64(0b1010, 0b0110), 2);
}

#[test]
fn jaccard_empty_rule_and_values() {
    assert_eq!(jaccard_similarity_packed(&[], &[], 0).unwrap(), 1.0);
    assert_eq!(jaccard_distance_packed(&[], &[], 0).unwrap(), 0.0);
    // {0,1} vs {1,2} over 8 bits: inter=1, union=3.
    let a = [0b0000_0011u8];
    let b = [0b0000_0110u8];
    let sim = jaccard_similarity_packed(&a, &b, 8).unwrap();
    assert!((sim - 1.0 / 3.0).abs() < 1e-6, "{sim}");
    assert!((jaccard_distance_packed(&a, &b, 8).unwrap() - 2.0 / 3.0).abs() < 1e-6);
    // Disjoint non-empty sets -> similarity 0.
    let c = [0b0000_0001u8];
    let d = [0b0000_0010u8];
    assert_eq!(jaccard_similarity_packed(&c, &d, 8).unwrap(), 0.0);
}
