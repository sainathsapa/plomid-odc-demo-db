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
// Moved from `src/key.rs` inline `#[cfg(test)]` — public-API only.
use plomid_index::key::{common_prefix_len, encode_i64_ordered, encode_u64_be, ByteKey};

#[test]
fn common_prefix_handles_prefix_relationships() {
    assert_eq!(common_prefix_len(b"abc", b"abcd"), 3);
    assert_eq!(common_prefix_len(b"abcd", b"abc"), 3);
    assert_eq!(common_prefix_len(b"abc", b"abc"), 3);
    assert_eq!(common_prefix_len(b"", b"abc"), 0);
    assert_eq!(common_prefix_len(b"", b""), 0);
    assert_eq!(common_prefix_len(b"foo", b"foobar"), 3);
    assert_eq!(common_prefix_len(b"foo", b"bar"), 0);
}

#[test]
fn common_prefix_spans_word_boundaries() {
    let left = b"0123456789abcdefghij";
    let right = b"0123456789abcdXfghij";
    // 14 matching bytes: the first ten, then four more ("abcd") before the
    // bytes diverge ('e' vs 'X').
    assert_eq!(common_prefix_len(left, right), 14);
    assert_eq!(&left[..14], &right[..14]);
    assert_ne!(left[14], right[14]);
}

#[test]
fn binary_keys_compare_byte_wise() {
    assert_eq!(common_prefix_len(&[0u8], &[0u8, 1]), 1);
    assert_eq!(common_prefix_len(&[0u8, 255, 1], &[0u8, 255, 2]), 2);
    assert_eq!(common_prefix_len(&[255u8], &[0u8]), 0);
}

#[test]
fn numeric_encodings_are_order_preserving() {
    assert!(encode_u64_be(1) < encode_u64_be(2));
    assert!(encode_i64_ordered(-5) < encode_i64_ordered(3));
    assert!(encode_i64_ordered(i64::MIN) < encode_i64_ordered(-1));
}

#[test]
fn byte_key_behaves_like_a_byte_string() {
    let key = ByteKey::from("abc");
    assert_eq!(key.as_bytes(), b"abc");
    assert_eq!(key.len(), 3);
    assert!(!key.is_empty());
    assert!(ByteKey::default().is_empty());
    assert_eq!(ByteKey::from_u64_be(7).as_bytes(), &encode_u64_be(7));

    let mut map = std::collections::HashMap::new();
    map.insert(key.clone(), 1u32);
    // `Borrow<[u8]>` lets a borrowed key probe an owned-key map.
    assert_eq!(map.get(b"abc".as_slice()), Some(&1));
}

#[test]
fn byte_keys_sort_like_bytes() {
    // `sort` on a fixed array avoids a heap allocation for this tiny case
    // (and satisfies the `useless_vec` lint).
    let mut keys = [ByteKey::from("b"), ByteKey::from("a"), ByteKey::from("ab")];
    keys.sort();
    let rendered: Vec<&[u8]> = keys.iter().map(ByteKey::as_bytes).collect();
    assert_eq!(rendered, vec![b"a".as_slice(), b"ab", b"b"]);
}
