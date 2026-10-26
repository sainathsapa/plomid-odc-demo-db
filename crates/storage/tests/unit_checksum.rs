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
// Moved from `src/checksum.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_storage::checksum::{compute_checksum, verify_checksum};

#[test]
fn crc32c_known_answer() {
    assert_eq!(compute_checksum(b"123456789"), 0xe306_9283);
}

#[test]
fn checksum_is_stable() {
    let bytes = b"page header and payload";
    assert_eq!(compute_checksum(bytes), compute_checksum(bytes));
    assert!(verify_checksum(bytes, compute_checksum(bytes)).is_ok());
}

#[test]
fn one_bit_flip_is_detected_as_corruption() {
    let mut bytes = *b"wal record";
    let expected = compute_checksum(&bytes);
    bytes[0] ^= 1;

    let error = verify_checksum(&bytes, expected).expect_err("bit flip must fail verification");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}
