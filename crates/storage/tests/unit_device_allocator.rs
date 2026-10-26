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
// Moved from `src/device/allocator.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_storage::device::allocator::round_up_to_extent;
use plomid_storage::physical::EXTENT_SIZE;

#[test]
fn requests_are_rounded_up_to_whole_extents() {
    assert_eq!(round_up_to_extent(1).expect("small"), EXTENT_SIZE);
    assert_eq!(round_up_to_extent(EXTENT_SIZE).expect("exact"), EXTENT_SIZE);
    assert_eq!(
        round_up_to_extent(EXTENT_SIZE + 1).expect("over"),
        2 * EXTENT_SIZE
    );
}

#[test]
fn unsatisfiable_requests_are_rejected() {
    assert_eq!(
        round_up_to_extent(0).expect_err("zero").kind(),
        ErrorKind::InvalidArgument
    );
    assert!(round_up_to_extent(u64::MAX).is_err());
}
