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
// Moved from `src/math/mod.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::*;

#[test]
fn dims_ok_and_mismatch() {
    assert!(check_dims_equal(3, 3).is_ok());
    assert_eq!(
        check_dims_equal(3, 4),
        Err(MathError::DimensionMismatch {
            expected: 3,
            actual: 4
        })
    );
}

#[test]
fn bytes_for_bits_values() {
    assert_eq!(bytes_for_bits(0), Ok(0));
    assert_eq!(bytes_for_bits(1), Ok(1));
    assert_eq!(bytes_for_bits(8), Ok(1));
    assert_eq!(bytes_for_bits(9), Ok(2));
    assert_eq!(bytes_for_bits(10), Ok(2));
    assert_eq!(bytes_for_bits(16), Ok(2));
    assert_eq!(bytes_for_bits(17), Ok(3));
}
