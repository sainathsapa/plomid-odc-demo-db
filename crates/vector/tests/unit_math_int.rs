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
// Moved from `src/math/int.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::int::*;
use plomid_vector::math::MathError;

#[test]
fn dots() {
    assert_eq!(dot_i8(&[1, 2, 3], &[4, 5, 6]), Ok(32));
    assert_eq!(dot_u8(&[200, 1], &[2, 3]), Ok(403));
    assert_eq!(dot_i32(&[100_000, -2], &[3, 500]), Ok(299_000));
    assert_eq!(dot_i8(&[], &[]), Ok(0));
    assert!(dot_i8(&[1], &[1, 2]).is_err());
}

#[test]
fn l2_values() {
    assert_eq!(l2_squared_i8(&[1, 2], &[4, 6]), Ok(25));
    assert_eq!(l2_distance_i8(&[1, 2], &[4, 6]), Ok(5.0));
    assert_eq!(l2_squared_u8(&[0], &[255]), Ok(65025));
}

#[test]
fn overflow_is_reported() {
    // i32 extremes: diff = 2^32 - 1... square overflows i64.
    assert_eq!(
        l2_squared_i32(&[i32::MAX], &[i32::MIN]),
        Err(MathError::Overflow {
            what: "l2_squared_i32"
        })
    );
    // Summation overflow for i32 dots.
    let big = vec![i32::MAX; 3];
    assert!(dot_i32(&big, &big).is_err());
}

#[test]
fn cosine_zero_norm_none() {
    assert_eq!(cosine_similarity_i8(&[0, 0], &[1, 0]), Ok(None));
    assert_eq!(cosine_distance_u8(&[0], &[0]), Ok(None));
    let s = cosine_similarity_i8(&[3, 4], &[3, 4]).unwrap().unwrap();
    assert!((s - 1.0).abs() < 1e-6);
}
