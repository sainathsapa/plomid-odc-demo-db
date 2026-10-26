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
// Moved from `src/math/scalar.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::scalar::*;

#[test]
fn dot_basic() {
    assert_eq!(dot_f32(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]), Ok(32.0));
    assert_eq!(dot_f32(&[], &[]), Ok(0.0));
    assert!(dot_f32(&[1.0], &[1.0, 2.0]).is_err());
}

#[test]
fn l2_basic() {
    assert_eq!(l2_squared_f32(&[1.0, 2.0], &[4.0, 6.0]), Ok(25.0));
    assert_eq!(l2_distance_f32(&[1.0, 2.0], &[4.0, 6.0]), Ok(5.0));
    assert_eq!(l2_squared_f32(&[], &[]), Ok(0.0));
}

#[test]
fn cosine_orthogonal_is_one() {
    let d = cosine_distance_f32(&[1.0, 0.0], &[0.0, 1.0]).unwrap();
    assert!(matches!(d, Some(v) if (v - 1.0).abs() < 1e-6));
    let ident = cosine_distance_f32(&[1.0, 2.0], &[1.0, 2.0]).unwrap();
    assert!(matches!(ident, Some(v) if v.abs() < 1e-6));
}

#[test]
fn cosine_zero_norm_is_none() {
    assert_eq!(cosine_distance_f32(&[0.0, 0.0], &[1.0, 0.0]), Ok(None));
    assert_eq!(cosine_similarity_f32(&[0.0], &[0.0]), Ok(None));
    assert_eq!(cosine_distance_f32(&[], &[]), Ok(None));
}
