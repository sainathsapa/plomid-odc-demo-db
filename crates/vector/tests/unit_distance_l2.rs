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
// Moved from `src/distance/l2.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::distance::l2::*;

#[test]
fn dense_null_and_values() {
    assert_eq!(l2_distance(None, Some(&[1.0])), Ok(None));
    assert_eq!(l2_distance_squared(Some(&[1.0]), None), Ok(None));
    assert_eq!(
        l2_distance(Some(&[1.0, 2.0]), Some(&[4.0, 6.0])),
        Ok(Some(5.0))
    );
    assert_eq!(
        l2_distance_squared(Some(&[1.0, 2.0]), Some(&[4.0, 6.0])),
        Ok(Some(25.0))
    );
    assert!(l2_distance(Some(&[1.0]), Some(&[1.0, 2.0])).is_err());
}

#[test]
fn sparse_values() {
    let a = [(0u32, 3.0f32)];
    let b = [(1u32, 4.0f32)];
    assert!((l2_distance_squared_sparse(&a, &b) - 25.0).abs() < 1e-6);
    assert!((l2_distance_sparse(&a, &b) - 5.0).abs() < 1e-6);
    assert_eq!(l2_distance_squared_sparse(&[], &[]), 0.0);
}
