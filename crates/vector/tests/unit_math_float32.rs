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
// Moved from `src/math/float32.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::float32::*;

#[test]
fn blocked_matches_scalar_within_ulp() {
    let a: Vec<f32> = (0..65).map(|i| (i as f32) * 0.25 - 4.0).collect();
    let b: Vec<f32> = (0..65).map(|i| (i as f32) * 0.5 + 1.0).collect();
    let ref_dot = plomid_vector::math::scalar::dot_f32(&a, &b).unwrap();
    let blk = dot_blocked(&a, &b).unwrap();
    assert!((blk - ref_dot).abs() <= 1e-4, "{blk} vs {ref_dot}");
    let ref_l2 = plomid_vector::math::scalar::l2_squared_f32(&a, &b).unwrap();
    let blk_l2 = l2_squared_blocked(&a, &b).unwrap();
    assert!((blk_l2 - ref_l2).abs() <= 1e-3, "{blk_l2} vs {ref_l2}");
    assert!(dot_blocked(&a, &b[..60]).is_err());
}

#[test]
fn normalize_roundtrip() {
    let mut v = [3.0f32, 4.0];
    assert!(normalize_in_place(&mut v));
    assert!((v[0] - 0.6).abs() < 1e-6 && (v[1] - 0.8).abs() < 1e-6);
    let mut z = [0.0f32, 0.0];
    assert!(!normalize_in_place(&mut z));
    assert_eq!(z, [0.0, 0.0]);
    let mut dst = [0.0f32; 2];
    assert!(normalize_into(&mut dst, &[3.0, 4.0]).unwrap());
    assert!((dst[0] - 0.6).abs() < 1e-6);
    assert!(!normalize_into(&mut dst, &[0.0, 0.0]).unwrap());
    assert!(normalize_into(&mut dst, &[1.0]).is_err());
}
