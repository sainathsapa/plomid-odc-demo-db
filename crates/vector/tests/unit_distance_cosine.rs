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
// Moved from `src/distance/cosine.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::distance::cosine::*;

#[test]
fn dense_null_and_zero_norm() {
    assert_eq!(cosine_distance(None, Some(&[1.0])), Ok(None));
    assert_eq!(cosine_distance(Some(&[1.0]), None), Ok(None));
    assert_eq!(cosine_distance(None, None), Ok(None));
    assert_eq!(cosine_distance(Some(&[0.0]), Some(&[1.0])), Ok(None));
    assert!(cosine_distance(Some(&[1.0]), Some(&[1.0, 2.0])).is_err());
}

#[test]
fn dense_values() {
    let d = cosine_distance(Some(&[1.0, 0.0]), Some(&[0.0, 1.0])).unwrap();
    assert!(matches!(d, Some(v) if (v - 1.0).abs() < 1e-6));
    let s = cosine_similarity(Some(&[1.0, 2.0]), Some(&[2.0, 4.0])).unwrap();
    assert!(matches!(s, Some(v) if (v - 1.0).abs() < 1e-6));
}

#[test]
fn sparse_values() {
    let a = [(0u32, 1.0f32), (2, 1.0)];
    let b = [(1u32, 1.0f32), (2, 1.0)];
    // dot=1, |a|=|b|=sqrt(2) -> sim=0.5 -> dist=0.5.
    let d = cosine_distance_sparse(&a, &b).unwrap().unwrap();
    assert!((d - 0.5).abs() < 1e-6, "{d}");
    assert_eq!(cosine_distance_sparse(&[], &b), Ok(None));
    assert_eq!(cosine_distance_sparse(&[], &[]), Ok(None));
}
