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
// Moved from `src/distance/dot.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::distance::dot::*;

#[test]
fn dense_null_and_values() {
    assert_eq!(dot(None, Some(&[1.0])), Ok(None));
    assert_eq!(neg_dot(Some(&[1.0]), None), Ok(None));
    assert_eq!(dot(Some(&[1.0, 2.0]), Some(&[3.0, 4.0])), Ok(Some(11.0)));
    assert_eq!(
        neg_dot(Some(&[1.0, 2.0]), Some(&[3.0, 4.0])),
        Ok(Some(-11.0))
    );
    assert!(dot(Some(&[1.0]), Some(&[1.0, 2.0])).is_err());
}

#[test]
fn sparse_values() {
    let a = [(0u32, 2.0f32), (2, 3.0)];
    let b = [(0u32, 4.0f32), (1, 9.0), (2, 5.0)];
    assert_eq!(dot_sparse(&a, &b), 2.0 * 4.0 + 3.0 * 5.0);
    assert_eq!(neg_dot_sparse(&a, &b), -(2.0 * 4.0 + 3.0 * 5.0));
    assert_eq!(dot_sparse(&[], &b), 0.0);
}
