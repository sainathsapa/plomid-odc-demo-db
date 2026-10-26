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
// Moved from `src/distance/hamming.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::distance::hamming::*;

#[test]
fn null_and_values() {
    assert_eq!(hamming_distance(None, Some(&[0xff]), 8), Ok(None));
    assert_eq!(hamming_distance(Some(&[0xff]), None, 8), Ok(None));
    assert_eq!(
        hamming_distance(Some(&[0b1010_1010]), Some(&[0b0101_0101]), 8),
        Ok(Some(8))
    );
    assert_eq!(
        hamming_distance(Some(&[0xff, 0xff]), Some(&[0x00, 0x00]), 10),
        Ok(Some(10))
    );
    assert!(hamming_distance(Some(&[0xff]), Some(&[0x00]), 9).is_err());
}

#[test]
fn normalized_values() {
    let n = hamming_distance_normalized(Some(&[0xff]), Some(&[0x00]), 8)
        .unwrap()
        .unwrap();
    assert!((n - 1.0).abs() < 1e-6);
    assert_eq!(
        hamming_distance_normalized(Some(&[]), Some(&[]), 0),
        Ok(Some(0.0))
    );
    assert_eq!(hamming_distance_normalized(None, Some(&[]), 0), Ok(None));
}
