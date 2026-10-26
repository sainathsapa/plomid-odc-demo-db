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
// Moved from `src/distance/jaccard.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::distance::jaccard::*;

#[test]
fn packed_null_and_empty_rule() {
    assert_eq!(jaccard_similarity_packed(None, Some(&[1]), 8), Ok(None));
    assert_eq!(jaccard_distance_packed(Some(&[1]), None, 8), Ok(None));
    assert_eq!(
        jaccard_similarity_packed(Some(&[]), Some(&[]), 0),
        Ok(Some(1.0))
    );
    assert_eq!(
        jaccard_distance_packed(Some(&[]), Some(&[]), 0),
        Ok(Some(0.0))
    );
}

#[test]
fn packed_values() {
    // {0,1} vs {1,2}: similarity 1/3.
    let s = jaccard_similarity_packed(Some(&[0b011]), Some(&[0b110]), 8)
        .unwrap()
        .unwrap();
    assert!((s - 1.0 / 3.0).abs() < 1e-6, "{s}");
    let d = jaccard_distance_packed(Some(&[0b011]), Some(&[0b110]), 8)
        .unwrap()
        .unwrap();
    assert!((d - 2.0 / 3.0).abs() < 1e-6, "{d}");
}

#[test]
fn sorted_and_sets_variants() {
    assert_eq!(jaccard_similarity_sorted(&[], &[]), 1.0);
    assert!((jaccard_similarity_sorted(&[0, 1], &[1, 2]) - 1.0 / 3.0).abs() < 1e-6);
    assert_eq!(jaccard_similarity_sorted(&[0], &[1]), 0.0);
    // Unsorted / duplicated inputs give the same answer as sorted ones.
    assert!((jaccard_similarity_sets(&[2, 0, 0, 1], &[2, 1, 2]) - 2.0 / 3.0).abs() < 1e-6);
    assert!((jaccard_distance_sets(&[2, 0, 0, 1], &[2, 1, 2]) - 1.0 / 3.0).abs() < 1e-6);
}
