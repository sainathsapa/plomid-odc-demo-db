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
// Moved from `src/search/candidate.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::candidate::*;

#[test]
fn ordering_tie_breaks_by_row_id() {
    let mut v = [
        Candidate::new(7, 1.0),
        Candidate::new(3, 1.0),
        Candidate::new(5, 0.5),
    ];
    v.sort();
    let ids: Vec<u64> = v.iter().map(|c| c.row_id).collect();
    assert_eq!(ids, vec![5, 3, 7]);
}

#[test]
fn nan_sorts_last_deterministically() {
    let mut v = [Candidate::new(1, f32::NAN), Candidate::new(2, 1.0)];
    v.sort();
    assert_eq!(v[0].row_id, 2);
    assert_eq!(v[1].row_id, 1);
    // Bitwise equality agrees with ordering.
    assert_eq!(Candidate::new(1, 1.0), Candidate::new(1, 1.0));
    assert_ne!(Candidate::new(1, 1.0), Candidate::new(2, 1.0));
}

#[test]
fn oversample_math() {
    assert_eq!(oversample_count(10, 4, 1000), 40);
    assert_eq!(oversample_count(10, 4, 25), 25);
    assert_eq!(oversample_count(0, 4, 1000), 0);
    assert_eq!(oversample_count(10, 0, 1000), 0);
    assert_eq!(
        oversample_count(usize::MAX, 2, usize::MAX),
        MAX_CANDIDATE_CAP
    );
    assert_eq!(
        oversample_count(10, 1, usize::MAX),
        10.min(MAX_CANDIDATE_CAP)
    );
}
