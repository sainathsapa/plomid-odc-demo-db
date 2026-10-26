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
// Moved from `src/search/topk.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::candidate::Candidate;
use plomid_vector::search::topk::*;

#[test]
fn smallest_keeps_best_k_with_row_id_ties() {
    let mut h = TopKHeap::new(2, TopKMode::Smallest);
    h.push(9, 5.0);
    h.push(3, 1.0);
    h.push(4, 1.0); // ties with row 3; smaller row_id wins ties
    h.push(1, 9.0);
    let out = h.into_sorted_vec();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_id, 3);
    assert_eq!(out[1].row_id, 4);
}

#[test]
fn largest_mode_ranks_scores_descending() {
    let mut h = TopKHeap::new(2, TopKMode::Largest);
    h.push(1, 1.0);
    h.push(2, 9.0);
    h.push(3, 5.0);
    h.push(4, 9.0); // tie with row 2; smaller row_id first
    let out = h.into_sorted_vec();
    assert_eq!(out.len(), 2);
    assert_eq!((out[0].row_id, out[1].row_id), (2, 4));
}

#[test]
fn zero_k_and_reuse() {
    let mut h = TopKHeap::new(0, TopKMode::Smallest);
    h.push(1, 1.0);
    assert!(h.is_empty());
    assert!(h.into_sorted_vec().is_empty());

    let mut h = TopKHeap::new(1, TopKMode::Smallest);
    h.push(1, 2.0);
    assert!(h.is_full());
    assert_eq!(h.worst(), Some(Candidate::new(1, 2.0)));
    h.push(2, 1.0);
    assert_eq!(h.worst(), Some(Candidate::new(2, 1.0)));
    h.clear();
    assert!(h.is_empty());
    h.push(3, 3.0);
    assert_eq!(h.len(), 1);
}
