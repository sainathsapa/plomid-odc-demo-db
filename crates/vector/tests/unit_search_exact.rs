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
// Moved from `src/search/exact.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::exact::*;
use plomid_vector::search::{radius::RadiusConfig, SearchMetric};

fn rows_owned() -> Vec<(u64, Option<Vec<f32>>)> {
    vec![
        (10, Some(vec![1.0, 0.0])),
        (20, Some(vec![0.0, 1.0])),
        (30, None),
        (40, Some(vec![0.9, 0.1])),
    ]
}

#[test]
fn top_k_l2_order_and_ties() {
    let rows = rows_owned();
    let it = rows.iter().map(|(id, v)| (*id, v.as_deref()));
    let out = exact_top_k(&[1.0, 0.0], it, SearchMetric::L2, 2, None).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_id, 10);
    assert_eq!(out[1].row_id, 40);
}

#[test]
fn top_k_dot_uses_largest_mode() {
    let rows = rows_owned();
    let it = rows.iter().map(|(id, v)| (*id, v.as_deref()));
    let out = exact_top_k(&[1.0, 0.0], it, SearchMetric::Dot, 2, None).unwrap();
    assert_eq!(out[0].row_id, 10);
    assert_eq!(out[1].row_id, 40);
}

#[test]
fn zero_k_and_dim_mismatch() {
    let rows = rows_owned();
    let it = rows.iter().map(|(id, v)| (*id, v.as_deref()));
    assert!(exact_top_k(&[1.0, 0.0], it, SearchMetric::L2, 0, None)
        .unwrap()
        .is_empty());
    let bad = [(1u64, Some([1.0, 2.0, 3.0].as_slice()))];
    assert!(exact_top_k(&[1.0, 0.0], bad.into_iter(), SearchMetric::L2, 1, None).is_err());
}

#[test]
fn batch_matches_single() {
    let rows = rows_owned();
    let q1: &[f32] = &[1.0, 0.0];
    let q2: &[f32] = &[0.0, 1.0];
    let batched = exact_batch_search(&[q1, q2], &rows, SearchMetric::L2, 2, None).unwrap();
    assert_eq!(batched.len(), 2);
    let it1 = rows.iter().map(|(id, v)| (*id, v.as_deref()));
    let single1 = exact_top_k(q1, it1, SearchMetric::L2, 2, None).unwrap();
    assert_eq!(batched[0], single1);
    assert_eq!(batched[1][0].row_id, 20);
}

#[test]
fn radius_delegates() {
    let cfg = RadiusConfig::new(0.5).unwrap();
    let out = exact_radius(&[1.0, 0.0], rows_owned(), SearchMetric::L2, &cfg, None).unwrap();
    let ids: Vec<u64> = out.iter().map(|c| c.row_id).collect();
    assert_eq!(ids, vec![10, 40]);
}
