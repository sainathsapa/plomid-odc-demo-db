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
// Moved from `src/search/rerank.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::candidate::oversample_count;
use plomid_vector::search::rerank::*;
use plomid_vector::search::{candidate::Candidate, SearchMetric};

fn store() -> std::collections::HashMap<u64, Vec<f32>> {
    [
        (1u64, vec![1.0, 0.0]),
        (2, vec![0.0, 1.0]),
        (3, vec![0.9, 0.1]),
    ]
    .into_iter()
    .collect()
}

#[test]
fn rerank_orders_exactly() {
    let db = store();
    // Approximate order is deliberately wrong; exact rerank must fix it.
    let approx = vec![
        Candidate::new(2, 0.0),
        Candidate::new(3, 0.1),
        Candidate::new(1, 0.2),
    ];
    let out = rerank(
        &approx,
        |id| db.get(&id).cloned(),
        &[1.0, 0.0],
        SearchMetric::L2,
        2,
        None,
    )
    .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_id, 1);
    assert_eq!(out[1].row_id, 3);
}

#[test]
fn rerank_skips_missing_rows() {
    let db = store();
    let approx = vec![Candidate::new(99, 0.0), Candidate::new(1, 0.5)];
    let out = rerank(
        &approx,
        |id| db.get(&id).cloned(),
        &[1.0, 0.0],
        SearchMetric::L2,
        5,
        None,
    )
    .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].row_id, 1);
}

#[test]
fn rerank_empty_and_oversample_sizing() {
    let db = store();
    let out = rerank(
        &[],
        |id| db.get(&id).cloned(),
        &[1.0, 0.0],
        SearchMetric::L2,
        5,
        None,
    )
    .unwrap();
    assert!(out.is_empty());
    let approx = vec![Candidate::new(1, 0.0)];
    let out = rerank(
        &approx,
        |id| db.get(&id).cloned(),
        &[1.0, 0.0],
        SearchMetric::L2,
        0,
        None,
    )
    .unwrap();
    assert!(out.is_empty());
    // Oversample helper drives the fetch size.
    assert_eq!(oversample_count(10, 4, 1000), 40);
}

#[test]
fn rerank_dim_mismatch_errors() {
    let approx = vec![Candidate::new(1, 0.0)];
    let err = rerank(
        &approx,
        |_| Some(vec![1.0, 2.0, 3.0]),
        &[1.0, 0.0],
        SearchMetric::L2,
        5,
        None,
    );
    assert!(err.is_err());
}
