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
// Moved from `src/search/radius.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::radius::*;
use plomid_vector::search::{SearchError, SearchMetric};

fn rows() -> Vec<(u64, Option<Vec<f32>>)> {
    vec![
        (1, Some(vec![1.0, 0.0])),
        (2, Some(vec![0.0, 1.0])),
        (3, None), // NULL: skipped
        (4, Some(vec![1.0, 0.0])),
    ]
}

#[test]
fn inclusive_boundary_and_null_skip() {
    let cfg = RadiusConfig::new(1.0).unwrap();
    let out = radius_search(&[1.0, 0.0], &rows(), SearchMetric::L2, &cfg, None).unwrap();
    // Distances: row1=0, row2=sqrt(2)>1, row4=0.
    let ids: Vec<u64> = out.iter().map(|c| c.row_id).collect();
    assert_eq!(ids, vec![1, 4]);
}

#[test]
fn limit_and_unsorted_order() {
    let cfg = RadiusConfig::new(10.0).unwrap().with_limit(1);
    let out = radius_search(&[1.0, 0.0], &rows(), SearchMetric::L2, &cfg, None).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].row_id, 1);

    let cfg = RadiusConfig::new(10.0).unwrap().with_sort(false);
    let out = radius_search(&[1.0, 0.0], &rows(), SearchMetric::L2, &cfg, None).unwrap();
    let ids: Vec<u64> = out.iter().map(|c| c.row_id).collect();
    assert_eq!(ids, vec![1, 2, 4]);
}

#[test]
fn invalid_radius_rejected() {
    assert!(RadiusConfig::new(-1.0).is_err());
    assert!(RadiusConfig::new(f32::NAN).is_err());
    let bad = RadiusConfig {
        radius: -2.0,
        limit: None,
        sort: true,
    };
    assert!(radius_search(&[1.0], &rows(), SearchMetric::L2, &bad, None).is_err());
}

#[test]
fn cancel_aborts() {
    let cfg = RadiusConfig::new(10.0).unwrap();
    let stop = || true;
    assert_eq!(
        radius_search(&[1.0, 0.0], &rows(), SearchMetric::L2, &cfg, Some(&stop)),
        Err(SearchError::Cancelled)
    );
}
