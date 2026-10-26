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
// Moved from src/join/reorder.rs inline #[cfg(test)].
use plomid_executor::join::reorder::*;

#[test]
fn build_side_picks_smaller() {
    assert_eq!(choose_build_side(10, 1000), BuildSide::Left);
    assert_eq!(choose_build_side(1000, 10), BuildSide::Right);
    // Ties build left (stable probe of the right).
    assert_eq!(choose_build_side(50, 50), BuildSide::Left);
}

#[test]
fn cardinality_estimate_scales() {
    assert_eq!(estimate_join_cardinality(1000, 2000, 0.001), 2000);
    assert_eq!(estimate_join_cardinality(0, 2000, 0.5), 0);
    assert_eq!(estimate_join_cardinality(100, 100, 0.0), 0);
}

#[test]
fn selectivity_one_is_exact_product() {
    // The `perform_join` output-cap pre-check relies on this: selectivity
    // 1.0 must equal `left * right` exactly for in-range inputs (no float
    // overshoot, so no valid under-cap query is rejected by rounding).
    assert_eq!(estimate_join_cardinality(3000, 3000, 1.0), 9_000_000);
    assert_eq!(
        estimate_join_cardinality(100_000, 100_000, 1.0),
        10_000_000_000
    );
}
