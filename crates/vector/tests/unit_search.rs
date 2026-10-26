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
// Moved from `src/search/mod.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::*;

#[test]
fn metric_modes() {
    assert_eq!(SearchMetric::Cosine.default_mode(), TopKMode::Smallest);
    assert_eq!(SearchMetric::L2.default_mode(), TopKMode::Smallest);
    assert_eq!(SearchMetric::NegDot.default_mode(), TopKMode::Smallest);
    assert_eq!(SearchMetric::Dot.default_mode(), TopKMode::Largest);
}

#[test]
fn k_validation() {
    assert!(check_k(0).is_ok());
    assert!(check_k(MAX_TOP_K).is_ok());
    assert_eq!(
        check_k(MAX_TOP_K + 1),
        Err(SearchError::InvalidK { k: MAX_TOP_K + 1 })
    );
}

#[test]
fn cancel_fires_on_first_row() {
    let stop = || true;
    assert_eq!(check_cancel(Some(&stop), 0), Err(SearchError::Cancelled));
    // Not on a non-check row even when the predicate is true.
    assert!(check_cancel(Some(&stop), 1).is_ok());
    assert!(check_cancel(None, 0).is_ok());
}
