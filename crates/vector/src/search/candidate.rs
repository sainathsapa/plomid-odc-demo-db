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
//! Search result rows and candidate-set sizing.

use std::cmp::Ordering;
use std::fmt;

/// One scored row: a stable row id plus its distance from the query.
///
/// Ordering is total and deterministic: first by `distance` with
/// [`f32::total_cmp`] (NaN sorts last, i.e. worst), then by `row_id`
/// ascending. Equality is bitwise on the distance plus the row id, so it
/// agrees with the ordering.
#[derive(Debug, Clone, Copy)]
pub struct Candidate {
    /// Stable row identifier (heap pointer, MVCC row id, ...).
    pub row_id: u64,
    /// Distance from the query (smaller = closer for distance metrics).
    pub distance: f32,
}

impl Candidate {
    /// Build a candidate.
    #[must_use]
    pub fn new(row_id: u64, distance: f32) -> Self {
        Self { row_id, distance }
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.row_id == other.row_id && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.row_id.cmp(&other.row_id))
    }
}

impl fmt::Display for Candidate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "row {} @ {}", self.row_id, self.distance)
    }
}

/// Default multiplier applied to `k` when fetching ANN candidates for rerank.
pub const DEFAULT_OVERSAMPLE_FACTOR: usize = 4;

/// Hard cap applied to candidate counts; bounds memory for hostile inputs.
pub const MAX_CANDIDATE_CAP: usize = 1_048_576;

/// Candidate count for an oversampled fetch: `min(k * factor, hard_cap)`
/// with checked multiplication (overflow saturates to the cap).
///
/// Returns `0` when `k == 0` or `factor == 0`.
#[must_use]
pub fn oversample_count(k: usize, factor: usize, hard_cap: usize) -> usize {
    if k == 0 || factor == 0 {
        return 0;
    }
    let cap = hard_cap.min(MAX_CANDIDATE_CAP);
    match k.checked_mul(factor) {
        Some(n) => n.min(cap),
        None => cap,
    }
}
