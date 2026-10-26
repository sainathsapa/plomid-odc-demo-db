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
#![forbid(unsafe_code)]
//! Aggregation of per-constituent distances into one parent score.
//! Lower-is-better is preserved for every variant.

/// Default parent distance used when no constituent contributes.
pub const MIN_DISTANCE: f32 = 0.0;

/// How constituent distances collapse to a parent distance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Aggregation {
    /// Min variant.
    Min,
    /// Max variant.
    Max,
    /// Avg variant.
    Avg,
    /// Sum variant.
    Sum,
    /// ColBERT-style MaxSim: for each query constituent keep the best
    /// (min-distance) document constituent, then average. Expects the caller
    /// to pass per-query-term minima; for a flat distance list it equals Min.
    MaxSim,
}

/// Aggregate a non-empty distance list; empty input yields `MIN_DISTANCE`.
#[must_use]
pub fn aggregate(agg: Aggregation, distances: &[f32]) -> f32 {
    if distances.is_empty() {
        return MIN_DISTANCE;
    }
    match agg {
        Aggregation::Min | Aggregation::MaxSim => {
            distances.iter().copied().fold(f32::MAX, f32::min)
        }
        Aggregation::Max => distances.iter().copied().fold(f32::MIN, f32::max),
        Aggregation::Avg => {
            let sum: f32 = distances.iter().sum();
            sum / (distances.len() as f32)
        }
        Aggregation::Sum => distances.iter().sum(),
    }
}

/// MaxSim over a per-query-term minima matrix: `minima[q]` is the best
/// document distance for query term `q`; the parent score is their mean.
#[must_use]
pub fn maxsim_from_minima(minima: &[f32]) -> f32 {
    aggregate(Aggregation::Avg, minima)
}
