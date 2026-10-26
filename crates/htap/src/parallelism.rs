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

//! Adaptive parallelism: OLAP derates 8 -> 4 -> 2 -> 1 with pressure.

use crate::config::PressureState;
use crate::workload::WorkloadClass;

/// Degree of parallelism for `class` under `pressure` given `requested`.
///
/// - Latency-sensitive classes (OLTP / system / interactive analytics) are
///   never derated: they get exactly what they asked for.
/// - Throughput / background classes derate: `Normal => requested`,
///   `Warning => requested/2`, `Pressure => requested/4`, `Critical => 1`.
///   With `requested = 8` that is the 8 -> 4 -> 2 -> 1 ladder.
pub fn parallelism_for(class: WorkloadClass, pressure: PressureState, requested: usize) -> usize {
    if requested == 0 {
        return 0;
    }
    if class.is_latency_sensitive() {
        return requested;
    }
    match pressure {
        PressureState::Normal => requested,
        PressureState::Warning => (requested / 2).max(1),
        PressureState::Pressure => (requested / 4).max(1),
        PressureState::Critical => 1,
    }
}

/// Derate a concurrency cap with pressure using the same ladder shape:
/// full at `Normal`, halved at `Warning`, quartered at `Pressure`,
/// exactly 1 at `Critical`. Never returns 0 for `max >= 1`.
pub fn derate_concurrency(max: usize, pressure: PressureState) -> usize {
    let max = max.max(1);
    match pressure {
        PressureState::Normal => max,
        PressureState::Warning => (max / 2).max(1),
        PressureState::Pressure => (max / 4).max(1),
        PressureState::Critical => 1,
    }
}
