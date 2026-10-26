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
// Moved from `src/parallelism.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::parallelism::*;
use plomid_htap::{PressureState, WorkloadClass};

#[test]
fn olap_ladder_8_4_2_1() {
    use plomid_htap::PressureState::*;
    assert_eq!(parallelism_for(WorkloadClass::Olap, Normal, 8), 8);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Warning, 8), 4);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Pressure, 8), 2);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Critical, 8), 1);
}

#[test]
fn oltp_never_derated() {
    use plomid_htap::PressureState::*;
    for p in [Normal, Warning, Pressure, Critical] {
        assert_eq!(parallelism_for(WorkloadClass::Oltp, p, 8), 8);
        assert_eq!(parallelism_for(WorkloadClass::System, p, 8), 8);
        assert_eq!(
            parallelism_for(WorkloadClass::InteractiveAnalytics, p, 8),
            8
        );
    }
}

#[test]
fn background_derates_like_olap() {
    assert_eq!(
        parallelism_for(WorkloadClass::Batch, PressureState::Pressure, 8),
        2
    );
    assert_eq!(
        parallelism_for(WorkloadClass::Background, PressureState::Critical, 8),
        1
    );
}

#[test]
fn derate_concurrency_never_zero() {
    assert_eq!(derate_concurrency(4, PressureState::Warning), 2);
    assert_eq!(derate_concurrency(4, PressureState::Pressure), 1);
    assert_eq!(derate_concurrency(4, PressureState::Critical), 1);
    assert_eq!(derate_concurrency(1, PressureState::Critical), 1);
}
