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
// Moved from `src/maintenance.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::maintenance::*;
use plomid_htap::{MaintenanceThrottle, PressureState};

#[test]
fn throttle_ladder() {
    assert_eq!(throttle(PressureState::Normal), MaintenanceThrottle::Full);
    assert_eq!(throttle(PressureState::Warning), MaintenanceThrottle::Full);
    assert_eq!(
        throttle(PressureState::Pressure),
        MaintenanceThrottle::Degraded
    );
    assert_eq!(
        throttle(PressureState::Critical),
        MaintenanceThrottle::Paused
    );
    assert!(throttle(PressureState::Critical).is_paused());
    assert!(!throttle(PressureState::Pressure).is_paused());
    assert!(throttle(PressureState::Pressure).allows_work());
}
