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
// Moved from `src/pressure.rs` inline `#[cfg(test)]` — public-API only.
use plomid_memory::config::PressureThresholds;
use plomid_memory::pressure::*;

#[test]
fn default_threshold_boundaries() {
    let t = PressureThresholds::default();
    assert_eq!(PressureState::from_usage(0.0, &t), PressureState::Normal);
    assert_eq!(PressureState::from_usage(0.6999, &t), PressureState::Normal);
    assert_eq!(PressureState::from_usage(0.70, &t), PressureState::Warning);
    assert_eq!(
        PressureState::from_usage(0.8499, &t),
        PressureState::Warning
    );
    assert_eq!(PressureState::from_usage(0.85, &t), PressureState::Pressure);
    assert_eq!(
        PressureState::from_usage(0.9499, &t),
        PressureState::Pressure
    );
    assert_eq!(PressureState::from_usage(0.95, &t), PressureState::Critical);
    assert_eq!(PressureState::from_usage(1.5, &t), PressureState::Critical);
    assert_eq!(
        PressureState::from_usage(f64::NAN, &t),
        PressureState::Normal
    );
    assert_eq!(
        PressureState::from_usage(f64::INFINITY, &t),
        PressureState::Critical
    );
}

#[test]
fn custom_thresholds_apply() {
    let t = PressureThresholds::new(0.5, 0.75, 0.9);
    assert_eq!(PressureState::from_usage(0.6, &t), PressureState::Warning);
    assert_eq!(PressureState::from_usage(0.8, &t), PressureState::Pressure);
}
