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
// Moved from `src/config.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::config::*;

#[test]
fn default_config_is_valid() {
    assert!(HtapConfig::default().validate().is_ok());
}

#[test]
fn pressure_mapping_uses_default_thresholds() {
    assert_eq!(PressureState::for_utilization(0), PressureState::Normal);
    assert_eq!(PressureState::for_utilization(69), PressureState::Normal);
    assert_eq!(PressureState::for_utilization(70), PressureState::Warning);
    assert_eq!(PressureState::for_utilization(84), PressureState::Warning);
    assert_eq!(PressureState::for_utilization(85), PressureState::Pressure);
    assert_eq!(PressureState::for_utilization(94), PressureState::Pressure);
    assert_eq!(PressureState::for_utilization(95), PressureState::Critical);
    assert_eq!(PressureState::for_utilization(100), PressureState::Critical);
}

#[test]
fn thresholds_are_configurable() {
    let cfg = HtapConfig {
        warning_threshold_pct: 50,
        pressure_threshold_pct: 60,
        critical_threshold_pct: 70,
        ..HtapConfig::default()
    };
    assert_eq!(cfg.pressure_for(55), PressureState::Warning);
    assert_eq!(cfg.pressure_for(65), PressureState::Pressure);
    assert_eq!(cfg.pressure_for(99), PressureState::Critical);
}

#[test]
fn overcommit_is_rejected() {
    let mut cfg = HtapConfig::default();
    cfg.olap_budget_bytes = cfg.global_budget_bytes;
    let err = cfg.validate().expect_err("overcommit must fail");
    assert!(err.reason().contains("overcommit"), "{}", err.reason());
}

#[test]
fn bad_thresholds_and_caps_rejected() {
    let cfg = HtapConfig {
        warning_threshold_pct: 90,
        ..HtapConfig::default()
    };
    assert!(cfg.validate().is_err());
    let cfg = HtapConfig {
        olap_max_concurrency: 0,
        ..HtapConfig::default()
    };
    assert!(cfg.validate().is_err());
}
