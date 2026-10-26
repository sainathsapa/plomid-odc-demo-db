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
use plomid_memory::config::*;

#[test]
fn default_global_budget_is_1gib() {
    assert_eq!(MemoryConfig::default().global_budget, 1 << 30);
}

#[test]
fn default_thresholds_are_70_85_95() {
    let t = PressureThresholds::default();
    assert_eq!((t.warning, t.pressure, t.critical), (0.70, 0.85, 0.95));
}

#[test]
fn with_global_budget_overrides_only_global() {
    let cfg = MemoryConfig::default().with_global_budget(4096);
    assert_eq!(cfg.global_budget, 4096);
    assert_eq!(
        cfg.query_budget_default,
        MemoryConfig::default().query_budget_default
    );
}
