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
// Moved from `src/mvcc/maintenance.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::mvcc::maintenance::*;

#[test]
fn metrics() {
    assert_eq!(fragmentation_metric(0, 0), 0.0);
    assert_eq!(fragmentation_metric(100, 100), 0.0);
    assert!((fragmentation_metric(100, 50) - 0.5).abs() < 1e-12);
    let cfg = MaintenanceConfig::default();
    assert!(should_cleanup_tombstones(25, 100, &cfg));
    assert!(!should_cleanup_tombstones(1, 100, &cfg));
    assert_eq!(advise(50, 50, 0, &cfg), MaintenanceAdvice::Healthy);
}
