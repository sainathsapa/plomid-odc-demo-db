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
// Moved from `src/layout/devices.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::DeviceId;
use plomid_storage::layout::devices::{device_dir_name, device_from_dir_name};

#[test]
fn directory_names_round_trip_through_identity() {
    assert_eq!(device_dir_name(DeviceId::new(1)), "D-00000000000000000001");
    assert_eq!(
        device_from_dir_name("D-00000000000000000002"),
        Some(DeviceId::new(2))
    );
    assert_eq!(device_from_dir_name("D-1"), None);
    assert_eq!(device_from_dir_name("T-00000000000000000001"), None);
}
