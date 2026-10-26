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
// Moved from `src/device/registration.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{DeviceId, ErrorKind};
use plomid_storage::device::physical::capacity_for_extents;
use plomid_storage::device::registration::{default_device_capacity, register_device};
use plomid_storage::device::registry::DeviceRegistry;
use plomid_storage::layout::DatabaseLayout;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-device-registration-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn registration_is_idempotent() {
    let root = scratch("idempotent");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("initialize");
    let device_id = DeviceId::new(1);
    let capacity = default_device_capacity().expect("default capacity");
    register_device(&layout, device_id, capacity).expect("first");
    register_device(&layout, device_id, capacity).expect("second");
    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert_eq!(registry.len(), 1);
    assert!(layout.device_physical_path(device_id).is_file());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn an_existing_device_keeps_its_identity_and_capacity() {
    let root = scratch("conflict");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("initialize");
    let device_id = DeviceId::new(1);
    let capacity = capacity_for_extents(1).expect("capacity");
    register_device(&layout, device_id, capacity).expect("register");
    let error = register_device(&layout, device_id, capacity * 4).expect_err("resize");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert_eq!(registry.get(device_id).expect("entry").capacity(), capacity);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_zero_device_identity_is_rejected() {
    let root = scratch("zero");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("initialize");
    assert!(register_device(
        &layout,
        DeviceId::new(0),
        default_device_capacity().expect("cap")
    )
    .is_err());
    let _ = std::fs::remove_dir_all(&root);
}
