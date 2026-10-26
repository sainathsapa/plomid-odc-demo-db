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
// Moved from `src/device/registry.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{DeviceId, ErrorKind};
use plomid_storage::device::metadata::DeviceRecord;
use plomid_storage::device::registry::DeviceRegistry;
use plomid_storage::layout::DatabaseLayout;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-device-registry-{label}-{}-{id}",
        std::process::id(),
    ))
}

fn make_device(layout: &DatabaseLayout, id: u64, capacity: u64) {
    let device_id = DeviceId::new(id);
    layout.ensure_device_dirs(device_id).expect("dirs");
    let record = DeviceRecord::new(device_id, capacity).expect("record");
    record
        .publish(&layout.device_meta_path(device_id))
        .expect("publish");
}

#[test]
fn empty_registry_when_no_devices() {
    let root = scratch("empty");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("init");
    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert!(registry.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn discovers_registered_devices() {
    let root = scratch("multi");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("init");
    make_device(&layout, 1, 1u64 << 40);
    make_device(&layout, 2, 1u64 << 41);

    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert_eq!(registry.len(), 2);
    assert!(registry.contains(DeviceId::new(1)));
    assert!(registry.contains(DeviceId::new(2)));
    assert!(!registry.contains(DeviceId::new(3)));
    assert_eq!(registry.total_capacity(), (1u64 << 40) + (1u64 << 41));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn the_next_free_identity_is_the_lowest_unused_one() {
    let root = scratch("next-id");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("init");
    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert_eq!(registry.next_free_id(), DeviceId::new(1));
    make_device(&layout, 1, 1u64 << 34);
    make_device(&layout, 2, 1u64 << 34);
    make_device(&layout, 4, 1u64 << 34);
    let registry = DeviceRegistry::discover(&layout).expect("discover");
    assert_eq!(registry.next_free_id(), DeviceId::new(3));
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn identity_mismatch_is_rejected() {
    let root = scratch("mismatch");
    let layout = DatabaseLayout::new(&root);
    layout.initialize().expect("init");
    // Create device directory for id 1, but write a record for id 2.
    let device_id = DeviceId::new(1);
    layout.ensure_device_dirs(device_id).expect("dirs");
    let wrong_record = DeviceRecord::new(DeviceId::new(2), 1u64 << 40).expect("record");
    wrong_record
        .publish(&layout.device_meta_path(device_id))
        .expect("publish");
    let error = DeviceRegistry::discover(&layout).expect_err("mismatch");
    assert_eq!(error.kind(), ErrorKind::Corruption);
    let _ = std::fs::remove_dir_all(&root);
}
