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
// Moved from `src/device/state.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_storage::checksum::compute_checksum;
use plomid_storage::device::physical::Lifecycle;
use plomid_storage::device::state::{
    read_device_state, DeviceStateRecord, DEVICE_STATE_HEADER_LEN, DEVICE_STATE_MAGIC,
    DEVICE_STATE_VERSION,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-device-state-{label}-{}-{id}",
        std::process::id(),
    ))
}

#[test]
fn record_round_trips() {
    let record = DeviceStateRecord::new(Lifecycle::Online);
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), DEVICE_STATE_HEADER_LEN);
    assert_eq!(DeviceStateRecord::decode(&bytes).expect("decode"), record);
}

#[test]
fn corruption_is_detected() {
    let record = DeviceStateRecord::new(Lifecycle::Quiescing);
    let bytes = record.encode().expect("encode");

    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(DeviceStateRecord::decode(&bad_magic).is_err());

    let mut bad_version = bytes.clone();
    bad_version[4] = 0xFF;
    assert!(DeviceStateRecord::decode(&bad_version).is_err());

    let mut bad_checksum = bytes.clone();
    bad_checksum[24] ^= 0x01;
    assert!(DeviceStateRecord::decode(&bad_checksum).is_err());

    assert!(DeviceStateRecord::decode(&bytes[..10]).is_err());
}

#[test]
fn invalid_state_value_is_rejected() {
    let mut bytes = vec![0u8; DEVICE_STATE_HEADER_LEN];
    bytes[0..4].copy_from_slice(&DEVICE_STATE_MAGIC);
    bytes[4..8].copy_from_slice(&DEVICE_STATE_VERSION.to_le_bytes());
    bytes[8..12].copy_from_slice(&(DEVICE_STATE_HEADER_LEN as u32).to_le_bytes());
    bytes[12..16].copy_from_slice(&999_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&1000_u64.to_le_bytes());
    let checksum = compute_checksum(&bytes[..24]);
    bytes[24..28].copy_from_slice(&checksum.to_le_bytes());
    assert!(DeviceStateRecord::decode(&bytes).is_err());
}

#[test]
fn publish_and_read_round_trips() {
    let dir = scratch("publish");
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("STATE.dat");
    let record = DeviceStateRecord::new(Lifecycle::Offline);
    record.publish(&path).expect("publish");
    let loaded = read_device_state(&path).expect("read");
    assert_eq!(loaded, record);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_state_reports_not_found() {
    let dir = scratch("missing");
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("STATE.dat");
    let error = read_device_state(&path).expect_err("absent");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    let _ = std::fs::remove_dir_all(&dir);
}
