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
// Moved from `src/device/metadata.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{DeviceId, ErrorKind};
use plomid_storage::device::metadata::{read_device_record, DeviceRecord, DEVICE_META_HEADER_LEN};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-device-meta-{label}-{}-{id}",
        std::process::id(),
    ))
}

#[test]
fn record_round_trips() {
    let record = DeviceRecord::new(DeviceId::new(1), 1u64 << 40).expect("record");
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), DEVICE_META_HEADER_LEN);
    assert_eq!(DeviceRecord::decode(&bytes).expect("decode"), record);
}

#[test]
fn corruption_is_detected() {
    let record = DeviceRecord::new(DeviceId::new(7), 1u64 << 40).expect("record");
    let bytes = record.encode().expect("encode");

    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0xFF;
    assert!(DeviceRecord::decode(&bad_magic).is_err());

    let mut bad_version = bytes.clone();
    bad_version[4] = 0xFF;
    assert!(DeviceRecord::decode(&bad_version).is_err());

    let mut bad_checksum = bytes.clone();
    bad_checksum[36] ^= 0x01;
    assert!(DeviceRecord::decode(&bad_checksum).is_err());

    assert!(DeviceRecord::decode(&bytes[..10]).is_err());
    assert!(DeviceRecord::decode(&[bytes.clone(), vec![0]].concat()).is_err());
}

#[test]
fn zero_device_id_is_rejected() {
    assert!(DeviceRecord::new(DeviceId::new(0), 1u64 << 40).is_err());
}

#[test]
fn zero_capacity_is_rejected() {
    assert!(DeviceRecord::new(DeviceId::new(1), 0).is_err());
}

#[test]
fn publish_and_read_round_trips() {
    let dir = scratch("publish");
    std::fs::create_dir_all(&dir).expect("dir");
    let record = DeviceRecord::new(DeviceId::new(3), 1u64 << 40).expect("record");
    let path = dir.join("DEVICE.dat");
    record.publish(&path).expect("publish");
    let loaded = read_device_record(&path).expect("read");
    assert_eq!(loaded, record);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_record_reports_not_found() {
    let dir = scratch("missing");
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("DEVICE.dat");
    let error = read_device_record(&path).expect_err("absent");
    assert_eq!(error.kind(), ErrorKind::NotFound);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn staging_artifact_is_ignored() {
    let dir = scratch("staged");
    std::fs::create_dir_all(&dir).expect("dir");
    let staged = dir.join("DEVICE.dat.tmp");
    std::fs::write(&staged, b"partial").expect("write");
    assert!(read_device_record(&staged).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}
