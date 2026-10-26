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
// Moved from `src/storage/policy.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::storage::policy::*;
use plomid_vector::storage::VectorStorageAdapter as _;
use plomid_vector::validation::MAX_PAYLOAD_BYTES;
use plomid_vector::value::DenseVectorValue;
use plomid_vector::{VectorElementType, VectorValue};

fn dense(vals: &[f32]) -> VectorValue {
    VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vals.to_vec()).unwrap(),
    )
}

fn cap(mmap: bool, class: StorageClass) -> StorageCapability {
    StorageCapability::test_seam(mmap, class, Some(16 * 1024 * 1024 * 1024))
}

fn dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "plomid-vector-policy-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.subsec_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn tiny_selects_memory_medium_selects_file_large_selects_mmap() {
    let policy = VectorStoragePolicy::default();
    let c = cap(true, StorageClass::Nvme);
    assert_eq!(
        policy.select(512, &c).mode,
        StorageMode::Memory,
        "tiny index must be memory resident"
    );
    assert_eq!(
        policy.select(8 * 1024 * 1024, &c).mode,
        StorageMode::File,
        "medium index must be plain file"
    );
    assert_eq!(
        policy.select(256 * 1024 * 1024, &c).mode,
        StorageMode::Mmap,
        "large index on NVMe must prefer mmap"
    );
}

#[test]
fn mmap_unavailable_falls_back_to_file_with_flag() {
    let policy = VectorStoragePolicy::default();
    let c = cap(false, StorageClass::Ssd);
    let d = policy.select(256 * 1024 * 1024, &c);
    assert_eq!(d.mode, StorageMode::File);
    assert!(d.fallback_active);
    assert!(!d.mmap_available);
}

#[test]
fn mmap_disabled_falls_back() {
    let mut policy = VectorStoragePolicy::default();
    policy.mmap.enabled = false;
    let c = cap(true, StorageClass::Nvme);
    let d = policy.select(256 * 1024 * 1024, &c);
    assert_eq!(d.mode, StorageMode::File);
    assert!(d.fallback_active);
}

#[test]
fn memory_pressure_spills_tiny_to_file() {
    let policy = VectorStoragePolicy {
        memory_budget_bytes: 1024,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Nvme);
    // Even a tiny 4 KiB index exceeds the 1 KiB budget.
    let d = policy.select(4096, &c);
    assert!(!d.is_memory_resident());
}

#[test]
fn explicit_memory_over_budget_fails_closed_to_file() {
    let policy = VectorStoragePolicy::memory_only(1024);
    let c = cap(true, StorageClass::Nvme);
    let d = policy.select(1_000_000, &c);
    assert_eq!(d.mode, StorageMode::File);
    assert!(d.fallback_active);
}

#[test]
fn hdd_avoids_mmap_random_faults() {
    let policy = VectorStoragePolicy::default();
    let c = cap(true, StorageClass::Hdd);
    let d = policy.select(256 * 1024 * 1024, &c);
    assert_eq!(d.mode, StorageMode::File, "HDD must not mmap large indexes");
}

#[test]
fn storage_pressure_unknown_class_never_assumes_fast() {
    let policy = VectorStoragePolicy::default();
    let c = cap(true, StorageClass::Unknown);
    let d = policy.select(256 * 1024 * 1024, &c);
    // Unknown storage still allows mmap (safe: file-backed), but the
    // decision records the unknown class honestly.
    assert_eq!(d.storage_class, StorageClass::Unknown);
    assert!(d.mode == StorageMode::Mmap || d.mode == StorageMode::File);
}

#[test]
fn diagnostics_answer_the_seven_questions() {
    let policy = VectorStoragePolicy::default();
    let c = cap(true, StorageClass::Nvme);
    let d = policy.select(256 * 1024 * 1024, &c);
    let lines = d.explain();
    assert_eq!(lines.len(), 8);
    assert!(lines.iter().any(|l| l.starts_with("Storage mode:")));
    assert!(lines.iter().any(|l| l.starts_with("Memory resident:")));
    assert!(lines.iter().any(|l| l.starts_with("Mmap backed:")));
    assert!(lines.iter().any(|l| l.starts_with("Reason:")));
    assert!(lines.iter().any(|l| l.starts_with("Memory budget:")));
    assert!(lines.iter().any(|l| l.starts_with("Storage capability:")));
    assert!(lines.iter().any(|l| l.starts_with("Mmap available:")));
    assert!(lines.iter().any(|l| l.starts_with("Fallback active:")));
    assert!(d.is_mmap_backed());
    assert!(!d.is_memory_resident());
}

#[test]
fn managed_memory_only_roundtrip() {
    let policy = VectorStoragePolicy::memory_only(64 * 1024 * 1024);
    let c = cap(true, StorageClass::Nvme);
    let mut s = ManagedVectorStorage::create(
        dir("mem"),
        "t".into(),
        "c".into(),
        Some(2),
        &policy,
        512,
        &c,
        None,
    )
    .unwrap();
    assert!(s.is_memory_resident());
    assert!(!s.is_mmap_backed());
    assert_eq!(s.append_payload(&dense(&[1.0, 2.0])).unwrap(), 0);
    assert_eq!(s.read_payload(0).unwrap(), dense(&[1.0, 2.0]));
    assert_eq!(s.prefetch(0, 10).unwrap(), 1);
    assert!(s.is_warm());
    s.flush().unwrap();
    let decision = s.close().unwrap();
    assert_eq!(decision.mode, StorageMode::Memory);
}

#[test]
fn managed_file_backed_restart_replays() {
    let d = dir("restart");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Ssd);
    let mut s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(2),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    assert_eq!(s.storage_mode(), StorageMode::File);
    s.append_payload(&dense(&[1.0, 2.0])).unwrap();
    s.append_payload(&dense(&[3.0, 4.0])).unwrap();
    s.checkpoint(7).unwrap();
    s.close().unwrap();
    // Cold start: nothing warmed yet.
    let (mut reopened, recovery) = ManagedVectorStorage::open(
        &d,
        "t".into(),
        "c".into(),
        Some(2),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    assert_eq!(recovery.replayed_records, 2);
    assert!(!reopened.is_warm());
    assert_eq!(reopened.read_payload(1).unwrap(), dense(&[3.0, 4.0]));
    // Warm start: prefetch then sequential access agrees.
    assert_eq!(reopened.prefetch(0, 10).unwrap(), 2);
    assert!(reopened.is_warm());
    assert_eq!(reopened.scan_batch(0, 10).unwrap().len(), 2);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn managed_mmap_mode_prefetch_and_sequential_agree() {
    let d = dir("mmapmode");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::Mmap,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Nvme);
    let mut s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    // On this unix CI host mmap is available; elsewhere this falls back
    // to file with the flag set — either way reads must agree.
    for i in 0..16u64 {
        s.append_payload(&dense(&[i as f32])).unwrap();
    }
    let warmed = s.prefetch(0, 16).unwrap();
    assert_eq!(warmed, 16);
    // Random access after warm prefetch returns identical rows.
    for i in 0..16u64 {
        assert_eq!(s.read_payload(i).unwrap(), dense(&[i as f32]));
    }
    // Sequential scan agrees with random reads.
    assert_eq!(s.scan_batch(0, 16).unwrap().len(), 16);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn crash_recovery_truncates_torn_tail() {
    let d = dir("torn");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Ssd);
    let mut s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    s.append_payload(&dense(&[1.0])).unwrap();
    drop(s);
    // Simulate crash mid-append: partial length prefix.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(d.join("records.dat"))
            .unwrap();
        f.write_all(&[0xFF, 0xFF]).unwrap();
        f.flush().unwrap();
    }
    let (reopened, recovery) = ManagedVectorStorage::open(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    assert!(recovery.truncated_tail);
    assert_eq!(reopened.row_count(), 1);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn corrupted_manifest_fails_closed() {
    let d = dir("corrupt");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Ssd);
    let s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    s.close().unwrap();
    std::fs::write(d.join("MANIFEST"), b"not-a-manifest").unwrap();
    assert!(ManagedVectorStorage::validate_image(&d).is_err());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn partial_file_reports_partial_kind() {
    let d = dir("partial");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Ssd);
    let mut s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    s.append_payload(&dense(&[1.0])).unwrap();
    drop(s);
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(d.join("records.dat"))
            .unwrap();
        f.write_all(&[9]).unwrap();
        f.flush().unwrap();
    }
    let v = ManagedVectorStorage::validate_image(&d).unwrap();
    assert_eq!(v.kind, ImageKind::Partial);
    assert!(v.truncated_tail);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn file_growth_and_truncation_semantics() {
    let d = dir("growth");
    let policy = VectorStoragePolicy {
        request: StorageModeRequest::File,
        ..VectorStoragePolicy::default()
    };
    let c = cap(true, StorageClass::Ssd);
    let mut s = ManagedVectorStorage::create(
        &d,
        "t".into(),
        "c".into(),
        Some(1),
        &policy,
        8 << 20,
        &c,
        None,
    )
    .unwrap();
    let before = std::fs::metadata(d.join("records.dat")).unwrap().len();
    s.append_payload(&dense(&[5.0])).unwrap();
    let after = std::fs::metadata(d.join("records.dat")).unwrap().len();
    assert!(after > before, "file must grow on append");
    // Overwrites append-and-repoint (file only grows; never truncates live data).
    s.write_payload(0, &dense(&[6.0])).unwrap();
    let grown = std::fs::metadata(d.join("records.dat")).unwrap().len();
    assert!(grown >= after);
    assert_eq!(s.read_payload(0).unwrap(), dense(&[6.0]));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn oversize_payload_rejected_without_allocation() {
    let policy = VectorStoragePolicy::default();
    let c = cap(true, StorageClass::Nvme);
    let mut s = ManagedVectorStorage::create(
        dir("budget"),
        "t".into(),
        "c".into(),
        None,
        &policy,
        512,
        &c,
        None,
    )
    .unwrap();
    let big = vec![0u8; MAX_PAYLOAD_BYTES + 1];
    assert!(s.append_row_bytes(&big).is_err());
    assert!(s.put_row_bytes(0, &big).is_err());
}

#[test]
fn mode_parsing_roundtrips() {
    assert_eq!(StorageMode::parse("MEMORY").unwrap(), StorageMode::Memory);
    assert_eq!(StorageMode::parse("file").unwrap(), StorageMode::File);
    assert_eq!(StorageMode::parse("mmap").unwrap(), StorageMode::Mmap);
    assert!(StorageMode::parse("cluster").is_err());
    assert_eq!(
        StorageModeRequest::parse("auto").unwrap(),
        StorageModeRequest::Auto
    );
}
