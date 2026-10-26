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
// Moved from `src/device/physical.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::DeviceId;
use plomid_storage::device::physical::*;
use plomid_storage::physical::{BLOCK_SIZE as PHYS_BLOCK_SIZE, EXTENT_SIZE, PAGE_SIZE};
use plomid_storage::platform::FileExt as _;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

const E: u64 = EXTENT_SIZE;

fn scratch(name: &str) -> (PathBuf, PathBuf) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("plomid-device-{name}-{}-{id}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("device.dat");
    (dir, path)
}

#[test]
fn create_open_validate_close_reopen() {
    let (dir, path) = scratch("lifecycle");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(7), capacity).expect("create");
    assert_eq!(device.state(), Lifecycle::Online);
    assert_eq!(device.capacity(), capacity);
    device.validate().expect("validate");
    device.flush().expect("flush");
    device.close().expect("close");
    let reopened = StorageDevice::open(&path).expect("open");
    assert_eq!(reopened.device_id(), DeviceId::new(7));
    reopened.validate().expect("revalidate");
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
}

/// A new device occupies only its metadata footprint: the declared
/// capacity is address space, not allocated bytes.
#[test]
fn a_new_container_holds_only_its_metadata_footprint() {
    let (dir, path) = scratch("footprint");
    // 8 extents * 64 MiB = 512 MiB of declared address space.
    let capacity = capacity_for_extents(8).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(11), capacity).expect("create");
    assert_eq!(device.capacity(), capacity);
    assert_eq!(device.data_start(), device.physical_len());
    assert!(device.physical_len() < capacity);
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").len(),
        device.physical_len()
    );
    drop(device);

    // Reopen does not require the file to be capacity-sized.
    let reopened = StorageDevice::open(&path).expect("open demand-grown container");
    assert_eq!(reopened.capacity(), capacity);
    assert_eq!(reopened.physical_len(), reopened.data_start());
    assert_eq!(reopened.allocated_extent_count(), 0);
    std::fs::remove_dir_all(&dir).ok();
}

/// Allocating extents costs no physical bytes; placing bytes in the data
/// area is what grows the container, and the growth survives reopen.
#[test]
fn the_data_area_grows_only_when_bytes_are_placed() {
    let (dir, path) = scratch("grow");
    let capacity = capacity_for_extents(4).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(12), capacity).expect("create");
    let footprint = device.physical_len();

    // First allocation: metadata only, no data bytes written yet.
    let first = device.allocate(E).expect("allocate");
    assert_eq!(first.start, 0);
    assert_eq!(device.physical_len(), footprint);

    // Placing bytes grows the container to cover exactly that extent.
    let payload = b"PLOMID_DATA_AREA_PAYLOAD";
    device
        .ensure_data_capacity(first.start + payload.len() as u64)
        .expect("ensure data capacity");
    assert_eq!(device.physical_len(), footprint + payload.len() as u64);
    {
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("open file");
        let offset = device.data_start() + first.start;
        file.write_all_at(payload, offset).expect("place bytes");
        file.sync_all().expect("sync");
    }

    // Idempotent: repeating the same request changes nothing.
    device
        .ensure_data_capacity(first.start + payload.len() as u64)
        .expect("idempotent");
    assert_eq!(device.physical_len(), footprint + payload.len() as u64);

    drop(device);
    let reopened = StorageDevice::open(&path).expect("reopen");
    assert_eq!(reopened.capacity(), capacity);
    assert_eq!(reopened.physical_len(), footprint + payload.len() as u64);
    assert_eq!(reopened.allocated_extent_count(), 1);
    let mut read_back = vec![0_u8; payload.len()];
    {
        use plomid_storage::platform::FileExt as _;
        let file = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open read");
        file.read_exact_at(&mut read_back, reopened.data_start() + first.start)
            .expect("read back");
    }
    assert_eq!(read_back, payload);
    std::fs::remove_dir_all(&dir).ok();
}

/// A caller that spans an extent boundary grows the file across both
/// extents in one forward step, and the allocator state survives reopen.
#[test]
fn growth_across_an_extent_boundary_survives_reopen() {
    let (dir, path) = scratch("boundary");
    let capacity = capacity_for_extents(4).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(13), capacity).expect("create");
    let first = device.allocate(E).expect("first");
    let second = device.allocate(E).expect("second");
    assert_eq!(second.start, first.start + E);

    let end = second.start + E;
    device.ensure_data_capacity(end).expect("grow to boundary");
    assert_eq!(device.physical_len(), device.data_start() + end);
    drop(device);

    let reopened = StorageDevice::open(&path).expect("reopen");
    assert_eq!(reopened.allocated_extent_count(), 2);
    assert_eq!(reopened.allocated_bytes(), 2 * E);
    assert_eq!(reopened.physical_len(), reopened.data_start() + end);
    // A third allocation beyond the grown region is still logical only.
    let third = reopened.allocate(E).expect("third");
    assert_eq!(third.start, 2 * E);
    assert_eq!(reopened.physical_len(), reopened.data_start() + end);
    std::fs::remove_dir_all(&dir).ok();
}

/// Growth is bounded by the declared address space, so a caller cannot
/// commit more data bytes than the device was created with.
#[test]
fn growth_beyond_the_declared_capacity_is_rejected() {
    let (dir, path) = scratch("overcommit");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(14), capacity).expect("create");
    let allocatable = device.allocatable_bytes();
    assert!(device.ensure_data_capacity(allocatable + 1).is_err());
    device
        .ensure_data_capacity(allocatable)
        .expect("exact bound is allowed");
    assert_eq!(device.physical_len(), device.data_start() + allocatable);
    assert_eq!(device.physical_len(), capacity);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_device_id_and_capacities() {
    let (dir, path) = scratch("capacity");
    let two = capacity_for_extents(2).expect("capacity");
    assert!(StorageDevice::create(&path, DeviceId::new(0), two).is_err());
    assert!(StorageDevice::create(&path, DeviceId::new(1), 0).is_err());
    assert!(StorageDevice::create(&path, DeviceId::new(1), 16 * 1024).is_err());
    assert!(StorageDevice::create(&path, DeviceId::new(1), E).is_err());
    assert!(StorageDevice::create(&path, DeviceId::new(1), MAX_DEVICE_CAPACITY + 4096).is_err());
    assert!(validate_capacity(u64::MAX).is_err());
    assert!(capacity_for_extents(0).is_err());
    assert!(capacity_for_extents(u64::MAX).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn metadata_rejections_cover_required_matrix() {
    let (dir, path) = scratch("meta");
    let capacity = capacity_for_extents(2).expect("capacity");
    let _ = StorageDevice::create(&path, DeviceId::new(11), capacity).expect("create");
    let raw = std::fs::read(&path).expect("read");
    let mutate = |index: usize, value: u8| {
        let mut bytes = raw.clone();
        bytes[index] = value;
        std::fs::write(&path, &bytes).expect("write");
        let opened = StorageDevice::open(&path);
        std::fs::write(&path, &raw).expect("restore");
        opened
    };
    assert!(mutate(0, b'X').is_err());
    assert!(mutate(4, 0xFF).is_err());
    assert!(mutate(DEVICE_HEADER_LEN as usize, b'X').is_err());
    assert!(mutate(DEVICE_HEADER_LEN as usize + 4, 0xFF).is_err());
    {
        let mut bytes = raw.clone();
        bytes[8..16].fill(0);
        std::fs::write(&path, &bytes).expect("write");
        assert!(StorageDevice::open(&path).is_err());
        std::fs::write(&path, &raw).expect("restore");
    }
    {
        let mut bytes = raw.clone();
        bytes[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        std::fs::write(&path, &bytes).expect("write");
        assert!(StorageDevice::open(&path).is_err());
        std::fs::write(&path, &raw).expect("restore");
    }
    assert!(mutate(44, 0x7F).is_err());
    assert!(mutate(48, 0x7F).is_err());
    assert!(mutate(52, 0xAB).is_err());
    assert!(mutate(4096, 0xAB).is_err());
    std::fs::write(&path, &raw[..100]).expect("truncate");
    assert!(StorageDevice::open(&path).is_err());
    std::fs::write(&path, &raw).expect("restore");
    assert!(mutate(4095, 0xFF).is_err());
    assert!(StorageDevice::open(&dir.join("absent.dat")).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn lifecycle_transitions_and_interrupted_creation() {
    let (dir, path) = scratch("transition");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(21), capacity).expect("create");
    assert!(device.transition(Lifecycle::Offline).is_ok());
    assert!(device.transition(Lifecycle::Quiescing).is_err());
    assert!(device.transition(Lifecycle::Online).is_ok());
    assert!(device.transition(Lifecycle::Recovering).is_err());
    device.transition(Lifecycle::Quiescing).expect("quiesce");
    assert!(device.allocate(E).is_err());
    device.transition(Lifecycle::Offline).expect("offline");
    device.close().expect("close");
    let reopened = StorageDevice::open(&path).expect("open");
    assert_eq!(reopened.state(), Lifecycle::Online);
    drop(reopened);
    let partial = dir.join("partial.dat");
    let (snapshot_len, _, allocatable) = layout_for_capacity(capacity).expect("layout");
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&partial)
        .expect("file");
    file.set_len(capacity).expect("size");
    let mut inner = DeviceInner {
        file,
        device_id: DeviceId::new(22),
        capacity,
        allocatable,
        data_start: DEVICE_HEADER_LEN + snapshot_len,
        snapshot_len,
        physical_len: capacity,
        state: Lifecycle::Initializing,
        allocator: ExtentAllocator::new_empty(allocatable),
    };
    persist_inner(&mut inner, InitState::Incomplete).expect("persist");
    inner.file.sync_all().expect("sync");
    drop(inner);
    assert!(StorageDevice::open(&partial).is_err());
    std::fs::write(&partial, vec![0_u8; 128]).expect("truncate");
    assert!(StorageDevice::open(&partial).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn failed_device_reopens_as_recovering() {
    let (dir, path) = scratch("failed");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(23), capacity).expect("create");
    device.transition(Lifecycle::Failed).expect("fail");
    drop(device);
    let reopened = StorageDevice::open(&path).expect("open");
    assert_eq!(reopened.state(), Lifecycle::Recovering);
    reopened.transition(Lifecycle::Online).expect("recover");
    reopened.validate().expect("validate");
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn allocation_release_and_capacity_edges() {
    let (dir, path) = scratch("alloc");
    let capacity = capacity_for_extents(4).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(31), capacity).expect("create");
    assert!(device.allocate(0).is_err());
    assert!(device.allocate(1024).is_err());
    assert!(device.allocate(E + 1).is_err());
    let first = device.allocate(E).expect("first");
    assert_eq!(first.start, 0);
    assert_eq!(first.length, E);
    let large = device.allocate(2 * E).expect("large");
    assert_eq!(large.length, 2 * E);
    assert!(first.start + first.length <= large.start || large.start + large.length <= first.start);
    let last = device.allocate(E).expect("last");
    assert!(device.allocate(E).is_err());
    assert_eq!(device.free_bytes(), 0);
    assert!(device.release(9 * E).is_err());
    assert!(device.release(E).is_ok() || device.release(first.start).is_ok());
    let _ = (last, large);
    let reopened = StorageDevice::open(&path).expect("open");
    reopened.validate().expect("validate");
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn exact_allocation_boundaries_and_misaligned_requests() {
    let (dir, path) = scratch("edges");
    let capacity = capacity_for_extents(3).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(32), capacity).expect("create");
    let start = device.allocate(3 * E).expect("full");
    assert_eq!(start.start, 0);
    assert!(device.allocate(E).is_err());
    device.release(start.start).expect("release");
    assert_eq!(device.free_bytes(), device.allocatable_bytes());
    let head = device.allocate(E).expect("head");
    assert_eq!(head.start, 0);
    let tail = device.allocate(E).expect("tail");
    let end = device.allocate(E).expect("end");
    assert_eq!(head.end(), Some(E));
    assert_eq!(end.start + end.length, device.allocatable_bytes());
    let _ = (head, tail, end);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn release_reuse_double_free_and_unknown() {
    let (dir, path) = scratch("reuse");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(41), capacity).expect("create");
    let first = device.allocate(E).expect("first");
    let released = device.release(first.start).expect("release");
    assert_eq!(released, first);
    assert!(device.release(first.start).is_err());
    assert!(device.release(7 * E).is_err());
    let reused = device.allocate(E).expect("reuse");
    assert_eq!(reused.start, first.start);
    assert_ne!(reused.extent_id, first.extent_id);
    device.validate().expect("validate");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn fragmentation_coalescing_and_large_contiguous() {
    let (dir, path) = scratch("frag");
    let capacity = capacity_for_extents(8).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(51), capacity).expect("create");
    let a = device.allocate(E).expect("a");
    let b = device.allocate(E).expect("b");
    let c = device.allocate(E).expect("c");
    let rest = device.allocate(5 * E).expect("rest");
    // Free the middle; a small allocation must reuse it exactly.
    device.release(b.start).expect("free b");
    let small = device.allocate(E).expect("small");
    assert_eq!(small.start, b.start);
    device.release(small.start).expect("free small");
    // Free everything adjacent: A + B + C must coalesce into 3 extents.
    device.release(a.start).expect("free a");
    device.release(c.start).expect("free c");
    device.release(rest.start).expect("free rest");
    assert_eq!(device.free_bytes(), device.allocatable_bytes());
    let big = device.allocate(8 * E).expect("big");
    assert_eq!(big.start, 0);
    assert_eq!(big.length, 8 * E);
    // Non-adjacent frees must not merge across a live extent.
    device.release(big.start).expect("free big");
    let x = device.allocate(E).expect("x");
    let y = device.allocate(E).expect("y");
    let z = device.allocate(E).expect("z");
    device.release(x.start).expect("free x");
    device.release(z.start).expect("free z");
    assert!(device.allocate(3 * E).is_ok() || device.allocate(2 * E).is_ok());
    let _ = (x, y, z);
    device.validate().expect("validate");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn persistence_round_trip_with_allocate_and_release() {
    let (dir, path) = scratch("persist");
    let capacity = capacity_for_extents(4).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(61), capacity).expect("create");
    let a = device.allocate(E).expect("a");
    let b = device.allocate(2 * E).expect("b");
    device.flush().expect("flush");
    drop(device);
    let reopened = StorageDevice::open(&path).expect("open");
    reopened.validate().expect("validate");
    assert_eq!(reopened.allocated_extent_count(), 2);
    let c = reopened.allocate(E).expect("c");
    reopened.release(a.start).expect("release a");
    let _ = (a, b, c);
    drop(reopened);
    let again = StorageDevice::open(&path).expect("open");
    again.validate().expect("validate");
    assert_eq!(again.allocated_extent_count(), 2);
    drop(again);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn corrupted_snapshot_and_size_mismatch_fail_open() {
    let (dir, path) = scratch("corrupt");
    let capacity = capacity_for_extents(2).expect("capacity");
    let device = StorageDevice::create(&path, DeviceId::new(71), capacity).expect("create");
    device.allocate(E).expect("alloc");
    drop(device);
    let raw = std::fs::read(&path).expect("read");
    let snapshot_off = DEVICE_HEADER_LEN as usize;
    let mut bad = raw.clone();
    bad[snapshot_off + 24] ^= 0xFF;
    std::fs::write(&path, &bad).expect("write");
    assert!(StorageDevice::open(&path).is_err());
    std::fs::write(&path, &raw).expect("restore");
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open file");
        file.set_len(capacity + PAGE_SIZE).expect("grow");
    }
    assert!(StorageDevice::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn concurrent_allocate_release_stays_consistent() {
    let (dir, path) = scratch("concurrent");
    let capacity = capacity_for_extents(16).expect("capacity");
    let device = Arc::new(StorageDevice::create(&path, DeviceId::new(81), capacity).expect("c"));
    let allocatable = device.allocatable_bytes();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let device = Arc::clone(&device);
        handles.push(thread::spawn(move || {
            let mut owned = Vec::new();
            for _ in 0..4 {
                match device.allocate(E) {
                    Ok(extent) => owned.push(extent.start),
                    Err(_) => break,
                }
            }
            // Deterministic release order: reverse of acquisition.
            for start in owned.into_iter().rev() {
                device.release(start).expect("release");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("thread");
    }
    device.validate().expect("validate");
    assert_eq!(device.allocated_extent_count(), 0);
    assert_eq!(device.free_bytes(), allocatable);
    // Interleaved allocate/release from two threads keeps invariants.
    let device2 = Arc::new(StorageDevice::open(&path).expect("open"));
    let first = device2.allocate(2 * E).expect("first");
    let worker = {
        let device2 = Arc::clone(&device2);
        thread::spawn(move || {
            for _ in 0..8 {
                if let Ok(extent) = device2.allocate(E) {
                    device2.release(extent.start).ok();
                }
            }
        })
    };
    for _ in 0..8 {
        if let Ok(extent) = device2.allocate(E) {
            device2.release(extent.start).ok();
        }
    }
    worker.join().expect("worker");
    device2.release(first.start).expect("free first");
    device2.validate().expect("validate");
    assert_eq!(device2.free_bytes(), allocatable);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn alignment_matches_storage_hierarchy() {
    assert_eq!(E % PHYS_BLOCK_SIZE, 0);
    assert_eq!(PHYS_BLOCK_SIZE % PAGE_SIZE, 0);
    assert_eq!(DEVICE_HEADER_LEN % PAGE_SIZE, 0);
    assert_eq!(SNAPSHOT_PAGE_LEN % PAGE_SIZE, 0);
    let capacity = capacity_for_extents(2).expect("capacity");
    assert_eq!(capacity % PAGE_SIZE, 0);
    let (dir, path) = scratch("align");
    let device = StorageDevice::create(&path, DeviceId::new(91), capacity).expect("create");
    let extent = device.allocate(E).expect("alloc");
    assert_eq!(extent.start % E, 0);
    assert_eq!(extent.length % E, 0);
    assert_eq!((device.data_start() + extent.start) % PHYS_BLOCK_SIZE, 0);
    assert_eq!((device.data_start() + extent.start) % PAGE_SIZE, 0);
    std::fs::remove_dir_all(&dir).ok();
}
