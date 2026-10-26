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
// Moved from `src/page_manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::page_manager::*;
use plomid_storage::physical::PageType;
use std::io::Seek;
use std::path::Path;
use std::path::PathBuf;

fn scratch(name: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("pages.plpm");
    (dir, path)
}

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open");
    file.seek(std::io::SeekFrom::Start(offset)).expect("seek");
    use std::io::Write as _;
    file.write_all(bytes).expect("write");
}

#[test]
fn round_trip_and_batched_io() {
    let (dir, path) = scratch("pm-rt");
    let mut manager = PageManager::create(&path).expect("create");
    let mut first = manager.allocate_page(PageType::Leaf).expect("alloc a");
    let mut second = manager.allocate_page(PageType::Internal).expect("alloc b");
    first.data_mut()[..4].copy_from_slice(b"aaaa");
    second.data_mut()[..4].copy_from_slice(b"bbbb");
    manager.write_page(&first).expect("write a");
    manager.write_page(&second).expect("write b");
    manager.flush().expect("flush");

    let batch = manager
        .read_pages(&[second.id(), first.id()])
        .expect("batched read");
    assert_eq!(batch.len(), 2);
    assert_eq!(&batch[0].data()[..4], b"bbbb");
    assert_eq!(&batch[1].data()[..4], b"aaaa");

    drop(manager);
    let mut reopened = PageManager::open(&path).expect("open");
    let meta = reopened.validate_page(first.id()).expect("validate");
    assert_eq!(meta.page_id, first.id());
    assert_eq!(meta.page_type, PageType::Leaf);
    reopened.flush().expect("flush");
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_magic() {
    let (dir, path) = scratch("pm-magic");
    let _ = PageManager::create(&path).expect("create");
    write_at(&path, 0, b"XXXX");
    assert!(PageManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_page_size() {
    let (dir, path) = scratch("pm-psize");
    let _ = PageManager::create(&path).expect("create");
    // Page-size field lives at bytes 8..16 of the header.
    let mut bad = [0_u8; 8];
    bad.copy_from_slice(&(4096_u64).to_le_bytes());
    write_at(&path, 8, &bad);
    assert!(PageManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_header_checksum_mismatch() {
    let (dir, path) = scratch("pm-cksum");
    let _ = PageManager::create(&path).expect("create");
    write_at(&path, 30, &[0xAB]);
    assert!(PageManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_truncated_file() {
    let (dir, path) = scratch("pm-trunc");
    let _ = PageManager::create(&path).expect("create");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open")
        .set_len(100)
        .expect("truncate");
    assert!(PageManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_misaligned_trailing_bytes() {
    let (dir, path) = scratch("pm-align");
    let mut manager = PageManager::create(&path).expect("create");
    let page = manager.allocate_page(PageType::Leaf).expect("alloc");
    manager.write_page(&page).expect("write");
    drop(manager);
    // Append one stray byte so file is no longer page aligned.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append");
    use std::io::Write as _;
    file.write_all(&[7]).expect("stray");
    drop(file);
    assert!(PageManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}
