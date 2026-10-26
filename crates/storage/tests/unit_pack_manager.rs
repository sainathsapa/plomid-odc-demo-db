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
// Moved from `src/pack_manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::PageId;
use plomid_storage::pack_manager::*;
use plomid_storage::page::Page;
use plomid_storage::physical::PAGES_PER_BLOCK;
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
    let path = dir.join("pack.plpk");
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
    drop(file);
}

fn block_pages(base: u64) -> Vec<Page> {
    use plomid_core::PageId;
    use plomid_storage::physical::PageType;
    (0..PAGES_PER_BLOCK as usize)
        .map(|i| Page::with_type(PageId::new(base + i as u64), PageType::Leaf))
        .collect()
}

#[test]
fn round_trip_and_release() {
    let (dir, path) = scratch("pk-rt");
    let mut pack = PackManager::create(&path, 64).expect("create");
    let a = pack.allocate_block().expect("alloc a");
    let b = pack.allocate_block().expect("alloc b");
    pack.write_block(a, &block_pages(100)).expect("write a");
    pack.write_blocks(&[(b, block_pages(200))])
        .expect("write b");
    pack.flush().expect("flush");

    assert_eq!(
        pack.read_block(a).expect("read a")[0].id(),
        PageId::new(100)
    );
    let batch = pack.read_blocks(&[b, a]).expect("batch");
    assert_eq!(batch[0][0].id(), PageId::new(200));
    assert_eq!(batch[1][0].id(), PageId::new(100));
    let listing = pack.block_directory();
    assert_eq!(listing.iter().filter(|e| e.is_some()).count(), 2);

    pack.release_block(a).expect("release a");
    assert!(pack.read_block(a).is_err());
    pack.validate().expect("validate after release");

    drop(pack);
    let mut reopened = PackManager::open(&path).expect("open");
    reopened.validate().expect("revalidate");
    let listing2 = reopened.block_directory();
    assert_eq!(listing2.iter().filter(|e| e.is_some()).count(), 1);
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_magic() {
    let (dir, path) = scratch("pk-magic");
    let _ = PackManager::create(&path, 16).expect("create");
    write_at(&path, 0, b"XXXX");
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_block_size() {
    let (dir, path) = scratch("pk-bsize");
    let _ = PackManager::create(&path, 16).expect("create");
    // Block-size field at header bytes 8..16.
    write_at(&path, 8, &(4096_u64).to_le_bytes());
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_page_size() {
    let (dir, path) = scratch("pk-psize");
    let _ = PackManager::create(&path, 16).expect("create");
    // Page-size field at header bytes 16..24.
    write_at(&path, 16, &(4096_u64).to_le_bytes());
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_invalid_block_directory() {
    let (dir, path) = scratch("pk-directory");
    let mut pack = PackManager::create(&path, 16).expect("create");
    let block = pack.allocate_block().expect("alloc");
    pack.write_block(block, &block_pages(1)).expect("write");
    drop(pack);
    // Corrupt the first directory entry's checksum field (entry starts at PACK_HEADER_LEN).
    write_at(&path, PACK_HEADER_LEN + 8, &[0x55, 0x55, 0x55, 0x55]);
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_truncated_pack() {
    let (dir, path) = scratch("pk-trunc");
    let _ = PackManager::create(&path, 64).expect("create");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open")
        .set_len(100)
        .expect("truncate");
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_footer_checksum_mismatch() {
    let (dir, path) = scratch("pk-ftrcksum");
    let pack = PackManager::create(&path, 16).expect("create");
    drop(pack);
    let footer_offset = PackManager::footer_offset(16).expect("footer offset");
    write_at(&path, footer_offset + 42, &[0x01]);
    assert!(PackManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}
