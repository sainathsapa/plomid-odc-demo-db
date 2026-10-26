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
// Moved from `src/block_manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::PageId;
use plomid_storage::block_manager::*;
use plomid_storage::page::Page;
use plomid_storage::physical::PageType;
use plomid_storage::physical::{BLOCK_SIZE, PAGES_PER_BLOCK, PAGE_SIZE};

fn block_pages(base: u64) -> Vec<Page> {
    (0..PAGES_PER_BLOCK)
        .map(|i| Page::with_type(PageId::new(base + i), PageType::Leaf))
        .collect()
}

#[test]
fn block_round_trip_and_batch() {
    let dir = std::env::temp_dir().join(format!(
        "plomid-block-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("blocks.plbm");
    let mut manager = BlockManager::create(&path).expect("create");
    let a = manager.allocate_block().expect("alloc a");
    let b = manager.allocate_block().expect("alloc b");
    manager.write_block(a, &block_pages(100)).expect("write a");
    manager
        .write_blocks(&[(b, block_pages(200))])
        .expect("write b");
    manager.flush().expect("flush");
    let pages = manager.read_block(a).expect("read a");
    assert_eq!(pages.len(), PAGES_PER_BLOCK as usize);
    assert_eq!(pages[0].id(), PageId::new(100));
    let batch = manager.read_blocks(&[b, a]).expect("batch");
    assert_eq!(batch[0][0].id(), PageId::new(200));
    assert_eq!(batch[1][0].id(), PageId::new(100));
    manager.validate_block(a).expect("validate");
    let off_a = manager.block_offset(a).expect("off a");
    let off_b = manager.block_offset(b).expect("off b");
    assert_eq!(off_b, off_a + BLOCK_SIZE);
    assert_eq!(
        manager.page_offset(a, 1).expect("page off"),
        off_a + PAGE_SIZE
    );
    drop(manager);
    let mut reopened = BlockManager::open(&path).expect("open");
    reopened.validate_block(b).expect("revalidate");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejects_misaligned_trailing_bytes() {
    let dir = std::env::temp_dir().join(format!(
        "plomid-block-bad-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("dir");
    let path = dir.join("blocks.plbm");
    let mut manager = BlockManager::create(&path).expect("create");
    let a = manager.allocate_block().expect("alloc");
    manager.write_block(a, &block_pages(1)).expect("write");
    drop(manager);
    // Append one stray byte so the file is no longer block aligned.
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("append");
    use std::io::Write as _;
    file.write_all(&[9]).expect("write stray");
    drop(file);
    assert!(BlockManager::open(&path).is_err());
    std::fs::remove_dir_all(&dir).ok();
}
