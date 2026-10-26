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
// Moved from `src/buffer_pool.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{ErrorKind, PageId};
use plomid_storage::buffer_pool::BufferPool;
use plomid_storage::PAGE_SIZE;
use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-buffer-pool-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn dirty_page_flushes_and_is_read_by_a_new_pool() {
    let path = temp_path("round-trip");
    let result = (|| {
        let mut pool = BufferPool::create(&path, 2)?;
        let mut handle = pool.allocate_page()?;
        handle.data_mut()[..5].copy_from_slice(b"hello");
        pool.unpin_page(handle, true)?;
        pool.sync()?;
        drop(pool);

        let mut reopened = BufferPool::open(&path, 2)?;
        let handle = reopened.get_page(PageId::new(0))?;
        assert_eq!(&handle.data()[..5], b"hello");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "page round trip failed: {result:?}");
}

#[test]
fn pinned_page_prevents_eviction() {
    let path = temp_path("pinned");
    let result = (|| {
        let mut pool = BufferPool::create(&path, 1)?;
        let handle = pool.allocate_page()?;
        let error = pool
            .allocate_page()
            .expect_err("pinned frame must block allocation");
        assert_eq!(error.kind(), ErrorKind::Conflict);
        pool.unpin_page(handle, false)?;
        let second = pool.allocate_page()?;
        assert_eq!(second.id(), PageId::new(1));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "pin/eviction test failed: {result:?}");
}

#[test]
fn corrupted_page_is_rejected_on_read() {
    let path = temp_path("corruption");
    let result = (|| {
        let mut pool = BufferPool::create(&path, 1)?;
        let handle = pool.allocate_page()?;
        pool.unpin_page(handle, false)?;
        pool.sync()?;
        drop(pool);

        let mut file = OpenOptions::new().write(true).open(&path)?;
        file.seek(SeekFrom::Start((PAGE_SIZE - 1) as u64))?;
        file.write_all(&[1])?;
        file.sync_all()?;
        drop(file);

        let mut reopened = BufferPool::open(&path, 1)?;
        let error = reopened
            .get_page(PageId::new(0))
            .expect_err("corrupted page must fail verification");
        assert_eq!(error.kind(), ErrorKind::Corruption);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "corruption test failed: {result:?}");
}
