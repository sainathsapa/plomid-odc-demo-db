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
// Moved from `src/fs.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::fs::{FileSystem, RealFs};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-storage-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn real_fs_round_trip_with_fsync() {
    let path = temp_path("round-trip");
    let fs = RealFs;
    let result = (|| {
        let mut file = fs.create(&path)?;
        assert_eq!(fs.write_at(&mut file, 0, b"durable page")?, 12);
        fs.fsync(&mut file)?;
        let mut buffer = [0_u8; 12];
        assert_eq!(fs.read_at(&mut file, 0, &mut buffer)?, 12);
        assert_eq!(&buffer, b"durable page");
        assert_eq!(fs.len(&file)?, 12);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(
        result.is_ok(),
        "real filesystem round trip failed: {result:?}"
    );
}
