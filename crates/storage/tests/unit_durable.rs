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
// Moved from `src/durable.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::durable::{publish_rename, stage_bytes, sync_dir, sync_file, verify_staged};
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-durable-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

#[test]
fn staged_publication_replaces_atomically() {
    let dir = scratch("publish");
    let result = (|| {
        let staged = dir.join("state.tmp");
        let final_path = dir.join("state");
        stage_bytes(&staged, b"first")?;
        verify_staged(&staged, b"first")?;
        sync_file(&staged)?;
        publish_rename(&staged, &final_path)?;
        sync_dir(&dir)?;
        assert_eq!(std::fs::read(&final_path)?, b"first");
        stage_bytes(&staged, b"second")?;
        publish_rename(&staged, &final_path)?;
        assert_eq!(std::fs::read(&final_path)?, b"second");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn staged_content_mismatch_is_detected() {
    let dir = scratch("verify");
    let result = (|| {
        let staged = dir.join("state.tmp");
        stage_bytes(&staged, b"published bytes")?;
        assert!(verify_staged(&staged, b"other bytes").is_err());
        assert!(verify_staged(&staged, b"published").is_err());
        verify_staged(&staged, b"published bytes")?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(result.is_ok(), "{result:?}");
}
