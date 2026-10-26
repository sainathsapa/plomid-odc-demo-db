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
// Moved from `src/layout/metadata.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::metadata::{publish, read, staged_path};
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-layout-meta-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn staging_path_is_a_sibling_of_the_final_name() {
    let final_path = std::path::Path::new("/tmp/db/objects/META.dat");
    assert_eq!(
        staged_path(final_path),
        std::path::Path::new("/tmp/db/objects/META.dat.tmp")
    );
}

#[test]
fn publication_creates_parents_and_replaces_atomically() {
    let dir = scratch("publish");
    let result = (|| {
        let target = dir.join("objects").join("tables").join("META.dat");
        publish(&target, b"first")?;
        assert_eq!(read(&target, "test record")?, b"first".to_vec());
        // The staging artifact never survives a completed publication.
        assert!(!staged_path(&target).exists());
        publish(&target, b"second")?;
        assert_eq!(read(&target, "test record")?, b"second".to_vec());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn missing_record_reports_not_found() {
    let dir = scratch("missing");
    let error = read(&dir.join("META.dat"), "test record").expect_err("absent");
    assert_eq!(error.kind(), plomid_core::ErrorKind::NotFound);
    assert_eq!(
        plomid_storage::layout::metadata::read_optional(&dir.join("META.dat")).expect("optional"),
        None
    );
}
