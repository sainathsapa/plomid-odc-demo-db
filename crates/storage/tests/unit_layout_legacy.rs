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
// Moved from `src/layout/legacy.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::legacy::{is_legacy_layout, reject_legacy_layout};
use plomid_storage::layout::DatabaseLayout;
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-layout-legacy-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn a_fresh_root_is_not_legacy() {
    let root = scratch("fresh");
    let layout = DatabaseLayout::new(&root);
    assert!(!is_legacy_layout(&layout));
    assert!(reject_legacy_layout(&layout).is_ok());
}

#[test]
fn every_legacy_marker_is_detected() {
    for marker in [
        "data",
        "manifest",
        "volumes",
        "generation",
        "generations",
        "database.db",
        "storage.db",
        "generation.db",
    ] {
        let root = scratch(marker);
        let layout = DatabaseLayout::new(&root);
        std::fs::create_dir_all(root.join("catalog")).expect("root");
        if marker.ends_with(".db") {
            std::fs::write(root.join(marker), b"legacy").expect("file");
        } else {
            std::fs::create_dir_all(root.join(marker)).expect("directory");
        }
        let error = reject_legacy_layout(&layout).expect_err("legacy layout");
        assert_eq!(error.kind(), plomid_core::ErrorKind::Unsupported);
        assert!(is_legacy_layout(&layout));
        let _ = std::fs::remove_dir_all(&root);
    }
}

#[test]
fn the_old_pointer_location_is_detected() {
    let root = scratch("old-pointer");
    let layout = DatabaseLayout::new(&root);
    std::fs::create_dir_all(layout.catalog_dir()).expect("catalog");
    std::fs::write(layout.catalog_dir().join("CURRENT"), b"legacy").expect("pointer");
    assert!(is_legacy_layout(&layout));
    // The old pointer is not the new one, which lives at the root.
    assert!(!layout.current_exists());
    let _ = std::fs::remove_dir_all(&root);
}
