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
// Moved from `src/layout/root.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::root::DatabaseLayout;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    std::env::temp_dir().join("plomid-layout-root-unit")
}

#[test]
fn root_level_paths_are_stable() {
    let layout = DatabaseLayout::new(root());
    assert_eq!(layout.root(), Path::new(&root()));
    assert_eq!(layout.current_path(), root().join("CURRENT"));
    assert_eq!(layout.current_staged_path(), root().join("CURRENT.tmp"));
    assert_eq!(layout.catalog_dir(), root().join("catalog"));
    assert_eq!(layout.wal_dir(), root().join("wal"));
    assert_eq!(layout.checkpoints_dir(), root().join("checkpoints"));
    assert_eq!(layout.objects_dir(), root().join("objects"));
    assert_eq!(layout.devices_dir(), root().join("devices"));
    assert_eq!(layout.shared_dir(), root().join("shared"));
    assert_eq!(layout.temp_dir(), root().join("temp"));
    assert_eq!(layout.lost_found_dir(), root().join("lost+found"));
}

#[test]
fn equal_roots_produce_equal_layouts() {
    assert_eq!(DatabaseLayout::new(root()), DatabaseLayout::new(root()));
}
