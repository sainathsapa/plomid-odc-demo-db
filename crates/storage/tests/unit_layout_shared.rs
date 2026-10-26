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
// Moved from `src/layout/shared.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::DatabaseLayout;
use std::path::PathBuf;

#[test]
fn shared_paths_are_grouped_under_shared() {
    let root = PathBuf::from("/tmp/plomid-layout-shared");
    let layout = DatabaseLayout::new(&root);
    let shared = root.join("shared");
    assert_eq!(layout.shared_tx_dir(), shared.join("tx"));
    assert_eq!(layout.shared_rowid_dir(), shared.join("rowid"));
    assert_eq!(layout.shared_statistics_dir(), shared.join("statistics"));
    assert_eq!(layout.shared_filters_dir(), shared.join("filters"));
    assert_eq!(
        layout.shared_dictionaries_dir(),
        shared.join("dictionaries")
    );
}
