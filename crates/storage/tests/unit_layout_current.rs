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
// Moved from `src/layout/current.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::current::CURRENT_FILE_NAME;
use plomid_storage::layout::DatabaseLayout;
use std::path::PathBuf;

#[test]
fn current_lives_at_the_database_root() {
    let root = PathBuf::from("/tmp/plomid-layout-current");
    let layout = DatabaseLayout::new(&root);
    assert_eq!(layout.current_path(), root.join(CURRENT_FILE_NAME));
    assert_eq!(layout.current_staged_path(), root.join("CURRENT.tmp"));
    assert!(!layout.current_exists());
}
