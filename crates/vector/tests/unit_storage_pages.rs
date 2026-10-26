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
// Moved from `src/storage/pages.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::storage::pages::*;

#[test]
fn roundtrip() {
    let page = Page::new(PageKind::Data, 7, 42, vec![1, 2, 3]).unwrap();
    let buf = page.to_buffer();
    assert_eq!(buf.len(), PAGE_SIZE);
    let back = Page::from_buffer(&buf).unwrap();
    assert_eq!(page, back);
}

#[test]
fn rejects_bad_magic() {
    let buf = vec![0u8; PAGE_SIZE];
    assert!(Page::from_buffer(&buf).is_err());
}
