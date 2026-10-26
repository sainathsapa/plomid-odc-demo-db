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
// Moved from `src/btree/root_meta.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{GenerationId, IndexId, ObjectId, PageId};
use plomid_index::btree::constants::{INDEX_FLAG_UNIQUE, INDEX_FORMAT_VERSION, ROOT_META_SIZE};
use plomid_index::btree::root_meta::*;

#[test]
fn root_meta_round_trips() {
    let meta = RootMeta {
        version: INDEX_FORMAT_VERSION,
        flags: INDEX_FLAG_UNIQUE,
        index_id: IndexId::new(42),
        object_id: ObjectId::new(7),
        generation: GenerationId::new(3),
        root_page: PageId::new(5),
        entry_count: 1000,
    };
    let encoded = meta.encode().unwrap();
    assert_eq!(encoded.len(), ROOT_META_SIZE);
    let decoded = RootMeta::decode(&encoded).unwrap();
    assert_eq!(decoded, meta);
    assert!(decoded.is_unique());
}

#[test]
fn non_unique_root_meta() {
    let meta = RootMeta {
        version: INDEX_FORMAT_VERSION,
        flags: 0,
        index_id: IndexId::new(0),
        object_id: ObjectId::new(0),
        generation: GenerationId::new(0),
        root_page: PageId::new(1),
        entry_count: 0,
    };
    let encoded = meta.encode().unwrap();
    let decoded = RootMeta::decode(&encoded).unwrap();
    assert!(!decoded.is_unique());
}

#[test]
fn rejects_bad_magic() {
    let data = vec![0u8; ROOT_META_SIZE];
    assert!(RootMeta::decode(&data).is_err());
}

#[test]
fn rejects_truncated_block() {
    assert!(RootMeta::decode(&[0u8; 32]).is_err());
}
