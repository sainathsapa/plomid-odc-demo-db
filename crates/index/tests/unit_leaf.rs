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
// Moved from `src/leaf.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::RowId;
use plomid_index::error::ArtError;
use plomid_index::leaf::{LeafInsert, LeafRemove, LeafValues};

fn rid(value: u64) -> RowId {
    RowId::new(value)
}

#[test]
fn non_unique_leaf_keeps_every_row() {
    let mut leaf = LeafValues::default();
    assert_eq!(leaf.insert(rid(9)), LeafInsert::Inserted);
    assert_eq!(leaf.insert(rid(3)), LeafInsert::Inserted);
    assert_eq!(leaf.insert(rid(7)), LeafInsert::Inserted);
    // A duplicate RowId is idempotent, never a silent overwrite.
    assert_eq!(leaf.insert(rid(7)), LeafInsert::AlreadyPresent);

    assert_eq!(leaf.len(), 3);
    let rows: Vec<u64> = leaf.iter().map(|row| row.get()).collect();
    // Sorted ascending for deterministic iteration.
    assert_eq!(rows, vec![3, 7, 9]);
}

#[test]
fn unique_leaf_rejects_a_second_distinct_row() {
    let mut leaf = LeafValues::default();
    assert_eq!(leaf.insert_unique(rid(1)), Ok(LeafInsert::Inserted));
    assert_eq!(leaf.insert_unique(rid(1)), Ok(LeafInsert::AlreadyPresent));
    assert_eq!(leaf.insert_unique(rid(2)), Err(ArtError::DuplicateKey));
    assert_eq!(leaf.as_slice(), &[rid(1)]);
}

#[test]
fn removal_reports_missing_rows() {
    let mut leaf = LeafValues::from_rows([rid(1), rid(2), rid(1)]);
    assert_eq!(leaf.as_slice(), &[rid(1), rid(2)]);
    assert_eq!(leaf.remove(rid(1)), LeafRemove::Removed);
    assert_eq!(leaf.remove(rid(1)), LeafRemove::NotFound);
    assert_eq!(leaf.remove(rid(2)), LeafRemove::Removed);
    assert!(leaf.is_empty());
    assert_eq!(leaf.first(), None);
}

#[test]
fn single_leaf_reports_membership_without_copying() {
    let leaf = LeafValues::single(rid(42));
    assert_eq!(leaf.first(), Some(rid(42)));
    assert!(leaf.contains(rid(42)));
    assert!(!leaf.contains(rid(43)));
    assert_eq!(leaf.memory_bytes(), std::mem::size_of::<RowId>());
}
