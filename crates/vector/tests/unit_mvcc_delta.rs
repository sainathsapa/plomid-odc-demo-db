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
// Moved from `src/mvcc/delta.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::mvcc::delta::*;

#[test]
fn commit_and_tombstone() {
    let mut committed = CommittedDelta::new();
    let mut pending = PendingWrites::new();
    pending.insert(0, vec![1, 2, 3]);
    pending.commit_into(7, 10, &mut committed);
    assert!(!committed.is_deleted(0, 10));
    let mut del = PendingWrites::new();
    del.delete(0);
    del.commit_into(8, 11, &mut committed);
    assert!(committed.is_deleted(0, 11));
    assert!(!committed.is_deleted(0, 10));
    assert_eq!(committed.tombstone_count(), 1);
}
