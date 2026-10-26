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
// Moved from `src/index/online_build.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::index::online_build::*;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric};
use plomid_vector::index::{RowId, VectorIndex};
use plomid_vector::mvcc::delta::CommittedDelta;
use plomid_vector::mvcc::delta::PendingWrites;
use plomid_vector::{VectorError, VectorValue};

fn dense_bytes(v: &[f32]) -> Vec<u8> {
    use plomid_vector::r#type::VectorElementType;
    use plomid_vector::value::DenseVectorValue;
    let value = DenseVectorValue::from_f32(VectorElementType::Float32, v.to_vec()).unwrap();
    plomid_vector::codec::encode_standalone(&VectorValue::Dense(value)).unwrap()
}

#[test]
fn snapshot_plus_delta_merges() {
    let mut idx = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2)).unwrap();
    let snapshot = vec![(1u64, vec![1.0, 0.0]), (2, vec![0.0, 1.0])];
    let mut pending = PendingWrites::new();
    pending.insert(3, dense_bytes(&[0.5, 0.5]));
    pending.delete(1);
    let mut delta = CommittedDelta::new();
    pending.commit_into(9, 11, &mut delta);

    let report = run_online_build(&mut idx, &snapshot, 10, &delta, None).unwrap();
    assert_eq!(report.base_rows, 2);
    assert_eq!(report.applied_entries, 2);
    assert_eq!(report.applied_tombstones, 1);
    assert_eq!(report.applied_lsn, Some(11));
    // Row 1 tombstoned, row 3 inserted: live set is {2, 3}.
    let hits = idx.search(&[0.0, 1.0], 5).unwrap();
    let ids: Vec<RowId> = hits.iter().map(|h| h.row_id).collect();
    assert!(ids.contains(&2) && ids.contains(&3) && !ids.contains(&1));
}

#[test]
fn cancel_never_publishes() {
    let mut idx = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2)).unwrap();
    let snapshot: Vec<(RowId, Vec<f32>)> = (0..500).map(|i| (i, vec![i as f32, 0.0])).collect();
    let delta = CommittedDelta::new();
    let stop = || true;
    let err = run_online_build(&mut idx, &snapshot, 0, &delta, Some(&stop));
    assert!(matches!(err, Err(VectorError::Cancelled(_))));
}

#[test]
fn old_delta_entries_ignored() {
    let mut idx = FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 1)).unwrap();
    let mut pending = PendingWrites::new();
    pending.insert(9, dense_bytes(&[7.0]));
    let mut delta = CommittedDelta::new();
    pending.commit_into(1, 5, &mut delta);
    // Snapshot at LSN 5 already contains row 9's effect; replay must not
    // duplicate-apply it (Flat insert overwrites idempotently here, and
    // the report proves nothing was replayed).
    let report = run_online_build(&mut idx, &[(9, vec![7.0])], 5, &delta, None).unwrap();
    assert_eq!(report.applied_entries, 0);
    assert_eq!(report.applied_lsn, None);
}
