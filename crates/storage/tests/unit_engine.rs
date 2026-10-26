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
// Moved from `src/engine.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{CommitTimestamp, TxnId};
use plomid_storage::engine::*;

#[test]
fn commit_result_fields_are_accessible() {
    let txn_id = TxnId::new(1);
    let ts = CommitTimestamp::new(1);
    let result = CommitResult {
        txn_id,
        commit_timestamp: ts,
    };
    assert_eq!(result.txn_id, txn_id);
    assert_eq!(result.commit_timestamp, ts);
}

#[test]
fn transaction_state_variants() {
    let states = [
        TransactionState::Active,
        TransactionState::Committed,
        TransactionState::Aborted,
    ];
    assert_eq!(states.len(), 3);
}

#[test]
fn shared_snapshot_handle_is_send_sync_and_sorted() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SharedSnapshot>();
    let handle = SharedSnapshot::new(0, 7, vec![3, 1, 3, 2]);
    assert_eq!(handle.owner(), 0);
    assert_eq!(handle.watermark(), 7);
    assert_eq!(handle.active(), &[1, 2, 3]);
}
