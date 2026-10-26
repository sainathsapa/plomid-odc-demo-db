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
#![forbid(unsafe_code)]
//! MVCC deltas: committed log plus transaction-local pending writes.
//!
//! NON-AUTHORITATIVE (see [`crate::durability`]): the commit log of record is
//! the engine WAL; this log is offline bookkeeping for the legacy
//! online-build protocol tests, never a second commit authority.
//!
//! Payloads are opaque bytes (see `storage::adapter` encoding).
//! A [`DeltaOp::Delete`] entry is a tombstone: the row id is dead
//! from that LSN on until cleaned by maintenance.

use std::collections::HashMap;

/// A single row mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaOp {
    /// Insert/overwrite with encoded payload bytes.
    Insert(Vec<u8>),
    /// Tombstone: row is logically deleted.
    Delete,
}

impl DeltaOp {
    /// `true` for [`DeltaOp::Delete`] tombstones.
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Delete)
    }
}

/// One committed delta entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedEntry {
    /// Stable row id the entry applies to.
    pub row_id: u64,
    /// Commit LSN of the entry.
    pub lsn: u64,
    /// Committing transaction id.
    pub txn_id: u64,
    /// The committed mutation.
    pub op: DeltaOp,
}

/// Ordered log of committed deltas (ascending LSN).
#[derive(Debug, Clone, Default)]
pub struct CommittedDelta {
    entries: Vec<CommittedEntry>,
}

impl CommittedDelta {
    /// Empty committed log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a committed entry. Callers must supply LSNs in order.
    pub fn push(&mut self, row_id: u64, lsn: u64, txn_id: u64, op: DeltaOp) {
        self.entries.push(CommittedEntry {
            row_id,
            lsn,
            txn_id,
            op,
        });
    }

    /// Number of committed entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when the log holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Borrowed entries in commit order.
    pub fn iter(&self) -> std::slice::Iter<'_, CommittedEntry> {
        self.entries.iter()
    }

    /// Latest committed op for `row_id` at or below `lsn`, if any.
    #[must_use]
    pub fn latest_op(&self, row_id: u64, lsn: u64) -> Option<&DeltaOp> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.row_id == row_id && e.lsn <= lsn)
            .map(|e| &e.op)
    }

    /// `true` when the latest op at/below `lsn` is a tombstone.
    #[must_use]
    pub fn is_deleted(&self, row_id: u64, lsn: u64) -> bool {
        matches!(self.latest_op(row_id, lsn), Some(DeltaOp::Delete))
    }

    /// Count tombstone (delete) entries in the log.
    #[must_use]
    pub fn tombstone_count(&self) -> usize {
        self.entries.iter().filter(|e| e.op.is_tombstone()).count()
    }

    /// Highest LSN in the log, if any.
    #[must_use]
    pub fn max_lsn(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.lsn).max()
    }

    /// Drop committed entries at or below `checkpoint_lsn`.
    /// Returns the number of entries removed.
    pub fn truncate_through(&mut self, checkpoint_lsn: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|e| e.lsn > checkpoint_lsn);
        before - self.entries.len()
    }
}

/// Transaction-local (uncommitted) write set: `row_id -> op`.
#[derive(Debug, Clone, Default)]
pub struct PendingWrites {
    writes: HashMap<u64, DeltaOp>,
}

impl PendingWrites {
    /// Empty transaction-local write set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage an insert/overwrite for `row_id`.
    pub fn insert(&mut self, row_id: u64, payload: Vec<u8>) {
        self.writes.insert(row_id, DeltaOp::Insert(payload));
    }

    /// Stage a tombstone for `row_id`.
    pub fn delete(&mut self, row_id: u64) {
        self.writes.insert(row_id, DeltaOp::Delete);
    }

    /// Staged op for `row_id`, if any.
    #[must_use]
    pub fn get(&self, row_id: u64) -> Option<&DeltaOp> {
        self.writes.get(&row_id)
    }

    /// Number of staged rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// `true` when no writes are staged.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Discard all pending writes (rollback).
    pub fn rollback(&mut self) {
        self.writes.clear();
    }

    /// Move every pending op into `committed` at `lsn`/`txn_id`.
    pub fn commit_into(self, txn_id: u64, lsn: u64, committed: &mut CommittedDelta) {
        for (row_id, op) in self.writes {
            committed.push(row_id, lsn, txn_id, op);
        }
    }
}
