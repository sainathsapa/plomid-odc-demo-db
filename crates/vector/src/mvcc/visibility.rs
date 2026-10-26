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
//! MVCC visibility: which row versions a snapshot may see.
//!
//! NON-AUTHORITATIVE (see [`crate::durability`]): visibility authority lives
//! in `plomid-txn`; the production path uses the engine snapshot plus
//! [`crate::freshness`] gating. These predicates remain as pure-function
//! documentation of the rule, never as a second visibility system.

/// Row-level visibility predicate.
///
/// `row_creator` is the LSN (or txn id, when the caller keys rows by
/// creator) that produced the row version; `snapshot_watermark` is the
/// reader's snapshot LSN; `active_set` holds creator ids that were
/// still in flight at snapshot time.
///
/// A row is visible when it was created at or below the watermark
/// and its creator was not active (i.e. committed before the snap).
#[must_use]
pub fn is_visible(row_creator: u64, snapshot_watermark: u64, active_set: &[u64]) -> bool {
    if row_creator > snapshot_watermark {
        return false;
    }
    !active_set.contains(&row_creator)
}

/// Full version visibility: creation LSN plus optional deletion LSN.
///
/// Visible when `created_lsn` is visible per [`is_visible`] and the
/// row was not deleted at or before the snapshot (`deleted_lsn` is
/// `None`, in the future, or belongs to an in-flight deleter).
#[must_use]
pub fn is_version_visible(
    created_lsn: u64,
    deleted_lsn: Option<u64>,
    snapshot_lsn: u64,
    active_set: &[u64],
) -> bool {
    if !is_visible(created_lsn, snapshot_lsn, active_set) {
        return false;
    }
    match deleted_lsn {
        None => true,
        Some(d) => {
            if d > snapshot_lsn {
                return true;
            }
            // Deleted at/before the snapshot by an in-flight txn:
            // the delete is not yet committed, so the row is visible.
            active_set.contains(&d)
        }
    }
}

/// Owned snapshot predicate for repeated checks.
#[derive(Debug, Clone, Default)]
pub struct VisibilitySnapshot {
    /// Snapshot watermark LSN.
    pub watermark_lsn: u64,
    /// Creator LSN/txn ids still active at snapshot time.
    pub active: Vec<u64>,
}

impl VisibilitySnapshot {
    /// Capture a snapshot predicate.
    #[must_use]
    pub fn new(watermark_lsn: u64, active: Vec<u64>) -> Self {
        Self {
            watermark_lsn,
            active,
        }
    }

    /// Row-level check against this snapshot.
    #[must_use]
    pub fn is_visible(&self, row_creator: u64) -> bool {
        is_visible(row_creator, self.watermark_lsn, &self.active)
    }

    /// Version check (creation + optional deletion LSN) for this snapshot.
    #[must_use]
    pub fn is_version_visible(&self, created_lsn: u64, deleted_lsn: Option<u64>) -> bool {
        is_version_visible(created_lsn, deleted_lsn, self.watermark_lsn, &self.active)
    }
}
