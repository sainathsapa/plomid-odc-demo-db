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
//! Online index-build protocol (Phase 17 in code).
//!
//! NON-AUTHORITATIVE build protocol (see [`crate::durability`]): merges a
//! snapshot image with committed deltas into a derived, rebuildable index.
//! Production visibility comes from the engine snapshot + freshness watermark;
//! this protocol's delta log is offline bookkeeping for that merge, never a
//! second commit authority. Cancellation still aborts before publication.
//!
//! Production indexes are built without stopping writes:
//!
//! ```text
//! capture consistent snapshot (rows + snapshot_lsn)
//!         ↓
//! scan/build base graph from the snapshot
//!         ↓
//! track concurrent committed mutations (CommittedDelta)
//!         ↓
//! catch up: apply every entry with lsn in (snapshot_lsn .. max_lsn]
//!         ↓
//! validate the merged structure
//!         ↓
//! flush + atomically publish READY (or report FAILED — never partial)
//! ```
//!
//! Transaction-local (uncommitted) writes are never applied: only
//! [`CommittedDelta`] entries. Cancellation aborts before publication, so a
//! cancelled build can never surface as READY. Non-dense payloads are
//! skipped and counted (dense-only indexes stay exact for those rows via
//! the caller's fallback path).

use crate::mvcc::delta::{CommittedDelta, DeltaOp};
use crate::search::{CancelFn, CANCEL_CHECK_EVERY};
use crate::{Result, VectorError, VectorValue};

use super::{IndexStats, RowId, VectorIndex};

/// Build lifecycle phases, mirroring [`crate::admin::IndexState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildPhase {
    /// Capturing the snapshot (caller-side).
    Capturing,
    /// Building the base structure from snapshot rows.
    Building,
    /// Replaying committed mutations after `snapshot_lsn`.
    CatchingUp,
    /// Validating the merged structure.
    Validating,
    /// Flushed and published; safe to serve.
    Ready,
}

/// Outcome of a completed online build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnlineBuildReport {
    /// Snapshot LSN the base build read.
    pub snapshot_lsn: u64,
    /// Highest delta LSN applied (`None` when the delta was empty).
    pub applied_lsn: Option<u64>,
    /// Snapshot rows in the base build.
    pub base_rows: usize,
    /// Committed delta entries applied during catch-up.
    pub applied_entries: usize,
    /// Committed tombstones applied during catch-up.
    pub applied_tombstones: usize,
    /// Non-dense payloads skipped (served by exact fallback).
    pub skipped_non_dense: usize,
}

/// Runs the full online-build protocol against `index`.
///
/// `snapshot_rows` must be a consistent read at `snapshot_lsn`; `delta`
/// carries every committed mutation (the log may also hold older entries —
/// only `lsn > snapshot_lsn` apply). On any error, or when `cancel` fires,
/// the index is left untouched by publication: the caller must treat it as
/// failed and either retry or fall back to exact search.
pub fn run_online_build<I: VectorIndex>(
    index: &mut I,
    snapshot_rows: &[(RowId, Vec<f32>)],
    snapshot_lsn: u64,
    delta: &CommittedDelta,
    cancel: Option<&CancelFn>,
) -> Result<OnlineBuildReport> {
    let cancelled = || VectorError::Cancelled("index build cancelled".to_string());
    let check = |counter: usize| -> Result<()> {
        if let Some(stop) = cancel {
            if counter.is_multiple_of(CANCEL_CHECK_EVERY) && stop() {
                return Err(cancelled());
            }
        }
        Ok(())
    };

    // Phase: Building — base graph from the snapshot.
    for (i, _) in snapshot_rows.iter().enumerate() {
        check(i)?;
    }
    index.build(snapshot_rows)?;
    check(snapshot_rows.len())?;

    // Phase: CatchingUp — replay committed mutations after the snapshot.
    let mut applied_entries = 0usize;
    let mut applied_tombstones = 0usize;
    let mut skipped_non_dense = 0usize;
    let mut applied_lsn: Option<u64> = None;
    for (i, entry) in delta.iter().filter(|e| e.lsn > snapshot_lsn).enumerate() {
        check(i)?;
        match &entry.op {
            DeltaOp::Delete => {
                index.delete(entry.row_id)?;
                applied_entries += 1;
                applied_tombstones += 1;
            }
            DeltaOp::Insert(bytes) => {
                match crate::codec::decode_standalone(bytes)
                    .map_err(|e| VectorError::IndexCorrupted(format!("delta payload: {e}")))?
                {
                    VectorValue::Dense(d) => {
                        index.insert(entry.row_id, &d.values_f32)?;
                        applied_entries += 1;
                    }
                    _ => {
                        skipped_non_dense += 1;
                    }
                }
            }
        }
        applied_lsn = Some(applied_lsn.map_or(entry.lsn, |m: u64| m.max(entry.lsn)));
    }

    // Phase: Validating + publish. Validation failure fails the build;
    // only a validated index is flushed and reported READY.
    index.validate()?;
    index.flush()?;

    Ok(OnlineBuildReport {
        snapshot_lsn,
        applied_lsn,
        base_rows: snapshot_rows.len(),
        applied_entries,
        applied_tombstones,
        skipped_non_dense,
    })
}

/// Current counters for an index (observability helper).
#[must_use]
pub fn describe_index<I: VectorIndex>(index: &I) -> IndexStats {
    index.stats()
}
