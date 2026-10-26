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
//! Durability convergence: vector index payloads are derived artifacts.
//!
//! # Authority rule
//!
//! Authoritative vector values live in storage-engine rows, ordered through
//! the existing foundation:
//!
//! ```text
//! VECTOR VALUES
//!       ↓
//!  txn / MVCC / WAL
//!       ↓
//!  Storage Engine generations
//!       ↓
//!  vector index payloads as derived/rebuildable data
//!       ↓
//!  vector freshness state
//!       ↓
//!  ANN acceleration
//! ```
//!
//! The vector query API and the ANN algorithms (exact search, HNSW,
//! IVF/DiskANN, scalar/CPU fallback) stay modular. The persistence layer
//! converges: this crate owns **no** second authoritative WAL, MVCC
//! visibility, backup system, recovery system, or GC authority.
//!
//! # Removed second authorities
//!
//! | Subsystem claimed here before | Authoritative owner (use that) |
//! |-------------------------------|--------------------------------|
//! | WAL / commit ordering | `plomid-wal` + `plomid-txn` commit path |
//! | MVCC visibility (`mvcc::*`) | `plomid-txn` snapshot + [`crate::freshness`] gating |
//! | durable row store (`storage::file`, manifests, pages, snapshots) | storage-engine generations + checkpoints |
//! | backup/restore (`backup::*`) | storage-engine generation/checkpoint backup |
//! | recovery / GC authority | storage-engine recovery + generation GC / `VACUUM` |
//!
//! The modules named above remain in-tree only as explicitly
//! **non-authoritative** legacy helpers and test scaffolding. Their headers
//! say so, and nothing on the production SQL path depends on them: the
//! executor builds every ANN graph from snapshot-visible storage-engine
//! rows and invalidates it via the committed-write watermark.
//!
//! # Derived artifacts that may persist separately
//!
//! A separately persisted index artifact is permitted **only** as a
//! non-authoritative derived cache: it must be explicitly marked (see
//! [`DerivedArtifact`]), it must rebuild safely from authoritative rows,
//! and deleting or corrupting it must never lose data — exact search keeps
//! serving from rows while ANN rebuilds. Today that set is:
//!
//! * DiskANN `vectors.dat` + `graph.dat` segments (executor `vector_ann/`),
//!   classified on open by `DiskAnnIndex::recover` and always rebuilt from
//!   snapshot rows before serving;
//! * in-memory HNSW/IVF/SQ/PQ/sparse graphs keyed by
//!   `(database, table, column, watermark, row count)`;
//! * [`crate::stats::VectorStats`] observations (derived, safe to drop).
//!
//! # Commit-order rule
//!
//! Both publication orders converge to the same visible state because index
//! artifacts never confer visibility:
//!
//! * `ROW COMMIT → VECTOR INDEX UPDATE`: the commit bumps the freshness
//!   watermark, which invalidates derived graphs; the next query rebuilds
//!   from the new snapshot or falls back to exact.
//! * `VECTOR INDEX BUILD/PUBLISH → ROW COMMIT`: a graph built before a
//!   commit carries the old watermark/row count, so the post-commit query
//!   treats it as stale and never serves it.
//!
//! # Crash matrix (executor integration tests)
//!
//! `crates/executor/tests/vector_durability_convergence.rs` kills the
//! process (drops the engine without graceful shutdown) during row WAL
//! commit, index write, index publish, generation publish, checkpoint,
//! `VACUUM`, and rebuild, then reopens and checks: vector results equal the
//! exact oracle, exact fallback serves, rebuild restores ANN, backup/restore
//! round-trips, and GC (`VACUUM`) preserves correctness.
//!
//! # Rebuild gate
//!
//! Delete or corrupt the derived ANN artifacts intentionally. Expected:
//! authoritative rows remain valid, exact vector search remains correct,
//! the ANN index rebuilds safely. No data loss.

/// Human-readable statement of the single authority for vector durability.
pub const AUTHORITY_NOTE: &str = "authoritative vector values live in storage-engine rows \
    (txn/MVCC/WAL/generations); vector index payloads are derived, non-authoritative, \
    rebuildable artifacts gated by freshness; ANN is acceleration only";

/// Owner of each durability subsystem after convergence.
///
/// Returned by [`authority_table`]: `(subsystem, authoritative owner)`.
#[must_use]
pub const fn authority_table() -> [(&'static str, &'static str); 5] {
    [
        ("WAL / commit ordering", "plomid-wal + plomid-txn"),
        (
            "MVCC visibility",
            "plomid-txn snapshot + vector freshness gating",
        ),
        (
            "durable row store",
            "storage-engine generations + checkpoints",
        ),
        (
            "backup / restore",
            "storage-engine generation/checkpoint backup",
        ),
        (
            "recovery / GC",
            "storage-engine recovery + generation GC / VACUUM",
        ),
    ]
}

/// A separately persisted vector artifact, explicitly marked derived.
///
/// Construction itself is the mark: there is no `authoritative` flag to set
/// because a separately persisted vector artifact can never be authoritative.
/// [`Self::note`] spells the rebuild contract for logs and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DerivedArtifact {
    /// Stable artifact name (e.g. `"diskann segments"`, `"ann graph cache"`).
    pub name: &'static str,
}

impl DerivedArtifact {
    /// Mark `name` as a non-authoritative derived artifact.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }

    /// One-line rebuild contract for this artifact.
    #[must_use]
    pub fn note(&self) -> String {
        format!(
            "{}: NON-AUTHORITATIVE derived cache — safe to delete; exact search serves \
             from authoritative rows while ANN rebuilds; no data loss",
            self.name
        )
    }
}

/// The derived artifacts permitted to persist separately today.
#[must_use]
pub const fn permitted_derived_artifacts() -> [DerivedArtifact; 3] {
    [
        DerivedArtifact::new("diskann vectors.dat + graph.dat segments"),
        DerivedArtifact::new("in-memory ann graph cache"),
        DerivedArtifact::new("vector statistics observations"),
    ]
}

/// Whether an index artifact with `persisted_separately` storage may serve
/// queries directly.
///
/// A separately persisted index artifact serves only after it is rebuilt
/// from authoritative rows and validated (decode + `validate`); otherwise
/// the caller must use exact search. (The gate is intentionally identical
/// for both storage shapes: even a non-separated image must prove itself
/// before serving.) This is the gate the executor's ANN path implements:
/// `false` here means "fall back to exact, rebuild before next ANN attempt".
#[must_use]
pub const fn may_serve_ann(persisted_separately: bool, rebuilt_and_validated: bool) -> bool {
    let _ = persisted_separately;
    rebuilt_and_validated
}
