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

//! [`MemoryContext`]: hierarchical accounting node plus the RAII
//! [`Allocation`] guard.
//!
//! Design notes:
//!
//! * Each context owns atomic `current` / `peak` / `alloc_count` /
//!   `spill_bytes` counters. The allocation fast path
//!   ([`MemoryContext::try_allocate`], [`MemoryContext::release`]) uses
//!   atomic compare-and-swap loops only and takes no locks.
//! * The parent link is a strong [`Arc`], so holding any leaf keeps its
//!   whole ancestor chain alive for correct upward propagation. The child
//!   set holds [`std::sync::Weak`] references (no reference cycles) and is
//!   touched only when creating children, snapshotting stats, or pruning
//!   dead entries — never on the alloc path.
//! * A `try_allocate` reserves root-first with per-level CAS; on the first
//!   enforcing budget failure every already-reserved ancestor is rolled
//!   back, so concurrent allocators can never push any counter past its
//!   budget.
//! * An optional lock-free notifier (installed by [`crate::MemoryManager`]
//!   via a [`std::sync::OnceLock`]) fires after each accounting change so
//!   pressure listeners run without polling. Standalone contexts simply
//!   carry no notifier.

use std::fmt;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, OnceLock, Weak,
};

use crate::error::MemoryError;
use crate::operator::OperatorKind;

/// Query workload class for [`ContextKind::WorkloadClass`] and
/// [`ContextKind::Query`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    /// Latency-sensitive transactional workload.
    Oltp,
    /// Throughput-oriented analytical workload.
    Olap,
    /// Internal / background system work.
    System,
}

impl WorkloadClass {
    /// Stable lowercase name used in scope strings.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Oltp => "oltp",
            Self::Olap => "olap",
            Self::System => "system",
        }
    }
}

/// Maintenance operation behind a [`ContextKind::Maintenance`] task node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MaintenanceOp {
    /// Unspecified / branch-root accounting node.
    General,
    /// Vacuum / dead-tuple cleanup.
    Vacuum,
    /// Checkpoint.
    Checkpoint,
    /// Compaction (LSM / columnar re-pack).
    Compaction,
    /// Index build / rebuild.
    IndexBuild,
    /// Statistics collection.
    StatsCollect,
    /// Backup / snapshot.
    Backup,
}

impl MaintenanceOp {
    /// Stable lowercase name used in scope strings.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Vacuum => "vacuum",
            Self::Checkpoint => "checkpoint",
            Self::Compaction => "compaction",
            Self::IndexBuild => "index_build",
            Self::StatsCollect => "stats_collect",
            Self::Backup => "backup",
        }
    }
}

/// Cache partition behind a [`ContextKind::Cache`] node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CacheKind {
    /// Unspecified / branch-root accounting node.
    General,
    /// Buffer-pool pages.
    BufferPool,
    /// Page cache.
    PageCache,
    /// Cached query results.
    ResultCache,
    /// Cached plans.
    PlanCache,
}

impl CacheKind {
    /// Stable lowercase name used in scope strings.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::BufferPool => "buffer_pool",
            Self::PageCache => "page_cache",
            Self::ResultCache => "result_cache",
            Self::PlanCache => "plan_cache",
        }
    }
}

/// Position of a [`MemoryContext`] inside the hierarchy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ContextKind {
    /// Engine-wide root. Exactly one per manager.
    Global,
    /// OLTP / OLAP / System workload branch under global.
    WorkloadClass {
        /// Which workload this branch accounts.
        class: WorkloadClass,
    },
    /// One running query under its workload branch.
    Query {
        /// Engine-assigned query id.
        query_id: u64,
        /// Workload the query was classified into.
        workload: WorkloadClass,
    },
    /// One physical operator under its query.
    Operator {
        /// Owning query id.
        query_id: u64,
        /// Operator accounting bucket.
        kind: OperatorKind,
    },
    /// One client connection, directly under global.
    Connection {
        /// Connection id.
        connection_id: u64,
    },
    /// One parallel worker under a query or connection context.
    Worker {
        /// Worker id (unique within its parent).
        worker_id: u64,
    },
    /// Maintenance branch node or one maintenance task under it.
    Maintenance {
        /// Which maintenance operation this node accounts.
        operation: MaintenanceOp,
    },
    /// Cache branch node or one cache partition under it.
    Cache {
        /// Which cache partition this node accounts.
        kind: CacheKind,
    },
}

impl ContextKind {
    /// Human-readable scope used in [`MemoryError::BudgetExceeded`].
    pub fn scope_name(&self) -> String {
        match *self {
            Self::Global => "global".to_string(),
            Self::WorkloadClass { class } => format!("workload:{}", class.as_str()),
            Self::Query { query_id, workload } => {
                format!("query:{query_id}:{}", workload.as_str())
            }
            Self::Operator { query_id, kind } => format!("operator:{}:{query_id}", kind.as_str()),
            Self::Connection { connection_id } => format!("connection:{connection_id}"),
            Self::Worker { worker_id } => format!("worker:{worker_id}"),
            Self::Maintenance { operation } => {
                format!("maintenance:{}", operation.as_str())
            }
            Self::Cache { kind } => format!("cache:{}", kind.as_str()),
        }
    }
}

/// Hierarchical memory accounting node.
///
/// Created via [`MemoryContext::new_root`] (detached roots, mainly tests)
/// or [`MemoryContext::create_child`]; production code uses
/// [`crate::MemoryManager`] factories. Always handled as `Arc<Self>`.
pub struct MemoryContext {
    kind: ContextKind,
    budget: AtomicU64,
    current: AtomicU64,
    peak: AtomicU64,
    alloc_count: AtomicU64,
    spill_bytes: AtomicU64,
    parent: Option<Arc<MemoryContext>>,
    children: Mutex<Vec<Weak<MemoryContext>>>,
    notifier: OnceLock<Arc<dyn Fn() + Send + Sync + 'static>>,
}

impl MemoryContext {
    /// Create a detached root context (no parent, no notifier).
    /// Prefer [`crate::MemoryManager`] factories in production code.
    pub fn new_root(kind: ContextKind, budget: u64) -> Arc<Self> {
        Arc::new(Self {
            kind,
            budget: AtomicU64::new(budget),
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            alloc_count: AtomicU64::new(0),
            spill_bytes: AtomicU64::new(0),
            parent: None,
            children: Mutex::new(Vec::new()),
            notifier: OnceLock::new(),
        })
    }

    /// Create a child context under `self`, inheriting the notifier.
    /// This is the only operation that locks (child-set insert during
    /// hierarchy creation).
    pub fn create_child(self: &Arc<Self>, kind: ContextKind, budget: u64) -> Arc<Self> {
        let child = Arc::new(Self {
            kind,
            budget: AtomicU64::new(budget),
            current: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            alloc_count: AtomicU64::new(0),
            spill_bytes: AtomicU64::new(0),
            parent: Some(Arc::clone(self)),
            children: Mutex::new(Vec::new()),
            notifier: OnceLock::new(),
        });
        if let Some(notify) = self.notifier.get().cloned() {
            let _ = child.notifier.set(notify);
        }
        self.children
            .lock()
            .expect("memory context child set poisoned")
            .push(Arc::downgrade(&child));
        child
    }

    /// Install the change notifier once. Called by
    /// [`crate::MemoryManager`] on the global root before any child
    /// exists, so every descendant inherits it at creation.
    pub(crate) fn install_notifier(&self, notify: Arc<dyn Fn() + Send + Sync + 'static>) {
        let _ = self.notifier.set(notify);
    }

    /// Fire the change notifier if one is installed. Lock-free fast path:
    /// a single [`OnceLock`] read; no locks are taken here.
    fn fire(&self) {
        if let Some(notify) = self.notifier.get() {
            notify();
        }
    }

    /// Kind of this context.
    pub fn kind(&self) -> ContextKind {
        self.kind
    }

    /// Budget in bytes (fixed at creation).
    pub fn budget(&self) -> u64 {
        self.budget.load(Ordering::Relaxed)
    }

    /// Currently held bytes (sum of live reservations in this subtree).
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// High-water mark of [`Self::current`].
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Total number of successful reservations ever made on this context
    /// (monotonic; releases do not decrement it).
    pub fn alloc_count(&self) -> u64 {
        self.alloc_count.load(Ordering::Relaxed)
    }

    /// Total spilled bytes recorded on this subtree (see
    /// [`Self::record_spill`]).
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes.load(Ordering::Relaxed)
    }

    /// Current usage as a fraction of budget (`0.0` when budget is zero
    /// and nothing is held, `INFINITY` when over a zero budget).
    pub fn usage_ratio(&self) -> f64 {
        let budget = self.budget();
        let current = self.current();
        if budget == 0 {
            if current == 0 {
                0.0
            } else {
                f64::INFINITY
            }
        } else {
            current as f64 / budget as f64
        }
    }

    /// Snapshot of live children (prunes dead [`Weak`] entries).
    /// Takes the child-set lock; never called on the alloc path.
    pub fn children(&self) -> Vec<Arc<MemoryContext>> {
        let mut guard = self
            .children
            .lock()
            .expect("memory context child set poisoned");
        guard.retain(|w| w.strong_count() > 0);
        guard.iter().filter_map(Weak::upgrade).collect()
    }

    /// Depth of this node (global root is 0).
    pub fn depth(&self) -> usize {
        let mut depth = 0;
        let mut cur = self.parent.as_ref();
        while let Some(next) = cur {
            depth += 1;
            cur = next.parent.as_ref();
        }
        depth
    }

    /// Walk the enclosing chain for the nearest query id, if any.
    pub fn enclosing_query_id(&self) -> Option<u64> {
        let mut cur: Option<&MemoryContext> = Some(self);
        while let Some(ctx) = cur {
            match ctx.kind {
                ContextKind::Query { query_id, .. } | ContextKind::Operator { query_id, .. } => {
                    return Some(query_id)
                }
                _ => cur = ctx.parent.as_deref(),
            }
        }
        None
    }

    /// Try to reserve `bytes` on this context and every ancestor.
    ///
    /// Returns [`MemoryError::BudgetExceeded`] identifying the enforcing
    /// scope when any level is over budget; partial reservations are
    /// rolled back so all counters stay within budget. Never panics.
    pub fn try_allocate(&self, bytes: u64) -> Result<(), MemoryError> {
        if bytes == 0 {
            return Ok(());
        }
        // Collect the chain leaf-first, then reserve root-first so the
        // tightest (usually global) budget fails fast.
        let mut chain: Vec<&MemoryContext> = vec![self];
        let mut cur = self.parent.as_ref();
        while let Some(next) = cur {
            chain.push(next);
            cur = next.parent.as_ref();
        }
        let mut reserved: Vec<&MemoryContext> = Vec::with_capacity(chain.len());
        for ctx in chain.into_iter().rev() {
            if let Err(e) = ctx.reserve_one(bytes) {
                for done in reserved {
                    done.unreserve(bytes);
                }
                return Err(e);
            }
            reserved.push(ctx);
        }
        self.fire();
        Ok(())
    }

    /// Reserve `bytes` and return an RAII [`Allocation`] guard that
    /// releases them on drop. Same budget semantics as
    /// [`Self::try_allocate`].
    pub fn allocate(self: &Arc<Self>, bytes: u64) -> Result<Allocation, MemoryError> {
        self.try_allocate(bytes)?;
        Ok(Allocation {
            ctx: Some(Arc::clone(self)),
            bytes,
        })
    }

    /// Release a previous manual [`Self::try_allocate`] reservation.
    /// Saturates at zero; releasing more than held never panics and
    /// never drives counters negative.
    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut cur: Option<&MemoryContext> = Some(self);
        while let Some(ctx) = cur {
            ctx.release_one(bytes);
            cur = ctx.parent.as_deref();
        }
        self.fire();
    }

    /// Resize a live reservation from `old_bytes` to `new_bytes`.
    /// Growing performs a budgeted reservation (may fail, leaving the
    /// old reservation untouched); shrinking releases the difference.
    pub fn reallocate(&self, old_bytes: u64, new_bytes: u64) -> Result<(), MemoryError> {
        if new_bytes == old_bytes {
            return Ok(());
        }
        if new_bytes > old_bytes {
            self.try_allocate(new_bytes - old_bytes)
        } else {
            self.release(old_bytes - new_bytes);
            Ok(())
        }
    }

    /// Record `bytes` spilled (e.g. sort/hash operator spilling to the
    /// spill crate). Unbudgeted: spills are tracked, not limited, and
    /// propagate to every ancestor like reservations do.
    pub fn record_spill(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut cur: Option<&MemoryContext> = Some(self);
        while let Some(ctx) = cur {
            ctx.spill_bytes.fetch_add(bytes, Ordering::Relaxed);
            cur = ctx.parent.as_deref();
        }
    }

    /// Reserve `bytes` on this single level via CAS. Never exceeds the
    /// local budget even under contention.
    fn reserve_one(&self, bytes: u64) -> Result<(), MemoryError> {
        let budget = self.budget.load(Ordering::Relaxed);
        let mut cur = self.current.load(Ordering::Relaxed);
        loop {
            let next = match cur.checked_add(bytes) {
                Some(n) if n <= budget => n,
                _ => {
                    return Err(MemoryError::BudgetExceeded {
                        scope: self.kind.scope_name(),
                        requested: bytes,
                        available: budget.saturating_sub(cur),
                    });
                }
            };
            match self
                .current
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::Relaxed);
                    self.alloc_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(actual) => cur = actual,
            }
        }
    }

    /// Roll back one [`Self::reserve_one`].
    fn unreserve(&self, bytes: u64) {
        self.release_one(bytes);
        // Balanced with the fetch_add in reserve_one: this context is
        // only unreserved after a successful reserve_one on it.
        self.alloc_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Saturating single-level release.
    fn release_one(&self, bytes: u64) {
        let mut cur = self.current.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(bytes);
            match self
                .current
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

impl fmt::Debug for MemoryContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryContext")
            .field("kind", &self.kind)
            .field("budget", &self.budget())
            .field("current", &self.current())
            .field("peak", &self.peak())
            .field("alloc_count", &self.alloc_count())
            .field("spill_bytes", &self.spill_bytes())
            .finish_non_exhaustive()
    }
}

/// RAII reservation guard: releases its bytes on the originating context
/// (and ancestors) when dropped. Obtain via [`MemoryContext::allocate`].
pub struct Allocation {
    ctx: Option<Arc<MemoryContext>>,
    bytes: u64,
}

impl Allocation {
    /// Reserved byte count.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Detach the guard without releasing: returns the byte count and
    /// leaves the reservation charged (caller must [`MemoryContext::release`]
    /// it manually later).
    pub fn detach(mut self) -> u64 {
        let _ = self.ctx.take();
        let bytes = self.bytes;
        self.bytes = 0;
        bytes
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        if let Some(ctx) = self.ctx.take() {
            if self.bytes > 0 {
                ctx.release(self.bytes);
            }
        }
    }
}

impl fmt::Debug for Allocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Allocation")
            .field("bytes", &self.bytes)
            .field("scope", &self.ctx.as_ref().map(|c| c.kind().scope_name()))
            .finish()
    }
}
