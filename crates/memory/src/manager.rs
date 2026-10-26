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

//! [`MemoryManager`]: owns the hierarchy roots, hands out contexts,
//! computes pressure, and dispatches pressure listeners.

use std::collections::HashMap;
use std::fmt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock, Weak,
};

use crate::config::MemoryConfig;
use crate::context::{CacheKind, ContextKind, MaintenanceOp, MemoryContext, WorkloadClass};
use crate::operator::OperatorKind;
use crate::pressure::PressureState;
use crate::stats::{
    CacheStats, ConnectionStats, GlobalStats, MaintenanceStats, QueryStats, WorkerStats,
};

/// Shared pressure-listener callback.
type PressureCallback = Arc<dyn Fn(PressureState) + Send + Sync + 'static>;

/// Owns the global [`MemoryContext`] and the maintenance / cache branch
/// roots, and manufactures every other context in the hierarchy.
///
/// Created as `Arc<Self>` because descendant contexts hold a notifier that
/// upgrades a [`Weak`] back to the manager:
///
/// ```rust
/// use plomid_memory::{MemoryConfig, MemoryManager};
/// let mgr = MemoryManager::new(MemoryConfig::default());
/// assert_eq!(mgr.pressure(), plomid_memory::PressureState::Normal);
/// ```
///
/// The manager (and every context) is `Send + Sync`; contexts may be
/// allocated from any thread.
pub struct MemoryManager {
    config: MemoryConfig,
    global: Arc<MemoryContext>,
    workloads: RwLock<HashMap<WorkloadClass, Arc<MemoryContext>>>,
    maintenance: Arc<MemoryContext>,
    cache_root: Arc<MemoryContext>,
    cache_partitions: RwLock<HashMap<CacheKind, Arc<MemoryContext>>>,
    last_notified: AtomicU8,
    listeners: RwLock<Vec<PressureCallback>>,
}

impl MemoryManager {
    /// Build a manager from `config`, creating the global root plus the
    /// maintenance and cache branch roots.
    pub fn new(config: MemoryConfig) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<Self>| {
            // Contexts fire `Fn()` after each accounting change; adapt to
            // the manager's transition check. The Weak breaks the
            // manager -> global -> notifier -> manager cycle.
            let fire: Arc<dyn Fn() + Send + Sync + 'static> = Arc::new({
                let weak = Weak::clone(weak);
                move || {
                    if let Some(mgr) = weak.upgrade() {
                        mgr.maybe_notify();
                    }
                }
            });
            let global = MemoryContext::new_root(ContextKind::Global, config.global_budget);
            global.install_notifier(fire);
            let maintenance = global.create_child(
                ContextKind::Maintenance {
                    operation: MaintenanceOp::General,
                },
                config.maintenance_budget,
            );
            let cache_root = global.create_child(
                ContextKind::Cache {
                    kind: CacheKind::General,
                },
                config.global_budget,
            );
            Self {
                config,
                global,
                workloads: RwLock::new(HashMap::new()),
                maintenance,
                cache_root,
                cache_partitions: RwLock::new(HashMap::new()),
                last_notified: AtomicU8::new(PressureState::Normal as u8),
                listeners: RwLock::new(Vec::new()),
            }
        })
    }

    /// The configuration this manager was built with.
    pub fn config(&self) -> &MemoryConfig {
        &self.config
    }

    /// The global root context.
    pub fn global_context(&self) -> Arc<MemoryContext> {
        Arc::clone(&self.global)
    }

    /// Budget configured for a workload branch.
    fn workload_budget(&self, class: WorkloadClass) -> u64 {
        match class {
            WorkloadClass::Oltp => self.config.oltp_reservation,
            WorkloadClass::Olap => self.config.olap_budget,
            WorkloadClass::System => self.config.global_budget,
        }
    }

    /// Get (creating on first use) the workload-class branch context.
    /// Hierarchy creation only; takes a lock solely on first creation.
    pub fn workload_context(&self, class: WorkloadClass) -> Arc<MemoryContext> {
        {
            let guard = self.workloads.read().expect("workload registry poisoned");
            if let Some(ctx) = guard.get(&class) {
                return Arc::clone(ctx);
            }
        }
        let mut guard = self.workloads.write().expect("workload registry poisoned");
        if let Some(ctx) = guard.get(&class) {
            return Arc::clone(ctx);
        }
        let budget = self.workload_budget(class);
        let ctx = self
            .global
            .create_child(ContextKind::WorkloadClass { class }, budget);
        guard.insert(class, Arc::clone(&ctx));
        ctx
    }

    /// Create a query context under `workload` with the configured
    /// default query budget.
    pub fn create_query_context(
        &self,
        workload: WorkloadClass,
        query_id: u64,
    ) -> Arc<MemoryContext> {
        self.create_query_context_with_budget(workload, query_id, self.config.query_budget_default)
    }

    /// Create a query context with an explicit budget.
    fn create_query_context_with_budget(
        &self,
        workload: WorkloadClass,
        query_id: u64,
        budget: u64,
    ) -> Arc<MemoryContext> {
        let parent = self.workload_context(workload);
        parent.create_child(ContextKind::Query { query_id, workload }, budget)
    }

    /// Create an operator context under `query` with a default budget:
    /// `result_buffer_budget` for [`OperatorKind::ResultBuffer`], the
    /// parent query's budget otherwise.
    pub fn create_operator_context(
        &self,
        query: &Arc<MemoryContext>,
        kind: OperatorKind,
    ) -> Arc<MemoryContext> {
        let budget = if kind == OperatorKind::ResultBuffer {
            self.config.result_buffer_budget
        } else {
            query.budget()
        };
        self.create_operator_context_with_budget(query, kind, budget)
    }

    /// Create an operator context with an explicit budget.
    pub fn create_operator_context_with_budget(
        &self,
        query: &Arc<MemoryContext>,
        kind: OperatorKind,
        budget: u64,
    ) -> Arc<MemoryContext> {
        let query_id = query.enclosing_query_id().unwrap_or(0);
        query.create_child(ContextKind::Operator { query_id, kind }, budget)
    }

    /// Create a temp-table workspace under `parent`, bounded by
    /// `temp_limit`. The node is accounted as
    /// [`OperatorKind::TempTable`].
    pub fn create_temp_context(&self, parent: &Arc<MemoryContext>) -> Arc<MemoryContext> {
        let query_id = parent.enclosing_query_id().unwrap_or(0);
        parent.create_child(
            ContextKind::Operator {
                query_id,
                kind: OperatorKind::TempTable,
            },
            self.config.temp_limit,
        )
    }

    /// Create a connection context directly under global with the
    /// configured default connection budget.
    pub fn create_connection_context(&self, connection_id: u64) -> Arc<MemoryContext> {
        self.create_connection_context_with_budget(connection_id, self.config.connection_budget)
    }

    /// Create a connection context with an explicit budget.
    fn create_connection_context_with_budget(
        &self,
        connection_id: u64,
        budget: u64,
    ) -> Arc<MemoryContext> {
        self.global
            .create_child(ContextKind::Connection { connection_id }, budget)
    }

    /// Create a worker context under `parent` (a query or connection
    /// context) with the configured default worker budget.
    pub fn create_worker_context(
        &self,
        parent: &Arc<MemoryContext>,
        worker_id: u64,
    ) -> Arc<MemoryContext> {
        self.create_worker_context_with_budget(parent, worker_id, self.config.worker_budget)
    }

    /// Create a worker context with an explicit budget.
    fn create_worker_context_with_budget(
        &self,
        parent: &Arc<MemoryContext>,
        worker_id: u64,
        budget: u64,
    ) -> Arc<MemoryContext> {
        parent.create_child(ContextKind::Worker { worker_id }, budget)
    }

    /// The maintenance branch root.
    pub fn maintenance_context(&self) -> Arc<MemoryContext> {
        Arc::clone(&self.maintenance)
    }

    /// Create a maintenance task node under the maintenance branch,
    /// sharing the branch budget.
    pub fn create_maintenance_task(&self, operation: MaintenanceOp) -> Arc<MemoryContext> {
        let budget = self.maintenance.budget();
        self.maintenance
            .create_child(ContextKind::Maintenance { operation }, budget)
    }

    /// The cache branch root.
    pub fn cache_context(&self) -> Arc<MemoryContext> {
        Arc::clone(&self.cache_root)
    }

    /// Get (creating on first use) the cache partition for `kind`.
    pub fn cache_partition(&self, kind: CacheKind) -> Arc<MemoryContext> {
        {
            let guard = self
                .cache_partitions
                .read()
                .expect("cache partition registry poisoned");
            if let Some(ctx) = guard.get(&kind) {
                return Arc::clone(ctx);
            }
        }
        let mut guard = self
            .cache_partitions
            .write()
            .expect("cache partition registry poisoned");
        if let Some(ctx) = guard.get(&kind) {
            return Arc::clone(ctx);
        }
        let ctx = self
            .cache_root
            .create_child(ContextKind::Cache { kind }, self.config.global_budget);
        guard.insert(kind, Arc::clone(&ctx));
        ctx
    }

    /// Current pressure from global usage. Pure loads; never blocks.
    pub fn pressure(&self) -> PressureState {
        PressureState::from_usage(self.global.usage_ratio(), &self.config.pressure_thresholds)
    }

    /// Last pressure state dispatched to listeners.
    pub fn notified_state(&self) -> PressureState {
        PressureState::from_u8(self.last_notified.load(Ordering::Acquire))
    }

    /// Evaluate pressure and invoke listeners when the state changed
    /// since the last dispatch. Returns the current state.
    ///
    /// Allocation/release paths call this automatically via the
    /// context notifier; call it explicitly to poll.
    pub fn check_pressure(&self) -> PressureState {
        self.maybe_notify();
        self.pressure()
    }

    /// Register a pressure listener invoked on every state transition.
    /// Listeners must be non-blocking and must not allocate from memory
    /// contexts (re-entrant allocation from a listener is unsupported).
    /// Transitions only are reported; the current state is not replayed
    /// at registration.
    pub fn on_pressure(&self, listener: impl Fn(PressureState) + Send + Sync + 'static) {
        self.listeners
            .write()
            .expect("pressure listener registry poisoned")
            .push(Arc::new(listener));
    }

    /// Fast path: one atomic load when the state is stable. Only a
    /// genuine transition takes the listener-registry lock (and the
    /// lock is released before any callback runs).
    fn maybe_notify(&self) {
        let mut state = self.pressure();
        loop {
            let prev = self.last_notified.load(Ordering::Acquire);
            if state as u8 == prev {
                return;
            }
            match self.last_notified.compare_exchange(
                prev,
                state as u8,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let listeners = self
                        .listeners
                        .read()
                        .expect("pressure listener registry poisoned")
                        .clone();
                    for listener in &listeners {
                        listener(state);
                    }
                    return;
                }
                Err(_) => {
                    // Lost a race with a concurrent transition; re-read
                    // and converge instead of notifying a stale state.
                    state = self.pressure();
                }
            }
        }
    }

    /// Engine-wide rollup.
    pub fn global_stats(&self) -> GlobalStats {
        GlobalStats::snapshot(&self.global, self.pressure())
    }

    /// Per-query rollup for a query context.
    pub fn query_stats(&self, query: &Arc<MemoryContext>) -> QueryStats {
        QueryStats::snapshot(query)
    }

    /// Per-connection rollup for a connection context.
    pub fn connection_stats(&self, ctx: &Arc<MemoryContext>) -> ConnectionStats {
        ConnectionStats::snapshot(ctx)
    }

    /// Per-worker rollup for a worker context.
    pub fn worker_stats(&self, ctx: &Arc<MemoryContext>) -> WorkerStats {
        WorkerStats::snapshot(ctx)
    }

    /// Maintenance-branch rollup.
    pub fn maintenance_stats(&self) -> MaintenanceStats {
        MaintenanceStats::snapshot(&self.maintenance)
    }

    /// Cache-branch rollup.
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats::snapshot(&self.cache_root)
    }
}

impl fmt::Debug for MemoryManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryManager")
            .field("config", &self.config)
            .field("global", &self.global)
            .field("pressure", &self.pressure())
            .finish_non_exhaustive()
    }
}
