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

//! Resource governor: hard limits, OLTP reservation, admission control.
//!
//! The governor owns the four pools plus the global budget and answers one
//! question per request: [`admit`](ResourceGovernor::admit) or not.
//!
//! Invariants:
//! - **Hard limits**: no pool ever exceeds its budget; no request exceeds
//!   the global budget.
//! - **OLTP reservation**: while OLTP pressure exists (OLTP holds >= half
//!   the reservation, or global pressure is at/above `Pressure`), shared
//!   traffic (OLAP / batch / background / maintenance / interactive
//!   analytics) may not push global usage past
//!   `global - (reservation - oltp_used)`. The unconsumed slice of the
//!   reservation is fenced off. Without OLTP pressure the shared pools may
//!   borrow the idle reserve (up to the global budget).
//! - **OLTP preference**: OLTP and system traffic bypass the reserve fence,
//!   the critical-pressure shed, and all derating.
//! - **Class -> pool mapping**: Oltp -> OLTP pool, System -> system pool,
//!   Maintenance -> maintenance pool, everything else (Olap, Batch,
//!   Background, InteractiveAnalytics) -> OLAP pool.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::config::{ConfigError, HtapConfig, PressureState};
use crate::maintenance::throttle;
use crate::parallelism::{derate_concurrency, parallelism_for};
use crate::pool::{PoolKind, ResourcePool};
use crate::snapshot::HtapSnapshot;
use crate::tenant::{TenantId, TenantRegistry};
use crate::workload::WorkloadClass;

/// Admission decision for a resource request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    /// Granted. `reduced_parallelism` is the degree of parallelism the
    /// query should run with (already derated for pressure; OLTP and
    /// system traffic are never derated).
    Admit { reduced_parallelism: usize },
    /// Not granted now, but retryable: concurrency exhausted, pool
    /// temporarily full, maintenance paused, or load shed under critical
    /// pressure.
    Queue { reason: String },
    /// Refused: hard limit, per-query limit, tenant quota, global
    /// exhaustion, or OLTP-reserve violation. Retrying without freeing
    /// resources (or dropping pressure) will not help.
    Reject { reason: String },
}

impl Admission {
    /// True for [`Admission::Admit`].
    pub fn is_admitted(&self) -> bool {
        matches!(self, Admission::Admit { .. })
    }
}

/// Fixed per-OLAP-query parallelism while maintenance is degraded.
const MAINTENANCE_PARALLELISM: usize = 2;

/// Owns all pools and the global budget; enforces admission control.
#[derive(Debug)]
pub struct ResourceGovernor {
    config: HtapConfig,
    global_budget_bytes: u64,
    global_used_bytes: AtomicU64,
    global_peak_bytes: AtomicU64,
    oltp_pool: ResourcePool,
    olap_pool: ResourcePool,
    maintenance_pool: ResourcePool,
    system_pool: ResourcePool,
    spill_total_bytes: AtomicU64,
    result_buffer_bytes: AtomicU64,
    cache_bytes: AtomicU64,
    tenants: Mutex<TenantRegistry>,
}

impl ResourceGovernor {
    /// Build a governor from a validated config.
    pub fn new(config: HtapConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        // OLTP pool budget = global: the reservation is a guaranteed floor,
        // not a ceiling — OLTP may burst into shared space.
        let oltp_pool = ResourcePool::new(
            PoolKind::Oltp,
            config.global_budget_bytes,
            config.oltp_reservation_bytes,
            config.oltp_max_concurrency,
            config.oltp_max_parallelism,
            8,
        );
        let olap_pool = ResourcePool::new(
            PoolKind::Olap,
            config.olap_budget_bytes,
            0,
            config.olap_max_concurrency,
            config.olap_max_parallelism,
            4,
        );
        let maintenance_pool = ResourcePool::new(
            PoolKind::Maintenance,
            config.maintenance_budget_bytes,
            0,
            config.maintenance_max_concurrency,
            MAINTENANCE_PARALLELISM,
            1,
        );
        let system_pool = ResourcePool::new(
            PoolKind::System,
            config.system_reserve_bytes,
            config.system_reserve_bytes,
            config.system_max_concurrency,
            1,
            10,
        );
        Ok(Self {
            global_budget_bytes: config.global_budget_bytes,
            config,
            global_used_bytes: AtomicU64::new(0),
            global_peak_bytes: AtomicU64::new(0),
            oltp_pool,
            olap_pool,
            maintenance_pool,
            system_pool,
            spill_total_bytes: AtomicU64::new(0),
            result_buffer_bytes: AtomicU64::new(0),
            cache_bytes: AtomicU64::new(0),
            tenants: Mutex::new(TenantRegistry::new()),
        })
    }

    /// The active config.
    pub fn config(&self) -> &HtapConfig {
        &self.config
    }

    /// Pool a class is accounted against.
    pub fn pool_kind_for(class: WorkloadClass) -> PoolKind {
        match class {
            WorkloadClass::Oltp => PoolKind::Oltp,
            WorkloadClass::System => PoolKind::System,
            WorkloadClass::Maintenance => PoolKind::Maintenance,
            WorkloadClass::Olap
            | WorkloadClass::Background
            | WorkloadClass::InteractiveAnalytics
            | WorkloadClass::Batch => PoolKind::Olap,
        }
    }

    fn pool_for(&self, class: WorkloadClass) -> &ResourcePool {
        match Self::pool_kind_for(class) {
            PoolKind::Oltp => &self.oltp_pool,
            PoolKind::Olap => &self.olap_pool,
            PoolKind::Maintenance => &self.maintenance_pool,
            PoolKind::System => &self.system_pool,
        }
    }

    /// Privileged traffic bypasses the reserve fence and load shedding.
    fn is_privileged(class: WorkloadClass) -> bool {
        matches!(class, WorkloadClass::Oltp | WorkloadClass::System)
    }

    /// OLAP-family traffic subject to fair sharing and shedding.
    fn is_sheddable(class: WorkloadClass) -> bool {
        matches!(
            class,
            WorkloadClass::Olap | WorkloadClass::Batch | WorkloadClass::Background
        )
    }

    fn is_olap_family(class: WorkloadClass) -> bool {
        matches!(
            class,
            WorkloadClass::Olap
                | WorkloadClass::Batch
                | WorkloadClass::Background
                | WorkloadClass::InteractiveAnalytics
        )
    }

    /// Global utilization percent (0-100).
    pub fn utilization_pct(&self) -> u8 {
        if self.global_budget_bytes == 0 {
            return 100;
        }
        let used = self
            .global_used_bytes
            .load(Ordering::Relaxed)
            .min(self.global_budget_bytes) as u128;
        ((used * 100) / self.global_budget_bytes as u128) as u8
    }

    /// Current pressure state from global utilization.
    pub fn pressure(&self) -> PressureState {
        self.config.pressure_for(self.utilization_pct())
    }

    /// True when the OLTP reserve must be fenced: OLTP already holds at
    /// least half the reservation, or global pressure is at/above
    /// `Pressure` (OLTP will need the reserve imminently).
    pub fn oltp_pressure_exists(&self) -> bool {
        let reservation = self.config.oltp_reservation_bytes;
        let oltp_used = self.oltp_pool.used_bytes();
        oltp_used.saturating_mul(2) >= reservation || self.pressure() >= PressureState::Pressure
    }

    /// Effective OLAP concurrency cap right now (derated with pressure).
    pub fn effective_olap_concurrency(&self) -> usize {
        derate_concurrency(self.config.olap_max_concurrency, self.pressure())
    }

    /// Effective concurrency cap for a class right now.
    fn effective_cap(&self, class: WorkloadClass) -> usize {
        let pressure = self.pressure();
        match Self::pool_kind_for(class) {
            PoolKind::Oltp => self.config.oltp_max_concurrency,
            PoolKind::System => self.config.system_max_concurrency,
            PoolKind::Olap => derate_concurrency(self.config.olap_max_concurrency, pressure),
            PoolKind::Maintenance => {
                derate_concurrency(self.config.maintenance_max_concurrency, pressure)
            }
        }
    }

    /// Admit-or-not for `(class, bytes, workers)`.
    ///
    /// `workers` is the number of concurrency slots requested (usually 1).
    /// On success the returned [`Admission::Admit`] carries the
    /// pressure-derated parallelism the query should run with.
    pub fn admit(&self, class: WorkloadClass, bytes: u64, workers: usize) -> Admission {
        if workers == 0 {
            return Admission::Reject {
                reason: "workers must be >= 1".to_string(),
            };
        }
        if Self::is_olap_family(class) && bytes > self.config.olap_max_bytes_per_query {
            return Admission::Reject {
                reason: format!(
                    "request {bytes}B exceeds per-query limit {}B: one query cannot starve the OLAP pool",
                    self.config.olap_max_bytes_per_query
                ),
            };
        }
        let pool = self.pool_for(class);
        if bytes > pool.budget_bytes() {
            return Admission::Reject {
                reason: format!(
                    "request {bytes}B exceeds {} pool budget {}B",
                    pool.kind().name(),
                    pool.budget_bytes()
                ),
            };
        }
        if bytes > self.global_budget_bytes {
            return Admission::Reject {
                reason: format!(
                    "request {bytes}B exceeds global budget {}B",
                    self.global_budget_bytes
                ),
            };
        }
        let pressure = self.pressure();

        // Maintenance never starves OLTP: paused (queued) at critical.
        if class == WorkloadClass::Maintenance && throttle(pressure).is_paused() {
            return Admission::Queue {
                reason: "maintenance paused under critical pressure".to_string(),
            };
        }

        // Hard global / reserve fence first so hard violations report as
        // Reject rather than being masked by transient Queue states.
        let global_used = self.global_used_bytes.load(Ordering::Relaxed);
        if Self::is_privileged(class) {
            if class == WorkloadClass::System && pool.used_bytes() + bytes > pool.budget_bytes() {
                return Admission::Reject {
                    reason: "system reserve exhausted".to_string(),
                };
            }
            if global_used + bytes > self.global_budget_bytes {
                return Admission::Reject {
                    reason: "global budget exhausted".to_string(),
                };
            }
        } else {
            // OLTP-reserve fence (hard reject).
            let ceiling = if self.oltp_pressure_exists() {
                let reservation = self.config.oltp_reservation_bytes;
                let oltp_used = self.oltp_pool.used_bytes().min(reservation);
                self.global_budget_bytes - (reservation - oltp_used)
            } else {
                self.global_budget_bytes
            };
            if global_used + bytes > ceiling {
                if global_used + bytes > self.global_budget_bytes {
                    return Admission::Reject {
                        reason: "global budget exhausted".to_string(),
                    };
                }
                return Admission::Reject {
                    reason: format!(
                        "would consume OLTP reservation ({}B): OLTP pressure exists, shared traffic fenced out",
                        self.config.oltp_reservation_bytes
                    ),
                };
            }
        }

        // Concurrency (semaphore-ish) gate with pressure-derated caps.
        let cap = self.effective_cap(class);
        if pool.in_use_slots() + workers > cap {
            return Admission::Queue {
                reason: format!(
                    "{} pool concurrency exhausted ({}+{workers} > cap {cap} under {pressure:?})",
                    pool.kind().name(),
                    pool.in_use_slots()
                ),
            };
        }

        if !Self::is_privileged(class) {
            // Shared traffic: pool budget is a soft (queueable) ceiling.
            if pool.used_bytes() + bytes > pool.budget_bytes() {
                return Admission::Queue {
                    reason: format!("{} pool budget exhausted, queued", pool.kind().name()),
                };
            }
            // Shed sheddable OLAP load at critical pressure (retryable).
            if pressure == PressureState::Critical && Self::is_sheddable(class) {
                return Admission::Queue {
                    reason: "olap load shed under critical pressure".to_string(),
                };
            }
        }

        pool.acquire(bytes, workers);
        let used = self.global_used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.global_peak_bytes.fetch_max(used, Ordering::Relaxed);
        Admission::Admit {
            reduced_parallelism: parallelism_for(class, self.pressure(), pool.max_parallelism()),
        }
    }

    /// Return resources previously admitted.
    pub fn release(&self, class: WorkloadClass, bytes: u64, workers: usize) {
        self.pool_for(class).release(bytes, workers);
        let used = self.global_used_bytes.load(Ordering::Relaxed);
        self.global_used_bytes
            .store(used.saturating_sub(bytes), Ordering::Relaxed);
    }

    /// Tenant-scoped admission: tenant quota first, then global rules.
    /// Unknown tenants are unlimited (single-tenant default).
    pub fn admit_for_tenant(
        &self,
        tenant: &TenantId,
        class: WorkloadClass,
        bytes: u64,
        workers: usize,
    ) -> Admission {
        {
            let registry = self.tenants.lock().expect("tenant lock");
            if let Err(reason) = registry.check(tenant, bytes, workers) {
                return Admission::Reject { reason };
            }
        }
        let decision = self.admit(class, bytes, workers);
        if decision.is_admitted() {
            self.tenants
                .lock()
                .expect("tenant lock")
                .record(tenant, bytes, workers);
        }
        decision
    }

    /// Return tenant-scoped resources.
    pub fn release_for_tenant(
        &self,
        tenant: &TenantId,
        class: WorkloadClass,
        bytes: u64,
        workers: usize,
    ) {
        self.release(class, bytes, workers);
        self.tenants
            .lock()
            .expect("tenant lock")
            .release(tenant, bytes, workers);
    }

    /// Register (or overwrite) a tenant quota. All configurable.
    pub fn register_tenant(&self, tenant: TenantId, quota: crate::tenant::TenantQuota) {
        self.tenants
            .lock()
            .expect("tenant lock")
            .register(tenant, quota);
    }

    /// Count cache occupancy in the global budget (caches shed via
    /// [`UnifiedBufferManager::evict_under_pressure`](crate::buffer_manager::UnifiedBufferManager::evict_under_pressure)).
    pub fn account_cache_bytes(&self, bytes: u64) {
        self.cache_bytes.fetch_add(bytes, Ordering::Relaxed);
        let used = self.global_used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.global_peak_bytes.fetch_max(used, Ordering::Relaxed);
    }

    /// Mirror a cache eviction into global stats.
    pub fn release_cache_bytes(&self, bytes: u64) {
        let cached = self.cache_bytes.load(Ordering::Relaxed);
        self.cache_bytes
            .store(cached.saturating_sub(bytes), Ordering::Relaxed);
        let used = self.global_used_bytes.load(Ordering::Relaxed);
        self.global_used_bytes
            .store(used.saturating_sub(bytes), Ordering::Relaxed);
    }

    /// Count result-buffer bytes in the global budget.
    pub fn account_result_buffer_bytes(&self, bytes: u64) {
        self.result_buffer_bytes.fetch_add(bytes, Ordering::Relaxed);
        let used = self.global_used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.global_peak_bytes.fetch_max(used, Ordering::Relaxed);
    }

    /// Release result-buffer bytes.
    pub fn release_result_buffer_bytes(&self, bytes: u64) {
        let held = self.result_buffer_bytes.load(Ordering::Relaxed);
        self.result_buffer_bytes
            .store(held.saturating_sub(bytes), Ordering::Relaxed);
        let used = self.global_used_bytes.load(Ordering::Relaxed);
        self.global_used_bytes
            .store(used.saturating_sub(bytes), Ordering::Relaxed);
    }

    /// Record spilled bytes (spill lives on disk: counted in totals, not in
    /// the global memory budget).
    pub fn record_spill_bytes(&self, bytes: u64) {
        self.spill_total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    // ---- diagnostics getters ----

    pub fn global_budget_bytes(&self) -> u64 {
        self.global_budget_bytes
    }
    pub fn global_used_bytes(&self) -> u64 {
        self.global_used_bytes.load(Ordering::Relaxed)
    }
    pub fn global_peak_bytes(&self) -> u64 {
        self.global_peak_bytes.load(Ordering::Relaxed)
    }
    /// Bytes still available before the global budget is exhausted.
    pub fn oltp_headroom_bytes(&self) -> u64 {
        self.global_budget_bytes
            .saturating_sub(self.global_used_bytes())
    }
    pub fn oltp_used_bytes(&self) -> u64 {
        self.oltp_pool.used_bytes()
    }
    pub fn oltp_reserved_bytes(&self) -> u64 {
        self.config.oltp_reservation_bytes
    }
    pub fn oltp_in_use_slots(&self) -> usize {
        self.oltp_pool.in_use_slots()
    }
    pub fn olap_used_bytes(&self) -> u64 {
        self.olap_pool.used_bytes()
    }
    pub fn olap_in_use_slots(&self) -> usize {
        self.olap_pool.in_use_slots()
    }
    pub fn maintenance_used_bytes(&self) -> u64 {
        self.maintenance_pool.used_bytes()
    }
    pub fn system_used_bytes(&self) -> u64 {
        self.system_pool.used_bytes()
    }
    pub fn spill_total_bytes(&self) -> u64 {
        self.spill_total_bytes.load(Ordering::Relaxed)
    }
    pub fn result_buffer_bytes(&self) -> u64 {
        self.result_buffer_bytes.load(Ordering::Relaxed)
    }
    pub fn cache_bytes(&self) -> u64 {
        self.cache_bytes.load(Ordering::Relaxed)
    }

    /// Base snapshot (pool/global counters only; use
    /// [`HtapSnapshot::collect`] to attach the live OLAP slot count).
    pub fn snapshot(&self) -> HtapSnapshot {
        HtapSnapshot {
            global_budget_bytes: self.global_budget_bytes(),
            global_used_bytes: self.global_used_bytes(),
            global_peak_bytes: self.global_peak_bytes(),
            utilization_pct: self.utilization_pct(),
            pressure: self.pressure(),
            oltp_used_bytes: self.oltp_used_bytes(),
            oltp_reserved_bytes: self.oltp_reserved_bytes(),
            oltp_in_use_slots: self.oltp_in_use_slots(),
            olap_used_bytes: self.olap_used_bytes(),
            olap_in_use_slots: self.olap_in_use_slots(),
            maintenance_used_bytes: self.maintenance_used_bytes(),
            system_used_bytes: self.system_used_bytes(),
            cache_bytes: self.cache_bytes(),
            result_buffer_bytes: self.result_buffer_bytes(),
            spill_total_bytes: self.spill_total_bytes(),
            admitted_olap: 0,
            waiting_count: 0,
            waiting: Vec::new(),
            per_query: Vec::new(),
        }
    }
}
