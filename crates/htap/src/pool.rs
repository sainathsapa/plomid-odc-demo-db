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

//! Fixed resource pools with hard limits, reservations, and shares.
//!
//! Each pool carries the same shape: a hard byte `budget_bytes`, a live
//! `used` counter, an informational `reserved` floor, a semaphore-like
//! concurrency counter (`in_use` vs `max_concurrency`), a
//! `max_parallelism` degree-of-parallelism cap, and a `weight` for
//! weighted-share division. [`ResourceGovernor`](crate::governor::ResourceGovernor)
//! owns one of each and enforces the cross-pool invariants.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Which pool a class of work is accounted against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolKind {
    Oltp,
    Olap,
    Maintenance,
    System,
}

impl PoolKind {
    /// Stable human-readable pool name for diagnostics.
    pub fn name(self) -> &'static str {
        match self {
            PoolKind::Oltp => "oltp",
            PoolKind::Olap => "olap",
            PoolKind::Maintenance => "maintenance",
            PoolKind::System => "system",
        }
    }
}

/// A fixed resource pool: hard byte limit + reservation + shares/weights.
///
/// `used_bytes` / `in_use_slots` are atomics so admission checks and
/// accounting are lock-free; cross-pool invariants live in the governor.
#[derive(Debug)]
pub struct ResourcePool {
    kind: PoolKind,
    budget_bytes: u64,
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    reserved_bytes: u64,
    max_concurrency: usize,
    in_use_slots: AtomicUsize,
    peak_in_use_slots: AtomicUsize,
    max_parallelism: usize,
    weight: u32,
}

impl ResourcePool {
    /// Build a pool. `reserved_bytes` is informational here (the governor
    /// enforces the global OLTP-reservation invariant); `weight` is the
    /// pool's share weight for weighted division.
    pub fn new(
        kind: PoolKind,
        budget_bytes: u64,
        reserved_bytes: u64,
        max_concurrency: usize,
        max_parallelism: usize,
        weight: u32,
    ) -> Self {
        Self {
            kind,
            budget_bytes,
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            reserved_bytes,
            max_concurrency: max_concurrency.max(1),
            in_use_slots: AtomicUsize::new(0),
            peak_in_use_slots: AtomicUsize::new(0),
            max_parallelism: max_parallelism.max(1),
            weight: weight.max(1),
        }
    }

    pub fn kind(&self) -> PoolKind {
        self.kind
    }
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
    pub fn in_use_slots(&self) -> usize {
        self.in_use_slots.load(Ordering::Relaxed)
    }
    pub fn max_parallelism(&self) -> usize {
        self.max_parallelism
    }
    /// Share weight for weighted-fair division across pools/queries.
    pub fn weight(&self) -> u32 {
        self.weight
    }

    /// Bytes still available under the hard limit.
    pub fn available_bytes(&self) -> u64 {
        self.budget_bytes.saturating_sub(self.used_bytes())
    }

    /// Utilization percent of the hard budget (0-100).
    pub fn utilization_pct(&self) -> u8 {
        if self.budget_bytes == 0 {
            return 100;
        }
        let used = self.used_bytes().min(self.budget_bytes) as u128;
        ((used * 100) / self.budget_bytes as u128) as u8
    }

    /// Account an admission. Callers must check limits first.
    pub fn acquire(&self, bytes: u64, workers: usize) {
        let used = self.used_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak_bytes.fetch_max(used, Ordering::Relaxed);
        let slots = self.in_use_slots.fetch_add(workers, Ordering::Relaxed) + workers;
        self.peak_in_use_slots.fetch_max(slots, Ordering::Relaxed);
    }

    /// Return resources. Saturates at zero so accounting bugs cannot wrap.
    pub fn release(&self, bytes: u64, workers: usize) {
        self.used_bytes
            .fetch_sub(bytes.min(self.used_bytes()), Ordering::Relaxed);
        let slots = self.in_use_slots.load(Ordering::Relaxed);
        self.in_use_slots
            .store(slots.saturating_sub(workers), Ordering::Relaxed);
    }
}

/// The OLTP pool: hard budget = whole global budget (OLTP may burst into
/// shared space); the reservation is a guaranteed *floor*, not a ceiling.
pub type OltpPool = ResourcePool;
/// The OLAP pool: hard-capped; fenced out of the OLTP reserve.
pub type OlapPool = ResourcePool;
/// The maintenance pool: hard-capped; paused at critical pressure.
pub type MaintenancePool = ResourcePool;
/// The system pool: hard-capped reserve for control-plane work.
pub type SystemReservePool = ResourcePool;
