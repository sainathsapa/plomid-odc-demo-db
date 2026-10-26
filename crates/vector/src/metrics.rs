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
//! Lock-free aggregate counters for the vector subsystem.
//!
//! Counters only — never per-vector or per-query logging. Snapshot with
//! [`VectorMetrics::snapshot`] for EXPLAIN/diagnostics export.

use std::sync::atomic::{AtomicU64, Ordering};

/// Aggregate vector counters (all atomics; `&self` mutation).
#[derive(Debug, Default)]
pub struct VectorMetrics {
    exact_queries: AtomicU64,
    ann_queries: AtomicU64,
    hnsw_queries: AtomicU64,
    ivf_queries: AtomicU64,
    diskann_queries: AtomicU64,
    sparse_queries: AtomicU64,
    binary_queries: AtomicU64,
    hybrid_queries: AtomicU64,
    rerank_queries: AtomicU64,
    build_latency_us: AtomicU64,
    index_bytes: AtomicU64,
    tombstone_ratio_x1000: AtomicU64,
    // Single-node storage / NUMA / freshness diagnostics (Phase 41 + G/L/J).
    // Counters only; last-decision detail lives in the policy structs that
    // own the human-readable `explain()` lines.
    storage_memory_selected: AtomicU64,
    storage_file_selected: AtomicU64,
    storage_mmap_selected: AtomicU64,
    storage_fallbacks: AtomicU64,
    numa_single_node: AtomicU64,
    numa_multi_node: AtomicU64,
    stale_plan_fallbacks: AtomicU64,
    // Phase 48 + 58 diagnostics.
    backups_completed: AtomicU64,
    restores_completed: AtomicU64,
    htap_degraded: AtomicU64,
    htap_rejected: AtomicU64,
}

/// Plain snapshot for diagnostics export.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct VectorMetricsSnapshot {
    /// Exact-scan queries served.
    pub exact_queries: u64,
    /// ANN queries served.
    pub ann_queries: u64,
    /// Queries served via HNSW.
    pub hnsw_queries: u64,
    /// Queries served via IVF.
    pub ivf_queries: u64,
    /// Queries served via DiskANN.
    pub diskann_queries: u64,
    /// Sparse-index queries served.
    pub sparse_queries: u64,
    /// Binary-index queries served.
    pub binary_queries: u64,
    /// Hybrid (dense+sparse) queries served.
    pub hybrid_queries: u64,
    /// Queries that performed exact rerank.
    pub rerank_queries: u64,
    /// Accumulated index-build latency.
    pub build_latency_us: u64,
    /// Current index footprint gauge.
    pub index_bytes: u64,
    /// Tombstone permille gauge (`0..=1000`).
    pub tombstone_ratio_x1000: u64,
    /// Storage-policy selections since process start (Phase 41).
    pub storage_memory_selected: u64,
    /// Storage-policy selections since process start (Phase 41).
    pub storage_file_selected: u64,
    /// Storage-policy selections since process start (Phase 41).
    pub storage_mmap_selected: u64,
    /// Times the requested storage mode fell back to a safe mode.
    pub storage_fallbacks: u64,
    /// Batch dispatches that ran in single-node (UMA/unknown) NUMA mode.
    pub numa_single_node: u64,
    /// Batch dispatches that ran with multi-node (multi-socket) topology.
    pub numa_multi_node: u64,
    /// Planner decisions forced conservative by stale/missing freshness.
    pub stale_plan_fallbacks: u64,
    /// Backups finalized (Phase 48).
    pub backups_completed: u64,
    /// Restores finalized (Phase 48).
    pub restores_completed: u64,
    /// Vector operations degraded under HTAP pressure (Phase 58).
    pub htap_degraded: u64,
    /// Vector operations rejected under HTAP pressure (Phase 58).
    pub htap_rejected: u64,
}

impl VectorMetrics {
    /// Zero-initialized counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Consistent (modulo races) snapshot.
    #[must_use]
    pub fn snapshot(&self) -> VectorMetricsSnapshot {
        VectorMetricsSnapshot {
            exact_queries: self.exact_queries.load(Ordering::Relaxed),
            ann_queries: self.ann_queries.load(Ordering::Relaxed),
            hnsw_queries: self.hnsw_queries.load(Ordering::Relaxed),
            ivf_queries: self.ivf_queries.load(Ordering::Relaxed),
            diskann_queries: self.diskann_queries.load(Ordering::Relaxed),
            sparse_queries: self.sparse_queries.load(Ordering::Relaxed),
            binary_queries: self.binary_queries.load(Ordering::Relaxed),
            hybrid_queries: self.hybrid_queries.load(Ordering::Relaxed),
            rerank_queries: self.rerank_queries.load(Ordering::Relaxed),
            build_latency_us: self.build_latency_us.load(Ordering::Relaxed),
            index_bytes: self.index_bytes.load(Ordering::Relaxed),
            tombstone_ratio_x1000: self.tombstone_ratio_x1000.load(Ordering::Relaxed),
            storage_memory_selected: self.storage_memory_selected.load(Ordering::Relaxed),
            storage_file_selected: self.storage_file_selected.load(Ordering::Relaxed),
            storage_mmap_selected: self.storage_mmap_selected.load(Ordering::Relaxed),
            storage_fallbacks: self.storage_fallbacks.load(Ordering::Relaxed),
            numa_single_node: self.numa_single_node.load(Ordering::Relaxed),
            numa_multi_node: self.numa_multi_node.load(Ordering::Relaxed),
            stale_plan_fallbacks: self.stale_plan_fallbacks.load(Ordering::Relaxed),
            backups_completed: self.backups_completed.load(Ordering::Relaxed),
            restores_completed: self.restores_completed.load(Ordering::Relaxed),
            htap_degraded: self.htap_degraded.load(Ordering::Relaxed),
            htap_rejected: self.htap_rejected.load(Ordering::Relaxed),
        }
    }

    /// Record one finished build's latency sample (accumulating).
    pub fn observe_build_latency_us(&self, us: u64) {
        self.build_latency_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Set current index footprint gauge.
    pub fn set_index_bytes(&self, bytes: u64) {
        self.index_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set tombstone ratio permille gauge (`0..=1000`).
    pub fn set_tombstone_ratio_x1000(&self, ratio: u64) {
        self.tombstone_ratio_x1000
            .store(ratio.min(1000), Ordering::Relaxed);
    }

    /// Increment helpers (one per counter).
    pub fn inc_exact(&self) {
        self.exact_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_ann(&self) {
        self.ann_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_hnsw(&self) {
        self.hnsw_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_ivf(&self) {
        self.ivf_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_diskann(&self) {
        self.diskann_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_sparse(&self) {
        self.sparse_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_binary(&self) {
        self.binary_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_hybrid(&self) {
        self.hybrid_queries.fetch_add(1, Ordering::Relaxed);
    }
    /// Increment helpers (one per counter).
    pub fn inc_rerank(&self) {
        self.rerank_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one storage-policy decision (Phase 41 diagnostics).
    ///
    /// `mode` is the *selected* mode (`memory`/`file`/`mmap`);
    /// `fallback` is true when the request fell back to a safe mode.
    pub fn observe_storage_decision(&self, mode: &str, fallback: bool) {
        match mode {
            "memory" => self.storage_memory_selected.fetch_add(1, Ordering::Relaxed),
            "mmap" => self.storage_mmap_selected.fetch_add(1, Ordering::Relaxed),
            _ => self.storage_file_selected.fetch_add(1, Ordering::Relaxed),
        };
        if fallback {
            self.storage_fallbacks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record one NUMA dispatch (Gap G diagnostics).
    pub fn observe_numa_dispatch(&self, multi_node: bool) {
        if multi_node {
            self.numa_multi_node.fetch_add(1, Ordering::Relaxed);
        } else {
            self.numa_single_node.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record one freshness-forced conservative plan (Gap J diagnostics).
    pub fn observe_stale_fallback(&self) {
        self.stale_plan_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one finalized backup (Phase 48 diagnostics).
    pub fn observe_backup(&self) {
        self.backups_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one finalized restore (Phase 48 diagnostics).
    pub fn observe_restore(&self) {
        self.restores_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one HTAP admission decision (Phase 58 diagnostics).
    pub fn observe_htap_decision(&self, degraded: bool, rejected: bool) {
        if degraded {
            self.htap_degraded.fetch_add(1, Ordering::Relaxed);
        }
        if rejected {
            self.htap_rejected.fetch_add(1, Ordering::Relaxed);
        }
    }
}
