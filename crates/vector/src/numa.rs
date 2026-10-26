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
//! NUMA awareness for one physical machine (Gap G).
//!
//! NUMA here means optimizing execution across CPU/memory domains *within a
//! single machine* (multi-socket server locality). It is not distributed
//! execution: no sharding, no scatter/gather, no cluster work.
//!
//! The same binary behaves correctly on laptops (no NUMA), single-socket
//! servers, multi-socket servers, ARM, and x86. Correctness never depends
//! on NUMA: topology only sizes worker chunks; results are bit-identical
//! in every mode (deterministic `(distance, row_id)` order per query).
//!
//! No platform-specific dependencies: detection reuses
//! `plomid_hw::HardwareProfile::numa_nodes` (count-only, `None` on
//! UMA/unknown) plus `available_parallelism`. There are no affinity
//! syscalls here (those would need `unsafe`/`libc`); CPU/memory pinning is
//! a documented future extension point behind [`NumaPolicy::pinning`]
//! (always `false` today). Where true NUMA hardware is unavailable, the
//! [`NumaTopology::test_seam`] constructor provides deterministic topology
//! injection for tests.

use crate::{Result, VectorError};

/// Env var for the NUMA switch: `auto` (default) | `off` | `<N nodes>`.
pub const NUMA_ENV_VAR: &str = "PLOMID_VECTOR_NUMA";

/// NUMA execution mode (diagnostic label; behavior differs only in chunking).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum NumaMode {
    /// NUMA ignored (UMA laptop, disabled, or unknown topology).
    #[default]
    Single,
    /// Multi-node topology observed; queries striped across nodes.
    Striped,
}

impl NumaMode {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Striped => "striped",
        }
    }

    /// True when striping across nodes.
    #[must_use]
    pub const fn is_multi_node(self) -> bool {
        matches!(self, Self::Striped)
    }
}

/// Observed CPU/memory topology for one machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumaTopology {
    /// NUMA nodes visible to this process (`1` = UMA/single).
    pub nodes: usize,
    /// Logical CPUs (total across all nodes).
    pub cpus: usize,
    /// Whether the count came from real detection (`false` = test seam).
    pub detected: bool,
    /// Why the topology looks this way, one line.
    pub note: String,
}

impl NumaTopology {
    /// Detect topology on this machine. Never fails: unknown becomes a
    /// single-node topology (correct fallback; striping needs ≥ 2 nodes).
    #[must_use]
    pub fn detect() -> Self {
        if let Ok(raw) = std::env::var(NUMA_ENV_VAR) {
            match raw.trim().to_ascii_lowercase().as_str() {
                "off" | "0" | "false" | "no" | "disabled" | "single" => {
                    return Self {
                        nodes: 1,
                        cpus: current_cpus(),
                        detected: true,
                        note: format!("{NUMA_ENV_VAR}=off: NUMA disabled by operator"),
                    };
                }
                "auto" | "" | "on" => {}
                other => {
                    if let Ok(n) = other.parse::<usize>() {
                        if (1..=1024).contains(&n) {
                            return Self {
                                nodes: n,
                                cpus: current_cpus(),
                                detected: false,
                                note: format!("{NUMA_ENV_VAR}={n}: operator-pinned test topology"),
                            };
                        }
                    }
                    return Self {
                        nodes: 1,
                        cpus: current_cpus(),
                        detected: true,
                        note: format!("{NUMA_ENV_VAR}='{other}' unparsable: single-node fail-safe"),
                    };
                }
            }
        }
        let hw = plomid_hw::HardwareProfile::detect();
        match hw.numa_nodes {
            Some(n) if n >= 2 => Self {
                nodes: n,
                cpus: hw.logical_cpus.max(1),
                detected: true,
                note: format!("detected {n} NUMA nodes, {} logical CPUs", hw.logical_cpus),
            },
            _ => Self {
                nodes: 1,
                cpus: hw.logical_cpus.max(1),
                detected: true,
                note: "single NUMA node (UMA laptop, single-socket, or unknown topology)"
                    .to_string(),
            },
        }
    }

    /// Deterministic test seam: fixed topology, no syscalls.
    ///
    /// `nodes == 0` is rejected (fail closed); `nodes == 1` is the UMA case.
    pub fn test_seam(nodes: usize, cpus: usize) -> Result<Self> {
        if nodes == 0 || nodes > 1024 {
            return Err(VectorError::IndexParameterInvalid(format!(
                "numa nodes {nodes} out of range 1..=1024"
            )));
        }
        Ok(Self {
            nodes,
            cpus: cpus.max(1),
            detected: false,
            note: format!("test seam: {nodes} nodes, {} cpus", cpus.max(1)),
        })
    }

    /// Execution mode for this topology.
    #[must_use]
    pub const fn mode(&self) -> NumaMode {
        if self.nodes >= 2 {
            NumaMode::Striped
        } else {
            NumaMode::Single
        }
    }

    /// True when multi-node striping applies.
    #[must_use]
    pub const fn is_multi_node(&self) -> bool {
        self.nodes >= 2
    }

    /// Diagnostic lines for existing diagnostics export.
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        vec![
            format!("NUMA mode: {}", self.mode().as_str()),
            format!("NUMA nodes: {}", self.nodes),
            format!("NUMA cpus: {}", self.cpus),
            format!("NUMA detected: {}", self.detected),
            format!("Note: {}", self.note),
        ]
    }
}

fn current_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

/// Worker-locality policy derived from topology + budgets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumaPolicy {
    /// Execution mode.
    pub mode: NumaMode,
    /// Worker threads for batch dispatch.
    pub workers: usize,
    /// Queries per worker chunk (ceil division output, ≥ 1).
    pub chunk_queries: usize,
    /// CPU/memory pinning requested (always false: extension point).
    pub pinning: bool,
}

impl NumaPolicy {
    /// Derive a policy for `total_queries` under `max_threads`.
    ///
    /// `max_threads == 0` means "runtime default" (topology CPU count).
    /// Workers are `min(queries, max_threads||cpus, 256)`; striping only
    /// labels chunk placement (contiguous query chunks per node) — results
    /// are concatenated in order, so output is identical in every mode.
    #[must_use]
    pub fn select(topology: &NumaTopology, total_queries: usize, max_threads: usize) -> Self {
        let want = if max_threads == 0 {
            topology.cpus
        } else {
            max_threads
        }
        .max(1);
        // Cap total queries into [1, min(want, cpus, 256)] with explicit
        // bounds (no max/min chains): identical to min-then-floor, and
        // `cap >= 1` always holds so the clamp below cannot panic.
        let cpus = topology.cpus.max(1);
        let mut cap = want;
        if cap > cpus {
            cap = cpus;
        }
        cap = cap.clamp(1, 256);
        let workers = total_queries.clamp(1, cap);
        let chunk_queries = total_queries.div_ceil(workers).max(1);
        Self {
            mode: topology.mode(),
            workers,
            chunk_queries,
            pinning: false,
        }
    }

    /// Contiguous query-index chunks for `total_queries` (one per worker).
    #[must_use]
    pub fn plan_chunks(&self, total_queries: usize) -> Vec<(usize, usize)> {
        if total_queries == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut start = 0usize;
        while start < total_queries {
            let end = (start + self.chunk_queries).min(total_queries);
            out.push((start, end));
            start = end;
        }
        out
    }

    /// Record this dispatch in the shared metrics (existing diagnostics).
    pub fn observe(&self, metrics: &crate::metrics::VectorMetrics) {
        metrics.observe_numa_dispatch(self.mode.is_multi_node());
    }

    /// Diagnostic lines.
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        vec![
            format!("NUMA dispatch: {}", self.mode.as_str()),
            format!("Workers: {}", self.workers),
            format!("Chunk queries: {}", self.chunk_queries),
            format!("Pinning: {}", self.pinning),
        ]
    }
}
