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
//! Per-query vector search options with validated ranges.
//!
//! Precedence (resolved by the caller): query option → transaction-local
//! setting → session setting → server default ([`VectorSearchOptions::default`]).

use crate::error::{Result, VectorError};

/// Exact vs approximate execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchMode {
    /// Brute-force exact scan.
    Exact,
    /// ANN index path (fails closed when no usable index exists).
    Ann,
    /// Planner chooses per query (default).
    #[default]
    Auto,
}

/// How metadata predicates combine with vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FilterMode {
    /// Planner chooses.
    #[default]
    Auto,
    /// Apply predicates before vector scoring.
    Pre,
    /// Score candidates first, then verify predicates + rerank.
    Post,
}

/// Compute backend preference (GPU is a compile-time extension point).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ComputeBackend {
    /// Scalar/SIMD CPU kernels.
    Cpu,
    /// GPU kernels (requires `gpu` feature downstream).
    Gpu,
    /// Planner/backend chooses (default).
    #[default]
    Auto,
}

/// Typed search controls shared by HNSW/IVF/PQ/SQ/sparse/binary paths.
/// Irrelevant options are ignored by incompatible indexes (never error).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchOptions {
    /// Execution mode.
    pub mode: SearchMode,
    /// HNSW/`DiskANN` candidate list size.
    pub ef_search: u32,
    /// IVF probe count.
    pub nprobe: u32,
    /// Oversampled candidate count before filtering/rerank (`None` = auto).
    pub candidate_k: Option<u32>,
    /// Exact-rerank depth (`None` = no rerank / index default).
    pub rerank_k: Option<u32>,
    /// Predicate application strategy.
    pub filter_mode: FilterMode,
    /// Compute backend preference.
    pub compute: ComputeBackend,
}

impl Default for VectorSearchOptions {
    fn default() -> Self {
        Self {
            mode: SearchMode::Auto,
            ef_search: 100,
            nprobe: 8,
            candidate_k: None,
            rerank_k: None,
            filter_mode: FilterMode::Auto,
            compute: ComputeBackend::Auto,
        }
    }
}

impl VectorSearchOptions {
    /// Validate ranges and cross-field consistency.
    pub fn validate(&self) -> Result<()> {
        if !(1..=10_000).contains(&self.ef_search) {
            return Err(VectorError::IndexParameterInvalid(format!(
                "ef_search {} out of range 1..=10000",
                self.ef_search
            )));
        }
        if !(1..=4096).contains(&self.nprobe) {
            return Err(VectorError::IndexParameterInvalid(format!(
                "nprobe {} out of range 1..=4096",
                self.nprobe
            )));
        }
        if let Some(k) = self.candidate_k {
            if !(1..=1_000_000).contains(&k) {
                return Err(VectorError::IndexParameterInvalid(format!(
                    "candidate_k {k} out of range 1..=1000000"
                )));
            }
        }
        if let Some(k) = self.rerank_k {
            if !(1..=1_000_000).contains(&k) {
                return Err(VectorError::IndexParameterInvalid(format!(
                    "rerank_k {k} out of range 1..=1000000"
                )));
            }
            if let Some(c) = self.candidate_k {
                if k > c {
                    return Err(VectorError::IndexParameterInvalid(format!(
                        "rerank_k {k} exceeds candidate_k {c}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Effective candidate depth for `limit`: explicit value or `limit * 8`
    /// (saturating, capped), so filtered ANN does not under-return.
    #[must_use]
    pub fn effective_candidate_k(&self, limit: u32) -> u32 {
        if let Some(k) = self.candidate_k {
            return k.max(limit);
        }
        limit.saturating_mul(8).max(limit).clamp(1, 1_000_000)
    }
}

/// Estimated resident bytes for an in-memory build over `rows` vectors at
/// `bytes_per_vector` payload bytes (graph adjacency excluded — callers add
/// their own factor). Saturating: hostile inputs clamp to `u64::MAX`
/// instead of overflowing. Resource governance (Phase 44 in code) compares
/// this against `vector.max_memory_bytes` before building.
#[must_use]
pub fn memory_estimate_bytes(rows: u64, bytes_per_vector: u64) -> u64 {
    rows.saturating_mul(bytes_per_vector.max(1))
}

/// Resolve a compute preference to an executable backend.
///
/// `Cpu` and `Auto` always resolve to `Cpu` (the portable kernels). `Gpu`
/// resolves to `Gpu` only as a *request*: execution still goes through
/// [`crate::backend::execute_with_cpu_fallback`], so an unlinked GPU kernel
/// degrades to CPU with `IndexUnsupported` instead of failing the query.
/// There is deliberately no silent third state.
#[must_use]
pub fn resolve_compute(preference: ComputeBackend) -> ComputeBackend {
    match preference {
        ComputeBackend::Cpu | ComputeBackend::Auto => ComputeBackend::Cpu,
        ComputeBackend::Gpu => ComputeBackend::Gpu,
    }
}
