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

//! Diagnostics snapshot: why is OLTP slow, who uses memory.

use crate::config::PressureState;
use crate::governor::ResourceGovernor;
use crate::workload::WorkloadClass;

/// One row of the per-query table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStat {
    pub query_id: u64,
    pub class: WorkloadClass,
    pub weight: u32,
    pub admitted: bool,
}

/// Point-in-time HTAP diagnostics.
///
/// Answers two questions:
/// - *Why is OLTP slow?* — see `pressure`, `utilization_pct`, and
///   `oltp_headroom_bytes()`.
/// - *Who consumes memory?* — per-pool `*_used_bytes`, `cache_bytes`,
///   `result_buffer_bytes`, `spill_total_bytes`.
///
/// Queue fields (`waiting`, `waiting_count`, `per_query`) are always empty:
/// admission is owned solely by the governor's concurrency slots, and no
/// live queue exists to report. `admitted_olap` mirrors the governor's live
/// OLAP slot count.
#[derive(Debug, Clone)]
pub struct HtapSnapshot {
    pub global_budget_bytes: u64,
    pub global_used_bytes: u64,
    pub global_peak_bytes: u64,
    pub utilization_pct: u8,
    pub pressure: PressureState,
    pub oltp_used_bytes: u64,
    pub oltp_reserved_bytes: u64,
    pub oltp_in_use_slots: usize,
    pub olap_used_bytes: u64,
    pub olap_in_use_slots: usize,
    pub maintenance_used_bytes: u64,
    pub system_used_bytes: u64,
    pub cache_bytes: u64,
    pub result_buffer_bytes: u64,
    pub spill_total_bytes: u64,
    pub admitted_olap: usize,
    pub waiting_count: usize,
    pub waiting: Vec<u64>,
    pub per_query: Vec<QueryStat>,
}

impl HtapSnapshot {
    /// Collect pool/global counters from `governor` into one snapshot.
    ///
    /// `admitted_olap` is the governor's live OLAP slot count; queue fields
    /// stay empty (see the struct docs).
    pub fn collect(governor: &ResourceGovernor) -> Self {
        let mut snap = governor.snapshot();
        snap.admitted_olap = governor.olap_in_use_slots();
        snap
    }

    /// Bytes available before the global budget is exhausted.
    pub fn oltp_headroom_bytes(&self) -> u64 {
        self.global_budget_bytes
            .saturating_sub(self.global_used_bytes)
    }

    /// Largest consumer among pools + cache + result buffers.
    pub fn top_consumer(&self) -> (&'static str, u64) {
        let mut top = ("oltp", self.oltp_used_bytes);
        for (name, bytes) in [
            ("olap", self.olap_used_bytes),
            ("maintenance", self.maintenance_used_bytes),
            ("system", self.system_used_bytes),
            ("cache", self.cache_bytes),
            ("result-buffer", self.result_buffer_bytes),
        ] {
            if bytes > top.1 {
                top = (name, bytes);
            }
        }
        top
    }

    /// Human-readable summary: pressure, top consumer, waiters, OLTP
    /// headroom, spill/cache totals.
    pub fn describe(&self) -> String {
        let (top_name, top_bytes) = self.top_consumer();
        let mut waiting = self.waiting.clone();
        waiting.sort_unstable();
        let waiting_preview: Vec<String> = waiting.iter().take(8).map(u64::to_string).collect();
        format!(
            "htap pressure={:?} util={}% global={}/{}B peak={}B\n\
             pools: oltp={}B (reserve={}B, slots={}) olap={}B (slots={}) maint={}B system={}B\n\
             top_consumer={top_name}({top_bytes}B) cache={}B result_buffers={}B spilled_total={}B\n\
             admitted_olap={} waiting={} [{}]{}oltp_headroom={}B",
            self.pressure,
            self.utilization_pct,
            self.global_used_bytes,
            self.global_budget_bytes,
            self.global_peak_bytes,
            self.oltp_used_bytes,
            self.oltp_reserved_bytes,
            self.oltp_in_use_slots,
            self.olap_used_bytes,
            self.olap_in_use_slots,
            self.maintenance_used_bytes,
            self.system_used_bytes,
            self.cache_bytes,
            self.result_buffer_bytes,
            self.spill_total_bytes,
            self.admitted_olap,
            self.waiting_count,
            waiting_preview.join(","),
            if self.waiting_count > 8 { "..." } else { "" },
            self.oltp_headroom_bytes(),
        )
    }
}
