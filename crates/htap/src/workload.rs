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

//! Workload classes and safe default classification.

/// What kind of work a query / transaction is.
///
/// The engine never *requires* callers to pick one: [`classify`] infers a
/// safe default when no hint is given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    /// Short point lookups / writes. Latency sensitive, always preferred.
    Oltp,
    /// Large scans, joins, aggregations. Throughput oriented.
    Olap,
    /// Compaction, vacuum, reindex, stats rebuilds. Never starves OLTP.
    Maintenance,
    /// Internal control-plane work (checkpoint coordination, heartbeats).
    System,
    /// Garbage collection, telemetry, prefetch. Lowest priority.
    Background,
    /// Small interactive analytics (dashboards). Latency sensitive but
    /// memory-hungry; accounted against the OLAP pool with priority.
    InteractiveAnalytics,
    /// Scheduled bulk jobs / ETL. Throughput oriented.
    Batch,
}

/// Scheduling priority tier derived from a [`WorkloadClass`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Priority {
    /// Serve first, never derate parallelism, bypass OLAP queues.
    LatencySensitive,
    /// Fair-share scheduling, derate parallelism under pressure.
    Throughput,
    /// Shed first, pause at critical pressure.
    Background,
}

impl WorkloadClass {
    /// Priority tier for this class.
    pub fn priority(self) -> Priority {
        match self {
            WorkloadClass::Oltp | WorkloadClass::System | WorkloadClass::InteractiveAnalytics => {
                Priority::LatencySensitive
            }
            WorkloadClass::Olap | WorkloadClass::Batch => Priority::Throughput,
            WorkloadClass::Maintenance | WorkloadClass::Background => Priority::Background,
        }
    }

    /// True for latency-sensitive classes (served first, never derated).
    pub fn is_latency_sensitive(self) -> bool {
        self.priority() == Priority::LatencySensitive
    }
}

/// Row-count threshold at or below which an unhinted read is treated as an
/// OLTP point lookup rather than an OLAP scan.
pub const SMALL_TXN_ROW_THRESHOLD: u64 = 1024;

/// Classify work with a safe inferred default.
///
/// Rules, in order:
/// 1. Explicit [`WorkloadClass::System`] / [`WorkloadClass::Maintenance`]
///    hints are honored (operational intent, e.g. vacuum writes).
/// 2. Write transactions are [`WorkloadClass::Oltp`] — writes take the
///    low-latency path regardless of hint.
/// 3. Reads estimated at or below [`SMALL_TXN_ROW_THRESHOLD`] rows are
///    [`WorkloadClass::Oltp`] (point lookups).
/// 4. Otherwise the caller hint wins, defaulting to [`WorkloadClass::Olap`].
///
/// Manual classification is never required: `classify(None, _, _)` always
/// yields a usable answer.
pub fn classify(
    hint: Option<WorkloadClass>,
    is_write_txn: bool,
    estimated_rows: Option<u64>,
) -> WorkloadClass {
    if let Some(h) = hint {
        if matches!(h, WorkloadClass::System | WorkloadClass::Maintenance) {
            return h;
        }
    }
    if is_write_txn {
        return WorkloadClass::Oltp;
    }
    if let Some(rows) = estimated_rows {
        if rows <= SMALL_TXN_ROW_THRESHOLD {
            return WorkloadClass::Oltp;
        }
    }
    hint.unwrap_or(WorkloadClass::Olap)
}
