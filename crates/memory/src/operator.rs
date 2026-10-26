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

//! [`OperatorKind`]: every operator maps to exactly one accounting kind.
//!
//! Accounting-only enum: variants carry no buffers or row types.
//! * `VectorSearch` is a no-op accounting bucket (no vector types live here).
//! * `BlobBuffer` accounts bytea/text chunks as plain byte counts — no new
//!   BLOB types are introduced.

use std::fmt;

/// Operator kinds for per-operator memory accounting.
///
/// The set is intentionally closed and exhaustive: any future executor
/// operator maps onto one of these buckets (unknown ones onto
/// [`OperatorKind::Other`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    /// Sequential heap / columnar scan.
    TableScan,
    /// Index-assisted scan.
    IndexScan,
    /// Sort (quicksort / external merge sort workspace).
    Sort,
    /// Hash join table.
    HashJoin,
    /// Merge join run buffers.
    MergeJoin,
    /// Nested-loop join state.
    NestedLoopJoin,
    /// Hash aggregation table.
    HashAggregate,
    /// Sorted aggregation state.
    SortAggregate,
    /// DISTINCT / deduplication set.
    Distinct,
    /// Window function frame buffers.
    Window,
    /// Materialized node output.
    Materialization,
    /// CTE evaluation buffer.
    Cte,
    /// Subquery evaluation workspace.
    Subquery,
    /// Set-operation (UNION / INTERSECT / EXCEPT) workspace.
    SetOp,
    /// Temp-table backing store (bounded by `MemoryConfig::temp_limit`).
    TempTable,
    /// Vector-search accounting bucket (no-op: byte counts only).
    VectorSearch,
    /// Client-facing result buffer (bounded by
    /// `MemoryConfig::result_buffer_budget`).
    ResultBuffer,
    /// bytea/text chunk accounting (plain byte counts, no BLOB types).
    BlobBuffer,
    /// JSON aggregation workspace.
    JsonAgg,
    /// Fallback bucket for future operators without a dedicated kind.
    Other,
}

impl OperatorKind {
    /// Stable, human-readable name of the kind.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TableScan => "TableScan",
            Self::IndexScan => "IndexScan",
            Self::Sort => "Sort",
            Self::HashJoin => "HashJoin",
            Self::MergeJoin => "MergeJoin",
            Self::NestedLoopJoin => "NestedLoopJoin",
            Self::HashAggregate => "HashAggregate",
            Self::SortAggregate => "SortAggregate",
            Self::Distinct => "Distinct",
            Self::Window => "Window",
            Self::Materialization => "Materialization",
            Self::Cte => "Cte",
            Self::Subquery => "Subquery",
            Self::SetOp => "SetOp",
            Self::TempTable => "TempTable",
            Self::VectorSearch => "VectorSearch",
            Self::ResultBuffer => "ResultBuffer",
            Self::BlobBuffer => "BlobBuffer",
            Self::JsonAgg => "JsonAgg",
            Self::Other => "Other",
        }
    }
}

impl fmt::Display for OperatorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every accounting bucket, for exhaustive coverage tests.
pub const ALL_KINDS: [OperatorKind; 20] = [
    OperatorKind::TableScan,
    OperatorKind::IndexScan,
    OperatorKind::Sort,
    OperatorKind::HashJoin,
    OperatorKind::MergeJoin,
    OperatorKind::NestedLoopJoin,
    OperatorKind::HashAggregate,
    OperatorKind::SortAggregate,
    OperatorKind::Distinct,
    OperatorKind::Window,
    OperatorKind::Materialization,
    OperatorKind::Cte,
    OperatorKind::Subquery,
    OperatorKind::SetOp,
    OperatorKind::TempTable,
    OperatorKind::VectorSearch,
    OperatorKind::ResultBuffer,
    OperatorKind::BlobBuffer,
    OperatorKind::JsonAgg,
    OperatorKind::Other,
];
