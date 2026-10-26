#![forbid(unsafe_code)]
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
//! Logical and physical query planning foundations for PLOMID.
//!
//! Adaptive row/column optimizer and cost model:
//!
//! - [`selectivity`]: per-predicate survival fractions from lightweight hints.
//! - [`cost`]: decomposed CPU/IO/memory plan costs with documented weights.
//! - [`adaptive`]: shape-aware [`adaptive::EngineChoice`] selection — structural
//!   rules first, cost comparison as the fallback.
//!
//! The canonical hybrid pipeline (`HybridPlan`) lives in the executor crate
//! (`plomid-executor`'s `hybrid` module), which owns the execution-side stage
//! model; this crate only reasons about hybrid *costs* via
//! [`adaptive::EngineCosts`]. An earlier optimizer-side duplicate of that
//! pipeline was deleted to keep a single source of truth.
//!
//! The optimizer consumes statistics owned elsewhere (executor row statistics,
//! columnar segment metadata) through plain-numeric hint structs; it never
//! collects, persists, or reimplements them.

pub mod adaptive;
pub mod cost;
pub mod selectivity;

pub use adaptive::{
    choose_engine, choose_engine_auto, EngineChoice, EngineCosts, EngineDecision, QueryShape,
};
pub use cost::{
    choose_join, cost_column_scan, cost_column_scan_parallel, cost_filtered_scan,
    cost_hash_aggregate, cost_hash_join, cost_nested_loop, cost_radius_scan, cost_vector_scan,
    CostInputs, JoinStrategy, PlanCost, VectorMode,
};
pub use selectivity::{
    combine_selectivity_and, combine_selectivity_or, estimate_selectivity, ColumnSelectivityHints,
    PredicateKind,
};
