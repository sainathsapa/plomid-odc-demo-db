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
// Moved from `src/adaptive.rs` inline `#[cfg(test)]` — public-API only.
use plomid_optimizer::adaptive::*;
use plomid_optimizer::cost::CostInputs;

fn costs_for(shape: &QueryShape) -> EngineCosts {
    EngineCosts::estimate(shape, &CostInputs::default())
}

#[test]
fn point_lookup_selects_row_store() {
    // WHERE id = 42 over a million-row indexed table.
    let shape = QueryShape::point_lookup(1_000_000, 12);
    let decision = choose_engine(&shape, &costs_for(&shape));
    assert_eq!(decision.choice, EngineChoice::Row, "{}", decision.reason);
}

#[test]
fn group_by_aggregation_selects_columnar() {
    // GROUP BY country SUM(revenue): full scan, 2 of 10 columns.
    let shape = QueryShape::columnar_aggregation(1_000_000, 2, 10);
    let decision = choose_engine(&shape, &costs_for(&shape));
    assert_eq!(decision.choice, EngineChoice::Column, "{}", decision.reason);
}

#[test]
fn selective_filter_plus_aggregation_selects_hybrid() {
    let shape = QueryShape {
        rows_touched: 1_000_000,
        cols_touched: 2,
        total_cols: 10,
        selectivity: 0.10,
        has_aggregation: true,
        has_join: false,
        join_cardinality: 0,
        randomness: 0.5,
        cache_hit_rate: 0.0,
        cpu_available: 4,
        memory_rows: 1_000_000,
        index_available: true,
    };
    let decision = choose_engine(&shape, &costs_for(&shape));
    assert_eq!(decision.choice, EngineChoice::Hybrid, "{}", decision.reason);
}

#[test]
fn wide_select_all_selects_row_store() {
    let shape = QueryShape {
        rows_touched: 500_000,
        cols_touched: 8,
        total_cols: 8,
        selectivity: 0.9,
        has_aggregation: false,
        has_join: false,
        join_cardinality: 0,
        randomness: 0.1,
        cache_hit_rate: 0.0,
        cpu_available: 2,
        memory_rows: 1_000_000,
        index_available: false,
    };
    let decision = choose_engine(&shape, &costs_for(&shape));
    assert_eq!(decision.choice, EngineChoice::Row, "{}", decision.reason);
}

#[test]
fn ambiguous_shape_falls_back_to_costs() {
    let shape = QueryShape {
        rows_touched: 1_000_000,
        cols_touched: 9,
        total_cols: 10,
        selectivity: 0.6,
        has_aggregation: false,
        has_join: false,
        join_cardinality: 0,
        randomness: 0.2,
        cache_hit_rate: 0.0,
        cpu_available: 1,
        memory_rows: 1_000_000,
        index_available: false,
    };
    let costs = costs_for(&shape);
    let decision = choose_engine(&shape, &costs);
    let expected = if costs.column.total < costs.row.total {
        EngineChoice::Column
    } else {
        EngineChoice::Row
    };
    assert_eq!(decision.choice, expected, "{}", decision.reason);
    assert!(decision.reason.contains("cost fallback"));
}

#[test]
fn auto_entry_point_agrees_with_manual_costs() {
    let shape = QueryShape::point_lookup(1_000_000, 12);
    let auto = choose_engine_auto(&shape, &CostInputs::default());
    assert_eq!(auto.choice, EngineChoice::Row);
}

#[test]
fn selectivity_hints_drive_shape_selectivity() {
    use plomid_optimizer::selectivity::{ColumnSelectivityHints, PredicateKind};

    // 1 / 50 distinct values.
    let hints = ColumnSelectivityHints {
        distinct_estimate: 50,
        ..ColumnSelectivityHints::unknown()
    };
    let shape = QueryShape::with_selectivity_hints(
        1_000,
        2,
        10,
        &[(PredicateKind::Equals, Some(hints))],
        true,
        false,
        false,
        0,
        0.0,
        0.0,
        1,
        1_000_000,
        false,
    );
    assert!(
        (shape.selectivity - 0.02).abs() < 1e-12,
        "got {}",
        shape.selectivity
    );

    // Defaults: equals 0.1, range 0.3 — AND multiplies, OR follows
    // independence, empty means "no filter".
    let and_shape = QueryShape::with_selectivity_hints(
        1_000,
        2,
        10,
        &[(PredicateKind::Equals, None), (PredicateKind::Range, None)],
        true,
        false,
        false,
        0,
        0.0,
        0.0,
        1,
        1_000_000,
        false,
    );
    assert!(
        (and_shape.selectivity - 0.03).abs() < 1e-12,
        "got {}",
        and_shape.selectivity
    );
    let or_shape = QueryShape::with_selectivity_hints(
        1_000,
        2,
        10,
        &[(PredicateKind::Equals, None), (PredicateKind::Range, None)],
        false,
        false,
        false,
        0,
        0.0,
        0.0,
        1,
        1_000_000,
        false,
    );
    assert!(
        (or_shape.selectivity - 0.37).abs() < 1e-12,
        "got {}",
        or_shape.selectivity
    );
    let unfiltered = QueryShape::with_selectivity_hints(
        1_000,
        2,
        10,
        &[],
        true,
        true,
        false,
        0,
        0.0,
        0.0,
        4,
        1_000_000,
        false,
    );
    assert_eq!(unfiltered.selectivity, 1.0);
}

#[test]
fn estimate_routes_parallel_shapes_through_parallel_scan() {
    let base = QueryShape::columnar_aggregation(1_000_000, 2, 10);
    let mut serial_shape = base.clone();
    serial_shape.cpu_available = 1;
    let mut parallel_shape = base;
    parallel_shape.cpu_available = 8;
    let serial = EngineCosts::estimate(&serial_shape, &CostInputs::default());
    let parallel = EngineCosts::estimate(&parallel_shape, &CostInputs::default());
    assert!(
        parallel.column.cheaper_than(&serial.column),
        "parallel {:?} should beat serial {:?}",
        parallel.column,
        serial.column,
    );
}
