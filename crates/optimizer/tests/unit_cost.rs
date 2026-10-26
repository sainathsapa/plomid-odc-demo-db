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
// Moved from `src/cost.rs` inline `#[cfg(test)]` — public-API only.
use plomid_optimizer::cost::*;

const ROWS: u64 = 1_000_000;

#[test]
fn index_scan_beats_seq_scan_at_two_percent() {
    let inputs = CostInputs::default();
    let index = cost_row_index_scan(0.02, ROWS, &inputs);
    let seq = cost_row_seq_scan(ROWS, &inputs);
    assert!(
        index.cheaper_than(&seq),
        "index {index:?} should beat seq {seq:?} at 2%"
    );
}

#[test]
fn seq_scan_beats_index_scan_at_seventy_percent() {
    let inputs = CostInputs::default();
    let index = cost_row_index_scan(0.70, ROWS, &inputs);
    let seq = cost_row_seq_scan(ROWS, &inputs);
    assert!(
        seq.cheaper_than(&index),
        "seq {seq:?} should beat index {index:?} at 70%"
    );
}

#[test]
fn narrow_column_scan_beats_wide_row_scan() {
    let inputs = CostInputs::default();
    let column = cost_column_scan(ROWS, 2, 20, 1.0, 1, &inputs);
    let row = cost_row_seq_scan(ROWS, &inputs);
    assert!(
        column.cheaper_than(&row),
        "column {column:?} should beat row {row:?} for 2/20 cols"
    );
}

#[test]
fn parallel_column_scan_beats_sequential() {
    let inputs = CostInputs::default();
    let sequential = cost_column_scan(ROWS, 2, 20, 1.0, 1, &inputs);
    let parallel = cost_column_scan_parallel(ROWS, 2, 20, 1.0, 4, &inputs);
    assert!(
        parallel.cheaper_than(&sequential),
        "parallel {parallel:?} should beat sequential {sequential:?}"
    );
}

#[test]
fn vector_ann_beats_exact_at_scale_and_loses_small() {
    let inputs = CostInputs::default();
    let exact_big = cost_vector_scan(1_000_000, 1536, 0, VectorMode::Exact, &inputs);
    let ann_big = cost_vector_scan(1_000_000, 1536, 80, VectorMode::Ann, &inputs);
    assert!(
        ann_big.cheaper_than(&exact_big),
        "ann {ann_big:?} should beat exact {exact_big:?} at 1M x 1536"
    );
    let exact_small = cost_vector_scan(100, 128, 0, VectorMode::Exact, &inputs);
    let ann_small = cost_vector_scan(100, 128, 80, VectorMode::Ann, &inputs);
    assert!(
        exact_small.cheaper_than(&ann_small),
        "exact {exact_small:?} should beat ann {ann_small:?} at 100 x 128"
    );
}

#[test]
fn vector_scan_degenerate_inputs_cost_zero() {
    let inputs = CostInputs::default();
    assert_eq!(
        cost_vector_scan(0, 128, 0, VectorMode::Exact, &inputs),
        PlanCost::zero()
    );
    assert_eq!(
        cost_vector_scan(100, 0, 0, VectorMode::Ann, &inputs),
        PlanCost::zero()
    );
}

#[test]
fn filtered_scan_charges_filter_and_picks_survivor_path() {
    let inputs = CostInputs::default();
    // Impossible filter: only the filter pass itself is priced.
    let empty = cost_filtered_scan(1_000_000, 128, 0.0, 10, &inputs);
    let full = cost_filtered_scan(1_000_000, 128, 1.0, 10, &inputs);
    assert!(
        empty.total < full.total,
        "empty {empty:?} should cost less than full {full:?}"
    );
    // Unknown selectivity degrades to the unfiltered cost, never less.
    let unknown = cost_filtered_scan(1_000_000, 128, f64::NAN, 10, &inputs);
    assert!(
        unknown.total >= full.total,
        "unknown {unknown:?} should cost at least full {full:?}"
    );
    assert_eq!(
        cost_filtered_scan(0, 128, 0.5, 10, &inputs),
        PlanCost::zero()
    );
}

#[test]
fn radius_scan_costs_scan_plus_well() {
    let inputs = CostInputs::default();
    let exact = cost_radius_scan(100_000, 128, 0, VectorMode::Exact, &inputs);
    let ann = cost_radius_scan(100_000, 128, 1024, VectorMode::Ann, &inputs);
    // Same working set priced both ways: well-bounded ANN verifies less.
    assert!(
        ann.total < exact.total,
        "ann {ann:?} should cost less than exact {exact:?} for a bounded well"
    );
    assert_eq!(
        cost_radius_scan(0, 128, 0, VectorMode::Exact, &inputs),
        PlanCost::zero()
    );
}

#[test]
fn hash_join_beats_nested_loop_at_scale() {
    let inputs = CostInputs::default();
    let hash = cost_hash_join(100_000, 100_000, 0.1, &inputs);
    let nested = cost_nested_loop(100_000, 100_000, &inputs);
    assert!(
        hash.cheaper_than(&nested),
        "hash {hash:?} should beat nested loop {nested:?}"
    );
}

#[test]
fn cheaper_than_compares_total_only() {
    let cheap = PlanCost {
        cpu: 1.0,
        io: 1.0,
        mem: 0.0,
        total: 9.0,
    };
    let pricey = PlanCost {
        cpu: 100.0,
        io: 0.0,
        mem: 0.0,
        total: 100.0,
    };
    assert!(cheap.cheaper_than(&pricey));
    assert!(!pricey.cheaper_than(&cheap));
    // Ties are not strictly cheaper (row store wins cost-fallback ties).
    assert!(!cheap.cheaper_than(&cheap));
    assert!(!cheap.cheaper_than(&PlanCost {
        total: 9.0,
        ..cheap
    }));
}

#[test]
fn join_strategy_prefers_hash_at_scale() {
    let inputs = CostInputs::default();
    assert_eq!(
        choose_join(100_000, 100_000, 0.1, &inputs),
        JoinStrategy::Hash
    );
}

#[test]
fn join_strategy_falls_back_to_nested_loop_when_hash_spills() {
    // Large build side spills the hash table past a tiny memory budget
    // while a 2-row probe keeps the nested-loop pairs small.
    let tight = CostInputs {
        mem_limit_rows: 100,
        ..CostInputs::default()
    };
    assert_eq!(
        choose_join(100_000, 2, 0.1, &tight),
        JoinStrategy::NestedLoop
    );
}

#[test]
fn spilling_aggregation_is_penalized() {
    let tight = CostInputs {
        mem_limit_rows: 100,
        ..CostInputs::default()
    };
    let spilling = cost_hash_aggregate(100_000, ROWS, &tight);
    let roomy = cost_hash_aggregate(100_000, ROWS, &CostInputs::default());
    assert!(spilling.total > roomy.total);
}

#[test]
fn empty_inputs_cost_zero() {
    let inputs = CostInputs::default();
    assert_eq!(cost_row_seq_scan(0, &inputs), PlanCost::zero());
    assert_eq!(cost_row_index_scan(0.5, 0, &inputs), PlanCost::zero());
}
