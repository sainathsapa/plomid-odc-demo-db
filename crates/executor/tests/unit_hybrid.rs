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
// Moved from src/hybrid.rs inline #[cfg(test)].
use plomid_executor::hybrid::*;

#[test]
fn full_plan_stage_order() {
    let plan = plan_hybrid(true, vec!["a".to_string(), "b".to_string()], true, true);
    assert_eq!(plan.len(), 5);
    assert!(matches!(plan.stages[0], HybridStage::IndexLookup { .. }));
    assert!(matches!(plan.stages[1], HybridStage::RowIds { .. }));
    assert!(matches!(plan.stages[2], HybridStage::ColumnFetch { .. }));
    assert!(matches!(plan.stages[3], HybridStage::VectorFilter { .. }));
    assert!(matches!(plan.stages[4], HybridStage::Aggregation { .. }));
}

#[test]
fn skipped_stages_are_omitted() {
    let plan = plan_hybrid(false, Vec::new(), false, false);
    assert!(plan.is_empty());
    let plan = plan_hybrid(false, vec!["a".to_string()], false, false);
    assert_eq!(plan.len(), 1);
    assert!(matches!(plan.stages[0], HybridStage::ColumnFetch { .. }));
}

#[test]
fn explain_contains_stages() {
    let plan = plan_hybrid(true, vec!["a".to_string()], true, true);
    let text = explain(&plan);
    for token in [
        "INDEX LOOKUP",
        "ROW IDS",
        "COLUMN FETCH",
        "VECTOR FILTER",
        "AGGREGATION",
    ] {
        assert!(text.contains(token), "explain missing {token}: {text}");
    }
}

#[test]
fn count_estimate_reads_row_ids() {
    let mut plan = plan_hybrid(true, vec!["a".to_string()], false, false);
    if let Some(HybridStage::RowIds { count }) = plan
        .stages
        .iter_mut()
        .find(|s| matches!(s, HybridStage::RowIds { .. }))
    {
        *count = 100;
    }
    assert_eq!(execute_count_estimate(&plan), 100);
    let filtered = plan_hybrid(true, vec!["a".to_string()], true, false);
    let mut filtered = filtered;
    if let Some(HybridStage::RowIds { count }) = filtered
        .stages
        .iter_mut()
        .find(|s| matches!(s, HybridStage::RowIds { .. }))
    {
        *count = 100;
    }
    assert_eq!(execute_count_estimate(&filtered), 90);
}
