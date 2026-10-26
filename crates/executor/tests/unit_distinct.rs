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
// Moved from src/distinct.rs inline #[cfg(test)].
use plomid_executor::distinct::*;
use plomid_sql::ColumnType;
use plomid_sql::{Expression, QueryResult, Value};

fn rows_result(rows: Vec<Vec<Value>>) -> QueryResult {
    QueryResult::Rows {
        columns: vec!["c".to_string()],
        column_types: vec![Some(ColumnType::bigint())],
        rows,
    }
}

fn distinct(rows: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let result = apply_distinct(rows_result(rows.to_vec()), true, &[], None, None);
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows");
    };
    rows
}

#[test]
fn distinct_keeps_first_seen_order() {
    let rows = vec![
        vec![Value::Int8(3)],
        vec![Value::Int8(1)],
        vec![Value::Int8(3)],
        vec![Value::Int8(2)],
        vec![Value::Int8(1)],
    ];
    assert_eq!(
        distinct(&rows),
        vec![
            vec![Value::Int8(3)],
            vec![Value::Int8(1)],
            vec![Value::Int8(2)]
        ]
    );
}

#[test]
fn distinct_merges_equal_normally_typed_values() {
    // `values_equal` compares numeric values through `f64`, so these three
    // are one group even though their SQL text differs.
    let rows = vec![
        vec![Value::Int8(1)],
        vec![Value::Float8(1.0)],
        vec![Value::Int4(1)],
        vec![Value::Float8(0.0)],
    ];
    assert_eq!(
        distinct(&rows),
        vec![vec![Value::Int8(1)], vec![Value::Float8(0.0)]]
    );
}

#[test]
fn distinct_merges_signed_zero_but_not_nan() {
    let rows = vec![
        vec![Value::Float8(0.0)],
        vec![Value::Float8(-0.0)],
        vec![Value::Float8(f64::NAN)],
        vec![Value::Float8(f64::NAN)],
    ];
    // `-0.0` equals `0.0`; a NaN equals nothing, including itself, so both
    // NaN rows survive.
    assert_eq!(distinct(&rows).len(), 3);
}

#[test]
fn distinct_deduplicates_nulls_and_multi_column_rows() {
    let rows = vec![
        vec![Value::Null, Value::Bool(true)],
        vec![Value::Null, Value::Bool(true)],
        vec![Value::Null, Value::Bool(false)],
        vec![Value::Text("x".into()), Value::Bool(true)],
    ];
    assert_eq!(distinct(&rows).len(), 3);
}

#[test]
fn distinct_defers_limit_and_offset_until_after_dedup() {
    let rows = vec![
        vec![Value::Int8(1)],
        vec![Value::Int8(1)],
        vec![Value::Int8(2)],
        vec![Value::Int8(3)],
    ];
    let result = apply_distinct(rows_result(rows), true, &[], Some(2), Some(1));
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows");
    };
    assert_eq!(rows, vec![vec![Value::Int8(2)], vec![Value::Int8(3)]]);
}

#[test]
fn distinct_on_rows_are_only_paged() {
    // DISTINCT ON rows arrive already de-duplicated by the engine.
    let rows = vec![vec![Value::Int8(1)], vec![Value::Int8(1)]];
    let result = apply_distinct(
        rows_result(rows),
        true,
        &[Expression::ColumnRef("c".into())],
        None,
        None,
    );
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows");
    };
    assert_eq!(rows.len(), 2);
}
