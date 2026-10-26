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
// Moved from src/projection.rs inline #[cfg(test)].
use plomid_executor::encoding::encode_row;
use plomid_executor::projection::*;
use plomid_sql::{Catalog, ColumnDef, ColumnType, InMemoryCatalog, Value};
use plomid_sql::{Expression, SelectTarget, TableSchema};
use std::collections::BTreeSet;

/// Builds `public.t(id, amount, flag)` through the catalog, so the schema
/// under test is exactly the one the executor sees at runtime.
fn schema() -> TableSchema {
    let mut catalog = InMemoryCatalog::new();
    let column = |name: &str| ColumnDef {
        name: name.to_string(),
        col_type: ColumnType::int4(),
        constraints: Vec::new(),
    };
    catalog
        .create_table(
            "public.t".to_string(),
            vec![column("id"), column("amount"), column("flag")],
            Vec::new(),
        )
        .expect("create table");
    catalog.get_table("public.t").expect("get table").clone()
}

fn function_call(name: &str, args: Vec<Expression>) -> SelectTarget {
    SelectTarget::Expr {
        expr: Expression::FunctionCall {
            name: name.into(),
            args,
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            returning: None,
            null_handling: None,
            unique_keys: None,
        },
        alias: None,
    }
}

fn count_star_target() -> SelectTarget {
    function_call("count", vec![Expression::Star])
}

fn mask_for(targets: &[SelectTarget], where_expr: Option<&Expression>) -> Option<ColumnMask> {
    let schema = schema();
    required_columns(&schema, targets, where_expr, None, None, &[], &[])
}

fn indexes_for(targets: &[SelectTarget]) -> Option<BTreeSet<usize>> {
    let schema = schema();
    requested_indexes(&schema, std::iter::empty(), targets)
}

#[test]
fn count_star_reads_no_column() {
    let targets = vec![count_star_target()];
    let mask = mask_for(&targets, None).expect("count(*) is projectable");
    assert!(mask.reads_no_column());
    assert_eq!(indexes_for(&targets), Some(BTreeSet::new()));
    assert!(is_bare_count_star(&targets));
}

#[test]
fn bare_function_call_count_star_is_bare() {
    // The parser produces `SELECT COUNT(*)` directly as a `FunctionCall`
    // target. Without this shape the most common production COUNT misses
    // the keys-only fast path and materializes every row.
    let direct = SelectTarget::FunctionCall {
        name: "count".into(),
        args: vec![Expression::Star],
    };
    assert!(is_bare_count_star(&[direct]));
    let other = SelectTarget::FunctionCall {
        name: "count".into(),
        args: vec![Expression::Literal(Value::Null)],
    };
    assert!(!is_bare_count_star(&[other]));
    let sum = SelectTarget::FunctionCall {
        name: "sum".into(),
        args: vec![Expression::Star],
    };
    assert!(!is_bare_count_star(&[sum]));
}

#[test]
fn count_star_with_filter_is_not_bare() {
    let mut target = count_star_target();
    if let SelectTarget::Expr {
        expr: Expression::FunctionCall { filter, .. },
        ..
    } = &mut target
    {
        *filter = Some(Box::new(Expression::IsNull(Box::new(
            Expression::ColumnRef("flag".into()),
        ))));
    }
    assert!(!is_bare_count_star(&[target]));
}

#[test]
fn count_of_a_column_is_not_bare() {
    // `count(NULL)` reads no column but must count zero, so the bare shape
    // is required before a row count may answer the statement.
    let targets = vec![function_call(
        "count",
        vec![Expression::Literal(Value::Null)],
    )];
    assert!(!is_bare_count_star(&targets));
    let mask = mask_for(&targets, None).expect("count(NULL) is projectable");
    assert!(mask.reads_no_column());
}

#[test]
fn sum_reads_only_its_argument() {
    let targets = vec![function_call(
        "sum",
        vec![Expression::ColumnRef("amount".into())],
    )];
    assert_eq!(indexes_for(&targets), Some(BTreeSet::from([1])));
    let mask = mask_for(&targets, None).expect("sum(amount) is projectable");
    assert_eq!(mask.selected(), [false, true, false]);
}

#[test]
fn predicate_columns_are_included() {
    let targets = vec![SelectTarget::All];
    let where_expr = Expression::IsNull(Box::new(Expression::ColumnRef("flag".into())));
    let mask = mask_for(&targets, Some(&where_expr)).expect("select * is projectable");
    // `SELECT *` needs every column regardless of the predicate.
    assert_eq!(mask.selected(), [true, true, true]);
}

#[test]
fn predicate_narrows_a_narrow_projection() {
    let targets = vec![count_star_target()];
    let where_expr = Expression::IsNull(Box::new(Expression::ColumnRef("flag".into())));
    let mask = mask_for(&targets, Some(&where_expr)).expect("count(*) where flag is null");
    assert_eq!(mask.selected(), [false, false, true]);
    assert!(!mask.reads_no_column());
}

#[test]
fn order_and_group_keys_are_included() {
    let schema = schema();
    let targets = vec![count_star_target()];
    let order_by = vec![plomid_sql::OrderByItem {
        expr: Expression::ColumnRef("amount".into()),
        descending: false,
        nulls_first: None,
    }];
    let group_by = plomid_sql::GroupByClause::Simple(vec![Expression::ColumnRef("flag".into())]);
    let mask = required_columns(
        &schema,
        &targets,
        None,
        Some(&group_by),
        None,
        &order_by,
        &[],
    )
    .expect("group/order keys are projectable");
    assert_eq!(mask.selected(), [false, true, true]);
}

#[test]
fn unknown_column_refuses_projection() {
    let targets = vec![SelectTarget::Expr {
        expr: Expression::ColumnRef("missing".into()),
        alias: None,
    }];
    assert!(mask_for(&targets, None).is_none());
    assert!(indexes_for(&targets).is_none());
}

#[test]
fn qualified_column_refs_resolve() {
    let targets = vec![function_call(
        "sum",
        vec![Expression::ColumnRef("public.t.amount".into())],
    )];
    assert_eq!(indexes_for(&targets), Some(BTreeSet::from([1])));
}

#[test]
fn window_function_refuses_projection() {
    let targets = vec![SelectTarget::WindowFunction {
        name: "row_number".into(),
        args: Vec::new(),
        over: plomid_sql::WindowSpec::default(),
    }];
    assert!(mask_for(&targets, None).is_none());
    assert!(indexes_for(&targets).is_none());
}

#[test]
fn unselected_positions_decode_to_null() {
    // The mask contract: unselected positions are NULL placeholders that
    // the caller has proven unreachable. Decoding must still step over the
    // real bytes so the walk stays aligned with the stored row.
    let row = encode_row(&[Value::Int8(7), Value::Int8(1234), Value::Bool(true)]).expect("encode");
    let values = plomid_executor::encoding::decode_row_selected(&row, &[false, true, false])
        .expect("partial decode");
    assert_eq!(values[0], Value::Null);
    assert_eq!(values[1], Value::Int8(1234));
    assert_eq!(values[2], Value::Null);

    let all = plomid_executor::encoding::decode_row(&row).expect("full decode");
    assert_eq!(
        all,
        vec![Value::Int8(7), Value::Int8(1234), Value::Bool(true)]
    );
}

#[test]
fn validate_row_accepts_well_formed_rows() {
    let row = encode_row(&[Value::Null, Value::Bool(false)]).expect("encode");
    plomid_executor::encoding::validate_row(&row).expect("well formed");
    assert!(plomid_executor::encoding::validate_row(&[]).is_err());
    assert!(plomid_executor::encoding::validate_row(&[0xff, 1, 0]).is_err());
}

#[test]
fn width_mismatch_falls_back_to_full_decode() {
    let row = encode_row(&[Value::Int8(1), Value::Int8(2)]).expect("encode");
    let values = plomid_executor::encoding::decode_row_selected(&row, &[false]).expect("fallback");
    assert_eq!(values, vec![Value::Int8(1), Value::Int8(2)]);
}
