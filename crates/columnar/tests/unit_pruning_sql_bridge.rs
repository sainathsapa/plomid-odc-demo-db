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
// Moved from src/pruning/sql_bridge.rs inline #[cfg(test)].
use plomid_columnar::pruning::predicate::{PruneOperator, PrunePredicate};
use plomid_columnar::pruning::sql_bridge::{
    lower_sql_expression, lower_sql_expression_strict, SqlLowering,
};
use plomid_core::ColumnId;
use plomid_storage::Field;
use std::collections::BTreeMap;

fn columns() -> BTreeMap<String, ColumnId> {
    BTreeMap::from([
        ("id".to_owned(), ColumnId::new(0)),
        ("name".to_owned(), ColumnId::new(1)),
    ])
}

fn col(name: &str) -> Box<plomid_sql::Expression> {
    Box::new(plomid_sql::Expression::ColumnRef(name.to_owned()))
}

fn lit(value: plomid_sql::Value) -> Box<plomid_sql::Expression> {
    Box::new(plomid_sql::Expression::Literal(value))
}

fn opaque() -> plomid_sql::Expression {
    plomid_sql::Expression::FunctionCall {
        name: "f".to_owned(),
        args: Vec::new(),
        distinct: false,
        filter: None,
        order_by: Vec::new(),
        returning: None,
        null_handling: None,
        unique_keys: None,
    }
}

#[test]
fn comparisons_lower_regardless_of_literal_side() {
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Equal(col("id"), lit(plomid_sql::Value::Int8(7))),
        &columns(),
    );
    assert_eq!(
        lowered,
        SqlLowering::Prunable(PrunePredicate::eq(ColumnId::new(0), Field::Integer(7)))
    );
    // `7 < id` means `id > 7`: the operator is mirrored.
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Less(lit(plomid_sql::Value::Int4(7)), col("id")),
        &columns(),
    );
    assert_eq!(
        lowered,
        SqlLowering::Prunable(PrunePredicate::compare(
            ColumnId::new(0),
            PruneOperator::Greater,
            Field::Integer(7),
        ))
    );
}

#[test]
fn unknown_columns_and_unsupported_shapes_do_not_lower() {
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Equal(col("missing"), lit(plomid_sql::Value::Int8(1))),
        &columns(),
    );
    assert!(!lowered.prunable());
    // A bare column is not a predicate the pruner understands.
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::ColumnRef("id".to_owned()),
        &columns(),
    );
    assert!(!lowered.prunable());
    // Float literals have no storage ordering: scan.
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Equal(col("id"), lit(plomid_sql::Value::Float8(1.5))),
        &columns(),
    );
    assert!(!lowered.prunable());
    // Function calls are opaque to the pruner.
    let lowered = lower_sql_expression(&opaque(), &columns());
    assert!(!lowered.prunable());
}

#[test]
fn compound_shapes_keep_their_conservative_meaning() {
    let left = plomid_sql::Expression::Equal(col("id"), lit(plomid_sql::Value::Int8(1)));
    let opaque = opaque();
    // `prunable AND opaque` keeps the prunable branch (either branch
    // proving impossibility prunes the conjunction).
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::And(Box::new(left.clone()), Box::new(opaque.clone())),
        &columns(),
    );
    assert!(lowered.prunable());
    // `prunable OR opaque` cannot prune: the opaque branch might match.
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Or(Box::new(left), Box::new(opaque)),
        &columns(),
    );
    assert!(!lowered.prunable());
}

#[test]
fn between_and_in_lower_to_conjunctions_and_disjunctions() {
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::Between {
            expr: col("id"),
            low: lit(plomid_sql::Value::Int8(1)),
            high: lit(plomid_sql::Value::Int8(9)),
            negated: false,
        },
        &columns(),
    );
    let expected = PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::GreaterOrEqual,
        Field::Integer(1),
    )
    .and(PrunePredicate::compare(
        ColumnId::new(0),
        PruneOperator::LessOrEqual,
        Field::Integer(9),
    ));
    assert_eq!(lowered, SqlLowering::Prunable(expected));
    let lowered = lower_sql_expression(
        &plomid_sql::Expression::In {
            expr: col("id"),
            list: vec![
                plomid_sql::Expression::Literal(plomid_sql::Value::Int8(1)),
                plomid_sql::Expression::Literal(plomid_sql::Value::Int8(2)),
            ],
            subquery: None,
            negated: false,
        },
        &columns(),
    );
    assert!(lowered.prunable());
}

#[test]
fn strict_declines_partial_and_that_lenient_keeps() {
    // `id = 1 AND opaque()`: lenient keeps `id = 1` for zone-map pruning
    // (sound: the opaque branch only removes rows); strict declines the
    // whole conjunction because it cannot serve as an exact row filter.
    let partial = plomid_sql::Expression::And(
        Box::new(plomid_sql::Expression::Equal(
            col("id"),
            lit(plomid_sql::Value::Int8(1)),
        )),
        Box::new(opaque()),
    );
    assert!(lower_sql_expression(&partial, &columns()).prunable());
    assert!(!lower_sql_expression_strict(&partial, &columns()).prunable());
    // Fully prunable conjunctions agree in both modes.
    let full = plomid_sql::Expression::And(
        Box::new(plomid_sql::Expression::Equal(
            col("id"),
            lit(plomid_sql::Value::Int8(1)),
        )),
        Box::new(plomid_sql::Expression::Greater(
            col("id"),
            lit(plomid_sql::Value::Int8(0)),
        )),
    );
    assert!(lower_sql_expression(&full, &columns()).prunable());
    assert!(lower_sql_expression_strict(&full, &columns()).prunable());
    assert_eq!(
        lower_sql_expression(&full, &columns()),
        lower_sql_expression_strict(&full, &columns()),
    );
}

#[test]
fn strict_keeps_fully_prunable_compound_shapes() {
    // NOT / BETWEEN / IN are already all-or-nothing, so strict keeps them
    // exactly when lenient does.
    let not = plomid_sql::Expression::Not(Box::new(plomid_sql::Expression::Equal(
        col("id"),
        lit(plomid_sql::Value::Int8(1)),
    )));
    assert_eq!(
        lower_sql_expression(&not, &columns()),
        lower_sql_expression_strict(&not, &columns()),
    );
    assert!(lower_sql_expression_strict(&not, &columns()).prunable());
    let between = plomid_sql::Expression::Between {
        expr: col("id"),
        low: lit(plomid_sql::Value::Int8(1)),
        high: lit(plomid_sql::Value::Int8(9)),
        negated: false,
    };
    assert!(lower_sql_expression_strict(&between, &columns()).prunable());
    // NOT of an opaque shape declines in both modes.
    let not_opaque = plomid_sql::Expression::Not(Box::new(opaque()));
    assert!(!lower_sql_expression(&not_opaque, &columns()).prunable());
    assert!(!lower_sql_expression_strict(&not_opaque, &columns()).prunable());
}
