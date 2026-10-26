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
// Moved from src/query/group.rs inline #[cfg(test)].
mod fast_key_tests {

    use plomid_executor::query::group::*;

    use plomid_sql::Expression;

    use plomid_sql::TableSchema;
    use plomid_sql::Value;

    fn legacy_key(value: &Value) -> String {
        format!("{value:?}")
    }

    /// Partition-equivalence with the legacy Debug-string keys: two
    /// values share a fast group if and only if they shared a legacy
    /// group. Randomized over every value family, including the
    /// adversarial cases (variant-mixed ints, `"Null"` texts, NaN
    /// payloads, signed zeros, empty strings).
    #[test]
    fn fast_key_partitions_like_debug_strings() {
        let mut state = 0x51ab_1e5du64;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state
        };
        let mut pool = vec![
            Value::Null,
            Value::Int2(0),
            Value::Int2(-1),
            Value::Int2(i16::MIN),
            Value::Int2(i16::MAX),
            Value::Int4(0),
            Value::Int4(-1),
            Value::Int4(i32::MIN),
            Value::Int4(i32::MAX),
            Value::Int8(0),
            Value::Int8(-1),
            Value::Int8(i64::MIN),
            Value::Int8(i64::MAX),
            Value::Bool(true),
            Value::Bool(false),
            Value::Float8(0.0),
            Value::Float8(-0.0),
            Value::Float8(f64::NAN),
            Value::Float8(f64::INFINITY),
            Value::Float8(f64::NEG_INFINITY),
        ];
        for _ in 0..40 {
            let text = format!("t{}", next() % 7);
            pool.push(Value::Text(text.clone()));
            pool.push(Value::VarChar(text));
        }
        pool.push(Value::Text("Null".to_string()));
        pool.push(Value::Text(String::new()));
        pool.push(Value::Text("Int8(1)".to_string()));
        for a in &pool {
            for b in &pool {
                let fast_eq = FastKey::from_value(a) == FastKey::from_value(b);
                let legacy_eq = legacy_key(a) == legacy_key(b);
                assert_eq!(fast_eq, legacy_eq, "partition mismatch for {a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn fast_key_debug_spelling_matches_legacy() {
        // The finish-time conversion must emit byte-identical key
        // strings (they are internal only, but identity keeps the
        // invariant auditable).
        for value in [
            Value::Null,
            Value::Int8(-5),
            Value::Bool(true),
            Value::Text("x".to_string()),
        ] {
            assert_eq!(FastKey::debug_key(&value), legacy_key(&value));
        }
    }

    fn test_schema() -> TableSchema {
        use plomid_sql::{Catalog, ColumnDef, ColumnType, InMemoryCatalog};
        let mut catalog = InMemoryCatalog::new();
        let column = |name: &str| ColumnDef {
            name: name.to_string(),
            col_type: ColumnType::int4(),
            constraints: Vec::new(),
        };
        catalog
            .create_table(
                "public.t".to_string(),
                vec![column("id"), column("g")],
                Vec::new(),
            )
            .expect("create table");
        catalog.get_table("public.t").expect("get table").clone()
    }

    #[test]
    fn routing_single_colref_is_fast_others_legacy() {
        let schema = test_schema();
        let col = |name: &str| Expression::ColumnRef(name.to_string());
        // Single plain column ref resolves to the fast path on first feed.
        let mut fast = StreamingGroupedAgg::new(&[], &[col("g")]);
        assert_eq!(fast.fast, KeyPlan::Undecided);
        fast.resolve_fast(&schema);
        assert_eq!(fast.fast, KeyPlan::Fast { idx: 1 });
        // Multi-key, unknown columns, and session functions stay legacy.
        let mut multi = StreamingGroupedAgg::new(&[], &[col("g"), col("id")]);
        multi.resolve_fast(&schema);
        assert_eq!(multi.fast, KeyPlan::Legacy);
        let mut missing = StreamingGroupedAgg::new(&[], &[col("nope")]);
        missing.resolve_fast(&schema);
        assert_eq!(missing.fast, KeyPlan::Legacy);
        let mut session = StreamingGroupedAgg::new(&[], &[col("current_timestamp")]);
        session.resolve_fast(&schema);
        assert_eq!(session.fast, KeyPlan::Legacy);
    }
}

mod par_sort_tests {

    use plomid_executor::query::expr::evaluate_expression;
    use plomid_executor::query::group::*;

    use plomid_sql::Expression;

    use plomid_sql::TableSchema;
    use plomid_sql::Value;
    use plomid_sql::{
        Catalog, ColumnDef, ColumnType, Constraint, ConstraintKind, InMemoryCatalog, OrderByItem,
    };

    fn col_type(name: &str) -> ColumnType {
        ColumnType::from_type_name(name).expect("known test type")
    }

    fn make_schema(cols: &[(&str, &str)], constraints: Vec<Constraint>) -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .create_table(
                "public.t".to_string(),
                cols.iter()
                    .map(|(name, ty)| ColumnDef {
                        name: (*name).to_string(),
                        col_type: col_type(ty),
                        constraints: Vec::new(),
                    })
                    .collect(),
                constraints,
            )
            .expect("create table");
        catalog.get_table("public.t").expect("get table").clone()
    }

    fn pk_schema() -> TableSchema {
        make_schema(
            &[("id", "bigint"), ("a", "integer"), ("b", "text")],
            vec![Constraint::new(
                ConstraintKind::PrimaryKey,
                vec!["id".to_string()],
            )],
        )
    }

    fn key(name: &str, descending: bool) -> OrderByItem {
        OrderByItem {
            expr: Expression::ColumnRef(name.to_string()),
            descending,
            nulls_first: None,
        }
    }

    #[test]
    fn total_order_single_pk_member() {
        let schema = pk_schema();
        assert!(par_sort_order_is_total(&schema, &[key("id", false)]));
        assert!(par_sort_order_is_total(&schema, &[key("id", true)]));
        assert!(par_sort_order_is_total(
            &schema,
            &[key("a", false), key("id", false)]
        ));
        assert!(par_sort_order_is_total(
            &schema,
            &[key("a", true), key("b", false), key("id", false)]
        ));
        // Tied keys without the PK never qualify.
        assert!(!par_sort_order_is_total(&schema, &[key("a", false)]));
        assert!(!par_sort_order_is_total(
            &schema,
            &[key("a", false), key("b", false)]
        ));
        assert!(!par_sort_order_is_total(&schema, &[]));
        // Unknown columns cannot prove coverage.
        assert!(!par_sort_order_is_total(&schema, &[key("nope", false)]));
        assert!(!par_sort_order_is_total(
            &schema,
            &[key("a", false), key("nope", false)]
        ));
        // Table qualification resolves to the same column.
        assert!(par_sort_order_is_total(&schema, &[key("t.id", false)]));
    }

    #[test]
    fn total_order_unique_not_null_but_not_nullable_unique() {
        let nullable = make_schema(
            &[("u", "bigint"), ("v", "integer")],
            vec![Constraint::new(
                ConstraintKind::Unique,
                vec!["u".to_string()],
            )],
        );
        // NULLs tie under the comparator, so a nullable UNIQUE is not total.
        assert!(!par_sort_order_is_total(&nullable, &[key("u", false)]));
        let not_null = make_schema(
            &[("u", "bigint"), ("v", "integer")],
            vec![
                Constraint::new(ConstraintKind::Unique, vec!["u".to_string()]),
                Constraint::new(ConstraintKind::NotNull, vec!["u".to_string()]),
            ],
        );
        assert!(par_sort_order_is_total(&not_null, &[key("u", false)]));
    }

    #[test]
    fn total_order_composite_pk_needs_full_tuple() {
        let schema = make_schema(
            &[("a", "integer"), ("b", "integer"), ("v", "integer")],
            vec![Constraint::new(
                ConstraintKind::PrimaryKey,
                vec!["a".to_string(), "b".to_string()],
            )],
        );
        assert!(!par_sort_order_is_total(&schema, &[key("a", false)]));
        assert!(!par_sort_order_is_total(&schema, &[key("b", false)]));
        assert!(par_sort_order_is_total(
            &schema,
            &[key("a", false), key("b", false)]
        ));
        assert!(par_sort_order_is_total(
            &schema,
            &[key("b", false), key("a", false)]
        ));
    }

    #[test]
    fn total_order_rejects_float_unique_columns() {
        let schema = make_schema(
            &[("f", "double precision"), ("v", "integer")],
            vec![
                Constraint::new(ConstraintKind::Unique, vec!["f".to_string()]),
                Constraint::new(ConstraintKind::NotNull, vec!["f".to_string()]),
            ],
        );
        // `value_cmp` projects through `f64` (NaN ties, large values can
        // collide), so float uniqueness is not comparator-total.
        assert!(!par_sort_order_is_total(&schema, &[key("f", false)]));
    }

    #[test]
    fn parallel_safe_keys_reject_session_and_volatile() {
        assert!(par_sort_keys_parallel_safe(&[key("a", false)]));
        assert!(!par_sort_keys_parallel_safe(&[key(
            "current_timestamp",
            false
        )]));
        let random_call = OrderByItem {
            expr: Expression::FunctionCall {
                name: "random".to_string(),
                args: Vec::new(),
                distinct: false,
                filter: None,
                order_by: Vec::new(),
                returning: None,
                null_handling: None,
                unique_keys: None,
            },
            descending: false,
            nulls_first: None,
        };
        assert!(!par_sort_keys_parallel_safe(&[random_call]));
        // Pure scalar calls over columns stay eligible.
        let lower_a = OrderByItem {
            expr: Expression::FunctionCall {
                name: "lower".to_string(),
                args: vec![Expression::ColumnRef("a".to_string())],
                distinct: false,
                filter: None,
                order_by: Vec::new(),
                returning: None,
                null_handling: None,
                unique_keys: None,
            },
            descending: false,
            nulls_first: None,
        };
        assert!(par_sort_keys_parallel_safe(&[lower_a]));
        // A volatile call nested inside an expression still declines.
        let nested = OrderByItem {
            expr: Expression::Add(
                Box::new(Expression::ColumnRef("a".to_string())),
                Box::new(Expression::FunctionCall {
                    name: "RANDOM".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    returning: None,
                    null_handling: None,
                    unique_keys: None,
                }),
            ),
            descending: false,
            nulls_first: None,
        };
        assert!(!par_sort_keys_parallel_safe(&[nested]));
    }

    /// Deterministic synthetic rows: heavy ties in `a`, unique `id`.
    /// `NULL` every 17th `a`, exactly like the SQL-level proof tables.
    fn synthetic_rows(n: i64) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(n as usize);
        // Interleave ids so input order is not sorted order.
        let mut state = 0x51ab_1e5du64;
        let mut ids: Vec<i64> = (0..n).collect();
        for i in (1..ids.len()).rev() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ids.swap(i, (state % (i as u64 + 1)) as usize);
        }
        for id in ids {
            let a = if id % 17 == 0 {
                Value::Null
            } else {
                Value::Int4((id % 50) as i32)
            };
            rows.push(vec![Value::Int8(id), a, Value::Int8(id)]);
        }
        rows
    }

    fn workers() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(2)
            .max(2)
    }

    #[test]
    fn par_sort_matches_sequential_on_total_orders() {
        let schema = pk_schema();
        let shapes: Vec<(Vec<OrderByItem>, Option<usize>)> = vec![
            (vec![key("id", false)], None),
            (vec![key("id", true)], None),
            (vec![key("a", false), key("id", false)], None),
            (vec![key("a", true), key("id", true)], None),
            (
                vec![key("a", false), key("b", false), key("id", false)],
                None,
            ),
            (vec![key("a", false), key("id", false)], Some(7)),
            (vec![key("id", true)], Some(1)),
            (vec![key("id", false)], Some(0)),
            (vec![key("a", false), key("id", false)], Some(4_999)),
        ];
        for (order_by, top_k) in &shapes {
            assert!(
                par_sort_order_is_total(&schema, order_by),
                "test shape must be total: {order_by:?}"
            );
            let rows = synthetic_rows(5_000);
            let expect = sort_rows(rows.clone(), &schema, order_by, *top_k).expect("sequential");
            let got = par_sort_rows(rows, &schema, order_by, *top_k, workers()).expect("parallel");
            assert_eq!(
                got, expect,
                "parallel must equal sequential: {order_by:?} {top_k:?}"
            );
            // And the parallel result is sorted per the comparator.
            let keys: Vec<Vec<Value>> = got
                .iter()
                .map(|row| {
                    order_by
                        .iter()
                        .map(|item| evaluate_expression(row, &schema, &item.expr).expect("keys"))
                        .collect()
                })
                .collect();
            assert!(
                keys.windows(2)
                    .all(|w| compare_order_keys(&w[0], &w[1], order_by)
                        != std::cmp::Ordering::Greater),
                "parallel output sorted: {order_by:?}"
            );
        }
    }

    #[test]
    fn par_sort_is_deterministic_without_total_order() {
        // Without the gate only key-order is guaranteed; the requirement is
        // determinism (run twice identical), never equality with sequential.
        let schema = pk_schema();
        let order_by = vec![key("a", false)];
        assert!(!par_sort_order_is_total(&schema, &order_by));
        let rows = synthetic_rows(5_000);
        let first =
            par_sort_rows(rows.clone(), &schema, &order_by, None, workers()).expect("run 1");
        let second = par_sort_rows(rows, &schema, &order_by, None, workers()).expect("run 2");
        assert_eq!(first, second, "parallel merge must be deterministic");
        assert_eq!(first.len(), 5_000);
    }

    #[test]
    fn par_sort_error_matches_sequential_row_order() {
        let schema = pk_schema();
        let order_by = vec![
            OrderByItem {
                expr: Expression::ColumnRef("nope".to_string()),
                descending: false,
                nulls_first: None,
            },
            key("id", false),
        ];
        let rows = synthetic_rows(100);
        let seq_err = sort_rows(rows.clone(), &schema, &order_by, None).expect_err("must fail");
        let par_err =
            par_sort_rows(rows, &schema, &order_by, None, workers()).expect_err("must fail");
        assert_eq!(format!("{seq_err}"), format!("{par_err}"));
    }

    #[test]
    fn par_sort_workers_for_threshold() {
        // Tier-sensitive reader: pin Laptop so concurrent tier tests
        // cannot move the gate mid-flight (Laptop is ambient on
        // Laptop-class hosts, so the pin is a no-op there).
        let _tier = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        );
        // The pinned Laptop tier exposes exactly the historical gate.
        assert_eq!(
            plomid_executor::parallel_budget::sort_min_rows(),
            PAR_SORT_MIN_ROWS
        );
        assert_eq!(par_sort_workers_for(0), None);
        assert_eq!(par_sort_workers_for(PAR_SORT_MIN_ROWS as u64 - 1), None);
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(
            par_sort_workers_for(PAR_SORT_MIN_ROWS as u64).is_some(),
            cpus >= 2
        );
        // The derivation clamps to the tier permits/cap instead of reporting
        // raw parallelism: under an Edge pin the same above-threshold input
        // reports at most the 2-permit width.
        drop(_tier);
        let _edge = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Edge),
        );
        assert_eq!(par_sort_workers_for(PAR_SORT_MIN_ROWS as u64 - 1), None);
        // Edge sort threshold is 200_000: 50_000 rows stay sequential.
        assert_eq!(par_sort_workers_for(199_999), None);
        // The derivation reports machine parallelism clamped first to the
        // tier's per-dispatch permits (Edge: 2) and then to the tier cap
        // (fewer than 2 workers stays sequential).
        let capped = cpus.min(2);
        if capped >= 2 {
            assert_eq!(
                par_sort_workers_for(200_000),
                Some(capped),
                "edge reports permit-clamped workers"
            );
        } else {
            assert_eq!(par_sort_workers_for(200_000), None);
        }
    }
}

mod group_spill_tests {

    use plomid_executor::error::SqlResult;
    use plomid_executor::query::aggregate::collect_aggregate_slots;

    use plomid_executor::query::group::*;

    use plomid_sql::Expression;
    use plomid_sql::QueryResult;
    use plomid_sql::SelectTarget;
    use plomid_sql::TableSchema;
    use plomid_sql::Value;
    use plomid_sql::{Catalog, ColumnDef, ColumnType, InMemoryCatalog};

    /// Serializes the spill tests: they share the process temp dir, so the
    /// cleanup-count assertion needs no sibling spill in flight.
    static GROUP_SPILL_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(name.to_string())
    }

    fn col_type(name: &str) -> ColumnType {
        ColumnType::from_type_name(name).expect("known test type")
    }

    /// Mixed-type schema: integer, text and float group keys plus integer and
    /// text aggregate inputs, so keys and values cover every codec path.
    fn spill_schema() -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .create_table(
                "public.t".to_string(),
                [
                    ("gi", "integer"),
                    ("gt", "text"),
                    ("gf", "double precision"),
                    ("v", "integer"),
                    ("w", "text"),
                ]
                .into_iter()
                .map(|(name, ty)| ColumnDef {
                    name: (*name).to_string(),
                    col_type: col_type(ty),
                    constraints: Vec::new(),
                })
                .collect(),
                Vec::new(),
            )
            .expect("create table");
        catalog.get_table("public.t").expect("get table").clone()
    }

    fn agg_target(name: &str, arg: Expression) -> SelectTarget {
        SelectTarget::Expr {
            expr: Expression::FunctionCall {
                name: name.to_string(),
                args: vec![arg],
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

    fn agg_target_distinct(name: &str, arg: Expression) -> SelectTarget {
        SelectTarget::Expr {
            expr: Expression::FunctionCall {
                name: name.to_string(),
                args: vec![arg],
                distinct: true,
                filter: None,
                order_by: Vec::new(),
                returning: None,
                null_handling: None,
                unique_keys: None,
            },
            alias: None,
        }
    }

    fn key_target(name: &str) -> SelectTarget {
        SelectTarget::Expr {
            expr: col(name),
            alias: None,
        }
    }

    fn count_star_call() -> Expression {
        Expression::FunctionCall {
            name: "count".to_string(),
            args: vec![Expression::Star],
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            returning: None,
            null_handling: None,
            unique_keys: None,
        }
    }

    fn having_count_gt(n: i64) -> Expression {
        Expression::Greater(
            Box::new(count_star_call()),
            Box::new(Expression::Literal(Value::Int8(n))),
        )
    }

    /// Deterministic mixed-type rows: NULLs in every key and value column
    /// (13/17/19/5/23 strides), heavy key ties, dyadic floats (exactly
    /// representable, never NaN or signed zero).
    fn mixed_rows(n: i64) -> Vec<Vec<Value>> {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut rows = Vec::with_capacity(n as usize);
        for i in 0..n {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let gi = if i % 13 == 0 {
                Value::Null
            } else {
                Value::Int4((i % 7) as i32)
            };
            let gt = if i % 17 == 0 {
                Value::Null
            } else {
                Value::Text(format!("k{}", i % 5))
            };
            let gf = if i % 19 == 0 {
                Value::Null
            } else {
                Value::Float8((state % 401) as f64 / 8.0 - 25.0)
            };
            let v = if i % 5 == 0 {
                Value::Null
            } else {
                Value::Int4((state % 2001) as i32 - 1000)
            };
            let w = if i % 23 == 0 {
                Value::Null
            } else {
                Value::Text(format!("w{}", state % 3))
            };
            rows.push(vec![gi, gt, gf, v, w]);
        }
        rows
    }

    /// Query shapes: single int key, mixed multi-key, float key, global
    /// aggregation, and a HAVING shape. Each is `(group_exprs, targets,
    /// having)`.
    fn shapes() -> Vec<(Vec<Expression>, Vec<SelectTarget>, Option<Expression>)> {
        vec![
            (
                vec![col("gi")],
                vec![
                    key_target("gi"),
                    agg_target("count", Expression::Star),
                    agg_target("count", col("v")),
                    agg_target("sum", col("v")),
                    agg_target("avg", col("v")),
                    agg_target("min", col("v")),
                    agg_target("max", col("v")),
                    agg_target_distinct("count", col("w")),
                ],
                None,
            ),
            (
                vec![col("gi"), col("gt")],
                vec![
                    key_target("gi"),
                    key_target("gt"),
                    agg_target("count", Expression::Star),
                    agg_target("sum", col("v")),
                    agg_target("avg", col("v")),
                    agg_target("min", col("w")),
                    agg_target("max", col("w")),
                    agg_target("sum", col("gf")),
                    agg_target("avg", col("gf")),
                ],
                None,
            ),
            (
                vec![col("gf")],
                vec![
                    key_target("gf"),
                    agg_target("count", Expression::Star),
                    agg_target("min", col("gf")),
                    agg_target("max", col("gf")),
                ],
                None,
            ),
            (
                Vec::new(),
                vec![
                    agg_target("count", Expression::Star),
                    agg_target("count", col("v")),
                    agg_target("sum", col("v")),
                    agg_target("avg", col("v")),
                    agg_target("min", col("v")),
                    agg_target("max", col("v")),
                    agg_target("sum", col("gf")),
                    agg_target("avg", col("gf")),
                ],
                None,
            ),
            (
                vec![col("gi")],
                vec![
                    key_target("gi"),
                    agg_target("count", Expression::Star),
                    agg_target("sum", col("v")),
                ],
                Some(having_count_gt(3)),
            ),
        ]
    }

    /// Canonical result: exact columns plus SQL-text rows in sorted order
    /// (result row order without ORDER BY is unspecified on every path, so
    /// tests compare canonically like the merge-equivalence proofs do).
    fn canonical(result: SqlResult<QueryResult>) -> (Vec<String>, Vec<String>) {
        match result.expect("query") {
            QueryResult::Rows { columns, rows, .. } => {
                let mut out: Vec<String> = rows
                    .into_iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| v.to_sql_text())
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect();
                out.sort();
                (columns, out)
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn run_spilled(
        schema: &TableSchema,
        targets: &[SelectTarget],
        group_exprs: &[Expression],
        having: Option<&Expression>,
        rows: Vec<Vec<Value>>,
        budget: usize,
    ) -> (Vec<String>, Vec<String>) {
        let slots = collect_aggregate_slots(targets, having);
        canonical(grouped_agg_spilled(
            schema,
            targets,
            group_exprs,
            having,
            rows,
            &slots,
            "db",
            "user",
            budget,
        ))
    }

    fn run_sequential(
        schema: &TableSchema,
        targets: &[SelectTarget],
        group_exprs: &[Expression],
        having: Option<&Expression>,
        rows: Vec<Vec<Value>>,
    ) -> (Vec<String>, Vec<String>) {
        let slots = collect_aggregate_slots(targets, having);
        canonical(execute_single_grouped_select(
            schema,
            targets,
            group_exprs,
            having,
            rows,
            &slots,
            "db",
            "user",
        ))
    }

    #[test]
    fn under_budget_delegates_byte_identical() {
        let _serial = GROUP_SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let rows = mixed_rows(200);
        for (group_exprs, targets, having) in &shapes() {
            let having_ref = having.as_ref();
            let expect = run_sequential(&schema, targets, group_exprs, having_ref, rows.clone());
            // At exactly the input length the in-memory path is taken.
            let got = run_spilled(
                &schema,
                targets,
                group_exprs,
                having_ref,
                rows.clone(),
                rows.len(),
            );
            assert_eq!(got, expect, "under budget must delegate: {group_exprs:?}");
        }
    }

    #[test]
    fn over_budget_spill_matches_sequential() {
        let _serial = GROUP_SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let rows = mixed_rows(200);
        for (group_exprs, targets, having) in &shapes() {
            let having_ref = having.as_ref();
            let expect = run_sequential(&schema, targets, group_exprs, having_ref, rows.clone());
            // Tiny budgets force many partitions (200 rows / 16 = 13; a zero
            // budget still moves forward).
            for budget in [16usize, 1, 0] {
                let got = run_spilled(
                    &schema,
                    targets,
                    group_exprs,
                    having_ref,
                    rows.clone(),
                    budget,
                );
                assert_eq!(
                    got, expect,
                    "spill must equal sequential: {group_exprs:?} budget={budget}"
                );
            }
        }
    }

    #[test]
    fn spill_cleans_up_temp_dirs() {
        let _serial = GROUP_SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let (group_exprs, targets, having) = &shapes()[1];
        let having_ref = having.as_ref();
        let before = group_spill_dir_count();
        let rows = mixed_rows(120);
        let got = run_spilled(&schema, targets, group_exprs, having_ref, rows.clone(), 16);
        let expect = run_sequential(&schema, targets, group_exprs, having_ref, rows);
        assert_eq!(got, expect);
        assert_eq!(
            group_spill_dir_count(),
            before,
            "group spill must remove its temp dir"
        );
    }

    #[test]
    fn spill_error_matches_sequential_row_order() {
        let _serial = GROUP_SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let group_exprs = vec![col("nope")];
        let targets = vec![agg_target("count", Expression::Star)];
        let slots = collect_aggregate_slots(&targets, None);
        let rows = mixed_rows(100);
        let seq_err = execute_single_grouped_select(
            &schema,
            &targets,
            &group_exprs,
            None,
            rows.clone(),
            &slots,
            "db",
            "user",
        )
        .expect_err("must fail");
        let spill_err = grouped_agg_spilled(
            &schema,
            &targets,
            &group_exprs,
            None,
            rows,
            &slots,
            "db",
            "user",
            16,
        )
        .expect_err("must fail");
        assert_eq!(format!("{seq_err}"), format!("{spill_err}"));
    }

    fn group_spill_dir_count() -> usize {
        std::fs::read_dir(std::env::temp_dir())
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("plomid-group-spill-")
                    })
                    .count()
            })
            .unwrap_or(0)
    }
}
