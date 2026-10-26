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
// Moved from src/query/select.rs inline #[cfg(test)].
mod routing_tests {

    use plomid_executor::executor::Executor;

    use plomid_executor::query::select::*;

    use plomid_sql::Catalog;

    use plomid_sql::Expression;

    use plomid_sql::Value;

    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch_engine() -> (
        Executor<plomid_txn::PlomidStorageEngine>,
        std::path::PathBuf,
    ) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("plomid-routing-it-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
        let wal = root.with_extension("wal");
        let engine = plomid_txn::PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
        (Executor::new(engine).expect("executor"), root)
    }

    fn eq(column: &str, literal: Value) -> Expression {
        Expression::Equal(
            Box::new(Expression::ColumnRef(column.to_string())),
            Box::new(Expression::Literal(literal)),
        )
    }

    fn fixture() -> (
        Executor<plomid_txn::PlomidStorageEngine>,
        std::path::PathBuf,
    ) {
        let (mut session, root) = scratch_engine();
        session
            .execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, code VARCHAR, n INTEGER);")
            .unwrap();
        session
            .execute("CREATE INDEX t_name_idx ON t (name);")
            .unwrap();
        session
            .execute("CREATE UNIQUE INDEX t_code_uidx ON t (code);")
            .unwrap();
        session.execute("CREATE INDEX t_n_idx ON t (n);").unwrap();
        (session, root)
    }

    #[test]
    fn text_equality_selects_exact_index_prefix() {
        let (session, root) = fixture();
        let catalog = session.catalog();
        let bounds = index_bounds_for_predicate(
            catalog,
            "public.t",
            Some(&eq("name", Value::Text("abc".to_string()))),
        )
        .expect("text equality on an indexed TEXT column probes the index");
        // The index name is catalog-generated; resolve it for the expectation.
        let index = catalog
            .all_indexes_for_table("public.t")
            .into_iter()
            .find(|index| plomid_executor::index::index_columns(index) == ["name".to_string()])
            .expect("name index");
        let expected_start = plomid_executor::index::index_tuple_prefix(
            &index.name,
            std::slice::from_ref(&Value::Text("abc".to_string())),
        );
        assert_eq!(bounds.0, index.name);
        assert_eq!(bounds.1, expected_start);
        assert!(bounds.1 < bounds.2);
        // Upper bound is the same stem with 0xff: only exact entries match.
        let mut stem = expected_start.clone();
        stem.pop();
        let mut expected_end = stem;
        expected_end.push(0xff);
        assert_eq!(bounds.2, expected_end);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    #[test]
    fn varchar_column_probes_with_column_spelling() {
        // VARCHAR columns hold VarChar-spelled entries (tag 3), TEXT columns
        // Text-spelled entries (tag 2): the probe follows the column, which
        // is what makes it agree with the writer even though SQL literals
        // arrive in a uniform spelling.
        let (session, root) = fixture();
        let catalog = session.catalog();
        let bounds = index_bounds_for_predicate(
            catalog,
            "public.t",
            Some(&eq("code", Value::Text("k-1".to_string()))),
        )
        .expect("varchar equality probes");
        let index = catalog
            .all_indexes_for_table("public.t")
            .into_iter()
            .find(|index| plomid_executor::index::index_columns(index) == ["code".to_string()])
            .expect("code index");
        let expected = plomid_executor::index::index_tuple_prefix(
            &index.name,
            std::slice::from_ref(&Value::VarChar("k-1".to_string())),
        );
        assert_eq!(bounds.1, expected);
        // The tag byte follows the writer spelling for VARCHAR (tag 3),
        // not the literal's TEXT spelling (tag 2).
        let prefix_len = format!("__plomid_index:{}:", index.name).len();
        assert_eq!(expected[prefix_len], 3);
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    #[test]
    fn text_routing_accepts_variants_and_rejects_ranges() {
        let (session, root) = fixture();
        let catalog = session.catalog();
        // Unknown/VarChar/Name literals normalize to the same probe.
        for literal in [
            Value::Unknown("abc".to_string()),
            Value::VarChar("abc".to_string()),
            Value::Name("abc".to_string()),
        ] {
            assert!(
                index_bounds_for_predicate(catalog, "public.t", Some(&eq("name", literal)))
                    .is_some(),
                "text-family literal probes"
            );
        }
        // Unique backing index qualifies identically.
        assert!(
            index_bounds_for_predicate(
                catalog,
                "public.t",
                Some(&eq("code", Value::Text("k-1".to_string())))
            )
            .is_some(),
            "unique text index probes"
        );
        // Ranges, NULL, unindexed columns, and disjunctions keep the scan.
        let less = Expression::Less(
            Box::new(Expression::ColumnRef("name".to_string())),
            Box::new(Expression::Literal(Value::Text("abc".to_string()))),
        );
        assert!(
            index_bounds_for_predicate(catalog, "public.t", Some(&less)).is_none(),
            "text ranges still scan"
        );
        assert!(
            index_bounds_for_predicate(catalog, "public.t", Some(&eq("name", Value::Null)))
                .is_none(),
            "NULL equality still scans"
        );
        assert!(
            index_bounds_for_predicate(
                catalog,
                "public.t",
                Some(&eq("missing", Value::Text("abc".to_string())))
            )
            .is_none(),
            "unindexed column still scans"
        );
        let or = Expression::Or(
            Box::new(eq("name", Value::Text("a".to_string()))),
            Box::new(eq("name", Value::Text("b".to_string()))),
        );
        assert!(
            index_bounds_for_predicate(catalog, "public.t", Some(&or)).is_none(),
            "disjunctions still scan"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    #[test]
    fn integer_routing_unchanged_and_cross_type_safe() {
        let (session, root) = fixture();
        let catalog = session.catalog();
        // Integer equality keeps its existing range probe.
        assert!(
            index_bounds_for_predicate(catalog, "public.t", Some(&eq("n", Value::Int4(7))))
                .is_some(),
            "integer equality still probes"
        );
        // Text literal against an integer column keeps prior behavior
        // (integer-coercible text probes; anything else scans) — the text
        // probe must never claim an integer column.
        assert!(
            index_bounds_for_predicate(
                catalog,
                "public.t",
                Some(&eq("n", Value::Text("x".to_string())))
            )
            .is_none(),
            "non-numeric text on int column still scans"
        );
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }
}

mod par_agg_tests {

    use plomid_executor::query::aggregate::{collect_aggregate_slots, AggregateSlot};

    use plomid_executor::query::group::StreamingGroupedAgg;

    use plomid_executor::query::select::*;

    use plomid_sql::Catalog;
    use plomid_sql::ColumnDef;
    use plomid_sql::ColumnType;
    use plomid_sql::Expression;

    use plomid_sql::InMemoryCatalog;
    use plomid_sql::QueryResult;
    use plomid_sql::SelectTarget;
    use plomid_sql::TableSchema;
    use plomid_sql::Value;

    /// Schema exercising every type family the gate cares about: ints `a`
    /// (`INT4`) and `b` (`INT8`), float `f`, `NUMERIC` `n`, text `t`, plus a
    /// column literally named `now` proving session-name shadowing still
    /// declines (a column named `now` evaluates to the statement timestamp,
    /// which workers cannot observe).
    fn gate_schema() -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        let column = |name: &str, type_name: &str| ColumnDef {
            name: name.to_string(),
            col_type: ColumnType::from_type_name(type_name).expect("known type"),
            constraints: Vec::new(),
        };
        catalog
            .create_table(
                "public.t".to_string(),
                vec![
                    column("a", "integer"),
                    column("b", "bigint"),
                    column("f", "double precision"),
                    column("n", "numeric"),
                    column("t", "text"),
                    column("now", "integer"),
                ],
                Vec::new(),
            )
            .expect("create table");
        catalog.get_table("public.t").expect("get table").clone()
    }

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(name.to_string())
    }

    fn rows_of(result: QueryResult) -> Vec<Vec<Value>> {
        match result {
            QueryResult::Rows { rows, .. } => rows,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn slot(
        name: &str,
        arg: Option<Expression>,
        distinct: bool,
        filter: Option<Expression>,
    ) -> AggregateSlot {
        AggregateSlot {
            name: name.to_string(),
            arg,
            second_arg: None,
            filter,
            distinct,
        }
    }

    fn eligible(slots: &[AggregateSlot], groups: &[Expression], estimated_rows: u64) -> bool {
        par_agg_eligible(slots, groups, &gate_schema(), estimated_rows)
    }

    fn count_star() -> AggregateSlot {
        slot("count", Some(Expression::Star), false, None)
    }

    #[test]
    fn gate_accepts_plain_integer_shapes() {
        let slots = vec![
            count_star(),
            slot("count", Some(col("a")), false, None),
            slot("sum", Some(col("a")), false, None),
            slot("avg", Some(col("b")), false, None),
            slot("min", Some(col("a")), false, None),
            slot("max", Some(col("b")), false, None),
            slot("COUNT", Some(col("a")), true, None),
        ];
        assert!(eligible(&slots, &[], 200_000));
        assert!(eligible(&slots, &[col("a")], 200_000));
        assert!(eligible(&slots, &[col("a"), col("b")], 200_000));
        // Pure GROUP BY (no slots) is trivially mergeable.
        assert!(eligible(&[], &[col("a")], 200_000));
        // Case-insensitive names.
        assert!(eligible(
            &[slot("Sum", Some(col("a")), false, None)],
            &[],
            200_000
        ));
    }

    #[test]
    fn gate_accepts_float_min_max_count() {
        // MIN/MAX/COUNT folds are order-free, so float arguments stay eligible;
        // only SUM/AVG reduction order matters (rule 5).
        for name in ["min", "max", "count"] {
            assert!(
                eligible(&[slot(name, Some(col("f")), false, None)], &[], 200_000),
                "{name}(float) must stay eligible"
            );
        }
    }

    #[test]
    fn gate_rejects_float_and_numeric_sum_avg() {
        for (name, arg) in [
            ("sum", col("f")),
            ("avg", col("f")),
            ("sum", col("n")),
            ("avg", col("n")),
            ("sum", col("t")),
            ("avg", col("t")),
        ] {
            assert!(
                !eligible(&[slot(name, Some(arg), false, None)], &[], 200_000),
                "{name} over non-integer must decline"
            );
        }
    }

    #[test]
    fn gate_rejects_non_plain_args() {
        let expr_arg = Expression::Add(Box::new(col("a")), Box::new(col("b")));
        // Expression arguments (SUM(v+1), ...) are untested under merge.
        assert!(!eligible(
            &[slot("sum", Some(expr_arg.clone()), false, None)],
            &[],
            200_000
        ));
        // COUNT(DISTINCT expr): only a plain column ref unions exactly.
        assert!(!eligible(
            &[slot("count", Some(expr_arg), true, None)],
            &[],
            200_000
        ));
        // SUM(DISTINCT col): DISTINCT outside plain COUNT keeps its
        // historical accumulator; never merge it.
        assert!(!eligible(
            &[slot("sum", Some(col("a")), true, None)],
            &[],
            200_000
        ));
        // COUNT(DISTINCT *): not a plain column ref.
        assert!(!eligible(
            &[slot("count", Some(Expression::Star), true, None)],
            &[],
            200_000
        ));
        // Non-COUNT over Star/None (invalid shapes the accumulator degrades).
        assert!(!eligible(
            &[slot("sum", Some(Expression::Star), false, None)],
            &[],
            200_000
        ));
        assert!(!eligible(&[slot("max", None, false, None)], &[], 200_000));
    }

    #[test]
    fn gate_rejects_filters_json_and_unknown_names() {
        let pred = Expression::IsNotNull(Box::new(col("a")));
        assert!(!eligible(
            &[slot("count", Some(Expression::Star), false, Some(pred))],
            &[],
            200_000
        ));
        for name in [
            "json_agg",
            "jsonb_agg",
            "json_object_agg",
            "string_agg",
            "array_agg",
        ] {
            assert!(
                !eligible(&[slot(name, Some(col("a")), false, None)], &[], 200_000),
                "{name} must decline"
            );
        }
        let mut two_arg = slot("count", Some(col("a")), false, None);
        two_arg.second_arg = Some(col("b"));
        assert!(!eligible(&[two_arg], &[], 200_000));
        assert!(!eligible(
            &[slot("sum", Some(col("missing")), false, None)],
            &[],
            200_000
        ));
    }

    #[test]
    fn gate_rejects_small_estimate() {
        let slots = vec![count_star()];
        assert!(!eligible(&slots, &[], 99_999));
        assert!(eligible(&slots, &[], 100_000));
        assert!(eligible(&slots, &[], 100_001));
    }

    #[test]
    fn gate_rejects_session_bound_shapes() {
        // A real column literally named `now` still evaluates to the statement
        // timestamp (session shadowing), which workers cannot observe.
        assert!(!eligible(
            &[slot("sum", Some(col("now")), false, None)],
            &[],
            200_000
        ));
        assert!(!eligible(
            &[slot("count", Some(col("now")), true, None)],
            &[],
            200_000
        ));
        assert!(!eligible(&[count_star()], &[col("now")], 200_000));
        assert!(!eligible(
            &[count_star()],
            &[Expression::ColumnRef("current_timestamp".to_string())],
            200_000
        ));
    }

    #[test]
    fn gate_rejects_statement_ctx_having_and_targets() {
        // Scope-4 parallel finish evaluates HAVING/targets on workers without
        // TLS: any statement-context reference there must decline (M2).
        let having_now = Expression::Greater(
            Box::new(col("v")),
            Box::new(Expression::ColumnRef("now".to_string())),
        );
        assert!(targets_need_statement_ctx(
            &[agg_target("count", Expression::Star)],
            Some(&having_now)
        ));
        assert!(targets_need_statement_ctx(
            &[agg_target(
                "count",
                Expression::ColumnRef("now".to_string())
            )],
            None
        ));
        assert!(!targets_need_statement_ctx(
            &[agg_target("count", Expression::Star)],
            None
        ));
    }

    #[test]
    fn expr_needs_statement_ctx_cases() {
        assert!(!expr_needs_statement_ctx(&col("a")));
        assert!(!expr_needs_statement_ctx(&Expression::Literal(
            Value::Int4(1)
        )));
        assert!(expr_needs_statement_ctx(&Expression::ColumnRef(
            "current_timestamp".to_string()
        )));
        assert!(expr_needs_statement_ctx(&Expression::ColumnRef(
            "current_user".to_string()
        )));
        // `now()` spelled as a call still names a session function.
        assert!(expr_needs_statement_ctx(&Expression::FunctionCall {
            name: "now".to_string(),
            args: Vec::new(),
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            returning: None,
            null_handling: None,
            unique_keys: None,
        }));
        // Pure scalar calls stay eligible, even nested.
        let abs = Expression::FunctionCall {
            name: "abs".to_string(),
            args: vec![col("a")],
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            returning: None,
            null_handling: None,
            unique_keys: None,
        };
        assert!(!expr_needs_statement_ctx(&abs));
        let mixed = Expression::Add(Box::new(col("a")), Box::new(col("now")));
        assert!(expr_needs_statement_ctx(&mixed));
        // Window calls fail closed (they never reach the streaming path).
        assert!(expr_needs_statement_ctx(&Expression::WindowFunction {
            name: "row_number".to_string(),
            args: Vec::new(),
            over: plomid_sql::WindowSpec::default(),
        }));
    }

    fn agg_call(name: &str, with_order: bool) -> SelectTarget {
        SelectTarget::Expr {
            expr: Expression::FunctionCall {
                name: name.to_string(),
                args: vec![col("a")],
                distinct: false,
                filter: None,
                order_by: if with_order {
                    vec![plomid_sql::OrderByItem {
                        expr: col("a"),
                        descending: false,
                        nulls_first: None,
                    }]
                } else {
                    Vec::new()
                },
                returning: None,
                null_handling: None,
                unique_keys: None,
            },
            alias: None,
        }
    }

    #[test]
    fn agg_call_order_by_detection() {
        assert!(!agg_call_has_order_by(&[agg_call("sum", false)], None));
        assert!(agg_call_has_order_by(&[agg_call("sum", true)], None));
        assert!(!agg_call_has_order_by(
            &[agg_call("count", false)],
            Some(&col("a"))
        ));
        assert!(agg_call_has_order_by(
            &[agg_call("count", false)],
            Some(&Expression::FunctionCall {
                name: "sum".to_string(),
                args: vec![col("a")],
                distinct: false,
                filter: None,
                order_by: vec![plomid_sql::OrderByItem {
                    expr: col("a"),
                    descending: false,
                    nulls_first: None,
                }],
                returning: None,
                null_handling: None,
                unique_keys: None,
            })
        ));
    }

    /// The pinned Laptop tier exposes exactly the historical scan gate:
    /// tiered thresholds never drift the default tier.
    #[test]
    fn tiered_scan_threshold_pins_standard() {
        let _tier = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        );
        assert_eq!(
            plomid_executor::parallel_budget::scan_min_rows(),
            PAR_MIN_ROWS
        );
    }

    /// Drives `par_streaming_agg` directly (bypassing the row-count gate) on a
    /// small table over a REAL engine scan and compares against the sequential
    /// SQL answer. Fast proof that the channel/worker/merge plumbing is exact
    /// end to end — including NULLs, a WHERE predicate, and both grouped and
    /// global shapes.
    #[test]
    fn par_streaming_agg_direct_matches_sequential_sql() {
        use plomid_executor::executor::Executor;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("plomid-paragg-direct-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
        let wal = root.with_extension("wal");
        let engine = plomid_txn::PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
        let mut session = Executor::new(engine).expect("executor");
        session
            .execute("CREATE TABLE pm (g INTEGER, v INTEGER);")
            .unwrap();
        let mut batch = String::from("INSERT INTO pm VALUES ");
        for i in 0..3000 {
            if i > 0 {
                batch.push_str(", ");
            }
            let g = if i % 11 == 0 {
                "NULL".to_string()
            } else {
                (i % 7).to_string()
            };
            let v = if i % 5 == 0 {
                "NULL".to_string()
            } else {
                ((i * 13 % 2001) - 1000).to_string()
            };
            batch.push_str(&format!("({g}, {v})"));
        }
        batch.push(';');
        session.execute(&batch).unwrap();

        // Sequential baselines via SQL (3K rows stay sequential).
        let grouped_sql = rows_of(
            session
                .execute(
                    "SELECT g, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM pm GROUP BY g;",
                )
                .unwrap(),
        );
        let global_sql = rows_of(
            session
                .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM pm;")
                .unwrap(),
        );
        let where_sql = rows_of(
            session
                .execute("SELECT COUNT(*), SUM(v) FROM pm WHERE v > 100;")
                .unwrap(),
        );

        let qualified = session.catalog().resolve_table_name("pm").expect("resolve");
        let schema = session
            .catalog()
            .get_table(&qualified)
            .expect("table")
            .clone();
        let scan_start = format!("{qualified}:").into_bytes();
        let scan_end = format!("{qualified}:\u{10FFFF}").into_bytes();

        // Grouped shape through the parallel entry point (workers = 4).
        let targets = vec![
            SelectTarget::Expr {
                expr: col("g"),
                alias: None,
            },
            agg_target("count", Expression::Star),
            agg_target("count", col("v")),
            agg_target("sum", col("v")),
            agg_target("min", col("v")),
            agg_target("max", col("v")),
            agg_target("avg", col("v")),
        ];
        let slots = collect_aggregate_slots(&targets, None);
        let group_exprs = vec![col("g")];
        let projection = plomid_executor::projection::required_columns(
            &schema,
            &targets,
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let engine = session.engine_mut();
        let out = par_streaming_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &projection,
            None,
            &slots,
            &group_exprs,
            &targets,
            None,
            "db",
            "user",
            3000,
            4,
        )
        .expect("parallel grouped");
        let QueryResult::Rows { rows, .. } = out else {
            panic!("expected rows");
        };
        assert_eq!(sorted_text(rows), sorted_text(grouped_sql));

        // Global shape (no groups: seeded single-group merge across workers).
        let global_targets = vec![
            agg_target("count", Expression::Star),
            agg_target("count", col("v")),
            agg_target("sum", col("v")),
            agg_target("min", col("v")),
            agg_target("max", col("v")),
            agg_target("avg", col("v")),
        ];
        let global_slots = collect_aggregate_slots(&global_targets, None);
        let global_projection = plomid_executor::projection::required_columns(
            &schema,
            &global_targets,
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let engine = session.engine_mut();
        let out = par_streaming_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &global_projection,
            None,
            &global_slots,
            &[],
            &global_targets,
            None,
            "db",
            "user",
            3000,
            4,
        )
        .expect("parallel global");
        let QueryResult::Rows { rows, .. } = out else {
            panic!("expected rows");
        };
        assert_eq!(rows, global_sql);

        // WHERE predicate shape.
        let where_targets = vec![
            agg_target("count", Expression::Star),
            agg_target("sum", col("v")),
        ];
        let where_slots = collect_aggregate_slots(&where_targets, None);
        let pred = Expression::Greater(
            Box::new(col("v")),
            Box::new(Expression::Literal(Value::Int4(100))),
        );
        let where_projection = plomid_executor::projection::required_columns(
            &schema,
            &where_targets,
            Some(&pred),
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let engine = session.engine_mut();
        let out = par_streaming_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &where_projection,
            Some(&pred),
            &where_slots,
            &[],
            &where_targets,
            None,
            "db",
            "user",
            3000,
            4,
        )
        .expect("parallel where");
        let QueryResult::Rows { rows, .. } = out else {
            panic!("expected rows");
        };
        assert_eq!(rows, where_sql);

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    #[test]
    fn partition_index_splits_are_balanced_and_complete() {
        // Even split.
        assert_eq!(
            partition_index_splits(1024, 4),
            vec![(0, 256), (256, 512), (512, 768), (768, 1024)]
        );
        // Remainder goes to the first partitions, one extra row each.
        assert_eq!(
            partition_index_splits(10, 4),
            vec![(0, 3), (3, 6), (6, 8), (8, 10)]
        );
        // More workers than rows: one row each, no empty ranges.
        assert_eq!(partition_index_splits(3, 8), vec![(0, 1), (1, 2), (2, 3)]);
        // Single row / zero rows.
        assert_eq!(partition_index_splits(1, 4), vec![(0, 1)]);
        assert!(partition_index_splits(0, 4).is_empty());
        // Coverage: concatenation in order is exactly [0, len).
        for (len, workers) in [(1, 1), (7, 3), (100_000, 12), (250_000, 8)] {
            let splits = partition_index_splits(len, workers);
            assert!(splits.len() <= workers);
            let mut next = 0;
            for (s, e) in &splits {
                assert_eq!(*s, next);
                assert!(e > s);
                next = *e;
            }
            assert_eq!(next, len);
        }
    }

    /// The shared-snapshot scan path answers grouped/global/WHERE shapes
    /// exactly like sequential SQL (3K rows fit the materialize cap, so the
    /// `Some` path runs).
    #[test]
    fn par_scan_agg_direct_matches_sequential_sql() {
        use plomid_executor::executor::Executor;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT: AtomicU64 = AtomicU64::new(1000);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("plomid-parscan-direct-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
        let wal = root.with_extension("wal");
        let engine = plomid_txn::PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
        let mut session = Executor::new(engine).expect("executor");
        session
            .execute("CREATE TABLE ps (g INTEGER, v INTEGER);")
            .unwrap();
        let mut batch = String::from("INSERT INTO ps VALUES ");
        for i in 0..3000 {
            if i > 0 {
                batch.push_str(", ");
            };
            let g = if i % 11 == 0 {
                "NULL".to_string()
            } else {
                (i % 7).to_string()
            };
            let v = if i % 5 == 0 {
                "NULL".to_string()
            } else {
                ((i * 13 % 2001) - 1000).to_string()
            };
            batch.push_str(&format!("({g}, {v})"));
        }
        batch.push(';');
        session.execute(&batch).unwrap();

        let grouped_sql = rows_of(
            session
                .execute(
                    "SELECT g, COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM ps GROUP BY g;",
                )
                .unwrap(),
        );
        let global_sql = rows_of(
            session
                .execute("SELECT COUNT(*), COUNT(v), SUM(v), MIN(v), MAX(v), AVG(v) FROM ps;")
                .unwrap(),
        );

        let qualified = session.catalog().resolve_table_name("ps").expect("resolve");
        let schema = session
            .catalog()
            .get_table(&qualified)
            .expect("table")
            .clone();
        let scan_start = format!("{qualified}:").into_bytes();
        let scan_end = format!("{qualified}:\u{10FFFF}").into_bytes();

        let targets = vec![
            SelectTarget::Expr {
                expr: col("g"),
                alias: None,
            },
            agg_target("count", Expression::Star),
            agg_target("count", col("v")),
            agg_target("sum", col("v")),
            agg_target("min", col("v")),
            agg_target("max", col("v")),
            agg_target("avg", col("v")),
        ];
        let slots = collect_aggregate_slots(&targets, None);
        let group_exprs = vec![col("g")];
        let projection = plomid_executor::projection::required_columns(
            &schema,
            &targets,
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let engine = session.engine_mut();
        let out = par_scan_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &projection,
            None,
            &slots,
            &group_exprs,
            &targets,
            None,
            "db",
            "user",
            4,
        )
        .expect("parallel scan grouped")
        .expect("fits the materialize cap");
        let QueryResult::Rows { rows, .. } = out else {
            panic!("expected rows");
        };
        assert_eq!(sorted_text(rows), sorted_text(grouped_sql));

        let global_targets = vec![
            agg_target("count", Expression::Star),
            agg_target("count", col("v")),
            agg_target("sum", col("v")),
            agg_target("min", col("v")),
            agg_target("max", col("v")),
            agg_target("avg", col("v")),
        ];
        let global_slots = collect_aggregate_slots(&global_targets, None);
        let global_projection = plomid_executor::projection::required_columns(
            &schema,
            &global_targets,
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let engine = session.engine_mut();
        let out = par_scan_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &global_projection,
            None,
            &global_slots,
            &[],
            &global_targets,
            None,
            "db",
            "user",
            4,
        )
        .expect("parallel scan global")
        .expect("fits the materialize cap");
        let QueryResult::Rows { rows, .. } = out else {
            panic!("expected rows");
        };
        assert_eq!(rows, global_sql);

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    /// A per-row failure (here: predicate over a missing column) is captured
    /// by the worker, stops the scan cleanly, and surfaces as the query
    /// error — exactly like the sequential path. Completing at all proves
    /// the producer was not stranded (no deadlock on the error path).
    #[test]
    fn par_streaming_agg_error_matches_sequential() {
        use plomid_executor::executor::Executor;
        use std::sync::atomic::{AtomicU64, Ordering};

        static NEXT: AtomicU64 = AtomicU64::new(1000);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("plomid-paragg-err-{id}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
        let wal = root.with_extension("wal");
        let engine = plomid_txn::PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
        let mut session = Executor::new(engine).expect("executor");
        session
            .execute("CREATE TABLE pe (g INTEGER, v INTEGER);")
            .unwrap();
        let mut batch = String::from("INSERT INTO pe VALUES ");
        for i in 0..2500 {
            if i > 0 {
                batch.push_str(", ");
            }
            batch.push_str(&format!("({}, {})", i % 7, i));
        }
        batch.push(';');
        session.execute(&batch).unwrap();

        // Sequential SQL errors on the unknown column.
        assert!(session
            .execute("SELECT COUNT(*) FROM pe WHERE missing = 1;")
            .is_err());

        let qualified = session.catalog().resolve_table_name("pe").expect("resolve");
        let schema = session
            .catalog()
            .get_table(&qualified)
            .expect("table")
            .clone();
        let scan_start = format!("{qualified}:").into_bytes();
        let scan_end = format!("{qualified}:\u{10FFFF}").into_bytes();
        let targets = vec![agg_target("count", Expression::Star)];
        let slots = collect_aggregate_slots(&targets, None);
        let projection = plomid_executor::projection::required_columns(
            &schema,
            &targets,
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_or_else(|| plomid_executor::projection::ColumnMask::all(schema.columns.len()));
        let pred = Expression::Equal(
            Box::new(Expression::ColumnRef("missing".to_string())),
            Box::new(Expression::Literal(Value::Int4(1))),
        );
        let engine = session.engine_mut();
        let out = par_streaming_agg(
            engine,
            &schema,
            &scan_start,
            &scan_end,
            &projection,
            Some(&pred),
            &slots,
            &[],
            &targets,
            None,
            "db",
            "user",
            2500,
            4,
        );
        assert!(out.is_err(), "worker predicate failure must surface");

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }

    /// Two-column `(g, v)` integer schema for merge-equivalence checks.
    fn merge_schema() -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        let column = |name: &str| ColumnDef {
            name: name.to_string(),
            col_type: ColumnType::int4(),
            constraints: Vec::new(),
        };
        catalog
            .create_table(
                "public.u".to_string(),
                vec![column("g"), column("v")],
                Vec::new(),
            )
            .expect("create table");
        catalog.get_table("public.u").expect("get table").clone()
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

    fn sorted_text(rows: Vec<Vec<Value>>) -> Vec<String> {
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
        out
    }

    /// Split-feed + `merge_from` must equal the single-threaded fold,
    /// including `AVG` (`NUMERIC` division at finalize), `COUNT DISTINCT`
    /// set union, NULL group keys, and an EMPTY partition (undecided plan).
    /// Runs once over the fast single-column path and once over the legacy
    /// multi-key path.
    #[test]
    fn merge_from_matches_sequential_feed() {
        let schema = merge_schema();
        // Deterministic rows: NULL group every 11th, NULL value every 5th.
        let mut state = 0x1234_5678_9abcu64;
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for i in 0..5000i64 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let g = if i % 11 == 0 {
                Value::Null
            } else {
                Value::Int4((i % 7) as i32)
            };
            let v = if i % 5 == 0 {
                Value::Null
            } else {
                Value::Int4((state % 2001) as i32 - 1000)
            };
            rows.push(vec![g, v]);
        }
        for group_exprs in [vec![col("g")], vec![col("g"), col("v")], Vec::new()] {
            let targets = if group_exprs.is_empty() {
                vec![
                    agg_target("count", Expression::Star),
                    agg_target("count", col("v")),
                    agg_target("sum", col("v")),
                    agg_target("avg", col("v")),
                    agg_target("min", col("v")),
                    agg_target("max", col("v")),
                ]
            } else {
                let mut targets = vec![SelectTarget::Expr {
                    expr: group_exprs[0].clone(),
                    alias: None,
                }];
                targets.push(agg_target("count", Expression::Star));
                targets.push(agg_target("count", col("v")));
                targets.push(agg_target("sum", col("v")));
                targets.push(agg_target("avg", col("v")));
                targets.push(agg_target("min", col("v")));
                targets.push(agg_target("max", col("v")));
                targets.push(SelectTarget::Expr {
                    expr: Expression::FunctionCall {
                        name: "count".to_string(),
                        args: vec![col("v")],
                        distinct: true,
                        filter: None,
                        order_by: Vec::new(),
                        returning: None,
                        null_handling: None,
                        unique_keys: None,
                    },
                    alias: None,
                });
                targets
            };
            let slots = collect_aggregate_slots(&targets, None);
            let mut sequential = StreamingGroupedAgg::new(&slots, &group_exprs);
            for row in &rows {
                sequential.feed(row, &schema).expect("feed");
            }
            let sequential_rows = match sequential
                .finish(&schema, &targets, None, "db", "user")
                .expect("finish")
            {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected rows, got {other:?}"),
            };
            // Four partitions, one of them empty (undecided plan merge).
            let bounds = [0usize, 1000, 1000, 3500, 5000];
            let mut merged: Option<StreamingGroupedAgg> = None;
            for window in bounds.windows(2) {
                let mut part = StreamingGroupedAgg::new(&slots, &group_exprs);
                for row in &rows[window[0]..window[1]] {
                    part.feed(row, &schema).expect("feed");
                }
                if let Some(acc) = merged.as_mut() {
                    acc.merge_from(part);
                } else {
                    merged = Some(part);
                }
            }
            let merged_rows = match merged
                .expect("partial")
                .finish(&schema, &targets, None, "db", "user")
                .expect("finish")
            {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected rows, got {other:?}"),
            };
            assert_eq!(
                sorted_text(merged_rows),
                sorted_text(sequential_rows),
                "group_exprs={group_exprs:?}"
            );
        }
    }

    /// The group shuffle preserves exact results: drain every partition into
    /// owner buckets, merge per owner in worker order, concat owners, finish.
    /// Covers fast single-key, legacy multi-key, and global shapes.
    #[test]
    fn group_shuffle_matches_sequential_feed() {
        let schema = merge_schema();
        let mut state = 0x1234_5678_9abcu64;
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for i in 0..5000i64 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let g = if i % 11 == 0 {
                Value::Null
            } else {
                Value::Int4((i % 7) as i32)
            };
            let v = if i % 5 == 0 {
                Value::Null
            } else {
                Value::Int4((state % 2001) as i32 - 1000)
            };
            rows.push(vec![g, v]);
        }
        for group_exprs in [vec![col("g")], vec![col("g"), col("v")], Vec::new()] {
            let targets = if group_exprs.is_empty() {
                vec![
                    agg_target("count", Expression::Star),
                    agg_target("count", col("v")),
                    agg_target("sum", col("v")),
                    agg_target("avg", col("v")),
                    agg_target("min", col("v")),
                    agg_target("max", col("v")),
                ]
            } else {
                let mut targets = vec![SelectTarget::Expr {
                    expr: group_exprs[0].clone(),
                    alias: None,
                }];
                targets.push(agg_target("count", Expression::Star));
                targets.push(agg_target("count", col("v")));
                targets.push(agg_target("sum", col("v")));
                targets.push(agg_target("avg", col("v")));
                targets.push(agg_target("min", col("v")));
                targets.push(agg_target("max", col("v")));
                targets
            };
            let slots = collect_aggregate_slots(&targets, None);
            let mut sequential = StreamingGroupedAgg::new(&slots, &group_exprs);
            for row in &rows {
                sequential.feed(row, &schema).expect("feed");
            }
            let sequential_rows = match sequential
                .finish(&schema, &targets, None, "db", "user")
                .expect("finish")
            {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected rows, got {other:?}"),
            };
            // Scope 1: four row partitions (one empty).
            let owners = 4;
            let bounds = [0usize, 1000, 1000, 3500, 5000];
            let mut partials = Vec::new();
            for window in bounds.windows(2) {
                let mut part = StreamingGroupedAgg::new(&slots, &group_exprs);
                for row in &rows[window[0]..window[1]] {
                    part.feed(row, &schema).expect("feed");
                }
                partials.push(part);
            }
            // Scope 2+3: shuffle by key hash, merge per owner in worker order.
            let mut per_owner: Vec<Vec<StreamingGroupedAgg>> =
                (0..owners).map(|_| Vec::new()).collect();
            for mut part in partials {
                for (owner, bucket) in per_owner.iter_mut().enumerate() {
                    bucket.push(part.drain_for_owner(owner, owners));
                }
            }
            let mut final_agg: Option<StreamingGroupedAgg> = None;
            for parts in per_owner {
                let mut acc: Option<StreamingGroupedAgg> = None;
                for part in parts {
                    if let Some(a) = acc.as_mut() {
                        a.merge_from(part);
                    } else {
                        acc = Some(part);
                    }
                }
                if let Some(a) = final_agg.as_mut() {
                    a.merge_from(acc.expect("owner partial"));
                } else {
                    final_agg = acc;
                }
            }
            let shuffled_rows = match final_agg
                .expect("partial")
                .finish(&schema, &targets, None, "db", "user")
                .expect("finish")
            {
                QueryResult::Rows { rows, .. } => rows,
                other => panic!("expected rows, got {other:?}"),
            };
            assert_eq!(
                sorted_text(shuffled_rows),
                sorted_text(sequential_rows),
                "group_exprs={group_exprs:?}"
            );
        }
    }
}

mod sort_spill_tests {

    use plomid_executor::query::expr::evaluate_expression;
    use plomid_executor::query::group::{compare_order_keys, sort_rows};

    use plomid_executor::query::select::*;

    use plomid_sql::Expression;

    use plomid_sql::TableSchema;
    use plomid_sql::Value;
    use plomid_sql::{Catalog, ColumnDef, ColumnType, InMemoryCatalog, OrderByItem};

    /// Serializes the spill tests: they share the process temp dir, so the
    /// cleanup-count assertion needs no sibling spill in flight.
    static SPILL_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn col_type(name: &str) -> ColumnType {
        ColumnType::from_type_name(name).expect("known test type")
    }

    fn spill_schema() -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .create_table(
                "public.t".to_string(),
                [("id", "bigint"), ("a", "integer"), ("b", "text")]
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

    fn key(name: &str, descending: bool) -> OrderByItem {
        OrderByItem {
            expr: Expression::ColumnRef(name.to_string()),
            descending,
            nulls_first: None,
        }
    }

    /// Deterministic mixed-type rows: unique `id`, heavy ties in `a`, NULLs,
    /// texts, and float-adjacent integers. Shuffled so input is unsorted.
    /// (No NaN: NaN never equals itself, so no exact-equality assertion could
    /// hold for it on any path.)
    fn mixed_rows(n: i64) -> Vec<Vec<Value>> {
        let mut state = 0x51ab_1e5du64;
        let mut ids: Vec<i64> = (0..n).collect();
        for i in (1..ids.len()).rev() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ids.swap(i, (state % (i as u64 + 1)) as usize);
        }
        ids.into_iter()
            .map(|id| {
                let a = if id % 17 == 0 {
                    Value::Null
                } else {
                    Value::Int4((id % 50) as i32)
                };
                let b = if id % 23 == 0 {
                    Value::Null
                } else {
                    Value::Text(format!("s{}", id % 11))
                };
                vec![Value::Int8(id), a, b]
            })
            .collect()
    }

    fn spill_dir_count() -> usize {
        std::fs::read_dir(std::env::temp_dir())
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("plomid-sort-spill-")
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    #[test]
    fn under_budget_delegates_to_sort_rows_byte_identical() {
        let _serial = SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let shapes: Vec<(Vec<OrderByItem>, Option<usize>)> = vec![
            (vec![key("id", false)], None),
            (vec![key("id", true)], None),
            (vec![key("a", false), key("id", false)], None),
            (vec![key("b", true), key("id", false)], Some(7)),
            (vec![key("a", false)], Some(0)),
        ];
        let rows = mixed_rows(200);
        for (order_by, top_k) in &shapes {
            // At exactly the input length the in-memory path is taken.
            let expect = sort_rows(rows.clone(), &schema, order_by, *top_k).expect("sequential");
            let got = sort_rows_spilled(rows.clone(), &schema, order_by, *top_k, rows.len())
                .expect("spilled");
            assert_eq!(
                got, expect,
                "under budget must delegate: {order_by:?} {top_k:?}"
            );
        }
    }

    #[test]
    fn over_budget_spill_matches_sort_rows_on_total_orders() {
        let _serial = SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        // Every shape ends in unique `id`: no ties, so the merge must equal
        // the sequential stable sort entry-for-entry.
        let shapes: Vec<(Vec<OrderByItem>, Option<usize>)> = vec![
            (vec![key("id", false)], None),
            (vec![key("id", true)], None),
            (vec![key("a", false), key("id", false)], None),
            (
                vec![key("a", true), key("b", false), key("id", false)],
                None,
            ),
            (vec![key("b", false), key("id", true)], Some(7)),
            (vec![key("id", false)], Some(1)),
            (vec![key("a", false), key("id", false)], Some(199)),
            (vec![key("id", false)], Some(50_000)),
        ];
        for (order_by, top_k) in &shapes {
            let rows = mixed_rows(200);
            let expect = sort_rows(rows.clone(), &schema, order_by, *top_k).expect("sequential");
            // Tiny budgets force many runs (200 rows / 16 = 13 runs; a zero
            // budget still moves forward one row at a time).
            for budget in [16usize, 1, 0] {
                let got = sort_rows_spilled(rows.clone(), &schema, order_by, *top_k, budget)
                    .expect("spilled");
                assert_eq!(
                    got, expect,
                    "spill must equal sequential: {order_by:?} {top_k:?} budget={budget}"
                );
            }
        }
    }

    #[test]
    fn over_budget_ties_stay_key_ordered_and_deterministic() {
        let _serial = SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let order_by = vec![key("a", false)];
        let rows = mixed_rows(200);
        let first = sort_rows_spilled(rows.clone(), &schema, &order_by, None, 16).expect("run 1");
        let second = sort_rows_spilled(rows, &schema, &order_by, None, 16).expect("run 2");
        assert_eq!(first, second, "spill merge must be deterministic");
        assert_eq!(first.len(), 200);
        let keys: Vec<Vec<Value>> = first
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
                .all(|w| compare_order_keys(&w[0], &w[1], &order_by) != std::cmp::Ordering::Greater),
            "spill output sorted"
        );
    }

    #[test]
    fn spill_error_matches_sequential_row_order() {
        let _serial = SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let order_by = vec![
            OrderByItem {
                expr: Expression::ColumnRef("nope".to_string()),
                descending: false,
                nulls_first: None,
            },
            key("id", false),
        ];
        let rows = mixed_rows(100);
        let seq_err = sort_rows(rows.clone(), &schema, &order_by, None).expect_err("must fail");
        let spill_err =
            sort_rows_spilled(rows, &schema, &order_by, None, 16).expect_err("must fail");
        assert_eq!(format!("{seq_err}"), format!("{spill_err}"));
    }

    #[test]
    fn spill_cleans_up_temp_dirs() {
        let _serial = SPILL_TEST_SERIAL.lock().expect("spill test lock");
        let schema = spill_schema();
        let order_by = vec![key("id", false)];
        let before = spill_dir_count();
        let rows = mixed_rows(120);
        let got = sort_rows_spilled(rows.clone(), &schema, &order_by, None, 16).expect("spilled");
        let expect = sort_rows(rows, &schema, &order_by, None).expect("sequential");
        assert_eq!(got, expect);
        assert_eq!(
            spill_dir_count(),
            before,
            "sort spill must remove its temp dir"
        );
    }
}

mod topk_gate_tests {

    use plomid_executor::query::group::par_sort_has_costly_keys;

    use plomid_executor::query::select::*;

    use plomid_sql::ColumnType;
    use plomid_sql::Expression;

    use plomid_sql::TableSchema;
    use plomid_sql::Value;
    use plomid_sql::{
        Catalog, ColumnDef, Constraint, ConstraintKind, InMemoryCatalog, OrderByItem,
    };

    fn col_type(name: &str) -> ColumnType {
        ColumnType::from_type_name(name).expect("known test type")
    }

    /// PK schema so the total-order gate can prove coverage (`id` PK).
    fn pk_schema() -> TableSchema {
        let mut catalog = InMemoryCatalog::new();
        catalog
            .create_table(
                "public.t".to_string(),
                [("id", "bigint"), ("a", "integer"), ("v", "bigint")]
                    .into_iter()
                    .map(|(name, ty)| ColumnDef {
                        name: (*name).to_string(),
                        col_type: col_type(ty),
                        constraints: Vec::new(),
                    })
                    .collect(),
                vec![Constraint::new(
                    ConstraintKind::PrimaryKey,
                    vec!["id".to_string()],
                )],
            )
            .expect("create table");
        catalog.get_table("public.t").expect("get table").clone()
    }

    fn key(name: &str) -> OrderByItem {
        OrderByItem {
            expr: Expression::ColumnRef(name.to_string()),
            descending: false,
            nulls_first: None,
        }
    }

    fn expr_key(expr: Expression) -> OrderByItem {
        OrderByItem {
            expr,
            descending: false,
            nulls_first: None,
        }
    }

    fn add_key() -> OrderByItem {
        // `v + 1`: real per-row evaluation over a plain column.
        expr_key(Expression::Add(
            Box::new(Expression::ColumnRef("v".to_string())),
            Box::new(Expression::Literal(Value::Int8(1))),
        ))
    }

    fn random_key() -> OrderByItem {
        expr_key(Expression::FunctionCall {
            name: "random".to_string(),
            args: Vec::new(),
            distinct: false,
            filter: None,
            order_by: Vec::new(),
            returning: None,
            null_handling: None,
            unique_keys: None,
        })
    }

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(name.to_string())
    }

    fn pin_laptop() -> plomid_executor::parallel_budget::TestTierGuard {
        plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        )
    }

    #[test]
    fn costly_keys_rule() {
        let schema = pk_schema();
        let _ = &schema;
        assert!(!par_sort_has_costly_keys(&[key("id")]));
        assert!(!par_sort_has_costly_keys(&[key("id"), key("a")]));
        assert!(!par_sort_has_costly_keys(&[expr_key(Expression::Literal(
            Value::Int8(1)
        ))]));
        assert!(par_sort_has_costly_keys(&[add_key(), key("id")]));
        assert!(par_sort_has_costly_keys(&[random_key(), key("id")]));
        assert!(!par_sort_has_costly_keys(&[]));
    }

    #[test]
    fn topk_gate_matrix() {
        // Tier-sensitive: pin Laptop so the threshold is the historical
        // 50_000 on every host class.
        let _tier = pin_laptop();
        let schema = pk_schema();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let parallel_possible = cpus >= 2;
        // Cheap colref keys over a total order: never parallel (fan-out
        // cannot amortize), even far above threshold.
        assert_eq!(
            par_topk_workers_for(&schema, &[key("id")], None, 10_000_000),
            None,
            "cheap keys must stay sequential at any scale"
        );
        // Costly expression keys + PK tiebreak: total and safe, but below
        // the 1M floor (measured loss at 100k/500k) -> sequential.
        assert_eq!(
            par_topk_workers_for(&schema, &[add_key(), key("id")], None, 100_000),
            None,
            "measured loss regime must stay sequential"
        );
        assert_eq!(
            par_topk_workers_for(&schema, &[add_key(), key("id")], None, 500_000),
            None,
            "measured loss regime must stay sequential"
        );
        // At/above the floor -> engages exactly when workers exist.
        assert_eq!(
            par_topk_workers_for(&schema, &[add_key(), key("id")], None, 1_000_000).is_some(),
            parallel_possible,
        );
        // Below threshold: sequential.
        assert_eq!(
            par_topk_workers_for(&schema, &[add_key(), key("id")], None, 49_999),
            None,
        );
        // Non-total order (no PK coverage): sequential.
        assert_eq!(
            par_topk_workers_for(&schema, &[key("a")], None, 1_000_000),
            None,
        );
        // Volatile keys: sequential.
        assert_eq!(
            par_topk_workers_for(&schema, &[random_key(), key("id")], None, 1_000_000),
            None,
        );
        // Statement-context WHERE (session id resolves from TLS workers do
        // not inherit): sequential.
        let ctx_where = Expression::Equal(
            Box::new(col("a")),
            Box::new(Expression::ColumnRef("current_user".to_string())),
        );
        assert_eq!(
            par_topk_workers_for(
                &schema,
                &[add_key(), key("id")],
                Some(&ctx_where),
                1_000_000
            ),
            None,
        );
        // Plain predicate WHERE: engages like the predicateless shape.
        let plain_where = Expression::Greater(
            Box::new(col("v")),
            Box::new(Expression::Literal(Value::Int8(10))),
        );
        assert_eq!(
            par_topk_workers_for(
                &schema,
                &[add_key(), key("id")],
                Some(&plain_where),
                1_000_000
            )
            .is_some(),
            parallel_possible,
        );
    }
}

mod vector_scan_engage_tests {

    use plomid_executor::executor::Executor;

    use plomid_sql::Catalog;

    use plomid_sql::Expression;

    use plomid_sql::QueryResult;

    use plomid_sql::Value;
    use plomid_txn::StorageEngine;

    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch_engine() -> (
        Executor<plomid_txn::PlomidStorageEngine>,
        std::path::PathBuf,
    ) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("plomid-vscan-engage-{id}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
        let wal = root.with_extension("wal");
        let engine = plomid_txn::PlomidStorageEngine::create(&root, &wal, 32).expect("engine");
        (Executor::new(engine).expect("executor"), root)
    }

    fn col(name: &str) -> Expression {
        Expression::ColumnRef(name.to_string())
    }

    fn lit(v: i64) -> Expression {
        Expression::Literal(Value::Int8(v))
    }

    /// Drives `try_vector_filter_scan` directly over a VACUUMed table:
    /// prunable shapes must engage (`Some` with the SQL row count) and
    /// partially-prunable shapes must decline (`None`, scalar fallback).
    #[test]
    fn scan_engages_prunable_and_declines_partial() {
        let (mut session, root) = scratch_engine();
        session
            .execute("CREATE TABLE s (id BIGINT PRIMARY KEY, a INT, v BIGINT);")
            .unwrap();
        let mut stmt = String::from("INSERT INTO s VALUES ");
        for i in 0..2000 {
            if i > 0 {
                stmt.push(',');
            }
            let a = if i % 17 == 0 {
                "NULL".to_string()
            } else {
                format!("{}", i % 50)
            };
            stmt.push_str(&format!("({i},{a},{i})"));
        }
        stmt.push(';');
        session.execute(&stmt).unwrap();
        session.execute("VACUUM s;").unwrap();

        let qualified = session.catalog().resolve_table_name("s").expect("resolve");
        let schema = session
            .catalog()
            .get_table(&qualified)
            .expect("table")
            .clone();
        let mut pos_columns = BTreeMap::new();
        for (index, column) in schema.columns.iter().enumerate() {
            pos_columns.insert(
                column.name.clone(),
                plomid_core::ColumnId::new(index as u64),
            );
        }
        let requested = vec![plomid_core::ColumnId::new(0), plomid_core::ColumnId::new(2)];
        let object = plomid_core::ObjectId::new(schema.table_id.get());
        let engine_root = session.engine_mut().root().to_path_buf();
        let store = plomid_columnar::ColumnarStore::open(&engine_root).expect("store");

        // Prunable equality: engages with exactly the SQL match count.
        let where_eq = Expression::Equal(Box::new(col("a")), Box::new(lit(7)));
        let engine = session.engine_mut();
        let scan = plomid_executor::query::vector::try_vector_filter_scan(
            engine,
            &schema,
            &where_eq,
            &requested,
            None,
            &pos_columns,
            &store,
            object,
        )
        .expect("scan")
        .expect("prunable WHERE must engage the vector scan");
        assert_eq!(scan.column_ids, requested);
        let expect = match session.execute("SELECT id, v FROM s WHERE a = 7;").unwrap() {
            QueryResult::Rows { rows, .. } => rows.len(),
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(scan.rows.len(), expect);
        assert!(expect > 0);

        // Partial conjunction (opaque arithmetic branch): declines.
        let partial = Expression::And(
            Box::new(where_eq),
            Box::new(Expression::Greater(
                Box::new(col("v")),
                Box::new(Expression::Add(Box::new(col("v")), Box::new(lit(1)))),
            )),
        );
        let engine = session.engine_mut();
        let declined = plomid_executor::query::vector::try_vector_filter_scan(
            engine,
            &schema,
            &partial,
            &requested,
            None,
            &pos_columns,
            &store,
            object,
        )
        .expect("decline is Ok(None)");
        assert!(declined.is_none(), "partial AND must decline to scalar");
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(root.with_extension("wal"));
    }
}
