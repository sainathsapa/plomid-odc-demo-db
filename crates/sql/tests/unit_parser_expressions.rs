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
// Moved from `src/parser/expressions.rs` inline `#[cfg(test)]` — public-API only.
mod tests {

    use plomid_sql::{Expression, InMemoryCatalog, Lexer, ParseError, Parser, Value};

    fn parse(sql: &str) -> Expression {
        let tokens = Lexer::new(sql).lex().unwrap();
        let mut catalog = InMemoryCatalog::new();
        let mut parser = Parser::new(tokens, &mut catalog);
        parser.parse_expression().unwrap()
    }

    fn parse_result(sql: &str) -> Result<Expression, ParseError> {
        let tokens = Lexer::new(sql).lex().unwrap();
        let mut catalog = InMemoryCatalog::new();
        let mut parser = Parser::new(tokens, &mut catalog);
        parser.parse_expression()
    }

    #[test]
    fn comparison_operators_parse() {
        for sql in [
            "a = 1", "a != 1", "a <> 1", "a > 1", "a >= 1", "a < 1", "a <= 1",
        ] {
            let expr = parse(sql);
            assert!(
                matches!(
                    expr,
                    Expression::Equal(_, _)
                        | Expression::NotEqual(_, _)
                        | Expression::Less(_, _)
                        | Expression::LessOrEqual(_, _)
                        | Expression::Greater(_, _)
                        | Expression::GreaterOrEqual(_, _)
                ),
                "{sql}: {expr:?}"
            );
        }
    }

    #[test]
    fn json_type_cast_preserves_full_type_name() {
        let expr = parse("'{\"a\":1}'::json");
        assert!(matches!(expr, Expression::TypeCast { type_name, .. } if type_name == "json"));
        let expr = parse("'{\"a\":1}'::jsonb");
        assert!(matches!(expr, Expression::TypeCast { type_name, .. } if type_name == "jsonb"));
    }

    #[test]
    fn multiword_type_cast_preserves_full_type_name() {
        // `::` postfix cast.
        let cases = [
            ("'3.14'::double precision", "double precision"),
            ("'x'::character varying", "character varying"),
            ("'x'::bit varying", "bit varying"),
            (
                "'2024-01-01 12:00:00'::timestamp with time zone",
                "timestamp with time zone",
            ),
            (
                "'2024-01-01 12:00:00'::timestamp without time zone",
                "timestamp without time zone",
            ),
            ("'12:00:00'::time with time zone", "time with time zone"),
            (
                "'12:00:00'::time without time zone",
                "time without time zone",
            ),
            ("'abc'::character varying(20)", "character varying(20)"),
            ("1::numeric(10,2)", "numeric(10,2)"),
        ];
        for (sql, expected) in cases {
            let expr = parse(sql);
            assert!(
                matches!(expr, Expression::TypeCast { ref type_name, .. } if type_name == expected),
                "{sql}: expected type_name = {expected:?}, got {expr:?}"
            );
        }
    }

    #[test]
    fn cast_keyword_accepts_multiword_types() {
        let expr = parse("CAST('3.14' AS double precision)");
        assert!(
            matches!(expr, Expression::Cast { ref type_name, .. } if type_name == "double precision"),
            "got {expr:?}"
        );
        let expr = parse("CAST('2024-01-01' AS timestamp with time zone)");
        assert!(
            matches!(expr, Expression::Cast { ref type_name, .. } if type_name == "timestamp with time zone"),
            "got {expr:?}"
        );
    }

    #[test]
    fn temporal_typed_literal_precision_parses() {
        // Parameterized temporal typed literals carry their precision through
        // to the AST (parser -> AST), where it must not be discarded.
        let expr = parse("TIME(3) '12:34:56.123456'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimeLiteral { ref text, ref precision }
                    if *precision == 3 && text == "12:34:56.123456"
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMP(0) '2026-01-15 12:34:56.123456'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimestampLiteral { ref text, ref precision }
                    if *precision == 0 && text == "2026-01-15 12:34:56.123456"
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMP(6) '2026-01-15 12:34:56.123456'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimestampLiteral { ref precision, .. } if *precision == 6
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMPTZ(3) '2026-01-15 12:34:56.123456+00'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimestamptzLiteral { ref precision, .. } if *precision == 3
            ),
            "got {expr:?}"
        );
        // Precision also attaches to the multi-word timestamp-with-time-zone
        // form in either position.
        let expr = parse("TIMESTAMP WITH TIME ZONE(3) '2026-01-15 12:34:56+00'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimestamptzLiteral { ref precision, .. } if *precision == 3
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMP(3) WITH TIME ZONE '2026-01-15 12:34:56+00'");
        assert!(
            matches!(
                expr,
                Expression::TypedTimestamptzLiteral { ref precision, .. } if *precision == 3
            ),
            "got {expr:?}"
        );
    }

    #[test]
    fn temporal_typed_literal_unparameterized_still_parses() {
        // Backward compatibility: the unparameterized forms must keep mapping
        // to their existing AST variants.
        let expr = parse("TIME '12:34:56'");
        assert!(
            matches!(expr, Expression::TimeLiteral(ref text) if text == "12:34:56"),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMP '2026-01-15 12:34:56'");
        assert!(
            matches!(
                expr,
                Expression::TimestampLiteral(ref text) if text == "2026-01-15 12:34:56"
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMPTZ '2026-01-15 12:34:56+00'");
        assert!(
            matches!(
                expr,
                Expression::TimestamptzLiteral(ref text)
                    if text == "2026-01-15 12:34:56+00"
            ),
            "got {expr:?}"
        );
        let expr = parse("TIMESTAMP WITH TIME ZONE '2026-01-15 12:34:56+00'");
        assert!(
            matches!(
                expr,
                Expression::TimestamptzLiteral(ref text)
                    if text == "2026-01-15 12:34:56+00"
            ),
            "got {expr:?}"
        );
    }

    #[test]
    fn temporal_typed_literal_invalid_precision_rejected() {
        // Out-of-range and malformed precision must be rejected, not clamped.
        for sql in [
            "TIME(7) '12:34:56'",
            "TIME(-1) '12:34:56'",
            "TIMESTAMP(7) '2026-01-15 12:34:56'",
            "TIMESTAMP(-1) '2026-01-15 12:34:56'",
            "TIMESTAMPTZ(7) '2026-01-15 12:34:56+00'",
            "TIMESTAMPTZ(-1) '2026-01-15 12:34:56+00'",
        ] {
            assert!(parse_result(sql).is_err(), "expected parse error for {sql}");
        }
    }

    #[test]
    fn boolean_precedence_groups_correctly() {
        // NOT binds tighter than AND, AND tighter than OR.
        let expr = parse("a OR b AND NOT c");
        assert!(matches!(expr, Expression::Or(_, _)));
    }

    #[test]
    fn arithmetic_precedence_groups_correctly() {
        // 1 + 2 * 3 must group as 1 + (2 * 3).
        let expr = parse("1 + 2 * 3");
        match expr {
            Expression::Add(_, right) => assert!(matches!(*right, Expression::Multiply(_, _))),
            other => panic!("unexpected shape: {other:?}"),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        // (1 + 2) * 3 must group as (1 + 2) * 3.
        let expr = parse("(1 + 2) * 3");
        match expr {
            Expression::Multiply(left, _) => assert!(matches!(*left, Expression::Add(_, _))),
            other => panic!("unexpected shape: {other:?}"),
        }
    }

    #[test]
    fn is_null_parses() {
        assert!(matches!(parse("name IS NULL"), Expression::IsNull(_)));
        assert!(matches!(
            parse("name IS NOT NULL"),
            Expression::IsNotNull(_)
        ));
    }

    #[test]
    fn function_call_parses() {
        let expr = parse("count(*)");
        assert!(matches!(
            expr,
                        Expression::FunctionCall { name, args, distinct: false, filter: None, order_by: _, returning: _, null_handling: _, unique_keys: _ } if name == "count" && args == vec![Expression::Star]
        ));
        let expr = parse("nextval('seq')");
        assert!(matches!(
            expr,
                        Expression::FunctionCall { name, args, distinct: false, filter: None, order_by: _, returning: _, null_handling: _, unique_keys: _ } if name == "nextval" && args.len() == 1
        ));
    }

    #[test]
    fn negative_literal_expressions_parse() {
        for sql in ["-1", "-10", "-100", "-age", "-(age + 1)", "+age"] {
            let expr = parse(sql);
            match sql {
                "-1" => assert!(
                    matches!(expr, Expression::Negate(inner) if matches!(*inner, Expression::Literal(Value::Int4(1))))
                ),
                "-10" => assert!(
                    matches!(expr, Expression::Negate(inner) if matches!(*inner, Expression::Literal(Value::Int4(10))))
                ),
                "-100" => assert!(
                    matches!(expr, Expression::Negate(inner) if matches!(*inner, Expression::Literal(Value::Int4(100))))
                ),
                "-age" => assert!(
                    matches!(expr, Expression::Negate(inner) if matches!(*inner, Expression::ColumnRef(_)))
                ),
                "-(age + 1)" => assert!(
                    matches!(expr, Expression::Negate(inner) if matches!(*inner, Expression::Add(_, _)))
                ),
                "+age" => assert!(matches!(expr, Expression::ColumnRef(_))),
                _ => panic!("unexpected test case: {sql}"),
            }
        }
    }

    #[test]
    fn check_constraint_with_negative_boundary_parses() {
        let expr = parse("balance >= -100");
        match &expr {
            Expression::GreaterOrEqual(_, right) => {
                assert!(
                    matches!(&**right, Expression::Negate(inner) if matches!(&**inner, Expression::Literal(Value::Int4(100))))
                );
            }
            other => panic!("expected GreaterOrEqual with negated literal, got {other:?}"),
        }
    }

    #[test]
    fn position_with_in_keyword_parses() {
        // TR2.1: POSITION('@' IN VALUE) → FunctionCall with args ['@', ColumnRef("VALUE")]
        let expr = parse("POSITION('@' IN VALUE)");
        match expr {
            Expression::FunctionCall { name, args, .. } => {
                assert_eq!(name.to_lowercase(), "position");
                assert_eq!(args.len(), 2);
                // First arg is the substring literal '@'
                assert!(matches!(&args[0], Expression::Literal(Value::Text(t)) if t == "@"));
                // Second arg is VALUE treated as a column reference
                assert!(
                    matches!(&args[1], Expression::ColumnRef(ref c) if c == "VALUE"),
                    "expected ColumnRef(\"VALUE\"), got {:?}",
                    args[1]
                );
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }
    }

    #[test]
    fn vector_literal_parses_dense() {
        // Canonical dense form with whitespace.
        let expr = parse("[0.12,0.34,0.56,0.78]");
        let Expression::VectorLiteral(lanes) = expr else {
            panic!("expected VectorLiteral");
        };
        assert_eq!(lanes.len(), 4);
        // Signs, exponents and surrounding whitespace are accepted.
        let expr = parse(" [ -0.1 , +2e3 , 1.5E-2 ] ");
        let Expression::VectorLiteral(lanes) = expr else {
            panic!("expected VectorLiteral");
        };
        assert_eq!(lanes, vec![-0.1f32, 2000.0, 0.015]);
    }

    #[test]
    fn vector_literal_malformed_rejected() {
        // Empty, trailing-comma, missing-comma, non-numeric, non-finite,
        // nested and unterminated shapes all fail as VECTOR_INVALID_LITERAL
        // (or a syntax error for the unterminated tail), never as TEXT.
        for sql in [
            "[]", "[1,2,]", "[1 2]", "[a]", "[1,NaN]", "[1,inf]", "[[1,2]]", "[1,]",
        ] {
            let err = parse_result(sql).expect_err(&format!("{sql} should fail"));
            assert!(
                err.to_string().contains("VECTOR_INVALID_LITERAL"),
                "{sql}: got {err}"
            );
        }
        assert!(parse_result("[1,2").is_err());
    }

    #[test]
    fn value_keyword_as_column_reference() {
        // TR2.3: x = VALUE should parse as a comparison
        let expr = parse("x = VALUE");
        assert!(matches!(expr, Expression::Equal(_, _)), "got {expr:?}");

        // TR2.3: VALUE used in arithmetic
        let expr = parse("VALUE + 1");
        assert!(matches!(expr, Expression::Add(_, _)), "got {expr:?}");
    }
    #[test]
    fn decimal_literal_spelling_decides_float_typing() {
        // P0-002 regression: a decimal/exponent spelling is a floating-point
        // literal even when its value is integral. Folding `3.0` to Int
        // reroutes `/` into integer division (`3.0 / 2.0` must be 1.5).
        for (sql, expected) in [
            ("3.0", 3.0f64),
            ("2.0", 2.0f64),
            ("1.50", 1.5f64),
            ("0.0", 0.0f64),
            ("1e3", 1000.0f64),
            ("2E-2", 0.02f64),
            ("1.5", 1.5f64),
        ] {
            match parse(sql) {
                Expression::Literal(Value::Float8(got)) => assert!(
                    (got - expected).abs() < f64::EPSILON,
                    "{sql}: expected Float8({expected}), got Float8({got})"
                ),
                other => panic!("{sql}: expected Float8 literal, got {other:?}"),
            }
        }
        // A leading minus parses as negation around the same Float8 literal.
        match parse("-3.0") {
            Expression::Negate(inner) => match *inner {
                Expression::Literal(Value::Float8(got)) => {
                    assert!((got - 3.0).abs() < f64::EPSILON, "-3.0: {got}")
                }
                other => panic!("-3.0: expected Float8 literal, got {other:?}"),
            },
            other => panic!("-3.0: expected Negate, got {other:?}"),
        }
        // Plain integers keep integer typing (integer division contract).
        match parse("3") {
            Expression::Literal(Value::Int4(3)) => {}
            other => panic!("3: expected Int4 literal, got {other:?}"),
        }
        match parse("-42") {
            Expression::Negate(inner) => match *inner {
                Expression::Literal(Value::Int4(42)) => {}
                other => panic!("-42: expected Int4 literal, got {other:?}"),
            },
            other => panic!("-42: expected Negate, got {other:?}"),
        }
        match parse("3000000000") {
            Expression::Literal(Value::Int8(_)) => {}
            other => panic!("large integer literal: got {other:?}"),
        }
    }

    #[test]
    fn like_carries_case_sensitivity_from_its_spelling() {
        // LIKE is case-sensitive, ILIKE folds case; the flag must survive
        // parsing (it was previously parsed and discarded, making ILIKE
        // silently case-sensitive).
        let flag_of = |sql: &str| match parse(sql) {
            Expression::Like {
                case_insensitive, ..
            } => case_insensitive,
            other => panic!("expected Like, got {other:?}"),
        };
        assert!(!flag_of("'ABC' LIKE 'abc'"));
        assert!(flag_of("'ABC' ILIKE 'abc'"));
        assert!(!flag_of("'ABC' NOT LIKE 'abc'"));
        assert!(flag_of("'ABC' NOT ILIKE 'abc'"));
        assert!(flag_of("'ABC' ILIKE 'a%' ESCAPE '!'"));
    }
}

mod quantified_comparison_tests {

    use plomid_sql::{
        ComparisonOperator, Expression, InMemoryCatalog, Lexer, Parser, Quantifier, SelectTarget,
        Statement,
    };

    fn parse_statement(sql: &str) -> Statement {
        let tokens = Lexer::new(sql).lex().unwrap();
        let mut catalog = InMemoryCatalog::new();
        let parser = Parser::new(tokens, &mut catalog);
        parser
            .parse_statements()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn any_with_greater_than_parses() {
        let stmt = parse_statement("SELECT 10 > ANY (SELECT 5);");
        match stmt {
            Statement::Select { targets, .. } => {
                assert_eq!(targets.len(), 1);
                let target = &targets[0];
                assert!(
                    matches!(target, SelectTarget::Expr { expr, .. } if matches!(expr, Expression::QuantifiedComparison { .. })),
                    "got {target:?}"
                );
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn all_with_less_than_parses() {
        let stmt = parse_statement("SELECT 10 < ALL (SELECT 20);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                assert!(
                    matches!(target, SelectTarget::Expr { expr, .. } if matches!(expr, Expression::QuantifiedComparison { .. })),
                    "got {target:?}"
                );
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn some_with_greater_than_parses() {
        let stmt = parse_statement("SELECT 10 > SOME (SELECT 5);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                assert!(
                    matches!(target, SelectTarget::Expr { expr, .. } if matches!(expr, Expression::QuantifiedComparison { .. })),
                    "got {target:?}"
                );
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn some_normalized_to_any() {
        let stmt = parse_statement("SELECT 10 > SOME (SELECT 5);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                if let SelectTarget::Expr { expr, .. } = target {
                    if let Expression::QuantifiedComparison { quantifier, .. } = expr {
                        assert_eq!(*quantifier, Quantifier::Any, "SOME should normalize to ANY");
                    } else {
                        panic!("expected QuantifiedComparison in expression");
                    }
                } else {
                    panic!("expected Expr target");
                }
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn all_with_greater_than_parses() {
        let stmt = parse_statement("SELECT 10 > ALL (SELECT 5);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                assert!(
                    matches!(target, SelectTarget::Expr { expr, .. } if matches!(expr, Expression::QuantifiedComparison { .. })),
                    "got {target:?}"
                );
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn not_equal_with_any_parses() {
        let stmt = parse_statement("SELECT 10 <> ANY (SELECT 5);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                if let SelectTarget::Expr { expr, .. } = target {
                    if let Expression::QuantifiedComparison { operator, .. } = expr {
                        assert_eq!(*operator, ComparisonOperator::NotEqual);
                    } else {
                        panic!("expected QuantifiedComparison in expression");
                    }
                } else {
                    panic!("expected Expr target");
                }
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn less_or_equal_with_all_parses() {
        let stmt = parse_statement("SELECT 10 <= ALL (SELECT 20);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                if let SelectTarget::Expr { expr, .. } = target {
                    if let Expression::QuantifiedComparison { operator, .. } = expr {
                        assert_eq!(*operator, ComparisonOperator::LessOrEqual);
                    } else {
                        panic!("expected QuantifiedComparison");
                    }
                } else {
                    panic!("expected Expr target");
                }
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn subquery_with_multiple_rows_parses() {
        // Test with a literal subquery instead of a table reference
        let stmt = parse_statement("SELECT price > ANY (SELECT 100);");
        match stmt {
            Statement::Select { targets, .. } => {
                let target = &targets[0];
                assert!(
                    matches!(target, SelectTarget::Expr { expr, .. } if matches!(expr, Expression::QuantifiedComparison { .. })),
                    "got {target:?}"
                );
            }
            other => panic!("expected SELECT statement, got {other:?}"),
        }
    }

    #[test]
    fn syntax_error_without_subquery() {
        let tokens = Lexer::new("SELECT 10 > ANY 5").lex().unwrap();
        let mut catalog = InMemoryCatalog::new();
        let parser = Parser::new(tokens, &mut catalog);
        let err = parser.parse_statements().unwrap_err();
        assert!(err.to_string().contains("syntax error"));
    }
}

mod value_tests {

    use plomid_sql::{InMemoryCatalog, Lexer, Parser, Value};

    fn parse_value_sql(sql: &str) -> Value {
        let tokens = Lexer::new(sql).lex().unwrap();
        let mut catalog = InMemoryCatalog::new();
        let mut parser = Parser::new(tokens, &mut catalog);
        parser.parse_value().unwrap()
    }

    #[test]
    fn negative_integer_values_parse() {
        assert_eq!(parse_value_sql("-1"), Value::Int4(-1));
        assert_eq!(parse_value_sql("-10"), Value::Int4(-10));
        assert_eq!(parse_value_sql("-100"), Value::Int4(-100));
        assert_eq!(parse_value_sql("+5"), Value::Int4(5));
    }
}

mod nesting_depth_tests {
    use plomid_sql::{InMemoryCatalog, Lexer, Parser};

    /// Parses on a 64MB-stack thread: in debug builds each precedence-descent
    /// level costs ~130KB of unoptimized frames (the giant `parse_primary_atom`
    /// match), so depths past ~15 overflow a default 2MB test thread before
    /// the 64-deep guard can fire. Release (production) frames are an order
    /// of magnitude smaller and the guard trips first there; the big stack
    /// makes these tests deterministic in both profiles.
    fn parse_result(sql: &str) -> Result<Vec<plomid_sql::Statement>, plomid_sql::ParseError> {
        let sql = sql.to_string();
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(move || {
                let tokens = Lexer::new(&sql).lex().unwrap();
                let mut catalog = InMemoryCatalog::new();
                Parser::new(tokens, &mut catalog).parse_statements()
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn deep_parens_rejected() {
        let ok = format!("SELECT {}1{};", "(".repeat(32), ")".repeat(32));
        assert!(parse_result(&ok).is_ok(), "32 parens must parse");
        let bad = format!("SELECT {}1{};", "(".repeat(300), ")".repeat(300));
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deep_not_chain_rejected() {
        let ok = format!("SELECT {}TRUE;", "NOT ".repeat(32));
        assert!(parse_result(&ok).is_ok(), "32 NOTs must parse");
        let bad = format!("SELECT {}TRUE;", "NOT ".repeat(300));
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deep_unary_chain_rejected() {
        let ok = format!("SELECT {}1;", "- ".repeat(32));
        assert!(parse_result(&ok).is_ok(), "32 unary minuses must parse");
        let bad = format!("SELECT {}1;", "- ".repeat(300));
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deep_power_chain_rejected() {
        let ok = format!("SELECT 1{};", "^1".repeat(32));
        assert!(parse_result(&ok).is_ok(), "32-deep power chain must parse");
        let bad = format!("SELECT 1{};", "^1".repeat(300));
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deep_subquery_nesting_rejected() {
        let ok = format!("SELECT {}1{};", "(SELECT ".repeat(10), ")".repeat(10));
        assert!(parse_result(&ok).is_ok(), "10-deep subqueries must parse");
        let bad = format!("SELECT {}1{};", "(SELECT ".repeat(300), ")".repeat(300));
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn deep_union_chain_rejected() {
        let ok = "SELECT 1 UNION SELECT 2 UNION SELECT 3;";
        assert!(parse_result(ok).is_ok());
        let bad = (0..300)
            .map(|i| format!("SELECT {i}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let err = parse_result(&bad).unwrap_err();
        assert!(
            err.to_string().contains("maximum depth"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reasonable_nesting_still_parses() {
        // Table-free (an empty catalog rejects unknown FROM tables before any
        // nesting question arises): mixed NOT/paren/argument/boolean/subquery
        // nesting a real query might carry.
        let sql = "SELECT NOT NOT ((1 + 2) * (3 - f(g(1)))) WHERE (1 = 1 AND (2 = 2 OR (3 = 3 AND 4 = 4))) AND EXISTS (SELECT 1);";
        assert!(parse_result(sql).is_ok());
    }
}
