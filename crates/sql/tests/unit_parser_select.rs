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
// Moved from `src/parser/select.rs` inline `#[cfg(test)]` — public-API only.
use plomid_sql::{Catalog, FromClause, InMemoryCatalog, JoinKind, Lexer, Parser, Statement};

fn parse(sql: &str) -> Statement {
    let tokens = Lexer::new(sql).lex().unwrap();
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![plomid_sql::ColumnDef {
                name: "id".to_string(),
                col_type: plomid_sql::ColumnType::int4(),
                constraints: Vec::new(),
            }],
            Vec::new(),
        )
        .unwrap();
    catalog
        .create_table(
            "orders".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "user_id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let parser = Parser::new(tokens, &mut catalog);
    parser.parse_statements().unwrap().remove(0)
}

#[test]
fn parses_order_by() {
    let Statement::Select { order_by, .. } = parse("SELECT * FROM users ORDER BY id DESC;") else {
        panic!("expected select");
    };
    assert_eq!(order_by.len(), 1);
    assert!(order_by[0].descending);
}

#[test]
fn parses_group_by_and_having() {
    let Statement::Select {
        group_by, having, ..
    } = parse("SELECT id, COUNT(*) FROM users GROUP BY id HAVING COUNT(*) > 1;")
    else {
        panic!("expected select");
    };
    assert!(group_by.is_some());
    assert!(having.is_some());
}

#[test]
fn parses_inner_join_with_aliases() {
    let sql = "SELECT u.id FROM users u INNER JOIN orders o ON u.id = o.user_id;";
    let stmt = parse(sql);
    let Statement::Select { from, .. } = stmt else {
        panic!("expected SELECT");
    };
    let Some(FromClause::Join { kind, on, .. }) = from else {
        panic!("expected FROM with JOIN");
    };
    assert_eq!(kind, JoinKind::Inner);
    assert!(on.is_some());
}

#[test]
fn parses_left_join() {
    let sql = "SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.user_id;";
    let stmt = parse(sql);
    let Statement::Select { from, .. } = stmt else {
        panic!("expected SELECT");
    };
    let Some(FromClause::Join { kind, .. }) = from else {
        panic!("expected FROM with JOIN");
    };
    assert_eq!(kind, JoinKind::Left);
}

#[test]
fn parses_set_operation() {
    let sql = "SELECT id FROM users UNION ALL SELECT id FROM users;";
    let stmt = parse(sql);
    assert!(matches!(
        stmt,
        Statement::SetOperation {
            op: plomid_sql::ast::SetOpKind::Union,
            all: true,
            ..
        }
    ));
}

#[test]
fn parses_plain_distinct() {
    let Statement::Select {
        distinct,
        distinct_on,
        ..
    } = parse("SELECT DISTINCT country FROM users;")
    else {
        panic!("expected select");
    };
    assert!(distinct);
    assert!(distinct_on.is_none());
}

#[test]
fn parses_distinct_on_single_expression() {
    let Statement::Select {
        distinct,
        distinct_on,
        order_by,
        ..
    } = parse("SELECT DISTINCT ON (country) id, country FROM users ORDER BY country, id;")
    else {
        panic!("expected select");
    };
    assert!(distinct);
    let distinct_on = distinct_on.expect("DISTINCT ON expressions");
    assert_eq!(distinct_on.len(), 1);
    assert_eq!(order_by.len(), 2);
}

#[test]
fn parses_distinct_on_multiple_expressions() {
    let Statement::Select {
        distinct, distinct_on, ..
    } = parse(
        "SELECT DISTINCT ON (country, status) id, country, status FROM users ORDER BY country, status, id;",
    )
    else {
        panic!("expected select");
    };
    assert!(distinct);
    assert_eq!(distinct_on.expect("DISTINCT ON expressions").len(), 2);
}

#[test]
fn parses_distinct_on_function_expression() {
    let Statement::Select {
        distinct,
        distinct_on,
        ..
    } = parse(
        "SELECT DISTINCT ON (LOWER(country)) id, country FROM users ORDER BY LOWER(country), id;",
    )
    else {
        panic!("expected select");
    };
    assert!(distinct);
    let distinct_on = distinct_on.expect("DISTINCT ON expressions");
    assert_eq!(distinct_on.len(), 1);
    assert!(matches!(
        distinct_on[0],
        plomid_sql::ast::Expression::FunctionCall { .. }
    ));
}

/// Extracts the frame of the first window function in a statement.
fn window_frame(sql: &str) -> plomid_sql::ast::FrameSpec {
    let Statement::Select { targets, .. } = parse(sql) else {
        panic!("expected select");
    };
    for target in targets {
        let over = window_over(&target);
        if let Some(over) = over {
            return over.frame.clone().expect("frame");
        }
    }
    panic!("no window function in {sql}");
}

fn window_over(target: &plomid_sql::SelectTarget) -> Option<&plomid_sql::ast::WindowSpec> {
    match target {
        plomid_sql::SelectTarget::WindowFunction { over, .. } => Some(over),
        plomid_sql::SelectTarget::Aliased { target, .. } => window_over(target),
        plomid_sql::SelectTarget::Expr { expr, .. } => {
            if let plomid_sql::ast::Expression::WindowFunction { over, .. } = expr {
                Some(over)
            } else {
                None
            }
        }
        _ => None,
    }
}

#[test]
fn parses_rows_frame_bounds() {
    let frame = window_frame(
        "SELECT SUM(a) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders;",
    );
    assert!(matches!(frame, plomid_sql::ast::FrameSpec::Rows { .. }));
    let plomid_sql::ast::FrameSpec::Rows { bounds } = frame else {
        panic!("expected rows frame");
    };
    assert!(matches!(
        bounds.start,
        plomid_sql::ast::FrameBound::UnboundedPreceding
    ));
    assert!(matches!(
        bounds.end,
        plomid_sql::ast::FrameBound::CurrentRow
    ));
}

#[test]
fn parses_single_bound_frame_as_start_to_current_row() {
    let frame =
        window_frame("SELECT SUM(a) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM orders;");
    let plomid_sql::ast::FrameSpec::Rows { bounds } = frame else {
        panic!("expected rows frame");
    };
    assert!(matches!(
        bounds.start,
        plomid_sql::ast::FrameBound::UnboundedPreceding
    ));
    assert!(matches!(
        bounds.end,
        plomid_sql::ast::FrameBound::CurrentRow
    ));
}

#[test]
fn parses_offset_frame_bounds() {
    let frame = window_frame(
        "SELECT SUM(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM orders;",
    );
    let plomid_sql::ast::FrameSpec::Rows { bounds } = frame else {
        panic!("expected rows frame");
    };
    let plomid_sql::ast::FrameBound::Preceding { offset } = &bounds.start else {
        panic!("expected preceding start");
    };
    assert!(matches!(
        offset.as_ref(),
        plomid_sql::ast::Expression::Literal(_)
    ));
    assert!(matches!(
        bounds.end,
        plomid_sql::ast::FrameBound::Following { .. }
    ));
}

#[test]
fn parses_groups_and_range_frame_kinds() {
    let frame = window_frame("SELECT SUM(a) OVER (ORDER BY id GROUPS 1 PRECEDING) FROM orders;");
    assert!(matches!(frame, plomid_sql::ast::FrameSpec::Groups { .. }));
    let frame = window_frame(
        "SELECT SUM(a) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders;",
    );
    assert!(matches!(frame, plomid_sql::ast::FrameSpec::Range { .. }));
}

#[test]
fn rejects_invalid_frame_start_after_end() {
    let tokens = Lexer::new(
        "SELECT SUM(a) OVER (ORDER BY id ROWS BETWEEN 5 FOLLOWING AND 2 PRECEDING) FROM orders;",
    )
    .lex()
    .unwrap();
    let mut catalog = InMemoryCatalog::new();
    let parser = Parser::new(tokens, &mut catalog);
    assert!(parser.parse_statements().is_err());
}

#[test]
fn rejects_frame_start_unbounded_following() {
    let tokens =
        Lexer::new("SELECT SUM(a) OVER (ORDER BY id ROWS UNBOUNDED FOLLOWING) FROM orders;")
            .lex()
            .unwrap();
    let mut catalog = InMemoryCatalog::new();
    let parser = Parser::new(tokens, &mut catalog);
    assert!(parser.parse_statements().is_err());
}
