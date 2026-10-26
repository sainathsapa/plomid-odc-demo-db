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
// Moved from `src/parser/insert.rs` inline `#[cfg(test)]` — public-API only.
use plomid_sql::ast::{Expression, SelectTarget};
use plomid_sql::{
    Catalog, ColumnDef, ColumnType, CopyDirection, InMemoryCatalog, InsertSource, InsertValue,
    Keyword, Lexer, OnConflict, OnConflictTarget, ParseError, Parser, Statement, TokenKind, Value,
};

fn make_catalog() -> InMemoryCatalog {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "name".to_string(),
                    col_type: ColumnType::text(),
                    constraints: Vec::new(),
                },
                ColumnDef {
                    name: "age".to_string(),
                    col_type: ColumnType::int4(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    catalog
}

#[test]
fn parses_multiple_value_rows() {
    let tokens = Lexer::new("INSERT INTO users (id, name) VALUES (1, 'A'), (2, 'B');")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { source, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    let InsertSource::Values(values) = source else {
        panic!("expected VALUES source")
    };
    assert_eq!(values.len(), 2);
    assert_eq!(
        values[0][1],
        InsertValue::Literal(Value::Text("A".to_string()))
    );
    assert_eq!(
        values[1][1],
        InsertValue::Literal(Value::Text("B".to_string()))
    );
}

#[test]
fn parses_default_keyword_in_values() {
    let tokens = Lexer::new("INSERT INTO users (id) VALUES (DEFAULT);")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { source, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    let InsertSource::Values(values) = source else {
        panic!("expected VALUES source")
    };
    assert_eq!(values[0][0], InsertValue::Default);
}

#[test]
fn parses_decimal_literal_in_values() {
    let tokens = Lexer::new("INSERT INTO users (id, name, age) VALUES (0.95, 'x', 1);")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { source, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    let InsertSource::Values(values) = source else {
        panic!("expected VALUES source")
    };
    assert!(matches!(
        values[0][0],
        InsertValue::Literal(Value::Float8(_))
    ));
}

#[test]
fn parses_default_values() {
    let tokens = Lexer::new("INSERT INTO users DEFAULT VALUES;")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { source, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    assert!(matches!(source, InsertSource::DefaultValues));
}

#[test]
fn rejects_duplicate_columns() {
    let tokens = Lexer::new("INSERT INTO users (id, id) VALUES (1, 2);")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let err = parser.parse_statements().unwrap_err();
    assert!(matches!(err, ParseError::DuplicateColumn(c) if c == "id"));
}

#[test]
fn parses_returning_all() {
    let tokens = Lexer::new("INSERT INTO users VALUES (1, 'x', 10) RETURNING *;")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { returning, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    let Some(r) = returning else {
        panic!("expected RETURNING");
    };
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0], SelectTarget::All));
}

#[test]
fn parses_returning_columns() {
    let tokens = Lexer::new("INSERT INTO users VALUES (1, 'x', 10) RETURNING id, name AS display;")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { returning, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    assert!(returning.is_some());
}

#[test]
fn parses_on_conflict_do_nothing() {
    let tokens = Lexer::new("INSERT INTO users VALUES (1, 'x', 10) ON CONFLICT DO NOTHING;")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { on_conflict, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    assert!(matches!(
        on_conflict,
        Some(OnConflict::DoNothing {
            target: OnConflictTarget::NoTarget
        })
    ));
}

#[test]
fn parses_on_conflict_columns_do_nothing() {
    let tokens = Lexer::new("INSERT INTO users VALUES (1, 'x', 10) ON CONFLICT (id) DO NOTHING;")
        .lex()
        .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { on_conflict, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    match on_conflict {
        Some(OnConflict::DoNothing { target }) => {
            assert_eq!(target, &OnConflictTarget::Columns(vec!["id".to_string()]));
        }
        other => panic!("expected DoNothing Columns(id), got {other:?}"),
    }
}

#[test]
fn parses_on_conflict_do_update() {
    let tokens = Lexer::new(
        "INSERT INTO users VALUES (1, 'x', 10) ON CONFLICT (id) \
         DO UPDATE SET name = EXCLUDED.name, age = age + 1 WHERE age < 99;",
    )
    .lex()
    .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { on_conflict, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    match on_conflict {
        Some(OnConflict::DoUpdate {
            target,
            assignments,
            where_expr,
        }) => {
            assert_eq!(target, &OnConflictTarget::Columns(vec!["id".to_string()]));
            assert_eq!(assignments.len(), 2);
            assert!(
                matches!(&assignments[0].0, Expression::ColumnRef(c) if c.as_str() == "name"),
                "first assignment target should be column `name`"
            );
            assert!(where_expr.is_some(), "expected a WHERE clause");
        }
        other => panic!("expected DoUpdate, got {other:?}"),
    }
}

#[test]
fn parses_three_rows_with_commas_in_strings() {
    let tokens = Lexer::new(
        "INSERT INTO users VALUES (1, 'hello, world', NULL), (2, 'another value', 42), (3, 'a,b,c', 5);",
    )
    .lex()
    .unwrap();
    let mut catalog = make_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Insert { source, .. } = &stmts[0] else {
        panic!("expected insert");
    };
    let InsertSource::Values(values) = source else {
        panic!("expected VALUES source")
    };
    assert_eq!(values.len(), 3);
    assert_eq!(
        values[0][1],
        InsertValue::Literal(Value::Text("hello, world".to_string()))
    );
    assert_eq!(values[0][2], InsertValue::Literal(Value::Null));
    assert_eq!(
        values[1][1],
        InsertValue::Literal(Value::Text("another value".to_string()))
    );
    assert_eq!(values[1][2], InsertValue::Literal(Value::Int4(42)));
}

#[test]
fn parses_copy_from_stdin() {
    let tokens = Lexer::new("COPY users FROM STDIN;").lex().unwrap();
    let mut catalog = make_catalog();
    let mut parser = Parser::new(tokens, &mut catalog);
    let stmts = (|| {
        parser.consume(TokenKind::Keyword(Keyword::Copy), "COPY")?;
        let table = parser.parse_table_name()?;
        let direction = if parser.check_keyword(Keyword::From) {
            parser.advance();
            CopyDirection::From
        } else {
            return Err(ParseError::unexpected(parser.peek(), "FROM"));
        };
        parser.consume(TokenKind::Keyword(Keyword::Stdin), "STDIN")?;
        Ok(vec![Statement::Copy {
            table,
            columns: None,
            direction,
        }])
    })();
    let stmts = stmts.unwrap();
    let Statement::Copy { direction, .. } = &stmts[0] else {
        panic!("expected copy");
    };
    assert_eq!(*direction, CopyDirection::From);
}
