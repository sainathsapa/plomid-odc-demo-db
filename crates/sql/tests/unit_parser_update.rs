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
// Moved from `src/parser/update.rs` inline `#[cfg(test)]` — public-API only.
use plomid_sql::ast::FromClause;
use plomid_sql::{Catalog, Expression, InMemoryCatalog, Lexer, Parser, Statement};

/// Builds a catalog with a `products(id, stock)` table so UPDATE parsing
/// can resolve the target relation.
fn products_catalog() -> InMemoryCatalog {
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "products".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "stock".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    // A source relation for `UPDATE ... FROM updates u` parsing.
    catalog
        .create_table(
            "updates".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "price".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    catalog
}

fn parse_update_sql(sql: &str) -> Statement {
    let tokens = Lexer::new(sql).lex().unwrap();
    let mut catalog = products_catalog();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    assert_eq!(stmts.len(), 1);
    stmts[0].clone()
}

#[test]
fn parses_update_target_alias_from_and_returning() {
    let sql = "UPDATE products p \
               SET stock = p.stock + 10 \
               FROM (SELECT 1 AS product_id) x \
               WHERE p.id = x.product_id \
               RETURNING p.id, p.stock";
    let Statement::Update {
        table,
        alias,
        from,
        assignments,
        where_expr,
        returning,
    } = parse_update_sql(sql)
    else {
        panic!("expected update");
    };
    assert_eq!(table, "products");
    assert_eq!(alias.as_deref(), Some("p"));
    match from {
        Some(FromClause::Subquery { alias, .. }) => assert_eq!(alias, "x"),
        other => panic!("expected subquery FROM, got {other:?}"),
    }
    assert_eq!(assignments.len(), 1);
    assert!(where_expr.is_some());
    assert_eq!(returning.map(|r| r.len()), Some(2));
}

#[test]
fn parses_update_as_alias() {
    let Statement::Update { alias, .. } =
        parse_update_sql("UPDATE products AS p SET stock = p.stock + 1 WHERE p.id = 1")
    else {
        panic!("expected update");
    };
    assert_eq!(alias.as_deref(), Some("p"));
}

#[test]
fn parses_update_without_alias_or_from() {
    let Statement::Update { alias, from, .. } =
        parse_update_sql("UPDATE products SET stock = stock + 10 WHERE id = 1")
    else {
        panic!("expected update");
    };
    assert!(alias.is_none());
    assert!(from.is_none());
}

#[test]
fn parses_update_from_table() {
    // FROM parsing must not require the source relation in the catalog.
    let Statement::Update { from, .. } = parse_update_sql(
        "UPDATE products p SET stock = p.stock + 1 FROM updates u WHERE p.id = u.id",
    ) else {
        panic!("expected update");
    };
    match from {
        Some(FromClause::Table { name, alias }) => {
            assert_eq!(name, "updates");
            assert_eq!(alias.as_deref(), Some("u"));
        }
        other => panic!("expected table FROM, got {other:?}"),
    }
}

#[test]
fn parses_arithmetic_assignment() {
    let tokens = Lexer::new("UPDATE users SET age = age + 1 WHERE id = 1;")
        .lex()
        .unwrap();
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "users".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "age".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Update { assignments, .. } = &stmts[0] else {
        panic!("expected update");
    };
    assert_eq!(assignments.len(), 1);
    assert!(matches!(assignments[0].1, Expression::Add(_, _)));
}

#[test]
fn parses_json_subscript_assignment() {
    let tokens = Lexer::new("UPDATE t SET payload['name'] = '\"UPDATED\"'::jsonb WHERE id = 1;")
        .lex()
        .unwrap();
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "t".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "payload".to_string(),
                    col_type: plomid_sql::ColumnType::new(
                        plomid_types::TypeOid::JSONB,
                        plomid_types::NO_TYPEMOD,
                    ),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Update { assignments, .. } = &stmts[0] else {
        panic!("expected update");
    };
    assert_eq!(assignments.len(), 1);
    assert!(matches!(
        assignments[0].0,
        Expression::ArrayIndex { ref array, ref index }
            if matches!(**array, Expression::ColumnRef(ref c) if c == "payload")
            && matches!(**index, Expression::Literal(_))
    ));
}

#[test]
fn parses_nested_json_subscript_assignment() {
    let tokens = Lexer::new("UPDATE t SET payload['items'][1] = '99'::jsonb WHERE id = 2;")
        .lex()
        .unwrap();
    let mut catalog = InMemoryCatalog::new();
    catalog
        .create_table(
            "t".to_string(),
            vec![
                plomid_sql::ColumnDef {
                    name: "id".to_string(),
                    col_type: plomid_sql::ColumnType::int4(),
                    constraints: Vec::new(),
                },
                plomid_sql::ColumnDef {
                    name: "payload".to_string(),
                    col_type: plomid_sql::ColumnType::new(
                        plomid_types::TypeOid::JSONB,
                        plomid_types::NO_TYPEMOD,
                    ),
                    constraints: Vec::new(),
                },
            ],
            Vec::new(),
        )
        .unwrap();
    let parser = Parser::new(tokens, &mut catalog);
    let stmts = parser.parse_statements().unwrap();
    let Statement::Update { assignments, .. } = &stmts[0] else {
        panic!("expected update");
    };
    assert_eq!(assignments.len(), 1);
    assert!(matches!(
        assignments[0].0,
        Expression::ArrayIndex { array: ref outer, .. }
            if matches!(&**outer, Expression::ArrayIndex { ref array, .. }
                if matches!(**array, Expression::ColumnRef(ref c) if c == "payload"))
    ));
}
