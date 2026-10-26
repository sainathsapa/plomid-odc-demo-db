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
// Moved from `src/parser/json.rs` inline `#[cfg(test)]` — public-API only.
//! Parser coverage for the JSON surface syntax defined in this module.
use plomid_sql::ast::{FromClause, JsonTableColumnDefault, JsonTableColumnKind, Statement};
use plomid_sql::{InMemoryCatalog, JsonTableColumn, Lexer, Parser};

/// Parses a SELECT statement and returns its FROM clause.
fn parse_from(sql: &str) -> FromClause {
    let tokens = Lexer::new(sql).lex().unwrap();
    let mut catalog = InMemoryCatalog::new();
    let parser = Parser::new(tokens, &mut catalog);
    match parser
        .parse_statements()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
    {
        Statement::Select {
            from: Some(from), ..
        } => from,
        other => panic!("expected SELECT with FROM, got {other:?}"),
    }
}

fn json_table_from(sql: &str) -> (String, Vec<JsonTableColumn>) {
    match parse_from(sql) {
        FromClause::TableFunction {
            name, json_columns, ..
        } => (name, json_columns),
        other => panic!("expected TableFunction, got {other:?}"),
    }
}

#[test]
fn json_table_basic_columns_parse() {
    let (name, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[{\"id\":1}]', '$[*]' COLUMNS (id INTEGER PATH '$.id', name TEXT PATH '$.name', active BOOLEAN PATH '$.active')) AS jt;",
    );
    assert_eq!(name, "json_table");
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0].name.as_deref(), Some("id"));
    assert_eq!(columns[0].type_name.as_deref(), Some("INTEGER"));
    assert_eq!(columns[0].path.as_deref(), Some("$.id"));
    assert!(matches!(columns[0].kind, JsonTableColumnKind::Regular));
    assert_eq!(columns[1].name.as_deref(), Some("name"));
    assert_eq!(columns[1].type_name.as_deref(), Some("TEXT"));
    assert!(matches!(columns[1].kind, JsonTableColumnKind::Regular));
    assert_eq!(columns[2].name.as_deref(), Some("active"));
    assert_eq!(columns[2].type_name.as_deref(), Some("BOOLEAN"));
}

#[test]
fn json_table_ordinality_column_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (rn FOR ORDINALITY, name TEXT PATH '$.name')) AS jt;",
    );
    assert!(matches!(columns[0].kind, JsonTableColumnKind::Ordinality));
    assert_eq!(columns[0].name.as_deref(), Some("rn"));
    assert!(columns[0].type_name.is_none());
    assert!(matches!(columns[1].kind, JsonTableColumnKind::Regular));
}

#[test]
fn json_table_exists_column_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (has_active BOOLEAN EXISTS PATH '$.active')) AS jt;",
    );
    assert!(matches!(columns[0].kind, JsonTableColumnKind::Exists));
    assert_eq!(columns[0].type_name.as_deref(), Some("BOOLEAN"));
    assert_eq!(columns[0].path.as_deref(), Some("$.active"));
}

#[test]
fn json_table_format_json_column_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (tags JSON FORMAT JSON PATH '$.tags')) AS jt;",
    );
    assert!(matches!(columns[0].kind, JsonTableColumnKind::Regular));
    assert_eq!(columns[0].type_name.as_deref(), Some("JSON"));
    assert_eq!(columns[0].path.as_deref(), Some("$.tags"));
}

#[test]
fn json_table_default_on_empty_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (name TEXT PATH '$.name' DEFAULT '\"UNKNOWN\"' ON EMPTY)) AS jt;",
    );
    assert_eq!(columns[0].default_mode, JsonTableColumnDefault::Empty);
    assert_eq!(columns[0].default_value.as_deref(), Some("\"UNKNOWN\""));
}

#[test]
fn json_table_on_empty_and_on_error_parse() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INTEGER PATH '$.id' DEFAULT '999' ON EMPTY DEFAULT '888' ON ERROR)) AS jt;",
    );
    assert_eq!(columns[0].default_mode, JsonTableColumnDefault::Empty);
    assert_eq!(columns[0].default_value.as_deref(), Some("999"));
    assert_eq!(columns[0].error_default_mode, JsonTableColumnDefault::Error);
    assert_eq!(columns[0].error_default_value.as_deref(), Some("888"));
}

#[test]
fn json_table_default_on_error_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('[]', '$[*]' COLUMNS (id INTEGER PATH '$.id' DEFAULT '999' ON ERROR)) AS jt;",
    );
    assert_eq!(columns[0].default_mode, JsonTableColumnDefault::Error);
    assert_eq!(columns[0].default_value.as_deref(), Some("999"));
    assert_eq!(columns[0].error_default_mode, JsonTableColumnDefault::None);
    assert_eq!(columns[0].error_default_value, None);
}

#[test]
fn json_table_nested_columns_parse() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('{}', '$[*]' COLUMNS (id INTEGER PATH '$.id', NESTED PATH '$.orders[*]' COLUMNS (order_id INTEGER PATH '$.id'))) AS jt;",
    );
    assert!(matches!(columns[0].kind, JsonTableColumnKind::Regular));
    let JsonTableColumnKind::Nested { columns: nested } = &columns[1].kind else {
        panic!("expected nested column, got {:?}", columns[1].kind);
    };
    assert!(columns[1].name.is_none());
    assert_eq!(columns[1].path.as_deref(), Some("$.orders[*]"));
    assert_eq!(nested.len(), 1);
    assert_eq!(nested[0].name.as_deref(), Some("order_id"));
}

#[test]
fn json_table_multi_level_nested_parses() {
    let (_, columns) = json_table_from(
        "SELECT * FROM JSON_TABLE('{}', '$[*]' COLUMNS (dept TEXT PATH '$.name', NESTED PATH '$.teams[*]' COLUMNS (team TEXT PATH '$.name', NESTED PATH '$.members[*]' COLUMNS (member TEXT PATH '$.name')))) AS jt;",
    );
    let JsonTableColumnKind::Nested { columns: teams } = &columns[1].kind else {
        panic!("expected nested teams column");
    };
    let JsonTableColumnKind::Nested { columns: members } = &teams[1].kind else {
        panic!("expected nested members column");
    };
    assert_eq!(members[0].name.as_deref(), Some("member"));
}

#[test]
fn json_table_column_without_path_is_rejected() {
    // A regular column must declare its PATH; accepting a bare
    // `id INTEGER` would silently bind an implicit path.
    let tokens = Lexer::new("SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (id INTEGER)) AS jt;")
        .lex()
        .unwrap();
    let mut catalog = InMemoryCatalog::new();
    let parser = Parser::new(tokens, &mut catalog);
    assert!(
        parser.parse_statements().is_err(),
        "COLUMNS (id INTEGER) without PATH must not parse"
    );
}
