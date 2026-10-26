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
// Moved from `src/lexer.rs` inline `#[cfg(test)]` — public-API only.
use plomid_sql::lexer::*;

#[test]
fn lex_create_table() {
    let tokens = Lexer::new("CREATE TABLE users (id INTEGER, name TEXT);")
        .lex()
        .unwrap();
    let kinds: Vec<_> = tokens.iter().map(|t| t.kind).collect();
    assert_eq!(
        kinds,
        vec![
            TokenKind::Keyword(Keyword::Create),
            TokenKind::Keyword(Keyword::Table),
            TokenKind::Identifier,
            TokenKind::LParen,
            TokenKind::Identifier,
            TokenKind::Keyword(Keyword::Integer),
            TokenKind::Comma,
            TokenKind::Identifier,
            TokenKind::Keyword(Keyword::Text),
            TokenKind::RParen,
            TokenKind::SemiColon,
            TokenKind::Eof,
        ]
    );
}

#[test]
fn lex_string_literal() {
    let tokens = Lexer::new("'Alice'").lex().unwrap();
    assert_eq!(tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(tokens[0].lexeme, "Alice");
}

#[test]
fn lex_vector_operators() {
    // Each three-character operator lexes as one token; neighbours that
    // share a prefix (`<`, `<=`, `<>`, `< -1`) are unaffected.
    for (sql, kind) in [
        ("<=>", TokenKind::VectorCosine),
        ("<->", TokenKind::VectorL2),
        ("<#>", TokenKind::VectorDot),
        ("<~>", TokenKind::VectorHamming),
        ("<%>", TokenKind::VectorJaccard),
    ] {
        let tokens = Lexer::new(sql).lex().unwrap();
        assert_eq!(tokens[0].kind, kind, "{sql}");
    }
    let kinds: Vec<_> = Lexer::new("a < -1")
        .lex()
        .unwrap()
        .iter()
        .map(|t| t.kind)
        .collect();
    assert!(kinds.contains(&TokenKind::Lt));
    assert!(kinds.contains(&TokenKind::Minus));
    let kinds: Vec<_> = Lexer::new("a <= b")
        .lex()
        .unwrap()
        .iter()
        .map(|t| t.kind)
        .collect();
    assert!(kinds.contains(&TokenKind::Le));
}

#[test]
fn lex_integer_literal() {
    let tokens = Lexer::new("42").lex().unwrap();
    assert_eq!(tokens[0].kind, TokenKind::IntegerLiteral);
    assert_eq!(tokens[0].lexeme, "42");
}

#[test]
fn lex_keywords_case_insensitive() {
    let tokens = Lexer::new("create table").lex().unwrap();
    assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Create));
    assert_eq!(tokens[1].kind, TokenKind::Keyword(Keyword::Table));
}

#[test]
fn lex_select_star() {
    let tokens = Lexer::new("SELECT * FROM users;").lex().unwrap();
    let kinds: Vec<_> = tokens.iter().map(|t| t.kind).collect();
    assert_eq!(
        kinds,
        vec![
            TokenKind::Keyword(Keyword::Select),
            TokenKind::Star,
            TokenKind::Keyword(Keyword::From),
            TokenKind::Identifier,
            TokenKind::SemiColon,
            TokenKind::Eof,
        ]
    );
}

#[test]
fn lex_transaction_keywords() {
    let tokens = Lexer::new("BEGIN; COMMIT; ROLLBACK;").lex().unwrap();
    let kinds: Vec<_> = tokens.iter().map(|t| t.kind).collect();
    assert_eq!(
        kinds,
        vec![
            TokenKind::Keyword(Keyword::Begin),
            TokenKind::SemiColon,
            TokenKind::Keyword(Keyword::Commit),
            TokenKind::SemiColon,
            TokenKind::Keyword(Keyword::Rollback),
            TokenKind::SemiColon,
            TokenKind::Eof,
        ]
    );
}

#[test]
fn lex_error_reports_position() {
    let err = Lexer::new("SELECT * FR@M users;").lex().unwrap_err();
    assert!(err.to_string().contains("syntax error at line 1"));
    assert!(err.to_string().contains("unexpected character"));
}
