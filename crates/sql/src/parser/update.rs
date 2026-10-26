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
//! UPDATE statement parsing with expression-based SET assignments.
//!
//! Also parses the optional trailing PostgreSQL `RETURNING` clause
//! (`UPDATE ... WHERE ... RETURNING id, col`), stored on
//! `Statement::Update::returning` and executed by `dml::execute_update[_txn]`.

use super::{ParseError, Parser};
use crate::{
    ast::{Expression, Statement},
    lexer::{Keyword, TokenKind},
};

/// Parses the `SET col = expr [, col = expr ...]` assignment list shared by
/// `UPDATE ... SET` and `INSERT ... ON CONFLICT DO UPDATE SET`. Returns one
/// `(target, value)` pair per assignment using the existing expression grammar
/// (no second expression language is introduced for ON CONFLICT).
pub(crate) fn parse_update_set_clause(
    parser: &mut Parser<'_>,
) -> Result<Vec<(Expression, Expression)>, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Set), "SET")?;
    let mut assignments: Vec<(Expression, Expression)> = Vec::new();
    loop {
        // `value` is a lexer keyword but PostgreSQL allows it as a column
        // name, so accept the soft keyword as an assignment target.
        let first_tok = parser.peek().clone();
        let ident = match first_tok.kind {
            TokenKind::Identifier | TokenKind::Keyword(Keyword::Value) => {
                let name = first_tok.lexeme.clone();
                parser.advance();
                name
            }
            _ => return Err(ParseError::unexpected(parser.peek(), "column name")),
        };
        // Allow subscripted column targets such as `payload['key']` or
        // `payload['items'][1]` used for jsonb in-place assignment.
        let mut target = Expression::ColumnRef(ident);
        while parser.check(TokenKind::LBracket) {
            parser.advance();
            let index = super::expressions::parse_expression(parser)?;
            parser.consume(TokenKind::RBracket, "]")?;
            target = Expression::ArrayIndex {
                array: Box::new(target),
                index: Box::new(index),
            };
        }
        parser.consume(TokenKind::Eq, "=")?;
        let expr = parser.parse_expression()?;
        assignments.push((target, expr));

        if !parser.check(TokenKind::Comma) {
            break;
        }
        parser.advance();
    }
    Ok(assignments)
}

/// Parses the optional target alias after `UPDATE <table>`:
/// `UPDATE products p` or `UPDATE products AS p`. PostgreSQL allows an
/// unreserved-keyword alias too, but a bare `Identifier` token is safe here
/// because every clause keyword (`SET`, `FROM`, `WHERE`, `RETURNING`)
/// lexes as `Keyword`, never `Identifier`.
pub(crate) fn parse_target_alias(parser: &mut Parser<'_>) -> Result<Option<String>, ParseError> {
    if parser.check_keyword(Keyword::As) {
        parser.advance();
        let tok = parser.peek().clone();
        if !matches!(tok.kind, TokenKind::Identifier) {
            return Err(ParseError::unexpected(parser.peek(), "alias name after AS"));
        }
        parser.advance();
        return Ok(Some(tok.lexeme.clone()));
    }
    if matches!(parser.peek().kind, TokenKind::Identifier) {
        let tok = parser.peek().clone();
        parser.advance();
        return Ok(Some(tok.lexeme.clone()));
    }
    Ok(None)
}

pub fn parse_update(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Update), "UPDATE")?;
    let table = parser.parse_table_name()?;
    let alias = parse_target_alias(parser)?;
    let assignments = parse_update_set_clause(parser)?;

    // Optional `FROM <relation>` source (PostgreSQL `UPDATE ... FROM`),
    // parsed with the same relation grammar as SELECT's FROM clause.
    let mut from = None;
    if parser.check_keyword(Keyword::From) {
        parser.advance();
        from = Some(super::select::parse_from_clause(parser)?);
    }

    let mut where_expr = None;
    if parser.check(TokenKind::Keyword(Keyword::Where)) {
        parser.advance();
        where_expr = Some(parser.parse_expression()?);
    }

    if parser.check_keyword(Keyword::Returning) {
        // Trailing `RETURNING <select targets>` (same target grammar as
        // `INSERT ... RETURNING`, including `*`). Consumes the keyword then
        // parses a SELECT target list; absence leaves `returning: None`.
        parser.advance();
        let returning = Some(super::select::parse_select_targets(parser)?);
        return Ok(Statement::Update {
            table,
            alias,
            from,
            assignments,
            where_expr,
            returning,
        });
    }

    Ok(Statement::Update {
        table,
        alias,
        from,
        assignments,
        where_expr,
        returning: None,
    })
}
