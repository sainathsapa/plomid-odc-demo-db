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
//! INSERT statement parsing, including multi-row VALUES lists,
//! DEFAULT VALUES, RETURNING, and ON CONFLICT clause shape validation.

use super::{select, ParseError, Parser};
use crate::{
    ast::{CopyDirection, InsertSource, InsertValue, OnConflict, OnConflictTarget, Statement},
    lexer::{Keyword, TokenKind},
    value_pg_type, Value,
};

pub fn parse_insert(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Insert), "INSERT")?;
    parser.consume(TokenKind::Keyword(Keyword::Into), "INTO")?;
    let table = parser.parse_table_name()?;

    let mut columns: Option<Vec<String>> = None;
    if parser.check(TokenKind::LParen) {
        parser.advance();
        let mut cols = Vec::new();
        loop {
            let column = match parser.peek().kind {
                TokenKind::Identifier => {
                    parser.advance();
                    parser.tokens[parser.pos - 1].lexeme.clone()
                }
                TokenKind::Keyword(Keyword::Value) => {
                    parser.advance();
                    parser.tokens[parser.pos - 1].lexeme.clone()
                }
                _ => {
                    return Err(ParseError::unexpected(parser.peek(), "column name"));
                }
            };
            if cols.iter().any(|c: &String| c == &column) {
                return Err(ParseError::DuplicateColumn(column));
            }
            cols.push(column);
            if !parser.check(TokenKind::Comma) {
                break;
            }
            parser.advance();
        }
        parser.consume(TokenKind::RParen, ")")?;
        columns = Some(cols);
    }

    let source = if parser.check_keyword(Keyword::Default) {
        parser.advance();
        parser.consume(TokenKind::Keyword(Keyword::Values), "VALUES")?;
        InsertSource::DefaultValues
    } else if parser.check_keyword(Keyword::Select) {
        let query = select::parse_select(parser)?;
        InsertSource::Select(Box::new(query))
    } else {
        parser.consume(TokenKind::Keyword(Keyword::Values), "VALUES")?;
        let mut rows = Vec::new();
        loop {
            let mut values = Vec::new();
            parser.consume(TokenKind::LParen, "(")?;
            loop {
                values.push(parse_insert_value(parser)?);
                if parser.check(TokenKind::Comma) {
                    parser.advance();
                } else {
                    break;
                }
            }
            parser.consume(TokenKind::RParen, ")")?;
            rows.push(values);
            if parser.check(TokenKind::Comma) {
                parser.advance();
            } else {
                break;
            }
        }
        InsertSource::Values(rows)
    };

    let on_conflict = if parser.check_keyword(Keyword::On) {
        parser.advance();
        parser.consume(TokenKind::Keyword(Keyword::Conflict), "CONFLICT")?;
        // Optional arbiter target: `ON CONFLICT (col, ...)`. Absent when
        // directly followed by `DO`.
        let target = if parser.check(TokenKind::LParen) {
            parser.advance();
            let mut cols = Vec::new();
            loop {
                let column = match parser.peek().kind {
                    TokenKind::Identifier | TokenKind::Keyword(Keyword::Value) => {
                        parser.advance();
                        parser.tokens[parser.pos - 1].lexeme.clone()
                    }
                    _ => return Err(ParseError::unexpected(parser.peek(), "column name")),
                };
                cols.push(column);
                if !parser.check(TokenKind::Comma) {
                    break;
                }
                parser.advance();
            }
            parser.consume(TokenKind::RParen, ")")?;
            OnConflictTarget::Columns(cols)
        } else {
            OnConflictTarget::NoTarget
        };
        parser.consume(TokenKind::Keyword(Keyword::Do), "DO")?;
        if parser.check_keyword(Keyword::Nothing) {
            parser.advance();
            Some(OnConflict::DoNothing { target })
        } else if parser.check_keyword(Keyword::Update) {
            parser.advance();
            let assignments = super::update::parse_update_set_clause(parser)?;
            let where_expr = if parser.check_keyword(Keyword::Where) {
                parser.advance();
                Some(parser.parse_expression()?)
            } else {
                None
            };
            Some(OnConflict::DoUpdate {
                target,
                assignments,
                where_expr,
            })
        } else {
            return Err(ParseError::unexpected(
                parser.peek(),
                "NOTHING or UPDATE after DO",
            ));
        }
    } else {
        None
    };

    let returning = if parser.check_keyword(Keyword::Returning) {
        parser.advance();
        Some(select::parse_select_targets(parser)?)
    } else {
        None
    };

    Ok(Statement::Insert {
        table,
        columns,
        source,
        returning,
        on_conflict,
    })
}

/// Parses a COPY statement skeleton. The network layer owns COPY protocol
/// framing; the parser only emits the parsed statement shape.
pub fn parse_copy(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Copy), "COPY")?;
    let table = parser.parse_table_name()?;

    let mut columns: Option<Vec<String>> = None;
    if parser.check(TokenKind::LParen) {
        parser.advance();
        let mut cols = Vec::new();
        loop {
            let column = match parser.peek().kind {
                TokenKind::Identifier => {
                    parser.advance();
                    parser.tokens[parser.pos - 1].lexeme.clone()
                }
                TokenKind::Keyword(Keyword::Value) => {
                    parser.advance();
                    parser.tokens[parser.pos - 1].lexeme.clone()
                }
                _ => {
                    return Err(ParseError::unexpected(parser.peek(), "column name"));
                }
            };
            if cols.iter().any(|c: &String| c == &column) {
                return Err(ParseError::DuplicateColumn(column));
            }
            cols.push(column);
            if !parser.check(TokenKind::Comma) {
                break;
            }
            parser.advance();
        }
        parser.consume(TokenKind::RParen, ")")?;
        columns = Some(cols);
    }

    let direction = if parser.check_keyword(Keyword::From) {
        parser.advance();
        CopyDirection::From
    } else if parser.check_keyword(Keyword::To) {
        parser.advance();
        CopyDirection::To
    } else {
        return Err(ParseError::unexpected(parser.peek(), "FROM or TO"));
    };

    if !parser.check_keyword(Keyword::Stdin) && !parser.check_keyword(Keyword::Stdout) {
        return Err(ParseError::Unsupported {
            message: "only COPY FROM STDIN and COPY TO STDOUT are supported".into(),
            detail: Some("file-based COPY requires server-side filesystem access".into()),
        });
    }
    parser.advance();

    Ok(Statement::Copy {
        table,
        columns,
        direction,
    })
}

/// Parses one `INSERT ... VALUES (?, ?, ...)` entry:
/// * `DEFAULT` keyword
/// * Literal / numeric / boolean / NULL value
/// * An arbitrary scalar expression (wrapped in `InsertValue::Expression`)
pub fn parse_insert_value(parser: &mut Parser<'_>) -> Result<InsertValue, ParseError> {
    if parser.check_keyword(Keyword::Default) {
        parser.advance();
        return Ok(InsertValue::Default);
    }
    if parser.check_keyword(Keyword::Null) {
        parser.advance();
        return Ok(InsertValue::Literal(Value::Null));
    }
    if parser.check_keyword(Keyword::True) {
        parser.advance();
        return Ok(InsertValue::Literal(Value::Bool(true)));
    }
    if parser.check_keyword(Keyword::False) {
        parser.advance();
        return Ok(InsertValue::Literal(Value::Bool(false)));
    }
    if parser.check_keyword(Keyword::Array) {
        let expression = parser.parse_expression()?;
        let value = literal_expression_value(&expression)?;
        return Ok(InsertValue::Literal(value));
    }
    // Parse as a full expression so that `::type` casts (e.g.
    // `'{\"a\":1}'::jsonb`) are preserved in INSERT value contexts.
    let expression = parser.parse_expression()?;
    if let ExpressionValue::Literal(lit) = extract_simple_literal(&expression) {
        Ok(InsertValue::Literal(lit))
    } else {
        Ok(InsertValue::Expression(Box::new(expression)))
    }
}

enum ExpressionValue {
    Literal(Value),
    Complex,
}

fn extract_simple_literal(expression: &crate::ast::Expression) -> ExpressionValue {
    match expression {
        crate::ast::Expression::Literal(v) => ExpressionValue::Literal(v.clone()),
        // A bare `[...]` vector literal parses to its own AST node; fold it
        // to the runtime vector value here so INSERT keeps the literal fast
        // path (the general expression path would evaluate it identically).
        crate::ast::Expression::VectorLiteral(values) => {
            match crate::ast::vector_literal_value(values) {
                Ok(value) => ExpressionValue::Literal(value),
                Err(_) => ExpressionValue::Complex,
            }
        }
        crate::ast::Expression::Negate(inner) => {
            if let crate::ast::Expression::Literal(Value::Int8(n)) = inner.as_ref() {
                ExpressionValue::Literal(Value::Int8(-*n))
            } else if let crate::ast::Expression::Literal(Value::Int4(n)) = inner.as_ref() {
                ExpressionValue::Literal(Value::Int4(-*n))
            } else if let crate::ast::Expression::Literal(Value::Int2(n)) = inner.as_ref() {
                ExpressionValue::Literal(Value::Int2(-*n))
            } else if let crate::ast::Expression::Literal(Value::Float8(n)) = inner.as_ref() {
                ExpressionValue::Literal(Value::Float8(-*n))
            } else if let crate::ast::Expression::Literal(Value::Float4(n)) = inner.as_ref() {
                ExpressionValue::Literal(Value::Float4(-*n))
            } else {
                ExpressionValue::Complex
            }
        }
        _ => ExpressionValue::Complex,
    }
}

fn literal_expression_value(expression: &crate::ast::Expression) -> Result<Value, ParseError> {
    let crate::ast::Expression::FunctionCall { name, args, .. } = expression else {
        return Err(ParseError::Unsupported {
            message: "unsupported INSERT value".into(),
            detail: Some("array expressions in INSERT VALUES".into()),
        });
    };
    if !name.eq_ignore_ascii_case("array") {
        return Err(ParseError::Unsupported {
            message: "unsupported INSERT value".into(),
            detail: Some("non-literal expressions in INSERT VALUES".into()),
        });
    }
    let elements = args
        .iter()
        .map(|arg| match arg {
            crate::ast::Expression::Literal(value) => Ok(value.clone()),
            crate::ast::Expression::FunctionCall { .. } => literal_expression_value(arg),
            _ => Err(ParseError::Unsupported {
                message: "unsupported ARRAY value".into(),
                detail: Some("ARRAY elements must be literals".into()),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let element_oid = elements
        .iter()
        .find_map(|value| match value {
            Value::Array { element_oid, .. } => Some(*element_oid),
            value if !value.is_null() => value_pg_type(value).map(|ty| ty.oid()),
            _ => None,
        })
        .unwrap_or(plomid_types::TypeOid::TEXT);
    Ok(Value::Array {
        element_oid,
        elements,
    })
}
