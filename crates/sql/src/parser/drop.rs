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
//! DROP statement parsing with optional IF EXISTS.

use super::{ParseError, Parser};
use crate::{
    ast::Statement,
    lexer::{Keyword, TokenKind},
};

pub fn parse_drop(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Drop), "DROP")?;
    let mut if_exists = false;
    let object = parser.peek().clone();
    if !matches!(
        object.kind,
        TokenKind::Keyword(
            Keyword::Table
                | Keyword::Schema
                | Keyword::Sequence
                | Keyword::Index
                | Keyword::View
                | Keyword::Type
                | Keyword::Domain
                | Keyword::Function
                | Keyword::Role
                | Keyword::User
        )
    ) {
        return Err(ParseError::Unsupported {
            message: "unsupported DROP object".into(),
            detail: Some(object.lexeme),
        });
    }
    parser.advance();
    let object = parser.tokens[parser.pos - 1].clone();
    if parser.check_keyword(Keyword::If) {
        parser.advance();
        parser.consume(TokenKind::Keyword(Keyword::Exists), "EXISTS")?;
        if_exists = true;
    }
    match object.kind {
        TokenKind::Keyword(Keyword::Table) => {
            let name = parser.parse_qualified_identifier("table name")?;
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropTable {
                name,
                if_exists,
                cascade,
            })
        }
        TokenKind::Keyword(Keyword::Schema) => {
            let name = parser.parse_qualified_identifier("schema name")?;
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropSchema {
                name,
                if_exists,
                cascade,
            })
        }
        TokenKind::Keyword(Keyword::Sequence) => {
            let name = parser.parse_qualified_identifier("sequence name")?;
            Ok(Statement::DropSequence { name, if_exists })
        }
        TokenKind::Keyword(Keyword::Index) => {
            let name = parser.parse_qualified_identifier("index name")?;
            Ok(Statement::DropIndex { name, if_exists })
        }
        TokenKind::Keyword(Keyword::View) => {
            let name = parser.parse_qualified_identifier("view name")?;
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropView {
                name,
                if_exists,
                cascade,
            })
        }
        TokenKind::Keyword(Keyword::Type) => {
            let name = parser.parse_qualified_identifier("type name")?;
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropType {
                name,
                if_exists,
                cascade,
            })
        }
        TokenKind::Keyword(Keyword::Domain) => {
            let name = parser.parse_qualified_identifier("domain name")?;
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropDomain {
                name,
                if_exists,
                cascade,
            })
        }
        TokenKind::Keyword(Keyword::Function) => {
            let name = parser.parse_qualified_identifier("function name")?;
            let mut args = Vec::new();
            // Parse optional argument type list: `func(integer, text)`
            if parser.check(TokenKind::LParen) {
                parser.advance();
                if !parser.check(TokenKind::RParen) {
                    loop {
                        let arg_type = parser.parse_type_name("argument type")?;
                        args.push(arg_type);
                        if parser.check(TokenKind::Comma) {
                            parser.advance();
                        } else {
                            break;
                        }
                    }
                }
                parser.consume(TokenKind::RParen, ")")?;
            }
            let cascade = parse_cascade_restrict(parser);
            Ok(Statement::DropFunction {
                name,
                args,
                if_exists,
                cascade,
            })
        }
        // PostgreSQL: `DROP ROLE|USER [IF EXISTS] name [, ...] [CASCADE|RESTRICT]`.
        // USER is an alias for ROLE. Multiple names are executed as successive
        // single-name drops by the caller convention — here we parse only the
        // first name and let the executor layer handle comma lists? No: keep
        // single-name (comma lists remain a syntax error, documented). CASCADE
        // /RESTRICT are accepted and ignored (drops always check ownership).
        TokenKind::Keyword(Keyword::Role) | TokenKind::Keyword(Keyword::User) => {
            let name = parser.parse_qualified_identifier("role name")?;
            let _ = parse_cascade_restrict(parser);
            Ok(Statement::DropRole { name, if_exists })
        }
        _ => Err(ParseError::Unsupported {
            message: "unsupported DROP object".to_string(),
            detail: Some(object.lexeme),
        }),
    }
}

/// Parses the optional `CASCADE` / `RESTRICT` clause after a DROP name.
/// PostgreSQL defaults to RESTRICT when neither is given.
fn parse_cascade_restrict(parser: &mut Parser<'_>) -> bool {
    if parser.check_keyword(Keyword::Cascade) || parser.check_identifier("CASCADE") {
        parser.advance();
        true
    } else if parser.check_keyword(Keyword::Restrict) || parser.check_identifier("RESTRICT") {
        parser.advance();
        false
    } else {
        false
    }
}
