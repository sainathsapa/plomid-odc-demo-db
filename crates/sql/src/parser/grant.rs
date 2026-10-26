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
use super::{ParseError, Parser};
use crate::{
    ast::Statement,
    lexer::{Keyword, TokenKind},
};
use plomid_security::GrantObject;

/// Privilege keywords that start an object-privilege grant. Anything else
/// after `GRANT` is the legacy role-membership form (`GRANT role TO member`).
fn is_privilege_word(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "TRUNCATE"
            | "USAGE"
            | "CREATE"
            | "CONNECT"
            | "ALL"
    )
}

pub fn parse_grant(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Grant), "GRANT")?;
    if is_privilege_word(&parser.peek().lexeme) {
        return parse_grant_privileges(parser, true);
    }
    let role = parser.parse_qualified_identifier("role name")?;
    parser.consume(TokenKind::Keyword(Keyword::To), "TO")?;
    let member = parser.parse_qualified_identifier("member role name")?;
    Ok(Statement::GrantRole { role, member })
}

pub fn parse_revoke(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    parser.consume(TokenKind::Keyword(Keyword::Revoke), "REVOKE")?;
    if is_privilege_word(&parser.peek().lexeme) {
        return parse_grant_privileges(parser, false);
    }
    let role = parser.parse_qualified_identifier("role name")?;
    parser.consume(TokenKind::Keyword(Keyword::From), "FROM")?;
    let member = parser.parse_qualified_identifier("member role name")?;
    Ok(Statement::RevokeRole { role, member })
}

/// Parses `privs ON [TABLE | SCHEMA | DATABASE] object TO/FROM role`.
/// `is_grant` selects the `TO` (grant) vs `FROM` (revoke) terminator.
fn parse_grant_privileges(
    parser: &mut Parser<'_>,
    is_grant: bool,
) -> Result<Statement, ParseError> {
    let mut privileges = Vec::new();
    loop {
        let word = parser.peek().lexeme.clone();
        if !is_privilege_word(&word) && !word.eq_ignore_ascii_case("PRIVILEGES") {
            break;
        }
        parser.advance();
        if word.eq_ignore_ascii_case("privileges") {
            continue;
        }
        privileges.push(word.to_ascii_uppercase());
        // `ALL PRIVILEGES` is the PostgreSQL spelling; the privilege
        // parser already accepts bare `ALL`, so consume the optional
        // qualifier here rather than requiring a comma.
        if word.eq_ignore_ascii_case("ALL")
            && (parser.peek().lexeme.eq_ignore_ascii_case("PRIVILEGES")
                || parser.peek().lexeme.eq_ignore_ascii_case("PRIVILEGE"))
        {
            parser.advance();
        }
        // Comma-separated lists: `GRANT SELECT, INSERT ON …`.
        if parser.check(TokenKind::Comma) {
            parser.advance();
            continue;
        }
        break;
    }
    if privileges.is_empty() {
        let tok = parser.peek().clone();
        return Err(ParseError::unexpected(&tok, "privilege name"));
    }
    parser.consume(TokenKind::Keyword(Keyword::On), "ON")?;
    // Optional object-class keyword (default TABLE, PostgreSQL-compatible).
    let mut object_kind = "TABLE";
    for candidate in ["TABLE", "SCHEMA", "DATABASE"] {
        if parser.peek().lexeme.eq_ignore_ascii_case(candidate) {
            parser.advance();
            object_kind = candidate;
            break;
        }
    }
    let object = if parser.peek().lexeme.eq_ignore_ascii_case("ALL")
        && parser.peek_n(1).lexeme.eq_ignore_ascii_case("TABLES")
    {
        parser.advance();
        parser.advance();
        parser.consume(TokenKind::Keyword(Keyword::In), "IN")?;
        parser.consume(TokenKind::Keyword(Keyword::Schema), "SCHEMA")?;
        GrantObject::AllTablesInSchema(parser.parse_qualified_identifier("schema name")?)
    } else {
        let object_name = parser.parse_qualified_identifier("object name")?;
        match object_kind {
            "SCHEMA" => GrantObject::Schema(object_name),
            "DATABASE" => GrantObject::Database(object_name),
            _ => GrantObject::Table(object_name),
        }
    };
    if is_grant {
        parser.consume(TokenKind::Keyword(Keyword::To), "TO")?;
    } else {
        parser.consume(TokenKind::Keyword(Keyword::From), "FROM")?;
    }
    let grantee = parser.parse_qualified_identifier("role name")?;
    if is_grant {
        Ok(Statement::GrantPrivileges {
            privileges,
            object,
            grantee,
        })
    } else {
        Ok(Statement::RevokePrivileges {
            privileges,
            object,
            grantee,
        })
    }
}
