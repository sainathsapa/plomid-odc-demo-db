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
pub mod alter;
pub mod comment;
pub mod create;
pub mod delete;
pub mod drop;
pub mod expressions;
pub mod grant;
pub mod insert;
pub mod json;
pub mod select;
pub mod set;
pub mod show;
pub mod types;
pub mod update;
pub mod use_;

use crate::{
    ast::{Statement, Value},
    lexer::{Keyword, LexError, Token, TokenKind},
};

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    UnexpectedToken {
        expected: String,
        found: String,
        line: usize,
        column: usize,
    },
    LexError(LexError),
    DuplicateTable(String),
    DuplicateColumn(String),
    UnknownTable(String),
    Unsupported {
        message: String,
        detail: Option<String>,
    },
}

impl ParseError {
    pub fn unexpected(token: &Token, expected: &str) -> Self {
        Self::UnexpectedToken {
            expected: expected.to_string(),
            found: token.lexeme.clone(),
            line: token.line as usize,
            column: token.column as usize,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedToken {
                expected,
                found,
                line,
                column,
            } => write!(
                f,
                "syntax error at line {line}, column {column}: expected {expected}, found {found}"
            ),
            Self::LexError(err) => write!(f, "lexer error at line {}: {}", err.line, err.message),
            Self::DuplicateTable(name) => write!(f, "table \"{name}\" already exists"),
            Self::DuplicateColumn(name) => write!(f, "duplicate column: {name}"),
            Self::UnknownTable(name) => write!(f, "table \"{name}\" does not exist"),
            Self::Unsupported { message, detail } => {
                if let Some(detail) = detail {
                    write!(f, "{message}: {detail}")
                } else {
                    write!(f, "{message}")
                }
            }
        }
    }
}

impl std::error::Error for ParseError {}

impl From<LexError> for ParseError {
    fn from(err: LexError) -> Self {
        Self::LexError(err)
    }
}

/// Maximum parser nesting depth for expressions and subqueries (DoS cap).
///
/// Every parenthesized group, function-argument list, `NOT`/`-`/`+`/`^`
/// self-recursion, scalar subquery, and `UNION`-chain link passes through a
/// guarded entry point sharing this budget, so a hostile `((((…))))`,
/// `NOT NOT …`, or `(SELECT (SELECT …))` chain fails closed instead of
/// overflowing the parser stack (and later the evaluator stack, which
/// recurses per AST node). Calibration (measured, `nesting_depth` probe):
/// the cheapest cycle (parenthesized groups) costs ~12-15 precedence-descent
/// frames per level; 300 subquery levels overflow even an 8MB stack, and a
/// 128 budget still overflows 2MB worker/test-thread stacks. At 64, release
/// builds (production) peak well under 1MB at the cap — safe on every
/// thread — while legitimate queries (a handful of nesting levels, far below
/// the executor's own 24-subquery cap) never come close. Debug builds keep
/// ~10x fatter unoptimized frames (the giant `parse_primary_atom` match),
/// so ~15+ levels can overflow a 2MB debug thread before the guard fires;
/// that residual is documented, not fixed, here — the in-tree depth tests
/// run on 64MB stacks so they assert guard behavior deterministically in
/// both profiles. Flat `AND`/`OR` chains do NOT consume this budget (they
/// parse iteratively); their deep-AST evaluator recursion is a separate
/// hardening item owned with the sibling `query/*` evaluator (see the
/// item-3 report).
pub const MAX_EXPRESSION_NESTING_DEPTH: u32 = 64;

pub struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    catalog: &'a mut dyn crate::Catalog,
    /// Names of CTEs visible in the current statement scope. `parse_with`
    /// populates this so `FROM cte_name` parses even though the name is not a
    /// real catalog table; the executor later inlines the rewritten query.
    cte_names: Vec<String>,
    /// JSON_TABLE `COLUMNS (...)` spec left by parsing a `JSON_TABLE(...)`
    /// function call inside an expression. `parse_from_atom` reads this once
    /// it recognises the call as a `FromClause::TableFunction`.
    pending_json_columns: Option<Vec<crate::JsonTableColumn>>,
    /// Current [`MAX_EXPRESSION_NESTING_DEPTH`] budget consumption. Incremented
    /// by [`Parser::nested`] around every recursive-descent cycle and
    /// decremented when it completes (including on error), so siblings at the
    /// same level each get a fresh budget.
    nesting: u32,
}

impl<'a> Parser<'a> {
    pub fn new(tokens: Vec<Token>, catalog: &'a mut dyn crate::Catalog) -> Self {
        Self {
            tokens,
            pos: 0,
            catalog,
            cte_names: Vec::new(),
            pending_json_columns: None,
            nesting: 0,
        }
    }

    /// Runs `f` inside one nesting level, failing closed past
    /// [`MAX_EXPRESSION_NESTING_DEPTH`]. The budget is released when `f`
    /// returns, on both success and error paths.
    pub fn nested<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        if self.nesting >= MAX_EXPRESSION_NESTING_DEPTH {
            return Err(ParseError::Unsupported {
                message: format!(
                    "expression nesting exceeds the maximum depth of {MAX_EXPRESSION_NESTING_DEPTH}"
                ),
                detail: None,
            });
        }
        self.nesting += 1;
        let result = f(&mut *self);
        self.nesting = self.nesting.saturating_sub(1);
        result
    }

    pub fn catalog(&mut self) -> &mut dyn crate::Catalog {
        self.catalog
    }

    pub fn parse_statements(mut self) -> Result<Vec<Statement>, ParseError> {
        tracing::trace!(target: "sql::parser", "parse_start token_count={}", self.tokens.len());
        let mut statements = Vec::new();
        while !self.check(TokenKind::Eof) {
            let stmt = self.parse_statement()?;
            tracing::trace!(target: "sql::parser", "parsed_statement kind={:?}", std::mem::discriminant(&stmt));
            statements.push(stmt);
            if self.check(TokenKind::SemiColon) {
                self.advance();
            } else if !self.check(TokenKind::Eof) {
                let token = self.peek().clone();
                return Err(ParseError::unexpected(&token, "; or end of input"));
            }
        }
        tracing::trace!(target: "sql::parser", "parse_complete statement_count={}", statements.len());
        Ok(statements)
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        // Reset JSON_TABLE column state between statements so a previous
        // statement's `pending_json_columns` cannot leak into the next one.
        self.pending_json_columns = None;
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Keyword(Keyword::Create) => create::parse_create(self),
            TokenKind::Keyword(Keyword::Insert) => insert::parse_insert(self),
            TokenKind::Keyword(Keyword::Select) => select::parse_select(self),
            TokenKind::Keyword(Keyword::Values) => select::parse_values_statement(self),
            TokenKind::Keyword(Keyword::Update) => update::parse_update(self),
            TokenKind::Keyword(Keyword::Delete) => delete::parse_delete(self),
            TokenKind::Keyword(Keyword::Set) => set::parse_set(self),
            TokenKind::Keyword(Keyword::Show) => show::parse_show(self),
            TokenKind::Keyword(Keyword::Describe | Keyword::Desc) => {
                self.advance();
                Ok(Statement::Describe {
                    name: self.parse_table_name()?,
                })
            }
            TokenKind::Keyword(Keyword::Use) => use_::parse_use(self),
            TokenKind::Keyword(Keyword::Drop) => drop::parse_drop(self),
            TokenKind::Keyword(Keyword::Alter) => alter::parse_alter(self),
            TokenKind::Keyword(Keyword::Grant) => grant::parse_grant(self),
            TokenKind::Keyword(Keyword::Revoke) => grant::parse_revoke(self),
            TokenKind::Keyword(Keyword::Begin) => {
                self.advance();
                consume_transaction_options(self);
                Ok(Statement::Begin)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("start") => {
                self.advance();
                let next = self.peek().clone();
                if matches!(next.kind, TokenKind::Identifier)
                    && next.lexeme.eq_ignore_ascii_case("transaction")
                {
                    self.advance();
                }
                consume_transaction_options(self);
                Ok(Statement::Begin)
            }
            TokenKind::Keyword(Keyword::With) => select::parse_with(self),
            TokenKind::Keyword(Keyword::Explain) => select::parse_explain(self),
            TokenKind::Keyword(Keyword::Copy) => insert::parse_copy(self),
            TokenKind::Keyword(Keyword::Commit) => {
                self.advance();
                consume_transaction_options(self);
                Ok(Statement::Commit)
            }
            TokenKind::Keyword(Keyword::Rollback) => {
                self.advance();
                if is_txn_opt_word(self, "to") {
                    self.advance();
                    if is_txn_opt_word(self, "savepoint") {
                        self.advance();
                    }
                    if matches!(self.peek().kind, TokenKind::Identifier) {
                        self.advance();
                    }
                }
                consume_transaction_options(self);
                Ok(Statement::Rollback)
            }
            TokenKind::Keyword(Keyword::Comment) => comment::parse_comment(self),
            TokenKind::Keyword(Keyword::Analyze) => {
                consume_housekeeping_tail(self);
                Ok(Statement::Analyze { table: None })
            }
            TokenKind::Keyword(Keyword::Vacuum) => {
                self.advance();
                vacuum_table(self)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("vacuum") => {
                self.advance();
                vacuum_table(self)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("reindex") => {
                self.advance();
                consume_housekeeping_tail(self);
                Ok(Statement::Reindex)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("lock") => {
                self.advance();
                consume_housekeeping_tail(self);
                Ok(Statement::Lock)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("cluster") => {
                self.advance();
                consume_housekeeping_tail(self);
                Ok(Statement::Cluster)
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("refresh") => {
                self.advance();
                if is_txn_opt_word(self, "materialized") {
                    self.advance();
                }
                if is_txn_opt_word(self, "view") {
                    self.advance();
                }
                consume_housekeeping_tail(self);
                Ok(Statement::RefreshMaterializedView {
                    name: String::new(),
                })
            }
            TokenKind::Identifier if self.peek().lexeme.eq_ignore_ascii_case("truncate") => {
                self.advance();
                // TRUNCATE empties the named table(s) of every row, so the
                // table names must reach the executor. Parse one or more
                // comma-separated names (PostgreSQL `TRUNCATE [TABLE] name
                // [, ...]`) before swallowing any trailing options
                // (RESTART IDENTITY / CASCADE / RESTRICT / ONLY ...).
                let mut tables: Vec<String> = Vec::new();
                loop {
                    tables.push(self.parse_table_name()?);
                    if self.check(TokenKind::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                consume_housekeeping_tail(self);
                Ok(Statement::Truncate { tables })
            }
            TokenKind::Keyword(Keyword::Do) => {
                self.advance();
                // DO $$ body $$ — consume the dollar-quoted body as a raw string.
                // PLOMID V1 only supports the subset required by compatibility
                // tests: PERFORM, RAISE NOTICE, and EXCEPTION handlers. The body
                // is preserved as text and executed by the executor.
                let token = self.peek().clone();
                if token.kind == TokenKind::StringLiteral {
                    self.advance();
                    Ok(Statement::Do { body: token.lexeme })
                } else {
                    Err(ParseError::unexpected(&token, "dollar-quoted string body"))
                }
            }
            _ => Err(ParseError::unexpected(
                &token,
                "SQL statement (CREATE, INSERT, SELECT, UPDATE, DELETE, BEGIN, COMMIT, ROLLBACK, SET, SHOW, USE)",
            )),
        }
    }

    pub fn check(&self, kind: TokenKind) -> bool {
        if self.pos < self.tokens.len() {
            self.tokens[self.pos].kind == kind
        } else {
            false
        }
    }

    pub fn check_keyword(&self, keyword: Keyword) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::Keyword(kind) if kind == keyword
        )
    }

    pub fn peek(&self) -> &Token {
        if self.pos < self.tokens.len() {
            &self.tokens[self.pos]
        } else {
            &self.tokens[self.tokens.len() - 1]
        }
    }

    pub fn peek_n(&self, offset: usize) -> &Token {
        &self.tokens[(self.pos + offset).min(self.tokens.len() - 1)]
    }

    pub fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    pub fn consume(&mut self, kind: TokenKind, expected: &str) -> Result<Token, ParseError> {
        if self.check(kind) {
            let token = self.tokens[self.pos].clone();
            self.pos += 1;
            Ok(token)
        } else {
            let token = self.peek().clone();
            Err(ParseError::unexpected(&token, expected))
        }
    }

    pub fn parse_qualified_identifier(&mut self, expected: &str) -> Result<String, ParseError> {
        // A small set of soft keywords that PostgreSQL allows as bare
        // identifiers/column names (`type`, `left`, `right`, ...).
        fn is_soft_identifier(kind: TokenKind) -> bool {
            matches!(
                kind,
                TokenKind::Identifier
                    | TokenKind::Keyword(
                        Keyword::Type
                            | Keyword::Replace
                            | Keyword::Left
                            | Keyword::Right
                            | Keyword::Default
                            | Keyword::Value
                            | Keyword::Key
                            | Keyword::Conflict
                            | Keyword::Row
                            // `format` is a function/column name (FORMAT is
                            // only a non-reserved JSON-clause word), so it
                            // must stay usable as a bare identifier.
                            | Keyword::Format
                    )
            )
        }
        let first_token = self.peek().clone();
        let first = match first_token.kind {
            kind if is_soft_identifier(kind) => {
                self.advance();
                first_token.lexeme
            }
            _ => return Err(ParseError::unexpected(&first_token, expected)),
        };
        let mut name = first;
        while self.check(TokenKind::Dot) {
            self.advance();
            let part_token = self.peek().clone();
            if !is_soft_identifier(part_token.kind) {
                return Err(ParseError::unexpected(&part_token, expected));
            }
            self.advance();
            name.push('.');
            name.push_str(&part_token.lexeme);
        }
        Ok(name)
    }

    /// Parses a (possibly qualified) type name after `::`. Unlike ordinary
    /// identifiers, type names may be SQL keywords (`TEXT`, `INTEGER`, ...).
    pub fn parse_type_name(&mut self, expected: &str) -> Result<String, ParseError> {
        let first_token = self.peek().clone();
        let first = match first_token.kind {
            TokenKind::Identifier | TokenKind::Keyword(_) => {
                self.advance();
                first_token.lexeme
            }
            _ => return Err(ParseError::unexpected(&first_token, expected)),
        };
        let mut name = first;

        // PostgreSQL has several multi-word type names (e.g. `double
        // precision`, `character varying`, `timestamp with time zone`). The
        // type registry owns their aliases, but the parser must preserve the
        // full name so those aliases resolve consistently for casts and
        // expressions. The trailing words lex as identifiers or keywords
        // (`WITH`, `TIME`), so greedily match the known multi-word forms.
        let peek = |tokens: &[Token], pos: usize, offset: usize| -> Option<String> {
            tokens
                .get(pos + offset)
                .map(|t| t.lexeme.to_ascii_lowercase())
        };
        let first_lower = name.to_ascii_lowercase();
        let second = peek(&self.tokens, self.pos, 0);
        let third = peek(&self.tokens, self.pos, 1);
        let fourth = peek(&self.tokens, self.pos, 2);
        let words: Option<Vec<String>> = match (
            first_lower.as_str(),
            second.as_deref(),
            third.as_deref(),
            fourth.as_deref(),
        ) {
            ("character", Some("varying"), _, _) => Some(vec!["varying".to_string()]),
            ("double", Some("precision"), _, _) => Some(vec!["precision".to_string()]),
            ("bit", Some("varying"), _, _) => Some(vec!["varying".to_string()]),
            ("timestamp", Some(w), Some("time"), Some("zone")) if w == "with" || w == "without" => {
                Some(vec![w.to_string(), "time".to_string(), "zone".to_string()])
            }
            ("time", Some(w), Some("time"), Some("zone")) if w == "with" || w == "without" => {
                Some(vec![w.to_string(), "time".to_string(), "zone".to_string()])
            }
            _ => None,
        };
        if let Some(words) = words {
            for word in &words {
                name.push(' ');
                name.push_str(word);
            }
            for _ in 0..words.len() {
                self.advance();
            }
        }
        while self.check(TokenKind::Dot) {
            self.advance();
            let token = self.peek().clone();
            match token.kind {
                TokenKind::Identifier | TokenKind::Keyword(_) => {
                    self.advance();
                    name.push('.');
                    name.push_str(&token.lexeme);
                }
                _ => return Err(ParseError::unexpected(&token, expected)),
            }
        }
        while self.check(TokenKind::LBracket) && self.peek_n(1).kind == TokenKind::RBracket {
            self.advance();
            self.consume(TokenKind::RBracket, "]")?;
            name.push_str("[]");
        }
        if self.check(TokenKind::LParen) {
            self.advance();
            name.push('(');
            loop {
                let token = self.consume(TokenKind::IntegerLiteral, "type modifier")?;
                name.push_str(&token.lexeme);
                if self.check(TokenKind::Comma) {
                    self.advance();
                    name.push(',');
                } else {
                    break;
                }
            }
            self.consume(TokenKind::RParen, ")")?;
            name.push(')');
        }
        Ok(name)
    }

    pub fn parse_table_name(&mut self) -> Result<String, ParseError> {
        let qualified = self.parse_qualified_identifier("table name")?;
        let table = qualified
            .rsplit('.')
            .next()
            .ok_or_else(|| ParseError::Unsupported {
                message: "invalid table name".to_string(),
                detail: None,
            })?
            .to_string();
        // Any schema-qualified name (including `public.`) resolves directly to
        // that schema — never through the session search_path. This keeps
        // `public.customers` and `plomid_test.customers` distinct even when
        // `search_path` lists an earlier schema.
        if qualified.contains('.') {
            return Ok(qualified);
        }
        if self
            .cte_names
            .iter()
            .any(|cte| cte.eq_ignore_ascii_case(&table))
        {
            return Ok(table);
        }
        if Self::is_system_relation_name(&table) {
            return Ok(table);
        }
        // Resolve views and tables to their fully-qualified catalog identity.
        // Returning the qualified name (e.g. `plomid_torture.orders`) keeps row
        // storage, scans, DML and catalog operations schema-aware even when the
        // user wrote an unqualified name.
        if let Some(view) = self.catalog().get_view(&table) {
            return Ok(view.name.clone());
        }
        self.catalog()
            .get_table(&table)
            .map(|schema| schema.name.clone())
            .map_err(|_| ParseError::UnknownTable(table.clone()))
    }

    fn is_system_relation_name(name: &str) -> bool {
        matches!(
            name.to_ascii_lowercase().as_str(),
            "pg_am"
                | "pg_database"
                | "pg_namespace"
                | "pg_collation"
                | "pg_class"
                | "pg_attribute"
                | "pg_attrdef"
                | "pg_type"
                | "pg_aggregate"
                | "pg_operator"
                | "pg_opclass"
                | "pg_amop"
                | "pg_amproc"
                | "pg_cast"
                | "pg_statistic"
                | "pg_conversion"
                | "pg_language"
                | "pg_enum"
                | "pg_index"
                | "pg_constraint"
                | "pg_description"
                | "pg_shdescription"
                | "pg_proc"
                | "pg_roles"
                | "pg_user"
                | "pg_group"
                | "pg_tablespace"
                | "pg_settings"
                | "pg_tables"
                | "pg_views"
                | "pg_indexes"
                | "pg_user_mapping"
                | "pg_extension"
                | "pg_replication_slots"
                | "pg_replication_origin_status"
                | "pg_stat_activity"
                | "pg_stat_database"
                | "pg_stat_user_tables"
                | "pg_stat_all_tables"
                | "pg_foreign_server"
                | "pg_foreign_data_wrapper"
                | "pg_auth_members"
                | "pg_depend"
                | "pg_shdepend"
                | "pg_trigger"
                | "pg_policy"
                | "pg_policies"
                | "pg_statistic_ext"
                | "pg_publication"
                | "pg_publication_rel"
                | "pg_inherits"
                | "pg_rewrite"
                | "pg_sequence"
                | "pg_sequences"
                | "pg_matviews"
                | "pg_show_all_settings"
                | "pg_default_acl"
                // In-band observability (mirrors
                // `plomid-executor`'s `SystemCatalog::is_relation`): without
                // these the bare-table spelling fails at parse time with
                // `UnknownTable`, while the table-function spelling
                // (`plomid_stat_counters()`) never resolves a table name.
                | "plomid_stat_counters"
                | "plomid_stat_analytical"
                | "plomid_stat_tables"
                | "plomid_htap_stats"
        )
    }

    pub fn check_identifier(&self, expected: &str) -> bool {
        matches!(self.peek().kind, TokenKind::Identifier)
            && self.peek().lexeme.eq_ignore_ascii_case(expected)
    }

    pub fn parse_value(&mut self) -> Result<Value, ParseError> {
        expressions::parse_value(self)
    }

    pub fn parse_expression(&mut self) -> Result<crate::ast::Expression, ParseError> {
        expressions::parse_expression(self)
    }

    /// Parses a single unqualified identifier.
    pub fn parse_identifier(&mut self) -> Result<String, ParseError> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Identifier => {
                self.advance();
                Ok(token.lexeme)
            }
            _ => Err(ParseError::unexpected(&token, "identifier")),
        }
    }
}

fn is_txn_opt_word(parser: &Parser<'_>, word: &str) -> bool {
    matches!(
        parser.peek().kind,
        TokenKind::Identifier | TokenKind::Keyword(_)
    ) && parser.peek().lexeme.eq_ignore_ascii_case(word)
}

fn consume_transaction_options(parser: &mut Parser<'_>) {
    if is_txn_opt_word(parser, "transaction") || is_txn_opt_word(parser, "work") {
        parser.advance();
    }
    loop {
        if is_txn_opt_word(parser, "isolation") {
            parser.advance();
            if is_txn_opt_word(parser, "level") {
                parser.advance();
            }
            // Consume the isolation level: READ COMMITTED, READ UNCOMMITTED,
            // REPEATABLE READ, SERIALIZABLE, or DEFAULT.
            // These are multi-word identifiers that need to be consumed.
            match parser.peek().kind {
                TokenKind::Identifier | TokenKind::Keyword(_) => {
                    let word = parser.peek().lexeme.to_ascii_lowercase();
                    if word == "read"
                        || word == "repeatable"
                        || word == "serializable"
                        || word == "default"
                    {
                        parser.advance();
                        // Consume the second word if needed (e.g., "committed" in "read committed")
                        if is_txn_opt_word(parser, "committed")
                            || is_txn_opt_word(parser, "uncommitted")
                            || is_txn_opt_word(parser, "only")
                            || is_txn_opt_word(parser, "write")
                        {
                            parser.advance();
                        }
                    } else {
                        // Unknown isolation level, consume one token
                        parser.advance();
                    }
                }
                _ => break,
            }
        } else if is_txn_opt_word(parser, "read") {
            parser.advance();
            if is_txn_opt_word(parser, "write") || is_txn_opt_word(parser, "only") {
                parser.advance();
            }
        } else if is_txn_opt_word(parser, "not") {
            parser.advance();
            if is_txn_opt_word(parser, "deferrable") {
                parser.advance();
            }
        } else if is_txn_opt_word(parser, "deferrable") || parser.check(TokenKind::Comma) {
            parser.advance();
        } else {
            break;
        }
    }
}

/// Parses the optional relation of a `VACUUM` statement.
///
/// Supported forms are `VACUUM` (every table in the current database) and
/// `VACUUM [schema.]table` (one table). PostgreSQL's option words are
/// tolerated: `FULL`/`ANALYZE` and the non-reserved `FREEZE`/`VERBOSE` are
/// skipped so a following relation is still recognized, which keeps
/// `VACUUM FULL t` targeting `t` rather than silently widening to every table.
/// Whatever the tail still holds is left to [`consume_housekeeping_tail`] so
/// the statement stream stays positioned exactly as before.
fn vacuum_table(parser: &mut Parser<'_>) -> Result<Statement, ParseError> {
    loop {
        match parser.peek().kind {
            TokenKind::Keyword(Keyword::Full) | TokenKind::Keyword(Keyword::Analyze) => {
                parser.advance();
            }
            TokenKind::Identifier
                if matches!(
                    parser.peek().lexeme.to_ascii_uppercase().as_str(),
                    "FREEZE" | "VERBOSE"
                ) =>
            {
                parser.advance();
            }
            _ => break,
        }
    }
    let table = if matches!(parser.peek().kind, TokenKind::Identifier) {
        Some(parser.parse_qualified_identifier("table name")?)
    } else {
        None
    };
    consume_housekeeping_tail(parser);
    Ok(Statement::Vacuum { table })
}

fn consume_housekeeping_tail(parser: &mut Parser<'_>) {
    loop {
        if parser.check(TokenKind::SemiColon) || matches!(parser.peek().kind, TokenKind::Eof) {
            break;
        }
        if parser.check(TokenKind::LParen) {
            let mut depth = 1i32;
            parser.advance();
            while depth > 0 {
                match parser.peek().kind {
                    TokenKind::LParen => depth += 1,
                    TokenKind::RParen => depth -= 1,
                    TokenKind::Eof => break,
                    _ => {}
                }
                parser.advance();
            }
        } else {
            parser.advance();
        }
    }
}
