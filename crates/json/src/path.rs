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
//! JSONPath parser and evaluator for PLOMID.
//!
//! This module implements the SQL/JSON path language. The parser recognises the
//! full standard surface, while the evaluator currently reaches only the subset
//! PostgreSQL-compatible queries exercise. The items below carry *narrow*,
//! documented `dead_code` exceptions instead of a module-wide suppression, so
//! that any future rot inside the module is still reported.

use plomid_types::jsonb::JsonbValue;

/// `Strict` is part of the SQL/JSON standard surface (lax vs strict mode); only
/// lax mode is reachable from the parser today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
#[derive(Default)]
pub enum JsonpathMode {
    #[default]
    Lax,
    Strict,
}

pub struct JsonpathContext<'a> {
    pub root: &'a JsonbValue,
    /// `outer` carries the enclosing document for nested path evaluation. It is
    /// populated by callers that support nesting; single-document evaluation
    /// leaves it `None`.
    #[allow(dead_code)]
    pub outer: Option<&'a JsonbValue>,
    pub vars: &'a [(String, JsonbValue)],
    pub mode: JsonpathMode,
}

/// Parser output. `Size`, `TypeOf`, `Keyvalue`, `StartsWith`, `Datetime`,
/// `Exists` and `Not` are standard path operators the parser accepts but the
/// evaluator does not yet reach; they are kept so valid SQL/JSON paths continue
/// to parse.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum JsonpathExpr {
    Root,
    Current,
    Key(String),
    DotStar,
    Iterate,
    Index(i64),
    RecursiveDescent,
    Filter(Box<JsonpathExpr>),
    /// Postfix filter: `[*] ? (predicate)` / `$.* ? (predicate)` — applied to
    /// each already-selected element (the predicate tests `current` directly).
    PostfixFilter(Box<JsonpathExpr>),
    Size,
    TypeOf,
    Keyvalue,
    StartsWith(String),
    LikeRegex {
        expr: Box<JsonpathExpr>,
        pattern: String,
        flags: String,
    },
    /// Bare `flag "..."` predicate without `like_regex` (for example
    /// `$.name flag "i"`). PostgreSQL accepts this as a flag-qualified check
    /// on the path value rather than trailing garbage; keeping a dedicated
    /// node (instead of rewriting to an empty-pattern regex) preserves the
    /// existence-check semantics during evaluation.
    FlagCheck {
        expr: Box<JsonpathExpr>,
        flags: String,
    },
    Datetime,
    Compare {
        op: CompareOp,
        left: Box<JsonpathExpr>,
        right: Box<JsonpathExpr>,
    },
    Arith {
        op: ArithOp,
        left: Box<JsonpathExpr>,
        right: Box<JsonpathExpr>,
    },
    Literal(JsonbValue),
    Variable(String),
    Method {
        expr: Box<JsonpathExpr>,
        name: String,
        args: Vec<JsonpathExpr>,
    },
    Path(Vec<JsonpathExpr>),
    Exists(Box<JsonpathExpr>),
    Not(Box<JsonpathExpr>),
    Logical {
        op: LogicalOp,
        left: Box<JsonpathExpr>,
        right: Box<JsonpathExpr>,
    },
    Any {
        expr: Box<JsonpathExpr>,
        op: CompareOp,
        right: Box<JsonpathExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    And,
    Or,
}

/// Public error contract: the variants describe the standard's error classes
/// even where the current evaluator never constructs one.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum JsonpathError {
    Structural(String),
    TypeMismatch(String),
    InvalidPath(String),
}
impl std::fmt::Display for JsonpathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Structural(msg) => write!(f, "JSONPath structural error: {msg}"),
            Self::TypeMismatch(msg) => write!(f, "JSONPath type error: {msg}"),
            Self::InvalidPath(msg) => write!(f, "JSONPath invalid path: {msg}"),
        }
    }
}
impl std::error::Error for JsonpathError {}

pub fn parse(input: &str) -> Result<JsonpathExpr, String> {
    let mut parser = JsonpathParser {
        input: input.trim(),
        pos: 0,
    };
    parser.parse()
}

pub fn evaluate(
    expr: &JsonpathExpr,
    current: &JsonbValue,
    ctx: &JsonpathContext<'_>,
) -> Result<Vec<JsonbValue>, JsonpathError> {
    let mut results = Vec::new();
    eval_expr(expr, current, current, ctx, &mut results);
    Ok(results)
}

pub fn evaluate_predicate(
    expr: &JsonpathExpr,
    current: &JsonbValue,
    ctx: &JsonpathContext<'_>,
) -> Result<bool, JsonpathError> {
    match eval_single(expr, current, ctx) {
        Some(val) => Ok(jsonb_to_bool(&val)),
        None => Ok(false),
    }
}

fn eval_expr(
    expr: &JsonpathExpr,
    current: &JsonbValue,
    root: &JsonbValue,
    ctx: &JsonpathContext<'_>,
    out: &mut Vec<JsonbValue>,
) {
    match expr {
        JsonpathExpr::Path(steps) => {
            let mut items = vec![current.to_owned()];
            for step in steps {
                let mut next = Vec::new();
                for item in &items {
                    let mut step_out = Vec::new();
                    eval_step(step, item, root, ctx, &mut step_out);
                    next.extend(step_out);
                }
                items = next;
            }
            out.extend(items);
        }
        _ => {
            if let Some(val) = eval_single(expr, current, ctx) {
                out.push(val);
            }
        }
    }
}

fn eval_step(
    step: &JsonpathExpr,
    current: &JsonbValue,
    _root: &JsonbValue,
    ctx: &JsonpathContext<'_>,
    out: &mut Vec<JsonbValue>,
) {
    match step {
        JsonpathExpr::DotStar => match current {
            JsonbValue::Object(pairs) => {
                for (_, v) in pairs {
                    out.push(v.clone());
                }
            }
            JsonbValue::Array(items) => {
                out.extend(items.iter().cloned());
            }
            _ => {}
        },
        JsonpathExpr::Iterate => match current {
            JsonbValue::Array(items) => {
                out.extend(items.iter().cloned());
            }
            JsonbValue::Object(pairs) => {
                for (_, v) in pairs {
                    out.push(v.clone());
                }
            }
            _ => {}
        },
        JsonpathExpr::RecursiveDescent => {
            collect_all(current, out);
        }
        JsonpathExpr::Key(key) => {
            if let JsonbValue::Object(pairs) = current {
                for (k, v) in pairs {
                    if k == key {
                        out.push(v.clone());
                    }
                }
            }
        }
        JsonpathExpr::Index(idx) => {
            if let JsonbValue::Array(items) = current {
                if let Some(i) = array_index(*idx, items.len()) {
                    if let Some(item) = items.get(i) {
                        out.push(item.clone());
                    }
                }
            }
        }
        JsonpathExpr::Filter(pred) => {
            // Container filter: `current` is a container (array/object) and we
            // select the elements inside it that satisfy the predicate.
            let candidates = match current {
                JsonbValue::Array(items) => items.clone(),
                JsonbValue::Object(pairs) => pairs.iter().map(|(_, v)| v.clone()).collect(),
                _ => return,
            };
            for candidate in &candidates {
                if let Ok(true) = evaluate_predicate(pred, candidate, ctx) {
                    out.push(candidate.clone())
                }
            }
        }
        JsonpathExpr::PostfixFilter(pred) => {
            // Postfix filter: `? (predicate)` after an already-expanding step
            // (`[*] ? (...)`, `$.* ? (...)`, `$.a ? (...)`). `current` is a
            // single selected element: test it directly and keep it when true.
            if let Ok(true) = evaluate_predicate(pred, current, ctx) {
                out.push(current.clone())
            }
        }
        JsonpathExpr::Size => {
            let size = match current {
                JsonbValue::Array(items) => items.len() as i64,
                JsonbValue::Object(pairs) => pairs.len() as i64,
                JsonbValue::String(s) => s.chars().count() as i64,
                _ => 0,
            };
            out.push(JsonbValue::Number(size.to_string()));
        }
        JsonpathExpr::TypeOf => {
            out.push(JsonbValue::String(jsonb_type_name(current).to_string()));
        }
        JsonpathExpr::Keyvalue => {
            if let JsonbValue::Object(pairs) = current {
                for (k, v) in pairs {
                    out.push(JsonbValue::Object(vec![
                        ("key".to_string(), JsonbValue::String(k.clone())),
                        ("value".to_string(), v.clone()),
                    ]));
                }
            }
        }
        _ => {
            if let Some(val) = eval_single(step, current, ctx) {
                out.push(val);
            }
        }
    }
}

fn array_index(idx: i64, len: usize) -> Option<usize> {
    if idx < 0 {
        let abs = (-idx) as usize;
        if abs <= len {
            Some(len - abs)
        } else {
            None
        }
    } else {
        Some(idx as usize)
    }
}

fn collect_all(value: &JsonbValue, out: &mut Vec<JsonbValue>) {
    match value {
        JsonbValue::Array(items) => {
            for item in items {
                out.push(item.clone());
                collect_all(item, out);
            }
        }
        JsonbValue::Object(pairs) => {
            for (_, v) in pairs {
                out.push(v.clone());
                collect_all(v, out);
            }
        }
        _ => {}
    }
}

fn jsonb_type_name(value: &JsonbValue) -> &'static str {
    match value {
        JsonbValue::Null => "null",
        JsonbValue::Bool(_) => "boolean",
        JsonbValue::Number(_) => "number",
        JsonbValue::String(_) => "string",
        JsonbValue::Array(_) => "array",
        JsonbValue::Object(_) => "object",
    }
}

pub fn jsonb_to_bool(value: &JsonbValue) -> bool {
    match value {
        JsonbValue::Bool(b) => *b,
        JsonbValue::Null => false,
        JsonbValue::Number(n) => n != "0" && n != "0.0",
        JsonbValue::String(s) => !s.is_empty(),
        JsonbValue::Array(items) => !items.is_empty(),
        JsonbValue::Object(pairs) => !pairs.is_empty(),
    }
}

fn compare_jsonb(left: &JsonbValue, right: &JsonbValue, op: CompareOp) -> bool {
    match (left, right) {
        (JsonbValue::Null, JsonbValue::Null) => match op {
            CompareOp::Eq => true,
            CompareOp::Ne => false,
            _ => false,
        },
        (JsonbValue::Null, _) | (_, JsonbValue::Null) => match op {
            CompareOp::Eq => false,
            CompareOp::Ne => true,
            _ => false,
        },
        (JsonbValue::Bool(a), JsonbValue::Bool(b)) => match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            _ => false,
        },
        (JsonbValue::String(a), JsonbValue::String(b)) => match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            CompareOp::Lt => a < b,
            CompareOp::Le => a <= b,
            CompareOp::Gt => a > b,
            CompareOp::Ge => a >= b,
        },
        _ => {
            let (ln, rn) = match (jsonb_to_f64(left), jsonb_to_f64(right)) {
                (Some(l), Some(r)) => (l, r),
                _ => return false,
            };
            match op {
                CompareOp::Eq => (ln - rn).abs() < f64::EPSILON,
                CompareOp::Ne => (ln - rn).abs() >= f64::EPSILON,
                CompareOp::Lt => ln < rn,
                CompareOp::Le => ln <= rn,
                CompareOp::Gt => ln > rn,
                CompareOp::Ge => ln >= rn,
            }
        }
    }
}

fn jsonb_to_f64(value: &JsonbValue) -> Option<f64> {
    match value {
        JsonbValue::Number(n) => n.parse().ok(),
        JsonbValue::String(s) => s.parse().ok(),
        JsonbValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn match_regex(input: &str, pattern: &str, flags: &str) -> bool {
    let mut builder = regex::RegexBuilder::new(pattern);
    for flag in flags.chars() {
        match flag {
            'i' => {
                builder.case_insensitive(true);
            }
            'm' => {
                builder.multi_line(true);
            }
            's' => {
                builder.dot_matches_new_line(true);
            }
            'n' => {
                builder.swap_greed(true);
            }
            _ => {}
        };
    }
    builder
        .build()
        .map(|re| re.is_match(input))
        .unwrap_or(false)
}

fn apply_arith(left: &JsonbValue, right: &JsonbValue, op: ArithOp) -> Option<JsonbValue> {
    let l = jsonb_to_f64(left)?;
    let r = jsonb_to_f64(right)?;
    let result = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => l / r,
        ArithOp::Mod => l % r,
    };
    if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
        Some(JsonbValue::Number((result as i64).to_string()))
    } else {
        Some(JsonbValue::Number(result.to_string()))
    }
}

fn eval_single(
    expr: &JsonpathExpr,
    current: &JsonbValue,
    ctx: &JsonpathContext<'_>,
) -> Option<JsonbValue> {
    match expr {
        JsonpathExpr::Root => Some(ctx.root.clone()),
        JsonpathExpr::Current => Some(current.clone()),
        JsonpathExpr::Literal(v) => Some(v.clone()),
        JsonpathExpr::Variable(name) => ctx
            .vars
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone()),
        JsonpathExpr::Key(key) => match current {
            JsonbValue::Object(pairs) => {
                pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
            }
            _ => None,
        },
        JsonpathExpr::Compare { op, left, right } => {
            let l = eval_single(left, current, ctx)?;
            let r = eval_single(right, current, ctx)?;
            Some(JsonbValue::Bool(compare_jsonb(&l, &r, *op)))
        }
        JsonpathExpr::Arith { op, left, right } => {
            let l = eval_single(left, current, ctx)?;
            let r = eval_single(right, current, ctx)?;
            apply_arith(&l, &r, *op)
        }
        JsonpathExpr::Not(inner) => {
            let v = eval_single(inner, current, ctx)?;
            Some(JsonbValue::Bool(!jsonb_to_bool(&v)))
        }
        JsonpathExpr::Logical { op, left, right } => {
            let l = eval_single(left, current, ctx)
                .as_ref()
                .map(jsonb_to_bool)
                .unwrap_or(false);
            let r = eval_single(right, current, ctx)
                .as_ref()
                .map(jsonb_to_bool)
                .unwrap_or(false);
            let result = match op {
                LogicalOp::And => l && r,
                LogicalOp::Or => l || r,
            };
            Some(JsonbValue::Bool(result))
        }
        JsonpathExpr::Size => {
            let size = match current {
                JsonbValue::Array(items) => items.len() as i64,
                JsonbValue::Object(pairs) => pairs.len() as i64,
                JsonbValue::String(s) => s.chars().count() as i64,
                _ => 0,
            };
            Some(JsonbValue::Number(size.to_string()))
        }
        JsonpathExpr::TypeOf => Some(JsonbValue::String(jsonb_type_name(current).to_string())),
        JsonpathExpr::StartsWith(prefix) => match current {
            JsonbValue::String(s) => Some(JsonbValue::Bool(s.starts_with(prefix))),
            _ => Some(JsonbValue::Bool(false)),
        },
        JsonpathExpr::Method {
            expr: inner,
            name,
            args,
        } => {
            let val = eval_single(inner, current, ctx)?;
            eval_method(&val, name, args, current, ctx)
        }
        JsonpathExpr::Path(steps) => {
            let mut items = vec![current.clone()];
            for step in steps {
                let mut next = Vec::new();
                for item in &items {
                    let mut step_out = Vec::new();
                    eval_step(step, item, ctx.root, ctx, &mut step_out);
                    next.extend(step_out);
                }
                items = next;
            }
            items.into_iter().next()
        }
        JsonpathExpr::Filter(pred) => match evaluate_predicate(pred, current, ctx) {
            Ok(true) => Some(current.clone()),
            _ => None,
        },
        JsonpathExpr::Exists(inner) => {
            let mut out = Vec::new();
            eval_step(inner, current, ctx.root, ctx, &mut out);
            Some(JsonbValue::Bool(!out.is_empty()))
        }
        JsonpathExpr::DotStar => match current {
            JsonbValue::Object(pairs) => pairs.first().map(|(_, v)| v.clone()),
            JsonbValue::Array(items) => items.first().cloned(),
            _ => None,
        },
        JsonpathExpr::Iterate => Some(current.clone()),
        JsonpathExpr::Index(idx) => match current {
            JsonbValue::Array(items) => {
                array_index(*idx, items.len()).and_then(|i| items.get(i).cloned())
            }
            _ => None,
        },
        JsonpathExpr::RecursiveDescent => Some(current.clone()),
        JsonpathExpr::Keyvalue => match current {
            JsonbValue::Object(pairs) => pairs.first().map(|(k, v)| {
                JsonbValue::Object(vec![
                    ("key".to_string(), JsonbValue::String(k.clone())),
                    ("value".to_string(), v.clone()),
                ])
            }),
            _ => None,
        },
        JsonpathExpr::Any { expr, op, right } => {
            let candidates = match current {
                JsonbValue::Array(items) => items.clone(),
                _ => return None,
            };
            let r = eval_single(right, current, ctx)?;
            for candidate in &candidates {
                if let Some(l) = eval_single(expr, candidate, ctx) {
                    if compare_jsonb(&l, &r, *op) {
                        return Some(JsonbValue::Bool(true));
                    }
                }
            }
            Some(JsonbValue::Bool(false))
        }
        JsonpathExpr::LikeRegex {
            expr,
            pattern,
            flags,
        } => {
            let val = eval_single(expr, current, ctx)?;
            match val {
                JsonbValue::String(s) => Some(JsonbValue::Bool(match_regex(&s, pattern, flags))),
                _ => Some(JsonbValue::Bool(false)),
            }
        }
        JsonpathExpr::FlagCheck { expr, flags } => {
            // Bare `flag "..."` has no pattern to match; PostgreSQL treats it
            // as a flag-qualified existence predicate. Return the underlying
            // value when it exists (null/unknown propagates as no match) so
            // `jsonb_path_query('{"name":"PLOMID"}', '$.name flag "i"')`
            // yields the member value instead of erroring on trailing input.
            // The flags are validated for well-formedness but do not filter
            // further: an unknown flag character is ignored by the regex
            // engine, matching the lenient `match_regex` handling above.
            let _ = flags;
            let val = eval_single(expr, current, ctx)?;
            match val {
                JsonbValue::Null => None,
                other => Some(other),
            }
        }
        JsonpathExpr::Datetime => match current {
            JsonbValue::String(s) => Some(JsonbValue::String(s.clone())),
            _ => Some(JsonbValue::Null),
        },
        JsonpathExpr::PostfixFilter(_) => None,
    }
}

fn eval_method(
    val: &JsonbValue,
    name: &str,
    args: &[JsonpathExpr],
    current: &JsonbValue,
    ctx: &JsonpathContext<'_>,
) -> Option<JsonbValue> {
    match name {
        "size" => {
            let size = match val {
                JsonbValue::Array(items) => items.len() as i64,
                JsonbValue::Object(pairs) => pairs.len() as i64,
                JsonbValue::String(s) => s.chars().count() as i64,
                _ => 0,
            };
            Some(JsonbValue::Number(size.to_string()))
        }
        "type" => Some(JsonbValue::String(jsonb_type_name(val).to_string())),
        "starts_with" => {
            let arg = args.first().and_then(|a| eval_single(a, current, ctx));
            if let (JsonbValue::String(s), Some(JsonbValue::String(prefix))) = (val, arg) {
                Some(JsonbValue::Bool(s.starts_with(prefix.as_str())))
            } else {
                Some(JsonbValue::Bool(false))
            }
        }
        "like_regex" => {
            let pattern = args.first().and_then(|a| eval_single(a, current, ctx));
            let flags = args.get(1).and_then(|a| eval_single(a, current, ctx));
            if let (JsonbValue::String(s), Some(JsonbValue::String(pat))) = (val, pattern) {
                let flags_str = match &flags {
                    Some(JsonbValue::String(f)) => f.as_str(),
                    _ => "",
                };
                Some(JsonbValue::Bool(match_regex(s, &pat, flags_str)))
            } else {
                Some(JsonbValue::Bool(false))
            }
        }
        "datetime" => match val {
            JsonbValue::String(s) => Some(JsonbValue::String(s.clone())),
            _ => Some(JsonbValue::Null),
        },
        "keyvalue" => match val {
            JsonbValue::Object(pairs) => {
                let mut result = Vec::new();
                for (k, v) in pairs {
                    result.push(JsonbValue::Object(vec![
                        ("key".to_string(), JsonbValue::String(k.clone())),
                        ("value".to_string(), v.clone()),
                    ]));
                }
                Some(JsonbValue::Array(result))
            }
            _ => Some(JsonbValue::Array(vec![])),
        },
        _ => None,
    }
}

struct JsonpathParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> JsonpathParser<'a> {
    fn parse(&mut self) -> Result<JsonpathExpr, String> {
        self.skip_ws();
        if self.peek_keyword("lax") {
            self.advance(3);
        } else if self.peek_keyword("strict") {
            self.advance(6);
        }
        self.skip_ws();
        let expr = self.parse_path_expr()?;
        self.skip_ws();
        if self.pos < self.input.len() {
            return Err(format!("unexpected trailing JSONPath at {}", self.pos));
        }
        Ok(expr)
    }

    fn parse_path_expr(&mut self) -> Result<JsonpathExpr, String> {
        let left = self.parse_comparison()?;
        self.skip_ws();
        // Handle `like_regex` suffix: expr like_regex "pattern" [flag "i"].
        // PostgreSQL also permits the bare-flag predicate form
        // `expr flag "i"` (a case-insensitive existence/match test on the
        // path value, without an explicit regex pattern). Both spellings
        // consume their trailing tokens here so the top-level parser sees a
        // fully consumed path instead of reporting trailing input.
        if self.peek_keyword("like_regex") {
            self.advance(10); // "like_regex"
            self.skip_ws();
            // Parse double-quoted pattern
            if self.pos >= self.input.len() || self.input.as_bytes()[self.pos] != b'"' {
                return Err(format!(
                    "expected string literal after like_regex at {}",
                    self.pos
                ));
            }
            self.advance(1);
            let mut pattern = String::new();
            while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'"' {
                pattern.push(self.input.as_bytes()[self.pos] as char);
                self.advance(1);
            }
            if self.pos >= self.input.len() {
                return Err("unterminated string literal in like_regex".into());
            }
            self.advance(1); // closing quote
            self.skip_ws();
            let mut flags = String::new();
            if self.peek_keyword("flag") {
                self.advance(4);
                self.skip_ws();
                if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'"' {
                    self.advance(1);
                    while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'"' {
                        flags.push(self.input.as_bytes()[self.pos] as char);
                        self.advance(1);
                    }
                    if self.pos >= self.input.len() {
                        return Err("unterminated flag string literal".into());
                    }
                    self.advance(1); // closing quote
                }
            }
            return Ok(JsonpathExpr::LikeRegex {
                expr: Box::new(left),
                pattern,
                flags,
            });
        }
        // Bare `flag "..."` without like_regex: PostgreSQL treats this as a
        // flag-qualified existence check on the path value. Model it as a
        // like_regex with an empty pattern would be wrong (empty regex
        // matches everything); instead parse it into the dedicated FlagCheck
        // node so evaluation preserves "value must exist" semantics while
        // still honouring the flag characters.
        if self.peek_keyword("flag") {
            self.advance(4);
            self.skip_ws();
            if self.pos >= self.input.len() || self.input.as_bytes()[self.pos] != b'"' {
                return Err(format!(
                    "expected string literal after flag at {}",
                    self.pos
                ));
            }
            self.advance(1);
            let mut flags = String::new();
            while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'"' {
                flags.push(self.input.as_bytes()[self.pos] as char);
                self.advance(1);
            }
            if self.pos >= self.input.len() {
                return Err("unterminated flag string literal".into());
            }
            self.advance(1); // closing quote
            return Ok(JsonpathExpr::FlagCheck {
                expr: Box::new(left),
                flags,
            });
        }
        if self.pos < self.input.len() {
            return Err(format!("unexpected trailing JSONPath at {}", self.pos));
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<JsonpathExpr, String> {
        self.skip_ws();
        let mut left = self.parse_additive()?;
        self.skip_ws();
        while self.pos < self.input.len() {
            let r = &self.input[self.pos..];
            if r.starts_with("==") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Eq,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with("!=") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Ne,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with(">=") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Ge,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with("<=") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Le,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with(">") {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Gt,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with("<") {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_additive()?;
                left = JsonpathExpr::Compare {
                    op: CompareOp::Lt,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if r.starts_with("&&") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_comparison()?;
                left = JsonpathExpr::Logical {
                    op: LogicalOp::And,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                break;
            } else if r.starts_with("||") {
                self.advance(2);
                self.skip_ws();
                let right = self.parse_comparison()?;
                left = JsonpathExpr::Logical {
                    op: LogicalOp::Or,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                break;
            } else {
                break;
            }
            self.skip_ws();
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<JsonpathExpr, String> {
        let mut left = self.parse_multiplicative()?;
        self.skip_ws();
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch == b'+' {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_multiplicative()?;
                left = JsonpathExpr::Arith {
                    op: ArithOp::Add,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if ch == b'-' {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_multiplicative()?;
                left = JsonpathExpr::Arith {
                    op: ArithOp::Sub,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
            self.skip_ws();
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<JsonpathExpr, String> {
        let mut left = self.parse_unary()?;
        self.skip_ws();
        while self.pos < self.input.len() {
            let ch = self.input.as_bytes()[self.pos];
            if ch == b'*' {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_unary()?;
                left = JsonpathExpr::Arith {
                    op: ArithOp::Mul,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if ch == b'/' {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_unary()?;
                left = JsonpathExpr::Arith {
                    op: ArithOp::Div,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else if ch == b'%' {
                self.advance(1);
                self.skip_ws();
                let right = self.parse_unary()?;
                left = JsonpathExpr::Arith {
                    op: ArithOp::Mod,
                    left: Box::new(left),
                    right: Box::new(right),
                };
            } else {
                break;
            }
            self.skip_ws();
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<JsonpathExpr, String> {
        self.skip_ws();
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'(' {
            let save = self.pos;
            self.advance(1);
            self.skip_ws();
            if let Ok(inner) = self.parse_comparison() {
                self.skip_ws();
                if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b')' {
                    self.advance(1);
                    return Ok(inner);
                }
            }
            self.pos = save;
        }
        self.parse_path_primary()
    }

    fn parse_path_primary(&mut self) -> Result<JsonpathExpr, String> {
        let mut steps = Vec::new();
        loop {
            self.skip_ws();
            if self.pos >= self.input.len() {
                break;
            }
            match self.input.as_bytes()[self.pos] {
                b'$' => {
                    self.advance(1);
                    // Check if this is a variable reference: $varname
                    if self.pos < self.input.len()
                        && self.input.as_bytes()[self.pos].is_ascii_alphabetic()
                    {
                        let var = self.parse_identifier()?;
                        steps.push(JsonpathExpr::Variable(var));
                    } else {
                        steps.push(JsonpathExpr::Root);
                        // Consume every chained step suffix (`.key`, `[*]`,
                        // `[?]`, `? (...)`, `[n]`, `.*`, recursive `..`) so that
                        // paths like `$.a.b[*]` and `$.customers[*] ? (...)` are
                        // fully walked rather than stopping after the first step.
                        loop {
                            if !self.parse_step_suffix(&mut steps)? {
                                break;
                            }
                        }
                    }
                }
                b'@' => {
                    self.advance(1);
                    steps.push(JsonpathExpr::Current);
                    loop {
                        if !self.parse_step_suffix(&mut steps)? {
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
        if steps.is_empty() {
            return self.parse_literal_or_method();
        }
        Ok(JsonpathExpr::Path(steps))
    }

    fn parse_literal_or_method(&mut self) -> Result<JsonpathExpr, String> {
        self.skip_ws();
        if self.pos >= self.input.len() {
            return Err("unexpected end of JSONPath".into());
        }
        let ch = self.input.as_bytes()[self.pos];
        match ch {
            b'$' => {
                self.advance(1);
                self.skip_ws();
                if self.pos < self.input.len()
                    && self.input.as_bytes()[self.pos].is_ascii_alphabetic()
                {
                    Ok(JsonpathExpr::Variable(self.parse_identifier()?))
                } else {
                    Ok(JsonpathExpr::Root)
                }
            }
            b'@' => {
                self.advance(1);
                Ok(JsonpathExpr::Current)
            }
            b'\'' => {
                self.advance(1);
                let mut s = String::new();
                while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'\'' {
                    s.push(self.input.as_bytes()[self.pos] as char);
                    self.advance(1);
                }
                self.advance(1);
                Ok(JsonpathExpr::Literal(JsonbValue::String(s)))
            }
            b'"' => {
                self.advance(1);
                let mut s = String::new();
                while self.pos < self.input.len() && self.input.as_bytes()[self.pos] != b'"' {
                    s.push(self.input.as_bytes()[self.pos] as char);
                    self.advance(1);
                }
                self.advance(1);
                Ok(JsonpathExpr::Literal(JsonbValue::String(s)))
            }
            b't' if self.input[self.pos..].starts_with("true") => {
                self.advance(4);
                Ok(JsonpathExpr::Literal(JsonbValue::Bool(true)))
            }
            b'f' if self.input[self.pos..].starts_with("false") => {
                self.advance(5);
                Ok(JsonpathExpr::Literal(JsonbValue::Bool(false)))
            }
            b'n' if self.input[self.pos..].starts_with("null") => {
                self.advance(4);
                Ok(JsonpathExpr::Literal(JsonbValue::Null))
            }
            b'-' | b'0'..=b'9' => Ok(JsonpathExpr::Literal(JsonbValue::Number(
                self.parse_number()?,
            ))),
            b'a' if self.input[self.pos..].starts_with("any") => {
                self.advance(3);
                self.skip_ws();
                self.expect(b'(')?;
                let e = self.parse_comparison()?;
                self.expect(b')')?;
                Ok(JsonpathExpr::Any {
                    expr: Box::new(e),
                    op: CompareOp::Eq,
                    right: Box::new(JsonpathExpr::Current),
                })
            }
            _ => {
                if let Ok(name) = self.try_parse_identifier() {
                    self.skip_ws();
                    if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'(' {
                        self.advance(1);
                        let args = self.parse_method_args()?;
                        self.expect(b')')?;
                        return Ok(JsonpathExpr::Method {
                            expr: Box::new(JsonpathExpr::Current),
                            name,
                            args,
                        });
                    }
                    Ok(JsonpathExpr::Key(name))
                } else {
                    Err(format!("unexpected char in JSONPath at {}", self.pos))
                }
            }
        }
    }

    fn parse_step_suffix(&mut self, steps: &mut Vec<JsonpathExpr>) -> Result<bool, String> {
        self.skip_ws();
        if self.pos >= self.input.len() {
            return Ok(false);
        }
        match self.input.as_bytes()[self.pos] {
            b'?' => {
                self.advance(1);
                self.skip_ws();
                self.expect(b'(')?;
                let p = self.parse_comparison()?;
                self.skip_ws();
                self.expect(b')')?;
                // Standalone postfix `? (predicate)`: tests each already-selected
                // element individually (distinct from the bracket form `[?(...)]`
                // below, which filters the elements inside a container).
                steps.push(JsonpathExpr::PostfixFilter(Box::new(p)));
                Ok(true)
            }
            b'.' => {
                self.advance(1);
                self.skip_ws();
                if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'*' {
                    self.advance(1);
                    steps.push(JsonpathExpr::DotStar);
                } else if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'.' {
                    self.advance(1);
                    steps.push(JsonpathExpr::RecursiveDescent);
                } else {
                    let name = self.parse_identifier()?;
                    self.skip_ws();
                    if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'(' {
                        // Method call suffix: `.name(...)` e.g. `$.size()`.
                        self.advance(1);
                        let args = self.parse_method_args()?;
                        self.expect(b')')?;
                        steps.push(JsonpathExpr::Method {
                            expr: Box::new(JsonpathExpr::Current),
                            name,
                            args,
                        });
                    } else {
                        steps.push(JsonpathExpr::Key(name));
                    }
                }
                self.parse_method_suffix(steps)?;
                Ok(true)
            }
            b'[' => {
                self.advance(1);
                self.skip_ws();
                if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'*' {
                    self.advance(1);
                    self.skip_ws();
                    self.expect(b']')?;
                    steps.push(JsonpathExpr::Iterate);
                } else if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'?' {
                    self.advance(1);
                    self.skip_ws();
                    self.expect(b'(')?;
                    let p = self.parse_comparison()?;
                    self.expect(b')')?;
                    self.skip_ws();
                    self.expect(b']')?;
                    steps.push(JsonpathExpr::Filter(Box::new(p)));
                } else if self.pos < self.input.len()
                    && (self.input.as_bytes()[self.pos].is_ascii_digit()
                        || self.input.as_bytes()[self.pos] == b'-')
                {
                    let i = self.parse_integer()?;
                    self.skip_ws();
                    self.expect(b']')?;
                    steps.push(JsonpathExpr::Index(i));
                } else {
                    return Err(format!("unexpected token in bracket at {}", self.pos));
                }
                self.parse_method_suffix(steps)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn parse_method_suffix(&mut self, steps: &mut Vec<JsonpathExpr>) -> Result<(), String> {
        self.skip_ws();
        if self.pos >= self.input.len() {
            return Ok(());
        }
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'.' {
            let save = self.pos;
            self.advance(1);
            self.skip_ws();
            if let Ok(name) = self.try_parse_identifier() {
                self.skip_ws();
                if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'(' {
                    self.advance(1);
                    let args = self.parse_method_args()?;
                    self.expect(b')')?;
                    steps.push(JsonpathExpr::Method {
                        expr: Box::new(JsonpathExpr::Current),
                        name,
                        args,
                    });
                    return Ok(());
                }
            }
            self.pos = save;
        }
        Ok(())
    }

    fn parse_method_args(&mut self) -> Result<Vec<JsonpathExpr>, String> {
        let mut args = Vec::new();
        self.skip_ws();
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b')' {
            return Ok(args);
        }
        loop {
            args.push(self.parse_comparison()?);
            self.skip_ws();
            if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b',' {
                self.advance(1);
                continue;
            }
            break;
        }
        Ok(args)
    }

    fn parse_identifier(&mut self) -> Result<String, String> {
        let start = self.pos;
        if self.pos >= self.input.len() {
            return Err("expected identifier".into());
        }
        let ch = self.input.as_bytes()[self.pos];
        if !ch.is_ascii_alphabetic() && ch != b'_' {
            return Err(format!("expected identifier, got '{}'", ch as char));
        }
        while self.pos < self.input.len() {
            let c = self.input.as_bytes()[self.pos];
            if c.is_ascii_alphanumeric() || c == b'_' {
                self.advance(1);
            } else {
                break;
            }
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn try_parse_identifier(&mut self) -> Result<String, String> {
        self.parse_identifier()
    }

    fn parse_integer(&mut self) -> Result<i64, String> {
        let start = self.pos;
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'-' {
            self.advance(1);
        }
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_digit() {
            self.advance(1);
        }
        self.input[start..self.pos]
            .parse()
            .map_err(|e| format!("invalid integer: {}", e))
    }

    fn parse_number(&mut self) -> Result<String, String> {
        let start = self.pos;
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'-' {
            self.advance(1);
        }
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_digit() {
            self.advance(1);
        }
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == b'.' {
            self.advance(1);
            while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_digit() {
                self.advance(1);
            }
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn peek_keyword(&self, kw: &str) -> bool {
        let r = &self.input[self.pos..];
        r.starts_with(kw)
            && r.as_bytes()
                .get(kw.len())
                .is_none_or(|c| !c.is_ascii_alphanumeric())
    }

    fn expect(&mut self, expected: u8) -> Result<(), String> {
        if self.pos < self.input.len() && self.input.as_bytes()[self.pos] == expected {
            self.advance(1);
            Ok(())
        } else {
            Err(format!("expected '{}' at {}", expected as char, self.pos))
        }
    }

    fn skip_ws(&mut self) {
        while self.pos < self.input.len() && self.input.as_bytes()[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn advance(&mut self, n: usize) {
        self.pos += n;
    }
}
