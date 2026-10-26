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
//! Cooperative query guardrails: statement timeout + cancellation.
//!
//! The executor is synchronous: a query runs to completion on the calling
//! thread with no `.await` points to hook. Threading a deadline parameter
//! through every operator would touch the sibling-owned `query/*` paths, so
//! the guard is thread-local instead. The network layer installs
//! (`with_guard`) a deadline + cancel flag around each blocking executor
//! call (which runs via `block_in_place` on the same worker thread); long
//! loops in the freely-owned `join/*` operators poll (`check_guard`) and
//! abort with a clean timeout/cancel error.
//!
//! Safety: `with_guard` saves and restores the previous guard, so nested
//! calls (e.g. Describe probes inside a query) compose. The guard is always
//! cleared when the closure returns, so a worker thread never leaks one
//! query's deadline into the next query scheduled on the same thread.

use crate::error::{SqlError, SqlResult};
use plomid_core::{ErrorKind, PlomidError};
use plomid_htap::{StatementPolicy, TimeoutError};
use std::cell::RefCell;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

thread_local! {
    static DEADLINE: RefCell<Option<Instant>> = const { RefCell::new(None) };
    static CANCEL: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

/// Runs `f` with the given deadline/cancel guard installed on this thread.
///
/// `deadline=None` means no timeout. `cancel=None` means no external cancel.
/// The previous guard (if any) is restored afterwards.
pub fn with_guard<R>(
    deadline: Option<Instant>,
    cancel: Option<Arc<AtomicBool>>,
    f: impl FnOnce() -> R,
) -> R {
    let prev_deadline = DEADLINE.with(|c| c.replace(deadline));
    let prev_cancel = CANCEL.with(|c| c.replace(cancel));
    let result = f();
    DEADLINE.with(|c| c.replace(prev_deadline));
    CANCEL.with(|c| c.replace(prev_cancel));
    result
}

/// Returns a timeout error when the installed deadline has passed, or a
/// cancel error when the installed cancel flag is set. Cheap enough to call
/// in hot loops (one atomic load + at most one `Instant::now`).
pub fn check_guard() -> SqlResult<()> {
    let cancelled = CANCEL.with(|c| {
        c.borrow()
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
    });
    if cancelled {
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Aborted,
            "canceling statement due to user request",
        )));
    }
    let expired = DEADLINE.with(|c| {
        c.borrow()
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(false)
    });
    if expired {
        return Err(SqlError::Storage(PlomidError::new(
            ErrorKind::Aborted,
            "canceling statement due to statement timeout",
        )));
    }
    Ok(())
}

/// True when the last `check_guard` failure was a user cancel (vs. timeout).
pub fn is_cancel_error(err: &SqlError) -> bool {
    let message = match err {
        SqlError::Syntax(e) | SqlError::Storage(e) => e.message().to_ascii_lowercase(),
    };
    message.contains("user request")
}

/// True when `err` is a guard timeout or cancel.
pub fn is_guard_error(err: &SqlError) -> bool {
    let message = match err {
        SqlError::Syntax(e) | SqlError::Storage(e) => e.message().to_ascii_lowercase(),
    };
    message.contains("canceling statement")
}

/// Parses a PostgreSQL timeout parameter into a `Duration`.
///
/// Accepted: bare integers (milliseconds, the PostgreSQL internal unit for
/// `statement_timeout`), plus `ms`/`s`/`min`/`h` suffixes with optional
/// fractional part, case-insensitive, optional surrounding quotes/space.
/// `0` (any unit) and empty/unparseable values return `None` (disabled /
/// fail-open so a client-specific value never breaks the session).
pub fn parse_timeout(value: Option<&str>) -> Option<Duration> {
    let raw = value?.trim().trim_matches('\'').trim_matches('"').trim();
    if raw.is_empty() {
        return None;
    }
    let lower = raw.to_ascii_lowercase();
    // Split numeric prefix from unit suffix.
    let split = lower
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+' || c == '-'))
        .unwrap_or(lower.len());
    let (number, unit) = (lower[..split].trim(), lower[split..].trim());
    if number.is_empty() {
        return None;
    }
    let number: f64 = number.parse().ok()?;
    if !number.is_finite() || number <= 0.0 {
        return None;
    }
    let millis = match unit {
        "" | "ms" | "millisecond" | "milliseconds" => number,
        "s" | "sec" | "secs" | "second" | "seconds" => number * 1_000.0,
        "min" | "mins" | "minute" | "minutes" => number * 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => number * 3_600_000.0,
        _ => return None,
    };
    if !millis.is_finite() || millis <= 0.0 {
        return None;
    }
    // Clamp to something a sleep/deadline can represent.
    let millis = millis.min(u64::MAX as f64 / 2.0);
    let millis = millis.max(1.0) as u64;
    Some(Duration::from_millis(millis))
}

/// Builds the HTAP [`StatementPolicy`] for a session timeout parameter.
///
/// This is the single parsing entry point: `None` (disabled / fail-open)
/// becomes [`StatementPolicy::unlimited`], otherwise the parsed millisecond
/// bound becomes `Some(ms)`. Production deadline computation goes through
/// here so the HTAP policy executes on every guarded query (see
/// `deadline_from_param`, called per query by the network layer).
pub fn policy_from_param(value: Option<&str>) -> StatementPolicy {
    StatementPolicy::new(parse_timeout(value).map(|d| d.as_millis().min(u64::MAX as u128) as u64))
}

/// Converts a session timeout parameter into an absolute deadline.
///
/// Implemented via [`policy_from_param`] + [`StatementPolicy::deadline`] so
/// the HTAP timeout policy (not a duplicate ad-hoc computation) bounds
/// wall-clock execution. `None` means no timeout.
pub fn deadline_from_param(value: Option<&str>) -> Option<Instant> {
    policy_from_param(value).deadline()
}

/// Maps an HTAP [`TimeoutError`] to the guard's timeout [`SqlError`].
///
/// The message preserves the `"canceling statement"` prefix so
/// [`is_guard_error`] still classifies it as a guard failure (and it never
/// contains `"user request"`, so [`is_cancel_error`] stays false). A `From`
/// impl is impossible here (orphan rule: both types are foreign), so this
/// named conversion is the wiring point.
pub fn timeout_error_to_sql(err: TimeoutError) -> SqlError {
    SqlError::Storage(PlomidError::new(
        ErrorKind::Aborted,
        format!("canceling statement due to statement timeout ({err})"),
    ))
}

/// Checks an HTAP [`StatementPolicy`] against its start time, mapping the
/// [`TimeoutError`] to the guard error via [`timeout_error_to_sql`].
///
/// This is the policy-based counterpart to [`check_guard`] (which polls the
/// installed thread-local deadline): callers holding an explicit
/// `(start, policy)` pair — e.g. admission waits with their own bound —
/// enforce the same timeout semantics without duplicating the comparison.
pub fn check_policy(start: Instant, policy: StatementPolicy) -> SqlResult<()> {
    policy.check(start).map_err(timeout_error_to_sql)
}
