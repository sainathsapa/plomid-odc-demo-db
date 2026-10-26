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

//! Statement timeouts: wall-clock deadlines, distinct from row limits.
//!
//! A [`StatementPolicy`] bounds *how long* a statement may run. It is
//! orthogonal to row limits (`LIMIT n` bounds *how many rows* a query
//! returns): a query can hit its timeout with zero rows produced, and a
//! `LIMIT 1` query over a huge scan still needs a timeout. Timeouts are
//! enforced by comparing `Instant::now()` against [`StatementPolicy::deadline`];
//! row limits are enforced by the executor's output iterator.

use std::time::{Duration, Instant};

/// Failure when a statement outlives its deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutError {
    limit_ms: u64,
    elapsed_ms: u128,
}

impl TimeoutError {
    pub fn limit_ms(&self) -> u64 {
        self.limit_ms
    }
    pub fn elapsed_ms(&self) -> u128 {
        self.elapsed_ms
    }
}

impl std::fmt::Display for TimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "statement timeout exceeded: ran {}ms with limit {}ms",
            self.elapsed_ms, self.limit_ms
        )
    }
}

impl std::error::Error for TimeoutError {}

/// Wall-clock execution bound for a single statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementPolicy {
    /// `None` means no timeout (unlimited). This is independent of any
    /// `LIMIT n` row cap on the query.
    pub statement_timeout_ms: Option<u64>,
}

impl Default for StatementPolicy {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl StatementPolicy {
    /// No timeout.
    pub fn unlimited() -> Self {
        Self {
            statement_timeout_ms: None,
        }
    }

    /// Timeout of `ms` milliseconds.
    pub fn with_timeout(ms: u64) -> Self {
        Self {
            statement_timeout_ms: Some(ms),
        }
    }

    /// Build from an optional millisecond value.
    pub fn new(statement_timeout_ms: Option<u64>) -> Self {
        Self {
            statement_timeout_ms,
        }
    }

    /// True when there is no timeout.
    pub fn is_unlimited(self) -> bool {
        self.statement_timeout_ms.is_none()
    }

    /// Absolute deadline measured from now, or `None` when unlimited.
    pub fn deadline(self) -> Option<Instant> {
        self.deadline_from(Instant::now())
    }

    /// Absolute deadline measured from `start`, or `None` when unlimited.
    pub fn deadline_from(self, start: Instant) -> Option<Instant> {
        self.statement_timeout_ms
            .map(|ms| start + Duration::from_millis(ms))
    }

    /// True if `start` is already past the timeout.
    pub fn is_expired(self, start: Instant) -> bool {
        match self.statement_timeout_ms {
            None => false,
            Some(ms) => start.elapsed() >= Duration::from_millis(ms),
        }
    }

    /// Check `start` against the timeout.
    pub fn check(self, start: Instant) -> Result<(), TimeoutError> {
        match self.statement_timeout_ms {
            None => Ok(()),
            Some(ms) => {
                let elapsed = start.elapsed();
                if elapsed >= Duration::from_millis(ms) {
                    Err(TimeoutError {
                        limit_ms: ms,
                        elapsed_ms: elapsed.as_millis(),
                    })
                } else {
                    Ok(())
                }
            }
        }
    }
}
