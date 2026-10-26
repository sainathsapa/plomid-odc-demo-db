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

//! Structured errors for spillable execution.
//!
//! All spill operators return [`SpillError`]; they never panic on resource
//! exhaustion or cancellation. Callers match on the variant to decide whether
//! to retry with a larger budget, surface "storage exhausted" to the session,
//! or abort the query.

use std::fmt;

/// Structured error for the spill crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpillError {
    /// An in-memory budget (row count / partition count) was misconfigured
    /// (e.g. zero) or a single indivisible unit cannot fit any budget.
    MemoryBudgetExceeded(String),
    /// Bounded temp storage (`temp_limit_bytes` or `max_files`) would be
    /// exceeded by the next write. Disk use is never unbounded.
    StorageExhausted(String),
    /// The caller-set cancellation flag (`Arc<AtomicBool>`) was observed.
    Cancelled,
    /// Underlying filesystem I/O failure.
    Io(String),
    /// A spilled record could not be decoded (corrupt temp file, etc.).
    Decode(String),
}

impl fmt::Display for SpillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemoryBudgetExceeded(m) => write!(f, "memory budget exceeded: {m}"),
            Self::StorageExhausted(m) => write!(f, "spill storage exhausted: {m}"),
            Self::Cancelled => write!(f, "operation cancelled"),
            Self::Io(m) => write!(f, "spill I/O error: {m}"),
            Self::Decode(m) => write!(f, "spill decode error: {m}"),
        }
    }
}

impl std::error::Error for SpillError {}

impl From<std::io::Error> for SpillError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// Convenience alias used across the crate.
pub type Result<T> = std::result::Result<T, SpillError>;
