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

//! Structured [`MemoryError`] type. Over-budget allocations return errors,
//! never panic.

use std::fmt;

/// Error returned by fallible memory operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemoryError {
    /// A reservation of `requested` bytes was refused by the context named
    /// in `scope`, which had `available` bytes left under its budget.
    BudgetExceeded {
        /// Human-readable scope of the enforcing context
        /// (e.g. `"global"`, `"query:42"`, `"operator:HashJoin"`).
        scope: String,
        /// Bytes requested.
        requested: u64,
        /// Bytes still available under the enforcing budget.
        available: u64,
    },
}

impl MemoryError {
    /// Scope string of the enforcing context.
    pub fn scope(&self) -> &str {
        match self {
            Self::BudgetExceeded { scope, .. } => scope,
        }
    }
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExceeded {
                scope,
                requested,
                available,
            } => write!(
                f,
                "memory budget exceeded at scope '{scope}': requested {requested} bytes, {available} available"
            ),
        }
    }
}

impl std::error::Error for MemoryError {}
