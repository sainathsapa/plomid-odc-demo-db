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

//! Bounded temp storage and spillable operators.
//!
//! # Ownership
//!
//! Owns spill files, byte/file budgets, and run lifecycle. Never owns the
//! operators that spill (executor query kernels call in), the memory
//! budgets that trigger spilling (memory), or scheduling.
//!
//! When in-memory budgets are exceeded, operators spill sorted runs or hash
//! partitions to temp files through [`SpillManager`] instead of growing the
//! heap. Every write checks `temp_limit_bytes`, every loop checks an
//! `Arc<AtomicBool>` cancellation flag, and every failure is a
//! [`SpillError`] — operators never panic.
//!
//! # Operators
//!
//! - [`ExternalSort`]: buffer-to-limit, spill sorted runs, k-way merge.
//! - [`SpillGroupedAgg`]: grouped count/sum with partition-to-disk fallback.
//! - [`SpillHashJoin`]: partitioned grace join for an over-budget build side.
//! - [`SpillDistinct`] / [`SpillSetOp`]: hash-partitioned dedup and
//!   union/intersect/except.
//! - [`window`]: note + [`window::BoundedFrameBuffer`] — window execution is
//!   `ExternalSort` plus bounded frame buffers, no separate spill path.
//!
//! # Memory accounting
//!
//! Each operator takes an optional `charge_fn: Arc<dyn Fn(i64)>` called with
//! `+bytes` on allocation and `-bytes` on free. The default is a no-op; wiring
//! a future `MemoryManager` in is one line (`with_charge(manager.hook())`).

#![forbid(unsafe_code)]

pub mod agg;
pub mod codec;
pub mod distinct;
pub mod error;
pub mod join;
pub mod manager;
pub mod sort;
pub mod window;

pub use agg::SpillGroupedAgg;
pub use codec::SpillEncode;
pub use distinct::{SetOpKind, SpillDistinct, SpillSetOp};
pub use error::{Result, SpillError};
pub use join::{JoinRow, SpillHashJoin};
pub use manager::{
    SpillConfig, SpillManager, SpillRun, DEFAULT_MAX_FILES, DEFAULT_TEMP_LIMIT_BYTES,
};
pub use sort::{ExternalSort, SortedIter};
pub use window::BoundedFrameBuffer;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Memory-accounting hook: called with `+bytes` on alloc, `-bytes` on free.
///
/// Pass to any operator's `with_charge`. The default ([`noop_charge`]) ignores
/// both directions; a future `MemoryManager` integration replaces it with a
/// hook that debits/credits the query budget.
pub type ChargeFn = Arc<dyn Fn(i64) + Send + Sync>;

/// Default no-op [`ChargeFn`].
pub fn noop_charge() -> ChargeFn {
    Arc::new(|_| {})
}

/// Fail with [`SpillError::Cancelled`] when `flag` is set.
pub fn check_cancelled(flag: &AtomicBool) -> Result<()> {
    if flag.load(Ordering::Relaxed) {
        return Err(SpillError::Cancelled);
    }
    Ok(())
}

pub(crate) fn cancelled(flag: &AtomicBool) -> Result<()> {
    check_cancelled(flag)
}
