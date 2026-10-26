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

//! Window / dedup note: no separate spill path.
//!
//! Windowed execution in PLOMID is built on two primitives that already exist
//! in this crate:
//!
//! 1. **Ordering + partitioning** via [`crate::ExternalSort`]: `PARTITION BY`
//!    becomes a hash partition (same scheme as [`crate::SpillGroupedAgg`])
//!    and `ORDER BY` becomes an external sort, so window input of any size is
//!    already bounded by `mem_limit_rows` plus one head row per run.
//! 2. **Bounded frame buffers**: window frames (`ROWS` / `RANGE`) only need
//!    the rows inside the current frame. [`BoundedFrameBuffer`] keeps at most
//!    `capacity` rows and evicts the oldest, so frame state is `O(frame)`
//!    rather than `O(partition)`.
//!
//! Dedup (`DISTINCT` without aggregation, `row_number() = 1` filters) reuses
//! [`crate::SpillDistinct`]. A dedicated window spill file would duplicate the
//! sort runs, so this module intentionally provides only the bounded frame
//! helper plus documentation.

use std::collections::VecDeque;

/// Bounded FIFO holding at most `capacity` rows for window-frame evaluation.
///
/// When full, [`BoundedFrameBuffer::push`] evicts the oldest row first, so
/// memory stays `O(capacity)`. It never spills: if a frame legitimately needs
/// more rows than `capacity`, the caller grows the buffer or errors via
/// [`crate::SpillError::MemoryBudgetExceeded`].
#[derive(Debug)]
pub struct BoundedFrameBuffer<T> {
    rows: VecDeque<T>,
    capacity: usize,
}

impl<T> BoundedFrameBuffer<T> {
    /// Create a frame buffer holding at most `capacity` rows.
    ///
    /// # Panics
    ///
    /// Never panics; `capacity == 0` is normalized to `1` so every frame can
    /// at least see the current row.
    pub fn new(capacity: usize) -> Self {
        Self {
            rows: VecDeque::new(),
            capacity: capacity.max(1),
        }
    }

    /// Push a row, evicting the oldest when full. Returns the evicted row, if any.
    pub fn push(&mut self, row: T) -> Option<T> {
        let evicted = if self.rows.len() >= self.capacity {
            self.rows.pop_front()
        } else {
            None
        };
        self.rows.push_back(row);
        evicted
    }

    /// Rows currently in the frame, oldest first.
    pub fn rows(&self) -> impl Iterator<Item = &T> {
        self.rows.iter()
    }

    /// Number of rows currently buffered.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the buffer holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Drop all buffered rows.
    pub fn clear(&mut self) {
        self.rows.clear();
    }
}
