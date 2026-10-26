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
//! Bounded top-k heap with deterministic tie-breaking.
//!
//! The heap never holds more than `k` entries (`O(k)` memory). Eviction
//! compares `(distance, row_id)` so equal distances keep the smaller
//! `row_id` — identical input always yields identical output.

use super::candidate::Candidate;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Which end of the ordering wins a slot in the heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TopKMode {
    /// Smaller distance wins (cosine / L2 / `<#>`).
    Smallest,
    /// Larger score wins (raw inner product used as a similarity).
    Largest,
}

/// Heap entry. Ordered so the *worst* entry compares greatest and therefore
/// sits at the top of the max-heap for `O(1)` eviction checks.
///
/// * `Smallest`: worst = largest distance; ties → largest `row_id`.
/// * `Largest`: worst = smallest score; ties → largest `row_id`.
#[derive(Debug, Clone, Copy)]
struct HeapEntry {
    distance: f32,
    row_id: u64,
    largest_wins: bool,
}

impl HeapEntry {
    fn key(&self) -> Candidate {
        Candidate::new(self.row_id, self.distance)
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.largest_wins == other.largest_wins
            && self.distance.to_bits() == other.distance.to_bits()
            && self.row_id == other.row_id
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(
            self.largest_wins, other.largest_wins,
            "mixed TopK modes in one heap"
        );
        if self.largest_wins {
            other
                .distance
                .total_cmp(&self.distance)
                .then_with(|| other.row_id.cmp(&self.row_id))
        } else {
            self.distance
                .total_cmp(&other.distance)
                .then_with(|| self.row_id.cmp(&other.row_id))
        }
    }
}

/// Bounded top-k accumulator.
#[derive(Debug, Clone)]
pub struct TopKHeap {
    k: usize,
    mode: TopKMode,
    heap: BinaryHeap<HeapEntry>,
}

impl TopKHeap {
    /// Create a heap keeping the best `k` entries under `mode`.
    ///
    /// `k == 0` is legal (every [`push`](Self::push) is a no-op). The initial
    /// allocation is capped at 1024 entries regardless of `k`; the heap grows
    /// geometrically only as far as `k` entries are actually pushed.
    #[must_use]
    pub fn new(k: usize, mode: TopKMode) -> Self {
        Self {
            k,
            mode,
            heap: BinaryHeap::with_capacity(k.min(1024)),
        }
    }

    /// The configured `k`.
    #[must_use]
    pub fn k(&self) -> usize {
        self.k
    }

    /// The configured mode.
    #[must_use]
    pub fn mode(&self) -> TopKMode {
        self.mode
    }

    /// Number of entries currently held (`<= k` always).
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Whether the heap holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Whether the heap already holds `k` entries.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.heap.len() >= self.k
    }

    /// Worst entry currently held (the next eviction victim), if any.
    #[must_use]
    pub fn worst(&self) -> Option<Candidate> {
        self.heap.peek().map(HeapEntry::key)
    }

    /// Offer `(row_id, distance)`. Keeps it iff it ranks within the best `k`.
    pub fn push(&mut self, row_id: u64, distance: f32) {
        if self.k == 0 {
            return;
        }
        let entry = HeapEntry {
            distance,
            row_id,
            largest_wins: self.mode == TopKMode::Largest,
        };
        if self.heap.len() < self.k {
            self.heap.push(entry);
        } else if let Some(top) = self.heap.peek() {
            if entry < *top {
                self.heap.pop();
                self.heap.push(entry);
            }
        }
    }

    /// Drop all entries, retaining the allocation for reuse.
    pub fn clear(&mut self) {
        self.heap.clear();
    }

    /// Drain into best-first order: `(distance asc, row_id asc)` for
    /// [`TopKMode::Smallest`], `(distance desc, row_id asc)` for
    /// [`TopKMode::Largest`].
    #[must_use]
    pub fn into_sorted_vec(mut self) -> Vec<Candidate> {
        let mut out: Vec<Candidate> = self.heap.drain().map(|e| e.key()).collect();
        match self.mode {
            TopKMode::Smallest => out.sort(),
            TopKMode::Largest => out.sort_by(|a, b| {
                b.distance
                    .total_cmp(&a.distance)
                    .then_with(|| a.row_id.cmp(&b.row_id))
            }),
        }
        out
    }
}
