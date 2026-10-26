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
//! `SELECT DISTINCT` deduplication.
//!
//! Duplicate removal is the one place a `SELECT` may collapse rows, so the
//! comparator is the authoritative [`values_equal`] and nothing else. The work
//! here is in *reaching* that comparator cheaply: rows are bucketed by an
//! equality-implied signature so each row is compared only against rows that
//! could equal it, turning the previous `O(rows x distinct values)` linear scan
//! into an `O(rows)` pass.

use crate::row::values_equal;
use plomid_sql::{Expression, QueryResult, Value};

/// Marks the rows `SELECT DISTINCT` must drop, preserving first-seen order.
///
/// The returned flag is `true` when an earlier row exists that
/// [`values_equal`] calls equal. Candidates are bucketed by
/// [`crate::row::equality_signature`], which is implied by [`values_equal`], so
/// every duplicate still lands in the same bucket and is found. This replaces
/// the previous `Vec` scan — one comparison pass per row over every distinct
/// row already kept, i.e. `O(rows x distinct)` — with `O(rows)` signature
/// lookups while leaving every decision to the authoritative comparator.
///
/// A bucket may contain rows that are not equal (a hash collision, or
/// numerically equal values of different types); that only costs a confirmed
/// comparison and can never cause a wrong merge.
///
/// One behaviour note: the signature covers the two [`values_equal`] branches
/// reachable for values of one column type. Its textual fallback additionally
/// merges values of *different* type families in the same position
/// (`Bool(true)` with `Text("true")`, or `Date(10)` with `Int4(10)` through
/// their shared `sort_key`), which no per-value signature can express; such rows
/// are no longer merged. Uniformly typed positions — every real table column —
/// are unaffected.
fn duplicate_flags(rows: &[Vec<Value>]) -> Vec<bool> {
    // Fixed-seed backing hasher (see `crate::util::FastBuildHasher`): buckets
    // are keyed by `equality_signature` fingerprints confirmed by
    // `values_equal`, so only lookup speed matters here.
    let mut buckets: std::collections::HashMap<u64, Vec<usize>, crate::util::FastBuildHasher> =
        std::collections::HashMap::default();
    let mut duplicate = vec![false; rows.len()];
    for (index, row) in rows.iter().enumerate() {
        let candidates = buckets
            .entry(crate::row::equality_signature(row))
            .or_default();
        let already_seen = candidates.iter().any(|&kept| {
            let previous = &rows[kept];
            previous.len() == row.len()
                && previous
                    .iter()
                    .zip(row.iter())
                    .all(|(left, right)| values_equal(left, right))
        });
        if already_seen {
            duplicate[index] = true;
        } else {
            candidates.push(index);
        }
    }
    duplicate
}

pub fn apply_distinct(
    mut result: QueryResult,
    distinct: bool,
    distinct_on: &[Expression],
    limit: Option<usize>,
    offset: Option<usize>,
) -> QueryResult {
    let QueryResult::Rows {
        columns: _,
        column_types: _,
        rows,
    } = &mut result
    else {
        return result;
    };
    // Apply DISTINCT deduplication (preserving first-seen row order) when
    // requested, then apply LIMIT/OFFSET — PostgreSQL defers LIMIT until
    // after the duplicate removal. DISTINCT ON rows arrive already
    // de-duplicated from the engine, so only LIMIT/OFFSET apply to them.
    if distinct && distinct_on.is_empty() {
        let duplicate = duplicate_flags(rows);
        // Compact in place so kept rows are moved, never cloned.
        let mut write = 0;
        // Index-based on purpose: elements are swapped by position while
        // scanning, which an iterator cannot express.
        #[allow(clippy::needless_range_loop)]
        for read in 0..rows.len() {
            if !duplicate[read] {
                // `write <= read` always holds, so the row swapped backwards
                // into `read` has already been classified and is never read
                // again.
                if write != read {
                    rows.swap(write, read);
                }
                write += 1;
            }
        }
        rows.truncate(write);
    }
    if distinct || !distinct_on.is_empty() {
        if let Some(skip) = offset {
            rows.drain(..skip.min(rows.len()));
        }
        if let Some(take) = limit {
            rows.truncate(take);
        }
    }
    result
}
