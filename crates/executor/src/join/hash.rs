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
//! Standalone hash join table: build, probe, bloom-assisted and partitioned.
//!
//! The key encoding is the canonical [`crate::index::index_value_bytes`]
//! used by the durable indexes, so integer-family spellings (`Int2`/`Int4`/
//! `Int8`) share one key and NULL keys never join. Text-family spellings
//! (`Text`/`VarChar`/`BpChar`/`Name`/`Unknown`/`Cstring`) additionally share
//! one bucket form (see [`join_key_bytes`]) because the durable encoding
//! tags them apart while the join recheck compares them by string content.
//! This mirrors the join engine's `join_hash_key` candidate semantics without duplicating them.
//!
//! The table stores build-side row INDICES (`Vec<usize>`) into the caller's
//! row slice instead of cloning full rows, so the build is as light as the
//! inline `right_buckets` map the sequential join used to carry. Probing
//! takes the caller's rows (`probe` by key value, `probe_row` by probe row)
//! and returns the matching build indices; the caller resolves them against
//! its own slice, preserving right-build/left-probe order.

use super::bloom::{hash_key, BloomFilter};
use plomid_sql::Value;
use std::collections::HashMap;

/// One logical row in the join engine.
pub type JoinRow = Vec<Value>;

/// Canonical join-key bytes, or `None` for NULL (which never joins).
#[must_use]
pub fn join_key_bytes(value: &Value) -> Option<Vec<u8>> {
    if value.is_null() {
        return None;
    }
    // Text-family canonicalization: Text/VarChar/BpChar/Name/Unknown/Cstring
    // compare by exact string content (`values_equal` resolves them through
    // the shared `SortKey::Str` branch), so they must share one bucket form
    // or equal keys never meet and the authoritative recheck never runs
    // (observed: `pg_namespace.nspname` Text vs `current_schemas()` Name
    // joining to empty). The durable `index_value_bytes` keeps its own tags;
    // this canonical form is join-local only. Merged keys are still rechecked
    // authoritatively, so a shared bucket can only add candidates.
    match value {
        Value::Text(s)
        | Value::VarChar(s)
        | Value::BpChar(s)
        | Value::Name(s)
        | Value::Unknown(s)
        | Value::Cstring(s) => {
            let mut bytes = Vec::with_capacity(5 + s.len());
            bytes.push(2);
            bytes.extend_from_slice(&(s.len() as u32).to_be_bytes());
            bytes.extend_from_slice(s.as_bytes());
            Some(bytes)
        }
        _ => Some(crate::index::index_value_bytes(value)),
    }
}

/// Stack-encoded canonical key bytes for integer-family join keys.
///
/// Bit-identical to the [`join_key_bytes`] encoding (`tag 1` plus big-endian
/// `i64`): `Int2`/`Int4`/`Int8` share one canonical form so cross-spelling
/// probes hit the same bucket. Lets probe loops skip the per-row `Vec`
/// allocation on the common integer-key shape; every other key shape still
/// goes through the owned encoding.
#[must_use]
pub fn int_key_bytes(value: &Value) -> Option<[u8; 9]> {
    let number: i64 = match value {
        Value::Int2(v) => i64::from(*v),
        Value::Int4(v) => i64::from(*v),
        Value::Int8(v) => *v,
        _ => return None,
    };
    let mut out = [0_u8; 9];
    out[0] = 1;
    out[1..].copy_from_slice(&number.to_be_bytes());
    Some(out)
}

/// In-memory hash table over one build column, storing build-side row indices.
#[derive(Debug, Clone, Default)]
pub struct HashJoinTable {
    /// Build-side column position driving the keys.
    pub build_col: usize,
    /// Canonical key bytes -> build-side row indices carrying that key, in
    /// build order so probes preserve right-build order per key. Backed by
    /// the fixed-seed [`crate::util::FastBuildHasher`]: keys are
    /// engine-internal canonical encodings and every probe is rechecked
    /// authoritatively, so the DoS-resistant `RandomState` only costs
    /// per-row time here.
    pub map: HashMap<Vec<u8>, Vec<usize>, crate::util::FastBuildHasher>,
}

impl HashJoinTable {
    /// Empty table over `build_col`.
    #[must_use]
    pub fn new(build_col: usize) -> Self {
        Self {
            build_col,
            map: HashMap::default(),
        }
    }

    /// Builds a table from `rows` in one call.
    #[must_use]
    pub fn from_rows(build_col: usize, rows: &[JoinRow]) -> Self {
        let mut table = Self::new(build_col);
        table.build(rows);
        table
    }

    /// Inserts the indices of every non-NULL-keyed row of `rows` into the table.
    pub fn build(&mut self, rows: &[JoinRow]) {
        let build_col = self.build_col;
        for (index, row) in rows.iter().enumerate() {
            let Some(key) = row.get(build_col).and_then(join_key_bytes) else {
                continue;
            };
            self.map.entry(key).or_default().push(index);
        }
    }

    /// Probes by key value, returning the matching build-side row indices.
    ///
    /// Only used by tests; the engine probes with precomputed bytes (see
    /// `probe_bytes`) so the Bloom gate and the table share one encoding.
    pub fn probe(&self, key: &Value) -> &[usize] {
        let Some(bytes) = join_key_bytes(key) else {
            return &[];
        };
        self.map.get(&bytes).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Probes with a full probe row, reading the key at `probe_col`.
    ///
    /// Integer-family keys encode on the stack (no allocation); every other
    /// key shape encodes once, exactly like [`join_key_bytes`]. Only used by
    /// tests; the engine probes with precomputed bytes (see `probe_bytes`)
    /// so the Bloom gate and the table share one encoding.
    pub fn probe_row(&self, row: &[Value], probe_col: usize) -> &[usize] {
        let Some(key) = row.get(probe_col) else {
            return &[];
        };
        if let Some(small) = int_key_bytes(key) {
            return self.probe_bytes(&small);
        }
        self.probe(key)
    }

    /// Probes with precomputed canonical key bytes (for example the bytes
    /// that already passed the Bloom gate), skipping a second encoding.
    #[must_use]
    pub fn probe_bytes(&self, key_bytes: &[u8]) -> &[usize] {
        self.map.get(key_bytes).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Number of distinct keys in the table.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True when the table holds no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total build row indices stored across all keys.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.map.values().map(Vec::len).sum()
    }

    /// Builds the table and a Bloom filter over its keys.
    ///
    /// The filter uses `fp_rate = 0.01` and carries `hash_key` of every
    /// canonical key, so the probe side can discard non-matching rows before
    /// hashing into the table.
    #[must_use]
    pub fn build_with_bloom(rows: &[JoinRow], build_col: usize) -> (Self, BloomFilter) {
        let mut table = Self::new(build_col);
        let mut filter = BloomFilter::new(rows.len(), 0.01);
        for (index, row) in rows.iter().enumerate() {
            let Some(key) = row.get(build_col).and_then(join_key_bytes) else {
                continue;
            };
            filter.insert(hash_key(&key));
            table.map.entry(key).or_default().push(index);
        }
        (table, filter)
    }

    /// Builds the table across `threads` workers with `std::thread::scope`.
    ///
    /// Each worker records its chunk's GLOBAL indices (`chunk_start + local`,
    /// so the merged table references the caller's slice exactly like
    /// [`Self::build`]); the partial maps merge on the calling thread in
    /// chunk order, so the result equals [`Self::build`] exactly.
    /// `threads == 0` behaves as `1`.
    #[must_use]
    pub fn parallel_build(rows: &[JoinRow], build_col: usize, threads: usize) -> Self {
        let threads = threads.max(1).min(rows.len().max(1));
        if threads <= 1 {
            return Self::from_rows(build_col, rows);
        }
        let chunk = rows.len().div_ceil(threads);
        let partials: Vec<HashMap<Vec<u8>, Vec<usize>, crate::util::FastBuildHasher>> =
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for (chunk_no, part) in rows.chunks(chunk).enumerate() {
                    let chunk_start = chunk_no * chunk;
                    handles.push(scope.spawn(move || {
                        let mut local: HashMap<Vec<u8>, Vec<usize>, crate::util::FastBuildHasher> =
                            HashMap::default();
                        for (local_index, row) in part.iter().enumerate() {
                            let Some(key) = row.get(build_col).and_then(join_key_bytes) else {
                                continue;
                            };
                            local
                                .entry(key)
                                .or_default()
                                .push(chunk_start + local_index);
                        }
                        local
                    }));
                }
                handles
                    .into_iter()
                    .map(|h| h.join().expect("hash build worker"))
                    .collect()
            });
        let mut table = Self::new(build_col);
        for mut local in partials {
            for (key, mut batch) in local.drain() {
                table.map.entry(key).or_default().append(&mut batch);
            }
        }
        table
    }
}
