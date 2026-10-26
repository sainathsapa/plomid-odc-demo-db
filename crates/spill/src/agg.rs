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

//! Spillable grouped aggregation (count + sum).
//!
//! [`SpillGroupedAgg`] aggregates `(key, value)` pairs into
//! `(key, count, sum)` with an in-memory budget of `mem_limit_rows` distinct
//! keys. Over budget it hash-partitions the map to `num_partitions` spill runs
//! and switches to partition write-through; [`SpillGroupedAgg::finish`]
//! re-aggregates one partition at a time so peak memory stays near
//! `distinct_keys / num_partitions` plus the leftover buffer.

use crate::codec::{partition_for, read_i64_le, read_u64_le, split_prefix_u32, SpillEncode};
use crate::error::{Result, SpillError};
use crate::manager::{SpillManager, SpillRun};
use crate::{cancelled, ChargeFn};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Default partition fan-out for spilled aggregation.
pub const DEFAULT_PARTITIONS: usize = 32;

fn encode_agg_record<K: SpillEncode>(k: &K, count: u64, sum: i64) -> Vec<u8> {
    let kb = k.encode();
    let mut out = Vec::with_capacity(4 + kb.len() + 16);
    out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
    out.extend_from_slice(&kb);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&sum.to_le_bytes());
    out
}

fn decode_agg_record<K: SpillEncode>(rec: &[u8]) -> Result<(K, u64, i64)> {
    let (klen, rest) = split_prefix_u32(rec)?;
    if rest.len() < klen + 16 {
        return Err(SpillError::Decode("agg record truncated".to_string()));
    }
    let k = K::decode(&rest[..klen])?;
    let off = klen;
    let count = read_u64_le(rest, off)?;
    let sum = read_i64_le(rest, off + 8)?;
    Ok((k, count, sum))
}

/// Grouped count/sum aggregation that partitions to disk over budget.
///
/// `K` is the group key; values are `i64` addends (count is derived).
pub struct SpillGroupedAgg<K> {
    map: HashMap<K, (u64, i64)>,
    map_charge: i64,
    partitions: Option<Vec<SpillRun>>,
    num_partitions: usize,
    mem_limit_rows: usize,
    manager: SpillManager,
    cancel: Arc<AtomicBool>,
    charge: ChargeFn,
    peak_rows: usize,
    peak_bytes: u64,
    spill_bytes: u64,
}

impl<K: Hash + Eq + Clone + SpillEncode> SpillGroupedAgg<K> {
    /// Create with a budget of `mem_limit_rows` distinct in-memory keys.
    pub fn new(manager: SpillManager, mem_limit_rows: usize) -> Result<Self> {
        Self::with_partitions(manager, mem_limit_rows, DEFAULT_PARTITIONS)
    }

    /// Create with an explicit partition fan-out.
    pub fn with_partitions(
        manager: SpillManager,
        mem_limit_rows: usize,
        num_partitions: usize,
    ) -> Result<Self> {
        if mem_limit_rows == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "grouped agg needs mem_limit_rows >= 1".to_string(),
            ));
        }
        if num_partitions == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "grouped agg needs num_partitions >= 1".to_string(),
            ));
        }
        Ok(Self {
            map: HashMap::new(),
            map_charge: 0,
            partitions: None,
            num_partitions,
            mem_limit_rows,
            manager,
            cancel: Arc::new(AtomicBool::new(false)),
            charge: crate::noop_charge(),
            peak_rows: 0,
            peak_bytes: 0,
            spill_bytes: 0,
        })
    }

    /// Attach a cancellation flag.
    pub fn with_cancel(mut self, cancel: Arc<AtomicBool>) -> Self {
        self.cancel = cancel;
        self
    }

    /// Attach a memory-accounting hook.
    pub fn with_charge(mut self, charge: ChargeFn) -> Self {
        self.charge = charge;
        self
    }

    fn charge_new_key(&mut self, k: &K) {
        let est = k.encode().len() as i64 + 64;
        self.map_charge += est;
        (self.charge)(est);
        if self.map.len() > self.peak_rows {
            self.peak_rows = self.map.len();
            self.peak_bytes = self.map_charge.max(0) as u64;
        }
    }

    /// Add one `(key, value)` pair.
    pub fn add(&mut self, key: K, value: i64) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.partitions.is_none() {
            match self.map.get_mut(&key) {
                Some(slot) => {
                    slot.0 += 1;
                    slot.1 += value;
                }
                None => {
                    self.charge_new_key(&key);
                    self.map.insert(key.clone(), (1, value));
                }
            }
            if self.map.len() > self.mem_limit_rows {
                self.spill_all()?;
            }
        } else {
            let p = partition_for(&key, self.num_partitions);
            let rec = encode_agg_record(&key, 1, value);
            let Some(runs) = self.partitions.as_mut() else {
                return Err(SpillError::Io("agg partitions missing".to_string()));
            };
            runs[p].write_record(&rec)?;
            self.spill_bytes += 4 + rec.len() as u64;
        }
        Ok(())
    }

    fn spill_all(&mut self) -> Result<()> {
        cancelled(&self.cancel)?;
        let mut runs = Vec::with_capacity(self.num_partitions);
        for _ in 0..self.num_partitions {
            runs.push(self.manager.create_run()?);
        }
        for (k, (c, s)) in self.map.drain() {
            cancelled(&self.cancel)?;
            let p = partition_for(&k, self.num_partitions);
            let rec = encode_agg_record(&k, c, s);
            runs[p].write_record(&rec)?;
            self.spill_bytes += 4 + rec.len() as u64;
        }
        for r in runs.iter_mut() {
            r.flush()?;
        }
        (self.charge)(-self.map_charge);
        self.map_charge = 0;
        self.partitions = Some(runs);
        Ok(())
    }

    /// Whether aggregation partitioned to disk.
    pub fn fell_back_to_disk(&self) -> bool {
        self.partitions.is_some()
    }

    /// Cumulative spilled bytes for this operator.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Peak in-memory estimate in bytes.
    pub fn peak_memory(&self) -> u64 {
        self.peak_bytes
    }

    /// Finish and return one `(key, count, sum)` per group.
    pub fn finish(mut self) -> Result<Vec<(K, u64, i64)>> {
        cancelled(&self.cancel)?;
        let Some(mut runs) = self.partitions.take() else {
            let out: Vec<_> = self.map.drain().map(|(k, (c, s))| (k, c, s)).collect();
            (self.charge)(-self.map_charge);
            return Ok(out);
        };
        // Bucket any in-mem leftovers (empty in write-through mode, but cheap).
        let mut leftover: Vec<Vec<(K, (u64, i64))>> = Vec::with_capacity(self.num_partitions);
        leftover.resize_with(self.num_partitions, Vec::new);
        for (k, v) in self.map.drain() {
            let p = partition_for(&k, self.num_partitions);
            leftover[p].push((k, v));
        }
        (self.charge)(-self.map_charge);

        let mut out = Vec::new();
        for (p, run) in runs.iter_mut().enumerate() {
            cancelled(&self.cancel)?;
            let mut part: HashMap<K, (u64, i64)> = HashMap::new();
            for rec in run.records()? {
                cancelled(&self.cancel)?;
                let (k, c, s) = decode_agg_record::<K>(&rec)?;
                part.entry(k)
                    .and_modify(|e| {
                        e.0 += c;
                        e.1 += s;
                    })
                    .or_insert((c, s));
            }
            for (k, (c, s)) in leftover[p].drain(..) {
                part.entry(k)
                    .and_modify(|e| {
                        e.0 += c;
                        e.1 += s;
                    })
                    .or_insert((c, s));
            }
            out.extend(part.into_iter().map(|(k, (c, s))| (k, c, s)));
        }
        Ok(out)
    }
}
