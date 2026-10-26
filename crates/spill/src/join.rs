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

//! Spillable partitioned (grace) hash join.
//!
//! [`SpillHashJoin`] buffers the build side up to `mem_limit_rows` rows. Over
//! budget it hash-partitions both sides to `num_partitions` spill runs with a
//! shared partition function and joins one partition at a time, so peak memory
//! stays near `build_rows / num_partitions`. Inner-join semantics: every
//! build/probe key match emits the cross product for that key.

use crate::codec::{partition_for, split_prefix_u32, SpillEncode};
use crate::error::{Result, SpillError};
use crate::manager::{SpillManager, SpillRun};
use crate::{cancelled, ChargeFn};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Default partition fan-out for the grace join.
pub const DEFAULT_JOIN_PARTITIONS: usize = 32;

/// One inner-join output row: `(key, build_row, probe_row)` with opaque rows.
pub type JoinRow<K> = (K, Vec<u8>, Vec<u8>);

fn encode_side_record<K: SpillEncode>(k: &K, row: &[u8]) -> Vec<u8> {
    let kb = k.encode();
    let mut out = Vec::with_capacity(8 + kb.len() + row.len());
    out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
    out.extend_from_slice(&kb);
    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
    out.extend_from_slice(row);
    out
}

fn decode_side_record<K: SpillEncode>(rec: &[u8]) -> Result<(K, Vec<u8>)> {
    let (klen, rest) = split_prefix_u32(rec)?;
    if rest.len() < klen + 4 {
        return Err(SpillError::Decode("join record truncated".to_string()));
    }
    let k = K::decode(&rest[..klen])?;
    let (rlen, rest2) = split_prefix_u32(&rest[klen..])?;
    if rest2.len() < rlen {
        return Err(SpillError::Decode("join row truncated".to_string()));
    }
    Ok((k, rest2[..rlen].to_vec()))
}

/// Inner grace hash join with opaque row payloads.
///
/// Keys implement [`SpillEncode`]; build/probe rows are opaque `Vec<u8>`.
pub struct SpillHashJoin<K> {
    build_map: HashMap<K, Vec<Vec<u8>>>,
    build_charge: i64,
    build_rows: u64,
    build_runs: Option<Vec<SpillRun>>,
    probe_runs: Option<Vec<SpillRun>>,
    // Streaming output for the fully in-memory path.
    output: Vec<JoinRow<K>>,
    num_partitions: usize,
    mem_limit_rows: usize,
    manager: SpillManager,
    cancel: Arc<AtomicBool>,
    charge: ChargeFn,
    peak_rows: usize,
    peak_bytes: u64,
    spill_bytes: u64,
}

impl<K: Hash + Eq + Clone + SpillEncode> SpillHashJoin<K> {
    /// Create with a build-side budget of `mem_limit_rows` rows.
    pub fn new(manager: SpillManager, mem_limit_rows: usize) -> Result<Self> {
        Self::with_partitions(manager, mem_limit_rows, DEFAULT_JOIN_PARTITIONS)
    }

    /// Create with an explicit partition fan-out.
    pub fn with_partitions(
        manager: SpillManager,
        mem_limit_rows: usize,
        num_partitions: usize,
    ) -> Result<Self> {
        if mem_limit_rows == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "hash join needs mem_limit_rows >= 1".to_string(),
            ));
        }
        if num_partitions == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "hash join needs num_partitions >= 1".to_string(),
            ));
        }
        Ok(Self {
            build_map: HashMap::new(),
            build_charge: 0,
            build_rows: 0,
            build_runs: None,
            probe_runs: None,
            output: Vec::new(),
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

    /// Insert one build-side `(key, row)` pair.
    pub fn insert_build(&mut self, key: K, row: Vec<u8>) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.build_runs.is_none() {
            let est = key.encode().len() as i64 + row.len() as i64 + 64;
            self.build_map.entry(key.clone()).or_default().push(row);
            self.build_charge += est;
            (self.charge)(est);
            self.build_rows += 1;
            if self.build_rows as usize > self.peak_rows {
                self.peak_rows = self.build_rows as usize;
                self.peak_bytes = self.build_charge.max(0) as u64;
            }
            if self.build_rows as usize > self.mem_limit_rows {
                self.spill_build()?;
            }
        } else {
            let p = partition_for(&key, self.num_partitions);
            let rec = encode_side_record(&key, &row);
            let Some(runs) = self.build_runs.as_mut() else {
                return Err(SpillError::Io("join build partitions missing".to_string()));
            };
            runs[p].write_record(&rec)?;
            self.spill_bytes += 4 + rec.len() as u64;
        }
        Ok(())
    }

    fn spill_build(&mut self) -> Result<()> {
        cancelled(&self.cancel)?;
        let mut runs = Vec::with_capacity(self.num_partitions);
        for _ in 0..self.num_partitions {
            runs.push(self.manager.create_run()?);
        }
        for (k, rows) in self.build_map.drain() {
            for row in rows {
                cancelled(&self.cancel)?;
                let p = partition_for(&k, self.num_partitions);
                let rec = encode_side_record(&k, &row);
                runs[p].write_record(&rec)?;
                self.spill_bytes += 4 + rec.len() as u64;
            }
        }
        for r in runs.iter_mut() {
            r.flush()?;
        }
        (self.charge)(-self.build_charge);
        self.build_charge = 0;
        self.build_runs = Some(runs);
        Ok(())
    }

    fn ensure_probe_runs(&mut self) -> Result<()> {
        if self.probe_runs.is_none() {
            let mut runs = Vec::with_capacity(self.num_partitions);
            for _ in 0..self.num_partitions {
                runs.push(self.manager.create_run()?);
            }
            self.probe_runs = Some(runs);
        }
        Ok(())
    }

    /// Probe with one `(key, row)` pair. In-memory path emits matches
    /// immediately; spilled path partitions the probe row for later.
    pub fn insert_probe(&mut self, key: K, row: Vec<u8>) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.build_runs.is_none() {
            if let Some(builds) = self.build_map.get(&key) {
                for b in builds {
                    self.output.push((key.clone(), b.clone(), row.clone()));
                }
            }
        } else {
            self.ensure_probe_runs()?;
            let p = partition_for(&key, self.num_partitions);
            let rec = encode_side_record(&key, &row);
            let Some(runs) = self.probe_runs.as_mut() else {
                return Err(SpillError::Io("join probe partitions missing".to_string()));
            };
            runs[p].write_record(&rec)?;
            self.spill_bytes += 4 + rec.len() as u64;
        }
        Ok(())
    }

    /// Whether the build side partitioned to disk.
    pub fn fell_back_to_disk(&self) -> bool {
        self.build_runs.is_some()
    }

    /// Cumulative spilled bytes for this operator.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Peak build-side in-memory estimate in bytes.
    pub fn peak_memory(&self) -> u64 {
        self.peak_bytes
    }

    /// Finish the join and return all `(key, build_row, probe_row)` matches.
    pub fn finish(mut self) -> Result<Vec<JoinRow<K>>> {
        cancelled(&self.cancel)?;
        let Some(mut build_runs) = self.build_runs.take() else {
            return Ok(self.output);
        };
        let mut probe_runs = self.probe_runs.take().unwrap_or_default();
        debug_assert!(probe_runs.len() == build_runs.len() || probe_runs.is_empty());
        let mut out = std::mem::take(&mut self.output);
        for p in 0..self.num_partitions {
            cancelled(&self.cancel)?;
            let mut table: HashMap<K, Vec<Vec<u8>>> = HashMap::new();
            for rec in build_runs[p].records()? {
                cancelled(&self.cancel)?;
                let (k, row) = decode_side_record::<K>(&rec)?;
                table.entry(k).or_default().push(row);
            }
            if p < probe_runs.len() {
                for rec in probe_runs[p].records()? {
                    cancelled(&self.cancel)?;
                    let (k, prow) = decode_side_record::<K>(&rec)?;
                    if let Some(brows) = table.get(&k) {
                        for b in brows {
                            out.push((k.clone(), b.clone(), prow.clone()));
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}
