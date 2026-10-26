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

//! Spillable distinct and set operations.
//!
//! [`SpillDistinct`] dedups via hash partitioning (no `Ord` bound needed).
//! [`SpillSetOp`] computes `UNION` / `INTERSECT` / `EXCEPT` (all DISTINCT
//! semantics) by partitioning both inputs with the same hash function and
//! applying the op per partition in memory.

use crate::codec::{partition_for, SpillEncode};
use crate::error::{Result, SpillError};
use crate::manager::{SpillManager, SpillRun};
use crate::{cancelled, ChargeFn};
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::agg::DEFAULT_PARTITIONS;

/// Hash-partitioned distinct over `T` (no ordering required).
pub struct SpillDistinct<T> {
    set: HashSet<T>,
    set_charge: i64,
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

impl<T: Hash + Eq + Clone + SpillEncode> SpillDistinct<T> {
    /// Create with a budget of `mem_limit_rows` distinct in-memory values.
    pub fn new(manager: SpillManager, mem_limit_rows: usize) -> Result<Self> {
        if mem_limit_rows == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "distinct needs mem_limit_rows >= 1".to_string(),
            ));
        }
        Ok(Self {
            set: HashSet::new(),
            set_charge: 0,
            partitions: None,
            num_partitions: DEFAULT_PARTITIONS,
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

    /// Insert one value.
    pub fn insert(&mut self, value: T) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.partitions.is_none() {
            if self.set.insert(value.clone()) {
                let est = value.encode().len() as i64 + 64;
                self.set_charge += est;
                (self.charge)(est);
                if self.set.len() > self.peak_rows {
                    self.peak_rows = self.set.len();
                    self.peak_bytes = self.set_charge.max(0) as u64;
                }
            }
            if self.set.len() > self.mem_limit_rows {
                self.spill_all()?;
            }
        } else {
            let p = partition_for(&value, self.num_partitions);
            let bytes = value.encode();
            let Some(runs) = self.partitions.as_mut() else {
                return Err(SpillError::Io("distinct partitions missing".to_string()));
            };
            runs[p].write_record(&bytes)?;
            self.spill_bytes += 4 + bytes.len() as u64;
        }
        Ok(())
    }

    fn spill_all(&mut self) -> Result<()> {
        cancelled(&self.cancel)?;
        let mut runs = Vec::with_capacity(self.num_partitions);
        for _ in 0..self.num_partitions {
            runs.push(self.manager.create_run()?);
        }
        for v in self.set.drain() {
            cancelled(&self.cancel)?;
            let p = partition_for(&v, self.num_partitions);
            let bytes = v.encode();
            runs[p].write_record(&bytes)?;
            self.spill_bytes += 4 + bytes.len() as u64;
        }
        for r in runs.iter_mut() {
            r.flush()?;
        }
        (self.charge)(-self.set_charge);
        self.set_charge = 0;
        self.partitions = Some(runs);
        Ok(())
    }

    /// Whether dedup partitioned to disk.
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

    /// Finish and return the distinct values.
    pub fn finish(mut self) -> Result<Vec<T>> {
        cancelled(&self.cancel)?;
        let Some(mut runs) = self.partitions.take() else {
            let out: Vec<T> = self.set.drain().collect();
            (self.charge)(-self.set_charge);
            return Ok(out);
        };
        let mut leftover: Vec<Vec<T>> = Vec::with_capacity(self.num_partitions);
        leftover.resize_with(self.num_partitions, Vec::new);
        for v in self.set.drain() {
            let p = partition_for(&v, self.num_partitions);
            leftover[p].push(v);
        }
        (self.charge)(-self.set_charge);
        let mut out = Vec::new();
        for (p, run) in runs.iter_mut().enumerate() {
            cancelled(&self.cancel)?;
            let mut part: HashSet<T> = HashSet::new();
            for rec in run.records()? {
                cancelled(&self.cancel)?;
                part.insert(T::decode(&rec)?);
            }
            part.extend(leftover[p].drain(..));
            out.extend(part);
        }
        Ok(out)
    }
}

/// Which DISTINCT set operation [`SpillSetOp`] computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    /// `left ∪ right` (distinct).
    Union,
    /// `left ∩ right` (distinct).
    Intersect,
    /// `left − right` (distinct).
    Except,
}

/// Partitioned set operation over two inputs with DISTINCT semantics.
pub struct SpillSetOp<T> {
    kind: SetOpKind,
    left_mem: HashSet<T>,
    right_mem: HashSet<T>,
    left_runs: Option<Vec<SpillRun>>,
    right_runs: Option<Vec<SpillRun>>,
    num_partitions: usize,
    mem_limit_rows: usize,
    manager: SpillManager,
    cancel: Arc<AtomicBool>,
    charge: ChargeFn,
    peak_bytes: u64,
    spill_bytes: u64,
}

impl<T: Hash + Eq + Clone + SpillEncode> SpillSetOp<T> {
    /// Create for `kind` with a combined in-memory budget of
    /// `mem_limit_rows` distinct values per side.
    pub fn new(manager: SpillManager, kind: SetOpKind, mem_limit_rows: usize) -> Result<Self> {
        if mem_limit_rows == 0 {
            return Err(SpillError::MemoryBudgetExceeded(
                "set-op needs mem_limit_rows >= 1".to_string(),
            ));
        }
        Ok(Self {
            kind,
            left_mem: HashSet::new(),
            right_mem: HashSet::new(),
            left_runs: None,
            right_runs: None,
            num_partitions: DEFAULT_PARTITIONS,
            mem_limit_rows,
            manager,
            cancel: Arc::new(AtomicBool::new(false)),
            charge: crate::noop_charge(),
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

    fn note_peak(&mut self, est: i64) {
        (self.charge)(est);
        let cur = (self.left_mem.len() + self.right_mem.len()) as u64 * 96;
        if cur > self.peak_bytes {
            self.peak_bytes = cur;
        }
    }

    /// Insert one left-side value.
    pub fn insert_left(&mut self, value: T) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.left_runs.is_none() {
            if self.left_mem.insert(value.clone()) {
                self.note_peak(value.encode().len() as i64 + 64);
            }
            if self.left_mem.len() > self.mem_limit_rows {
                self.spill_side(true)?;
            }
        } else {
            let p = partition_for(&value, self.num_partitions);
            let bytes = value.encode();
            let Some(runs) = self.left_runs.as_mut() else {
                return Err(SpillError::Io("set-op left partitions missing".to_string()));
            };
            runs[p].write_record(&bytes)?;
            self.spill_bytes += 4 + bytes.len() as u64;
        }
        Ok(())
    }

    /// Insert one right-side value.
    pub fn insert_right(&mut self, value: T) -> Result<()> {
        cancelled(&self.cancel)?;
        if self.right_runs.is_none() {
            if self.right_mem.insert(value.clone()) {
                self.note_peak(value.encode().len() as i64 + 64);
            }
            if self.right_mem.len() > self.mem_limit_rows {
                self.spill_side(false)?;
            }
        } else {
            let p = partition_for(&value, self.num_partitions);
            let bytes = value.encode();
            let Some(runs) = self.right_runs.as_mut() else {
                return Err(SpillError::Io(
                    "set-op right partitions missing".to_string(),
                ));
            };
            runs[p].write_record(&bytes)?;
            self.spill_bytes += 4 + bytes.len() as u64;
        }
        Ok(())
    }

    fn spill_side(&mut self, left: bool) -> Result<()> {
        cancelled(&self.cancel)?;
        let mut runs = Vec::with_capacity(self.num_partitions);
        for _ in 0..self.num_partitions {
            runs.push(self.manager.create_run()?);
        }
        let set = if left {
            &mut self.left_mem
        } else {
            &mut self.right_mem
        };
        for v in set.drain() {
            cancelled(&self.cancel)?;
            let p = partition_for(&v, self.num_partitions);
            let bytes = v.encode();
            runs[p].write_record(&bytes)?;
            self.spill_bytes += 4 + bytes.len() as u64;
        }
        for r in runs.iter_mut() {
            r.flush()?;
        }
        if left {
            self.left_runs = Some(runs);
        } else {
            self.right_runs = Some(runs);
        }
        Ok(())
    }

    /// Whether either side partitioned to disk.
    pub fn fell_back_to_disk(&self) -> bool {
        self.left_runs.is_some() || self.right_runs.is_some()
    }

    /// Cumulative spilled bytes for this operator.
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }

    /// Peak in-memory estimate in bytes.
    pub fn peak_memory(&self) -> u64 {
        self.peak_bytes
    }

    /// Finish and return the result values (order unspecified).
    pub fn finish(mut self) -> Result<Vec<T>> {
        cancelled(&self.cancel)?;
        if self.left_runs.is_none() && self.right_runs.is_none() {
            return Ok(match self.kind {
                SetOpKind::Union => self.left_mem.union(&self.right_mem).cloned().collect(),
                SetOpKind::Intersect => self
                    .left_mem
                    .intersection(&self.right_mem)
                    .cloned()
                    .collect(),
                SetOpKind::Except => self.left_mem.difference(&self.right_mem).cloned().collect(),
            });
        }
        // Bucket in-mem leftovers per partition on both sides.
        let mut lleft: Vec<HashSet<T>> = Vec::with_capacity(self.num_partitions);
        lleft.resize_with(self.num_partitions, HashSet::new);
        for v in self.left_mem.drain() {
            let p = partition_for(&v, self.num_partitions);
            lleft[p].insert(v);
        }
        let mut lright: Vec<HashSet<T>> = Vec::with_capacity(self.num_partitions);
        lright.resize_with(self.num_partitions, HashSet::new);
        for v in self.right_mem.drain() {
            let p = partition_for(&v, self.num_partitions);
            lright[p].insert(v);
        }
        if let Some(runs) = self.left_runs.as_mut() {
            for (p, run) in runs.iter_mut().enumerate() {
                cancelled(&self.cancel)?;
                for rec in run.records()? {
                    cancelled(&self.cancel)?;
                    lleft[p].insert(T::decode(&rec)?);
                }
            }
        }
        if let Some(runs) = self.right_runs.as_mut() {
            for (p, run) in runs.iter_mut().enumerate() {
                cancelled(&self.cancel)?;
                for rec in run.records()? {
                    cancelled(&self.cancel)?;
                    lright[p].insert(T::decode(&rec)?);
                }
            }
        }
        let mut out = Vec::new();
        for p in 0..self.num_partitions {
            cancelled(&self.cancel)?;
            match self.kind {
                SetOpKind::Union => out.extend(lleft[p].union(&lright[p]).cloned()),
                SetOpKind::Intersect => {
                    out.extend(lleft[p].intersection(&lright[p]).cloned());
                }
                SetOpKind::Except => out.extend(lleft[p].difference(&lright[p]).cloned()),
            }
        }
        Ok(out)
    }
}
