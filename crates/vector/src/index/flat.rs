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
#![forbid(unsafe_code)]
//! Brute-force dense store with tombstones. Exact search; the reference
//! implementation every approximate index is validated against.

use std::collections::HashMap;

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, dense_distance, Generation, TombstoneMap,
};
use super::{IndexConfig, IndexKind, IndexStats, Metric, RowId, SearchResult, VectorIndex};

/// Brute-force exact index: stores every live vector and scans them all.
///
/// The correctness oracle for recall tests and the guaranteed fallback when
/// no ANN structure is READY. Memory is `O(vectors * dimension)`; search is
/// `O(vectors)` distance evaluations with a bounded Top-K heap.
#[derive(Clone, Debug)]
pub struct FlatIndex {
    config: IndexConfig,
    vectors: HashMap<RowId, Vec<f32>>,
    tombstones: TombstoneMap,
    generation: Generation,
}

impl FlatIndex {
    /// Creates an empty exact index for `config` after validation.
    ///
    /// Accepts any dimension/metrics triple; incompatible persisted bytes are
    /// rejected at decode time rather than here.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            vectors: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            generation: Generation::new(),
        })
    }

    /// dimension: see implementation for contract.
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    /// metric: see implementation for contract.
    pub fn metric(&self) -> Metric {
        self.config.metric
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.config.dimension, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    fn scored(&self, query: &[f32]) -> Result<Vec<SearchResult>> {
        check_dimension(self.config.dimension, query.len())?;
        check_finite(query)?;
        let mut out = Vec::new();
        for (id, vec) in &self.vectors {
            if self.tombstones.is_deleted(*id) {
                continue;
            }
            let d = dense_distance(query, vec, self.config.metric)?;
            out.push(SearchResult::new(*id, d));
        }
        Ok(out)
    }

    /// Remove tombstoned rows permanently.
    pub fn compact(&mut self) {
        let dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        for id in dead {
            self.vectors.remove(&id);
        }
        self.tombstones.clear();
        self.generation.next();
    }
}

impl VectorIndex for FlatIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        self.vectors.clear();
        self.tombstones.clear();
        for (id, vec) in rows {
            self.check_vec(vec)?;
            self.vectors.insert(*id, vec.clone());
        }
        self.generation.next();
        Ok(())
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        self.check_vec(vector)?;
        if self.vectors.len() >= super::common::MAX_VECTORS && !self.vectors.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("flat capacity".to_string()));
        }
        self.vectors.insert(row_id, vector.to_vec());
        self.tombstones.unmark(row_id);
        self.generation.next();
        Ok(())
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.vectors.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
            return Ok(false);
        }
        let g = self.generation.next();
        self.tombstones.mark_deleted(row_id, g);
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let mut hits = self.scored(query)?;
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(k);
        Ok(hits)
    }

    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>> {
        if !radius.is_finite() {
            return Err(VectorError::IndexParameterInvalid(
                "radius must be finite".to_string(),
            ));
        }
        let mut hits = self.scored(query)?;
        hits.retain(|h| h.distance <= radius);
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        Ok(hits)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::Flat {
            return Err(VectorError::IndexCorrupted(
                "flat kind mismatch".to_string(),
            ));
        }
        for (id, v) in &self.vectors {
            check_dimension(self.config.dimension, v.len())?;
            check_finite(v)?;
            let _ = id;
        }
        for (id, _) in self.tombstones.iter() {
            if !self.vectors.contains_key(id) {
                return Err(VectorError::IndexCorrupted(format!(
                    "tombstone for missing {id}"
                )));
            }
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.vectors.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}
