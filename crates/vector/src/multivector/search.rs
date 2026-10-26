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
//! Parent-grouped multi-vector search: score constituents, group by parent,
//! dedup parents, exact rerank.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::index::common::{check_dimension, check_finite, dense_distance};
use crate::index::{Metric, RowId, SearchResult};
use crate::{Result, VectorError};

use super::aggregate::{aggregate, Aggregation};
use super::representation::MultiVector;

/// One stored constituent.
#[derive(Clone, Debug)]
struct Entry {
    parent: RowId,
    ordinal: u32,
    vector: Vec<f32>,
}

/// Index over `(parent_row, ordinal)` constituents.
#[derive(Clone, Debug)]
pub struct MultiVectorIndex {
    dim: usize,
    metric: Metric,
    aggregation: Aggregation,
    entries: Vec<Entry>,
    parents: HashMap<RowId, Vec<usize>>,
    deleted_parents: HashSet<RowId>,
    generation: u64,
}

impl MultiVectorIndex {
    /// Creates an empty multi-vector entity index.
    ///
    /// Constituent vectors are indexed under `(parent_row, ordinal)` and
    /// grouped back to parents at search time with the configured
    /// aggregation; duplicate parents are never returned twice.
    #[must_use]
    pub fn new(dim: usize, metric: Metric, aggregation: Aggregation) -> Self {
        Self {
            dim,
            metric,
            aggregation,
            entries: Vec::new(),
            parents: HashMap::new(),
            deleted_parents: HashSet::new(),
            generation: 0,
        }
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.dim, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    /// insert_parent: see implementation for contract.
    pub fn insert_parent(&mut self, parent: RowId, multi: &MultiVector) -> Result<()> {
        if multi.dim != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim as u32,
                got: multi.dim as u32,
            });
        }
        multi.validate()?;
        if self.parents.len() >= crate::index::common::MAX_VECTORS
            && !self.parents.contains_key(&parent)
        {
            return Err(VectorError::ResourceLimit(
                "multivector capacity".to_string(),
            ));
        }
        self.remove_parent_inner(parent);
        let mut idxs = Vec::with_capacity(multi.vectors.len());
        for (ord, vec) in multi.vectors.iter().enumerate() {
            self.check_vec(vec)?;
            idxs.push(self.entries.len());
            self.entries.push(Entry {
                parent,
                ordinal: ord as u32,
                vector: vec.clone(),
            });
        }
        self.parents.insert(parent, idxs);
        self.deleted_parents.remove(&parent);
        self.generation = self.generation.wrapping_add(1);
        Ok(())
    }

    fn remove_parent_inner(&mut self, parent: RowId) {
        if let Some(idxs) = self.parents.remove(&parent) {
            let drop: HashSet<usize> = idxs.into_iter().collect();
            let mut kept = Vec::with_capacity(self.entries.len());
            for (i, e) in self.entries.drain(..).enumerate() {
                if !drop.contains(&i) {
                    kept.push(e);
                }
            }
            self.entries = kept;
            // rebuild position map
            self.parents.clear();
            for (i, e) in self.entries.iter().enumerate() {
                self.parents.entry(e.parent).or_default().push(i);
            }
        }
        self.deleted_parents.remove(&parent);
    }

    /// delete_parent: see implementation for contract.
    pub fn delete_parent(&mut self, parent: RowId) -> bool {
        if !self.parents.contains_key(&parent) || self.deleted_parents.contains(&parent) {
            return false;
        }
        self.deleted_parents.insert(parent);
        self.generation = self.generation.wrapping_add(1);
        true
    }

    fn constituent_distances(&self, query: &[f32], entry: &Entry) -> Result<f32> {
        dense_distance(query, &entry.vector, self.metric)
    }

    /// Score one dense query against every live constituent, group hits by
    /// parent, aggregate, dedup and exact-rerank (recompute from stored
    /// constituents for determinism).
    pub fn search_single(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.check_vec(query)?;
        let mut per_parent: HashMap<RowId, Vec<f32>> = HashMap::new();
        for e in &self.entries {
            if self.deleted_parents.contains(&e.parent) {
                continue;
            }
            per_parent
                .entry(e.parent)
                .or_default()
                .push(self.constituent_distances(query, e)?);
        }
        let mut hits: Vec<SearchResult> = per_parent
            .into_iter()
            .map(|(parent, ds)| SearchResult::new(parent, aggregate(self.aggregation, &ds)))
            .collect();
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// Multi query: MaxSim-style — for each query constituent take the best
    /// document distance, then aggregate across query terms.
    pub fn search_multi(&self, query: &MultiVector, k: usize) -> Result<Vec<SearchResult>> {
        if query.dim != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim as u32,
                got: query.dim as u32,
            });
        }
        // per parent: for each query term, min over doc constituents
        let mut hits: Vec<SearchResult> = Vec::new();
        let mut parents: Vec<RowId> = self.parents.keys().copied().collect();
        parents.sort_unstable();
        for parent in parents {
            if self.deleted_parents.contains(&parent) {
                continue;
            }
            let idxs = &self.parents[&parent];
            let mut minima = Vec::with_capacity(query.vectors.len());
            for q in &query.vectors {
                self.check_vec(q)?;
                let mut best = f32::MAX;
                for &ei in idxs {
                    let d = dense_distance(q, &self.entries[ei].vector, self.metric)?;
                    if d < best {
                        best = d;
                    }
                }
                minima.push(best);
            }
            let score = match self.aggregation {
                Aggregation::MaxSim => super::aggregate::maxsim_from_minima(&minima),
                other => aggregate(other, &minima),
            };
            hits.push(SearchResult::new(parent, score));
        }
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// Drop tombstoned parents permanently.
    pub fn compact(&mut self) {
        if self.deleted_parents.is_empty() {
            return;
        }
        let dead: Vec<RowId> = self.deleted_parents.iter().copied().collect();
        for p in dead {
            self.remove_parent_inner(p);
        }
        self.deleted_parents.clear();
        self.generation = self.generation.wrapping_add(1);
    }

    /// validate: see implementation for contract.
    pub fn validate(&self) -> Result<()> {
        for (parent, idxs) in &self.parents {
            if idxs.is_empty() {
                return Err(VectorError::IndexCorrupted(
                    "parent without constituents".to_string(),
                ));
            }
            let mut ords: Vec<u32> = Vec::with_capacity(idxs.len());
            for &i in idxs {
                let Some(e) = self.entries.get(i) else {
                    return Err(VectorError::IndexCorrupted(
                        "bad constituent index".to_string(),
                    ));
                };
                if e.parent != *parent {
                    return Err(VectorError::IndexCorrupted(
                        "constituent parent mismatch".to_string(),
                    ));
                }
                self.check_vec(&e.vector)?;
                ords.push(e.ordinal);
            }
            // Ordinals must be exactly 0..n (insertion order, no gaps/dups).
            ords.sort_unstable();
            for (pos, o) in ords.iter().enumerate() {
                if *o != pos as u32 {
                    return Err(VectorError::IndexCorrupted(
                        "constituent ordinal gap".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// stats: see implementation for contract.
    pub fn stats(&self) -> (usize, usize, u64) {
        (
            self.parents.len(),
            self.deleted_parents.len(),
            self.generation,
        )
    }
}
