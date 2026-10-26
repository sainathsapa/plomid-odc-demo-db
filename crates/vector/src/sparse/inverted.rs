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
//! Sparse inverted index: `dim -> posting list`, candidate generation plus
//! exact verification.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::index::{Metric, RowId, SearchResult};
use crate::{Result, VectorError};

use super::distance::{sparse_cosine_distance, sparse_dot, sparse_l2_squared};
use super::representation::SparseVector;

/// Inverted sparse index with exact re-verification.
#[derive(Clone, Debug)]
pub struct SparseInvertedIndex {
    dim: u32,
    metric: Metric,
    /// dimension -> (row, value) postings, kept sorted by row for determinism.
    postings: HashMap<u32, Vec<(RowId, f32)>>,
    vectors: HashMap<RowId, SparseVector>,
    deleted: HashSet<RowId>,
    generation: u64,
}

impl SparseInvertedIndex {
    /// Creates an empty sparse inverted index for `dim`-wide vectors.
    ///
    /// Posting lists map dimension-id to `(row_id, value)` pairs;
    /// candidate generation is metric-aware and verified exactly.
    #[must_use]
    pub fn new(dim: u32, metric: Metric) -> Self {
        Self {
            dim,
            metric,
            postings: HashMap::new(),
            vectors: HashMap::new(),
            deleted: HashSet::new(),
            generation: 0,
        }
    }

    /// len: see implementation for contract.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// is_empty: see implementation for contract.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// insert: see implementation for contract.
    pub fn insert(&mut self, row_id: RowId, vector: SparseVector) -> Result<()> {
        if vector.dim != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim as usize as u32,
                got: vector.dim as usize as u32,
            });
        }
        vector.validate()?;
        if self.vectors.len() >= crate::index::common::MAX_VECTORS
            && !self.vectors.contains_key(&row_id)
        {
            return Err(VectorError::ResourceLimit("sparse capacity".to_string()));
        }
        if let Some(old) = self.vectors.get(&row_id).cloned() {
            self.deindex(row_id, &old);
        }
        self.index(row_id, &vector);
        self.vectors.insert(row_id, vector);
        self.deleted.remove(&row_id);
        self.generation = self.generation.wrapping_add(1);
        Ok(())
    }

    fn index(&mut self, row_id: RowId, v: &SparseVector) {
        for (i, x) in v.indices.iter().zip(v.values.iter()) {
            let list = self.postings.entry(*i).or_default();
            match list.binary_search_by_key(&row_id, |(id, _)| *id) {
                Ok(_) => {}
                Err(pos) => list.insert(pos, (row_id, *x)),
            }
        }
    }

    fn deindex(&mut self, row_id: RowId, v: &SparseVector) {
        for i in &v.indices {
            if let Some(list) = self.postings.get_mut(i) {
                list.retain(|(id, _)| *id != row_id);
                if list.is_empty() {
                    self.postings.remove(i);
                }
            }
        }
    }

    /// delete: see implementation for contract.
    pub fn delete(&mut self, row_id: RowId) -> bool {
        if !self.vectors.contains_key(&row_id) || self.deleted.contains(&row_id) {
            return false;
        }
        self.deleted.insert(row_id);
        self.generation = self.generation.wrapping_add(1);
        true
    }

    fn distance(&self, a: &SparseVector, b: &SparseVector) -> Result<f32> {
        match self.metric {
            Metric::L2 => Ok(sparse_l2_squared(a, b)),
            Metric::Cosine => Ok(sparse_cosine_distance(a, b)),
            Metric::Dot => Ok(-sparse_dot(a, b)),
            Metric::Hamming | Metric::Jaccard => Err(VectorError::IndexUnsupported(
                "sparse needs L2/Cosine/Dot".to_string(),
            )),
        }
    }

    /// Candidate generation: union of posting lists for query dims,
    /// falling back to all live rows when there is no overlap.
    pub fn candidates(&self, query: &SparseVector) -> Vec<RowId> {
        let mut set: HashSet<RowId> = HashSet::new();
        for i in &query.indices {
            if let Some(list) = self.postings.get(i) {
                for (id, _) in list {
                    if !self.deleted.contains(id) {
                        set.insert(*id);
                    }
                }
            }
        }
        if set.is_empty() {
            self.vectors
                .keys()
                .copied()
                .filter(|id| !self.deleted.contains(id))
                .collect()
        } else {
            let mut out: Vec<RowId> = set.into_iter().collect();
            out.sort_unstable();
            out
        }
    }

    /// Candidate retrieval followed by exact verification.
    pub fn search(&self, query: &SparseVector, k: usize) -> Result<Vec<SearchResult>> {
        if query.dim != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim as usize as u32,
                got: query.dim as usize as u32,
            });
        }
        query.validate()?;
        let cands = self.candidates(query);
        let mut hits = Vec::with_capacity(cands.len());
        for id in cands {
            if let Some(v) = self.vectors.get(&id) {
                hits.push(SearchResult::new(id, self.distance(query, v)?));
            }
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

    /// Drop tombstoned rows permanently.
    pub fn compact(&mut self) {
        if self.deleted.is_empty() {
            return;
        }
        let dead: Vec<RowId> = self.deleted.iter().copied().collect();
        for id in dead {
            if let Some(v) = self.vectors.remove(&id) {
                self.deindex(id, &v);
            }
        }
        self.deleted.clear();
        self.generation = self.generation.wrapping_add(1);
    }

    /// validate: see implementation for contract.
    pub fn validate(&self) -> Result<()> {
        for (id, v) in &self.vectors {
            if v.dim != self.dim {
                return Err(VectorError::IndexCorrupted(
                    "sparse dim mismatch".to_string(),
                ));
            }
            v.validate()?;
            for (i, x) in v.indices.iter().zip(v.values.iter()) {
                let found = self
                    .postings
                    .get(i)
                    .map(|l| l.iter().any(|(rid, xv)| rid == id && xv == x))
                    .unwrap_or(false);
                if !found {
                    return Err(VectorError::IndexCorrupted(
                        "posting missing row".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}
