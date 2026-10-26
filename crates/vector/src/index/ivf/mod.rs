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
//! IVF-Flat: deterministic k-means training, posting lists, `nprobe` search.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, dense_distance, enc_f32, enc_u32, enc_u64,
    enc_usize, l2_squared, Decoder, Generation, Lcg64, TombstoneMap, DEFAULT_SEED, MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, Metric, RowId, SearchResult, VectorIndex};

/// IVF posting-list layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const IVF_VERSION: u32 = 1;
/// Default IVF list count; validate against dataset size to avoid empty-list overhead.
pub const DEFAULT_NLIST: usize = 16;
/// Default IVF probed lists per query; recall scales with `nprobe`.
pub const DEFAULT_NPROBE: usize = 4;
const KMEANS_ITERS: usize = 10;

/// IVF-Flat index: deterministic k-means centroids plus exact posting lists.
///
/// Training samples deterministically under the configured seed. Queries
/// probe `nprobe` nearest lists and compute exact distances, so recall is a
/// measured function of `(nlist, nprobe)`, never a guess.
#[derive(Clone, Debug)]
pub struct IvfFlatIndex {
    config: IndexConfig,
    dim: usize,
    metric: Metric,
    nlist: usize,
    nprobe: usize,
    centroids: Vec<Vec<f32>>,
    postings: Vec<Vec<(RowId, Vec<f32>)>>,
    id_to_list: HashMap<RowId, usize>,
    tombstones: TombstoneMap,
    trained: bool,
    generation: Generation,
    seed: u64,
}

impl IvfFlatIndex {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        let nlist = config.params.nlist.unwrap_or(DEFAULT_NLIST).clamp(1, 4096);
        let nprobe = config
            .params
            .nprobe
            .unwrap_or(DEFAULT_NPROBE)
            .clamp(1, 4096);
        let seed = config.params.seed.unwrap_or(DEFAULT_SEED);
        Ok(Self {
            dim: config.dimension,
            metric: config.metric,
            nlist,
            nprobe: nprobe.min(nlist),
            centroids: Vec::new(),
            postings: vec![Vec::new(); nlist],
            id_to_list: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            trained: false,
            generation: Generation::new(),
            seed,
            config,
        })
    }

    /// is_trained: see implementation for contract.
    pub fn is_trained(&self) -> bool {
        self.trained
    }

    /// set_nprobe: see implementation for contract.
    pub fn set_nprobe(&mut self, nprobe: usize) {
        self.nprobe = nprobe.clamp(1, self.nlist);
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.dim, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    /// Deterministic k-means (L2 assignment, mean update).
    pub fn train(&mut self, vectors: &[Vec<f32>]) -> Result<()> {
        if vectors.is_empty() {
            return Err(VectorError::IndexParameterInvalid(
                "train needs vectors".to_string(),
            ));
        }
        for v in vectors {
            self.check_vec(v)?;
        }
        check_budget(vectors.len())?;
        let k = self.nlist.min(vectors.len()).max(1);
        // Deterministic init: strided sample after seeded shuffle of indices.
        let mut rng = Lcg64::new(self.seed ^ 0x1F3B_5C5D_7E91_A647);
        let mut order: Vec<usize> = (0..vectors.len()).collect();
        rng.shuffle(&mut order);
        let mut centroids: Vec<Vec<f32>> =
            order.iter().take(k).map(|&i| vectors[i].clone()).collect();
        // If k < nlist, pad by duplicating with tiny deterministic jitter.
        while centroids.len() < self.nlist {
            let src = centroids[centroids.len() % k].clone();
            let mut rng2 = Lcg64::new(self.seed.wrapping_add(centroids.len() as u64));
            let jitter: Vec<f32> = src.iter().map(|_| (rng2.next_f32() - 0.5) * 1e-3).collect();
            let v: Vec<f32> = src.iter().zip(jitter.iter()).map(|(a, b)| a + b).collect();
            centroids.push(v);
        }
        for _ in 0..KMEANS_ITERS {
            let mut sums = vec![vec![0.0f32; self.dim]; self.nlist];
            let mut counts = vec![0usize; self.nlist];
            for v in vectors {
                let mut best = 0usize;
                let mut best_d = f32::MAX;
                for (i, c) in centroids.iter().enumerate() {
                    let d = l2_squared(v, c);
                    if d < best_d {
                        best_d = d;
                        best = i;
                    }
                }
                for (d, x) in sums[best].iter_mut().zip(v.iter()) {
                    *d += *x;
                }
                counts[best] += 1;
            }
            for i in 0..self.nlist {
                if counts[i] > 0 {
                    let n = counts[i] as f32;
                    for d in 0..self.dim {
                        centroids[i][d] = sums[i][d] / n;
                    }
                }
            }
        }
        self.centroids = centroids;
        self.postings = vec![Vec::new(); self.nlist];
        self.id_to_list.clear();
        self.tombstones.clear();
        self.trained = true;
        self.generation.next();
        Ok(())
    }

    /// assign: see implementation for contract.
    pub fn assign(&self, vector: &[f32]) -> Result<usize> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        let mut best = 0usize;
        let mut best_d = f32::MAX;
        for (i, c) in self.centroids.iter().enumerate() {
            let d = l2_squared(vector, c);
            if d < best_d {
                best_d = d;
                best = i;
            }
        }
        Ok(best)
    }

    fn nearest_lists(&self, query: &[f32], nprobe: usize) -> Result<Vec<usize>> {
        let mut scored: Vec<(f32, usize)> = self
            .centroids
            .iter()
            .enumerate()
            .map(|(i, c)| (l2_squared(query, c), i))
            .collect();
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        Ok(scored
            .into_iter()
            .take(nprobe.min(self.nlist))
            .map(|(_, i)| i)
            .collect())
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        dense_distance(a, b, self.metric)
    }

    /// Drop tombstoned postings permanently.
    pub fn compact(&mut self) {
        if self.tombstones.is_empty() {
            return;
        }
        for list in self.postings.iter_mut() {
            list.retain(|(id, _)| !self.tombstones.is_deleted(*id));
        }
        let dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        for id in dead {
            self.id_to_list.remove(&id);
        }
        self.tombstones.clear();
        self.generation.next();
    }

    /// encode: see implementation for contract.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc_u32(&mut out, IVF_VERSION);
        enc_u32(&mut out, self.dim as u32);
        enc_u32(&mut out, self.nlist as u32);
        enc_u32(&mut out, self.nprobe as u32);
        enc_u32(
            &mut out,
            match self.metric {
                Metric::Cosine => 1,
                Metric::L2 => 2,
                Metric::Dot => 3,
                Metric::Hamming => 4,
                Metric::Jaccard => 5,
            },
        );
        enc_u64(&mut out, self.generation.current());
        enc_u32(&mut out, u32::from(self.trained));
        enc_usize(&mut out, self.centroids.len());
        for c in &self.centroids {
            for v in c {
                enc_f32(&mut out, *v);
            }
        }
        enc_usize(&mut out, self.postings.len());
        for list in &self.postings {
            enc_usize(&mut out, list.len());
            for (id, vec) in list {
                enc_u64(&mut out, *id);
                for v in vec {
                    enc_f32(&mut out, *v);
                }
            }
        }
        out
    }

    /// decode: see implementation for contract.
    pub fn decode(bytes: &[u8], config: IndexConfig) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let v = d.u32()?;
        if v != IVF_VERSION {
            return Err(VectorError::IndexCorrupted("bad ivf version".to_string()));
        }
        let dim = d.u32()? as usize;
        let nlist = d.u32()? as usize;
        let nprobe = d.u32()? as usize;
        let mc = d.u32()?;
        let metric = match mc {
            1 => Metric::Cosine,
            2 => Metric::L2,
            3 => Metric::Dot,
            4 => Metric::Hamming,
            5 => Metric::Jaccard,
            _ => return Err(VectorError::IndexCorrupted("bad metric".to_string())),
        };
        check_dimension(config.dimension, dim)?;
        if nlist == 0 || nlist > 4096 {
            return Err(VectorError::IndexCorrupted("bad nlist".to_string()));
        }
        let gen = d.u64()?;
        let trained = d.u32()? != 0;
        let nc = d.usize_val()?;
        if nc > 4096 {
            return Err(VectorError::IndexCorrupted(
                "too many centroids".to_string(),
            ));
        }
        let mut centroids = Vec::with_capacity(nc);
        for _ in 0..nc {
            let mut c = Vec::with_capacity(dim);
            for _ in 0..dim {
                c.push(d.f32_val()?);
            }
            centroids.push(c);
        }
        let np = d.usize_val()?;
        if np != nlist {
            return Err(VectorError::IndexCorrupted(
                "posting/list mismatch".to_string(),
            ));
        }
        let mut postings: Vec<Vec<(RowId, Vec<f32>)>> = Vec::with_capacity(np);
        let mut id_to_list: HashMap<RowId, usize> = HashMap::new();
        for li in 0..np {
            let m = d.usize_val()?;
            if m > MAX_VECTORS {
                return Err(VectorError::IndexCorrupted("posting too large".to_string()));
            }
            let mut list = Vec::with_capacity(m.min(1024));
            for _ in 0..m {
                let id = d.u64()?;
                let mut vec = Vec::with_capacity(dim);
                for _ in 0..dim {
                    vec.push(d.f32_val()?);
                }
                if id_to_list.insert(id, li).is_some() {
                    return Err(VectorError::IndexCorrupted("duplicate row".to_string()));
                }
                list.push((id, vec));
            }
            postings.push(list);
        }
        d.finish()?;
        let idx = Self {
            dim,
            metric,
            nlist,
            nprobe: nprobe.min(nlist),
            centroids,
            postings,
            id_to_list,
            tombstones: TombstoneMap::empty(),
            trained,
            generation: Generation(gen),
            seed: config.params.seed.unwrap_or(DEFAULT_SEED),
            config,
        };
        idx.validate()?;
        Ok(idx)
    }
}

impl VectorIndex for IvfFlatIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        if rows.is_empty() {
            self.centroids.clear();
            self.postings = vec![Vec::new(); self.nlist];
            self.id_to_list.clear();
            self.tombstones.clear();
            self.trained = false;
            self.generation.next();
            return Ok(());
        }
        let vecs: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
        self.train(&vecs)?;
        // insert postings
        for (id, vec) in rows {
            let li = self.assign(vec)?;
            self.postings[li].push((*id, vec.clone()));
            self.id_to_list.insert(*id, li);
        }
        self.generation.next();
        Ok(())
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        self.check_vec(vector)?;
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        if self.id_to_list.len() >= MAX_VECTORS && !self.id_to_list.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("ivf capacity".to_string()));
        }
        // remove old posting if present
        if let Some(old) = self.id_to_list.get(&row_id).copied() {
            if let Some(list) = self.postings.get_mut(old) {
                list.retain(|(id, _)| *id != row_id);
            }
        }
        let li = self.assign(vector)?;
        self.postings[li].push((row_id, vector.to_vec()));
        self.id_to_list.insert(row_id, li);
        self.tombstones.unmark(row_id);
        self.generation.next();
        Ok(())
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.id_to_list.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
            return Ok(false);
        }
        let g = self.generation.next();
        self.tombstones.mark_deleted(row_id, g);
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        check_dimension(self.dim, query.len())?;
        check_finite(query)?;
        let lists = self.nearest_lists(query, self.nprobe)?;
        let mut hits = Vec::new();
        for li in lists {
            for (id, vec) in &self.postings[li] {
                if self.tombstones.is_deleted(*id) {
                    continue;
                }
                hits.push(SearchResult::new(*id, self.dist(query, vec)?));
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

    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>> {
        if !radius.is_finite() {
            return Err(VectorError::IndexParameterInvalid(
                "radius must be finite".to_string(),
            ));
        }
        // Radius scans all lists for correctness (still bounded).
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        check_dimension(self.dim, query.len())?;
        check_finite(query)?;
        let mut hits = Vec::new();
        for list in &self.postings {
            for (id, vec) in list {
                if self.tombstones.is_deleted(*id) {
                    continue;
                }
                let dist = self.dist(query, vec)?;
                if dist <= radius {
                    hits.push(SearchResult::new(*id, dist));
                }
            }
        }
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        Ok(hits)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::IvfFlat
            && self.config.kind != IndexKind::IvfSq
            && self.config.kind != IndexKind::IvfPq
        {
            return Err(VectorError::IndexCorrupted("ivf kind mismatch".to_string()));
        }
        if self.postings.len() != self.nlist || self.centroids.len() != self.nlist {
            if self.trained {
                return Err(VectorError::IndexCorrupted(
                    "ivf list/centroid mismatch".to_string(),
                ));
            }
        }
        for c in &self.centroids {
            check_dimension(self.dim, c.len())?;
            check_finite(c)?;
        }
        for (li, list) in self.postings.iter().enumerate() {
            for (id, vec) in list {
                check_dimension(self.dim, vec.len())?;
                if self.id_to_list.get(id) != Some(&li) {
                    return Err(VectorError::IndexCorrupted(
                        "ivf posting map mismatch".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.id_to_list.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}
