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
//! Scalar quantization (SQ8): per-dimension min/max, 8-bit codes,
//! dequantized exact search. Global scan: no coarse quantizer or posting
//! lists in v1 (the `ivf_sq` SQL method name is a compatibility label).

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, dense_distance, enc_f32, enc_u32, enc_u64,
    enc_usize, Decoder, Generation, TombstoneMap, MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, RowId, SearchResult, VectorIndex};

/// SQ8 codebook layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const SQ_VERSION: u32 = 1;

/// SQ8 scalar-quantization index: per-dimension min/max plus one byte/lane.
///
/// Quantizer metadata persists with the index. Search dequantizes codes to
/// approximations, then exact-reranks from authoritative vectors when
/// configured — never presenting quantized distances as exact.
#[derive(Clone, Debug)]
pub struct Sq8Index {
    config: IndexConfig,
    dim: usize,
    mins: Vec<f32>,
    maxs: Vec<f32>,
    codes: HashMap<RowId, Vec<u8>>,
    tombstones: TombstoneMap,
    trained: bool,
    generation: Generation,
}

impl Sq8Index {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            dim: config.dimension,
            mins: vec![0.0; config.dimension],
            maxs: vec![0.0; config.dimension],
            codes: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            trained: false,
            generation: Generation::new(),
            config,
        })
    }

    /// is_trained: see implementation for contract.
    pub fn is_trained(&self) -> bool {
        self.trained
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.dim, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    /// train: see implementation for contract.
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
        let mut mins = vec![f32::MAX; self.dim];
        let mut maxs = vec![f32::MIN; self.dim];
        for v in vectors {
            for (d, x) in v.iter().enumerate() {
                if *x < mins[d] {
                    mins[d] = *x;
                }
                if *x > maxs[d] {
                    maxs[d] = *x;
                }
            }
        }
        // Guard degenerate ranges (constant lane, or NaN-infected training
        // data): widen by 1.0 instead of dividing by zero below.
        for d in 0..self.dim {
            if maxs[d]
                .partial_cmp(&mins[d])
                .is_none_or(|ord| ord != std::cmp::Ordering::Greater)
            {
                maxs[d] = mins[d] + 1.0;
            }
        }
        self.mins = mins;
        self.maxs = maxs;
        self.codes.clear();
        self.tombstones.clear();
        self.trained = true;
        self.generation.next();
        Ok(())
    }

    /// quantize: see implementation for contract.
    pub fn quantize(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        self.check_vec(vector)?;
        let mut out = Vec::with_capacity(self.dim);
        for (d, x) in vector.iter().enumerate() {
            let span = self.maxs[d] - self.mins[d];
            let norm = ((x - self.mins[d]) / span).clamp(0.0, 1.0);
            let t = (norm * 255.0).round() as u8;
            out.push(t);
        }
        Ok(out)
    }

    /// dequantize: see implementation for contract.
    pub fn dequantize(&self, code: &[u8]) -> Result<Vec<f32>> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        if code.len() != self.dim {
            return Err(VectorError::IndexParameterInvalid(
                "bad code length".to_string(),
            ));
        }
        let mut out = Vec::with_capacity(self.dim);
        for (d, b) in code.iter().enumerate() {
            let span = self.maxs[d] - self.mins[d];
            out.push(self.mins[d] + (*b as f32) / 255.0 * span);
        }
        Ok(out)
    }

    /// Drop tombstoned codes permanently.
    pub fn compact(&mut self) {
        if self.tombstones.is_empty() {
            return;
        }
        let dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        for id in dead {
            self.codes.remove(&id);
        }
        self.tombstones.clear();
        self.generation.next();
    }

    /// encode: see implementation for contract.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc_u32(&mut out, SQ_VERSION);
        enc_u32(&mut out, self.dim as u32);
        enc_u64(&mut out, self.generation.current());
        enc_u32(&mut out, u32::from(self.trained));
        for v in &self.mins {
            enc_f32(&mut out, *v);
        }
        for v in &self.maxs {
            enc_f32(&mut out, *v);
        }
        enc_usize(&mut out, self.codes.len());
        let mut ids: Vec<RowId> = self.codes.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            enc_u64(&mut out, id);
            let code = &self.codes[&id];
            enc_usize(&mut out, code.len());
            out.extend_from_slice(code);
        }
        out
    }

    /// decode: see implementation for contract.
    pub fn decode(bytes: &[u8], config: IndexConfig) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let v = d.u32()?;
        if v != SQ_VERSION {
            return Err(VectorError::IndexCorrupted("bad sq version".to_string()));
        }
        let dim = d.u32()? as usize;
        check_dimension(config.dimension, dim)?;
        let gen = d.u64()?;
        let trained = d.u32()? != 0;
        let mut mins = Vec::with_capacity(dim);
        let mut maxs = Vec::with_capacity(dim);
        for _ in 0..dim {
            mins.push(d.f32_val()?);
        }
        for _ in 0..dim {
            maxs.push(d.f32_val()?);
        }
        let m = d.usize_val()?;
        if m > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("too many codes".to_string()));
        }
        let mut codes = HashMap::with_capacity(m.min(1024));
        for _ in 0..m {
            let id = d.u64()?;
            let n = d.usize_val()?;
            if n != dim {
                return Err(VectorError::IndexCorrupted("bad code length".to_string()));
            }
            // The length prefix was already consumed above (`enc_usize`
            // writes a bare u64, not an `enc_bytes` frame): take exactly
            // `n` raw code bytes. (`d.bytes()` would misread the first
            // code lanes as a second prefix.)
            codes.insert(id, d.take(n)?.to_vec());
        }
        d.finish()?;
        let idx = Self {
            dim,
            mins,
            maxs,
            codes,
            tombstones: TombstoneMap::empty(),
            trained,
            generation: Generation(gen),
            config,
        };
        idx.validate()?;
        Ok(idx)
    }
}

impl VectorIndex for Sq8Index {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        if rows.is_empty() {
            self.codes.clear();
            self.trained = false;
            self.generation.next();
            return Ok(());
        }
        let vecs: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
        self.train(&vecs)?;
        for (id, vec) in rows {
            self.codes.insert(*id, self.quantize(vec)?);
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
        if self.codes.len() >= MAX_VECTORS && !self.codes.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("sq capacity".to_string()));
        }
        self.codes.insert(row_id, self.quantize(vector)?);
        self.tombstones.unmark(row_id);
        self.generation.next();
        Ok(())
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.codes.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
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
        let mut hits = Vec::with_capacity(self.codes.len());
        for (id, code) in &self.codes {
            if self.tombstones.is_deleted(*id) {
                continue;
            }
            let approx = self.dequantize(code)?;
            hits.push(SearchResult::new(
                *id,
                dense_distance(query, &approx, self.config.metric)?,
            ));
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
        let mut hits = self.search(query, self.codes.len())?;
        hits.retain(|h| h.distance <= radius);
        Ok(hits)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::IvfSq {
            return Err(VectorError::IndexCorrupted("sq kind mismatch".to_string()));
        }
        check_dimension(self.dim, self.mins.len())?;
        check_dimension(self.dim, self.maxs.len())?;
        for (id, code) in &self.codes {
            if code.len() != self.dim {
                return Err(VectorError::IndexCorrupted(format!("bad code for {id}")));
            }
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.codes.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}
