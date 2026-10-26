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
//! 8-bit product quantization: `n_segments` must divide `dim`.
//! Per-subspace codebooks (<=256 entries), `encode`/`decode`, ADC search.
//! Global scan: no coarse quantizer or posting lists in v1 (the `ivf_pq`
//! SQL method name is a compatibility label).

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, enc_f32, enc_u32, enc_u64, enc_usize, l2_squared,
    Decoder, Generation, Lcg64, TombstoneMap, DEFAULT_SEED, MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, Metric, RowId, SearchResult, VectorIndex};

/// PQ codebook layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const PQ_VERSION: u32 = 1;
/// PQ codebook entries per segment (8-bit codes; fixed by the v1 format).
pub const PQ_CODEBOOK_SIZE: usize = 256;
/// Segments used when `IndexParams::n_segments` is unset.
///
/// The executor always builds with default params, so this is also the
/// divisor the DDL executable envelope enforces on the column dimension
/// (see `method_executable_for_type`): an `ivf_pq` index on a dimension
/// that is not a multiple of this constant could never train.
pub const PQ_DEFAULT_SEGMENTS: usize = 8;
const PQ_TRAIN_ITERS: usize = 8;

/// PQ index: 8-bit product-quantized codes scored with asymmetric distance
/// computation over **all** live rows (global scan, no inverted lists).
///
/// `n_segments` must divide the dimension. Candidates come from asymmetric
/// distance computation against codebooks; production queries exact-rerank
/// the oversampled shortlist from authoritative rows.
///
/// Historical note: this type is named `IvfPq` and the SQL method is
/// `ivf_pq` for compatibility. v0.1.0 does not implement a coarse IVF
/// quantizer or posting lists here; only [`crate::index::ivf::IvfFlatIndex`]
/// partitions.
#[derive(Clone, Debug)]
pub struct PqIndex {
    config: IndexConfig,
    dim: usize,
    n_segments: usize,
    sub_dim: usize,
    /// codebooks[sub][centroid][sub_dim]
    codebooks: Vec<Vec<Vec<f32>>>,
    codes: HashMap<RowId, Vec<u8>>,
    tombstones: TombstoneMap,
    trained: bool,
    generation: Generation,
    seed: u64,
}

impl PqIndex {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        let n_segments = config.params.n_segments.unwrap_or(PQ_DEFAULT_SEGMENTS);
        if !config.dimension.is_multiple_of(n_segments) {
            return Err(VectorError::IndexParameterInvalid(format!(
                "segments {n_segments} must divide dim {}",
                config.dimension
            )));
        }
        if n_segments == 0 || n_segments > config.dimension {
            return Err(VectorError::IndexParameterInvalid(
                "bad segment count".to_string(),
            ));
        }
        Ok(Self {
            dim: config.dimension,
            sub_dim: config.dimension / n_segments,
            n_segments,
            codebooks: Vec::new(),
            codes: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            trained: false,
            generation: Generation::new(),
            seed: config.params.seed.unwrap_or(DEFAULT_SEED),
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

    fn sub<'a>(&self, v: &'a [f32], seg: usize) -> &'a [f32] {
        let s = seg * self.sub_dim;
        &v[s..s + self.sub_dim]
    }

    /// Train one codebook per subspace with deterministic k-means.
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
        let mut codebooks: Vec<Vec<Vec<f32>>> = Vec::with_capacity(self.n_segments);
        for seg in 0..self.n_segments {
            let k = PQ_CODEBOOK_SIZE.min(vectors.len()).max(1);
            let mut rng = Lcg64::new(self.seed.wrapping_add(seg as u64 * 0x9E37_79B9));
            let mut order: Vec<usize> = (0..vectors.len()).collect();
            rng.shuffle(&mut order);
            let mut centers: Vec<Vec<f32>> = order
                .iter()
                .take(k)
                .map(|&i| self.sub(&vectors[i], seg).to_vec())
                .collect();
            for _ in 0..PQ_TRAIN_ITERS {
                let mut sums = vec![vec![0.0f32; self.sub_dim]; k];
                let mut counts = vec![0usize; k];
                for v in vectors {
                    let s = self.sub(v, seg);
                    let mut best = 0usize;
                    let mut best_d = f32::MAX;
                    for (i, c) in centers.iter().enumerate() {
                        let d = l2_squared(s, c);
                        if d < best_d {
                            best_d = d;
                            best = i;
                        }
                    }
                    for (d, x) in sums[best].iter_mut().zip(s.iter()) {
                        *d += *x;
                    }
                    counts[best] += 1;
                }
                for i in 0..k {
                    if counts[i] > 0 {
                        let n = counts[i] as f32;
                        for d in 0..self.sub_dim {
                            centers[i][d] = sums[i][d] / n;
                        }
                    }
                }
            }
            codebooks.push(centers);
        }
        self.codebooks = codebooks;
        self.codes.clear();
        self.tombstones.clear();
        self.trained = true;
        self.generation.next();
        Ok(())
    }

    /// Quantize one vector to `n_segments` bytes.
    pub fn encode_vector(&self, vector: &[f32]) -> Result<Vec<u8>> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        self.check_vec(vector)?;
        let mut out = Vec::with_capacity(self.n_segments);
        for seg in 0..self.n_segments {
            let s = self.sub(vector, seg);
            let mut best = 0u8;
            let mut best_d = f32::MAX;
            for (i, c) in self.codebooks[seg].iter().enumerate() {
                let d = l2_squared(s, c);
                if d < best_d {
                    best_d = d;
                    best = i as u8;
                }
            }
            out.push(best);
        }
        Ok(out)
    }

    /// Reconstruct an approximate vector from codes.
    pub fn decode_vector(&self, code: &[u8]) -> Result<Vec<f32>> {
        if !self.trained {
            return Err(VectorError::IndexStateInvalid(
                "index is not trained".to_string(),
            ));
        }
        if code.len() != self.n_segments {
            return Err(VectorError::IndexParameterInvalid(
                "bad code length".to_string(),
            ));
        }
        let mut out = Vec::with_capacity(self.dim);
        for (seg, b) in code.iter().enumerate() {
            let idx = *b as usize;
            if idx >= self.codebooks[seg].len() {
                return Err(VectorError::IndexCorrupted("code out of range".to_string()));
            }
            out.extend_from_slice(&self.codebooks[seg][idx]);
        }
        Ok(out)
    }

    /// Asymmetric distance computation table for one query.
    fn adc_table(&self, query: &[f32]) -> Result<Vec<Vec<f32>>> {
        let mut table = vec![vec![0.0f32; PQ_CODEBOOK_SIZE]; self.n_segments];
        for (seg, row) in table.iter_mut().enumerate().take(self.n_segments) {
            let s = self.sub(query, seg);
            for (i, c) in self.codebooks[seg].iter().enumerate() {
                row[i] = match self.config.metric {
                    Metric::L2 => l2_squared(s, c),
                    Metric::Cosine => {
                        let d = super::common::dot(s, c);
                        let ns = super::common::dot(s, s).sqrt();
                        let nc = super::common::dot(c, c).sqrt();
                        if ns == 0.0 || nc == 0.0 {
                            1.0
                        } else {
                            1.0 - (d / (ns * nc)).clamp(-1.0, 1.0)
                        }
                    }
                    Metric::Dot => -super::common::dot(s, c),
                    Metric::Hamming | Metric::Jaccard => {
                        return Err(VectorError::IndexUnsupported(
                            "pq needs Cosine/L2/Dot".to_string(),
                        ))
                    }
                };
            }
        }
        Ok(table)
    }

    fn adc_distance(table: &[Vec<f32>], code: &[u8]) -> f32 {
        let mut acc = 0.0f32;
        for (seg, b) in code.iter().enumerate() {
            acc += table[seg][*b as usize];
        }
        acc
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
        enc_u32(&mut out, PQ_VERSION);
        enc_u32(&mut out, self.dim as u32);
        enc_u32(&mut out, self.n_segments as u32);
        enc_u64(&mut out, self.generation.current());
        enc_u32(&mut out, u32::from(self.trained));
        enc_usize(&mut out, self.codebooks.len());
        for cb in &self.codebooks {
            enc_usize(&mut out, cb.len());
            for c in cb {
                for v in c {
                    enc_f32(&mut out, *v);
                }
            }
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
        if v != PQ_VERSION {
            return Err(VectorError::IndexCorrupted("bad pq version".to_string()));
        }
        let dim = d.u32()? as usize;
        let n_segments = d.u32()? as usize;
        check_dimension(config.dimension, dim)?;
        if !dim.is_multiple_of(n_segments) {
            return Err(VectorError::IndexCorrupted(
                "segments do not divide dim".to_string(),
            ));
        }
        let gen = d.u64()?;
        let trained = d.u32()? != 0;
        let ncb = d.usize_val()?;
        let sub_dim = dim / n_segments;
        let mut codebooks = Vec::with_capacity(ncb);
        for _ in 0..ncb {
            let k = d.usize_val()?;
            if k > PQ_CODEBOOK_SIZE {
                return Err(VectorError::IndexCorrupted(
                    "codebook too large".to_string(),
                ));
            }
            let mut cb = Vec::with_capacity(k);
            for _ in 0..k {
                let mut c = Vec::with_capacity(sub_dim);
                for _ in 0..sub_dim {
                    c.push(d.f32_val()?);
                }
                cb.push(c);
            }
            codebooks.push(cb);
        }
        let m = d.usize_val()?;
        if m > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("too many codes".to_string()));
        }
        let mut codes: HashMap<RowId, Vec<u8>> = HashMap::with_capacity(m.min(1024));
        for _ in 0..m {
            let id = d.u64()?;
            let n = d.usize_val()?;
            if n != n_segments {
                return Err(VectorError::IndexCorrupted("bad code length".to_string()));
            }
            // Same contract as SQ: the u64 prefix is already consumed, so
            // take the raw code bytes (not a second `enc_bytes` frame).
            let raw = d.take(n)?;
            codes.insert(id, raw.to_vec());
        }
        d.finish()?;
        let idx = Self {
            dim,
            n_segments,
            sub_dim,
            codebooks,
            codes,
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

impl VectorIndex for PqIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        if rows.is_empty() {
            self.codebooks.clear();
            self.codes.clear();
            self.trained = false;
            self.generation.next();
            return Ok(());
        }
        let vecs: Vec<Vec<f32>> = rows.iter().map(|(_, v)| v.clone()).collect();
        self.train(&vecs)?;
        for (id, vec) in rows {
            self.codes.insert(*id, self.encode_vector(vec)?);
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
            return Err(VectorError::ResourceLimit("pq capacity".to_string()));
        }
        self.codes.insert(row_id, self.encode_vector(vector)?);
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
        let table = self.adc_table(query)?;
        let mut hits: Vec<SearchResult> = self
            .codes
            .iter()
            .filter(|(id, _)| !self.tombstones.is_deleted(**id))
            .map(|(id, code)| SearchResult::new(*id, Self::adc_distance(&table, code)))
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
        if self.config.kind != IndexKind::IvfPq {
            return Err(VectorError::IndexCorrupted("pq kind mismatch".to_string()));
        }
        if !self.dim.is_multiple_of(self.n_segments) {
            return Err(VectorError::IndexCorrupted(
                "segments do not divide dim".to_string(),
            ));
        }
        for cb in &self.codebooks {
            if cb.len() > PQ_CODEBOOK_SIZE || cb.is_empty() {
                if self.trained {
                    return Err(VectorError::IndexCorrupted("bad codebook size".to_string()));
                }
            }
            for c in cb {
                check_dimension(self.sub_dim, c.len())?;
                check_finite(c)?;
            }
        }
        for (id, code) in &self.codes {
            if code.len() != self.n_segments {
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
