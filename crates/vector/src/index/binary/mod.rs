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
//! Binary index: bit-packed vectors, exact Hamming scan plus a partitioned
//! inverted map keyed by 64-bit block value for candidate pre-filtering.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{Result, VectorError};

use super::common::{
    check_budget, enc_u32, enc_u64, enc_usize, hamming_distance_words, jaccard_distance_words,
    Decoder, Generation, TombstoneMap, MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, Metric, RowId, SearchResult, VectorIndex};

/// Packed binary artifact layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const BINARY_VERSION: u32 = 1;

/// Default cap on ANN candidates exact-scored per query.
pub const BINARY_ANN_DEFAULT_MAX_CANDIDATES: usize = 4096;

/// ANN probe budget for Hamming multi-index search.
///
/// Multi-index hashing over the per-block inverted map: rows sharing an
/// exact 64-bit block with the query are always probed; `neighborhood >= 1`
/// additionally probes every 1-bit flip of each query block, `>= 2` every
/// 2-bit flip (bounded by [`BinaryAnnProbes::max_candidates`]). Candidates
/// are exact-scored (Hamming) and ordered by `(distance, row_id)`, so output
/// ordering is deterministic for a fixed index state and budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinaryAnnProbes {
    /// Upper bound on candidates exact-scored per query (clamped to `>= 1`).
    pub max_candidates: usize,
    /// Block neighborhood radius in bit flips: 0, 1, or 2 (clamped).
    pub neighborhood: u32,
}

impl Default for BinaryAnnProbes {
    fn default() -> Self {
        Self {
            max_candidates: BINARY_ANN_DEFAULT_MAX_CANDIDATES,
            neighborhood: 1,
        }
    }
}

/// Probe accounting for one [`BinaryIndex::ann_search_words`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct BinaryAnnStats {
    /// Candidates exact-scored (after cap + deterministic truncation).
    pub candidates_examined: usize,
    /// Live rows in the index at query time.
    pub live_rows: usize,
    /// True when no block probe matched and the full live set was scored.
    pub full_scan_fallback: bool,
}

/// Binary bit-packed index with partitioned block lists.
///
/// Dimensions count bits (`ceil(dim/8)` bytes); padding bits are masked on
/// every distance so they never affect Hamming/Jaccard. Non-byte-aligned
/// dimensions (1, 7, 9, 65, …) are first-class, not edge cases.
#[derive(Clone, Debug)]
pub struct BinaryIndex {
    config: IndexConfig,
    dim_bits: usize,
    words: usize,
    entries: HashMap<RowId, Vec<u64>>,
    /// inverted[block] : block value -> rows containing it (exact-block match).
    inverted: Vec<HashMap<u64, Vec<RowId>>>,
    tombstones: TombstoneMap,
    generation: Generation,
}

impl BinaryIndex {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        let dim_bits = config.dimension;
        let words = dim_bits.div_ceil(64);
        let mut inverted = Vec::with_capacity(words);
        for _ in 0..words {
            inverted.push(HashMap::new());
        }
        Ok(Self {
            dim_bits,
            words,
            entries: HashMap::new(),
            inverted,
            tombstones: TombstoneMap::empty(),
            generation: Generation::new(),
            config,
        })
    }

    /// words: see implementation for contract.
    pub fn words(&self) -> usize {
        self.words
    }

    /// Pack big-endian bits (`bytes[0]` holds bits `[0..8)`) into words.
    pub fn pack_bits(bytes: &[u8], dim_bits: usize) -> Result<Vec<u64>> {
        if bytes.len() * 8 < dim_bits {
            return Err(VectorError::IndexParameterInvalid(
                "short bit buffer".to_string(),
            ));
        }
        let words = dim_bits.div_ceil(64);
        let mut out = vec![0u64; words];
        for b in 0..dim_bits {
            let byte = bytes[b / 8];
            let bit = (byte >> (7 - (b % 8))) & 1;
            if bit == 1 {
                let w = b / 64;
                let o = 63 - (b % 64);
                out[w] |= 1u64 << o;
            }
        }
        Ok(out)
    }

    /// Pack raw words (little interpretation, masked to `dim_bits`).
    pub fn pack_words(words: &[u64], dim_bits: usize) -> Result<Vec<u64>> {
        let need = dim_bits.div_ceil(64);
        if words.len() != need {
            return Err(VectorError::DimensionMismatch {
                expected: need as u32,
                got: words.len() as u32,
            });
        }
        let mut out = words.to_vec();
        let excess = need * 64 - dim_bits;
        if excess > 0 {
            if let Some(last) = out.last_mut() {
                let mask = u64::MAX << excess;
                *last &= mask;
            }
        }
        Ok(out)
    }

    fn check_words(&self, words: &[u64]) -> Result<()> {
        if words.len() != self.words {
            return Err(VectorError::DimensionMismatch {
                expected: self.words as u32,
                got: words.len() as u32,
            });
        }
        Ok(())
    }

    fn distance(&self, a: &[u64], b: &[u64]) -> Result<f32> {
        match self.config.metric {
            Metric::Hamming => Ok(hamming_distance_words(a, b) as f32),
            Metric::Jaccard => Ok(jaccard_distance_words(a, b)),
            Metric::L2 => Ok(hamming_distance_words(a, b) as f32),
            Metric::Cosine => {
                // Cosine over {0,1}: 1 - inter / sqrt(na*nb).
                let mut inter = 0u64;
                let mut na = 0u64;
                let mut nb = 0u64;
                for (x, y) in a.iter().zip(b.iter()) {
                    inter += (x & y).count_ones() as u64;
                    na += x.count_ones() as u64;
                    nb += y.count_ones() as u64;
                }
                if na == 0 || nb == 0 {
                    return Ok(1.0);
                }
                Ok(1.0 - (inter as f32) / ((na as f32).sqrt() * (nb as f32).sqrt()))
            }
            Metric::Dot => {
                let mut inter = 0u64;
                for (x, y) in a.iter().zip(b.iter()) {
                    inter += (x & y).count_ones() as u64;
                }
                Ok(-(inter as f32))
            }
        }
    }

    fn index_row(&mut self, row_id: RowId, words: &[u64]) {
        for (b, w) in words.iter().enumerate() {
            self.inverted[b].entry(*w).or_default().push(row_id);
        }
    }

    fn deindex_row(&mut self, row_id: RowId, words: &[u64]) {
        for (b, w) in words.iter().enumerate() {
            if let Some(list) = self.inverted[b].get_mut(w) {
                list.retain(|id| *id != row_id);
                if list.is_empty() {
                    self.inverted[b].remove(w);
                }
            }
        }
    }

    /// Exact scan (correct); inverted map is maintained for block pre-filter
    /// and structural validation.
    fn scan(&self, query: &[u64]) -> Result<Vec<SearchResult>> {
        let mut out = Vec::new();
        for (id, words) in &self.entries {
            if self.tombstones.is_deleted(*id) {
                continue;
            }
            out.push(SearchResult::new(*id, self.distance(query, words)?));
        }
        Ok(out)
    }

    /// Candidate pre-filter: rows sharing at least one block value.
    /// Falls back to the full live set when no block matches.
    pub fn candidates(&self, query: &[u64]) -> Vec<RowId> {
        use std::collections::HashSet;
        let mut set: HashSet<RowId> = HashSet::new();
        for (b, w) in query.iter().enumerate() {
            if let Some(list) = self.inverted.get(b).and_then(|m| m.get(w)) {
                for id in list {
                    if !self.tombstones.is_deleted(*id) {
                        set.insert(*id);
                    }
                }
            }
        }
        if set.is_empty() {
            self.entries
                .keys()
                .copied()
                .filter(|id| !self.tombstones.is_deleted(*id))
                .collect()
        } else {
            set.into_iter().collect()
        }
    }

    /// Approximate Hamming top-`k` with probe accounting.
    ///
    /// Only [`Metric::Hamming`] is accelerated. Every other metric fails
    /// with `IndexUnsupported` — callers must route those to [`Self::scan`]
    /// (exact). In particular Jaccard has no pigeonhole relationship with
    /// block equality: sets sharing an identical 64-bit block can still sit
    /// at Jaccard distance near 1 (cardinality is global), while near-duplicate
    /// dense sets at Jaccard distance ~0 share no exact block with high
    /// probability — so the same inverted structure would both miss and
    /// misrank. Hamming, by contrast, concentrates differences in few blocks
    /// for near neighbors, which is exactly what the block probes exploit.
    ///
    /// Recall is approximate by design (see `neighborhood`); ordering is
    /// deterministic: candidates are row-sorted before the cap, then scored
    /// exactly and ordered by `(distance, row_id)`.
    ///
    /// When probes match fewer than `k` candidates the full live set is
    /// scored instead (`full_scan_fallback`): a short approximate top-k is
    /// never returned silently. Uniform-random-style data frequently falls
    /// into that branch — but not guaranteed: non-word-aligned widths have
    /// degenerate trailing-word blocks whose probe hits alone can fill the
    /// candidate set (measured recall 0.5–0.9 at dim 130 versus 1.0 at
    /// widths 64/128; see `crates/vector/tests/ann_quality_matrix.rs`).
    pub fn ann_search_words_with_stats(
        &self,
        query: &[u64],
        k: usize,
        probes: BinaryAnnProbes,
    ) -> Result<(Vec<SearchResult>, BinaryAnnStats)> {
        if self.config.metric != Metric::Hamming {
            return Err(VectorError::IndexUnsupported(format!(
                "binary ANN accelerates Hamming only (index metric {:?}); route {:?} to exact search",
                self.config.metric, self.config.metric,
            )));
        }
        self.check_words(query)?;
        let live: Vec<(RowId, &[u64])> = self
            .entries
            .iter()
            .filter(|(id, _)| !self.tombstones.is_deleted(**id))
            .map(|(id, words)| (*id, words.as_slice()))
            .collect();
        let mut stats = BinaryAnnStats {
            candidates_examined: 0,
            live_rows: live.len(),
            full_scan_fallback: false,
        };
        if live.is_empty() || k == 0 {
            return Ok((Vec::new(), stats));
        }
        // The candidate cap is a performance knob; `k` is a correctness
        // requirement, so the cap never shrinks the result below top-k.
        let max_candidates = probes.max_candidates.max(1).max(k);
        let neighborhood = probes.neighborhood.min(2);
        let mut set: std::collections::HashSet<RowId> = std::collections::HashSet::new();
        // Radius 0: exact-block matches on every block.
        for (b, w) in query.iter().enumerate() {
            if let Some(list) = self.inverted.get(b).and_then(|m| m.get(w)) {
                for id in list {
                    if !self.tombstones.is_deleted(*id) {
                        set.insert(*id);
                    }
                }
            }
            if set.len() >= max_candidates {
                break;
            }
        }
        // Radius 1..=neighborhood: bit-flip neighbors of each query block.
        // Enumerated in ascending flip order per block so the union is built
        // deterministically; the final row-sort before the cap removes any
        // residual order dependence from hash iteration.
        if set.len() < max_candidates && neighborhood >= 1 {
            'outer: for (b, w) in query.iter().enumerate() {
                let Some(map) = self.inverted.get(b) else {
                    continue;
                };
                for bit in 0..64u32 {
                    let neighbor = w ^ (1u64 << bit);
                    if let Some(list) = map.get(&neighbor) {
                        for id in list {
                            if !self.tombstones.is_deleted(*id) {
                                set.insert(*id);
                                if set.len() >= max_candidates {
                                    break 'outer;
                                }
                            }
                        }
                    }
                    if neighborhood >= 2 {
                        for bit2 in (bit + 1)..64u32 {
                            let neighbor2 = w ^ (1u64 << bit) ^ (1u64 << bit2);
                            if let Some(list) = map.get(&neighbor2) {
                                for id in list {
                                    if !self.tombstones.is_deleted(*id) {
                                        set.insert(*id);
                                        if set.len() >= max_candidates {
                                            break 'outer;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        let fallback = set.is_empty() || set.len() < k;
        let mut candidate_ids: Vec<RowId> = if fallback {
            // Too few probes matched to fill top-k (uniform-random-style data
            // where even true neighbors share no near block): score everything
            // rather than return a short, low-recall top-k. Flagged so recall
            // accounting and EXPLAIN can tell acceleration from fallback.
            // The candidate cap does NOT apply here: truncating the fallback
            // set by row id would silently drop true neighbors and break the
            // exact-recall contract this branch exists to uphold.
            stats.full_scan_fallback = true;
            live.iter().map(|(id, _)| *id).collect()
        } else {
            set.into_iter().collect()
        };
        if !fallback {
            candidate_ids.sort_unstable();
            candidate_ids.truncate(max_candidates);
        }
        stats.candidates_examined = candidate_ids.len();
        let mut hits = Vec::with_capacity(candidate_ids.len().min(k.max(1)));
        for id in candidate_ids {
            let Some(words) = self.entries.get(&id) else {
                continue;
            };
            hits.push(SearchResult::new(
                id,
                hamming_distance_words(query, words) as f32,
            ));
        }
        hits.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        hits.truncate(k);
        Ok((hits, stats))
    }

    /// Approximate Hamming top-`k` (default probe budget).
    ///
    /// Same contract as [`Self::ann_search_words_with_stats`]; Jaccard (and
    /// every non-Hamming metric) is rejected — route it to exact [`Self::scan`].
    pub fn ann_search_words(
        &self,
        query: &[u64],
        k: usize,
        probes: BinaryAnnProbes,
    ) -> Result<Vec<SearchResult>> {
        self.ann_search_words_with_stats(query, k, probes)
            .map(|(hits, _)| hits)
    }

    /// insert_words: see implementation for contract.
    pub fn insert_words(&mut self, row_id: RowId, words: &[u64]) -> Result<()> {
        let packed = Self::pack_words(words, self.dim_bits)?;
        if self.entries.len() >= MAX_VECTORS && !self.entries.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("binary capacity".to_string()));
        }
        if let Some(old) = self.entries.get(&row_id).cloned() {
            self.deindex_row(row_id, &old);
        }
        self.index_row(row_id, &packed);
        self.entries.insert(row_id, packed);
        self.tombstones.unmark(row_id);
        self.generation.next();
        Ok(())
    }

    /// Drop tombstoned entries permanently.
    pub fn compact(&mut self) {
        if self.tombstones.is_empty() {
            return;
        }
        let dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        for id in dead {
            if let Some(words) = self.entries.remove(&id) {
                self.deindex_row(id, &words);
            }
        }
        self.tombstones.clear();
        self.generation.next();
    }

    /// encode: see implementation for contract.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc_u32(&mut out, BINARY_VERSION);
        enc_u32(&mut out, self.dim_bits as u32);
        enc_u64(&mut out, self.generation.current());
        enc_u32(
            &mut out,
            match self.config.metric {
                Metric::Cosine => 1,
                Metric::L2 => 2,
                Metric::Dot => 3,
                Metric::Hamming => 4,
                Metric::Jaccard => 5,
            },
        );
        enc_usize(&mut out, self.entries.len());
        let mut ids: Vec<RowId> = self.entries.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            enc_u64(&mut out, id);
            for w in &self.entries[&id] {
                enc_u64(&mut out, *w);
            }
        }
        enc_usize(&mut out, self.tombstones.len());
        let mut dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        dead.sort_unstable();
        for id in dead {
            enc_u64(&mut out, id);
        }
        out
    }

    /// decode: see implementation for contract.
    pub fn decode(bytes: &[u8], config: IndexConfig) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let v = d.u32()?;
        if v != BINARY_VERSION {
            return Err(VectorError::IndexCorrupted(
                "bad binary version".to_string(),
            ));
        }
        let dim_bits = d.u32()? as usize;
        check_budget(dim_bits)?;
        if dim_bits != config.dimension {
            return Err(VectorError::DimensionMismatch {
                expected: config.dimension as u32,
                got: dim_bits as u32,
            });
        }
        let gen = d.u64()?;
        let mc = d.u32()?;
        let _metric = match mc {
            1..=5 => mc,
            _ => return Err(VectorError::IndexCorrupted("bad metric".to_string())),
        };
        let words = dim_bits.div_ceil(64);
        let m = d.usize_val()?;
        if m > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("too many entries".to_string()));
        }
        let mut idx = Self::new(config)?;
        for _ in 0..m {
            let id = d.u64()?;
            let mut w = Vec::with_capacity(words);
            for _ in 0..words {
                w.push(d.u64()?);
            }
            let packed = Self::pack_words(&w, dim_bits)?;
            idx.index_row(id, &packed);
            idx.entries.insert(id, packed);
        }
        let nd = d.usize_val()?;
        for _ in 0..nd {
            idx.tombstones.mark_deleted(d.u64()?, gen);
        }
        d.finish()?;
        idx.generation = Generation(gen);
        idx.validate()?;
        Ok(idx)
    }
}

impl VectorIndex for BinaryIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        // Dense f32 rows are binarized at 0.0 for the generic trait path.
        check_budget(rows.len())?;
        self.entries.clear();
        for m in self.inverted.iter_mut() {
            m.clear();
        }
        self.tombstones.clear();
        for (id, vec) in rows {
            if vec.len() != self.dim_bits {
                return Err(VectorError::DimensionMismatch {
                    expected: self.dim_bits as u32,
                    got: vec.len() as u32,
                });
            }
            let mut words = vec![0u64; self.words];
            for (b, x) in vec.iter().enumerate() {
                if *x > 0.0 {
                    let w = b / 64;
                    let o = 63 - (b % 64);
                    words[w] |= 1u64 << o;
                }
            }
            self.index_row(*id, &words);
            self.entries.insert(*id, words);
        }
        self.generation.next();
        Ok(())
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        if vector.len() == self.words {
            // Already word-scaled floats? Not valid; require dim match.
        }
        if vector.len() != self.dim_bits {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim_bits as u32,
                got: vector.len() as u32,
            });
        }
        // Binarize at 0.
        let mut words = vec![0u64; self.words];
        for (b, x) in vector.iter().enumerate() {
            if !x.is_finite() {
                return Err(VectorError::IndexParameterInvalid(
                    "non-finite bit".to_string(),
                ));
            }
            if *x > 0.0 {
                let w = b / 64;
                let o = 63 - (b % 64);
                words[w] |= 1u64 << o;
            }
        }
        self.insert_words(row_id, &words)
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.entries.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
            return Ok(false);
        }
        let g = self.generation.next();
        self.tombstones.mark_deleted(row_id, g);
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        if query.len() != self.dim_bits {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim_bits as u32,
                got: query.len() as u32,
            });
        }
        let mut words = vec![0u64; self.words];
        for (b, x) in query.iter().enumerate() {
            if !x.is_finite() {
                return Err(VectorError::IndexParameterInvalid(
                    "non-finite bit".to_string(),
                ));
            }
            if *x > 0.0 {
                let w = b / 64;
                let o = 63 - (b % 64);
                words[w] |= 1u64 << o;
            }
        }
        let mut hits = self.scan(&words)?;
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
        let mut hits = self.search(query, self.entries.len())?;
        hits.retain(|h| h.distance <= radius);
        Ok(hits)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::Binary {
            return Err(VectorError::IndexCorrupted(
                "binary kind mismatch".to_string(),
            ));
        }
        if self.inverted.len() != self.words {
            return Err(VectorError::IndexCorrupted(
                "inverted block count".to_string(),
            ));
        }
        for (id, words) in &self.entries {
            self.check_words(words)?;
            // every block value must list this row
            for (b, w) in words.iter().enumerate() {
                let listed = self.inverted[b]
                    .get(w)
                    .map(|l| l.contains(id))
                    .unwrap_or(false);
                if !listed {
                    return Err(VectorError::IndexCorrupted(
                        "inverted missing row".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.entries.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}
