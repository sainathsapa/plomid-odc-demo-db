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
//! Simplified production-safe HNSW.
//!
//! Deterministic LCG levels, bounded degrees, beam search, tombstones,
//! `compact`, `validate` and versioned `encode`/`decode` persistence.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, dense_distance, enc_f32, enc_u32, enc_u64,
    enc_usize, Decoder, Generation, Lcg64, TombstoneMap, DEFAULT_SEED, MAX_LEVEL, MAX_M,
    MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, Metric, RowId, SearchResult, VectorIndex};

/// HNSW persisted-graph layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const HNSW_VERSION: u32 = 1;
/// Default HNSW degree (tunable via `IndexParams::m`; benchmark before changing).
pub const DEFAULT_M: usize = 16;
/// Default HNSW build beam width (tunable via `IndexParams::ef_construction`).
pub const DEFAULT_EF_CONSTRUCTION: usize = 200;
/// Default HNSW query beam width, overridable per query via search options.
pub const DEFAULT_EF_SEARCH: usize = 100;

#[derive(Clone, Debug)]
struct Node {
    row_id: RowId,
    level: usize,
    vector: Vec<f32>,
    /// Neighbours per level (row ids).
    neighbors: Vec<Vec<RowId>>,
}

/// HNSW graph index with deterministic builds and tombstoned deletes.
///
/// Levels are sampled from a seeded LCG so a fixed seed reproduces
/// bit-identical graphs. Deletes tombstone; compaction reclaims. The
/// authoritative vectors stay in table rows — this graph only accelerates.
#[derive(Clone, Debug)]
pub struct HnswIndex {
    config: IndexConfig,
    dim: usize,
    metric: Metric,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    max_level: usize,
    entry: Option<RowId>,
    nodes: HashMap<RowId, Node>,
    tombstones: TombstoneMap,
    generation: Generation,
    rng: Lcg64,
}

impl HnswIndex {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig) -> Result<Self> {
        config.validate()?;
        let m = config.params.m.unwrap_or(DEFAULT_M).clamp(2, MAX_M);
        let ef_construction = config
            .params
            .ef_construction
            .unwrap_or(DEFAULT_EF_CONSTRUCTION)
            .clamp(8, 4096);
        let ef_search = config
            .params
            .ef_search
            .unwrap_or(DEFAULT_EF_SEARCH)
            .clamp(1, 4096);
        let seed = config.params.seed.unwrap_or(DEFAULT_SEED);
        Ok(Self {
            dim: config.dimension,
            metric: config.metric,
            m,
            ef_construction,
            ef_search,
            max_level: 0,
            entry: None,
            nodes: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            generation: Generation::new(),
            rng: Lcg64::new(seed ^ 0x51ab_3f11_9c2d_4a77),
            config,
        })
    }

    /// with_ef_search: see implementation for contract.
    pub fn with_ef_search(&mut self, ef: usize) {
        self.ef_search = ef.clamp(1, 4096);
    }

    fn sample_level(&mut self) -> usize {
        // Geometric distribution, deterministic via LCG.
        let mut lvl = 0usize;
        while lvl + 1 < MAX_LEVEL && self.rng.next_f32() < 0.5 {
            lvl += 1;
        }
        lvl
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        dense_distance(a, b, self.metric)
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.dim, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    fn live_vector(&self, id: RowId) -> Option<&Vec<f32>> {
        if self.tombstones.is_deleted(id) {
            return None;
        }
        self.nodes.get(&id).map(|n| &n.vector)
    }

    fn greedy_descend(
        &self,
        query: &[f32],
        entry: RowId,
        top: usize,
        bottom: usize,
    ) -> Result<RowId> {
        let mut cur = entry;
        let mut cur_vec = self
            .nodes
            .get(&cur)
            .map(|n| n.vector.clone())
            .unwrap_or_default();
        let mut cur_d = self.dist(query, &cur_vec).unwrap_or(f32::MAX);
        for lvl in ((bottom + 1)..=top).rev() {
            let mut improved = true;
            while improved {
                improved = false;
                let nbrs: Vec<RowId> = self
                    .nodes
                    .get(&cur)
                    .map(|n| {
                        if lvl < n.neighbors.len() {
                            n.neighbors[lvl].clone()
                        } else {
                            Vec::new()
                        }
                    })
                    .unwrap_or_default();
                for nb in nbrs {
                    if self.tombstones.is_deleted(nb) {
                        continue;
                    }
                    if let Some(nv) = self.live_vector(nb) {
                        let d = self.dist(query, nv).unwrap_or(f32::MAX);
                        if d < cur_d {
                            cur_d = d;
                            cur = nb;
                            cur_vec = nv.clone();
                            improved = true;
                        }
                    }
                }
            }
        }
        let _ = cur_vec;
        Ok(cur)
    }

    fn beam_layer(
        &self,
        query: &[f32],
        seeds: &[RowId],
        ef: usize,
        level: usize,
    ) -> Result<Vec<(f32, RowId)>> {
        #[derive(Clone, Copy, PartialEq)]
        struct Cand {
            d: f32,
            id: RowId,
        }
        impl Eq for Cand {}
        impl Ord for Cand {
            fn cmp(&self, other: &Self) -> Ordering {
                // reverse for min-heap behaviour via BinaryHeap (max-heap)
                other
                    .d
                    .partial_cmp(&self.d)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| self.id.cmp(&other.id))
            }
        }
        impl PartialOrd for Cand {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        #[derive(Clone, Copy, PartialEq)]
        struct Best {
            d: f32,
            id: RowId,
        }
        impl Eq for Best {}
        impl Ord for Best {
            fn cmp(&self, other: &Self) -> Ordering {
                self.d
                    .partial_cmp(&other.d)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| self.id.cmp(&other.id))
            }
        }
        impl PartialOrd for Best {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut visited: HashSet<RowId> = HashSet::new();
        let mut cands: BinaryHeap<Cand> = BinaryHeap::new(); // min-heap via reversed ord
        let mut best: BinaryHeap<Best> = BinaryHeap::new(); // max-heap of current ef best
        for s in seeds {
            if self.tombstones.is_deleted(*s) {
                continue;
            }
            if let Some(nv) = self.live_vector(*s) {
                let d = self.dist(query, nv)?;
                if visited.insert(*s) {
                    cands.push(Cand { d, id: *s });
                    best.push(Best { d, id: *s });
                    if best.len() > ef {
                        best.pop();
                    }
                }
            }
        }
        if cands.is_empty() {
            return Ok(Vec::new());
        }
        while let Some(c) = cands.pop() {
            let worst_best = best.peek().map(|b| b.d).unwrap_or(f32::MAX);
            if c.d > worst_best {
                break;
            }
            let nbrs: Vec<RowId> = self
                .nodes
                .get(&c.id)
                .map(|n| {
                    if level < n.neighbors.len() {
                        n.neighbors[level].clone()
                    } else {
                        Vec::new()
                    }
                })
                .unwrap_or_default();
            for nb in nbrs {
                if !visited.insert(nb) {
                    continue;
                }
                if self.tombstones.is_deleted(nb) {
                    continue;
                }
                let nv = match self.live_vector(nb) {
                    Some(v) => v.clone(),
                    None => continue,
                };
                let d = self.dist(query, &nv)?;
                if best.len() < ef || d < worst_best {
                    cands.push(Cand { d, id: nb });
                    best.push(Best { d, id: nb });
                    if best.len() > ef {
                        best.pop();
                    }
                }
            }
        }
        let mut out: Vec<(f32, RowId)> = best.into_iter().map(|b| (b.d, b.id)).collect();
        out.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        Ok(out)
    }

    fn select_neighbors(
        &self,
        query: &[f32],
        candidates: &[(f32, RowId)],
        limit: usize,
    ) -> Result<Vec<RowId>> {
        let _ = query;
        let mut c = candidates.to_vec();
        c.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        Ok(c.into_iter().take(limit).map(|(_, id)| id).collect())
    }

    fn connect(&mut self, id: RowId, level: usize, peers: &[RowId], cap: usize) {
        // Only link peers that exist at this layer; lower-level nodes have
        // no adjacency slot here (their neighbor count must stay
        // `level + 1` per `validate`). Precompute under immutable borrows
        // so the edge updates below never grow a node beyond its level.
        let eligible: Vec<RowId> = peers
            .iter()
            .copied()
            .filter(|p| *p != id && self.nodes.get(p).map(|nb| nb.level).unwrap_or(0) >= level)
            .collect();
        if let Some(n) = self.nodes.get_mut(&id) {
            while n.neighbors.len() <= level {
                n.neighbors.push(Vec::new());
            }
            for p in &eligible {
                if !n.neighbors[level].contains(p) {
                    n.neighbors[level].push(*p);
                }
            }
            // deterministic prune: keep sorted by row id beyond cap? keep insertion order truncated.
            // Prefer keeping smallest row ids for determinism when overfull.
            if n.neighbors[level].len() > cap {
                let mut v = std::mem::take(&mut n.neighbors[level]);
                v.sort_unstable();
                v.dedup();
                v.truncate(cap);
                n.neighbors[level] = v;
            }
        }
        for p in &eligible {
            if let Some(nb) = self.nodes.get_mut(p) {
                while nb.neighbors.len() <= level {
                    nb.neighbors.push(Vec::new());
                }
                if !nb.neighbors[level].contains(&id) {
                    nb.neighbors[level].push(id);
                }
                if nb.neighbors[level].len() > cap {
                    let mut v = std::mem::take(&mut nb.neighbors[level]);
                    v.sort_unstable();
                    v.dedup();
                    v.truncate(cap);
                    nb.neighbors[level] = v;
                }
            }
        }
    }

    fn insert_inner(&mut self, row_id: RowId, vector: Vec<f32>, level: usize) -> Result<()> {
        let entry = self.entry;
        // Register node first (empty adjacency).
        let mut neighbors: Vec<Vec<RowId>> = Vec::new();
        neighbors.resize(level + 1, Vec::new());
        self.nodes.insert(
            row_id,
            Node {
                row_id,
                level,
                vector: vector.clone(),
                neighbors,
            },
        );
        if entry.is_none() {
            self.entry = Some(row_id);
            self.max_level = level;
            self.generation.next();
            return Ok(());
        }
        let mut cur_entry = entry.unwrap_or(row_id);
        let top = self.max_level;
        if level < top {
            cur_entry = self.greedy_descend(&vector, cur_entry, top, level)?;
        } else if level > top {
            // new top levels: entry stays for lower layers; update top.
            self.max_level = level;
        }
        let ef = self.ef_construction;
        for lc in (0..=level.min(top.max(level))).rev() {
            if lc > level {
                continue;
            }
            let seeds = vec![cur_entry];
            let cands = self.beam_layer(&vector, &seeds, ef, lc)?;
            // refine cur_entry for next lower layer
            if let Some((_, best)) = cands.first() {
                cur_entry = *best;
            }
            let cap = if lc == 0 { self.m * 2 } else { self.m };
            let peers = self.select_neighbors(&vector, &cands, self.m)?;
            self.connect(row_id, lc, &peers, cap);
        }
        if self.nodes.get(&row_id).map(|n| n.level).unwrap_or(0) > top {
            self.entry = Some(row_id);
        }
        self.generation.next();
        Ok(())
    }

    fn search_inner(&self, query: &[f32], k: usize, ef: usize) -> Result<Vec<SearchResult>> {
        check_dimension(self.dim, query.len())?;
        check_finite(query)?;
        let entry = match self.entry {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        if self.tombstones.is_deleted(entry) {
            // find any live node as fallback entry
            let mut fallback: Option<RowId> = None;
            let mut ids: Vec<RowId> = self.nodes.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                if !self.tombstones.is_deleted(id) {
                    fallback = Some(id);
                    break;
                }
            }
            let Some(fb) = fallback else {
                return Ok(Vec::new());
            };
            let cands = self.beam_layer(query, &[fb], ef.max(k), 0)?;
            return Ok(cands
                .into_iter()
                .take(k)
                .map(|(d, id)| SearchResult::new(id, d))
                .collect());
        }
        let top = self.max_level;
        let cur = self.greedy_descend(query, entry, top, 0)?;
        // beam at layer 0
        let cands = self.beam_layer(query, &[cur], ef.max(k), 0)?;
        let _ = cur;
        Ok(cands
            .into_iter()
            .take(k)
            .map(|(d, id)| SearchResult::new(id, d))
            .collect())
    }

    /// Remove tombstoned nodes and scrub adjacency.
    pub fn compact(&mut self) {
        let dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        if dead.is_empty() {
            return;
        }
        let dead_set: HashSet<RowId> = dead.iter().copied().collect();
        for id in &dead {
            self.nodes.remove(id);
        }
        for node in self.nodes.values_mut() {
            for lvl in node.neighbors.iter_mut() {
                lvl.retain(|nb| !dead_set.contains(nb));
            }
        }
        self.tombstones.clear();
        // fix entry
        if let Some(e) = self.entry {
            if dead_set.contains(&e) {
                let mut ids: Vec<RowId> = self.nodes.keys().copied().collect();
                ids.sort_unstable();
                self.entry = ids.first().copied();
                self.max_level = self
                    .entry
                    .and_then(|id| self.nodes.get(&id))
                    .map(|n| n.level)
                    .unwrap_or(0);
            }
        }
        self.generation.next();
    }

    /// encode: see implementation for contract.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc_u32(&mut out, HNSW_VERSION);
        enc_u32(&mut out, self.dim as u32);
        enc_u32(&mut out, self.m as u32);
        enc_u32(&mut out, self.ef_construction as u32);
        enc_u32(&mut out, self.ef_search as u32);
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
        enc_u64(&mut out, self.entry.unwrap_or(u64::MAX));
        enc_usize(&mut out, self.nodes.len());
        let mut ids: Vec<RowId> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let n = &self.nodes[&id];
            enc_u64(&mut out, n.row_id);
            enc_usize(&mut out, n.level);
            enc_usize(&mut out, n.vector.len());
            for v in &n.vector {
                enc_f32(&mut out, *v);
            }
            enc_usize(&mut out, n.neighbors.len());
            for lvl in &n.neighbors {
                enc_usize(&mut out, lvl.len());
                for nb in lvl {
                    enc_u64(&mut out, *nb);
                }
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
        if v != HNSW_VERSION {
            return Err(VectorError::IndexCorrupted(format!("bad hnsw version {v}")));
        }
        let dim = d.u32()? as usize;
        let m = d.u32()? as usize;
        let efc = d.u32()? as usize;
        let efs = d.u32()? as usize;
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
        let gen = d.u64()?;
        let entry_raw = d.u64()?;
        let n = d.usize_val()?;
        if n > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("too many nodes".to_string()));
        }
        let mut nodes: HashMap<RowId, Node> = HashMap::with_capacity(n.min(1024));
        let mut max_level = 0usize;
        for _ in 0..n {
            let row_id = d.u64()?;
            let level = d.usize_val()?;
            if level >= MAX_LEVEL {
                return Err(VectorError::IndexCorrupted("bad level".to_string()));
            }
            max_level = max_level.max(level);
            let vlen = d.usize_val()?;
            if vlen != dim {
                return Err(VectorError::IndexCorrupted("bad vector len".to_string()));
            }
            let mut vec = Vec::with_capacity(vlen);
            for _ in 0..vlen {
                vec.push(d.f32_val()?);
            }
            let nl = d.usize_val()?;
            if nl > MAX_LEVEL {
                return Err(VectorError::IndexCorrupted(
                    "bad neighbor levels".to_string(),
                ));
            }
            let mut neighbors = Vec::with_capacity(nl);
            for _ in 0..nl {
                let c = d.usize_val()?;
                if c > MAX_M * 4 {
                    return Err(VectorError::IndexCorrupted("degree overflow".to_string()));
                }
                let mut lvl = Vec::with_capacity(c);
                for _ in 0..c {
                    lvl.push(d.u64()?);
                }
                neighbors.push(lvl);
            }
            nodes.insert(
                row_id,
                Node {
                    row_id,
                    level,
                    vector: vec,
                    neighbors,
                },
            );
        }
        let nd = d.usize_val()?;
        let mut tombstones = TombstoneMap::empty();
        for _ in 0..nd {
            let id = d.u64()?;
            tombstones.mark_deleted(id, gen);
        }
        d.finish()?;
        let idx = Self {
            dim,
            metric,
            m: m.clamp(2, MAX_M),
            ef_construction: efc.clamp(8, 4096),
            ef_search: efs.clamp(1, 4096),
            max_level,
            entry: if entry_raw == u64::MAX {
                None
            } else {
                Some(entry_raw)
            },
            nodes,
            tombstones,
            generation: Generation(gen),
            rng: Lcg64::new(config.params.seed.unwrap_or(DEFAULT_SEED)),
            config,
        };
        idx.validate()?;
        let _ = metric;
        Ok(idx)
    }
}

impl VectorIndex for HnswIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        self.nodes.clear();
        self.tombstones.clear();
        self.entry = None;
        self.max_level = 0;
        // Deterministic insertion order: sorted by row id.
        let mut sorted: Vec<(RowId, Vec<f32>)> =
            rows.iter().map(|(id, v)| (*id, v.clone())).collect();
        sorted.sort_by_key(|(id, _)| *id);
        for (id, vec) in sorted {
            self.check_vec(&vec)?;
            let lvl = self.sample_level();
            self.insert_inner(id, vec, lvl)?;
        }
        Ok(())
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        self.check_vec(vector)?;
        if self.nodes.len() >= MAX_VECTORS && !self.nodes.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("hnsw capacity".to_string()));
        }
        // Overwrite: drop old adjacency, reinsert.
        let was_live = self.nodes.contains_key(&row_id);
        if was_live {
            // remove old edges pointing at it
            for n in self.nodes.values_mut() {
                for lvl in n.neighbors.iter_mut() {
                    lvl.retain(|nb| *nb != row_id);
                }
            }
            self.nodes.remove(&row_id);
        }
        self.tombstones.unmark(row_id);
        let lvl = self.sample_level();
        self.insert_inner(row_id, vector.to_vec(), lvl)?;
        Ok(())
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.nodes.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
            return Ok(false);
        }
        let g = self.generation.next();
        self.tombstones.mark_deleted(row_id, g);
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.search_inner(query, k, self.ef_search)
    }

    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>> {
        if !radius.is_finite() {
            return Err(VectorError::IndexParameterInvalid(
                "radius must be finite".to_string(),
            ));
        }
        // Exact-ish radius scan over live vectors (bounded) for correctness.
        check_dimension(self.dim, query.len())?;
        check_finite(query)?;
        let mut out = Vec::new();
        for (id, n) in &self.nodes {
            if self.tombstones.is_deleted(*id) {
                continue;
            }
            let dist = self.dist(query, &n.vector)?;
            if dist <= radius {
                out.push(SearchResult::new(*id, dist));
            }
        }
        out.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        Ok(out)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::Hnsw {
            return Err(VectorError::IndexCorrupted(
                "hnsw kind mismatch".to_string(),
            ));
        }
        for (id, n) in &self.nodes {
            if *id != n.row_id {
                return Err(VectorError::IndexCorrupted("node id mismatch".to_string()));
            }
            check_dimension(self.dim, n.vector.len())?;
            check_finite(&n.vector)?;
            if n.level >= MAX_LEVEL {
                return Err(VectorError::IndexCorrupted("level overflow".to_string()));
            }
            if n.neighbors.len() != n.level + 1 {
                return Err(VectorError::IndexCorrupted(
                    "neighbor layer count".to_string(),
                ));
            }
            for lvl in &n.neighbors {
                if lvl.len() > self.m * 2 + 8 {
                    return Err(VectorError::IndexCorrupted("degree overflow".to_string()));
                }
                for nb in lvl {
                    if *nb == *id {
                        return Err(VectorError::IndexCorrupted("self edge".to_string()));
                    }
                    if !self.nodes.contains_key(nb) {
                        return Err(VectorError::IndexCorrupted(format!(
                            "dangling edge {id}->{nb}"
                        )));
                    }
                }
            }
        }
        if let Some(e) = self.entry {
            if !self.nodes.contains_key(&e) {
                return Err(VectorError::IndexCorrupted("bad entry".to_string()));
            }
        } else if !self.nodes.is_empty() {
            return Err(VectorError::IndexCorrupted("missing entry".to_string()));
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.nodes.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}
