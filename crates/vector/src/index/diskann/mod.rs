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
//! DiskANN-style index: bounded in-memory graph cache plus a file-backed
//! segment store under a data directory. Vectors spill to `vectors.dat`
//! (fixed records); the graph and offsets flush to `graph.dat`.
//! Beam search pages missing vectors in from disk on demand.
//!
//! NON-AUTHORITATIVE derived cache (see [`crate::durability`]): the segment
//! files accelerate queries only. Authoritative vectors live in
//! storage-engine table rows; the executor always rebuilds this index from
//! snapshot-visible rows before serving (`recover` merely classifies the old
//! checkpoint for logging), and a missing/torn/version-skewed checkpoint is
//! rebuild evidence, never served corruption.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::{Result, VectorError};

use super::common::{
    check_budget, check_dimension, check_finite, dense_distance, enc_u32, enc_u64, enc_usize,
    io_fail, Decoder, Generation, Lcg64, TombstoneMap, DEFAULT_SEED, MAX_VECTORS,
};
use super::{IndexConfig, IndexKind, IndexStats, RowId, SearchResult, VectorIndex};

/// SSD graph artifact layout version.
///
/// See encode/decode for the framed layout; mismatches fail closed.
pub const DISKANN_VERSION: u32 = 1;
const RECORD_MAGIC: u32 = 0x4441_4E4E; // "DANN"
const MAX_DEGREE: usize = 32;
const DEFAULT_CACHE: usize = 1024;
/// Out-degree slots reserved for deterministic long-range peers in
/// [`DiskAnnIndex::repair_edges`]; the rest go to nearest neighbours.
const LONG_RANGE_DEGREE: usize = 4;
/// Domain salt for the long-range peer sampling stream.
const LONG_RANGE_SALT: u64 = 0xD15E_A5E0_1234_5678;
/// Upper bound on the per-node candidate sample scored by edge repair.
const CANDIDATE_SAMPLE: usize = 1024;

/// Crash-recovery outcome for persisted DiskANN segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskAnnRecovery {
    /// Checkpointed graph validated and adopted.
    Ready,
    /// Caller must rebuild (from authoritative rows) for `reason`.
    RebuildRequired {
        /// Human-readable cause (missing/torn/version-skewed checkpoint).
        reason: String,
    },
}

/// SSD-oriented graph index with a bounded RAM resident set.
///
/// Graph adjacency and hot vectors stay resident; cold payloads page from
/// segment files with batched I/O. RAM/SSD bytes per query are measured in
/// benchmarks, not assumed. Never persists device pointers — only portable bytes.
#[derive(Clone, Debug)]
pub struct DiskAnnIndex {
    config: IndexConfig,
    dim: usize,
    data_dir: PathBuf,
    graph: HashMap<RowId, Vec<RowId>>,
    cache: HashMap<RowId, Vec<f32>>,
    offsets: HashMap<RowId, u64>,
    tombstones: TombstoneMap,
    generation: Generation,
    max_cached: usize,
    seed: u64,
}

impl DiskAnnIndex {
    /// Creates an empty index after validating `config`.
    ///
    /// Dimension/metric/kind mismatches fail here with
    /// `IndexParameterInvalid`; persisted-byte mismatches fail at decode.
    pub fn new(config: IndexConfig, data_dir: impl Into<PathBuf>) -> Result<Self> {
        config.validate()?;
        let data_dir = data_dir.into();
        Ok(Self {
            dim: config.dimension,
            data_dir,
            graph: HashMap::new(),
            cache: HashMap::new(),
            offsets: HashMap::new(),
            tombstones: TombstoneMap::empty(),
            generation: Generation::new(),
            max_cached: DEFAULT_CACHE,
            seed: config.params.seed.unwrap_or(DEFAULT_SEED),
            config,
        })
    }

    /// open: see implementation for contract.
    pub fn open(config: IndexConfig, data_dir: impl Into<PathBuf>) -> Result<Self> {
        let (index, _) = Self::recover(config, data_dir)?;
        Ok(index)
    }

    /// Crash-recovery classification for the persisted segments.
    ///
    /// Never fails the caller: every outcome is actionable —
    /// [`DiskAnnRecovery::Ready`] adopts the checkpointed graph,
    /// [`DiskAnnRecovery::RebuildRequired`] carries the human-readable
    /// reason and returns a fresh index to rebuild into. A torn or
    /// version-skewed `graph.dat` is evidence for rebuild, not corruption
    /// served to queries (authoritative vectors live in table rows).
    pub fn recover(
        config: IndexConfig,
        data_dir: impl Into<PathBuf>,
    ) -> Result<(Self, DiskAnnRecovery)> {
        let data_dir = data_dir.into();
        let fresh = Self::new(config.clone(), &data_dir)?;
        let graph_path = fresh.graph_path();
        if !graph_path.exists() {
            return Ok((
                fresh,
                DiskAnnRecovery::RebuildRequired {
                    reason: "no graph checkpoint".to_string(),
                },
            ));
        }
        let bytes = match std::fs::read(&graph_path) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Ok((
                    fresh,
                    DiskAnnRecovery::RebuildRequired {
                        reason: format!("unreadable graph checkpoint: {e}"),
                    },
                ));
            }
        };
        let loaded = match Self::decode_state(&bytes, config.clone(), data_dir) {
            Ok(loaded) => loaded,
            Err(e) => {
                return Ok((
                    fresh,
                    DiskAnnRecovery::RebuildRequired {
                        reason: format!("graph checkpoint undecodable: {e}"),
                    },
                ));
            }
        };
        if loaded.dim != config.dimension || loaded.config.metric != config.metric {
            return Ok((
                fresh,
                DiskAnnRecovery::RebuildRequired {
                    reason: "graph checkpoint identity mismatch".to_string(),
                },
            ));
        }
        if let Err(e) = loaded.validate() {
            return Ok((
                fresh,
                DiskAnnRecovery::RebuildRequired {
                    reason: format!("graph checkpoint invalid: {e}"),
                },
            ));
        }
        Ok((loaded, DiskAnnRecovery::Ready))
    }

    fn vectors_path(&self) -> PathBuf {
        self.data_dir.join("vectors.dat")
    }

    fn graph_path(&self) -> PathBuf {
        self.data_dir.join("graph.dat")
    }

    fn ensure_dir(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir).map_err(io_fail)?;
        Ok(())
    }

    /// Persist directory entries after file creation/rename so a crash
    /// cannot lose the name itself. Unix-only: opening a directory is not
    /// portable (Windows rejects it), so other platforms skip this step —
    /// file-level `sync_all` still holds there.
    fn sync_dir(dir: &Path) -> Result<()> {
        #[cfg(unix)]
        {
            std::fs::File::open(dir)
                .and_then(|f| f.sync_all())
                .map_err(io_fail)?;
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
        }
        Ok(())
    }

    fn check_vec(&self, v: &[f32]) -> Result<()> {
        check_dimension(self.dim, v.len())?;
        check_finite(v)?;
        Ok(())
    }

    fn dist(&self, a: &[f32], b: &[f32]) -> Result<f32> {
        dense_distance(a, b, self.config.metric)
    }

    /// Fetch a vector, paging from disk when evicted from the cache.
    fn get_vector(&mut self, row_id: RowId) -> Result<Option<Vec<f32>>> {
        if self.tombstones.is_deleted(row_id) {
            return Ok(None);
        }
        if let Some(v) = self.cache.get(&row_id) {
            return Ok(Some(v.clone()));
        }
        let Some(off) = self.offsets.get(&row_id).copied() else {
            return Ok(None);
        };
        let v = Self::read_record_at(&self.vectors_path(), off, self.dim)?;
        self.admit_cache(row_id, v.clone());
        Ok(Some(v))
    }

    fn admit_cache(&mut self, row_id: RowId, v: Vec<f32>) {
        if self.cache.len() >= self.max_cached && !self.cache.contains_key(&row_id) {
            // Deterministic eviction: smallest row id (bounded RAM, no LRU bookkeeping).
            if let Some(min) = self.cache.keys().copied().min() {
                self.cache.remove(&min);
            }
        }
        self.cache.insert(row_id, v);
    }

    fn read_record_at(path: &Path, offset: u64, dim: usize) -> Result<Vec<f32>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(path).map_err(io_fail)?;
        f.seek(SeekFrom::Start(offset)).map_err(io_fail)?;
        let mut hdr = [0u8; 16];
        f.read_exact(&mut hdr)
            .map_err(|e| VectorError::IndexCorrupted(e.to_string()))?;
        let magic = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let got_dim = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
        let _row = u64::from_le_bytes([
            hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15],
        ]);
        if magic != RECORD_MAGIC || got_dim != dim {
            return Err(VectorError::IndexCorrupted("bad record header".to_string()));
        }
        let mut buf = vec![0u8; dim * 4];
        f.read_exact(&mut buf)
            .map_err(|e| VectorError::IndexCorrupted(e.to_string()))?;
        let mut out = Vec::with_capacity(dim);
        for chunk in buf.chunks_exact(4) {
            out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(out)
    }

    fn append_record(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        use std::io::Write;
        self.ensure_dir()?;
        let path = self.vectors_path();
        let offset = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_fail)?;
        f.write_all(&RECORD_MAGIC.to_le_bytes()).map_err(io_fail)?;
        f.write_all(&(self.dim as u32).to_le_bytes())
            .map_err(io_fail)?;
        f.write_all(&row_id.to_le_bytes()).map_err(io_fail)?;
        for v in vector {
            f.write_all(&v.to_le_bytes()).map_err(io_fail)?;
        }
        f.flush().map_err(io_fail)?;
        // Durability: a committed vector must survive a crash, so the
        // record reaches stable storage before success is reported.
        f.sync_all().map_err(io_fail)?;
        Self::sync_dir(&self.data_dir)?;
        self.offsets.insert(row_id, offset);
        Ok(())
    }

    /// Greedy robust-prune style edge repair: connect to up to MAX_DEGREE
    /// live neighbours (brute force over cached/spilled vectors), reserving
    /// [`LONG_RANGE_DEGREE`] slots for deterministic long-range peers.
    ///
    /// The target row is paged in when evicted: `build` writes every vector
    /// before wiring edges, so at repair time only the bounded cache window
    /// is resident. Returning early on a cache miss (the historical
    /// behavior) left every row outside that window with no out-edges at
    /// all — the graph degenerated into random back-edges above
    /// `DEFAULT_CACHE` rows, which is the F-008 build-region gap (recall
    /// 0.00/0.20 on clustered dim-64 fixtures at 2k/8k).
    ///
    /// The bounded candidate sample is drawable only from local density:
    /// with clustered data every nearest-`MAX_DEGREE` pick can come from the
    /// same cluster, which leaves separately dense regions with no edges
    /// between them. Beam search then cannot leave the region of its entry
    /// point. Real Vamana seeds random edges for the same connectivity
    /// reason; here a fixed-size random subset of the live ids is kept per
    /// node, so every node has a bounded small-world path into the whole set
    /// regardless of id ordering and without weakening the
    /// nearest-neighbour core.
    fn repair_edges(&mut self, row_id: RowId) -> Result<()> {
        self.repair_edges_with(row_id, None)
    }

    /// Edge repair with an optional in-memory view of the build rows.
    ///
    /// `build` already holds every vector in memory, so passing that view
    /// avoids re-opening `vectors.dat` once per candidate (the dominant
    /// cost of a large build). Incremental `insert` passes `None` and keeps
    /// paging through the bounded cache/segment store.
    fn repair_edges_with(
        &mut self,
        row_id: RowId,
        build_rows: Option<&HashMap<RowId, &[f32]>>,
    ) -> Result<()> {
        // Page the target in when the bounded cache evicted it (see above).
        let target = match build_rows {
            Some(rows) => match rows.get(&row_id) {
                Some(vector) => vector.to_vec(),
                None => return Ok(()),
            },
            None => match self.get_vector(row_id)? {
                Some(vector) => vector,
                None => return Ok(()),
            },
        };
        // Candidate pool: deterministic sample of live ids.
        let mut ids: Vec<RowId> = self
            .offsets
            .keys()
            .copied()
            .filter(|id| *id != row_id && !self.tombstones.is_deleted(*id))
            .collect();
        ids.sort_unstable();
        // Long-range picks are drawn from the full live set before the
        // candidate sample consumes it. Distinct salt so the long-range
        // sample decorrelates from the sampled candidate pool.
        let mut long_range: Vec<RowId> = Vec::new();
        if ids.len() > MAX_DEGREE {
            let mut rng = Lcg64::new(self.seed ^ row_id ^ LONG_RANGE_SALT);
            let mut attempts = 0usize;
            while long_range.len() < LONG_RANGE_DEGREE && attempts < LONG_RANGE_DEGREE * 16 {
                attempts += 1;
                let pick = ids[rng.next_usize(ids.len())];
                if !long_range.contains(&pick) {
                    long_range.push(pick);
                }
            }
        }
        // Bound work: sample at most [`CANDIDATE_SAMPLE`] candidates
        // deterministically. Too small a sample leaves each dense region
        // navigable only through a sparse random slice of itself (F-008:
        // clustered dim-64 recall cliffs); 1 024 keeps a large local
        // neighbourhood visible while bounding build work.
        if ids.len() > CANDIDATE_SAMPLE {
            let mut rng = Lcg64::new(self.seed ^ row_id);
            let mut picked = Vec::with_capacity(CANDIDATE_SAMPLE);
            let mut pool = ids;
            while picked.len() < CANDIDATE_SAMPLE && !pool.is_empty() {
                let i = rng.next_usize(pool.len());
                picked.push(pool.swap_remove(i));
            }
            ids = picked;
            ids.sort_unstable();
        }
        let mut scored: Vec<(f32, RowId)> = Vec::with_capacity(ids.len());
        for id in ids {
            // avoid double borrow: read via the build view, cache or offsets
            // without holding a `&self` borrow across the distance call.
            let vec = match build_rows {
                Some(rows) => match rows.get(&id) {
                    Some(vector) => vector.to_vec(),
                    None => continue,
                },
                None => {
                    if let Some(v) = self.cache.get(&id) {
                        v.clone()
                    } else if let Some(off) = self.offsets.get(&id).copied() {
                        match Self::read_record_at(&self.vectors_path(), off, self.dim) {
                            Ok(v) => v,
                            Err(_) => continue,
                        }
                    } else {
                        continue;
                    }
                }
            };
            if let Ok(d) = self.dist(&target, &vec) {
                scored.push((d, id));
            }
        }
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        let nearest_budget = MAX_DEGREE.saturating_sub(long_range.len());
        let mut peers: Vec<RowId> = scored
            .into_iter()
            .take(nearest_budget)
            .map(|(_, id)| id)
            .collect();
        for id in long_range {
            if !peers.contains(&id) {
                peers.push(id);
            }
        }
        self.graph.insert(row_id, peers.clone());
        // Back-edges (bounded).
        for p in peers {
            let entry = self.graph.entry(p).or_default();
            if !entry.contains(&row_id) {
                entry.push(row_id);
                if entry.len() > MAX_DEGREE {
                    entry.sort_unstable();
                    entry.dedup();
                    entry.truncate(MAX_DEGREE);
                }
            }
        }
        Ok(())
    }

    fn beam_search(&mut self, query: &[f32], k: usize, beam: usize) -> Result<Vec<SearchResult>> {
        check_dimension(self.dim, query.len())?;
        check_finite(query)?;
        let starts: Vec<RowId> = {
            let mut ids: Vec<RowId> = self
                .offsets
                .keys()
                .copied()
                .filter(|id| !self.tombstones.is_deleted(*id))
                .collect();
            ids.sort_unstable();
            ids.into_iter().take(1).collect()
        };
        if starts.is_empty() {
            return Ok(Vec::new());
        }
        let width = beam.max(k).clamp(1, 512);
        let mut visited: HashSet<RowId> = HashSet::new();
        let mut frontier: Vec<(f32, RowId)> = Vec::new();
        for s in starts {
            if let Some(v) = self.get_vector(s)? {
                frontier.push((self.dist(query, &v)?, s));
                visited.insert(s);
            }
        }
        let mut best: Vec<(f32, RowId)> = Vec::new();
        while !frontier.is_empty() {
            frontier.sort_by(|a, b| {
                a.0.partial_cmp(&b.0)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| a.1.cmp(&b.1))
            });
            frontier.truncate(width);
            let (d, id) = frontier.remove(0);
            // insert into best
            match best.binary_search_by(|(bd, bid)| {
                bd.partial_cmp(&d)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| bid.cmp(&id))
            }) {
                Ok(pos) | Err(pos) => {
                    best.insert(pos, (d, id));
                    if best.len() > width {
                        best.pop();
                    }
                }
            }
            let nbrs = self.graph.get(&id).cloned().unwrap_or_default();
            for nb in nbrs {
                if !visited.insert(nb) {
                    continue;
                }
                if let Some(v) = self.get_vector(nb)? {
                    let dd = self.dist(query, &v)?;
                    let worst = best.last().map(|(w, _)| *w).unwrap_or(f32::MAX);
                    if best.len() < width || dd < worst {
                        frontier.push((dd, nb));
                    }
                }
            }
            if best.len() >= width {
                // early stop when frontier cannot improve
                if let Some((fd, _)) = frontier.first() {
                    if *fd >= best.last().map(|(w, _)| *w).unwrap_or(f32::MAX) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        best.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        best.truncate(k);
        Ok(best
            .into_iter()
            .map(|(dist, id)| SearchResult::new(id, dist))
            .collect())
    }

    fn encode_state(&self) -> Vec<u8> {
        let mut out = Vec::new();
        enc_u32(&mut out, DISKANN_VERSION);
        enc_u32(&mut out, self.dim as u32);
        enc_u64(&mut out, self.generation.current());
        enc_usize(&mut out, self.graph.len());
        let mut ids: Vec<RowId> = self.graph.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            enc_u64(&mut out, id);
            let nbrs = &self.graph[&id];
            enc_usize(&mut out, nbrs.len());
            for nb in nbrs {
                enc_u64(&mut out, *nb);
            }
        }
        enc_usize(&mut out, self.offsets.len());
        let mut oids: Vec<RowId> = self.offsets.keys().copied().collect();
        oids.sort_unstable();
        for id in oids {
            enc_u64(&mut out, id);
            enc_u64(&mut out, self.offsets[&id]);
        }
        enc_usize(&mut out, self.tombstones.len());
        let mut dead: Vec<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        dead.sort_unstable();
        for id in dead {
            enc_u64(&mut out, id);
        }
        out
    }

    fn decode_state(bytes: &[u8], config: IndexConfig, data_dir: PathBuf) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let v = d.u32()?;
        if v != DISKANN_VERSION {
            return Err(VectorError::IndexCorrupted(
                "bad diskann version".to_string(),
            ));
        }
        let dim = d.u32()? as usize;
        check_dimension(config.dimension, dim)?;
        let gen = d.u64()?;
        let ng = d.usize_val()?;
        if ng > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("graph too large".to_string()));
        }
        let mut graph: HashMap<RowId, Vec<RowId>> = HashMap::with_capacity(ng.min(1024));
        for _ in 0..ng {
            let id = d.u64()?;
            let m = d.usize_val()?;
            if m > MAX_DEGREE * 2 {
                return Err(VectorError::IndexCorrupted("degree overflow".to_string()));
            }
            let mut nbrs = Vec::with_capacity(m);
            for _ in 0..m {
                nbrs.push(d.u64()?);
            }
            graph.insert(id, nbrs);
        }
        let no = d.usize_val()?;
        if no > MAX_VECTORS {
            return Err(VectorError::IndexCorrupted("too many offsets".to_string()));
        }
        let mut offsets = HashMap::with_capacity(no.min(1024));
        for _ in 0..no {
            let id = d.u64()?;
            let off = d.u64()?;
            offsets.insert(id, off);
        }
        let nd = d.usize_val()?;
        let mut tombstones = TombstoneMap::empty();
        for _ in 0..nd {
            tombstones.mark_deleted(d.u64()?, gen);
        }
        d.finish()?;
        // Page the cache from disk lazily; start empty.
        Ok(Self {
            dim,
            data_dir,
            graph,
            cache: HashMap::new(),
            offsets,
            tombstones,
            generation: Generation(gen),
            max_cached: DEFAULT_CACHE,
            seed: config.params.seed.unwrap_or(DEFAULT_SEED),
            config,
        })
    }

    /// Remove tombstoned rows from graph/cache (segment file is compacted
    /// by rewriting live records).
    pub fn compact(&mut self) -> Result<()> {
        if self.tombstones.is_empty() {
            return Ok(());
        }
        let dead: HashSet<RowId> = self.tombstones.iter().map(|(id, _)| *id).collect();
        for id in &dead {
            self.graph.remove(id);
            self.cache.remove(id);
            self.offsets.remove(id);
        }
        for nbrs in self.graph.values_mut() {
            nbrs.retain(|nb| !dead.contains(nb));
        }
        self.tombstones.clear();
        // Rewrite segment file with live records only.
        self.ensure_dir()?;
        let live: Vec<(RowId, Vec<f32>)> = {
            let mut out = Vec::new();
            let mut ids: Vec<RowId> = self.offsets.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                if let Some(v) = self.cache.get(&id).cloned() {
                    out.push((id, v));
                } else if let Some(off) = self.offsets.get(&id).copied() {
                    if let Ok(v) = Self::read_record_at(&self.vectors_path(), off, self.dim) {
                        out.push((id, v));
                    }
                }
            }
            out
        };
        let tmp = self.data_dir.join("vectors.dat.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(io_fail)?;
            let mut off = 0u64;
            self.offsets.clear();
            for (id, vec) in &live {
                self.offsets.insert(*id, off);
                f.write_all(&RECORD_MAGIC.to_le_bytes()).map_err(io_fail)?;
                f.write_all(&(self.dim as u32).to_le_bytes())
                    .map_err(io_fail)?;
                f.write_all(&id.to_le_bytes()).map_err(io_fail)?;
                for v in vec {
                    f.write_all(&v.to_le_bytes()).map_err(io_fail)?;
                }
                off += (16 + self.dim * 4) as u64;
            }
            f.flush().map_err(io_fail)?;
            // Durability: the compacted segment must be stable before the
            // atomic rename publishes it.
            f.sync_all().map_err(io_fail)?;
        }
        std::fs::rename(&tmp, self.vectors_path()).map_err(io_fail)?;
        Self::sync_dir(&self.data_dir)?;
        self.generation.next();
        Ok(())
    }

    /// Test helper: number of vectors currently resident in RAM.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }
}

impl VectorIndex for DiskAnnIndex {
    fn build(&mut self, rows: &[(RowId, Vec<f32>)]) -> Result<()> {
        check_budget(rows.len())?;
        self.graph.clear();
        self.cache.clear();
        self.offsets.clear();
        self.tombstones.clear();
        self.ensure_dir()?;
        // Truncate segment store.
        std::fs::write(self.vectors_path(), Vec::new()).map_err(io_fail)?;
        let mut sorted: Vec<(RowId, Vec<f32>)> =
            rows.iter().map(|(id, v)| (*id, v.clone())).collect();
        sorted.sort_by_key(|(id, _)| *id);
        for (id, vec) in &sorted {
            self.check_vec(vec)?;
            self.append_record(*id, vec)?;
            self.admit_cache(*id, vec.clone());
        }
        // Wire edges deterministically in sorted order, reading the vectors
        // from the already-materialized build rows.
        let ids: Vec<RowId> = sorted.iter().map(|(id, _)| *id).collect();
        let build_rows: HashMap<RowId, &[f32]> = sorted
            .iter()
            .map(|(id, vector)| (*id, vector.as_slice()))
            .collect();
        for id in ids {
            self.repair_edges_with(id, Some(&build_rows))?;
        }
        self.generation.next();
        Ok(())
    }

    fn insert(&mut self, row_id: RowId, vector: &[f32]) -> Result<()> {
        self.check_vec(vector)?;
        if self.offsets.len() >= MAX_VECTORS && !self.offsets.contains_key(&row_id) {
            return Err(VectorError::ResourceLimit("diskann capacity".to_string()));
        }
        self.append_record(row_id, vector)?;
        self.admit_cache(row_id, vector.to_vec());
        self.tombstones.unmark(row_id);
        self.repair_edges(row_id)?;
        self.generation.next();
        Ok(())
    }

    fn delete(&mut self, row_id: RowId) -> Result<bool> {
        if !self.offsets.contains_key(&row_id) || self.tombstones.is_deleted(row_id) {
            return Ok(false);
        }
        let g = self.generation.next();
        self.tombstones.mark_deleted(row_id, g);
        Ok(true)
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // Clone-on-search keeps `&self` API while paging from disk.
        let mut this = Self {
            config: self.config.clone(),
            dim: self.dim,
            data_dir: self.data_dir.clone(),
            graph: self.graph.clone(),
            cache: self.cache.clone(),
            offsets: self.offsets.clone(),
            tombstones: TombstoneMap::empty(),
            generation: Generation(self.generation.current()),
            max_cached: self.max_cached,
            seed: self.seed,
        };
        // restore tombstones
        for (id, g) in self.tombstones.iter() {
            this.tombstones.mark_deleted(*id, *g);
        }
        this.beam_search(query, k, 64)
    }

    fn range_search(&self, query: &[f32], radius: f32) -> Result<Vec<SearchResult>> {
        if !radius.is_finite() {
            return Err(VectorError::IndexParameterInvalid(
                "radius must be finite".to_string(),
            ));
        }
        let mut hits = self.search(query, self.offsets.len())?;
        hits.retain(|h| h.distance <= radius);
        Ok(hits)
    }

    fn flush(&mut self) -> Result<()> {
        self.ensure_dir()?;
        // Write + fsync through a temp file, then atomically publish, so a
        // crash during checkpoint leaves either the previous valid graph or
        // the new one — never a torn `graph.dat`.
        let tmp = self.data_dir.join("graph.dat.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(io_fail)?;
            f.write_all(&self.encode_state()).map_err(io_fail)?;
            f.flush().map_err(io_fail)?;
            f.sync_all().map_err(io_fail)?;
        }
        std::fs::rename(&tmp, self.graph_path()).map_err(io_fail)?;
        Self::sync_dir(&self.data_dir)?;
        // `vectors.dat` is already write-through; graph + offsets are the
        // crash-recovery metadata.
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.config.kind != IndexKind::DiskAnn {
            return Err(VectorError::IndexCorrupted(
                "diskann kind mismatch".to_string(),
            ));
        }
        for (id, nbrs) in &self.graph {
            if nbrs.len() > MAX_DEGREE {
                return Err(VectorError::IndexCorrupted("degree overflow".to_string()));
            }
            for nb in nbrs {
                if *nb == *id {
                    return Err(VectorError::IndexCorrupted("self edge".to_string()));
                }
            }
        }
        for (id, v) in &self.cache {
            check_dimension(self.dim, v.len())?;
            check_finite(v)?;
            if !self.offsets.contains_key(id) {
                return Err(VectorError::IndexCorrupted(
                    "cache without offset".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            num_vectors: self.offsets.len(),
            num_deleted: self.tombstones.len(),
            generation: self.generation.current(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::Metric;

    /// F-008 regression at the wiring level: edge repair must page a
    /// target in when the bounded cache evicted it.
    ///
    /// Historical bug: `repair_edges` returned early on a cache miss, so in
    /// a build larger than `DEFAULT_CACHE` every row outside the cache
    /// window got no out-edges at all (recall 0.00 @2 k / 0.20 @8 k on the
    /// clustered dim-64 audit fixture). This test builds past the cache
    /// window, then repairs the evicted targets through the paging path and
    /// asserts they get the same peers as the in-memory build view.
    #[test]
    fn edge_repair_pages_targets_beyond_the_cache_window() {
        let n = DEFAULT_CACHE + 4;
        let dir = std::env::temp_dir().join(format!(
            "plomid-diskann-repair-paging-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut index =
            DiskAnnIndex::new(IndexConfig::new(IndexKind::DiskAnn, Metric::L2, 4), &dir).unwrap();
        let rows: Vec<(RowId, Vec<f32>)> = (0..n)
            .map(|i| {
                (
                    i as RowId,
                    vec![
                        ((i % 17) as f32) * 0.25,
                        ((i % 13) as f32) * 0.5,
                        (i as f32) * 0.01,
                        1.0,
                    ],
                )
            })
            .collect();
        index.build(&rows).unwrap();

        // Every row is wired at build time.
        for (id, _) in &rows {
            let peers = index.graph.get(id).expect("every row must be wired");
            assert!(!peers.is_empty(), "row {id} has no out-edges");
            assert!(peers.len() <= MAX_DEGREE, "row {id} exceeds MAX_DEGREE");
        }

        // The first ids were evicted from the cache by the build; repairing
        // them again must page the target + candidates in and produce
        // exactly the peers the in-memory build view produces.
        let rows_by_id: HashMap<RowId, &[f32]> = rows
            .iter()
            .map(|(id, vector)| (*id, vector.as_slice()))
            .collect();
        for id in 0..4u64 {
            index.graph.remove(&id); // observe this repair in isolation
            index.repair_edges_with(id, None).unwrap();
            let paged = index
                .graph
                .get(&id)
                .unwrap_or_else(|| panic!("paged target {id} must be wired"))
                .clone();
            assert!(!paged.is_empty(), "paged target {id} has no out-edges");
            assert!(
                paged.len() <= MAX_DEGREE,
                "paged target {id} exceeds degree"
            );

            index.graph.remove(&id);
            index.repair_edges_with(id, Some(&rows_by_id)).unwrap();
            let in_memory = index.graph.get(&id).expect("build view wired").clone();
            assert_eq!(
                paged, in_memory,
                "paged and in-memory repair disagree for row {id}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
