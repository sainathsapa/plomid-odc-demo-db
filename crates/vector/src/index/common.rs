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
//! Shared index utilities: tombstones, generations, deterministic RNG,
//! dense distances, validation helpers and a tiny little-endian codec.
//!
//! Everything in this module is deliberately dependency-free (`std` only)
//! so that every index implementation — HNSW graphs, IVF posting lists,
//! PQ/SQ codebooks, DiskANN segments — shares one canonical definition of
//! correctness-critical behaviour: what a deleted row looks like, how
//! determinism is seeded, how distances rank, and how persisted bytes are
//! framed. The executor never imports this module directly; it goes through
//! the [`VectorIndex`](super::VectorIndex) trait.

use std::collections::HashMap;

use crate::{Result, VectorError};

use super::{Metric, RowId};

/// Portable dimension ceiling shared by every index implementation.
///
/// Matches the SQL type system (`1..=65536`): an index must never reject a
/// dimension the column type accepts. Memory safety comes from
/// overflow-checked allocation (`check_budget`), not from a lower cap here.
pub const MAX_DIMENSION: usize = 65536;
/// Maximum entries any single in-memory index holds.
///
/// This is a library backstop, not the operational limit: the executor's
/// `vector.max_index_vectors` setting governs builds at runtime (default
/// 1M, tunable up to this backstop). Beyond single-graph scale, shard by
/// key range/tenant with one index per shard and merge Top-K at query time
/// (see the distributed-readiness boundary). Memory math per vector is
/// roughly `dimension × 4B × 2` resident for HNSW — size the setting from
/// measured RAM, not hope.
pub const MAX_VECTORS: usize = 16_000_000;
/// Maximum HNSW level depth.
///
/// Caps graph height so level sampling stays `O(1)` and persisted neighbour
/// arrays keep a statically known bound for validation.
pub const MAX_LEVEL: usize = 16;
/// Maximum HNSW degree parameter `m` accepted by validation.
///
/// Real builds use far less (default 16); this is a fail-closed ceiling.
pub const MAX_M: usize = 64;

/// Default deterministic seed used when no seed is configured.
///
/// Fixed across releases so that unseeded builds remain reproducible in
/// tests and benchmarks. Production deployments seeking decorrelated
/// sharding should set an explicit per-index seed.
pub const DEFAULT_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Deleted-row set with the generation at which each row was removed.
///
/// Deletes tombstone rather than physically unlinking: readers holding an
/// older snapshot keep traversing valid memory, and background compaction
/// reclaims tombstoned entries once no active snapshot can observe them.
/// The stored generation powers staleness metrics and rebuild thresholds.
#[derive(Clone, Debug, Default)]
pub struct TombstoneMap {
    /// Tombstoned row identity to the mutation generation that removed it.
    inner: HashMap<RowId, u64>,
}

impl TombstoneMap {
    /// Creates an empty tombstone set.
    ///
    /// Equivalent to [`empty`](Self::empty); provided for call sites that
    /// read more naturally with `new`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Creates an empty tombstone set.
    ///
    /// Preferred in index constructors where `empty` signals "no deletions
    /// have been observed yet" rather than a heap allocation event.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Marks `row_id` deleted at `generation`.
    ///
    /// Idempotent: re-marking an already tombstoned row returns `false` and
    /// keeps the original generation so staleness accounting stays stable.
    pub fn mark_deleted(&mut self, row_id: RowId, generation: u64) -> bool {
        if self.inner.contains_key(&row_id) {
            return false;
        }
        self.inner.insert(row_id, generation);
        true
    }

    /// Whether `row_id` is currently tombstoned.
    ///
    /// Consulted on every candidate path before distance computation so that
    /// deleted rows cost a hash lookup, never a full vector decode.
    pub fn is_deleted(&self, row_id: RowId) -> bool {
        self.inner.contains_key(&row_id)
    }

    /// Clears the tombstone for `row_id` (re-insert / overwrite path).
    ///
    /// Returns `true` when a tombstone was present. Upserts always unmark
    /// first so a resurrected row becomes searchable immediately.
    pub fn unmark(&mut self, row_id: RowId) -> bool {
        self.inner.remove(&row_id).is_some()
    }

    /// Number of tombstoned entries awaiting compaction.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no rows are tombstoned.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Iterates `(row_id, deletion_generation)` pairs.
    ///
    /// Used by persistence (sorted before encoding for determinism) and by
    /// compaction to enumerate reclamation candidates.
    pub fn iter(&self) -> impl Iterator<Item = (&RowId, &u64)> {
        self.inner.iter()
    }

    /// Drops all tombstones, typically after compaction reclaimed them.
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/// Monotonic generation counter, bumped on every mutation.
///
/// Persisted alongside index bytes so that crash recovery can distinguish
/// "valid READY index at generation N" from "torn write, rebuild required".
/// Wrapping arithmetic is used so a pathological counter can never panic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Generation(pub u64);

impl Generation {
    /// Starts at generation zero (fresh, never persisted).
    #[must_use]
    pub const fn new() -> Self {
        Self(0)
    }

    /// Advances the counter and returns the new generation.
    ///
    /// Called on build, insert, delete, and compaction: any state change
    /// that a concurrent snapshot boundary might need to order.
    pub const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }

    /// Reads the current generation without mutating it.
    #[must_use]
    pub const fn current(self) -> u64 {
        self.0
    }
}

/// Deterministic 64-bit LCG with xorshifted output (no external RNG).
///
/// All randomness in index builds — HNSW level sampling, IVF k-means
/// seeding, PQ codebook initialisation — flows through this generator so
/// that a fixed seed reproduces bit-identical structures across runs and
/// architectures. Never used for security purposes.
#[derive(Clone, Debug)]
pub struct Lcg64 {
    /// Internal linear-congruential state.
    state: u64,
}

impl Lcg64 {
    /// Seeds the generator.
    ///
    /// Callers typically XOR the configured seed with a per-index domain
    /// constant so that two indexes with the same seed still decorrelate.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Advances the LCG and returns a uniformly mixed 64-bit output.
    pub const fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // xorshifted output for a bit more uniformity
        let mut x = self.state;
        x ^= x >> 29;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        x
    }

    /// Uniform float in `[0, 1)`.
    ///
    /// Consumes the top 24 bits for exact `f32` granularity; used by HNSW
    /// level sampling (`p < 0.5` per level).
    pub fn next_f32(&mut self) -> f32 {
        // [0, 1)
        let v = self.next_u64() >> 40; // 24 bits
        (v as f32) / 16_777_216.0
    }

    /// Uniform integer in `[0, bound)`, or `0` when `bound == 0`.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % (bound as u64)) as usize
    }

    /// Deterministic Fisher-Yates shuffle in place.
    ///
    /// Used to randomise k-means seeding and training-sample order without
    /// depending on the host RNG, keeping builds reproducible.
    pub fn shuffle<T>(&mut self, data: &mut [T]) {
        if data.len() < 2 {
            return;
        }
        for i in (1..data.len()).rev() {
            let j = self.next_usize(i + 1);
            data.swap(i, j);
        }
    }
}

/// Squared L2 distance, tier-dispatched (`f32` accumulation, tolerance
/// contract — see `math::dispatch`).
///
/// Internal helper for hot loops that defer the final `sqrt`; the public
/// distance API applies skill-appropriate accumulators per element type.
pub fn l2_squared(a: &[f32], b: &[f32]) -> f32 {
    crate::math::dispatch::l2_squared(a, b)
        .1
        .unwrap_or(f32::NAN)
}

/// Raw inner product, tier-dispatched.
///
/// Positive-is-similar; index code negates it (`-dot`) at the call site so
/// that every metric shares the ascending Top-K convention.
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    crate::math::dispatch::dot(a, b).1.unwrap_or(f32::NAN)
}

/// Cosine distance in `[0, 2]`; `0` for identical directions.
///
/// Zero-norm inputs map to distance `1.0` here (maximally dissimilar) for
/// graph-connectivity purposes; the SQL-facing distance layer instead maps
/// zero-norm cosine to NULL and excludes such rows from ANN candidate sets.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let d = dot(a, b);
    let na = dot(a, a).sqrt();
    let nb = dot(b, b).sqrt();
    if na == 0.0 || nb == 0.0 {
        return 1.0;
    }
    let cos = (d / (na * nb)).clamp(-1.0, 1.0);
    1.0 - cos
}

/// Dense distance with the lower-is-better convention.
///
/// `Dot` is negated (`-dot`); `Hamming`/`Jaccard` are rejected here because
/// packed binary vectors take a separate bit-level path.
pub fn dense_distance(a: &[f32], b: &[f32], metric: Metric) -> Result<f32> {
    match metric {
        Metric::L2 => Ok(l2_squared(a, b)),
        Metric::Cosine => Ok(cosine_distance(a, b)),
        Metric::Dot => Ok(-dot(a, b)),
        Metric::Hamming | Metric::Jaccard => Err(VectorError::IndexUnsupported(
            "Hamming/Jaccard require binary vectors".to_string(),
        )),
    }
}

/// Hamming distance between packed 64-bit words (bit count of XOR).
///
/// Operates on pre-packed words; callers mask padding bits before packing
/// so that unused tail bits never influence the count.
pub fn hamming_distance_words(a: &[u64], b: &[u64]) -> u64 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x ^ y).count_ones() as u64)
        .sum()
}

/// Jaccard distance between packed bit vectors: `1 - |A∩B|/|A∪B|`.
///
/// An empty union (both sets empty) maps to `0.0` (identical) per the
/// documented empty-set rule; this keeps all-binary-zero rows searchable
/// instead of undefined.
pub fn jaccard_distance_words(a: &[u64], b: &[u64]) -> f32 {
    let mut inter = 0u64;
    let mut union = 0u64;
    for (x, y) in a.iter().zip(b.iter()) {
        inter += (x & y).count_ones() as u64;
        union += (x | y).count_ones() as u64;
    }
    if union == 0 {
        return 0.0;
    }
    1.0 - (inter as f32) / (union as f32)
}

/// Rejects dimension mismatches with a typed error.
///
/// Every index entry point calls this before touching payloads so that a
/// misconfigured query fails fast instead of reading out of bounds.
pub fn check_dimension(expected: usize, got: usize) -> Result<()> {
    if expected != got {
        return Err(VectorError::DimensionMismatch {
            expected: expected as u32,
            got: got as u32,
        });
    }
    Ok(())
}

/// Rejects non-finite lanes before they can poison an index.
///
/// Production determinism rule: storage and query vectors are always finite;
/// NaN/Infinity are rejected at parse/cast boundaries and rechecked here.
pub fn check_finite(v: &[f32]) -> Result<()> {
    for x in v {
        if !x.is_finite() {
            return Err(VectorError::NonFiniteValue(
                "vector contains non-finite value".to_string(),
            ));
        }
    }
    Ok(())
}

/// Enforces the global entry-count bound before bulk allocation.
///
/// Prevents a crafted `build()` batch from forcing an unchecked reservation;
/// larger estates must shard or use the DiskANN tier.
pub fn check_budget(num_vectors: usize) -> Result<()> {
    if num_vectors > MAX_VECTORS {
        return Err(VectorError::ResourceLimit(format!(
            "vector count {num_vectors} exceeds max {MAX_VECTORS}"
        )));
    }
    Ok(())
}

/// Maps an invalid build/search parameter onto the error hierarchy.
///
/// Constructor helper so index code reads `return Err(invalid_param(..))`
/// instead of spelling the variant at every call site.
pub fn invalid_param(msg: impl Into<String>) -> VectorError {
    VectorError::IndexParameterInvalid(msg.into())
}

/// Maps a dimension mismatch onto the error hierarchy.
pub fn dim_mismatch(expected: usize, got: usize) -> VectorError {
    VectorError::DimensionMismatch {
        expected: expected as u32,
        got: got as u32,
    }
}

/// Standard "index exists but was never trained" error.
///
/// Returned by IVF/PQ/SQ search paths before their codebooks/centroids have
/// been built from a training sample.
pub fn not_trained() -> VectorError {
    VectorError::IndexStateInvalid("index is not trained".to_string())
}

/// Maps persisted-byte inconsistencies onto corruption errors.
///
/// Any checksum, version, length-prefix, or reference violation becomes
/// `IndexCorrupted`, which fails closed and routes to rebuild.
pub fn corrupt(msg: impl Into<String>) -> VectorError {
    VectorError::IndexCorrupted(msg.into())
}

/// Maps valid-but-unimplemented combinations onto unsupported errors.
pub fn unsupported(msg: impl Into<String>) -> VectorError {
    VectorError::IndexUnsupported(msg.into())
}

/// Maps oversized requests onto resource-limit errors.
pub fn budget_exceeded(msg: impl Into<String>) -> VectorError {
    VectorError::ResourceLimit(msg.into())
}

/// Maps I/O failures onto corruption errors without leaking paths.
///
/// The underlying OS message is preserved for diagnostics but never
/// includes absolute file locations.
pub fn io_fail(e: std::io::Error) -> VectorError {
    VectorError::Corruption(format!("io error: {e}"))
}

// ---------------------------------------------------------------------------
// Little-endian byte codec used by every persist encode/decode path.
// ---------------------------------------------------------------------------

/// Appends a `u32` in explicit little-endian order.
///
/// All persisted index bytes are little-endian regardless of host, so an
/// x86-built artifact reloads bit-identically on ARM.
pub fn enc_u32(dst: &mut Vec<u8>, v: u32) {
    dst.extend_from_slice(&v.to_le_bytes());
}

/// Appends a `u64` in explicit little-endian order.
pub fn enc_u64(dst: &mut Vec<u8>, v: u64) {
    dst.extend_from_slice(&v.to_le_bytes());
}

/// Appends a `usize` as 8 bytes in explicit little-endian order.
///
/// Fixed 8-byte width keeps persisted layouts host-independent (32-bit vs
/// 64-bit processes read the same bytes).
pub fn enc_usize(dst: &mut Vec<u8>, v: usize) {
    enc_u64(dst, v as u64);
}

/// Appends an `f32` in explicit little-endian order.
pub fn enc_f32(dst: &mut Vec<u8>, v: f32) {
    dst.extend_from_slice(&v.to_le_bytes());
}

/// Appends a length-prefixed byte string (`u64 len` + raw bytes).
pub fn enc_bytes(dst: &mut Vec<u8>, b: &[u8]) {
    enc_usize(dst, b.len());
    dst.extend_from_slice(b);
}

/// Cursor decoder over a persisted byte slice.
///
/// Every `decode()` path funnels through this cursor so truncation,
/// trailing-byte, and length-prefix abuse all fail with the same
/// `IndexCorrupted` vocabulary instead of panicking on slicing.
pub struct Decoder<'a> {
    /// Source buffer being decoded.
    buf: &'a [u8],
    /// Current read offset.
    pos: usize,
}

impl<'a> Decoder<'a> {
    /// Borrows `buf` at position zero.
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Unconsumed byte count.
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// Takes `n` bytes or fails closed on truncation.
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(VectorError::IndexCorrupted("truncated buffer".to_string()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Decodes a little-endian `u32`.
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Decodes a little-endian `u64`.
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    /// Decodes a `usize` previously written by [`enc_usize`].
    pub fn usize_val(&mut self) -> Result<usize> {
        let v = self.u64()?;
        usize::try_from(v).map_err(|_| VectorError::IndexCorrupted("usize overflow".to_string()))
    }

    /// Decodes a little-endian `f32`.
    pub fn f32_val(&mut self) -> Result<f32> {
        let b = self.take(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Decodes a length-prefixed byte string written by [`enc_bytes`].
    ///
    /// Rejects absurd length prefixes (>256 MiB) before allocating so that
    /// crafted artifacts cannot force a huge reservation.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.usize_val()?;
        if n > 256 * 1024 * 1024 {
            return Err(VectorError::IndexCorrupted(
                "length prefix too large".to_string(),
            ));
        }
        self.take(n)
    }

    /// Requires exact end-of-buffer consumption.
    ///
    /// Trailing bytes indicate version skew or torn writes and fail closed.
    pub fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(VectorError::IndexCorrupted("trailing bytes".to_string()));
        }
        Ok(())
    }
}
