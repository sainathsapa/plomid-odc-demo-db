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
//! Bloom pre-filter for hash joins.
//!
//! A build-side Bloom filter lets the probe side skip rows that cannot match
//! before touching the hash table. False positives only add candidates; false
//! negatives are never allowed, so the join result is unchanged.
//!
//! Ownership split (deliberate, not duplication): durable sealed-segment
//! membership pruning belongs to `plomid-filters`' `XorFilter` (persisted
//! sidecars, versioned format). This filter is the ephemeral per-query role:
//! infallible construction, no cross-crate dependency on the join hot path,
//! sized per build side at ~1% FPR. Do not unify the two without preserving
//! both properties.

/// FNV-1a 64-bit hash of `data`.
#[must_use]
pub fn hash_key(data: &[u8]) -> u64 {
    const OFFSET: u64 = 14_695_981_039_346_656_037;
    const PRIME: u64 = 1_099_511_628_211;
    let mut hash = OFFSET;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Second hash for double-hashing, derived from `hash`.
fn second_hash(hash: u64) -> u64 {
    let mut h = hash.rotate_left(17) ^ 0xc2b2_ae3d_27d4_eb4f;
    if h == 0 {
        h = 1;
    }
    // Keep it odd so the probe sequence covers the table.
    h | 1
}

/// Bloom pre-filter over 64-bit key hashes.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit vector, 64 bits per word.
    pub bits: Vec<u64>,
    /// Number of hash functions.
    pub k: u8,
}

impl BloomFilter {
    /// Sizes the filter for `expected_rows` with false-positive rate `fp_rate`.
    ///
    /// `fp_rate` is clamped to `(0, 1)` exclusive; `expected_rows == 0` still
    /// yields a minimal 64-bit filter.
    #[must_use]
    pub fn new(expected_rows: usize, fp_rate: f64) -> Self {
        let p = fp_rate.clamp(0.000_001, 0.999_999);
        let ln2 = std::f64::consts::LN_2;
        let m = if expected_rows == 0 {
            64_u64
        } else {
            let m_float = -((expected_rows as f64) * p.ln()) / (ln2 * ln2);
            (m_float.ceil() as u64).max(64)
        };
        let words = m.div_ceil(64) as usize;
        let k = if expected_rows == 0 {
            1_u8
        } else {
            let k_float = (m as f64 / expected_rows as f64) * ln2;
            k_float.round().clamp(1.0, 32.0) as u8
        };
        Self {
            bits: vec![0_u64; words],
            k,
        }
    }

    /// Number of addressable bits.
    #[must_use]
    pub fn bit_len(&self) -> usize {
        self.bits.len() * 64
    }

    fn positions(&self, hash: u64) -> Vec<usize> {
        let m = self.bit_len() as u64;
        if m == 0 {
            return Vec::new();
        }
        let h2 = second_hash(hash);
        (0..u64::from(self.k))
            .map(|i| (hash.wrapping_add(i.wrapping_mul(h2)) % m) as usize)
            .collect()
    }

    /// Records `hash` in the filter.
    pub fn insert(&mut self, hash: u64) {
        for pos in self.positions(hash) {
            self.bits[pos / 64] |= 1_u64 << (pos % 64);
        }
    }

    /// True when `hash` may have been inserted (no false negatives).
    #[must_use]
    pub fn may_contain(&self, hash: u64) -> bool {
        self.positions(hash)
            .iter()
            .all(|pos| self.bits[*pos / 64] & (1_u64 << (*pos % 64)) != 0)
    }
}
