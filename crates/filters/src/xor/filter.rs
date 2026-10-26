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
//! [`XorFilter`]: construction and membership.

use crate::constants::{
    XOR_ALPHA_DEN, XOR_ALPHA_NUM, XOR_FINGERPRINT_MODULUS, XOR_MAX_BUILD_ATTEMPTS,
    XOR_MAX_ELEMENTS, XOR_MAX_SLOTS, XOR_SLOTS_SLACK,
};
use crate::xor::error::XorError;
use crate::xor::hash::{self, KeyHashes};

/// Seed mixed into the first construction attempt; later attempts multiply an
/// increasing attempt counter into it, so a filter that fails to peel moves on
/// to a genuinely different arrangement rather than retrying the same one.
const SEED_BASE: u64 = 0x9E37_79B9_7F4A_7C15;

/// The slot ratio must clear the peelability threshold of a random 3-uniform
/// hypergraph, `m/n > 11/9`.
///
/// This is a compile-time check because the failure it guards against is not a
/// runtime one: below the threshold peeling cannot succeed for *any* seed, so
/// `XorFilter::build` would simply return [`XorError::BuildFailed`] forever and
/// every call site would be broken. A ratio that cannot work should not compile,
/// not fail a test someone may not run. Compared in integers to keep the
/// inequality exact.
const _: () = assert!(
    XOR_ALPHA_NUM * 9 > XOR_ALPHA_DEN * 11,
    "XOR_ALPHA_NUM/XOR_ALPHA_DEN is at or below 11/9, where a random 3-uniform \
     hypergraph never peels and XOR filter construction can never succeed"
);

/// A probabilistic membership filter over byte-string keys.
///
/// ```text
/// XorFilter { seed, element_count, fingerprints: [u8; slots] }
/// contains(key) = fingerprints[a] ^ fingerprints[b] ^ fingerprints[c] == fp(key)
/// ```
///
/// The empty filter has zero slots: it is a valid filter that answers `false`
/// for every key, which is what a caller expects for an empty key set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorFilter {
    /// Seed the slot indices are derived from.
    seed: u64,
    /// Number of keys the filter was built from.
    element_count: u64,
    /// One fingerprint per slot; never zero.
    fingerprints: Vec<u8>,
}

/// Returns the slot count for `count` elements:
/// `ceil(count * XOR_ALPHA_NUM / XOR_ALPHA_DEN) + XOR_SLOTS_SLACK`, rounded up
/// to a multiple of three.
///
/// Two properties matter here and neither is a free tuning knob:
///
/// * The multiple of three keeps [`hash::derive`]'s three index thirds
///   disjoint, which is what guarantees the three slots of a key are distinct.
/// * The total must stay above the peelability threshold of a random 3-uniform
///   hypergraph (`m/n > 11/9`). Shrinking the ratio below that threshold does
///   not merely make construction less likely to succeed — it makes it
///   impossible, because the 2-core of the slot graph can never empty and every
///   retry seed fails identically. [`XOR_ALPHA_NUM`] and [`XOR_SLOTS_SLACK`]
///   carry the full rationale.
pub fn slot_count_for(count: usize) -> usize {
    let numerator = (count as u128) * u128::from(XOR_ALPHA_NUM);
    let scaled = numerator.div_ceil(u128::from(XOR_ALPHA_DEN));
    // At most XOR_MAX_SLOTS, so this conversion is exact for allowed inputs.
    let mut slots = (scaled as usize).saturating_add(XOR_SLOTS_SLACK);
    slots = slots.clamp(3, XOR_MAX_SLOTS);
    let remainder = slots % 3;
    if remainder != 0 {
        slots += 3 - remainder;
    }
    slots
}

impl XorFilter {
    /// Builds a filter over `keys`.
    ///
    /// # Errors
    ///
    /// Returns [`XorError::TooManyElements`] when `keys` exceeds the format's
    /// element budget, and [`XorError::BuildFailed`] when every attempt at
    /// peeling and verifying the key set fails. A failed build returns no
    /// filter: callers fall back to exact evaluation.
    pub fn build<'a, I>(keys: I) -> Result<Self, XorError>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let keys: Vec<&[u8]> = keys.into_iter().collect();
        if keys.len() > XOR_MAX_ELEMENTS {
            return Err(XorError::TooManyElements {
                requested: keys.len(),
                limit: XOR_MAX_ELEMENTS,
            });
        }
        if keys.is_empty() {
            return Ok(Self::empty());
        }
        for attempt in 0..XOR_MAX_BUILD_ATTEMPTS {
            let seed = SEED_BASE.wrapping_mul(u64::from(attempt) + 1);
            if let Some(filter) = Self::try_build(&keys, seed) {
                return Ok(filter);
            }
        }
        Err(XorError::BuildFailed {
            attempts: XOR_MAX_BUILD_ATTEMPTS,
        })
    }

    /// Returns the empty filter: zero slots, `false` for every key.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            seed: 0,
            element_count: 0,
            fingerprints: Vec::new(),
        }
    }

    /// Rebuilds a filter from decoded image parts.
    ///
    /// Only the codec calls this, and only after it has checked the shape: the
    /// slot count inside the format limit, the element count consistent with it,
    /// and the fingerprint array exactly as long as the slot count. The parts
    /// are therefore trusted by construction; no other caller can reach this.
    #[must_use]
    pub(crate) fn from_parts(seed: u64, element_count: u64, fingerprints: Vec<u8>) -> Self {
        Self {
            seed,
            element_count,
            fingerprints,
        }
    }

    /// Returns whether `key` may be in the built set.
    ///
    /// `false` is definitive (the key is absent); `true` means the key is
    /// present or a false positive, so the caller must continue with exact
    /// evaluation.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        let slots = self.fingerprints.len();
        if slots == 0 {
            return false;
        }
        let hashes = hash::derive(hash::hash_key(key), slots, self.seed);
        self.matches(&hashes)
    }

    /// Returns whether the slot fingerprints of `hashes` encode its fingerprint.
    #[must_use]
    fn matches(&self, hashes: &KeyHashes) -> bool {
        let combined =
            self.fingerprints[hashes.a] ^ self.fingerprints[hashes.b] ^ self.fingerprints[hashes.c];
        u64::from(combined) == hashes.fingerprint
    }

    /// Returns the number of keys the filter was built from.
    #[must_use]
    pub fn len(&self) -> usize {
        self.element_count as usize
    }

    /// Returns whether the filter was built from no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.element_count == 0
    }

    /// Returns the number of fingerprint slots.
    #[must_use]
    pub fn slots(&self) -> usize {
        self.fingerprints.len()
    }

    /// Returns the seed the slot indices were derived from.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the fingerprint bytes, one per slot.
    #[must_use]
    pub fn fingerprints(&self) -> &[u8] {
        &self.fingerprints
    }

    /// Returns the false-positive probability of one membership test.
    #[must_use]
    pub fn false_positive_rate(&self) -> f64 {
        1.0 / XOR_FINGERPRINT_MODULUS as f64
    }

    /// One construction attempt with a fixed `seed`.
    ///
    /// Returns `None` unless the whole key set peels *and* the assembled filter
    /// verifies against every key, so a returned filter has no false negatives
    /// by test, not by argument.
    fn try_build(keys: &[&[u8]], seed: u64) -> Option<Self> {
        let slots = slot_count_for(keys.len());
        let mut hashes: Vec<KeyHashes> = keys
            .iter()
            .map(|key| hash::derive(hash::hash_key(key), slots, seed))
            .collect();
        // Repeated keys collapse *before* peeling, because a duplicated key
        // counts twice against each slot it touches and would leave those slots
        // permanently at a count above one. The slot list is reordered by the
        // deduplication, so nothing below may index it by key position.
        deduplicate(&mut hashes);

        let order = peel(&hashes, slots)?;
        let fingerprints = assign(&hashes, &order, slots);
        let filter = Self {
            seed,
            element_count: keys.len() as u64,
            fingerprints,
        };

        // The constructor's contract: every key that went in must be found.
        // `contains` re-derives the key's slots and re-runs the membership
        // equation, so this covers distinct and repeated keys alike without
        // relying on the deduplicated order.
        if keys.iter().all(|key| filter.contains(key)) {
            Some(filter)
        } else {
            None
        }
    }
}

/// Collapses repeated keys in `hashes` in place, leaving one entry per distinct
/// hash.
///
/// A repeated key is the *normal* case for a column filter — a column with a
/// repeated value produces a byte-identical key — and it is what makes the
/// difference between a constructible key set and an unconstructible one. A
/// duplicated key touches each of its three slots twice, so those slots sit at a
/// count of two; [`peel`] only ever peels through a slot held by exactly one
/// key, so a duplicated key can never be peeled and the build fails no matter
/// which seed is tried.
///
/// Two keys are one entry when they derive the same three slots *and* the same
/// fingerprint. Such a pair states the identical membership equation, so no
/// filter can tell them apart and collapsing them discards nothing. Sorting on
/// all four fields puts equal entries adjacent, which is what [`Vec::dedup`]
/// needs; it is the only part of construction that is not linear.
fn deduplicate(hashes: &mut Vec<KeyHashes>) {
    hashes.sort_unstable_by_key(|key| (key.a, key.b, key.c, key.fingerprint));
    hashes.dedup();
}

/// Peels the keys, returning each with the slot it was peeled through.
///
/// A key is peeled once exactly one of its three slots holds no other key: that
/// slot's value is then derivable from the key's fingerprint alone. Peel order
/// is what makes the assembly exact — the slot a key is peeled through is
/// written once, and the other two slots are already final by then. A failure
/// to peel every key is a structural property of this key set and seed, so the
/// caller retries with another seed.
fn peel(hashes: &[KeyHashes], slots: usize) -> Option<Vec<(usize, usize)>> {
    // Per slot: how many not-yet-peeled keys touch it, and their index XOR. A
    // slot with a count of one has its single key recoverable from that XOR.
    let mut counts = vec![0u16; slots];
    let mut occupants = vec![0u32; slots];
    for (index, key) in hashes.iter().enumerate() {
        for slot in [key.a, key.b, key.c] {
            counts[slot] = counts[slot].saturating_add(1);
            occupants[slot] ^= index as u32;
        }
    }

    let mut queue: Vec<usize> = (0..slots).filter(|slot| counts[*slot] == 1).collect();
    let mut peeled = vec![false; hashes.len()];
    let mut order = Vec::with_capacity(hashes.len());
    while let Some(slot) = queue.pop() {
        if counts[slot] != 1 {
            continue;
        }
        let index = occupants[slot] as usize;
        if peeled[index] {
            continue;
        }
        peeled[index] = true;
        order.push((index, slot));
        counts[slot] = 0;
        for other in [hashes[index].a, hashes[index].b, hashes[index].c] {
            counts[other] = counts[other].saturating_sub(1);
            occupants[other] ^= index as u32;
            if counts[other] == 1 {
                queue.push(other);
            }
        }
    }
    if order.len() == hashes.len() {
        Some(order)
    } else {
        None
    }
}

/// Fills the slots in reverse peel order.
///
/// For each key the XOR of its three slots must equal its fingerprint, so the
/// peeled slot is set to `fp ^ (the other two slots)`. Those two are already
/// final (their keys were peeled later, and so assigned earlier here) and the
/// peeled slot is written exactly once, which makes the whole assignment exact.
fn assign(hashes: &[KeyHashes], order: &[(usize, usize)], slots: usize) -> Vec<u8> {
    let mut fingerprints = vec![0u8; slots];
    for (index, target) in order.iter().rev() {
        let key = &hashes[*index];
        let others = [key.a, key.b, key.c]
            .into_iter()
            .filter(|slot| slot != target)
            .collect::<Vec<usize>>();
        let occupied = fingerprints[others[0]] ^ fingerprints[others[1]];
        fingerprints[*target] = occupied ^ (key.fingerprint as u8);
    }
    fingerprints
}
