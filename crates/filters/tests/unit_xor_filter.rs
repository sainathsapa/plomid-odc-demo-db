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
// Moved from `src/xor/filter.rs` inline `#[cfg(test)]` — public-API only.
use plomid_filters::xor::filter::{slot_count_for, XorFilter};
use plomid_filters::xor::hash;
use plomid_filters::xor::XorError;
use plomid_filters::{XOR_ALPHA_DEN, XOR_ALPHA_NUM, XOR_MAX_ELEMENTS, XOR_SLOTS_SLACK};

/// Builds `count` distinct keys whose byte images do not collide.
fn keys_for(count: usize, nonce: u64) -> Vec<[u8; 4]> {
    (0..count as u64)
        .map(|index| {
            let mixed = index
                .wrapping_mul(2_654_435_761)
                .wrapping_add(nonce.wrapping_mul(0x9E37_79B9))
                ^ nonce.wrapping_mul(0x85EB_CA6B);
            (mixed as u32).to_le_bytes()
        })
        .collect()
}

fn build(keys: &[[u8; 4]]) -> XorFilter {
    XorFilter::build(keys.iter().map(|key| &key[..]))
        .expect("construction must succeed for this key set")
}

/// The regression guard for the construction failure this module shipped
/// with: a slot ratio below `11/9` makes peeling impossible, so every seed
/// fails and `build` can never return a filter. The same inequality is
/// asserted at compile time above; this test pins the ratio itself so a
/// silent retune of the constant has to be deliberate.
#[test]
fn slot_ratio_is_the_documented_value() {
    // Bound to locals so the comparison is a runtime one, which is what the
    // test is for: the compile-time assertion above proves the inequality,
    // this proves which ratio is in force.
    let (numerator, denominator) = (XOR_ALPHA_NUM, XOR_ALPHA_DEN);
    assert_eq!(numerator, 27, "the slot ratio was retuned");
    assert_eq!(denominator, 20, "the slot ratio was retuned");
    assert!(
        numerator * 9 > denominator * 11,
        "ratio {numerator}/{denominator} is at or below 11/9, \
             where a random 3-uniform hypergraph never peels"
    );
}

#[test]
fn slot_count_is_a_nonempty_multiple_of_three() {
    for count in [0usize, 1, 2, 3, 4, 17, 64, 1000, 999_983] {
        let slots = slot_count_for(count);
        assert_eq!(slots % 3, 0, "count {count} gave {slots} slots");
        assert!(slots >= 3, "count {count} gave {slots} slots");
    }
    // The slack is what keeps small key sets constructible; without it the
    // rounding to a multiple of three leaves them at an unpeelable load.
    assert_eq!(slot_count_for(1), XOR_SLOTS_SLACK + 4);
    assert_eq!(slot_count_for(3), XOR_SLOTS_SLACK + 7);
}

#[test]
fn slot_count_grows_proportionally_for_large_key_sets() {
    // The additive slack must not distort the asymptotic overhead. Checked
    // in integers so the expectation is exact: 1 000 000 * 27/20 is exactly
    // 1 350 000, plus the 32-slot slack, rounded up to the next multiple of
    // three.
    let slots = slot_count_for(1_000_000);
    assert_eq!(slots, 1_350_033);
    // ...and the result still clears the 11/9 threshold with room to spare.
    assert!(slots * 9 > 1_000_000 * 11);
}

#[test]
fn every_key_gets_three_distinct_slots() {
    let slots = slot_count_for(64);
    for key in keys_for(64, 7) {
        let hashes = hash::derive(hash::hash_key(&key), slots, 0);
        assert!(hashes.a < slots && hashes.b < slots && hashes.c < slots);
        assert_ne!(hashes.a, hashes.b);
        assert_ne!(hashes.b, hashes.c);
        assert_ne!(hashes.a, hashes.c);
        assert!(hashes.fingerprint != 0, "fingerprints are never zero");
    }
}

/// Small key sets are the case the additive slack exists for: at these sizes
/// the proportional term alone is a coin flip.
#[test]
fn small_key_sets_build_for_every_nonce() {
    for count in [1usize, 2, 3, 5, 7, 16, 64] {
        for nonce in 0..8u64 {
            let keys = keys_for(count, nonce);
            let filter = build(&keys);
            assert_eq!(filter.slots(), slot_count_for(count));
            assert_eq!(filter.len(), count);
            assert!(!filter.is_empty());
        }
    }
}

#[test]
fn large_key_sets_build_and_keep_every_key() {
    for nonce in 0..4u64 {
        let keys = keys_for(10_000, nonce);
        let filter = build(&keys);
        assert_eq!(filter.len(), 10_000);
        assert!(keys.iter().all(|key| filter.contains(key)));
    }
}

#[test]
fn absent_keys_are_rejected_at_the_expected_rate() {
    let filter = build(&keys_for(1000, 0x1234));
    // Probe values far above the key space: the builder packs keys in
    // [0, 1000), so these are all absent.
    let probes = 20_000u32;
    let positives = (u32::MAX - probes..u32::MAX)
        .filter(|value| filter.contains(&value.to_le_bytes()))
        .count();
    // Expected is 1/256 ≈ 0.39%, so ~78 of 20 000. The band absorbs the
    // sampling noise of one seed while staying far below the 1/2 a filter
    // that ignored its fingerprints would reach.
    assert!(
        positives < 200,
        "false-positive rate too high: {positives}/{probes}"
    );
    assert!(positives > 0, "the check must be able to fail at all");
}

#[test]
fn empty_key_set_yields_a_filter_that_contains_nothing() {
    let filter = XorFilter::build(std::iter::empty::<&[u8]>()).expect("an empty build is fine");
    assert!(filter.is_empty());
    assert_eq!(filter.len(), 0);
    assert_eq!(filter.slots(), 0);
    assert!(!filter.contains(b""));
    assert!(!filter.contains(b"anything"));
}

#[test]
fn empty_filter_matches_the_empty_image() {
    let filter = XorFilter::empty();
    assert_eq!(filter, XorFilter::deserialize(&filter.serialize()).unwrap());
}

#[test]
fn filters_round_trip_through_an_image() {
    for count in [1usize, 7, 1000] {
        let keys = keys_for(count, 0x5EED);
        let filter = build(&keys);
        let decoded = XorFilter::deserialize(&filter.serialize()).expect("own image must decode");
        assert_eq!(decoded, filter);
        assert!(keys.iter().all(|key| decoded.contains(key)));
    }
}

#[test]
fn element_budget_is_enforced() {
    // The budget check runs before any slot allocation, so a lazily-shaped
    // iterator is enough to drive it.
    let over = XOR_MAX_ELEMENTS + 1;
    let result = XorFilter::build((0..over).map(|_| b"k".as_ref()));
    assert_eq!(
        result,
        Err(XorError::TooManyElements {
            requested: over,
            limit: XOR_MAX_ELEMENTS,
        })
    );
}

#[test]
fn false_positive_rate_is_the_fingerprint_space() {
    let filter = build(&keys_for(16, 3));
    assert!((filter.false_positive_rate() - 1.0 / 256.0).abs() < f64::EPSILON);
}

/// Repeated keys are the normal case for a column filter, and they used to
/// make construction fail on *every* seed: a duplicated key touches each of
/// its slots twice, so no slot ever holds exactly one key and nothing can be
/// peeled. This is the regression guard for that failure.
#[test]
fn repeated_keys_collapse_before_peeling() {
    let distinct = keys_for(1000, 0xD00D);
    // Every key three times over, as a column with three-way repeats.
    let keys: Vec<[u8; 4]> = distinct
        .iter()
        .chain(&distinct)
        .chain(&distinct)
        .copied()
        .collect();
    let filter = build(&keys);
    assert_eq!(filter.len(), keys.len());
    assert!(keys.iter().all(|key| filter.contains(key)));
}

#[test]
fn a_single_repeated_key_collapses_to_one_entry() {
    let key = [7u8, 0, 0, 0];
    let filter = build(&vec![key; 64]);
    assert_eq!(filter.len(), 64);
    assert!(filter.contains(&key));
}

#[test]
fn repeated_key_filters_round_trip_through_an_image() {
    let distinct = keys_for(300, 0xBEEF);
    let doubled: Vec<[u8; 4]> = distinct.iter().chain(&distinct).copied().collect();
    let filter = build(&doubled);
    let decoded = XorFilter::deserialize(&filter.serialize()).expect("own image must decode");
    assert_eq!(decoded, filter);
    assert_eq!(decoded.len(), doubled.len());
    assert!(doubled.iter().all(|key| decoded.contains(key)));
}

/// Locks in the fix at the scale it was found at. The benchmark's 100 000
/// random keys contained two repeats and construction failed there; this
/// repeats that shape with a far higher repeat count so the guard does not
/// depend on a birthday collision happening to occur.
#[test]
fn large_key_set_with_many_repeats_builds() {
    let mut keys = keys_for(90_000, 0xF00D);
    let head: Vec<[u8; 4]> = keys.iter().take(10_000).copied().collect();
    keys.extend(head);
    assert_eq!(keys.len(), 100_000);
    let filter = build(&keys);
    assert_eq!(filter.len(), 100_000);
    assert!(keys.iter().all(|key| filter.contains(key)));
}
