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
// Moved from `src/selectivity.rs` inline `#[cfg(test)]` — public-API only.
use plomid_optimizer::selectivity::*;

#[test]
fn equals_uses_one_over_distinct() {
    let hints = ColumnSelectivityHints {
        distinct_estimate: 100,
        ..ColumnSelectivityHints::unknown()
    };
    let sel = estimate_selectivity(PredicateKind::Equals, Some(&hints));
    assert!((sel - 0.01).abs() < 1e-12, "got {sel}");
}

#[test]
fn equals_without_stats_falls_back_to_default() {
    assert_eq!(
        estimate_selectivity(PredicateKind::Equals, None),
        DEFAULT_EQUALS_SELECTIVITY
    );
    assert_eq!(
        estimate_selectivity(
            PredicateKind::Equals,
            Some(&ColumnSelectivityHints::unknown())
        ),
        DEFAULT_EQUALS_SELECTIVITY
    );
}

#[test]
fn range_defaults_to_one_third() {
    assert_eq!(
        estimate_selectivity(PredicateKind::Range, None),
        DEFAULT_RANGE_SELECTIVITY
    );
    let hints = ColumnSelectivityHints {
        row_count: 1_000_000,
        min: Some(0.0),
        max: Some(100.0),
        ..ColumnSelectivityHints::unknown()
    };
    assert_eq!(
        estimate_selectivity(PredicateKind::Range, Some(&hints)),
        DEFAULT_RANGE_SELECTIVITY
    );
}

#[test]
fn is_null_trusts_observed_fraction() {
    let hints = ColumnSelectivityHints {
        null_fraction: 0.07,
        ..ColumnSelectivityHints::unknown()
    };
    assert!((estimate_selectivity(PredicateKind::IsNull, Some(&hints)) - 0.07).abs() < 1e-12);
    assert_eq!(
        estimate_selectivity(PredicateKind::IsNull, None),
        DEFAULT_IS_NULL_SELECTIVITY
    );
}

#[test]
fn like_and_other_have_documented_defaults() {
    assert_eq!(
        estimate_selectivity(PredicateKind::Like, None),
        DEFAULT_LIKE_SELECTIVITY
    );
    assert_eq!(
        estimate_selectivity(PredicateKind::Other, None),
        DEFAULT_OTHER_SELECTIVITY
    );
    assert_eq!(
        estimate_selectivity(PredicateKind::InList, None),
        DEFAULT_IN_LIST_SELECTIVITY
    );
}

#[test]
fn estimates_always_stay_unit_range() {
    let hints = ColumnSelectivityHints {
        null_fraction: 7.5,
        distinct_estimate: 0,
        ..ColumnSelectivityHints::unknown()
    };
    for kind in [
        PredicateKind::Equals,
        PredicateKind::Range,
        PredicateKind::InList,
        PredicateKind::IsNull,
        PredicateKind::Like,
        PredicateKind::Other,
    ] {
        let sel = estimate_selectivity(kind, Some(&hints));
        assert!((0.0..=1.0).contains(&sel), "{kind}: {sel}");
    }
}

#[test]
fn and_or_combinators_follow_independence() {
    assert!((combine_selectivity_and(0.5, 0.2) - 0.1).abs() < 1e-12);
    assert!((combine_selectivity_or(0.5, 0.2) - 0.6).abs() < 1e-12);
}
