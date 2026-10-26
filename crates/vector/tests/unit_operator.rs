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
// Moved from `src/operator.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::admin::IndexState;
use plomid_vector::operator::*;
use plomid_vector::search::SearchError;

#[test]
fn every_vector_error_maps_to_a_category() {
    // Exhaustive: adding a variant without mapping fails here.
    let cases = [
        (
            plomid_vector::VectorError::InvalidDimension("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::InvalidElementType("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::InvalidLiteral("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::NonFiniteValue("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::NullArgument("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::ZeroNorm("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::TypeMismatch("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::UnsupportedCombination("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::IndexUnsupported("x".into()),
            ErrorCategory::UnavailableIndex,
        ),
        (
            plomid_vector::VectorError::IndexParameterInvalid("x".into()),
            ErrorCategory::InvalidConfiguration,
        ),
        (
            plomid_vector::VectorError::IndexStateInvalid("x".into()),
            ErrorCategory::UnavailableIndex,
        ),
        (
            plomid_vector::VectorError::IndexCorrupted("x".into()),
            ErrorCategory::IndexCorruption,
        ),
        (
            plomid_vector::VectorError::RebuildRequired("x".into()),
            ErrorCategory::Migration,
        ),
        (
            plomid_vector::VectorError::DimensionMismatch {
                expected: 1,
                got: 2,
            },
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::MetricMismatch("x".into()),
            ErrorCategory::InvalidInput,
        ),
        (
            plomid_vector::VectorError::ResourceLimit("x".into()),
            ErrorCategory::ResourceExhaustion,
        ),
        (
            plomid_vector::VectorError::Cancelled("x".into()),
            ErrorCategory::Cancellation,
        ),
        (
            plomid_vector::VectorError::Corruption("x".into()),
            ErrorCategory::StorageFailure,
        ),
    ];
    assert_eq!(cases.len(), 18, "all 18 VectorError variants covered");
    for (e, c) in &cases {
        assert_eq!(categorize_vector_error(e), *c, "{e}");
        // Every category documents an (observed) SQLSTATE behavior.
        assert!(!sqlstate_for_category(*c).is_empty());
    }
}

#[test]
fn every_search_error_maps_to_a_category() {
    let cases = [
        (
            SearchError::DimensionMismatch {
                expected: 1,
                actual: 2,
            },
            ErrorCategory::InvalidInput,
        ),
        (
            SearchError::InvalidLength { what: "x", len: 1 },
            ErrorCategory::InvalidInput,
        ),
        (
            SearchError::OutOfRange {
                value: 1,
                lo: 0,
                hi: 2,
            },
            ErrorCategory::InvalidInput,
        ),
        (
            SearchError::Overflow { what: "x" },
            ErrorCategory::ResourceExhaustion,
        ),
        (SearchError::InvalidK { k: 1 }, ErrorCategory::InvalidInput),
        (
            SearchError::InvalidRadius { radius: 1.0 },
            ErrorCategory::InvalidInput,
        ),
        (SearchError::NonFiniteQuery, ErrorCategory::InvalidInput),
        (SearchError::Cancelled, ErrorCategory::Cancellation),
        (
            SearchError::FetchFailed { row_id: 1 },
            ErrorCategory::StorageFailure,
        ),
    ];
    assert_eq!(cases.len(), 9, "all 9 SearchError variants covered");
    for (e, c) in &cases {
        assert_eq!(categorize_search_error(e), *c, "{e}");
    }
}

#[test]
fn config_inventory_is_complete_and_honest() {
    let inv = config_inventory();
    assert_eq!(inv.len(), 15, "9 SQL knobs + 6 env vars; extend with docs");
    for s in &inv {
        assert!(!s.name.is_empty() && !s.effect.is_empty() && !s.on_invalid.is_empty());
    }
    // SQL knobs are dynamic; env vars need restart.
    assert!(inv
        .iter()
        .filter(|s| !s.name.contains('.'))
        .all(|s| s.restart_required));
    assert!(inv
        .iter()
        .filter(|s| s.name.contains('.'))
        .all(|s| !s.restart_required));
}

#[test]
fn index_status_view_matches_lifecycle_gate() {
    let v = IndexStatusView::new(
        "d".into(),
        IndexState::Ready,
        plomid_vector::freshness::Freshness::Fresh,
    );
    assert!(v.queryable);
    let v = IndexStatusView::new(
        "d".into(),
        IndexState::Ready,
        plomid_vector::freshness::Freshness::Stale,
    );
    assert!(!v.queryable);
    assert!(v.reason.contains("exact fallback"));
    let v = IndexStatusView::new(
        "d".into(),
        IndexState::Building,
        plomid_vector::freshness::Freshness::Fresh,
    );
    assert!(!v.queryable);
}

#[test]
fn support_checklist_covers_investigations() {
    let list = support_checklist();
    assert_eq!(list.len(), 12);
    for (symptom, source) in &list {
        assert!(!symptom.is_empty() && !source.is_empty());
    }
}
