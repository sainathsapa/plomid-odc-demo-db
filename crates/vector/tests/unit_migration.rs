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
// Moved from `src/migration.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::index::IndexKind;
use plomid_vector::migration::*;

#[test]
fn every_artifact_is_versioned_at_one() {
    // Today every version is 1: the matrix is explicit, not assumed.
    for a in VersionedArtifact::all() {
        assert_eq!(a.current_version(), 1, "{}", a.as_str());
    }
    assert_eq!(VersionedArtifact::all().len(), 13);
}

#[test]
fn same_version_supported_newer_never() {
    for a in VersionedArtifact::all() {
        let c = a.current_version();
        assert_eq!(check_transition(a, c), Compatibility::Supported);
        assert!(check_transition(a, c).can_open());
        assert!(check_transition(a, c).can_query());
        assert_eq!(check_transition(a, c + 1), Compatibility::NotSupported);
        assert!(!check_transition(a, c + 1).can_open());
    }
}

#[test]
fn older_indexes_rebuild_older_framing_fails() {
    // No older version exists yet; the policy is pinned at v0 semantics.
    for a in [
        VersionedArtifact::Hnsw,
        VersionedArtifact::Ivf,
        VersionedArtifact::Sq,
        VersionedArtifact::Pq,
        VersionedArtifact::Binary,
        VersionedArtifact::DiskAnn,
        VersionedArtifact::IndexManifest,
    ] {
        let v = check_transition(a, 0);
        assert_eq!(v, Compatibility::RebuildRequired, "{}", a.as_str());
        assert!(v.must_rebuild());
        assert!(!v.can_query());
    }
    for a in [
        VersionedArtifact::Codec,
        VersionedArtifact::StorageManifest,
        VersionedArtifact::StoragePage,
        VersionedArtifact::Snapshot,
        VersionedArtifact::Stats,
        VersionedArtifact::Backup,
    ] {
        assert_eq!(
            check_transition(a, 0),
            Compatibility::NotSupported,
            "{}",
            a.as_str()
        );
    }
}

#[test]
fn blob_classification_drives_rebuild() {
    use plomid_vector::index::VectorIndex as _;
    // Real HNSW bytes classify supported.
    let rows: Vec<(u64, Vec<f32>)> = (0..16u64).map(|i| (i, vec![i as f32, 0.0])).collect();
    let mut idx =
        plomid_vector::index::hnsw::HnswIndex::new(plomid_vector::index::IndexConfig::new(
            IndexKind::Hnsw,
            plomid_vector::index::Metric::L2,
            2,
        ))
        .unwrap();
    idx.build(&rows).unwrap();
    let blob = idx.encode();
    assert_eq!(
        classify_index_blob(IndexKind::Hnsw, &blob),
        Compatibility::Supported
    );
    // Version-skewed bytes (bump the leading u32) require rebuild.
    let mut skewed = blob.clone();
    skewed[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        classify_index_blob(IndexKind::Hnsw, &skewed),
        Compatibility::NotSupported
    );
    let mut older = blob.clone();
    older[0..4].copy_from_slice(&0u32.to_le_bytes());
    assert_eq!(
        classify_index_blob(IndexKind::Hnsw, &older),
        Compatibility::RebuildRequired
    );
    // Truncation is corruption, never a version.
    assert_eq!(
        classify_index_blob(IndexKind::Hnsw, &blob[..2]),
        Compatibility::NotSupported
    );
    // Kinds without bytes always rebuild.
    assert_eq!(
        classify_index_blob(IndexKind::Flat, &blob),
        Compatibility::RebuildRequired
    );
}

#[test]
fn enforce_maps_to_distinct_error_codes() {
    assert!(enforce(VersionedArtifact::Codec, 1).is_ok());
    let e = enforce(VersionedArtifact::Codec, 2).unwrap_err();
    assert_eq!(e.code(), "VECTOR_INDEX_CORRUPTED");
    let e = enforce(VersionedArtifact::Hnsw, 0).unwrap_err();
    assert_eq!(e.code(), "VECTOR_INDEX_REBUILD_REQUIRED");
}

#[test]
fn matrix_is_total_and_explained() {
    let m = matrix();
    assert_eq!(m.len(), 13);
    for row in &m {
        assert!(row.backup_required);
        assert!(!row.rollback_possible);
    }
    let lines = explain_matrix();
    assert_eq!(lines.len(), 15);
    assert!(lines.iter().any(|l| l.contains("hnsw")));
}
