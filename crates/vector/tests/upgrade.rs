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

//! Upgrade / downgrade / format-compatibility matrix (Phase 49 in code).
//!
//! Every persisted vector/index format version must fail closed on mismatch:
//! old readers never silently reinterpret new bytes, and new readers never
//! reinterprettag-swapped or version-bumped bytes. Higher-level upgrade
//! policy (rebuild path) lives in the executor; this suite pins the
//! byte-level contract.

use plomid_vector::index::manifest::{IndexManifest, MANIFEST_VERSION};
use plomid_vector::index::{IndexKind, Metric};
use plomid_vector::{
    decode_standalone, encode_standalone, DenseVectorValue, VectorElementType, VectorValue, MAGIC,
    VERSION,
};

fn set_version(mut bytes: Vec<u8>, version: u8) -> Vec<u8> {
    // Standalone header: MAGIC[4] version[1] kind[1] elem[1] rsv[1] dim[4] ...
    assert_eq!(bytes[0..4], MAGIC);
    bytes[4] = version;
    bytes
}

#[test]
fn standalone_version_bump_fails_closed() {
    let dense = DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, 2.0]).unwrap();
    let bytes = encode_standalone(&VectorValue::Dense(dense)).unwrap();
    assert_eq!(&bytes[0..4], MAGIC);
    assert_eq!(bytes[4], VERSION);
    // A future version must not decode under v1 rules.
    let err = decode_standalone(&set_version(bytes.clone(), VERSION + 1)).unwrap_err();
    assert_eq!(err.code(), "VECTOR_CORRUPTION", "unexpected: {err}");
    // Round-trip under the current version still works.
    assert!(matches!(
        decode_standalone(&bytes).unwrap(),
        VectorValue::Dense(_)
    ));
}

#[test]
fn manifest_version_bump_fails_closed() {
    let manifest = IndexManifest::new(IndexKind::Hnsw, Metric::L2, 128, 7);
    let bytes = manifest.encode();
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        MANIFEST_VERSION
    );
    let mut future = bytes.clone();
    future[0..4].copy_from_slice(&(MANIFEST_VERSION + 1).to_le_bytes());
    let err = IndexManifest::decode(&future).unwrap_err();
    assert_eq!(err.code(), "VECTOR_INDEX_CORRUPTED", "unexpected: {err}");
    assert_eq!(IndexManifest::decode(&bytes).unwrap(), manifest);
}

#[test]
fn manifest_unknown_kind_and_metric_fail_closed() {
    let manifest = IndexManifest::new(IndexKind::IvfPq, Metric::Dot, 64, 1);
    let mut bytes = manifest.encode();
    // version[0..4] kind[4..8] metric[8..12]
    bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
    assert!(IndexManifest::decode(&bytes).is_err());
    let mut bytes = manifest.encode();
    bytes[8..12].copy_from_slice(&99u32.to_le_bytes());
    assert!(IndexManifest::decode(&bytes).is_err());
    // Truncation at every prefix length fails (no partial reads).
    let full = manifest.encode();
    for cut in 0..full.len() {
        assert!(
            IndexManifest::decode(&full[..cut]).is_err(),
            "prefix {cut} accepted"
        );
    }
}

#[test]
fn typmod_survives_all_kinds_across_versions() {
    use plomid_vector::{VectorKind, VectorType};
    let cases = [
        VectorType::new(VectorKind::Dense, VectorElementType::Float32, 1536).unwrap(),
        VectorType::new(VectorKind::Sparse, VectorElementType::Float32, 30000).unwrap(),
        VectorType::with_max_vectors(VectorKind::Multi, VectorElementType::Float32, 768, 128)
            .unwrap(),
        VectorType::new(VectorKind::Binary, VectorElementType::Binary, 1024).unwrap(),
    ];
    for t in cases {
        let back = VectorType::typmod_decode(t.typmod_encode().unwrap()).unwrap();
        assert_eq!(
            (back.kind, back.element, back.dimension),
            (t.kind, t.element, t.dimension)
        );
    }
}
