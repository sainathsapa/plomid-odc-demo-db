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
// Moved from `src/capability.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::capability::*;
use plomid_vector::index::IndexKind;
use plomid_vector::r#type::{VectorElementType, VectorKind, VectorMetric};

#[test]
fn dense_float_supported_everywhere_numeric() {
    use plomid_vector::index::IndexKind as I;
    for m in [VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot] {
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::Flat),
            Capability::Supported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::Hnsw),
            Capability::Supported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::IvfFlat),
            Capability::Supported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::IvfSq),
            Capability::Supported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::IvfPq),
            Capability::Supported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::DiskAnn),
            Capability::Supported
        );
        // Wrong-kind indexes stay closed.
        assert_eq!(
            capability(
                VectorKind::Dense,
                VectorElementType::Float32,
                m,
                I::SparseInverted
            ),
            Capability::Unsupported
        );
        assert_eq!(
            capability(VectorKind::Dense, VectorElementType::Float32, m, I::Binary),
            Capability::Unsupported
        );
    }
}

#[test]
fn binary_and_sparse_envelopes() {
    use plomid_vector::index::IndexKind as I;
    assert_eq!(
        capability(
            VectorKind::Binary,
            VectorElementType::Binary,
            VectorMetric::Hamming,
            I::Binary
        ),
        Capability::Supported
    );
    // Jaccard has no block-equality signal: the binary index cannot serve
    // it (queries stay on the exact scan, unaccelerated but correct).
    assert_eq!(
        capability(
            VectorKind::Binary,
            VectorElementType::Binary,
            VectorMetric::Jaccard,
            I::Binary
        ),
        Capability::Unsupported
    );
    assert_eq!(
        capability(
            VectorKind::Binary,
            VectorElementType::Binary,
            VectorMetric::L2,
            I::Binary
        ),
        Capability::Unsupported
    );
    assert_eq!(
        capability(
            VectorKind::Sparse,
            VectorElementType::Float32,
            VectorMetric::Dot,
            I::SparseInverted
        ),
        Capability::Supported
    );
    assert_eq!(
        capability(
            VectorKind::Sparse,
            VectorElementType::Binary,
            VectorMetric::Dot,
            I::SparseInverted
        ),
        Capability::Unsupported
    );
    // INT8 on PQ needs float promotion; INT4 on PQ is rejected.
    assert_eq!(
        capability(
            VectorKind::Dense,
            VectorElementType::Int8,
            VectorMetric::L2,
            I::IvfPq
        ),
        Capability::SupportedWithConversion
    );
    assert_eq!(
        capability(
            VectorKind::Dense,
            VectorElementType::Int4,
            VectorMetric::L2,
            I::IvfPq
        ),
        Capability::Unsupported
    );
    assert_eq!(
        capability(
            VectorKind::Dense,
            VectorElementType::Int4,
            VectorMetric::L2,
            I::IvfSq
        ),
        Capability::SupportedWithConversion
    );
}

#[test]
fn multi_vectors_have_no_dense_ann_execution() {
    use plomid_vector::index::IndexKind as I;
    // The executor serves dense methods from dense f32 lanes only; a
    // MULTI_VECTOR column keeps its exact constituent kernel. The matrix
    // must not claim index acceleration that cannot execute.
    for index in [I::Hnsw, I::IvfFlat, I::IvfSq, I::IvfPq, I::DiskAnn] {
        for metric in [VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot] {
            assert_eq!(
                capability(VectorKind::Multi, VectorElementType::Float32, metric, index),
                Capability::Unsupported,
                "{index:?} must not claim MULTI acceleration for {metric:?}"
            );
        }
    }
}

/// The capability matrix and the `VectorType` predicates (which gate DDL and
/// the executor) describe the same envelope; drift between them is a false
/// claim by construction, so pin them together over the full product.
#[test]
fn matrix_matches_type_predicates() {
    use plomid_vector::index::IndexKind as I;
    use plomid_vector::VectorType;
    const KINDS: [VectorKind; 4] = [
        VectorKind::Dense,
        VectorKind::Sparse,
        VectorKind::Multi,
        VectorKind::Binary,
    ];
    const ELEMS: [VectorElementType; 7] = [
        VectorElementType::Float32,
        VectorElementType::Float16,
        VectorElementType::BFloat16,
        VectorElementType::Int8,
        VectorElementType::Uint8,
        VectorElementType::Int4,
        VectorElementType::Binary,
    ];
    const METRICS: [VectorMetric; 5] = [
        VectorMetric::L2,
        VectorMetric::Cosine,
        VectorMetric::Dot,
        VectorMetric::Hamming,
        VectorMetric::Jaccard,
    ];
    for kind in KINDS {
        for element in ELEMS {
            // Binary element only exists on the Binary kind; skip invalid
            // combinations (VectorType::new would reject them anyway).
            if element == VectorElementType::Binary && kind != VectorKind::Binary {
                continue;
            }
            let Ok(ty) = VectorType::with_max_vectors(kind, element, 4, 1) else {
                continue;
            };
            for metric in METRICS {
                let expect = |index: I, predicate: fn(&VectorType, VectorMetric) -> bool| {
                    let want = predicate(&ty, metric);
                    let got = capability(kind, element, metric, index).is_usable();
                    assert_eq!(
                        got, want,
                        "capability drift: {kind:?}/{element:?}/{metric:?}/{index:?}: \
                         matrix={got} but type predicate={want}"
                    );
                };
                expect(I::Hnsw, |t, m| t.supports_hnsw(m));
                expect(I::IvfFlat, |t, m| t.supports_ivf(m));
                expect(I::IvfSq, |t, m| t.supports_sq(m));
                expect(I::IvfPq, |t, m| t.supports_pq(m));
                // DiskANN shares the HNSW envelope in the executor.
                expect(I::DiskAnn, |t, m| t.supports_hnsw(m));
                // Flat is the exact oracle: usable exactly where the exact
                // scan is defined (binary kind is Hamming/Jaccard-only;
                // numeric kinds take everything else).
                let flat_want = ty.supports_exact(metric);
                assert_eq!(
                    capability(kind, element, metric, I::Flat).is_usable(),
                    flat_want,
                    "flat capability drift for {kind:?}/{element:?}/{metric:?}"
                );
            }
            // Sparse inverted and binary envelopes.
            if kind == VectorKind::Sparse && element != VectorElementType::Binary {
                for metric in [VectorMetric::L2, VectorMetric::Cosine, VectorMetric::Dot] {
                    assert!(capability(kind, element, metric, I::SparseInverted).is_usable());
                }
            }
            if kind == VectorKind::Binary {
                assert!(capability(kind, element, VectorMetric::Hamming, I::Binary).is_usable());
                assert!(!capability(kind, element, VectorMetric::Jaccard, I::Binary).is_usable());
            }
        }
    }
}

#[test]
fn matrix_is_total() {
    use plomid_vector::index::IndexKind as I;
    const KINDS: [VectorKind; 4] = [
        VectorKind::Dense,
        VectorKind::Sparse,
        VectorKind::Multi,
        VectorKind::Binary,
    ];
    const ELEMS: [VectorElementType; 7] = [
        VectorElementType::Float32,
        VectorElementType::Float16,
        VectorElementType::BFloat16,
        VectorElementType::Int8,
        VectorElementType::Uint8,
        VectorElementType::Int4,
        VectorElementType::Binary,
    ];
    const METRICS: [VectorMetric; 5] = [
        VectorMetric::L2,
        VectorMetric::Cosine,
        VectorMetric::Dot,
        VectorMetric::Hamming,
        VectorMetric::Jaccard,
    ];
    const INDEXES: [I; 8] = [
        I::Flat,
        I::Hnsw,
        I::IvfFlat,
        I::IvfSq,
        I::IvfPq,
        I::DiskAnn,
        I::SparseInverted,
        I::Binary,
    ];
    // 4*7*5*8 = 1120 combinations, each exactly one capability.
    let mut counted = 0;
    for k in KINDS {
        for e in ELEMS {
            for m in METRICS {
                for i in INDEXES {
                    let c = capability(k, e, m, i);
                    assert!(!c.name().is_empty());
                    counted += 1;
                }
            }
        }
    }
    assert_eq!(counted, 1120);
}

#[test]
fn check_fails_closed() {
    assert!(check(
        VectorKind::Dense,
        VectorElementType::Float32,
        VectorMetric::L2,
        IndexKind::Hnsw
    )
    .is_ok());
    let err = check(
        VectorKind::Dense,
        VectorElementType::Float32,
        VectorMetric::L2,
        IndexKind::Binary,
    )
    .unwrap_err();
    assert_eq!(err.code(), "VECTOR_INDEX_UNSUPPORTED");
}
