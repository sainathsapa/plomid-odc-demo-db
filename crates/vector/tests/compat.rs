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
//! Persisted-format compatibility fixtures: typmod identity round-trips,
//! codec version rejection, and golden byte stability.
//!
//! These pin the on-disk contract so a future layout change fails loudly
//! here instead of silently reinterpreting old bytes.

use plomid_vector::{VectorElementType, VectorKind, VectorType};

#[test]
fn typmod_round_trips_every_kind_and_element() {
    let cases = [
        (VectorKind::Dense, VectorElementType::Float32, 1),
        (VectorKind::Dense, VectorElementType::Float32, 1536),
        (VectorKind::Dense, VectorElementType::Float32, 65536),
        (VectorKind::Dense, VectorElementType::Float16, 768),
        (VectorKind::Dense, VectorElementType::BFloat16, 768),
        (VectorKind::Dense, VectorElementType::Int8, 128),
        (VectorKind::Dense, VectorElementType::Uint8, 128),
        (VectorKind::Dense, VectorElementType::Int4, 65),
        (VectorKind::Binary, VectorElementType::Binary, 1),
        (VectorKind::Binary, VectorElementType::Binary, 9),
        (VectorKind::Binary, VectorElementType::Binary, 65536),
        (VectorKind::Sparse, VectorElementType::Float32, 30000),
        (VectorKind::Sparse, VectorElementType::Int8, 30000),
        (VectorKind::Multi, VectorElementType::Float32, 768),
    ];
    for (kind, element, dim) in cases {
        let ty = VectorType::new(kind, element, dim).unwrap();
        let typmod = ty.typmod_encode().unwrap();
        let back = VectorType::typmod_decode(typmod).unwrap();
        assert_eq!(
            (back.kind, back.element, back.dimension),
            (kind, element, dim)
        );
    }
}

#[test]
fn typmod_rejects_garbage() {
    // Negative and zero typmods are reserved, never a type.
    assert!(VectorType::typmod_decode(-1).is_err());
    assert!(VectorType::typmod_decode(0).is_err());
    // Unknown kind/element discriminants fail closed.
    assert!(VectorType::typmod_decode((9 << 24) | (1 << 16)).is_err());
    assert!(VectorType::typmod_decode((1 << 24) | (9 << 16)).is_err());
}

#[test]
fn codec_rejects_wrong_magic_and_version() {
    use plomid_vector::{DenseVectorValue, VectorValue};
    let value = VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, 2.0]).unwrap(),
    );
    let mut bytes = plomid_vector::encode_standalone(&value).unwrap();
    // Corrupt magic.
    bytes[0] ^= 0xFF;
    assert!(plomid_vector::decode_standalone(&bytes).is_err());
    // Corrupt version byte (header layout: PVEC + version).
    let mut bytes = plomid_vector::encode_standalone(&value).unwrap();
    bytes[4] = bytes[4].wrapping_add(1);
    assert!(plomid_vector::decode_standalone(&bytes).is_err());
    // Truncation and trailing bytes both fail.
    let bytes = plomid_vector::encode_standalone(&value).unwrap();
    assert!(plomid_vector::decode_standalone(&bytes[..bytes.len() - 1]).is_err());
    let mut plus = bytes.clone();
    plus.push(0);
    assert!(plomid_vector::decode_standalone(&plus).is_err());
}

#[test]
fn golden_bytes_stable_for_dense() {
    // A fixed vector must encode to fixed bytes across releases; if this
    // changes, bump the codec version and add a migration fixture instead.
    use plomid_vector::{DenseVectorValue, VectorValue};
    let value = VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, -2.0]).unwrap(),
    );
    let bytes = plomid_vector::encode_standalone(&value).unwrap();
    assert_eq!(&bytes[0..4], b"PVEC");
    assert_eq!(plomid_vector::decode_standalone(&bytes).unwrap(), value);
}

#[test]
fn sql_names_round_trip() {
    for (sql, kind, dim) in [
        ("VECTOR(FLOAT32, 4)", VectorKind::Dense, 4),
        ("VECTOR(INT8, 128)", VectorKind::Dense, 128),
        ("VECTOR(BINARY, 64)", VectorKind::Binary, 64),
        ("SPARSE_VECTOR(FLOAT32, 30000)", VectorKind::Sparse, 30000),
        ("MULTI_VECTOR(FLOAT32, 8, 768)", VectorKind::Multi, 768),
    ] {
        let ty = VectorType::parse_sql(sql).unwrap();
        assert_eq!((ty.kind, ty.dimension), (kind, dim), "{sql}");
    }
    // Shorthand is a SQL-layer alias only; the crate requires arity 2.
    assert!(VectorType::parse_sql("VECTOR(4)").is_err());
}
