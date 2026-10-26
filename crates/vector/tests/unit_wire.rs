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
// Moved from `src/wire.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::{VectorElementType, VectorKind, VectorType};
use plomid_vector::value::DenseVectorValue;
use plomid_vector::wire::*;
use plomid_vector::VectorValue;

fn dense_f32(vals: Vec<f32>) -> VectorValue {
    VectorValue::Dense(DenseVectorValue::from_f32(VectorElementType::Float32, vals).unwrap())
}

fn all_types() -> Vec<VectorType> {
    // Binary-encodable shapes: Float32 dense + packed binary.
    vec![
        VectorType::new(VectorKind::Dense, VectorElementType::Float32, 3).unwrap(),
        VectorType::new(VectorKind::Binary, VectorElementType::Binary, 9).unwrap(),
    ]
}

fn text_only_types() -> Vec<VectorType> {
    // Non-Float32 dense elements: text-only on the wire (binary fails
    // closed rather than silently requantizing).
    vec![
        VectorType::new(VectorKind::Dense, VectorElementType::Float16, 4).unwrap(),
        VectorType::new(VectorKind::Dense, VectorElementType::Int8, 2).unwrap(),
        VectorType::new(VectorKind::Dense, VectorElementType::Uint8, 2).unwrap(),
        VectorType::new(VectorKind::Dense, VectorElementType::Int4, 8).unwrap(),
    ]
}

fn sample(t: &VectorType) -> VectorValue {
    // Values carry the column's element tag (quantized lanes are
    // integral and range-checked by from_f32).
    match (t.kind, t.element) {
        (VectorKind::Dense, e) => VectorValue::Dense(
            DenseVectorValue::from_f32(
                e,
                (0..t.dimension)
                    .map(|i| {
                        let v = i as f32 * 0.5;
                        match e {
                            VectorElementType::Int8 => (v % 8.0).round(),
                            VectorElementType::Uint8 => (v % 8.0).round().abs(),
                            VectorElementType::Int4 => ((v % 4.0).round()).clamp(-8.0, 7.0),
                            _ => v,
                        }
                    })
                    .collect(),
            )
            .unwrap(),
        ),
        (VectorKind::Binary, _) => plomid_vector::literal::parse_binary("'101010101'")
            .map(VectorValue::Binary)
            .unwrap(),
        _ => panic!("test helper covers dense + binary"),
    }
}

#[test]
fn wire_contract_holds_for_all_fixed_shapes() {
    for t in all_types() {
        let v = sample(&t);
        let wire = encode_for_wire(&v, &t).unwrap();
        assert_eq!(wire.oid, WIRE_OID_VECTOR);
        check_wire_vector(&wire, &t).unwrap();
    }
}

#[test]
fn non_float_dense_is_text_only_on_the_wire() {
    for t in text_only_types() {
        let v = sample(&t);
        // Binary refuses (fail closed); text round-trips exactly.
        assert!(encode_for_wire(&v, &t).is_err());
        let text = plomid_vector::format::pgwire_text_encode(&v);
        let back = decode_text_wire(&text, t.kind, t.dimension, t.max_vectors).unwrap();
        // Text carries f32 working-precision lanes (element retag is
        // the catalog layer's job): lanes + dimension must survive.
        match (&v, &back) {
            (VectorValue::Dense(a), VectorValue::Dense(b)) => {
                assert_eq!(a.dimension, b.dimension);
                assert_eq!(a.values_f32, b.values_f32);
            }
            _ => panic!("dense round-trip changed kind"),
        }
    }
}

#[test]
fn sparse_and_multi_roundtrip_as_variable_shapes() {
    let sparse = plomid_vector::literal::parse_sparse("{3:0.5, 1:1.5}/16")
        .map(VectorValue::Sparse)
        .unwrap();
    let st = VectorType::new(VectorKind::Sparse, VectorElementType::Float32, 16).unwrap();
    let wire = encode_for_wire(&sparse, &st).unwrap();
    assert!(expected_binary_len(st.kind, st.element, st.dimension)
        .unwrap()
        .is_none());
    check_wire_vector(&wire, &st).unwrap();
    let multi = plomid_vector::literal::parse_multi("[[1,2],[3,4]]", 2)
        .map(VectorValue::Multi)
        .unwrap();
    let mt =
        VectorType::with_max_vectors(VectorKind::Multi, VectorElementType::Float32, 2, 2).unwrap();
    check_wire_vector(&encode_for_wire(&multi, &mt).unwrap(), &mt).unwrap();
}

#[test]
fn wire_encode_rejects_mismatched_shapes() {
    let t = VectorType::new(VectorKind::Dense, VectorElementType::Float32, 3).unwrap();
    // Wrong dimension.
    assert!(encode_for_wire(&dense_f32(vec![1.0, 2.0]), &t).is_err());
    // Wrong kind.
    let b = plomid_vector::literal::parse_binary("'101'")
        .map(VectorValue::Binary)
        .unwrap();
    assert!(encode_for_wire(&b, &t).is_err());
    // Wrong OID / typmod trip the checker.
    let mut wire = encode_for_wire(&dense_f32(vec![1.0, 2.0, 3.0]), &t).unwrap();
    wire.oid = 25;
    assert!(check_wire_vector(&wire, &t).is_err());
}

#[test]
fn invalid_text_and_binary_fail_closed() {
    // Garbage text, truncated binary, and cross-kind decodes all fail.
    assert!(decode_text_wire("[1,", VectorKind::Dense, 2, 1).is_err());
    assert!(decode_text_wire("[NaN,0]", VectorKind::Dense, 2, 1).is_err());
    assert!(decode_binary_wire(
        &[0u8; 3],
        VectorKind::Dense,
        VectorElementType::Float32,
        2,
        1
    )
    .is_err());
    assert!(decode_binary_wire(
        &[0u8; 8],
        VectorKind::Binary,
        VectorElementType::Binary,
        9,
        1
    )
    .is_err());
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn binary_params_refuse_by_contract() {
    // Pinned: binary vector *parameters* are unsupported (fail closed,
    // text only). If this ever flips, the network tests must flip too.
    assert!(!BINARY_PARAMS_SUPPORTED);
}
