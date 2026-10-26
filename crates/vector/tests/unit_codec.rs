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
// Moved from `src/codec.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::codec::*;
use plomid_vector::r#type::{VectorElementType, VectorKind};
use plomid_vector::validation;
use plomid_vector::value::{
    BinaryVectorValue, DenseVectorValue, MultiVectorValue, SparseVectorValue, VectorValue,
};

fn row_ctx(v: &VectorValue) -> (VectorKind, VectorElementType, u32, u32) {
    match v {
        VectorValue::Dense(d) => (VectorKind::Dense, d.element, d.dimension, 1),
        VectorValue::Sparse(s) => (
            VectorKind::Sparse,
            VectorElementType::Float32,
            s.dimension,
            1,
        ),
        VectorValue::Multi(m) => (VectorKind::Multi, m.element, m.dimension, m.max_vectors),
        VectorValue::Binary(b) => (
            VectorKind::Binary,
            VectorElementType::Binary,
            b.dimension_bits,
            1,
        ),
    }
}

#[test]
fn round_trip_all_kinds() {
    let f = VectorElementType::Float32;
    let vals = vec![
        VectorValue::Dense(DenseVectorValue::from_f32(f, vec![1.0, -2.0]).unwrap()),
        VectorValue::Sparse(SparseVectorValue::new(30000, vec![15], vec![0.5]).unwrap()),
        VectorValue::Multi(
            MultiVectorValue::new(f, 2, 4, vec![vec![1.0, 2.0], vec![3.0, 4.0]]).unwrap(),
        ),
        VectorValue::Binary(BinaryVectorValue::new(9, vec![0xAA, 0x80]).unwrap()),
    ];
    for v in &vals {
        let bytes = encode_standalone(v).unwrap();
        assert_eq!(&bytes[0..4], b"PVEC");
        let mut expect = (*v).clone();
        // Standalone artifacts do not preserve the catalog cap.
        if let VectorValue::Multi(m) = &mut expect {
            m.max_vectors = validation::MAX_DIMENSION;
        }
        assert_eq!(decode_standalone(&bytes).unwrap(), expect);
        let row = encode_row_payload(v).unwrap();
        let (k, e, d, m) = row_ctx(v);
        assert_eq!(decode_row_payload(&row, k, e, d, m).unwrap(), *v);
    }
}

#[test]
fn crc_detects_flip() {
    let v = VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0]).unwrap(),
    );
    let mut bytes = encode_standalone(&v).unwrap();
    bytes[HEADER_LEN] ^= 0xFF;
    assert!(decode_standalone(&bytes).is_err());
}
