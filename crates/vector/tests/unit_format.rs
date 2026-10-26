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
// Moved from `src/format.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::format::*;
use plomid_vector::r#type::{VectorElementType, VectorKind};
use plomid_vector::value::DenseVectorValue;
use plomid_vector::value::VectorValue;

#[test]
fn text_round_trip() {
    let v = VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0, 2.0]).unwrap(),
    );
    let text = pgwire_text_encode(&v);
    assert_eq!(text, "[1,2]");
    let back = pgwire_text_decode(&text, VectorKind::Dense, 2, 1).unwrap();
    assert_eq!(back, v);
    assert_eq!(
        format_sql_typed(&v, "VECTOR(FLOAT32, 2)"),
        "'[1,2]'::VECTOR(FLOAT32, 2)"
    );
}

#[test]
fn binary_round_trip() {
    let v = VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vec![1.0]).unwrap(),
    );
    let bytes = pgwire_binary_encode(&v).unwrap();
    let back =
        pgwire_binary_decode(&bytes, VectorKind::Dense, VectorElementType::Float32, 1, 1).unwrap();
    assert_eq!(back, v);
}
