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
// Moved from `src/type.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::*;
#[test]
fn edges() {
    let f = VectorElementType::Float32;
    for bad in [0u32, 65537] {
        assert!(VectorType::new(VectorKind::Dense, f, bad).is_err());
    }
    for good in [1u32, 768, 65536] {
        assert!(VectorType::new(VectorKind::Dense, f, good).is_ok());
    }
    assert!(VectorType::new(VectorKind::Sparse, VectorElementType::Binary, 8).is_err());
    let t = VectorType::new(VectorKind::Dense, f, 65535).unwrap();
    let b = VectorType::typmod_decode(t.typmod_encode().unwrap()).unwrap();
    assert_eq!((b.kind, b.element, b.dimension), (t.kind, t.element, 65535));
    let m = VectorType::parse_sql("MULTI_VECTOR(FLOAT32, 128, 768)").unwrap();
    assert_eq!((m.max_vectors, m.dimension), (128, 768));
}
