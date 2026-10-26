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
// Moved from `src/value.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::VectorElementType;
use plomid_vector::value::*;

#[test]
fn sparse_sorts_and_rejects_dup_oob() {
    let s = SparseVectorValue::new(10, vec![7, 2], vec![1.0, 2.0]).unwrap();
    assert_eq!(s.indices, vec![2, 7]);
    assert!(SparseVectorValue::new(10, vec![3, 3], vec![1.0, 2.0]).is_err());
    assert!(SparseVectorValue::new(10, vec![10], vec![1.0]).is_err());
    assert!(SparseVectorValue::new(10, vec![1], vec![f32::NAN]).is_err());
    let empty = SparseVectorValue::new(10, vec![1], vec![0.0]).unwrap();
    assert_eq!(empty.nnz(), 0);
}

#[test]
fn binary_masks_padding() {
    let v = BinaryVectorValue::new(9, vec![0xFF, 0xFF]).unwrap();
    assert_eq!(v.bytes, vec![0xFF, 0x80]);
    assert!(v.get_bit(0));
    assert!(!v.get_bit(8 + 1));
}

#[test]
fn zero_norm_detection() {
    let d = DenseVectorValue::from_f32(VectorElementType::Float32, vec![0.0, 0.0]).unwrap();
    assert!(VectorValue::Dense(d).is_zero_norm());
}
