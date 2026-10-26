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
// Moved from `src/validation.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::VectorElementType;
use plomid_vector::validation::*;

#[test]
fn dim_edges() {
    assert!(validate_dimension(0).is_err());
    assert!(validate_dimension(1).is_ok());
    assert!(validate_dimension(65536).is_ok());
    assert!(validate_dimension(65537).is_err());
}

#[test]
fn payload_math_checked() {
    assert_eq!(
        dense_payload_bytes(VectorElementType::Float32, 768).unwrap(),
        3072
    );
    assert_eq!(binary_payload_bytes(9).unwrap(), 2);
    assert!(sparse_payload_bytes(30001, 30000).is_err());
    assert!(check_finite(f32::NAN, "q").is_err());
    assert!(check_finite(f32::INFINITY, "q").is_err());
    assert!(check_finite(1.5, "q").is_ok());
}
