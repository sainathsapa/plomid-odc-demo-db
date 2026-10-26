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
// Moved from `src/error.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::error::*;

#[test]
fn display_is_stable() {
    let e = VectorError::DimensionMismatch {
        expected: 3,
        got: 4,
    };
    assert_eq!(e.to_string(), "dimension mismatch: expected 3, got 4");
    let e2 = VectorError::InvalidLiteral("truncated".to_string());
    assert!(e2.to_string().contains("truncated"));
}

#[test]
fn is_std_error() {
    let e: Box<dyn std::error::Error> = Box::new(VectorError::Cancelled("shutdown".to_string()));
    assert!(e.to_string().contains("cancelled"));
}
