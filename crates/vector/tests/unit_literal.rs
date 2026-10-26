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
// Moved from `src/literal.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::literal::*;

#[test]
fn dense_ws_signs_exponents() {
    let d = parse_dense(" [ -0.1 , +2e3 , 1.5E-2 ] ", None).unwrap();
    assert_eq!(d.dimension, 3);
    assert!(parse_dense("[]", None).is_err());
    assert!(parse_dense("[1, NaN]", None).is_err());
    assert!(parse_dense("[1, inf]", None).is_err());
    assert!(parse_dense("[1,2]", Some(3)).is_err());
    assert_eq!(format_dense(&parse_dense("[1,2]", None).unwrap()), "[1,2]");
}

#[test]
fn sparse_round_trip() {
    let s = parse_sparse("{381:0.41, 15:0.82}/30000").unwrap();
    assert_eq!(s.indices, vec![15, 381]);
    assert_eq!(format_sparse(&s), "{15:0.82,381:0.41}/30000");
    assert!(parse_sparse("{1:1,1:2}/10").is_err());
    assert!(parse_sparse("{10:1}/10").is_err());
}

#[test]
fn binary_both_forms() {
    let a = parse_binary("'010101'").unwrap();
    let b = parse_binary("[0,1,0,1,0,1]").unwrap();
    assert_eq!(a, b);
    assert_eq!(format_binary(&a), "'010101'");
}

#[test]
fn multi_round_trip() {
    let m = parse_multi("[[1,2],[3,4]]", 8).unwrap();
    assert_eq!(m.count(), 2);
    assert_eq!(format_multi(&m), "[[1,2],[3,4]]");
    assert!(parse_multi("[[1,2],[3]]", 8).is_err());
}
