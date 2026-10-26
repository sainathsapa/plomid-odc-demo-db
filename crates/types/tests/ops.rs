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
// Moved from `src/ops.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::PgValue::*;
use plomid_types::{builtin_operators, BinaryOp, UnaryOp};

#[test]
fn binary_arith() {
    let reg = builtin_operators();
    let r = reg.apply_binary(BinaryOp::Add, Int4(2), Int4(3)).unwrap();
    assert_eq!(r, Int4(5));
    let r = reg
        .apply_binary(BinaryOp::Multiply, Int4(6), Int4(7))
        .unwrap();
    assert_eq!(r, Int4(42));
    let r = reg
        .apply_binary(BinaryOp::Divide, Float8(10.0), Float8(4.0))
        .unwrap();
    assert_eq!(r, Float8(2.5));
}

#[test]
fn comparisons() {
    let reg = builtin_operators();
    assert_eq!(
        reg.apply_binary(BinaryOp::Less, Int4(1), Int4(2)).unwrap(),
        Bool(true)
    );
    assert_eq!(
        reg.apply_binary(BinaryOp::GreaterOrEqual, Int4(5), Int4(5))
            .unwrap(),
        Bool(true)
    );
    assert_eq!(
        reg.apply_binary(BinaryOp::NotEqual, Text("a".into()), Text("b".into()))
            .unwrap(),
        Bool(true)
    );
}

#[test]
fn concat_op() {
    let reg = builtin_operators();
    let r = reg
        .apply_binary(BinaryOp::Concat, Text("a".into()), Text("b".into()))
        .unwrap();
    assert_eq!(r, Text("ab".into()));
    assert_eq!(
        reg.apply_binary(BinaryOp::Concat, Null, Text("x".into()))
            .unwrap(),
        Null
    );
}

#[test]
fn div_by_zero_errors() {
    let reg = builtin_operators();
    assert!(reg
        .apply_binary(BinaryOp::Divide, Int4(1), Int4(0))
        .is_err());
    assert!(reg
        .apply_binary(BinaryOp::Modulo, Int8(1), Int8(0))
        .is_err());
}

#[test]
fn unary_ops() {
    let reg = builtin_operators();
    assert_eq!(reg.apply_unary(UnaryOp::Negate, Int4(5)).unwrap(), Int4(-5));
    assert_eq!(
        reg.apply_unary(UnaryOp::Positive, Float8(3.5)).unwrap(),
        Float8(3.5)
    );
    assert_eq!(
        reg.apply_unary(UnaryOp::Not, Bool(true)).unwrap(),
        Bool(false)
    );
    assert_eq!(reg.apply_unary(UnaryOp::Not, Null).unwrap(), Null);
}
