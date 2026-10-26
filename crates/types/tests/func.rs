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
// Moved from `src/func.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::builtin_functions;
use plomid_types::PgValue::*;

#[test]
fn lookup_and_call_lower() {
    let reg = builtin_functions();
    let res = reg
        .call("LOWER", vec![Text("HELLO".into())])
        .unwrap()
        .unwrap();
    assert_eq!(res, Text("hello".into()));
}

#[test]
fn strict_null_propagates() {
    let reg = builtin_functions();
    let res = reg.call("length", vec![Null]).unwrap().unwrap();
    assert_eq!(res, Null);
}

#[test]
fn concat_skips_nulls() {
    let reg = builtin_functions();
    let res = reg
        .call("concat", vec![Text("a".into()), Null, Text("b".into())])
        .unwrap()
        .unwrap();
    assert_eq!(res, Text("ab".into()));
}

#[test]
fn coalesce_returns_first_nonnull() {
    let reg = builtin_functions();
    let res = reg
        .call("coalesce", vec![Null, Int4(42), Int4(99)])
        .unwrap()
        .unwrap();
    assert_eq!(res, Int4(42));
}

#[test]
fn power_works() {
    let reg = builtin_functions();
    let res = reg.call("pow", vec![Int4(2), Int4(10)]).unwrap().unwrap();
    match res {
        Float8(v) => assert!((v - 1024.0).abs() < 0.001),
        _ => panic!("expected float8"),
    }
}

#[test]
fn trim_variants() {
    let reg = builtin_functions();
    let t = Text("  hi  ".into());
    let l = reg.call("ltrim", vec![t.clone()]).unwrap().unwrap();
    assert_eq!(l, Text("hi  ".into()));
    let r = reg.call("rtrim", vec![t.clone()]).unwrap().unwrap();
    assert_eq!(r, Text("  hi".into()));
    let b = reg.call("btrim", vec![t]).unwrap().unwrap();
    assert_eq!(b, Text("hi".into()));
}

#[test]
fn substring_basic() {
    let reg = builtin_functions();
    let res = reg
        .call("substr", vec![Text("abcdef".into()), Int4(2), Int4(3)])
        .unwrap()
        .unwrap();
    assert_eq!(res, Text("bcd".into()));
}

#[test]
fn position_basic_semantics() {
    let reg = builtin_functions();
    // 1-based position, 0 when absent, 1 for an empty substring.
    assert_eq!(
        reg.call("position", vec![Text("a".into()), Text("banana".into())])
            .unwrap()
            .unwrap(),
        Int4(2)
    );
    assert_eq!(
        reg.call("position", vec![Text("n".into()), Text("banana".into())])
            .unwrap()
            .unwrap(),
        Int4(3)
    );
    assert_eq!(
        reg.call("position", vec![Text("z".into()), Text("banana".into())])
            .unwrap()
            .unwrap(),
        Int4(0)
    );
    assert_eq!(
        reg.call("position", vec![Text("".into()), Text("banana".into())])
            .unwrap()
            .unwrap(),
        Int4(1)
    );
}

#[test]
fn position_null_propagates() {
    let reg = builtin_functions();
    // strict: a NULL argument yields a NULL result (never panics).
    assert_eq!(
        reg.call("position", vec![Null, Text("banana".into())])
            .unwrap()
            .unwrap(),
        Null
    );
    assert_eq!(
        reg.call("position", vec![Text("a".into()), Null])
            .unwrap()
            .unwrap(),
        Null
    );
}

#[test]
fn position_is_character_based_for_multibyte() {
    let reg = builtin_functions();
    // 'é' is two UTF-8 bytes. In 'élève' the 'l' is the 2nd character, so
    // position must be 2 (char-based); a byte-based calculation would give 3.
    let res = reg
        .call("position", vec![Text("l".into()), Text("élève".into())])
        .unwrap()
        .unwrap();
    assert_eq!(res, Int4(2));
}
