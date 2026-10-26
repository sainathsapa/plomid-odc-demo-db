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
// Moved from `src/jsonb.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::JsonbValue;

#[test]
fn parse_encode_decode_roundtrip() {
    let text = r#"{"b": [1, 2.5, null, true], "a": "x\"y", "b": 3}"#;
    let tree = JsonbValue::parse(text).unwrap();
    // Duplicate key: last wins. Keys sorted by length then lexicographic.
    assert_eq!(tree.to_text(), r#"{"a":"x\"y","b":3}"#);
    let bytes = tree.encode();
    assert_eq!(JsonbValue::decode(&bytes).unwrap(), tree);
    let a = JsonbValue::parse("1.0").unwrap();
    let b = JsonbValue::parse("1").unwrap();
    assert!(a.jsonb_eq(&b));
}

#[test]
fn containment() {
    let doc = JsonbValue::parse(r#"{"a": 1, "b": [2, 3]}"#).unwrap();
    assert!(doc.contains(&JsonbValue::parse(r#"{"a": 1}"#).unwrap()));
    assert!(!doc.contains(&JsonbValue::parse(r#"{"a": 2}"#).unwrap()));
    assert!(doc.contains(&JsonbValue::parse(r#"[3]"#).unwrap()));
}

#[test]
fn parse_errors() {
    assert!(JsonbValue::parse("{").is_err());
    assert!(JsonbValue::parse("[1,]").is_err());
    assert!(JsonbValue::parse("1 2").is_err());
    assert!(JsonbValue::parse("1e").is_err());
    assert!(JsonbValue::parse("tru").is_err());
}

#[test]
fn number_equality_does_not_round_large_values_through_f64() {
    let one = JsonbValue::parse("123456789012345678901234567890").unwrap();
    let two = JsonbValue::parse("123456789012345678901234567891").unwrap();
    let equivalent = JsonbValue::parse("1.23456789012345678901234567890e29").unwrap();
    assert!(!one.jsonb_eq(&two));
    assert!(one.jsonb_eq(&equivalent));
}
