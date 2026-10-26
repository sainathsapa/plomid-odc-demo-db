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
// Moved from `src/path.rs` inline `#[cfg(test)]` — public-API only.
use plomid_json::path::*;
use plomid_types::jsonb::JsonbValue;

fn doc(text: &str) -> JsonbValue {
    JsonbValue::parse(text).expect("valid json")
}

fn ev(root: &JsonbValue, path: &str) -> Vec<JsonbValue> {
    let expr = parse(path).unwrap_or_else(|err| {
        panic!("path should parse: {path}: {err:?}");
    });

    let ctx = JsonpathContext {
        root,
        outer: None,
        vars: &Vec::<(String, JsonbValue)>::new(),
        mode: JsonpathMode::Lax,
    };

    evaluate(&expr, root, &ctx).unwrap_or_else(|err| {
        panic!("path should evaluate: {path}: {err:?}");
    })
}

fn numbers(vals: Vec<JsonbValue>) -> Vec<String> {
    vals.iter()
        .map(|v| match v {
            JsonbValue::Number(n) => n.clone(),
            other => panic!("expected number, got {other:?}"),
        })
        .collect()
}

fn strings(vals: Vec<JsonbValue>) -> Vec<String> {
    vals.iter()
        .map(|v| match v {
            JsonbValue::String(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        })
        .collect()
}

#[test]
fn parses_chained_member_paths() {
    let d = doc(r#"{"a":{"b":{"c":1}}}"#);

    assert_eq!(numbers(ev(&d, "$.a.b.c")), vec!["1"]);

    assert_eq!(ev(&d, "$.a.b"), vec![doc(r#"{"c":1}"#)]);
}

#[test]
fn parses_chained_iterate_paths() {
    // Regression: `$.a[*]` previously stopped after `.a`.
    let d = doc("{\"a\":[1,2]}");
    assert_eq!(numbers(ev(&d, "$.a[*]")), vec!["1", "2"]);
    // Chained member then iterate.
    let d2 = doc("{\"x\":{\"a\":[3,4]}}");
    assert_eq!(numbers(ev(&d2, "$.x.a[*]")), vec!["3", "4"]);
}

#[test]
fn parses_method_calls_size_and_type() {
    // Regression: `$.size()` / `$.type()` previously left a trailing `(`.
    let d = doc("[1,2,3]");
    let sizes = ev(&d, "$.size()");
    assert_eq!(sizes.len(), 1);
    assert_eq!(sizes[0], JsonbValue::Number("3".to_string()));
    // `$.type()` on an array reports the jsonb type name.
    assert_eq!(
        ev(&d, "$.type()"),
        vec![JsonbValue::String("array".to_string())]
    );
    // Method applied to an object size().
    let obj = doc("{\"a\":1,\"b\":2}");
    assert_eq!(
        ev(&obj, "$.size()"),
        vec![JsonbValue::Number("2".to_string())]
    );
}
// AST-structure helpers: walk the parse tree and collect variant names
// so that regression tests can assert on the structure (e.g. that the
// stand-alone `? (predicate)` form yields `PostfixFilter`, not `Filter`).
fn walk(expr: &JsonpathExpr, out: &mut Vec<&'static str>) {
    let name = match expr {
        JsonpathExpr::Root => "Root",
        JsonpathExpr::Current => "Current",
        JsonpathExpr::Key(_) => "Key",
        JsonpathExpr::DotStar => "DotStar",
        JsonpathExpr::Iterate => "Iterate",
        JsonpathExpr::Index(_) => "Index",
        JsonpathExpr::RecursiveDescent => "RecursiveDescent",
        JsonpathExpr::Filter(inner) => {
            walk(inner, out);
            "Filter"
        }
        JsonpathExpr::PostfixFilter(inner) => {
            walk(inner, out);
            "PostfixFilter"
        }
        JsonpathExpr::Size => "Size",
        JsonpathExpr::TypeOf => "TypeOf",
        JsonpathExpr::Keyvalue => "Keyvalue",
        JsonpathExpr::StartsWith(_) => "StartsWith",
        JsonpathExpr::LikeRegex { .. } => "LikeRegex",
        JsonpathExpr::FlagCheck { .. } => "FlagCheck",
        JsonpathExpr::Datetime => "Datetime",
        JsonpathExpr::Compare { .. } => "Compare",
        JsonpathExpr::Arith { .. } => "Arith",
        JsonpathExpr::Literal(_) => "Literal",
        JsonpathExpr::Variable(_) => "Variable",
        JsonpathExpr::Method { .. } => "Method",
        JsonpathExpr::Path(steps) => {
            for s in steps {
                walk(s, out);
            }
            "Path"
        }
        JsonpathExpr::Exists(inner) => {
            walk(inner, out);
            "Exists"
        }
        JsonpathExpr::Not(inner) => {
            walk(inner, out);
            "Not"
        }
        JsonpathExpr::Logical { left, right, .. } => {
            walk(left, out);
            walk(right, out);
            "Logical"
        }
        JsonpathExpr::Any { expr, right, .. } => {
            walk(expr, out);
            walk(right, out);
            "Any"
        }
    };
    out.push(name);
}

fn tree_walk(expr: &JsonpathExpr) -> Vec<String> {
    let mut out = Vec::new();
    walk(expr, &mut out);
    out.into_iter().map(|s| s.to_string()).collect()
}

fn contains(names: &[String], s: &str) -> bool {
    names.iter().any(|x| x == s)
}

#[test]
fn parses_filters() {
    let d = doc("{\"items\":[1,2,3,4]}");
    assert_eq!(
        numbers(ev(&d, "$.items[*] ? (@ >= 2)")),
        vec!["2", "3", "4"]
    );
    assert_eq!(
        numbers(ev(&d, "$.items[*] ? (@ >= 2 && @ <= 3)")),
        vec!["2", "3"]
    );
    let d2 = doc("{\"customers\":[{\"active\":true,\"id\":1},{\"active\":false,\"id\":2}]}");
    assert_eq!(ev(&d2, "$.customers[*] ? (@.active == true)").len(), 1);
    let d3 = doc("{\"a\":1,\"b\":5}");
    assert_eq!(numbers(ev(&d3, "$.* ? (@ > 1)")), vec!["5"]);
}

#[test]
fn postfix_filter_yields_postfixfilter_in_tree() {
    // `$.items[*] ? (@ >= 2)` must contain PostfixFilter, NOT Filter.
    let expr = parse("$.items[*] ? (@ >= 2)").expect("should parse");
    let names = tree_walk(&expr);
    assert!(
        contains(&names, "PostfixFilter"),
        "expected PostfixFilter in {:?}",
        names
    );
    assert!(contains(&names, "Root"));
    assert!(contains(&names, "Key"));
    assert!(contains(&names, "Iterate"));
    assert!(contains(&names, "Compare"));
    assert!(!contains(&names, "Filter"));
}

#[test]
fn bracket_filter_yields_filter_in_tree() {
    // `$.items[?(@ >= 2)]` (bracket form) must still be a Filter
    // applied to the container elements — distinct from PostfixFilter.
    let expr = parse("$.items[?(@ >= 2)]").unwrap_or_else(|err| {
        panic!("should parse: {err:?}");
    });
    let names = tree_walk(&expr);
    assert!(
        contains(&names, "Filter"),
        "expected Filter (bracket form) in {:?}",
        names
    );
    assert!(contains(&names, "Key"));
    assert!(contains(&names, "Compare"));
    assert!(!contains(&names, "PostfixFilter"));
}

#[test]
fn chained_postfix_filter_structure() {
    let expr = parse("$.a[*] ? (@ > 1)").expect("should parse");
    let names = tree_walk(&expr);
    assert!(contains(&names, "Path"));
    assert!(contains(&names, "Root"));
    assert!(contains(&names, "Key"));
    assert!(contains(&names, "Iterate"));
    assert!(contains(&names, "PostfixFilter"));
    assert!(contains(&names, "Compare"));
    assert!(!contains(&names, "Filter"));
}

#[test]
fn method_call_structure_is_method_not_postfix() {
    let expr = parse("$.size()").unwrap_or_else(|err| {
        panic!("should parse: {err:?}");
    });
    let names = tree_walk(&expr);
    assert!(contains(&names, "Method"));
    assert!(!contains(&names, "PostfixFilter"));
    assert!(!contains(&names, "Filter"));
}

#[test]
fn current_single_element_filter_on_nested_object() {
    let d =
        doc("{\"customers\":[{\"name\":\"a\",\"active\":true},{\"name\":\"b\",\"active\":false}]}");
    let res = ev(&d, "$.customers[*] ? (@.active == true)");
    assert_eq!(res.len(), 1, "only the active customer should remain");
    assert!(
        matches!(res[0], JsonbValue::Object(ref pairs) if pairs.iter().any(|(k, _)| k == "name"))
    );
}

#[test]
fn postfix_filter_string_equality() {
    let d = doc(r#"["a","b","c"]"#);
    assert_eq!(strings(ev(&d, r#"$[*] ? (@ == "b")"#)), vec!["b"]);
}
