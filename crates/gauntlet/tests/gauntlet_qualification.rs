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

//! Final HTAP Gauntlet qualification (integration, scale-gated).
//!
//! Runs all six families with small CI defaults, checks correctness anchors,
//! and writes the markdown report to `target/gauntlet_report.md`.
//! Budget: well under ~120 s via the CI defaults in `run_all_qualification`.
//! Scale-gated (`#[ignore]`): live benchmark families never run in the
//! default unit loop — use `make gauntlet` or the nightly scale workflow.

use plomid_gauntlet::report::{render_markdown, run_all_qualification};
use serde_json::Value;

/// Recursively collect every value stored under `key`.
fn find_all<'a>(v: &'a Value, key: &str, out: &mut Vec<&'a Value>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                if k == key {
                    out.push(child);
                }
                find_all(child, key, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                find_all(item, key, out);
            }
        }
        _ => {}
    }
}

/// Largest finite positive number found under any of `keys` (0.0 if none).
fn max_positive(v: &Value, keys: &[&str]) -> f64 {
    let mut best = 0.0;
    let mut found = Vec::new();
    for key in keys {
        find_all(v, key, &mut found);
    }
    for item in found {
        match item {
            Value::Number(n) => {
                if let Some(f) = n.as_f64() {
                    if f.is_finite() && f > best {
                        best = f;
                    }
                }
            }
            Value::Array(items) => {
                for el in items {
                    if let Some(f) = el.as_f64() {
                        if f.is_finite() && f > best {
                            best = f;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    best
}

/// Length of the first array found under any of `keys`.
fn first_array_len(v: &Value, keys: &[&str]) -> Option<usize> {
    for key in keys {
        let mut found = Vec::new();
        find_all(v, key, &mut found);
        for item in found {
            if let Some(arr) = item.as_array() {
                return Some(arr.len());
            }
            // Some families may serialize samples as a count instead.
            if let Some(n) = item.as_u64() {
                return Some(n as usize);
            }
        }
    }
    None
}

#[test]
#[ignore = "scale: live benchmark families take minutes; runs in CI via make test-scale"]
fn final_htap_gauntlet_qualification() {
    let report = run_all_qualification();

    let oltp: Value = serde_json::from_str(&report.oltp).expect("oltp payload is JSON");
    let htap: Value = serde_json::from_str(&report.htap).expect("htap payload is JSON");
    let freshness: Value =
        serde_json::from_str(&report.freshness).expect("freshness payload is JSON");
    let scale: Value = serde_json::from_str(&report.scale).expect("scale payload is JSON");
    for (name, payload) in [("olap", &report.olap), ("resource", &report.resource)] {
        let v: Value =
            serde_json::from_str(payload).unwrap_or_else(|_| panic!("{name} payload is JSON"));
        assert!(
            v.is_object() || v.is_array(),
            "{name} payload is structured"
        );
    }

    // Correctness anchor: OLTP measured a positive row count.
    let oltp_count = max_positive(&oltp, &["count", "rows", "n", "total"]);
    assert!(oltp_count > 0.0, "oltp count>0, got {oltp_count}");

    // Correctness anchor: HTAP penalty is finite and positive, both on the
    // report field and inside the HTAP payload (either spelling).
    assert!(
        report.htap_penalty.is_finite() && report.htap_penalty > 0.0,
        "report htap_penalty finite >0, got {}",
        report.htap_penalty
    );
    let payload_penalty = max_positive(&htap, &["htap_penalty", "penalty", "interference"]);
    assert!(
        payload_penalty.is_finite() && payload_penalty > 0.0,
        "htap payload penalty finite >0, got {payload_penalty}"
    );

    // Correctness anchor: freshness recorded samples.
    let fresh_samples = first_array_len(&freshness, &["samples", "lags_ms", "lags"])
        .map(|len| len as f64)
        .unwrap_or_else(|| max_positive(&freshness, &["n", "count", "samples"]));
    assert!(fresh_samples > 0.0, "freshness samples>0");

    // Correctness anchor: scale ran exactly the 2 CI points.
    let scale_points = first_array_len(&scale, &["points", "rows", "results"])
        .unwrap_or_else(|| max_positive(&scale, &["n_points"]) as usize);
    assert_eq!(scale_points, 2, "scale points==2");

    // Render and persist the markdown report.
    let md = render_markdown(&report);
    assert!(md.contains("INSPECT"), "pipeline checklist present");
    assert!(md.contains("STOP"), "pipeline STOP present");
    for q in plomid_gauntlet::report::GAUNTLET_QUESTIONS {
        assert!(md.contains(q), "question present: {q}");
    }
    // Persist beside the workspace target dir (CWD here is the package dir).
    let out = format!(
        "{}/../../target/gauntlet_report.md",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::create_dir_all(
        std::path::Path::new(&out)
            .parent()
            .expect("report path has a parent"),
    )
    .expect("create target dir");
    std::fs::write(&out, &md).expect("write target/gauntlet_report.md");
}
