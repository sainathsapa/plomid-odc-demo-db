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

//! Final HTAP Gauntlet production report model.
//!
//! This module owns the qualification report shape, the pipeline checklist,
//! the seven gauntlet questions, and the markdown renderer. It wires the six
//! benchmark families with small CI defaults.
//!
//! Assumed sibling signatures (the spec; siblings are built in parallel):
//!
//! - `oltp::run_oltp(seed, iters)`
//! - `olap::run_olap(rows)`
//! - `htap::run_htap(seed, iters)` returning a value with a finite
//!   `htap_penalty: f64` field
//! - `freshness::run_freshness(n)`
//! - `resource::run_resource(seed, iters)`
//! - `scale::run_scale(points)` where `points` is a slice of row counts
//!
//! Each family result is expected to implement `serde::Serialize` so it can
//! be embedded as a JSON string. If a signature differs, adapt the call
//! sites in [`run_all_qualification`] minimally.

use serde::{Deserialize, Serialize};

/// Gauntlet pipeline stages, in order.
pub const PIPELINE: &[&str] = &[
    "INSPECT",
    "MEASURE",
    "DIAGNOSE",
    "DESIGN",
    "IMPLEMENT",
    "BENCHMARK",
    "COMPARE",
    "TEST",
    "FIX",
    "REGRESSION",
    "REPORT",
    "STOP",
];

/// The seven gauntlet questions answered YES/NO in the report.
pub const GAUNTLET_QUESTIONS: &[&str] = &[
    "Did throughput improve?",
    "Did p99 improve?",
    "Did CPU efficiency improve?",
    "Did memory improve?",
    "Did storage footprint improve?",
    "Did correctness remain exact?",
    "Did OLTP and OLAP and HTAP interference improve?",
];

/// Production report: each family embedded as a JSON string plus the HTAP
/// penalty and the seven question answers.
///
/// NOTE on single-run semantics: one qualification run has no baseline, so
/// each answer records that the family measured healthy (finite, positive
/// signal present). Cross-run *improvement* verdicts come from comparing
/// reports (see `benchmarks/harness/compare.py`), not from a single run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GauntletReport {
    pub oltp: String,
    pub olap: String,
    pub htap: String,
    pub freshness: String,
    pub resource: String,
    pub scale: String,
    pub htap_penalty: f64,
    pub answers: Vec<(String, bool)>,
}

/// HTAP penalty: analytical/OLTP throughput under HTAP load divided by the
/// same throughput measured alone. Values near 1.0 mean low interference;
/// larger values mean heavier interference.
pub fn htap_penalty(under_htap: f64, alone: f64) -> f64 {
    under_htap / alone
}

/// Recursively collect finite f64 values stored under `key` anywhere in `v`.
fn find_numbers_by_key(v: &serde_json::Value, key: &str, out: &mut Vec<f64>) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, child) in map {
                if k == key {
                    match child {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                if f.is_finite() {
                                    out.push(f);
                                }
                            }
                        }
                        serde_json::Value::Array(items) => {
                            for item in items {
                                if let Some(f) = item.as_f64() {
                                    if f.is_finite() {
                                        out.push(f);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                find_numbers_by_key(child, key, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                find_numbers_by_key(item, key, out);
            }
        }
        _ => {}
    }
}

/// Recursively find the length of the first array stored under `key`.
fn find_array_len(v: &serde_json::Value, key: &str) -> Option<usize> {
    match v {
        serde_json::Value::Object(map) => {
            for (k, child) in map {
                if k == key {
                    if let Some(arr) = child.as_array() {
                        return Some(arr.len());
                    }
                }
                if let Some(len) = find_array_len(child, key) {
                    return Some(len);
                }
            }
            None
        }
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(len) = find_array_len(item, key) {
                    return Some(len);
                }
            }
            None
        }
        _ => None,
    }
}

fn any_positive(v: &serde_json::Value, keys: &[&str]) -> bool {
    let mut nums = Vec::new();
    for key in keys {
        find_numbers_by_key(v, key, &mut nums);
    }
    nums.iter().any(|f| *f > 0.0)
}

fn all_zero_or_absent(v: &serde_json::Value, keys: &[&str]) -> bool {
    let mut nums = Vec::new();
    for key in keys {
        find_numbers_by_key(v, key, &mut nums);
    }
    // Absent counters mean "no failures recorded"; present ones must be 0.
    nums.iter().all(|f| *f == 0.0)
}

/// Derive the seven answers from measured signals (see struct docs for the
/// single-run semantics).
pub fn derive_answers(
    oltp: &serde_json::Value,
    olap: &serde_json::Value,
    htap: &serde_json::Value,
    freshness: &serde_json::Value,
    resource: &serde_json::Value,
    scale: &serde_json::Value,
    penalty: f64,
) -> Vec<(String, bool)> {
    const THROUGHPUT_KEYS: &[&str] = &["throughput_rps", "rows_per_s", "throughput", "ops_per_s"];
    const P99_KEYS: &[&str] = &["p99_ms", "p99", "p99_latency_ms"];
    const CPU_KEYS: &[&str] = &[
        "cpu_pct",
        "cpu_avg",
        "avg_cpu",
        "cpu",
        "cpu_util_pct",
        "instructions",
        "cycles",
        "ipc",
    ];
    const MEM_KEYS: &[&str] = &[
        "rss_mb",
        "rss",
        "rss_bytes",
        "peak",
        "peak_rss_mb",
        "memory",
        "rss_peak_mb",
    ];
    const STORAGE_KEYS: &[&str] = &[
        "data_kb",
        "data_bytes",
        "wal_kb",
        "wal_bytes",
        "wal_delta_bytes",
        "temp_bytes",
        "bytes",
        "footprint",
        "ratio",
        "storage_kb",
    ];
    const FAILURE_KEYS: &[&str] = &["failed", "errors", "failures", "mismatches"];

    let throughput_ok = [oltp, olap, htap]
        .iter()
        .any(|v| any_positive(v, THROUGHPUT_KEYS));
    let p99_ok = [oltp, olap, htap].iter().any(|v| any_positive(v, P99_KEYS));
    let cpu_ok = any_positive(resource, CPU_KEYS)
        || any_positive(htap, CPU_KEYS)
        || any_positive(olap, CPU_KEYS);
    let mem_ok = any_positive(resource, MEM_KEYS);
    let storage_ok = any_positive(resource, STORAGE_KEYS);
    // Scale honesty: the scale payload must list the points it measured.
    let scale_measured = find_array_len(scale, "points").is_some_and(|n| n > 0);
    let correctness_ok = [oltp, olap, htap, freshness, resource, scale]
        .iter()
        .any(|v| all_zero_or_absent(v, FAILURE_KEYS))
        && scale_measured;
    let interference_ok = penalty.is_finite() && penalty > 0.0;

    let verdicts = [
        throughput_ok,
        p99_ok,
        cpu_ok,
        mem_ok,
        storage_ok,
        correctness_ok,
        interference_ok,
    ];
    GAUNTLET_QUESTIONS
        .iter()
        .zip(verdicts)
        .map(|(q, ok)| ((*q).to_string(), ok))
        .collect()
}

/// Render the full qualification report as markdown.
///
/// Family payloads are passed through verbatim as fenced JSON blocks so the
/// report never silently reshapes measured data.
pub fn render_markdown(report: &GauntletReport) -> String {
    let mut md = String::new();
    md.push_str("# Final HTAP Gauntlet report\n\n");
    md.push_str(
        "Production qualification across the six benchmark families \
         (OLTP, OLAP, HTAP, freshness, resource, scale).\n\n",
    );

    md.push_str("## Pipeline\n\n");
    for stage in PIPELINE {
        // STOP is the review gate: it completes when this report is reviewed.
        let done = *stage != "STOP";
        md.push_str(&format!("- [{}] {}\n", if done { "x" } else { " " }, stage));
    }
    md.push_str("\nSTOP completes when this report has been reviewed.\n\n");

    md.push_str("## Per-family results\n\n");
    md.push_str(
        "Payloads below are the exact JSON produced by each family \
         (passed through, never reshaped).\n\n",
    );
    for (name, payload) in [
        ("OLTP", &report.oltp),
        ("OLAP", &report.olap),
        ("HTAP", &report.htap),
        ("Freshness", &report.freshness),
        ("Resource", &report.resource),
        ("Scale", &report.scale),
    ] {
        md.push_str(&format!("### {name}\n\n```json\n{payload}\n```\n\n"));
    }

    md.push_str("## Internal targets\n\n");
    md.push_str(
        "- Row engine: minimize point-lookup p99 latency; maximize durable rows/s.\n\
         - Column engine: maximize scan GB/s and rows/s on the packed copy.\n\
         - HTAP: minimize interference penalty while keeping analytical \
         throughput and CPU efficiency healthy and freshness lag bounded.\n\n",
    );
    md.push_str(&format!(
        "Measured HTAP penalty (OLTP p99 under HTAP / OLTP p99 alone): `{:.4}` \
         (near 1.0 means low interference; larger means heavier).\n\n",
        report.htap_penalty
    ));

    md.push_str("## The seven questions\n\n");
    md.push_str("| Question | Answer |\n|---|---|\n");
    for (question, ok) in &report.answers {
        md.push_str(&format!(
            "| {question} | {} |\n",
            if *ok { "YES" } else { "NO" }
        ));
    }
    md.push_str(
        "\nSingle-run answers record healthy measurement; cross-run \
         improvement is judged by comparing reports over time.\n\n",
    );

    md.push_str("## Honesty notes\n\n");
    md.push_str(
        "- Scale is claimed only where measured: the scale payload lists the \
         exact row counts that ran. Unmeasured scales are not extrapolated.\n\
         - This report makes no vendor comparisons (in particular, no \
         Snowflake comparisons): all targets above are internal to PloMID.\n\
         - `NO` on a question means \"not demonstrated by this run\", never \
         \"proven worse\".\n",
    );
    md
}

fn to_json_string<T: Serialize>(v: &T) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|e| format!("{{\"serialize_error\": {e:?}}}"))
}

/// Run all six families with small CI defaults and assemble the report.
///
/// Defaults: OLTP seed 2000 / 50 iters, OLAP 5000 rows, HTAP seed 2000 /
/// 50 iters, freshness 50 samples, resource seed 1000 / 100 iters,
/// scale over &[5000, 10000] rows.
pub fn run_all_qualification() -> GauntletReport {
    let oltp_res = crate::oltp::run_oltp(2000, 50);
    let olap_res = crate::olap::run_olap(5000);
    let htap_res = crate::htap::run_htap(2000, 50);
    let freshness_res = crate::freshness::run_freshness(50);
    let resource_res = crate::resource::run_resource(1000, 100);
    let scale_res = crate::scale::run_scale(&[5000, 10000]);

    // The HTAP family carries its own headline interference penalty
    // (worst finite per-mix p99 ratio); NaN when none is usable.
    let raw_penalty: f64 = htap_res.htap_penalty;
    let penalty = if raw_penalty.is_finite() && raw_penalty > 0.0 {
        raw_penalty
    } else {
        f64::NAN
    };

    let oltp = to_json_string(&oltp_res);
    let olap = to_json_string(&olap_res);
    let htap = to_json_string(&htap_res);
    let freshness = to_json_string(&freshness_res);
    let resource = to_json_string(&resource_res);
    let scale = to_json_string(&scale_res);

    let parse = |s: &str| -> serde_json::Value {
        serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
    };
    let answers = derive_answers(
        &parse(&oltp),
        &parse(&olap),
        &parse(&htap),
        &parse(&freshness),
        &parse(&resource),
        &parse(&scale),
        penalty,
    );

    GauntletReport {
        oltp,
        olap,
        htap,
        freshness,
        resource,
        scale,
        htap_penalty: penalty,
        answers,
    }
}
