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
// Moved from `src/report.rs` inline `#[cfg(test)]` — public-API only.
use plomid_gauntlet::report::*;
use serde_json::json;

#[test]
fn answers_detect_live_family_signals() {
    let oltp =
        json!({"insert": {"throughput_rps": 100.0, "p99_ms": 2.0}, "correctness_rows": 2100});
    let olap = json!({"cpu_util_pct": 99.9, "peak_rss_mb": 0.0, "queries": []});
    let htap = json!({"htap_penalty": 1.4, "penalties": [["70/20/10", 1.4]]});
    let freshness = json!({"p99_ms": 1.0, "samples": 50});
    let resource = json!({"before": {"rss_bytes": 1, "data_bytes": 1}, "wal_delta_bytes": 0});
    let scale = json!({"points": [{"rows": 5000}, {"rows": 10000}]});
    let answers = derive_answers(&oltp, &olap, &htap, &freshness, &resource, &scale, 1.4);
    assert_eq!(answers.len(), GAUNTLET_QUESTIONS.len());
    for (q, ok) in &answers {
        assert!(*ok, "expected YES for live signals: {q}");
    }
}
