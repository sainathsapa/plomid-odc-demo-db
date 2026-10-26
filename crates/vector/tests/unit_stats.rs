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
// Moved from `src/stats.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::stats::*;

#[test]
fn unknown_is_conservative() {
    let s = VectorStats::unknown(128);
    assert!(!s.is_observed());
    assert!(!s.has_selectivity());
    assert!(!s.has_recall());
    assert!(!s.is_fresh(0));
    assert_eq!(s.null_fraction(), 0.0);
    assert_eq!(s.expected_rows(0.5), Some(0));
    assert_eq!(s.expected_rows(f64::NAN), None);
    assert_eq!(s.expected_rows(2.0), None);
}

#[test]
fn observation_and_fractions() {
    let mut s = VectorStats::unknown(4);
    s.observe_row(true, false, false);
    s.observe_row(false, true, false);
    s.observe_row(false, false, true);
    s.observe_row(false, false, true);
    assert_eq!((s.row_count, s.live_rows, s.null_count), (4, 3, 1));
    assert_eq!(s.zero_norm_count, 1);
    assert_eq!(s.indexed_rows, 2);
    assert!((s.null_fraction() - 0.25).abs() < 1e-12);
    assert!((s.zero_norm_fraction() - 1.0 / 3.0).abs() < 1e-12);
    assert!((s.indexed_fraction() - 2.0 / 3.0).abs() < 1e-12);
    assert_eq!(s.expected_rows(0.5), Some(1));
    s.build_watermark = 7;
    assert!(s.is_fresh(7));
    assert!(!s.is_fresh(8));
}

#[test]
fn selectivity_and_recall_validation() {
    let mut s = VectorStats::unknown(8);
    s.observe_selectivity(0.25);
    assert!(s.has_selectivity());
    s.observe_selectivity(f64::NAN);
    assert!(!s.has_selectivity());
    s.observe_selectivity(5.0);
    assert_eq!(s.selectivity, 1.0);
    s.observe_recall(0.97, 80);
    assert!(s.has_recall());
    s.observe_recall(1.5, 80);
    assert!(!s.has_recall());
    s.observe_recall(0.9, 0);
    assert!(!s.has_recall());
}

#[test]
fn codec_roundtrip_and_version_gate() {
    let mut s = VectorStats::unknown(1536);
    for _ in 0..100 {
        s.observe_row(false, false, true);
    }
    s.observe_selectivity(0.1);
    s.observe_recall(0.98, 160);
    s.build_watermark = 42;
    s.stats_tick = 3;
    let back = VectorStats::decode(&s.encode()).unwrap();
    assert_eq!(s, back);
    let mut bad = s.encode();
    bad[0..4].copy_from_slice(&(STATS_VERSION + 1).to_le_bytes());
    assert!(VectorStats::decode(&bad).is_err());
    assert!(VectorStats::decode(&bad[..bad.len() / 2]).is_err());
    let mut trailing = s.encode();
    trailing.push(0);
    assert!(VectorStats::decode(&trailing).is_err());
}
