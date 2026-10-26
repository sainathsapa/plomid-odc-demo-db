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
// Moved from `src/numa.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::numa::*;

#[test]
fn unavailable_topology_is_single_node() {
    let t = NumaTopology::test_seam(1, 8).unwrap();
    assert_eq!(t.mode(), NumaMode::Single);
    assert!(!t.is_multi_node());
    let p = NumaPolicy::select(&t, 16, 0);
    assert_eq!(p.mode, NumaMode::Single);
    assert!(
        !p.pinning,
        "pinning is a future extension point, never on today"
    );
    // Chunks cover every query exactly once, in order.
    let chunks = p.plan_chunks(16);
    let covered: usize = chunks.iter().map(|(s, e)| e - s).sum();
    assert_eq!(covered, 16);
    assert_eq!(chunks[0].0, 0);
}

#[test]
fn detected_multi_node_stripes() {
    let t = NumaTopology::test_seam(2, 16).unwrap();
    assert_eq!(t.mode(), NumaMode::Striped);
    assert!(t.is_multi_node());
    let p = NumaPolicy::select(&t, 64, 8);
    assert_eq!(p.workers, 8);
    assert_eq!(p.chunk_queries, 8);
    let lines = t.explain();
    assert!(lines.iter().any(|l| l.contains("striped")));
}

#[test]
fn invalid_topology_rejected() {
    assert!(NumaTopology::test_seam(0, 8).is_err());
    assert!(NumaTopology::test_seam(2048, 8).is_err());
}

#[test]
fn worker_shutdown_restart_sizes_deterministically() {
    // Shutdown (max_threads=1) then restart (max_threads=0) is a pure
    // function of inputs: same inputs always yield the same policy.
    let t = NumaTopology::test_seam(2, 16).unwrap();
    let stopped = NumaPolicy::select(&t, 32, 1);
    assert_eq!(stopped.workers, 1);
    let restarted = NumaPolicy::select(&t, 32, 0);
    assert_eq!(restarted, NumaPolicy::select(&t, 32, 0));
    assert!(restarted.workers >= stopped.workers);
}

#[test]
fn resource_limits_clamp_workers() {
    let t = NumaTopology::test_seam(4, 512).unwrap();
    let p = NumaPolicy::select(&t, 10_000, 10_000);
    assert!(p.workers <= 256, "safety ceiling holds on huge iron");
    let tiny = NumaPolicy::select(&t, 3, 64);
    assert_eq!(tiny.workers, 3, "never more workers than queries");
}

#[test]
fn concurrent_queries_chunk_cover_is_exact() {
    let t = NumaTopology::test_seam(2, 8).unwrap();
    for total in [0usize, 1, 7, 8, 9, 100] {
        let p = NumaPolicy::select(&t, total, 4);
        let chunks = p.plan_chunks(total);
        let mut next = 0usize;
        for (s, e) in chunks {
            assert_eq!(s, next);
            assert!(e > s);
            next = e;
        }
        assert_eq!(next, total);
    }
}

#[test]
fn detect_never_fails_and_explains() {
    let t = NumaTopology::detect();
    assert!(t.nodes >= 1);
    assert!(t.cpus >= 1);
    assert_eq!(t.explain().len(), 5);
}
