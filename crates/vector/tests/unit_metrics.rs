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
// Moved from `src/metrics.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::metrics::*;

#[test]
fn counters_snapshot() {
    let m = VectorMetrics::new();
    m.inc_exact();
    m.inc_ann();
    m.inc_hnsw();
    m.observe_build_latency_us(50);
    m.set_index_bytes(1024);
    m.set_tombstone_ratio_x1000(2000);
    let s = m.snapshot();
    assert_eq!((s.exact_queries, s.ann_queries, s.hnsw_queries), (1, 1, 1));
    assert_eq!(s.tombstone_ratio_x1000, 1000);
}
