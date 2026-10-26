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
// Moved from `src/pool.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::pool::*;

#[test]
fn acquire_release_tracks_peak_and_saturates() {
    let pool = ResourcePool::new(PoolKind::Olap, 100, 0, 2, 8, 4);
    pool.acquire(60, 1);
    pool.acquire(30, 1);
    assert_eq!(pool.used_bytes(), 90);
    assert_eq!(pool.peak_bytes(), 90);
    assert_eq!(pool.available_bytes(), 10);
    assert_eq!(pool.utilization_pct(), 90);
    pool.release(200, 5); // saturates, never wraps
    assert_eq!(pool.used_bytes(), 0);
    assert_eq!(pool.in_use_slots(), 0);
    assert_eq!(pool.peak_bytes(), 90);
}
