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
// Moved from `src/config.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::config::*;

#[test]
fn zero_knobs_clamp_instead_of_panicking() {
    let cfg = StreamConfig::new()
        .with_batch_size(0)
        .with_max_buffered_batches(0);
    assert_eq!(cfg.batch_size(), 1);
    assert_eq!(cfg.max_buffered_batches(), 1);
    assert_eq!(cfg.effective_capacity::<u64>(), 1);
}

#[test]
fn byte_budget_tightens_capacity() {
    let cfg = StreamConfig::new()
        .with_batch_size(1000)
        .with_max_buffered_batches(8)
        // 8-byte rows, 1000 per batch => ~8KB per batch; 20KB budget => 2.
        .with_buffer_budget_bytes(20 * 1024);
    assert_eq!(cfg.effective_capacity::<u64>(), 2);
}

#[test]
fn byte_budget_never_drops_below_one() {
    let cfg = StreamConfig::new()
        .with_batch_size(1000)
        .with_max_buffered_batches(8)
        .with_buffer_budget_bytes(1);
    assert_eq!(cfg.effective_capacity::<u64>(), 1);
}
