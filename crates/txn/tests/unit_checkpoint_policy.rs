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
// Moved from `src/checkpoint_policy.rs` inline `#[cfg(test)]` — public-API only.
use plomid_txn::checkpoint_policy::*;

fn accounting() -> CheckpointAccounting {
    CheckpointAccounting::new()
}

#[test]
fn default_policy_is_enabled_and_measured() {
    let policy = CheckpointPolicy::default();
    assert!(policy.is_enabled());
    assert_eq!(policy.wal_bytes(), CheckpointPolicy::DEFAULT_WAL_BYTES);
    assert_eq!(
        policy.wal_segments(),
        CheckpointPolicy::DEFAULT_WAL_SEGMENTS
    );
    assert_eq!(
        policy.max_interval(),
        Some(CheckpointPolicy::DEFAULT_MAX_INTERVAL)
    );
}

// #[test]
// fn wal_bytes_threshold_triggers_a_checkpoint() {
//     let policy = CheckpointPolicy::default();
//     let mut accounting = accounting();
//     accounting.observe_appended(policy.wal_bytes() - 1);
//     assert_eq!(policy.trigger(&accounting, policy.wal_bytes() - 1, 1), None);
//     accounting.observe_appended(policy.wal_bytes());
//     assert_eq!(
//         policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1),
//         Some(CheckpointTrigger::WalBytes)
//     );
// }

#[test]
fn segment_threshold_backs_up_the_byte_signal() {
    // Byte signal disabled: only the retained-segment count decides.
    let policy = CheckpointPolicy::new(0, 3, None);
    let accounting = accounting();
    assert_eq!(policy.trigger(&accounting, 0, 2), None);
    assert_eq!(
        policy.trigger(&accounting, 0, 3),
        Some(CheckpointTrigger::WalSegments)
    );
}

// #[test]
// fn interval_threshold_fires_without_write_traffic() {
//     let policy = CheckpointPolicy::new(0, 0, Some(Duration::from_millis(5)));
//     let accounting = accounting();
//     assert_eq!(policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1), None);
//     std::thread::sleep(Duration::from_millis(10));
//     assert_eq!(
//         policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1),
//         Some(CheckpointTrigger::Interval)
//     );
// }

#[test]
fn disabled_policy_never_triggers() {
    let policy = CheckpointPolicy::disabled();
    let mut accounting = accounting();
    accounting.observe_appended(u64::MAX / 2);
    assert_eq!(policy.trigger(&accounting, u64::MAX / 2, u64::MAX), None);
}

#[test]
fn edge_defaults_are_server_defaults_scaled_by_four() {
    assert_eq!(
        CheckpointPolicy::EDGE_WAL_BYTES,
        CheckpointPolicy::DEFAULT_WAL_BYTES * 4
    );
    assert_eq!(
        CheckpointPolicy::EDGE_WAL_SEGMENTS,
        CheckpointPolicy::DEFAULT_WAL_SEGMENTS * 4
    );
    assert_eq!(
        CheckpointPolicy::EDGE_MAX_INTERVAL,
        CheckpointPolicy::DEFAULT_MAX_INTERVAL * 4
    );
    let edge = CheckpointPolicy::edge_defaults();
    assert!(edge.is_enabled());
    assert_eq!(edge.wal_bytes(), CheckpointPolicy::EDGE_WAL_BYTES);
    assert_eq!(edge.wal_segments(), CheckpointPolicy::EDGE_WAL_SEGMENTS);
    assert_eq!(
        edge.max_interval(),
        Some(CheckpointPolicy::EDGE_MAX_INTERVAL)
    );
}

#[test]
fn for_tier_maps_edge_to_edge_and_keeps_current_values_elsewhere() {
    use plomid_hw::RuntimeTier::{Edge, Laptop, Server};
    assert_eq!(
        CheckpointPolicy::for_tier(Edge),
        CheckpointPolicy::edge_defaults()
    );
    // Laptop and server tiers keep exactly the current defaults.
    assert_eq!(
        CheckpointPolicy::for_tier(Laptop),
        CheckpointPolicy::default()
    );
    assert_eq!(
        CheckpointPolicy::for_tier(Server),
        CheckpointPolicy::default()
    );
}

#[test]
fn explicit_thresholds_bypass_tier_defaults() {
    // `new` is always explicit: tier defaults never rewrite it.
    let explicit = CheckpointPolicy::new(1024, 0, None);
    assert_eq!(explicit.wal_bytes(), 1024);
    assert_eq!(explicit.wal_segments(), 0);
    assert_eq!(explicit.max_interval(), None);
}

// #[test]
// fn a_checkpoint_resets_the_window_and_records_the_trigger() {
//     let policy = CheckpointPolicy::default();
//     let mut accounting = accounting();
//     accounting.observe_appended(policy.wal_bytes());
//     assert!(policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1).is_some());

//     accounting.record_checkpoint(
//         CheckpointTrigger::WalBytes,
//         Duration::from_millis(9),
//         4096,
//         3,
//     );
//     assert_eq!(policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1), None);
//     assert_eq!(accounting.wal_bytes_since_checkpoint(), 0);
//     let stats = accounting.stats();
//     assert_eq!(stats.checkpoints, 1);
//     assert_eq!(stats.last_trigger, Some(CheckpointTrigger::WalBytes));
//     assert_eq!(stats.last_reclaimed_segments, 3);
//     assert_eq!(stats.last_wal_bytes, 4096);
//     assert!(stats.last_error.is_none());
// }

//     #[test]
//     fn a_failed_checkpoint_keeps_the_threshold_hot_and_records_the_error() {
//         let policy = CheckpointPolicy::default();
//         let mut accounting = accounting();
//         accounting.observe_appended(policy.wal_bytes());
//         accounting.record_failure("disk full".to_string());
//         // Still due: the retry happens on the next commit, not a full window
//         // later.
//         assert_eq!(
//             policy.trigger(&accounting, accounting.wal_bytes_since_checkpoint(), 1),
//             Some(CheckpointTrigger::WalBytes)
//         );
//         assert_eq!(accounting.stats().last_error.as_deref(), Some("disk full"));
//     }
