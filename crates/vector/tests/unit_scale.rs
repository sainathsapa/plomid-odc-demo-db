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
// Moved from `src/scale.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::scale::*;
use plomid_vector::storage::StorageClass;

const GIB: u64 = 1024 * 1024 * 1024;

fn auto(cpus: usize, ram: Option<u64>, class: StorageClass, ds: u64, conc: usize) -> ScaleDecision {
    ScaleDecision::select(
        &ScaleInputs::test_seam(cpus, ram, class, ds, conc),
        &ScaleConfig::explicit(None),
    )
}

#[test]
fn laptop_workstation_small_production_large_cover_the_range() {
    assert_eq!(
        auto(2, Some(4 * GIB), StorageClass::Ssd, 100 << 20, 1).tier,
        ScaleTier::Laptop,
        "low-memory machine -> laptop"
    );
    assert_eq!(
        auto(32, Some(64 * GIB), StorageClass::Ssd, 1 << 30, 4).tier,
        ScaleTier::ProductionServer,
        "many-core machine -> production"
    );
    assert_eq!(
        auto(8, Some(256 * GIB), StorageClass::Nvme, 200 * GIB, 8).tier,
        ScaleTier::LargeMemoryHighIo,
        "high-memory machine -> large-memory tier"
    );
    // Slow storage never promotes past workstation on mid iron.
    let slow = auto(8, Some(16 * GIB), StorageClass::Hdd, 1 << 30, 2);
    assert!(
        slow.tier == ScaleTier::SmallServer
            || slow.tier == ScaleTier::Workstation
            || slow.tier == ScaleTier::Laptop,
        "slow storage tier={:?}",
        slow.tier
    );
    // Fast storage on the same iron prefers mmap-capable tiers.
    let fast = auto(8, Some(16 * GIB), StorageClass::Nvme, 1 << 30, 2);
    assert!(fast.mmap_preferred);
}

#[test]
fn small_and_large_datasets_shift_within_reason() {
    let small = auto(8, Some(16 * GIB), StorageClass::Ssd, 10 << 20, 1);
    let large = auto(8, Some(16 * GIB), StorageClass::Ssd, 200 * GIB, 1);
    // Dataset size alone never demotes below workstation on this iron,
    // but large datasets must not shrink batch/cache hints.
    assert!(large.batch_rows >= small.batch_rows);
    assert!(large.cache_bytes >= 32 * 1024 * 1024);
}

#[test]
fn explicit_override_wins_over_detection() {
    let inputs = ScaleInputs::test_seam(2, Some(4 * GIB), StorageClass::Hdd, 1 << 20, 1);
    let pinned = ScaleDecision::select(
        &inputs,
        &ScaleConfig::explicit(Some(ScaleTier::ProductionServer)),
    );
    assert_eq!(pinned.tier, ScaleTier::ProductionServer);
    assert!(pinned.overridden);
    assert!(pinned
        .explain()
        .iter()
        .any(|l| l.contains("Overridden: true")));
    // Auto on the same inputs stays laptop.
    let auto_d = ScaleDecision::select(&inputs, &ScaleConfig::explicit(None));
    assert_eq!(auto_d.tier, ScaleTier::Laptop);
    assert!(!auto_d.overridden);
}

#[test]
fn knobs_scale_monotonically() {
    let laptop = auto(2, Some(4 * GIB), StorageClass::Ssd, 1 << 20, 1);
    let prod = auto(32, Some(64 * GIB), StorageClass::Nvme, 10 * GIB, 8);
    let large = auto(64, Some(256 * GIB), StorageClass::Nvme, 200 * GIB, 16);
    assert!(laptop.workers <= prod.workers);
    assert!(prod.workers <= large.workers);
    assert!(laptop.memory_budget_bytes <= large.memory_budget_bytes);
    assert!(laptop.build_concurrency <= large.build_concurrency);
    assert!(laptop.maintenance_concurrency <= large.maintenance_concurrency);
}

#[test]
fn tier_names_parse() {
    assert_eq!(ScaleTier::parse("laptop").unwrap(), ScaleTier::Laptop);
    assert_eq!(
        ScaleTier::parse("WorkStation").unwrap(),
        ScaleTier::Workstation
    );
    assert_eq!(
        ScaleTier::parse("small-server").unwrap(),
        ScaleTier::SmallServer
    );
    assert_eq!(
        ScaleTier::parse("production_server").unwrap(),
        ScaleTier::ProductionServer
    );
    assert_eq!(
        ScaleTier::parse("large_memory_high_io").unwrap(),
        ScaleTier::LargeMemoryHighIo
    );
    assert!(ScaleTier::parse("cluster").is_err());
}

#[test]
fn storage_policy_follows_tier() {
    let laptop = auto(2, Some(4 * GIB), StorageClass::Hdd, 1 << 20, 1);
    assert!(!laptop.storage_policy().mmap.enabled);
    let large = auto(64, Some(256 * GIB), StorageClass::Nvme, 200 * GIB, 16);
    assert!(large.storage_policy().mmap.enabled);
}

#[test]
fn unknown_ram_never_assumes_large() {
    let d = auto(32, None, StorageClass::Unknown, 1 << 20, 1);
    assert_ne!(d.tier, ScaleTier::LargeMemoryHighIo);
    assert_ne!(d.tier, ScaleTier::ProductionServer);
}
