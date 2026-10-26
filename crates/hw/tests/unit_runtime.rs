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
// Moved from `src/runtime.rs` inline `#[cfg(test)]` — public-API only.
use plomid_hw::profile::{Architecture, CpuFeatures, HardwareProfile, StorageKind};
use plomid_hw::runtime::*;

fn mock_hardware(cpus: usize, ram: Option<u64>, storage: StorageKind) -> HardwareProfile {
    HardwareProfile {
        arch: Architecture::X86_64,
        cpu: CpuFeatures {
            scalar: true,
            sse42: false,
            avx2: false,
            avx512: false,
            neon: false,
        },
        logical_cpus: cpus,
        physical_cpus: None,
        cache_bytes: [None, None, None],
        ram_total_bytes: ram,
        ram_available_bytes: None,
        storage,
        storage_bandwidth_bps: None,
        storage_iops_estimate: None,
        numa_nodes: None,
        notes: Vec::new(),
    }
}

const GIB: u64 = 1024 * 1024 * 1024;

fn classified(cpus: usize, ram: Option<u64>) -> RuntimeTier {
    RuntimeTier::classify(&mock_hardware(cpus, ram, StorageKind::Unknown))
}

#[test]
fn edge_is_small_device_boundary() {
    assert_eq!(classified(4, Some(4 * GIB)), RuntimeTier::Edge);
    assert_eq!(classified(2, Some(2 * GIB)), RuntimeTier::Edge);
    // Either side over the line escapes Edge.
    assert_eq!(classified(5, Some(4 * GIB)), RuntimeTier::Laptop);
    assert_eq!(classified(4, Some(8 * GIB)), RuntimeTier::Laptop);
    // Unknown RAM never forces the small profile (matches is_small_device).
    assert_eq!(classified(4, None), RuntimeTier::Laptop);
    assert_eq!(classified(1, None), RuntimeTier::Laptop);
}

#[test]
fn server_needs_both_cores_and_ram() {
    assert_eq!(classified(16, Some(32 * GIB)), RuntimeTier::Server);
    assert_eq!(classified(64, Some(256 * GIB)), RuntimeTier::Server);
    assert_eq!(classified(15, Some(64 * GIB)), RuntimeTier::Laptop);
    assert_eq!(classified(16, Some(16 * GIB)), RuntimeTier::Laptop);
    assert_eq!(classified(32, None), RuntimeTier::Laptop);
}

#[test]
fn laptop_is_the_middle_default() {
    assert_eq!(classified(8, Some(16 * GIB)), RuntimeTier::Laptop);
    assert_eq!(classified(8, None), RuntimeTier::Laptop);
}

#[test]
fn override_parsing_accepts_all_spellings() {
    assert_eq!(
        RuntimeTier::parse_override("edge"),
        Ok(Some(RuntimeTier::Edge))
    );
    assert_eq!(
        RuntimeTier::parse_override("LAPTOP"),
        Ok(Some(RuntimeTier::Laptop))
    );
    assert_eq!(
        RuntimeTier::parse_override(" Server "),
        Ok(Some(RuntimeTier::Server))
    );
    assert_eq!(RuntimeTier::parse_override("auto"), Ok(None));
    assert_eq!(RuntimeTier::parse_override("AUTO"), Ok(None));
    assert_eq!(RuntimeTier::parse_override(""), Ok(None));
    assert!(RuntimeTier::parse_override("turbo").is_err());
    assert!(RuntimeTier::parse_override("med").is_err());
}

#[test]
fn explicit_constructor_pins_tier_for_tests() {
    let hw = mock_hardware(64, Some(256 * GIB), StorageKind::Nvme);
    let pinned = RuntimeProfile::with_tier(RuntimeTier::Edge, hw, ProfileSource::Explicit);
    assert_eq!(pinned.tier(), RuntimeTier::Edge);
    assert_eq!(pinned.source(), ProfileSource::Explicit);
    // ...while classification of the same hardware says Server.
    let auto = RuntimeProfile::from_hardware(
        mock_hardware(64, Some(256 * GIB), StorageKind::Nvme),
        ProfileSource::Auto,
    );
    assert_eq!(auto.tier(), RuntimeTier::Server);
}

#[test]
fn worker_permits_derive_from_measured_budget() {
    let permits = |tier, cpus, ram| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(cpus, ram, StorageKind::Unknown),
            ProfileSource::Explicit,
        )
        .worker_permits()
    };
    assert_eq!(permits(RuntimeTier::Edge, 4, Some(4 * GIB)), 2);
    assert_eq!(permits(RuntimeTier::Edge, 1, Some(GIB)), 1);
    assert_eq!(permits(RuntimeTier::Laptop, 12, Some(16 * GIB)), 8);
    assert_eq!(permits(RuntimeTier::Laptop, 4, Some(8 * GIB)), 4);
    assert_eq!(permits(RuntimeTier::Server, 32, Some(64 * GIB)), 32);
}

#[test]
fn server_permits_scale_past_64_on_big_iron() {
    let permits = |tier, cpus, ram| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(cpus, ram, StorageKind::Unknown),
            ProfileSource::Explicit,
        )
        .worker_permits()
    };
    // Full logical CPUs through the scaled budget: 128c/512GiB fans out
    // 128-wide where the old 64-clamp stopped.
    assert_eq!(permits(RuntimeTier::Server, 128, Some(512 * GIB)), 128);
    assert_eq!(permits(RuntimeTier::Server, 64, Some(128 * GIB)), 64);
    // Unknown RAM stays conservative even when pinned to Server.
    assert_eq!(permits(RuntimeTier::Server, 128, None), 32);
}

#[test]
fn edge_and_laptop_rails_hold_on_big_iron() {
    let permits = |tier, cpus, ram| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(cpus, ram, StorageKind::Unknown),
            ProfileSource::Explicit,
        )
        .worker_permits()
    };
    // The tier rails bind, not the budget: a 128-wide budget still
    // yields 2 on Edge and 8 on Laptop.
    assert_eq!(permits(RuntimeTier::Edge, 128, Some(512 * GIB)), 2);
    assert_eq!(permits(RuntimeTier::Laptop, 128, Some(512 * GIB)), 8);
    assert_eq!(permits(RuntimeTier::Laptop, 16, Some(16 * GIB)), 8);
}

#[test]
fn laptop_pool_pages_match_the_historical_default() {
    // 16 GiB laptop: buffer = min(256MiB, 2GiB) = 256 MiB → /16 = 16 MiB → 1024 pages.
    let laptop = RuntimeProfile::from_hardware(
        mock_hardware(8, Some(16 * GIB), StorageKind::Ssd),
        ProfileSource::Auto,
    );
    assert_eq!(laptop.tier(), RuntimeTier::Laptop);
    assert_eq!(laptop.buffer_capacity_bytes(), 256 * 1024 * 1024);
    assert_eq!(laptop.pool_capacity_pages(), 1024);
    // Edge shrinks, server grows.
    let edge = RuntimeProfile::from_hardware(
        mock_hardware(4, Some(4 * GIB), StorageKind::Unknown),
        ProfileSource::Auto,
    );
    assert_eq!(edge.pool_capacity_pages(), 256);
    let server = RuntimeProfile::from_hardware(
        mock_hardware(32, Some(128 * GIB), StorageKind::Nvme),
        ProfileSource::Auto,
    );
    assert_eq!(server.pool_capacity_pages(), 4096);
}

#[test]
fn compression_and_maintenance_match_the_tier_table() {
    let profile = |tier, storage| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(8, Some(16 * GIB), storage),
            ProfileSource::Explicit,
        )
    };
    assert!(profile(RuntimeTier::Edge, StorageKind::Unknown).prefer_compression());
    assert!(profile(RuntimeTier::Server, StorageKind::Nvme).prefer_compression());
    assert!(!profile(RuntimeTier::Laptop, StorageKind::Ssd).prefer_compression());
    assert!(profile(RuntimeTier::Laptop, StorageKind::Hdd).prefer_compression());
    assert_eq!(
        profile(RuntimeTier::Edge, StorageKind::Ssd).maintenance_scale(),
        2.0
    );
    assert_eq!(
        profile(RuntimeTier::Laptop, StorageKind::Ssd).maintenance_scale(),
        1.0
    );
    assert_eq!(
        profile(RuntimeTier::Server, StorageKind::Ssd).maintenance_scale(),
        0.5
    );
}

#[test]
fn parallel_threshold_falls_as_permits_rise() {
    let threshold = |tier, cpus, ram| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(cpus, ram, StorageKind::Unknown),
            ProfileSource::Explicit,
        )
        .parallel_row_threshold()
    };
    let edge = threshold(RuntimeTier::Edge, 4, Some(4 * GIB));
    let laptop = threshold(RuntimeTier::Laptop, 8, Some(16 * GIB));
    let server = threshold(RuntimeTier::Server, 32, Some(64 * GIB));
    assert_eq!(edge, 65_536);
    assert_eq!(laptop, 16_384);
    assert_eq!(server, 4096);
    assert!(edge > laptop && laptop > server);
}

#[test]
fn vector_batch_stays_l2_sized_except_on_edge() {
    // Unknown L2 → 256 KiB fallback band → 8192 rows.
    let rows = |tier| {
        RuntimeProfile::with_tier(
            tier,
            mock_hardware(8, Some(16 * GIB), StorageKind::Unknown),
            ProfileSource::Explicit,
        )
        .vector_batch_rows()
    };
    assert_eq!(rows(RuntimeTier::Laptop), 8192);
    assert_eq!(rows(RuntimeTier::Server), 8192);
    assert_eq!(rows(RuntimeTier::Edge), 4096);
}

#[test]
fn recheck_with_auto_reclassifies_on_mock_hardware() {
    // Auto source follows the hardware: Pi-class -> Server-class flips
    // Edge -> Server, and back.
    let mut auto = RuntimeProfile::from_hardware(
        mock_hardware(4, Some(4 * GIB), StorageKind::Unknown),
        ProfileSource::Auto,
    );
    assert_eq!(auto.tier(), RuntimeTier::Edge);
    assert!(auto.recheck_with(mock_hardware(32, Some(64 * GIB), StorageKind::Unknown)));
    assert_eq!(auto.tier(), RuntimeTier::Server);
    assert!(auto.recheck_with(mock_hardware(4, Some(4 * GIB), StorageKind::Unknown)));
    assert_eq!(auto.tier(), RuntimeTier::Edge);
    // Same-class hardware reports no change.
    assert!(!auto.recheck_with(mock_hardware(2, Some(2 * GIB), StorageKind::Unknown)));
    assert_eq!(auto.tier(), RuntimeTier::Edge);
}

#[test]
fn recheck_with_keeps_explicit_and_cli_pins() {
    // Explicit / Cli / Env pins keep their tier while the hardware still
    // swaps underneath (the elastic cap follows it downstream).
    for source in [
        ProfileSource::Explicit,
        ProfileSource::Cli,
        ProfileSource::Env,
    ] {
        let mut pinned = RuntimeProfile::with_tier(
            RuntimeTier::Edge,
            mock_hardware(4, Some(4 * GIB), StorageKind::Unknown),
            source,
        );
        assert!(!pinned.recheck_with(mock_hardware(32, Some(64 * GIB), StorageKind::Unknown)));
        assert_eq!(pinned.tier(), RuntimeTier::Edge, "source={source}");
        assert_eq!(
            pinned.hardware().logical_cpus,
            32,
            "hardware still refreshes"
        );
        assert_eq!(pinned.source(), source);
    }
}
