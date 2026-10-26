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
// Moved from `src/buffer_manager.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::buffer_manager::CacheClass as C;
use plomid_htap::buffer_manager::*;
use plomid_htap::PressureState;

/// Serializes the env-mutating sizing test against the singleton-sizing
/// test: the singleton initializes once from the live env, so the env
/// window and the init must never interleave.
static SIZING_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn manager() -> UnifiedBufferManager {
    UnifiedBufferManager::new(1000, 400)
}
fn put(m: &UnifiedBufferManager, k: &str, c: CacheClass, b: u64, hint: Option<bool>) {
    assert!(m.admit(k, c, b, hint).is_admitted(), "admit {k}");
}

/// Explicit profile for tier-sizing tests (mirrors the `plomid-hw`
/// seam-test mocks): unknown L2, the tier's canonical CPUs/RAM.
fn profile_for(tier: plomid_hw::RuntimeTier) -> plomid_hw::RuntimeProfile {
    use plomid_hw::{Architecture, CpuFeatures, HardwareProfile, ProfileSource, StorageKind};
    let (cpus, ram) = match tier {
        plomid_hw::RuntimeTier::Edge => (4, 4 * 1024 * 1024 * 1024),
        plomid_hw::RuntimeTier::Laptop => (12, 16 * 1024 * 1024 * 1024),
        plomid_hw::RuntimeTier::Server => (32, 64 * 1024 * 1024 * 1024),
    };
    plomid_hw::RuntimeProfile::with_tier(
        tier,
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
            ram_total_bytes: Some(ram),
            ram_available_bytes: None,
            storage: StorageKind::Unknown,
            storage_bandwidth_bps: None,
            storage_iops_estimate: None,
            numa_nodes: None,
            notes: Vec::new(),
        },
        ProfileSource::Explicit,
    )
}

#[test]
fn tiered_sizing_pins_laptop_to_historical_defaults() {
    use plomid_hw::RuntimeTier;
    // Hold the sizing serial across the env window below.
    let _serial = SIZING_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // Edge shrinks to its 64 MiB cap (16 MiB reserve); Laptop pins the
    // historical 256 MiB / 64 MiB pair; Server is capped back to it so
    // the cache cannot exceed the memory crate's OLAP budget.
    assert_eq!(
        buffer_sizing_for_profile(&profile_for(RuntimeTier::Edge)),
        (64 * 1024 * 1024, 16 * 1024 * 1024)
    );
    assert_eq!(
        buffer_sizing_for_profile(&profile_for(RuntimeTier::Laptop)),
        (
            GLOBAL_BUFFER_CAPACITY_BYTES,
            GLOBAL_BUFFER_OLTP_RESERVE_BYTES
        )
    );
    assert_eq!(
        buffer_sizing_for_profile(&profile_for(RuntimeTier::Server)),
        (
            GLOBAL_BUFFER_CAPACITY_BYTES,
            GLOBAL_BUFFER_OLTP_RESERVE_BYTES
        )
    );
    assert_eq!(GLOBAL_BUFFER_CAPACITY_BYTES, 256 * 1024 * 1024);
    assert_eq!(GLOBAL_BUFFER_OLTP_RESERVE_BYTES, 64 * 1024 * 1024);
    // Explicit env configuration wins over the derived default (and a
    // reserve above capacity clamps instead of inverting the fence).
    let saved_cap = std::env::var("PLOMID_BUFFER_CAPACITY_BYTES").ok();
    let saved_reserve = std::env::var("PLOMID_BUFFER_OLTP_RESERVE_BYTES").ok();
    std::env::set_var("PLOMID_BUFFER_CAPACITY_BYTES", "134217728");
    std::env::remove_var("PLOMID_BUFFER_OLTP_RESERVE_BYTES");
    let (capacity, reserve) = global_buffer_sizing();
    assert_eq!(capacity, 128 * 1024 * 1024);
    assert_eq!(
        reserve,
        32 * 1024 * 1024,
        "reserve stays 1/4 without override"
    );
    std::env::set_var("PLOMID_BUFFER_OLTP_RESERVE_BYTES", "4294967296");
    assert_eq!(
        global_buffer_sizing(),
        (128 * 1024 * 1024, 128 * 1024 * 1024),
        "reserve clamps to capacity"
    );
    match saved_cap {
        Some(value) => std::env::set_var("PLOMID_BUFFER_CAPACITY_BYTES", value),
        None => std::env::remove_var("PLOMID_BUFFER_CAPACITY_BYTES"),
    }
    match saved_reserve {
        Some(value) => std::env::set_var("PLOMID_BUFFER_OLTP_RESERVE_BYTES", value),
        None => std::env::remove_var("PLOMID_BUFFER_OLTP_RESERVE_BYTES"),
    }
}

#[test]
fn policy_name_and_pressure_targets() {
    let m = manager();
    assert_eq!(
        CachePolicy::name(&m as &dyn CachePolicy),
        "unified-workload-aware"
    );
    put(&m, "oltp/1", C::OltpHot, 400, None);
    put(&m, "olap/1", C::OlapChunk, 400, Some(true));
    // Occupancy 800: Warning 10% = 80, Pressure 25% = 200, Critical 50% = 400.
    assert_eq!(m.target_eviction_bytes(PressureState::Normal), 0);
    assert_eq!(m.target_eviction_bytes(PressureState::Warning), 80);
    assert_eq!(m.target_eviction_bytes(PressureState::Pressure), 200);
    assert_eq!(m.target_eviction_bytes(PressureState::Critical), 400);
}

#[test]
fn oltp_reserve_protected_from_olap_eviction() {
    let m = manager();
    put(&m, "oltp/1", C::OltpHot, 400, None);
    put(&m, "olap/1", C::OlapChunk, 300, Some(true));
    put(&m, "meta/1", C::Metadata, 200, None);
    assert_eq!(m.occupancy(), 900);
    // Pressure target = 225: must come from olap/meta, never OLTP reserve.
    let freed = m.evict_under_pressure(PressureState::Pressure);
    assert!(freed >= 225, "freed {freed}");
    assert!(m.contains("oltp/1"), "HOT OLTP must survive");
    assert_eq!(m.class_bytes(C::OltpHot), 400);
}

#[test]
fn hot_oltp_evictable_only_at_critical() {
    // Reserve 500: evicting either 400B HOT OLTP entry would dip below it.
    let m = UnifiedBufferManager::new(1000, 500);
    put(&m, "oltp/1", C::OltpHot, 400, None);
    put(&m, "oltp/2", C::OltpHot, 400, None);
    assert_eq!(m.evict_under_pressure(PressureState::Pressure), 0);
    assert_eq!(m.occupancy(), 800);
    // Critical may dip into the reserve: half of 800.
    assert_eq!(m.evict_under_pressure(PressureState::Critical), 400);
    assert_eq!(m.occupancy(), 400);
}

#[test]
fn streaming_bypass_occupies_nothing() {
    let m = manager();
    assert_eq!(
        m.admit("olap/scan-1", C::OlapChunk, 300, None),
        AdmitDecision::AdmitStreaming
    );
    assert_eq!(
        m.admit("tmp/sort-1", C::Temp, 100, None),
        AdmitDecision::AdmitStreaming
    );
    assert_eq!(m.occupancy(), 0);
    assert_eq!(m.len(), 0);
    assert_eq!(m.stats().streaming_bypass_count, 2);
    // Reuse hint admits as Warm instead of bypassing.
    put(&m, "olap/reused", C::OlapChunk, 200, Some(true));
    assert_eq!(m.occupancy(), 200);
    assert_eq!(m.temperature_of("olap/reused"), Some(Temperature::Warm));
}

#[test]
fn reuse_promotes_streaming_to_warm() {
    let m = manager();
    assert!(m
        .admit_with_temp("olap/c", C::OlapChunk, 100, Temperature::Streaming)
        .is_admitted());
    assert!(m.note_reuse("olap/c"));
    assert_eq!(m.temperature_of("olap/c"), Some(Temperature::Warm));
    // Hit-driven promotion: two hits Streaming -> Warm.
    assert!(m
        .admit_with_temp("olap/d", C::OlapChunk, 100, Temperature::Streaming)
        .is_admitted());
    assert!(m.record_hit("olap/d"));
    assert_eq!(m.temperature_of("olap/d"), Some(Temperature::Streaming));
    assert!(m.record_hit("olap/d"));
    assert_eq!(m.temperature_of("olap/d"), Some(Temperature::Warm));
    assert!(!m.record_hit("missing"));
    assert!(!m.note_reuse("missing"));
}

#[test]
fn eviction_order_is_cold_first() {
    let m = UnifiedBufferManager::new(1000, 0);
    put(&m, "oltp/hot", C::OltpHot, 400, None);
    put(&m, "olap/warm", C::OlapChunk, 300, Some(true));
    assert!(m
        .admit_with_temp("meta/cold", C::Metadata, 200, Temperature::Cold)
        .is_admitted());
    assert_eq!(m.occupancy(), 900);
    // Warning target = 90: cold (200B) covers it alone; warm/hot untouched.
    let freed = m.evict_under_pressure(PressureState::Warning);
    assert!(freed >= 90, "freed {freed}");
    assert!(!m.contains("meta/cold"), "cold must go first");
    assert!(m.contains("olap/warm"));
    assert!(m.contains("oltp/hot"));
}

#[test]
fn eviction_scales_with_pressure() {
    // Entry-granular: freed may overshoot the byte target by < 1 entry.
    let mut prev = 0u64;
    for pressure in [
        PressureState::Normal,
        PressureState::Warning,
        PressureState::Pressure,
        PressureState::Critical,
    ] {
        let m = UnifiedBufferManager::new(1000, 0);
        put(&m, "oltp/1", C::OltpHot, 8, None);
        put(&m, "olap/1", C::OlapChunk, 8, Some(true));
        put(&m, "meta/1", C::Metadata, 8, None);
        put(&m, "idx/1", C::Index, 8, None);
        let before = m.occupancy();
        let expected = before * eviction_pct(pressure) / 100;
        let freed = m.evict_under_pressure(pressure);
        if pressure == PressureState::Normal {
            assert_eq!(freed, 0);
        } else {
            assert!(
                freed >= expected,
                "{pressure:?}: freed {freed} < target {expected}"
            );
            assert!(freed < before, "must retain hot entries");
        }
        assert_eq!(m.occupancy(), before - freed);
        assert!(freed >= prev, "eviction must scale with pressure");
        prev = freed;
    }
}

#[test]
fn scan_bypass_heuristic() {
    use plomid_htap::UnifiedBufferManager as U;
    assert!(U::should_bypass_cache(C::Temp, 10, 0.99));
    assert!(U::should_bypass_cache(C::OlapChunk, 1_000_000, 0.1));
    assert!(U::should_bypass_cache(C::OlapChunk, 200_000, 0.7));
    assert!(!U::should_bypass_cache(C::OlapChunk, 100, 0.9));
    assert!(!U::should_bypass_cache(C::OltpHot, 1_000_000, 0.0));
    assert!(!U::should_bypass_cache(C::Index, 50, 0.0));
}

#[test]
fn global_instance_has_documented_sizing() {
    // Serialized against the env-mutating sizing test (see SIZING_SERIAL).
    let _serial = SIZING_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let global = global_buffer_manager();
    assert_eq!(global.capacity_bytes(), GLOBAL_BUFFER_CAPACITY_BYTES);
    assert_eq!(
        global.oltp_reserve_bytes(),
        GLOBAL_BUFFER_OLTP_RESERVE_BYTES
    );
    // Same instance on every call: the OLAP scan path shares one ledger.
    assert!(std::ptr::eq(global, global_buffer_manager()));
}
