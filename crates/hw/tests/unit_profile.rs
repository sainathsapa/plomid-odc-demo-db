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
// Moved from `src/profile.rs` inline `#[cfg(test)]` — public-API only.
use plomid_hw::profile::*;

fn mock(cpus: usize, ram: Option<u64>) -> HardwareProfile {
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
        storage: StorageKind::Unknown,
        storage_bandwidth_bps: None,
        storage_iops_estimate: None,
        numa_nodes: None,
        notes: Vec::new(),
    }
}

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn worker_budget_stays_small_on_pi_class() {
    // CPU term binds (4 < RAM term 128); the Edge tier caps permits at 2
    // downstream, so small boxes never fan out wide.
    assert_eq!(mock(4, Some(4 * GIB)).worker_budget(), 4);
    assert_eq!(mock(4, Some(GIB)).worker_budget(), 4);
    assert_eq!(mock(2, Some(2 * GIB)).worker_budget(), 2);
}

#[test]
fn worker_budget_scales_past_64_on_big_iron() {
    // Canonical 32c/64GiB server is unchanged (32); 64c keeps the old
    // 64; 128c/512GiB breaks the old rail with 128.
    assert_eq!(mock(32, Some(64 * GIB)).worker_budget(), 32);
    assert_eq!(mock(64, Some(128 * GIB)).worker_budget(), 64);
    assert_eq!(mock(128, Some(512 * GIB)).worker_budget(), 128);
}

#[test]
fn worker_budget_ram_binds_before_cpus_on_thin_boxes() {
    // 64c with 1 GiB: RAM term (1GiB/4/8MiB) = 32 < 64 CPUs.
    assert_eq!(mock(64, Some(GIB)).worker_budget(), 32);
}

#[test]
fn worker_budget_hits_the_safety_ceiling() {
    assert_eq!(
        mock(512, Some(8 * 1024 * GIB)).worker_budget(),
        MAX_WORKER_BUDGET
    );
    assert_eq!(
        mock(4096, Some(8 * 1024 * GIB)).worker_budget(),
        MAX_WORKER_BUDGET
    );
}

#[test]
fn worker_budget_unknown_ram_is_conservative() {
    // Unknown RAM never sizes fan-out blind: the RAM term falls back to
    // 32, matching the executor's process-wide unknown-RAM fallback.
    assert_eq!(mock(128, None).worker_budget(), 32);
    assert_eq!(mock(64, None).worker_budget(), 32);
    // Tiny boxes stay CPU-bound underneath the fallback.
    assert_eq!(mock(4, None).worker_budget(), 4);
}

#[test]
fn worker_budget_degenerate_inputs_fail_safe() {
    assert_eq!(mock(0, Some(16 * GIB)).worker_budget(), 1);
    assert_eq!(mock(1, Some(64 * 1024 * 1024)).worker_budget(), 1);
    assert_eq!(mock(2, Some(0)).worker_budget(), 1);
}

#[test]
fn cgroup_memory_limit_parsing_v1_and_v2() {
    // Real limits pass through (trailing newline tolerated).
    assert_eq!(
        parse_cgroup_memory_limit("1073741824\n"),
        Some(1_073_741_824)
    );
    assert_eq!(parse_cgroup_memory_limit("536870912"), Some(536_870_912));
    // v2 "max" (any case) = no limit.
    assert_eq!(parse_cgroup_memory_limit("max\n"), None);
    assert_eq!(parse_cgroup_memory_limit("MAX"), None);
    // Degenerate input = no limit, never a parse panic or zero.
    assert_eq!(parse_cgroup_memory_limit(""), None);
    assert_eq!(parse_cgroup_memory_limit("   \n"), None);
    assert_eq!(parse_cgroup_memory_limit("not-a-number"), None);
    assert_eq!(parse_cgroup_memory_limit("0"), None);
    // Absurd kernel sentinels (v1 LONG_MAX, page-rounded, u64::MAX, and
    // the exact 2^60 boundary) = no limit; 2^60 - 1 is still a limit.
    assert_eq!(parse_cgroup_memory_limit("9223372036854771712"), None);
    assert_eq!(parse_cgroup_memory_limit("9223372036854775807"), None);
    assert_eq!(parse_cgroup_memory_limit("18446744073709551615"), None);
    assert_eq!(parse_cgroup_memory_limit("1152921504606846976"), None);
    assert_eq!(
        parse_cgroup_memory_limit("1152921504606846975"),
        Some(1_152_921_504_606_846_975)
    );
}

#[test]
fn cgroup_ram_takes_the_minimum() {
    // Container cap below host RAM wins (the Docker `--memory` case).
    assert_eq!(
        effective_ram_total(Some(64 * GIB), Some(512 * 1024 * 1024)),
        Some(512 * 1024 * 1024)
    );
    // Cap above host RAM: host reading wins.
    assert_eq!(
        effective_ram_total(Some(512 * 1024 * 1024), Some(64 * GIB)),
        Some(512 * 1024 * 1024)
    );
    // Missing cgroup files → current behavior unchanged.
    assert_eq!(effective_ram_total(Some(8 * GIB), None), Some(8 * GIB));
    assert_eq!(effective_ram_total(None, None), None);
    // Missing meminfo with a present limit still reports the real bound.
    assert_eq!(effective_ram_total(None, Some(GIB)), Some(GIB));
}

#[test]
fn cgroup_cpu_v2_parsing() {
    assert_eq!(parse_cgroup_cpu_max("200000 100000"), Some(2.0));
    assert_eq!(parse_cgroup_cpu_max("50000 100000"), Some(0.5));
    assert_eq!(parse_cgroup_cpu_max("25000 100000\n"), Some(0.25));
    // Unset quota = no limit.
    assert_eq!(parse_cgroup_cpu_max("max 100000"), None);
    assert_eq!(parse_cgroup_cpu_max("MAX 100000"), None);
    // Degenerate input = no limit, never a panic or zero.
    assert_eq!(parse_cgroup_cpu_max(""), None);
    assert_eq!(parse_cgroup_cpu_max("200000"), None);
    assert_eq!(parse_cgroup_cpu_max("200000 0"), None);
    assert_eq!(parse_cgroup_cpu_max("-1 100000"), None);
    assert_eq!(parse_cgroup_cpu_max("lots 100000"), None);
}

#[test]
fn cgroup_cpu_v1_parsing() {
    assert_eq!(parse_cgroup_cpu_quota_v1("200000", "100000"), Some(2.0));
    assert_eq!(parse_cgroup_cpu_quota_v1("50000\n", "100000\n"), Some(0.5));
    // Quota -1 (any negative) = no limit.
    assert_eq!(parse_cgroup_cpu_quota_v1("-1", "100000"), None);
    // Degenerate input = no limit.
    assert_eq!(parse_cgroup_cpu_quota_v1("", "100000"), None);
    assert_eq!(parse_cgroup_cpu_quota_v1("200000", ""), None);
    assert_eq!(parse_cgroup_cpu_quota_v1("200000", "0"), None);
    assert_eq!(parse_cgroup_cpu_quota_v1("quota", "period"), None);
}

#[test]
fn cgroup_cpu_min_selection_with_floor() {
    // Quota below affinity wins (the Docker `--cpus` case).
    assert_eq!(effective_cpu_count(32, Some(2.0)), 2);
    // Quota above affinity: affinity wins, never exceeded.
    assert_eq!(effective_cpu_count(2, Some(32.0)), 2);
    // Fractional quotas ceil to stay usable, never zero.
    assert_eq!(effective_cpu_count(8, Some(0.5)), 1);
    assert_eq!(effective_cpu_count(8, Some(1.5)), 2);
    assert_eq!(effective_cpu_count(8, Some(1.0)), 1);
    // Missing quota → current behavior unchanged.
    assert_eq!(effective_cpu_count(8, None), 8);
    // Degenerate input fails safe at 1, never 0.
    assert_eq!(effective_cpu_count(0, None), 1);
    assert_eq!(effective_cpu_count(0, Some(4.0)), 1);
    assert_eq!(effective_cpu_count(8, Some(-1.0)), 8);
    assert_eq!(effective_cpu_count(8, Some(0.0)), 8);
    assert_eq!(effective_cpu_count(8, Some(f64::NAN)), 8);
    assert_eq!(effective_cpu_count(8, Some(f64::INFINITY)), 8);
}

#[test]
fn container_capped_profile_flows_into_tiers_and_budgets() {
    // 64c/256GiB host under `--cpus=2 --memory=1g`: detection records the
    // capped fields, and tier + budget follow with no caller changes.
    let capped_cpus = effective_cpu_count(64, parse_cgroup_cpu_max("200000 100000"));
    let capped_ram = effective_ram_total(Some(256 * GIB), parse_cgroup_memory_limit("1073741824"));
    assert_eq!((capped_cpus, capped_ram), (2, Some(GIB)));
    let hw = mock(capped_cpus, capped_ram);
    assert!(hw.is_small_device());
    // RAM term (1GiB/4/8MiB) = 32 > 2 CPUs, so the CPU term binds.
    assert_eq!(hw.worker_budget(), 2);
}

#[test]
fn refresh_dynamic_limits_keeps_stable_topology_and_stays_sane() {
    // Live refresh touches only volatile limits; stable topology rides
    // through unchanged. Volatile values come from the real host here,
    // so assert sanity bounds rather than exact numbers.
    let mut hw = HardwareProfile {
        arch: Architecture::Aarch64,
        cpu: CpuFeatures {
            scalar: true,
            sse42: false,
            avx2: false,
            avx512: false,
            neon: true,
        },
        logical_cpus: 4,
        physical_cpus: Some(4),
        cache_bytes: [
            Some(64 * 1024),
            Some(4 * 1024 * 1024),
            Some(32 * 1024 * 1024),
        ],
        ram_total_bytes: Some(4 * GIB),
        ram_available_bytes: Some(2 * GIB),
        storage: StorageKind::Nvme,
        storage_bandwidth_bps: Some(1_000_000),
        storage_iops_estimate: Some(10_000),
        numa_nodes: Some(1),
        notes: vec!["startup".to_string()],
    };
    hw.refresh_dynamic_limits();
    // Stable fields untouched.
    assert_eq!(hw.arch, Architecture::Aarch64);
    assert!(hw.cpu.neon);
    assert_eq!(hw.physical_cpus, Some(4));
    assert_eq!(
        hw.cache_bytes,
        [
            Some(64 * 1024),
            Some(4 * 1024 * 1024),
            Some(32 * 1024 * 1024)
        ]
    );
    assert_eq!(hw.storage, StorageKind::Nvme);
    assert_eq!(hw.numa_nodes, Some(1));
    assert_eq!(hw.notes, vec!["startup".to_string()]);
    // Volatile fields sane: at least one CPU, and available never
    // exceeds the total when both are known.
    assert!(hw.logical_cpus >= 1);
    if let (Some(total), Some(available)) = (hw.ram_total_bytes, hw.ram_available_bytes) {
        assert!(available <= total, "{available} <= {total}");
    }
    // Dynamic readers agree with the refresh on this host.
    if let Some(cpus) = read_dynamic_cpu_count() {
        assert_eq!(hw.logical_cpus, cpus);
    }
}
