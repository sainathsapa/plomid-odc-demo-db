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
// Moved from `src/banner.rs` inline `#[cfg(test)]` — public-API only.
use plomid_server::banner::*;
use plomid_server::config::ServerConfig;

fn mock_hardware(
    cpus: usize,
    ram: Option<u64>,
    storage: plomid_hw::StorageKind,
) -> plomid_hw::HardwareProfile {
    plomid_hw::HardwareProfile {
        arch: plomid_hw::Architecture::X86_64,
        cpu: plomid_hw::CpuFeatures {
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

fn test_config() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".parse().unwrap(),
        port: 5432,
        data_dir: "./data".into(),
        copy_dir: "./data/copy".into(),
        username: "plomid".into(),
        password: "plomid".into(),
        auth_method: "scram".into(),
        tls: plomid_server::config::TlsConfig {
            enabled: false,
            cert: "".into(),
            key: "".into(),
            required: false,
        },
        log_level: None,
        allow_remote_plaintext: false,
        allow_legacy_md5: false,
        checkpoint_enabled: true,
        checkpoint_wal_bytes: 67_108_864,
        checkpoint_segments: 16,
        checkpoint_interval_secs: 900,
        profile: None,
        max_connections: 100,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        max_copy_buffer_bytes: 64 * 1024 * 1024,
        max_prepared_statements: 1024,
        max_portals: 1024,
        audit_log: None,
    }
}

fn laptop_runtime() -> plomid_hw::RuntimeProfile {
    const GIB: u64 = 1024 * 1024 * 1024;
    plomid_hw::RuntimeProfile::from_hardware(
        mock_hardware(8, Some(16 * GIB), plomid_hw::StorageKind::Ssd),
        plomid_hw::ProfileSource::Auto,
    )
}

#[test]
fn logo_is_horizontal_and_compact() {
    assert!(
        LOGO_LINES.len() <= LOGO_MAX_LINES,
        "logo has {} lines, max {LOGO_MAX_LINES}",
        LOGO_LINES.len()
    );
    assert!(
        LOGO_LINES.len() >= 2,
        "logo must be a wordmark, not one word"
    );
    for line in LOGO_LINES {
        assert!(line.is_ascii(), "logo line is not pure ASCII: {line:?}");
    }
    let width = LOGO_LINES.iter().map(|l| l.len()).max().unwrap_or(0);
    let height = LOGO_LINES.len();
    assert!(
        width > height * 2,
        "logo must read horizontal (width {width} vs height {height})"
    );
}

#[test]
fn banner_is_pure_ascii_without_color() {
    let out = render_banner(&test_config(), &laptop_runtime(), "0.1.0", false);
    assert!(
        out.is_ascii(),
        "banner must render on any terminal encoding"
    );
    for bad in ['Æ', '◌', '⋯', '│', '─', '┌', '┐', '└', '┘', '—', '·'] {
        assert!(!out.contains(bad), "banner contains mojibake-prone {bad:?}");
    }
    assert!(!out.contains('\x1b'), "uncolored banner must carry no ANSI");
}

#[test]
fn banner_shows_hw_runtime_fields() {
    let out = render_banner(&test_config(), &laptop_runtime(), "0.1.0", false);
    for field in [
        "laptop",
        "auto",
        "8 logical",
        "16.0 GiB",
        "ssd",
        "workers",
        "pool",
        "1024 pages",
        "compression",
        "off",
        "buffer",
    ] {
        assert!(
            out.contains(field),
            "banner missing runtime field {field:?}"
        );
    }
}

#[test]
fn banner_shows_profile_override_when_pinned() {
    let mut config = test_config();
    config.profile = Some("edge".to_string());
    let hw = mock_hardware(
        8,
        Some(16 * 1024 * 1024 * 1024),
        plomid_hw::StorageKind::Ssd,
    );
    let runtime = plomid_hw::RuntimeProfile::with_tier(
        plomid_hw::RuntimeTier::Edge,
        hw,
        plomid_hw::ProfileSource::Cli,
    );
    let out = render_banner(&config, &runtime, "0.1.0", false);
    assert!(out.contains("edge"), "got:\n{out}");
    assert!(out.contains("flag"), "got:\n{out}");
    assert!(out.contains("--profile=edge"), "got:\n{out}");
}

#[test]
fn banner_flags_cgroup_limits_and_unknown_ram() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let mut hw = mock_hardware(2, Some(GIB), plomid_hw::StorageKind::Unknown);
    hw.notes
        .push("cgroup memory limit caps RAM: Some(268435456000) -> Some(1073741824)".to_string());
    hw.notes
        .push("cgroup CPU quota caps parallelism: 64 -> 2".to_string());
    let runtime = plomid_hw::RuntimeProfile::from_hardware(hw, plomid_hw::ProfileSource::Auto);
    let out = render_banner(&test_config(), &runtime, "0.1.0", false);
    assert!(out.contains("cgroup-limited"), "got:\n{out}");

    let unknown = plomid_hw::RuntimeProfile::from_hardware(
        mock_hardware(8, None, plomid_hw::StorageKind::Unknown),
        plomid_hw::ProfileSource::Auto,
    );
    let out = render_banner(&test_config(), &unknown, "0.1.0", false);
    assert!(out.contains("unknown"), "got:\n{out}");
}

#[test]
fn banner_keeps_address_edition_platform_auth_tls() {
    let out = render_banner(&test_config(), &laptop_runtime(), "0.1.0", false);
    for field in [
        "127.0.0.1:5432",
        "postgres wire v3",
        std::env::consts::OS,
        std::env::consts::ARCH,
        "scram-sha-256",
        "tls",
        "off",
    ] {
        assert!(out.contains(field), "banner missing {field:?}");
    }
}

#[test]
fn banner_tone_is_terse_with_no_hype() {
    let out = render_banner(&test_config(), &laptop_runtime(), "0.1.0", false);
    let lower = out.to_lowercase();
    for word in [
        "autonomous",
        "blazing",
        "ultra",
        "supercharg",
        "world-class",
        "cutting-edge",
        "next-gen",
        "revolution",
    ] {
        assert!(!lower.contains(word), "banner hypes with {word:?}");
    }
}

#[test]
fn ready_panel_reports_final_states_with_timings() {
    let stages = [
        StageStatus {
            name: "storage engine",
            millis: Some(120),
        },
        StageStatus {
            name: "write-ahead log",
            millis: None,
        },
        StageStatus {
            name: "query executor",
            millis: None,
        },
        StageStatus {
            name: "network listener",
            millis: Some(12),
        },
    ];
    let out = render_ready(&stages, 182, "127.0.0.1:5432", false);
    assert!(out.is_ascii());
    assert!(out.contains("ready in 182 ms"), "got:\n{out}");
    assert!(out.contains("120 ms"), "got:\n{out}");
    assert!(out.contains("12 ms"), "got:\n{out}");
    assert!(out.contains("accepting connections on"), "got:\n{out}");
    assert!(out.contains("127.0.0.1:5432"), "got:\n{out}");
    for stale in ["initialising", "mounting", "booting", "binding", "starting"] {
        assert!(
            !out.contains(stale),
            "ready screen leaks pre-start {stale:?}"
        );
    }
}

#[test]
fn ready_panel_renders_submillisecond_stages_honestly() {
    let stages = [StageStatus {
        name: "network listener",
        millis: Some(0),
    }];
    let out = render_ready(&stages, 5, "127.0.0.1:5432", false);
    assert!(out.contains("ready  <1 ms"), "got:\n{out}");
    assert!(!out.contains("0 ms"), "got:\n{out}");
}

#[test]
fn colored_panels_keep_ascii_borders_aligned() {
    let out = render_banner(&test_config(), &laptop_runtime(), "0.1.0", true);
    // Strip ANSI, then every border line must be pure ASCII and aligned.
    let mut plain = String::new();
    let mut esc = false;
    for ch in out.chars() {
        if esc {
            if ch == 'm' {
                esc = false;
            }
            continue;
        }
        if ch == '\x1b' {
            esc = true;
            continue;
        }
        plain.push(ch);
    }
    assert!(plain.is_ascii());
    let borders: Vec<&str> = plain
        .lines()
        .filter(|l| l.trim_start().starts_with('+'))
        .collect();
    assert!(!borders.is_empty(), "panels must keep ASCII borders");
    let width = vis(borders[0]);
    for b in &borders {
        assert_eq!(vis(b), width, "misaligned border: {b:?}");
    }
}
