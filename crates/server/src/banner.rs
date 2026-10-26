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
//! Terminal startup screen for the PLOMID database server.
//!
//! Design contract (PostgreSQL-terse, production tone):
//!
//! * Pure ASCII output. The previous screen embedded multi-byte block glyphs
//!   that rendered as mojibake on terminals without that encoding; every
//!   rendered byte here is `< 0x80`. Box borders are `+`, `-`, `|`.
//! * Horizontal and compact: the wordmark is [`LOGO_LINES`] lines wide, never
//!   a tall vertical composition.
//! * The `runtime` panel reads exclusively from [`plomid_hw::RuntimeProfile`]
//!   accessors — no re-detection, no duplicated budgets. Cgroup capping is
//!   surfaced from [`plomid_hw::HardwareProfile`] `notes` (its documented UI
//!   contract), not re-derived.
//! * The `boot` panel shows `starting` before services open and final `ready`
//!   states with per-stage milliseconds after. Stage timings are measured in
//!   `main` around engine-open / listener-bind. The write-ahead log mounts
//!   inside `PlomidStorageEngine::open` and the executor has no open step in
//!   this binary, so those two rows report `ready` without a separate timer
//!   rather than inventing numbers (splitting them would mean touching
//!   engine-open logic, which this screen must not do).
//!
//! Rendering is pure (`String` in, `String` out) so unit tests capture and
//! assert on the exact screen.

use crate::config::ServerConfig;

/// Horizontal ASCII wordmark. Five lines, plain `/\|_-` shapes only.
pub const LOGO_LINES: &[&str] = &[
    "    **      **********     **             ********      **        ***   **    **********",
    " ********            **    **           ***      ***    ****    *****   **             ***",
    "***    ***           **    **          **         **    ** ******* **   **    **        **",
    "***  *****  **********     **          **         **    **   **    **   **    **        **",
    "***  **     **             **           ***      ***    **         **   **    **       ***",
    "  *         **             **********     ********      **         **   **    **********  ",
];

/// Maximum rendered logo lines; the compactness test pins this.
pub const LOGO_MAX_LINES: usize = 10;

/// PostgreSQL-compatibility marker kept on the address line.
const EDITION: &str = "postgres wire v3";

/// One boot-stage row for the ready screen.
pub struct StageStatus {
    /// Component name, e.g. `"storage engine"`.
    pub name: &'static str,
    /// Stage latency in milliseconds, or `None` when the stage has no
    /// separately measurable open step (reported as plain `ready`).
    pub millis: Option<u64>,
}

/// Visible (non-ANSI) length, so box edges stay aligned with colors present.
pub fn vis(s: &str) -> usize {
    let mut n = 0;
    let mut esc = false;
    for ch in s.chars() {
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
        n += 1;
    }
    n
}

/// ANSI palette, or empty strings when the output is not a terminal.
struct Palette<'a> {
    accent: &'a str,
    green: &'a str,
    yellow: &'a str,
    dim: &'a str,
    bold: &'a str,
    reset: &'a str,
}

impl<'a> Palette<'a> {
    fn for_color(color: bool) -> Self {
        if color {
            Self {
                // PLOMID brand: copper accent (#e2683c).
                accent: "\x1b[38;2;226;104;60m",
                green: "\x1b[32m",
                yellow: "\x1b[33m",
                dim: "\x1b[2m",
                bold: "\x1b[1m",
                reset: "\x1b[0m",
            }
        } else {
            Self {
                accent: "",
                green: "",
                yellow: "",
                dim: "",
                bold: "",
                reset: "",
            }
        }
    }
}

/// One ASCII panel box (`+---+` borders, `|` sides) with a dim title.
struct Panel<'a> {
    title: &'a str,
    width: usize,
    pal: &'a Palette<'a>,
}

impl Panel<'_> {
    fn row(&self, out: &mut String, content: &str) {
        let need = self.width.saturating_sub(vis(content));
        out.push_str("  | ");
        out.push_str(content);
        for _ in 0..need {
            out.push(' ');
        }
        out.push_str("|\n");
    }

    fn open(&self, out: &mut String) {
        let p = self.pal;
        out.push_str(&format!(
            "  {dim}{}{reset}\n",
            self.title,
            dim = p.dim,
            reset = p.reset
        ));
        out.push_str("  +");
        for _ in 0..self.width + 2 {
            out.push('-');
        }
        out.push_str("+\n");
    }

    fn close(&self, out: &mut String) {
        out.push_str("  +");
        for _ in 0..self.width + 2 {
            out.push('-');
        }
        out.push_str("+\n");
    }
}

/// Human-readable byte count, one decimal for GiB/MiB, exact for KiB/B.
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= GIB {
        format!("{:.1} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.0} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.0} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn storage_label(kind: plomid_hw::StorageKind) -> &'static str {
    match kind {
        plomid_hw::StorageKind::Nvme => "nvme",
        plomid_hw::StorageKind::Ssd => "ssd",
        plomid_hw::StorageKind::Hdd => "hdd",
        plomid_hw::StorageKind::Unknown => "unknown",
    }
}

fn source_label(source: plomid_hw::ProfileSource) -> &'static str {
    match source {
        plomid_hw::ProfileSource::Auto => "auto",
        plomid_hw::ProfileSource::Env => "env",
        plomid_hw::ProfileSource::Cli => "flag",
        plomid_hw::ProfileSource::Explicit => "explicit",
    }
}

fn auth_label(method: &str) -> &str {
    match method {
        "none" | "disabled" => "disabled",
        "md5" => "md5",
        "password" | "cleartext" => "password",
        _ => "scram-sha-256",
    }
}

fn tls_label(config: &ServerConfig) -> String {
    if !config.tls.enabled {
        return "off".to_string();
    }
    if config.tls.required {
        "on (required)".to_string()
    } else {
        "on (optional)".to_string()
    }
}

/// RAM row: detected total, flagged when a cgroup limit capped it.
fn ram_label(runtime: &plomid_hw::RuntimeProfile) -> String {
    let hw = runtime.hardware();
    match hw.ram_total_bytes {
        None => "unknown".to_string(),
        Some(bytes) => {
            let mut label = format_bytes(bytes);
            if hw.notes.iter().any(|n| n.contains("cgroup memory limit")) {
                label.push_str(" (cgroup-limited)");
            }
            label
        }
    }
}

/// CPU row: logical count, flagged when a cgroup quota capped it.
fn cpu_label(runtime: &plomid_hw::RuntimeProfile) -> String {
    let hw = runtime.hardware();
    let mut label = format!("{} logical", hw.logical_cpus);
    if let Some(physical) = hw.physical_cpus {
        label.push_str(&format!(" ({physical} physical)"));
    }
    if hw.notes.iter().any(|n| n.contains("cgroup CPU quota")) {
        label.push_str(" [cgroup-limited]");
    }
    label
}

/// Pre-start screen: wordmark, starting status, configuration, runtime.
/// `version` is the server crate version (passed in so tests control it).
#[must_use]
pub fn render_banner(
    config: &ServerConfig,
    runtime: &plomid_hw::RuntimeProfile,
    version: &str,
    color: bool,
) -> String {
    let pal = Palette::for_color(color);
    let mut out = String::new();
    debug_assert!(
        LOGO_LINES.len() <= LOGO_MAX_LINES,
        "wordmark must stay horizontal and compact"
    );
    out.push('\n');
    for line in LOGO_LINES {
        out.push_str(&format!(
            "{}{}  {line}{}\n",
            pal.bold, pal.accent, pal.reset
        ));
    }
    out.push_str(&format!(
        "  {dim}plomid v{version} - {EDITION}{reset}\n",
        dim = pal.dim,
        reset = pal.reset,
    ));
    out.push('\n');

    // Boot panel: pre-start states only; the ready screen replaces these.
    let boot = Panel {
        title: "boot: system status (starting)",
        width: 76,
        pal: &pal,
    };
    boot.open(&mut out);
    for name in [
        "storage engine",
        "write-ahead log",
        "query executor",
        "network listener",
    ] {
        boot.row(
            &mut out,
            &format!(
                "{yellow}..{reset}  {bold}{name:16}{reset}  {dim}starting{reset}",
                yellow = pal.yellow,
                reset = pal.reset,
                bold = pal.bold,
                dim = pal.dim,
            ),
        );
    }
    boot.close(&mut out);
    out.push('\n');

    // Configuration panel: connection facts.
    let cfg = Panel {
        title: "configuration",
        width: 76,
        pal: &pal,
    };
    cfg.open(&mut out);
    let rows: [(&str, String); 6] = [
        ("listen", config.host.to_string()),
        ("port", config.port.to_string()),
        ("data dir", config.data_dir.display().to_string()),
        ("role", config.username.clone()),
        ("auth", auth_label(&config.auth_method).to_string()),
        ("tls", tls_label(config)),
    ];
    for (key, value) in rows {
        cfg.row(
            &mut out,
            &format!(
                "{dim}{key:>10}{reset}  |  {green}{value}{reset}",
                dim = pal.dim,
                reset = pal.reset,
                green = pal.green,
            ),
        );
    }
    cfg.close(&mut out);
    out.push('\n');

    // Runtime panel: everything from the RuntimeProfile API.
    let hw = runtime.hardware();
    let rt = Panel {
        title: "runtime",
        width: 76,
        pal: &pal,
    };
    rt.open(&mut out);
    // Buffer-pool page size mirrors the pool sizing in plomid_hw
    // (16 KiB pages); display-only arithmetic.
    let pool_pages = runtime.pool_capacity_pages();
    let pool_bytes = pool_pages as u64 * 16 * 1024;
    let mut rows: Vec<(&str, String)> = vec![
        (
            "profile",
            format!("{} ({})", runtime.tier(), source_label(runtime.source())),
        ),
        ("cpu", cpu_label(runtime)),
        ("ram", ram_label(runtime)),
        ("storage", storage_label(hw.storage).to_string()),
        ("workers", runtime.worker_permits().to_string()),
        ("buffer", format_bytes(runtime.buffer_capacity_bytes())),
        (
            "pool",
            format!("{pool_pages} pages ({})", format_bytes(pool_bytes)),
        ),
        (
            "compression",
            if runtime.prefer_compression() {
                "on".to_string()
            } else {
                "off".to_string()
            },
        ),
    ];
    if let Some(pinned) = config.profile.as_deref() {
        rows.push(("override", format!("--profile={pinned}")));
    }
    for (key, value) in rows {
        rt.row(
            &mut out,
            &format!(
                "{dim}{key:>12}{reset}  |  {green}{value}{reset}",
                dim = pal.dim,
                reset = pal.reset,
                green = pal.green,
            ),
        );
    }
    rt.close(&mut out);
    out.push('\n');

    out.push_str(&format!(
        "  {dim}address{reset}  {accent}{bold}{}:{port}{reset}   \
         {dim}edition{reset}  {accent}{EDITION}{reset}   \
         {dim}platform{reset}  {accent}{os}/{arch}{reset}\n",
        config.host,
        port = config.port,
        dim = pal.dim,
        reset = pal.reset,
        accent = pal.accent,
        bold = pal.bold,
        os = std::env::consts::OS,
        arch = std::env::consts::ARCH,
    ));
    out.push_str(&format!(
        "  {dim}ctrl-c to stop{reset}   {dim}debug  {yellow}PLOMID_LOG_LEVEL=debug{reset}\n",
        dim = pal.dim,
        reset = pal.reset,
        yellow = pal.yellow,
    ));
    out.push('\n');
    out
}

/// Post-start screen: final per-component states with stage latencies.
#[must_use]
pub fn render_ready(
    stages: &[StageStatus],
    total_ms: u64,
    listen_addr: &str,
    color: bool,
) -> String {
    let pal = Palette::for_color(color);
    let mut out = String::new();
    let status = Panel {
        title: &format!("status: ready in {total_ms} ms"),
        width: 76,
        pal: &pal,
    };
    status.open(&mut out);
    for stage in stages {
        let latency = match stage.millis {
            Some(0) => "<1 ms".to_string(),
            Some(ms) => format!("{ms} ms"),
            None => String::new(),
        };
        let state = if latency.is_empty() {
            format!("{green}ready{reset}", green = pal.green, reset = pal.reset,)
        } else {
            format!(
                "{green}ready{reset}  {latency}",
                green = pal.green,
                reset = pal.reset,
            )
        };
        status.row(
            &mut out,
            &format!(
                "{green}ok{reset}  {bold}{:16}{reset}  {state}",
                stage.name,
                green = pal.green,
                reset = pal.reset,
                bold = pal.bold,
            ),
        );
    }
    status.close(&mut out);
    out.push_str(&format!(
        "  {dim}accepting connections on{reset} {accent}{bold}{listen_addr}{reset}\n",
        dim = pal.dim,
        reset = pal.reset,
        accent = pal.accent,
        bold = pal.bold,
    ));
    out.push('\n');
    out
}
