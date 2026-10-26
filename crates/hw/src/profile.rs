#![forbid(unsafe_code)]
//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
//
// Host hardware detection. Every field that the OS does not expose to an
// unprivileged process is `None` with a machine-readable `note` — never
// guessed, never zero-filled. Budgets derived from the profile are pure
// functions so later phases can adopt them without re-measuring.

use std::fmt;

/// Instruction-set architecture of the running process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Architecture {
    X86_64,
    Aarch64,
    Arm32,
    Other(&'static str),
}

impl Architecture {
    /// Architecture this binary was compiled for (hence is running on).
    /// One binary serves one ISA; the *same codebase* serves all of them,
    /// and per-CPU features below are detected at runtime within the ISA.
    #[must_use]
    pub fn host() -> Self {
        match std::env::consts::ARCH {
            "x86_64" => Self::X86_64,
            "aarch64" => Self::Aarch64,
            "arm" => Self::Arm32,
            other => Self::Other(other),
        }
    }
}

impl fmt::Display for Architecture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::X86_64 => write!(f, "x86_64"),
            Self::Aarch64 => write!(f, "arm64"),
            Self::Arm32 => write!(f, "arm32"),
            Self::Other(name) => write!(f, "{name}"),
        }
    }
}

/// Runtime CPU feature set. `scalar` is always true: it is the fallback
/// every query path works with. Everything else is probed at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuFeatures {
    pub scalar: bool,
    pub sse42: bool,
    pub avx2: bool,
    pub avx512: bool,
    pub neon: bool,
}

impl CpuFeatures {
    /// Probe the host CPU. Pure capability reporting: no side effects.
    #[must_use]
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self {
                scalar: true,
                sse42: std::arch::is_x86_feature_detected!("sse4.2"),
                avx2: std::arch::is_x86_feature_detected!("avx2"),
                avx512: std::arch::is_x86_feature_detected!("avx512f"),
                neon: false,
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self {
                scalar: true,
                sse42: false,
                avx2: false,
                avx512: false,
                // ASIMD/NEON is mandatory on aarch64; probe anyway so the
                // field always reflects measurement, not assumption.
                neon: std::arch::is_aarch64_feature_detected!("neon"),
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self {
                scalar: true,
                sse42: false,
                avx2: false,
                avx512: false,
                neon: false,
            }
        }
    }
}

/// Block-device class behind the data directory (best effort).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Nvme,
    Ssd,
    Hdd,
    Unknown,
}

/// Complete host description for adaptive budgets and dispatch.
#[derive(Debug, Clone)]
pub struct HardwareProfile {
    pub arch: Architecture,
    pub cpu: CpuFeatures,
    pub logical_cpus: usize,
    pub physical_cpus: Option<usize>,
    /// L1d, L2, L3 bytes where exposed (index 0..2, missing levels None).
    pub cache_bytes: [Option<u64>; 3],
    pub ram_total_bytes: Option<u64>,
    pub ram_available_bytes: Option<u64>,
    pub storage: StorageKind,
    /// Sustained sequential bandwidth estimate, bytes/s. `None` until an
    /// active measurement phase provides one (never inferred from kind).
    pub storage_bandwidth_bps: Option<u64>,
    /// Random 4K IOPS estimate. Same policy as bandwidth: measured or None.
    pub storage_iops_estimate: Option<u64>,
    /// NUMA nodes visible to this process (`None` = unknown/UMA).
    pub numa_nodes: Option<usize>,
    /// Why fields are None, one line per gap (stable contract for UIs).
    pub notes: Vec<String>,
}

/// Absolute safety ceiling for [`HardwareProfile::worker_budget`]: a
/// thread-explosion backstop for a single dispatch, never a sizing target.
/// Normal operation sizes from CPUs and RAM far below this; only 256+ core
/// machines touch it. Half the executor's process-wide 512 ceiling (which
/// covers roughly two concurrent full dispatches), so one dispatch never
/// needs more.
pub const MAX_WORKER_BUDGET: usize = 256;

/// Per-worker memory arena assumed by [`HardwareProfile::worker_budget`]'s
/// RAM term (8 MiB). Mirrors `plomid-memory` `MemoryConfig::worker_budget`
/// (read from that crate by the executor's process-wide cap; kept as a
/// literal here so this crate stays dependency-free — update both together).
const WORKER_ARENA_BYTES: u64 = 8 * 1024 * 1024;

/// Fraction of total RAM the worker budget may assume: one quarter, matching
/// the executor's process-wide cap so per-dispatch and process-wide sizing
/// stay coherent.
const WORKER_RAM_FRACTION: u64 = 4;

/// RAM term (in workers) when total RAM is unknown: never size fan-out
/// blind. Matches the executor's unknown-RAM fallback (32) so the two caps
/// agree on unmeasurable hosts.
const UNKNOWN_RAM_WORKERS: u64 = 32;

impl HardwareProfile {
    /// Detect the host. Never fails: unknowns become `None` + notes.
    #[must_use]
    pub fn detect() -> Self {
        let mut notes = Vec::new();
        let arch = Architecture::host();
        let cpu = CpuFeatures::detect();
        let logical_cpus = {
            let base = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or_else(|_| {
                    notes.push("available_parallelism failed; assuming 1".to_string());
                    1
                });
            // Cgroup CPU quota (Docker `--cpus`, k8s limits) only ever shrinks
            // this: `available_parallelism` already reflects sched affinity,
            // so the min never exceeds it. Off Linux the cgroup files are
            // absent, yielding None → `base` unchanged.
            match read_cgroup_cpu_count() {
                Some(quota) => {
                    let capped = effective_cpu_count(base, Some(quota));
                    if capped != base {
                        notes.push(format!(
                            "cgroup CPU quota caps parallelism: {base} -> {capped}"
                        ));
                    }
                    capped
                }
                None => base,
            }
        };
        let physical_cpus = detect_physical_cpus(&mut notes);
        let cache_bytes = detect_caches(&mut notes);
        let (ram_total_bytes, ram_available_bytes) = detect_ram(&mut notes);
        let storage = detect_storage_kind(&mut notes);
        let numa_nodes = detect_numa(&mut notes);
        if arch == Architecture::Arm32 {
            notes.push("arm32: scalar-only runtime; no SIMD backends".to_string());
        }
        if matches!(arch, Architecture::Other(_)) {
            notes.push("unrecognized arch: scalar-only runtime".to_string());
        }
        Self {
            arch,
            cpu,
            logical_cpus,
            physical_cpus,
            cache_bytes,
            ram_total_bytes,
            ram_available_bytes,
            storage,
            storage_bandwidth_bps: None,
            storage_iops_estimate: None,
            numa_nodes,
            notes,
        }
    }

    /// Worker budget for data-parallel work: parallelism without
    /// oversubscription. `min(256 ceiling, logical CPUs, (RAM/4) / 8 MiB
    /// per-worker arena)`, floored at 1; unknown RAM contributes a
    /// conservative 32 to the RAM term instead of sizing fan-out blind.
    /// Small boxes stay small (a 4c/4GiB Pi yields 4, and the Edge tier
    /// further caps permits at 2); big iron scales (128c/512GiB yields 128,
    /// where the old fixed 64-clamp stopped). Every term is at most the
    /// corresponding term of the executor's process-wide cap (1x vs 2x CPUs,
    /// 256 vs 512 ceiling, same RAM fraction), so one dispatch never sizes
    /// past what the process cap grants.
    #[must_use]
    pub fn worker_budget(&self) -> usize {
        let cpus = self.logical_cpus.max(1) as u64;
        let ram_term = match self.ram_total_bytes {
            Some(ram) => (ram / WORKER_RAM_FRACTION / WORKER_ARENA_BYTES).max(1),
            None => UNKNOWN_RAM_WORKERS,
        };
        cpus.min(ram_term).min(MAX_WORKER_BUDGET as u64).max(1) as usize
    }

    /// Preferred vector batch size in rows: sized so one batch of 8-byte
    /// lanes fits comfortably in L2 (or a 256 KiB fallback band when L2
    /// is unknown), clamped to [1K, 1M] rows.
    #[must_use]
    pub fn batch_rows(&self) -> usize {
        let l2 = self.cache_bytes[1].unwrap_or(256 * 1024);
        (l2 / 8 / 4).clamp(1024, 1_048_576) as usize
    }

    /// Whether the box looks like a small edge device (Pi-class): at most
    /// 4 logical CPUs and at most 4 GiB RAM (when RAM is known; unknown
    /// RAM never forces the small profile).
    #[must_use]
    pub fn is_small_device(&self) -> bool {
        if self.logical_cpus > 4 {
            return false;
        }
        match self.ram_total_bytes {
            Some(ram) => ram <= 4 * 1024 * 1024 * 1024,
            None => false,
        }
    }

    /// Lightweight live refresh of the volatile limits only: logical CPUs
    /// (sched affinity + cgroup quota, e.g. k8s VPA / `docker update`) and
    /// total/available RAM (meminfo + cgroup limit, e.g. container resize).
    ///
    /// Cheap by design: two sysfs/proc numbers, no topology walks (physical
    /// CPUs, caches, block devices, NUMA), no `sysctl` spawns beyond the
    /// macOS RAM path, no env reads. Runs at most every
    /// [`crate::runtime::RECHECK_INTERVAL`] via the executor's interval gate,
    /// so steady-state cost is one `Instant` check per admission.
    ///
    /// Fail-safe: a transient read failure keeps the previous value instead
    /// of flapping to a conservative fallback. Only a successful re-read
    /// replaces the stored limit. `notes` are startup diagnostics and are
    /// left untouched so repeated rechecks never grow them.
    pub fn refresh_dynamic_limits(&mut self) {
        if let Some(cpus) = read_dynamic_cpu_count() {
            self.logical_cpus = cpus;
        }
        let (total, available) = read_dynamic_ram();
        if let Some(total) = total {
            self.ram_total_bytes = Some(total);
            // Available can never exceed the effective total (a 60 GiB host
            // `MemAvailable` is not usable under a 512 MiB container cap).
            self.ram_available_bytes = match (available, self.ram_available_bytes) {
                (Some(a), _) => Some(a.min(total)),
                (None, prev) => prev.map(|p| p.min(total)),
            };
        } else if let Some(available) = available {
            // Total unreadable but available parsed: clamp to the stored
            // total when known, otherwise carry the reading.
            self.ram_available_bytes = match self.ram_total_bytes {
                Some(t) => Some(available.min(t)),
                None => Some(available),
            };
        }
    }

    /// Best SIMD width the runtime may use, or `None` for scalar.
    #[must_use]
    pub fn vector_isa(&self) -> VectorIsa {
        VectorIsa::select(&self.cpu, self.arch)
    }
}

/// SIMD backend selected at runtime. Order is widest-first within the
/// host ISA; scalar is always available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIsa {
    Scalar,
    Neon,
    Avx2,
    Avx512,
}

impl VectorIsa {
    #[must_use]
    pub fn select(cpu: &CpuFeatures, arch: Architecture) -> Self {
        match arch {
            Architecture::X86_64 => {
                if cpu.avx512 {
                    Self::Avx512
                } else if cpu.avx2 {
                    Self::Avx2
                } else {
                    Self::Scalar
                }
            }
            Architecture::Aarch64 => {
                if cpu.neon {
                    Self::Neon
                } else {
                    Self::Scalar
                }
            }
            Architecture::Arm32 | Architecture::Other(_) => Self::Scalar,
        }
    }

    /// Lane count used by the canonical f64 reduction (identical in every
    /// backend, including scalar — this is what makes results
    /// bit-identical across ISAs).
    pub const F64_LANES: usize = 4;
}

impl fmt::Display for VectorIsa {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar => write!(f, "scalar"),
            Self::Neon => write!(f, "neon"),
            Self::Avx2 => write!(f, "avx2"),
            Self::Avx512 => write!(f, "avx512"),
        }
    }
}

fn detect_physical_cpus(notes: &mut Vec<String>) -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        let out = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.physicalcpu")
            .output();
        match out {
            Ok(o) if o.status.success() => {
                if let Ok(n) = String::from_utf8_lossy(&o.stdout).trim().parse() {
                    return Some(n);
                }
                notes.push("hw.physicalcpu unparsable".to_string());
                None
            }
            _ => {
                notes.push("sysctl hw.physicalcpu unavailable".to_string());
                None
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Unique (physical id, core id) pairs across /proc/cpuinfo.
        let text = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        if text.is_empty() {
            notes.push("/proc/cpuinfo unreadable".to_string());
            return None;
        }
        let mut package = String::new();
        let mut cores = std::collections::BTreeSet::new();
        for line in text.lines() {
            if let Some(v) = line.trim().strip_prefix("physical id") {
                package = v.trim_start_matches([':', ' ', '\t']).to_string();
            } else if let Some(v) = line.trim().strip_prefix("core id") {
                cores.insert((
                    package.clone(),
                    v.trim_start_matches([':', ' ', '\t']).to_string(),
                ));
            }
        }
        if cores.is_empty() {
            notes.push("no core-id topology in /proc/cpuinfo".to_string());
            None
        } else {
            Some(cores.len())
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        notes.push("physical-CPU topology not implemented on this OS".to_string());
        None
    }
}

fn detect_caches(notes: &mut Vec<String>) -> [Option<u64>; 3] {
    #[cfg(target_os = "macos")]
    {
        fn sysctl(name: &str) -> Option<u64> {
            let out = std::process::Command::new("sysctl")
                .arg("-n")
                .arg(name)
                .output()
                .ok()?;
            if !out.status.success() {
                return None;
            }
            String::from_utf8_lossy(&out.stdout).trim().parse().ok()
        }
        let caches = [
            sysctl("hw.l1dcachesize"),
            sysctl("hw.l2cachesize"),
            sysctl("hw.l3cachesize"),
        ];
        if caches.iter().all(|c| c.is_none()) {
            notes.push("macOS cache sysctls unavailable".to_string());
        }
        caches
    }
    #[cfg(target_os = "linux")]
    {
        // Per-CPU cache index dirs: level + size files.
        let mut levels: [Option<u64>; 4] = [None, None, None, None];
        for cpu in ["cpu0"] {
            for idx in 0..8 {
                let base = format!("/sys/devices/system/cpu/{cpu}/cache/index{idx}");
                let level: usize = std::fs::read_to_string(format!("{base}/level"))
                    .unwrap_or_default()
                    .trim()
                    .parse()
                    .unwrap_or(0);
                if !(1..=4).contains(&level) {
                    continue;
                }
                if levels[level - 1].is_some() {
                    continue;
                }
                let raw = std::fs::read_to_string(format!("{base}/size")).unwrap_or_default();
                levels[level - 1] = parse_kb_size(raw.trim());
            }
        }
        if levels[0].is_none() {
            notes.push("Linux cache sysfs unreadable".to_string());
        }
        [levels[0], levels[1], levels[2]]
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        notes.push("cache topology not implemented on this OS".to_string());
        [None, None, None]
    }
}

#[cfg(target_os = "linux")]
fn parse_kb_size(raw: &str) -> Option<u64> {
    // sysfs sizes look like "48K", "2048K", "36M".
    let (digits, suffix) =
        raw.split_at(raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len()));
    let n: u64 = digits.parse().ok()?;
    match suffix {
        "K" => Some(n * 1024),
        "M" => Some(n * 1024 * 1024),
        _ => None,
    }
}

fn detect_ram(notes: &mut Vec<String>) -> (Option<u64>, Option<u64>) {
    #[cfg(target_os = "macos")]
    {
        let total = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok());
        // Available = free + inactive pages (best effort, same definition
        // Activity Monitor approximates for "available").
        let available = std::process::Command::new("vm_stat")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| {
                let text = String::from_utf8_lossy(&o.stdout);
                let mut pages = 0u64;
                let mut page_size = 16384u64;
                for line in text.lines() {
                    let line = line.trim();
                    if line.starts_with("page size of") {
                        // e.g. "(page size of 16384 bytes)".
                        for token in line.split_whitespace() {
                            if let Ok(n) = token.parse::<u64>() {
                                page_size = n;
                                break;
                            }
                        }
                    }
                    if line.starts_with("Pages free:") || line.starts_with("Pages inactive:") {
                        if let Some(n) = line.split_whitespace().nth(2) {
                            pages += n.trim_end_matches('.').parse::<u64>().unwrap_or(0);
                        }
                    }
                }
                pages * page_size
            });
        if total.is_none() {
            notes.push("hw.memsize unavailable".to_string());
        }
        if available.is_none() {
            notes.push("vm_stat available-memory parse failed".to_string());
        }
        (total, available)
    }
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        if text.is_empty() {
            notes.push("/proc/meminfo unreadable".to_string());
            return (None, None);
        }
        fn kb(key: &str, text: &str) -> Option<u64> {
            text.lines().find_map(|line| {
                let rest = line.trim().strip_prefix(key)?.trim();
                rest.split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|n| n * 1024)
            })
        }
        let total = kb("MemTotal:", &text);
        let available = kb("MemAvailable:", &text);
        if total.is_none() {
            notes.push("MemTotal missing from /proc/meminfo".to_string());
        }
        // Cgroup memory limit (Docker `--memory`, k8s limits) only ever
        // shrinks the total: host meminfo is an over-read inside a container.
        // Missing/unset cgroup files yield None → host reading unchanged.
        // Available can never exceed the effective total, so it is clamped
        // to match (a 60 GiB host `MemAvailable` is not usable under a
        // 512 MiB container cap).
        let total = match read_cgroup_memory_limit() {
            Some(limit) => {
                let capped = effective_ram_total(total, Some(limit));
                if capped != total {
                    notes.push(format!(
                        "cgroup memory limit caps RAM: {total:?} -> {capped:?}"
                    ));
                }
                capped
            }
            None => total,
        };
        let available = match (available, total) {
            (Some(a), Some(t)) => Some(a.min(t)),
            (a, _) => a,
        };
        (total, available)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        notes.push("RAM detection not implemented on this OS".to_string());
        (None, None)
    }
}

/// Cgroup "no limit" sentinel threshold: the kernel reports absurd values
/// (typically `LONG_MAX` ≈ 2^63 − 1, page-rounded variants) when no memory
/// limit is set. Anything ≥ 2^60 is treated as unconstrained.
///
/// Only wired into the Linux detect path (these files exist solely there);
/// the `allow` keeps the helper — and its unit tests — compiling warning-free
/// on macOS/Windows, where the reader below always yields `None`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const CGROUP_NO_LIMIT_BYTES: u64 = 1 << 60;

/// Parse one cgroup memory-limit file's contents: v2 `memory.max` (a byte
/// count or the literal `max`) or v1 `memory.limit_in_bytes` (a byte count).
/// Returns `None` for unconstrained input (`max`, empty, unparsable, zero,
/// or an absurd ≥ 2^60 value) so callers fall back to the host reading
/// unchanged. Pure: takes file contents, so unit tests need no containers.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn parse_cgroup_memory_limit(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if s.is_empty() || s.eq_ignore_ascii_case("max") {
        return None;
    }
    let n: u64 = s.parse().ok()?;
    if n == 0 || n >= CGROUP_NO_LIMIT_BYTES {
        return None;
    }
    Some(n)
}

/// Effective total RAM: `min(meminfo_total, cgroup_limit)`. A missing cgroup
/// limit leaves the host reading untouched; a missing meminfo reading with a
/// present limit still reports the limit (a real upper bound on usable RAM).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn effective_ram_total(meminfo_total: Option<u64>, cgroup_limit: Option<u64>) -> Option<u64> {
    match (meminfo_total, cgroup_limit) {
        (Some(total), Some(limit)) => Some(total.min(limit)),
        (total, None) => total,
        (None, Some(limit)) => Some(limit),
    }
}

/// Read this process's cgroup RAM limit: v2 `memory.max` first, then v1
/// `memory.limit_in_bytes`. Returns the first constrained value found;
/// `None` means unconstrained or files absent (the common case off Linux,
/// where these paths never exist, so behavior there is unchanged).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn read_cgroup_memory_limit() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = std::fs::read_to_string(path) {
            if let Some(limit) = parse_cgroup_memory_limit(&raw) {
                return Some(limit);
            }
        }
    }
    None
}

/// Parse v2 `cpu.max` contents (`$MAX $PERIOD`, e.g. `200000 100000` = 2
/// CPUs). Returns the allowance in fractional CPUs (`quota / period`), or
/// `None` for unconstrained (`max ...`), degenerate, or unparsable input.
/// Pure: takes file contents, so unit tests need no containers.
pub fn parse_cgroup_cpu_max(raw: &str) -> Option<f64> {
    let mut parts = raw.split_whitespace();
    let max = parts.next()?;
    if max.eq_ignore_ascii_case("max") {
        return None;
    }
    let quota: f64 = max.parse().ok()?;
    let period: f64 = parts.next()?.parse().ok()?;
    if !quota.is_finite() || !period.is_finite() || quota <= 0.0 || period <= 0.0 {
        return None;
    }
    Some(quota / period)
}

/// Parse a v1 CPU quota pair (`cpu.cfs_quota_us` / `cpu.cfs_period_us`
/// contents). A negative quota (`-1`) means unconstrained → `None`. Pure:
/// takes file contents, so unit tests need no containers.
pub fn parse_cgroup_cpu_quota_v1(quota_raw: &str, period_raw: &str) -> Option<f64> {
    let quota: i64 = quota_raw.trim().parse().ok()?;
    if quota < 0 {
        return None;
    }
    let period: f64 = period_raw.trim().parse().ok()?;
    if !period.is_finite() || period <= 0.0 {
        return None;
    }
    Some(quota as f64 / period)
}

/// Effective logical CPU count: `min(available_parallelism, ceil(quota))`.
/// `available_parallelism` already reflects sched affinity, so the result
/// never exceeds it. The ceiling keeps fractional quotas (e.g. 0.5 CPU)
/// usable instead of rounding to zero, and the floor of 1 keeps degenerate
/// input fail-safe.
pub fn effective_cpu_count(available: usize, cgroup_cpus: Option<f64>) -> usize {
    let base = available.max(1);
    match cgroup_cpus {
        None => base,
        Some(cpus) if !cpus.is_finite() || cpus <= 0.0 => base,
        Some(cpus) => base.min((cpus.ceil() as usize).max(1)),
    }
}

/// Read this process's cgroup CPU allowance in fractional CPUs: v2 `cpu.max`
/// first, then the v1 `cpu.cfs_quota_us` / `cpu.cfs_period_us` pair.
/// `None` means unconstrained or files absent (off Linux: always None, so
/// behavior there is unchanged).
fn read_cgroup_cpu_count() -> Option<f64> {
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        if let Some(cpus) = parse_cgroup_cpu_max(&raw) {
            return Some(cpus);
        }
    }
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?;
    parse_cgroup_cpu_quota_v1(&quota, &period)
}

/// Lightweight CPU re-read for [`HardwareProfile::refresh_dynamic_limits`]:
/// `available_parallelism` (already affinity-aware, covers hot-plugged CPUs)
/// capped by the cgroup quota. `None` on any failure so the caller keeps the
/// previous value instead of flapping. No notes, no topology walks.
pub fn read_dynamic_cpu_count() -> Option<usize> {
    let base = std::thread::available_parallelism().map(|n| n.get()).ok()?;
    Some(effective_cpu_count(base, read_cgroup_cpu_count()))
}

/// Lightweight RAM re-read for [`HardwareProfile::refresh_dynamic_limits`]:
/// same sources as detection (Linux meminfo + cgroup limit, macOS
/// `hw.memsize` + `vm_stat`) but without notes. `(None, None)` on failure so
/// the caller keeps the previous values.
fn read_dynamic_ram() -> (Option<u64>, Option<u64>) {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        if text.is_empty() {
            // Meminfo transiently unreadable: still honor a present cgroup
            // cap (a real upper bound), otherwise signal failure.
            return match read_cgroup_memory_limit() {
                Some(limit) => (Some(limit), None),
                None => (None, None),
            };
        }
        fn kb(key: &str, text: &str) -> Option<u64> {
            text.lines().find_map(|line| {
                let rest = line.trim().strip_prefix(key)?.trim();
                rest.split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|n| n * 1024)
            })
        }
        let total = kb("MemTotal:", &text);
        let available = kb("MemAvailable:", &text);
        let cgroup = read_cgroup_memory_limit();
        let total = match cgroup {
            Some(limit) => effective_ram_total(total, Some(limit)),
            None => total,
        };
        let available = match (available, total) {
            (Some(a), Some(t)) => Some(a.min(t)),
            (a, _) => a,
        };
        // A present cgroup cap with missing meminfo still reports the bound.
        let total = match (total, cgroup) {
            (None, Some(limit)) => Some(limit),
            (t, _) => t,
        };
        (total, available)
    }
    #[cfg(target_os = "macos")]
    {
        let total = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok());
        let available = std::process::Command::new("vm_stat")
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| {
                let text = String::from_utf8_lossy(&o.stdout);
                let mut pages = 0u64;
                let mut page_size = 16384u64;
                for line in text.lines() {
                    let line = line.trim();
                    if line.starts_with("page size of") {
                        for token in line.split_whitespace() {
                            if let Ok(n) = token.parse::<u64>() {
                                page_size = n;
                                break;
                            }
                        }
                    }
                    if line.starts_with("Pages free:") || line.starts_with("Pages inactive:") {
                        if let Some(n) = line.split_whitespace().nth(2) {
                            pages += n.trim_end_matches('.').parse::<u64>().unwrap_or(0);
                        }
                    }
                }
                pages * page_size
            });
        (total, available)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (None, None)
    }
}

fn detect_storage_kind(notes: &mut Vec<String>) -> StorageKind {
    #[cfg(target_os = "linux")]
    {
        // Any rotational block device behind / : HDD. SSD/NVMe distinction
        // needs transport info; NVMe namespaces expose no rotational file,
        // so absence across SSD-class devices stays Unknown, never guessed.
        let mut saw_non_rotational = false;
        let Ok(dir) = std::fs::read_dir("/sys/block") else {
            notes.push("/sys/block unreadable".to_string());
            return StorageKind::Unknown;
        };
        for entry in dir.flatten() {
            let path = entry.path().join("queue/rotational");
            if let Ok(raw) = std::fs::read_to_string(&path) {
                match raw.trim() {
                    "1" => return StorageKind::Hdd,
                    "0" => saw_non_rotational = true,
                    _ => {}
                }
            }
        }
        if saw_non_rotational {
            return StorageKind::Ssd;
        }
        notes.push("no block-device rotational info".to_string());
        StorageKind::Unknown
    }
    #[cfg(not(target_os = "linux"))]
    {
        notes.push("storage-kind detection not implemented on this OS".to_string());
        StorageKind::Unknown
    }
}

fn detect_numa(notes: &mut Vec<String>) -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        match std::fs::read_dir("/sys/devices/system/node") {
            Ok(dir) => {
                let n = dir
                    .flatten()
                    .filter(|e| {
                        e.file_name().to_string_lossy().starts_with("node")
                            && e.path().join("cpumap").exists()
                    })
                    .count();
                if n == 0 {
                    notes.push("no NUMA nodes enumerated".to_string());
                    None
                } else {
                    Some(n)
                }
            }
            Err(_) => {
                notes.push("/sys/devices/system/node unreadable (UMA assumed)".to_string());
                None
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        notes.push("NUMA topology not exposed on this OS".to_string());
        None
    }
}
