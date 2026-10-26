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

//! Live cross-platform process gauges for resource accounting.
//!
//! No `unsafe`, no new dependencies — only [`std`]. Every failure mode
//! degrades to `0` (with a comment at the site) instead of panicking, so
//! gauntlet/report code can always call these on any platform.

use std::path::Path;

/// Point-in-time OS view of this process.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessSnapshot {
    /// Resident set size in bytes (0 when unreadable on this platform).
    pub rss_bytes: u64,
    /// Open file-descriptor count (0 when unreadable on this platform).
    pub fd_count: u64,
}

/// Sample RSS + FD count for the current process. Never panics.
pub fn process_snapshot() -> ProcessSnapshot {
    #[cfg(target_os = "linux")]
    {
        ProcessSnapshot {
            rss_bytes: linux_rss_bytes(),
            fd_count: linux_fd_count(),
        }
    }
    #[cfg(target_os = "macos")]
    {
        ProcessSnapshot {
            rss_bytes: macos_rss_bytes(),
            fd_count: dev_fd_count(),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // No live OS gauge implemented for this platform; report zeros
        // rather than inventing numbers.
        ProcessSnapshot {
            rss_bytes: 0,
            fd_count: 0,
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_rss_bytes() -> u64 {
    // /proc/self/status carries `VmRSS: <kB> kB`. Any IO/parse failure -> 0.
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // First whitespace-separated field is the kB value.
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            return kb.saturating_mul(1024);
        }
    }
    // No VmRSS line (unexpected kernel format) -> 0.
    0
}

#[cfg(target_os = "linux")]
fn linux_fd_count() -> u64 {
    // Each open FD appears as one entry under /proc/self/fd.
    // Unreadable dir (sandboxed procfs) -> 0.
    std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn macos_rss_bytes() -> u64 {
    // `ps -o rss=` prints resident size in KiB on macOS. The child-process
    // spawn or any parse failure -> 0 (ps may be missing in minimal CI images).
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output();
    match out {
        Ok(o) if o.status.success() => {
            let text = String::from_utf8_lossy(&o.stdout);
            text.split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kib| kib.saturating_mul(1024))
                .unwrap_or(0) // Unparseable ps output -> 0.
        }
        // Spawn failure or non-zero exit -> 0.
        _ => 0,
    }
}

#[cfg(target_os = "macos")]
fn dev_fd_count() -> u64 {
    // /dev/fd mirrors open FDs on macOS. Unreadable -> 0.
    std::fs::read_dir("/dev/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or(0)
}

/// Recursively sum file sizes under `path` (WAL / data / temp dirs).
///
/// Iterative (no recursion depth risk), symlink-safe: symlinks are never
/// followed (uses `symlink_metadata` and skips non-file/non-dir types), and
/// every IO error (vanished files, permission denied, concurrent teardown)
/// skips that entry rather than panicking or aborting the total.
pub fn dir_size_bytes(path: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut stack: Vec<std::path::PathBuf> = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        // Deleted-between-sample-and-read or permission error -> skip subtree.
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue, // Entry vanished mid-scan -> skip.
            };
            // symlink_metadata does not follow symlinks.
            let meta = match std::fs::symlink_metadata(entry.path()) {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_symlink() {
                continue; // Never follow symlinks (avoids cycles/escape).
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
            } else if meta.is_dir() {
                stack.push(entry.path());
            }
            // Sockets, fifos, devices, etc. contribute 0.
        }
    }
    total
}
