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
//! Durable single-file publication policy for persisted metadata.
//!
//! Catalog states, generation records, and the publication pointer are all
//! published with one policy, the same policy checkpoints use:
//!
//! ```text
//! BUILD stage the complete image into <final>.tmp
//! FLUSH push the staged bytes through the file's buffered writer
//! VERIFY re-read the staged image and validate it before it can publish
//! SYNC   fsync the staged file: the durability boundary
//! PUBLISH atomically rename the staged file onto its final name, then fsync
//!         the containing directory so the rename itself is durable
//! ```
//!
//! A crash before the rename leaves only a `.tmp` artifact, which discovery
//! ignores, so the previously published file stays authoritative. Final names
//! are never written in place, so a reader observes either the old complete
//! image or the new complete image.
//!
//! Checkpoints keep their own phase-instrumented staging loop (failure
//! injection points and phase timings are part of the checkpoint contract) and
//! share the synchronization helpers below, so the crate has exactly one
//! durability boundary implementation for metadata files.

use crate::codec::{corruption, invalid};
use plomid_core::{ErrorKind, PlomidError, Result};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Creates the directory at `path` if it does not exist.
///
/// Data directories hold credentials-adjacent state (catalog role passwords,
/// WAL, checkpoints), so they are restricted to `0o700` on Unix. Non-Unix
/// hosts keep the platform default.
pub(crate) fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(PlomidError::from)?;
    restrict_dir(path);
    Ok(())
}

#[cfg(unix)]
pub(crate) fn restrict_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
pub(crate) fn restrict_dir(_path: &Path) {}

#[cfg(unix)]
pub(crate) fn restrict_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
pub(crate) fn restrict_file(_path: &Path) {}

/// BUILD + FLUSH: writes the complete image and flushes it through the file.
///
/// The staged file is created or truncated, so a leftover artifact from an
/// interrupted publication can never contribute bytes to a new image.
pub fn stage_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut open_options = OpenOptions::new();
    open_options
        .create(true)
        .truncate(true)
        .read(true)
        .write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        open_options.mode(0o600);
    }
    let mut file = open_options.open(path).map_err(PlomidError::from)?;
    restrict_file(path);
    let mut written = 0_usize;
    while written < bytes.len() {
        let count = file.write(&bytes[written..]).map_err(PlomidError::from)?;
        if count == 0 {
            return Err(PlomidError::new(
                ErrorKind::Io,
                "staged metadata file accepted no bytes",
            ));
        }
        written = written
            .checked_add(count)
            .ok_or_else(|| invalid("staged metadata write overflow"))?;
    }
    file.flush().map_err(PlomidError::from)
}

/// VERIFY: re-reads a staged image and requires it to be exactly `expected`.
pub fn verify_staged(path: &Path, expected: &[u8]) -> Result<()> {
    let staged = fs::read(path).map_err(PlomidError::from)?;
    if staged.len() != expected.len() {
        return Err(corruption("staged metadata length changed"));
    }
    if staged != expected {
        return Err(corruption("staged metadata contents changed"));
    }
    Ok(())
}

/// SYNC: makes the contents of a file durable according to the host
/// filesystem.
///
/// Shared with the WAL crate: segment rotation and checkpoint-marker
/// publication fsync through this same boundary.
pub fn sync_file(path: &Path) -> Result<()> {
    // `FlushFileBuffers` (what `sync_all` uses on Windows) requires a write
    // handle, so a read-only reopen fails with `ERROR_ACCESS_DENIED` there.
    // Unix `fsync` accepts read-only descriptors, so keep that behavior.
    #[cfg(windows)]
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(PlomidError::from)?;
    #[cfg(not(windows))]
    let file = std::fs::File::open(path).map_err(PlomidError::from)?;
    file.sync_all().map_err(PlomidError::from)
}

/// SYNC: makes the directory entries themselves durable.
///
/// A rename (or file creation) is atomic, but it only survives a crash
/// once the containing directory is synchronized. Shared with the WAL
/// crate, which must fsync the WAL directory after publishing its
/// checkpoint marker and after creating a segment file on rotation.
///
/// Unix-only: opening a directory as a file is rejected on Windows
/// (`ERROR_ACCESS_DENIED`), so other platforms skip this step. Their
/// filesystem journals the rename/creation metadata; file-level
/// `sync_all` remains the durability boundary there.
pub fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let file = std::fs::File::open(dir).map_err(PlomidError::from)?;
        file.sync_all().map_err(PlomidError::from)
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// PUBLISH: atomically replaces `final_path` with the staged file.
pub fn publish_rename(staged: &Path, final_path: &Path) -> Result<()> {
    fs::rename(staged, final_path).map_err(PlomidError::from)
}

/// Removes a file, ignoring absence.
///
/// Used to clear staging artifacts and to reclaim generation storage, never to
/// remove a file that is still referenced by durable metadata.
pub(crate) fn remove_if_exists(path: &Path) {
    let _ = fs::remove_file(path);
}
