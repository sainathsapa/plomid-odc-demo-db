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
//! Durable metadata framing and publication for layout-owned metadata files.
//!
//! Every layout metadata file — table `META.dat`, generation `META.dat`, index
//! `META.dat`, device `DEVICE.dat`, device `state/STATE.dat` — is one explicit
//! little-endian record:
//!
//! ```text
//! magic[4] | format_version[u32] | header_len[u32] | record fields | checksum[u32] | reserved
//! ```
//!
//! Raw Rust structs are never written: every field has a documented offset, an
//! explicit width, and an explicit byte order, so no compiler layout detail is
//! observable on disk. The checksum is CRC32C over the bytes that precede it,
//! using the crate's single checksum implementation; a record whose checksum,
//! magic, version, header length, or reserved bytes disagree with its contents
//! is rejected as corruption.
//!
//! # Publication
//!
//! Metadata updates never write a final name in place. They follow the crate's
//! single durability policy:
//!
//! ```text
//! BUILD   encode the complete new image in memory
//! FLUSH   write it into <name>.<pid>.<seq>.tmp
//! VERIFY  re-read the staged bytes and require them to be the built image
//! SYNC    fsync the staged file: the durability boundary
//! PUBLISH atomically rename it onto the final name, then fsync the directory
//! ```
//!
//! The staging name is unique per publication: two sessions publishing the
//! same missing default record must never truncate or rename each other's
//! artifact. A crash before the rename leaves only a `.tmp` artifact, which
//! every discovery path ignores, so the previously published record stays
//! authoritative. A reader therefore observes either the old complete record or
//! the new complete record, never a partially written one.

use crate::codec::{corruption, put_u32, Cursor};
use crate::durable;
use plomid_core::{ErrorKind, PlomidError, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Length of the framing prefix shared by every metadata record.
pub(crate) const META_PREFIX_LEN: usize = 12;

/// Appends the framing prefix of a metadata record.
pub(crate) fn encode_prefix(out: &mut Vec<u8>, magic: [u8; 4], version: u32, header_len: u32) {
    out.extend_from_slice(&magic);
    put_u32(out, version);
    put_u32(out, header_len);
}

/// Decodes and validates the framing prefix of a metadata record.
///
/// Returns the declared header length together with a cursor positioned at the
/// first record field. The caller decodes its own fields and then checks the
/// fields that follow them (checksum and reserved bytes) against its own
/// offsets.
pub(crate) fn decode_prefix<'a>(
    bytes: &'a [u8],
    magic: [u8; 4],
    version: u32,
    what: &str,
) -> Result<(Cursor<'a>, usize)> {
    let mut cursor = Cursor::new(bytes);
    let stored_magic = cursor.fixed::<4>(what)?;
    if stored_magic != magic {
        return Err(corruption(format!("{what} has an invalid magic")));
    }
    let stored_version = cursor.u32(what)?;
    if stored_version != version {
        return Err(corruption(format!(
            "{what} has unsupported format version {stored_version}"
        )));
    }
    let header_len = cursor.u32(what)? as usize;
    if header_len < META_PREFIX_LEN || header_len > bytes.len() {
        return Err(corruption(format!(
            "{what} declares an impossible header length"
        )));
    }
    Ok((cursor, header_len))
}

/// Requires that a metadata record is exactly `header_len` bytes long.
pub(crate) fn ensure_header_len(bytes: &[u8], header_len: usize, what: &str) -> Result<()> {
    if bytes.len() != header_len {
        return Err(corruption(format!(
            "{what} is not the declared header length"
        )));
    }
    Ok(())
}

/// Requires every reserved byte of a metadata record to be zero.
pub(crate) fn ensure_reserved_zero(reserved: &[u8], what: &str) -> Result<()> {
    if reserved.iter().any(|byte| *byte != 0) {
        return Err(corruption(format!("{what} has non-zero reserved bytes")));
    }
    Ok(())
}

/// Monotonic per-process suffix keeping concurrent publishers from sharing a
/// staging artifact.
static NEXT_STAGING_ID: AtomicU64 = AtomicU64::new(0);

/// Returns the canonical staging path for `final_path`.
///
/// This is the fixed `<name>.tmp` sibling used by publication paths that
/// serialize their writers (for example generation metadata). [`publish`]
/// derives a per-publication unique sibling from it instead, because two
/// sessions may concurrently publish the same missing default record.
#[must_use]
pub fn staged_path(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    name.push_str(super::names::STAGING_SUFFIX);
    final_path.with_file_name(name)
}

/// Returns a staging path unique to one publication in this process.
fn unique_staged_path(final_path: &Path) -> PathBuf {
    let sequence = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
    let mut name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    name.push_str(&format!(
        ".{}.{}{}",
        std::process::id(),
        sequence,
        super::names::STAGING_SUFFIX
    ));
    final_path.with_file_name(name)
}

/// Publishes a complete metadata image atomically.
///
/// The image is staged at a publication-unique path, re-read, synchronized,
/// and renamed onto `final_path`, and the containing directory is synchronized
/// afterwards so the rename itself survives a crash. Missing parent directories
/// are created, which is what lets a generation directory appear together with
/// its metadata.
pub fn publish(final_path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = final_path.parent() {
        durable::ensure_dir(parent)?;
    }
    let staged = unique_staged_path(final_path);
    let result = (|| -> Result<()> {
        durable::stage_bytes(&staged, bytes)?;
        durable::verify_staged(&staged, bytes)?;
        durable::sync_file(&staged)?;
        durable::publish_rename(&staged, final_path)?;
        if let Some(parent) = final_path.parent() {
            durable::sync_dir(parent)?;
        }
        Ok(())
    })();
    if result.is_err() {
        // A failed in-process publication must not leave its unique artifact
        // behind; crash recovery still tolerates any `.tmp` that remains.
        durable::remove_if_exists(&staged);
    }
    result
}

/// Reads the complete image of a published metadata file.
pub fn read(path: &Path, what: &str) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            PlomidError::with_detail(
                ErrorKind::NotFound,
                format!("{what} is missing"),
                format!("path={}", path.display()),
            )
        } else {
            PlomidError::from(error)
        }
    })
}

/// Reads a metadata file, reporting absence as `Ok(None)`.
pub fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(PlomidError::from(error)),
    }
}
