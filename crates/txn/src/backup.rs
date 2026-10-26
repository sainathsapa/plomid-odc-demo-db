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
//! Cold engine-directory backup / restore drill (Phase 12).
//!
//! Authority rule: the storage engine owns durability (WAL + checkpoint +
//! generations + catalog). A backup is a filesystem copy of the engine data
//! directory taken after a checkpoint with the engine closed (cold), plus a
//! `PLOMID_BACKUP_MANIFEST` written *last*. Restore refuses a directory
//! without a manifest (interrupted backup), refuses unknown manifest
//! versions with `Unsupported` (no silent downgrade), and refuses to
//! overwrite an existing database (restore-to-clean only).
//!
//! Excluded from the copy: `*.tmp` staged files (torn publication
//! targets) and the derived `vector_ann` cache (rebuilt from authoritative
//! rows on reopen; see `vector_durability_convergence`). Everything else —
//! row segments, catalog, checkpoints, generation pointer, WAL, columnar
//! artifacts — is copied verbatim with sizes recorded.

use plomid_core::{ErrorKind, PlomidError};
use std::path::Path;

/// Backup manifest format version. Bumped only with a migration path;
/// restore refuses anything else (fail closed, never downgrade silently).
pub const BACKUP_FORMAT_VERSION: u32 = 1;
/// Manifest filename inside the backup directory (written last).
pub const BACKUP_MANIFEST_FILE: &str = "PLOMID_BACKUP_MANIFEST";
/// Manifest magic line.
const BACKUP_MAGIC: &str = "PLOMID_BACKUP_V1";
/// Derived cache excluded from backups (rebuilt from rows).
const DERIVED_CACHE_DIR: &str = "vector_ann";

/// Parsed backup manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    /// Manifest format version (must equal [`BACKUP_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Durable format versions captured at backup time.
    pub catalog_version: u32,
    pub checkpoint_version: u32,
    pub generation_version: u32,
    pub wal_record_version: u32,
    pub segment_version: u32,
    /// Unix seconds when the backup finished.
    pub created_unix: u64,
    /// File count (excluding manifest) and total bytes.
    pub files: usize,
    pub bytes: u64,
}

impl BackupManifest {
    pub fn current(files: usize, bytes: u64) -> Self {
        Self {
            format_version: BACKUP_FORMAT_VERSION,
            catalog_version: plomid_core::constants::CATALOG_FORMAT_VERSION,
            checkpoint_version: plomid_core::constants::CHECKPOINT_FORMAT_VERSION,
            generation_version: plomid_core::constants::GENERATION_FORMAT_VERSION,
            wal_record_version: plomid_core::constants::WAL_RECORD_VERSION as u32,
            segment_version: plomid_core::constants::SEGMENT_VERSION,
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            files,
            bytes,
        }
    }

    pub fn encode(&self) -> String {
        format!(
            "{BACKUP_MAGIC}\nbackup_format={}\ncatalog={}\ncheckpoint={}\ngeneration={}\nwal_record={}\nsegment={}\ncreated_unix={}\nfiles={}\nbytes={}\n",
            self.format_version,
            self.catalog_version,
            self.checkpoint_version,
            self.generation_version,
            self.wal_record_version,
            self.segment_version,
            self.created_unix,
            self.files,
            self.bytes,
        )
    }

    pub fn decode(text: &str) -> Result<Self, PlomidError> {
        let corrupt = |detail: &str| {
            PlomidError::with_detail(
                ErrorKind::Corruption,
                "backup manifest is corrupt",
                detail.to_string(),
            )
        };
        let unsupported = |detail: String| {
            PlomidError::with_detail(ErrorKind::Unsupported, "unsupported backup version", detail)
        };
        let mut lines = text.lines();
        if lines.next() != Some(BACKUP_MAGIC) {
            return Err(corrupt("bad magic"));
        }
        let get = |key: &str| -> Result<u64, PlomidError> {
            for line in text.lines().skip(1) {
                if let Some(rest) = line.strip_prefix(key) {
                    if let Some(value) = rest.strip_prefix('=') {
                        return value.parse::<u64>().map_err(|_| corrupt("bad integer"));
                    }
                }
            }
            Err(corrupt("missing key"))
        };
        let format_version = get("backup_format")? as u32;
        if format_version != BACKUP_FORMAT_VERSION {
            return Err(unsupported(format!(
                "backup_format={format_version} != {BACKUP_FORMAT_VERSION} — migrate, never downgrade silently"
            )));
        }
        Ok(Self {
            format_version,
            catalog_version: get("catalog")? as u32,
            checkpoint_version: get("checkpoint")? as u32,
            generation_version: get("generation")? as u32,
            wal_record_version: get("wal_record")? as u32,
            segment_version: get("segment")? as u32,
            created_unix: get("created_unix")?,
            files: get("files")? as usize,
            bytes: get("bytes")?,
        })
    }
}

/// Whether `dir` holds durable engine state (any of the canonical markers).
fn has_durable_markers(dir: &Path) -> bool {
    if !dir.is_dir() {
        return false;
    }
    const MARKERS: [&str; 4] = ["CURRENT", "wal", "checkpoints", "catalog"];
    MARKERS.iter().any(|m| dir.join(m).exists())
        || std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|e| e.file_name().to_string_lossy().starts_with("catalog-"))
            })
            .unwrap_or(false)
}

fn copy_tree(
    src: &Path,
    dst: &Path,
    files: &mut usize,
    bytes: &mut u64,
) -> Result<(), PlomidError> {
    let io = |e: std::io::Error| PlomidError::from(e);
    std::fs::create_dir_all(dst).map_err(io)?;
    let entries = std::fs::read_dir(src).map_err(io)?;
    for entry in entries {
        let entry = entry.map_err(io)?;
        let name = entry.file_name();
        let name_lossy = name.to_string_lossy();
        // Torn staged targets never enter a backup.
        if name_lossy.ends_with(".tmp") {
            continue;
        }
        if name_lossy == DERIVED_CACHE_DIR {
            continue;
        }
        // Never copy a nested manifest (stale from a prior backup-as-source).
        if name_lossy == BACKUP_MANIFEST_FILE {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        let file_type = entry.file_type().map_err(io)?;
        if file_type.is_dir() {
            copy_tree(&from, &to, files, bytes)?;
        } else if file_type.is_file() {
            let meta = std::fs::metadata(&from).map_err(io)?;
            std::fs::copy(&from, &to).map_err(io)?;
            *files += 1;
            *bytes += meta.len();
        }
        // Symlinks/sockets/devices never occur in engine dirs; skip silently
        // rather than following them (jail-equivalent: never follow).
    }
    Ok(())
}

/// Cold backup: copy `src_data_dir` → `dst_backup_dir` (must not exist),
/// manifest written last via tmp+rename.
///
/// Call with the engine checkpointed and closed. Returns the manifest.
pub fn backup_database(
    src_data_dir: &Path,
    dst_backup_dir: &Path,
) -> Result<BackupManifest, PlomidError> {
    if !has_durable_markers(src_data_dir) {
        return Err(PlomidError::with_detail(
            ErrorKind::NotFound,
            "no database at backup source",
            format!("root={}", src_data_dir.display()),
        ));
    }
    if dst_backup_dir.exists() {
        return Err(PlomidError::with_detail(
            ErrorKind::AlreadyExists,
            "backup destination already exists",
            format!("root={}", dst_backup_dir.display()),
        ));
    }
    let io = PlomidError::from;
    std::fs::create_dir_all(dst_backup_dir).map_err(io)?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    copy_tree(src_data_dir, dst_backup_dir, &mut files, &mut bytes)?;
    let manifest = BackupManifest::current(files, bytes);
    let tmp = dst_backup_dir.join(format!("{BACKUP_MANIFEST_FILE}.tmp"));
    std::fs::write(&tmp, manifest.encode()).map_err(io)?;
    std::fs::rename(&tmp, dst_backup_dir.join(BACKUP_MANIFEST_FILE)).map_err(io)?;
    Ok(manifest)
}

/// Restore a backup into a clean directory. Refuses missing/corrupt
/// manifests, unknown versions (`Unsupported`), and non-empty destinations.
pub fn restore_database(
    src_backup_dir: &Path,
    dst_data_dir: &Path,
) -> Result<BackupManifest, PlomidError> {
    let manifest_path = src_backup_dir.join(BACKUP_MANIFEST_FILE);
    let text = std::fs::read_to_string(&manifest_path).map_err(|_| {
        PlomidError::with_detail(
            ErrorKind::Corruption,
            "backup has no manifest (interrupted backup?)",
            format!("root={}", src_backup_dir.display()),
        )
    })?;
    let manifest = BackupManifest::decode(&text)?;
    // Version every durable format: refuse silently reading a newer engine.
    if manifest.catalog_version != plomid_core::constants::CATALOG_FORMAT_VERSION
        || manifest.checkpoint_version != plomid_core::constants::CHECKPOINT_FORMAT_VERSION
        || manifest.generation_version != plomid_core::constants::GENERATION_FORMAT_VERSION
        || manifest.wal_record_version != plomid_core::constants::WAL_RECORD_VERSION as u32
        || manifest.segment_version != plomid_core::constants::SEGMENT_VERSION
    {
        return Err(PlomidError::with_detail(
            ErrorKind::Unsupported,
            "backup format versions do not match this binary",
            format!(
                "backup catalog={} checkpoint={} generation={} wal={} segment={}",
                manifest.catalog_version,
                manifest.checkpoint_version,
                manifest.generation_version,
                manifest.wal_record_version,
                manifest.segment_version,
            ),
        ));
    }
    if dst_data_dir.exists() && has_durable_markers(dst_data_dir) {
        return Err(PlomidError::with_detail(
            ErrorKind::AlreadyExists,
            "restore destination already holds a database",
            format!("root={}", dst_data_dir.display()),
        ));
    }
    let io = PlomidError::from;
    std::fs::create_dir_all(dst_data_dir).map_err(io)?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    copy_tree(src_backup_dir, dst_data_dir, &mut files, &mut bytes)?;
    // Remove any copied manifest from the live data dir (it is backup
    // metadata, not engine state).
    let _ = std::fs::remove_file(dst_data_dir.join(BACKUP_MANIFEST_FILE));
    Ok(manifest)
}

/// Read a backup manifest without restoring (operator inspection).
pub fn read_backup_manifest(backup_dir: &Path) -> Result<BackupManifest, PlomidError> {
    let text = std::fs::read_to_string(backup_dir.join(BACKUP_MANIFEST_FILE)).map_err(|_| {
        PlomidError::with_detail(
            ErrorKind::Corruption,
            "backup has no manifest",
            format!("root={}", backup_dir.display()),
        )
    })?;
    BackupManifest::decode(&text)
}
