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
//! Detection of storage layouts this build does not adopt.
//!
//! Earlier storage roots used a flat layout: physical segment files under
//! `<root>/data/`, a `<root>/manifest` segment listing, a `<root>/volumes`
//! registry, generation metadata under a database-level `<root>/generation/`
//! or `<root>/generations/` directory, and the publication pointer inside
//! `<root>/catalog/CURRENT`.
//!
//! This build does not adopt that layout and does not fall back to it. Two
//! layouts cannot both be authoritative, and silently reading the old one would
//! expose a database whose logical and physical organization no longer match
//! the code that reads it. Migration is not implemented, so an old root is
//! reported as an incompatible layout and left untouched: nothing is renamed,
//! moved, rewritten, or deleted by this module.

use super::DatabaseLayout;
use plomid_core::{ErrorKind, PlomidError, Result};
use std::path::Path;

/// Refuses a database root that carries an incompatible legacy layout.
pub fn reject_legacy_layout(layout: &DatabaseLayout) -> Result<()> {
    let markers = legacy_markers(layout);
    if markers.is_empty() {
        return Ok(());
    }
    Err(PlomidError::with_detail(
        ErrorKind::Unsupported,
        "storage root uses an incompatible legacy layout; \
         automatic migration is not implemented, so the root was left unchanged",
        format!(
            "root={} markers={}",
            layout.root().display(),
            markers.join(",")
        ),
    ))
}

/// Reports whether a database root carries an incompatible legacy layout.
#[must_use]
pub fn is_legacy_layout(layout: &DatabaseLayout) -> bool {
    !legacy_markers(layout).is_empty()
}

/// Paths whose presence proves the root uses the pre-`PLOMID_DATA` layout.
fn legacy_markers(layout: &DatabaseLayout) -> Vec<String> {
    let root = layout.root();
    let mut markers = Vec::new();
    for name in [
        "data",
        "manifest",
        "manifest.tmp",
        "volumes",
        "volumes.tmp",
        "generation",
        "generations",
        "database.db",
        "storage.db",
        "generation.db",
    ] {
        let path = root.join(name);
        if path.exists() {
            markers.push(name.to_string());
        }
    }
    // The publication pointer used to live inside the catalog directory.
    if root.join("catalog").join("CURRENT").exists() {
        markers.push("catalog/CURRENT".to_string());
    }
    // A single-file database anywhere at the root.
    if let Some(name) = database_file_at_root(root) {
        markers.push(name);
    }
    markers.sort();
    markers.dedup();
    markers
}

/// Returns the name of a root-level `.db` file, if one exists.
fn database_file_at_root(root: &Path) -> Option<String> {
    let entries = std::fs::read_dir(root).ok()?;
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.ends_with(".db").then_some(name)
        })
        .collect();
    names.sort();
    names.into_iter().next()
}
