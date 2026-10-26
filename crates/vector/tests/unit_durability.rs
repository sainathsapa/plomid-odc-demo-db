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
// Moved from `src/durability.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::durability::*;

#[test]
fn authority_table_covers_every_second_authority() {
    let table = authority_table();
    assert_eq!(table.len(), 5);
    let subsystems: Vec<&str> = table.iter().map(|(s, _)| *s).collect();
    for required in [
        "WAL / commit ordering",
        "MVCC visibility",
        "durable row store",
        "backup / restore",
        "recovery / GC",
    ] {
        assert!(
            subsystems.contains(&required),
            "missing second-authority entry: {required}"
        );
    }
    // No owner may point back into this crate as an authority.
    for (_, owner) in table {
        assert!(
            !owner.contains("plomid-vector"),
            "authority must not be plomid-vector itself: {owner}"
        );
    }
}

#[test]
fn derived_artifacts_are_marked_rebuildable() {
    for artifact in permitted_derived_artifacts() {
        let note = artifact.note();
        assert!(
            note.contains("NON-AUTHORITATIVE"),
            "artifact must be marked: {note}"
        );
        assert!(
            note.contains("rebuild"),
            "artifact must be rebuildable: {note}"
        );
        assert!(
            note.contains("No data loss") || note.contains("no data loss"),
            "{note}"
        );
    }
}

#[test]
fn separately_persisted_artifact_never_serves_without_rebuild() {
    assert!(!may_serve_ann(true, false));
    assert!(may_serve_ann(true, true));
}
