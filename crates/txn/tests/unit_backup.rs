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
// Moved from `src/backup.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_txn::backup::*;

#[test]
fn manifest_roundtrip_and_version_gate() {
    let manifest = BackupManifest::current(3, 100);
    let decoded = BackupManifest::decode(&manifest.encode()).expect("roundtrip");
    assert_eq!(decoded, manifest);
    let tampered = manifest
        .encode()
        .replace("backup_format=1", "backup_format=99");
    let err = BackupManifest::decode(&tampered).expect_err("must refuse");
    assert_eq!(err.kind(), ErrorKind::Unsupported);
    let corrupt = BackupManifest::decode("garbage").expect_err("must refuse");
    assert_eq!(corrupt.kind(), ErrorKind::Corruption);
}

#[test]
fn manifest_path_helpers_do_not_panic() {
    let _: std::path::PathBuf = std::path::PathBuf::from(BACKUP_MANIFEST_FILE);
}
