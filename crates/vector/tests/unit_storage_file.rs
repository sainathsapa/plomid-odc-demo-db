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
// Moved from `src/storage/file.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::r#type::VectorElementType;
use plomid_vector::storage::file::*;
use plomid_vector::storage::VectorStorageAdapter as _;
use plomid_vector::value::DenseVectorValue;
use plomid_vector::VectorValue;
use std::path::PathBuf;

fn dense(vals: &[f32]) -> VectorValue {
    VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vals.to_vec()).unwrap(),
    )
}

fn dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "plomid-vector-filestore-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

#[test]
fn roundtrip_overwrite_scan_and_checkpoint() {
    let d = dir("roundtrip");
    let mut s =
        FileVectorStorage::create(&d, "public.docs".into(), "embedding".into(), Some(2)).unwrap();
    assert_eq!(s.append_payload(&dense(&[1.0, 2.0])).unwrap(), 0);
    assert_eq!(s.append_payload(&dense(&[3.0, 4.0])).unwrap(), 1);
    assert_eq!(s.row_count(), 2);
    s.write_payload(0, &dense(&[5.0, 6.0])).unwrap();
    assert_eq!(s.read_payload(0).unwrap(), dense(&[5.0, 6.0]));
    assert_eq!(s.scan_batch(0, 10).unwrap().len(), 2);
    assert!(s.append_payload(&dense(&[1.0])).is_err());
    s.checkpoint(99).unwrap();
    assert!(d.join(MANIFEST_FILE).exists());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn reopen_recovers_records_and_manifest_lsn() {
    let d = dir("reopen");
    let mut s = FileVectorStorage::create(&d, "t".into(), "c".into(), Some(2)).unwrap();
    s.append_payload(&dense(&[1.0, 2.0])).unwrap();
    s.append_payload(&dense(&[7.0, 8.0])).unwrap();
    s.checkpoint(42).unwrap();
    drop(s);
    let (reopened, recovery) =
        FileVectorStorage::open(&d, "t".into(), "c".into(), Some(2), false).unwrap();
    assert_eq!(
        recovery,
        FileRecovery {
            replayed_records: 2,
            manifest_lsn: 42,
            truncated_tail: false,
        }
    );
    assert_eq!(reopened.row_count(), 2);
    assert_eq!(reopened.read_payload(1).unwrap(), dense(&[7.0, 8.0]));
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn torn_tail_truncates_instead_of_corrupting() {
    let d = dir("torn");
    let mut s = FileVectorStorage::create(&d, "t".into(), "c".into(), Some(1)).unwrap();
    s.append_payload(&dense(&[1.0])).unwrap();
    // Simulate a crash mid-append: a partial length prefix.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(d.join(RECORDS_FILE))
            .unwrap();
        f.write_all(&[0xFF, 0xFF]).unwrap();
        f.flush().unwrap();
    }
    drop(s);
    let (reopened, recovery) =
        FileVectorStorage::open(&d, "t".into(), "c".into(), Some(1), false).unwrap();
    assert!(recovery.truncated_tail);
    assert_eq!(reopened.row_count(), 1);
    assert_eq!(reopened.read_payload(0).unwrap(), dense(&[1.0]));
    let _ = std::fs::remove_dir_all(&d);
}
