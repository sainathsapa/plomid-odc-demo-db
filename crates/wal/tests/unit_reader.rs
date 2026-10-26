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
// Moved from `src/reader.rs` inline `#[cfg(test)]` — public-API only.
use plomid_wal::reader::{TailState, WalReader};
use plomid_wal::{RecordType, WalWriter};
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-wal-{label}-{}-{id}", std::process::id()))
}

#[test]
fn writes_and_reads_multiple_records_with_increasing_lsns() {
    let path = temp_path("records");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        assert_eq!(writer.append(RecordType::Begin, b"a")?.get(), 1);
        assert_eq!(writer.append(RecordType::Data, b"payload")?.get(), 2);
        writer.sync()?;
        drop(writer);
        let mut reader = WalReader::open(&path)?;
        assert!(reader.segment_header().is_some());
        let first = reader
            .next_record()?
            .ok_or_else(|| plomid_wal::reader::corruption("missing record"))?;
        let second = reader
            .next_record()?
            .ok_or_else(|| plomid_wal::reader::corruption("missing record"))?;
        assert_eq!(first.payload, b"a");
        assert_eq!(second.payload, b"payload");
        assert!(matches!(reader.tail_state()?, TailState::Clean { .. }));
        assert!(reader.next_record()?.is_none());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "WAL record test failed: {result:?}");
}

#[test]
fn checksum_failure_is_corruption() {
    let path = temp_path("checksum");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.append(RecordType::Data, b"payload")?;
        writer.sync()?;
        drop(writer);
        let mut file = fs::OpenOptions::new().write(true).open(&path)?;
        // Flip a byte in the payload region (after 32-byte header + 36-byte
        // record header), invalidating the CRC.
        file.seek(SeekFrom::Start(32 + 20))?;
        file.write_all(b"X")?;
        file.sync_all()?;
        drop(file);
        let mut reader = WalReader::open(&path)?;
        let error = reader.next_record().expect_err("corrupt WAL must fail");
        assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "WAL checksum test failed: {result:?}");
}

#[test]
fn truncated_tail_is_incomplete() {
    let path = temp_path("torn-tail");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.append(RecordType::Commit, b"complete")?;
        writer.sync()?;
        drop(writer);
        let length = fs::metadata(&path)?.len();
        let file = fs::OpenOptions::new().write(true).open(&path)?;
        file.set_len(length - 1)?;
        file.sync_all()?;
        drop(file);
        let mut reader = WalReader::open(&path)?;
        let state = reader.tail_state()?;
        match state {
            TailState::Incomplete { file_len, .. } => {
                assert_eq!(file_len, length - 1);
            }
            other => panic!("expected incomplete tail, got {other:?}"),
        }
        // A torn record after a complete one is a truncated tail, not
        // arbitrary corruption: the reader surfaces it as corruption with
        // a valid_end detail for the future recovery layer.
        let error = reader.next_record().unwrap_err();
        assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
        assert!(error.detail().is_some());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "WAL torn-tail test failed: {result:?}");
}

#[test]
fn corrupt_single_record_tail_is_rejected() {
    let path = temp_path("single-torn");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.append(RecordType::Data, b"only")?;
        writer.sync()?;
        drop(writer);
        let length = fs::metadata(&path)?.len();
        let file = fs::OpenOptions::new().write(true).open(&path)?;
        file.set_len(length - 1)?;
        file.sync_all()?;
        drop(file);
        let mut reader = WalReader::open(&path)?;
        let error = reader.next_record().unwrap_err();
        assert_eq!(error.kind(), plomid_core::ErrorKind::Corruption);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "single torn tail test failed: {result:?}");
}
