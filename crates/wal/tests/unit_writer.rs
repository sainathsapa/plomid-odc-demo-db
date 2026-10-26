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
// Moved from `src/writer.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_wal::writer::{AppendLocation, DurabilityMode, TestInject, WalWriter};
use plomid_wal::{RecordType, SharedWal, WalReader};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

fn temp_path(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "plomid-wal-group-{label}-{}-{id}",
        std::process::id()
    ))
}

#[test]
fn batch_commit_makes_all_records_readable_after_reopen() {
    let path = temp_path("durable");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        let lsns = writer.append_batch(&[
            (RecordType::Begin, b"begin"),
            (RecordType::Data, b"change"),
            (RecordType::Commit, b"commit"),
        ])?;
        assert_eq!(
            lsns,
            vec![
                plomid_core::Lsn::new(1),
                plomid_core::Lsn::new(2),
                plomid_core::Lsn::new(3),
            ]
        );
        assert_eq!(writer.durability_mode(), DurabilityMode::Force);
        writer.commit(*lsns.last().ok_or_else(|| {
            plomid_core::PlomidError::new(
                plomid_core::ErrorKind::InvalidArgument,
                "empty WAL batch",
            )
        })?)?;
        drop(writer);

        let mut reader = WalReader::open(&path)?;
        assert_eq!(
            reader.next_record()?.map(|record| record.payload),
            Some(b"begin".to_vec())
        );
        assert_eq!(
            reader.next_record()?.map(|record| record.payload),
            Some(b"change".to_vec())
        );
        assert_eq!(
            reader.next_record()?.map(|record| record.payload),
            Some(b"commit".to_vec())
        );
        assert!(reader.next_record()?.is_none());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "group commit test failed: {result:?}");
}

#[test]
fn segment_header_is_written_and_validated() {
    let path = temp_path("header");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.append(RecordType::Data, b"payload")?;
        writer.sync()?;
        drop(writer);
        let mut reader = WalReader::open(&path)?;
        let header = reader.segment_header().ok_or_else(|| {
            plomid_core::PlomidError::new(
                plomid_core::ErrorKind::Corruption,
                "missing segment header",
            )
        })?;
        assert_eq!(header.sequence, 1);
        assert_eq!(header.first_lsn.get(), 1);
        let record = reader.next_record()?.ok_or_else(|| {
            plomid_core::PlomidError::new(plomid_core::ErrorKind::Corruption, "missing record")
        })?;
        assert_eq!(record.lsn.get(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "segment header test failed: {result:?}");
}

#[test]
fn buffered_mode_is_explicit_and_sync_still_forces_durability() {
    let path = temp_path("buffered");
    let result = (|| {
        let mut writer = WalWriter::create_with_mode(&path, DurabilityMode::Buffered)?;
        let lsn = writer.append(RecordType::Data, b"buffered")?;
        writer.commit(lsn)?;
        writer.sync()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "buffered mode test failed: {result:?}");
}

#[test]
fn shared_wal_concurrent_append_produces_contiguous_lsns() {
    let path = temp_path("shared");
    let result = (|| {
        let wal = SharedWal::create(&path)?;
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    for i in 0..50 {
                        wal.append(RecordType::Data, format!("rec-{i}").as_bytes())
                            .unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        wal.sync()?;
        drop(wal);
        let mut reader = WalReader::open(&path)?;
        let mut expected = 1_u64;
        while let Some(record) = reader.next_record()? {
            assert_eq!(record.lsn.get(), expected, "LSN must be contiguous");
            expected += 1;
        }
        assert_eq!(expected, 201, "expected 200 records");
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "shared concurrent test failed: {result:?}");
}

#[test]
fn append_location_round_trip() {
    let loc = AppendLocation {
        sequence: 7,
        offset: 12345,
    };
    assert_eq!(AppendLocation::decode(&loc.encode()).unwrap(), loc);
}
#[test]
fn append_failure_does_not_advance_lsn_or_report_durability() {
    let path = temp_path("append-fail");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.set_inject(TestInject::FailAppend);
        let error = writer
            .append(RecordType::Data, b"will-fail")
            .expect_err("injected append must fail");
        assert_eq!(error.kind(), ErrorKind::Io);
        // LSN and durable watermark are unchanged.
        assert_eq!(writer.next_lsn().get(), 1);
        assert_eq!(writer.durable_lsn().get(), 0);
        // The writer may still operate after the transient failure.
        writer.set_inject(TestInject::None);
        let lsn = writer.append(RecordType::Data, b"ok")?;
        writer.commit(lsn)?;
        assert_eq!(writer.durable_lsn().get(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "append failure test failed: {result:?}");
}

#[test]
fn sync_failure_is_not_reported_as_durable() {
    let path = temp_path("sync-fail");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        writer.append(RecordType::Data, b"pending")?;
        writer.set_inject(TestInject::FailSync);
        let error = writer.sync().expect_err("injected sync must fail");
        assert_eq!(error.kind(), ErrorKind::Io);
        // The record was written but must NOT be reported durable.
        assert_eq!(writer.durable_lsn().get(), 0);
        // Clearing the fault and syncing again makes it durable.
        writer.set_inject(TestInject::None);
        writer.sync()?;
        assert_eq!(writer.durable_lsn().get(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "sync failure test failed: {result:?}");
}

#[test]
fn commit_fsync_failure_does_not_advance_durable_watermark() {
    let path = temp_path("commit-fail");
    let result = (|| {
        let mut writer = WalWriter::create(&path)?;
        let lsn = writer.append(RecordType::Data, b"pending")?;
        writer.set_inject(TestInject::FailSync);
        let error = writer.commit(lsn).expect_err("injected commit must fail");
        assert_eq!(error.kind(), ErrorKind::Io);
        assert_eq!(writer.durable_lsn().get(), 0);
        writer.set_inject(TestInject::None);
        writer.commit(lsn)?;
        assert_eq!(writer.durable_lsn().get(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "commit failure test failed: {result:?}");
}

#[test]
fn buffered_commit_claims_no_durability_until_sync() {
    use plomid_core::Lsn;
    let path = temp_path("buffered-watermark");
    let result = (|| {
        let mut writer = WalWriter::create_with_mode(&path, DurabilityMode::Buffered)?;
        let lsn = writer.append(RecordType::Data, b"buffered")?;
        writer.commit(lsn)?;
        assert_eq!(
            writer.durable_lsn(),
            Lsn::new(0),
            "a Buffered commit must not advance the durable watermark"
        );
        writer.sync()?;
        assert_eq!(writer.durable_lsn(), lsn);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "buffered commit test failed: {result:?}");
}

#[test]
fn flush_failure_propagates_in_buffered_mode() {
    let path = temp_path("flush-fail");
    let result = (|| {
        let mut writer = WalWriter::create_with_mode(&path, DurabilityMode::Buffered)?;
        writer.set_inject(TestInject::FailFlush);
        let error = writer.flush().expect_err("injected flush must fail");
        assert_eq!(error.kind(), ErrorKind::Io);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = fs::remove_file(&path);
    assert!(result.is_ok(), "flush failure test failed: {result:?}");
}
