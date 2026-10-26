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
// Moved from `src/segmented.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::ErrorKind;
use plomid_wal::segmented::SegmentedWal;
use plomid_wal::{DurabilityMode, RecordType, WalReader};
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn rotates_and_reopens_wal_segments() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-segments-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    // Small valid segment: 100 bytes. SEGMENT_HEADER_SIZE is 32, and a
    // "payload" record frame is 47 bytes (36 header + 7 payload + 4 CRC).
    // Usable space per segment is 68 bytes, so each segment holds exactly
    // one record and three appends force three segments.
    const TEST_SEGMENT_BYTES: u64 = 100;
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        for _ in 0..3 {
            let lsn = wal.append(RecordType::Data, b"payload")?;
            wal.commit(lsn)?;
        }
        wal.sync()?;
        assert!(wal.segment_count() > 1);
        drop(wal);
        let reopened = SegmentedWal::open(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        assert!(reopened.segment_count() > 1);
        let first = WalReader::open(&root.join("WAL-000000000001.dat"))?
            .next_record()?
            .ok_or_else(|| {
                plomid_core::PlomidError::new(ErrorKind::Corruption, "missing WAL record")
            })?;
        assert_eq!(first.lsn.get(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn reclaims_only_segments_before_durable_checkpoint() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-reclaim-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    // Use a small but valid segment size: 200 bytes.
    // SEGMENT_HEADER_SIZE is 32, leaving 168 usable bytes.
    // Each "payload" record frame is 47 bytes (36 header + 7 payload + 4 CRC).
    // 168 / 47 = 3 records fit per segment; the 4th forces rotation.
    // 6 appends + 1 checkpoint = 7 records across at least 2 segments,
    // so reclaim can remove the older segment(s).
    const TEST_SEGMENT_BYTES: u64 = 200;
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        // Write enough records to force at least one rotation.
        for _ in 0..6 {
            let lsn = wal.append(RecordType::Data, b"payload")?;
            wal.commit(lsn)?;
        }
        assert!(
            wal.segment_count() >= 2,
            "expected rotation with 6 appends into 200-byte segments"
        );
        let checkpoint = wal.checkpoint()?;
        let removed = wal.reclaim_before(checkpoint)?;
        assert!(
            removed > 0,
            "expected at least one segment reclaimed, got {}",
            removed
        );
        assert_eq!(wal.checkpoint_lsn()?, Some(checkpoint));
        drop(wal);
        let reopened = SegmentedWal::open(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        assert_eq!(reopened.segment_count(), 1);
        assert!(reopened.next_lsn().get() >= checkpoint.get());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

/// Regression: reclamation must never leave an orphan `Data`/`Commit`
/// record behind. A transaction spanning several segments (its `Begin` in
/// an early one, its `Commit` in a later one) must therefore either be
/// removed as a whole prefix or stay completely retained — the old rule
/// removed the `Begin`'s segment on its own and recovery then failed with
/// "WAL Data record has no Begin".
#[test]
fn reclaim_never_splits_a_transaction_across_segments() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-reclaim-span-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    /// Records which transaction markers replay observed.
    #[derive(Default)]
    struct SeenRecords {
        begin: bool,
        commit: bool,
    }
    impl plomid_wal::ReplayHandler for SeenRecords {
        fn on_record(&mut self, record: &plomid_wal::Record) -> plomid_core::Result<bool> {
            match record.record_type {
                RecordType::Begin => self.begin = true,
                RecordType::Commit => self.commit = true,
                _ => {}
            }
            Ok(true)
        }
    }

    // A segment target far below the transaction's own size, so the only
    // way it could be split is by rotating inside the transaction.
    const TEST_SEGMENT_BYTES: u64 = 200;
    use plomid_core::TxnId;
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        // Markers use the same payloads the engine writes: an 8-byte txn id
        // (`encode_begin`/`encode_commit`). Raw non-marker payloads would
        // be undecodable and would (correctly) pin the prefix instead.
        wal.append(RecordType::Begin, &plomid_wal::encode_begin(TxnId::new(1)))?;
        for _ in 0..6 {
            wal.append(RecordType::Data, b"payload")?;
        }
        assert_eq!(
            wal.segment_count(),
            1,
            "a transaction must never be split across segments: the segment \
             overshoots its target instead of rotating mid-transaction"
        );
        let checkpoint = wal.checkpoint()?;
        let removed = wal.reclaim_before(checkpoint)?;
        assert_eq!(
            removed, 0,
            "no segment may be reclaimed while a transaction it opened is unfinished"
        );
        // Regression assertion for the failure this test was written from:
        // with the old rule the prefix was removed and this replay failed
        // with "WAL Data record has no Begin".
        let outcome = plomid_wal::replay_directory(&root, &mut SeenRecords::default())?;
        assert!(outcome.last_lsn.is_some(), "retained WAL must replay");

        // The transaction commits AFTER the checkpoint boundary above, and
        // a second transaction is what forces the rotation that seals this
        // segment — at the transaction boundary, never inside it.
        let commit = wal.append(
            RecordType::Commit,
            &plomid_wal::encode_commit_with_timestamp(
                TxnId::new(1),
                plomid_core::CommitTimestamp::new(1),
            ),
        )?;
        wal.commit(commit)?;
        wal.append(RecordType::Begin, &plomid_wal::encode_begin(TxnId::new(2)))?;
        wal.append(RecordType::Data, b"payload")?;
        wal.append(
            RecordType::Commit,
            &plomid_wal::encode_commit(TxnId::new(2)),
        )?;
        assert!(
            wal.segment_count() >= 2,
            "rotation resumes between transactions, count={}",
            wal.segment_count()
        );
        assert_eq!(
            wal.reclaim_before(checkpoint)?,
            0,
            "a boundary taken inside a transaction pins its segment even after it commits"
        );

        // A checkpoint taken after the Commit defines a boundary the
        // transaction does not cross, so the whole closed prefix (including
        // the Begin that started it) becomes reclaimable as one unit.
        let boundary = wal.checkpoint()?;
        let removed = wal.reclaim_before(boundary)?;
        assert!(
            removed >= 1,
            "a boundary past the committing record must un-pin the prefix, got {removed}"
        );
        // Safety: the retained WAL must never start with an orphan
        // Data/Commit record, so the full retained log replays cleanly.
        let mut seen = SeenRecords::default();
        let outcome = plomid_wal::replay_directory(&root, &mut seen)?;
        assert!(outcome.last_lsn.is_some());
        drop(wal);
        // Reopening the reclaimed WAL keeps the durable boundary.
        let reopened = SegmentedWal::open(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        assert_eq!(reopened.checkpoint_lsn()?, Some(boundary));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn record_larger_than_segment_fails_without_writing() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-oversize-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, 200, DurabilityMode::Force)?;
        let big = vec![b'x'; 10 * 1024];
        let error = wal
            .append(RecordType::Data, &big)
            .expect_err("oversize must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        assert_eq!(wal.segment_count(), 1);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

/// MINOR-1: `commit` honors the durability mode exactly like
/// `WalWriter::commit`. `Buffered` validates the LSN but fsyncs nothing,
/// so success claims no durability; only `sync` (any mode) or a `Force`
/// commit advances the durable watermark.
#[test]
fn commit_honors_durability_mode() {
    use plomid_core::Lsn;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-commit-mode-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, 1024, DurabilityMode::Buffered)?;
        let lsn = wal.append(RecordType::Data, b"buffered")?;
        wal.commit(lsn)?;
        assert_eq!(
            wal.durable_lsn(),
            Lsn::new(0),
            "a Buffered commit must not advance the durable watermark"
        );
        wal.sync()?;
        assert_eq!(wal.durable_lsn(), lsn);
        // Unappended and zero LSNs are rejected in every mode.
        assert_eq!(
            wal.commit(Lsn::new(0)).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            wal.commit(Lsn::new(lsn.get() + 100)).unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
        let mut forced = SegmentedWal::create(&root.join("force"), 1024, DurabilityMode::Force)?;
        let lsn = forced.append(RecordType::Data, b"forced")?;
        forced.commit(lsn)?;
        assert_eq!(forced.durable_lsn(), lsn);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

/// MAJOR-1: the checkpoint marker is published atomically (no `.tmp`
/// left behind, marker bytes name the boundary) on both publication
/// paths, and the boundary survives a restart.
#[test]
fn checkpoint_marker_publishes_atomically_and_survives_reopen() {
    use plomid_core::Lsn;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-marker-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, 1024, DurabilityMode::Force)?;
        let first = wal.append(RecordType::Data, b"one")?;
        wal.commit(first)?;
        let boundary = wal.checkpoint()?;
        assert!(!root.join("checkpoint.tmp").exists());
        let raw = std::fs::read(root.join("checkpoint"))?;
        assert_eq!(raw, boundary.get().to_le_bytes());
        assert_eq!(wal.checkpoint_lsn()?, Some(boundary));
        // The capped path publishes its cap with the same atomicity.
        let second = wal.append(RecordType::Data, b"two")?;
        wal.commit(second)?;
        let capped = wal.checkpoint_with_watermarks_capped(1, 1, first)?;
        assert_eq!(capped, first.min(second));
        assert!(!root.join("checkpoint.tmp").exists());
        let raw = std::fs::read(root.join("checkpoint"))?;
        assert_eq!(raw, capped.get().to_le_bytes());
        assert_eq!(wal.checkpoint_lsn()?, Some(capped));
        // Simulated crash: drop without further I/O and reopen; the
        // published marker must still be the durable boundary.
        drop(wal);
        let reopened = SegmentedWal::open(&root, 1024, DurabilityMode::Force)?;
        assert_eq!(reopened.checkpoint_lsn()?, Some(capped));
        assert!(reopened.next_lsn() > Lsn::new(0));
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}

/// MAJOR-2: rotation durably publishes the new segment before any of
/// its records can be acknowledged. The header is valid immediately at
/// rotation time, and a restart observes the full rotated log.
#[test]
fn rotation_publishes_new_segment_before_records_are_acked() {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "plomid-wal-rotation-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    const TEST_SEGMENT_BYTES: u64 = 100;
    let result = (|| {
        let mut wal = SegmentedWal::create(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        let mut lsns = Vec::new();
        for _ in 0..3 {
            lsns.push(wal.append(RecordType::Data, b"payload")?);
        }
        assert!(
            wal.segment_count() > 1,
            "three 47-byte frames must rotate 100-byte segments"
        );
        // The rotated-in segment carries a valid header before any
        // commit barrier runs: its entry and header are already durable.
        let rotated = WalReader::open(&root.join("WAL-000000000002.dat"))?
            .segment_header()
            .ok_or_else(|| {
                plomid_core::PlomidError::new(
                    ErrorKind::Corruption,
                    "rotated segment has no valid header",
                )
            })?;
        assert_eq!(rotated.sequence, 2);
        // Simulated crash with no further sync: reopening must still see
        // every rotated segment and every LSN in order.
        drop(wal);
        let reopened = SegmentedWal::open(&root, TEST_SEGMENT_BYTES, DurabilityMode::Force)?;
        assert_eq!(reopened.segment_count(), 3);
        let mut expected = 1_u64;
        for path in reopened.segment_paths() {
            let mut reader = WalReader::open(&path)?;
            while let Some(record) = reader.next_record()? {
                assert_eq!(record.lsn.get(), expected);
                expected += 1;
            }
        }
        assert_eq!(expected, 4);
        Ok::<(), plomid_core::PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(root);
    assert!(result.is_ok(), "{result:?}");
}
