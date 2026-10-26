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
//! Versioned WAL segment header.
//!
//! Every segment file starts with a fixed 32-byte header, encoded explicitly
//! little-endian (never a raw Rust struct):
//!
//! ```text
//! magic[4] = PLWS | version[u32] = 1 | sequence[u64] | first_lsn[u64]
//! reserved[u32] = 0 | crc32c[u32]
//! ```
//!
//! The trailing CRC32C covers the preceding 28 bytes using the shared
//! `crc32c` component (Castagnoli, initial `0xFFFFFFFF`, final XOR
//! `0xFFFFFFFF`). Readers validate magic, version, and checksum before
//! trusting sequence or LSN fields. Ordering across a directory is derived
//! from these validated headers, never from filesystem listing order alone.

use plomid_core::{ErrorKind, Lsn, PlomidError, Result, TxnId};
use plomid_storage::{compute_checksum, verify_checksum};
use std::collections::BTreeSet;

/// Segment constants are defined once in `plomid_core::constants` and
/// re-exported here under module-local names.
pub use plomid_core::{
    SEGMENT_HEADER_SIZE, SEGMENT_HEADER_SIZE as SegmentHeaderSize, SEGMENT_MAGIC,
    SEGMENT_MAGIC as SegmentMagic, SEGMENT_VERSION, SEGMENT_VERSION as SegmentVersion,
};

/// Validated segment identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// Monotonic segment sequence number (1-based).
    pub sequence: u64,
    /// First LSN stored in this segment.
    pub first_lsn: Lsn,
}

impl SegmentHeader {
    /// Encodes the header deterministically.
    #[must_use]
    pub fn encode(self) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut bytes = [0_u8; SEGMENT_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&SEGMENT_MAGIC);
        bytes[4..8].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.sequence.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.first_lsn.get().to_le_bytes());
        bytes[24..28].copy_from_slice(&0_u32.to_le_bytes());
        let checksum = compute_checksum(&bytes[..28]);
        bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Decodes and validates a header; rejects bad magic, version, reserved
    /// bytes, zero sequence, zero first LSN, or checksum mismatch.
    pub fn decode(bytes: &[u8; SEGMENT_HEADER_SIZE]) -> Result<Self> {
        if bytes[0..4] != SEGMENT_MAGIC {
            return Err(corruption("invalid WAL segment magic"));
        }
        let version = u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_| corruption("invalid WAL segment version"))?,
        );
        if version != SEGMENT_VERSION {
            return Err(corruption("unsupported WAL segment version"));
        }
        let sequence = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| corruption("invalid WAL segment sequence"))?,
        );
        if sequence == 0 {
            return Err(corruption("WAL segment sequence is zero"));
        }
        let first_lsn = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| corruption("invalid WAL segment LSN"))?,
        );
        if first_lsn == 0 {
            return Err(corruption("WAL segment first LSN is zero"));
        }
        if bytes[24..28] != [0, 0, 0, 0] {
            return Err(corruption("WAL segment has non-zero reserved bytes"));
        }
        let expected = u32::from_le_bytes(
            bytes[28..32]
                .try_into()
                .map_err(|_| corruption("invalid WAL segment checksum"))?,
        );
        verify_checksum(&bytes[..28], expected)?;
        Ok(Self {
            sequence,
            first_lsn: Lsn::new(first_lsn),
        })
    }
}

fn corruption(message: &'static str) -> PlomidError {
    PlomidError::new(ErrorKind::Corruption, message)
}

// WAL segment set with one globally increasing LSN stream.
//
// # Layout
//
// Segments live under the WAL directory (conventionally `<data>/wal/`) as
// `WAL-<sequence>.dat` with zero-padded 12-digit, 1-based sequences, e.g.
// `WAL-000000000001.dat`. Each segment starts with a versioned
// [`SegmentHeader`]; readers order segments by the
// validated header sequence, never by filesystem directory listing order.
//
// # Rotation
//
// A new segment is created before writing a record that would not fit into
// the remaining space of the active segment. A record never splits across
// segments; if a single record cannot fit an otherwise valid segment, appends
// fail with a structured error rather than writing a partial frame.

use crate::format::frame_len_for_payload;
use crate::{DurabilityMode, RecordType, WalReader, WalWriter};
use plomid_core::{ObjectId, TxId};
use plomid_storage::sync_dir;
use std::path::{Path, PathBuf};

#[cfg(unix)]
fn restrict_dir(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
}

#[cfg(not(unix))]
fn restrict_dir(_path: &Path) {}

#[cfg(unix)]
fn restrict_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_file(_path: &Path) {}

/// WAL naming and boundary constants are defined once in
/// `plomid_core::constants`; module-local aliases keep the body unchanged.
pub use plomid_core::{
    CHECKPOINT_MARKER_NAME, DEFAULT_WAL_SEGMENT_SIZE_BYTES, WAL_DIR_NAME,
    WAL_NEW_PREFIX as NEW_PREFIX, WAL_NEW_SUFFIX as NEW_SUFFIX,
};

/// Tunables controlling segment creation and durability.
#[derive(Clone, Copy, Debug)]
pub struct WalConfig {
    /// Maximum physical size of one segment file, header included.
    pub max_segment_bytes: u64,
    /// Durability mode for the writers.
    pub mode: DurabilityMode,
}

impl WalConfig {
    /// Production configuration: [`DEFAULT_WAL_SEGMENT_SIZE_BYTES`] segments,
    /// fsync on commit.
    ///
    /// The rotation size matches what the lifecycle defaults hand to the
    /// running server, so this constructor and the deployed configuration
    /// agree instead of offering two different "production" sizes.
    #[must_use]
    pub fn production() -> Self {
        Self {
            max_segment_bytes: DEFAULT_WAL_SEGMENT_SIZE_BYTES,
            mode: DurabilityMode::Force,
        }
    }

    /// Explicit configuration; used by tests to force tiny segments.
    #[must_use]
    pub fn with_segment_size(max_segment_bytes: u64, mode: DurabilityMode) -> Self {
        Self {
            max_segment_bytes: max_segment_bytes.max(1),
            mode,
        }
    }
}

/// Durable WAL files rotated by byte size. Each file is independently framed
/// and validated; LSNs continue monotonically across the whole set.
pub struct SegmentedWal {
    root: PathBuf,
    max_segment_bytes: u64,
    mode: DurabilityMode,
    segments: Vec<(u64, WalWriter)>,
    next_lsn: Lsn,
    /// WAL bytes appended through this handle (frame bytes, counted on every
    /// append whatever the caller). The checkpoint policy reads its byte signal
    /// from here rather than from a commit-path hook: the engine has more than
    /// one commit path (transactional and concurrent), and a counter that lives
    /// in only one of them silently never fires for writes through the other.
    appended_bytes: u64,
    /// Value of [`Self::appended_bytes`] when the newest checkpoint marker was
    /// published; the difference is the WAL a checkpoint could still reclaim.
    checkpoint_bytes: u64,
    /// Transactions that have begun in this process but not yet
    /// committed/aborted. Rotation is deferred while this is non-zero, so every
    /// sealed segment ends at a transaction boundary.
    ///
    /// That invariant is what makes whole-file reclamation safe: recovery
    /// rejects a `Data`/`Commit` whose `Begin` is gone, so a retained segment
    /// that opened with the tail of a transaction whose `Begin` sat in a removed
    /// segment would make the database unopenable. Sealing only between
    /// transactions means a segment is either retained with all of its
    /// transactions, or removed with all of them.
    open_transactions: usize,
}

impl SegmentedWal {
    /// Creates a fresh WAL directory with segment 1.
    pub fn create(root: &Path, max_segment_bytes: u64, mode: DurabilityMode) -> Result<Self> {
        Self::create_with_config(root, WalConfig::with_segment_size(max_segment_bytes, mode))
    }

    /// Creates a fresh WAL directory from explicit configuration.
    pub fn create_with_config(root: &Path, config: WalConfig) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(PlomidError::from)?;
        restrict_dir(root);
        let mut wal = Self {
            root: root.to_path_buf(),
            max_segment_bytes: config.max_segment_bytes.max(1),
            mode: config.mode,
            segments: Vec::new(),
            next_lsn: Lsn::new(1),
            appended_bytes: 0,
            checkpoint_bytes: 0,
            open_transactions: 0,
        };
        wal.allocate(1)?;
        Ok(wal)
    }

    /// Opens an existing WAL directory, validating headers and ordering.
    pub fn open(root: &Path, max_segment_bytes: u64, mode: DurabilityMode) -> Result<Self> {
        Self::open_with_config(root, WalConfig::with_segment_size(max_segment_bytes, mode))
    }

    /// Opens an existing WAL directory from explicit configuration.
    ///
    /// Segment order is derived from validated headers (and, for a trailing
    /// unpublished file that has no complete header yet, from the validated
    /// filename), never from directory order. Gaps in the
    /// sequence are rejected: retention removes only proven-complete prefixes,
    /// so a missing sequence indicates loss rather than policy.
    pub fn open_with_config(root: &Path, config: WalConfig) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(PlomidError::from)?;
        restrict_dir(root);
        let mut found: Vec<(u64, PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(root).map_err(PlomidError::from)? {
            let entry = entry.map_err(PlomidError::from)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "checkpoint" || name == "checkpoint.tmp" {
                continue;
            }
            if entry.file_type().map_err(PlomidError::from)?.is_dir() {
                continue;
            }
            if let Some(seq) = parse_sequence(&name) {
                found.push((seq, entry.path()));
            }
        }
        if found.is_empty() {
            return Self::create_with_config(root, config);
        }
        found.sort_by_key(|(seq, _)| *seq);
        for window in found.windows(2) {
            if window[1].0 != window[0].0 + 1 {
                return Err(PlomidError::new(
                    ErrorKind::Corruption,
                    "WAL segment sequence gap",
                ));
            }
        }
        let mut wal = Self {
            root: root.to_path_buf(),
            max_segment_bytes: config.max_segment_bytes.max(1),
            mode: config.mode,
            segments: Vec::new(),
            next_lsn: Lsn::new(1),
            // A reopened WAL has no transaction in flight: whatever the crash
            // left incomplete is in the recovered tail, not something this
            // process is mid-way through.
            open_transactions: 0,
            appended_bytes: 0,
            checkpoint_bytes: 0,
        };
        // Seed the byte signal from what is already on disk, so a server that
        // starts with un-checkpointed WAL still reaches its byte threshold
        // instead of waiting for the segment backstop. With a published marker
        // the retained WAL is already covered by it, so the delta starts at 0.
        let on_disk: u64 = wal
            .segments
            .iter()
            .filter_map(|(id, _)| segment_path(&wal.root, *id).metadata().ok())
            .map(|metadata| metadata.len())
            .sum();
        wal.appended_bytes = on_disk;
        if read_checkpoint_marker(&wal.root)?.is_some() {
            wal.checkpoint_bytes = on_disk;
        }
        let mut expected_next_lsn: Option<Lsn> = None;
        for (seq, path) in &found {
            let mut reader = WalReader::open(path)?;
            let header = reader.segment_header();
            if let Some(header) = header {
                if header.sequence != *seq {
                    return Err(PlomidError::new(
                        ErrorKind::Corruption,
                        "WAL filename mismatches header sequence",
                    ));
                }
            }
            let mut last = None;
            while let Some(record) = reader.next_record()? {
                if let Some(expected) = expected_next_lsn {
                    if record.lsn != expected {
                        return Err(PlomidError::new(
                            ErrorKind::Corruption,
                            "WAL LSN discontinuity across segments",
                        ));
                    }
                }
                expected_next_lsn = Some(
                    record
                        .lsn
                        .get()
                        .checked_add(1)
                        .map(Lsn::new)
                        .ok_or_else(|| {
                            PlomidError::new(ErrorKind::Internal, "WAL LSN exhausted")
                        })?,
                );
                last = Some(record.lsn);
            }
            wal.next_lsn = last
                .map(|lsn| {
                    lsn.get()
                        .checked_add(1)
                        .map(Lsn::new)
                        .ok_or_else(|| PlomidError::new(ErrorKind::Internal, "WAL LSN exhausted"))
                })
                .transpose()?
                .unwrap_or(header.map_or(Lsn::new(1), |h| h.first_lsn));
            wal.segments
                .push((*seq, WalWriter::open_with_mode(path, config.mode)?));
        }
        Ok(wal)
    }

    /// Number of segments currently tracked.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Returns the WAL directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the tracked segment paths in validated sequence order.
    ///
    /// This is the segment set `open`/`create` already validated: sequence
    /// gaps, filename-to-header mismatches, and header corruption were rejected
    /// before this WAL was constructed. Recovery reuses this ordering instead of
    /// re-enumerating the directory, so segment order is never derived from
    /// filesystem listing order.
    #[must_use]
    pub fn segment_paths(&self) -> Vec<PathBuf> {
        self.segments
            .iter()
            .map(|(_, writer)| writer.path().to_path_buf())
            .collect()
    }

    /// Returns the configured durability mode.
    #[must_use]
    pub fn durability_mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Returns the next LSN that will be assigned.
    #[must_use]
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }
    /// Appends one record with explicit transaction and object identity.
    ///
    /// Rotates before writing when the record would not fit, so a record is
    /// always physically self-contained. If the record itself cannot fit an
    /// otherwise valid empty segment, a structured error is returned and no
    /// partial frame is written.
    pub fn append_with_ids(
        &mut self,
        record_type: RecordType,
        tx_id: TxId,
        object_id: ObjectId,
        payload: &[u8],
    ) -> Result<Lsn> {
        let frame = frame_len_for_payload(payload.len())? as u64;
        let active = self
            .segments
            .last_mut()
            .ok_or_else(|| PlomidError::new(ErrorKind::Internal, "WAL has no active segment"))?;
        let active_offset = active.1.offset();
        // Never rotate inside a transaction: a sealed segment must be
        // self-contained, or reclamation could strand a `Data`/`Commit` whose
        // `Begin` went with an earlier segment (recovery rejects that as
        // corruption). The segment grows past its target size instead, bounded
        // by the size of the transaction in flight.
        if self.open_transactions == 0
            && frame
                .checked_add(active_offset)
                .is_some_and(|end| end > self.max_segment_bytes)
        {
            let capacity = self
                .max_segment_bytes
                .checked_sub(SEGMENT_HEADER_SIZE as u64)
                .ok_or_else(|| {
                    PlomidError::new(
                        ErrorKind::InvalidArgument,
                        "WAL segment size is smaller than its header",
                    )
                })?;
            if frame > capacity {
                return Err(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    "WAL record does not fit any segment",
                ));
            }
            let next_seq = active
                .0
                .checked_add(1)
                .ok_or_else(|| PlomidError::new(ErrorKind::Internal, "WAL segment exhausted"))?;
            self.allocate(next_seq)?;
        }
        let lsn = self
            .segments
            .last_mut()
            .ok_or_else(|| PlomidError::new(ErrorKind::Internal, "WAL has no active segment"))?
            .1
            .append_with_ids(record_type, tx_id, object_id, payload)?;
        self.next_lsn = Lsn::new(
            lsn.get()
                .checked_add(1)
                .ok_or_else(|| PlomidError::new(ErrorKind::Internal, "WAL LSN exhausted"))?,
        );
        self.appended_bytes = self.appended_bytes.saturating_add(frame);
        plomid_core::observability::record_wal_append(frame);
        match record_type {
            RecordType::Begin => self.open_transactions += 1,
            // Abort closes a transaction exactly like Commit does, so both
            // reopen the window in which rotation is allowed.
            RecordType::Commit | RecordType::Abort => {
                self.open_transactions = self.open_transactions.saturating_sub(1);
            }
            _ => {}
        }
        Ok(lsn)
    }

    /// Appends one record (no explicit identity).
    pub fn append(&mut self, record_type: RecordType, payload: &[u8]) -> Result<Lsn> {
        self.append_with_ids(record_type, TxId::new(0), ObjectId::new(0), payload)
    }

    /// Group-commit barrier: makes every record through `lsn` durable.
    ///
    /// A transaction may cross a rotation boundary, so every segment is
    /// synced; the Commit marker cannot become durable while an earlier data
    /// record remains only in a producer cache.
    ///
    /// Durability follows [`DurabilityMode`], exactly like
    /// [`WalWriter::commit`]: in [`DurabilityMode::Force`] every segment is
    /// fsynced before success is reported; in [`DurabilityMode::Buffered`]
    /// this validates `lsn` and returns without fsyncing, so success claims
    /// no durability. [`Self::sync`] always fsyncs regardless of mode.
    pub fn commit(&mut self, lsn: Lsn) -> Result<()> {
        if self.segments.is_empty() {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "WAL has no active segment",
            ));
        }
        if lsn.get() == 0 || lsn.get() >= self.next_lsn.get() {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "WAL commit LSN is not appended",
            ));
        }
        if self.mode == DurabilityMode::Buffered {
            return Ok(());
        }
        self.sync()
    }

    /// Advances the durable watermark to `lsn` without issuing an fsync.
    ///
    /// Companion to [`Self::commit`] for a group durability coordinator that
    /// flushed the segment files through its own handles: the watermark only
    /// moves forward and never past the last appended record.
    pub fn mark_durable(&mut self, lsn: Lsn) {
        if let Some((_, writer)) = self.segments.last_mut() {
            writer.mark_durable(lsn);
        }
    }

    /// Returns the last appended LSN, or 0 for an empty WAL.
    #[must_use]
    pub fn last_lsn(&self) -> Lsn {
        Lsn::new(self.next_lsn.get().saturating_sub(1))
    }

    /// Returns the highest LSN known durable on the active segment's handle.
    ///
    /// In [`DurabilityMode::Force`] this advances on [`Self::commit`] and
    /// [`Self::sync`]; in [`DurabilityMode::Buffered`] only [`Self::sync`]
    /// (and rotation's header fsync) advance it, so a `Buffered` commit that
    /// reports success leaves this unchanged.
    #[must_use]
    pub fn durable_lsn(&self) -> Lsn {
        self.segments
            .last()
            .map(|(_, writer)| writer.durable_lsn())
            .unwrap_or_else(|| Lsn::new(0))
    }

    /// WAL bytes appended since the newest checkpoint marker was published.
    ///
    /// This is the automatic checkpoint policy's strongest signal: it bounds
    /// both the disk a checkpoint can reclaim and the replay a restart must
    /// perform, and unlike a commit-path counter it sees every append on every
    /// commit path.
    #[must_use]
    pub fn bytes_since_checkpoint(&self) -> u64 {
        self.appended_bytes.saturating_sub(self.checkpoint_bytes)
    }

    /// Flushes and fsyncs every open segment, regardless of durability mode.
    ///
    /// This is the unconditional durability boundary: unlike [`Self::commit`],
    /// which skips the fsync in [`DurabilityMode::Buffered`], `sync` always
    /// fsyncs, exactly like [`WalWriter::sync`].
    ///
    /// Segments with no appended-but-undurable records are skipped: their
    /// watermark already covers their last record, so a previous successful
    /// barrier made them durable and they gained no new bytes since (sealed
    /// segments are immutable; the active segment is the only one that grows).
    /// Skipping never weakens durability — it elides redundant fsyncs for
    /// sealed segments on every commit, previously one fsync per segment per
    /// commit. A failed fsync never advances the watermark, so the next
    /// barrier retries instead of skipping.
    pub fn sync(&mut self) -> Result<()> {
        // Skip recording when no segment needed an fsync: the histogram
        // describes real sync work, not no-op barrier checks.
        let mut synced_any = false;
        let started = std::time::Instant::now();
        for (_, writer) in &mut self.segments {
            // No new records since the last durable barrier: the file image is
            // already proven durable. `durable + 1 == next` also holds for a
            // freshly rotated empty segment whose header `allocate` already
            // synced.
            if writer.durable_lsn().get().saturating_add(1) >= writer.next_lsn().get() {
                continue;
            }
            writer.sync()?;
            synced_any = true;
        }
        if synced_any {
            plomid_core::observability::record_wal_sync(started.elapsed().as_micros() as u64);
        }
        Ok(())
    }
    /// Writes and durably records a checkpoint boundary.
    ///
    /// WAL files are intentionally retained until a durable checkpoint proves
    /// a prefix unreachable. This writes a Checkpoint record and, once durable,
    /// atomically publishes the checkpoint marker.
    pub fn checkpoint(&mut self) -> Result<Lsn> {
        self.checkpoint_with_payload(&[])
    }

    /// Checkpoints WAL together with allocator high-water marks.
    pub fn checkpoint_with_watermarks(
        &mut self,
        max_txn_id: u64,
        max_commit_timestamp: u64,
    ) -> Result<Lsn> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&max_txn_id.to_le_bytes());
        payload.extend_from_slice(&max_commit_timestamp.to_le_bytes());
        self.checkpoint_with_payload(&payload)
    }

    /// Checkpoints with the recovery boundary capped below the record LSN.
    ///
    /// Normally the marker names the checkpoint record itself (everything
    /// appended so far is covered). When prepared-but-unpublished commits
    /// exist, their WAL records are already appended but their data pages are
    /// not yet applied: covering them with the marker while reclaiming up to
    /// it would delete the only durable trace of commits that have not yet
    /// become visible. Capping the marker at (or below) the oldest such
    /// commit keeps those records above the replay boundary and out of
    /// reclamation. Replay past the cap is idempotent, and the checkpoint
    /// record itself (which sits above the cap) replays as a no-op, so a
    /// capped marker loses nothing except reclaimable prefix it must keep.
    /// Returns the boundary actually published (the cap).
    pub fn checkpoint_with_watermarks_capped(
        &mut self,
        max_txn_id: u64,
        max_commit_timestamp: u64,
        cap: Lsn,
    ) -> Result<Lsn> {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&max_txn_id.to_le_bytes());
        payload.extend_from_slice(&max_commit_timestamp.to_le_bytes());
        let record_lsn = self.append(RecordType::Checkpoint, &payload)?;
        self.commit(record_lsn)?;
        let boundary = cap.min(record_lsn);
        publish_checkpoint_marker(&self.root, boundary)?;
        // The byte signal still restarts from the physical end: it measures
        // appended volume, while the marker file carries the replay boundary.
        plomid_core::observability::record_wal_checkpoint_covered(
            self.appended_bytes.saturating_sub(self.checkpoint_bytes),
        );
        self.checkpoint_bytes = self.appended_bytes;
        Ok(boundary)
    }

    fn checkpoint_with_payload(&mut self, payload: &[u8]) -> Result<Lsn> {
        let lsn = self.append(RecordType::Checkpoint, payload)?;
        self.commit(lsn)?;
        publish_checkpoint_marker(&self.root, lsn)?;
        // The marker now covers everything appended so far: the byte signal
        // restarts from here.
        plomid_core::observability::record_wal_checkpoint_covered(
            self.appended_bytes.saturating_sub(self.checkpoint_bytes),
        );
        self.checkpoint_bytes = self.appended_bytes;
        Ok(lsn)
    }

    /// Appends a transaction's prepared records in one writer boundary. LSN
    /// assignment and segment rotation remain ordered, while callers avoid
    /// repeatedly crossing the WAL synchronization API for each row.
    pub fn append_batch(&mut self, records: &[(RecordType, &[u8])]) -> Result<Vec<Lsn>> {
        let mut lsns = Vec::with_capacity(records.len());
        for (record_type, payload) in records {
            lsns.push(self.append(*record_type, payload)?);
        }
        Ok(lsns)
    }

    /// Reclaims only complete, non-active segments whose last record precedes
    /// a durable retention boundary. The active segment is never removed, and
    /// metadata is updated only after each deletion succeeds.
    ///
    /// # Transaction boundary rule
    ///
    /// Reclamation removes a *contiguous prefix* of the log. A transaction may
    /// straddle a rotation: its `Begin` is appended when the transaction starts
    /// and its `Data`/`Commit` records when it commits, so a marker can sit in
    /// an earlier segment than the records it opens. Recovery replays the
    /// retained segments from their first record and rejects a `Data` or
    /// `Commit` whose `Begin` is gone ("WAL Data record has no Begin"), so a
    /// prefix ending at segment `k` is removable only when **every transaction
    /// begun in segments `1..=k` also completed within `1..=k`**.
    ///
    /// The scan therefore carries the open-transaction set across segment
    /// boundaries (a `Commit` in a later segment closes a `Begin` in an earlier
    /// one) and looks for the *last record position* at which no transaction was
    /// open. Everything before that position is removable, expressed in whole
    /// files: every sealed segment that ends before it, plus the segment that
    /// contains it — provided that segment is closed at its **own** end. The
    /// segment holding the position is otherwise retained, because the records
    /// after it (typically the `Begin` of the transaction that straddles the
    /// boundary) still need their predecessors.
    ///
    /// Requiring the *whole* prefix to be transaction-free at a segment end is
    /// not enough: in any real workload the last transaction of a segment
    /// commits in the next one, so an end-of-segment-only rule would almost never
    /// find a removable prefix and the WAL would grow without bound. Anchoring
    /// on the last closed record position instead reclaims every segment whose
    /// records all precede a transaction-free point, which is exactly the set
    /// recovery can no longer need.
    ///
    /// This is the WAL-retention analogue of PostgreSQL's oldest-running-xact
    /// rule: retention is bounded by the oldest unfinished transaction and the
    /// segment holding its completion, not by the newest record. A long
    /// transaction pins only the prefix its own records occupy, and once it
    /// commits the next checkpoint reclaims through that `Commit`, so
    /// reclamation always makes progress again.
    ///
    /// Cost is one sequential pass over the sealed segments (the records the
    /// caller is about to discard), which is why reclamation runs on checkpoint
    /// only, never per commit.
    pub fn reclaim_before(&mut self, boundary: Lsn) -> Result<usize> {
        if self.segments.len() <= 1 {
            return Ok(0);
        }
        // Transactions whose Begin has been seen without a Commit/Abort yet,
        // carried across segment boundaries while scanning forward.
        let mut open_transactions = BTreeSet::new();
        // Set when a transaction marker could not be decoded. Recovery
        // validates payloads strictly, so an undecodable marker means this
        // scan cannot prove the prefix transaction-free; the conservative
        // outcome is to stop extending the prefix, never to reclaim on an
        // assumption.
        let mut undecodable_marker = false;
        // Number of sealed segments proven removable so far. A segment joins the
        // prefix only when it is both transaction-complete (the cumulative open
        // set is empty at its end) and entirely below `boundary`; the first
        // segment that fails either condition ends the prefix, because removing
        // a later segment while keeping this one would punch a hole in the log.
        let mut prefix_len = 0usize;
        let sealed = self.segments.len() - 1;
        'segments: for (index, (id, _)) in self.segments.iter().enumerate() {
            if index >= sealed {
                break; // the active segment is never removed
            }
            let path = segment_path(&self.root, *id);
            let mut reader = WalReader::open(&path)?;
            while let Some(record) = reader.next_record()? {
                // The boundary ends what reclamation can prove: records at or
                // after it may still be needed to replay the crash tail, so the
                // scan stops there and closed points already found stay valid.
                if record.lsn >= boundary {
                    break 'segments;
                }
                match record.record_type {
                    RecordType::Begin => match transaction_marker(&record.payload) {
                        Some(txn) => {
                            open_transactions.insert(txn);
                        }
                        None => undecodable_marker = true,
                    },
                    // Abort closes a transaction exactly like Commit does: its
                    // data was never durable, so nothing in the prefix depends
                    // on its Begin.
                    RecordType::Commit | RecordType::Abort => {
                        match transaction_marker(&record.payload) {
                            Some(txn) => {
                                open_transactions.remove(&txn);
                            }
                            None => undecodable_marker = true,
                        }
                    }
                    _ => {}
                }
            }
            // The segment is removable in full only when every transaction it
            // contains also completed inside it. `open_transactions` is carried
            // across segments, so a transaction whose `Commit` lives in a later
            // segment keeps this one — and, because removal is a prefix, every
            // segment after it — retained.
            if open_transactions.is_empty() && !undecodable_marker {
                prefix_len = index + 1;
                continue;
            }
            break;
        }
        let removable: Vec<u64> = self.segments[..prefix_len]
            .iter()
            .map(|(id, _)| *id)
            .collect();
        for id in &removable {
            std::fs::remove_file(segment_path(&self.root, *id)).map_err(PlomidError::from)?;
        }
        if !removable.is_empty() {
            self.segments.retain(|(id, _)| !removable.contains(id));
        }
        Ok(removable.len())
    }

    /// Returns the last atomically persisted checkpoint marker, if present.
    pub fn checkpoint_lsn(&self) -> Result<Option<Lsn>> {
        read_checkpoint_marker(&self.root)
    }

    fn allocate(&mut self, id: u64) -> Result<()> {
        let path = segment_path(&self.root, id);
        let mut writer = WalWriter::create_segment(&path, self.mode, id, self.next_lsn)?;
        // Directory-entry durability on rotation: the group-commit barrier
        // fsyncs segment *contents* through its own handles, but a crash
        // loses the whole file when its directory entry was never synced —
        // including records an earlier barrier already acknowledged. Fsync
        // the new segment (header durable) and the WAL directory (entry
        // durable) here, before any record in the segment can be acked, in
        // every durability mode: this is metadata publication, not the
        // record-commit path that `Buffered` skips.
        writer.sync()?;
        sync_dir(&self.root)?;
        self.segments.push((id, writer));
        plomid_core::observability::record_wal_rotation();
        Ok(())
    }
}

/// Atomically publishes the WAL checkpoint marker for `boundary`.
///
/// The marker file is staged as `checkpoint.tmp`, fsynced, renamed onto
/// `checkpoint`, and only then is the WAL directory itself fsynced: the
/// rename is atomic but survives a crash only once the directory entry is
/// durable — the same tmp + file-sync + rename + dir-sync publication the
/// storage checkpoint path performs.
fn publish_checkpoint_marker(root: &Path, boundary: Lsn) -> Result<()> {
    let temporary = root.join("checkpoint.tmp");
    std::fs::write(&temporary, boundary.get().to_le_bytes()).map_err(PlomidError::from)?;
    restrict_file(&temporary);
    let file = std::fs::OpenOptions::new()
        .read(true)
        // `sync_all` needs `GENERIC_WRITE` on Windows (`FlushFileBuffers`);
        // a read-only handle is rejected with `ERROR_ACCESS_DENIED`.
        .write(true)
        .open(&temporary)
        .map_err(PlomidError::from)?;
    file.sync_all().map_err(PlomidError::from)?;
    drop(file);
    std::fs::rename(&temporary, root.join("checkpoint")).map_err(PlomidError::from)?;
    sync_dir(root)?;
    Ok(())
}

/// Decodes the transaction identity from a `Begin`/`Commit`/`Abort` payload.
///
/// All three markers carry the `txn_id` in their first 8 bytes. `Commit`
/// payloads are either 8 bytes (`encode_commit`) or 16 bytes
/// (`encode_commit_with_timestamp`, the production commit path appends the
/// MVCC timestamp); this matches `decode_commit` in `recovery.rs`, so anything
/// recovery accepts as a marker, reclamation tracks too. `None` means the
/// payload is not a recognizable marker; the caller treats that as "tracking
/// is impossible" and stops extending its removable prefix, which can only
/// pin the log longer, never reclaim something unsafe.
fn transaction_marker(payload: &[u8]) -> Option<TxnId> {
    if payload.len() == 8 || payload.len() == 16 {
        Some(TxnId::new(u64::from_le_bytes(
            payload[..8].try_into().ok()?,
        )))
    } else {
        None
    }
}

/// Returns the sequence number parsed from a `WAL-<n>.dat` filename.
pub(crate) fn parse_sequence(name: &str) -> Option<u64> {
    name.strip_prefix(NEW_PREFIX)
        .and_then(|n| n.strip_suffix(NEW_SUFFIX))
        .map(|n| n.trim_start_matches('0'))
        .map(|seq| if seq.is_empty() { "0" } else { seq })
        .and_then(|seq| seq.parse::<u64>().ok())
        .filter(|seq| *seq != 0)
}

fn segment_path(root: &Path, id: u64) -> PathBuf {
    root.join(format!("{NEW_PREFIX}{id:012}{NEW_SUFFIX}"))
}

// CHECKPOINT_MARKER_NAME is re-exported from `plomid_core` above.

/// Reads the durable WAL checkpoint boundary from a WAL directory.
///
/// The marker file is replaced atomically only after the records it describes
/// have been fsynced, so its value is the highest LSN the WAL can prove it made
/// durable. `Ok(None)` means no boundary has been published yet; a marker that
/// exists but is truncated or holds a zero LSN is corruption rather than an
/// absent boundary, because the publication path never writes either form.
pub fn read_checkpoint_marker(root: &Path) -> Result<Option<Lsn>> {
    let path = root.join(CHECKPOINT_MARKER_NAME);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(PlomidError::from)?;
    let value: [u8; 8] = bytes
        .try_into()
        .map_err(|_| PlomidError::new(ErrorKind::Corruption, "invalid segmented WAL checkpoint"))?;
    let lsn = u64::from_le_bytes(value);
    if lsn == 0 {
        return Err(PlomidError::new(
            ErrorKind::Corruption,
            "segmented WAL checkpoint has an invalid LSN",
        ));
    }
    Ok(Some(Lsn::new(lsn)))
}
