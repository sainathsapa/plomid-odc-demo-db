#![forbid(unsafe_code)]
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
//! Write-ahead log foundations for PLOMID.
//!
//! # Ownership
//!
//! Owns the append-only log: framing, segment rotation, the group-commit
//! barrier, checkpoint markers, and the checkpoint-aware recovery
//! pipeline. Never owns the storage image (storage), commit ordering
//! beyond LSN assignment (txn), or mount orchestration (the txn engine
//! calls recovery; it does not live here).

pub mod checkpoint_recovery;
pub mod format;
pub mod reader;
pub mod recovery;
pub mod segmented;
pub mod writer;

pub use checkpoint_recovery::{
    durable_lsn, recover_into, recover_storage, replay_after_checkpoint, select_checkpoint,
    validate_checkpoint_against_storage, validate_wal_environment, CheckpointReplayReport,
    CheckpointSelection, RecoveryState,
};
pub use format::{
    frame_len_for_payload, Record, RecordType, MAX_PAYLOAD_SIZE, WAL_HEADER_SIZE,
    WAL_RECORD_VERSION,
};
pub use reader::{TailState, WalReader};
pub use recovery::{
    encode_abort, encode_begin, encode_commit, encode_commit_with_timestamp, encode_data, recover,
    recover_segmented, recover_target, replay_directory, replay_records, truncate_crash_tail,
    DataOperation, RecoveryReport, ReplayHandler, ReplayTarget, SegmentedReplayTarget,
};
pub use segmented::{
    read_checkpoint_marker, SegmentHeader, SegmentHeaderSize, SegmentMagic, SegmentVersion,
    SegmentedWal, WalConfig, CHECKPOINT_MARKER_NAME, DEFAULT_WAL_SEGMENT_SIZE_BYTES, WAL_DIR_NAME,
};
pub use writer::{AppendLocation, DurabilityMode, SharedWal, WalWriter, LOCATION_SIZE};
