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
//! Automatic checkpoint policy for a long-running database.
//!
//! WAL segments are retained until a durable checkpoint proves a prefix
//! unreachable, so a server that never checkpoints accumulates WAL for the
//! lifetime of the process: unbounded disk use on a busy system, and a restart
//! that must replay everything since the last (possibly creation-time)
//! checkpoint. The engine owns a complete, crash-safe checkpoint
//! ([`crate::PlomidStorageEngine::checkpoint`]: flush data pages, write and
//! sync the checkpoint record, then reclaim only segments the durable
//! checkpoint proves unreachable), but nothing in the normal lifecycle ever
//! called it.
//!
//! This module adds the *policy* only. It deliberately does not introduce a
//! background thread, a timer wheel or a scheduler: the decision is a pure
//! function of cheap counters, and the caller that finishes a commit runs the
//! checkpoint when the policy says it is due. That keeps the work on the
//! committing thread (no second durability path, no lock held while deciding)
//! and keeps it testable without sleeping.
//!
//! # Signals
//!
//! * **WAL bytes appended since the last checkpoint.** The strongest signal:
//!   it bounds both the disk a checkpoint can reclaim and the replay work a
//!   restart must perform. Accounted from the commit path, which is where
//!   essentially all WAL volume originates; non-commit appends (abort markers)
//!   are covered by the segment signal below.
//! * **Retained WAL segment count.** A backstop that also catches append
//!   sources the byte counter does not see, and bounds the number of files a
//!   checkpoint has to reclaim.
//! * **Maximum interval.** Bounds the *age* of the newest checkpoint on a
//!   system with little or no write traffic, where neither counter moves.
//!   Checkpointing is cheap when there is little to flush (measured ~9 ms for
//!   a small database), so a long interval costs almost nothing.
//!
//! # Defaults
//!
//! The defaults are derived from measurements rather than chosen for feel
//! (see the report accompanying this module):
//!
//! * WAL replay throughput measured at 25–51 MB/s (`cargo bench -p
//!   plomid-storage --bench checkpoint_recovery`, small/large record shapes),
//!   so [`CheckpointPolicy::DEFAULT_WAL_BYTES`] of 64 MiB bounds the worst-case
//!   replay a restart performs to roughly 1.3–2.6 seconds of that measured
//!   throughput.
//! * WAL rotation is 8 MiB (`DEFAULT_WAL_SEGMENT_SIZE_BYTES`), so 64 MiB is
//!   about eight segments; [`CheckpointPolicy::DEFAULT_WAL_SEGMENTS`] is set to
//!   twice that as a margin, so the count only fires if byte accounting has
//!   missed a source.
//! * Measured WAL amplification of ~288 bytes per inserted row (10 000 rows in
//!   one transaction: 2 883 584 WAL bytes) means the byte default corresponds
//!   to roughly 233 000 single-row commits between checkpoints — checkpoints
//!   stay rare relative to commits, never per-commit.
//! * Checkpoint creation measured at ~9 ms per checkpoint in the same bench, so
//!   the default interval ([`CheckpointPolicy::DEFAULT_MAX_INTERVAL`], 15
//!   minutes) is affordable even for an almost-idle server.

use plomid_core::Lsn;
use std::time::{Duration, Instant};

/// What made a checkpoint due. Recorded so operators can tell a threshold
/// trigger from an explicit administrative checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointTrigger {
    /// Appended WAL bytes reached the configured threshold.
    WalBytes,
    /// Retained WAL segment count reached the configured threshold.
    WalSegments,
    /// The maximum interval since the last checkpoint elapsed.
    Interval,
    /// An explicit checkpoint call (`CHECKPOINT`, admin command, test).
    Explicit,
    /// The server is shutting down cleanly, so the last checkpoint before exit
    /// bounds what a restart has to replay.
    Shutdown,
}

impl CheckpointTrigger {
    /// Stable name for logs and diagnostics.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WalBytes => "wal_bytes",
            Self::WalSegments => "wal_segments",
            Self::Interval => "interval",
            Self::Explicit => "explicit",
            Self::Shutdown => "shutdown",
        }
    }
}

/// Configurable thresholds that decide when a checkpoint runs.
///
/// A threshold of `0` (or `None` for the interval) disables that signal; all
/// signals disabled means only explicit checkpoints run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointPolicy {
    enabled: bool,
    wal_bytes: u64,
    wal_segments: u64,
    max_interval: Option<Duration>,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            wal_bytes: Self::DEFAULT_WAL_BYTES,
            wal_segments: Self::DEFAULT_WAL_SEGMENTS,
            max_interval: Some(Self::DEFAULT_MAX_INTERVAL),
        }
    }
}

impl CheckpointPolicy {
    /// Default byte threshold (64 MiB ≈ 1.3–2.6 s of measured WAL replay).
    pub const DEFAULT_WAL_BYTES: u64 = 64 * 1024 * 1024;
    /// Default retained-segment threshold (16 × 8 MiB = 128 MiB).
    pub const DEFAULT_WAL_SEGMENTS: u64 = 16;
    /// Default maximum interval between checkpoints.
    pub const DEFAULT_MAX_INTERVAL: Duration = Duration::from_secs(15 * 60);

    /// Edge-tier WAL byte threshold (256 MiB = 4x [`Self::DEFAULT_WAL_BYTES`]).
    ///
    /// Small devices spare slow storage by checkpointing less often: a larger
    /// byte threshold bounds restart replay to roughly 5–10 s of measured
    /// WAL replay throughput instead of 1.3–2.6 s.
    pub const EDGE_WAL_BYTES: u64 = 256 * 1024 * 1024;
    /// Edge-tier retained-segment threshold (64 = 4x
    /// [`Self::DEFAULT_WAL_SEGMENTS`]).
    pub const EDGE_WAL_SEGMENTS: u64 = 64;
    /// Edge-tier maximum interval between checkpoints (60 min = 4x
    /// [`Self::DEFAULT_MAX_INTERVAL`]).
    pub const EDGE_MAX_INTERVAL: Duration = Duration::from_secs(60 * 60);

    /// Builds a policy from explicit thresholds.
    ///
    /// `wal_bytes`/`wal_segments` of `0` disable that signal; `max_interval`
    /// of `None` disables the interval signal.
    #[must_use]
    pub fn new(wal_bytes: u64, wal_segments: u64, max_interval: Option<Duration>) -> Self {
        Self {
            enabled: true,
            wal_bytes,
            wal_segments,
            max_interval,
        }
    }

    /// A policy that never triggers an automatic checkpoint.
    ///
    /// Explicit checkpoints are unaffected: the durability boundary itself
    /// never changes, only who asks for it.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            wal_bytes: 0,
            wal_segments: 0,
            max_interval: None,
        }
    }

    /// Edge-tier defaults for small devices and slow storage: 4x the server
    /// thresholds ([`Self::EDGE_WAL_BYTES`], [`Self::EDGE_WAL_SEGMENTS`],
    /// [`Self::EDGE_MAX_INTERVAL`]) so checkpoints run less often where
    /// each one costs more I/O.
    #[must_use]
    pub fn edge_defaults() -> Self {
        Self {
            enabled: true,
            wal_bytes: Self::EDGE_WAL_BYTES,
            wal_segments: Self::EDGE_WAL_SEGMENTS,
            max_interval: Some(Self::EDGE_MAX_INTERVAL),
        }
    }

    /// Tier default: [`Self::edge_defaults`] on the edge tier,
    /// [`Self::default`] (the current server values) otherwise.
    ///
    /// This is a *default*: it applies only when the operator did not set an
    /// explicit value (see the server config's explicit-flag tracking), so
    /// operator-explicit configuration always wins over the tier default.
    /// Pure over its input so callers can feed any tier source.
    #[must_use]
    pub fn for_tier(tier: plomid_hw::RuntimeTier) -> Self {
        if matches!(tier, plomid_hw::RuntimeTier::Edge) {
            Self::edge_defaults()
        } else {
            Self::default()
        }
    }

    /// Whether automatic checkpointing is enabled at all.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Configured WAL byte threshold (`0` = disabled).
    #[must_use]
    pub fn wal_bytes(&self) -> u64 {
        self.wal_bytes
    }

    /// Configured retained-segment threshold (`0` = disabled).
    #[must_use]
    pub fn wal_segments(&self) -> u64 {
        self.wal_segments
    }

    /// Configured maximum interval (`None` = disabled).
    #[must_use]
    pub fn max_interval(&self) -> Option<Duration> {
        self.max_interval
    }

    /// Returns the trigger that makes a checkpoint due, if any.
    ///
    /// `wal_bytes_since_checkpoint` and `retained_segments` are read from the
    /// WAL itself (frame bytes appended since the marker, and the number of
    /// segment files currently retained), so the signals describe the log rather
    /// than one commit path. Evaluation order is byte, segment, interval, so a
    /// report names the most informative cause.
    #[must_use]
    pub fn trigger(
        &self,
        accounting: &CheckpointAccounting,
        wal_bytes_since_checkpoint: u64,
        retained_segments: u64,
    ) -> Option<CheckpointTrigger> {
        if !self.enabled {
            return None;
        }
        if self.wal_bytes > 0 && wal_bytes_since_checkpoint >= self.wal_bytes {
            return Some(CheckpointTrigger::WalBytes);
        }
        if self.wal_segments > 0 && retained_segments >= self.wal_segments {
            return Some(CheckpointTrigger::WalSegments);
        }
        if let Some(interval) = self.max_interval {
            if accounting.last_checkpoint.elapsed() >= interval {
                return Some(CheckpointTrigger::Interval);
            }
        }
        None
    }
}

/// Live counters the policy is evaluated against.
///
/// Owned by the engine and refreshed from the WAL when the policy is evaluated,
/// so evaluating a policy performs no I/O and takes no locks of its own.
#[derive(Debug)]
pub struct CheckpointAccounting {
    /// WAL bytes appended since the last checkpoint, as observed from the WAL.
    wal_bytes_since_checkpoint: u64,
    /// When the last checkpoint completed, or the engine open time.
    last_checkpoint: Instant,
    /// Completed automatic + explicit checkpoints.
    checkpoints: u64,
    /// The trigger of the last checkpoint.
    last_trigger: Option<CheckpointTrigger>,
    /// Duration of the last checkpoint.
    last_duration: Duration,
    /// WAL bytes the last checkpoint covered.
    last_wal_bytes: u64,
    /// Segments the last checkpoint reclaimed.
    last_reclaimed_segments: usize,
    /// Last checkpoint failure, kept for diagnostics: a checkpoint failure
    /// never fails the commit that triggered it.
    last_error: Option<String>,
}

impl Default for CheckpointAccounting {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointAccounting {
    /// Starts accounting with "now" as the reference point for the interval.
    pub fn new() -> Self {
        Self {
            wal_bytes_since_checkpoint: 0,
            last_checkpoint: Instant::now(),
            checkpoints: 0,
            last_trigger: None,
            last_duration: Duration::ZERO,
            last_wal_bytes: 0,
            last_reclaimed_segments: 0,
            last_error: None,
        }
    }

    /// Mirrors the WAL's appended-byte count so statistics and the policy
    /// report the same number.
    ///
    /// The WAL owns this value: it counts every append on every commit path
    /// (transactional and concurrent, data and markers), which a counter fed by
    /// one commit path cannot. This struct records what was observed when the
    /// policy was last evaluated.
    pub fn observe_appended(&mut self, bytes_since_checkpoint: u64) {
        self.wal_bytes_since_checkpoint = bytes_since_checkpoint;
    }

    /// Records a successful checkpoint and restarts the interval window.
    pub(crate) fn record_checkpoint(
        &mut self,
        trigger: CheckpointTrigger,
        duration: Duration,
        wal_bytes: u64,
        reclaimed_segments: usize,
    ) {
        self.wal_bytes_since_checkpoint = 0;
        self.last_checkpoint = Instant::now();
        self.checkpoints += 1;
        self.last_trigger = Some(trigger);
        self.last_duration = duration;
        self.last_wal_bytes = wal_bytes;
        self.last_reclaimed_segments = reclaimed_segments;
        self.last_error = None;
    }

    /// Records a failed checkpoint. Thresholds are *not* reset, so the next
    /// commit retries instead of waiting a whole window.
    pub(crate) fn record_failure(&mut self, error: String) {
        self.last_error = Some(error);
    }

    /// WAL bytes since the last successful checkpoint.
    // #[must_use]
    // pub(crate) fn wal_bytes_since_checkpoint(&self) -> u64 {
    //     self.wal_bytes_since_checkpoint
    // }
    /// Snapshot of the accounting for diagnostics and tests.
    #[must_use]
    pub(crate) fn stats(&self) -> CheckpointStats {
        CheckpointStats {
            wal_bytes_since_checkpoint: self.wal_bytes_since_checkpoint,
            checkpoints: self.checkpoints,
            last_trigger: self.last_trigger,
            last_duration: self.last_duration,
            last_wal_bytes: self.last_wal_bytes,
            last_reclaimed_segments: self.last_reclaimed_segments,
            last_error: self.last_error.clone(),
        }
    }
}

/// Read-only view of checkpoint accounting.
#[derive(Clone, Debug)]
pub struct CheckpointStats {
    /// WAL bytes appended since the last successful checkpoint.
    pub wal_bytes_since_checkpoint: u64,
    /// Completed checkpoints (automatic and explicit).
    pub checkpoints: u64,
    /// Trigger of the most recent checkpoint.
    pub last_trigger: Option<CheckpointTrigger>,
    /// Duration of the most recent checkpoint.
    pub last_duration: Duration,
    /// WAL bytes the most recent checkpoint covered.
    pub last_wal_bytes: u64,
    /// Segments the most recent checkpoint reclaimed.
    pub last_reclaimed_segments: usize,
    /// Message of the most recent checkpoint failure, if it failed.
    pub last_error: Option<String>,
}

/// What one checkpoint did. Returned by the engine's checkpoint entry points so
/// a caller (or test) can assert on the outcome instead of inferring it.
#[derive(Clone, Copy, Debug)]
pub struct CheckpointOutcome {
    /// Why this checkpoint ran.
    pub trigger: CheckpointTrigger,
    /// Durable WAL boundary the checkpoint published.
    pub lsn: Lsn,
    /// Wall-clock duration of the whole checkpoint.
    pub duration: Duration,
    /// WAL bytes appended since the previous checkpoint.
    pub wal_bytes: u64,
    /// WAL segments reclaimed after the checkpoint became durable.
    pub reclaimed_segments: usize,
}
