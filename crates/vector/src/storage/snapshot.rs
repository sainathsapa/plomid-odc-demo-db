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
#![forbid(unsafe_code)]
//! Read snapshots: an LSN watermark plus the row count it covers.
//!
//! NON-AUTHORITATIVE (see [`crate::durability`]): snapshot authority lives in
//! `plomid-txn`; this struct is legacy framing for derived images only.

use crate::{Result, VectorError};

/// Snapshot magic: ASCII `PLMS`.
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"PLMS";
/// Snapshot encoding version.
pub const SNAPSHOT_VERSION: u32 = 1;
/// Encoded length: magic(4) + version(4) + lsn(8) + row_count(8).
pub const SNAPSHOT_LEN: usize = 24;

/// A read snapshot: visible data is everything committed at or
/// below `lsn`, over `row_count` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Snapshot {
    /// Snapshot watermark LSN.
    pub lsn: u64,
    /// Row count covered by the snapshot.
    pub row_count: u64,
}

impl Snapshot {
    /// Capture a read snapshot at `lsn` covering `row_count` rows.
    #[must_use]
    pub fn new(lsn: u64, row_count: u64) -> Self {
        Self { lsn, row_count }
    }

    /// Serialize to exactly [`SNAPSHOT_LEN`] bytes (little-endian).
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(SNAPSHOT_LEN);
        out.extend_from_slice(&SNAPSHOT_MAGIC);
        out.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        out.extend_from_slice(&self.lsn.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out
    }

    /// Alias for [`Self::serialize`].
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.serialize()
    }

    /// Parse output of [`Self::serialize`].
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SNAPSHOT_LEN {
            return Err(VectorError::Corruption(format!(
                "snapshot too short: {} < {SNAPSHOT_LEN}",
                bytes.len()
            )));
        }
        if bytes[0..4] != SNAPSHOT_MAGIC {
            return Err(VectorError::Corruption("bad snapshot magic".to_string()));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap_or([0; 4]));
        if version != SNAPSHOT_VERSION {
            return Err(VectorError::Corruption(format!(
                "unsupported snapshot version {version}"
            )));
        }
        let lsn = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or([0; 8]));
        let row_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0; 8]));
        Ok(Self { lsn, row_count })
    }

    /// Alias for [`Self::deserialize`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::deserialize(bytes)
    }
}
