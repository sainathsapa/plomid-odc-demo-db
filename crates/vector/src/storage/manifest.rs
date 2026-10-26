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
//! Checkpoint manifest: format version, checkpoint LSN, and the
//! source table/column identity a vector payload belongs to.
//!
//! NON-AUTHORITATIVE (see [`crate::durability`]): this manifest describes a
//! rebuildable derived image only. Checkpoint/generation authority lives in
//! the storage engine; deleting this manifest must force a rebuild from
//! authoritative rows, never data loss.

use crate::{Result, VectorError};

/// Manifest magic: ASCII `PLMM`.
pub const MANIFEST_MAGIC: [u8; 4] = *b"PLMM";
/// Current manifest encoding version.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;
/// Upper bound for table/column identifier bytes (DoS guard).
pub const MAX_IDENT_LEN: usize = 1024;

/// Checkpoint manifest for one vector column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorManifest {
    /// Encoding version (must equal [`MANIFEST_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Checkpoint LSN: deltas at or below this LSN are durable.
    pub checkpoint_lsn: u64,
    /// Source table identity (e.g. `public.docs`).
    pub table: String,
    /// Source column identity (e.g. `embedding`).
    pub column: String,
    /// Expected dense dimensionality (0 = not enforced here).
    pub dims: u32,
    /// Row count at checkpoint time.
    pub row_count: u64,
}

impl VectorManifest {
    /// Build a manifest for `table.column` at `checkpoint_lsn`.
    #[must_use]
    pub fn new(
        checkpoint_lsn: u64,
        table: String,
        column: String,
        dims: u32,
        row_count: u64,
    ) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            checkpoint_lsn,
            table,
            column,
            dims,
            row_count,
        }
    }

    /// Serialize with explicit little-endian integers and
    /// `u32`-length-prefixed UTF-8 identifiers.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let t = self.table.as_bytes();
        let c = self.column.as_bytes();
        let mut out = Vec::with_capacity(4 + 4 + 8 + 4 + 8 + 4 + t.len() + 4 + c.len());
        out.extend_from_slice(&MANIFEST_MAGIC);
        out.extend_from_slice(&self.format_version.to_le_bytes());
        out.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        out.extend_from_slice(&self.dims.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&(t.len() as u32).to_le_bytes());
        out.extend_from_slice(t);
        out.extend_from_slice(&(c.len() as u32).to_le_bytes());
        out.extend_from_slice(c);
        out
    }

    /// Parse output of [`Self::to_bytes`], validating magic/version.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut off = 0usize;
        let take = |bytes: &[u8], off: &mut usize, n: usize| -> Result<Vec<u8>> {
            if bytes.len() < *off + n {
                return Err(VectorError::Corruption("manifest truncated".to_string()));
            }
            let v = bytes[*off..*off + n].to_vec();
            *off += n;
            Ok(v)
        };
        let magic = take(bytes, &mut off, 4)?;
        if magic != MANIFEST_MAGIC {
            return Err(VectorError::Corruption("bad manifest magic".to_string()));
        }
        let u32_at = |bytes: &[u8], off: &mut usize| -> Result<u32> {
            let v = take(bytes, off, 4)?;
            Ok(u32::from_le_bytes(v.try_into().unwrap_or([0; 4])))
        };
        let u64_at = |bytes: &[u8], off: &mut usize| -> Result<u64> {
            let v = take(bytes, off, 8)?;
            Ok(u64::from_le_bytes(v.try_into().unwrap_or([0; 8])))
        };
        let format_version = u32_at(bytes, &mut off)?;
        if format_version != MANIFEST_FORMAT_VERSION {
            return Err(VectorError::Corruption(format!(
                "unsupported manifest version {format_version}"
            )));
        }
        let checkpoint_lsn = u64_at(bytes, &mut off)?;
        let dims = u32_at(bytes, &mut off)?;
        let row_count = u64_at(bytes, &mut off)?;
        let table_len = u32_at(bytes, &mut off)? as usize;
        if table_len > MAX_IDENT_LEN {
            return Err(VectorError::Corruption(
                "manifest table ident too long".to_string(),
            ));
        }
        let table_raw = take(bytes, &mut off, table_len)?;
        let column_len = u32_at(bytes, &mut off)? as usize;
        if column_len > MAX_IDENT_LEN {
            return Err(VectorError::Corruption(
                "manifest column ident too long".to_string(),
            ));
        }
        let column_raw = take(bytes, &mut off, column_len)?;
        let table = String::from_utf8(table_raw)
            .map_err(|_| VectorError::Corruption("manifest table not utf-8".to_string()))?;
        let column = String::from_utf8(column_raw)
            .map_err(|_| VectorError::Corruption("manifest column not utf-8".to_string()))?;
        Ok(Self {
            format_version,
            checkpoint_lsn,
            table,
            column,
            dims,
            row_count,
        })
    }
}
