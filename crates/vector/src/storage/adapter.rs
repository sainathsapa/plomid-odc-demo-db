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
//! Storage adapter: row-oriented vector payload access with stable
//! `u64` row ids, plus an in-memory implementation for tests.
//!
//! NON-AUTHORITATIVE trait (see [`crate::durability`]): implementors hold
//! derived, rebuildable images only. The production SQL path reads
//! authoritative rows through the storage engine, never through this trait.
//!
//! Row bytes are the self-describing standalone encoding from
//! [`crate::codec`] (`encode_standalone` / `decode_standalone`,
//! magic + version + CRC framing); the adapter adds stable row ids,
//! optional dense-dimension enforcement, and batched scans.

use crate::codec::{decode_standalone, encode_standalone};
use crate::r#type::VectorKind;
use crate::{Result, VectorError, VectorValue};

/// Row-oriented vector storage with stable `u64` row ids.
///
/// Row ids are append-stable: `append_*` returns `row_count()` before
/// the append, and ids are never reused by this trait.
pub trait VectorStorageAdapter {
    /// Number of rows (live slots; ids are `0..row_count`).
    fn row_count(&self) -> u64;

    /// Expected dense dims, if the adapter enforces one.
    fn dims(&self) -> Option<u32> {
        None
    }

    /// Encode a value to row bytes (validates dense dims when enforced).
    fn encode_payload(&self, value: &VectorValue) -> Result<Vec<u8>> {
        if let Some(d) = self.dims() {
            if value.kind() == VectorKind::Dense && value.dimension() != d {
                return Err(VectorError::DimensionMismatch {
                    expected: d,
                    got: value.dimension(),
                });
            }
        }
        encode_standalone(value)
    }

    /// Decode row bytes to a value.
    fn decode_payload(&self, row_bytes: &[u8]) -> Result<VectorValue> {
        decode_standalone(row_bytes)
    }

    /// Raw row bytes for `row_id`.
    fn get_row_bytes(&self, row_id: u64) -> Result<Vec<u8>>;

    /// Overwrite raw row bytes for `row_id`.
    fn put_row_bytes(&mut self, row_id: u64, row_bytes: &[u8]) -> Result<()>;

    /// Append raw row bytes, returning the new stable row id.
    fn append_row_bytes(&mut self, row_bytes: &[u8]) -> Result<u64>;

    /// Decode the payload stored at `row_id`.
    fn read_payload(&self, row_id: u64) -> Result<VectorValue> {
        let raw = self.get_row_bytes(row_id)?;
        self.decode_payload(&raw)
    }

    /// Encode and overwrite the payload at `row_id`.
    fn write_payload(&mut self, row_id: u64, value: &VectorValue) -> Result<()> {
        let raw = self.encode_payload(value)?;
        self.put_row_bytes(row_id, &raw)
    }

    /// Encode and append, returning the new stable row id.
    fn append_payload(&mut self, value: &VectorValue) -> Result<u64> {
        let raw = self.encode_payload(value)?;
        self.append_row_bytes(&raw)
    }

    /// Scan `[start_row, start_row + limit)` decoding each payload.
    /// Returns the `(row_id, value)` pairs actually read.
    fn scan_batch(&self, start_row: u64, limit: usize) -> Result<Vec<(u64, VectorValue)>>;
}

pub(crate) fn bounds_err(what: &str) -> VectorError {
    VectorError::IndexParameterInvalid(what.to_string())
}

/// In-memory adapter for tests and small working sets.
#[derive(Debug, Clone, Default)]
pub struct InMemoryVectorStorage {
    rows: Vec<Vec<u8>>,
    dims: Option<u32>,
}

impl InMemoryVectorStorage {
    /// Empty store, optionally enforcing dense `dims`.
    #[must_use]
    pub fn new(dims: Option<u32>) -> Self {
        Self {
            rows: Vec::new(),
            dims,
        }
    }

    fn check_row(&self, row_id: u64) -> Result<usize> {
        let idx = usize::try_from(row_id).map_err(|_| bounds_err("row_id overflow"))?;
        if idx >= self.rows.len() {
            return Err(bounds_err(&format!(
                "row_id {row_id} out of bounds ({})",
                self.rows.len()
            )));
        }
        Ok(idx)
    }
}

impl VectorStorageAdapter for InMemoryVectorStorage {
    fn row_count(&self) -> u64 {
        self.rows.len() as u64
    }

    fn dims(&self) -> Option<u32> {
        self.dims
    }

    fn get_row_bytes(&self, row_id: u64) -> Result<Vec<u8>> {
        let idx = self.check_row(row_id)?;
        Ok(self.rows[idx].clone())
    }

    fn put_row_bytes(&mut self, row_id: u64, row_bytes: &[u8]) -> Result<()> {
        // Validate encoding before storing.
        let _ = decode_standalone(row_bytes)?;
        let idx = self.check_row(row_id)?;
        self.rows[idx] = row_bytes.to_vec();
        Ok(())
    }

    fn append_row_bytes(&mut self, row_bytes: &[u8]) -> Result<u64> {
        let _ = decode_standalone(row_bytes)?;
        let id = self.rows.len() as u64;
        self.rows.push(row_bytes.to_vec());
        Ok(id)
    }

    fn scan_batch(&self, start_row: u64, limit: usize) -> Result<Vec<(u64, VectorValue)>> {
        let start = usize::try_from(start_row).map_err(|_| bounds_err("start_row overflow"))?;
        if start > self.rows.len() {
            return Err(bounds_err("start_row out of bounds"));
        }
        let end = (start + limit).min(self.rows.len());
        let mut out = Vec::with_capacity(end - start);
        for (i, raw) in self.rows[start..end].iter().enumerate() {
            out.push((start_row + i as u64, decode_standalone(raw)?));
        }
        Ok(out)
    }
}
