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
//! The persisted value stream: a self-delimiting sequence of value entries.
//!
//! Every logical row contributes `[length u32 little-endian][value bytes]`, in
//! ordinal order, exactly as version-1 columnar segments have always written a
//! chunk payload. NULL state is not represented here; it stays authoritative in
//! the column's null bitmap, so a zero-length entry on a non-NULL row is an
//! empty value rather than a NULL.

use crate::format;
use crate::layout::{corruption, invalid};
use plomid_core::Result;

/// Walks the value entries of a chunk payload with full bounds checking.
#[derive(Debug)]
pub struct EntryReader<'a> {
    raw: &'a [u8],
    cursor: usize,
    remaining: u64,
}

impl<'a> EntryReader<'a> {
    /// Creates a reader that must observe exactly `value_count` entries.
    #[must_use]
    pub fn new(raw: &'a [u8], value_count: u64) -> Self {
        Self {
            raw,
            cursor: 0,
            remaining: value_count,
        }
    }

    /// Returns the next entry, or `None` once `value_count` entries were read.
    pub fn next_entry(&mut self) -> Result<Option<&'a [u8]>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let prefix = format::COLUMNAR_VALUE_LENGTH_PREFIX;
        let prefix_end = self
            .cursor
            .checked_add(prefix)
            .ok_or_else(|| corruption("value entry length prefix overflows"))?;
        let length_bytes = self
            .raw
            .get(self.cursor..prefix_end)
            .ok_or_else(|| corruption("value entry length prefix is truncated"))?;
        let length = u32::from_le_bytes(
            length_bytes
                .try_into()
                .map_err(|_| corruption("value entry length prefix is truncated"))?,
        ) as usize;
        let end = prefix_end
            .checked_add(length)
            .ok_or_else(|| corruption("value entry length overflows the address space"))?;
        let value = self
            .raw
            .get(prefix_end..end)
            .ok_or_else(|| corruption("value entry lies outside the chunk payload"))?;
        self.cursor = end;
        self.remaining -= 1;
        Ok(Some(value))
    }

    /// Requires the stream to hold exactly `value_count` entries and no more.
    pub fn finish(self) -> Result<()> {
        if self.remaining != 0 {
            return Err(corruption(
                "chunk payload holds fewer value entries than its row count",
            ));
        }
        if self.cursor != self.raw.len() {
            return Err(corruption(
                "chunk payload holds trailing bytes after its value entries",
            ));
        }
        Ok(())
    }
}

/// Validates that `raw` is exactly `value_count` well-formed value entries.
pub fn validate_stream(raw: &[u8], value_count: u64) -> Result<()> {
    let mut reader = EntryReader::new(raw, value_count);
    while reader.next_entry()?.is_some() {}
    reader.finish()
}

/// Appends one value entry to `out`, returning the entry's byte length.
pub fn append_entry(out: &mut Vec<u8>, value: &[u8]) -> Result<usize> {
    let length = u32::try_from(value.len()).map_err(|_| {
        invalid(format!(
            "value of {} bytes exceeds the maximum encodable length",
            value.len()
        ))
    })?;
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(value);
    Ok(format::COLUMNAR_VALUE_LENGTH_PREFIX + value.len())
}

/// Returns the byte length one value entry contributes, failing on overflow.
pub fn entry_len(value_len: usize) -> Result<usize> {
    format::COLUMNAR_VALUE_LENGTH_PREFIX
        .checked_add(value_len)
        .ok_or_else(|| corruption("value entry length overflows"))
}
