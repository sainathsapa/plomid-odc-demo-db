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
//! Bounds-checked little-endian decoding helpers for persisted metadata.
//!
//! Every durable metadata format in this crate reads persisted bytes through
//! [`Cursor`]. The cursor never indexes beyond the buffer it was created with:
//! a short or malformed image produces a structured error instead of a panic,
//! which is what lets corrupt catalog, generation, and pointer files be
//! rejected safely during recovery.

use plomid_core::{ErrorKind, PlomidError, Result};

/// Builds a corruption error for malformed persisted metadata.
pub(crate) fn corruption(message: impl Into<String>) -> PlomidError {
    PlomidError::new(ErrorKind::Corruption, message)
}

/// Builds an invalid-argument error for a rejected request.
pub(crate) fn invalid(message: impl Into<String>) -> PlomidError {
    PlomidError::new(ErrorKind::InvalidArgument, message)
}

/// Returns `count * stride` when it is representable, otherwise `None`.
///
/// Persisted counts are validated against the number of remaining bytes before
/// any allocation, so a corrupt count can never trigger a huge allocation or an
/// arithmetic overflow.
pub fn checked_len(count: usize, stride: usize) -> Option<usize> {
    count.checked_mul(stride)
}

/// A bounds-checked reader over a persisted little-endian byte image.
pub struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    /// Creates a cursor positioned at the start of `bytes`.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// Number of bytes consumed so far.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Number of bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    /// Returns true when every byte of the image has been consumed.
    pub fn is_empty(&self) -> bool {
        self.position >= self.bytes.len()
    }

    fn take(&mut self, len: usize, what: &str) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| corruption(format!("{what} length overflow")))?;
        let slice = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| corruption(format!("{what} is truncated")))?;
        self.position = end;
        Ok(slice)
    }

    /// Reads one unsigned byte.
    pub fn u8(&mut self, what: &str) -> Result<u8> {
        Ok(self.take(1, what)?[0])
    }

    /// Reads a little-endian `u32`.
    pub fn u32(&mut self, what: &str) -> Result<u32> {
        let slice = self.take(4, what)?;
        let array: [u8; 4] = slice
            .try_into()
            .map_err(|_| corruption(format!("{what} is truncated")))?;
        Ok(u32::from_le_bytes(array))
    }

    /// Reads a little-endian `u64`.
    pub fn u64(&mut self, what: &str) -> Result<u64> {
        let slice = self.take(8, what)?;
        let array: [u8; 8] = slice
            .try_into()
            .map_err(|_| corruption(format!("{what} is truncated")))?;
        Ok(u64::from_le_bytes(array))
    }

    /// Reads a fixed-size byte array.
    pub fn fixed<const N: usize>(&mut self, what: &str) -> Result<[u8; N]> {
        let slice = self.take(N, what)?;
        slice
            .try_into()
            .map_err(|_| corruption(format!("{what} is truncated")))
    }

    /// Requires that the whole image has been consumed.
    pub fn ensure_end(&self, what: &str) -> Result<()> {
        if !self.is_empty() {
            return Err(corruption(format!("{what} has unexpected trailing data")));
        }
        Ok(())
    }
}

/// Appends a little-endian `u32`.
pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Appends a little-endian `u64`.
pub fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
