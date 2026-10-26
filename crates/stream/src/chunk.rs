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

//! Bounded chunking for large `bytea`/`text`/`jsonb` values.
//!
//! A multi-megabyte value must never be copied wholesale into the transport
//! path. [`ChunkedValue`] borrows the value's canonical bytes and hands out
//! bounded slices (default 64 KiB) via [`ChunkedValue::next_chunk`], so only
//! the active chunk is in RAM. Callers must stream chunks (write each to the
//! wire as it arrives); collecting all chunks re-materializes the value and
//! defeats the purpose.
//!
//! This helper is generic over existing byte slices — it introduces no new
//! BLOB or vector types.
//!
//! ```
//! use plomid_stream::ChunkedValue;
//!
//! let mut value = ChunkedValue::from_text("hello world");
//! assert_eq!(value.next_chunk(), Some(&b"hello world"[..]));
//! assert_eq!(value.next_chunk(), None);
//! ```

/// Borrowed large value split into bounded chunks for streaming.
#[derive(Debug, Clone, Copy)]
pub struct ChunkedValue<'a> {
    data: &'a [u8],
    chunk_size: usize,
    position: usize,
}

impl<'a> ChunkedValue<'a> {
    /// Default transport chunk: 64 KiB.
    pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

    /// Chunk `data` with the default 64 KiB quantum.
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            chunk_size: Self::DEFAULT_CHUNK_SIZE,
            position: 0,
        }
    }

    /// Chunk a text value's bytes (UTF-8 boundaries are *not* preserved:
    /// chunk edges may split a code point; reassemble before decoding).
    pub fn from_text(text: &'a str) -> Self {
        Self::new(text.as_bytes())
    }

    /// Chunk `data` with a custom quantum. Values of `0` are clamped to `1`
    /// (never panic).
    pub fn with_chunk_size(data: &'a [u8], chunk_size: usize) -> Self {
        Self {
            data,
            chunk_size: chunk_size.max(1),
            position: 0,
        }
    }

    /// Next bounded slice, or `None` when fully consumed. Each returned slice
    /// borrows from the original payload: no copy, only the active chunk is
    /// live.
    pub fn next_chunk(&mut self) -> Option<&'a [u8]> {
        if self.position >= self.data.len() {
            return None;
        }
        let end = (self.position + self.chunk_size).min(self.data.len());
        let chunk = &self.data[self.position..end];
        self.position = end;
        Some(chunk)
    }

    /// Total payload length in bytes.
    pub fn data_len(&self) -> usize {
        self.data.len()
    }

    /// Bytes not yet chunked.
    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    /// Configured chunk quantum.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Bytes already handed out.
    pub fn position(&self) -> usize {
        self.position
    }

    /// True when the payload is empty or fully consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Number of chunks the payload splits into (0 for empty payloads).
    pub fn total_chunks(&self) -> usize {
        if self.data.is_empty() {
            0
        } else {
            self.data.len().div_ceil(self.chunk_size)
        }
    }

    /// Rewind to the first chunk.
    pub fn reset(&mut self) {
        self.position = 0;
    }
}

impl<'a> Iterator for ChunkedValue<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk()
    }
}
