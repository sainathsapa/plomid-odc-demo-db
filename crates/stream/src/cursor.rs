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

//! Server-side cursor over a result stream.

use crate::error::StreamError;
use crate::stream::ResultStream;

/// Scrollable handle over a [`ResultStream`] with fetch/close semantics.
///
/// `fetch(n)` pulls up to `n` rows (across as many transport batches as
/// needed) and advances the position counter. Cancellation propagates to the
/// wrapped stream: [`Cursor::cancel`] stops in-progress and future fetches.
pub struct Cursor<T> {
    stream: ResultStream<T>,
    position: u64,
    closed: bool,
}

impl<T> Cursor<T> {
    /// Wrap an open stream. The cursor owns it and closes it on
    /// [`Cursor::close`].
    pub fn new(stream: ResultStream<T>) -> Self {
        Self {
            stream,
            position: 0,
            closed: false,
        }
    }

    /// Fetch up to `n` rows. Returns fewer (possibly empty) at exhaustion.
    /// Errors after [`Cursor::close`], on cancellation, or on upstream
    /// failure.
    pub fn fetch(&mut self, n: usize) -> Result<Vec<T>, StreamError> {
        if self.closed {
            return Err(StreamError::Closed);
        }
        let mut out = Vec::new();
        if n == 0 {
            return Ok(out);
        }
        // Cap the pre-allocation: large fetches stream in batch quanta.
        out.reserve(n.min(1024));
        let mut remaining = n;
        while remaining > 0 {
            let want = remaining.min(self.stream.batch_size());
            match self.stream.next_batch(want)? {
                Some(batch) => {
                    if batch.is_empty() {
                        break;
                    }
                    remaining -= batch.len();
                    self.position += batch.len() as u64;
                    out.extend(batch);
                }
                None => break,
            }
        }
        Ok(out)
    }

    /// Release the wrapped stream. Idempotent.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.stream.close();
    }

    /// Propagate cancellation to the wrapped stream.
    pub fn cancel(&self) {
        self.stream.cancel();
    }

    /// True after [`Cursor::close`].
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// True after [`Cursor::cancel`].
    pub fn is_cancelled(&self) -> bool {
        self.stream.is_cancelled()
    }

    /// Rows returned by `fetch` so far.
    pub fn position(&self) -> u64 {
        self.position
    }
}
