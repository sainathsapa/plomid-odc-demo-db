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

//! Structured stream errors: returned, never panicked, never truncated.

use std::fmt;

/// Terminal or policy failure of a result stream.
///
/// There is intentionally no "limit reached" variant: reaching an explicit
/// user-level limit is normal exhaustion and surfaces as `Ok(None)`, exactly
/// like running out of source rows.
// (Manual Debug impl below: Box<dyn Error> has no Debug impl, so a derive is impossible.)
pub enum StreamError {
    /// [`crate::ResultStream::cancel`] (or cursor/pipeline cancel) was
    /// observed. Partial batches already delivered are kept by the caller;
    /// the in-progress batch is discarded.
    Cancelled,
    /// The stream or cursor was closed. Delivery after close is a caller bug.
    Closed,
    /// A non-blocking admission probe found the bounded buffer full. The
    /// blocking producer path never returns this: it blocks instead, so
    /// memory stays bounded.
    BufferExceeded {
        /// Batches currently in flight.
        buffered: usize,
        /// Configured bound.
        capacity: usize,
    },
    /// The row source itself failed. Carries the boxed source error so the
    /// executor can forward storage/scan failures without losing context.
    Upstream(Box<dyn std::error::Error + Send + Sync>),
}

impl StreamError {
    /// Box any source failure into [`StreamError::Upstream`].
    pub fn upstream<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Upstream(Box::new(error))
    }

    /// True only for [`StreamError::Cancelled`].
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "result stream cancelled"),
            Self::Closed => write!(f, "result stream is closed"),
            Self::BufferExceeded { buffered, capacity } => write!(
                f,
                "stream buffer full ({buffered}/{capacity} batches); slow down the producer"
            ),
            Self::Upstream(source) => write!(f, "upstream row source failed: {source}"),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Upstream(boxed) => Some(boxed.as_ref()),
            _ => None,
        }
    }
}

// Box<dyn Error> has no Debug impl, so provide one manually.
impl fmt::Debug for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "Cancelled"),
            Self::Closed => write!(f, "Closed"),
            Self::BufferExceeded { buffered, capacity } => f
                .debug_struct("BufferExceeded")
                .field("buffered", buffered)
                .field("capacity", capacity)
                .finish(),
            Self::Upstream(source) => f
                .debug_tuple("Upstream")
                .field(&source.to_string())
                .finish(),
        }
    }
}
