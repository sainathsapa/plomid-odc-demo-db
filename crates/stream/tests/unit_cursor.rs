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
// Moved from `src/cursor.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::config::StreamConfig;
use plomid_stream::cursor::*;
use plomid_stream::{ResultStream, StreamError};

#[test]
fn fetch_zero_returns_empty_without_touching_stream() {
    let mut cursor = Cursor::new(ResultStream::new(0u64..10));
    assert!(cursor.fetch(0).unwrap().is_empty());
    assert_eq!(cursor.position(), 0);
}

#[test]
fn fetch_after_close_errors() {
    let mut cursor = Cursor::new(ResultStream::new(0u64..10));
    cursor.close();
    cursor.close();
    assert!(cursor.is_closed());
    assert!(matches!(cursor.fetch(5), Err(StreamError::Closed)));
}

#[test]
fn cancel_propagates_to_fetch() {
    let mut cursor = Cursor::new(ResultStream::with_config(
        0u64..1_000_000,
        StreamConfig::new().with_batch_size(64),
    ));
    assert_eq!(cursor.fetch(10).unwrap().len(), 10);
    cursor.cancel();
    assert!(cursor.is_cancelled());
    assert!(matches!(cursor.fetch(10), Err(StreamError::Cancelled)));
}
