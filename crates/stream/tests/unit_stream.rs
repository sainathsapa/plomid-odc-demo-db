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
// Moved from `src/stream.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::stream::*;
use plomid_stream::{StreamConfig, StreamError};

#[test]
fn empty_source_yields_none() {
    let mut stream = ResultStream::new(std::iter::empty::<u64>());
    assert!(stream.next_batch(10).unwrap().is_none());
    assert!(stream.next_batch(10).unwrap().is_none());
}

#[test]
fn max_rows_smaller_than_batch_size_is_honored() {
    let mut stream =
        ResultStream::with_config(0u64..100, StreamConfig::new().with_batch_size(1000));
    let batch = stream.next_batch(7).unwrap().unwrap();
    assert_eq!(batch.len(), 7);
    assert_eq!(stream.position(), 7);
}

#[test]
fn close_is_idempotent_and_terminal() {
    let mut stream = ResultStream::new(0u64..100);
    stream.close();
    stream.close();
    assert!(stream.is_closed());
    assert!(matches!(stream.next_batch(10), Err(StreamError::Closed)));
}
