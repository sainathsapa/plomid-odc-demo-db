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
// Moved from `src/chunk.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::chunk::*;

#[test]
fn empty_payload_yields_no_chunks() {
    let mut value = ChunkedValue::new(&[]);
    assert!(value.is_empty());
    assert_eq!(value.total_chunks(), 0);
    assert_eq!(value.next_chunk(), None);
}

#[test]
fn uneven_tail_chunk() {
    let data = vec![1u8; ChunkedValue::DEFAULT_CHUNK_SIZE + 7];
    let mut value = ChunkedValue::new(&data);
    assert_eq!(value.total_chunks(), 2);
    assert_eq!(
        value.next_chunk().unwrap().len(),
        ChunkedValue::DEFAULT_CHUNK_SIZE
    );
    assert_eq!(value.next_chunk().unwrap().len(), 7);
    assert_eq!(value.next_chunk(), None);
    assert!(value.is_empty());
}

#[test]
fn reset_replays_chunks() {
    let data = vec![2u8; 100];
    let mut value = ChunkedValue::with_chunk_size(&data, 30);
    assert_eq!(value.total_chunks(), 4);
    let first: Vec<&[u8]> = value.by_ref().collect();
    value.reset();
    let second: Vec<&[u8]> = value.by_ref().collect();
    assert_eq!(first, second);
}
