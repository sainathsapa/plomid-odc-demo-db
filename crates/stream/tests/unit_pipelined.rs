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
// Moved from `src/pipelined.rs` inline `#[cfg(test)]` — public-API only.
use plomid_stream::pipelined::*;
use plomid_stream::{StreamConfig, StreamPolicy};

#[test]
fn close_without_consume_joins_unbounded_producer() {
    let mut stream = PipelinedStream::spawn(
        0u64..,
        StreamConfig::new().with_batch_size(64),
        StreamPolicy::allow_all(),
    );
    stream.close();
    assert!(!stream.is_running());
    assert!(stream.is_closed());
}
