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

//! Result-streaming bridge.
//!
//! Reuses the existing [`plomid_sql::QueryResult`] and [`plomid_sql::Value`]
//! types — no new row representation. Converts an already-executed
//! `QueryResult::Rows` into a pull-based [`ResultStream`] so network/cursor
//! layers can fetch bounded batches with backpressure instead of holding the
//! full `Vec` on the wire buffer.
//!
//! No `MAX_RESULT_ROWS` lives here: `limit=None` streams to exhaustion.
//! SQL `LIMIT` remains the only user-level cap (`stream.take(n)`).

use plomid_sql::{QueryResult, Value};
use plomid_stream::{Cursor, ResultStream, StreamConfig};

/// Split an executed `QueryResult::Rows` into a lazy row stream.
///
/// Consumes `rows` into the stream source; batches are pulled via
/// `next_batch`. Non-row results (`Inserted`, `Committed`, …) yield `None`.
pub fn rows_to_stream(
    result: QueryResult,
    config: StreamConfig,
) -> Option<(Vec<String>, ResultStream<Vec<Value>>)> {
    match result {
        QueryResult::Rows { columns, rows, .. } => {
            let stream = ResultStream::with_config(rows.into_iter(), config);
            Some((columns, stream))
        }
        _ => None,
    }
}

/// Same as [`rows_to_stream`] with an explicit user-level `LIMIT`.
/// `None` limit streams every row.
pub fn rows_to_stream_limited(
    result: QueryResult,
    config: StreamConfig,
    limit: Option<u64>,
) -> Option<(Vec<String>, ResultStream<Vec<Value>>)> {
    let (columns, stream) = rows_to_stream(result, config)?;
    let stream = match limit {
        Some(n) => stream.take(n),
        None => stream,
    };
    Some((columns, stream))
}

/// Wrap a row stream in a cursor for incremental `FETCH`-style consumption.
/// Reuses [`Cursor`] — no new cursor state machine here.
pub fn cursor_for_rows(rows: Vec<Vec<Value>>, config: StreamConfig) -> Cursor<Vec<Value>> {
    Cursor::new(ResultStream::with_config(rows.into_iter(), config))
}
