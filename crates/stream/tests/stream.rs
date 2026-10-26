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

//! End-to-end acceptance tests for `plomid-stream`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use plomid_stream::{
    BackpressureController, ChunkedValue, Cursor, PipelinedStream, ResultStream, StreamConfig,
    StreamError, StreamPolicy,
};

// 1M+ rows stream incrementally: full count delivered, no batch ever exceeds
// the transport quantum (peak buffered < batch_size * buffered_batches needs
// only one live batch in pull mode), rows arrive in order.
#[test]
fn streams_beyond_one_million_rows_incrementally() {
    const TOTAL: u64 = 1_000_001;
    let mut stream = ResultStream::with_config(0..TOTAL, StreamConfig::new().with_batch_size(1000));
    let mut count = 0u64;
    let mut peak_batch = 0usize;
    let mut batches = 0u64;
    while let Some(batch) = stream.next_batch(1000).unwrap() {
        assert!(!batch.is_empty());
        peak_batch = peak_batch.max(batch.len());
        batches += 1;
        for value in batch {
            assert_eq!(value, count, "rows must arrive in source order");
            count += 1;
        }
    }
    assert_eq!(count, TOTAL);
    assert_eq!(stream.position(), TOTAL);
    assert!(peak_batch <= 1000, "no batch exceeds the quantum");
    assert!(peak_batch >= 1000, "expected full quanta, got {peak_batch}");
    assert_eq!(batches, 1001, "1M+1 rows at 1000/batch");
}

// Slow consumer: the producer blocks instead of growing memory. All rows
// arrive in order, the bounded queue never exceeds capacity, and the producer
// observably blocked.
#[test]
fn slow_consumer_backpressure_bounds_memory() {
    const TOTAL: u64 = 20_000;
    let config = StreamConfig::new()
        .with_batch_size(200)
        .with_max_buffered_batches(2);
    let mut stream = PipelinedStream::spawn(0..TOTAL, config, StreamPolicy::allow_all());
    let mut count = 0u64;
    while let Some(batch) = stream.next_batch().unwrap() {
        for value in &batch {
            assert_eq!(*value, count);
            count += 1;
        }
        // Slow consumer: force the producer to wait on a full queue.
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(count, TOTAL);
    assert_eq!(stream.position(), TOTAL);
    let metrics = stream.metrics();
    assert_eq!(metrics.capacity, 2);
    // Bounded: full queue (2) plus at most the producer's in-hand batch
    // blocked on admission — never unbounded growth.
    assert!(
        metrics.peak_buffered_batches <= 3,
        "peak {} escapes the bound",
        metrics.peak_buffered_batches
    );
    assert_eq!(
        metrics.buffered_batches, 0,
        "consumer drained everything: {metrics:?}"
    );
    assert!(
        metrics.blocked_producer_nanos > 0,
        "producer never blocked: {metrics:?}"
    );
    stream.close();
    assert!(!stream.is_running());
}

// Pipelined peak under a fast consumer also stays within the bound while
// delivering everything (buffer-budget path exercised too).
#[test]
fn pipelined_stream_respects_byte_budget_capacity() {
    let config = StreamConfig::new()
        .with_batch_size(1000)
        .with_max_buffered_batches(8)
        .with_buffer_budget_bytes(20 * 1024); // 1000 u64 rows ~= 8KB/batch => capacity 2
    assert_eq!(config.effective_capacity::<u64>(), 2);
    let mut stream = PipelinedStream::spawn(0u64..5000, config, StreamPolicy::allow_all());
    let mut count = 0u64;
    while let Some(batch) = stream.next_batch().unwrap() {
        count += batch.len() as u64;
    }
    assert_eq!(count, 5000);
    assert!(stream.metrics().peak_buffered_batches <= 3);
    stream.close();
}

// Cursor fetch/close/position semantics.
#[test]
fn cursor_fetch_close_position() {
    let stream = ResultStream::with_config(0u64..2500, StreamConfig::new().with_batch_size(1000));
    let mut cursor = Cursor::new(stream);
    assert!(!cursor.is_closed());
    assert_eq!(cursor.position(), 0);

    let first = cursor.fetch(1500).unwrap();
    assert_eq!(first.len(), 1500);
    assert_eq!(first[0], 0);
    assert_eq!(first[1499], 1499);
    assert_eq!(cursor.position(), 1500);

    // Only 1000 rows remain: short fetch, then exhaustion.
    let second = cursor.fetch(2000).unwrap();
    assert_eq!(second.len(), 1000);
    assert_eq!(second[0], 1500);
    assert_eq!(cursor.position(), 2500);
    assert!(cursor.fetch(10).unwrap().is_empty());
    assert_eq!(cursor.position(), 2500);

    cursor.close();
    assert!(cursor.is_closed());
    assert!(matches!(cursor.fetch(1), Err(StreamError::Closed)));
}

// Cancel on the pull stream stops delivery immediately.
#[test]
fn pull_stream_cancel_stops_batches() {
    let mut stream = ResultStream::new(0u64..1_000_000);
    assert!(stream.next_batch(10).unwrap().is_some());
    assert!(!stream.is_cancelled());
    stream.cancel();
    assert!(stream.is_cancelled());
    assert!(matches!(stream.next_batch(10), Err(StreamError::Cancelled)));
    assert!(matches!(
        stream.for_each_batch(|_| {}),
        Err(StreamError::Cancelled)
    ));
}

// Cancel releases the producer thread: close returns promptly even for an
// unbounded source, the worker exits, and no handle leaks.
#[test]
fn cancel_releases_producer_thread() {
    let mut stream = PipelinedStream::spawn(
        0u64..,
        StreamConfig::new()
            .with_batch_size(100)
            .with_max_buffered_batches(4),
        StreamPolicy::allow_all(),
    );
    for _ in 0..3 {
        assert!(stream.next_batch().unwrap().is_some());
    }
    assert!(stream.is_running());
    stream.cancel();
    assert!(stream.is_cancelled());
    assert!(matches!(stream.next_batch(), Err(StreamError::Cancelled)));

    let started = Instant::now();
    stream.close();
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "close must not hang on an unbounded source"
    );
    assert!(!stream.is_running());
    assert!(stream.is_closed());
    // Second close is a no-op, never double-joins.
    stream.close();
}

// Explicit LIMIT delivers exactly n rows from a much larger source.
#[test]
fn explicit_limit_works() {
    let config = StreamConfig::new().with_batch_size(1000).with_limit(100);
    let mut stream = ResultStream::with_config(0u64..1_000_000, config);
    let mut count = 0u64;
    while let Some(batch) = stream.next_batch(1000).unwrap() {
        count += batch.len() as u64;
    }
    assert_eq!(count, 100);

    // The `take` operator narrows (and composes with) any configured limit.
    let mut stream = ResultStream::new(0u64..1_000_000).take(25);
    let delivered = stream.for_each_batch(|_| {}).unwrap();
    assert_eq!(delivered, 25);
    assert_eq!(stream.position(), 25);
}

// No truncation without a limit: limit=None streams to source exhaustion,
// including a partial trailing batch.
#[test]
fn no_truncation_without_limit() {
    let mut stream = ResultStream::with_config(
        0u64..2500,
        StreamConfig::new().with_batch_size(1000).without_limit(),
    );
    assert_eq!(stream.limit(), None);
    let mut count = 0u64;
    let mut batches = 0u32;
    while let Some(batch) = stream.next_batch(1000).unwrap() {
        batches += 1;
        count += batch.len() as u64;
    }
    assert_eq!((count, batches), (2500, 3));
}

// Multi-MB bytea-style payload streams in 64 KiB chunks; reassembly is exact.
#[test]
fn chunked_bytea_streams_in_bounded_chunks() {
    const PAYLOAD: usize = 4 * 1024 * 1024;
    const QUANTUM: usize = 64 * 1024;
    let payload = vec![0xABu8; PAYLOAD];
    let mut chunked = ChunkedValue::new(&payload);
    assert_eq!(chunked.total_chunks(), 64);

    let mut chunks = 0u32;
    let mut bytes = 0usize;
    let mut peak = 0usize;
    while let Some(chunk) = chunked.next_chunk() {
        assert!(!chunk.is_empty());
        peak = peak.max(chunk.len());
        chunks += 1;
        bytes += chunk.len();
    }
    assert_eq!((chunks, bytes, peak), (64, PAYLOAD, QUANTUM));

    // Reassembly equals the original; str payloads chunk identically.
    let mut rebuilt = Vec::with_capacity(PAYLOAD);
    for chunk in ChunkedValue::new(&payload) {
        rebuilt.extend_from_slice(chunk);
    }
    assert_eq!(rebuilt, payload);

    let text = "héllo chunked world!";
    let mut seen = Vec::new();
    let mut chunked = ChunkedValue::from_text(text);
    while let Some(chunk) = chunked.next_chunk() {
        seen.extend_from_slice(chunk);
    }
    assert_eq!(seen, text.as_bytes());
}

// Auth hook: denied rows are skipped — never buffered, counted, or leaked.
#[test]
fn auth_policy_skips_denied_rows() {
    let odd_only = StreamPolicy::from_fn(|value: &u64| value % 2 == 1);
    let mut stream = ResultStream::with_policy(
        0u64..1000,
        StreamConfig::new().with_batch_size(128),
        odd_only,
    );
    let leaked = Arc::new(AtomicU64::new(0));
    let leaked_clone = Arc::clone(&leaked);
    let delivered = stream
        .for_each_batch(|batch| {
            for value in &batch {
                assert_eq!(value % 2, 1, "even row leaked through policy");
                leaked_clone.fetch_add(1, Ordering::Relaxed);
            }
        })
        .unwrap();
    assert_eq!(delivered, 500);
    assert_eq!(leaked.load(Ordering::Relaxed), 500);
    assert_eq!(stream.position(), 500, "denied rows are not counted");
}

// Upstream failure: buffered rows deliver first, then the boxed error —
// never silent truncation, never a panic.
#[test]
fn upstream_error_surfaces_after_buffered_rows() {
    let source = (0u64..10).map(|value| {
        if value == 5 {
            Err(std::io::Error::other("disk boom"))
        } else {
            Ok(value)
        }
    });
    let mut stream = ResultStream::from_fallible(
        source,
        StreamConfig::new().with_batch_size(100),
        StreamPolicy::allow_all(),
    );
    let first = stream.next_batch(100).unwrap().unwrap();
    assert_eq!(first, vec![0, 1, 2, 3, 4]);
    let error = stream.next_batch(100).unwrap_err();
    assert!(matches!(error, StreamError::Upstream(_)));
    assert!(error.to_string().contains("disk boom"));
}

// Non-blocking admission probe errors instead of growing the buffer.
#[test]
fn buffer_exceeded_policy_errors_instead_of_growing() {
    let controller = BackpressureController::new(2);
    assert!(controller.try_acquire().is_ok());
    controller.notify_send(0);
    controller.notify_send(0);
    let error = controller.try_acquire().unwrap_err();
    assert!(
        matches!(
            error,
            StreamError::BufferExceeded {
                buffered: 2,
                capacity: 2
            }
        ),
        "unexpected: {error:?}"
    );
    controller.notify_receive();
    assert!(controller.try_acquire().is_ok());
    controller.notify_drop(1);
    assert_eq!(controller.dropped_on_cancel(), 1);
}
