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
//! In-memory process-wide server counters (production observability).
//!
//! This is the single authority for operational counters. Every layer records
//! here with lock-free atomics — no new ports, no new dependencies (only
//! `std`), no log scraping:
//!
//! * connections opened/closed — recorded by the network listener next to its
//!   existing connection accounting (`plomid-network` server).
//! * queries total / errors total / slow queries — recorded once per
//!   `Executor::execute_all` call at the executor choke point, so in-process
//!   use and wire use count identically and exactly once.
//! * slow-query wall time — count plus max wall per statement class, gated by
//!   [`SLOW_QUERY_THRESHOLD_MS`].
//! * maintenance passes — recorded at the end of every successful
//!   `maintain_table` pass, which is the single choke point for inline
//!   automatic passes, background-worker passes, and explicit `VACUUM`.
//! * checkpoints — recorded on every successful engine checkpoint.
//! * analytical (fast-path) counters — total plus per-path counts via
//!   [`note_analytical_path`]. Currently wired paths (both in the executor's
//!   `join/support.rs`):
//!
//!   * `"indexed_join"` — selective INNER JOIN served from indexes instead of
//!     full scans (`try_indexed_join`).
//!   * `"parallel_join"` — equi-INNER hash join fanned out to workers
//!     (`perform_join_parallel`).
//!
//!   NOT wired (deliberately skipped): the `analytical_path` tracing sites in
//!   `crates/executor/src/query/select.rs` (`"vector"`, `"row_count"`,
//!   `"columnar_scan"`, `"row_count_filter"`, `"row_group"`,
//!   `"par_scan_agg"`, `"par_streaming_agg"`) live in a module this change
//!   does not touch. They share this same [`note_analytical_path`] API, so a
//!   follow-up only adds one-line calls next to those tracing sites — no
//!   schema or API change needed. Until then `plomid_stat_analytical()`
//!   reports the wired paths plus a zero-marker row per skipped path (see
//!   [`SKIPPED_ANALYTICAL_PATHS`]) so operators can tell "never taken" apart
//!   from "not instrumented".
//!
//! Exposure is in-band only: the executor's system catalog serves
//! `plomid_stat_counters()`, `plomid_stat_analytical()`,
//! `plomid_stat_tables()`, and `plomid_htap_stats()` from [`snapshot`] plus
//! the columnar-freshness map. No new ports, no new threads, no new files on
//! disk.
//!
//! HTAP counters (spill totals, result-buffer gauge, admission decisions)
//! also live here as process-wide atomics. The sort-spill path records its
//! per-sort total next to its existing spill trace site; the admission and
//! result-buffer recorders are public for the worker that owns those hooks
//! and report zero until it wires them. Live hierarchy gauges
//! (global/OLTP/OLAP current, reserves) are NOT duplicated here — this
//! layer holds no `MemoryManager` handle, so `plomid_htap_stats()` reads
//! them from the governance global-manager singleton (the same manager
//! result buffers charge against) alongside these counters and the
//! per-class latency driver.
//!
//! All counters are process-wide and monotonic within a process lifetime.
//! [`reset_for_tests`] exists for test isolation only and is never called by
//! production code.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Wall time at or above which a completed query counts as slow.
///
/// Kept as a single constant (not per-class thresholds) so dashboards and the
/// CLI can reason about one SLO boundary. Per-class detail is preserved in
/// the max-wall map instead.
pub const SLOW_QUERY_THRESHOLD_MS: u64 = 100;

/// Analytical paths with tracing sites that exist but are NOT instrumented by
/// this change (they live in `crates/executor/src/query/select.rs`, which is
/// out of scope). Listed here so the SQL surface can render explicit
/// zero-marker rows for them instead of silently omitting them.
pub const SKIPPED_ANALYTICAL_PATHS: &[&str] = &[
    "vector",
    "row_count",
    "columnar_scan",
    "row_count_filter",
    "row_group",
    "par_scan_agg",
    "par_streaming_agg",
];

/// Per-class slow-query detail: how many queries of the class were slow, and
/// the slowest wall time observed for the class.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SlowClassStat {
    /// Slow queries observed for this statement class.
    pub count: u64,
    /// Maximum wall time (ms) observed for this statement class (slow or not;
    /// tracks the class worst case, which is what an SLO dashboard needs).
    pub max_ms: u64,
}

/// Fixed-bucket latency histogram (microseconds).
///
/// Buckets are `≤1ms, ≤10ms, ≤100ms, ≤1s, ≤10s, >10s`: coarse enough for a
/// handful of atomics, fine enough to tell lock contention (µs–ms) from
/// I/O stalls (ms–s). `total_us` / `count` give the mean; `max_us` the worst
/// case. All updates are lock-free atomics.
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 6],
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl LatencyHistogram {
    const fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    fn observe(&self, micros: u64) {
        let bucket = if micros <= 1_000 {
            0
        } else if micros <= 10_000 {
            1
        } else if micros <= 100_000 {
            2
        } else if micros <= 1_000_000 {
            3
        } else if micros <= 10_000_000 {
            4
        } else {
            5
        };
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .total_us
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(micros))
            });
        self.max_us.fetch_max(micros, Ordering::Relaxed);
    }

    fn snapshot(&self) -> LatencySnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        LatencySnapshot {
            count: load(&self.count),
            total_us: load(&self.total_us),
            max_us: load(&self.max_us),
            buckets: [
                load(&self.buckets[0]),
                load(&self.buckets[1]),
                load(&self.buckets[2]),
                load(&self.buckets[3]),
                load(&self.buckets[4]),
                load(&self.buckets[5]),
            ],
        }
    }

    fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.total_us.store(0, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
    }
}

/// A point-in-time copy of one [`LatencyHistogram`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatencySnapshot {
    /// Observations recorded.
    pub count: u64,
    /// Sum of observed microseconds (mean = `total_us / count`).
    pub total_us: u64,
    /// Maximum observed microseconds.
    pub max_us: u64,
    /// Per-bucket counts: `≤1ms, ≤10ms, ≤100ms, ≤1s, ≤10s, >10s`.
    pub buckets: [u64; 6],
}

/// A point-in-time copy of every counter. Returned by [`snapshot`].
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// Completed `execute_all` calls (success and failure).
    pub queries_total: u64,
    /// Completed `execute_all` calls that returned an error.
    pub errors_total: u64,
    /// Accepted wire connections.
    pub connections_opened: u64,
    /// Finished wire connections (normal or error close).
    pub connections_closed: u64,
    /// Queries at or above [`SLOW_QUERY_THRESHOLD_MS`].
    pub slow_queries_total: u64,
    /// Maximum query wall time observed (ms, any class).
    pub slow_max_ms: u64,
    /// Successful `maintain_table` passes (inline, worker, and `VACUUM`).
    pub maintenance_passes_total: u64,
    /// Successful engine checkpoints (any trigger).
    pub checkpoints_total: u64,
    /// Fast-path (analytical) executions across all wired paths.
    pub analytical_total: u64,
    /// Per-path fast-path counts, sorted by path. Includes zero-marker rows
    /// for [`SKIPPED_ANALYTICAL_PATHS`] that have never been recorded.
    pub analytical_by_path: Vec<(String, u64)>,
    /// Per-statement-class wall detail, sorted by class. Only classes that
    /// have executed at least one query appear.
    pub slow_by_class: Vec<(String, SlowClassStat)>,
    /// Cumulative sort-spill bytes across all over-budget sorts.
    pub spill_bytes_total: u64,
    /// Current result-buffer bytes held (gauge).
    pub result_buffer_bytes: u64,
    /// High-water mark of [`Snapshot::result_buffer_bytes`].
    pub result_buffer_peak: u64,
    /// Cumulative admission decisions granting query entry.
    pub admission_admitted: u64,
    /// Cumulative admission decisions refusing query entry.
    pub admission_rejected: u64,
    /// Queries currently waiting on admission (gauge).
    pub admission_waiting: u64,
    /// Vector exact-scan distance evaluations served (scalar-function path).
    pub vector_exact_queries: u64,
    /// Vector queries served from an ANN graph (any method).
    pub vector_ann_queries: u64,
    /// ANN cache hits (graph reused without rebuild).
    pub vector_ann_cache_hits: u64,
    /// ANN cache misses (graph built or rebuilt).
    pub vector_ann_cache_misses: u64,
    /// ANN-eligible queries that fell back to exact scan.
    pub vector_ann_fallbacks: u64,
    /// Sparse-kind vector distance evaluations.
    pub vector_sparse_queries: u64,
    /// Binary-kind vector distance evaluations.
    pub vector_binary_queries: u64,
    /// Vector index builds (graph constructions).
    pub vector_index_builds: u64,
    /// Successful TLS handshakes (SSLRequest upgrades).
    pub tls_connections: u64,
    /// Failed TLS handshakes (client or server side).
    pub tls_handshake_failures: u64,
    /// Plaintext startups refused while TLS is required (FATAL 28000).
    pub tls_plaintext_refusals: u64,
    /// Failed password/SCRAM authentication attempts.
    pub auth_failures: u64,
    /// Statements refused by authorization (42501).
    pub authz_denials: u64,
    /// Accounts locked after consecutive authentication failures.
    pub account_lockouts: u64,
    /// Authentication attempts rejected because the account was locked.
    pub auth_locked_out: u64,
    /// Successful MD5 (legacy downgrade-path) logins.
    pub legacy_auth_uses: u64,
    /// Transactions begun (any facade).
    pub txn_started: u64,
    /// Transactions committed.
    pub txn_committed: u64,
    /// Transactions aborted / rolled back.
    pub txn_aborted: u64,
    /// Write conflicts observed (lock contention + unique violations).
    pub txn_conflicts: u64,
    /// Transactions currently active (gauge).
    pub txn_active: u64,
    /// Transaction lifetime (begin → commit/abort).
    pub txn_duration_us: LatencySnapshot,
    /// Commit path latency (commit call → durable + published).
    pub txn_commit_latency_us: LatencySnapshot,
    /// Database-mutex acquisition wait.
    pub engine_mutex_wait_us: LatencySnapshot,
    /// Write-gate (Condvar) acquisition wait.
    pub gate_wait_us: LatencySnapshot,
    /// Group-commit durability barrier wait.
    pub wal_barrier_wait_us: LatencySnapshot,
    /// Group-commit batch formation wait (follower share).
    pub group_commit_wait_us: LatencySnapshot,
    /// Maintenance worker queue wait (submit → pass start).
    pub maintenance_queue_wait_us: LatencySnapshot,
    /// WAL payload bytes appended (all records).
    pub wal_bytes_total: u64,
    /// WAL records appended.
    pub wal_appends_total: u64,
    /// WAL bytes covered by checkpoints (durable boundary advances).
    pub wal_checkpoint_bytes_total: u64,
    /// Un-checkpointed WAL bytes (gauge: backlog).
    pub wal_backlog_bytes: u64,
    /// WAL segment rotations (new files created).
    pub wal_rotations: u64,
    /// WAL flush durations.
    pub wal_flush_us: LatencySnapshot,
    /// WAL sync (fsync) durations.
    pub wal_sync_us: LatencySnapshot,
    /// Buffer pool hits.
    pub buffer_hits: u64,
    /// Buffer pool misses (page read from storage).
    pub buffer_misses: u64,
    /// Buffer pool evictions.
    pub buffer_evictions: u64,
    /// Dirty pages observed at the last pool sync (gauge sample).
    pub buffer_dirty_pages_last_sync: u64,
    /// WAL bytes covered by engine checkpoints.
    pub checkpoint_bytes_total: u64,
    /// Engine checkpoint durations.
    pub checkpoint_duration_us: LatencySnapshot,
    /// Columnar segments examined by scans.
    pub columnar_segments_examined: u64,
    /// Columnar segments skipped by pruning.
    pub columnar_segments_skipped: u64,
    /// Columns materialized by scans.
    pub columnar_columns_read: u64,
    /// Vectorized batches evaluated.
    pub columnar_batches: u64,
    /// Spill files created.
    pub spill_files_total: u64,
    /// Vector index rebuilds (freshness-driven, timed below).
    pub vector_rebuilds: u64,
    /// Vector index rebuild durations.
    pub vector_rebuild_us: LatencySnapshot,
    /// Index-freshness outcomes: served fresh.
    pub vector_fresh: u64,
    /// Index-freshness outcomes: stale (rebuilt or exact fallback).
    pub vector_stale: u64,
    /// Admin actions by name (reindex, vacuum, ...).
    pub admin_by_action: Vec<(String, u64)>,
}

struct Counters {
    queries_total: AtomicU64,
    errors_total: AtomicU64,
    connections_opened: AtomicU64,
    connections_closed: AtomicU64,
    slow_queries_total: AtomicU64,
    slow_max_ms: AtomicU64,
    maintenance_passes_total: AtomicU64,
    checkpoints_total: AtomicU64,
    analytical_total: AtomicU64,
    spill_bytes_total: AtomicU64,
    result_buffer_bytes: AtomicU64,
    result_buffer_peak: AtomicU64,
    admission_admitted: AtomicU64,
    admission_rejected: AtomicU64,
    admission_waiting: AtomicU64,
    vector_exact_queries: AtomicU64,
    vector_ann_queries: AtomicU64,
    vector_ann_cache_hits: AtomicU64,
    vector_ann_cache_misses: AtomicU64,
    vector_ann_fallbacks: AtomicU64,
    vector_sparse_queries: AtomicU64,
    vector_binary_queries: AtomicU64,
    vector_index_builds: AtomicU64,
    tls_connections: AtomicU64,
    tls_handshake_failures: AtomicU64,
    tls_plaintext_refusals: AtomicU64,
    auth_failures: AtomicU64,
    authz_denials: AtomicU64,
    account_lockouts: AtomicU64,
    auth_locked_out: AtomicU64,
    legacy_auth_uses: AtomicU64,
    txn_started: AtomicU64,
    txn_committed: AtomicU64,
    txn_aborted: AtomicU64,
    txn_conflicts: AtomicU64,
    txn_active: AtomicU64,
    txn_duration_us: LatencyHistogram,
    txn_commit_latency_us: LatencyHistogram,
    engine_mutex_wait_us: LatencyHistogram,
    gate_wait_us: LatencyHistogram,
    wal_barrier_wait_us: LatencyHistogram,
    group_commit_wait_us: LatencyHistogram,
    maintenance_queue_wait_us: LatencyHistogram,
    wal_bytes_total: AtomicU64,
    wal_appends_total: AtomicU64,
    wal_checkpoint_bytes_total: AtomicU64,
    wal_backlog_bytes: AtomicU64,
    wal_rotations: AtomicU64,
    wal_flush_us: LatencyHistogram,
    wal_sync_us: LatencyHistogram,
    buffer_hits: AtomicU64,
    buffer_misses: AtomicU64,
    buffer_evictions: AtomicU64,
    buffer_dirty_pages_last_sync: AtomicU64,
    checkpoint_bytes_total: AtomicU64,
    checkpoint_duration_us: LatencyHistogram,
    columnar_segments_examined: AtomicU64,
    columnar_segments_skipped: AtomicU64,
    columnar_columns_read: AtomicU64,
    columnar_batches: AtomicU64,
    spill_files_total: AtomicU64,
    vector_rebuilds: AtomicU64,
    vector_rebuild_us: LatencyHistogram,
    vector_fresh: AtomicU64,
    vector_stale: AtomicU64,
    /// Low-cardinality maps only (statement classes, analytical path names).
    /// Critical sections only hash short strings.
    paths: Mutex<HashMap<String, u64>>,
    classes: Mutex<HashMap<String, SlowClassStat>>,
    admin_actions: Mutex<HashMap<String, u64>>,
}

impl Counters {
    fn new() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            connections_opened: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            slow_queries_total: AtomicU64::new(0),
            slow_max_ms: AtomicU64::new(0),
            maintenance_passes_total: AtomicU64::new(0),
            checkpoints_total: AtomicU64::new(0),
            analytical_total: AtomicU64::new(0),
            spill_bytes_total: AtomicU64::new(0),
            result_buffer_bytes: AtomicU64::new(0),
            result_buffer_peak: AtomicU64::new(0),
            admission_admitted: AtomicU64::new(0),
            admission_rejected: AtomicU64::new(0),
            admission_waiting: AtomicU64::new(0),
            vector_exact_queries: AtomicU64::new(0),
            vector_ann_queries: AtomicU64::new(0),
            vector_ann_cache_hits: AtomicU64::new(0),
            vector_ann_cache_misses: AtomicU64::new(0),
            vector_ann_fallbacks: AtomicU64::new(0),
            vector_sparse_queries: AtomicU64::new(0),
            vector_binary_queries: AtomicU64::new(0),
            vector_index_builds: AtomicU64::new(0),
            tls_connections: AtomicU64::new(0),
            tls_handshake_failures: AtomicU64::new(0),
            tls_plaintext_refusals: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            authz_denials: AtomicU64::new(0),
            account_lockouts: AtomicU64::new(0),
            auth_locked_out: AtomicU64::new(0),
            legacy_auth_uses: AtomicU64::new(0),
            txn_started: AtomicU64::new(0),
            txn_committed: AtomicU64::new(0),
            txn_aborted: AtomicU64::new(0),
            txn_conflicts: AtomicU64::new(0),
            txn_active: AtomicU64::new(0),
            txn_duration_us: LatencyHistogram::new(),
            txn_commit_latency_us: LatencyHistogram::new(),
            engine_mutex_wait_us: LatencyHistogram::new(),
            gate_wait_us: LatencyHistogram::new(),
            wal_barrier_wait_us: LatencyHistogram::new(),
            group_commit_wait_us: LatencyHistogram::new(),
            maintenance_queue_wait_us: LatencyHistogram::new(),
            wal_bytes_total: AtomicU64::new(0),
            wal_appends_total: AtomicU64::new(0),
            wal_checkpoint_bytes_total: AtomicU64::new(0),
            wal_backlog_bytes: AtomicU64::new(0),
            wal_rotations: AtomicU64::new(0),
            wal_flush_us: LatencyHistogram::new(),
            wal_sync_us: LatencyHistogram::new(),
            buffer_hits: AtomicU64::new(0),
            buffer_misses: AtomicU64::new(0),
            buffer_evictions: AtomicU64::new(0),
            buffer_dirty_pages_last_sync: AtomicU64::new(0),
            checkpoint_bytes_total: AtomicU64::new(0),
            checkpoint_duration_us: LatencyHistogram::new(),
            columnar_segments_examined: AtomicU64::new(0),
            columnar_segments_skipped: AtomicU64::new(0),
            columnar_columns_read: AtomicU64::new(0),
            columnar_batches: AtomicU64::new(0),
            spill_files_total: AtomicU64::new(0),
            vector_rebuilds: AtomicU64::new(0),
            vector_rebuild_us: LatencyHistogram::new(),
            vector_fresh: AtomicU64::new(0),
            vector_stale: AtomicU64::new(0),
            paths: Mutex::new(HashMap::new()),
            classes: Mutex::new(HashMap::new()),
            admin_actions: Mutex::new(HashMap::new()),
        }
    }
}

fn counters() -> &'static Counters {
    static COUNTERS: OnceLock<Counters> = OnceLock::new();
    COUNTERS.get_or_init(Counters::new)
}

/// Classifies a SQL batch by its first keyword for per-class slow-query
/// accounting. Zero-allocation dispatch over a fixed class set; anything
/// unrecognized (including empty input) is `"OTHER"`.
#[must_use]
pub fn statement_class(sql: &str) -> &'static str {
    let word = sql
        .split_whitespace()
        .next()
        .map(|word| word.trim_end_matches(';'))
        .unwrap_or("");
    if word.eq_ignore_ascii_case("SELECT") {
        return "SELECT";
    }
    if word.eq_ignore_ascii_case("INSERT") {
        return "INSERT";
    }
    if word.eq_ignore_ascii_case("UPDATE") {
        return "UPDATE";
    }
    if word.eq_ignore_ascii_case("DELETE") {
        return "DELETE";
    }
    if word.eq_ignore_ascii_case("CREATE") {
        return "CREATE";
    }
    if word.eq_ignore_ascii_case("DROP") {
        return "DROP";
    }
    if word.eq_ignore_ascii_case("ALTER") {
        return "ALTER";
    }
    if word.eq_ignore_ascii_case("VACUUM") {
        return "VACUUM";
    }
    if word.eq_ignore_ascii_case("ANALYZE") {
        return "ANALYZE";
    }
    if word.eq_ignore_ascii_case("EXPLAIN") {
        return "EXPLAIN";
    }
    if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("START") {
        return "BEGIN";
    }
    if word.eq_ignore_ascii_case("COMMIT") {
        return "COMMIT";
    }
    if word.eq_ignore_ascii_case("ROLLBACK") {
        return "ROLLBACK";
    }
    if word.eq_ignore_ascii_case("SET") {
        return "SET";
    }
    if word.eq_ignore_ascii_case("SHOW") {
        return "SHOW";
    }
    if word.eq_ignore_ascii_case("TRUNCATE") {
        return "TRUNCATE";
    }
    if word.eq_ignore_ascii_case("COPY") {
        return "COPY";
    }
    if word.eq_ignore_ascii_case("WITH") {
        return "WITH";
    }
    if word.eq_ignore_ascii_case("VALUES") {
        return "VALUES";
    }
    "OTHER"
}

/// Records one accepted wire connection. Called next to the listener's own
/// connection accounting.
pub fn record_connection_open() {
    counters()
        .connections_opened
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one finished wire connection (normal or error close).
pub fn record_connection_close() {
    counters()
        .connections_closed
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one completed query batch: total, error, and slow-query detail.
///
/// `statement_class` should come from [`statement_class`]; `elapsed_ms` is
/// the batch wall time. Slow gating uses [`SLOW_QUERY_THRESHOLD_MS`]; the
/// per-class max tracks every batch (not only slow ones) so it always holds
/// the class worst case.
pub fn record_query(class: &str, elapsed_ms: u64, is_error: bool) {
    let counters = counters();
    counters.queries_total.fetch_add(1, Ordering::Relaxed);
    if is_error {
        counters.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    if elapsed_ms >= SLOW_QUERY_THRESHOLD_MS {
        counters.slow_queries_total.fetch_add(1, Ordering::Relaxed);
    }
    counters
        .slow_max_ms
        .fetch_max(elapsed_ms, Ordering::Relaxed);
    if let Ok(mut guard) = counters.classes.lock() {
        let entry = guard.entry(class.to_string()).or_default();
        if elapsed_ms >= SLOW_QUERY_THRESHOLD_MS {
            entry.count = entry.count.saturating_add(1);
        }
        entry.max_ms = entry.max_ms.max(elapsed_ms);
    }
}

/// Records one successful maintenance pass (`maintain_table` completion,
/// covering inline automatic passes, background-worker passes, and explicit
/// `VACUUM` — they all converge on that one function).
pub fn record_maintenance_pass() {
    counters()
        .maintenance_passes_total
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one successful engine checkpoint (any trigger).
pub fn record_checkpoint() {
    counters().checkpoints_total.fetch_add(1, Ordering::Relaxed);
}

/// Records one vector exact-scan distance evaluation (scalar-function path).
///
/// Called once per `vector_distance_function` evaluation that reaches the
/// kernels (NULL inputs return before recording). Raw vectors never enter
/// logs; only the counter moves.
pub fn record_vector_exact() {
    counters()
        .vector_exact_queries
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one vector query served from an ANN graph (any method).
pub fn record_vector_ann() {
    counters()
        .vector_ann_queries
        .fetch_add(1, Ordering::Relaxed);
}

/// Records an ANN cache hit (graph reused without rebuild).
pub fn record_vector_ann_cache_hit() {
    counters()
        .vector_ann_cache_hits
        .fetch_add(1, Ordering::Relaxed);
}

/// Records an ANN cache miss (graph built or rebuilt).
pub fn record_vector_ann_cache_miss() {
    counters()
        .vector_ann_cache_misses
        .fetch_add(1, Ordering::Relaxed);
}

/// Records an ANN-eligible query that fell back to the exact scan.
pub fn record_vector_ann_fallback() {
    counters()
        .vector_ann_fallbacks
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one sparse-kind vector distance evaluation.
pub fn record_vector_sparse() {
    counters()
        .vector_sparse_queries
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one binary-kind vector distance evaluation.
pub fn record_vector_binary() {
    counters()
        .vector_binary_queries
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one vector index graph construction.
pub fn record_vector_index_build() {
    counters()
        .vector_index_builds
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one successful TLS handshake (`SSLRequest` → encrypted session).
pub fn record_tls_handshake() {
    counters().tls_connections.fetch_add(1, Ordering::Relaxed);
}

/// Records one failed TLS handshake (either side).
pub fn record_tls_handshake_failure() {
    counters()
        .tls_handshake_failures
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one plaintext startup refused while TLS is required.
pub fn record_tls_plaintext_refusal() {
    counters()
        .tls_plaintext_refusals
        .fetch_add(1, Ordering::Relaxed);
}

/// Records one failed password/SCRAM authentication attempt.
pub fn record_auth_failure() {
    counters().auth_failures.fetch_add(1, Ordering::Relaxed);
}

/// Records one statement refused by authorization (42501).
pub fn record_authz_denial() {
    counters().authz_denials.fetch_add(1, Ordering::Relaxed);
}

/// Records one account lockout after consecutive authentication failures.
pub fn record_account_lockout() {
    counters().account_lockouts.fetch_add(1, Ordering::Relaxed);
}

/// Records one authentication attempt rejected while the account is locked.
pub fn record_auth_locked_out() {
    counters().auth_locked_out.fetch_add(1, Ordering::Relaxed);
}

/// Records one successful MD5 (legacy downgrade-path) login.
pub fn record_legacy_auth_use() {
    counters().legacy_auth_uses.fetch_add(1, Ordering::Relaxed);
}

/// Records one transaction begin (any facade) and raises the active gauge.
pub fn record_txn_begin() {
    let counters = counters();
    counters.txn_started.fetch_add(1, Ordering::Relaxed);
    counters.txn_active.fetch_add(1, Ordering::Relaxed);
}

/// Records one transaction end: committed or aborted with its lifetime.
///
/// `committed` selects the outcome counter; `duration_us` feeds the
/// lifetime histogram either way. The active gauge saturates at zero so an
/// unmatched end (double abort after a failed commit) never underflows it.
pub fn record_txn_end(committed: bool, duration_us: u64) {
    let counters = counters();
    if committed {
        counters.txn_committed.fetch_add(1, Ordering::Relaxed);
    } else {
        counters.txn_aborted.fetch_add(1, Ordering::Relaxed);
    }
    counters.txn_duration_us.observe(duration_us);
    let active = &counters.txn_active;
    let _ = active.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

/// Records one write conflict (lock contention or unique violation).
pub fn record_txn_conflict() {
    counters().txn_conflicts.fetch_add(1, Ordering::Relaxed);
}

/// Records one commit-path latency sample (microseconds).
pub fn record_commit_latency(micros: u64) {
    counters().txn_commit_latency_us.observe(micros);
}

/// Records one database-mutex acquisition wait (microseconds).
pub fn record_engine_mutex_wait(micros: u64) {
    counters().engine_mutex_wait_us.observe(micros);
}

/// Records one write-gate (Condvar) acquisition wait (microseconds).
pub fn record_gate_wait(micros: u64) {
    counters().gate_wait_us.observe(micros);
}

/// Records one group-commit durability barrier wait (microseconds).
pub fn record_wal_barrier_wait(micros: u64) {
    counters().wal_barrier_wait_us.observe(micros);
}

/// Records one group-commit batch-formation wait (microseconds): time a
/// follower spent queued behind the leader's flush.
pub fn record_group_commit_wait(micros: u64) {
    counters().group_commit_wait_us.observe(micros);
}

/// Records one maintenance worker queue wait, submit → pass start
/// (microseconds).
pub fn record_maintenance_queue_wait(micros: u64) {
    counters().maintenance_queue_wait_us.observe(micros);
}

/// Records one WAL append of `bytes` payload framing (frame length).
///
/// Saturates instead of wrapping. The backlog gauge tracks
/// un-checkpointed bytes; see [`record_wal_checkpoint_covered`].
pub fn record_wal_append(bytes: u64) {
    let counters = counters();
    counters.wal_appends_total.fetch_add(1, Ordering::Relaxed);
    let _ = counters
        .wal_bytes_total
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_add(bytes))
        });
    let _ = counters
        .wal_backlog_bytes
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_add(bytes))
        });
}

/// Records WAL bytes newly covered by a checkpoint boundary.
///
/// Drops the backlog gauge by the covered amount (saturating) so it always
/// reads as un-checkpointed bytes.
pub fn record_wal_checkpoint_covered(bytes: u64) {
    let counters = counters();
    let _ = counters.wal_checkpoint_bytes_total.try_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |total| Some(total.saturating_add(bytes)),
    );
    let _ = counters
        .wal_backlog_bytes
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_sub(bytes))
        });
}

/// Records one WAL segment rotation (new file created).
pub fn record_wal_rotation() {
    counters().wal_rotations.fetch_add(1, Ordering::Relaxed);
}

/// Records one WAL flush duration (microseconds).
pub fn record_wal_flush(micros: u64) {
    counters().wal_flush_us.observe(micros);
}

/// Records one WAL sync (fsync) duration (microseconds).
pub fn record_wal_sync(micros: u64) {
    counters().wal_sync_us.observe(micros);
}

/// Records one buffer pool hit.
pub fn record_buffer_hit() {
    counters().buffer_hits.fetch_add(1, Ordering::Relaxed);
}

/// Records one buffer pool miss (page read from storage).
pub fn record_buffer_miss() {
    counters().buffer_misses.fetch_add(1, Ordering::Relaxed);
}

/// Records one buffer pool eviction.
pub fn record_buffer_eviction() {
    counters().buffer_evictions.fetch_add(1, Ordering::Relaxed);
}

/// Records the dirty-page count observed at a pool sync (gauge sample).
pub fn record_buffer_dirty_pages(pages: u64) {
    counters()
        .buffer_dirty_pages_last_sync
        .store(pages, Ordering::Relaxed);
}

/// Records one engine checkpoint's covered WAL bytes.
pub fn record_checkpoint_bytes(bytes: u64) {
    let _ = counters().checkpoint_bytes_total.try_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |total| Some(total.saturating_add(bytes)),
    );
}

/// Records one engine checkpoint duration (microseconds).
pub fn record_checkpoint_duration(micros: u64) {
    counters().checkpoint_duration_us.observe(micros);
}

/// Records one columnar scan's segment/column/batch accounting.
///
/// `examined`/`skipped` are per-segment outcomes, `columns` the
/// materialized column count, `batches` the vectorized batches evaluated.
pub fn record_columnar_scan(examined: u64, skipped: u64, columns: u64, batches: u64) {
    let counters = counters();
    let add = |value: &AtomicU64, delta: u64| {
        let _ = value.try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
            Some(total.saturating_add(delta))
        });
    };
    add(&counters.columnar_segments_examined, examined);
    add(&counters.columnar_segments_skipped, skipped);
    add(&counters.columnar_columns_read, columns);
    add(&counters.columnar_batches, batches);
}

/// Records one spill file creation.
pub fn record_spill_file() {
    counters().spill_files_total.fetch_add(1, Ordering::Relaxed);
}

/// Records one vector index rebuild with its duration (microseconds).
pub fn record_vector_rebuild(micros: u64) {
    let counters = counters();
    counters.vector_rebuilds.fetch_add(1, Ordering::Relaxed);
    counters.vector_rebuild_us.observe(micros);
}

/// Records one index-freshness outcome: `true` served fresh from the
/// derived graph, `false` went stale (rebuild or exact fallback).
pub fn record_vector_freshness(fresh: bool) {
    let counters = counters();
    if fresh {
        counters.vector_fresh.fetch_add(1, Ordering::Relaxed);
    } else {
        counters.vector_stale.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records one operator-initiated admin action (`reindex`, `vacuum`, ...).
///
/// Low-cardinality names only; the map mirrors the analytical-path
/// accounting. Unknown callers must not invent per-object names here.
pub fn note_admin_action(action: &str) {
    if let Ok(mut guard) = counters().admin_actions.lock() {
        *guard.entry(action.to_string()).or_insert(0) += 1;
    }
}

/// Records one fast-path (analytical) execution for `path`.
///
/// Wired paths today: `"indexed_join"` and `"parallel_join"` (both recorded
/// from the executor's `join/support.rs`, the only analytical call sites this
/// change touches). The `query/select.rs` paths (`"vector"`,
/// `"row_count"`, `"columnar_scan"`, `"row_count_filter"`, `"row_group"`,
/// `"par_scan_agg"`, `"par_streaming_agg"`) share this API but are not
/// incremented until those sites are instrumented.
pub fn note_analytical_path(path: &str) {
    let counters = counters();
    counters.analytical_total.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut guard) = counters.paths.lock() {
        *guard.entry(path.to_string()).or_insert(0) += 1;
    }
}

/// Records `bytes` of sort-spill output on the process total.
///
/// Called once per over-budget sort with that sort's cumulative spilled
/// bytes (next to the sort's existing spill trace site), so the total never
/// double-counts. Saturates instead of wrapping.
pub fn record_spill_bytes(bytes: u64) {
    let total = &counters().spill_bytes_total;
    let _ = total.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(bytes))
    });
}

/// Records growth of result-buffer bytes held (gauge).
///
/// Also raises the peak when growth exceeds it. Saturates instead of
/// wrapping. The owning worker calls [`record_result_buffer_shrink`] on
/// release; until it wires these, the gauge reports zero.
pub fn record_result_buffer_grow(bytes: u64) {
    let current = &counters().result_buffer_bytes;
    let next = current
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(bytes))
        })
        .map_or(u64::MAX, |previous| previous.saturating_add(bytes));
    counters()
        .result_buffer_peak
        .fetch_max(next, Ordering::Relaxed);
}

/// Records release of result-buffer bytes held (gauge).
///
/// Saturates at zero: an unmatched release never underflows the gauge.
pub fn record_result_buffer_shrink(bytes: u64) {
    let current = &counters().result_buffer_bytes;
    let _ = current.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(bytes))
    });
}

/// Records one admission decision: `true` granted query entry, `false`
/// refused it. The worker that owns the admission hook calls this; until
/// it wires it, both totals report zero.
pub fn record_admission(admitted: bool) {
    let counters = counters();
    if admitted {
        counters.admission_admitted.fetch_add(1, Ordering::Relaxed);
    } else {
        counters.admission_rejected.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records a query starting to wait on admission (waiting gauge +1).
pub fn record_admission_queued() {
    counters().admission_waiting.fetch_add(1, Ordering::Relaxed);
}

/// Records a waiting query leaving the admission queue (gauge -1).
///
/// Saturates at zero: an unmatched dequeue never underflows the gauge.
pub fn record_admission_dequeued() {
    let waiting = &counters().admission_waiting;
    let _ = waiting.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(1))
    });
}

/// Copies every counter into a [`Snapshot`].
///
/// Per-path rows include zero markers for [`SKIPPED_ANALYTICAL_PATHS`] that
/// were never recorded, so SQL consumers can distinguish "not instrumented"
/// from "instrumented but never taken" by joining against the marker set.
#[must_use]
pub fn snapshot() -> Snapshot {
    let counters = counters();
    let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
    let mut analytical_by_path: Vec<(String, u64)> = counters
        .paths
        .lock()
        .ok()
        .map(|guard| {
            guard
                .iter()
                .map(|(path, count)| (path.clone(), *count))
                .collect()
        })
        .unwrap_or_default();
    for skipped in SKIPPED_ANALYTICAL_PATHS {
        if !analytical_by_path.iter().any(|(path, _)| path == skipped) {
            analytical_by_path.push((skipped.to_string(), 0));
        }
    }
    analytical_by_path.sort_by(|left, right| left.0.cmp(&right.0));
    let mut slow_by_class: Vec<(String, SlowClassStat)> = counters
        .classes
        .lock()
        .ok()
        .map(|guard| {
            guard
                .iter()
                .map(|(class, stat)| (class.clone(), *stat))
                .collect()
        })
        .unwrap_or_default();
    slow_by_class.sort_by(|left, right| left.0.cmp(&right.0));
    Snapshot {
        queries_total: load(&counters.queries_total),
        errors_total: load(&counters.errors_total),
        connections_opened: load(&counters.connections_opened),
        connections_closed: load(&counters.connections_closed),
        slow_queries_total: load(&counters.slow_queries_total),
        slow_max_ms: load(&counters.slow_max_ms),
        maintenance_passes_total: load(&counters.maintenance_passes_total),
        checkpoints_total: load(&counters.checkpoints_total),
        analytical_total: load(&counters.analytical_total),
        analytical_by_path,
        slow_by_class,
        spill_bytes_total: load(&counters.spill_bytes_total),
        result_buffer_bytes: load(&counters.result_buffer_bytes),
        result_buffer_peak: load(&counters.result_buffer_peak),
        admission_admitted: load(&counters.admission_admitted),
        admission_rejected: load(&counters.admission_rejected),
        admission_waiting: load(&counters.admission_waiting),
        vector_exact_queries: load(&counters.vector_exact_queries),
        vector_ann_queries: load(&counters.vector_ann_queries),
        vector_ann_cache_hits: load(&counters.vector_ann_cache_hits),
        vector_ann_cache_misses: load(&counters.vector_ann_cache_misses),
        vector_ann_fallbacks: load(&counters.vector_ann_fallbacks),
        vector_sparse_queries: load(&counters.vector_sparse_queries),
        vector_binary_queries: load(&counters.vector_binary_queries),
        vector_index_builds: load(&counters.vector_index_builds),
        tls_connections: load(&counters.tls_connections),
        tls_handshake_failures: load(&counters.tls_handshake_failures),
        tls_plaintext_refusals: load(&counters.tls_plaintext_refusals),
        auth_failures: load(&counters.auth_failures),
        authz_denials: load(&counters.authz_denials),
        account_lockouts: load(&counters.account_lockouts),
        auth_locked_out: load(&counters.auth_locked_out),
        legacy_auth_uses: load(&counters.legacy_auth_uses),
        txn_started: load(&counters.txn_started),
        txn_committed: load(&counters.txn_committed),
        txn_aborted: load(&counters.txn_aborted),
        txn_conflicts: load(&counters.txn_conflicts),
        txn_active: load(&counters.txn_active),
        txn_duration_us: counters.txn_duration_us.snapshot(),
        txn_commit_latency_us: counters.txn_commit_latency_us.snapshot(),
        engine_mutex_wait_us: counters.engine_mutex_wait_us.snapshot(),
        gate_wait_us: counters.gate_wait_us.snapshot(),
        wal_barrier_wait_us: counters.wal_barrier_wait_us.snapshot(),
        group_commit_wait_us: counters.group_commit_wait_us.snapshot(),
        maintenance_queue_wait_us: counters.maintenance_queue_wait_us.snapshot(),
        wal_bytes_total: load(&counters.wal_bytes_total),
        wal_appends_total: load(&counters.wal_appends_total),
        wal_checkpoint_bytes_total: load(&counters.wal_checkpoint_bytes_total),
        wal_backlog_bytes: load(&counters.wal_backlog_bytes),
        wal_rotations: load(&counters.wal_rotations),
        wal_flush_us: counters.wal_flush_us.snapshot(),
        wal_sync_us: counters.wal_sync_us.snapshot(),
        buffer_hits: load(&counters.buffer_hits),
        buffer_misses: load(&counters.buffer_misses),
        buffer_evictions: load(&counters.buffer_evictions),
        buffer_dirty_pages_last_sync: load(&counters.buffer_dirty_pages_last_sync),
        checkpoint_bytes_total: load(&counters.checkpoint_bytes_total),
        checkpoint_duration_us: counters.checkpoint_duration_us.snapshot(),
        columnar_segments_examined: load(&counters.columnar_segments_examined),
        columnar_segments_skipped: load(&counters.columnar_segments_skipped),
        columnar_columns_read: load(&counters.columnar_columns_read),
        columnar_batches: load(&counters.columnar_batches),
        spill_files_total: load(&counters.spill_files_total),
        vector_rebuilds: load(&counters.vector_rebuilds),
        vector_rebuild_us: counters.vector_rebuild_us.snapshot(),
        vector_fresh: load(&counters.vector_fresh),
        vector_stale: load(&counters.vector_stale),
        admin_by_action: counters
            .admin_actions
            .lock()
            .ok()
            .map(|guard| {
                let mut rows: Vec<(String, u64)> = guard
                    .iter()
                    .map(|(action, count)| (action.clone(), *count))
                    .collect();
                rows.sort_by(|left, right| left.0.cmp(&right.0));
                rows
            })
            .unwrap_or_default(),
    }
}

/// Resets every counter to zero. Test isolation only — never called by
/// production code. Concurrent recorders during a reset may have their
/// increment overwritten; tests must not record from other threads while
/// resetting. Intentionally ungated (no `#[cfg(test)]`) so integration tests,
/// which build against the non-test crate profile, can isolate too.
pub fn reset_for_tests() {
    let counters = counters();
    let store = |value: &AtomicU64| value.store(0, Ordering::Relaxed);
    store(&counters.queries_total);
    store(&counters.errors_total);
    store(&counters.connections_opened);
    store(&counters.connections_closed);
    store(&counters.slow_queries_total);
    store(&counters.slow_max_ms);
    store(&counters.maintenance_passes_total);
    store(&counters.checkpoints_total);
    store(&counters.analytical_total);
    store(&counters.spill_bytes_total);
    store(&counters.result_buffer_bytes);
    store(&counters.result_buffer_peak);
    store(&counters.admission_admitted);
    store(&counters.admission_rejected);
    store(&counters.admission_waiting);
    store(&counters.vector_exact_queries);
    store(&counters.vector_ann_queries);
    store(&counters.vector_ann_cache_hits);
    store(&counters.vector_ann_cache_misses);
    store(&counters.vector_ann_fallbacks);
    store(&counters.vector_sparse_queries);
    store(&counters.vector_binary_queries);
    store(&counters.vector_index_builds);
    store(&counters.tls_connections);
    store(&counters.tls_handshake_failures);
    store(&counters.tls_plaintext_refusals);
    store(&counters.auth_failures);
    store(&counters.authz_denials);
    store(&counters.account_lockouts);
    store(&counters.auth_locked_out);
    store(&counters.legacy_auth_uses);
    store(&counters.txn_started);
    store(&counters.txn_committed);
    store(&counters.txn_aborted);
    store(&counters.txn_conflicts);
    store(&counters.txn_active);
    counters.txn_duration_us.reset();
    counters.txn_commit_latency_us.reset();
    counters.engine_mutex_wait_us.reset();
    counters.gate_wait_us.reset();
    counters.wal_barrier_wait_us.reset();
    counters.group_commit_wait_us.reset();
    counters.maintenance_queue_wait_us.reset();
    store(&counters.wal_bytes_total);
    store(&counters.wal_appends_total);
    store(&counters.wal_checkpoint_bytes_total);
    store(&counters.wal_backlog_bytes);
    store(&counters.wal_rotations);
    counters.wal_flush_us.reset();
    counters.wal_sync_us.reset();
    store(&counters.buffer_hits);
    store(&counters.buffer_misses);
    store(&counters.buffer_evictions);
    store(&counters.buffer_dirty_pages_last_sync);
    store(&counters.checkpoint_bytes_total);
    counters.checkpoint_duration_us.reset();
    store(&counters.columnar_segments_examined);
    store(&counters.columnar_segments_skipped);
    store(&counters.columnar_columns_read);
    store(&counters.columnar_batches);
    store(&counters.spill_files_total);
    store(&counters.vector_rebuilds);
    counters.vector_rebuild_us.reset();
    store(&counters.vector_fresh);
    store(&counters.vector_stale);
    if let Ok(mut guard) = counters.paths.lock() {
        guard.clear();
    }
    if let Ok(mut guard) = counters.classes.lock() {
        guard.clear();
    }
    if let Ok(mut guard) = counters.admin_actions.lock() {
        guard.clear();
    }
}
