# 07 — Performance and scaling

Environment (all numbers): Apple M2 Pro (12c), 16 GiB, APFS, loopback
127.0.0.1:55433, release binary `plomid-server 0.1.0-beta.2`, single psycopg
client unless noted, warm cache, medians of 1–3 runs (samples too small for
p99 — tails NOT REPORTED, see gaps). No tuning; default config.

## Measured (end-to-end PGWire, client-observed)

| Workload | Result | Source |
|---|---|---|
| Bulk INSERT 10K rows (10×1K multi-row, 1 txn) | 0.20 s → **~50K rows/s** | `probe4_bulk_perf.log` |
| Bulk INSERT 100K rows (50×2K multi-row + ON CONFLICT, 1 txn) | 2.3 s → **~43K rows/s** | `probe13_ops.log` |
| Point SELECT by PK (10K table) | **~0.17 ms** / read | `probe4` |
| Selective filter `k=42` (100/10K; ~100/100K) | 4.5 ms @10K; **2 ms @100K** | `probe4`, `probe13` |
| `COUNT(*)` 10K / 100K | 0.7 ms / **70 ms** | `probe4`, `probe13` |
| `GROUP BY k` 100 groups @10K / 997 groups @100K | 9.7 ms / **110 ms** | `probe4`, `probe13` |
| 8 concurrent writers × 50 inserts | 400/400, no errors | `probe5_concurrent.log` |
| Graceful restart (10K + vectors) | ready ~120 ms; zero diff | server log, `probe6` |
| Crash (`kill -9`) recovery replay | ~110 ms open; 7 records / 1 txn replayed | `probe7`, server log |
| Vector exact top-2 (5 rows, d=3) | <2 ms incl. EXPLAIN path | `probe3_vector.log` |

Slopes: bulk ~linear (50K→43K rows/s 10K→100K); COUNT 0.7→70 ms looks
super-linear (100× for 10× rows — needs profiling before claims; single sample).

## Profiles / resources: mostly gaps

CPU profile, allocation hot spots, lock-wait histograms, WAL bytes/fsync counts,
buffer hit ratios, pruning rows-touched-vs-returned: **not instrumented from
outside** (server emits counters via tracing; no flamegraph/`perf` run — macOS
`perf` unavailable, Instruments not driven). RSS sampled once (~4 MB at idle
start — likely under-reports; no peak-RSS under load captured). No memory-limit
run; no spill-threshold run; no connection-churn cost run.

## Bottlenecks (architectural, from code + measurements)

1. Global engine Mutex + group-commit barrier remain the write-contention
   ceiling (unchanged from prior audit; 8-way OK, 64-way NOT RUN).
2. `COUNT(*)` scaling slope needs a profile before 1M claims.
3. P0-002 blocks all ratio/float analytics regardless of speed.
4. Columnar freshness under sustained OLTP degrades analytics to row scans
   (correctness-first gating verified; SLO cost unmeasured).

## Invalid/contaminated runs

- `probe1` counts (explained: P0-001 session-state contamination — retained as
  evidence, superseded by `probe15–17` repros).
- No parallel benchmark runs overlapped live probes except the background
  `cargo test` compile (CPU-shared; timings may be slightly pessimistic —
  stated direction, not corrected).
