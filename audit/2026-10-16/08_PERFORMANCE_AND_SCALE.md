# 08 — Performance and scale (single host, M2 Pro, 16 GiB)

Evidence: `artifacts/probes/logs/scale_probe.log`,
`restart_measurement.txt`, lifecycle logs, `04` (F-006).

## Storage / relational (measured, NOTES)

| Workload | Result |
|---|---|
| 100 k-row INSERT | 2.7 s → 37 351 rows/s |
| 500 point SELECTs | 16 496 ms total → 33.0 ms/op |
| Aggregate over 10 k of 100 k | 38 ms |
| 50 k × dim-64 vector INSERT | 5.1 s → 9 808 rows/s |
| 1 M-row INSERT | 96.0 s → 10 418 rows/s |
| Filter count over 1 M | 1 000 rows in 1 153 ms |

Single-host samples; no tail claims. The 1 M-row load closes the
“1 M+ scale” evidence gap at the load level (not at query-concurrency
level).

## F-001 — the shadow over all insert numbers

INSERTs into a fresh vector table **while its auto-created vector index
exists** fail from ~2–4 k rows (dim 64): `B+Tree node exceeds page
payload`. Minimal repro: `storage_split_fail.log` (batch 2 fails) vs
`storage_split_control.log` (auto index dropped → 4 000 rows OK);
mechanism notes in `artifacts/storage-split/storage_split_mechanism.txt`.
The lifecycle bench drops the auto index before bulk load (with an
in-source comment); the 9 808/10 418 rows/s numbers above are therefore
measured on tables **without** a live vector index during the load.
Fix or fail-closed guard required (F-001; see `15` for acceptance).

## ANN latency and build (dim 64)

| Case | Cold (build) | Warm p50/med | Notes |
|---|---|---|---|
| HNSW 8 k | 3.2 s | 41 ms | in-memory; restart rebuild 3.3 s |
| DiskANN 8 k | 38.7 s | 61 ms | ~5 ms/vector; restart rebuild 42 s |
| HNSW 50 k | 10.5 s (scale probe) / 40.9–44.1 s (later sessions) | 127–267 ms | high variance across runs/load; same fixture and binary |
| HNSW 50 k, second query | — | ~125 ms | duplicate-pass anomaly under some patterns (F-009) |

Warm queries hit the cache (hits ≥ 1 per query); no rebuilds while the
table is unchanged (watermark-stable). The 50 k build-time variance
(10.5 s vs 40.9–44.1 s) is recorded honestly as observed; a controlled
isolation run was not attempted (time budget), and no claim of typical
build time should cite one sample.

## F-006 — per-connection catalog load

Big data directory (7.5 MB catalog; lifecycle alloy with 8 tables +
1.4 GB data): every **new connection** takes ~4.9–5.0 s before its first
server message, because `Executor::new` loads the full catalog image per
connection (`crates/executor/src/executor.rs:191-192`). Small dirs:
~23 ms. Server boot itself accepts in 114–149 ms. Impact: short-lived
clients and monitoring/health checks pay seconds per connect on large
catalogs; connection pooling or a session catalog cache is required
before M3/M4 exposure.

## Restart/scale acknowledgment

- Boot accept/ready stays sub-200 ms regardless of catalog size; the
  per-connection cost above is client-side.
- The compact lifecycle matrix re-run (2 k/8 k × hnsw/diskann, incl.
  restart) is recorded in `06` §3; the earlier 128 k-family run
  completed its boot-0 cells (2 k–128 k) but its timing table was not
  preserved (disclosed gap).
- Query-level concurrency/throughput under multiple clients was not
  re-measured this cycle (existing suite exercises functionality, not
  throughput).
