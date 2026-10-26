# 08 — Performance and scale (release binary `6c861f51`, M2 Pro, disposable dirs)

Re-measured on the current commit; prior figures (43–50K rows/s bulk,
~0.2 ms point, ~2 ms filter, ~110 ms GROUP BY at 100K) kept as baseline
claims only. Raw: `artifacts/probes/workloads.log`, `scale100k.log`.

| Workload | Measured |
|---|---|
| Bulk multi-row insert 1K | 36,719 rows/s (0.03 s) |
| Bulk multi-row insert 10K | 31,929 rows/s (0.31 s) |
| Bulk multi-row insert 100K | 42,195 rows/s (2.37 s) |
| `COUNT(*)` 10K / 100K | 0.8 ms / 7.5 ms |
| Point read id=42424 at 100K | 0.3 ms (0.14–0.17 ms at 1–10K) |
| Filter `g=5` (~1K rows) at 100K | 27.0 ms (3.4 ms at 10K) |
| GROUP BY 97 groups at 100K | 82.2 ms (6.6 ms at 10K) |
| 8-way unique-race fan-in | 0.05 s, exactly-once |

Throughput/latency only; p50/p95/p99 distributions, CPU/RSS/WAL bytes,
lock-wait, prune counts NOT measured — single-sample timings, no tail
claims. Debug-build numbers from the interim audit are not repeated here.
Ceiling validated: 100K rows. Above 100K (1M/1M+): NOT RUN, no extrapolation.
No benchmark settings were changed to obtain these numbers.
