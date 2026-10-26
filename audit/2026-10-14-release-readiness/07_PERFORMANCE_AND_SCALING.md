# 07 — Performance and scaling

Environment for every number: M2 Pro/12c/16G, macOS, loopback psycopg,
machine load ~3–4. **Debug binary unless marked REL.** Debug numbers are
correctness-provenance, not SLA claims.

| Workload | Result | Provenance |
| -------- | ------ | ---------- |
| 100K multi-row INSERT (2000-row batches) | 42.5 s, 2,352 rows/s | debug, `artifacts/bulk.py` |
| 10K same method | 4.96 s, ~2K rows/s (same rate → not scale degradation) | debug |
| Batch shape 100/500/2000 (2000 rows total) | 54/248/946 ms per stmt (linear per-row ~0.5 ms, no batch-quadratic) | debug, `batchshape.py` |
| executemany 2000×1-row autocommit | 11.11 s, 180 rows/s (per-statement overhead dominates) | debug, `exmany.py` |
| 100K `COUNT(*)` / `SUM` / selective filter / `DISTINCT` | 34 / 89 / 76 / 168 ms | debug, all exact |
| 100K group-by-97 exact | 488 ms | debug, oracle-verified |
| Prior release-mode 10K/100K inserts | 50K / 43K rows/s | prior audit, REL binary, unloaded box — **not re-run on this HEAD** |

Reads are fast; writes pay ~0.5 ms/row debug (PK checks + WAL + commit per
batch) and ~5.5 ms/statement round-trip+commit for single-row autocommit.
The ~20× gap to prior release numbers is attributed to build (debug vs
release) + load + method, explicitly **not** to a product change (same-rate
at 10K vs 100K rules out scale degradation in this range).

Cliffs/risks: single-row-autocommit chatty clients (180 rows/s) need
batching; no spill/backpressure limits proven at large intermediates;
`MAX_STAGED_*` caps unenforced (DoS-adjacent, §8). Profiles (perf/flamegraph)
NOT RUN — no hotspot beyond per-row-cost characterization is claimed.
Contaminated/invalid runs: none reported as results (the two probe artifacts
in §3 are documented as probe bugs, engine exonerated).
