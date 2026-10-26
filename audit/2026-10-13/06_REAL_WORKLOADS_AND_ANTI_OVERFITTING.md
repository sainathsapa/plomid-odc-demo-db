# 06 — Real workloads and anti-overfitting

Seed discipline: `random.seed(42)` (10K) / `random.seed(7)` (100K); Zipf-like via
`k=i%100/997`; narrow rows `(id,k,text)`; 0–1 secondary structures (PK only);
warm cache (same process); autocommit + one explicit-txn bulk path; ratios:
bulk-load, point reads, selective filter, group-by, mixed concurrent writers.

## Workload classes vs claimed scope

1. **OLTP**: point reads ~0.2 ms; 10K bulk 50K rows/s; 100K bulk 43K rows/s;
   8-way concurrent inserts 400/400; unique race exactly-once; read-modify-write
   (`UPDATE…FROM`) correct. Hot-key/Zipf sustained throughput: NOT RUN.
2. **Analytical SQL**: filter 100K in 2 ms; `GROUP BY` 100K (997 groups) 110 ms;
   `COUNT` 100K 70 ms; joins/aggregates/windows correct ≤10K. **Float-ratio
   analytics BLOCKED by P0-002** (any `/` over non-integral floats fails).
   Spill/>memory intermediates: NOT RUN.
3. **HTAP/mixed**: writers + readers concurrently (8 writers while counting) OK;
   p95/p99 under analytical load: NOT RUN (sample sizes too small for tails).
4. **Bulk ingest/lifecycle**: variable batches (1K/2K-statement multi-row INSERTs)
   + `ON CONFLICT DO NOTHING` OK; reopen/restart identical; post-VACUUM state
   identical (effectiveness unmeasured).
5. **Vector/AI retrieval**: 5-row exact k-NN across 3 metrics + ties/zero/dup +
   rollback + restart — all correct; mixed SQL+vector under concurrent writes:
   NOT RUN; recall@k at ANN scale: NOT RUN (exact path served, by design).
6. **Protocol shape**: prepared/unprepared, pooled-vs-fresh (fresh OK; pooled
   hurt by P1-003), disconnect-mid-txn (server aborts staged txn on teardown,
   `handler.rs:684-693`), large results (100K count OK; MB-scale rows NOT RUN).
7. **Degradation**: cold-vs-warm, memory-limit, slow-disk, low-disk, WAL-backlog,
   large-WAL restart: NOT RUN.

## Anti-overfitting

- Held-out shapes (not in repo benches): `UPDATE…FROM…RETURNING`, correlated
  `EXISTS`, `CASE`-over-aggregate, `COUNT(DISTINCT)`, ILIKE, multi-statement
  driver batches — 5/7 correct-or-honestly-rejected; the batch case exposed
  P0-001 and the float case P0-002. The suite was **not** overfit to these, which
  is precisely why live held-out probing found real bugs.
- Correctness oracle: hand-computed expectations + PG semantics (no PG instance
  run side-by-side — differential harness is a P2 action, not claimed).
- Every timed query validated row content/counts before timing; setup time
  reported separately (bulk insert time includes client batching).
- No TPC claim: engine cannot run official TPC-C/H (missing FK, ILIKE, float
  division, multi-statement driver batches). Label any subset "custom, deviating".

## Scale matrix (measured this audit)

| Rows | Bulk load | Point read | Selective filter | GROUP BY | COUNT |
|---|---|---|---|---|---|
| 10K | 0.20 s (50K/s) | ~0.2 ms | 4.5 ms | 9.7 ms | 0.7 ms |
| 100K | 2.3 s (43K/s) | — | 2 ms | 110 ms | 70 ms |
| 1M/5M/10M | NOT RUN | NOT RUN | NOT RUN | NOT RUN | NOT RUN |

Hardware: M2 Pro/16 GiB, APFS, loopback, release binary, single client
(concurrency only in §4 race test). No extrapolation beyond 100K.
