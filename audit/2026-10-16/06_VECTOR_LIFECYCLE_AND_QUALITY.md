# 06 — Vector lifecycle, DDL envelope, and quality

Evidence: `artifacts/probes/logs/vector_commit_probe.log` (77/77),
`qualify/qualify.log` (83/0), `quality-matrix/quality_final.log` (2/0),
`hnsw_dim64_investigate.log`, `hnsw_ef_sweep.log`,
`hnsw_reindex_ef_probe.log`, `reindex_semantics_probe.log`,
`scale_probe.log`, `restart_measurement.txt`, lifecycle bench logs under
`lifecycle/` (see “Compact re-run” below).

## 1. DDL envelope: executable or explicitly rejected — PASS

The tranche's core claim re-verified live (77/77):

- `IVF_FLAT`, `IVF_SQ`, `IVF_PQ`, HNSW, DiskANN, BINARY, FLAT index DDL
  all execute, and the ANN path returns the same top-k as the exact scan
  on the probe fixtures; every executed method shows a real cache
  `miss=1 builds=1` on first query and hits afterwards.
- Incompatible combinations are rejected **at DDL time** with explicit
  reason: e.g. `USING hnsw` on `VECTOR(BINARY, 64)` →
  `VECTOR_INDEX_UNSUPPORTED: method "hnsw" cannot execute on VECTOR(BINARY, 64);
  binary columns accept USING BINARY (Hamming ANN) or USING FLAT (exact)`.
  No accepted-but-unexecutable index was found.
- IVF-SQ/PQ: covered as executed methods on the live path; the tranche's
  resolution of the former claim rows was doc/metadata-level (no
  partition implementation), matching what the audit sees: quantized
  methods run through the same cached pipeline.

## 2. ANN cache semantics (code + live)

- Cache key = (database, table, column, WHERE-fingerprint, metric);
  freshness = (write watermark, row count, live vectors). Miss → build
  (bounded by `vector.max_ann_rows`, memory governor); hit → search.
- `vector.ef_search` is captured **at build time**
  (`vector_ann.rs:147-150` → `with_ef_search`; search uses the built
  backend’s ef). Live: after the graph exists, `SET vector.ef_search =
  400` changes neither results nor ids at 50 k; an ef sweep
  (100/400/1600) on the sine fixture gives identical ids and best
  distance every time; `ef=6400` is rejected by the registry
  (`1..=4096`). No `REINDEX`-style per-query re-tune exists — and
  `REINDEX` is a wire no-op anyway (F-002, `04`).
- `SET vector.*` applies server-wide in v1 (session `SET` is global
  state) — relevant when multiple clients share a server.
- Counters: `ann_cache_hits_total` counts run_ann invocations; one query
  can legitimately show ≥1 hit (see F-009).

## 3. Lifecycle numbers (dim 64, 8 k rows, single host; fresh data dir)

| method | pre-restart cold | warm p50 | post-restart cold | post-restart warm p50 | data |
|---|---|---|---|---|---|
| hnsw | 3 231 ms (miss=1, builds=1) | 41 ms | 3 326 ms (rebuild) | 40 ms | count=8000, ids identical |
| diskann | 38 740 ms | 61 ms | 42 027 ms (rebuild) | 55 ms | count=8000, ids identical |

- Boot times: accept=114–149 ms, ready=165–179 ms (fresh + restart).
- Index state after restart: `READY` (both).
- DiskANN costs ~5 ms/vector to build at dim 64 and **restart rebuilds
  from snapshot rows** even though `graph.dat` (737 KB) and
  `vectors.dat` (2.2 MB) exist on disk; log:
  `diskann checkpoint ready; rebuilding from snapshot rows`. Checkpoint
  *persistence* is real; checkpoint *reuse for restart speed* is not
  (verdict for the tranche's restart-recovery requirement).
- HNSW is in-memory and rebuilt from rows on restart (deterministic ids;
  matches documentation).

### Compact lifecycle matrix (2 k / 8 k, dim 64, hnsw+diskann, incl. restart)

Run: `tests/python/vector_lifecycle_bench.py`, fresh data dir, release
binary `8b0a6b60…972f`, port 6191, `QUERIES=8 K=10` (clustered 8-center
fixture; loads ran index-free per the bench's F-001 workaround comment).
Raw evidence: `artifacts/lifecycle/compact_lifecycle.json` (counter
deltas, cache listings, log classification embedded),
`compact_lifecycle.md`, `compact_bench_stdout.txt`, restart boot log
`compact_server_01.log`; boot-0 log (7.7 MB debug) stays in scratch —
its checkpoint lines are extracted in `compact_server_00_extract.txt`.

| method | n | ddl | cold (first query) | warm p50 | "rebuild"* | restart cold | restart warm p50 | recall@10 | state |
|---|---|---|---|---|---|---|---|---|---|
| hnsw | 2 000 | 4 ms | 271 ms (miss=1, build=1) | 7.7 ms | 9 ms | 298 ms (miss=1, build=1) | 7.6 ms | 1.00 | READY |
| hnsw | 8 000 | 5 ms | 1 728 ms (miss=1, build=1) | 31.7 ms | 41 ms | 1 758 ms (miss=1, build=1) | 29.9 ms | 0.90 | READY |
| diskann | 2 000 | 5 ms | 10 431 ms (miss=1, build=1) | 12.5 ms | 14 ms | 11 457 ms (miss=1, build=1) | 11.4 ms | **0.00** | READY |
| diskann | 8 000 | 5 ms | 36 811 ms (miss=1, build=1) | 41.9 ms | 54 ms | 40 394 ms (miss=1, build=1) | 41.4 ms | **0.20** | READY |

- `*rebuild` is *not* a rebuild: the query after `REINDEX` is a warm
  cache hit on every cell (`rebuild_miss=0`) — independent
  re-confirmation of F-002 (the bench's `rebuild_ms` column is
  meaningless by construction, as flagged in `13`).
- Counter deltas prove the path (not just latency): every cold and
  restart query = `ann_cache_misses_total+1`, `index_builds_total+1`;
  the build dominates `cold_ms` (e.g. hnsw 8 k: 1 706 ms of 1 728 ms).
  Warm queries show `ann_cache_hits_total` deltas above 1/query
  (12 per 8 warm queries; 10 per 5 post-rebuild queries) with no
  miss-priced latency — see F-009/F-013 for counter semantics;
  `ann_fallbacks_total` stayed 0.
- Boots (same fresh dir): boot 0 accept=110 ms / ready=175 ms;
  restart accept=135 ms / ready=163 ms; graceful SIGTERM shutdown
  123 ms. The boot-1 auth anomaly (F-007) did **not** recur — it
  remains observed-once.
- Restart: all four cells `state=READY` and rebuild from snapshot rows
  at ≈ cold cost (hnsw 298/1 758 ms; diskann 11 457/40 394 ms).
  DiskANN checkpoints persisted (2 k: `graph.dat` 454 072 B +
  `vectors.dat` 544 000 B; 8 k: 745 432 B + 2 176 000 B; unchanged
  across restart) and were found **valid** on boot 1 — but still not
  used for speed: boot 0 logs `diskann checkpoint unusable (no graph
  checkpoint); rebuilding from snapshot rows` (first builds), boot 1
  logs `diskann checkpoint ready; rebuilding from snapshot rows`
  (2 lines each). Persistence real; reuse for restart speed absent.
- Build-time variance: hnsw 8 k cold = 1.7 s this run vs 3.2 s in the
  restart-only measurement above; diskann 8 k ≈ 37–39 s in both.
  Treat per-build millisecond claims as ±2× on this host.
- EXPLAIN vs execution (F-010, fresh instance): at n=2 000 both cells
  print `Vector: Vector Exact scan …` while execution demonstrably ran
  the ANN path (build priced into the measured cold query, miss=1,
  build=1 — the bench sets `vector.auto_ann_min_rows=0`). At n=8 000
  the plan prints `ANN scan`. Plan text still disagrees with execution
  below the 4 096 planner floor.
- Recall on the bench's own clustered dim-64 fixture: HNSW 1.00 (2 k)
  → 0.90 (8 k); **DiskANN 0.00 (2 k) / 0.20 (8 k)** — the dim-64
  quality cliff is not HNSW-only and is worst on DiskANN (extends
  F-008). The declared quality gates (calibrated fixtures) remain
  green; this fixture family is where they stop generalizing.
- Loads (index-free): ~8 000 rows/s (8 k×64 in ~1.0 s).

### 128 k cell (earlier lifecycle bench, boot 0)

All 8 cells (2 k/8 k/32 k/128 k × hnsw/diskann) completed their
boot-0 matrix (insert batch counts in the log confirm 2/8/32/128 rows×
1000 per table); the run then failed at its restart phase onto the
recorded auth anomaly (F-007). The bench’s stdout table was not preserved
(disclosed evidence gap); recovered from `server_00.log`: 5 DiskANN
checkpoint lines (`unusable (no graph checkpoint)` first build;
`checkpoint ready; rebuilding from snapshot rows` for subsequent
builds) — i.e. the rebuild-from-rows path is what the matrix measured.

## 4. ANN quality — declared gates green; new cliffs found

- Qualify gate: **83 passed / 0 failed** (executable-only DDL, stats
  surface shape, quality floors).
- Quality matrix (`ann_quality_matrix`): **2 passed / 0 failed** in
  112.8 s — the calibrated tripwire fixtures hold.
- Independent dim-64 investigation (50 k rows, seed 20261016, exact
  oracle, real distances recorded):
  - sine-structured fixture: recall@10 = **0/10**; ann best distance
    2.3151 vs exact worst 1.9935; the ANN result set clusters in a wrong
    region (~ids 23768–23775) rather than the query neighborhood.
  - uniform-random fixture: recall@10 = **6/10**; ANN-only ids sit at
    d≈5.2 while exact ids are d≈1.9.
  - raising `ef` (100→1600) does not change the result set; the cliff is
    a build/region-coverage issue on packed/clustered high-dim data, not
    an ef tuning issue (F-008).
- Interpretation: the declared floors are honest for their fixtures;
  they do not generalize to dim ≥ 64 clustered data. For M2 Beta this
  is a documented caveat plus a targeted investigation item.

## 5. EXPLAIN vs execution (F-010, P3)

`EXPLAIN` prints the vector cost comparison from the planner floor
(`estimated cost exact=16000.0 ann=765.8` at n=2000) and the formulation
uses the planner’s `AUTO_ANN_MIN_ROWS=4096`, while execution gating
actually follows the live `vector.auto_ann_min_rows` (0 in the probe →
execution takes the ANN path and builds). Plan text and executed
strategy can therefore disagree at small n. Cosmetic-to-moderate UX/doc
issue; no correctness impact (results equal).

## 6. Duplicate ANN pass on large tables (F-009, P3)

On the 50 k fixture, after ordinary session warm-up, a single
`ORDER BY l2_distance … LIMIT 10` response showed
`ann_queries_total +2`, `ann_cache_hits_total +2` and ~2× latency
(126 ms → 250 ms) with identical ids; the 8 k control showed exactly one
`sql::execute` per query with one pass. Mechanism not root-caused
(candidate-generation double pass vs internal retry); impact limited to
latency on large tables; recorded with raw logs
(`audit16-trace-dbl.log`, `hnsw_reindex_ef_probe.log`).

## 7. Verdict for the tranche's own claims

- “Every accepted index DDL is executable or explicitly rejected” — PASS
  (live).
- “Checkpoint reuse / restart recovery” — PARTIAL: checkpoints persist,
  restart rebuilds from rows (log-confirmed), HNSW in-memory rebuild
  deterministic.
- “ANN quality validated on declared methodology” — PASS for the
  declared matrix; new independent evidence bounds it (F-008).
- “No silent cache-invalidation surprises” — FAIL: REINDEX/ANALYZE are
  silent wire no-ops (F-002/F-003).
