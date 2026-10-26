# Beta 2 — 23 Performance Architecture Rules (Phase 23)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.
Rule: the architecture below is preserved as measured fact. Every feature
must stay live, reachable, measurable, fallback-correct, portable,
memory-bounded, and crash-safe. No full cost-based planner until workload
evidence proves the need.

## Feature register (all verified live in-tree)

| Feature | Liveness proof | Fallback / bound |
|---|---|---|
| Streaming row execution | `executor` streams rows; `rows_to_stream` bridge + 1000-row wire batches (`handler.rs`) | bounded batch quantum; backpressure preserved |
| Bounded analytical memory | `SCAN_MATERIALIZE_MAX_ROWS=250k`, `SORT_SPILL_MAX_ROWS=250k` (`executor/src/query/select.rs`) | spill to `plomid-spill` `SpillManager` (1 GiB/1024-file caps), k-way merge, `Drop` unlink |
| Vectorized columnar execution | `crates/columnar` vectorized kernels + benches | scalar path where unjustified; pruning-gated |
| Zone-map/BRIN pruning | columnar pruning (`PRUNING_FORMAT_VERSION=1`) | correctness independent of pruning (pruning only skips) |
| XorFilter where proven | owned `XorFilter` in storage segment path; Roaring copy removed | executor `join/bloom.rs` documents the single remaining role |
| BTree durable index | `crates/index` BTree, durable generations | crash-safe; checksummed |
| ART derived/cache index | `index/tree.rs` ART non-durable, lazy rebuild | cache miss → durable BTree/heap; never authoritative |
| Lazy ART | built on demand per database, dropped on USE switch | unbuilt state is normal, not an error |
| HTAP governor | `crates/htap` governor + gauntlet penalty metric | degrades to sequential, never wrong |
| Spill | `crates/spill` bounded temp + per-write limit checks | fail-closed over cap |
| Result streaming | `ResultStream`/backpressure, peak ≤ cap+1 | reserve-before-send, cancel+drain |
| Hardware dispatch | `crates/hw/src/vector` scalar/NEON/AVX2/AVX-512, bit-identical folds | runtime selection; scalar always available |
| Exact vector fallback | exact heap TopK; ANN refusal above `max_ann_rows` serves exact | `vector_durability_convergence` proves fallback on torn/corrupt ANN |
| ANN as accelerator | HNSW/IVF/DiskANN/SQ/PQ backends, freshness-gated | stale/freshness-failed → exact; never serves corruption |
| Fail-closed freshness | freshness gates + `REINDEX` rebuild from authoritative rows | deleted/corrupt derived cache → rebuild, no loss |

## Anti-bloat gates (verified, not aspirational)

- **Parallel TopK stays gated**: `PAR_TOPK_MIN_ROWS=1_000_000`
  (`executor/src/query/select.rs:92-103`) with the measured justification in
  comments (100k expression-key TopK ~1.4x *slower* parallel, 76 vs 53 ms
  best-of-3 release) — engagement needs 1M-scale curves proving a win
  before lowering. Not enabled for looks.
- **Oversampled ANN is live and bounded**: `vector.candidate_oversample`
  (default 8, range 1–64) feeds exact rerank (`search/rerank.rs`), capped by
  `MAX_CANDIDATE_CAP` and `vector.max_ann_rows` (refuse→exact). Not staged-off.
- **No cost-based planner inflation**: `crates/optimizer` stays rule/shape
  based; every regression must attribute to CPU/allocation/lock/WAL/IO/etc.
  per the acceptance model — do not fix blind.

## Status

GREEN (all 15 features live with fallback/bound evidence; both anti-bloat
gates hold with measurements in comments). Any change weakening a fallback
or lowering a gate without new 1M-scale curves fails this phase.
