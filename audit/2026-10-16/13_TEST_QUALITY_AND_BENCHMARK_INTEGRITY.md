# 13 — Test quality and benchmark integrity

Scope: the tranche's new vector tests/benches (commit `6890955`) and the
lifecycle bench, reviewed against the observed behavior and this audit's
independent evidence. See also `12` for gate results.

## What the new tests do well

- `vector_commit_probe` (77 checks) proves **execution**, not just DDL
  acceptance: cache miss→build→hit deltas, exact-vs-ANN result
  comparison, explicit rejection reasons for incompatible methods, and
  counter assertions (`miss=1 builds=1`).
- `tests/python/vector_qualify.py` (83 checks) and
  `ann_quality_matrix` (2 tests, 112.8 s) assert declared floors with
  exact-oracle comparisons on deterministic fixtures — honest floors as
  tripwires, no threshold gaming observed.
- 13 executor vector test binaries / 110 tests pass in a targeted run
  (`artifacts/executor-tests/executor_vector_tests2.log`; the full
  workspace suite was not completed this cycle — see `12`).
- The lifecycle bench records per-cell stats deltas (`miss/builds/hits`)
  alongside wall times and keeps raw per-boot server logs — good
  practice. It also documents its own workaround for F-001 in a source
  comment. The compact 2 k/8 k run completed (incl. restart) this cycle
  — `06` §3.

## Integrity issues found

1. **Benchmark rebuild column is invalidated by F-002.** The lifecycle
   bench measures `rebuild_ms` immediately after `REINDEX`; since
   `REINDEX` is a wire no-op, that query is a warm cache hit. Any
   `rebuild`-labelled numbers from this bench (including the 128 k cell)
   must be re-derived or the bench changed to invalidate honestly
   (e.g. drop/recreate the index or restart). `rebuild_miss` fields in
   `lifecycle.json` will show 0 — a tell.
2. **Bench inserts bypass the broken path (F-001).** The auto index is
   dropped before bulk load (documented in-source). That is the right
   choice for measuring explicit indexes, but it means the bench cannot
   be cited as validation of the default CREATE+INSERT workflow.
3. **Counter semantics can mislead.** `ann_cache_hits_total` counts
   invocations and can exceed 1 per query (observed 2× at 50 k, F-009;
   1.5–2×/query on the compact 2 k/8 k cells — 12 hits per 8 warm
   queries); assertions like “hits == 1 per query” will flake even on
   small tables.
4. **EXPLAIN assertions vs execution (F-010).** Plan text uses the
   planner floor; execution follows live settings. Tests should assert
   the executed path (stats deltas), not the plan string.
5. **Partial runs lose their numbers.** `lifecycle.json`/`lifecycle.md`
   are written only after the restart phase; when the earlier run hit
   the auth anomaly (F-007), the boot-0 matrix table was lost. Write
   intermediate results incrementally. (The re-run completed including
   restart; results in `06` §3.)
6. **Probe hygiene.** During this cycle, probe-side bugs were found and
   fixed (psycopg protocol routing for `params=()`; `fetchone` vs
   `fetchall`; `DROP INDEX IF EXISTS`; `DISTINCT` + NULL oracles) and
   two staged logs predated their fixed scripts (`p0_revalidation`,
   `param_avg_ilike_probe`) — both were regenerated for the bundle.
   Any reader of earlier intermediate logs should use the regenerated
   ones.
7. **Boot-accept probe race in the lifecycle bench (harness nit,
   P3-note).** `spawn_server` sets `accept_ms` only if its TCP probe
   succeeds before the psycopg probe in the same loop iteration; if
   the listener binds in the gap, `ready_ms` is set and the loop
   `break`s with `accept_ms=None`, crashing the run on its first log
   line. Hit once this cycle (instant crash, zero measurements lost);
   the unmodified script succeeded on re-run. Recommend a re-probe or
   format fallback. Product source untouched.

## Anti-overfitting review

This audit's independent dim-64 fixtures (sine and uniform-random,
seed 20261016) returned recall@10 = 0/10 and 6/10 at 50 k with real
distances exposing wrong-region clusters (F-008), while the declared
matrix passes; the compact matrix adds **DiskANN 0.00 (2 k) / 0.20
(8 k)** on the bench's own clustered fixture (`06` §3). This is not
evidence of deliberate overfit — the matrix declares its fixtures and
floors — but it bounds the generalization of “ANN quality validated”
to those fixtures and dimensions, and it shows the current test set
would not catch the cliff. Recommended: add a dim-64 clustered fixture
(generator available in
`artifacts/probes/scripts/hnsw_dim64_investigate.py`) with a documented
minimum recall or an explicit “known limitation” marker.
