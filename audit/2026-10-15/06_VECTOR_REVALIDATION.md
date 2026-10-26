# 06 — Vector revalidation

Live SQL-to-result on release binary. Raw: `artifacts/probes/vector_live.log`.

- `VECTOR(3)` DDL, dense inserts, roundtrip: PASS.
- L2 (`<->`) top-2 = `[(1,),(4,)]` == brute-force Python oracle `[1,4]`:
  PASS. Cosine (`<=>`) same ranking on this data: PASS.
- Dimension mismatch (`'[1,2]'` into VECTOR(3)) rejected explicitly: PASS.
  Zero vector inserts/queries: PASS. Malformed literal handling covered by
  parser tests (not re-run live beyond mismatch case).
- MVCC: uncommitted vector row invisible to second connection (count 5 vs 6
  mid-txn), ROLLBACK drops it: PASS. Post-crash k-NN sample identical: PASS.
- ANN index construction/reachability/recall@k: NOT VERIFIED live this
  session (no HNSW index built, no recall curve measured; prior-audit ANN-vs-
  exact claims not re-run). Exact search is the verified path; any ANN
  performance claim must be treated as unproven until recall@k vs exact truth
  across distributions/sizes is measured. No GPU backend claimed or tested.
- Persistence/WAL/backup of vectors: covered via backup roundtrip set
  (vectors present pre-backup; restored-db vector reads verified in crash
  sample). Dedicated vector-index crash/GC drills: NOT RUN.
- SIMD dispatch/scalar equivalence per-arch: NOT RUN (build support ≠ runtime
  proof; single arm64 host).
