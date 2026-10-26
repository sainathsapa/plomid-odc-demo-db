# 07 — Real workloads and anti-overfitting

Evidence: `artifacts/probes/logs/workload_probe.log` (11/11),
`hnsw_dim64_investigate.log`, `scale_probe.log`.

## Workload families — PASS

The workload probe exercises OLTP (row inserts/updates/point reads),
OLAP (aggregates, GROUP BY, window shapes on the same tables), and HTAP
interleavings with deterministic oracles computed client-side:

- duplicates/NULL handling: `DISTINCT` with NULL → `[1, 2, 3, None]` —
  PASS;
- HTAP top-k returns exactly 10 rows with the relational aggregate side
  intact (`A/B/C/D = 250` each) — PASS;
- timings are recorded as NOTES only (insert=238 ms, point×200=155 ms);
  no tail claims are made.

## Anti-overfitting evidence (this audit’s independent fixtures)

The tranche’s quality matrix uses calibrated fixtures with declared
floors. To probe generalization, this audit built **new, independent**
fixtures with the exact oracle and deterministic seeds:

- 50 k × dim-64 sine-structured vectors (reconstruction of the scale
  probe’s phase-B generator) and a uniform-random variant (seed
  20261016);
- per-query exact top-10 ids and real L2 distances, ANN result ids, and
  cache deltas recorded side by side.

Result: recall cliffs the calibrated matrix does not surface
(0/10 and 6/10; wrong-region clusters; distances logged per id) — F-008.
This does not contradict the matrix (its fixtures pass as declared); it
bounds the claim “ANN quality validated” to the declared fixtures and
dimensions, and it shows the cliffs are not an ef-tuning artifact (ef
sweep has no effect post-build).

A second anti-overfit check: the storage-split defect (F-001) was hit by
an *unmodified* bulk-load path in the lifecycle bench; the bench now
drops the auto index before loading and documents why (source comment).
That is an honest local workaround but means the benchmark no longer
measures the failing path — flagged in `13`.

## Workload-level interaction (notes)

- Write-write contention: second writer commits after ~5 s with no
  serialization error; the first session is reaped by the
  idle-in-transaction timeout (see `09`).
- DDL inside a transaction is accepted (note; see `09`).
- On large tables, per-connection catalog load (~5 s, F-006) dominates
  short-lived client workloads unless pooling is used.
