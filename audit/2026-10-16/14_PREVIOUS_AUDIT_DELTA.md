# 14 — Previous audit delta (2026-10-15 → this cycle)

Baseline: `audit/2026-10-15/15_FINDINGS_REGISTER.md` (commit `c9298e9`),
which carried dispositions of the 2026-10-14-release-readiness and
2026-10-13 audits. Dispositions below are this cycle's, live-evidenced.

## Findings

| ID | Baseline | This cycle | Evidence |
|---|---|---|---|
| PLOMID-REAUDIT-001 (P1, concurrent-DDL ID collision) | not reproduced; blocked modes 4–5 | **Still NOT REPRODUCED** (interleaved create/drop 4×25 clean). Related **F-004** found live: concurrent same-name CREATE — both succeed (name-level race, P1). ID-level collision remains UNVERIFIED. | `catalog_ddl_probe.log`, `namesake_race_probe.log` |
| PLOMID-REAUDIT-002 (P2, untyped param division) | documented limit + casts | **UNCHANGED / accepted as documented**: `%s/%s` floats → `'1'` text; `::float8` and `CAST(... AS DOUBLE PRECISION)` hatches verified. | `param_avg_ilike_probe.log` (regenerated) |
| PLOMID-REAUDIT-003 (P2, AVG typing + DROP ROLE unsupported) | reproducible | **CLOSED**: `pg_typeof(AVG(float8))='float8'`; `AVG(int)` numeric (PG parity); `DROP ROLE` implemented with guards (bootstrap/privileges). | `param_avg_ilike_probe.log`, `rbac_probe.log` |

## Evidence gaps

| Gap | Status this cycle |
|---|---|
| G-01 full workspace suite | **INTERRUPTED (scope cut)** — operator stopped the chain at ~51 min (still compiling); no gate completed, recorded as evidence gap in `12` / `artifacts/suite/INTERRUPTED.md` |
| G-02 1 M+ scale | **ADVANCED** — 1 M-row load measured (10 418 rows/s); 100 k/50 k probes; 128 k lifecycle cell ran (timings lost — see `13`) |
| G-03 disk-full / WAL-full / OOM | **NOT RUN** |
| G-04 torn-WAL/partial-checkpoint chaos | **PARTIAL** — torn tail → explicit startup refusal PASS; kill -9 recovery PASS; no systematic chaos matrix |
| G-05 Linux x86_64 / ARM64 runtime | **NOT RUN** (macOS arm64 only) |
| G-06 ANN recall@k curve | **PARTIAL + strengthened** — qualify/matrix green; independent dim-64 fixtures found cliffs (F-008); full recall-vs-k curve not swept |
| G-07 hot backup / PITR | **BY DESIGN PARTIAL** — hot backup refused (deliberate); PITR out of scope; needs explicit Beta exclusion statement |
| G-08 SLSA attestation | **NOT RUN** |
| G-09 ILIKE live matrix | **CLOSED** — 10/10 PASS; new WHERE-ESCAPE deviation (F-005); docs staleness (F-011) |

## New findings introduced this cycle

F-001 storage B+Tree split (top), F-002 REINDEX wire no-op, F-003
ANALYZE wire no-op, F-004 same-name CREATE race, F-005 WHERE ESCAPE,
F-006 per-connection catalog load, F-007 boot-1 auth anomaly
(observed-once), F-008 dim-64 recall cliffs, F-009 duplicate ANN pass,
F-010 EXPLAIN-vs-execution, F-011 docs staleness, F-012 GRANT ALL note,
F-013 counter semantics. Register: `15`.
