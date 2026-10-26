# 16 — Release decision

> **Superseded for the current source tree:** see the
> [2026-10-23 release decision refresh](../2026-10-23/RELEASE_DECISION.md).
> This file remains the independent audit snapshot at `6890955`.

Audited artifact: commit `6890955`, binary
`sha256 8b0a6b60…972f`, single macOS arm64 host. No product source
modified. Full evidence: `README.md` index; findings: `15`.

## Decisions

| Mode | Decision | Conditions / blockers |
|---|---|---|
| **M1 — Local use** (loopback, single user, disposable data) | **GO** | None beyond ordinary budget. Former P0s verified intact; DDL envelope verified; suites green where claimed. |
| **M2 — Local Beta** (loopback, real data, operator present) | **GO WITH CONDITIONS** | 1) Document F-001 trigger + workaround (drop vector auto index before bulk load) or fix the split before any load >~2 k rows at dim ≥ 64. 2) Treat `REINDEX`/`ANALYZE` as non-operational until F-002/F-003 are fixed (do not script around them). 3) Record the dim-64 ANN caveat (F-008) in the Beta notes. 4) fmt drift (`12`) is a hygiene item, not a blocker. |
| **M3 — Secure remote single-tenant** | **GO WITH CONDITIONS** | All of M2, plus: pool connections in front (F-006, ~5 s/connect on large catalogs), monitor post-crash lockouts (F-007), keep TLS-required mode (verified). |
| **M4 — Multi-user remote** | **NO-GO** | F-004 (concurrent same-name CREATE both succeed) must be fixed and regression-tested; F-007 unclosed; re-audit write/write + DDL interleavings after the fix. |
| **M5 — Cloud/SaaS** | **NO-GO** | No HA/PITR statement for Beta (G-07 needs explicit exclusion wording), G-03/G-04/G-05/G-08 unverified, single-host measurements only, F-001/F-004/F-006 unresolved. |

## Prioritized next actions

1. **F-001** — fix or guard the storage split (highest user-visible
   impact; blocks trustworthy insert-path claims).
2. **F-004** — close the concurrent same-name CREATE race; extend
   `concurrent_ddl_ids.rs`.
3. **F-002 / F-003** — decide the admin-statement contract (execute vs
   documented stub) and make protocol, executor, and docs agree.
4. **F-008** — add a dim-64 clustered quality fixture (floor or
   documented limitation) and investigate the build-time region gap.
   DiskANN is the worst case: recall@10 0.00/0.20 at 2 k/8 k on the
   bench's own clustered fixture (`06` §3).
5. **F-005** — implement ESCAPE in the WHERE-scope matcher.
6. **F-006** — cache the catalog image per session or document pooling
   as a hard requirement; re-measure.
7. **F-007** — add crash-restart instrumentation; capture on recurrence.
8. **F-009/F-010/F-011/F-012/F-013** — cleanup pass with the docs and
   bench fixes listed in `13`/`15`.

## Method notes and residual uncertainty

- Verdicts separate IMPLEMENTED / MEASURED / UNSUPPORTED / UNVERIFIED;
  no production-readiness inference from test counts.
- Single-host, no-tail measurement regime; build-time variance for
  50 k HNSW recorded honestly (10.5 s vs 40.9–44.1 s samples).
- Two staged probe logs were regenerated with their fixed scripts
  (`p0_revalidation` → 30/30; `param_avg_ilike_probe` → 11/13 with two
  real ESCAPE FAILs); the bundle contains the authoritative runs.
- The 128 k lifecycle run completed its boot-0 cells but the timing
  table was not preserved (disclosed in `06`/`13`); the compact 2 k/8 k
  matrix re-run (incl. restart, dim-64 DiskANN recall 0.00/0.20 —
  extends F-008) is recorded in `06` §3.
