# PLOMID independent whole-database release re-audit — 2026-10-16 (UTC)

Independent, read-only audit of the live PLOMID server at commit `6890955`
("fix vector contracts: executable-only DDL, honest ANN claims, quality
evidence"), branch `starting-v0.1.0-beta-2`.

Baseline: `audit/2026-10-15/` (at `c9298e9`), which itself carried the
dispositions of `audit/2026-10-14-release-readiness/` and
`audit/2026-10-13/`. Every open finding and evidence gap from that baseline is
re-tested or explicitly marked NOT RUN here (see `14_PREVIOUS_AUDIT_DELTA.md`).

Scope discipline: no product source file was modified by this audit. All live
probes ran against the release binary
`sha256 8b0a6b60a3dcefb8b95103fe999e5416342be075937de0b9814e42fd061e972f`
booted from disposable data directories; raw logs are kept under `artifacts/`.

**Verdict summary: see `00_EXECUTIVE_RELEASE_VERDICT.md` and
`16_RELEASE_DECISION.md`.**

## Index

| File | Content |
|---|---|
| `00_EXECUTIVE_RELEASE_VERDICT.md` | per-mode verdicts, blockers, headline deltas |
| `01_PROVENANCE_AND_DELTA.md` | commit/binary provenance, environment, delta vs baseline |
| `02_FORMER_P0_REVALIDATION.md` | extended-protocol batch truncation (former P0-001), float constant folding (former P0-002) |
| `03_SQL_CORRECTNESS_AND_REGRESSION.md` | SQL subset regression incl. REAUDIT-002/003 re-tests |
| `04_CATALOG_AND_DDL.md` | cross-connection catalog visibility, restart persistence, fenced concurrent-DDL ID race (REAUDIT-001) |
| `05_DURABILITY_BACKUP_RESTORE.md` | cold backup/restore, hot-backup fencing, kill -9 recovery, torn WAL tail |
| `06_VECTOR_LIFECYCLE_AND_QUALITY.md` | DDL envelope live matrix, execution proof, lifecycle bench (cold/warm/rebuild/restart), quality matrix, EXPLAIN-vs-execution discrepancy |
| `07_REAL_WORKLOADS_AND_ANTI_OVERFITTING.md` | OLTP/OLAP/HTAP families, duplicates/NULLs, deterministic oracles |
| `08_PERFORMANCE_AND_SCALE.md` | re-measured throughput/latency, 100K ceiling, 1M attempt, 128K vector scale |
| `09_CONCURRENCY_AND_FAILURE_TESTS.md` | interleaved DDL, writer races, crash/torn-WAL, storage B+Tree split finding (minimal repro) |
| `10_SECURITY_AND_DEPLOYMENT.md` | RBAC fail-closed matrix, DATABASE-DDL authz re-test, TLS modes, non-loopback guard |
| `11_OPERATIONS_AND_SUPPLY_CHAIN.md` | ops safety, backup/restore runbook, supply-chain posture |
| `12_VERIFICATION_AND_PORTABILITY.md` | fmt status, workspace gate attempt (interrupted, scope cut), targeted suites, platform coverage |
| `13_TEST_QUALITY_AND_BENCHMARK_INTEGRITY.md` | overfitting review of the new ANN tests/benchmarks; claims vs evidence |
| `14_PREVIOUS_AUDIT_DELTA.md` | disposition of every 2026-10-15 finding and evidence gap |
| `15_FINDINGS_REGISTER.md` | all findings with severity, confidence, repro, acceptance criteria |
| `16_RELEASE_DECISION.md` | final per-mode release decision with conditions |
| `artifacts/` | raw logs and probe scripts |

## Conventions

- PASS/FAIL/NOTE lines in probe logs are assertions over live behavior; NOTES
  record behavior without a correctness claim (e.g., measured counters,
  documented limits).
- "Measured" values are single-host samples (M2 Pro, macOS); no tail claims
  are made from them.
- Distinctions used throughout: IMPLEMENTED (code exists) / MEASURED (live
  evidence exists) / UNSUPPORTED (explicitly rejected or not provided) /
  UNVERIFIED (no evidence). Production readiness is never inferred from test
  counts or synthetic wins alone.
