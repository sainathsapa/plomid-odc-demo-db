# PLOMID release-readiness audit — 2026-10-14 (HEAD `6a935b4`)

Independent whole-database audit. Tree untouched except this bundle
(`git status` clean at start; all probes ran against disposable
`/tmp/plomid-audit-20261014/*` data dirs and loopback ports 55432–55435).

| # | Report | Covers |
| - | ------ | ------ |
| 00 | [Executive release verdict](00_EXECUTIVE_RELEASE_VERDICT.md) | GO / NO-GO per mode, grade, blockers, top-10 actions |
| 01 | [Repository & audit provenance](01_REPOSITORY_AND_AUDIT_PROVENANCE.md) | commit, env, commands, delta window vs 2026-10-13 audit |
| 02 | [Architecture & live paths](02_ARCHITECTURE_AND_LIVE_PATHS.md) | protocol→bytes map, path matrix, duplicate ownership |
| 03 | [SQL semantics & correctness](03_SQL_SEMANTICS_AND_CORRECTNESS.md) | claim matrix, live oracles, reproducers, unsupported set |
| 04 | [Transactions, concurrency & MVCC](04_TRANSACTIONS_CONCURRENCY_AND_MVCC.md) | isolation contract, races, live concurrency evidence |
| 05 | [Durability, recovery, vector & backup](05_DURABILITY_RECOVERY_VECTOR_AND_BACKUP.md) | commit contract, crash matrix, vector convergence, backup drill |
| 06 | [Real workloads & anti-overfitting](06_REAL_WORKLOADS_AND_ANTI_OVERFITTING.md) | workload portfolio, oracles, held-out checks, scale limits |
| 07 | [Performance & scaling](07_PERFORMANCE_AND_SCALING.md) | measured numbers with provenance, cliffs, invalid runs |
| 08 | [Security & deployment readiness](08_SECURITY_AND_DEPLOYMENT_READINESS.md) | boundary, auth/TLS/RBAC evidence, container, P0-003 |
| 09 | [Operations & failure modes](09_OPERATIONS_AND_FAILURE_MODES.md) | pressure, shutdown, maintenance, backup/restore drills, gaps |
| 10 | [Portability & release supply chain](10_PORTABILITY_AND_RELEASE_SUPPLY_CHAIN.md) | platforms, deny/SBOM/signing, reproducibility |
| 11 | [Test quality & coverage gaps](11_TEST_QUALITY_AND_COVERAGE_GAPS.md) | inventory, false-confidence risks, NOT RUN commands |
| 12 | [Previous-audit delta](12_PREVIOUS_AUDIT_DELTA.md) | every prior finding: fixed / partial / present / regressed |
| 13 | [Findings register](13_FINDINGS_REGISTER.md) | `PLOMID-AUDIT-101+`, severity, repro, source pointers |
| 14 | [Beta release gates](14_BETA_RELEASE_GATES.md) | pass / fail / not-run with thresholds and proof links |
| — | [artifacts/](artifacts/) | probe scripts, raw logs, drill scripts, env manifest |

Prior bundle (historical only): `audit/2026-10-13/` (verdict NO-GO everywhere,
commit `bcd6bf4`). Delta window: `bcd6bf4..6a935b4` (7 commits).
