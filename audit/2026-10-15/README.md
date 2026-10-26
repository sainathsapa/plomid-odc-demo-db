# PLOMID post-fix whole-database independent release re-audit — 2026-10-15 (UTC)

Independent re-audit of the live PLOMID server at commit `f969bc6`
(branch `starting-v0.1.0-beta-2`) plus pre-existing uncommitted working-tree
fixes (17 files, P0-003 DATABASE-DDL authz + ILIKE, preserved untouched).
Baseline: `audit/2026-10-13/` (verdict NO-GO everywhere at `bcd6bf4`, two P0s
+ P1 catalog staleness + unwired backup). Interim: `audit/2026-10-14-release-readiness/`
(at `6a935b4`, P0-001/P0-002 re-verified fixed, new P0-003 DATABASE DDL authz
bypass found; addendum `15_FIX_VERIFICATION_ADDENDUM.md` claims P0-003 + ILIKE fixed).

**Verdict summary: former P0-001 and P0-002 are FIXED_AND_REVALIDATED live;
P0-003 (DATABASE DDL authz) is FIXED_AND_REVALIDATED live on this tree;
P1 catalog visibility and backup/restore are resolved end-to-end.**

| Release mode | Verdict |
|---|---|
| 1. developer-only local use | **GO_WITH_EXPLICIT_LIMITS** |
| 2. single-user local Beta | **GO_WITH_EXPLICIT_LIMITS** |
| 3. secure single-tenant remote | **GO_WITH_EXPLICIT_LIMITS** |
| 4. multi-user remote | **NO-GO** (1M+/chaos/multi-arch gaps + true-concurrent-DDL ID race) |
| 5. cloud/SaaS or managed service | **NO-GO** (all of mode 4 + hot backup/PITR absent by design, no SLO proof) |

## Index

- `00_EXECUTIVE_RELEASE_VERDICT.md` — per-mode verdicts, blockers, limits
- `01_PROVENANCE_AND_DELTA.md` — commit, env, delta, binary, command inventory
- `02_FORMER_P0_REVALIDATION.md` — P0-001 + P0-002 live evidence
- `03_SQL_CORRECTNESS_AND_REGRESSION.md` — SQL subset regression matrix
- `04_CATALOG_AND_DDL.md` — two-connection DDL visibility + restart
- `05_DURABILITY_BACKUP_RESTORE.md` — backup lifecycle, crash, WAL/checkpoint
- `06_VECTOR_REVALIDATION.md` — vector data path vs brute-force oracle
- `07_REAL_WORKLOADS_AND_ANTI_OVERFITTING.md` — OLTP/OLAP/HTAP families
- `08_PERFORMANCE_AND_SCALE.md` — re-measured release-binary numbers
- `09_CONCURRENCY_AND_FAILURE_TESTS.md` — races, drops, malformed input
- `10_SECURITY_AND_DEPLOYMENT.md` — TLS/auth/RBAC/live enforcement
- `11_OPERATIONS_AND_SUPPLY_CHAIN.md` — ops safety, deps, SBOM/signing
- `12_VERIFICATION_AND_PORTABILITY.md` — fmt/check/clippy/deny/tests, platforms
- `13_TEST_QUALITY_AND_BENCHMARK_INTEGRITY.md` — overfitting review
- `14_PREVIOUS_AUDIT_DELTA.md` — disposition of every 2026-10-13 finding
- `15_FINDINGS_REGISTER.md` — current findings with acceptance criteria
- `16_RELEASE_DECISION.md` — final per-mode decision matrix
- `artifacts/` — raw logs: `p0-001/`, `p0-002/`, `probes/` (server logs,
  live probes, backup/restore, crash, scale)

## How to reproduce (all against disposable dirs, release binary `6c861f51`)

```bash
./target/release/plomid-server --data /tmp/plomid-audit15/data \
  --host 127.0.0.1 --port 55461 --username plomid --password 'SECRET'
python3 /tmp/plomid-audit15/p0_probe.py      # P0 matrix (see artifacts/p0-*/)
./target/release/plomid-server backup --data <DIR> --to <BK>
./target/release/plomid-server restore --from <BK> --to <CLEAN>
```

No product source, test, benchmark, dependency, config, or existing report
was changed by this audit. `git status` shows only the pre-existing dirty
fix files (preserved, listed in `01`) plus this new bundle
(`audit/2026-10-15/`). The pre-existing `audit/2026-10-14-release-readiness/`
directory was not touched. Full workspace suite was NOT RUN per explicit
operator instruction after targeted suites passed (recorded in `12`).
