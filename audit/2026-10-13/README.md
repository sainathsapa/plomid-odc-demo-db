# PLOMID release-readiness audit — 2026-10-13 (UTC)

Independent whole-database audit of the live PLOMID server at commit
`bcd6bf4` (branch `starting-v0.1.0-beta-2`). Prior consolidated audit:
`audit/PLOMID_FULL_AUDIT_REPORT.md` (commit `0877cb0`, 2026-10-08).

**Verdict summary: NO-GO for every release mode** — two new P0 correctness
findings (silent extended-protocol multi-statement truncation; integral-float
literal folding corrupting division + wire output) block even local use until
fixed. Both have small, well-located fixes. TLS/RBAC/crypto/transaction-staging/
vector-convergence claims from the delta window all **verified live**.

| Release mode | Verdict |
|---|---|
| Local developer use (loopback, single user) | **NO-GO** → GO_WITH_EXPLICIT_LIMITS after P0-001 + P0-002 fixed |
| Single-user local beta | **NO-GO** → same condition |
| Secure single-tenant remote | **NO-GO** → GO_WITH_EXPLICIT_LIMITS after P0s + P1-003 bounded/fixed |
| Multi-user remote | **NO-GO** (P1-003 catalog staleness; needs fix + soak) |
| Cloud / SaaS | **NO-GO** (all above + scale/chaos/backup evidence gaps) |

## Index

- `00_EXECUTIVE_RELEASE_VERDICT.md` — verdicts, grade, blockers, conditions to change
- `01_REPOSITORY_AND_AUDIT_PROVENANCE.md` — commit, env, commands, delta window
- `02_ARCHITECTURE_AND_LIVE_PATHS.md` — system map + path matrix
- `03_SQL_SEMANTICS_AND_CORRECTNESS.md` — claim matrix, results, reproducers
- `04_TRANSACTIONS_CONCURRENCY_AND_MVCC.md` — isolation contract + schedules run
- `05_DURABILITY_RECOVERY_VECTOR_AND_BACKUP.md` — commit contract, crash matrix, vector, backup
- `06_REAL_WORKLOADS_AND_ANTI_OVERFITTING.md` — workload portfolio + held-out notes
- `07_PERFORMANCE_AND_SCALING.md` — measured numbers with provenance
- `08_SECURITY_AND_DEPLOYMENT_READINESS.md` — TLS/auth/RBAC/COPY/limits, live evidence
- `09_OPERATIONS_AND_FAILURE_MODES.md` — pressure/shutdown/maintenance/visibility
- `10_PORTABILITY_AND_RELEASE_SUPPLY_CHAIN.md` — platforms, deps, SBOM, signing
- `11_TEST_QUALITY_AND_COVERAGE_GAPS.md` — inventory + false-confidence risks
- `12_PREVIOUS_AUDIT_DELTA.md` — every material prior finding re-verified
- `13_FINDINGS_REGISTER.md` — deduplicated P0–P3 + evidence gaps
- `14_BETA_RELEASE_GATES.md` — pass/fail/not-run gates with thresholds
- `artifacts/` — raw probe logs (`probe*.log`), server logs, configs/seeds

## How to reproduce

All live probes ran against disposable data dirs (`/tmp/plomid-audit-live/*`)
with the release binary built from this tree (HEAD commit is comment-only vs the
binary build; see `01`). Example:

```bash
./target/release/plomid-server --data /tmp/plomid-audit-live/data \
  --host 127.0.0.1 --port 55433 --username plomid --password 'SECRET'
psql -h 127.0.0.1 -p 55433 -U plomid -d plomid
python3 tests/python/vector_qualify.py  # repo driver (not run here; live SQL used instead)
```

Artifact logs record exact SQL, outputs, and timings. No product source was
modified; `git status` shows only this new `audit/2026-10-13/` directory.
