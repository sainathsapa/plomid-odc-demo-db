# 00 — Executive release verdict (2026-10-15)

Commit `f969bc6` + dirty P0-003/ILIKE fixes (see `01`). Host macOS 15.4 arm64,
M2 Pro 12-core, 16 GiB, rustc 1.99.0. Release binary rebuilt from this tree
(sha256 `6c861f51…`), all live probes against it on disposable dirs/ports.

## Verdicts

1. **Developer-only local use: GO_WITH_EXPLICIT_LIMITS** (high confidence).
   Both former P0s fixed live, no regressions in covered SQL/txn paths,
   crash keeps committed / drops uncommitted, cold backup roundtrip works.
2. **Single-user local Beta: GO_WITH_EXPLICIT_LIMITS** (high confidence).
   Same + restart consistency, catalog visibility, TLS optional.
3. **Secure single-tenant remote: GO_WITH_EXPLICIT_LIMITS** (medium-high).
   P0-003 DATABASE-DDL authz now denied live (lowpriv CREATE/DROP → 42501,
   `rbac` 9/9), TLS-required refuses plaintext, wrong-password FATAL,
   non-loopback without TLS refused at startup.
4. **Multi-user remote: NO-GO** (medium). Blockers: true-interleaved
   concurrent DDL can mint colliding table IDs (physical dirs are id-keyed;
   residual from remediation report, not retested live — unsafe to run on
   this host); 1M+ rows NOT RUN; disk-full/OOM/kill-point chaos NOT RUN;
   Linux/x86_64 runtime NOT RUN.
5. **Cloud/SaaS: NO-GO** (high). All mode-4 gaps + cold-only backup by
   design (no hot backup/PITR), no multi-arch proof, no SLSA attestation,
   no SLO/pressure evidence.

## Top blockers (in order)

1. PLOMID-REAUDIT-001 (P1): concurrent-DDL ID collision risk under true
   interleaving — needs a DDL serialization lane + stress proof.
2. Evidence gaps (each NOT RUN, commands in `09`/`12`): >100K scale (1M/1M+
   pending), disk-full/WAL-full, OOM, torn-WAL/partial-checkpoint drills,
   x86_64/Linux/ARM64 runtime.
3. PLOMID-REAUDIT-002 (P2): untyped-param division (`SELECT %s/%s` → `'1'`
   TEXT) requires explicit `::float8` casts; document as a hard limit.
4. PLOMID-REAUDIT-003 (P2): `DROP ROLE` unsupported; `AVG` returns
   `Decimal`/Numeric while siblings return float8 (mixed-type aggregate).

## What would change the verdict

- Mode 4 → GO_WITH_EXPLICIT_LIMITS: DDL lane fixed + soak, 1M scale on
  release binary, chaos matrix on disposable dirs, Linux x86_64 runtime proof.
- Mode 5 → GO_WITH_EXPLICIT_LIMITS: above + hot-backup/PITR decision
  published (or explicit out-of-scope), multi-arch + SLSA, SLO evidence.
