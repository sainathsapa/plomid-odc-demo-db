# 16 — Release decision

Per-mode verdicts (confidence, supported/explicitly-unsupported claims,
conditions to change):

1. **Developer-only local use: GO_WITH_EXPLICIT_LIMITS** (high). Supported:
   corrected extended/simple multi-statement semantics, float division + int
   contract, txn commit/rollback, catalog visibility, cold backup roundtrip,
   crash recovery, TLS-capable loopback. Limits: §15 REAUDIT-002/003 notes
   ship in release notes; no >100K support claim; cold-backup-only runbook.
2. **Single-user local Beta: GO_WITH_EXPLICIT_LIMITS** (high). Same + restart
   consistency and single-user backup drill. Condition to hold: operators
   stop the server for backup (enforced exit 2 otherwise).
3. **Secure single-tenant remote: GO_WITH_EXPLICIT_LIMITS** (medium-high).
   Supported: SCRAM, TLS-required, DATABASE-DDL authz, table RBAC
   fail-closed, non-loopback guard. Limits: single tenant only; documented
   param-cast + AVG-type + DROP ROLE notes; no hot-backup/PITR.
4. **Multi-user remote: NO-GO** (medium). Blocked by REAUDIT-001 + G-01/G-02/
   G-03/G-04/G-05. Flips when: DDL lane + soak, full suite green, 1M scale on
   release binary, chaos matrix, Linux runtime proof.
5. **Cloud/SaaS: NO-GO** (high). Blocked by all mode-4 items + G-07/G-08 and
   SLO/pressure evidence. Flips when mode-4 conditions plus published
   backup/PITR scope, multi-arch + SLSA, measured SLOs.

No GO was issued on former-P0 absence alone: the whole database (SQL, txn,
catalog, durability, vectors, concurrency, security, ops, supply chain) was
reassessed above. Missing evidence is listed as NOT RUN, never as a
restriction disguising unknown risk. STOP — no implementation follows.
