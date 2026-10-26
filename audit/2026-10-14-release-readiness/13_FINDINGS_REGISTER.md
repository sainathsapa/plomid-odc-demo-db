# 13 — Findings register (new IDs `PLOMID-AUDIT-101+`)

## P0

### PLOMID-AUDIT-101 — missing authorization on CREATE/DROP DATABASE
Any authenticated role creates/drops any database. Repro:
`CREATE ROLE lowpriv WITH LOGIN PASSWORD 'x'` (as superuser) →
`CREATE DATABASE rbac_evil2` / `DROP DATABASE rbac_victim` as lowpriv both
return OK with matching `SHOW DATABASES` side effects
(`artifacts/rbac_check.py`, `probe_current.log` 13/15).
Source: `network/src/server/handler.rs:1763-1768` intercept →
`handle_database_ddl :2277-2401` (zero authz calls); executor `CREATEDB` arm
`executor/src/authz.rs:772-778` unreachable on wire path. Impact: total data
destruction by any authenticated user in every shared mode. Fix: authorize
(superuser/CREATEDB; owner-or-superuser DROP) + side-effect regression test.

## P1

None open (prior P1s fixed or re-scoped).

## P2

- **102** — `DROP DATABASE` works over extended protocol, `unsupported DROP
  object` over simple protocol. Unify or reject both.
- **103** — `ILIKE` case-sensitive (silently PG-incompatible). Implement or
  document as hard limit.
- **104** — staged-transaction caps (`MAX_STAGED_STATEMENTS/BYTES`,
  `core/src/constants.rs:213-216`) defined but unenforced (unbounded
  multi-statement staging; DoS-adjacent).
- **105** — no slow-consumer write timeout; stalled client holds task +
  semaphore permit up to `max_connections`.
- **106** — `start.sh` defaults (`0.0.0.0`/`secret` + auto plaintext opt-in)
  drift from server defaults; operator footgun.
- **107** — non-ARM/x86 runtime is compile-evidence only here; 1M+ scale,
  sustained HTAP p99, chaos-beyond-SIGKILL, disk-full/OOM live behavior all
  NOT RUN (each an EVIDENCE GAP blocking the corresponding claim).

## P3

- **108** — wrong SQLSTATEs (`SerializationFailure` for DB exists/not-exists).
- **109** — `SHOW ALL` one-empty-row stub; `FLOAT` name, FK, CTAS,
  `CHECKPOINT` stmt, `DROP ROLE`, `CREATE ROLE IF NOT EXISTS`, SQL `PREPARE`
  unsupported (all fail-closed; document the subset).
- **110** — optimizer crate influence on hot row path untraced
  (PARTIALLY_WIRED); `SERIALIZABLE`/abort-on-error stricter-than-PG semantics
  need user docs; barrier-failure UNKNOWN-outcome runbook unwritten;
  `warnings[duplicate]`/`license-not-encountered` deny warnings (policy-accepted).
