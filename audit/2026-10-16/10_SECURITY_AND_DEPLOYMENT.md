# 10 — Security and deployment

Evidence: `artifacts/probes/logs/rbac_probe.log` (**14/14**),
`boot1_incident_excerpt.log`, `audit16-reindex-admin.log`,
`audit16-analyze-wire.log`, `mini_scram.py` (in
`artifacts/probes/scripts/`).

## RBAC / authorization — PASS (fail-closed matrix)

- Granted SELECT works; INSERT/UPDATE/DELETE/OWNERSHIP actions are
  denied with explicit reason strings (`role "auditor" lacks …`).
- REVOKE is enforced on the next connection (no cached grants).
- P0-003 (DATABASE DDL): `CREATE DATABASE` / `DROP DATABASE` denied for
  a non-CREATEDB role — re-tested, PASS.
- `DROP ROLE` guards: bootstrap role refused; role with dependent
  privileges refused (see `03`).

## Transport security — PASS (re-tested this cycle)

- Non-loopback **plaintext is refused at startup**: `non-loopback binds
  require TLS; pass --tls --tls-cert/--tls-key (optionally with
  --tls-required)`.
- Self-signed cert generated; `--tls-required` accepts TLS clients and
  refuses plaintext (`FATAL: TLS is required for this listener (SSL
  off); reconnect with sslmode=require`).
- SCRAM-SHA-256 auth flows verified with the fixed `mini_scram.py`
  (protocol int32 + GS2 header): small catalogs authenticate in ~23 ms.
- F-006 note: on large catalogs every connection (including auth) pays
  ~5 s of catalog load before auth completes (see `04`/`08`); this is a
  DoS-amplification consideration for internet-exposed deployments
  (each connection costs seconds of work before credentials are even
  checked). No bypass observed.

## Lockout / brute-force controls

- 5 failed auths → 300 s in-memory lockout; error:
  `FATAL: account "plomid" is temporarily locked after repeated
  authentication failures`; restart clears state; retry loops re-lock.
- F-007 (boot-time anomaly) lives in this surface: after one crash
  restart, correct credentials were denied as `bad_credentials` and the
  account moved to `lockout`/`locked_out` (observed once, not
  reproduced; details `09`). Operational mitigation: monitor
  `action=lockout` events; treat post-crash lockouts as suspect.

## Administrative statement handling

- `REINDEX`/`ANALYZE` are protocol-level no-ops (F-002/F-003) — note
  for security review: the statements neither execute nor log admin
  actions over the wire; authorization still passes through the admin
  gate (`security_decision … action=admin … decision=allow` observed
  for `REINDEX` in `audit16-reindex-admin.log`), and no privilege
  escalation was found.
- `VACUUM` executes and records an admin action (`plomid_stat_admin` →
  `vacuum`), showing the admin-stat surface is functional.

## Not re-run / gaps

- External TLS clients with public CAs, certificate rotation, mTLS:
  not in scope this cycle (baseline evidence unchanged).
- Network-level DoS/rate limits beyond auth lockout: not measured.
