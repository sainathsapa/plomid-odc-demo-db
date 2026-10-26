# 08 — Security and deployment readiness

Threat boundary assessed: loopback single-user (safe-by-default) vs any
remote/multi-user use (blocked by P0-003 until fixed).

## Verified enforced (live unless noted)

- Loopback default; non-loopback plaintext refused at startup without TLS or
  explicit opt-in (code + prior live; re-check code-unchanged).
- SCRAM default; wrong password → FATAL (live); table RBAC deny-without-grant
  (42501), allow-after-grant (live); `COPY … FROM` file denied for non-superuser
  path present (code-traced; live file-COPY as non-superuser spot NOT RUN).
- TLS 1.2+ via rustls 0.23.46 (patched floor; `cargo deny` clean): live
  handshake `sslmode=require` OK, plaintext FATAL under `--tls-required`.
- Default credential (`plomid/plomid`) refused on non-loopback (code).
- Per-IP rate limit + per-user lockout (code; restart-clears documented).
- Constant-time compares, CSPRNG salts, no secret leakage in logs/errors
  (code-traced; secret-redaction spot NOT RUN live).
- 64 MiB frontend frame cap, COPY buffer caps, prepared/portal caps,
  `max_connections` semaphore (code; exhaustion-at-100 NOT RUN live).
- Docker: non-root user, 0700 data dir, volume, TCP healthcheck, no insecure
  opt-in baked in (code/Dockerfile-traced; image build+run NOT RUN here).
- `start.sh` drifts from server defaults (`0.0.0.0` + `secret`, auto-adds
  plaintext opt-in) — operator footgun, P2.

## The blocker: P0-003 DATABASE DDL authz bypass (live-verified)

`CREATE DATABASE` / `DROP DATABASE` via the wire intercept run with **zero
privilege check**: role `lowpriv` (no grants) created `rbac_evil2` and dropped
`rbac_victim`, confirmed by `SHOW DATABASES` before/after
(`artifacts/rbac_check.py`, `probe_current.log`). Simple-protocol `DROP`
inconsistently errors while extended succeeds (P2 asymmetry). Remediation:
authorize in `handle_database_ddl` + regression test with side-effect checks.

## Partial / missing

- `SHOW DATABASES` name leak: lowpriv sees only a subset in practice
  (unconnected DBs hidden in this run) — code path unfiltered; treat as P2
  hardening gap pending a negative test.
- Staged-transaction caps defined-but-unenforced; no slow-consumer write
  timeout (permit held); GSSENC/`lock_timeout` accepted-not-enforced
  (documented). P2/P3.
- MD5 legacy path exists behind explicit flag (counted/warned) — acceptable.
