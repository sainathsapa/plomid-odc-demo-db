# 10 — Security and deployment

Live on release binary. Raw: `rbac_live.log`, `server-tls*.log`.

- Bind exposure: non-loopback without TLS refused at startup (fail-closed);
  loopback default. PASS.
- TLS: `sslmode=require` handshake OK; `--tls-required` refuses plaintext
  (`FATAL: TLS is required…`); wrong password FATAL. Docker/container
  entrypoint not exercised (no image run) — env defaults reviewed via
  `--help` (SCRAM default, MD5 behind `--allow-legacy-md5`, sane
  timeouts/caps). PASS for server-flag surface.
- Auth: SCRAM-SHA-256 live (SASL exchange observed); wrong-password and
  unknown-user behavior covered by in-tree `rbac` 9/9 (identical-failure).
  Credential storage/rotation covered by in-tree tests (not re-run live
  beyond wrong-password FATAL).
- RBAC: table-level deny-without-grant / fail-closed verified live
  (lowpriv `SELECT`/`CREATE TABLE` denied); DATABASE DDL (P0-003 scope)
  denied live for lowpriv CREATE+DROP (42501, `lacks CREATEDB`), superuser
  path unaffected; grant path + SQLSTATEs covered by in-tree 9/9. `DROP ROLE`
  unsupported (fail-closed `unsupported DROP object`). PASS with noted limit.
- Secrets/defaults: no default credentials accepted (custom user/pass
  enforced); error leakage not observed (typed 42501/42P04-style errors, no
  backtraces on wire in probed paths).
- Limits: `--max-connections`, statement/idle-in-txn timeouts, COPY caps,
  prepared-statement/portal caps present as flags; exhaustion behavior
  NOT RUN live.
- Prior "TLS/RBAC/crypto fixed" claims revalidated on this binary — not
  carried by reference.
