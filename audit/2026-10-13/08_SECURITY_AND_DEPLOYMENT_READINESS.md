# 08 — Security and deployment readiness

Intended Beta 2 boundary (docs + code): loopback single-user by default;
opt-in remote only via TLS or explicit `--allow-remote-plaintext` on trusted
networks; SCRAM auth; RBAC least-privilege. All verified live unless noted.

## Findings (all live-tested 2026-10-13)

| Check | Result | Evidence |
|---|---|---|
| TLS handshake (`sslmode=require`, self-signed) | **PASS** — `SELECT 1` over TLS | `probe14_tls.log` |
| Plaintext refused on `--tls-required` | **PASS** — `FATAL: TLS is required … (SSL off)` | `probe14` |
| Non-loopback bind without TLS refuses at startup | **PASS** — fail-closed config error | `probe14` (exit path) |
| Wrong password | **PASS** — `FATAL: password authentication failed` | `probe8_security.log` |
| RBAC deny without grant (SELECT/INSERT/COPY) | **PASS** — `42501 … lacks SELECT`; COPY denied | `probe8` |
| RBAC allow after GRANT (fresh conn) | **PASS** | `probe8b_rbac2.log` |
| Audited crypto (SHA-256/HMAC/PBKDF2/constant-time/CSPRNG) | **PASS by code + vectors** | `security/src/credential.rs:37-206`, `tests/unit_credential.rs` |
| MD5 behind `--allow-legacy-md5`, logged | PASS by code/flags | `config.rs`, `--help` |
| COPY jail (canonicalize + O_NOFOLLOW + superuser-first) | PASS by code + live denial | `handler.rs:889-936,3715-3971` |
| Connection/prepared/portal caps + timeouts | PASS by code + defaults observed in startup log | `server/mod.rs:95-145`, effective_config dump |
| `PLOMID_PASSWORD` no default; `plomid` refused remote | PASS by code + Makefile guard | `Makefile:36-42,147-150`, `config.rs:877-883` |
| Docker: non-root, read-only, no-new-privs, no insecure env | PASS by file review (image not built here) | `Dockerfile:42-69`, `Makefile:159-160` |

## Residual risks / gaps

- **P1-003 (catalog staleness)** is also a security-operability issue: revoked
  privileges may linger on live sessions until reconnect (GRANT side verified
  latent; REVOKE latency NOT TESTED — assume latent, P1).
- No mTLS, no at-rest encryption, no column/RLS/grant-option/`SET ROLE`
  (all explicitly documented non-goals — acceptable if boundary stays).
- Dependency advisories: `cargo deny` clean today; no vuln-SLA/bot evidence.
- Penetration test, fuzzing of protocol framing, DoS budgets (huge SQL/vectors/
  fan-out): NOT RUN against live listener. `MAX_COPY_*`, statement timeout
  (30 s), idle-in-txn (60 s) defaults exist; adversarial verification missing.
- Secrets handling: password via env/flag (redacted in `effective_summary`);
  no leak observed in logs (error paths use filename-only denials).

**Remote multi-user production is blocked** by P0s + P1-003, not by transport/
authN/authZ, which are the parts that verified strong.
