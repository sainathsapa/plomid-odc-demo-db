# Beta 2 — 22 Security Certification-Ready Design Rules (Phase 22)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.
Rule: every security-relevant feature carries the full evidence chain
below so a future bundle can be assembled. **No certification of any kind
is claimed from source code** (see §Non-claims).

## Control chains

### TLS transport

- Requirement: credentials and rows never cross untrusted networks in clear.
- Threat: passive sniffing / credential theft on non-loopback binds.
- Implementation: rustls TLS 1.2+, `--tls-required` fail-closed plaintext
  refusal (`crates/network/src/tls.rs`, server TLS config).
- Invariant: a non-loopback listener without TLS requires explicit
  `--allow-remote-plaintext` opt-in and logs a warning every start.
- Test: `network/tests/tls.rs` (handshake, expiry, mismatch).
- Negative test: plaintext on TLS-required listener → `FATAL 28000`;
  expired/unparsable PEM fails at startup, not first handshake.
- Evidence: audit `04_TLS.md`. Owner: @plomid. Status: GREEN.

### Authentication (SCRAM + lockout + rate limiting)

- Requirement: only holders of the role secret establish sessions.
- Threat: password guessing, credential replay, user enumeration via
  oracle responses or timing.
- Implementation: SCRAM-SHA-256 against catalog verifiers (first-run
  bootstrap binds server secret once); per-IP growing-delay limiter;
  per-account 5-failure/5-minute lockout; constant-time compares; dummy
  exchange for unknown users (`network/src/scram.rs`, `server/guardrails.rs`).
- Invariant: failures are identical-shaped for known/unknown users; no
  secret material in logs or wire errors.
- Test: `network/tests/rbac.rs` (multi-user SCRAM, rotation immediacy).
- Negative test: wrong-password vs unknown-user fail identically (`28P01`);
  locked account refuses even the right password.
- Evidence: audit `02_SECURITY.md`. Owner: @plomid. Status: GREEN.

### Authorization / RBAC

- Requirement: every statement authorized before execution (least privilege,
  fail closed).
- Threat: privilege escalation by design, cross-tenant reads, file-COPY-as-anyone.
- Implementation: statement gate on all paths incl. txn-group pre-check,
  USE switch, file COPY (`executor/src/authz.rs`, `check_file_copy`,
  `handler.rs` gates); deny `42501` + audit event.
- Invariant: superuser-only for file COPY, promotion, non-table DDL;
  drop-cleans-rights; `has_*_privilege` returns real answers.
- Test: `executor/tests/rbac.rs` (15), `network/tests/rbac.rs` (7).
- Negative test: grantee-with-ALL file COPY → `42501`; non-superuser
  `CREATE ROLE … SUPERUSER` denied; CONNECT withdrawal enforced.
- Evidence: audit `03_AUTHZ_RBAC.md`. Owner: @plomid. Status: GREEN.

### Audited cryptography

- Requirement: no hand-rolled primitives on any security path.
- Threat: broken hash/MAC/KDF, weak salts, MD5 downgrade.
- Implementation: RustCrypto `sha2/hmac/pbkdf2/subtle/base64`; CSPRNG salts
  fail-closed; MD5 legacy-only behind `--allow-legacy-md5` with deprecation
  funnel (`network/src/scram.rs`, session auth).
- Invariant: MD5 can never serve verifier-held roles; every MD5 success is
  counted and loudly logged.
- Test: `network/tests/crypto.rs`. Negative test: verifier role over MD5
  refused; CSPRNG exhaustion closes the connection.
- Evidence: audit `05_CRYPTO.md`. Owner: @plomid. Status: GREEN.

### COPY isolation

- Requirement: server-filesystem COPY cannot escape its jail or leak paths.
- Threat: symlink/parent traversal, TOCTOU plant, FIFO hang, lax file modes.
- Implementation: `O_NOFOLLOW` pinned opens, strict syntax gate, `0600`
  files, superuser+table-priv gate, filename-only errors
  (`network/src/server/handler.rs` COPY jail).
- Invariant: a planted link anywhere in the chain fails closed; errors never
  echo server paths.
- Test: `copy_sandbox.rs` (11) + `pinned_open_tests` (12).
- Negative test: traversal/absolute/symlink/FIFO/directory all → `not
  permitted`; non-superuser → `42501` before any filesystem touch.
- Evidence: Phase 11 report. Owner: @plomid. Status: GREEN.

### Security audit logging

- Requirement: every security decision is reconstructible.
- Threat: repudiation, silent grants, undetected brute force.
- Implementation: durable append-only sink (`security/src/sink.rs`),
  allow+deny events for auth/authz/COPY/admin, tracing + counters.
- Invariant: decisions are rare relative to traffic (sink never on hot path);
  unset path leaves tracing+counters (documented, not silent).
- Test: `security` sink tests (rotation, append-only).
- Negative test: audit-log-open failure fails the server closed at startup.
- Evidence: audit `02_SECURITY.md`. Owner: @plomid. Status: GREEN.

### Secret handling and input/resource caps

- Requirement: secrets never persist in logs; clients cannot exhaust the server.
- Threat: secret-in-log, lock-file DoS, unbounded COPY/prepared/portal state.
- Implementation: `Debug` redaction, 64 MiB COPY/file caps, 1024
  prepared/portal caps, 10K staged statements, connection semaphore fail-fast.
- Invariant: caps fail closed with a clear error, never silent truncation.
- Test: config redaction test, COPY-overflow tests, cap tests.
- Negative test: oversized COPY input/file → refusal, not OOM.
- Evidence: `10_OBSERVABILITY.md` (pending) + config tests. Owner: @plomid.
  Status: GREEN.

### Secure defaults (server + Docker)

- Requirement: default bind is loopback-only; remote is explicit and loud.
- Threat: insecure-remote bake-in with public default credentials.
- Implementation: `--host 127.0.0.1` default; non-loopback without TLS fails
  closed unless opted in; `USER plomid`, `0700` dirs, `0600` files.
- Invariant: `auth=none` + non-loopback never starts; default `plomid/plomid`
  + non-loopback fails closed without a unique password.
- Test: `server` config tests (fail-closed remote, redaction).
- Negative test: `--tls-required` without `--tls` refuses to start.
- Evidence: audit `00_BASELINE.md` §§8–9. Owner: @plomid. Status: GREEN.

## Non-claims (binding)

From source code alone, PLOMID does **not** claim: ISO certification, SOC 2
attestation, Common Criteria certification, FIPS validation, DO-178C
qualification, HIPAA compliance, or PCI DSS compliance. Where any of these
is targeted, the evidence bundle is this file + `docs/compliance/` and the
program must engage the appropriate auditor/evaluator/lab — tracked in
`TRACEABILITY.md` as UNKNOWN, never upgraded by a code change.
