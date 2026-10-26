# Beta 2 — 04 TLS / Remote Transport Security (Phase 1 Evidence)

Plan: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §5
Baseline: `audit/beta2/00_BASELINE.md` (HEAD `0877cb0`, all F-findings OPEN)
Status: **GREEN** — all §5 requirements implemented, tested, documented.

## 1. Requirement → implementation → evidence

| Requirement (§5) | Implementation | Evidence |
|---|---|---|
| Real TLS boundary, audited stack, no hand-rolled TLS | rustls 0.23 + tokio-rustls 0.26 (`aws_lc_rs` provider); `ring` deliberately avoided (license allow-list); PEM via rustls-pki-types (rustls-pemfile rejected — unmaintained, RUSTSEC-2025-0134, deny-clean) | `crates/network/Cargo.toml:25-33` |
| TLS server handshake | `SSLRequest` → `S` + `TlsAcceptor::accept`; GSSENC always `N` | `crates/network/src/tls.rs:negotiate`, `crates/network/src/server/mod.rs` (task negotiation) |
| Configured certificate/key | Reuses existing `TlsConfig{enabled,cert,key,required}` (`--tls --tls-cert/--tls-key`, `PLOMID_TLS_CERT/KEY`) | `crates/server/src/config.rs:30-51` (kept, docs refreshed) |
| Required vs optional mode | `--tls-required` → plaintext refused FATAL 28000 pre-auth | `crates/network/src/server/handler.rs:run`, `tls.rs:TLS_REQUIRED_*` |
| Plaintext refusal when required | FATAL 28000 (mirrors PG `hostssl` refusal) + close, counted | `tls.rs`, `record_tls_plaintext_refusal` |
| Cert/key load validation | Existence, PEM parse, non-empty chain, key present, leaf expiry, key↔chain match — all at startup, fail closed | `tls.rs:load_server_config`, `check_leaf_validity` |
| Secure defaults | Loopback default; remote needs TLS or explicit opt-in; built-in password refused on non-loopback | `config.rs:validate_security`, `DEFAULT_PASSWORD` |
| Loopback dev stays easy | Defaults unchanged on `127.0.0.1` (pair `plomid/plomid` accepted) | `config.rs` tests |
| No insecure env in production image | `PLOMID_ALLOW_INSECURE_REMOTE=true` removed from `Dockerfile` ENV | `Dockerfile`, `tls.rs:docker_image_has_no_insecure_remote_default` |

## 2. Architecture notes

- Negotiation runs **inside the per-connection task** (never the accept loop): a stalled client cannot block other accepts.
- Negotiation reads are exact-sized and **unbuffered** (no `BufReader` readahead), so no handshake byte strands during the plaintext→TLS upgrade.
- Post-negotiation I/O is transport-agnostic via `Transport::{Plain,Tls}` (`AsyncRead`/`AsyncWrite` delegation); the PGWire handler is unchanged past the startup payload.
- `CancelRequest` handling moved with negotiation into the server task (same fail-closed semantics); `ConnectionHandler` dropped its `cancel_registry` field.
- Observability: `tls_connections`, `tls_handshake_failures`, `tls_plaintext_refusals` in `plomid-core` counters + `plomid_stat_counters()` columns (CLI parses by name — additive-safe).
- No certification is claimed: TLS is necessary transport evidence, never sufficient for any framework (§5, runbook scope-limits section).

## 3. Tests (all green 2026-10-08)

`crates/network/tests/tls.rs` — 10 passed:

- `tls_handshake_success_and_query_over_encrypted_channel` (S → rustls handshake → SCRAM-independent cleartext auth → `SELECT 1` = T/D/C/Z; `tls_connections` +1)
- `tls_handshake_fails_for_untrusted_certificate` (empty root store → handshake Err)
- `tls_required_mode_refuses_plaintext_startup` (E/28000/TLS-required + EOF, `tls_plaintext_refusals` +1)
- `tls_required_mode_still_serves_encrypted_clients`
- `tls_optional_mode_still_accepts_plaintext`
- `server_without_tls_answers_n_to_ssl_request`
- `tls_load_accepts_valid_material` / `tls_load_rejects_expired_certificate` / `tls_load_rejects_mismatched_key`
- `docker_image_has_no_insecure_remote_default` (scans workspace `Dockerfile`)

Unit: `tls.rs` (missing/empty/garbage/keyless material fail closed); `server/config.rs` +11 tests incl. new `remote_bind_refuses_the_built_in_default_password`.

Regression: `plomid-network` (58+5+37+10+5 unit/integration incl. cancel-abort), `plomid-server` (11+21), `plomid-core` (18), `plomid-executor observability` (updated column list) — all green. `cargo deny check` green (exit 0; warn-only: duplicate `syn` 2/3 via x509-parser chain, one license-not-encountered — same warning class as baseline). `cargo clippy --workspace --all-targets --locked` clean. `cargo fmt --check` clean.

Real-binary smoke (debug `plomid-server` + openssl cert, 2026-10-08):

- `--tls --tls-cert/--tls-key --tls-required` → `tls_ready required=true` + `server_ready`; plaintext startup → `E`/`28000`/`TLS is required` + EOF.
- `SSLRequest` → `S` → TLSv1.3 handshake (openssl/Python client) → cleartext auth → `SELECT 1` = T/D/C/Z.
- Remote `0.0.0.0` + built-in password → `configuration error: refusing the built-in default password…`; with unique password + opt-in → `server_ready`.

## 4. Docs

- New runbook: `docs/v0.1.0/operations/tls.mdx` (modes, openssl mint, TLS/Docker run, password rule, verification, scope limits) + nav entry in `docs/docs.json`.
- Updated: `operations/run-server.mdx` (Docker env, auth/TLS line), `operations/configuration.mdx` (TLS rows, `validate_security`), `reference/config-reference.mdx` (TLS + password rows), `start.sh` (stale "until TLS lands" comments), `Makefile docker-run` (refuses empty/`plomid` password, passes opt-in explicitly, warns).

## 5. Finding disposition

- **F-02** (P0 remote): transport half **CLOSED** — handshake implemented, insecure image default removed, default-password refused remotely. Remaining F-02-adjacent work moves to later phases, not this finding: password rotation/first-boot (Phase 2 auth), audited-crypto swap (Phase 3), mTLS (not planned), hot reload (not planned).
- Remote/multi-user/cloud as a whole stays **NOT READY** (F-03/F-04/F-05 still open) — unchanged from baseline §13.

## 6. Residual risks / non-goals

- Self-signed CA UX is operator-owned (docs give openssl recipe; no bundled CA tooling).
- No TLS session-ticket rotation tuning; rustls defaults apply.
- `start.sh` remains the trusted-network dev path (explicit opt-in + warning, unchanged semantics).
