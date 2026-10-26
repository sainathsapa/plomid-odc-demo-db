# Beta 2 — 05 Crypto (Phase 3 Evidence)

Plan: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §7
Baseline: F-05 OPEN (hand-rolled SHA-256/HMAC/PBKDF2/MD5/Base64, non-CT compares, weak fallback salt, MD5 offered) · Prior: `audit/beta2/03_AUTHZ_RBAC.md`
Status: **GREEN** — all §7 requirements implemented, tested, documented.

## 1. Requirement → implementation → evidence

| Requirement (§7) | Implementation | Evidence |
|---|---|---|
| Audited hash | `sha2::Sha256` | `security/Cargo.toml`, `credential.rs:sha256` |
| Audited HMAC | `hmac::Hmac<Sha256>` | `credential.rs:hmac_sha256` |
| Audited PBKDF2 | `pbkdf2::pbkdf2_hmac<Sha256>` | `credential.rs:pbkdf2_sha256` |
| Audited constant-time compare | `subtle::ConstantTimeEq` | `credential.rs:constant_time_eq` (all secret compares route here) |
| CSPRNG-only salts/nonces | `rand_core::OsRng`; `random_salt()` returns `Result` (fail closed) | `credential.rs:random_salt`, `SaltError`; old time/pid/counter fallback deleted |
| Audited Base64 | `base64` crate (STANDARD engine) | `credential.rs:base64_*` |
| MD5 as downgrade path | `md-5` crate (response wrapper only); `--auth md5` needs `--allow-legacy-md5`, else startup error | `config.rs:validate_security`, `main.rs` legacy warning |
| Secure-first | SCRAM default unchanged; MD5 never offered unless configured | config default `scram`; downgrade-refusal wire test |
| Legacy-use warning/observability | Startup warn + per-login warn + `legacy_auth_uses` counter + `plomid_stat_security` column | `main.rs`, `handler.rs:authenticate_md5`, `observability.rs` |
| Removal plan | Documented below + in runbooks | §4 |
| No silent weak fallback | No fallback code exists (deleted); every salt/nonce site propagates `SaltError` (`?`) | grep: `fallback_salt` absent; ct call-site review §3 |

Signatures unchanged across the swap: no caller modified for the primitive change itself (only salt fallibility + MD5 gating touched call sites).

## 2. Notable verification outcome

The new official-vector tests caught a latent bug the hand-rolled code had carried undetected: the old HMAC disagreed with RFC 4231 case 2 (self-consistent client+server masked it; no external vector ever ran). The audited crates agree with Python `hashlib` AND OpenSSL CLI on every vector (SHA-256 FIPS 180-4, HMAC RFC 4231 ×3 + long-key, PBKDF2 RFC 7914 ×4 dkLen-exact, Base64 RFC 4648, MD5 RFC 1321). Transcribed-from-memory vectors are distrusted by policy — see test comments.

## 3. Constant-time path review (all secret comparisons)

- SCRAM proof: `ScramSession::handle_final` → `constant_time_eq` (subtle).
- Verifier check: `RoleCredential::verify` → derived-key `constant_time_eq`.
- Cleartext/legacy/bootstrap: `constant_time_eq` on secrets.
- MD5 response: `constant_time_eq` at call site.
- No `==` on secret material remains in auth paths (reviewed 2026-10-08; `authenticate()`/`authenticate_md5()` single-pair helpers are test-only shims, never reached by wire code which uses the methods above).

## 4. MD5 deprecation + removal plan

- Today: `--auth md5` requires `--allow-legacy-md5` (`PLOMID_ALLOW_LEGACY_MD5`), else startup fails with a redirect to SCRAM. With the flag: startup warns every time; each MD5 login warns + increments `legacy_auth_uses`.
- Funnel: MD5 serves bootstrap + not-yet-upgraded legacy roles; first success stores a verifier, after which the role is SCRAM-only (fleet migration is one login per role).
- Removal target (Beta 3): delete `--auth md5`, `--allow-legacy-md5`, `ServerAuthConfig::md5`, the MD5 wire exchange, `md5_password_response`, and the `md-5` dependency; at that point `legacy_role_password` rows become inert and are dropped by the next catalog rewrite. Condition: zero `legacy_auth_uses` in operator telemetry for one release cycle.

## 5. Tests (all green 2026-10-08)

- `plomid-security` unit (13): FIPS/RFC vectors per primitive, credential round-trip, ct basics, CSPRNG uniqueness, `SaltError` explicitness.
- `network/crypto.rs` wire (2): MD5 bootstrap→SCRAM-only funnel + counter; unsolicited-MD5 downgrade refusal on SCRAM listeners.
- `server` config (13): new MD5-disabled-by-default + legacy-switch tests.
- Regression: full network (124), server (34), executor lib + rbac + observability, sql, core suites green; `fmt`/workspace-`clippy`/`deny` clean.
- Live binary: `--auth md5` w/o flag → startup error; with flag → starts + `legacy_md5_enabled` warning; MD5 bootstrap OK then second MD5 refused (upgraded), SCRAM unaffected.
- Harness fix: `PlomidServer::bind_ephemeral` replaces the racy discover-drop-rebind pattern in all network test helpers (pre-existing ~1/10 `AddrInUse` flake, eliminated; 8/8 clean runs).

## 6. Compliance note (per plan)

Audited primitives are necessary security evidence but create NO FIPS 140-3 validation, and no such claim is made anywhere in docs, logs, or code. A FIPS claim would require a validated module boundary plus external validation.

## 7. Finding disposition

- **F-05** (P1): **CLOSED** — no hand-rolled crypto remains in the tree (verified: `hand-rolled` now appears only in historical comments marking deletions), MD5 is a gated, counted, planned-for-removal downgrade path, CSPRNG failure is fail-closed by construction.
