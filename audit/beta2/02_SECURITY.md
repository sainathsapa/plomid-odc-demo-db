# Beta 2 — 02 Security posture (Phase 2 update)

Plan: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §§6–7
Baseline: `audit/beta2/00_BASELINE.md` (F-04/F-05 OPEN) · Prior: `audit/beta2/04_TLS.md`
Status: **YELLOW-GREEN** — F-04 transport/decision-path half closed (real RBAC enforced); F-05 internals unchanged (owned by Phase 3).

## 1. What changed in Phase 2

| Area | Before | After | Evidence |
|---|---|---|---|
| Authorization decision | None (`has_* → true`, empty `security` crate) | Statement gate on every path (autocommit + txn-group pre-check + USE switch + file COPY) | `crates/executor/src/authz.rs`, `handler.rs:handle_database_switch`, `finish_copy_file` |
| RBAC vocabulary home | Empty crate (F-D1) | Privilege taxonomy + audit events + credential derivation in `plomid-security` (leaf, depended on by sql/executor/network) | `crates/security/src/{lib,privilege,audit,credential}.rs` |
| Credential storage | Single static pair (server memory) | Per-role verifiers `(salt, iterations, hash)` in catalog meta; plaintext never persists | `catalog.role_credentials`, `RoleCredential::encode` |
| Authentication scope | One pair for every user | SCRAM universal (stored verifiers); cleartext universal; MD5 bootstrap/legacy-only; `none` = trust identity + still authorize | `handler.rs:authenticate_*` |
| First-run setup | Implicit default pair | First login as configured name binds secret + promotes to superuser, exactly once | `bootstrap_superuser`, `ensure_superuser` upgrade |
| Rotation | None | `ALTER ROLE … PASSWORD` (self or admin), immediate effect | `Statement::AlterRole`, DDL arm |
| Brute force | Per-IP delays only | + per-username lockout (5 fails → 5 min, in-memory) | `guardrails.rs:AccountLockout` |
| Secret comparison | `==` / slice-eq | `constant_time_eq` everywhere (password, legacy, SCRAM proof, MD5) | `credential.rs`, `scram.rs:handle_final` |
| Denial signal | None | `42501` (`PL-PERMISSION-DENIED`) + audit event (log + counters) + `plomid_stat_security()` | `core/error.rs`, `observability.rs`, `system_catalog.rs` |
| `has_*_privilege` | Unconditional `true` | Real answers from the statement privilege snapshot; enforcement never consults them | `authz.rs:AuthSnapshot`, `scalar.rs` intercept |
| Default posture | Everyone superuser (`ensure_role`) | Least-privilege placeholders; owner-rights; PUBLIC only on `public.USAGE` | `ensure_role` / `ensure_superuser` |

## 2. Threat model deltas (honest)

Closed: single-credential impersonation of the whole cluster (per-role secrets), privilege-escalation-by-design (enforcement), silent insecure grants (fail-closed defaults + drop-cleans-rights), timing oracles on secret compare, user enumeration via SCRAM shape (dummy exchange), unauthenticated file-COPY-as-anyone (superuser gate + jail preserved).

NOT closed (tracked): hand-rolled KDF/hash internals (F-05 → Phase 3), MD5 availability (legacy-only now, deprecate in Phase 3), durable lockout (by design — no self-lockout), per-connection catalog freshness (privilege changes take effect on reconnect — same rule as all DDL, documented), column/RLS/grant-option/`SET ROLE`/`DROP ROLE` (non-goals), at-rest encryption/audit retention (F-09), any certification (never from code alone).

## 3. Backward compatibility

- Bootstrap superuser flows (all existing tests/clients) behave identically: first login binds, subsequent logins verify.
- Loopback defaults unchanged; remote rules from Phase 1 unchanged and extended (default password still refused).
- `auth=none` (loopback only) now means *trust identity, still authorize* — strictly stronger than before.
- Pre-Phase-2 tables lack recorded owners: superusers unaffected; others need grants (documented upgrade note in the RBAC runbook).

## 4. Tests

Executor `rbac.rs` (15): DML×4, DDL, inheritance, PUBLIC, schema USAGE, TRUNCATE, drop-cleans, admin gating, CONNECT, COPY statement ordering, file gate, txn denial, audit counters, `has_*`, ownership/rotation. Network `rbac.rs` (7): multi-user SCRAM, identical-failure oracle, rotation immediacy, lockout, file-COPY denial, wire txn recovery, isolation + CONNECT withdrawal. All green; prior suites green (network 110+, server 32, sql 127, executor lib 188, security 7+).
