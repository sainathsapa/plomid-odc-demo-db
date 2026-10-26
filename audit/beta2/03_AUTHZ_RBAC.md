# Beta 2 — 03 AuthZ / RBAC (Phase 2 Evidence)

Plan: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §6 (Authorization)
Status: **GREEN** — real RBAC enforced on every minimum-scope category, all 11 negative-test classes green.

## 1. Requirement → implementation → evidence

| Requirement (§6) | Implementation | Evidence |
|---|---|---|
| users/roles | Catalog roles (`LOGIN/NOLOGIN`, attributes); least-privilege auto-provision | `catalog/mod.rs:RoleDefinition`, `ensure_role` |
| database access | `CONNECT` default-allow + withdrawable; startup + USE gates | `check_database_connect`, `handler.rs` CONNECT gate + `handle_database_switch` |
| schema access | `USAGE`/`CREATE`; `public.USAGE` default-allow | `check_schema_privilege`, schema grant arms |
| table SELECT/INSERT/UPDATE/DELETE | Per-statement checks incl. subqueries/joins/CTE sources, `INSERT…SELECT`, `UPDATE…FROM`, `DELETE…USING` | `authz.rs:require_reads/require_write/statement_tables` |
| DDL privileges | Ownership (recorded at CREATE, cleaned at DROP, carried on RENAME) or role attribute; `CREATE SCHEMA` via DB `CREATE`/CREATEDB | DDL arms + `authorize_create`, `*_auth` catalog helpers |
| COPY privileges | File COPY superuser-only + table priv; stream COPY = table priv | `Executor::check_file_copy`, handler gate, Copy arm |
| administrative privileges | `CREATEROLE` (roles/membership), `CREATEDB` (databases), SUPERUSER (promotion, file COPY, bare VACUUM/REINDEX, non-table DDL) | attribute gates; creation-time anti-escalation (`CREATE ROLE … SUPERUSER` needs superuser) |
| GRANT/REVOKE meaningful | `GRANT/REVOKE privs ON TABLE/SCHEMA/DATABASE`, membership grant/revoke, `ALTER ROLE`; owner-or-admin to grant | parser `grant.rs`/`alter.rs`, DDL arms, persisted meta sections |
| `has_*` removed from decision path | Enforcement reads live catalog only (grep-verifiable: no enforcement call site touches scalar/types funcs); functions now answer from the statement snapshot | `authz.rs:AuthSnapshot`, `scalar.rs` intercept |

## 2. Negative tests (all green; executor `rbac.rs` + network `rbac.rs`)

- SELECT/INSERT/UPDATE/DELETE without grant → 42501; granted → allowed (per-privilege independence verified).
- ALTER/DROP without ownership → 42501; owner proceeds grant-free.
- Inheritance: grant-via-role works; membership revoke and object revoke both remove access.
- Database isolation: per-database engines (structural) + `REVOKE CONNECT` → `USE` 42501.
- File COPY as grantee-with-ALL → 42501; superuser passes the gate.
- Admin: `CREATE ROLE`/`GRANT`/promotion denied below privilege; `CREATEROLE` can create but not promote; creation-time `SUPERUSER` denied to non-superusers.
- Transactions: staged INSERT denied at COMMIT (42501), nothing written, session reusable (executor); wire: COMMIT 42501 → ROLLBACK recovers → row count unchanged.
- Audit: each denial increments `authz_denials` exactly once; `auth_failures`/`account_lockouts` on auth path; all visible via `plomid_stat_security()`.

## 3. Design notes (reviewer-facing)

- Existence before privilege (PG behavior): missing objects report `NotFound`, forbidden ones `42501`.
- Attributes never inherit; only object privileges flow through membership (`INHERIT` honored, `PUBLIC` in every closure).
- Owners hold all rights implicitly (never stored); superusers bypass (single `admin` check first).
- System relations (`pg_catalog`, `information_schema`, `plomid_stat_*`) are metadata-readable by all authenticated roles.
- Group pre-check denies before the transaction begins (atomicity); the snapshot guard spans the group so `has_*` matches the checked view.
- Known limitation: privilege/catalog changes visible on next connection (per-connection images — same as all DDL).

## 4. Finding disposition

- **F-04** (P1): decision-path half **CLOSED** — enforcement real, empty crate staffed (`security` now depended on by three crates). Residual: none in §6 scope; crypto-adjacent facets live in F-05/Phase 3.
- Remote/multi-user as a whole stays **NOT READY** (F-03 txn semantics, F-05 crypto internals open).

## 5. Live-binary smoke (2026-10-08, debug `plomid-server`, Python SCRAM client)

- First `plomid` login binds the server secret + promotes (subsequent `SELECT has_table_privilege` true).
- `CREATE ROLE app` → app SCRAM login OK → pre-grant `SELECT` denied → `GRANT` → reconnect → `SELECT` allowed.
- `ALTER ROLE app PASSWORD`: old stops working (`28P01`), new works immediately.
- 5 wrong passwords → 6th (correct) refused: `28P01 account "app" is temporarily locked after repeated authentication failures`.

## 6. Pre-existing failure (not a Phase 2 regression)

`plomid-executor --test sql_compat postgresql_compatibility_regressions` (DBeaver `search_path_probe` emptiness) fails identically on the clean tree (verified via `git stash` + rerun). Unrelated to authorization (superuser bypass active); tracked separately.
