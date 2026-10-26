# 15 — Fix-verification addendum (2026-10-15)

Point-in-time record `00`–`14` stands. After the audit, three items were
fixed and live-verified (new code, new tests, new live probes):

- **P0-003 DATABASE DDL authz bypass → FIXED.** Wire intercept now gates via
  `Executor::check_database_ddl` (superuser or `CREATEDB`, same rule as the
  executor's `C::Database` arm); denials are 42501 with audit events and leave
  no side effects. Regression: `network/tests/rbac.rs::
  database_ddl_requires_createdb_and_reports_sqlstates` (denials verified
  against `SHOW DATABASES`, grant path, conditional forms, SQLSTATEs) +
  `executor/tests/rbac.rs::database_ddl_gate_follows_createdb_across_reopen`.
- **Latent refresh-poisoning bug found through the fix and fixed.**
  `CREATE/DROP DATABASE` synced registry names into session images without
  persisting, marking every subsequently-connected session dirty and silently
  disabling live-catalog refresh (the P1-003 mechanism) for it. The intercept
  now persists the name sync (`Executor::persist_catalog`), which also bumps
  the durable generation so grants/revokes take effect on live sessions.
- **P1 DROP/SQLSTATE → FIXED.** `IF EXISTS` / `IF NOT EXISTS` supported;
  exists → `42P04`, missing → `3D000`, default-db drop → `42501`; identical
  behavior on simple and extended protocols (the audit's "protocol
  inconsistency" was statement-shape-dependent, corrected here).
- **P1 ILIKE → IMPLEMENTED** (not scoped out). `Expression::Like` carries
  `case_insensitive`; both matchers fold with Unicode lowercase; both live
  evaluators (row + join), txn SELECT rewrites, and EXPLAIN rendering
  propagate the flag. Regressions: parser flag test, `sql_compat` ILIKE
  matrix, `unit_scalar` fold matrix. Live: ILIKE/LIKE/`%`/`_`/`NOT`/unicode
  all correct; `ESCAPE` works (`E'\\'` spelling; plain `'\'` follows the
  pre-existing deliberate `standard_conforming_strings`-style lexing).
- Pre-existing, untouched, noted: row-path `LIKE…ESCAPE` is ignored (join
  path honors it); backslash-literal lexing differs from PG (deliberate,
  lexer-commented); `CREATEDB` holders can drop databases they didn't create
  (no ownership recorded — documented residual).

Verification: all new/affected suites green (network rbac 9/9, executor rbac
16/16 incl. keeper, sql_compat 53/53, unit_scalar 4/4, parser 37/37),
`fmt` clean, `clippy` (CI lint set) clean on executor/network/sql, live
ALL PASS above on debug server rebuilt from HEAD (release binary also rebuilt
OK in the prior session).
