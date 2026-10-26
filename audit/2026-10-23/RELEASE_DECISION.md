# Release decision refresh — 2026-10-23

**Status:** post-remediation refresh of the 2026-10-16 release decision.
This is **not** an independent audit and does not replace the evidence
bundle in `audit/2026-10-16/`. It re-evaluates the current source tree after
the finding fixes and reconciles the database-DDL authorization claim in the
live docs.

## Artifact and scope

- Branch: `starting-v0.1.0-beta-2`
- Source base: `cd9271d` (database-DDL docs reconciliation), atop
  `a605cec` (F-009/F-010/F-012/F-013), `472390e` (F-001/F-004 register),
  and the earlier finding fixes through `e128a1d`.
- No product source was changed by this refresh; only docs and this
  decision file.
- **No release binary was rebuilt or hashed.** All tests below ran against
  debug test binaries on macOS 15.4 arm64. The previous decision's
  binary-level evidence remains the authoritative artifact for `6890955`.
- Not run by this refresh: full workspace suite, docs-site build, release
  binary smoke test, multi-user soak/chaos, 1M+ scale, disk-full/OOM,
  Linux/x86_64, or a fresh performance measurement.

## Database-DDL authorization reconciliation

The live docs claimed: “Any authenticated user can `CREATE`/`DROP
DATABASE` without the required privilege check.” That claim is stale.
Database DDL is intercepted before executor dispatch, and
`Executor::check_database_ddl` now requires superuser or the `CREATEDB`
role attribute before any mutation. Denials return `42501`, are audited,
and leave no side effects. The gate was fixed in `5d05899` and revalidated
on 2026-10-15; the docs simply had not been updated.

Re-verified on 2026-10-23:

- `crates/network/tests/rbac.rs::database_ddl_requires_createdb_and_reports_sqlstates`
  — pass: low-privilege `CREATE`/`DROP` denied with `42501` and no state
  change; conditional forms denied; `ALTER ROLE … CREATEDB` confers the
  capability; superuser path and SQLSTATEs verified.
- `crates/executor/tests/rbac.rs::database_ddl_gate_follows_createdb_across_reopen`
  — pass: denial without `CREATEDB`, allow after grant, persistence across
  executor reopen, superuser allow.

The accurate residual limitation is **not** an open bypass:

- `DROP DATABASE` is not owner-scoped. Per-database ownership is not
  tracked, so any `CREATEDB` role can drop any database, including one it
  did not create. `CREATEDB` must be treated as a destructive capability.
- Column/row-level controls and grant delegation are still absent.
- Shared multi-user deployment therefore remains unsupported, while
  single-operator remote remains the supported beta profile.

Docs updated in `cd9271d`: `index.mdx`, `getting-started/beta-status.mdx`,
`reference/support-matrix.mdx`, `operations/rbac.mdx`,
`operations/profile-secure-remote.mdx`,
`operations/profile-production.mdx`, and `sql/roles-permissions.mdx`.

## Finding disposition at this source base

The authoritative detail is in
`audit/2026-10-16/15_FINDINGS_REGISTER.md`; all entries now have a status.

| Finding | Status | Release relevance |
| --- | --- | --- |
| F-001 — B+Tree split breaks bulk INSERT | **Fixed** (`9e35ced`) | M2 insert path unblocked |
| F-002 — `REINDEX` silent wire no-op | **Fixed** | Admin path works |
| F-003 — `ANALYZE` silent wire no-op | **Fixed** | Statistics path works |
| F-004 — Concurrent same-name `CREATE TABLE` | **Fixed** (`9e35ced`) | M4-specific blocker cleared |
| F-005 — `LIKE/ILIKE … ESCAPE` ignored | **Fixed** | SQL correctness |
| F-006 — ~5 s catalog load per connection | **Resolved** | Session-open target met |
| F-007 — Post-crash auth anomaly (observed once) | **Mitigated** | Instrumentation + monitoring; original event unreproduced |
| F-008 — Dim-64 ANN recall cliff | **Fixed** | Quality floors met |
| F-009 — Duplicate ANN pass | **Fixed** | One ANN invocation per Execute after the statement Describe |
| F-010 — `EXPLAIN` vs execution strategy | **Fixed** | Live gates aligned |
| F-011 — Documentation staleness | **Complete** | ILIKE/docs pass; this refresh added DB-DDL reconciliation |
| F-012 — `GRANT … ON ALL TABLES` | **Implemented** | Schema-wide grants work |
| F-013 — ANN counter semantics | **Documented** | Invocation counters, not statement counters |
| F-014 — Concurrent first-session open | **Fixed** (`63dee6a`) | Multi-session startup unblocked |

## Verification performed (2026-10-23)

All results below are passes unless marked otherwise. These are targeted
regressions, not a full suite.

| Area | Test target | Result |
| --- | --- | --- |
| F-001 | `plomid-storage --test unit_btree` | 11 passed |
| F-001 | `plomid-executor --test vector_auto_index_bulk_load` | 10,000 dim-64 rows, auto index, count/point-read/READY — passed |
| F-002/F-003 | `plomid-network --test maintenance_wire` | `reindex_and_analyze_execute_over_the_wire` — passed |
| F-004/F-014 | `plomid-executor --test create_table_race` | 6 passed (autocommit, transaction, group-batch, stale-session, DROP+CREATE, fresh-root) |
| F-004 | `plomid-sql --test concurrent_ddl_ids` | 9 passed |
| F-005 | `plomid-executor --test sql_compat` | 54 passed, including `where_like_and_ilike_honor_escape` |
| F-006 | `plomid-sql --test unit_catalog` | 15 passed, including sharded round-trip |
| F-006 | `catalog_load_scale` scale tier (`--include-ignored`) | 60k-table catalog: `Executor::new` 216–241 ms debug (ceiling 2 s) — passed |
| F-008 | `plomid-vector --test ann_dim64_clustered` | 2k recall 1.00/1.00; 8k recall 0.90/0.90 (DiskANN/HNSW) — passed |
| F-009 | `plomid-network --test ann_describe` + Describe integration tests | 1 + 3 passed |
| F-010 | `plomid-executor --test vector_explain_gate` | passed |
| F-012 | `plomid-executor --test rbac all_tables_in_schema…` | passed |
| DB-DDL gate | `plomid-network --test rbac database_ddl…` | passed |
| DB-DDL gate | `plomid-executor --test rbac database_ddl…` | passed |

F-007 was **not** re-run: the original crash-restart anomaly remains
unreproduced. Its instrumentation, dirty-restart regression, and lockout
monitoring are in the tree, and a recurrence is now self-diagnosing.

## Decisions

Global release-hygiene gate applies to every GO below: rebuild the release
binary from this source tree, hash it, run the full workspace suite and
docs build, and smoke-test the binary. No binary-level claim is made by
this refresh.

| Mode | Decision | Conditions / blockers |
| --- | --- | --- |
| **M1 — Local use** (loopback, single user, disposable data) | **GO** | No functional blocker in this refresh. Satisfy the release-hygiene gate. |
| **M2 — Local Beta** (loopback, real data, operator present) | **GO WITH CONDITIONS** | The old F-001 workaround and F-002/F-003/F-008 cautions are no longer needed. Conditions: release-hygiene gate; keep F-007 lockout monitoring; treat `CREATEDB` as destructive. Documented limits remain (`DROP ROLE`, cold-only backup, sparse/multivector, CPU-only GPU). |
| **M3 — Secure remote single-tenant** | **GO WITH CONDITIONS** | All M2 conditions. Keep TLS-required mode and a unique secret (verified in the prior audit, not re-run here). Pool or recycle short-lived connections; F-006 is fixed but pooling remains the documented recommendation. Treat the host as single-operator: `DROP DATABASE` is not owner-scoped. |
| **M4 — Multi-user remote** | **NO-GO** | F-004 and F-014 are fixed and the targeted races pass, but the required independent multi-user re-audit is still missing: write/write + DDL interleavings and a soak have not been rerun. Also open for M4: F-007 unreproduced; `DROP DATABASE` not owner-scoped; no column/row-level control or grant delegation; full suite and Linux/x86_64/chaos/1M+ scale gates not rerun. |
| **M5 — Cloud/SaaS** | **NO-GO** | All M4 gaps, plus no HA/PITR (cold-only backup by design), no multi-arch runtime proof, no supply-chain/SLO evidence, and only single-host measurements. |

### What changed since 2026-10-16

- F-001, F-004, F-006, and F-008 — previously M2/M4 blockers — are fixed
  and have targeted regressions.
- F-002/F-003/F-005/F-014 are fixed; F-009/F-010/F-012/F-013 are closed.
- F-007 is mitigated, not root-caused.
- The database-DDL authorization claim was corrected: the gate exists;
  the real residual is owner-scoped `DROP DATABASE`, not an open bypass.

### What would change the verdicts

- **M4 → GO WITH CONDITIONS:** complete an independent multi-user re-audit
  covering true write/write and DDL interleavings, plus a soak; close or
  explicitly scope owner-scoped `DROP DATABASE`; capture or retire F-007;
  run the full suite, chaos/scale, and Linux/x86_64 gates.
- **M5 → GO WITH CONDITIONS:** all M4 evidence, plus an explicit HA/PITR
  decision, multi-arch runtime proof, supply-chain attestations, and SLO
  evidence.

## Method notes and residual uncertainty

- This decision is source-level. It does not certify a binary, a release
  artifact, or cross-platform behavior.
- Test counts are not used as a production-readiness proof; the verdicts
  separate verified fixes from un-run evidence.
- The release-hygiene gate (binary rebuild, full suite, docs build) is
  deliberately left as a condition rather than claimed complete.
