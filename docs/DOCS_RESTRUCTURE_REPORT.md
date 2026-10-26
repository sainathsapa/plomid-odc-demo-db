# PLOMID Docs Restructure Report — v0.1.0 Developer Portal

Date: 2026-10-14. Scope: `docs/docs.json` + `docs/v0.1.0/` (103 → 130 navigated pages).
This file is intentionally outside the Mintlify navigation (no `docs.json` entry).

> **Addendum (2026-10-21):** this report records the 2026-10-14 verification pass.
> Some claims have since changed: `ILIKE` folds case and honors `ESCAPE`,
> `ANALYZE`/`REINDEX` execute over client connections, TLS negotiates, and
> catalog sharding changed session-open cost. The live `docs/v0.1.0/` pages
> are authoritative.

## 1. Final information architecture

| Group | Intent | Pages |
|---|---|---|
| Overview | Land, orient, warn (beta), learn path | index, what-is-plomid, beta-status **(new)**, installation, quickstart, first-database |
| Install & Deploy **(new)** | One guide per verified method + matrix | overview, binaries, shell-installer, docker, source, offline |
| Connect & Build Apps **(new)** | Params, psql, Python, JS, pooling/errors, sample app | overview, psql, python, javascript, pooling-errors (+ build-application relocated) |
| Learn | Concepts → data model → SQL → arch → paths | learn/* + architecture/{overview,request-lifecycle,query-pipeline,how-it-works} |
| Guides | Task outcomes incl. new retrieval + backup guides | build/* (minus build-application), guides/semantic-retrieval **(new)**, guides/backup-local **(new)**, cookbook/* |
| SQL | Reference, incl. corrected compatibility | 22 pages incl. compatibility + roles-permissions (warnings added) |
| JSON / Time-series / Data | Kept, de-duplicated from each other by links | 5 + 2 + 7 pages |
| Vector Search **(new)** | First-class vector section | 13 pages (overview → troubleshooting) |
| Indexes & Transactions | Kept | 5 pages |
| Storage & Operations | + backup-restore promoted into nav | 16 pages incl. backup-restore **(new to nav)** |
| Reference | + durable support matrix | support-matrix **(new)** + 5 existing |
| Roadmap | Renamed from "Future models"; vector corrected | vector-graph (rewritten), elastic-engine |
| Architecture Internals | Implementation detail, separated from users | 7 pages (6 arch deep-dives + storage/engine-internals relocated in nav only) |
| Contribute | Contributors, migration, troubleshooting | contributing/*, migration/postgres-users, troubleshooting/overview |

No files were moved on disk (all existing paths stable; only nav regrouping + new files).
`docs.json` parses; nav ↔ files reconcile 130/130 with zero orphans, zero missing, zero duplicates.

## 2. Old → new page mapping

### Retained and improved (edited in place)
- `index.mdx` — rewritten: beta warning, 5-step learning path, corrected capability table (vectors Supported), reference shortcuts.
- `getting-started/installation.mdx` — rewritten as canonical overview pointing at `install/*`; old source-build-only content moved to `install/source.mdx`.
- `getting-started/quickstart.mdx` — expanded to full CRUD + index + transaction + persistence; corrected server command (`plomid-server`, `--username`).
- `getting-started/what-is-plomid.mdx`, `first-database.mdx` — implementation refs removed; database-DDL transaction note added.
- `future/vector-graph.mdx` — rewritten: corrects "vectors unavailable" falsehood → points at `vector/`; graph/blobs/distributed stay roadmap-firewalled.
- `sql/compatibility.mdx` — fixed three stale claims: TLS (was "handshake declined", now Supported), vector pointer (was internal source paths, now `/vector/`), ILIKE (now states verified false behavior); added `DROP ROLE/USER` + DDL-in-transaction rows.
- `sql/roles-permissions.mdx` — added open-bypass warning; `DROP ROLE` non-goal confirmed.
- `sql/functions.mdx` — removed false "no vector-distance operators" line.
- `operations/profile-secure-remote.mdx`, `profile-production.mdx` — added single-operator-only warnings.
- `operations/backup-restore.mdx` — added to nav; added durability diagram; removed stale "Beta 2" scoping ambiguity.
- `data/types.mdx` — fixed raw `<col>` JSX breakage that failed `mintlify format`.
- `sql/explain.mdx` — fixed broken `../../USER_GUIDE.md` link (target does not exist) → `/build/inspect-query`.
- 25 user-facing files — removed `crates/*`, `*.rs`, parser-type, and provenance internals from prose (getting-started, build, learn, sql, data, json, operations/run-server, configuration).
- `install/docker.mdx` — added container diagram. All other install/connect/vector/guides pages are new (below).

### Created (28)
`getting-started/beta-status.mdx`, `install/{overview,binaries,shell-installer,docker,source,offline}.mdx`,
`connect/{overview,psql,python,javascript,pooling-errors}.mdx`,
`vector/{overview,quickstart,vector-types,distance-operators,similarity-search,indexes,hnsw,filtering,exact-vs-approximate,application-integration,performance,limitations,troubleshooting}.mdx`,
`guides/{semantic-retrieval,backup-local}.mdx`, `reference/support-matrix.mdx`.

### Relocated in nav only (no disk moves)
`build/build-application` → Connect & Build Apps; `architecture/how-it-works` → Learn;
`storage/engine-internals` → Architecture Internals.

### Removed: none.
Pre-existing working-tree deletions `docs/compliance/TRACEABILITY.md` + `evidence.json` predate this task (absent before first edit; `git show HEAD:` empty) and were left untouched.

## 3. Content gaps discovered
1. `future/vector-graph.mdx` falsely claimed vectors unavailable — fixed by new section + page rewrite.
2. `sql/compatibility.mdx` falsely claimed TLS handshake declined — fixed (live handshake verified).
3. `operations/backup-restore.mdx` was orphaned (not in nav) — promoted.
4. No install matrix, no per-method guides, no connect guides, no vector section, no support matrix — all created.
5. `sql/explain.mdx` linked a non-existent `docs/USER_GUIDE.md` — fixed.
6. `data/types.mdx` raw `<col>` broke MDX formatting — fixed.

## 4. Installation methods — evidence
- `install.sh` exists; `--dry-run` prints platform matrix; checksum refusal coded; Windows explicitly unsupported. Live: `sh install.sh --dry-run` → platform detected, "no releases yet" (honest — documented as check-first).
- `Dockerfile` verified (Debian slim, single binary, `plomid` user, fail-closed remote). No compose file ships (`ls docker-compose*` empty) — documented as Unsupported.
- `release.yml` publishes dmg+tar.gz (mac ×2), zip+exe (win), tar.gz (linux ×2), deb/rpm, SBOM, SHA256SUMS, multi-arch Docker (best-effort), Homebrew tap (best-effort).
- Server flags verified via `--help`: `--data`/`--username` (docs previously mixed `--data-dir`/`--user` in places — corrected in rewritten pages).
- `start.sh` is a dev-only `cargo run` wrapper (documented as such, not a release path).

## 5. Vector capabilities — evidence (live server `target/release/plomid-server`, ports 55433–55440)
Supported: dense `VECTOR(FLOAT32,3)` DDL/insert/select; `<->` ordering `[1,3,2]` exact values (0 / 0.1414 / 1.4142); `<=>` (0 / 0.006 / 1); `<#>` ascending (1,3,2); `<~>` Hamming (=1); `<%>` Jaccard (=0.333); `l2/cosine/dot` + similarity functions; `VECTOR(BINARY,8)` with `'10101010'` literals; `VECTOR(INT8,3)`; `USING hnsw` create + identical re-query; `EXPLAIN` vector section (`Vector Exact scan`, `Index: usable`, exact=9.0/ann=44.0 costs); `WHERE + ORDER BY` filtering (`[1,2]`); `SHOW vector.ann_enabled`; dim-mismatch hard error; HNSW-in-explicit-txn refusal; sparse/binary `SELECT` fallbacks.
Durability: `backup` (16 files) → `backup-inspect` (manifest v1) → `restore` (clean dir) → restart serves vector top-1 correctly.
Drivers: `psycopg` 3 + `node-postgres` full workflows incl. text vector params (BIGINT-as-string noted for `pg`).
Security negative: low-privilege role `CREATE DATABASE evil_db` → `CREATE DATABASE` (bypass OPEN, cleaned up after). `DROP ROLE/USER` → `unsupported DROP object`. `ILIKE 'ABC'/'abc'` → false. Extended-protocol DDL-in-txn → refused. TLS `sslmode=require` → `1` (handshake works).
Limits observed, documented: sparse literal insert rejected; dense brackets into BINARY rejected (kind mismatch); `inner_product()` missing (use `dot_product`); `CREATE INDEX` in txn refused; GPU none (CPU-only documented from code: `GpuStub`, `execute_with_cpu_fallback`).

## 6. Intentionally deferred
Sparse/multivector SQL write paths, GPU, hybrid SQL surface, distributed vectors, Kubernetes/Compose/DR, hot/PITR backup, `DROP ROLE`, ILIKE folding, DDL-bypass fix (engineering-owned) — each marked Experimental/Planned/Unsupported in `reference/support-matrix.mdx`, never in how-to paths.

## 7. Tests and builds executed (exact outcomes)
- `python3 -c "import json; json.load(open('docs/docs.json'))"` → OK.
- Frontmatter + fence-balance sweep, 130 files → NONE missing.
- Internal `/v0.1.0/*` link resolution, 130 pages → NONE broken. Relative-link sweep → 1 broken (`USER_GUIDE.md`) → fixed → NONE.
- `mintlify format` → final run "formatted 1 of 130" (all clean; 1 pre-existing `<col>` failure fixed first).
- Live example replay (`verify.py`, fresh data dir): **30 passed, 0 failed** (quickstart CRUD/index/txn-rollback; vector L2/cosine/HNSW/EXPLAIN/filter/functions/settings; KB retrieval; shared schema; both negative tests).
- Backup → inspect → restore → restart re-query: all exit 0, vector results correct post-restore.
- Restart persistence: counts + vector top-1 survive.
- Python + Node driver scripts: connect/query/vector-param/close all OK.
- TLS live handshake (`sslmode=require` → 1): OK.
- `cargo` test tiers NOT re-run (binaries prebuilt; live-server verification used instead — stronger for doc claims). No benchmark numbers published.

## 8. Remaining blockers
1. **DDL authorization bypass OPEN** — engineering fix required; docs correctly gated to single-user until verified.
2. **ILIKE semantics** — parser discards case-insensitivity; documented workaround (`lower()`), fix engineering-owned.
3. No published release artifacts for current beta tag yet — install guides instruct check-first (`--list`, releases page).
4. `docs/compliance/*` deletions predate this task — owner should confirm intent.

## 9. Second sweep — hidden-feature audit (same day, pre-release)

**Doc bugs fixed (docs contradicted code or itself):**
- `reference/scalar-functions.mdx` claimed "No vector-distance functions" — false; replaced with the real 8-function list + pointer. Also removed `lastval()` / `user()` / `current_setting(...)` examples (verified: `lastval` unsupported, `user` is a reserved word → syntax error, `current_setting` unsupported) and added working `current_schemas(true)` (verified `{public}`).
- Savepoint contradiction reconciled across `transactions/examples.mdx`, `cookbook/ecommerce.mdx`, `build/run-transactions.mdx`: true partial rollback over client connections (verified live: 99 kept, 100 rewound) vs parser-level full rollback; drivers that *prepare* savepoint statements may see rejection. Portable guidance: keep transactions retryable.
- `contributing/repo-structure.mdx`: removed phantom `lifecycle` crate row (no such crate; lifecycle lives in `txn` engine + `server` wiring), fixed `filters` row (XOR only, no `roaring.rs`), added missing `vector/htap/gauntlet/spill/memory/stream/hw/security` rows, corrected crate count 19 → 25. Same phantom fixed in `contributing/overview.mdx`.
- `reference/errors.mdx`: removed stale `tls_mode_not_yet_implemented` row (TLS works); added `28000`, `57014`, `25P03` (idle reap), `42P04` (database already-exists), `53200` resource-exhaustion, auth-path detail; de-internalized the intro.
- `operations/htap-gauntlet.mdx`: headline penalty corrected to p99-latency ratio (was throughput ratio).
- ILIKE wording unified everywhere (`migration/postgres-users.mdx`, `sql/operators.mdx`): parses, behaves as case-sensitive `LIKE`.
- `data/types.mdx`: fixed raw-`<col>` JSX breakage again + replaced internals with a catalog/system row (`reg*`, `tid/xid/cid`, `pg_lsn`, `money`, `cidr`, `macaddr8`, `varbit`, `tsquery`) and range-completeness note.

**Implemented-but-undocumented, now documented (all live-verified unless noted):**
- Functions: `jsonb_path_query[_array|_first][_tz]`, `jsonb_path_match_op` (+ `strict`-parses-but-lax-only note) in `json/functions.mdx` + `sql/functions.mdx`; `current_schemas(bool)`; `pg_get_userbyid` / `pg_get_statisticsobjdef_columns` NULL-stubs.
- Settings/introspection: all 9 `vector.*` SHOW + `pg_show_all_settings` row inventory; `vector.compute` in `vector/indexes.mdx`; `pg_settings` 7-row inventory; `IntervalStyle` startup param; generic `SET`-accept-and-store rule + `SET ROLE` no-op + `RESET`/`DISCARD ALL` unsupported (pooler warning) in `show-describe.mdx`.
- CLI/config: `--copy-dir`, `--audit-log`, `--profile` (server + `plomid server start`), `test run` default + hard-error modules, `help-backup` + flag forms — in `cli-reference.mdx`, `config-reference.mdx`, `configuration.mdx`, `cli.mdx`, `backup-restore.mdx`.
- Catalog: 20+ missing `pg_*` relations + `information_schema` extras + all 14 `plomid_*` functions (incl. previously zero-hit `plomid_stat_tables`, `plomid_vector_indexes`) in `pg-catalog.mdx`; `plomid_stat_tables/vector_indexes` rows + TLS columns + security 7-column list in `logging-diagnostics.mdx`.
- SQL: `TABLE`/`TABLESAMPLE`/bare-`VALUES`/SQL-`PREPARE`/`RESET` unsupported lines, `setval()` note, savepoint protocol note, `USING btree|art` relational acceptance (verified) + UNIQUE `23505` message + no-`REINDEX`-needed note, `EXTRACT` exact field list (code-derived; `week`/`epoch` verified rejected), `age()` semantics, `SET TRANSACTION` scope rules + idle-reap `25P03` in `using-transactions.mdx`, pruning-invisible-in-`EXPLAIN` note, `||` on jsonb, cast/vector/range operator rows, `ANALYZE`/flush non-tunability in `columnar.mdx`, spill/HW-tier/SIMD/governor/memory sections in `performance.mdx`, audit record format in `rbac.mdx`.
- `reference/support-matrix.mdx` extended with all of the above deltas.

**Verification rerun:** nav 130 / files 130, frontmatter all OK, all page + relative links resolve, no user-dir leakage, 34 mermaid diagrams, `mintlify format` clean. New claims spot-verified live (`current_schemas`, `jsonb_path_*`, 9 vector settings, `plomid_stat_security/tables`, EXTRACT rejections, `USING btree|art`, PK violation message, `help-backup`). Full 30-check replay from §7 unaffected (no example SQL changed, only prose + new reference rows).

## 10. Third sweep — deep-read + command verification (next day, pre-release)

Fresh angles: full deep-read of 22 previously unaudited pages, verification that every `make` target / CLI command / env var / artifact in docs exists, anchor resolution, placeholder scan.

**All commands verified to exist** (zero missing): every `make` target referenced, every `plomid`/`plomid-server` subcommand and flag (incl. `--json`, benchmark options, `--profile` forwarding), `install.sh` flags/env, `start.sh` usage, Dockerfile description, release.yml asset names, all `PLOMID_*` env vars (client-only `PLOMID_DB`, installer-only `PLOMID_VERSION/DIR`, harness `PLOMID_SCALE_ROWS` correctly scoped), all 8 vector functions + 9 `vector.*` settings in new pages. No TODO/FIXME/TBD/placeholder-in-code anywhere (one benign prose "placeholder" in a SQL comment). All `#anchor` links resolve.

**Bugs found live and fixed:**
- `GROUP BY` ordinals rejected engine-wide (verified: `GROUP BY 1` errors; `ORDER BY 1` fine; alias works for bare columns only). **7 broken examples** across `timeseries/queries.mdx`, `cookbook/overview.mdx`, `cookbook/complex-queries.mdx`, `build/query-time.mdx` rewritten to full expressions and verified live; rule added to `ordering-grouping.mdx` + support matrix.
- `timeseries/queries.mdx` had 3 more unrunnable examples: timestamp `generate_series` (integers-only — replaced with verified arithmetic grid + live-tested), tuple comparison `(ts,id) < (...)` (no row comparison — replaced with single-column keyset), `DELETE ... RETURNING count(*)` (replaced with `RETURNING *`; both verified live).
- Roaring bitmaps on the pruning path: **dead code** (`ROARING_MAGIC` constants have zero users; filters crate owns XOR only). Removed from `timeseries/overview.mdx`, `storage/columnar.mdx`, `architecture/mvcc-filtering.mdx` (diagrams + pipeline), `sql/explain.mdx`; true pipeline is `ZoneMap → BRIN → XOR → exact`.
- `enum_range()` documented but unsupported (verified) — removed from `enums-domains.mdx`; `Some([])` leak removed.
- `inet('host', prefix)` 2-arg form ignores the prefix (verified: returns /32) — doc now shows `inet('10.0.0.1/24')` with the caveat.
- `sql/overview.mdx`: bare `VALUES` listed as supported (verified syntax error — removed), ROLE row scoped (`DROP ROLE` unsupported), utility row split (wire-acked vs SQL-unsupported), `REFRESH` leak removed.
- `sql/select.mdx`: runnable `JSON_TABLE` example (verified live), `__anon`/`needs_general_engine`/`MAX_SUBQUERY_DEPTH` leaks removed, ILIKE wording unified.
- Stale "until TLS lands" (2 pages — TLS landed), "Beta 2" hot-reload note → v0.1.0, built-in-password defaults clarified per entrypoint, `mount_trace`/`checkpoint_with_report` identifiers generalized, `KV deployment` undefined term removed.
- `build/create-database.mdx` per-DB-WAL overclaim softened; cookbook removed unverified "10,726-line" number, "indexed join/CTE materialization" overclaim, ambiguous ILIKE note; offline profile removed unverified pool-math + backup-method claims (points at offline tools now).
- `transactions/mvcc.mdx` + `behavior-contracts.mdx` moved to Architecture Internals (contributor-level detail, no disk moves).
- Re-verified as still-true: ILIKE=false, `tls_connections` counter present, NULLS ordering defaults, `currval`, `_pg_expandarray` bare, `(g || ' minutes')::interval`, `json_populate_record`, expression-index creation, file-COPY rejection.

**Final validation:** nav 130 / files 130, frontmatter all OK, all links + anchors resolve, `mintlify format` clean (0 failures), no user-dir leakage, remaining "GROUP BY 1"/"Roaring" hits are intentional (documenting the rejection/removal).


