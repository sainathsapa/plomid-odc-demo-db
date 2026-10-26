# 15 — Findings register

Severity: **P1** blocks a release mode or breaks a default data path;
**P2** materially wrong/dangerous → fix or document before wider Beta;
**P3** minor/UX/quality; **NOTE** recorded behavior, no severity.
“Confidence” = live evidence + code as cited. Modes as in `00`.

---

## F-001 — Storage B+Tree split breaks bulk INSERT on vector tables (P1)

- **Where:** storage B+Tree split under vector-column auto index; not
  fixed by the tranche (pre-existing; deliberately not fixed).
- **Evidence:** `storage_split_fail.log` (batch 2 of 4×1000 dim-64 rows
  → `B+Tree node exceeds page payload`) vs `storage_split_control.log`
  (auto index dropped → 4 000 rows OK); mechanism notes in
  `artifacts/storage-split/storage_split_mechanism.txt`; bench
  workaround comment (`tests/python/vector_lifecycle_bench.py`).
- **Impact:** `CREATE TABLE (v VECTOR(FLOAT32,64))` + bulk INSERT fails
  from ~2–4 k rows in the default flow. Provides silent upgrade trap
  for new users; blocks M2 default-path confidence and all insert
  throughput claims measured elsewhere.
- **Acceptance:** size-aware/separator-aware split (or fail-closed with
  a clear error and a documented workaround before 4 k rows); regression
  test: dim-64 vector table with auto index, 10 000-row bulk load, no
  split error.
- **Status (2026-10-19; re-verified 2026-10-23):** fixed in `9e35ced`.
  Internal-node splits now choose the promotion boundary by encoded
  size (`btree::choose_internal_split`, matching the existing leaf
  split), so a page mixing multi-kilobyte vector-index keys with short
  PK keys cannot leave an oversized half. Regressions:
  `crates/storage/tests/unit_btree.rs` (mixed-size internal/leaf splits)
  and `crates/executor/tests/vector_auto_index_bulk_load.rs`
  (`bulk_load_with_dim64_auto_vector_index`, 10 000 rows in 1 000-row
  batches with the auto index present, then count/point-read/READY
  checks).

## F-002 — `REINDEX` is a silent wire no-op; ANN caches never invalidate (P2)

- **Where:** `crates/network/src/server/handler.rs` ~L1789 short-circuits
  `REINDEX` (also `ANALYZE`, `COMMENT`, `LOCK`, `CLUSTER`, `REFRESH`)
  with a fabricated CommandComplete before executor dispatch;
  `execute_reindex` (`ddl.rs:947`) unreachable over the wire.
- **Evidence:** `plomid_stat_admin()` lacks `reindex` after `REINDEX`
  (`audit16-reindex-admin.log`); trace shows no `sql::execute` for the
  statement (`audit16-reindex-trace.log`); caches stay fresh and ids
  identical after `REINDEX` on 5 k/50 k tables
  (`reindex_semantics_probe.log`, `hnsw_reindex_ef_probe.log`);
  in-process executor test passes (`observability_metrics`); compact
  matrix: `rebuild_miss=0` on all four cells — the post-`REINDEX` query
  is a warm hit (`06` §3).
- **Impact:** operators cannot force graph drops/cache invalidation;
  benches that assume REINDEX invalidates are wrong (`13`); docs
  contradict the implemented executor path.
- **Acceptance:** route `REINDEX` to the executor (drop graphs,
  invalidate, log `reindex dropped_graphs=`); wire-level regression test
  asserting invalidation; OR explicit protocol-level error + removed
  executor implementation + docs agreement.
- **Status (2026-10-19):** fixed. The wire short-circuit is gone:
  `REINDEX` (and `COMMENT`/`ANALYZE`/`LOCK`/`CLUSTER`/`REFRESH`) now
  execute through the engine on both the autocommit and in-transaction
  paths (`handler.rs`; in-txn `REINDEX` added in `session_txn.rs`, and
  the compatibility stubs acknowledge from the engine). Graphs drop and
  the next query misses/rebuilds, `reindex` lands in `plomid_stat_admin()`
  and the audit sink. Regression:
  `crates/network/tests/maintenance_wire.rs`.

## F-003 — `ANALYZE` is a silent wire no-op, contradicting docs (P2)

- **Where:** same short-circuit. Executor implements real collection
  (`executor.rs:2114` → `analyze()` → `statistics::collect/save`).
- **Evidence:** `ANALYZE an` over the wire with `--log-level debug` →
  no `sql::statistics: analyze complete`, no `sql::execute` trace
  (`audit16-analyze-wire.log`). `docs/v0.1.0/architecture/executor.mdx:33`
  claims persisted statistics.
- **Impact:** statistics never refresh over the network; doc/behavior
  contradiction; operator expectations broken.
- **Acceptance:** route or document; if intentionally stubbed, move the
  fabricated-tag behavior into documented protocol stubs and amend
  executor.mdx.
- **Status (2026-10-19):** fixed by routing. `ANALYZE` over the wire runs
  `executor.rs:analyze` (collect + persist), is counted in
  `plomid_stat_admin()` (`analyze`), and inside an explicit transaction
  block refuses with an engine error instead of a fabricated
  CommandComplete. `executor.mdx` amended. Regression:
  `crates/network/tests/maintenance_wire.rs`.

## F-004 — Concurrent same-name `CREATE TABLE`: both succeed (P1 for M4)

- **Evidence:** `catalog_ddl_probe.log` (`FAIL same-name CREATE race:
  exactly one winner: ['ok', 'ok']`; one catalog entry afterwards);
  `namesake_race_probe.log` (sequential control rejects; same-schema and
  diff-schema pairs both `ok`; survivor schema race-dependent).
- **Impact:** catalog determinism under concurrent DDL; the losing
  client holds a nonexistent/incorrect shape. Blocks M4/M5.
- **Acceptance:** serialize name resolution + create (or commit-time
  unique violation); exactly one winner; regression test extension of
  `crates/sql/tests/concurrent_ddl_ids.rs`.
- **Status (2026-10-19; re-verified 2026-10-23):** fixed in `9e35ced`.
  Autocommit `CREATE TABLE` re-probes durable existence under a
  `CreateGate` and saves while holding it; explicit-transaction and
  grouped commits re-probe their freshly claimed names under the same
  gate, with the loser aborting and adopting durable state. Regression:
  `crates/executor/tests/create_table_race.rs` covers the autocommit,
  transaction, and group-batch races (exactly one winner) plus
  stale-session and DROP+CREATE controls; the same-name case also runs
  in `crates/sql/tests/concurrent_ddl_ids.rs`. F-014 was filed while
  fixing this race and is closed separately.

## F-005 — WHERE-clause `LIKE/ILIKE … ESCAPE` ignored (P2)

- **Evidence:** 2 probe FAILs vs PostgreSQL oracle
  (`param_avg_ilike_probe.log`); JOIN path honors ESCAPE (NOTE `[5]`);
  code drop in `crates/executor/src/query/expr.rs` (escape dropped
  before matcher; contrast `join/eval.rs:346`).
- **Impact:** silent wrong (mostly empty) results for escaped patterns.
- **Acceptance:** honor ESCAPE in row-scan LIKE/ILIKE; add WHERE
  ESCAPE cases to the qualify matrix.
- **Status (2026-10-21):** fixed. Row scans now call the same escape-aware
  `scalar::like_match_regex` matcher as joins instead of the local matcher
  that discarded `escape`; the recursive matcher was removed so both
  paths also share linear-time behavior. `sql_compat::
  where_like_and_ilike_honor_escape` covers literal `_`/`%`, `ILIKE`
  folding, `NOT LIKE`, and a non-backslash escape through a real table
  scan.

## F-006 — ~5 s per-connection catalog load on large catalogs (P2)

- **Evidence:** big dir (7.5 MB catalog): ~4.9–5.0 s to first server
  message per connection, auth attempts show the same cost
  (`boot1_incident_excerpt.log`); small dir 23 ms;
  `executor.rs:191-192` full `load_catalog` per connection.
- **Impact:** latency + DoS amplification for M3+; breaks naive
  short-lived client workloads.
- **Acceptance:** catalog cache/version-probe (target ≪100 ms/connect
  on 10 MB catalogs) or documented pooling mandate.
- **Status (2026-10-21):** resolved by the sharded catalog layout; no cache
  needed. Measured on a dev machine with a 60,000-table catalog
  (7,500,111 catalog value bytes): `load_catalog` 56–61 ms and
  `Executor::new` 56–74 ms in release (222 / 217–243 ms in debug), versus
  the audit's ~5 s. A first open of a legacy single-blob image decodes in
  ~31 ms release and pays a one-time migration save. The sharded layout
  is guarded by `sharded_save_load_roundtrips_many_tables`; the new
  ignored scale test
  `session_open_on_a_large_sharded_catalog_stays_fast`
  (`crates/executor/tests/catalog_load_scale.rs`, run by `make
  test-scale`) enforces the 2 s debug ceiling. `performance.mdx`
  documents the conditions and still recommends pooling for
  short-lived clients.

## F-007 — Boot-time auth anomaly after crash restart (P2, observed once)

- **Evidence:** `boot1_incident_excerpt.log` (correct-credential denies
  as `bad_credentials`, ~5 s/attempt → `too_many_failures` lockout →
  `locked_out`; big dirty dir after SIGKILL mid-shutdown). Not
  reproduced with plain SIGKILL-idle restart of a cloned dir (~4.91 s
  auth OK). Not PBKDF2/rate-limiter/lockout-residue in isolation.
- **Impact:** post-crash availability risk; potential false lockouts.
- **Acceptance:** instrumentation to capture a recurrence with full
  state; add regression scenario (crash-after-dirty-state restart);
  interim monitoring on `security_decision action=lockout|locked_out`.
- **Status (2026-10-21):** instrumentation, regression, and monitoring
  landed. `security_auth_diagnostics` now accompanies every password,
  MD5, and SCRAM decision with `total_ms`, `executor_ms` (pre-auth
  catalog load), `auth_ms`, rate-limit failures/delay, lockout state,
  and credential provenance (`role_present`, `has_verifier`,
  `has_legacy_password`, `bootstrap_name`). This separates the two costs
  the incident log conflated: the ~5 s pre-auth wait is
  `Executor::new_shared` catalog load (F-006; now ~56–74 ms at the
  audit's scale), while a genuine credential failure shows a small
  `executor_ms` and a failed `auth_ms`. Monitoring adds the
  `auth_locked_out` counter to `plomid_stat_security()` (distinct from
  `account_lockouts`) and the crash-restart alert rule in
  `operations/logging-diagnostics.mdx` / `operations/rbac.mdx`.
  Regression: `auth_succeeds_after_dirty_restart_with_large_catalog`
  seeds a stored verifier plus a 2 000-table catalog, aborts the server
  without clean shutdown, reopens the same storage + WAL, and requires
  the first correct login to succeed;
  `locked_out_attempts_increment_the_dedicated_counter` covers the
  monitoring signal. The original `bad_credentials` event remains
  unreproduced; a recurrence is now self-diagnosing rather than silent.

## F-008 — ANN recall cliffs on dim-64 clustered fixtures (HNSW and DiskANN) (P2)

- **Evidence:** `hnsw_dim64_investigate.log` (50 k sine: 0/10, ann best
  d=2.3151 vs exact worst 1.9935, wrong-region ids; random: 6/10,
  d≈5.2 vs 1.9), `hnsw_ef_sweep.log` (ef 100/400/1600 identical ids —
  build-baked), scale probe recall@10=0.00; compact matrix on the
  bench's own clustered dim-64 fixture (`06` §3): HNSW 1.00 (2 k) →
  0.90 (8 k), **DiskANN 0.00 (2 k) / 0.20 (8 k)** — the cliff is not
  HNSW-only and is worst on DiskANN.
- **Impact:** quality claims bounded; users with clustered high-dim
  data may get near-useless results while declared gates pass.
- **Acceptance:** add clustered dim-64 fixture (generator in audit
  scripts) with a floor or explicit known-limitation marker; investi-
  gate build-time region coverage (graph construction seeding/beam).
- **Status (2026-10-19):** fixed. Root cause: `build` wired edges only
  for targets still inside the bounded cache — every row past
  `DEFAULT_CACHE` (1024) got no out-edges and the graph degenerated to
  random back-edges — compounded by a 256-candidate sample and no
  long-range seeding. Fix (`vector/src/index/diskann/mod.rs`): page
  evicted repair targets, repair from the in-memory build rows,
  1024-candidate sample, 4 reserved long-range peers per node. Fixture +
  floors: `crates/vector/tests/ann_dim64_clustered.rs` — recall@10
  1.00 (2 k) / 0.90 (8 k, ignored scale tier) vs floors 0.90/0.80
  (pre-fix 0.00/0.20); unit tripwire
  `edge_repair_pages_targets_beyond_the_cache_window`; floors documented
  in `docs/v0.1.0/vector/exact-vs-approximate.mdx`.

## F-009 — Duplicate ANN pass on 50 k tables (P3)

- **Evidence:** single statement → `ann_queries_total +2`,
  `ann_cache_hits_total +2`, ~2× latency (126→250 ms), identical ids
  (`hnsw_reindex_ef_probe.log`, `audit16-trace-dbl.log`; 8 k control:
  one pass, one `sql::execute` per query). Compact matrix 2 k/8 k:
  warm hit deltas 12 per 8 queries and 10 per 5 post-`REINDEX` queries
  (1.5–2×/query) without miss-priced latency — counter increments are
  not pinned to 1/query even at small sizes (`06` §3).
- **Impact:** latency only; unresolved mechanism (candidate-generation
  path vs internal retry). Acceptance: root-cause + fix, or document
  with counter semantics (F-013).
- **Status (2026-10-23):** fixed. Root cause was the wire Describe path,
  not the ANN matcher: `handle_describe` executes read-only statements
  with NULL placeholders to infer result metadata, and prepared clients
  Describe the portal before every Execute, so each ANN query ran once
  for the portal Describe and again for Execute. `PreparedStatement` now
  caches the inferred `(columns, column_types)` and reuses it for later
  statement/portal Describes. The cache is keyed by catalog generation,
  database, and session search path, so DDL/name-resolution changes
  trigger a fresh probe. The first statement Describe still performs one
  legitimate shape probe. Regression:
  `crates/network/tests/ann_describe.rs::prepared_portal_describe_does_not_rerun_ann`
  asserts one invocation for the statement Describe and exactly one per
  subsequent portal-Describe + Execute round.

## F-010 — EXPLAIN text vs executed strategy can disagree (P3)

- **Evidence:** `vector_commit_probe.log` NOTE (explain cost
  exact=16000.0/ann=765.8 at n=2000, execution ANN due to live
  `auto_ann_min_rows=0`; planner floor 4096 used by explain only);
  compact matrix (fresh instance): n=2 000 cells print `Vector Exact
  scan` in EXPLAIN while execution ran the ANN path (miss=1, build=1,
  build priced into the measured query); n=8 000 prints `ANN scan`
  (`06` §3).
- **Acceptance:** align explain gating with execution settings or label
  the line as estimate-only.
- **Status (2026-10-23):** fixed. EXPLAIN's vector section now takes its
  exact-vs-ANN decision directly from the live execution gates
  (`vector.auto_ann_min_rows`, `vector.max_ann_rows`, LIMIT > 0, usable
  index) instead of applying a second constant `AUTO_ANN_MIN_ROWS` floor.
  Regression:
  `crates/executor/tests/vector_explain_gate.rs::explain_vector_strategy_follows_live_execution_gates`
  covers the default floor, lowered threshold, and resource ceiling.

## F-011 — Documentation staleness cluster (P3)

- ILIKE case-sensitivity claims: `docs/v0.1.0/reference/compatibility.mdx`,
  `reference/support-matrix.mdx`, `beta-status.mdx` (live: case-folding
  works; 10/10).
- `executor.mdx:33` ANALYZE “real statistics” over the wire (F-003).
- `executor.mdx:34` REINDEX “no-op/stub” vs executor implementation
  (F-002) — contradictory in both directions.
- DiskANN wording: checkpoints persist but cold start “rebuilds from
  snapshot rows” (`06`); phrase accordingly.
- Acceptance: one docs pass tied to live evidence; add a doc-vs-probe
  lint item to the qualify checklist.
- **Status (2026-10-21):** complete. Every live `docs/v0.1.0/` ILIKE
  claim now states case-insensitive matching with `ESCAPE`
  (`sql/compatibility`, `reference/support-matrix`,
  `getting-started/beta-status`, `sql/operators`, `sql/expressions`,
  `sql/select`, `cookbook/overview`, `migration/postgres-users`,
  `connect/overview`); the beta-status restriction row was removed.
  `architecture/executor.mdx` already matched F-002/F-003. ANN restart
  wording now says checkpoints persist but the serving graph rebuilds
  from snapshot rows (`vector/hnsw`, `vector/indexes`,
  `vector/limitations`). `tests/PLOMID_FULL_PRODUCTION_QUALIFICATION.sql`
  §00 is the doc-vs-probe item: live ILIKE/ESCAPE probes that must print
  PASS; `operations/htap-gauntlet.mdx` lists it as a pre-`STOP`
  checklist item. `sql_compat::where_like_and_ilike_honor_escape` now
  executes the same probes, so the docs, the checklist, and the
  regression suite share one set of expressions. Also corrected the
  stale TLS-declined line in `migration/postgres-users.mdx`. Evidence:
  `sql_compat::where_like_and_ilike_honor_escape` plus the audit's G-09
  10/10 live matrix.

## F-012 — `GRANT … ON ALL TABLES` unsupported (NOTE)

Syntax error `expected object name, found ALL` (probe note). Table-level
grants work (RBAC matrix). Document or implement.
- **Status (2026-10-23):** implemented. `GRANT` and `REVOKE` accept
  `ON ALL TABLES IN SCHEMA <schema>`. Execution validates the schema,
  role, and table-privilege list, then expands to the tables present at
  statement time (matching PostgreSQL: tables created later are not
  covered). Non-admin authorization requires ownership of every table in
  the expansion. Docs updated in `sql/roles-permissions.mdx` and
  `sql/overview.mdx`. Regression:
  `crates/executor/tests/rbac.rs::all_tables_in_schema_grant_applies_to_current_tables_only`.

## F-013 — Stats counter semantics can mislead (NOTE)

`ann_cache_hits_total`/`ann_queries_total` count invocations; >1 per
query is possible (F-009). Document in `plomid_vector_stats` docs;
tests should assert “≥1 hit” not “==1”.
- **Status (2026-10-23):** documented. `operations/logging-diagnostics.mdx`
  now states that these counters count ANN invocations rather than SQL
  statements: the first prepared Describe may add one read-only shape
  probe, later portal Describes reuse the cached shape (F-009), and the
  per-execution accounting remains the `sql::execute` trace event. The
  guidance is to assert `>= 1`, not `== 1`, for a query that may be
  described.

## F-014 — Concurrent first session opens on a fresh root can fail (P2)

*(Discovered 2026-10-19 during the F-004 fix; not part of the original
audit run.)*

- **Where:** `crates/storage/src/layout/metadata.rs::publish` stages every
  image at the target's fixed `<file>.tmp` path (`layout/names.rs:37`
  `STAGING_SUFFIX`); `materialize_database`/`materialize_schema`
  (`executor/ddl.rs`, `if !path.is_file() { record.publish(&path) }`)
  publish the shared default database/schema metadata whenever a session
  opens on a root that has not materialized them yet (`Executor::new` →
  `materialize_session_defaults`; the wire server opens one executor per
  connection, `network/server/handler.rs:373`).
- **Evidence:** two threads opening `Executor::new_shared` concurrently on
  a fresh engine root: one fails with `session: Storage(PlomidError {
  kind: Io, message: "I/O operation failed (NotFound)", source: … "No
  such file or directory" })`. Both publishers observe `!path.is_file()`,
  both stage to the same `<file>.tmp`, the first `rename` succeeds, the
  second fails `ENOENT`. Reproduced deterministically in a 2-thread
  repro; the F-004 regression suite
  (`crates/executor/tests/create_table_race.rs`) works around it with a
  pre-race warm-up session and documents why.
- **Impact:** concurrent first connections to a fresh instance (or after
  its metadata directories are rebuilt) can fail to open a session with
  an internal-looking I/O error; a retry succeeds. When concurrent
  publishers of the same target disagree on content, the shared staging
  path can also publish whichever bytes landed last.
- **Acceptance:** unique staging path per publisher (pid + monotonic
  suffix) or an idempotent publish that re-checks the final file on
  rename `ENOENT`; regression test: N sessions opened concurrently on a
  fresh root all succeed.
- **Status (2026-10-21):** fixed. `metadata::publish` now stages at a
  process-unique `<file>.<pid>.<seq>.tmp` (the fixed `staged_path` remains
  the name used by serialized publication paths and as an observation
  path), so concurrent publishers no longer truncate or rename each
  other's artifact. The `create_table_race` warm-up workaround is removed
  and `concurrent_first_sessions_on_a_fresh_root_all_succeed` opens 8
  sessions simultaneously on a fresh root.

## Closed this cycle (no action)

REAUDIT-003 items (AVG typing PG-parity; `DROP ROLE` with guards) —
evidence in `03`/`14`.
