# 04 — Catalog and DDL

Evidence: `artifacts/probes/logs/catalog_ddl_probe.log` (11/12),
`namesake_race_probe.log` (2/4 by design), `reindex_semantics_probe.log`,
`server_01.log` cluster, plus this cycle's REINDEX/ANALYZE wire
investigation (logs in `artifacts/probes/logs/audit16-*.log`).

## Cross-connection visibility and persistence — PASS

- B sees A's new table rows; A sees B's write; an index created by A is
  usable by B; B's prepared/parameterized query works against A's DDL;
  `DROP` is immediately invisible to B (`table "catvis" does not exist`).
- Restart: created table persists; dropped table stays dropped.

## REAUDIT-001 (concurrent-DDL ID collision) — not reproduced

Interleaved create/drop stress: 4×25 operations in 1.9 s, zero errors,
zero marker mismatches, no leftover `race_` tables. The physical
ID-collision mechanism was not exercised destructively (as in the
baseline, by design); status: **UNVERIFIED, no live reproduction** —
`crates/sql/tests/concurrent_ddl_ids.rs` exists and is part of the suite
run (`12`).

## F-004 — concurrent same-name `CREATE TABLE`: both succeed (P1)

- Sequential control: duplicate `CREATE` rejected
  (`table "public.seq_dup" already exists`) — PASS.
- Concurrent same-schema and different-schema pairs: **both clients got
  `ok`**; exactly one catalog entry exists afterwards; the surviving
  schema is race-dependent (`[('id','int4')]` in the diff-schema pair).
- Hazard: the losing client holds a table shape that does not exist;
  a subsequent DML from it either errors or, for name-compatible shapes,
  silently targets the winner. Catalog is deterministic afterwards, so
  this is an ordering/serialization gap in name resolution + existence
  check, not a storage leak.
- Impacted modes: M4 (blocks), M5. Local single-user modes are not
  affected in practice. Acceptance: a DDL serialization/speculative
  create path (or unique-name commit conflict) so exactly one creator
  wins; regression test extending `concurrent_ddl_ids.rs`.

## F-002 — `REINDEX` over the wire is a silent no-op (P2)

Chain of evidence (all this cycle):

1. Live: `REINDEX` returns success but `plomid_stat_admin()` shows no
   `reindex` row (`VACUUM` does record `vacuum` — stats surface works).
2. Trace: the handler logs `query_received`/`query_started`
   (`statement_type="REINDEX"`) but **no `sql::execute` trace** and no
   `query_completed` for that query — the executor is never invoked.
3. Code: `crates/network/src/server/handler.rs` (~L1789) short-circuits
   `COMMENT | ANALYZE | REINDEX | LOCK | CLUSTER | REFRESH` with a
   fabricated `CommandComplete` before dispatch. The executor's
   `execute_reindex` (`crates/executor/src/ddl.rs:947`, drops graph
   indexes + invalidates ANN caches, logs
   `sql::ddl: reindex dropped_graphs=…`) is unreachable over the wire.
4. Consequence measured: after `REINDEX`, ANN queries stay cache-fresh
   (`fresh=1`, identical ids, no rebuild) on 5 k and 50 k tables; no
   `reindex dropped_graphs=` line ever appears. In-process the same
   statement works (`crates/executor/tests/observability_metrics.rs`
   passes) — wire/engine divergence.

Impact: operators cannot force ANN graph rebuilds or cache invalidation
via SQL; the lifecycle bench's `rebuild_ms` column (measured after
`REINDEX`) actually measures warm hits (see `13`). Doc context: the
executor table lists `REINDEX/DO/…` as *no-op/stub*, but the executor
implements real behavior and the protocol layer silently hides it — the
two disagree.

## F-003 — `ANALYZE` over the wire is a silent no-op, contradicting docs (P2)

- Same short-circuit block. Live with `--log-level debug`:
  `ANALYZE an` returns `ANALYZE`; **no `sql::statistics: analyze complete`
  line**, no `sql::execute` trace. In-process, `Statement::Analyze` is
  special-cased (`executor.rs:2114`) to `analyze()` which collects and
  persists row/null/distinct/min/max statistics
  (`executor.rs:279` → `statistics::collect/save`).
- `docs/v0.1.0/architecture/executor.mdx:33` states ANALYZE performs
  “real statistics collection … persisted + cached”. Over a network
  connection that never happens. Either route the statement to the
  executor or fix the docs/protocol statement.

## F-006 — per-connection catalog load (~5 s on big dirs) (P2, perf)

On the 1.4 GB lifecycle-alloyed data directory (7.5 MB catalog), every
*new connection* is slow to first response (~4.9–5.0 s; small dirs:
23 ms). Root cause: `Executor::new` → `load_catalog`
(`crates/executor/src/executor.rs:191-192`) loads the full catalog image
per connection; the accept/ready times of a booted server look fine
(149–179 ms) — the cost lands on each client session. Authentication
traces in `boot1_incident_excerpt.log` show the same ~5.0 s per attempt
inside the auth path. Workaround for clients: connection pooling.

## Other

- `GRANT … ON ALL TABLES` unsupported (syntax) — NOTE (F-012).
- Session-scoped `SET` statements other than registered knobs are
  acknowledged locally (documented behavior; `SET vector.*` applies
  server-wide in v1 — see `06`).
