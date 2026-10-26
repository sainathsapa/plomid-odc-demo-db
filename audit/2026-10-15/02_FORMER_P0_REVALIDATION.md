# 02 — Former P0 revalidation

Binary: release `6c861f51`, ports 55461+. Raw logs: `artifacts/p0-001/`,
`artifacts/p0-002/`. Current symbols: `handle_parse` (`handler.rs:1422`,
gate at :1435), `handle_query` (:1724, pre-split at :1747), txn arms at
:1888/:1898/:2106/:2175, `handle_session_transaction` (:2710),
`extended_parse_statement_count` (:5195), `split_simple_protocol_statements`
(:5228); float literal `expressions.rs:1935–1950`, `infer_expr_type` /
`arithmetic_static_type` (`join/plan.rs:336+`).

## P0-001 — extended-protocol batch handling: PASS (genuinely fixed)

- Unchanged old reproducer shape (`BEGIN; INSERT INTO ms3_t VALUES (1);
  COMMIT;` via psycopg parameterless execute): all three parts applied,
  `SELECT *` → `[(1,)]` (`p0_001_live.log`). No silent loss; session stays
  usable. (Parameterless psycopg execute travels the simple Query path, which
  now pre-splits per PostgreSQL multi-statement semantics.)
- True extended Parse with multi-statement body (`prepare="multi_stmt_test"`):
  rejected `cannot insert multiple commands into a prepared statement`
  (42601), zero side effects (`COUNT(*)` → 0), session Idle and reusable
  (`ext_parse_probe.log`). Single with trailing semicolon accepted.
- In-tree wire suites on this tree: `extended_batch` 7/7 (pipelined singles
  exactly-once incl. coalesced write, multi-Parse 42601 + Sync recovery,
  byte-fragmented delivery exactly-once, in-pipeline error + txn abort +
  ROLLBACK, mid-frame disconnect safety, simple multi-batch commit);
  `unit_protocol_codec` 19/19 (coalesced order, 1-byte fragmentation incl.
  header splits, oversized/short lengths, mid-header EOF clean, mid-payload
  EOF error-never-partial).
- Live extras: 50× repeated `SELECT 1` no dup/drop/desync; error recovery
  (`no_such_table` → next `SELECT 42` ok); txn abort + ROLLBACK leaves zero
  rows (followup corrected an initial probe artifact: stray `BEGIN` after
  `commit()` in txn-mode had double-opened; clean rerun → `[]`);
  mid-txn disconnect leaves no uncommitted row (`frag_coalesce.log`).
- Raw-socket byte-fragmentation live: NOT RUN (SCRAM handshake blocks a
  hand-rolled socket; no regression possible without auth). Covered by the
  codec tests above + live pipelined/repeat evidence. Reproducible hook:
  extend `extended_batch.rs` (already has fragmented-delivery test) — done.

## P0-002 — floating-point constant handling: PASS (genuinely fixed)

- Old reproducer `SELECT 3.0 / 2.0` → `(1.5,)` OID 701 float8
  (`matrix.log`); pre-fix gave `(1,)` OID 20. `pg_typeof(1.0)` → `float8`
  (was `int4`); `pg_typeof(1)` stays `int4`.
- Typed matrix (all OID-checked): `1.0/2.0→0.5`, `7.5/2.0→3.75`,
  `10.0/4.0→2.5`, `1.5+1→2.5`, `2.5*2→5.0`, `0.1+0.2→0.30000000000000004`,
  `(1.0+2.0)*3.0→9.0`, `10.0%3.0→1.0`, `-3.0/2.0→-1.5`, `1e2/4.0→25.0`,
  `1000000.0/3.0→333333.3333333333`; int contract intact (`7/2→3`,
  `1/3→0`, OID 20); `1.0/0.0` errors division-by-zero (no panic);
  NULL propagates (`NULL/2.0`, `1.0/NULL` → NULL); text/text division fails
  closed (`operator requires numeric operands`); `INSERT 3.0` into INT
  truncates to 3 (DML coercion preserved); aggregates/predicates/UPDATE all
  correct (`SUM 7.0`, `AVG 7.625` as Decimal, predicate + update paths).
- Folded-vs-runtime: literal `3.0/2.0` = 1.5; explicit-cast params
  (`%s::float8/%s::float8`, `CAST(%s AS DOUBLE PRECISION)/2`) = 1.5.
  **Restriction:** untyped `%s/%s` with float params returns `'1'` TEXT
  (params arrive text-typed, no float inference) — needs explicit casts.
  Documented as REAUDIT-002, not a P0 regression (literals, the P0 scope,
  are correct; PG would infer types server-side — we don't).
- In-tree suites: network `numeric_arithmetic` 5/5, executor
  `numeric_arithmetic` 7/7, parser 37/37 — all PASS on this tree.
