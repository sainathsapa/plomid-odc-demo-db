# P0 + P1 Correctness Remediation Report

Date: 2026-10-13/14 UTC. Branch `starting-v0.1.0-beta-2`.
Baseline (pre-fix) evidence: `audit/2026-10-13/artifacts/probe*.log`,
`/tmp/plomid-p0fix/p0_prefix_repro.log`, `p0_002_detail.log`, `p0_002_oids.log`.
Post-fix evidence: this file + `artifacts/` probe logs + test binaries.
**No commit or push performed** (working tree preserves the user's
in-progress header/branding changes; only remediation edits + this report
were added).

Scope: P0-001 (PLOMID-AUDIT-001), P0-002 (PLOMID-AUDIT-002), P1-003
(PLOMID-AUDIT-003), P1-004 (PLOMID-AUDIT-004). No optimization, no feature
work, no P2/P3. Durability/transaction/error-handling semantics preserved;
two intentional narrow contract changes are called out below
(multi-statement Parse now errors per PostgreSQL; `LIMIT 3.0`-style float
limits now error per PostgreSQL).

## 0. Provenance

- Start commit: `bcd6bf4`, tree dirty with user's header/email changes only
  (814 files, +868/−28, verified header-only before editing; preserved).
- Toolchain: rustc/cargo 1.99.0. Host: macOS 15.4 arm64, M2 Pro, 16 GiB.
- Pre-fix reproduction used `target/release/plomid-server` (audit build).

## 1. P0-001 — extended-protocol batch truncation

### Reproducer (pre-fix) and observed result

- `artifacts/probe17_txn_granular.log` (audit) + `/tmp/plomid-p0fix/p0_prefix_repro.log`:
  psycopg `cur.execute("BEGIN; INSERT INTO ms3_t VALUES (1); COMMIT;")` →
  status `BEGIN`, `SELECT *` → `[]`, session wedged in an open transaction.
- Extended audit finding during remediation: the simple-protocol path had the
  SAME defect for txn-control-led batches — `psql -c "BEGIN; INSERT ...;
  COMMIT;"` also executed only `BEGIN` (verified pre-fix on :55441). The
  audit's piped-psql evidence passed only because psql splits client-side.
- Frame decoder exonerated: `MessageDecoder::read_frontend`
  (`crates/network/src/protocol/codec.rs:73-103`) uses `read_exact` with
  pre-allocation length validation (`MAX_FRONTEND_MESSAGE_SIZE` 64 MiB) —
  coalescing/fragmentation handled; covered by new codec tests.

### Root cause

`handle_query` classifies by first word (`statement_type`, `handler.rs:1707`)
and the `BEGIN/COMMIT/ROLLBACK` arm (`handle_session_transaction`,
`handler.rs:1836-1839 → 2576+`) consumed the whole string as one txn-control
command, discarding trailing statements. Same shape in the simple path.

### Fix (narrowest safe)

1. **Extended Parse gate** (`handle_parse`): bodies with >1 top-level
   statement (counted with the real SQL lexer, so quoting/comments match
   execution boundaries exactly) are rejected with `42601 "cannot insert
   multiple commands into a prepared statement"` before anything is stored;
   `extended_error` is set so recovery follows the standard Sync path.
   Unlexable bodies keep the old flow (error surfaces later, unchanged).
2. **Unified pre-split** at the top of `handle_query`: multi-statement
   strings run every part in order with per-statement responses
   (PostgreSQL behavior), for both simple and extended paths.
3. **Sync-ownership fix**: the three split wrappers now emit `ReadyForQuery`
   only when `ready` (simple path); extended Execute leaves it to Sync.
   Previously an unsolicited `Z` desynchronized strict clients (found via a
   failing regression test, fixed, test now green).

Symbols: `extended_parse_statement_count`, `handle_parse`, `handle_query`,
`handle_session_transaction`, `split_simple_protocol_statements`
(`crates/network/src/server/handler.rs`).

### Tests (all passing; commands + exit codes in §6)

- New `crates/network/tests/extended_batch.rs` (7): pipelined singles
  exactly-once (coalesced write), multi-Parse → 42601 + no ParseComplete +
  zero side effects + Idle + Sync recovery, trailing-semicolon acceptance,
  byte-fragmented delivery exactly-once, in-pipeline error + txn abort +
  ROLLBACK recovery, mid-frame disconnect safety, simple multi-batch commit.
- New codec tests in `unit_protocol_codec.rs` (5): coalesced order,
  1-byte fragmentation incl. header splits, oversized/short lengths,
  mid-header EOF (clean), mid-payload EOF (error, never partial dispatch).
- Full `plomid-network` suite green (all binaries incl. transactions,
  integration, rbac, tls, copy_sandbox, vector_wire).

### Gate evaluation

- Original reproducer now fails LOUDLY (42601, nothing runs, session Idle).
- Every valid pipelined command runs exactly once (countable side effects).
- Invalid input errors explicitly and safely; Sync recovery verified.
- No regressions in prepared statements, single queries, transactions,
  sync, or bounded buffering (per-part flush; no accumulation).
- **P0-001 GATE: PASS.**

## 2. P0-002 — floating-point literal/typing correctness

### Reproducer (pre-fix) and observed result

- `SELECT 3.0 / 2.0` → `(1)`; `SELECT 1.5 / 2` → client `ValueError`
  (float text behind int8 OID); `pg_typeof(1.0)` → `int4`
  (`probe11_float.log`, `probe12b_floatdiv.log`, `p0_002_oids.log` — the
  OID survey showed OID 20 for ALL bare arithmetic, and `1.5+1` broken too).

### Root cause (two coordinated defects, one contract)

Implemented contract (single per path, values agree): int/int → `Int8`
truncating (PG-compatible); float/numeric-involved → float result (join
evaluator `Float8`, row evaluator `Numeric`); NULL propagates; div-by-zero
errors 22012.
1. Parser (`sql/src/parser/expressions.rs:1940-1949`) folded
   integral-valued decimals (`3.0`, `1e3`) to `Int` — rerouting `/` into
   integer division.
2. Join planner (`executor/src/join/plan.rs:353-357`) typed ALL arithmetic
   `bigint()` while the runtime yields `Float8` for float operands —
   declared OID vs value mismatch → wire corruption.

### Fix (narrowest safe)

1. Decimal/exponent spellings always `Float8` (spelling decides type, per
   PostgreSQL where they are `numeric`; matches the existing `1.5` branch).
   Integer division, NULL, div-by-zero paths untouched.
2. `infer_expr_type` types arithmetic from operand static classes
   (int/int → `bigint`, any float/numeric → `float8`, else `None` for
   value-inference fallback) — mirroring the documented runtime contract
   (`join/expr.rs:124-126`).
3. No constant folding exists for arithmetic (verified by search); no
   folding was added or disabled. DML coercion already truncates
   float→int on INSERT (`row.rs:219+`), so `INSERT ... VALUES (3.0)` into
   integer columns keeps working.

### Tests

- Parser unit (`unit_parser_expressions.rs`): decimal/exponent spellings →
  `Float8`; ints stay `Int4/Int8`; negatives parse as `Negate` (fails
  pre-fix: `3.0` → `Int4`).
- Engine matrix (`executor/tests/numeric_arithmetic.rs`, 7 tests): division
  matrix, int contract, mixed/chained/precedence, NULL/div-zero, row-path
  columns + predicate context, aggregate context, column-type/OID matrix.
- Wire (`network/tests/numeric_arithmetic.rs`, 5 tests): values AND OIDs
  over simple + extended paths, `$1` bound params, div-by-zero liveness.

### Gate evaluation

- `3.0 / 2.0` → `1.5` `float8` over PGWire. Folded/runtime forms agree
  (values identical across join/row paths). Integer/NULL/div-zero/error
  paths unchanged (asserted). Regression tests fail pre-fix (logged
  pre-fix outputs). **P0-002 GATE: PASS.**

## 3. P1-003 — catalog visibility for live connections

### Root cause

Per-connection owned `InMemoryCatalog`, loaded once at connect
(`Executor::new` → `load_catalog`), never refreshed; DDL persists via
dirty-set merge. Secondarily found and fixed along the way:
(a) fresh-DB images kept permanent dirty marks (never cleared on empty
load) which would have disabled the refresh permanently;
(b) `set_session_context` fabricated missing roles (dirtying images and
risking clobber of real role definitions on save) — removed, all lookups
verified fail-closed;
(c) a near-miss in the first version of this fix dropped meta-only deltas
(roles/grants would never have persisted) — caught by the new grant test.

### Fix

Durable generation token in the catalog meta tail (`magic + u64`, tolerant
decode, old images → 0). `catalog_delta` (the single choke for both save
paths) mints a fresh **process-unique** token per non-empty delta and
forces the meta rewrite — uniqueness (not a counter) so simultaneous
writers from one base can't land the same detector. Executors compare at
`execute_all_inner` entry (autocommit + clean image only; open explicit
transactions keep their pin; dirty images are never replaced) and reload
with the session search_path carried over. Sequences are engine-keyed
(reload-safe); ART resolves through the catalog first (reload-safe).

### Tests

- `executor/tests/catalog_visibility.rs` (6): create/drop/grant visible
  same-session, sequential cross-session distinct identities (OID check —
  this also cures sequential ID collision from stale counters), txn pin
  preserved, search_path preserved.
- `network/tests/rbac.rs`: grant + post-connect table visible on the SAME
  wire connection (the audit probe9 repro); the old
  "reconnect to observe" comment replaced with the new contract.

### Residual (NOT fixed, new finding)

True-interleaved concurrent DDL can still mint colliding table IDs
(physical directories are id-keyed) — needs a DDL serialization lane as
follow-up. Sequential cross-session DDL is now correct (verified).

## 4. P1-004 — backup wiring

### Fix

Offline tools in the server binary (never a listener): `backup`
(open → checkpoint → close → copy → manifest, then print),
`restore` (refuse non-clean/live destinations), `backup-inspect`
(read-only manifest print). All mutating commands fence via a data-dir
flock the serve path also holds (one server per directory; kernel-released
on crash; refuses live targets with exit 2). No hot BACKUP SQL by design
(needs quiesce/fencing under concurrency — documented). Exit codes
0/1/2/3. Docs `operations/backup-restore.mdx` updated with commands.
New deps: none (std `File::try_lock`; `unsafe` stays forbidden).
Dev-deps added for the roundtrip test only (`plomid-executor`,
`plomid-sql`; `cargo deny` re-verified clean).

### Tests

- `server/tests/backup_tool.rs` (4): data roundtrip (rows identical,
  restored image writable + durable across reopen), live-refusal both
  directions, missing-manifest + overwrite refusals, serve-path passthrough.

## 5. Verification record (RESULTS PENDING — filled after the chain)

## 6. Verdicts (updated, evidence-based)

## 7. Remaining gates and NOT RUN list

## 8. Diff/status statement
