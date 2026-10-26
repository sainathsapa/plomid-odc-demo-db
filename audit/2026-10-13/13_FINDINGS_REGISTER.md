# 13 — Findings register

Severity: P0 release-stopping · P1 major · P2 important · P3 cleanup.
`EVIDENCE GAP` = missing proof that blocks a claim even without a proven bug.

## P0

### PLOMID-AUDIT-001 — extended-protocol multi-statement truncation (silent)
- Severity P0 · confidence high · reproducibility deterministic (3/3 + psycopg/psql contrast).
- Impact: silent write loss + wedged sessions for all driver clients batching statements.
- Reproduce: psycopg `cur.execute("BEGIN; INSERT INTO t VALUES (1); COMMIT;")` →
  status `BEGIN`, row absent, txn left open (`artifacts/probe17_txn_granular.log`).
- Source: `crates/network/src/server/handler.rs:1706` (first-word classify),
  `:1835-1839` (whole-string route), `:2575-2650` (BEGIN arm drops rest).
  Contrast: simple-protocol buffered path (`:2115-2133`) and in-txn split
  (`:2049-2065`) handle batches correctly.
- Release impact: blocks ALL modes. Remediation: split extended batches or
  fail closed (`cannot insert multiple commands into a prepared statement`);
  add psycopg batch regression test to `test-ci`.

### PLOMID-AUDIT-002 — integral float literals folded to int; division corrupt
- Severity P0 · confidence high · deterministic.
- Impact: `SELECT 3.0/2.0` → `(1)` (silent wrong answer); non-integral float
  division → wire data clients cannot parse (`ValueError … '0.75'`).
- Reproduce: `SELECT 3.0/2.0;`, `SELECT 1.5/2;`, `SELECT pg_typeof(1.0); → int4`
  (`artifacts/probe11_float.log`, `probe12b_floatdiv.log`).
- Source: `crates/sql/src/parser/expressions.rs:1940-1949` (fold `N.0` to
  Int4/Int8); `crates/types/src/ops.rs:244-282` computes Float8 correctly so the
  defect is typing/metadata, not arithmetic.
- Release impact: blocks ALL modes (any ratio/percentage analytics). Remediation:
  type decimal literals numeric/float per PG; fix `/` result typing; add wire-level
  float matrix test.

## P1

### PLOMID-AUDIT-003 — DDL/catalog invisible to live sessions until reconnect
- Severity P1 · confidence high · deterministic (`artifacts/probe9_catalog_visibility.log`).
- New tables/grants → `UndefinedTable` on pre-existing conns (superuser + auditor);
  fresh conns fine. Breaks pools/long-lived apps; REVOKE latency untested (assume latent).
- Release impact: blocks multi-user remote; bounds single-tenant remote with a
  reconnect-after-DDL rule. Remediation: catalog version + refresh on use.

### PLOMID-AUDIT-004 — backup/restore not wired to the live server
- Severity P1 (capability gap) · confidence high.
- `txn/src/backup.rs:206-280` has zero callers in server/network/executor/src
  (only `executor/tests/backup_restore.rs`); no SQL/admin/CLI trigger.
- Release impact: no operator backup story for any remote mode. Remediation:
  wire + restore drill, or explicit scope-out with tested copy runbook.

## P2

### PLOMID-AUDIT-005 — ILIKE is case-sensitive (silently PG-incompatible)
- Documented as `ILIKE→LIKE` but returns wrong answers (`'ABC' ILIKE 'a%'` →
  False). Implement or reject. (`probe10`.)

### PLOMID-AUDIT-006 — foreign keys cannot be declared
- Inline `REFERENCES` → `SyntaxError`; docs "unenforced" understates absence.
  Keep app-level-check pattern; do not claim FK. (`probe10`.)

### PLOMID-AUDIT-007 — no manual checkpoint; `SHOW ALL` empty stub
- `CHECKPOINT` is a syntax error (auto/shutdown only); `SHOW ALL` returns one
  empty row. Operator visibility gaps. (`probe13`.)

## P3

### PLOMID-AUDIT-008 — `FLOAT` type name rejected; legacy txn/delete-candidate code
- `1::FLOAT` errors (use `DOUBLE PRECISION`); unreferenced `txn::Transaction` /
  `hot_row` paths are dead production surface — delete or gate.

## Evidence gaps (blocking the corresponding claims)

- G-001 full workspace suite NOT VERIFIED (compiling at report time).
- G-002 clippy-ci NOT RUN (build-lock contention + budget).
- G-003 gauntlet qualification NOT RUN here (build wait >120 s).
- G-004 1M+ rows, p99+ tails, HTAP-under-load SLOs: NOT RUN.
- G-005 disk-full/OOM/pressure/corruption/kill-point matrix: NOT RUN (unsafe host).
- G-006 x86_64/Linux/Windows runtime, GPU, hw parity beyond NEON: NOT RUN here.
- G-007 backup/restore drill, upgrade/rollback, cross-path restore: NOT RUN.
- G-008 ANN recall@k at scale; vector churn/rebuild cost: NOT RUN.
- G-009 formal isolation proof; 64-way contention; pooled-catalog soak: NOT RUN.
