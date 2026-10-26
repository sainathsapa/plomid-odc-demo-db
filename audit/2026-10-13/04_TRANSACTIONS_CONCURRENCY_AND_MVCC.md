# 04 — Transactions, concurrency, MVCC

Actual contract (code + live): **single-writer MVCC, true statement-time staging
into a per-txn write overlay** (`txn/src/concurrent.rs:479,759-769`;
`executor/src/session_txn.rs:173-247,289-358`; `network/handler.rs:2621-2625`
"no re-execution"). Isolation: `READ COMMITTED` (per-statement snapshot refresh)
and `REPEATABLE READ` (BEGIN-pinned snapshot); `SERIALIZABLE` rejected;
`READ UNCOMMITTED` maps to RC. Statement error aborts the whole session txn
(PG parity; `InFailedTransaction` until ROLLBACK). Savepoints true
(overlay+catalog rewind). DDL transactional except VACUUM/ANALYZE/CLUSTER/
CREATE DATABASE/vector-index/sequences (documented carve-outs).

## Schedules run (live, `probe2_txn.log`, `probe5_concurrent.log`)

| Schedule | Result |
|---|---|
| autocommit insert/select/update/delete + RETURNING | correct counts + payloads |
| explicit BEGIN→INSERT→COMMIT (separate statements) | committed, visible, counted |
| BEGIN→INSERT→ROLLBACK | fully absent afterwards |
| mid-txn read-your-writes (`SELECT` sees staged row) | correct (4 vs 3) |
| savepoint: insert, sp, insert, rollback-to-sp, commit | exactly `[(20,)]` |
| unique violation in-txn → ROLLBACK | state intact |
| post-error SELECT in same txn | blocked `InFailedTransaction` (PG parity) |
| 8 threads × 50 autocommit inserts | 400/400 committed, count 10400 exact |
| 8-way unique-key race (same key) | exactly 1 winner, 7 `UniqueViolation` |
| single-string `BEGIN;…;COMMIT` via driver | **BROKEN — P0-001** (only BEGIN runs; writes lost; session stuck open) |
| single-string via psql (simple protocol) | correct (buffered path) |

No dirty reads, no lost updates, no duplicate uniques observed in any run.
Finite runs only — no formal proof claimed.

## P0-001 detail (release-stopping)

Extended-protocol `Parse("BEGIN; INSERT …; COMMIT;")` → `Execute` → `handle_query`
classifies by first word (`statement_type(sql)`, `handler.rs:1706`) → routes whole
string to `handle_session_transaction` (`:1835-1839`) → BEGIN arm (`:2584-2649`)
opens the txn and **returns, discarding the rest**. No error. Consequences
measured: `probe17` — `SELECT *` empty after "successful" batch; `probe16` —
session stuck `InTransaction`, later statements execute in wrong txn context,
cascade into `InFailedSqlTransaction` (this also explains every anomaly in
`probe1`). Any driver batching statements (migrations, ORMs, `executemany`-style
flows) silently loses writes. PG raises `cannot insert multiple commands into a
prepared statement`. Remediation: split extended batches like the in-txn path
(`:2049-2065`) or fail closed with that error. Regression test: psycopg
single-`execute` batch asserting COMMIT status + row presence.

## P1-003 (catalog staleness — also concurrency/pooling impact)

`probe9_catalog_visibility.log`: table created+granted on conn A is
`UndefinedTable` on pre-existing conns B (superuser) and C (auditor); visible
only on fresh conns. Same root delays GRANT visibility. Long-lived / pooled
clients will error after every DDL until reconnect. Fix: catalog version +
refresh/reload on use (or documented reconnect rule + version error).
