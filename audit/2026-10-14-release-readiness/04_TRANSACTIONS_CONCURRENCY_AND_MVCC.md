# 04 — Transactions, concurrency and MVCC

Actual contract (code-traced, file:line in static evidence): **ReadCommitted
(default, fresh snapshot per statement) + RepeatableRead (BEGIN-pinned)**.
`SERIALIZABLE` refused, never downgraded (`handler.rs:2658-2681`);
`READ UNCOMMITTED` maps to RC. Snapshot isolation semantics
(`allocator.rs:360-365`, `mvcc/version.rs:93-110`): own writes visible,
uncommitted/invisible never visible. Lost-update guard via write lanes +
post-acquire re-read (`concurrent.rs:546-748`); cross-table multi-lane
writes fail fast with `Conflict` (aborts txn, no deadlock). Commit:
`commit_prepare` (reserve ts, WAL batch append) → `commit_publish` (install
MVCC, group barrier after publish); barrier failure post-publish is
UNKNOWN-outcome, never rolled back. **Statement error aborts the whole
explicit txn** (no PG-style continue-to-savepoint; `session_txn.rs:241-247`);
`idle_in_transaction_session_timeout` aborts + FATAL `25P03` + closes.
Uncommitted/aborted rows unreachable via row, index, columnar, and vector
paths (columnar/vector excluded from in-txn SELECT; autocommit columnar is
freshness-gated with scalar fallback).

## Live evidence (this audit)

- `BEGIN;INSERT;ROLLBACK` → row absent; committed rows survive restart.
- 8-way concurrent same-PK insert → exactly 1 row, 1 winner (twice).
- `kill -9` with open uncommitted txn → committed kept, uncommitted absent.
- Catalog/DDL visible to live sessions without reconnect (P1-003 fixed).
- In-txn `CREATE VECTOR INDEX` refused (ANN files can't roll back) — sound.

## Limits / gaps

- No serializable workloads; apps needing it must enforce externally.
- Abort-on-any-error is stricter than PG — document for ORM users.
- Barrier-failure UNKNOWN outcome needs an operator runbook entry (unwritten).
- Multi-client histories/oracle beyond the race probe NOT RUN (bounded);
  in-tree `mvcc_adversarial`/`par_fuzz_*` suites cited (§11).
