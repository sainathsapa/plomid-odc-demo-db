# Beta 2 — 06 Transaction Semantics (Phase 4 Evidence)

Plan: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §8
Baseline: F-03 OPEN (`validate/stage → replay full SQL text at COMMIT`)
Status: **GREEN** — true staging implemented on the existing MVCC foundation; text replay retired from every transaction path.

## 1. What changed (and what was already true)

The engine already owned true MVCC transactions (`ConcurrentTransaction`: write overlay with read-your-writes, statement snapshots, domain/row/unique lanes, WAL + MVCC publish). Single-batch `BEGIN;…;COMMIT` text already used them. The gap was exclusively the multi-message wire session: statements were validated-but-not-executed, staged as TEXT, and re-parsed/re-planned/re-executed at COMMIT (TOCTOU, double evaluation, no read-your-writes, COMMIT-time errors, SELECTs running as autocommit).

After Phase 4 the session holds a real open transaction across messages (`begin_owned` — fully owned handle, no self-borrow):

```text
BEGIN → open txn (overlay + BEGIN snapshot + catalog image)
statement → parse/bind → authorize → execute NOW into overlay
read → snapshot (+RC refresh) merged with overlay
COMMIT → validate + WAL/MVCC publish only (no re-execution)
ROLLBACK → drop overlay + restore catalog image
```

Retired: `pending_statements` text staging, trial `validate_dml` + replay, `coalesce_staged_inserts`, extended-protocol text batching, statement-generation dedup, RR facade pins in session flow.

## 2. Requirement → evidence

| Requirement (§8) | Evidence |
|---|---|
| Parse/bind/validate at statement time | `execute_one_in_session_txn` + `execute_statement_in_session_txn` (`session_txn.rs`); errors abort immediately |
| Stage transactional effects | Overlay puts/deletes via `_txn` DML paths; session catalog for DDL |
| Read-your-writes | `txn_select.rs` (overlay-merged scans) + txn-aware DML reads; 18 executor + 11 wire tests |
| Commit validation, WAL + MVCC publish | `commit_session_txn` → `txn.commit()` (prepare/publish, conflict-checked); catalog saved first (crash leaves at most an empty table) |
| No SQL-text re-run | No replay path remains (grep: `replay` only in historical comments) |
| Defaults / generated / volatile / sequences / identity | Evaluated once at statement time; sequences via independent commits (PG gaps); identity re-derives safely |
| Constraints | Statement-time (unique probes see overlay + committed); whole txn aborts on violation |
| RETURNING | At statement time from staged rows (new/old tuple per DML kind, PG parity) |
| Rollback / isolation | Overlay drop + catalog restore; RC refresh / RR pin; SERIALIZABLE still rejected |
| Error timing | Constraint/type/privilege errors at the failing statement; session failed until ROLLBACK |

All 11 required test classes green: conflicting INSERT (executor + wire, block-then-duplicate), UPDATE conflict (no lost update, `v=110`), staged read→write, multi-statement rollback, commit after reads/writes, constraint timing, sequence/identity, generated/defaults, volatile stability, RYW, RC-vs-RR visibility.

## 3. Narrowed guarantees (explicit, not silent)

- Statement errors abort the whole transaction (no error-recovery via `ROLLBACK TO`; savepoints cover user-directed partial rollback of successful statements only).
- RR pins reads but writers re-read at lane acquisition (no serialization failures — weaker than PG RR).
- Single-batch `BEGIN;SELECT…;COMMIT` text keeps the group path (DML + catalog-persist now; SELECT still `Unsupported` there — pre-existing; wire multi-message is the full path).
- In-txn SELECT uses the row pipeline (no columnar/ANN/index accel), materializes before LIMIT, rejects window functions / recursive CTEs / table functions / positional ORDER BY with explicit errors.
- `VACUUM`/`ANALYZE`/`CLUSTER`/`CREATE/DROP DATABASE` rejected in txn blocks (PG parity); file COPY superuser-only; stream COPY follows table privs; privileges/catalog snapshot per connection.
- `INSERT … nextval()` in VALUES fails identically in autocommit and txn (pre-existing limitation, unchanged).

## 4. Hardening notes

- Teardown + idle-in-transaction expiry abort open session transactions (no stranded lanes/manager ids); teardown also removes created-table dirs.
- DDL abort reverts catalog + overlay + created dirs (filesystem/columnar/stats effects of drops defer to commit cleanups).
- `sql_compat.postgresql_compatibility_regressions` fails identically on the clean tree (pre-existing, unrelated — verified via `git stash`).

## 5. Tests (all green 2026-10-08)

- `executor/txn_semantics.rs` (18) + `network/transactions.rs` (11) as above.
- Regression: network (135: incl. updated replay-era tests now asserting statement-time semantics), server, sql, security, core, executor lib (188) + rbac + observability + targeted SQL suites; `fmt`/workspace-`clippy`/`deny` clean.
- Live binary: multi-message txn with pre-commit RYW (`TDDCZ`), `COMMIT`, `kill -9`, restart replays WAL (`applied_records=15, applied_transactions=3`), both rows present.

## 6. Finding disposition

- **F-03** (P1): **CLOSED** — no statement-replay path remains; multi-statement transactions are single MVCC transactions with statement-time effects and publish-only commit.
