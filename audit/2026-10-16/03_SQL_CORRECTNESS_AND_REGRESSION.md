# 03 — SQL correctness and regression

Evidence: `artifacts/probes/logs/` — `workload_probe.log`,
`param_avg_ilike_probe.log` (regenerated with the fixed script),
`vector_commit_probe.log`, `txn_mvcc_probe.log`, `qualify.log`.

## Probe suites

- `workload_probe` **11/11** — OLTP/OLAP/HTAP families with deterministic
  oracles; `DISTINCT` with NULL returns `[1, 2, 3, None]`; HTAP top-k
  returns 10 with the relational side intact
  (`[('A',250),…]`); note: insert=238 ms, point×200=155 ms.
- `vector_commit_probe` **77/77** — vector DDL/DML/execution checks; see
  `06` for the vector-relevant subset.
- `txn_mvcc_probe` **7/7** — isolation/MVCC semantics; see `09`.
- qualify gate **83 passed / 0 failed** (`artifacts/qualify/qualify.log`).

## G-09 — ILIKE live matrix: PASS (docs stale)

Regenerated run: case folding (`%C`, `'ABC'`), wildcards (`_bc`, `a_c`,
`%`), NULL handling, NOT ILIKE and a LIKE contrast case — **10/10 PASS**.
This closes G-09 for the matrix itself; the doc claims that ILIKE is
case-sensitive (`docs/v0.1.0/reference/compatibility.mdx`,
`support-matrix.mdx`, `beta-status.mdx`) are stale (F-011).

## G-09b — WHERE-clause `ESCAPE` ignored (finding F-005)

With a PostgreSQL oracle on the same fixture:

- `WHERE v LIKE 'a!_c' ESCAPE '!'` → expected `[5]`, got `[]` — **FAIL**
- `WHERE v LIKE 'a!%c' ESCAPE '!'` (literal `%`) → expected `[6]`, got `[]` — **FAIL**
- JOIN-condition `ESCAPE '!'` → `[5]` — NOTE (honored on the join path)

Code root cause located in review (not modified): the WHERE-clause
evaluator drops the `ESCAPE` argument before calling the matcher
(`crates/executor/src/query/expr.rs`), while the JOIN path passes it
through (`crates/executor/src/join/eval.rs:346`). Impact: silent wrong
(mostly empty) result sets for patterns that rely on an escape character.
Confidence: high (live + code). Severity: P2.

## REAUDIT-002 (unchanged, documented limit)

Untyped `%s/%s` with `(3.0, 2.0)` → `'1'` (text substitution). Escape
hatches verified live: `::float8` → `1.5`, `CAST(%s AS DOUBLE PRECISION)/2`
→ `1.5`, `%s + 1` with `(2,)` → `'3'`; explicit-cast params `1.5`. This
matches the documented hard limit; no change.

## REAUDIT-003 — closed

- `AVG(v)` = `7.625`, `pg_typeof` = **float8** (matches float8 siblings);
  `AVG(int)` renders `Decimal('7.5')` (PostgreSQL returns numeric for
  integer input — parity).
- `DROP ROLE` is implemented: leased role dropped; `IF EXISTS` accepted;
  guard refusals: `role "plomid" cannot be dropped`,
  `role "owned_role" cannot be dropped because privileges depend on it`.
- NOTE (not a finding): `SUM/COUNT` evaluated in a bare expression context
  returns `function evaluation requires query context` — aggregate
  evaluation stays query-scoped.

## Other notes

- `GRANT … ON ALL TABLES` syntax gap (probe note): `expected object name,
  found ALL` — table-level grants only (F-012 note).
- WHERE-vs-JOIN ESCAPE asymmetry and ILIKE docs drift are the only SQL
  correctness deltas found this cycle.
