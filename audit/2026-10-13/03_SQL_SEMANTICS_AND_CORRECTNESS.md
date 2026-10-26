# 03 — SQL semantics and correctness

Claimed subset source: `docs/v0.1.0/sql/{overview,compatibility}.mdx` +
`migration/postgres-users.mdx`. Every row below was executed live over PGWire
(`artifacts/probe*.log`); "PG" = PostgreSQL 14 behavior.

## Verified correct (live)

Types/casts: `INT` PK, `TEXT`, `DOUBLE PRECISION` columns + `CAST(x AS DOUBLE
PRECISION)` + `'1.5'::DOUBLE PRECISION` + `pg_typeof(1.5)=float8`; date cast.
Predicates/NULL: three-valued logic correct (`NULL AND TRUE→NULL`),
`LIKE`, `NULLS FIRST`, `LIMIT/OFFSET`, `DISTINCT`, `COUNT(DISTINCT)`,
`GROUP BY/HAVING`, correlated `EXISTS`, `UPDATE…FROM…RETURNING`,
`CASE`-over-aggregate, window `ROW_NUMBER()`, prepared `PREPARE/EXECUTE $1`,
`ON CONFLICT DO NOTHING`, `CHECK` + `NOT NULL` enforcement (violations raise
`CheckViolation`/`NotNullViolation`), joins with duplicates/NULLs.
Equivalence: index vs scan counts match; exact vector k-NN == post-restart k-NN;
fresh vs long-lived connection same results (except catalog staleness P1-003).

## Bugs found (live, with reproducers)

- **P0-002 float literals/division** (`probe11_float.log`, `probe12b_floatdiv.log`):
  `SELECT 3.0/2.0;` → `(1)` (expect `1.5`); `SELECT 1.5/2;` → client
  `ValueError: invalid literal for int() … '0.75'` (wire corruption);
  `SELECT pg_typeof(1.0);` → `int4` (expect numeric/float).
  Root: `crates/sql/src/parser/expressions.rs:1940-1949` folds integral-valued
  decimals to `Int4/Int8`; `/` typing then mismatches (`types/src/ops.rs:244-282`
  computes Float8 but column presents int). Minimal repro:
  `CREATE TABLE f_t(x DOUBLE PRECISION); SELECT 3.0/2.0;` — wrong on any data.
- **P2-005 ILIKE case-sensitive** (`probe10`): `'ABC' ILIKE 'a%'` → `False`
  (PG: true). Docs admit `ILIKE→LIKE`; silent wrong results — should either
  implement or reject. `ILIKE` path: executor predicate eval.
- **P2-006 FK absent**: inline `REFERENCES` → `SyntaxError` (parser has no FK
  support at all; docs say "unenforced" which understates — cannot be declared).
  Workaround: application-level checks (documented pattern, untested here).

## Unsupported (correctly rejected — not failures)

`CHECKPOINT` (syntax error — auto/shutdown only), `information_schema.table_privileges`
(missing), `1::FLOAT` type name (use `DOUBLE PRECISION`/`REAL`), `SERIALIZABLE`
(rejected), `CREATE VECTOR INDEX` in explicit txn (rejected by design),
`VACUUM/ANALYZE/CLUSTER/CREATE DATABASE` in-txn (rejected, PG-parity).

## Gaps / NOT VERIFIED

- Differential testing vs PG beyond hand probes: no harness run (property tests
  exist in-repo for vector math; SQL-level differential over PGWire not run).
- `SHOW ALL` returns a single empty row (observability stub, P3).
- Timezone/collation/decimal-precision matrices not exercised.
- Row-vs-columnar result equivalence at scale: spot-checked only (counts/group-by
  match at 100K; no full row-diff harness).
