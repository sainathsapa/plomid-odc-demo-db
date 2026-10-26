# 03 — SQL semantics and correctness

Live probes: `artifacts/probe_current.py` (+`.log`, 13/15 — the 2 FAILs are
the P0-003 bypass, recorded as findings not regressions),
`artifacts/sem.py`, psql spot checks. Oracles: independent Python
computations, never the engine's own logic.

## Verified correct (live, both protocols unless noted)

- P0-002: `3.0/2.0=1.5`, `5.0/2=2.5`, `1/2=0`; debug + release binaries.
  In-tree regression tests: `sql/tests/unit_parser_expressions.rs:380-419`,
  `executor/tests/numeric_arithmetic.rs:91-241`,
  `network/tests/numeric_arithmetic.rs` (simple + extended + bind params).
- P0-001: extended `BEGIN;CREATE;INSERT;COMMIT` batch → `(2,30)`.
- Joins with NULLs/duplicates: `COUNT(*)=2` over `{(1,10),(NULL,20),(1,30)}⋈{(1,100),(NULL,200)}` — matches oracle.
- `DELETE USING` deletes exactly the joined rows (verified as data state).
- `GROUP BY/HAVING`, `COUNT(DISTINCT)` with NULLs (=1), `ORDER BY…NULLS
  LAST…LIMIT/OFFSET`, 3VL (`NULL OR FALSE → NULL` correct), int/float
  aggregates, `UPDATE…RETURNING` shape.
- 100K-row `COUNT/SUM/filter/DISTINCT/group-by` all exact vs Python oracle.

## Incorrect (findings)

- **ILIKE is case-sensitive** (`'ABC' ILIKE 'abc'` → false). Silent
  PG-incompatibility → P2-005 retained (workaround: `LOWER()` both sides).
- **DROP DATABASE protocol asymmetry**: works over extended protocol,
  `unsupported DROP object` over simple protocol → P2 (new).
- Wrong SQLSTATEs: DB exists/not-exists use `SerializationFailure`
  (want `42P04`/`3D000`) → P3.

## Unsupported, fail-closed (verified errors, acceptable for beta if documented)

FK declaration (syntax error), `FLOAT` type name, `CHECKPOINT` statement,
`CREATE TABLE AS` (syntax error), `DROP ROLE`, `CREATE ROLE IF NOT EXISTS`,
`SHOW ALL` (one empty row stub), `SERIALIZABLE` (refused), SQL-level
`PREPARE` (no parser entry; wire Parse/Bind is the prepared path).

## Probe artifacts initially misread (corrected, documented so the record is honest)

- `vector <=> vs l2_distance` "disagreement": `<=>` is **cosine** by design
  (`expressions.rs:321`); against a cosine oracle it matches exactly.
- `SELECT COUNT(*) (0)` over the join: my batch probe fetched the leading
  `DROP TABLE`'s empty result (psycopg exposes the first result set), then a
  later `DELETE USING` probe legitimately removed the joined rows. Clean
  re-run: join count = 2. No engine defect.
- `NULL OR FALSE → NULL` is correct 3VL; my first expectation was wrong.

## Gaps

Collation beyond bytewise, `NUMERIC` edge semantics, timezones, subquery
flattening edge cases, and window-function depth were not differentially
tested → EVIDENCE GAPs listed in §13 (P2/P3 by exposure).
