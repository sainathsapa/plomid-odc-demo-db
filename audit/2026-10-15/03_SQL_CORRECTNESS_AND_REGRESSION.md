# 03 — SQL correctness and regression

Live on release binary unless noted. Evidence: `artifacts/probes/workloads.log`.

- SELECT/INSERT/UPDATE/DELETE + batch DML: LIVE_AND_END_TO_END. Multi-row
  INSERT (500–2000/batch), point update, `UPDATE … WHERE`, delete paths all
  correct; bulk 1K/10K/100K roundtrips exact.
- Expression precedence/casts/numerics/comparisons/NULL+3VL:
  LIVE_AND_END_TO_END except REAUDIT-002 (untyped params) and REAUDIT-003
  (`AVG` → Decimal/Numeric vs float8 siblings). `= NULL` → `[]`, `IS NULL`
  correct, chained/mixed float arithmetic correct (§02).
- Transactions/COMMIT/ROLLBACK/UNIQUE + 8-way unique race:
  LIVE_AND_END_TO_END. Single-insert rollback → `[]`; dup-key → error +
  ROLLBACK → `[]`; 8-way same-key race exactly-once (1 row, 1 winner).
- Prepared statements + error recovery: LIVE_AND_END_TO_END (§02).
- Joins/aggregates/GROUP BY/ORDER BY/LIMIT/subqueries: LIVE_AND_END_TO_END.
  Join+GROUP (`a:20.0 b:7.5`), ORDER+LIMIT, IN-subquery, COUNT/SUM/AVG all
  match hand-computed oracles.
- DDL/catalog visibility: LIVE_AND_END_TO_END (see `04`).
- Index-path vs full-scan equivalence: LIVE_WITH_RESTRICTIONS. Indexed
  point read correct and fast (0.14–0.3 ms); no forced plan-diff harness run
  — optimizer equivalence asserted via result equality on point/filter/group
  queries, not via plan inspection. NOT a full index-vs-scan proof.
- JSON/time-series/vector SQL: PARTIALLY_WIRED. Vector SQL verified (§06);
  JSON/time-series exercised only as stored payload in backup set, not as a
  function-matrix — marked NOT_VERIFIED for function depth.
- Randomized/property differential: NOT RUN live (no harness executed this
  session); in-tree property suites exist but were not run (full suite
  halted per operator instruction). Listed as evidence gap, not a pass.
