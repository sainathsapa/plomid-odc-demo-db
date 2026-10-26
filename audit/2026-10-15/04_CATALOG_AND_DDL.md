# 04 — Catalog and DDL

Two simultaneous real connections (A owner, B pre-existing), release binary.
Raw: `artifacts/probes/catalog_live.log`.

- CREATE TABLE on A → B `SELECT` sees rows without reconnect: PASS
  (`[(1,'hello')]`). B write → A `COUNT(*)` = 2: PASS.
- Prepared `SELECT … WHERE id=%s` on B after A-side DDL: PASS.
- DROP on A → B gets `table does not exist` (no stale false-success): PASS.
- CREATE INDEX on A → B writes/reads via index: PASS.
- Restart: `rst`/`idxvis` present, dropped `catvis` still gone: PASS
  (`server-restart.log`).
- In-tree `catalog_visibility` 6/6 PASS (create/drop/grant visible
  same-session, sequential cross-session distinct OIDs, txn pin preserved,
  search_path preserved).
- Isolation contract: open explicit transactions keep pinned image
  (in-tree test); autocommit + clean image reloads on generation change.
- Residual (from remediation report, NOT retested live — deliberately not
  run as it risks id-keyed directory collision): true-interleaved concurrent
  DDL may mint colliding table IDs → needs a DDL serialization lane.
  → PLOMID-REAUDIT-001 (P1). Sequential cross-session DDL verified correct.
