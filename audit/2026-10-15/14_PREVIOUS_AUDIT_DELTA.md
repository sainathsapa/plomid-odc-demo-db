# 14 — Previous-audit delta (2026-10-13 → this audit)

Baseline verdict was NO-GO everywhere. Dispositions (current evidence
supersedes old logs cited):

- P0-001 extended-protocol truncation → **FIXED_AND_REVALIDATED** (§02;
  old `probe16/17` + `p0_prefix_repro` superseded by `ext_parse_probe.log`,
  `frag_coalesce.log`, 7/7 + 19/19 suites).
- P0-002 float folding/division → **FIXED_AND_REVALIDATED** (§02; old
  `probe11/12b`, `p0_002_oids` superseded by `matrix.log`, 5/5 + 7/7 + 37/37).
- P1-003 catalog staleness → **FIXED_AND_REVALIDATED** (§04; old `probe9`
  superseded by `catalog_live.log` + 6/6 suite + restart check).
- P1-004 unwired backup → **FIXED_AND_REVALIDATED** (§05; cold CLI roundtrip
  23 files + live refusals; old gap closed).
- Interim P0-003 DATABASE DDL authz (10-14, new) → **FIXED_AND_REVALIDATED**
  on this tree (`rbac_live.log` denials + 9/9 suite incl. SQLSTATE test).
- Interim ILIKE case-sensitivity (10-14 P2) → fix present in dirty tree
  (plumbing inspected in diff); live ILIKE matrix NOT RUN → **FIXED_BUT_NOT_REVALIDATED**.
- Old NOT-RUN items now run: release binary rebuilt + re-measured through
  100K; kill -9 crash; backup/restore; TLS-required/plaintext/wrong-pw;
  unique race; catalog two-session. Still NOT RUN: full workspace suite
  (halted per instruction), >100K/1M+, disk-full/OOM, kill-point chaos,
  x86/Linux runtime, Gauntlet/benches, ANN recall curve, hot backup (by design).
