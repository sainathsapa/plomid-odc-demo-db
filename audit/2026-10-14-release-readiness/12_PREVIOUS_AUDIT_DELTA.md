# 12 — Previous-audit delta (`bcd6bf4` → `6a935b4`)

| Prior ID | Before | Now | Evidence |
| -------- | ------ | --- | -------- |
| 001 P0 extended-multistmt truncation | silent first-only | **FIXED**, all parts run both protocols | live `(2,30)`; `handler.rs` split sites; no fix commit msg, verified live not assumed |
| 002 P0 float fold / division | `3.0/2.0→1`, wire poison | **FIXED** | live 1.5 debug+release; fold gone (`expressions.rs:1934-1950`); `/` typing (`join/expr.rs:201-214`); parser+executor+wire regression tests |
| 003 P1 catalog invisible to live sessions | reconnect needed | **FIXED** | live: pre-connected session reads new table; `refresh_catalog_if_stale` |
| 004 P1 backup/restore unwired | no live path | **PARTIALLY FIXED / re-scoped** | cold offline CLI verified end-to-end (backup/inspect/restore/fencing); hot SQL backup absent by design |
| 005 P2 ILIKE case-sensitive | silently wrong | **STILL PRESENT** | live `'ABC' ILIKE 'abc'` → false |
| 006 P2 FK cannot be declared | syntax error | **STILL PRESENT** (fail-closed, acceptable) | live syntax error |
| 007 P2 no manual CHECKPOINT / SHOW ALL stub | — | **STILL PRESENT** | live: CHECKPOINT syntax error; SHOW ALL one empty row |
| 008 P3 FLOAT name / legacy code | rejected | **STILL PRESENT** (fail-closed) | live `type "float" is not supported` |
| Scale evidence | 100K release 43K rows/s | **MIXED**: 100K re-run exact-correct on debug (2.3K rows/s); release throughput not re-run on HEAD | §7 provenance table |
| TLS/RBAC spot claims | verified | **RE-VERIFIED**, plus new P0-003 found | §8 |
| Chaos beyond SIGKILL | gap | **STILL GAP** | §9 NOT RUN list |
| Multi-arch runtime / SLSA | gap | **STILL GAP** | §10 |

Net: two P0s fixed and live-proven, one P1 fixed, one P1 re-scoped with a
verified cold path, one **new P0** (DATABASE DDL authz bypass) replacing the
old blockers for every shared release mode.
