# 09 — Concurrency and failure tests

Safe deterministic interleavings on disposable dirs. Raw: `workloads.log`,
`frag_coalesce.log`, `crash.log`, `rbac_live.log`.

- Concurrent unique inserts (8-way same key): exactly-once, 1 winner — PASS.
- Reader/writer visibility + ROLLBACK: uncommitted invisible, rollback drops
  — PASS (scalar + vector).
- Catalog DDL vs active sessions: PASS sequential (§04); true-interleaved
  concurrent DDL: NOT RUN (collision risk documented — REAUDIT-001).
- Index maintenance vs writes: covered implicitly by race test (unique index
  enforced under concurrency); dedicated build-during-write: NOT RUN.
- Checkpoint/maintenance vs workload: NOT RUN.
- Connection drop mid-transaction: no uncommitted residue — PASS.
- Restart clean + `kill -9` crash: committed kept / uncommitted dropped,
  vectors intact — PASS.
- Malformed frames: oversized/short lengths rejected, mid-header EOF clean,
  mid-payload EOF error-never-partial (in-tree codec 19/19); live raw-socket
  malformed NOT RUN (SCRAM). Resource exhaustion (disk-full/WAL-full/OOM,
  connection ceiling, torn-WAL): NOT RUN — unsafe on this host. Repro hooks:
  start server on a small-RAM disk image dir and fill WAL (disk-full), set
  `ulimit -v` + bulk load (OOM), `kill -9` mid-checkpoint + missing-file
  restarts (torn-WAL). Missing product hooks: no fault-injection flags for
  torn-WAL/partial-checkpoint — evidence gap, not a code change request.
