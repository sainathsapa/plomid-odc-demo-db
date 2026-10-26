# 05 — Durability, recovery, vector, backup

Commit contract (verified live + code): WAL `append_batch` → fsync (`commit_prepare`,
`engine_impl.rs:384-482`) → pages → `publish_commit` → MVCC install (`:500-540`).
`COMMIT` ack follows group-commit durability (`concurrent.rs:1043-1151`). What can
be lost: anything not yet acked; acked commits survived `kill -9` in every run.

## Crash matrix (all on disposable `/tmp/plomid-audit-live/*`)

| Case | Result | Evidence |
|---|---|---|
| Graceful SIGTERM → restart | all rows + vectors + k-NN identical; shutdown checkpoint `lsn=22161 … reclaimed_segments=0` | `probe6`, server log |
| `kill -9` with open uncommitted txn | committed row survives; uncommitted absent; WAL replay `applied_records=7 applied_transactions=1` | `probe7_crash.log` |
| Restart with vector table + derived ANN cache | count + top-k identical | `probe6_post_restart.log` |
| Torn WAL / bad checksum / missing files / disk-full / permission-denied / kill-points around commit/index-build/checkpoint | **NOT RUN** (unsafe on dev host; no kill-point harness found) | — |
| Backup consistency during writes / restore drill | **NOT RUN — capability not wired (P1-004)** | — |

## Vector-specific verdict: CONVERGED, with measured exact-search correctness

- Authority doctrine holds live: row vectors ride txn MVCC/WAL; `vector_ann/`
  files are watermark-gated derived cache, rebuilt from snapshot rows, safe to
  delete (`vector_ann.rs:46-65,370-408`); engine backup excludes the cache
  (`txn/src/backup.rs:30-47,161-200`).
- Legacy `vector/src/{storage,mvcc,backup}`: headers declare non-authoritative;
  zero production callers (grep). TEST_ONLY.
- Measured: L2/cosine/IP distances exact; ties return all matching ids;
  zero-vector + duplicate-vector k-NN correct; insert-rollback invisible;
  EXPLAIN shows `Vector Exact scan … Backend: cpu(neon)` with exact fallback.
- NOT measured: ANN recall@k vs exact at scale (ANN path not forced in probes;
  `try_ann_order` gates rows>4096 — below test sizes, exact served, which is the
  safe default); delete/update-heavy vector churn + rebuild cost; 1M-row vector.

## Backup/restore: P1-004 (missing capability, not just missing proof)

`txn/src/backup.rs:206-280` (manifest-last cold backup, refuses bad versions)
exists but has **zero callers** in `server/`, `network/`, `executor/src/`
(only `executor/tests/backup_restore.rs`). No SQL (`BACKUP`, `CHECKPOINT` is a
syntax error), no CLI trigger, no schedule, no restore drill over the wire.
Beta 2 therefore has **no operator backup story**. Remediation: wire to an admin
statement/endpoint with a restore-to-clean-dir drill, or explicitly scope out
with a tested filesystem-copy runbook (crash-consistent via shutdown checkpoint).

## Maintenance

Auto-checkpoint policy (64 MiB / 16 segs / 15 min) on committing thread + shutdown
checkpoint observed working; `VACUUM`/`ANALYZE` accepted (effect not measured);
compaction/GC pressure behavior, worker failure/retry, duplicate jobs: NOT RUN.
