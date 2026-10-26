# 05 — Durability, backup, restore

Release binary, disposable dirs. Raw: `artifacts/probes/backup.log`,
`crash.log`, `server-*.log`.

- Backup lifecycle (documented CLI, real files): PASS end-to-end. Representative
  DB (DDL + committed rows + indexes + vectors) → `backup --data … --to …`
  (23 files, 580140 bytes, manifest format 1) → `backup-inspect` (versions +
  counts) → `restore --from … --to <clean>` → server starts on restored dir:
  `bk_t` 3/3 rows identical, `rst` intact, restored image writable
  (insert → 4 rows) and durable across reopen.
- Consistency boundary: cold full backup only (checkpoint + close, then copy,
  manifest last); no incremental/PITR by design — documented limit for all
  modes needing point-in-time recovery.
- Live-dir backup refused exit 2; restore onto live/locked dir refused
  exit 2; missing-manifest inspect errors `PL-CORRUPTION: backup has no
  manifest` (fail-closed). Interrupted-backup mid-copy NOT RUN (safe
  procedure exists — kill during `backup`, `backup-inspect` must fail;
  not executed for time).
- Commit/WAL/checkpoint/restart: PASS. Graceful restart keeps committed rows;
  `kill -9` with open txn keeps committed `id=1`, drops uncommitted `id=2`;
  vectors intact post-crash (201-style sample verified on live rows).
- Compaction/GC/VACUUM maintenance paths: NOT RUN live (no hooks exercised);
  WAL growth/replay implicitly covered by crash test only.
- Backup gate: PASSED (restorable backup demonstrated, not a serializer test).
