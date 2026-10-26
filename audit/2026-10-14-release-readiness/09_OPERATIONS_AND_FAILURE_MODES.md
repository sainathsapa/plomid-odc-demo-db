# 09 — Operations and failure modes

## Verified

- Graceful SIGTERM: drains (5 s grace), flushes, checkpoints, syncs;
  restart clean, data present.
- `kill -9` with open txn: committed kept, uncommitted absent, vectors intact.
- Cold backup → inspect → restore-to-clean → start → verify: all green;
  live-dir backup refused (exit 2).
- Statement/idle-in-txn timeouts enforced in code (defaults 30 s/60 s);
  cancel path present in code; live timeout-firing NOT RUN.
- Data-dir 0700 + single-server advisory lock; copy-dir jail + symlink
  fail-closed (code-traced).
- Checkpoint policy knobs (`--checkpoint-*-*`, `--no-auto-checkpoint`)
  wired from CLI to engine (code-traced); manual `CHECKPOINT` SQL absent.

## Not verified / gaps

- Disk-full / WAL-full / failed-fsync / read-only FS: NOT RUN (unsafe on dev
  box; no fault-injection CLI). Procedure: loopback-mounted small volume or
  `ulimit`/quota-constrained data dir + bulk ingest + kill; record first
  error, restart recovery, operator message quality.
- OOM/near-memory-limit, 100+ connections, slow-consumer permit exhaustion,
  large-WAL restart: NOT RUN (bounded).
- Torn-WAL / partial-checkpoint / missing-file / corrupt-page live drills:
  NOT RUN (unit fail-point coverage cited §11; live matrix is the gap).
- Upgrade/rollback across versions, PITR, log rotation, readiness/liveness
  endpoints beyond TCP healthcheck: absent or NOT RUN.
- `SHOW ALL` stub, barrier-failure UNKNOWN-outcome runbook: unwritten (P3).
