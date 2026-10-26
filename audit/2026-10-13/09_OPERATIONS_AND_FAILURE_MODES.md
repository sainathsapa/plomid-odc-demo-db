# 09 — Operations and failure modes

## Verified live

- Graceful SIGTERM: drains, runs shutdown checkpoint (5 stages, 41 ms), exits
  clean (`drained_cleanly=true`).
- Disconnect mid-txn: server best-effort aborts staged txn, releases RR pin
  (`handler.rs:684-693`) — no orphan pin observed (post-crash reopen clean).
- Maintenance: auto-checkpoint policy active; `VACUUM`/`ANALYZE` accepted;
  shutdown checkpoint reclaims (`reclaimed_segments=0` — nothing to reclaim).
- Config validation: bad combos (non-loopback w/o TLS, `--tls-required` w/o
  `--tls`, `--auth none` remote) fail closed with actionable messages.
- Logging: structured events (TLS refusals, RBAC decisions, checkpoints,
  recovery stats); password redacted; log rotation: not configured (operator
  concern, P3).
- Health signals: banner + `network_ready`/`server_ready`; no separate
  readiness/liveness endpoint (P3 gap for orchestrators).

## NOT verified (all EVIDENCE GAPs, with safe procedures)

- Disk-full / WAL-full / failed fsync / read-only FS: procedure — disposable
  dir on small tmpfs/loop device, fill to 100%, run writes, expect clean errors
  + restart recovery; **do not run on the dev volume**.
- Memory pressure / OOM: procedure — `ulimit`/cgroup-capped server + 1M
  group-by/sort; expect spill + bounded RSS, no abort.
- Maintenance failure/retry, duplicate jobs, worker shutdown races: no harness.
- Corrupt WAL tail / bad checksum / missing segment / stale generation refs:
  procedure — flip bytes in a disposable copy's WAL tail, reopen, expect
  truncate-to-boundary or fail-closed (never silent).
- Upgrade/rollback across versions, cross-path restore: no drill (blocked also
  by P1-004).
- `SHOW ALL` empty stub and missing manual `CHECKPOINT` reduce operability
  (P2-007/P3).

## Operator guidance (as-tested)

Single-user loopback: defaults safe. Remote: `--tls --tls-cert/--tls-key
--tls-required` + SCRAM + least-privilege roles + reconnect-after-DDL rule
(P1-003) until fixed. No backup story yet (P1-004) — do not promise PITR.
