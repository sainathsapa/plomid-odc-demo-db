# 05 — Durability, backup/restore, crash recovery

Evidence: `artifacts/probes/logs/backup_crash_probe.log` (**12/12**),
`storage_split_fail.log` / `storage_split_control.log`, restart
measurement (`restart_measurement.txt`).

## Backup and restore — PASS

- **Hot backup is refused by design**: `refusing backup: data directory
  is locked by a live process (stop the server on … first)` — explicit,
  not silent.
- **Cold backup**: 11 files, 562 646 bytes, manifest format 1;
  `backup-inspect` reports versions (catalog=2, checkpoint=1,
  generation=1, wal=2, segment=1).
- **Restore to clean dir**: verified — relational rows `(3, 1, 3)`,
  vector query top=2, index state `READY`, one write + reopen.
- **Restart after restore**: index state `READY`, ANN query top=3.

## Kill -9 recovery — PASS

- After `kill -9`, recovery left a clean state: `inserted=10000
  total=10003` (count is an exact multiple of the batch size issued
  pre-kill), post-crash writes work.
- WAL candidates after recovery: `wal/WAL-000000000001.dat`,
  `wal/checkpoint`.

## Torn WAL tail — explicit refusal (honest record)

Appending garbage to the WAL tail makes the server **refuse to start**
(`server exited during startup: …` with a framed error header). It does
not silently accept or partially recover. This is the recorded behavior;
recovery tooling for that state (truncate-to-checkpoint flow) is not
part of this audit's scope (G-04 remains partial; F-013-adjacent
operational note in `11`).

## Restart lifecycle of vector indexes (cross-ref `06`)

- HNSW (in-memory): cold after restart re-builds from rows
  (3.3 s at 8 k×64; ids identical to pre-restart → deterministic).
- DiskANN: checkpoint files are persisted under
  `data/vector_ann/<db>/<table>/<col>/` (`graph.dat`, `vectors.dat`) but
  the cold path logs `diskann checkpoint ready; rebuilding from snapshot
  rows` and re-builds (~42 s at 8 k×64) instead of a warm checkpoint
  load. Checkpoints therefore reduce nothing on restart by themselves —
  claim wording in docs should say “rebuilds from snapshot rows”.

## Gaps

- Disk-full / WAL-full / OOM behavior: **NOT RUN** (G-03).
- Systematic crash-point chaos (kill windows at commit boundaries):
  **NOT RUN** beyond the cases above (G-04 partial).
- PITR: out of scope by design; needs an explicit Beta exclusion
  statement (G-07).
