# 02 — Architecture and live paths

Static map produced by read-only code trace (all paths cited file:line);
live-reachability cross-checked by the probes in §§3–5, 8 where noted.

## Request path (user SQL → durable bytes → result)

PGWire listener (`network/src/server/mod.rs:bind_with_limits`, semaphore
`max_connections`, per-conn task, SSL/GSSENC/Cancel pre-startup) → startup +
TLS-required gate → auth dispatch (scram/password/md5/none;
`handler.rs:417-451,1128,1191,1282`) → simple `Query` or extended
Parse/Bind/Execute/Sync (`handler.rs:550-652`) → `handle_query` (statement
splitter, txn-control routing, COPY sniff) → `execute_all` (admission,
catalog refresh-if-stale, authz gate per statement) → session-txn staging
(RC refresh / RR pin) → statement dispatch (DML/SELECT/DDL) → txn manager
(lanes, snapshots, `commit_prepare` WAL-append → `commit_publish` MVCC
install + group barrier) → storage (row pages / B-tree / ART / columnar
generations / vector rows-as-values) → WAL prefix durability, checkpoint,
GC. Shutdown: drain (5 s grace) → engine stop/flush/checkpoint/sync.

## Path matrix (condensed; full matrix with line numbers in agent evidence)

| Path | Class | Proof / note |
| ---- | ----- | ------------ |
| Startup, config validation, 0700 data dir + lock, copy-dir jail | LIVE_AND_END_TO_END | `server/src/main.rs:49-206`, `config.rs:256-524,634-890` |
| SCRAM/password auth, wrong-pw uniform FATAL, rate-limit + lockout | LIVE_AND_END_TO_END | live-verified (§8); `handler.rs:1137-1396`, `guardrails.rs` |
| Simple + extended protocol, Parse/Bind/Describe/Close, COPY IN/OUT, cancel | LIVE_AND_END_TO_END | live-verified; `handler.rs:550-674,730-1097` |
| Multi-statement batches both protocols | LIVE_AND_END_TO_END | live-verified (P0-001 fixed) |
| `CREATE/DROP DATABASE` | LIVE_WITH_RESTRICTIONS | works; **no authz (P0-003)**; DROP fails simple/errors-asymmetric (P2) |
| DDL/DML/SELECT, prepared params, savepoints | LIVE_AND_END_TO_END | live-verified; `session_txn.rs`, `ddl.rs`, `dml.rs` |
| `PREPARE` SQL statement | NOT_VERIFIED | no parser entry; wire Parse/Bind is the prepared path |
| Optimizer crate on hot row path | PARTIALLY_WIRED | no single `optimize→plan` call traced on `execute_all_inner`; ANN planning live |
| Row/page/B-tree/ART/columnar/WAL/checkpoint/GC/recovery | LIVE_AND_END_TO_END | live-verified (restart, kill-9, backup); `engine_impl.rs`, `generation.rs` |
| Hot `BACKUP` SQL | UNREACHABLE_OR_DEAD | by design; cold offline CLI only (§5) |
| Vector exact/ANN/rebuild/backup-via-rows | LIVE_AND_END_TO_END with restrictions | §5; GPU stub, FLAT/BINARY/multi-ANN exact-only |
| `SERIALIZABLE`, FK, CTAS, CHECKPOINT stmt, DROP ROLE | fail-closed unsupported | live-verified errors (§3) |

## Duplicate ownership

Mostly legitimate layering (durable catalog → in-memory image → session
snapshot; wire status → executor staging → txn truth; WAL-prefix single
durability authority; one `recover_storage` pipeline; lib-vs-tool backup
split). Real duplication kept convergent by sync code: database-name lists
in registry vs executor vs catalog; CONNECT/COPY gates at wire + per-statement
(double-gating documented as defense-in-depth). No divergent-truth defect
found. Detail: static map evidence §§5–6.
