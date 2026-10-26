# 02 — Architecture and live paths

Call-graph root verified: `crates/server/src/main.rs` → `network::PlomidServer`
→ `executor::Executor<ConcurrentPlomidStorageEngine>` → `txn::PlomidStorageEngine`
→ mvcc/wal/storage. Two independent code-read subagent traces plus live probes
confirm the matrix below. Full per-crate maps: prior report §§02–03 (still
accurate except rows marked Δ).

## Path matrix (LIVE = reached from `plomid-server` binary + probe-proven)

| Path | Pointer | Status | Proof |
|---|---|---|---|
| PGWire handshake/auth/session | `network/src/server/mod.rs:164`, `tls.rs`, `scram.rs`, `handler.rs:292-413` | LIVE_AND_END_TO_END | psycopg + psql connects; wrong-pw FATAL; TLS live |
| Simple-protocol multi-stmt + txn batch | `handler.rs:2115-2133`, `executor::execute_all_with_search_path` | LIVE_AND_END_TO_END | psql `BEGIN;…;COMMIT` commits (probe §P0-001 notes) |
| **Extended-protocol multi-stmt** | `handler.rs:1706,1835-1839,2575-2650` | **LIVE_WITH_SILENT_TRUNCATION (P0-001)** | only 1st stmt runs; rest dropped, no error |
| SQL lexer/parser/binder/catalog | `sql/src/lexer.rs`, `parser/mod.rs`, `catalog/mod.rs`, `executor.rs:903-986` | LIVE_AND_END_TO_END | DDL/DML/select probes |
| Executor dispatch + session txn | `executor.rs`, `transaction.rs`, `session_txn.rs:173-358` | LIVE_AND_END_TO_END | txn/savepoint probes |
| txn engine/commit/group-commit | `txn/src/engine_impl.rs:384-540,721-960`, `concurrent.rs:479-1151` | LIVE_AND_END_TO_END | concurrent + crash probes |
| MVCC store/snapshot/visibility | `mvcc/src/store.rs`, `version.rs`, `txn/src/allocator.rs:240-364` | LIVE_AND_END_TO_END | mid-txn reads, RR/RC probes |
| WAL segmented/checkpoint/recovery | `wal/src/segmented.rs`, `checkpoint_recovery.rs` | LIVE_AND_END_TO_END | kill -9 replay `applied_records=7` |
| Storage generations/catalog/checkpoint | `storage/src/manager.rs`, `generation.rs`, `catalog.rs`, `checkpoint.rs` | LIVE_AND_END_TO_END | restart-identical results |
| Columnar derived store/pruning/freshness | `columnar/src/store.rs:21-28`, `executor/query/select.rs:1658-1822` | LIVE_AND_END_TO_END (gated) | EXPLAIN shows column fetch; fallback correct |
| BTree durable + ART memory | `index/src/btree/tree.rs`, `generation.rs:363`, `executor.rs:304,360` | LIVE_AND_END_TO_END | PK/unique enforced under race |
| Vector types/exact/ANN-gated | `vector/src/{type,value,literal,search}`, `executor/src/vector_ann.rs:667-774` | LIVE_AND_END_TO_END (exact); ANN accelerator w/ exact fallback | k-NN + EXPLAIN probes |
| Vector index files (`vector_ann/`) | `executor/src/vector_ann.rs:46-65,370-408` | LIVE derived cache (excluded from backup) | restart k-NN identical |
| Legacy `vector/src/{storage,mvcc,backup}` | headers declare non-authoritative | TEST_ONLY (no prod callers) | grep: zero callers in server/network/executor/txn |
| Optimizer (advisory) | `optimizer/src/lib.rs`, `select.rs:1658` | LIVE_WITH_RESTRICTIONS (engine choice only) | EXPLAIN hybrid plan |
| HTAP governor/gates | `htap/src/*`, `executor/governance.rs` | LIVE (narrow) | admitted 100K + concurrent load |
| Memory/spill/stream | `memory/`, `spill/`, `stream/` via executor | LIVE (bounded) | 100K group-by 110 ms, no OOM |
| RBAC enforcement | `executor/src/authz.rs:122-189,748-754` | LIVE_AND_END_TO_END | deny/allow probes, 42501 |
| TLS | `network/src/tls.rs`, `server/main.rs:252-274` | LIVE_AND_END_TO_END (Δ fixed) | live handshake + refusal |
| COPY jail + caps | `handler.rs:889-936,3715-3971` | LIVE_AND_END_TO_END | auditor COPY denied |
| `txn::backup_database` | `txn/src/backup.rs:206-280` | **TEST_ONLY (P1-004)** — zero callers in server/network/executor | grep proof |
| `lifecycle` crate | — | **removed** (Δ; merged into engine open path) | `ls crates/` |
| `filters` Roaring | — | **removed** (Δ; XorFilter kept) | commit `3fe1c1e` |
| `security` crate | `security/src/{credential,privilege,audit,sink}.rs` | LIVE vocabulary + crypto (Δ staffed) | SCRAM/RBAC live |
| CLI `plomid` | `crates/cli` | TOOLING (not server path) | — |
| Gauntlet | `crates/gauntlet` | TEST_ONLY harness (correct) | — |

## Duplicate-owner check (Δ since prior audit)

- Catalog: single authority (`sql::InMemoryCatalog` staged deltas + storage
  publish). **Residual: P1-003** — live sessions hold stale images; no refresh.
- Txn state: single live authority (`ConcurrentTransaction` + overlay);
  legacy `txn::Transaction` unreferenced by server (candidate delete, P3).
- WAL/durability: single authority (`SegmentedWal` + group commit); vector
  second-WAL removed/legacy-gated (Δ verified).
- Recovery orchestration: single (`PlomidStorageEngine::open:734-960`).
- Backup: **split unresolved** — engine cold backup (unwired) vs vector legacy
  backup (test-only). P1-004.
- Permissions: single enforcement (`executor::authz`), vocabulary in `security`.
- No other competing engines found. Layering (columnar/ART/ANN as derived) is
  legitimate and correctly fail-closed.
