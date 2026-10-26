# Beta 2 — 14 Upgrade / Rollback Policy (Phase 12)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.

## Version inventory (every durable format versioned)

- Core (`crates/core/src/constants.rs:58-81`): device/allocator/page/block/
  pack/manager 1; `CATALOG 2`; `CHECKPOINT 1`; `GENERATION 1`; `POINTER 1`;
  `SEGMENT 1`; `WAL_RECORD 2`; row/executor/columnar/chunk/encoding 1–2.
- WAL: `PLW2` record v2 (`wal/format.rs:153,172,230`), segment `PLWS` v1
  (`wal/segmented.rs:27,63,83`) — mismatch fail-closed.
- Generation pointer + catalog: version-checked decode
  (`storage/generation.rs:530,562`; `storage/catalog.rs:1448-1457`).
- Vector artifacts: backup/manifest/binary/IVF/stats v1
  (`vector/backup.rs:65`, `index/manifest.rs:40` …); compatibility matrix
  `vector/migration.rs:45` (`SupportedWithMigration` reserved, none exists).
- Backup manifest: `backup_format=1` (`txn/backup.rs:12`).

## Tested

- Backup from old → restore new: emulated by version-tamper test
  (`executor/tests/backup_restore.rs:incompatible_backup_version…`):
  future `backup_format=99` → `Unsupported`, loud, no silent downgrade.
- Incompatible physical format refusal: same test + existing
  `vector/tests/upgrade.rs:23` (`standalone_version_bump_fails_closed`),
  storage `physical.rs:173,368,477` magic/version gates.
- Interrupted upgrade: manifest-last backup + `interrupted_backup…` test
  (partial copy → `Corruption`); checkpoint `.tmp` ignored +
  `storage/tests/checkpoint_recovery.rs:217` (7 fail-points leave gen1).
- Rollback policy: **restore-to-clean only**
  (`restore_refuses_to_overwrite…` → `AlreadyExists`). No in-place
  downgrade path exists by design.

## Policy (Beta 2)

- Upgrades are cold backup → new binary → restore to clean dir → verify.
  No automatic migration except catalog single-blob→sharded on load
  (`sql/catalog/mod.rs:637`) and columnar journal 1→2 accept.
- Downgrade = restore the pre-upgrade cold backup. Never open a newer
  image with an older binary (refused `Unsupported`).

## Status

GREEN (refusal gates + rollback policy, tested). YELLOW: no general
migrate-forward tool; cross-version matrix is refusal-only.
