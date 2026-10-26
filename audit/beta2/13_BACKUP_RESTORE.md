# Beta 2 — 13 Backup / Restore Drill (Phase 12)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree (txn backup module, executor drill).

## Mechanism

- `crates/txn/src/backup.rs:1` — cold engine-directory backup. Manifest
  `PLOMID_BACKUP_MANIFEST` written **last** via tmp+rename (`backup_database`,
  `restore_database`, `read_backup_manifest`).
- Manifest (`BackupManifest`): `backup_format=1` + captured durable versions
  (`catalog/checkpoint/generation/wal_record/segment` from
  `crates/core/src/constants.rs:66-71`) + file/byte counts. Unknown
  `backup_format` → `Unsupported` (no silent downgrade); version skew vs
  binary → `Unsupported`; missing manifest (interrupted backup) →
  `Corruption`; restore-to-occupied dir → `AlreadyExists` (restore-to-clean
  rollback policy).
- Excluded: `*.tmp` staged targets, `vector_ann` derived cache (rebuilt from
  authoritative rows), nested manifests, non-regular files (never followed).
- Durable versions (single-sourced): `CATALOG=2, CHECKPOINT=1, GENERATION=1,
  WAL_RECORD=2, SEGMENT=1` (`constants.rs:66-71`); vector/columnar artifact
  versions unchanged (vector `BACKUP_FORMAT=1`, columnar journal accepts 1–2).

## Drill (all green)

`crates/executor/tests/backup_restore.rs` (4 tests):

| Test | Verifies |
|---|---|
| `full_backup_restore_roundtrip` | schema, 501 rows, `SUM=380760` checksum, index point lookup + ordered scan, txn batch `BEGIN..COMMIT`, checkpoint, restore, re-verify, new write 502 durable across second reopen |
| `interrupted_backup_has_no_manifest_and_restore_refuses` | partial copy (CURRENT only) → `Corruption` |
| `incompatible_backup_version_is_refused_no_silent_downgrade` | `backup_format=99` → `Unsupported` |
| `restore_refuses_to_overwrite_existing_database_rollback_policy` | restore over live src → `AlreadyExists` |

Vector/columnar boundary: existing
`executor/tests/vector_durability_convergence.rs:553`
`backup_restore_through_engine_boundary_excludes_derived_cache` (checkpoint,
dir copy minus `vector_ann`, reopen + `VACUUM` converge) — still green,
unchanged.

## Operator runbook (Beta 2)

1. `checkpoint` the engine (or stop writes), close it.
2. `backup_database(src, dst)` — fails if dst exists.
3. Keep the manifest with the backup; restore with `restore_database` to a
   clean directory only. Never merge a backup into a live directory.
4. No hot (live-writer) backup in Beta 2 — quiesce first. No PITR (no WAL
   archiving); full cold copies only.

## Status

GREEN (cold full backup/restore + version gates + rollback policy, tested).
YELLOW: hot backup, incremental/PITR, old→new migration (reserved
`SupportedWithMigration`, none exists — all versions are 1/2).
