# Beta 2 — 12 Chaos / Failure Qualification (Phase 13)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.

## Gate

`crates/executor/tests/chaos_matrix.rs` — 13 tests, all green
(`cargo test -p plomid-executor --test chaos_matrix`):

| # | Case | Fault | Verdict |
|---|---|---|---|
| 1 | kill during INSERT | drop, no checkpoint | committed rows present, reopen+writes OK |
| 2 | kill during UPDATE | drop mid-update | table intact (200), writable |
| 3 | kill during DELETE | drop mid-delete | 200 or 150 (atomic replay), normalized to 150 |
| 4 | kill during txn commit | committed batch kept, uncommitted absent | 201 rows, id=1002 absent |
| 5 | kill during checkpoint | checkpoint then kill | 200 or 250 boundary, normalized to 250 |
| 6 | kill during VACUUM | delete+VACUUM then kill | boundary, normalized to 100 |
| 7 | kill during backup | partial copy, no manifest | source clean; restore → `Corruption` |
| 8 | WAL unavailable | WAL dir moved away | loud: checkpoint-covered open (200 rows) or explicit error |
| 9 | corrupt WAL middle segment | byte flip | loud `Corruption/Unsupported/Io/Wal` or intact 200 |
| 10 | corrupt checkpoint | xor newest `.ckpt` | fallback to prior + replay (200) or loud error |
| 11 | corrupt generation CURRENT | garbage pointer | repair→200 or loud error |
| 12 | corrupt derived vector index | corrupt `vector_ann/*` | exact fallback top-1 correct |
| 13 | shutdown during active writes | writes + checkpoint + close | 300 rows, +1 write durable |

Every reopen case additionally asserts: index point lookup works,
subsequent `INSERT` works, second reopen keeps the write (no data loss).

## Infrastructure promoted

Pre-existing fail-points now exercised by a release gate (previously
unit-only): storage checkpoint (`checkpoint.rs:170`), generation
publish/GC (`generation.rs:951,974`), columnar/compaction
(`store.rs:141`, `compaction.rs:115`), index build
(`index/generation.rs:147`), vector backup (`backup.rs:235,251`), WAL
`TestInject` (`wal/writer.rs:220`). Real-`SIGKILL` drivers remain manual
(`benchmarks/production/phase-11/crash_matrix.py`,
`phase-10/server_kill9_recovery.py`); CI runs these deterministic
drop-reopen equivalents (no timing flakes).

## Not covered (explicit gaps)

- Disk-full `ENOSPC` / WAL-full in the live server path (no fault
  injection for `RealFs`; unit-only `FailSync` watermark tests).
- `fsync`-error mid-publish in production (`group_commit.rs:865` notes the
  injection gap).
- kill during vector index build/publish beyond derived-corruption
  fallback (covered at row/index level in
  `vector_durability_convergence`, not as live-server kill).
- No data loss observed in any covered case.

## Status

GREEN (13/13 deterministic gates). YELLOW: ENOSPC/fsync-error live-path
injection and real-SIGKILL CI remain manual (scripts exist, not gated).
