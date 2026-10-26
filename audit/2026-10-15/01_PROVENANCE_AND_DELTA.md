# 01 — Provenance and delta

- Date/time: 2026-10-15 09:01–10:30 UTC. TZ UTC.
- OS/kernel: Darwin 24.4.0 arm64 (macOS 15.4). CPU Apple M2 Pro, 12 logical /
  12 physical cores. RAM 16 GiB. Disk APFS 926 GiB total, ~142 GiB free.
- Toolchain: rustc 1.99.0, cargo 1.99.0 (pinned `rust-toolchain.toml` 1.99.0).
  Python 3.12.4, psycopg 3.2.9, psql 14.17 (client only).
- Repo root `/Users/sainathsapa/dsk/PloMID/plomid`, branch
  `starting-v0.1.0-beta-2`, HEAD `f969bc6a79e46114603d8ba9e4950c19b956b42a`.
- Baseline audited commit (2026-10-13): `bcd6bf4`. Delta `bcd6bf4..f969bc6` =
  8 commits (98bc75d P0-001/P0-002 fixes + header sweep, 7 CI/docs/test
  commits). P0-001 fix verified in `git show 98bc75d -- handler.rs`
  (Parse gate + unified pre-split + Sync-ownership fix); P0-002 fix in same
  commit (decimal/exponent spellings → Float8, `infer_expr_type` operand
  classes). Nothing inferred from messages — diffs inspected.
- Dirty state (pre-existing, preserved, NOT the audit's): 17 modified files
  = post-10-14 P0-003 DATABASE-DDL authz (`check_database_ddl` intercept,
  conditional forms, SQLSTATEs) + ILIKE `case_insensitive` plumbing. Full
  list in `git status --short` (executor 9, network handler + rbac test,
  sql ast/expressions + parser test). Untracked: `audit/2026-10-14-…/` (prior
  session) + this bundle. No stash/reset/clean/commit/tag/push performed.
- Workspace: 25 crates, binaries `plomid-server` + `plomid` CLI.
- Release binary: canonical `cargo build --release -p plomid-server`
  (exit 0, 65 s), sha256 `6c861f51362b…abf91b0c`. All live probes targeted
  this binary (ports 55461/55462/55464/55465, disposable
  `/tmp/plomid-audit15/*`, user plomid). Debug test binaries used only for
  in-tree regression suites (ports ephemeral).
- Existing audit folders preserved: `audit/2026-10-13/`,
  `audit/2026-10-14-release-readiness/`, `audit/beta2/`,
  `audit/PLOMID_FULL_AUDIT_REPORT.md` — none modified.

## Command inventory

| Command | Result |
|---|---|
| `git status/log/show/diff` provenance chain | ran, recorded |
| `cargo build --release -p plomid-server` | PASS (exit 0) |
| live PGWire probes (`artifacts/p0-*`, `artifacts/probes/*`) | ran, all logged |
| `extended_batch` 7/7, `unit_protocol_codec` 19/19 | PASS |
| network `numeric_arithmetic` 5/5, executor `numeric_arithmetic` 7/7 | PASS |
| executor `catalog_visibility` 6/6, sql parser 37/37 | PASS |
| network `rbac` 9/9 (incl. `database_ddl_requires_createdb…`) | PASS |
| `cargo fmt --all --check` | PASS (exit 0) |
| `cargo deny check` | PASS (advisories/bans/licenses/sources ok) |
| `cargo clippy --workspace --lib --tests` (CI deny set) | PASS exit 0 (1 advisory style warning) |
| `cargo check --workspace --all-targets` | PASS (exit 0) |
| `cargo test --workspace --jobs 2` (full suite) | NOT RUN — operator halted after targeted suites per explicit instruction (see `12`) |
| Gauntlet/acceptance, release benches | NOT RUN (same reason + scale benches excluded from release path by project) |
| 1M+ rows, disk-full/OOM, torn-WAL drills, x86/Linux runtime | NOT RUN (unsafe/unavailable on this host; commands in `09`/`12`) |
| raw-socket byte-fragmented live probe | NOT RUN live (SCRAM auth); covered by codec unit tests + live pipelined/repeat probes |
