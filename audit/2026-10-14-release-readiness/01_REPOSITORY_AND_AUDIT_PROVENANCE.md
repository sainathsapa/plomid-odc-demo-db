# 01 — Repository and audit provenance

- Root: `/Users/sainathsapa/dsk/PloMID/plomid`. Date: 2026-10-14T16:51Z+ (UTC).
- OS: Darwin 24.4.0 arm64, Apple M2 Pro, 12 logical CPUs, 16 GiB RAM.
- Toolchain: rustc/cargo 1.99.0 (pinned by `rust-toolchain.toml`);
  psql 18 client; python 3.12 + psycopg 3.
- Git: branch `starting-v0.1.0-beta-2`, HEAD `6a935b4` ("ci test fix 2"),
  tree **clean** at audit start (prior session work committed as `a183045`,
  `6a935b4`; no stashes touched).
- Delta window vs prior audit: `bcd6bf4..6a935b4` (7 commits):
  `before test_compact`, CI scale/bench gating ×2, `fix: disk full`,
  `compatch fix` (executor join/hash), `ci test fix`, `ci test fix 2`
  (Cargo.lock rustls 0.23.45→0.23.46, storage soak gating + deterministic
  regression). **No commit message references P0-001/P0-002 fixes** — both
  were re-verified live rather than assumed.
- Prior audit: `audit/2026-10-13/` (verdict NO-GO everywhere, commit
  `bcd6bf4`) + `audit/beta2/` topical reports. Treated as historical leads
  only; every delta claim below was re-checked against current code/live runs.
- Workspace: 25 crates (`core crc32c storage wal mvcc txn sql optimizer json
  executor network security server cli types columnar index filters htap hw
  spill stream memory gauntlet vector`). Binaries: `plomid-server`, `plomid`
  CLI. Features: vector `simd` default (+`avx2/avx512/neon/sve/gpu` tiers);
  columnar `zstd` optional. Profiles: `test = line-tables-only`,
  `release = thin LTO + codegen-units 1 + strip`.
- Commands used: `cargo build -p plomid-server` (debug 2m21s),
  `cargo build --release -p plomid-server` (58.93s incr, exit 0),
  `cargo deny check` (exit 0), server `--help`/`help-backup`, psql simple
  protocol, psycopg extended protocol. Full `cargo test --workspace`
  NOT RUN (bounded; prior CI + targeted runs cited in §11).
- Servers run: debug binary ports 55432/55433/55434(TLS)/release 55435,
  data dirs under `/tmp/plomid-audit-20261014/` (disposable). Server stopped
  after drills except where noted.
