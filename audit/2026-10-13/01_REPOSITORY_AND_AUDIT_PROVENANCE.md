# 01 — Repository and audit provenance

- Repo root: `/Users/sainathsapa/dsk/PloMID/plomid`
- Date/time: 2026-10-13 09:34–10:30 UTC (probe window)
- OS/kernel: Darwin 24.4.0 arm64 (macOS 15.4, M2 Pro, 12 cores)
- CPU: Apple M2 Pro; RAM 16 GiB; disk APFS 926 GiB (176 GiB free at start)
- Toolchain: rustc 1.99.0, cargo 1.99.0, pinned `rust-toolchain.toml` 1.99.0
- Git branch: `starting-v0.1.0-beta-2`; commit `bcd6bf4314ced1cc986c026930177a0d35c24211`
- Tree state at start: **clean** (`git status --porcelain` empty, ahead of origin by 25,
  no untracked files). End state: only new `audit/2026-10-13/` bundle.
- Prior audit: `audit/PLOMID_FULL_AUDIT_REPORT.md` @ `0877cb0` (2026-10-08);
  delta window `0877cb0..bcd6bf4` = 25 commits (see `12`). HEAD commit
  (`bcd6bf4`, "rename company name") is comment/header-only (verified via
  `git show HEAD` — no `.rs` logic change outside headers).
- Binary provenance: `target/release/plomid-server` v0.1.0-beta.2, built
  2026-10-13 12:57 local — after all logic commits, before header-only HEAD.
  Functionally identical to HEAD (no behavioral delta in HEAD).
- Workspace: 25 crates (lifecycle removed since prior audit: 26→25),
  binaries `plomid-server` + `plomid` CLI, 22 criterion benches (9 crates),
  365 integration test files, `Makefile` gates (`test`, `test-ci`, `test-local`,
  `test-scale`, `gauntlet`, `clippy`, `deny`, `release-beta`).

## Commands actually run (this audit)

| Command | Result |
|---|---|
| `git status / rev-parse / log` | clean, `bcd6bf4`, +25 vs prior |
| `cargo fmt --all --check` | **pass** (exit 0) |
| `cargo deny check` | **pass** (advisories/bans/licenses/sources ok) |
| `cargo test --workspace --lib --tests` | **started, still compiling at report time → NOT VERIFIED** (see `11`, `14`) |
| `cargo test -p plomid-gauntlet --lib` | **NOT RUN** (blocked >120 s on build; machine compiling full suite) |
| `cargo clippy --workspace --lib --tests …` | **NOT RUN** (cargo build-lock held by suite + time budget) |
| live PGWire probes `artifacts/probe*.log` (23 files) | all executed, outputs recorded |
| TLS live handshake + refusal + non-loopback bind refusal | **pass** (`probe14`) |
| graceful restart + `kill -9` crash + recovery | **pass** (`probe6`, `probe7`) |
| 10K/100K scale inserts + concurrency (8-way) + unique race | **pass** (`probe4/5/13`) |

## Evidence limitations

Single host (arm64 macOS); no x86_64/Linux/Windows runtime; no GPU; no
disk-full/OOM/pressure injection (unsafe on dev laptop); no multi-statement
fuzz beyond recorded repros; full suite + clippy pending (machine budget).
Nothing was tuned: default server flags except host/port/credentials and one
TLS instance on :55434 with a disposable self-signed cert.
