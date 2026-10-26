# Beta 2 — Phase 0 Baseline and Re-audit Delta

Date (UTC): 2026-10-07 (recorded 2026-10-08 session, `date` context Oct 07 2026 UTC)
Plan source: `audit/PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` §4
Audit authority: `audit/PLOMID_FULL_AUDIT_REPORT.md` (consolidated, commit `0877cb0`)
Rule for this phase: **no production source modified** (audit/beta2 docs only).

## 1. Git state

| Item | Value |
|---|---|
| HEAD | `0877cb04001447fdfccc7eaca102b60e50c4d237` |
| Branch | `starting-v0.1.0-beta-2` |
| Audited commit (plan §1) | `0877cb04001447fdfccc7eaca102b60e50c4d237` |
| `git status --short` | `?? audit/` only (tracked tree clean) |
| `git status` detail | `On branch starting-v0.1.0-beta-2`, ahead of `origin/starting-v0.1.0-beta-2` by 1 commit (`0877cb0 removed irrelevent docs`) |
| `git log --oneline -5` | `0877cb0 removed irrelevent docs` / `afe304a vector: replace manual min/max chains with clamp (CI deny fix)` / `64e49db hw: fix clippy deny failures in x86-only code` / `033b6c1 hw: fix nonexistent str::strip in Linux-only detection code` / `64a9f4c toolchain: pin Rust 1.99.0 local + CI, zero clippy/fmt drift` |

Interpretation:

- HEAD **equals** the audited commit. Zero tracked-file drift since audit.
- The "ahead of origin by 1" is the audited commit itself not yet pushed, not a post-audit change.
- The only untracked path is `audit/` (the audit report + Beta 2 plan, both audit outputs, not production source). No production file was touched in Phase 0.

## 2. Environment

| Item | Value |
|---|---|
| Host | `Darwin Sainaths-MacBook-Pro.local 24.4.0 Darwin Kernel Version 24.4.0 … RELEASE_ARM64_T6020 arm64` (Apple M2 Pro class, darwin/arm64) |
| `rustc --version` | `rustc 1.99.0 (b940084d7 2026-09-28)` |
| `cargo --version` | `cargo 1.99.0 (5f94df478 2026-08-27)` |
| `rust-toolchain.toml` | channel `1.99.0`, profile `minimal`, components `rustfmt, clippy` — matches audit |
| Audit-host match | Yes: same toolchain 1.99.0, same darwin/arm64 host class as audit |

## 3. Workspace inventory

- `Cargo.toml:2-29`: 26 members, version `0.1.0-beta.2`, edition 2021, `rust-version 1.99.0`, Apache-2.0.
- Members: `core, crc32c, storage, wal, lifecycle, mvcc, txn, sql, optimizer, json, executor, network, security, server, cli, types, columnar, index, filters, htap, hw, spill, stream, memory, gauntlet, vector`.
- `resolver = "2"`, `workspace.lints.rust unsafe_code = "forbid"`, release profile thin-LTO / single codegen unit / strip symbols (`Cargo.toml:106-112`).
- Internal deps single-sourced at workspace version (`Cargo.toml:73-104`); no git/path deps outside workspace; no `build.rs`; no proc macros.

Measured 2026-10-08 (`find crates -name "*.rs" | wc -l` = 565 files; `cat … | wc -l` = 266,413 lines):

| Crate | .rs files | Lines |
|---|---|---|
| executor | 121 | 78522 |
| storage | 64 | 28467 |
| vector | 90 | 27673 |
| sql | 25 | 17654 |
| columnar | 46 | 32912 |
| network | 13 | 10511 |
| index | 26 | 10436 |
| txn | 16 | 9454 |
| types | 17 | 7408 |
| hw | 12 | 6448 |
| filters | 25 | 4672 |
| wal | 7 | 4142 |
| json | 7 | 3842 |
| htap | 13 | 3733 |
| gauntlet | 12 | 3084 |
| memory | 10 | 2670 |
| core | 5 | 2152 |
| server | 4 | 1976 |
| lifecycle | 13 | 1881 |
| stream | 10 | 1867 |
| optimizer | 4 | 1624 |
| cli | 1 | 1272 |
| spill | 10 | 2283 |
| mvcc | 6 | 876 |
| crc32c | 7 | 832 |
| security | 1 | 22 |

Note on audit totals: the audit summary says "~539 files / ~273k lines" but its own per-crate table sums to the same 565 files measured here, and per-crate file counts match exactly. Line delta (~273k vs 266,413) is counting methodology, not source drift — `git status` proves the tracked tree is clean. Binaries: `plomid-server` (`crates/server/src/main.rs`), `plomid` CLI (wire-client only). All other crates are libs. Examples: 2. Bench targets: 22 across 9 suites. Integration test files: 100+ (executor 66, storage 17, vector 8, columnar 8, index 9, txn 7, network 3; root `tests/*.sql` 12 files + `tests/perf/*.py` 17 + `tests/python/*.py` 11). Fuzz targets: none (deterministic `par_fuzz_*` unit-style only).

## 4. Cargo.lock

- `Cargo.lock`: 1,233 lines, `version = 4`, 136 `[[package]]` entries (26 `plomid-*` + ~110 externals).
- Key pins (unchanged from audit): `tokio 1.53.1`, `bytes 1.12.1`, `tracing 0.1.44`, `tracing-subscriber 0.3.23`, `clap 4.6.6`, `regex 1.11`, `serde/serde_json 1`, `criterion 0.5` (dev/bench), `zstd 0.13` (columnar optional, off by default).
- No git deps; source is `registry+https://github.com/rust-lang/crates.io-index` only (spot-checked `tokio`, `clap`).
- Release builds use `--locked` (`release.yml`); `Cargo.lock` committed.

## 5. Existing audit and docs state

- `audit/`: exactly 2 files, both untracked audit outputs — `PLOMID_FULL_AUDIT_REPORT.md` (116 KB, consolidated 00–23, generated 2026-10-08 from `0877cb0`) and `PLOMID-BETA2-FOUNDATION-SECURITY-PERFORMANCE-PLAN.md` (42 KB, 40 sections, Phases 0–25 + gates). No `audit/beta2/` existed before this phase.
- `docs/`: `docs.json` + `docs/v0.1.0/` (94 files: architecture, build, contributing, cookbook, data, future, getting-started, index, indexes, json, learn, migration, operations, reference, sql, storage, timeseries, transactions, troubleshooting). No `docs/EXTENSION_CONTRACT.md`, no `docs/compliance/TRACEABILITY.md`, no error-code catalog, no TLS runbook, no production/secure-remote/single-node profiles yet.
- Root docs: `README.md`, `CONTRIBUTING.md`, `SECURITY.md` (private-report, no contact email, no SLA/versions table), `RELEASE_BASELINE.md` (stale: references branch `RLS`, commit `9587f97`, 20-crate workspace with `cron`/`ring` — superseded by audit §01; do not use as authority).
- `benchmark_results/`: `benchmark_report.md`, `benchmark_results.json`, `comparison.csv`, `vector-BENCHMARKS.md`.
- `benchmarks/`: `harness/` (runner, run_all, compare, schema, workloads) + `production/phase-00..26` + top-level `phase-00..26` dirs.

## 6. Changed-since-audit list

**Empty — no tracked-file changes since the audited commit.**

- `git diff --stat 0877cb04001447fdfccc7eaca102b60e50c4d237` → empty.
- `git status --short` → `?? audit/` only.
- Per-crate file/line counts match the audit's per-crate table exactly (see §3).
- Therefore every finding below is re-verified against unchanged code; statuses carry over unmodified.

## 7. Re-verified finding status (F-01–F-15)

All re-checked 2026-10-08 on HEAD `0877cb0`. **All 15 remain OPEN as audited.** No silent downgrades.

| ID | Severity | Finding | Re-verification (HEAD) | Status |
|---|---|---|---|---|
| F-01 | P1 | Vector index/storage is a parallel persistence system | `crates/vector/src/lib.rs:26` still declares independence from SQL/catalog/storage; `vector/Cargo.toml` deps = `plomid-hw` only (zero storage/wal/mvcc); own `storage/{file,pages,snapshot,manifest,adapter,policy}`, `mvcc/visibility.rs`, `backup.rs`, `freshness.rs` all present | OPEN, confirmed |
| F-02 | P0 remote / P1 loopback | TLS not implemented; insecure remote defaults | No rustls/native-tls in tree; `handler.rs:291` still `write_all(b"N")`; `TlsConfig` struct exists (`config.rs:30-44`) but handshake unimplemented; `Dockerfile:59-62` still bakes `PLOMID_HOST=0.0.0.0` + `PLOMID_ALLOW_INSECURE_REMOTE=true` with public default creds | OPEN, confirmed |
| F-03 | P1 | Explicit txns replay SQL text at COMMIT | `handler.rs:1732-1766` validate+stage then `handler.rs:2321` `execute_all_with_search_path(&batch)` at COMMIT; autocommit path `:1801-1808` writes immediately — replay semantics unchanged | OPEN, confirmed |
| F-04 | P1 | No authorization/RBAC; `security` crate empty | `security/src/lib.rs:1-22` docs-only, zero exports; zero `plomid-security` users; `has_*_privilege → true` still in `types/func.rs:235-239` + `executor/scalar.rs:135-137`; `GrantRole → Created` only | OPEN, confirmed |
| F-05 | P1 | Hand-rolled crypto + MD5 downgrade | `network/scram.rs` hand-rolled SHA-256/HMAC/PBKDF2/base64, no audited crypto crates; `session.rs:315-320` non-constant-time `==`, `:350-391` /dev/urandom + weak fallback salt, `:393-456` hand MD5; MD5 offered (`handler.rs:1031-1050` pid-derived salt) | OPEN, confirmed |
| F-06 | P2 | Lifecycle/mount authority duplicated, unwired | `lifecycle/` 13 files present; zero live `use` outside `crates/lifecycle` (grep `plomid_lifecycle` outside crate → none); server wires `hw+network+txn` only | OPEN, confirmed |
| F-07 | P2 | Filters crate fate unresolved | `filters/` 25 files present; zero `use` in server/network/executor (as audited); refinement: **storage-internal use exists** (`storage/manager/segment.rs:35,70,228-269` `XorFilter` build/load) — so crate is storage-wired but executor-scan-path unwired while `executor/join/bloom.rs:52` duplicates membership-filter role. Decision still required | OPEN, confirmed (narrowed) |
| F-08 | P2 | Production defaults/runbooks incomplete | `max_connections = 100_000` (`server/mod.rs:99`); timeouts default `0` = disabled (`session.rs:91-96`); default creds `plomid/plomid` (`config.rs:218-234`); no migration/upgrade framework; no backup/restore/shutdown/disk-full runbooks | OPEN, confirmed |
| F-09 | P2 | No at-rest encryption / audit-log / retention | Negative evidence unchanged — no such code; scope doc still missing | OPEN, confirmed |
| F-10 | P2 | Audit-relevant metrics incomplete | No engine-Mutex wait, WAL bytes/fsync rate, buffer hit-rate, prune-effectiveness histograms in reviewed paths; counters exist but not these | OPEN, confirmed |
| F-11 | P2 | No asserted 10K–10M scale matrix / chaos qualification | No scale-matrix report in repo (only raw `benchmark_results/` + unasserted `htap_interference` A–H + phase harnesses); no kill/disk-full/fsync-error gates | OPEN, confirmed |
| F-12 | P2 | No SBOM / provenance / signing / update SLA | `cargo deny check` still ok (see §8); no SPDX/CycloneDX, no cosign/sigstore, unpinned CI actions, no Dependabot/`cargo audit` gate observed | OPEN, confirmed |
| F-13 | P3 | Error-code docs / wire mapping incomplete | Codes exist (`core/error.rs`), exhaustive mapping test absent | OPEN, confirmed |
| F-14 | P3 | COPY jail TOCTOU + blocking fs in async + privilege scope | `handler.rs:3267-3294,878-1001` canonicalize+starts_with without O_NOFOLLOW/fd pinning; `std::fs` in async COPY/registry paths; COPY runs with server privileges | OPEN, confirmed |
| F-15 | P3 | ART/BTree durability split + spill split undocumented | `index/tree.rs:70` ART non-durable needs operator doc + rebuild-equivalence test; `group.rs:135-141` split needs cross-link | OPEN, confirmed |

## 8. Current server defaults (verified, not changed)

From `crates/server/src/config.rs` + `crates/network/src/`:

- Bind: `--host` default `127.0.0.1` (loopback), `--port` default `5432`, `--data` default `./data`, `--copy-dir` default `<data-dir>/copy`.
- Auth: `--username`/`PLOMID_USER` default `plomid`, `--password`/`PLOMID_PASSWORD` default `plomid` (public pair); `--auth`/`PLOMID_AUTH` default `scram`, accepts `scram/password/md5/none`; `none` on non-loopback is rejected at startup (`config.rs:515-518`).
- TLS: `--tls` off by default, `--tls-cert/--tls-key` empty by default, `--tls-required` off; `--tls-required` without `--tls` fails closed (`config.rs:512-514`). Non-loopback bind without TLS fails closed unless `--allow-remote-plaintext` / `PLOMID_ALLOW_INSECURE_REMOTE=1` (`config.rs:519-526`), with a logged warning; auth still required.
- Checkpoint: enabled by default; 64 MiB WAL bytes / 16 segments / 900 s interval (`config.rs:279-310`), with edge-tier substitution for unset flags (`apply_tier_checkpoint_defaults`); `--no-auto-checkpoint` disables.
- Connections: `max_connections = 100_000` via `Semaphore` fail-fast (`server/mod.rs:90-132`) — audited as overly large, unchanged.
- Timeouts: `statement_timeout / idle_in_transaction_session_timeout / lock_timeout` default `0` = disabled (`session.rs:91-96`) — unchanged.
- Secrets: `Debug` redacts password (`config.rs:87-107`, test `debug_redacts_password` pins it).
- Config tests pin fail-closed remote behavior (`config.rs:598-636`).

## 9. Current Docker defaults (verified, not changed)

From `Dockerfile:31-73`:

- Base `debian:bookworm-slim`, binary-only copy (`dist/linux-${TARGETARCH}/plomid-server`), `USER plomid:plomid`, `WORKDIR /var/lib/plomid/data`, `VOLUME /var/lib/plomid/data`, `EXPOSE 5432`, TCP-connect `HEALTHCHECK`.
- `ENV PLOMID_HOST=0.0.0.0 PLOMID_PORT=5432 PLOMID_DATA_DIR=/var/lib/plomid/data PLOMID_ALLOW_INSECURE_REMOTE=true` — the F-02 insecure-remote bake-in is still present; the file's own comment acknowledges the opt-in + warning and tells operators to pass `PLOMID_PASSWORD` when publishing the port.

## 10. Vector implementation state (verified, not changed)

- Query/execution: LIVE and well-gated (exact heap TopK + ANN HNSW/IVF/DiskANN/SQ/PQ/sparse with pinned-read/freshness/1M-row gates, scalar/CPU fallback, EXPLAIN parity) — keep.
- Persistence: PARALLEL system (F-01) — own files/manifest/sync/visibility/backup/GC, zero reuse of `storage/wal/mvcc` — unchanged.
- Hardware: CPU dispatch live (`hw` + `vector/math/dispatch.rs`); GPU is a config seam only (`backend.rs:415-512` stub always-Err, `gpu=[]` feature) — not real support.
- No "drop index files → online rebuild" gate in the live server path (as audited).

## 11. Baseline health checks (targeted only — full suite is a later gate)

| Command | Result (2026-10-08) |
|---|---|
| `cargo fmt --all -- --check` | PASS (exit 0, no diff) |
| `cargo test -p plomid-mvcc --lib` | PASS — 4 passed, 0 failed |
| `cargo test -p plomid-server --lib` | PASS — 10 passed, 0 failed (config policy, fail-closed remote, redaction, tier defaults) |
| `cargo clippy -p plomid-core --all-targets` | PASS (clean, exit 0) |
| `cargo deny check` | PASS — `advisories ok, bans ok, licenses ok, sources ok` |
| `cargo tree --depth 1 -p plomid-server` (audit-era reference) | Replaced by direct manifest read: server deps = `hw+network+txn` only (matches audit §03 server-wiring note) |

Full `cargo test --workspace`, gauntlet qualification, and any destructive/crash runs were **not** executed in Phase 0 per the audit rules (they are Phase 24–25 / roadmap gate #14).

## 12. Baseline performance references (no new measurements in Phase 0)

- `benchmark_results/benchmark_report.md` (generated 2026-10-05): 100K-row, 10K batch, 100 repeats, 8 workers / 200 concurrent ops, macOS-15.4-arm64, Python 3.12.4 — e.g. 10M-row-insert table (PostgreSQL 3.5027 s vs PLOMID 14.9587 s on the sampled shape) plus p50/p95/p99 latency tables. Raw material only; **not** an asserted scale matrix.
- `benchmark_results/benchmark_results.json`, `comparison.csv`, `vector-BENCHMARKS.md`: same raw-material status.
- Crate benches: 22 targets across 9 suites (columnar 6, executor 3, vector 2, storage 4, index 2, hw 1, filters 1, crc32c 1, txn 1).
- Harnesses: `benchmarks/harness/` (runner/run_all/compare/schema/workloads) + `benchmarks/production/phase-00..26` + `tests/perf/*.py` (17 drivers) + `tests/python/*.py` (11) + `tests/*.sql` (12 qualification scripts incl. `PLOMID_FULL_PRODUCTION_QUALIFICATION.sql`, `PLOMID_TORTURE.sql`, `VECTOR.sql`).
- Gauntlet: `crates/gauntlet/src/` (freshness, harness, htap, metrics, olap, oltp, report, resource, scale, vector) — qualification harness, test-only, never runtime.
- Audit verdict carries over: architecture is performance-sane (bounded spill/stream/memory, gated parallelism, fail-closed HTAP); **no 10K–10M × workload matrix with p50/p95/p99/p99.9/max + CPU/RSS/WAL accounting exists** (F-11). Do not claim scale in Beta 2 until the matrix gate runs.

## 13. Phase 0 exit

- [x] `git status` inspected (clean except `audit/`)
- [x] HEAD recorded (`0877cb0` = audited commit)
- [x] Branch recorded (`starting-v0.1.0-beta-2`)
- [x] Rust/cargo versions recorded (1.99.0 / 1.99.0)
- [x] Workspace members inspected (26, listed)
- [x] `Cargo.lock` inspected (136 pkgs, 1,233 lines, key pins)
- [x] Existing `/audit/` inspected (2 files)
- [x] Current docs inspected (94 files, gaps listed)
- [x] Changes since audited commit identified (**none**)
- [x] Targeted health checks run (fmt, mvcc, server-config, clippy-core, deny — all green)
- [x] Vector state verified (query live, storage parallel, GPU seam)
- [x] Server defaults verified (loopback default, fail-closed remote rule, public default pair, timeouts/conn limits as audited)
- [x] Docker defaults verified (insecure-remote bake-in still present)
- [x] No production source modified (only this file created)

Next: Phase 1 (TLS / remote transport security) per the foundation plan. This baseline is the diff-anchor for every later `audit/beta2/*` artifact.
