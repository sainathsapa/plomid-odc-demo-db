# 12 — Verification gates and portability

Evidence: `artifacts/suite/` (`fmt-check.txt`; `test.txt` = compile
progress only; `INTERRUPTED.md` = scope-cut record),
`artifacts/executor-tests/executor_vector_tests2.log`,
`artifacts/qualify/qualify.log`.

## Gate status for commit `6890955` (binary `8b0a6b60…972f`)

The full workspace gate chain (`test` → `check-ci` → `clippy-ci` →
`deny` → `release-locked`) was launched this cycle and **stopped by
operator decision** after ~51 minutes, still inside the one-shot
`cargo test --workspace --jobs 2` compile/link of the large test
binaries. No gate in the chain completed. This is a deliberate scope
cut recorded as an evidence gap — **not** a pass and **not** a
failure. Verification weight for this audit rests on the targeted
suites below and the live probe matrix.

| Gate | Command | Result |
|---|---|---|
| Workspace tests | `cargo test --workspace --jobs 2` | **INTERRUPTED (scope cut)** — SIGTERM at ~51 min, still compiling; zero test results (`INTERRUPTED.md`, `test.txt`). |
| Type check (CI) | `cargo check --workspace --lib --tests` | **NOT RUN** — chain stopped before this step. |
| Clippy (CI) | `cargo clippy --workspace --lib --tests -- -D correctness/suspicious/complexity/perf` | **NOT RUN** — chain stopped before this step. |
| Dependency/licence policy | `cargo deny check` | **NOT RUN** — chain stopped before this step (`cargo-deny 0.20.2` installed, gate never executed). |
| Locked release build | `cargo build --release -p plomid-server --locked` | **NOT RUN** as a gate — the audited binary was built from this tree earlier this cycle and sha-256-verified (`8b0a6b60…972f`; every probe ran against it), but the dedicated `--locked` re-run was not completed. |
| Formatting | `cargo fmt --all --check` | **FAIL (exit 1)** — 24 hunks across 9 files, pre-existing drift: `crates/executor/src/query/aggregate.rs` (5), `project.rs` (1), `window.rs` (2), `tests/p2_avg_drop_role.rs` (4), `tests/unit_query_aggregate.rs` (2), `crates/sql/src/ast.rs` (1), `catalog/encoding.rs` (2), `catalog/mod.rs` (4), `tests/concurrent_ddl_ids.rs` (3). |

Targeted suites (all green, this cycle):

- qualify gate: **83 passed / 0 failed** (`qualify.log`).
- ANN quality matrix: **2 passed / 0 failed** (112.8 s,
  `quality-matrix/quality_final.log`).
- Executor vector test binaries: **13 binaries / 110 tests passed**
  (`executor-tests/executor_vector_tests2.log`).
- Live probe suites: p0 30/30; vector_commit 77/77; workload 11/11;
  txn 7/7; backup 12/12; rbac 14/14; catalog 11/12 (1 real FAIL =
  F-004); namesake 2/4 (by-design FAILs = F-004); param_avg/ILIKE 11/13
  (2 real FAILs = F-005); scale 1/1 with notes; storage split repro +
  control (F-001).

Notes:

- The fmt drift predates this audit and was **not** introduced by the
  audit artifacts (probe scripts live under `audit/`, excluded from the
  workspace; `artifacts/suite/fmt-check.txt` is the raw output).
- The gate run is the audit's own execution of the Makefile/CI commands;
  it is not a claim about the project's CI history.

## Portability

- All measurements: macOS arm64 (Apple M2 Pro, 12 cores, 16 GiB), local
  disk, loopback networking.
- Linux x86_64 / ARM64 runtime: **NOT RUN** (G-05 remains open).
- Docker image / cross-build path: not exercised this cycle.
- Windows: out of scope.
