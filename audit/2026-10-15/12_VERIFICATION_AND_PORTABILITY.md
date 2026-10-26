# 12 — Verification and portability

Canonical gates (`Makefile`: `fmt-check`, `check-ci`, `clippy-ci`, `deny`,
`test-ci` = `cargo test --workspace --jobs 2`, `release`).

| Gate | Result |
|---|---|
| `cargo fmt --all --check` | PASS (exit 0) |
| `cargo check --workspace --all-targets` | PASS (exit 0) |
| `cargo clippy --workspace --lib --tests` (CI deny set) | PASS (exit 0; 1 advisory style warning) |
| `cargo deny check` | PASS |
| `cargo build --release -p plomid-server` | PASS (exit 0, sha256 `6c861f51…`) |
| targeted: extended_batch 7/7, codec 19/19, numeric 5/5+7/7, catalog 6/6, rbac 9/9, parser 37/37 | PASS |
| `cargo test --workspace` (full) | NOT RUN — operator halted per explicit instruction after targeted suites; never counted as pass |
| Gauntlet/acceptance, release benches | NOT RUN |
| arch/API checks in CI | NOT RUN (no runner invoked) |

Portability: build+runtime PASS on macOS arm64 only. Linux x86_64 / Linux
ARM64 / others: build NOT RUN, runtime NOT RUN — no parity inferred.
Repro commands for CI/target hardware: `make fmt-check`, `make check-ci`,
`make clippy-ci`, `cargo deny check --all-features`, `make test-ci`,
`cargo build --release -p plomid-server` on each target runner.
