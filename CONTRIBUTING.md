# Contributing to PLOMID

PLOMID is developed as a Rust workspace. Read the [coding standards](docs/coding-standards.md)
and the [ADR process](docs/adr/README.md) before making changes.

## Local development

From the repository root, run: 

```bash
cargo build --workspace
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
make test-ci
cargo deny check
```

Install `cargo-deny` if it is not already available. CI runs the same quality
gates, including the dependency and license policy.

### Test tiers

| Tier | Command | What runs | Where |
| ---- | ------- | --------- | ----- |
| A — fast unit | `make test-unit` | isolated `src/` unit tests, no scale/bench workloads | every PR + release |
| B — bounded integration | `make test-integration` | all `tests/` targets (SQL, txn, storage, index, WAL/recovery, protocol, regressions); `#[ignore]` scale gates off | every PR + release |
| C — required verification | `make test-ci` (= A + B, plus CI jobs for fmt/check/clippy/deny) | routine correctness gate, 30-min budget | every PR + release |
| D — heavy/explicit | `make test-scale` (`--include-ignored` + `PLOMID_SCALE_ROWS=100000 PLOMID_FUZZ_CASES=200`), `make bench-check`, `make gauntlet` | scale/load/soak/fuzz-200, criterion benches, HTAP Gauntlet | nightly schedule or manual dispatch only |

Rules: never run Tier D in the default gate; never `#[ignore]` a real
correctness test to get green CI (genuinely expensive variants move to Tier D
with a smaller bounded regression kept in Tier B); every CI job has an
explicit `timeout-minutes`, and a timeout or disk-exhaustion failure is a
failure, never a pass.

Install `cargo-deny` if it is not already available. CI runs the same quality
gates, including the dependency and license policy.

## Pull requests

- Keep each pull request focused on one task and preserve the dependency order
  of the roadmap.
- Add or update tests for behavior changes. Storage, WAL, recovery, MVCC, and
  transaction changes require explicit correctness and failure-path coverage.
- Document public API or architectural changes. Add an ADR for a material
  design decision and link it in the pull request.
- Explain validation performed, known limitations, and any follow-up work.
- Do not add secrets or forbidden external database foundations.

Pull requests require review from the owners in [`CODEOWNERS`](CODEOWNERS) and
must pass the required CI checks.

## Maintainer

- **Copyright:** © 2026 PLOMID Technologies INDIA
- **Author:** Sainath Sapa
- **Email:** info.djsai@gmail.com
- **GitHub:** https://github.com/sainathsapa (`@sainathsapa`)
- **Organization:** PLOMID Technologies INDIA — https://github.com/plomid
