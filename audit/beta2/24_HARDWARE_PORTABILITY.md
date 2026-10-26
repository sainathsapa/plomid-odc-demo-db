# Beta 2 — 24 Hardware / Portability Qualification (Phase 24)

Date (UTC): 2026-10-09. Host: Apple M2 Pro, arm64, Darwin.
`rustc 1.99.0`. Commit: `7acf160` + working tree.

## Measured here (Apple Silicon — this host only)

| Check | Command | Result |
|---|---|---|
| Build | `cargo check --workspace` (via clippy gate) | clean, 0 warnings on touched crates |
| HW unit + parity | `cargo test -p plomid-hw` | 30 + 10 passed |
| SIMD backend selection | `hw/src/vector` runtime dispatch (widest supported; scalar forced in tests) | NEON selected on this host; scalar forced path green |
| Scalar fallback | `VectorOps::force_backend` tests + parity suite | bit-identical folds enforced (`((a0+a1)+a2)+a3`) |
| SQL correctness | `plomid-executor` suites (incl. backup 4/4, chaos 13/13) | green |
| OLTP / OLAP / HTAP | gauntlet families (`make gauntlet` CI gate) | green in CI; scale points measured §11 |
| Vector exact | `cargo test -p plomid-vector --lib distance` | 13 passed |
| ANN | `cargo test -p plomid-gauntlet --lib vector` | 1 passed (34-suite vector qualification in full gate) |
| Startup | server open + WAL replay + checkpoint select | exercised by every reopen test above |
| Correctness | scale exactness (counts/spots/ranges), 20K-request checksum | green |

## Matrix (honest state — do not claim beyond row 1)

| Family | Status |
|---|---|
| Apple Silicon ARM64 | GREEN (this report) |
| Raspberry Pi ARM64 | UNMEASURED |
| AMD x86-64 | UNMEASURED (CI `test` runs x86-64 ubuntu-latest: build+suite green there per CI, not a perf matrix) |
| Intel x86-64 | UNMEASURED |
| Linux x86-64 | CI build+test only, no perf numbers |
| Linux ARM64 | CI `test-arm` (hw parity + txn lib) only |
| AWS ARM64 / x86 vCPU | UNMEASURED |
| Docker constrained CPU/memory | UNMEASURED |

Portability is by construction (runtime feature detection + scalar
fallback; no `std::arch` without a guarded fallback — `hw/src/vector`
`scalar.rs` always compiles). But construction is not measurement: every
UNMEASURED row stays UNMEASURED until run.

## GPU position (binding)

GPU is a config seam only (`backend.rs` stub always-Err, `gpu=[]`
feature). It is **not** support and must never be marketed as such. It
stays unimplemented unless demand proves the need.

## Status

GREEN (Apple Silicon row measured). All other rows explicitly UNMEASURED.
