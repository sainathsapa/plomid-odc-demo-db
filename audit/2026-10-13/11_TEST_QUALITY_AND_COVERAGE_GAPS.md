# 11 — Test quality and coverage gaps

Inventory: 25 crates, **365 integration test files**, 12 SQL suites
(`tests/*.sql` incl. `VECTOR_FULL_QUALIFICATION.sql`), 18 `tests/perf/*.py`
wire drivers, 12 `tests/python/*.py`, 22 criterion benches, 3 `#[ignore]` scale
tests (1K/10K/100K/300K matrix, 1M soak, insert-scaling) gated to nightly/scale
CI. Canonical gates: `make test-ci` (fast), `make test-full`/`test-scale`
(100K + fuzz-200), `make gauntlet` (six-family qualification).

## Is the suite proving behavior or hand-picked cases?

Mostly honest, with a structural blind spot. Strengths: fail-closed gates
(pinned-read disables ANN, freshness gates, `executable:false` declined paths),
adversarial/numerics/property vector tests, race tests (unique concurrency),
recovery tests. Blind spot now proven: **two P0s lived in seams the suite never
drove end-to-end** — driver-style extended-protocol batches (P0-001) and
float-literal typing through PGWire with client-side parsing (P0-002; unit
`op_divide` is correct, the bug is in parser typing + result metadata).
Mocks replacing disk: columnar/vector derived designs are genuinely on-engine;
legacy vector files are test-only and labeled. `#[ignore]` scale tests exist but
didn't run here. No evidence of benchmark-only shortcuts in product behavior.

## This audit's verification status

- `cargo test --workspace --lib --tests`: **NOT VERIFIED** (started 15:09 UTC,
  still compiling at report time on this 16 GiB host; 0-byte log at last check).
- `cargo clippy --workspace --lib --tests` (project-prescribed flags):
  **NOT RUN** (cargo build lock held by suite + time budget).
- `cargo test -p plomid-gauntlet --lib`: **NOT RUN** (>120 s build wait).
- `cargo fmt --check`: PASS. `cargo deny check`: PASS.
- Live PGWire probes (23 logs): the effective coverage for this report.

## Missing / weak classes

SQL-over-wire differential vs PG; multi-statement driver-batch tests; float
matrix over the wire; catalog-refresh tests for live sessions; backup/restore
drill wired to CI; disk-full/OOM/corruption suites; 1M+ scale in fast CI;
fuzz/property at SQL layer (only deterministic join/agg fuzz exists); portability
runtime beyond arm64; REVOKE-latency test; tail-latency (p99+) benchmarks.
Skipped/ignored: 3 scale tests by design (nightly). No silently-ignored tests
found in the sampled set.
