# 11 — Test quality and coverage gaps

Inventory (current tree): ~2,462 `#[test]` fns across ~370 `tests/*.rs`
integration files + in-`src` units; 22 criterion benches (never run under
`cargo test`); 33 `#[ignore]` scale/soak gates (Tier D, nightly `scale.yml`);
fuzz differentials default to 5 cases locally (`PLOMID_FUZZ_CASES=200` in
scale); crash/recovery/concurrency/corruption suites present per crate
(e.g. `vector_durability_convergence`, `wal_recovery_adversarial`,
`mvcc_adversarial`, `chaos_matrix`, `backup_restore`).

## False-confidence risks checked

- Mocks replacing disk: in-process engines use **real temp-dir storage**
  (scratch + cleanup pattern), not mocks — good.
- SQL/PGWire bypass: engine tests bypass the wire (expected layering); wire
  behavior covered by `network/tests/*` incl. float-division over simple +
  extended + bind params (added for P0-002) — good, and this audit's live
  probes cover the wire independently.
- Same-implementation oracles: fuzz differentials triangulate SQL vs
  independent Rust hand-folds + fresh engines — acceptable; my live probes
  used fully independent Python oracles.
- Fixed-seed fixtures: deterministic seeds everywhere (reproducible, but a
  determined seed-specific blind spot can't be excluded) — P3 note.
- Skipped: 33 ignored gates all documented Tier D with explicit commands;
  none conceals a correctness failure (the storage soak gating this week kept
  a deterministic regression in CI — the sanctioned pattern).
- Noisy benchmarks as gates: benches are compile-check only in CI — good.
- Platform-specific: ARM NEON suite runs natively in CI (`test-arm`) — good.

## Commands NOT RUN (bounded audit)

Full `cargo test --workspace` (>30 min cold, loaded box); `make test-scale`
(100K/300K/1M soak, nightly scope); `make bench-check`; `make gauntlet`;
Docker build/run; `cargo cyclonedx`. Cited instead: prior CI evidence where
available, targeted crate runs performed in the immediately preceding test
audit (storage unit_generation 10/1, network unit_tls 4/4, executor
par_sort/par_fuzz reduction validation), and this audit's live probes.
`cargo fmt --check` equivalent: clean (run during test audit).
`cargo deny check`: exit 0 (this audit).
