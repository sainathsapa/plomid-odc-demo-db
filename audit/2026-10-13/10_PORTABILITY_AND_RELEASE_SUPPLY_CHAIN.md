# 10 — Portability and release supply chain

## Tested vs compile-only vs unsupported

| Platform | Status |
|---|---|
| macOS arm64 (M2 Pro) | **TESTED** (this audit: live server + probes) |
| macOS x86_64 | compile-only (CI matrix; no runtime here) |
| Linux x86_64 / ARM64 | compile-only (release workflow builds `--locked`; Docker path; no runtime here) |
| Windows | unsupported (no claim found; no evidence) |
| SIMD: NEON live (`Backend: cpu(neon)` in EXPLAIN); x86 AVX2/AVX-512, scalar fallback | NEON live; others via `hw/tests/parity.rs` (not re-run here) + criterion benches |
| GPU | config seam only (`GpuStub` → CPU fallback); do not market |

Debug/release differences: release binary used throughout; debug behavior NOT
compared (scale tests run debug in CI by default — note for gate parity).

## Supply chain (verified 2026-10-13)

- `cargo deny check`: **pass** (advisories/bans/licenses/sources ok).
- `cargo fmt --check`: **pass**.
- SBOM: `dist/artifacts/plomid-0.1.0-beta.2-sbom.spdx.json` (201 pkgs) +
  release manifest shipped (Δ since prior audit — closes prior SBOM gap).
- Reproducibility: `--locked` release builds, pinned toolchain 1.99.0, no
  vendoring, no git/path deps outside workspace, no build.rs/proc-macros.
- Gaps: **no artifact signing / SLSA attestation** (acknowledged residual in
  commit `7850889`); Homebrew/docker push `continue-on-error`; no update-SLA bot
  evidence; clean-machine install drill NOT RUN.
