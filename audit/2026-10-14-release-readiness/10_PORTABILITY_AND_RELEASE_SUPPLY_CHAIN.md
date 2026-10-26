# 10 — Portability and release supply chain

- Build matrix claimed: macOS arm64/x64, Linux x64/arm64, Windows x64
  (release.yml). **Runtime-verified here: macOS arm64 only** (debug + release
  binaries built from HEAD and exercised). All other platforms: compile-only
  evidence via CI (not inspected run-by-run this audit) → EVIDENCE GAP P2.
- SIMD: AVX2/NEON + scalar fallback live by design; AVX-512/SVE execute
  narrower kernels (labelled); ARM NEON parity suite runs in CI
  (`test-arm`); x86 runtime NOT RUN here.
- Debug vs release: both built from HEAD (`2m21s` incr debug,
  `58.93s` incr release, exit 0); release exercised for DDL/DML/float.
- Dependencies: `cargo deny check` exit 0 (advisories/bans/licenses/sources
  ok) after rustls 0.23.45→0.23.46 bump (RUSTSEC-2026-0346/0347+); remaining
  `warning[duplicate]` (syn/r-efi/getrandom, policy `warn`) and unused
  BSD-2-Clause allowance are warnings-only by explicit policy.
- SBOM: SPDX generated in release pipeline (not re-run here); SLSA
  attestation still missing (prior gap, unchanged). Signatures: cosign
  keyless in pipeline (config only, not verified live).
- Reproducibility: locked tree (`Cargo.lock` committed), pinned toolchain
  1.99.0, `--locked` release builds. Deterministic-build diff NOT RUN.
