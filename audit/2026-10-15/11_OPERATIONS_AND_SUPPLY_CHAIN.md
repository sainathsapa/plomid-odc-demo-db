# 11 — Operations and supply chain

- Shutdown: graceful (SIGTERM after probes, clean exit); crash (SIGKILL)
  recovery verified (§05). Startup failure (non-loopback w/o TLS) fails
  closed with actionable message. Corrupt input (missing manifest) errors
  `PL-CORRUPTION`, no panic. Operator visibility: server logs + `--help`
  runbook surface; `help-backup` documents the cold-only contract.
- Resource pressure (disk-full/WAL-full/OOM, connection ceiling): NOT RUN.
- Dependencies: `cargo deny check` PASS (advisories/bans/licenses/sources
  ok). No new dependencies added by the audit (none added at all).
- Provenance/SBOM/signing: SBOM artifact exists in-tree (`dist/artifacts/…
  sbom.spdx.json`, unchanged); SLSA attestation missing (prior residual,
  still open); release workflow/checksums not re-exercised this session.
- Release packaging adequacy: local/loopback distribution supportable from
  this binary + cold-backup runbook; remote/managed claims need the
  attestation + multi-arch proof (§12).
