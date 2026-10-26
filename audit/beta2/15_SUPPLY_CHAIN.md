# Beta 2 — 15/16 Supply Chain / SBOM / Release Integrity (Phase 16)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.

## Dependency controls — GREEN

- `cargo deny check`: **ok** (advisories/bans/licenses/sources) — run
  2026-10-09, `deny.toml` allow-list (Apache-2.0/MIT/BSD/ISC/Unicode),
  `wildcards=deny`, git deps denied, crates.io-index only.
- `cargo audit`: new CI job `.github/workflows/ci.yml:audit`
  (`cargo install cargo-audit --locked && cargo audit`). Advisories were
  previously covered only via `deny[advisories]`; now both gates run.
- `Cargo.lock` committed (201 packages at report time).

## CI integrity — GREEN

- All `uses:` pinned to immutable SHAs with moving-tag comments
  (`ci.yml`: checkout `11d5960a`, toolchain `70dec608`, cache `49a0bdc7`,
  deny-action `b66acf5e`, upload-artifact `ea165f8d`; `release.yml` same
  + anchore/cosign/qemu/buildx/login/build-push SHAs). No unpinned action.
- Dependabot: `.github/dependabot.yml` (cargo + actions, weekly, limit 5).

## SBOM — GREEN (generated)

- Release: `release.yml:sbom` (anchore `e22c3899`, SPDX JSON →
  `dist/artifacts/plomid-<tag>-sbom.spdx.json`, uploaded).
- Local evidence (this report):
  `dist/artifacts/plomid-0.1.0-beta.2-sbom.spdx.json` — SPDX 2.3,
  201 packages from `Cargo.lock` (transitive, purl refs), sha256
  `48115a0a5be7ad8c32a4d53a37a757531e1b99f1d47beb562b2feb57e2d95a97`.
  Generator: stdlib-only lockfile parser (no new dependency).

## Artifact integrity — YELLOW

- Release: `SHA256SUMS.txt` + keyless Sigstore `cosign sign-blob`
  (`.sig`/`.pem` per file, `id-token:write`) — in `release.yml:publish`.
- SLSA `attest-build-provenance`/`attest-sbom`: MISSING (blob signing
  only). SHA-256 alone is checksums, not provenance — per plan, do not
  treat it as such.
- Local manifest: `dist/artifacts/plomid-0.1.0-beta.2-manifest.json`
  (commit, toolchain `1.99.0`, build env, timestamp, SBOM hash, deny
  status) — process evidence, not a release attestation.

## Update process — GREEN (documented)

`SECURITY.md`: response SLA table, supported versions, supply-chain
section (lockfile + deny + Dependabot + SPDX + Sigstore + SHA-pinned
actions), advisory process. Vulnerability SLA + update procedure live
there, not duplicated here.

## Status

GREEN: deny ok, audit job added, SHAs pinned, Dependabot on, SPDX
generated (201 pkgs), release signing in CI. YELLOW: SLSA provenance
attestation missing (tracked, not claimed).
