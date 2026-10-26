# 12 — Previous-audit delta (0877cb0 → bcd6bf4, 25 commits)

Prior verdict: B-, viable loopback single-user dev with limits; blockers TLS,
authZ, crypto, txn replay, vector durability split, lifecycle/filters wiring,
metrics, scale/chaos proof, SBOM. Re-verified 2026-10-13 (code + live).

| Prior finding | Then | Now | Evidence |
|---|---|---|---|
| F-02 P0 remote TLS unimplemented | BLOCKED | **FIXED, live-verified** | `b0cfcfc`; `probe14`: handshake + refusal + bind guard |
| F-04 no authorization / empty `security` | BLOCKED | **FIXED, live-verified** | `ae8dcd7`; `probe8/8b`: 42501 deny + grant-allow |
| F-05 hand-rolled crypto / MD5 / non-CT compare | P1 | **FIXED** | `254b487`; `credential.rs:37-206` audited crates, tests with RFC vectors |
| F-03 explicit-txn SQL-text replay | P1 | **FIXED, live-verified** | `70e265f`; overlay staging + savepoints + abort semantics probed |
| F-01 vector parallel durability | P1 | **FIXED by design, live-verified (exact path)** | `cbc6c31`; doctrine + derived cache; restart k-NN identical |
| lifecycle unwired mount authority | P2 | **FIXED** (crate removed, merged into `PlomidStorageEngine::open`) | `34de456`; `ls crates/` |
| filters unwired (Roaring) | P2 | **FIXED** (Roaring removed, XorFilter kept) | `3fe1c1e` |
| insecure-remote baked into Dockerfile | P0-remote | **FIXED** (fail-closed + explicit opt-in) | `Dockerfile:55-69`, `Makefile:147-160`, live bind refusal |
| COPY TOCTOU | P1/P2 | **FIXED** | `f7f74ed`; `O_NOFOLLOW` walk + live denial |
| mid-txn reads / group results | gap | **FIXED, live-verified** | `92de3d4`; `probe2` |
| DDL self-deadlock / settings bypass | bug | **FIXED** (commit `42702d4`, `13478d1`, `7d5e9b4`) | code reviewed; DDL probed OK |
| observability (perf/audit counters) | gap | **PARTIALLY FIXED** (tracing events live; lock-WAIT/WAL/IO metrics still thin) | server logs show events; `11` gaps |
| scale matrix 1K/10K/100K/300K asserted | gap | **PARTIALLY FIXED** (ignored-gated tests exist; 100K measured here; 300K/1M not run) | `4f7b4bc`, `probe13` |
| SBOM/provenance/signing | P2 | **PARTIALLY FIXED** (SPDX + manifest ship; SLSA/signing missing) | `7850889`, `dist/artifacts/` |
| audit-log sink | gap | **FIXED** | `16c4630`; `--audit-log` flag (emission not load-tested) |
| safe production defaults | gap | **FIXED** | `3db8662`; timeouts/caps in startup dump |
| error catalog / docs / RBAC runbook / TLS docs | P3 | **FIXED** | `58dede8`; docs tree reviewed |
| **NEW P0-001 ext-protocol truncation** | — (latent) | **OPEN, release-stopping** | `probe15-17`, `handler.rs:1706,1835-39,2575-2650` |
| **NEW P0-002 float folding** | — (latent) | **OPEN, release-stopping** | `probe11/12b`, `expressions.rs:1940-49` |
| **NEW P1-003 catalog staleness** | — (latent) | **OPEN** | `probe9` |
| **NEW P1-004 backup unwired** | flagged as test-only | **OPEN as capability gap** | grep proof |

Net: every fixable prior blocker was fixed and verified; the audit's value this
round is the two latent P0s plus two P1s that only live PGWire probing exposes.
No regressions found in previously-green areas (row durability, MVCC, RBAC,
TLS all re-probed green).
