# 01 — Provenance and delta

## Target

- Commit under audit: `68909555b15840716fe105f47deeb0f81000f01e`
  ("fix vector contracts: executable-only DDL, honest ANN claims, quality
  evidence"), branch `starting-v0.1.0-beta-2`, 22 files, +1819/−130.
- Baseline for comparison: `c9298e9` ("fix vector ann and vector contracts"),
  audited in `audit/2026-10-15/`.
- Working tree during the audit: clean (`git status --short` = 0 entries at
  audit start). No product source file was modified by this audit.

## Binary

- `target/release/plomid-server`
  sha256 `8b0a6b60a3dcefb8b95103fe999e5416342be075937de0b9814e42fd061e972f`
- Reproducibility: a no-op `cargo build --release` after the commit left the
  hash byte-identical.
- All live probes in this bundle ran against this binary, booted from
  disposable data directories.

## Environment

- Host: macOS arm64 (Apple M2 Pro), 2026-10-16.
- Toolchain: rustc 1.99.0 (b940084d7 2026-09-28); Python 3.12.4;
  psycopg 3.2.9; OpenSSL CLI for self-signed TLS material.
- Server boot recipe used everywhere:
  `./target/release/plomid-server --data <DIR> --host 127.0.0.1 --port <P>
  --username plomid --password <SECRET> [--log-level warn|debug]`
- Backup/restore tools: `plomid-server backup --data <DIR> --to <BK>`,
  `restore --from <BK> --to <CLEAN>`, `backup-inspect --backup <BK>`.

## Delta vs the audited baseline (c9298e9 → 6890955)

| Area | Change |
|---|---|
| DDL/runtime contract | One executable envelope (`vector_ann::method_executable_for_type` + `method_rejection_hint`) shared by CREATE INDEX validation, runtime planning, auto-index registration, and operator views; `ivf_pq` rejected at DDL when `dim % 8 != 0`; dense ANN restricted to Dense kinds; `flat` accepted everywhere; binary Jaccard stays exact-only; Hamming ANN requires explicit `USING BINARY`. |
| Capability/type metadata | `supports_sq` includes Int4; `supports_pq` includes Int8/Uint8; auto-index registers HNSW for Dense columns only. |
| Operator views | `plomid_vector_indexes()` reports READY only when the recorded method can execute for the column type; otherwise DEGRADED with a hint. |
| Docs | `ivf_sq`/`ivf_pq` inverted-list claims removed (global quantized scans; only `ivf_flat` partitions); measured ANN boundaries documented (PQ dim-8 cosine, DiskANN tight/clustered drops, binary non-word-aligned Hamming recall). |
| New tests | `vector_index_contract.rs` (DDL envelope matrix), `vector_index_execution_proof.rs` (every accepted dense method executes + reuses cache at dim 8), `ann_quality_matrix.rs` (independently generated fixtures, exact oracle, calibrated floors). |
| New harness | `tests/python/vector_lifecycle_bench.py` (cold/warm/rebuild/restart for HNSW + DiskANN at 2K/8K/32K/128K). |
| SQL gate | `VECTOR_FULL_QUALIFICATION.sql` gains the dim-4 `ivf_pq` EXPECT-ERROR case. |

Everything else in the product is byte-identical to `c9298e9`. In particular,
no storage-engine, WAL, catalog, authz, or protocol code changed in this
tranche — the pre-existing storage B+Tree defect reported in section 09 was
out of scope by explicit instruction and is documented, not fixed.

## Pre-existing formatting drift (NOT introduced by this commit)

`cargo fmt --all --check` reports drift in files last modified by `451937c`:
`crates/executor/src/query/aggregate.rs`, `crates/executor/src/query/project.rs`,
`crates/executor/src/window.rs`, `crates/executor/tests/p2_avg_drop_role.rs`,
`crates/executor/tests/unit_query_aggregate.rs`. All files touched by
`6890955` are rustfmt-clean.

## Command inventory (evidence sources)

- New bundle raw logs: `artifacts/` (probes, lifecycle, storage-split,
  quality-matrix, executor tests, qualify gate, scale, suite).
- Logs carried over from the instrumentation phase of this audit session
  (same binary, same tree):
  - `artifacts/qualify/qualify.log` — `vector_qualify.py`: 83 passed / 0 failed
  - `artifacts/quality-matrix/quality_final.log` — `ann_quality_matrix`: 2
    passed in 112.83s
  - `artifacts/executor-tests/executor_vector_tests2.log` — 13 vector-related
    executor test binaries, 110 tests, `--no-fail-fast`, all green
