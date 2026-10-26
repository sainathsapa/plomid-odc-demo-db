# PLOMID — Full Independent Audit Report (Consolidated)

> Single-file consolidation of /audit/README.md + 00–23. Generated 2026-10-08 from commit 0877cb04001447fdfccc7eaca102b60e50c4d237. No production source modified.

---

======================================================================
FILE: README.md
======================================================================

# PLOMID Audit — README

- Audit date: 2026-10-07 (UTC)
- Repository: `/Users/sainathsapa/dsk/PloMID/plomid`
- Commit: `0877cb04001447fdfccc7eaca102b60e50c4d237` (branch `starting-v0.1.0-beta-2`)
- Toolchain: rustc 1.99.0, cargo 1.99.0, pinned `rust-toolchain.toml` channel 1.99.0
- Host: darwin arm64 (Apple M2 Pro)
- Mode: AUDIT FIRST — no production source modified. Verified via `git status --short` (clean).
- Scope: whole workspace (26 crates), live-path wiring, storage/MVCC/WAL, executor/optimizer/vector/HTAP, performance, security, supply-chain, IP, compliance, certifiability, production readiness, test quality.

## Commands executed

- `git rev-parse HEAD; git status --short; git log --oneline -5`
- `rustc --version; cargo --version`
- Per-crate `find crates/* -name "*.rs" | wc -l` + line totals (~539 files, ~273k lines)
- `grep -rn "unsafe"` (135 hits, all `hw`+`crc32c`), `\.unwrap\(\)` (2495), `\.expect\(` (2875), `panic!` (429), `TODO|FIXME` (2 work items)
- `cargo test -p plomid-mvcc --lib` (4 passed), `cargo clippy -p plomid-core --all-targets` (clean)
- `cargo deny check` (advisories/bans/licenses/sources ok; 3 license-not-encountered warnings)
- `cargo tree --depth 1 -p plomid-server`
- Read-only inspection of `Cargo.toml`, `deny.toml`, `clippy.toml`, `rustfmt.toml`, `Dockerfile`, `Makefile`, `.github/workflows/ci.yml`, `release.yml`, `SECURITY.md`, `LICENSE`, `NOTICE`, `crates/*/Cargo.toml`, `crates/*/src/lib.rs`, key hot-path files listed in Evidence Index.

## Report index

| File | Section |
|---|---|
| `00_EXECUTIVE_SUMMARY.md` | Executive report |
| `01_WORKSPACE_INVENTORY.md` | Repository inventory |
| `02_ARCHITECTURE_AND_CRATE_BOUNDARIES.md` | Crate ownership/boundaries |
| `03_LIVE_PATH_WIRING.md` | Live-path wiring (most important) |
| `04_DATA_MODEL_INTEGRATION.md` | Single authoritative data path |
| `05_VECTOR_INTEGRATION.md` | Vector audit |
| `06_FUTURE_MODULE_READINESS.md` | Multimodel readiness |
| `07_CODE_QUALITY.md` | Code quality |
| `08_PERFORMANCE_AND_SCALING.md` | Performance/scale |
| `09_STORAGE_DURABILITY_RECOVERY.md` | Storage/durability/recovery |
| `10_SECURITY.md` | Security |
| `11_SUPPLY_CHAIN_SBOM.md` | Supply chain / SBOM |
| `12_IP_AND_PROVENANCE.md` | IP / provenance |
| `13_COMPLIANCE_READINESS.md` | Compliance mapping |
| `14_AUDITABILITY_AND_CERTIFIABILITY.md` | Auditability |
| `15_PRODUCTION_READINESS.md` | Production readiness |
| `16_TEST_COVERAGE_AND_TEST_QUALITY.md` | Test quality |
| `17_PERFORMANCE_ARCHITECTURE.md` | Perf architecture quality |
| `18_DEAD_CODE_FRAGMENTATION.md` | Dead code / fragmentation |
| `19_EXTENSION_ARCHITECTURE.md` | Extension contracts |
| `20_FINDINGS_REGISTER.md` | Findings register |
| `21_DECISION_MATRIX.md` | Decision matrix |
| `22_REMEDIATION_ROADMAP.md` | Roadmap |
| `23_EVIDENCE_INDEX.md` | Evidence index |

## Limitations

- Single-host (Apple Silicon arm64). x86-64 AVX2/AVX-512, NEON-on-Pi, GPU, Docker quota, cloud vCPU, high-core NUMA paths are classified UNVERIFIED unless covered by existing parity tests/benchmarks.
- No full-workspace `cargo test` run (per audit rules: targeted only). Full suite is a remediation gate (see 22).
- No destructive/crash-injection runs performed; recovery claims rest on code + existing restart/adversarial tests.
- No legal conclusions on licenses/patents; IP section is provenance triage only.
- No network penetration test; security is code-review + config review.


======================================================================
FILE: 00_EXECUTIVE_SUMMARY.md
======================================================================

# 00 — Executive Summary

For: Founder/CTO · Principal engineer · Security reviewer · External auditor · Enterprise evaluator.
Commit `0877cb0` (v0.1.0-beta.2), 26 crates, ~539 files, ~273k lines, toolchain 1.99.0, host darwin/arm64. No production source touched.

## Overall audit grade: B- (strong core, blocked periphery)

Production readiness: **NOT READY for 1.0 remote/multi-user/cloud. Viable loopback single-user dev/beta with documented limits.** No data-loss evidence in the row path; the blockers are transport/auth, txn semantics, vector durability split, and missing scale/chaos proof — all fixable without redesign.

## Architecture health: COHERENT core, ONE violation

One authoritative row path exists and is genuinely live: PGWire → session → parser/binder → executor → txn (commit_prepare/publish) → MVCC → storage → WAL → checkpoint/recovery (03, with file:line traces). Columnar correctly reuses it (derived, rebuildable, fail-closed freshness). Optimizer is narrow/advisory by design — do not inflate it. The single structural violation: **vector index/storage is a parallel database** (own files/manifest/MVCC-visibility/backup, zero reuse of storage/WAL — 04/05, F-01/P1). Everything else flagged (lifecycle/filter/security crates) is unwired-code decisions, not competing engines.

## Live-path/wiring verdict

Row/JSON/analytical/vector-query paths are LIVE with correct fallbacks (row/exact/scalar). ANN is a well-gated accelerator, not a separate truth. GPU is a config seam (stub + CPU fallback), not real — do not market it. Parallel TopK and oversampled-ANN are correctly staged off. Server binary directly constructs only hw+txn+network; the rest lives behind executor per-query gating — acceptable if documented, otherwise server config misleads.

## Biggest fragmentation/dead-code findings

1. `security` crate empty (22 lines, zero callers) — delete or staff it (F-04).
2. `lifecycle` (13 files) unwired mount authority duplicating wal/txn recovery — wire or merge (F-06).
3. `filters` (25 files/4.7k lines) unwired while join Bloom duplicates its role — wire with proof or remove (F-07).
Everything else is intentional layering/staging.

## Biggest performance findings

Architecture is performance-sane (bounded spill/stream/memory, tiered parallelism, measured restraint on parallel TopK). **No scale claims are demonstrated**: the 10K–10M × workload matrix with p99.9 + RSS/WAL accounting does not exist; Pi/x86/cloud/quotas are UNVERIFIED in this audit (existing benches + phase-00..26 harness + htap A–H are the raw material, unasserted). Global engine-Mutex is the contention ceiling — needs a wait-time metric, not a redesign. Do not optimize; measure (F-10/F-11).

## Biggest security findings

P0-remote: **TLS unimplemented** (always-`N`), insecure-remote baked into Dockerfile, public default creds (F-02). P1: single static credential, hand-rolled SHA-256/HMAC/PBKDF2/MD5 with non-constant-time compare + weak fallback salt + MD5 offered (F-05); **no authorization** (`has_*_privilege → true`, F-04). Protocol caps/rate-limiting/jail/perms are good bones. Loopback single-user dev is defensible; anything remote/multi-user/cloud is BLOCKED.

## Biggest IP/supply-chain findings

Original-code posture is good (no vendoring, Apache-2.0 + NOTICE, deny bans copyleft/foreign engines, `cargo deny check` ok). Gaps are evidentiary: **no SBOM (SPDX), no provenance/attestations, no signing** (sha256 only), unpinned CI actions, no update SLA/bot (F-12); CLA/history scan + asset-license confirmation pending (12). No patent opinion possible — counsel screen required for ANN/compression/index if in litigious markets.

## Compliance readiness summary

Technical partial readiness where controls exist (caps, perms, backup/recovery, CI gates); organizational gaps dominate everywhere (ISMS, incident response, vuln SLA, DPIAs, QMS). Every framework in 13 maps to the same five technical closures (TLS, RBAC/audit-log, encryption scope, SBOM/signing, evidence bundle) + an org program. No certification is claimed or earned — correct status throughout 13 is APPLICABILITY REVIEW / EVIDENCE / ORGANIZATIONAL GAP / THIRD-PARTY REQUIRED as applicable.

## Findings count

P0: 1 (F-02 remote TLS) · P1: F-01 (vector durability), F-02, F-03 (txn replay), F-04 (authZ), F-05 (crypto) · P2: F-06–F-12 (lifecycle, filters, defaults/runbooks, encryption scope, metrics, matrix/chaos, SBOM) · P3: F-13–F-15 (error catalog, COPY hardening, docs/tests). Full register: 20.

## Next 5 engineering actions (exact)

1. TLS/auth scope decision + insecure-image fix (NOW, S) — 22 #1–4.
2. RBAC scope decision + empty-crate fate (NOW, S) — 22 #2.
3. Audited-crypto swap + MD5 deprecation (NOW, S) — 22 #3.
4. Explicit-txn true semantics or narrowed guarantee + tests (NEXT, M/L) — 22 #5.
5. Vector durability convergence + kill-matrix gates (NEXT, L) — 22 #6.
Then: mount authority, filters fate, metrics, contract freeze → scale-matrix + chaos + runbooks + SBOM → 1.0 gates.

## What can be safely frozen

Row storage/MVCC/WAL/txn pipeline, columnar-derived design + freshness gating, ANN-as-accelerator framing, spill/stream/memory boundedness, hw dispatch + scalar fallbacks, optimizer-narrow scope, error style, deny policy, test-suite breadth.

## What must be fixed before Graph/KV/Geo/Blob

F-01 (single durability path), F-03 (txn semantics), extension contracts frozen (19 → docs), GC/backup/observability coverage proof. KV/AI-memory then need only small extensions; Graph/Geo/Blob each need one core abstraction (adjacency, spatial index, chunk store) — 06.

## What must be fixed before Production 1.0

NOW+NEXT+_candidate gates in 22 #1–14: TLS/auth/RBAC/crypto, txn + vector durability, mount authority, metrics, scale-matrix + chaos, upgrade/backup runbooks + drills, SBOM/signing, full-suite green.

## What can remain after 1.0

At-rest encryption (unless market requires), cross-arch CI expansion, Miri/fuzz promotion, COPY hardening polish, error-catalog docs, GPU kernels (only on demand).

## Report location

Complete audit under `/audit/` (README + 00–23). Start at `00_EXECUTIVE_SUMMARY.md` (this file), then `03_LIVE_PATH_WIRING.md`, `20_FINDINGS_REGISTER.md`, `22_REMEDIATION_ROADMAP.md`. Evidence for every claim: `23_EVIDENCE_INDEX.md`.


======================================================================
FILE: 01_WORKSPACE_INVENTORY.md
======================================================================

# 01 — Workspace Inventory

Commit `0877cb0`, version `0.1.0-beta.2`, edition 2021, rust-version 1.99.0, license Apache-2.0.

## Workspace members (Cargo.toml:2-29) — 26 crates

core, crc32c, storage, wal, lifecycle, mvcc, txn, sql, optimizer, json, executor,
network, security, server, cli, types, columnar, index, filters, htap, hw, spill,
stream, memory, gauntlet, vector.

## Size (measured 2026-10-07)

| Crate | .rs files | Lines |
|---|---|---|
| executor | 121 | 78522 |
| storage | 64 | 28467 |
| vector | 90 | 27673 |
| sql | 25 | 17654 |
| columnar | 46 | 32912 |
| network | 13 | 10511 |
| index | 26 | 10436 |
| txn | 16 | 9454 |
| types | 17 | 7408 |
| hw | 12 | 6448 |
| filters | 25 | 4672 |
| wal | 7 | 4142 |
| json | 7 | 3842 |
| htap | 13 | 3733 |
| gauntlet | 12 | 3084 |
| memory | 10 | 2670 |
| core | 5 | 2152 |
| server | 4 | 1976 |
| lifecycle | 13 | 1881 |
| stream | 10 | 1867 |
| optimizer | 4 | 1624 |
| cli | 1 | 1272 |
| spill | 10 | 2283 |
| mvcc | 6 | 876 |
| crc32c | 7 | 832 |
| security | 1 | 22 |
| Total | ~539 | ~273k |

## Binaries / libs / examples / benches / tests

- Binaries: `plomid-server` (crates/server/src/main.rs), `plomid` CLI (crates/cli/src/main.rs, wire-client only).
- Libraries: all crates except server/cli are libs.
- Examples: 2 (`hw/examples/prof_dump.rs`, `executor/examples/tier_sim.rs`).
- Benches: 9 bench suites, 22 targets: columnar(6), executor(3), vector(2), storage(4), index(2), hw(1), filters(1), crc32c(1), txn(1). Executor `query_shapes` is a binary-style bench, not criterion.
- Integration tests: executor 66 files, storage 17, vector 8, columnar 8, index 9, txn 7, network 3, others 1 each; root `tests/*.sql` (12 files) + `tests/perf/*.py` (17) + `tests/python/*.py` (11).
- Fuzz targets: none (cargo-fuzz). Closest: `executor/tests/par_fuzz_join.rs`, `par_fuzz_agg.rs` (deterministic unit-style fuzz, not libFuzzer).
- Generated code: none observed. No `build.rs` in workspace. No proc macros.
- Build scripts: none. Native deps: none (`cc`/`bindgen` absent; SIMD via `std::arch` intrinsics only).

## Feature flags / cfgs

- `columnar`: `default=[]`, `zstd=[dep:zstd]` (zstd 0.13 optional).
- `vector`: `default=[simd]`, `simd/avx2/avx512/neon/sve/gpu` (gpu = stub-only, see 05).
- Platform cfgs: `hw/src/profile.rs` (x86_64/aarch64, macos/linux/other), `hw/src/vector/*` per-ISA, `storage` fs/platform abstraction. `crc32c` x86_64/aarch64 modules.

## Dependencies (external, pinned in Cargo.lock)

tokio 1.53.1, bytes 1.12.1, tracing 0.1.44, tracing-subscriber 0.3.23, clap 4.6.6, regex 1.11, serde 1 + serde_json 1 (gauntlet only), criterion 0.5 (dev/bench), zstd 0.13 (optional). No git/path deps outside workspace. No `anyhow/thiserror/miette`, no crypto/TLS crates, no external DB engines (explicitly banned in deny.toml).

## Runtime vs dev vs build deps

- Runtime: tokio (network/server only), bytes, tracing, clap, regex, serde/serde_json (gauntlet), zstd (optional).
- Dev-only: criterion, tracing-subscriber (executor dev), plomid-wal as executor dev-dep.
- Build-only: none.

## Scripts / CI / release / containers / config

- `Makefile`: check/test/test-local/test-scale/gauntlet/clippy/deny/fmt/docker/linux-binary/release-beta targets.
- CI `.github/workflows/ci.yml`: fmt → check → clippy → deny → test-full (90m) → test-arm → gauntlet → vector+bench-no-run.
- Release `release.yml`: version-tag gate, fmt/check/clippy/test-full/gauntlet gates, 5 platform builds `--locked`, nfpm packages, sha256 checksums, docker + homebrew (continue-on-error).
- `Dockerfile`: bookworm-slim runtime, binary-only copy, USER plomid, VOLUME/EXPOSE/HEALTHCHECK. Hardening flags live in `Makefile docker-run`, not Dockerfile. `ENV PLOMID_ALLOW_INSECURE_REMOTE=true`.
- Config: `server/src/config.rs` (TLS/auth/copy-dir/checkpoint/hw profile), `deny.toml`, `clippy.toml`, `rustfmt.toml`.
- Docs: `docs/` (docs.json + v0.1.0), per-crate docs, `benchmarks/harness` + `benchmarks/production/phase-00..26`, `benchmark_results/`, `dist/RELEASE.md`.
- Licenses: `LICENSE` (Apache-2.0), `NOTICE` (© 2026 PLOMID Technologies INDIA, Sainath Sapa).
- Security policy: `SECURITY.md` (private-report, no contact email, no SLA/versions table).
- SBOM/provenance: none (no SPDX/CycloneDX, no cosign/sigstore; checksums only).

## Machine-readable summary (JSON)

```json
{"workspace_version":"0.1.0-beta.2","crates":26,"rs_files":539,"lines":273488,
"binaries":["plomid-server","plomid"],"libs":24,"examples":2,"bench_targets":22,
"integration_test_files":134,"fuzz_targets":0,"build_rs":0,"proc_macros":0,
"external_runtime_deps":["tokio","bytes","tracing","clap","regex","serde","serde_json","zstd(optional)"],
"sbom":false,"artifact_signing":false,"tls_implemented":false}
```


======================================================================
FILE: 02_ARCHITECTURE_AND_CRATE_BOUNDARIES.md
======================================================================

# 02 — Architecture and Crate Boundaries

## Dependency graph (direct `plomid-*` deps, from each Cargo.toml)

```
core, hw, crc32c, stream, spill, security: leaves (no plomid deps; htap→core+hw)
mvcc→core; optimizer→core; memory→core
filters→core+crc32c
wal→core+storage; lifecycle→core+storage+wal
storage→core+htap+filters+crc32c
txn→core+hw+mvcc+wal+storage
types→core+vector; json→core+types
vector→hw
columnar→core+htap+hw+storage+sql+txn+mvcc (+optional zstd)
index→core+storage+wal+txn
sql→core+txn+types+json+vector
executor→core+htap+hw+sql+json+storage+txn+types+index+vector+columnar+memory+optimizer+stream+spill
network→core+executor+sql+txn+types+vector
server→hw+network+txn; cli→core; gauntlet→core+sql+executor+txn+storage+htap+memory+columnar+vector
```

No cycles observed at crate level. `executor` is the integration hub (16 plomid deps). `server` binary directly wires only `hw+network+txn`.

## Classification

| Crate | Class | Responsibility | Public API owner | Verdict |
|---|---|---|---|---|
| core | A | constants, error, observability | ErrorKind/PlomidError, constants | KEEP |
| storage | A | pages, BTree pages, buffer pool, generations, catalog state, checkpoint | StorageEngine(+Txn), GenerationManager | KEEP + HARDEN |
| wal | A | WAL frames, segmented WAL, recovery | SegmentedWal, WalWriter, recover_* | KEEP + HARDEN |
| mvcc | A | version store, snapshots | Snapshot, VersionStore | KEEP |
| txn | A | engine impl, commit pipeline, checkpoint policy, group commit | PlomidStorageEngine, commit_prepare/publish | KEEP + HARDEN |
| sql | A | lexer/parser/binder/catalog | Parser, Statement, InMemoryCatalog | KEEP + HARDEN |
| executor | A | DML/SELECT/joins/vector-ANN/columnar gating/governance | Executor::execute_all* | KEEP + HARDEN |
| network | A | PGWire server, handler, session, SCRAM | PlomidServer, Executor wiring | KEEP + HARDEN |
| types | A | value system, typmod, OID, coercion | PgValue/VectorValue, ColumnType | KEEP |
| server | A | binary, config, lifecycle wiring | config, main | KEEP + HARDEN |
| columnar | B | derived column store on storage engine | ColumnarStore | KEEP + HARDEN (derived, rebuildable) |
| index | B | BTree (durable) + ART (memory) + generations | BTreeIndex, ArtIndex | KEEP; MERGE ART/BTree docs, clarify durability split |
| optimizer | B | advisory cost/selectivity/engine choice | choose_engine_auto | KEEP (narrow scope is correct; do not inflate) |
| htap | B | governor, compaction gate, pools | ResourceGovernor, CompactionGate | KEEP |
| hw | B | CPU detect, SIMD kernels, runtime profile | RuntimeProfile, VectorOps | KEEP + HARDEN |
| memory | B | memory manager, pressure | MemoryManager | KEEP |
| spill | B | external sort/agg/join/distinct | SpillManager | KEEP |
| stream | B | result streaming, backpressure | ResultStream, rows_to_stream | KEEP |
| vector | B* | vector types/math/indexes/storage/planner | VectorValue, planner_api, indexes | KEEP + HARDEN — but storage path must converge (see 04/05) |
| json | B | JSON ops/path/table | ops/path | KEEP |
| cli | C | wire-client REPL | none (binary) | KEEP (correctly thin) |
| crc32c | C | checksums | crc32c() | KEEP (leaf) |
| gauntlet | D | qualification harness | harness | KEEP as test-only; never runtime |
| lifecycle | G | mount/demount stages | LifecycleManager | UNCERTAIN — UNWIRED (see 03/18); candidate MERGE into txn/storage or wire explicitly |
| security | G | stub (22 lines, no exports) | none | UNCERTAIN — REMOVE AFTER VALIDATION or implement; do not keep empty crate |
| filters | G | XorFilter + Roaring | XorFilter, RoaringBitmap | UNCERTAIN — UNWIRED at runtime; KEEP only if wired to scan path with evidence |

Classes: A=core live, B=supporting live, C=optional, D=test-only, G=unwired/uncertain.

## Boundary notes

- Durability ownership is split across storage/wal/txn/columnar/index/lifecycle. Authoritative write path is `txn::engine_impl` (commit_prepare/publish) — correct center. Lifecycle duplicates mount/recovery orchestration that `wal::checkpoint_recovery` + `txn` already own; either wire lifecycle as the single mount authority or merge it.
- `security` owns nothing; auth/TLS/RBAC live in `network`/`server`. An empty crate is a certification liability (implies controls that do not exist).
- `filters` owns useful primitives but no live caller in server/network/executor imports; join Bloom (`executor/join/bloom.rs`) duplicates its role locally.
- `optimizer` is correctly narrow (advisory). Do not mistake import-count for planner coverage; only SELECT engine-choice + vector selectivity consult it.
- `columnar` correctly declares "owns no storage/WAL/MVCC" (store.rs:21-28). `vector` declares independence from storage crates (lib.rs:26) — the one boundary violation that matters (see 04).


======================================================================
FILE: 03_LIVE_PATH_WIRING.md
======================================================================

# 03 — Live-Path Wiring

Method: runtime reachability from `server/src/main.rs` + `network/src/server/*`, not import presence.

## SQL (LIVE)

`server/src/main.rs:149` `PlomidStorageEngine::open` → `:207` `PlomidServer::bind` →
`network/src/server/mod.rs:210` `run_inner` accept loop → `:287` `ConnectionHandler::new` →
`handler.rs:247` `run()` → `:311` `parse_startup` → `:332-336` auth →
`:394` `Executor::new_shared(engine)` → `:448` message loop →
`:456-484` Query/Parse/Bind/Describe/Execute dispatch →
`:1416` `handle_query` → `:1801` `engine.execute_all_with_search_path` (autocommit) or
`:1732-1766` validate+stage (explicit txn) with `:2321` replay at COMMIT.
`executor/src/executor.rs:725` `execute_all` → `:732` governance admit → `:756` Lexer →
`:839` `Parser::parse_statements` (binder takes `&mut InMemoryCatalog`) →
`:886` `execute_transaction_group` (transaction.rs:95) or `:945` `execute_autocommit` →
`:1604` statement dispatch. Verdict: LIVE.

Caveats (do not confuse with "not wired"):
- `handler.rs:1449-1496` short-circuits DEALLOCATE/DISCARD/LISTEN/NOTIFY/etc. without engine — correct PG compat stubs, but catalog-visible behavior gap if clients expect effects.
- Optimizer is advisory-only: `select.rs:1658` `choose_engine_auto` + `vector_ann.rs:857` selectivity + `explain.rs:395-460`. No general planner; joins/CTE/window go to `join::execute_join_select` (select.rs:1474-1512). Classify as NARROW-LIVE, not a full planner.

## INSERT (LIVE)

`executor.rs:1631` → `dml.rs:1572` `execute_insert_rows` → `:1587` begin → `:1314`
`execute_insert_rows_txn` → rowid alloc (`:370-376`) → unique reservation locks (`:432,438`) →
constraints (`:635-747` unique/PK/FK) → `encode_row` + `txn.put` (`:441-442`) →
`stage_index_puts` (`:443`) + `stage_delta_row` (`:446`) → `txn.commit()` →
`txn/engine_impl.rs:335` prepare + `:451` publish → `wal.append:584` + `wal.commit:588` + mvcc_install.
Explicit-txn path stages then replays full SQL text at COMMIT (`handler.rs:2321`) — LIVE but statement-replay, not row-version staging (see Finding F-03). Verdict: LIVE.

## SELECT (LIVE, two branches)

`executor.rs:1673` → `select.rs:1432` `execute_select` → `:1474` general-engine bailout →
`:1556` pinned-read guard → `:1562` index-bounds probe → `:1580` projection analysis →
`:1658` optimizer consult → `:1673-1700` freshness gate (fresh→columnar/vector allowed; delta-covered→scalar only; stale→row) →
columnar/vector branch (`:1714-1822`: pruning `:1727`, vector agg `:1743`, vector filter scan `:1781`, scalar columnar scan `:1797`) or
row/index branch (`:1823+`: `scan_for_each` 1024-row chunks, predicate eval, agg/sort/TopK).
MVCC via engine scan/get snapshot visibility. Wire out via `handler.rs:2451` / COPY `handler.rs:829-853` (1000-row batches). Verdict: LIVE with correctness-first gating.

## Analytical SELECT (LIVE)

Eligibility `select.rs:518` `par_agg_eligible` (COUNT/SUM/AVG/MIN/MAX/COUNT-DISTINCT, int-only SUM/AVG) →
`par_streaming_agg:608` / `par_scan_agg:793` (250k-row materialize cap) / `par_topk_streaming:1045` (1M-row gate, staged parallel TopK noted unwired at `:1004`) →
columnar generation via `ColumnarStore::open(engine.root())` (`executor.rs:185`), publish in `ddl.rs:675,762,821` →
vectorized kernels `columnar/src/vector.rs:109` (RuntimeTier) → spill via `SpillManager` (`select.rs:1298`, `group.rs:185`) at 250k-row thresholds.
Verdict: LIVE; parallel TopK is STAGED (sequential is live fallback).

## Vector (LIVE exact + ANN accelerator; storage split)

Type: `sql/src/parser/types.rs:207,255` → `types/src/typmod.rs:139` + `value.rs:94` → `vector/src/lib.rs:42-120`.
Catalog: column typmod persisted in sql catalog; executor decodes via `VectorType::typmod_decode` (`vector_ann.rs:738`, `explain.rs:278`).
Execution: `vector_ann.rs:667` `try_ann_order` (gates: unpinned, k≠0, enabled, rows≤1M, rows>4096, single asc distance ORDER BY, kind check, rerank) called at `select.rs:2106,2372,2864`; `try_ann_radius:1735` at `select.rs:2043`. Backends `vector_ann.rs:78-88` (Hnsw/DiskAnn/IvfFlat/Sq8/Pq/Sparse) with exact fallback. HW: `vector→hw` dispatch lives in `hw::vector` + `columnar/vector.rs:109`, not `vector/backend.rs` (which is CpuBackend + GpuStub). GPU = config seam only (`backend.rs:415-512` all Err; `vector/Cargo.toml:33` `gpu=[]`).
MVCC/WAL split: row vectors ride txn MVCC/WAL; vector *indexes/storage* (`vector/src/storage/*`, `mvcc/visibility.rs`, `backup.rs`) are a separate fileset with own manifest/format/sync (see 04/05). Verdict: QUERY PATH LIVE; STORAGE PATH PARALLEL (Finding F-01).

## Server-binary wiring summary

`server/Cargo.toml:17-19` declares only hw+network+txn. Directly constructed in `server/src`: RuntimeProfile (hw), PlomidStorageEngine (txn), PlomidServer (network). columnar/vector/htap/spill/stream/memory/optimizer/index/json are constructed inside executor/network handler, never in server/src. They are LIVE at runtime but UNCONFIGURABLE at the server layer (no feature/config selects them; admission is per-query). This is acceptable if deliberate (single binary, per-query gating) but must be documented; otherwise server config is misleading.

## Unwired (no live caller in server/cli/network/executor)

- `lifecycle::LifecycleManager` — zero `use` in live code.
- `security::*` — empty crate, zero `use`.
- `filters::{XorFilter, RoaringBitmap}` — zero `use` in live code (join Bloom is separate).
- `crc32c` — no direct binary use; WIRED transitively via storage/wal/columnar checksums.
- `gauntlet` — test/bench harness only (correct).


======================================================================
FILE: 04_DATA_MODEL_INTEGRATION.md
======================================================================

# 04 — Data-Model Integration (Single Authoritative Path)

Requirement: ONE transaction/MVCC/WAL/catalog/storage foundation.

## Row + JSON (PASS)

- Source of truth: storage engine BTree pages + generations.
- WAL: `txn/engine_impl.rs:584,588` segmented WAL; recovery `wal/recovery.rs`, checkpoint selection `wal/checkpoint_recovery.rs`.
- MVCC: `mvcc::VersionStore` via txn (`engine_impl.rs:42,1108-1221`), snapshot visibility `version.rs:94-108`.
- Catalog: `sql::InMemoryCatalog` persisted via storage (`load_catalog` in `executor.rs:182-185`).
- Indexes: BTree durable + replay (`index/btree/tree.rs:1828-1839`); ART in-memory rebuild via `IndexGenerationStore::derive_art` (`executor.rs:304,360`).
- Backup/restore/recovery/GC/observability: storage checkpoint (`checkpoint.rs:435+`), generation GC (`generation.rs:2411`), WAL reclaim (`segmented.rs:708`), observability via core counters.
- JSON: expression/table/path ops on row values (`executor/lib.rs:63-64` re-export plomid_json); no separate store. PASS.

## Columnar (PASS — derived, rebuildable)

- `columnar/src/store.rs:21-28,52-57,75-83`: "owns no storage, WAL, MVCC, checkpoint, recovery, or checksum… payloads inside the existing storage engine… No columnar-specific recovery."
- Zero `plomid_wal` imports in columnar/src. Publish rides `txn.commit()` + `GenerationManager::publish`. Freshness fail-closed (`columnar_freshness.rs:303,337`). Compaction journal is crash-safe staging, not a second WAL.
- Deletion/GC via orphan-slice reclaim + generation release. PASS with hardening (see F-06 on journal/GC pressure testing).

## Vector (FAIL — parallel persistence)

- `vector/src/lib.rs:26`: "depends only on std and never on SQL, catalog, or storage crates."
- Own files: `storage/file.rs:23-28` records.dat + MANIFEST, `storage/manifest.rs:27-49`, `storage/pages.rs`, `index/manifest.rs`, `codec.rs`; own sync discipline `file.rs:155,172,200-205`; own backup `backup.rs:42,540,606-615,743` (stage→verify→sync→rename, manifest last); own MVCC `mvcc/visibility.rs:34,47,71` (LSN-based, no import of plomid-mvcc); own freshness `freshness.rs`; per-index compact (hnsw/ivf/pq/sq/flat/sparse/multivector).
- Single comment reference to plomid-wal style (`backup.rs:225`); zero code reuse of SegmentedWal/StorageEngine/VersionStore.
- Consequence: two backup systems, two recovery systems, two visibility systems, two GC systems. Row-vector values are durable via txn; vector *indexes/files* are not covered by point-in-time WAL recovery, checkpoint, or generation GC. This is the audit's principal architectural violation. See Finding F-01 (P1). Remediation converges vector index storage onto storage-engine payloads (like columnar) or documents a deliberate two-tier model with crash-matrix proof — the former is recommended.

## KV / Graph / Geo / Blob (not present — readiness only)

No KV/graph/geo/blob engines exist. Assessment only (see 06). Constraint for all: reuse row/typed storage + txn + MVCC + WAL + catalog + index framework; new value kinds via types registry, new access paths via index/executor extension points (see 19). Any new `storage/*`-style fileset or `mvcc/*` clone must be rejected at design review.

## Parallel-system checklist

| System | Row | Columnar | Vector |
|---|---|---|---|
| WAL | txn segmented WAL | same (reuse) | SEPARATE files+manifest |
| MVCC | VersionStore | freshness gate (derived) | SEPARATE LSN visibility |
| Txn manager | txn engine | same | row part same; index part separate |
| Catalog | sql catalog | same | typmod same; index catalog separate (AnnCache key) |
| Backup | storage checkpoint | same | SEPARATE backup.rs |
| Recovery | wal replay | same (GenerationManager::recover) | SEPARATE replay/truncate |
| GC | generation+WAL reclaim | orphan reclaim | per-index compact |


======================================================================
FILE: 05_VECTOR_INTEGRATION.md
======================================================================

# 05 — Vector Integration

## Type system (LIVE)

- Parser `sql/src/parser/types.rs:207,255,261`; typmod `types/src/typmod.rs:139-147,221`; value `types/src/value.rs:94,225`; OID `types/src/oid.rs:110,235`; column map `column.rs:164`. Dimension enforcement + null/error semantics in `vector/src/type.rs`, `value.rs`, `literal.rs`, `codec.rs`; wire text/binary `wire.rs`, `format.rs`; PGWire encode `network/src/server/handler.rs:3438-3443`.

## SQL / catalog / executor (LIVE)

- Catalog: VECTOR oid+typmod in table schema; executor resolves + `typmod_decode` (`vector_ann.rs:738`, `explain.rs:278`).
- Exact: `search/exact.rs:40,84,106` (heap TopK, radius, batch without NxM matrix); `search/topk.rs:95-181`; metric check `search/mod.rs:265` (NaN rejected); `MAX_TOP_K=1M`, `CANCEL_CHECK_EVERY=64`.
- ANN: Hnsw/IvfFlat/DiskAnn/Sq/Pq/Sparse/Flat/Binary + concurrent/online_build (`vector/src/index/*`); executor `vector_ann.rs:78-271` build/search/range/stats + cache invalidate; eligibility `ann_eligible:432`, `try_ann_order:667`, `try_ann_radius:1735`; planner `planner_api.rs:57-600` (usable/freshness/cost/filter/radius strategies); EXPLAIN parity (`explain.rs:251-466`).
- Correctness gates (all LIVE): pinned-read disables ANN (`vector_ann.rs:685`, `select.rs:1556,2293,2701`); freshness Ready+Fresh required (`planner_api.rs:82,735,778`); `PostFilterOversampledAnn` marked `executable:false` (`planner_api.rs:438`) — correctly declined.
- Tests: `vector/tests/{hardening,numerics,property,adversarial,compat,acceptance,upgrade,single_node}` + `executor/tests/vector_planning*` + `network/tests/vector_wire.rs` + scalar equivalence coverage. Good depth for exact/ANN planning.

## MVCC / WAL / persistence (PARALLEL — see 04)

- Vector index/storage does not import plomid-storage/wal/mvcc. Own `storage/{file,pages,snapshot,manifest,adapter,policy}`, `mvcc/{mod,delta,visibility,maintenance}`, `backup.rs`, `freshness.rs`. Row values durable; index files recover via own truncate/replay (`file.rs:106,230-272`), not WAL replay. Update/delete/vacuum via per-index paths + `storage/policy.rs:798,806,1118,1305`. Backup/restore via `backup.rs` (separate manifest). EXPLAIN/observability present (`stats.rs`, `explain.rs`). Rebuildability: indexes rebuildable from rows (good property) but no tested "drop index files → online rebuild" gate in live server path.

## Hardware dispatch (LIVE CPU, SAFE; GPU SEAM)

- CPU: `hw/src/profile.rs:65` detect (x86 sse4.2/avx2/avx512f; aarch64 neon; else scalar); `hw/src/vector/*` kernels with probe-then-unsafe, `target_feature` gating, unaligned loads only; `vector/math/dispatch.rs:161,181` backend detect + `PLOMID_VECTOR_SIMD` override; tolerance 1e-4 rel/1e-5 abs, deterministic tie-break. Parity tests `hw/tests/parity.rs`. Classify CPU dispatch SAFE on reviewed paths.
- AVX-512 note: `x86_avx512.rs:12-15` deliberately 256-bit lanes for bit-identity; `not_mask` stays scalar (needs AVX512BW, only F probed) — correct conservatism.
- GPU: `vector/backend.rs:415-512` GpuStub always Err(IndexUnsupported); `vector/Cargo.toml:33` `gpu=[]`; `options.rs:52,57`; `executor/vector_settings.rs:119,169`; `explain.rs:346` parses `gpu` then CPU fallback; `executor/tests/vector_admin.rs:217` proves degradation. Classify GPU as CONFIG SEAM, NOT REAL. Do not market GPU support.

## Scalar fallback (LIVE)

`math/scalar.rs` f64 oracle + `dispatch.rs` scalar arm + `CpuBackend::ann_search` exact fallback (`backend.rs:358`) + executor exact-scan fallback on any ANN gate failure. Pinned/delta-covered/stale paths force exact — correctness-first, verified in code.

## Verdict

Query/execution integration: LIVE and well-gated. Durability integration: PARALLEL (P1). Hardware: CPU LIVE/SAFE; GPU NOT IMPLEMENTED. Keep exact+ANN query path; converge storage; keep GPU as documented seam until kernels exist.


======================================================================
FILE: 06_FUTURE_MODULE_READINESS.md
======================================================================

# 06 — Future Module Readiness

Rule: future models must plug into catalog + types + txn + MVCC + WAL + storage + indexes + optimizer + executor + observability + backup/recovery. No parallel engines.

## Graph — REQUIRES CORE ABSTRACTION

Reuse available: row storage/typed records (nodes/edges as tables), transactions/MVCC/WAL (correct), catalog (labels as tables/indexes), SQL + vector search (similarity on embeddings), aggregation (path stats via joins).
Gap: no graph-aware index (adjacency), no traversal operator (BFS/DFS/path), no Cypher/PGQ layer, no planner costing for traversals. Executor `join` + recursive CTE support unverified for deep traversals; spill caps (250k) would bound naive traversals.
Readiness: REQUIRES CORE ABSTRACTION (adjacency index + traversal operator + planner rules). Do not start before F-01/F-03 fixed and extension contracts (19) frozen.

## AI memory — READY WITH SMALL EXTENSION (if scoped correctly)

Decompose: (a) embeddings → vector model (exists, exact+ANN live); (b) documents → row/JSON + chunking convention (no new engine); (c) KV/session → row/typed KV convention (no new engine); (d) semantic retrieval → filtered vector search (planner_api filter strategies exist; oversampled-ANN unwired — use FilterFirstExact path); (e) metadata filtering → SQL predicates (live); (f) temporal → timestamp + MVCC snapshots (live).
Danger: creating a "memory engine" with its own store (repeat of vector storage mistake). Readiness: READY WITH SMALL EXTENSION (chunking conventions + retrieval SQL/operators + retention/GC policy on existing stores). No new persistence.

## Key-value — READY WITH SMALL EXTENSION

B+Tree (`index/btree`) + ART + row/typed storage + txn/MVCC/WAL all reusable. KV is a thin API (get/put/scan/prefix) over existing ordered-index + row encoding, plus a KV SQL/functions surface. No new WAL/MVCC/catalog needed. Readiness: READY WITH SMALL EXTENSION (KV API + prefix-scan operator + conformance tests). Must not add KV WAL/files.

## Geo/spatial — REQUIRES CORE ABSTRACTION

Reuse: typed values (new spatial types via types registry — extension point exists), expressions, index framework shape, columnar storage, SQL, MVCC/WAL (automatic if built on rows).
Gap: no spatial type, no spatial predicate, no spatial index (R-tree/geohash), no planner selectivity for spatial, no vectorized spatial kernels. Readiness: REQUIRES CORE ABSTRACTION (spatial types + index kind + executor predicates + optimizer selectivity). Smallest viable slice: point type + bounding-box index on existing BTree/ART + a few predicates.

## Blob/object — REQUIRES CORE ABSTRACTION

Large objects need chunking + streaming + checksum + lifecycle + retention + backup integration, but metadata/catalog/transactions must stay in the single foundation. Current COPY jail + `MAX_COPY_BUFFER_BYTES=64MiB` + streaming (`ResultStream`/backpressure) give a starting pattern, but no chunked-LOB type, no dedup/GC for chunks, no streaming-DML path. Readiness: REQUIRES CORE ABSTRACTION (LOB chunk store on storage engine + streaming I/O + lifecycle/GC + backup coverage). Must not become a sidecar filesystem with its own catalog.

## Summary table

| Model | Status | Prerequisite |
|---|---|---|
| KV | READY WITH SMALL EXTENSION | 19 contracts frozen; KV API on BTree/row |
| AI memory (scoped) | READY WITH SMALL EXTENSION | conventions on vector+row/JSON; no new engine |
| Graph | REQUIRES CORE ABSTRACTION | adjacency index + traversal operator + costing |
| Geo | REQUIRES CORE ABSTRACTION | spatial types + index + predicates + selectivity |
| Blob | REQUIRES CORE ABSTRACTION | chunk store + streaming + lifecycle/GC/backup |
| Distributed | NOT READY (out of scope) | single-node durability first; no claim |

What must be fixed before ANY new model: F-01 (single durability path), F-03 (true txn semantics), F-02 (TLS/auth), extension contracts (19), plus GC/backup/observability coverage proof.


======================================================================
FILE: 07_CODE_QUALITY.md
======================================================================

# 07 — Code Quality

## Error handling (GOOD)

Custom `core/src/error.rs:35,66,95-261`: ErrorKind + PlomidError{kind,message,source} + codes (PL-IO/CORRUPTION/INVALID-ARGUMENT/NOT-FOUND/ALREADY-EXISTS/CONFLICT/ABORTED/INTERNAL) + SqlError{Syntax,Storage}. No anyhow/thiserror. `?` + `ok_or_else`/`map_err` dominant in non-test code. Wire maps to ErrorResponse + SQLSTATE in network handler. Recommendation: keep; add error-code documentation + exhaustive wire-mapping test (P3).

## Panic paths (ACCEPTABLE, test-skewed)

Totals: `.unwrap()` 2495, `.expect()` 2875, `panic!` 429 — overwhelmingly `#[cfg(test)]`/tests/benches. Production `expect` are lock-poison (`txn/engine_impl.rs:889,1110-1948`, `memory/manager.rs`) or length-checked `try_into().expect` decoders (`encoding.rs:159,164`, `statistics.rs:250-280`); `network/scram.rs:82`, `session.rs:423` chunk invariants. Production `unreachable!` in parser branches (`sql/lexer.rs:408`, `parser/expressions.rs:*`, `json/subscript.rs:49`) — fail-crash on logic bug; acceptable if covered by parser tests (they are, extensively). `storage/physical.rs` decoder unwraps preceded by length/magic checks. No `todo!/unimplemented!` in live code. TODO count: 2 (both closed/deferred notes in `select.rs:2584`, `parallel_budget.rs:48`). Zero FIXME.

## Ownership / traits / API (GOOD)

Clear engine traits (`StorageEngine/StorageEngineTransaction`), catalog ownership (sql), commit pipeline ownership (txn). `executor` hub is large (121 files/78k lines) but cohesive around statement dispatch; DML/query/join/vector submodules well separated. No crate cycles. Public API quality: lib.rs re-exports disciplined; per-crate docs present.

## Async correctness (GOOD with noted blocking I/O)

Pattern is `block_in_place` via `network/handler.rs:74-95` for engine calls; `tokio::Mutex` held only for clone/snapshot then dropped before I/O; `std::Mutex` never held across `.await` in sampled paths; pipelined stream uses bounded `sync_channel` + cancel + drain-join (no leak). Blocking-in-async finding: `std::fs` (canonicalize/read/write/metadata) inside async COPY/registry paths (`handler.rs:878,908,970,3279-3289`; `registry.rs:34-62`); `/dev/urandom` open per auth-config construction (low risk). No `spawn_blocking`/`tokio::fs` observed. Risk is latency/jitter under load, not correctness. Recommendation: move COPY/registry fs ops to `block_in_place`/`spawn_blocking` + add TOCTOU hardening (see 10). P2.

## Locks (DOCUMENTED, needs ordering doc)

Inventory: engine `Arc<Mutex<PlomidStorageEngine>>` + `DatabaseEngines` tokio::Mutex + Semaphore(100k); storage device/generation Mutexes; columnar SpaceEngine/SpaceScheduler + DeltaStore; sql staged-delta Mutex; core observability; memory/htap managers; cancel/auth registries. Txn lock order documented `txn/src/lib.rs:30-49` (engine→versions→registry→gates→barrier); `engine_impl.rs:1110,1174` versions-under-engine. No deadlock evidence found in review. Gap: no single lock-ordering document + no contention metrics on engine Mutex (the global commit bottleneck). Recommendation: lock-order doc + engine-Mutex wait-time histogram (P2/P3).

## Resource handling (GOOD)

Spill caps (1GiB temp, 1024 files, 256MiB record, per-write limit check, Drop unlink); stream backpressure (reserve-before-send, peak≤cap+1, cancel); COPY/staged/prepared/portal caps (core/constants.rs); connection semaphore fail-fast; timeouts (statement/idle-in-txn/lock, default-disabled — see 10/15). Temp-dir ownership via SpillManager; no leak evidence.

## Integer/serialization/endian/arch (GOOD)

Checked arithmetic in WAL framing (`format.rs:135-262`, MAX_PAYLOAD enforcement); explicit LE in vector/storage manifests; `u64::MAX-1` reserved sentinels (constants.rs:595-596); platform abstraction (`storage/platform.rs`, `fs.rs`); `cfg(target_arch/target_os)` confined to hw/profile + crc32c + storage platform. Cast lints deliberately allowed (workspace) — acceptable with test parity (`hw/tests/parity.rs`, vector numerics).

## Unsafe (EXCELLENT containment)

`workspace.lints.rust unsafe_code=forbid` + per-file `#![forbid]` (~70 hits). Only exceptions: `hw` (242 unsafe refs, per-ISA kernels, probe-then-unsafe, SAFETY docs) + `crc32c` (13 refs, arch intrinsics). Zero unsafe in storage/wal/mvcc/txn/lifecycle/columnar/vector/index/memory. Invariants: CPUID-probed ISA, `target_feature` gating, unaligned loads only, min-len shared prefix, no FMA assumptions. Coverage: `hw/tests/parity.rs` + vector numerics/property tests. No Miri in CI — recommend targeted Miri for hw kernels as hardening (P3), not a blocker.

## Feature flags / cfgs (GOOD, minimal)

Two features only (columnar zstd, vector simd/gpu); no cfg explosion. Recommend keeping it that way; gate future backends the same way.

Overall: maintainable, well-factored Rust. Main quality debts are lock-order documentation, blocking-fs-in-async cleanup, and error-code docs — all P2/P3, none blocking.


======================================================================
FILE: 08_PERFORMANCE_AND_SCALING.md
======================================================================

# 08 — Performance and Scaling

Language rule: no "fast/production-scale" claims without workload+hardware. This audit ran no new load tests; verdicts below are ARCHITECTURE review + existing harness inventory unless noted MEASURED.

## Architecture review (per-path)

- Scalar/row: BTree + scan_for_each 1024-row chunks + projection masks + per-row predicate. Sound; global engine-Mutex + group-commit barrier are the write-contention ceiling (see txn/group_commit.rs). No lock-wait metrics exported — gap.
- SIMD: NEON/AVX2/AVX-512 dispatch safe (hw), vector math tolerance-tested. Apple Silicon MEASURED implicitly (host arm64; mvcc lib tests + clippy pass). x86 AVX2/512, Pi ARM64, cloud vCPU, container quotas: UNVERIFIED by this audit (existing `hw/tests/parity.rs` + benches cover logic, not this host).
- Batch/alloc: projection-selected decode, 1000-row wire batches, 64KiB chunk slices, 16k vector batch rows (4k edge clamp). Copy-cost bench motivated IntView 16x win (columnar/benches/copy_cost.rs:34) — example of benchmark-driven fix done right.
- WAL/sync: append→flush→sync(commit) boundary clean; group commit present; fsync discipline uniform (durable.rs policy). No fsync-rate or WAL-bytes metrics exported — gap for HTAP tuning.
- Checkpoint/maintenance: tiered checkpoint policy + CompactionGate + background worker + inline fallback (`executor.rs:600`, `maintenance.rs:326-344`). Debt accounting exists; degraded-mode behavior under maintenance failure needs chaos proof (gap).
- Buffer pool: present (`storage/buffer_pool.rs:388-409` sync-skip-if-clean); eviction-order tests exist; hit-rate metrics not surfaced — gap.
- Columnar pruning: zone-map/BRIN + XOR→Roaring→exact pipeline documented (`filters/lib.rs:27`); eligibility fail-closed. Pruning effectiveness at scale UNVERIFIED here.
- Spill: bounded (250k thresholds, 1GiB/1024-file caps) — memory-bounded by design. Good.
- Parallelism: tiered scan_min_rows + par agg/sort/TopK gates (100k/50k/1M). Parallel TopK correctly staged-off after measured 1.4x regression note (`select.rs:1004`). Good discipline.

## Required scale matrix: NOT DEMONSTRATED in this audit

10K/100K/1M/5M/10M × (PK/unique/range/INSERT/UPDATE/DELETE/batch/COUNT/agg/GROUP BY/ORDER BY LIMIT/exact/ANN/mixed HTAP) with p50/p95/p99/p99.9/max + CPU/RSS/WAL/storage/alloc/IO/lock-wait/maintenance — none run here. Existing assets that partially cover it:

- Benches: executor relational (10k/100k/1M), query_shapes (10k+scale check), time_series; vector_search (1536d, 10k×128, 5k×128 ANN vs flat, planner µs, NUMA 2k×64, storage open/scan); htap_interference (A–H mixed); columnar flush/compaction/space/compression; storage device/checkpoint/catalog/io; index art/btree; hw vector_ops 1M; txn hot_row.
- Harnesses: `benchmarks/production/phase-00..26`, `benchmarks/harness/{runner,run_all,compare}`, `benchmark_results/`, `tests/perf/*.py` (17 drivers), gauntlet qualification.
- What is missing: a single scale-matrix report with hardware provenance + p99.9 + RSS/WAL accounting. Recommend producing it as a remediation gate (P1, scope M) before 1.0 claims.

## Scaling cliffs (architectural risks, code-referenced)

1. Global engine Mutex (network `Arc<Mutex<PlomidStorageEngine>>` + handler blocking calls) — write/readmix contention ceiling; needs wait-time metric + concurrent-txn benchmark (P1 hardening, not redesign).
2. Explicit-txn statement replay at COMMIT (`handler.rs:2321`) — TOCTOU + double-eval cost under large staged batches (10k statements/64MiB caps bound it, but semantics differ from MVCC txn). See F-03.
3. Vector index files outside WAL/checkpoint — crash/restart at scale risks index/row divergence; rebuild cost unbounded without tested online-rebuild. See F-01.
4. Columnar generation freshness — sustained OLTP invalidates generations; delta-covered scalar fallback preserves correctness but analytical latency degrades to row scans. Needs mixed-workload SLO proof (htap_interference harness is the right tool; publish results).
5. Sort/group spill at 250k — correct boundedness, but ORDER BY/GROUP BY over 1M+ without columnar eligibility will spill; needs scale-matrix numbers, not a code change.

## Verdict

Architecture is performance-sane (bounded memory, gated parallelism, fail-closed HTAP). Scale claims are UNVERIFIED pending the matrix. Do NOT optimize during audit — measure first (roadmap 22).


======================================================================
FILE: 09_STORAGE_DURABILITY_RECOVERY.md
======================================================================

# 09 — Storage / Durability / Recovery

## Formats (verified)

- Page: 16 KiB, 48-byte header (magic/version/type/page_id/generation/lsn/payload_len/flags/checksum) + payload + 4-byte trailing CRC32C (`storage/page.rs:21-47,85-178`, `physical.rs:88-107`). Block 256KiB=16 pages; packs with header/footer; PhysicalLocation never encodes paths (`physical.rs:211-574`).
- WAL frame: `PLW2` magic + ver + type + lsn + tx + object + payload + CRC32C (`wal/format.rs:23-62,135-262`); types Data/Begin/Commit/Abort/Checkpoint; MAX_PAYLOAD enforced; version-mismatch → Unsupported.
- Segment header: `PLWS` + seq + first_lsn + crc (`wal/segmented.rs:27-115`).
- Checkpoint: `<root>/checkpoints/checkpoint-<gen:020>.ckpt`, 64-byte header `PLCK` + metadata + CRC (`storage/checkpoint.rs:37-64`); never overwrite; tmp+verify+sync+rename+dir-sync (`:699-730`).
- Vector files (SEPARATE): records.dat `[len u32 LE][bytes]` + MANIFEST `PLMM` (`vector/storage/file.rs:23-28`, `manifest.rs:27-49`).

## Durability discipline (GOOD — uniform policy)

`storage/durable.rs:26-38`: BUILD tmp → FLUSH → VERIFY re-read → SYNC (fsync boundary) → PUBLISH rename + dir fsync. Applied consistently: checkpoints (`checkpoint.rs:699-730`), generations (`generation.rs:2251-2386`, order files→catalog→pointer, each dir synced), WAL rotation/markers (`segmented.rs:791-827`), columnar publish (`store.rs:723-782` via txn commit + generation publish), vector files (`file.rs:155-332`, per-append sync + rename; `backup.rs:606-615` manifest-last). `RealFs::fsync=sync_all` (`fs.rs:119-135`); buffer pool skips fsync only if proven clean (`buffer_pool.rs:388-409`).

## Lifecycle graph (code mapping)

CREATE (GenerationManager::open `generation.rs:1096`, BTreeIndex::create, FileVectorStorage::create, ColumnarPublish::new) → WRITE (txn put/delete, WAL append, VersionStore::install, vector append+sync) → PUBLISH (durable::publish_rename; checkpoint build_and_publish; generation flush/verify/sync/publish; WAL marker) → READ (GenerationReader, engine get/scan, VersionStore::visible, columnar SegmentReader, BTree lookup) → UPDATE (txn commit/abort, group barrier, new GenerationMetadata superseding Active→Sealed) → COMPACT (Sealed→Columnized→Compactable→Compact `catalog.rs:168-244`; columnar compact; per-index compact) → VACUUM (orphan-slice reclaim, generation release/Reclaim) → GC (generation gc `generation.rs:2411` gated, VersionStore::gc, IndexGenerationStore::gc Retired-only, WAL reclaim_before txn-boundary-safe) → CRASH (torn-tail truncation: wal last-segment-only `recovery.rs:499-520`, vector `file.rs:230-272`, lifecycle `mount_recovery.rs:82-85`; fail-point enums) → RECOVER (RecoveryState machine `checkpoint_recovery.rs:71`; recover_storage/into, replay_after_checkpoint, select_checkpoint, GenerationManager::recover, ColumnarStore::recover) → DELETE (retire/remove/release, mark_retire/Reclaim terminal; files removed only after proven unreachable).

Illegal transitions guarded: `GenerationLifecycle::can_transition_to` (`catalog.rs:234`), `ObjectRecord::set_lifecycle` (`:813`), mount/demount state guards (`lifecycle/mount.rs:69-97`, `demount.rs:41-71`).

## Recovery (GOOD for row path; SPLIT for vector)

- Row: committed-only replay (buffer Data until Commit, ignore Abort/incomplete, idempotent Put/Delete `recovery.rs:372-436`); boundary = checkpoint_lsn, replay strictly after (`checkpoint_recovery.rs:30-47`); LSN continuity enforced; unneeded segments skipped via headers; mount order open→validate→checkpoint→WAL→catalog→generation→Ready (`lifecycle/lib.rs:40-50`); restart/generation/adversarial tests present (storage/tests/restart, checkpoint_recovery, generation_lifecycle; executor wal_recovery_adversarial, generation_orphan_recovery/repair/gc).
- BTree: reuses WAL via ReplayHandler/ReplayTarget (`index/btree/tree.rs:1821-1839`), no index WAL. ART: explicitly non-durable (`index/tree.rs:70`), rebuilt — correct if documented.
- Columnar: no separate recovery (`store.rs:300` → GenerationManager::recover). Correct.
- Vector: own truncate/replay + manifest LSN, not WAL replay. Crash consistency of *index files* is internally disciplined but *cross-system atomicity* (row commit vs index publish) is unproven — the gap behind F-01. Needs crash-matrix test (kill during row-commit vs index-checkpoint in both orders) or convergence.

## Gaps

1. Vector/row cross-system atomicity unproven (P1 — F-01).
2. No disk-full / WAL-full / fsync-error injection proof in live server path (P1 — chaos gate in roadmap).
3. fsync-error handling paths (e.g., `sync_all` failure mid-publish) reviewed in code but not fault-injected (P2).
4. Corruption-detection coverage (page CRC, WAL CRC, checkpoint validate) good; add `CHECK`/`REINDEX`-style operator tooling + corruption-recovery runbook (P2).

Overall: row durability architecture is sound and uniformly implemented. Vector split is the sole structural defect.


======================================================================
FILE: 10_SECURITY.md
======================================================================

# 10 — Security

Tools: code review + `cargo deny check` (ok) + config review. No pentest. Passing tools ≠ secure.

## Authentication (P1 hardening required)

- Single static credential pair, default `plomid/plomid` (`server/config.rs:218-234`); `V1 supports a single username and password` (`network/session.rs:230-233`).
- Methods: cleartext + MD5 + SCRAM-SHA-256 server-side (`handler.rs:1008-1137`; `scram.rs:21-192` hand-rolled SHA-256/HMAC/PBKDF2/base64; `session.rs:393-456` hand-rolled MD5). Zero external crypto crates — hand-rolled crypto is a risk (no audited primitive).
- Issues: plaintext password held in memory + non-constant-time `==` (`session.rs:315-320`); MD5 offered (downgrade surface; salt = pid bytes `handler.rs:1031-1050`); fallback salt weak if /dev/urandom absent (`session.rs:364-391`); Debug redacts (good, `:244-255`).
- Brute-force: bounded LRU rate limiter (128 keys, 100ms×2^n cap 5s, `guardrails.rs:51-143`) + shared tokio::Mutex — good, needs cross-connection/global accounting proof.

## TLS (P0/P1 — not implemented)

- No rustls/native-tls/tokio-rustls anywhere. `TlsConfig{enabled,cert,key,required}` exists (`server/config.rs:30-44`) but handshake is `write_all(b"N")` always (`handler.rs:286-303`).
- Fail-closed remote rule exists (`config.rs:511-532`: non-loopback requires TLS config or explicit `--allow-remote-plaintext`) and tests pin it (`:598-636`). But `Dockerfile:59-62` bakes `PLOMID_ALLOW_INSECURE_REMOTE=true` + `0.0.0.0`, and default creds are public — insecure-by-default deployment.
- Finding F-02 (P0 for any remote/cloud; P1 loopback-only dev): implement TLS or ship loopback-only default + rotate-on-first-boot + document plaintext scope.

## Authorization / RBAC (P1 gap)

- `GRANT/REVOKE` lexed/parsed; `GrantRole` → catalog → `Created("GRANT ROLE")` (`executor/ddl.rs:1344-1347`); `ensure_role` on connect (`executor.rs:660`).
- Enforcement stubbed: `has_*_privilege → true` (`types/func.rs:235-238`, `scalar.rs:135-137`) for client-compat browsing. No per-object ACL. `security` crate empty (22 lines). Any multi-user/RBAC claim is false — F-04.

## Protocol / parsing / exhaustion (GOOD bones)

- Caps in `core/constants.rs` (64MiB payload/frontend/COPY/staged-bytes, 4MiB metadata, 1024 prepared/portals, 10k staged stmts, depth 24, recursion 1M, length 10M). Enforced: codec `codec.rs:90,120`, startup `handler.rs:256`, COPY `handler.rs:629`, staging `session.rs:170-181`, prepared/portal bounds, 1000-row send batches.
- Connections: Semaphore(100k) fail-fast (`server/mod.rs:89-132,235-243`); default 100k is overly large — recommend small default + documented tuning (P2).
- Timeouts: statement/idle-in-txn/lock (default 0=disabled; `session.rs:91-96`, `handler.rs:2670-2725`). Ship sane defaults or document DoS acceptance (P2).
- Cancel: secret-mixed pid/secret (`mod.rs:336-346`), registry add/remove, fail-closed lookup (`handler.rs:266-280`). Good.

## Filesystem (P2 hardening)

- DB names allow-listed (`registry.rs:95-110`); COPY jailed via canonicalize+starts_with (`handler.rs:3267-3294,867-1001`); perms 0600/0700 (`registry.rs:54,64`, `handler.rs:1911`, `wal/segmented.rs:148,157`).
- Gaps: TOCTOU (check-then-read/write without O_NOFOLLOW/fd pinning); `std::fs` in async (latency); no COPY-TO output quota beyond materialization; COPY runs with server privileges (documented in test name — must be operator-gated).

## Secrets / logging / errors (P2)

- No secret-in-log evidence; Debug redacts; COPY errors avoid echoing paths (`handler.rs:3248-3249`); SQLSTATE mapping present. Needs: secret-zeroizing on drop, log-redaction test, error-leak test (P2/P3).

## Supply-chain-adjacent (see 11)

deny bans external DB engines + copyleft; unknown-registry/git denied. No secret scan/SBOM/signing in CI (P2).

## Admin / metrics surfaces

- No separate admin port; `plomid_stat_counters`/`plomid_htap_stats` via SQL (cli `:828,861`); metrics exposure unauthenticated beyond PG auth — acceptable once auth/TLS real; document.
- Shutdown: `drain(live,timeout)` bounded (`server/mod.rs:169-180`); maintenance link via network re-export. Needs shutdown-while-writing chaos proof (roadmap).

Verdict: loopback single-user dev use is defensible; any remote/multi-user/cloud claim is BLOCKED on F-02/F-04 + crypto replacement.


======================================================================
FILE: 11_SUPPLY_CHAIN_SBOM.md
======================================================================

# 11 — Supply Chain / SBOM

## Dependency inventory (direct externals)

| Dep | Version (lock) | Used by | Notes |
|---|---|---|---|
| tokio | 1.53.1 | network, server | net/io-util/rt/macros/sync/time only; no full features |
| bytes | 1.12.1 | network | PGWire framing |
| tracing | 0.1.44 | 10+ crates | observability; no log facade split issue |
| tracing-subscriber | 0.3.23 | server (+executor dev) | env-filter |
| clap | 4.6.6 | server, cli | derive+env |
| regex | 1.11 | json, executor | JSON path / lexer helpers |
| serde + serde_json | 1 | gauntlet (+dev) | harness only, not runtime hot path |
| criterion | 0.5 | dev/bench | harness=false |
| zstd | 0.13 | columnar optional | off by default |
| transitive | mio/socket2/cc/etc. | via tokio | no C++ native DB libs |

Internal: 24 `plomid-*` path deps single-sourced at workspace version (deny `wildcards=deny` satisfied). No git deps, no local path outside workspace, no build.rs, no proc macros, no FFI/native link.

## Policy (GOOD)

`deny.toml`: yanked=deny; allow-list Apache-2.0/MIT/BSD-2/3/ISC/Unicode-3.0 only (copyleft/ECAL/BUSL/SSPL fail); bans external DB engines (clickhouse/duckdb/rocksdb/mysql/postgres/rusqlite/tokio-postgres with rationale); unknown-registry/git denied; crates.io-index only. CI runs `cargo-deny-action@v2` + `make deny`. This audit: `cargo deny check` → advisories/bans/licenses/sources ok (3 license-not-encountered warnings for BSD-2/3/ISC — harmless: allow-list broader than current tree).

## Pinning / updates / provenance

- `Cargo.lock` committed; release builds `--locked` (release.yml). Version bumps via `make release-beta` (workspace + members lockstep 0.1.0-beta.2).
- No dependency-update process documented (no Renovate/Dependabot observed); no vulnerability-scan beyond deny advisories (no `cargo audit` separate); no SBOM generation; no provenance (SLSA/attestations); no reproducible-builds proof; no artifact signing (sha256 only; docker/homebrew continue-on-error weakens release guarantees).
- CISA SBOM minimum elements (supplier, component name/version, license, dependency relationship, timestamp): NOT produced. SPDX (ISO/IEC 5962:2021, v3.0 current) vocabulary: not used.

## Licenses / attribution

- Workspace Apache-2.0; every member `license.workspace=true`; `LICENSE` full text; `NOTICE` © 2026 PLOMID Technologies INDIA / Sainath Sapa. No GPL/AGPL in tree (deny would fail). No vendored/embedded third-party code observed; no binary blobs; test fixtures hand-built in-repo (`tests/*.sql`, `tests/perf`, `tests/python`); single PNG asset. No missing-attribution evidence found. (Legal review still required for release — see 12.)

## Flags

- Copyleft obligations: none detected in runtime tree (dev-only criterion/serde_json are MIT/Apache). Confirm at release via SBOM license scan (roadmap).
- Unclear provenance: none in source; release pipeline provenance (who built `dist/` binaries, toolchain image digest) undocumented — fix with attestations (P2).
- Generated/copied code: none; algorithm origins unattributed but not copied verbatim per grep (see 12).

## Recommendation (P2, scope S/M)

Generate SPDX SBOM at release (`cargo sbom`/`cargo-deny list` + ntia/cisa fields), publish checksums + sigstore/cosign signatures, pin CI actions by SHA, add Dependabot + `cargo audit` gate, document update SLA. No new runtime deps without deny + license review.


======================================================================
FILE: 12_IP_AND_PROVENANCE.md
======================================================================

# 12 — IP and Provenance

No legal conclusions. Triage only; counsel review required before commercial release.

## Ownership indicators

- Uniform headers: `© 2026 PLOMID Technologies INDIA / Platform for Modern Intelligence and Data / Author: Sainath Sapa` on *.rs/*.toml/Dockerfile/Makefile/yml/sh (80-hit sample).
- `Cargo.toml:37-39` authors/repository/homepage; `NOTICE:1-11` copyright + Apache-2.0 pointer.
- History: shallow review (`git log --oneline -5` shows vector/hw/toolchain work); full history not audited — recommend `git log --follow` + contributor scan before 1.0 (CLA/DCO check).

## Code origins (grep + inspection)

- Vendored dirs: none (`vendor/`, `third_party/`, fixtures globs → 0). `ported from|based on|Inspired by|adapted from|vendored` → 12 hits, all false positives (comments about column types/LSN/size_of/re-exports).
- Algorithm implementations (HNSW/IVF/PQ/SQ/DiskANN/Roaring/XOR/B-Tree/ART/SHA-256/MD5/base64/PBKDF2): original implementations per inspection (hand-rolled crypto included — a security concern, not an IP one). No upstream license headers, no copied test vectors except standard SHA-256 "abc" vector (`network/scram.rs` unit test — facts/standards vectors are not copyrightable expression, but record the source standard: FIPS 180-4 / RFC 6234 as appropriate).
- Benchmark harnesses: in-repo (`benchmarks/harness`, criterion benches); no external benchmark code copied.
- Docs/examples: in-repo; emblem PNG (`assets/plomid-emblem-copper.png`) — confirm original or licensed artwork + font licenses (P3 checklist item).
- Model/AI assets: none (no weights/embeddings shipped).

## Classification

| Item | Status |
|---|---|
| Own source files | IP CLEAR (pending history/CLA scan) |
| Apache-2.0 license text + NOTICE | IP CLEAR |
| External runtime deps (MIT/Apache/BSD/Unicode) | IP ATTRIBUTION REQUIRED (automate via SBOM/NOTICE aggregation) |
| SHA-256 test vector, std arch intrinsics usage | IP CLEAR (standards/facts; cite spec) |
| Artwork (`assets/*.png`), docs reuse | IP PROVENANCE UNCLEAR — confirm authorship/license |
| Contributor agreements, employment-IP assignment | LEGAL REVIEW REQUIRED |
| Patent / freedom-to-operate (HNSW/ANN/compression/indexing) | PATENT/FREEDOM-TO-OPERATE REVIEW REQUIRED — code audit cannot establish clearance |

## Required actions (before 1.0)

1. Contributor/CLA audit + `git log` authorship map (S).
2. SBOM-driven NOTICE aggregation for all transitive deps (S, with 11).
3. Asset license confirmation (S).
4. Patent-counsel screen for ANN/compression/index claims if operating in litigious markets (org-level, XL timeline; do not block engineering except to avoid willful-blindness memos).
5. No code changes needed except NOTICE automation; do not add copied reference implementations to "save time" — current original-code posture is the asset.


======================================================================
FILE: 13_COMPLIANCE_READINESS.md
======================================================================

# 13 — Compliance Readiness

Statuses: NOT APPLICABLE | APPLICABILITY REVIEW REQUIRED | PARTIAL | SUBSTANTIALLY READY | EVIDENCE GAP | ORGANIZATIONAL GAP | THIRD-PARTY CERTIFICATION REQUIRED. Product vs org controls separated. No certification claimed.

| Framework | Edition | Applies? | Technical readiness | Evidence | Org gap | 3rd-party needed | Priority |
|---|---|---|---|---|---|---|---|
| ISO/IEC 27001:2022 | current | APPLICABILITY REVIEW REQUIRED (if offering managed/cloud) | PARTIAL (access caps, perms, backup/recovery, change gates in CI) | constants/caps, perms, checkpoint/recovery tests, CI gates | ISMS, risk treatment, incident response, vuln mgmt SLA — ORGANIZATIONAL GAP | THIRD-PARTY CERTIFICATION REQUIRED for cert | P2 |
| ISO/IEC 27017:2026 | current (do not cite 2015) | APPLICABILITY REVIEW REQUIRED (cloud offering) | PARTIAL (tenant=database isolation via registry+search_path; no cross-tenant proofs) | registry allow-list, DatabaseEngines per-db | shared-responsibility matrix, tenant-isolation proof — GAP | audit if claimed | P3 |
| ISO/IEC 27018:2025 | current | APPLICABILITY REVIEW REQUIRED (PII in public cloud) | EVIDENCE GAP (no PII controls: no encryption at rest, no redaction, no retention) | none | processor obligations, DPIA — GAP | audit if claimed | P3 |
| ISO/IEC 27701:2025 | current | APPLICABILITY REVIEW REQUIRED | EVIDENCE GAP (same + no controller/processor mapping) | none | privacy program — GAP | audit if claimed | P3 |
| NIST CSF 2.0 | current | Applies as framework (self-assessment) | PARTIAL (Identify: inventory 01; Protect: caps/perms/deny; Detect: observability counters; Respond/Recover: recovery tests, drain) | 01/07/09/10/15 | playbooks, metrics retention — GAP | none (framework) | P2 |
| NIST SP 800-218 SSDF 1.1 | finalized baseline | Applies (dev practices) | PARTIAL (pinned toolchain, deny, CI gates, review discipline) | toolchain/deny/CI | formal SSDF attestation, vuln-response SLA — GAP | none (self-assert + evidence) | P2 |
| SSDF 1.2 draft (Rev.1) | DRAFT — not certifiable | Track only | n/a | n/a | monitor; do not represent as requirement | n/a | P3 |
| OWASP ASVS 5.0 | 5.0 | Mostly NOT APPLICABLE (no web app); API-adjacent parts REVIEW | PARTIAL (protocol limits, auth rate-limit, COPY jail, error non-leakage) | 10 | web-surface N/A doc — GAP (one-page applicability note) | none unless web admin added | P3 |
| IEC 62443-4-1 / -4-2 | current | APPLICABILITY REVIEW REQUIRED (only if OT targeted) | PARTIAL (-4-2: auth/caps/perms present, crypto/TLS absent; -4-1: CI gates present, SDL docs absent) | 10/CI | SDL documentation, patch lifecycle — GAP; integrator boundary doc | certification if OT claimed | P3 |
| ISO/IEC 15408:2026 (CC) | 2026 | APPLICABILITY REVIEW REQUIRED (only if contract requires) | PARTIAL (clean boundaries, small TCB candidates: core/storage/wal/mvcc/txn; no ST/SFR/evidence) | 02/07/14 | ST/SFR, assurance docs, lab — GAP | evaluation required | P3 |
| FIPS 140-3 | current | APPLICABILITY REVIEW REQUIRED (only where contract requires validated module) | EVIDENCE GAP (hand-rolled SHA-256/MD5; no validated module boundary; TLS absent) | scram.rs/session.rs (negative evidence) | procurement of validated module + boundary doc — GAP | validation required; never claim from algorithm use | P1 if targeted market needs it |
| DO-178C/ED-12C, DO-330 | current | NOT APPLICABLE unless airborne use positioned | n/a (no DAL, no traceability, no independence) | n/a | full DAL process — GAP | qualification required | P3 (do not pursue speculatively) |
| ASD-STE100 | controlled-doc spec | Documentation style only, not software cert | n/a | n/a | tech-writing standard if adopted | n/a | P3 |
| ISO/IEC 42001:2023 | 2023 | APPLICABILITY REVIEW REQUIRED (only if AI products/services operated) | EVIDENCE GAP (vector feature ≠ AI management system) | 05 | AI governance — GAP | certification if claimed | P3 |
| ISO 9001:2026 | company/process level | APPLICABILITY REVIEW REQUIRED | EVIDENCE GAP at code level (quality process is org-level) | CI discipline (supporting) | QMS — GAP | certification required | P3 |
| SOC 2 | TSC (SaaS/managed) | APPLICABILITY REVIEW REQUIRED (only if Cloud/SaaS offered) | PARTIAL (change mgmt, backup/recovery, access caps as supporting evidence) | CI/release gates, 09/15 | controls program + auditor — GAP; source code cannot certify | engagement required | P2 if SaaS planned |
| GDPR / India DPDP | regulations | APPLICABILITY REVIEW REQUIRED (deployment-dependent) | EVIDENCE GAP (no encryption, erasure, retention, audit log, DPA support) | none | legal/program — GAP | n/a (legal) | P2 |
| HIPAA / PCI DSS | sectoral | APPLICABILITY REVIEW REQUIRED (only if PHI/payment in scope) | EVIDENCE GAP (same + no audit trail) | none | program + assessor — GAP | assessment if in scope | P3 |

Cross-cutting technical gaps driving most rows: TLS (F-02), RBAC/audit log (F-04/F-09), at-rest encryption (F-09), SBOM/signing (11), vuln-response SLA (org). Fix those once; every framework row improves.


======================================================================
FILE: 14_AUDITABILITY_AND_CERTIFIABILITY.md
======================================================================

# 14 — Auditability and Certifiability

## Current posture (GOOD bones)

- Deterministic toolchain (1.99.0 pinned, minimal profile, rustfmt+clippy components), fmt config, clippy correctness gates, deny gates, `--locked` releases.
- Pinned internal versions (workspace lockstep), Cargo.lock committed, versioned checkpoint/generation/manifest formats (PLCK/PLW2/PLWS/PLMM + versions).
- Explicit invariants (freshness fail-closed, lifecycle transition guards, RecoveryState machine, lock-order comment in txn).
- Small safety-critical surface: unsafe confined to hw+crc32c with SAFETY docs + parity tests.
- Structured observability (`core/observability.rs` counters, htap/memory stats via SQL).
- Reproducible tests (deterministic HNSW LCG levels, sorted row-id order; GROUP_SPILL_TEST_SERIAL; TEST_LOCK in observability).
- Failure injection: fail-point enums (storage checkpoint, generation publish, columnar store) — good, underused in CI gates.

## Gaps (cost-of-audit drivers)

1. Durability semantics spread across storage/wal/txn/columnar/vector/index/lifecycle (see 02/04/09). Principle: one authoritative lifecycle boundary owns durability; vector converges; lifecycle either owns mount or merges. (Links F-01/F-06.)
2. No SBOM/checksums/signing/source-to-binary traceability (see 11). Add SPDX + attestations + pinned-action SHAs.
3. No requirement→test→evidence traceability matrix. Gauntlet qualification exists but mapping of requirement→test→artifact is informal. Add a lightweight matrix (gauntlet report + 23 index is the start).
4. Missing metrics for audit-relevant contention: engine-Mutex wait, WAL bytes/fsync rate, buffer hit rate, prune effectiveness, lock wait. Add histograms (P2).
5. Error-code catalog undocumented (codes exist, mapping untested exhaustively). Add docs + wire-mapping test (P3).
6. Benchmark reproducibility: harnesses exist but no single scale-matrix report with hardware provenance (see 08). Freeze workload + seed + hardware record format.
7. Config provenance: server config present; needs "effective-config" log line + redaction proof at startup (P3).
8. Deterministic seeds: good in kernels; extend to all property/fuzz tests with printed seeds (P3).

## Recommendations (reduce audit cost)

- Converge durability ownership (F-01/F-06) before any certification attempt — auditors charge per trust boundary.
- Keep `unsafe` surface frozen; any new unsafe requires SAFETY + parity + Miri note.
- Freeze extension contracts (19) so future models do not re-expand the TCB.
- Publish per-release evidence bundle: SBOM + checksums + test report + benchmark matrix + config schema. This bundle is the certification input for every framework in 13.


======================================================================
FILE: 15_PRODUCTION_READINESS.md
======================================================================

# 15 — Production Readiness

Scale: YELLOW at best (loopback single-user), RED for remote/multi-user/cloud. See 21.

| Area | Status | Notes |
|---|---|---|
| Install/startup/config | YELLOW | `install.sh`/`start.sh`/Docker/Makefile present; config covers auth/TLS-required-flags/copy-dir/checkpoint/HW profile; effective-config logging + redaction proof missing |
| Schema init/migration/upgrade/downgrade | ORANGE | No migration framework observed; versioned formats exist but upgrade/downgrade/rollback strategy undocumented and untested |
| Backup/restore | YELLOW (row) / ORANGE (vector) | Row checkpoint + WAL replay tested (restart/recovery suites); vector separate backup.rs unproven cross-system; no operator runbook |
| Observability/health | YELLOW | Counters + htap/memory SQL stats; missing: readiness/liveness endpoints, engine-Mutex/WAL/buffer metrics, log rotation policy |
| Auth/authZ/TLS | RED (remote) / YELLOW (loopback) | F-02/F-04 block remote/multi-user |
| Resource limits | YELLOW | Caps + spill/stream bounded; defaults (100k conns, timeouts 0) need production tuning |
| Graceful shutdown | YELLOW | Bounded drain; shutdown-while-writing chaos proof missing |
| Crash/corruption recovery | YELLOW (row) / ORANGE (vector) | Row replay/idempotence well-tested; vector cross-system kill-matrix missing; corruption-recovery tooling minimal |
| Disk-full/memory/WAL-full/conn-exhaustion | ORANGE | Caps exist; exhaustion behavior (degraded mode, error codes, operator controls) unproven |
| Maintenance failure/degraded mode | ORANGE | Debt accounting + inline fallback exist; failure-injection proof missing |
| Docker/Linux/ARM64/x86/cloud | YELLOW (Linux/arm64 dev) / UNKNOWN (others) | Dockerfile binary-only + USER/volumes good; insecure-remote baked; Pi/x86/cloud/quotas unverified in this audit |

Blocking before Production 1.0 (see 22): TLS/auth story (F-02), RBAC scope decision (F-04), vector convergence or documented two-tier + kill-matrix (F-01), txn replay semantics (F-03), scale-matrix + chaos gates, upgrade/rollback + backup/restore runbooks, SBOM/signing, default hardening (conns/timeouts/creds).


======================================================================
FILE: 16_TEST_COVERAGE_AND_TEST_QUALITY.md
======================================================================

# 16 — Test Coverage and Test Quality

Do not judge from counts. Judge from what is exercised.

## What is actually tested (GOOD depth in core paths)

- Unit: protocol codec, guardrails delays, session auth/staging caps, server config policy, SCRAM vectors, core error codes, parallel_budget/governance/htap governor, crc32c vectors, hw parity unit.
- Integration: 66 executor SQL suites (DML, SELECT shapes, joins, window, CTE, generated columns, RETURNING, ON CONFLICT, FK/grouping, temporal, JSON, bit literals, observability), 17 storage suites (layout/devices/checkpoint/generations/durable lifecycle), 9 index suites (ART/BTree/duplicates/randomized/prefix/shrink), 8 columnar (segments/compaction/journal/vector/encoding/pruning), 8 vector (hardening/numerics/property/adversarial/compat/acceptance/upgrade/single-node), lifecycle/htap/stream/memory/spill suites.
- PGWire: `network/tests/{integration.rs, vector_wire.rs, copy_sandbox.rs}` + `executor/tests/postgres_client_compat.rs` — handshake, session init, catalog browsing (pgadmin/DBeaver compat incl. `has_*_privilege` stubs), transactions/isolation, SELECT ONE, GRANT paths, binary vector params, COPY jail behavior.
- Persistence-reopen: `storage/tests/restart.rs:46-213` (fresh/durable/reopen/rediscover/corruption-shape), `executor/tests/bulk_reopen.rs:51-81` (5000-row batch reopen without checkpoint), checkpoint_recovery/generation_lifecycle/durable_lifecycle/legacy, executor wal_recovery_adversarial/generation_orphan_recovery/repair/gc/consistency_lifecycle.
- Crash/adversarial: wal_recovery_adversarial, vector adversarial/hardening/property, index randomized, executor par_fuzz_join/par_fuzz_agg/mvcc_adversarial/oltp_hardening.
- Concurrency: unique_concurrency (8×50 threads, exactly-one-winner), unique_reservation, pinned_read/automatic_checkpoint, par_scan/sort/join/streaming/send_sync/merge_determinism, cancel timing tests.
- Vector/scalar equivalence: vector numerics + hw parity + executor vector_planning — good.

## Gaps (false-confidence risks)

1. Mocks vs real storage: most executor suites use real engine (good). Residual risk: suites that bypass PGWire (direct `execute_all`) do not prove wire encoding/limits — covered only by the 3 network suites + compat suite. Keep both layers; do not delete direct suites.
2. WAL-bypass: suites that never kill/restart prove logic, not durability. Restart coverage is good for row path, MISSING for vector index/row cross-system divergence (kill during row-commit vs index-checkpoint both orders) — links F-01.
3. Concurrent readers/writers at scale: winner-tests exist; sustained mixed HTAP with p99.9 + maintenance running is harness-only (`htap_interference` A–H never asserted) — promote to gate with thresholds (roadmap).
4. Malformed input: protocol caps tested; parser-recursion/complexity bombing (deep nesting, 10MiB literals) needs explicit adversarial suite (P2).
5. Resource exhaustion: spill caps + staged caps tested in isolation; disk-full/WAL-full/fsync-error injection in live server path missing (P1 chaos gate).
6. CPU-arch equivalence: parity tests exist but CI `test-arm` covers only hw parity + txn lib; full vector/columnar equivalence across ISAs is UNVERIFIED in CI (P2).
7. Success-code-only assertions: some compat suites assert tags, not row content; keep content-oracles (par_scan_equiv/par_sort_equiv are the right pattern — expand).
8. Full workspace suite not run in this audit (per rules); targeted only (`plomid-mvcc` 4 passed, clippy clean). Full suite + gauntlet is a roadmap gate.

## Verdict

Test depth is a genuine strength (breadth + reopen + adversarial + concurrency). The gaps are durability-chaos, cross-arch CI, and scale-matrix promotion — all P1/P2 gates, not rewrites.


======================================================================
FILE: 17_PERFORMANCE_ARCHITECTURE.md
======================================================================

# 17 — Performance Architecture Quality

Question per optimization: is it on the live path, reachable from fresh connections, authoritative, correct on fallback, portable, bounded?

| Optimization | Live? | Authoritative? | Fallback correct? | Portable? | Verdict |
|---|---|---|---|---|---|
| Segment routing/generation publish | YES (ddl.rs:667, columnar store flush) | YES (storage engine) | n/a | YES | KEEP — real runtime |
| XorFilters/Roaring (filters crate) | NO live caller (unwired) | n/a | join Bloom covers joins only | YES | WIRE OR REMOVE — currently benchmark-only value |
| B+Tree (index/btree, durable) | YES (DML stage + scan) | YES | row scan | YES | KEEP + HARDEN |
| ART (index/tree, memory) | YES (derive_art, executor.rs:304,360) | NO (rebuildable) | BTree/row scan | YES | KEEP, document non-durable split |
| Bulk index generation | YES (build_generation/derive_art paths) | derived | per-row probe | YES | KEEP |
| Vectorized execution (columnar/vector.rs + hw) | YES (fresh-only agg/filter scan) | derived | scalar row scan (fail-closed) | YES (tiered, scalar fallback) | KEEP — gating is correct |
| HNSW/ANN (+IVF/DiskANN/SQ/PQ) | YES as accelerator (vector_ann try_ann_*) | NO (exact is truth) | exact scan | YES (CPU; GPU seam) | KEEP — accelerator framing is right |
| Lazy ART | YES (derive on demand) | derived | full scan | YES | KEEP |
| Columnar pruning (zonemap/BRIN/lower_sql) | YES (select.rs:1727) | derived | full generation scan | YES | KEEP, add prune-effectiveness metric |
| Batch execution (1024 chunks, 1000 wire batches) | YES | n/a | n/a | YES | KEEP |
| Spill (250k thresholds, bounded manager) | YES | n/a | in-memory path first | YES | KEEP — boundedness is the feature |
| HTAP governor/resource controls | YES (admit per batch, charge buffers) | policy | reject/degrade-exact | YES | KEEP + add wait/pressure metrics |
| Optimizer decisions | YES narrow (engine choice + selectivity) | advisory | gate wins on disagree | YES | KEEP narrow; do NOT inflate into full planner prematurely |
| Hardware dispatch | YES (hw profile + RuntimeTier gates) | n/a | scalar | YES (probe-then-unsafe) | KEEP + HARDEN (Miri note, P3) |
| Parallel TopK | STAGED (sequential live; `:1004` unwired note) | n/a | sequential heap | n/a | CORRECTLY STAGED — keep gated until cost-gated evidence |
| PostFilterOversampledAnn | UNWIRED (`executable:false`) | n/a | PreFilterSubsetAnn/exact | n/a | CORRECTLY DECLINED — needs stable row ids |
| executor HybridPlan | EXPLAIN-only (hybrid.rs:128,147,214) | n/a | n/a | n/a | KEEP as explainer; do not execute from it |

## Duplication check (correctness first, then cost)

- Two B-trees + ART: overlapping ordered-index roles, different durability (durable vs memory). Keep both only with documented split + a test proving ART rebuild ≡ BTree content after restart (P2).
- Three membership filters (Xor, Roaring, join Bloom): layered pipeline documented but only join Bloom is live. Either wire filters crate into scan path with prune-win proof or remove the crate (see 18).
- Two TopK heaps (vector distance heap vs generic ORDER BY heap + backend sort-truncate): intentional type split; keep, but add a comment cross-link (P3).
- Distance kernels (hw SIMD vs vector scalar oracle vs SQL NULL wrappers vs backend naive): documented tolerance boundary; keep oracle + wrappers, they are the correctness anchor.
- Spill agg duplicated (`spill::SpillGroupedAgg` unused by executor; `group.rs:154` reimplements over byte layer): intentional (Value lacks Ord/Hash). Keep both, document why, add equivalence test (P2).

Overall: optimizations improve the real runtime (not just benches); fallbacks stay correct; parallelism is conservatively gated with measured notes. The one perf-architecture debt is unwired filters + missing contention/prune metrics.


======================================================================
FILE: 18_DEAD_CODE_FRAGMENTATION.md
======================================================================

# 18 — Dead Code / Fragmentation

Do NOT delete during audit. Evidence + risk + recommendation per item.

## F-D1. `security` crate (empty)

- Evidence: `crates/security/src/lib.rs:1-22` docs only, zero exports; `Cargo.toml:12` no deps; zero `use plomid_security` in workspace.
- Reachability: UNREACHABLE (nothing imports it).
- Risk of deletion: LOW (nothing depends on it) but MEDIUM perception risk (removing the name may look like abandoning security).
- Recommendation: REMOVE AFTER VALIDATION (delete crate + workspace member) and own the real story: controls live in network/server. Or implement it (authz/audit-log home) — but do not keep an empty crate. (Links F-04.)

## F-D2. `lifecycle` crate (unwired)

- Evidence: 13 files/1881 lines (`manager.rs`, `mount*.rs`, `stages.rs:29,53,74`); zero live `use` in server/cli/network/executor; mount logic duplicates `wal::checkpoint_recovery::select_checkpoint/recover_storage` + `txn` open paths.
- Reachability: UNWIRED (test-only via `lifecycle/tests/lifecycle_tests.rs`).
- Risk: MEDIUM — two mount authorities will diverge; deleting loses staged demount discipline (`DemountStage` stop→flush→checkpoint→sync→close is good).
- Recommendation: MERGE or WIRE — either make LifecycleManager the single mount/demount authority called by server/main (preferred if its staging is kept) or merge its stage logic into txn and delete the crate. Needs design decision, not silent deletion.

## F-D3. `filters` crate (unwired)

- Evidence: 25 files/4672 lines (XorFilter + Roaring); zero live `use` in server/network/executor; join Bloom (`executor/join/bloom.rs:52`) serves joins separately; pipeline doc `filters/lib.rs:27` describes a chain with no caller.
- Reachability: UNWIRED at runtime; exercised by `filters/benches` + unit tests.
- Risk of deletion: LOW-MEDIUM (leaf dep of nothing except storage? — check: `storage/Cargo.toml` lists plomid-filters; verify call sites before removal).
- Recommendation: WIRE OR REMOVE — prove prune-win on scan path and wire it, or delete. Keeping a 4.7k-line unwired crate inflates audit surface. Note: storage depends on filters at crate level; confirm whether any storage code actually calls it (grep showed no executor use; storage-internal use needs a follow-up grep before action).

## F-D4. `SpillGroupedAgg` (generic, unused by executor)

- Evidence: `spill/src/agg.rs` vs `executor/query/group.rs:135-141,154` comment ("Value lacks Ord/Hash") + reimplementation over byte layer.
- Reachability: reachable as library, not from live GROUP BY.
- Risk of deletion: MEDIUM (typed paths may use it; bench coverage).
- Recommendation: NO ACTION (keep both, document split, add equivalence test). Intentional duplication, not dead code.

## F-D5. Parallel TopK + PostFilterOversampledAnn (staged)

- Evidence: `select.rs:1004` staged note + `PAR_TOPK_MAX_CAP` unused-cap; `planner_api.rs:438` `executable:false`.
- Recommendation: NO ACTION (correctly staged). Track in roadmap, not dead code.

## F-D6. GPU backend (seam, not dead)

- Evidence: `vector/backend.rs:415-512` stub + `gpu=[]` feature + settings/explain parsing with CPU fallback.
- Recommendation: NO ACTION (keep seam, document NOT IMPLEMENTED). Do not delete — the fallback behavior is tested (`vector_admin.rs:217`).

## F-D7. Optimizer HybridPlan duplicate (resolved)

- Evidence: `optimizer/lib.rs:34` notes deletion; canonical `executor/hybrid.rs` is EXPLAIN-only.
- Recommendation: NO ACTION (already resolved). Keep explainer out of execution.

## F-D8. `crc32c` direct-use absence

- Evidence: no binary `use crc32c`; transitively via storage/wal/columnar.
- Recommendation: NO ACTION (correct leaf placement).

## F-D9. unwrap/expect/panic density

- 2495/2875/429 hits, test-skewed. Not dead code; track production `expect` (lock-poison/decoder) as accepted idiom. No action beyond continued `?`-first discipline.

## Summary

Real fragmentation: security (empty), lifecycle (unwired authority), filters (unwired 4.7k lines). Everything else flagged during review is intentional staging or documented layering. No deletions without the validation gates in 22.


======================================================================
FILE: 19_EXTENSION_ARCHITECTURE.md
======================================================================

# 19 — Extension Architecture

Goal: Graph/KV/Geo/Blob/AI-memory/Vector plug into ONE foundation without parallel storage/WAL/MVCC/catalog/txn/query/observability systems.

## Stable internal contracts (as-found, to freeze)

| Contract | Owner | Entry points | Extension rule |
|---|---|---|---|
| Value/type system | types | `PgValue/VectorValue`, `ColumnType`/typmod, `TypeOid`, registry (`types/src/registry.rs`, `ops.rs:26,486`) | New types register here; no sidecar value enums |
| SQL surface | sql | Lexer/Parser/Statement, `InMemoryCatalog`/`TableSchema` | New syntax lowers to Statement; catalog objects versioned |
| Planning (advisory) | optimizer | `choose_engine_auto`, `estimate_selectivity`, cost fns | New access paths add cost/selectivity fns; executor gate still decides |
| Execution operators | executor | `execute_select`/`execute_insert_rows*`, join/group/vector_ann/hybrid, `StreamPolicy` | New operators reuse scan/snapshot/spill/stream; EXPLAIN parity required |
| Row encoding | executor/encoding | `encode_row/decode_row/row_key` | New encodings version + round-trip tests; no forked codecs |
| Transactions/MVCC | txn + mvcc | `PlomidStorageEngine::begin/commit*`, `VersionStore`, `Snapshot` | All models participate; no private version stores (vector is the violation to fix) |
| Durability | storage + wal | `StorageEngine(+Txn)`, `SegmentedWal`, `GenerationManager::publish`, `durable::publish_rename` | All payloads ride these; tmp→verify→sync→rename discipline mandatory |
| Catalog | sql (+storage persist) | `load_catalog`, schema/index definitions | New objects are catalog entries with backup/restore coverage |
| Indexes | index | `BTreeIndex` (durable), `ArtIndex` (memory), `IndexGenerationStore` | New index kinds implement the generation lifecycle + replay story; ART-style memory indexes must declare non-durability + rebuild proof |
| Columnar/HTAP | columnar + htap | `ColumnarStore`, freshness gates, `ResourceGovernor`, `CompactionGate` | Derived stores only; fail-closed freshness; admission via governor |
| Spill/stream/memory | spill/stream/memory | `SpillManager`, `ResultStream`/`rows_to_stream`, `MemoryManager` | Bounded operators; no unbounded materialization |
| Observability | core | counters, htap/memory stats SQL | Every new operator emits counters + EXPLAIN lines |
| Maintenance | executor/maintenance | `MaintenanceCoordinator/Policy`, background worker | New jobs register here; no private background threads owning data |

## Canonical extension model (checklist for any new model)

1. Types: register in types (no parallel value system).
2. Catalog: new objects as catalog entries (no parallel catalog).
3. Storage: payloads in storage engine generations (no new files/WAL — columnar is the template; vector storage is the anti-template).
4. MVCC/txn: visibility via VersionStore/snapshots (no LSN-shadow visibility).
5. Indexes: generation-lifecycle + replay/rebuild story declared up front.
6. Optimizer: cost + selectivity + eligibility predicate; executor gate wins on disagreement.
7. Executor: operator reusing scan/spill/stream; exact-fallback or row-fallback preserved; EXPLAIN renders the plan.
8. Freshness: derived data declares fresh/delta/stale rules; pinned reads force exact/row.
9. Maintenance/GC/backup: job registered, GC proven reachable-only, backup covers new payloads, restore tested by reopen.
10. Observability: counters + stats-SQL + evidence-index entries.

## What this forbids (design-review red lines)

- New `storage/*`-style filesets, new WAL formats, new MVCC visibility, new catalogs, new backup systems, new permission systems, new background data-owners — unless a deliberate documented reason survives architecture review (none currently does).

## Recommendation

Freeze this contract table as `docs/EXTENSION_CONTRACT.md` (post-audit, S scope) and require every future-model RFC to fill the 10-row checklist with file:line evidence. Add the red-lines to PR review guidance.


======================================================================
FILE: 20_FINDINGS_REGISTER.md
======================================================================

# 20 — Findings Register

Severities: P0 release/security-critical · P1 production blocker · P2 hardening/maintainability · P3 cleanup.

## F-01 · P1 · Vector index/storage is a parallel persistence system

- CATEGORY: architecture/durability
- STATUS: confirmed
- EVIDENCE: `vector/src/lib.rs:26` independence claim; own `storage/{file,pages,snapshot,manifest,adapter,policy}`, `mvcc/visibility.rs:34,47,71`, `backup.rs:42,540,606-743`, `freshness.rs`; zero `plomid-storage/wal/mvcc` imports in vector/src (one comment at backup.rs:225). Row vectors durable via txn; index files use own sync/manifest/replay (`storage/file.rs:155-332`).
- FILE/LINE: `crates/vector/src/lib.rs:26`; `storage/file.rs:23-36`; `mvcc/visibility.rs:34-71`; `backup.rs:42`
- IMPACT: two backup/recovery/visibility/GC systems; cross-system atomicity unproven; crash divergence risk; doubles audit surface.
- ROOT CAUSE: vector built as standalone engine, later attached at query layer only.
- RECOMMENDATION: converge vector index payloads onto storage-engine generations (columnar template `columnar/store.rs:21-83`) with freshness-gated serving; or document two-tier + prove kill-matrix both orders. Former recommended.
- VALIDATION: kill-matrix test (row-commit vs index-checkpoint both orders) + reopen-divergence test + backup/restore round-trip covering both systems.
- DEPENDENCIES: blocks Graph/KV/Geo/Blob (precedent), 1.0 durability story.
- COMPLIANCE: backup/recovery evidence gap (27001 A.8.13, SOC 2 CC7/CC9).
- SECURITY: availability/integrity risk on crash. PERFORMANCE: rebuild cost unbounded.

## F-02 · P0 (remote) / P1 (loopback) · TLS not implemented; insecure-remote baked into image

- CATEGORY: security/transport
- STATUS: confirmed
- EVIDENCE: no rustls/native-tls in tree; `handler.rs:286-303` always `N`; `server/config.rs:30-44,511-532`; `Dockerfile:59-62` `PLOMID_ALLOW_INSECURE_REMOTE=true` + 0.0.0.0; default creds public.
- IMPACT: all remote traffic plaintext; credential theft; compliance failure (FIPS/TLS rows).
- RECOMMENDATION: implement TLS (rustls) with cert validation + required-mode enforcement; until then ship loopback-only default, rotate-on-first-boot password, remove insecure env from Dockerfile, document plaintext scope.
- VALIDATION: handshake test (required-mode refuses plaintext; verifies cert chain), image-env scan, docs check.
- COMPLIANCE: blocks 27001/27017/PCI/HIPAA remote claims. SECURITY: critical. PERFORMANCE: negligible (handshake only).

## F-03 · P1 · Explicit transactions replay SQL text at COMMIT (TOCTOU + double-eval)

- CATEGORY: correctness/transactions
- STATUS: confirmed
- EVIDENCE: `handler.rs:1732-1766` validate+stage (no storage write) → `:2321` `execute_all_with_search_path(&batch)` at COMMIT; autocommit writes immediately (`:1801-1808`).
- IMPACT: staged statements re-planned/re-evaluated at commit against changed state (phantom reads, constraint bypass window, non-repeatable reads); error attribution at COMMIT, not statement time; perf cost of double planning.
- RECOMMENDATION: true MVCC txn staging (row-version intents under commit timestamp) or narrow the guarantee explicitly (statement-replay = "deferred autocommit batch", document isolation limits, forbid reads-then-writes depending on staged reads). Former preferred.
- VALIDATION: concurrent-modification test (staged INSERT vs concurrent conflicting commit → must conflict, not silently win/lose), isolation-level tests, error-timing tests.
- DEPENDENCIES: blocks repeatable-read claims, Graph/KV txn reuse story.

## F-04 · P1 · No authorization enforcement; `security` crate empty

- CATEGORY: security/architecture
- STATUS: confirmed
- EVIDENCE: `has_*_privilege → true` (`types/func.rs:235-238`, `executor/scalar.rs:135-137`); `GrantRole → Created` only (`ddl.rs:1344-1347`); `security/src/lib.rs:1-22` empty; single static credential (`session.rs:230-233`).
- IMPACT: any multi-user/RBAC claim false; privilege escalation by design; empty crate misleads auditors.
- RECOMMENDATION: decide scope — (a) single-user embedded story (document, enforce loopback/single-role, delete empty crate) or (b) real RBAC (object ACLs, GRANT enforcement, audit log). No middle state.
- VALIDATION: privilege-negative tests (denied access returns error, not rows) or scope-doc + loopback enforcement test.

## F-05 · P1 · Hand-rolled crypto (SHA-256/HMAC/PBKDF2/MD5/base64) with weak spots

- CATEGORY: security/crypto
- STATUS: confirmed
- EVIDENCE: `network/scram.rs:63-192` hand-rolled hashes; `session.rs:393-456` MD5, `:315-320` non-constant-time compare, `:350-391` /dev/urandom + weak fallback salt; MD5 offered with pid-derived salt (`handler.rs:1031-1050`).
- IMPACT: side-channel + correctness risk in auth path; FIPS boundary impossible; MD5 downgrade surface.
- RECOMMENDATION: replace with audited crates (sha2/hmac/pbkdf2/subtle/md-5/base64), constant-time compare, CSPRNG-only salt (fail-closed if unavailable), deprecate MD5, document SCRAM iteration count.
- VALIDATION: Wycheproof-style vectors, timing-smoke test, downgrade-refusal test.
- IP/SUPPLY: new deps need deny+license review (all Apache/MIT — fine).

## F-06 · P2 · Durability ownership split (lifecycle vs txn/wal) + unwired authorities

- CATEGORY: architecture/auditability
- STATUS: confirmed
- EVIDENCE: `lifecycle` 13 files, zero live callers; mount stages duplicate `wal::checkpoint_recovery` + txn open; `DemountStage` discipline good but unused by server/main.
- IMPACT: two mount authorities will diverge; audit cost per boundary; demount discipline not exercised in prod path.
- RECOMMENDATION: single mount/demount authority (wire LifecycleManager from server/main or merge into txn). Design decision + wiring test.
- VALIDATION: mount/demount integration test through server binary path, crash-tail test via same path.

## F-07 · P2 · `filters` crate unwired (4.7k lines) while join Bloom duplicates role

- CATEGORY: fragmentation/performance
- STATUS: confirmed
- EVIDENCE: zero live `use` of XorFilter/Roaring in server/network/executor; `executor/join/bloom.rs:52` separate Bloom; pipeline doc without caller (`filters/lib.rs:27`).
- IMPACT: audit surface without runtime value; prune-win unproven.
- RECOMMENDATION: wire with measured prune-win or remove (check storage-internal callers first — storage lists filters as dep).
- VALIDATION: prune-effectiveness benchmark + scan-path test, or deletion + full-suite green.

## F-08 · P2 · Missing production defaults + runbooks (conn/timeout/creds/upgrade/backup)

- CATEGORY: production readiness
- STATUS: confirmed
- EVIDENCE: `max_connections=100000` (`server/mod.rs:89-102`); timeouts default 0 (`session.rs:91-96`); default creds; no migration/upgrade/downgrade framework; no backup/restore/shutdown/disk-full runbooks; `Dockerfile` insecure default.
- IMPACT: DoS amplification; operator error; unplanned-downtime recovery unproven.
- RECOMMENDATION: sane defaults (small conns, statement/idle timeouts, forced password rotation), plus runbooks: upgrade/rollback, backup/restore, disk-full, WAL-full, corruption (`REINDEX`-style tooling), shutdown. Scope S/M each.
- VALIDATION: default-scan test, chaos tests (disk-full, kill-during-checkpoint), restore drill.

## F-09 · P2 · No at-rest encryption, audit log, or retention/erasure story

- CATEGORY: security/compliance
- STATUS: confirmed (negative evidence — no such code)
- IMPACT: blocks PII/PHI/payment claims (27018/27701/GDPR/DPDP/HIPAA/PCI); forensic gap.
- RECOMMENDATION: decide scope: document "no encryption/PII controls in v1; deploy accordingly" + add audit-log + retention hooks as NEXT items; full encryption only if market requires (XL).
- VALIDATION: scope doc + audit-log tests when built.

## F-10 · P2 · Observability gaps for audit-relevant contention

- CATEGORY: observability/performance
- STATUS: confirmed (negative evidence)
- EVIDENCE: no engine-Mutex wait, WAL bytes/fsync rate, buffer hit rate, prune effectiveness, lock-wait histograms in reviewed paths; counters exist but not these.
- IMPACT: cannot prove scaling cliffs absent; tuning blind.
- RECOMMENDATION: add histograms/counters (S scope), surface via stats-SQL, include in scale-matrix report.
- VALIDATION: metric-presence tests + matrix report fields.

## F-11 · P2 · Scale matrix + chaos gates missing (claims UNVERIFIED)

- CATEGORY: performance/reliability
- STATUS: confirmed (no such report in repo)
- IMPACT: 10K–10M × workload matrix with p50–p99.9 + RSS/WAL unspecified; Pi/x86/cloud/quotas unverified in this audit.
- RECOMMENDATION: produce hardware-provenance matrix (M scope) using existing benches/harnesses; promote htap_interference A–H to asserted gate; add kill/disk-full/fsync-error chaos (M).
- VALIDATION: published report + CI gate thresholds.

## F-12 · P2 · Supply-chain evidence gaps (SBOM/signing/update SLA)

- CATEGORY: supply-chain
- STATUS: confirmed — `cargo deny check` ok; no SBOM/provenance/signing; actions unpinned; no update bot observed.
- IMPACT: enterprise procurement + incident-response gaps; release integrity unproven.
- RECOMMENDATION: SPDX SBOM + attestations + cosign/sigstore + pinned actions + Dependabot/`cargo audit` + update SLA (S/M).
- VALIDATION: release-bundle presence test.

## F-13 · P3 · Error-code catalog + wire-mapping untested exhaustively

- CATEGORY: quality/certifiability — EVIDENCE: codes exist (`core/error.rs:254-261`), mapping ad hoc in handler. RECOMMENDATION: doc + exhaustive mapping test (S).

## F-14 · P3 · COPY jail TOCTOU + std::fs-in-async + COPY privilege scope

- CATEGORY: security/quality — EVIDENCE: `handler.rs:3267-3294,878-1001`; registry fs in async. RECOMMENDATION: O_NOFOLLOW/fd-pinning or document TOCTOU acceptance; move fs to blocking pool; gate COPY by role once RBAC scoped (F-04). (S)

## F-15 · P3 · ART/BTree durability split + SpillGroupedAgg split undocumented at operator level

- CATEGORY: docs/certifiability — EVIDENCE: `index/tree.rs:70` (ART non-durable, good) needs operator-facing doc + rebuild-equivalence test; `group.rs:135-141` needs cross-link. RECOMMENDATION: docs + equivalence tests (S).

Counts: P0: 1 (F-02 remote scope) · P1: 4 (F-01, F-02, F-03, F-04, F-05 — five items with F-02 counted once) · P2: 7 (F-06–F-12 minus overlaps) · P3: 3 (F-13–F-15).


======================================================================
FILE: 21_DECISION_MATRIX.md
======================================================================

# 21 — Decision Matrix

Statuses: GREEN production-ready · YELLOW usable with remediation · ORANGE significant gap · RED blocker · UNKNOWN insufficient evidence.

## Area status

| Area | Status | Evidence | Blocking? | Recommended action | Target |
|---|---|---|---|---|---|
| Row/JSON live path | GREEN | 03 INSERT/SELECT traces; reopen/adversarial tests | No | Harden defaults/metrics | 1.0 |
| Columnar/HTAP derived path | YELLOW | freshness fail-closed; delta scalar fallback; interference harness unasserted | No (correctness ok) | Assert mixed-workload SLOs (F-11) | 1.0 |
| Vector query (exact+ANN) | YELLOW | gated ANN + exact fallback + parity tests | No (query side) | Keep; fix storage side (F-01) | 1.0 query; storage NEXT |
| Vector durability | ORANGE | parallel files/manifest/visibility/backup (04/05) | YES for durability claims | Converge (F-01) | NEXT (pre-1.0 if durability claimed) |
| Explicit transactions | ORANGE | statement-replay at COMMIT (F-03) | YES for isolation claims | True MVCC staging or narrowed guarantee | NEXT pre-1.0 |
| TLS/transport | RED (remote) / YELLOW (loopback) | no impl; insecure image default (F-02) | YES for remote/cloud | Implement or loopback-only + rotation | NOW |
| AuthN (single static cred, hand crypto) | ORANGE | F-04/F-05 | YES for multi-user | Audited crypto + rotation + scope doc | NOW–NEXT |
| AuthZ/RBAC | RED (if multi-user claimed) | stubs return true; empty crate | YES for multi-user | Scope decision (F-04) | NOW (decision) |
| Storage/WAL/recovery (row) | GREEN–YELLOW | uniform sync discipline; replay/idempotence tests | No | Chaos proof (F-11) | 1.0 |
| Performance architecture | YELLOW | sane gating; scale matrix missing | No (no perf claims w/o matrix) | Matrix (F-11) + metrics (F-10) | 1.0 |
| Test quality | YELLOW (strength) | breadth + reopen + adversarial + concurrency | No | Chaos + arch-CI + asserted HTAP (F-11) | 1.0 |
| Supply chain | YELLOW | deny ok; SBOM/signing missing (F-12) | YES for enterprise procurement | SBOM + signing + SLA | 1.0 |
| IP/provenance | YELLOW | original code; SBOM/CLA/asset gaps | No (with counsel review) | CLA scan + NOTICE automation (12) | 1.0 |
| Compliance | YELLOW (technical) / ORANGE (org) | partial controls; org gaps dominate (13) | YES where contract requires cert | Close F-02/F-04/F-09/F-12 + org program | per-market |
| Production ops | ORANGE | defaults + runbooks missing (F-08) | YES for 1.0 | Defaults + runbooks + drills | 1.0 |
| Fragmentation | YELLOW | 3 items (security/lifecycle/filters) | No (decisions needed) | Wire/merge/remove per 18/22 | NEXT |

## Component verdicts

| Component | Verdict |
|---|---|
| core, storage, wal, mvcc, txn, sql, executor, network, types, server | KEEP + HARDEN |
| columnar, index (BTree+ART), optimizer (narrow), htap, hw, memory, spill, stream, json, crc32c, cli | KEEP (harden metrics/docs where noted) |
| vector query/execution | KEEP + HARDEN |
| vector storage/mvcc/backup | MERGE (converge onto foundation) — F-01 |
| lifecycle | MERGE or WIRE (single mount authority) — F-06 |
| security (empty) | DEPRECATE (delete) or staff with RBAC/audit-log home — F-04 |
| filters | WIRE (with proof) or REMOVE AFTER VALIDATION — F-07 |
| gauntlet | KEEP (test-only, never runtime) |
| GPU abstraction | NO ACTION (documented seam) |
| Parallel TopK / oversampled ANN | NO ACTION (correctly staged) |
| New crates | NEW ABSTRACTION REQUIRED only via 19 checklist; default is NO new crates |


======================================================================
FILE: 22_REMEDIATION_ROADMAP.md
======================================================================

# 22 — Remediation Roadmap

Dependency-ordered. Scopes: S days · M 1–3 wks · L month+ · XL multi-month/org. Gates are validation, not activity.

## NOW (unblock safe dev + remote story)

1. TLS/auth scope decision (F-02/F-04/F-05) — S/M. Why: remote story false until decided. Risk: low (config-first). Perf: negligible. Gate: required-mode handshake test + image-env scan + scope doc. Prereq: none.
2. RBAC scope decision: single-user-loopback vs real RBAC (F-04) + empty-crate fate (18 F-D1) — S. Gate: decision record + (delete crate OR negative-privilege tests). Prereq: #1.
3. Replace hand-rolled crypto with audited crates + constant-time + CSPRNG-only salt + MD5 deprecation (F-05) — S. Gate: vector suite + downgrade-refusal test. Prereq: #1.
4. Production defaults + insecure-image fix (F-08 slice): loopback default, forced rotation, small conn default, sane timeouts, remove baked insecure env — S. Gate: default-scan test. Prereq: #1.

## NEXT (pre-1.0 correctness/durability)

5. Explicit-txn semantics (F-03) — M/L. True MVCC staging or narrowed documented guarantee + isolation tests. Gate: concurrent-conflict + error-timing tests. Prereq: #2.
6. Vector durability convergence (F-01) — L. Payloads onto storage generations; freshness serving kept; kill-matrix + reopen-divergence + backup round-trip gates. Prereq: #5 (txn semantics must be settled first).
7. Single mount/demount authority (F-06) — S/M. Wire or merge lifecycle. Gate: server-path mount/demount + crash-tail tests. Prereq: none (parallel with #5).
8. Filters fate (F-07) — S. Wire with prune-win proof or remove. Gate: benchmark + scan test or deletion + green suite. Prereq: none.
9. Observability metrics (F-10) — S. Engine-Mutex wait, WAL bytes/fsync, buffer hits, prune effectiveness. Gate: metric-presence tests. Prereq: none.
10. Extension contract freeze (19 → docs/EXTENSION_CONTRACT.md) — S. Gate: doc + RFC checklist adoption. Prereq: #5/#6 direction set.

## PRODUCTION CANDIDATE (1.0 gates)

11. Scale matrix 10K–10M + asserted HTAP A–H + chaos (kill/disk-full/fsync-error/shutdown-while-writing) (F-11) — M. Gate: published report with hardware provenance + CI thresholds. Prereq: #5,#6,#9.
12. Upgrade/rollback + backup/restore + corruption-recovery runbooks + drills (F-08/F-09 slice) — M. Gate: drill logs. Prereq: #6.
13. SBOM/signing/update-SLA + NOTICE automation + CLA/asset scan (F-12, 12) — S/M. Gate: release-bundle presence test. Prereq: none.
14. Full workspace suite + gauntlet green + clippy/deny/fmt gates on release tag — S (execution). Gate: CI artifacts. Prereq: all above.
15. Compliance evidence bundle per target market (13): map F-02/F-04/F-09/F-12 closures to framework rows; org program owned outside code — M/org. Gate: bundle + auditor engagement where claimed.

## POST-1.0

- At-rest encryption + audit log + retention/erasure (F-09 full) — L/XL, only if market requires.
- Cross-arch CI expansion (full equivalence, not just hw parity) — M.
- Miri for hw kernels, fuzz harness promotion (cargo-fuzz targets from par_fuzz_*) — M.
- COPY TOCTOU hardening + fs-in-blocking-pool (F-14) — S.
- Error-code catalog + ART/BTree equivalence docs/tests (F-13/F-15) — S.

## VECTOR/AI · GRAPH · KV · GEO · BLOB (after foundation)

- Vector/AI: online-rebuild gate + planner-cost refresh after convergence (M). No GPU kernels until demanded + proven.
- KV: thin API on BTree/row (M) per 06/19. Prereq: #5,#6,#10.
- Graph/Geo/Blob: each REQUIRES CORE ABSTRACTION (06) — adjacency/traversal, spatial index, chunk store — each with 19-checklist RFC. No parallel engines. Prereq: 1.0 + #10.

## DISTRIBUTED — explicitly NOT planned; no action.

## What NOT to build / NOT to optimize

- No new runtime crates / engines (KV/Graph/Geo/Blob as engines) — use 19 contracts.
- No full-planner rewrite — optimizer stays narrow/advisory.
- No GPU kernels without a customer + fallback-preserving design.
- No at-rest encryption / multi-region / OT-safety work speculatively — scope to contracted markets.
- No microbenchmark optimization without matrix evidence — measure (gate #11) first.


======================================================================
FILE: 23_EVIDENCE_INDEX.md
======================================================================

# 23 — Evidence Index

Every material claim points here. Format: claim → evidence (file:line or command+output).

## Workspace / toolchain

- 26 members, versions, lints, release profile → `Cargo.toml:1-112`
- Toolchain pin → `rust-toolchain.toml:7-9`; fmt → `rustfmt.toml:6-11`; clippy policy → `clippy.toml:1`, `Makefile:86-87`
- Commit/branch/clean tree → `git rev-parse HEAD` = 0877cb0, `git status --short` clean, `git log --oneline -5`
- Sizes (~539 files/~273k lines) → `find crates/* -name "*.rs"` + wc (shell output in README)
- Deps/direct externals → `crates/*/Cargo.toml [dependencies]`; lock pins → `Cargo.lock` (bytes 1.12.1, tokio 1.53.1, tracing 0.1.44, clap 4.6.6, regex 1.11, serde/serde_json 1, criterion 0.5, zstd 0.13)
- Deny policy + result → `deny.toml:1-50`, `cargo deny check` (ok + 3 license-not-encountered warnings)
- CI/release → `.github/workflows/ci.yml:18-120`, `release.yml:37-84`, `Makefile:61-204`
- Docker/image → `Dockerfile:31-73` (incl. insecure env :59-62), `Makefile:137-138` hardening in run target
- Security policy → `SECURITY.md:1-24`; licenses → `LICENSE:1-198`, `NOTICE:1-11`

## Live paths (03)

- Server open/bind/run → `server/src/main.rs:149,207,221-222,282`; accept/handler → `network/src/server/mod.rs:190,210,287,306`; `handler.rs:247,311,332-336,394-413,448-484`
- Query dispatch/staging/replay → `handler.rs:1416,1449-1496,1558-1561,1732-1766,1801-1808,2321`; batch inserts `:679`; SHOW `:1590`; send path `:2451`, COPY `:829-853`, batch size `:72`
- Executor choke/dispatch → `executor/src/executor.rs:725,732,738,754-756,837-839,886,945,1004,1604-1815`; catalog load + columnar open `:182-185`; txn group `transaction.rs:95,114,126`
- Optimizer narrow use → `query/select.rs:50,1627-1658`; `vector_ann.rs:857-891`; `query/explain.rs:395-460`; exports `optimizer/src/lib.rs:41-56`
- Txn commit pipeline → `txn/src/engine_impl.rs:42,44,335,451,584,588,917-979,1108-1221,1539-1948`; re-exports `txn/src/lib.rs:59-71`
- DML chain → `executor/src/dml.rs:1572,1587-1603,1314,370-376,432-493,635-747,846-899,104-118,1188-1303,1978,2312`; encoding `:29`; delta `:446,757,1303,1978,2317`; freshness hooks `executor.rs:1518-1602`, `after_commit:600`
- SELECT branches → `select.rs:1416-1474,1548-1700,1714-1822,1823+`; scan chunk `:75`; vector sites `query/vector.rs:80,177,268,295`; columnar kernels `columnar/src/vector.rs:109`
- Analytical → `select.rs:518,608,793,1045,1004,115,127,1298,1314`; `group.rs:50-1151,1227-1241`; spill `group.rs:130,185,218,313-314`; columnar lifecycle `ddl.rs:130,458-524,656-675,762,821`; freshness `columnar_freshness.rs:1-72,303,337`
- Vector SQL → `sql/parser/types.rs:207,255,261`; `types/typmod.rs:139-147,221`; `types/value.rs:94,225`; `types/oid.rs:110,235`; `vector/src/lib.rs:42-137`; ANN `vector_ann.rs:78-88,214-271,355-432,667,767-803,1735`; planner `vector/planner_api.rs:57-600,735-778`; EXPLAIN `explain.rs:251-466`; wire `handler.rs:3438-3443`, `vector/wire.rs`, `format.rs`
- HW/GPU → `hw/src/profile.rs:65-88,297-314`; `hw/vector/mod.rs:53-220`; `vector/math/dispatch.rs:53-243`; `vector/backend.rs:26,48-68,146-625`; `vector/Cargo.toml:33`; `executor/vector_settings.rs:119,169`; `vector_admin.rs:217`
- Unwired → lifecycle/security/filters zero-`use` (grep); crc32c transitive-only; gauntlet harness-only (`gauntlet/Cargo.toml:11-22`)

## Data models (04/05/06)

- Columnar reuse → `columnar/src/store.rs:21-28,52-57,75-83,300`; zero plomid_wal imports (grep)
- Vector split → `vector/src/lib.rs:26`; `storage/file.rs:23-36,155-332`; `storage/manifest.rs:27-49`; `mvcc/visibility.rs:34-71`; `backup.rs:42,540,606-743`
- Index durability split → `index/btree/tree.rs:1821-1839`; `index/tree.rs:70`; `index/generation.rs:228-738`

## Quality/security/supply/IP (07/10/11/12)

- unsafe 135 confined → grep `unsafe` (hw+crc32c only); forbid → `Cargo.toml:42-43` + ~70 `#![forbid]` hits; exceptions `hw/Cargo.toml:22-30`, `crc32c/Cargo.toml:19-27`
- unwrap 2495/expect 2875/panic 429/TODO 2 → grep counts; samples `network/scram.rs:82`, `session.rs:423`, `executor/encoding.rs:159-164`, `statistics.rs:250-280`
- Locks/async → `txn/lib.rs:30-49`; `handler.rs:74-95,266-280,878-1001,3267-3294`; `server/mod.rs:89-243`; `guardrails.rs:36-160`; `stream/*`, `spill/manager.rs:47-300`
- Error style → `core/error.rs:35-261`
- Auth/TLS/RBAC/files → `network/session.rs:228-391`; `network/scram.rs:21-285`; `handler.rs:286-303,1008-1137`; `server/config.rs:30-44,207-234,360-363,511-636`; `types/func.rs:235-238`; `executor/scalar.rs:135-137`; `registry.rs:31-110`; perms `registry.rs:54,64`, `handler.rs:1911`, `wal/segmented.rs:148,157`
- Caps → `core/constants.rs:126-614`
- Tests → `crates/*/tests` counts (executor 66, storage 17, vector 8, columnar 8, index 9, txn 7, network 3); `tests/*.sql` (12); restart `storage/tests/restart.rs:46-213`; reopen `executor/tests/bulk_reopen.rs:51-81`; concurrency `unique_concurrency.rs:67-238`; parity `hw/tests/parity.rs`
- Benches → `crates/*/benches` (22 targets); `benchmarks/production/phase-00..26`, `benchmarks/harness`, `benchmark_results/`, `tests/perf`, `executor/benches/query_shapes.rs`
- Measured in-audit → `cargo test -p plomid-mvcc --lib` (4 passed); `cargo clippy -p plomid-core` (clean); `cargo tree -p plomid-server` (depth-1)

## Standards (13)

- Edition mapping table in 13 (27001:2022, 27017:2026, 27018:2025, 27701:2025, CSF 2.0, SSDF 1.1 + 1.2-draft note, ASVS 5.0, 62443-4-1/-4-2, 15408:2026, FIPS 140-3, DO-178C/DO-330, ASD-STE100 doc-only, 42001:2023, 9001:2026, SOC 2 TSC, GDPR/DPDP/HIPAA/PCI). No certification artifacts in repo (negative evidence — no certs/attestations/SBOM dirs).

## How to re-verify

1. `git status --short` (must be clean — audit added only `audit/`).
2. Re-run grep counts in README; `cargo deny check`; `cargo test -p plomid-mvcc --lib`.
3. Full suite + gauntlet per roadmap gate #14 before acting on remediation claims.

