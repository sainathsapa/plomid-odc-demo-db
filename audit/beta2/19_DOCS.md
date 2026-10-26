# Beta 2 — 19 Documentation for Auditability (Phase 19)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.

## Coverage map (required topic → doc)

| Required topic | Doc | Audience |
|---|---|---|
| Architecture | `docs/v0.1.0/architecture/overview.mdx`, `how-it-works.mdx`, `query-pipeline.mdx`, `request-lifecycle.mdx`, `executor.mdx` | developer (invariants) |
| Ownership boundaries | `architecture/lock-ordering.mdx` (lock order + crate ownership), `CODEOWNERS` | developer |
| Transaction semantics | `transactions/using-transactions.mdx`, `behavior-contracts.mdx`, `mvcc.mdx`, `examples.mdx`, `build/run-transactions.mdx` | both |
| Durability model | `storage/wal-checkpoints-recovery.mdx`, `storage/engine-internals.mdx` | developer |
| Vector persistence model | `vector_durability_convergence` test docs + `architecture/storage-algorithms.mdx` | developer |
| ART vs durable BTree | `architecture/index-algorithms.mdx` | developer |
| Columnar derived-store | `storage/columnar.mdx` | developer |
| Maintenance lifecycle | executor maintenance docs + `operations/logging-diagnostics.mdx` | developer |
| Recovery lifecycle | `storage/wal-checkpoints-recovery.mdx` | both |
| Backup / restore | `operations/backup-restore.mdx` (**added this phase**) | user (behavior/config only) |
| Security model | `operations/profile-secure-remote.mdx`, `profile-single-node-offline.mdx`, `SECURITY.md` | user |
| RBAC | `sql/roles-permissions.mdx`, `operations/rbac.mdx` | user |
| TLS | `operations/tls.mdx` | user |
| Configuration | `operations/configuration.mdx`, `reference/config-reference.mdx`, `reference/cli-reference.mdx` | user |
| Operator runbooks | `operations/run-server.mdx`, `cli.mdx`, `backup-restore.mdx` (**new**), profiles | user |
| Performance methodology | `operations/performance.mdx`, `operations/htap-gauntlet.mdx` | user |
| Benchmark environment | `audit/beta2/11_PERFORMANCE_MATRIX.md` (host/provenance) + `benchmark_results/` | developer |
| Supported hardware | `operations/htap-gauntlet.mdx` + matrix doc (Apple Silicon measured; rest unmeasured, stated) | user |
| Extension contract | `architecture/extension-contract.mdx` (frozen; KV/AI-memory/graph/geo/blob/distributed ordering) | developer |
| Future-model RFC requirements | `future/elastic-engine.mdx`, `future/vector-graph.mdx` | developer |

## Invariant discipline

- Developer docs state invariants (single lifecycle authority, manifest-last
  backup, jail semantics, staged-overlay transactions, derived-cache
  rebuildability). Spot-checked: lock-ordering, wal-checkpoints-recovery,
  index-algorithms state their invariants at the top.
- User docs state behavior/config only. The new `backup-restore.mdx` was
  written to that rule: cold/full/clean-restore procedure and limits, no
  generation-pointer or segment-format internals (those live in
  `storage/*` + audit docs).

## Gap closed this phase

`operations/backup-restore.mdx` did not exist (only `wal-checkpoints-recovery`
internals + audit drills). Added; linked from the runbook set. Remaining
known doc debt: per-SKU hardware support statements (correctly absent until
measured — claiming them now would violate the plan's own rule).

## Status

GREEN (every required topic has a doc; audiences separated; one gap filled).
