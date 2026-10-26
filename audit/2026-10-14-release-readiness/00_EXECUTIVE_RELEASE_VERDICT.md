# 00 — Executive release verdict

Audit date: 2026-10-14 UTC. Commit `6a935b4` (branch
`starting-v0.1.0-beta-2`, tree clean). Host: macOS 15.4 arm64, Apple M2 Pro
(12 cores), 16 GiB RAM, rustc 1.99.0, debug server rebuilt from HEAD unless
noted. Full provenance: `01_REPOSITORY_AND_AUDIT_PROVENANCE.md`.

## Verdicts

- Local developer use: **GO_WITH_EXPLICIT_LIMITS**
- Single-user local beta: **GO_WITH_EXPLICIT_LIMITS**
- Secure single-tenant remote: **NO-GO**
- Multi-user remote: **NO-GO**
- Cloud / SaaS: **NO-GO**

Overall grade: **B− (engine core verified strong live; one remotely
exploitable data-destroying P0 blocks every shared mode)**.
Confidence: **high** for live-verified claims below; **medium** for scale
(100K measured debug, 1M+ not run); **low** where marked NOT RUN.

## Why remote is NO-GO (one critical blocker)

**P0-003 (new, live-verified): missing authorization on CREATE/DROP
DATABASE.** Any authenticated low-privilege role can create and **drop any
database** — verified end-to-end with side-effect proof (role `lowpriv`
created `rbac_evil2` and dropped `rbac_victim`; both confirmed via
`SHOW DATABASES` before/after). Root cause: the PGWire
`CREATE/DROP DATABASE` intercept (`crates/network/src/server/handler.rs:1763-1768`
→ `handle_database_ddl :2277-2401`) performs filesystem/engine mutation with
zero `authorize`/`is_superuser`/`CREATEDB` check; the executor's `CREATEDB`
arm (`authz.rs:772-778`) is unreachable on the wire path. Table-level RBAC
itself is correctly enforced (42501 deny-without-grant, allow-after-grant,
verified live), so this is scoped to DATABASE DDL — but database dropping is
data destruction by any authenticated user. Artifacts:
`artifacts/rbac_check.py` + §8. Until fixed, no shared mode is safe.

## What verified GOOD (live, end-to-end, this audit)

- Prior **P0-001 fixed**: extended-protocol `BEGIN;…;COMMIT` batch applies
  fully (`(2,30)`), plus DDL+DML batches.
- Prior **P0-002 fixed**: `3.0/2.0 → 1.5`, `1/2 → 0`, on debug AND release
  binaries; wire-level regression tests exist in-tree.
- Prior **P1-003 fixed**: DDL/catalog visible to already-connected sessions.
- Transactions: commit/rollback, mid-txn behavior, 8-way unique race
  exactly-once, `DELETE USING` exact, joins/aggregates/3VL correct vs oracles.
- Durability: graceful restart keeps committed; `kill -9` with open txn keeps
  committed (`id=1`) drops uncommitted (`id=2`); vectors intact (201/201).
- Backup: cold offline `backup` (52 files + manifest) → `backup-inspect` →
  `restore` to clean dir → server starts, all data present; live-dir backup
  refused with exit 2.
- Vector vs independent Python oracles: exact L2, `<->`, cosine `<=>`, HNSW
  ANN all match; zero-vectors, txn rollback, post-crash k-NN identical.
- TLS: `sslmode=require` handshake OK; plaintext refused FATAL under
  `--tls-required`; non-default creds + wrong-password FATAL verified.
- Reads are fast even in debug: 100K `COUNT(*)` 34 ms, group-by-97 488 ms.

## Known blockers/limits for local modes (explicit limits, not NO-GO)

1. **P2-005 (still present): `ILIKE` is case-sensitive** (`'ABC' ILIKE 'abc'`
   → false) — silently PG-incompatible; workaround `LOWER()` both sides.
2. **P2 (new): `DROP DATABASE` works over extended protocol but errors
   `unsupported DROP object` over simple protocol** — protocol asymmetry;
   superusers must use extended clients for DROP.
3. Unsupported-but-fail-closed set (verified): FK declaration, `FLOAT` type
   name, `CHECKPOINT` statement, `CREATE TABLE AS`, `DROP ROLE`,
   `CREATE ROLE IF NOT EXISTS`, `SHOW ALL` returns one empty row.
4. No hot backup/PITR by design — cold offline CLI only (verified working);
   single-user operators must stop the server to back up.
5. Write throughput measured on **debug** build only (2.3K rows/s batched
   multi-row, 180 rows/s single-row autocommit); prior release-mode numbers
   (43–50K rows/s) not re-run on this HEAD — see §7 for provenance caveats.

## Top 10 actions (exact order)

1. Fix P0-003: authorize `handle_database_ddl` (superuser/CREATEDB for
   CREATE, owner-or-superuser for DROP) + regression test (lowpriv
   CREATE/DROP denied, side-effect verified) in the `test-ci` gate.
2. Unify DROP DATABASE across protocols (or reject in both) + fix wrong
   SQLSTATEs (`SerializationFailure` for exists/not-exists).
3. Decide ILIKE scope: implement case-insensitivity or document as hard
   limit with compat note (it currently returns silently PG-wrong answers).
4. Publish the hot-backup/runbook decision: keep cold-only (document stop/
   backup/restore drill from §5) or scope PITR out of Beta 2 explicitly.
5. Re-run this audit's probe set green + full `make test-ci` on release HEAD.
6. 1M-row scale matrix on release binary (1K/10K/100K done; 1M pending).
7. Chaos beyond SIGKILL: torn-WAL/partial-checkpoint/missing-file drills on
   disposable dirs (unit fail-points exist; live matrix NOT RUN).
8. Enforce staged-transaction caps (`MAX_STAGED_STATEMENTS/BYTES` defined
   but unenforced) or remove the constants.
9. Multi-arch runtime proof (Linux x86_64/ARM64) + SLSA attestation.
10. Add slow-consumer write timeout or document permit-holding behavior.

## What would change the verdict

Remote modes flip to GO_WITH_EXPLICIT_LIMITS when: P0-003 fixed with live
re-verification + regression test in CI, ILIKE scoped (fixed or documented),
and the §14 gate list is green with no new P0/P1. Cloud/SaaS additionally
needs 1M scale evidence, chaos matrix, multi-arch runtime proof, and hot
backup/PITR or an explicit out-of-scope statement.
