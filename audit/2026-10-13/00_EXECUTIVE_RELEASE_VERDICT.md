# 00 — Executive release verdict

Audit date: 2026-10-13 UTC. Commit `bcd6bf4314ced1cc986c026930177a0d35c24211`
(branch `starting-v0.1.0-beta-2`, tree clean except this bundle).
Host: macOS 15.4 arm64, Apple M2 Pro (12 cores), 16 GiB RAM, rustc 1.99.0.

## Verdicts

- Local developer use: **NO-GO**
- Single-user local beta: **NO-GO**
- Secure single-tenant remote: **NO-GO**
- Multi-user remote: **NO-GO**
- Cloud / SaaS: **NO-GO**

Overall grade: **C+ (core verified strong, two silent-correctness P0s block ship)**.
Confidence: **high** for live-verified claims below; **medium** for scale (≤100K
measured, 1M+ not run); **low** where marked NOT RUN.

## Why NO-GO everywhere (critical blockers)

1. **P0-001 — extended-protocol multi-statement truncation (silent).**
   Any driver (psycopg/JDBC/ORM) sending `BEGIN; …; COMMIT;` (or any batch whose
   first keyword is txn control) via Parse/Bind/Execute executes **only the
   first statement** and silently drops the rest — writes vanish with no error,
   sessions wedge in open txns. Simple-protocol (psql) is unaffected.
   `crates/network/src/server/handler.rs:1706,1835-1839,2575-2650`.
   Artifacts: `probe15/16/17_multistmt*.log`.
2. **P0-002 — integral float literals folded to int; division corrupt.**
   `3.0/2.0` returns `1`; any non-integral float division emits wire data the
   client cannot parse (`ValueError: invalid literal for int() … '0.75'`).
   Root cause: `crates/sql/src/parser/expressions.rs:1940-1949`.
   Artifacts: `probe11_float.log`, `probe12b_floatdiv.log`.

Both are small, located fixes (fail-closed reject or split for 001; stop folding
`N.0` to int + fix `/` result typing for 002). Until fixed, no mode is safe:
silent wrong results are worse than errors.

## What verified GOOD (live, end-to-end, this audit)

- True MVCC explicit transactions: commit/rollback, mid-txn reads, savepoints,
  statement-error-aborts-txn, unique races exactly-once, 8-way concurrent
  writers 400/400 committed (`probe2`, `probe5`).
- Durability: graceful restart preserves rows + vectors; `kill -9` preserves
  committed, drops uncommitted, replays WAL (`applied_records=7`, `probe7`).
- Vector exact search correct (L2/cosine/IP, ties, zero/dup vectors, txn
  rollback, post-restart k-NN identical) (`probe3`, `probe6`).
- Real TLS: handshake OK, plaintext refused on `--tls-required` with FATAL,
  non-loopback without TLS refuses at startup (`probe14`).
- RBAC enforced: deny-without-grant + allow-after-grant + 42501, wrong password
  rejected, file COPY denied for non-superuser (`probe8`).
- Scale measured: 100K insert 2.3 s (~43K rows/s), count 70 ms, group-by 110 ms,
  selective filter 2 ms; 10K at ~50K rows/s (`probe4`, `probe13`).

## Known P1s (block remote/multi-user, not local dev)

- **P1-003**: DDL/catalog invisible to already-connected sessions until reconnect
  (affects pools/long-lived clients; GRANTs share the root cause) (`probe9`).
- **P1-004**: backup/restore has no live server path (engine `backup_database`
  uncalled outside tests; no SQL/admin trigger).

## Top 10 actions (exact order)

1. Fix P0-001: split or fail-closed-reject multi-statement extended Execute.
2. Fix P0-002: type `N.N` literals numeric/float; fix `/` result typing; add
   differential float tests over PGWire (not just unit).
3. Add regression tests: extended-protocol batch (psycopg) + float-division
   matrix, run in CI (`test-ci` gate, not ignored).
4. Fix P1-003: catalog refresh/visibility for live sessions (or versioned
   snapshot + documented reconnect rule + `connection_close_idle` guidance).
5. Resolve P1-004: wire backup/restore to an admin path or explicitly scope it
   out of Beta 2 with a file-copy runbook + restore drill.
6. Re-run this audit's probe set green + full `cargo test --workspace --lib --tests`.
7. Publish 1M-row scale matrix (row counts in §5B: 1K/10K/100K done; 1M pending).
8. Chaos beyond SIGKILL: torn-WAL/partial-checkpoint/missing-file drills on
   disposable dirs; record matrix.
9. Decide ILIKE (P2-005) + FK (P2-006) scope: implement or document as hard
   errors with compat notes (FK already hard-errors; ILIKE silently wrong case).
10. Multi-arch runtime proof (Linux x86_64/ARM64) + SLSA attestation (SBOM/SPDX
    already ships; signing missing).

## Conditions that change the verdict

- P0-001 + P0-002 fixed + regression tests green in CI →
  **GO_WITH_EXPLICIT_LIMITS for local dev / single-user beta**
  (limits: ≤100K rows measured; no backup story; reconnect after DDL;
  ILIKE/FK unsupported; floatNUMERIC edge review).
- + P1-003 fixed/bounded + backup drill green + 1M matrix →
  **GO_WITH_EXPLICIT_LIMITS for secure single-tenant remote**
  (loopback or TLS-required, SCRAM, RBAC least-privilege, no superuser apps).
- Multi-user remote / SaaS require additionally: pooled-catalog soak, chaos
  matrix, disk-full/OOM behavior, 12-month-scale maintenance proof — all NOT RUN.

## Answers to the eight founder questions (short)

1. Vector end-to-end? **Yes for exact search under txn/restart** (measured);
   ANN is a gated accelerator with exact fallback; vector *index files* are
   derived cache excluded from backup — converged design, but recall-at-scale
   numbers not measured here.
2. Credible workloads today? Single-node OLTP/OLAP ≤100K rows, exact vector k-NN,
   concurrent writers with unique safety — at measured numbers in `07`.
3. Risky/unsupported? Multi-statement driver batches (P0-001), float division
   (P0-002), DDL on live conns (P1-003), backup (P1-004), ILIKE/FK, >100K rows,
   anything needing multi-arch proof.
4. Test suite proving behavior? Large and mostly honest (365 integration files,
   fail-closed gates); but it missed two P0s that only show over live PGWire —
   suite is necessary, not sufficient.
5. Top blockers? P0-001, P0-002, then P1-003, P1-004 (see order above).
6. Measured numbers? See `07` (all with hardware/workload provenance).
7. Not run? Full workspace suite (still compiling at report time), clippy-ci
   (build-lock contention), 1M+ scale, disk-full/OOM, kill-point chaos, x86/Linux
   runtime, formal isolation proof — each listed with exact command in `14`.
8. Safe to ship now? **No mode.** Fastest path: fix two P0s (~days) →
   limited local beta.
