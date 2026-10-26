# 00 — Executive release verdict

Independent whole-database re-audit of commit `6890955`
(`starting-v0.1.0-beta-2`), release binary
`sha256 8b0a6b60…972f`, single macOS arm64 host. Read-only wrt product
source. Method and evidence rules: `README.md`; delta vs baseline:
`14_PREVIOUS_AUDIT_DELTA.md`; all findings: `15_FINDINGS_REGISTER.md`.

## Headline result

The vector-contract tranche holds up: former P0s are intact (30/30), the
index DDL envelope is executable-or-explicitly-rejected (77/77), the qualify
gate and quality matrix are green (83/0, 2/0), and this audit found no new
vector-contract hole in the tranche's claimed scope *on its own fixtures*.

Three classes of issues temper the verdict:

1. **Storage defect, unrelated to the tranche but fatal to one common
   workload**: INSERTs into a new vector table whose auto-created vector
   index still exists fail from ~2–4 k rows (dim ≥ 64) with
   `B+Tree node exceeds page payload`. Root-caused as a count-based
   internal split leaving an oversized half (F-001, top finding). The
   lifecycle bench works around it by dropping the auto index before bulk
   load; nothing in the product surfaces or repairs it.
2. **The wire layer silently swallows admin statements**: `REINDEX` and
   `ANALYZE` (also `COMMENT`, `LOCK`, `CLUSTER`, `REFRESH`) are answered
   with a fabricated success tag *before* the executor, so the implemented
   `reindex`/`analyze` code paths are unreachable over the network. ANN
   graphs are never invalidated by `REINDEX` (F-002) and statistics are
   never collected by `ANALYZE`, contradicting `docs/v0.1.0/architecture/executor.mdx`
   (F-003).
3. **Concurrency gaps**: two concurrent same-name `CREATE TABLE`s both
   succeed (sequential control correctly rejects; survivor schema is
   race-dependent) (F-004), and one boot-time auth anomaly (bad-credentials
   even with correct password → lockout) was observed once during a
   restart after a `SIGKILL` mid-shutdown and not reproduced (F-007).

Anti-overfit evidence: independent dim-64 fixtures (sine-structured and
uniform-random, fixed seed, exact oracle) expose HNSW recall cliffs
(0/10 and 6/10 at 50 k) far outside the calibrated quality matrix (F-008);
DiskANN builds are slow (~5 ms/vector; 38.7 s for 8 k×64) and restart
rebuilds from snapshot rows despite persisting checkpoints (`06`).

## Per-mode verdicts

| Mode | Verdict | Basis |
|---|---|---|
| **M1 — Local use** (loopback, single user, disposable data) | **PASS** | P0s intact 30/30; DDL envelope 77/77; backups/restore 12/12; crash recovery clean; suite state in `12`. |
| **M2 — Local Beta** (loopback, real data, operator present, backups) | **PASS with conditions** | F-001 (documented trigger + workaround), F-002/F-003 (no SQL-level cache/stats control), F-008 (dim-64 recall caveat), fmt drift (`12`). |
| **M3 — Secure remote access** (TLS, RBAC, single tenant) | **PASS with conditions** | TLS/RBAC 14/14 incl. non-loopback refusal; conditions: F-006 (~5 s per-connection catalog load on big dirs), F-007 monitoring, F-001. |
| **M4 — Multi-user remote** | **BLOCKED** | F-004 concurrent same-name CREATE (both succeed) breaks catalog determinism for concurrent DDL; F-007 auth anomaly unclosed; write-write/interaction notes in `09`. |
| **M5 — Cloud/SaaS** | **NOT READY** | No HA/PITR statement for Beta (by design — needs explicit out-of-scope wording, G-07); G-03 (disk-full/OOM), G-04 (chaos matrix), G-05 (Linux runtime), G-08 (SLSA) remain unverified; single-host measurements only. |

Status vocabulary used throughout: IMPLEMENTED (code path exists),
MEASURED (live evidence this cycle), UNSUPPORTED (explicitly rejected/not
provided), UNVERIFIED (no evidence).

## What changed vs 2026-10-15 baseline

- REAUDIT-003 **closed** (AVG typing PG-parity; `DROP ROLE` implemented
  with guards). REAUDIT-002 unchanged (documented limit, escape hatches
  verified). REAUDIT-001 not reproduced; a related name-level race was
  found live (F-004).
- G-09 closed with an ESCAPE deviation finding (F-005). G-02 materially
  advanced (1 M-row load measured; 128 k lifecycle cell ran).
- New findings F-001…F-013 (register). Former P0s re-verified intact.

## Conditions to lift M4 / approach M5 (prioritized, full list in `16`)

1. F-001 storage split fix or a fail-closed guard + docs.
2. F-004 serialize name resolution + uniqueness under DDL concurrency.
3. F-002/F-003 route admin statements to the executor (or document the
   stub at the protocol layer).
4. F-008 dimension/scale-aware ANN quality work (investigate clustered
   fixtures; consider build-time diagnostics).
5. F-007 instrument auth failures to capture a second occurrence.
