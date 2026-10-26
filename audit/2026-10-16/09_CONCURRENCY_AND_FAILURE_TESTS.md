# 09 — Concurrency and failure tests

Evidence: `artifacts/probes/logs/txn_mvcc_probe.log` (7/7),
`namesake_race_probe.log`, `catalog_ddl_probe.log`,
`storage_split_*.log`, `boot1_incident_excerpt.log`.

## Transactions / MVCC — PASS (documented semantics asserted)

- Multi-statement transaction atomic — PASS.
- READ COMMITTED refresh: each statement sees newly committed state
  (1/2/3 → 1/2/2 oracle) — PASS; REPEATABLE READ snapshot pinned
  (2/2/3) — PASS.
- Write-write same row (NOTES, no assertion of PG parity):
  - second writer’s commit waited ~5.0 s and then succeeded **without a
    serialization error**; the first session was reaped by the
    idle-in-transaction timeout (`terminating connection due to
    idle-in-transaction timeout`); final value `('B',)`, row readable.
  - Operational consequence: an idle-in-transaction waiter can be killed
    by the default 60 s idle timeout (`SET
    idle_in_transaction_session_timeout` per session; 0 disables), and
    the lock is released on reap. Documented here as observed behavior.
- DDL inside a transaction is accepted (NOTE). Combined with F-004 this
  widens the concurrent-DDL surface.

## Concurrent DDL

- Interleaved create/drop (4×25, distinct names): clean (no errors, no
  marker mismatches, no leftovers) — PASS.
- Same-name concurrent `CREATE TABLE`: **both succeed** (F-004, P1) —
  sequential control rejects correctly. Survivor schema race-dependent.
  This is the blocking multi-user finding.

## F-001 — storage B+Tree split under bulk insert (P1, live repro)

- Failure: batch 2 of a 4×1000-row dim-64 vector-table load fails with
  `B+Tree node exceeds page payload`; control (auto index dropped
  before load) completes 4 000 rows.
- Root cause established earlier this cycle: count-based internal split
  can leave an oversized half when large vector separators mix with
  small ones. Pre-existing storage defect, deliberately **not fixed**
  by the tranche; this audit did not modify source.
- Impact: default workflow (`CREATE TABLE` with vector column → bulk
  INSERT) breaks at a few thousand rows; users must drop the auto index
  and recreate it after load (bench pattern).

## Boot-1 auth anomaly (F-007, observed once — honest record)

- During the earlier lifecycle bench restart phase, the restarted server
  (big dirty directory after a `SIGKILL` mid-shutdown) denied
  authentication for subsequent correct-credential attempts:
  trace shows `security_decision … action=authenticate … decision=deny
  reason=bad_credentials` (each attempt ~5.0 s), then
  `action=lockout … reason=too_many_failures`, then `reason=locked_out`
  — excerpt in `boot1_incident_excerpt.log`.
- Not reproduced: a clone of the same big directory restarted after a
  plain `SIGKILL` while idle authenticates fine (~4.9 s, catalog-load
  cost). Narrowing: not PBKDF2 cost, not the rate limiter in isolation,
  not lockout residue (cleared by restart); tied to the crash-after-
  heavy-dirty-state sequence.
- Status: UNVERIFIED-observed-once; further repro deliberately skipped
  per audit plan. Flag for M3/M4 monitoring: alert on
  `action=lockout`/`locked_out` after a crash restart.

## Torn WAL tail / crash

- Torn tail → explicit startup refusal (see `05`); `kill -9` mid-load
  recovers cleanly (10 000 rows, multiple-of-batch). Chaos matrix
  (G-04) remains partial.
