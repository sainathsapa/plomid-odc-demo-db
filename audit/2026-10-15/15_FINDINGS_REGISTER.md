# 15 — Findings register

## PLOMID-REAUDIT-001 — P1, concurrent-DDL ID collision risk

- Confidence high (code-shape + remediation-report admission); reproducibility
  NOT demonstrated live (deliberately not run — collision could corrupt the
  disposable catalog; needs a fenced harness).
- Impact: two interleaved CREATEs may mint the same table ID (id-keyed
  physical dirs) → clobber/loss. Blocks modes 4–5.
- Source: catalog generation/delta path (see remediation report §3 residual;
  current tree preserves the mechanism).
- Acceptance: DDL serialization lane + interleaved-create stress (N×M
  concurrent CREATE/DROP with distinct names, zero collisions, zero stale
  reads) green + regression test in CI.

## PLOMID-REAUDIT-002 — P2, untyped-param division needs explicit casts

- Confidence high, reproducible: `SELECT %s/%s` with (3.0,2.0) → `('1',)`
  TEXT OID 25; with `::float8` casts → `(1.5,)` OID 701 (`ext_parse_probe`/
  followup logs).
- Impact: parameterized float math silently wrong-typed unless callers cast;
  literals unaffected. Blocks no mode if documented + enforced via compat
  note; misleading if omitted.
- Acceptance: documented hard limit in compatibility matrix + explicit-cast
  examples, or server-side param type inference.

## PLOMID-REAUDIT-003 — P2, `AVG` returns Decimal vs float8 siblings + `DROP ROLE` unsupported

- Confidence high, reproducible (`AVG 7.625` Decimal in `workloads.log`;
  `DROP ROLE` → `unsupported DROP object`).
- Impact: client type-switch breakage; role-lifecycle scripting gap.
- Acceptance: documented types + `DROP ROLE` either implemented or listed as
  hard unsupported with workaround.

## Evidence gaps (not findings against behavior; block modes 4–5)

G-01 full workspace suite; G-02 1M+ scale; G-03 disk-full/WAL-full/OOM;
G-04 torn-WAL/partial-checkpoint chaos; G-05 Linux x86_64/ARM64 runtime;
G-06 ANN recall@k curve; G-07 hot backup/PITR (by design — needs an explicit
out-of-scope statement for Beta); G-08 SLSA attestation; G-09 ILIKE live
matrix (fix present, unvalidated live).
