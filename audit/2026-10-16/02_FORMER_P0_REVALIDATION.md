# 02 — Former P0 revalidation

Evidence: `artifacts/probes/logs/p0_revalidation.log` (regenerated
2026-10-16 with the fixed probe against the release binary; an earlier
staged log ran the pre-fix script whose `params=()` routing hits psycopg's
simple Query path and mis-asserted P0-001b — superseded). Result:
**30/30 checks passed**.

## P0-001 — extended-protocol batch truncation (former P0)

| Check | Result |
|---|---|
| P0-001a simple multi-statement body applied fully | PASS |
| P0-001b extended `Parse` with multi-statement body **rejected** (real param forces the extended path) | PASS — `cannot insert multiple commands into a prepared statement` (PostgreSQL parity) |
| P0-001b zero side effects + session reusable (count=0, `SELECT 42`) | PASS |
| P0-001c 50× repeated `SELECT 1` stay in sync | PASS |
| P0-001d error recovery leaves session usable | PASS |

Live cross-check (this audit, fresh server, `--log-level debug`):
`"BEGIN; INSERT INTO p0_ms3_t VALUES (%s); COMMIT;"` with `(2,)` raises
`SyntaxError: cannot insert multiple commands into a prepared statement`;
count stays 0. The former truncation class (silent partial execution /
desync) is not present in this path.

## P0-002 — float constant folding (former P0)

20 checks PASS, including: `0.1+0.2 = 0.30000000000000004`;
`pg_typeof(3.0/2.0)='float8'`; int contract `7/2=3`, `1/3=0`; NULL
propagation; division-by-zero raises; text division fails closed
(`operator requires numeric operands`); `INSERT 3.0` into INT coerces;
folded literal and explicit-cast parameters (`::float8` → 1.5) correct.

## P0-003 (carried) — DATABASE DDL authorization

Re-tested under RBAC in `10` (`CREATE/DROP DATABASE` denied without
CREATEDB): PASS.

## Residual (not a P0)

Untyped extended-protocol parameters remain textual substitution
(`%s/%s` with floats → `'1'`, documented limit; explicit casts required) —
REAUDIT-002, unchanged; see `03` and `14`.
