#!/usr/bin/env python3
"""REAUDIT-002/003 + ILIKE (G-09) probe for the 2026-10-16 audit.

REAUDIT-002  untyped extended-protocol parameters are textual substitution:
             `%s / %s` with float params yields text, explicit casts are
             required. Documented limit (compatibility.mdx); assert the
             documented behavior and the escape hatches.
REAUDIT-003  AVG result typing vs float8 siblings; DROP ROLE support with
             PostgreSQL-style guards (docs claim Supported since 5d05899;
             beta-status.mdx still says Unsupported — resolve live).
G-09         ILIKE live matrix (case folding, wildcards, NULL, indexed and
             non-indexed columns, ESCAPE).

Usage: python3 param_avg_ilike_probe.py [DSN]
"""

import sys

import psycopg

DSN = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "postgresql://plomid:plomid@127.0.0.1:6100/plomid"
)

FAILURES: list[str] = []
CHECKS = 0


def check(label: str, actual, expected) -> None:
    global CHECKS
    CHECKS += 1
    ok = actual == expected
    print(f"{'PASS' if ok else 'FAIL'} {label}: {actual!r}" + ("" if ok else f" != {expected!r}"))
    if not ok:
        FAILURES.append(label)


def note(label: str, value) -> None:
    print(f"NOTE {label}: {value!r}")


def scalar(cur, sql: str, params=None):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def rows(cur, sql: str, params=None):
    cur.execute(sql, params)
    return [r[0] for r in cur.fetchall()]


def try_rows(cur, sql: str, params=None):
    try:
        return ("OK", rows(cur, sql, params))
    except Exception as error:  # noqa: BLE001
        return ("ERR", str(error).splitlines()[0][:110])


def try_scalar(cur, sql: str, params=None):
    try:
        return ("OK", scalar(cur, sql, params))
    except Exception as error:  # noqa: BLE001
        return ("ERR", str(error).splitlines()[0][:110])


def try_exec(cur, sql: str, params=None):
    try:
        cur.execute(sql, params)
        return ("OK", "executed")
    except Exception as error:  # noqa: BLE001
        return ("ERR", str(error).splitlines()[0][:110])


def main() -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            # ---- REAUDIT-002: untyped parameters are text substitution.
            untyped = try_scalar(cur, "SELECT %s / %s", (3.0, 2.0))
            note("REAUDIT-002 untyped %s/%s (3.0,2.0)", untyped)
            cast = try_scalar(cur, "SELECT %s::float8 / %s::float8", (3.0, 2.0))
            check("REAUDIT-002 explicit ::float8 casts", cast, ("OK", 1.5))
            cast2 = try_scalar(cur, "SELECT CAST(%s AS DOUBLE PRECISION) / 2", (3.0,))
            note("REAUDIT-002 CAST(%s AS DOUBLE PRECISION)/2", cast2)
            typed = try_scalar(cur, "SELECT %s + 1", (2,))
            note("REAUDIT-002 %s + 1 with (2,)", typed)

            # ---- REAUDIT-003: AVG typing.
            cur.execute("DROP TABLE IF EXISTS avg_t")
            cur.execute("CREATE TABLE avg_t (v DOUBLE PRECISION)")
            cur.execute("INSERT INTO avg_t VALUES (7.0), (8.25)")
            avg = try_scalar(cur, "SELECT AVG(v) FROM avg_t")
            note("REAUDIT-003 AVG(v)", avg)
            avg_type = try_scalar(cur, "SELECT pg_typeof(AVG(v)) FROM avg_t")
            note("REAUDIT-003 pg_typeof(AVG(v))", avg_type)
            sibling = try_scalar(cur, "SELECT SUM(v) / COUNT(v) FROM avg_t")
            note("REAUDIT-003 SUM/COUNT sibling", sibling)
            int_avg = try_scalar(cur, "SELECT AVG(v) FROM (SELECT 7::int4 AS v UNION ALL SELECT 8) t")
            note("REAUDIT-003 AVG(int) is exact/Decimal?", int_avg)
            cur.execute("DROP TABLE avg_t")

            # ---- REAUDIT-003: DROP ROLE with guards.
            cur.execute("DROP ROLE IF EXISTS audit_probe_ro")
            cur.execute("CREATE ROLE audit_probe_ro")
            drop1 = try_scalar(cur, "SELECT 1")
            try:
                with psycopg.connect(DSN, autocommit=True) as other:
                    cur.execute("GRANT SELECT ON ALL TABLES TO audit_probe_ro")
            except Exception as error:  # noqa: BLE001
                note("grant probe skipped", str(error).splitlines()[0][:80])
            revoked = try_exec(cur, "DROP ROLE audit_probe_ro")
            note("REAUDIT-003 DROP ROLE leased role", revoked)
            missing = try_exec(cur, "DROP ROLE IF EXISTS audit_probe_ro")
            note("REAUDIT-003 DROP ROLE IF EXISTS missing", missing)
            self_drop = try_exec(cur, "DROP ROLE plomid")
            note("REAUDIT-003 DROP ROLE plomid (guard)", self_drop)

            # ---- G-09: ILIKE matrix.
            cur.execute("DROP TABLE IF EXISTS ilike_t")
            cur.execute("CREATE TABLE ilike_t (id INT, s TEXT)")
            cur.execute(
                "INSERT INTO ilike_t VALUES (1,'ABC'),(2,'abc'),(3,'aBc'),"
                "(4,'xyz'),(5,'a_c'),(6,'a%c'),(7,''),(8,NULL)"
            )
            patterns = [
                ("ILIKE 'abc'", "SELECT id FROM ilike_t WHERE s ILIKE 'abc' ORDER BY id", [1, 2, 3]),
                ("ILIKE 'ABC'", "SELECT id FROM ilike_t WHERE s ILIKE 'ABC' ORDER BY id", [1, 2, 3]),
                ("ILIKE 'a%'", "SELECT id FROM ilike_t WHERE s ILIKE 'a%' ORDER BY id", [1, 2, 3, 5, 6]),
                ("ILIKE '%C'", "SELECT id FROM ilike_t WHERE s ILIKE '%C' ORDER BY id", [1, 2, 3, 5, 6]),
                ("ILIKE '%b%'", "SELECT id FROM ilike_t WHERE s ILIKE '%b%' ORDER BY id", [1, 2, 3]),
                ("ILIKE '_bc'", "SELECT id FROM ilike_t WHERE s ILIKE '_bc' ORDER BY id", [1, 2, 3]),
                ("ILIKE 'a_c'", "SELECT id FROM ilike_t WHERE s ILIKE 'a_c' ORDER BY id", [1, 2, 3, 5, 6]),
                ("ILIKE '%'", "SELECT id FROM ilike_t WHERE s ILIKE '%' ORDER BY id", [1, 2, 3, 4, 5, 6, 7]),
                ("LIKE 'abc' (contrast)", "SELECT id FROM ilike_t WHERE s LIKE 'abc' ORDER BY id", [2]),
                ("NOT ILIKE 'abc'", "SELECT id FROM ilike_t WHERE s NOT ILIKE 'abc' ORDER BY id", [4, 5, 6, 7]),
            ]
            for label, sql, expected in patterns:
                ids = try_rows(cur, sql)
                actual = ids[1] if ids[0] == "OK" else ids
                check(f"G-09 {label}", actual, expected)
            # ESCAPE oracle = PostgreSQL semantics. WHERE-clause evaluation
            # (query/expr.rs) drops the escape field; JOIN conditions
            # (join/eval.rs) pass it. Both probes below should match PG.
            escape = try_rows(cur, "SELECT id FROM ilike_t WHERE s ILIKE 'a!_c' ESCAPE '!' ORDER BY id")
            check("G-09 WHERE ESCAPE '!' a!_c", escape[1] if escape[0] == "OK" else escape, [5])
            escape_nomatch = try_rows(cur, "SELECT id FROM ilike_t WHERE s ILIKE 'a!%c' ESCAPE '!' ORDER BY id")
            check("G-09 WHERE ESCAPE '!' a!%c (literal %)", escape_nomatch[1] if escape_nomatch[0] == "OK" else escape_nomatch, [6])
            cur.execute("DROP TABLE IF EXISTS ilike_sink")
            cur.execute("CREATE TABLE ilike_sink (k INT)")
            cur.execute("INSERT INTO ilike_sink VALUES (1)")
            join_escape = try_rows(
                cur,
                "SELECT t.id FROM ilike_t t JOIN ilike_sink s ON t.s ILIKE 'a!_c' ESCAPE '!' ORDER BY t.id",
            )
            note("G-09 JOIN-condition ESCAPE '!' a!_c", join_escape)
            cur.execute("DROP TABLE ilike_sink")
            cur.execute("DROP TABLE ilike_t")

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("FAILURES: " + "; ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
