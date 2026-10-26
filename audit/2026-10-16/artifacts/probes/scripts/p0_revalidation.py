#!/usr/bin/env python3
"""P0 revalidation probe for the 2026-10-16 audit.

Re-tests the two former P0s exactly as defined in audit/2026-10-15:
  P0-001  extended-protocol batch handling (parameterless multi-statement via
          the simple path applies all parts; a true extended Parse with a
          multi-statement body must fail 42601 with zero side effects and a
          reusable session).
  P0-002  floating-point constant handling (literal float math at float8,
          integer contract intact, fail-closed text division, NULL
          propagation, DML coercion; untyped-param behavior recorded as the
          documented REAUDIT-002 limit, not a failure).

Usage: python3 p0_revalidation.py [DSN]
Exit code 0 only if every asserted check passes.
"""

import re
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


def expect_error(label: str, conn, sql: str, pattern: str, params=None) -> None:
    global CHECKS
    CHECKS += 1
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        print(f"FAIL {label}: no error raised")
        FAILURES.append(label)
    except Exception as error:  # noqa: BLE001
        text = str(error).splitlines()[0]
        ok = re.search(pattern, text) is not None
        print(f"{'PASS' if ok else 'FAIL'} {label}: {text[:120]}")
        if not ok:
            FAILURES.append(label)


def scalar(cur, sql: str, params=None):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def main() -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS p0_ms3_t")
            cur.execute("CREATE TABLE p0_ms3_t (v INT)")

        # ---- P0-001a: parameterless multi-statement (simple Query path).
        with conn.cursor() as cur:
            try:
                cur.execute("BEGIN; INSERT INTO p0_ms3_t VALUES (1); COMMIT;")
                simple_error = None
            except Exception as error:  # noqa: BLE001
                simple_error = str(error).splitlines()[0]
        with conn.cursor() as cur:
            rows = cur.execute("SELECT v FROM p0_ms3_t").fetchall()
        check("P0-001a simple multi-statement applied", (simple_error, rows), (None, [(1,)]))
        with conn.cursor() as cur:
            cur.execute("DELETE FROM p0_ms3_t")

        # ---- P0-001b: true extended Parse with multi-statement body.
        # A real parameter forces psycopg's extended path (empty tuples are
        # sent via the simple Query path by psycopg, which is not the P0).
        expect_error(
            "P0-001b extended Parse multi-statement rejected",
            conn,
            "BEGIN; INSERT INTO p0_ms3_t VALUES (%s); COMMIT;",
            r"42601|cannot insert multiple commands|syntax",
            params=(2,),
        )
        with conn.cursor() as cur:
            count = scalar(cur, "SELECT COUNT(*) FROM p0_ms3_t")
            usable = scalar(cur, "SELECT 42")
        check("P0-001b zero side effects + session reusable", (count, usable), (0, 42))

        # ---- P0-001c: repeated queries stay in sync.
        with conn.cursor() as cur:
            values = [scalar(cur, "SELECT 1") for _ in range(50)]
        check("P0-001c 50x repeated SELECT 1", values, [1] * 50)

        # ---- P0-001d: error recovery leaves the session usable.
        expect_error(
            "P0-001d error recovery",
            conn,
            "SELECT * FROM p0_ms3_no_such_table",
            r"does not exist|not found|no_such",
        )
        with conn.cursor() as cur:
            usable = scalar(cur, "SELECT 42")
        check("P0-001d session usable after error", usable, 42)

        # ---- P0-002: float matrix (value + type OID where available).
        with conn.cursor() as cur:
            cases = [
                ("SELECT 3.0 / 2.0", 1.5),
                ("SELECT 1.0 / 2.0", 0.5),
                ("SELECT 7.5 / 2.0", 3.75),
                ("SELECT 10.0 / 4.0", 2.5),
                ("SELECT 1.5 + 1", 2.5),
                ("SELECT 2.5 * 2", 5.0),
                ("SELECT 0.1 + 0.2", 0.30000000000000004),
                ("SELECT (1.0 + 2.0) * 3.0", 9.0),
                ("SELECT 10.0 % 3.0", 1.0),
                ("SELECT -3.0 / 2.0", -1.5),
                ("SELECT 1e2 / 4.0", 25.0),
                ("SELECT 1000000.0 / 3.0", 333333.3333333333),
            ]
            for sql, expected in cases:
                check(f"P0-002 {sql}", scalar(cur, sql), expected)
            check("P0-002 int contract 7/2", scalar(cur, "SELECT 7 / 2"), 3)
            check("P0-002 int contract 1/3", scalar(cur, "SELECT 1 / 3"), 0)
            check("P0-002 NULL propagation", scalar(cur, "SELECT NULL / 2.0"), None)
            check("P0-002 NULL propagation 2", scalar(cur, "SELECT 1.0 / NULL"), None)
            try:
                tag = scalar(cur, "SELECT pg_typeof(3.0 / 2.0)")
                check("P0-002 pg_typeof(3.0/2.0)", tag, "float8")
                check("P0-002 pg_typeof(1.0)", scalar(cur, "SELECT pg_typeof(1.0)"), "float8")
                check("P0-002 pg_typeof(1)", scalar(cur, "SELECT pg_typeof(1)"), "int4")
            except Exception as error:  # noqa: BLE001
                print(f"NOTE pg_typeof probe unavailable: {str(error).splitlines()[0][:100]}")
        expect_error(
            "P0-002 division by zero errors",
            conn,
            "SELECT 1.0 / 0.0",
            r"division by zero|divide",
        )
        expect_error(
            "P0-002 text division fails closed",
            conn,
            "SELECT 'a' / 'b'",
            r"numeric|operator|argument",
        )

        # ---- P0-002 DML coercion.
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS p0_int_t")
            cur.execute("CREATE TABLE p0_int_t (v INT)")
            cur.execute("INSERT INTO p0_int_t VALUES (3.0)")
            coerced = scalar(cur, "SELECT v FROM p0_int_t")
            check("P0-002 INSERT 3.0 into INT coerces", coerced, 3)
            cur.execute("DROP TABLE p0_int_t")

        # ---- P0-002 folded-vs-runtime + documented untyped-param limit.
        with conn.cursor() as cur:
            folded = scalar(cur, "SELECT 3.0 / 2.0")
            check("P0-002 folded literal", folded, 1.5)
            cast = scalar(cur, "SELECT %s::float8 / %s::float8", (3.0, 2.0))
            check("P0-002 explicit-cast params", cast, 1.5)
            untyped = scalar(cur, "SELECT %s / %s", (3.0, 2.0))
            print(
                f"NOTE REAUDIT-002 untyped %s/%s with (3.0, 2.0) -> {untyped!r} "
                f"(documented limit; expected '1' text)"
            )

    print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
    if FAILURES:
        print("FAILURES: " + "; ".join(FAILURES))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
