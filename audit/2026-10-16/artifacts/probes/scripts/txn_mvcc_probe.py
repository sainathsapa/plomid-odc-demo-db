#!/usr/bin/env python3
"""Transaction / MVCC probe for the 2026-10-16 audit.

Covers: BEGIN/COMMIT/ROLLBACK visibility across two live connections, pinned
snapshot within an open transaction, DDL-in-transaction refusal, multi-
statement txn on one connection, and a same-row write-write record (behavior
recorded honestly rather than asserted).

Usage: python3 txn_mvcc_probe.py [DSN]
"""

import sys
import time

import psycopg

DSN = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "postgresql://plomid:plomid@127.0.0.1:6100/plomid"
)

PASS = 0
FAIL = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    print(f"{'PASS' if ok else 'FAIL'} {label}" + (f": {detail}" if detail else ""), flush=True)
    if ok:
        PASS += 1
    else:
        FAIL += 1


def note(label: str, value) -> None:
    print(f"NOTE {label}: {value}", flush=True)


def scalar(cur, sql: str):
    cur.execute(sql)
    return cur.fetchone()


def main() -> int:
    setup = psycopg.connect(DSN, autocommit=True)
    with setup.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS txn_t")
        cur.execute("CREATE TABLE txn_t (id INT PRIMARY KEY, s TEXT)")

    a = psycopg.connect(DSN, autocommit=True)
    b = psycopg.connect(DSN, autocommit=True)
    try:
        # ---- Uncommitted write invisible to B.
        with a.cursor() as ca:
            ca.execute("BEGIN")
            ca.execute("INSERT INTO txn_t VALUES (1,'one')")
        with b.cursor() as cb:
            cb.execute("SELECT COUNT(*) FROM txn_t")
            check("uncommitted invisible to B", cb.fetchone()[0] == 0)
        with a.cursor() as ca:
            ca.execute("COMMIT")
        with b.cursor() as cb:
            cb.execute("SELECT COUNT(*) FROM txn_t")
            check("committed visible to B", cb.fetchone()[0] == 1)

        # ---- ROLLBACK discards.
        with a.cursor() as ca:
            ca.execute("BEGIN")
            ca.execute("INSERT INTO txn_t VALUES (2,'two')")
            ca.execute("ROLLBACK")
        with b.cursor() as cb:
            cb.execute("SELECT COUNT(*) FROM txn_t")
            check("rollback discards", cb.fetchone()[0] == 1)

        # ---- Default isolation (READ COMMITTED): each statement refreshes
        # to the latest committed snapshot (docs: transactions/using-transactions).
        with a.cursor() as ca:
            ca.execute("BEGIN")
            ca.execute("SELECT COUNT(*) FROM txn_t")
            before = ca.fetchone()[0]
        with b.cursor() as cb:
            cb.execute("INSERT INTO txn_t VALUES (3,'three')")
        with a.cursor() as ca:
            ca.execute("SELECT COUNT(*) FROM txn_t")
            during = ca.fetchone()[0]
            ca.execute("COMMIT")
        with a.cursor() as ca:
            ca.execute("SELECT COUNT(*) FROM txn_t")
            after = ca.fetchone()[0]
        check("read-committed refresh (default)", (before, during, after) == (1, 2, 2), f"{before}/{during}/{after}")

        # ---- REPEATABLE READ: BEGIN image pinned plus own writes.
        rr_error = None
        rr_before = rr_during = rr_after = None
        try:
            with a.cursor() as ca:
                ca.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                ca.execute("SELECT COUNT(*) FROM txn_t")
                rr_before = ca.fetchone()[0]
            with b.cursor() as cb:
                cb.execute("INSERT INTO txn_t VALUES (4,'four')")
            with a.cursor() as ca:
                ca.execute("SELECT COUNT(*) FROM txn_t")
                rr_during = ca.fetchone()[0]
                ca.execute("COMMIT")
            with a.cursor() as ca:
                ca.execute("SELECT COUNT(*) FROM txn_t")
                rr_after = ca.fetchone()[0]
        except Exception as error:  # noqa: BLE001
            rr_error = str(error).splitlines()[0][:110]
        if rr_error is None:
            check("repeatable-read pinned snapshot", (rr_before, rr_during, rr_after) == (2, 2, 3), f"{rr_before}/{rr_during}/{rr_after}")
        else:
            note("repeatable-read experiment", rr_error)

        # ---- Multi-statement transaction on one connection.
        with a.cursor() as ca:
            ca.execute("DELETE FROM txn_t")
        with a.cursor() as ca:
            ca.execute("BEGIN")
            ca.execute("INSERT INTO txn_t VALUES (10,'ten')")
            ca.execute("INSERT INTO txn_t VALUES (11,'eleven')")
            ca.execute("COMMIT")
        with b.cursor() as cb:
            cb.execute("SELECT COUNT(*) FROM txn_t")
            check("multi-statement txn atomic", cb.fetchone()[0] == 2)

        # ---- DDL inside a transaction: documented refusal (record).
        with a.cursor() as ca:
            ca.execute("BEGIN")
            ddl_error = None
            try:
                ca.execute("CREATE TABLE txn_ddl_probe (id INT)")
            except Exception as error:  # noqa: BLE001
                ddl_error = str(error).splitlines()[0][:110]
            ca.execute("COMMIT") if ddl_error is None else ca.execute("ROLLBACK")
        note("DDL in transaction", ddl_error or "accepted")
        with b.cursor() as cb:
            leftover = None
            try:
                cb.execute("SELECT 1 FROM txn_ddl_probe")
                leftover = "exists"
            except Exception:  # noqa: BLE001
                leftover = "absent"
            if ddl_error is not None:
                check("refused DDL leaves no table", leftover == "absent", leftover)

        # ---- Write-write on the same row from two open txns: record behavior.
        # Bounded experiment: the idle-in-transaction timeout (default 60s,
        # here 5s) is part of the observed semantics.
        with a.cursor() as ca, b.cursor() as cb:
            ca.execute("SET idle_in_transaction_session_timeout = '5s'")
            ca.execute("BEGIN")
            ca.execute("UPDATE txn_t SET s='A' WHERE id=10")
            behavior = None
            t0 = time.time()
            try:
                cb.execute("BEGIN")
                cb.execute("UPDATE txn_t SET s='B' WHERE id=10")
                cb.execute("COMMIT")
                behavior = f"second writer committed after {time.time()-t0:.1f}s (no error)"
            except Exception as error:  # noqa: BLE001
                behavior = f"second writer error after {time.time()-t0:.1f}s: " + str(error).splitlines()[0][:100]
                try:
                    cb.execute("ROLLBACK")
                except Exception:  # noqa: BLE001
                    pass
            try:
                ca.execute("COMMIT")
            except Exception as error:  # noqa: BLE001
                behavior = (behavior or "") + " | first COMMIT: " + str(error).splitlines()[0][:90]
        note("write-write same row", behavior)
        with b.cursor() as cb:
            value = scalar(cb, "SELECT s FROM txn_t WHERE id=10")
            note("final value of id=10", value)
            check("row still readable after write-write", value is not None, f"{value}")
    finally:
        a.close()
        b.close()
        with setup.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS txn_t")
            cur.execute("DROP TABLE IF EXISTS txn_ddl_probe")
        setup.close()

    print(f"\n{PASS}/{PASS + FAIL} checks passed")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
