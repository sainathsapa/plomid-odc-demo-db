#!/usr/bin/env python3
"""Realistic workload probe for the 2026-10-16 audit.

Families: OLTP point-ops, OLAP aggregates/group-by, HTAP vector+relational,
anti-overfitting checks (duplicates, NULLs, deterministic expectations).

Usage: python3 workload_probe.py [DSN]
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


def main() -> int:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            # ---------------- OLTP.
            cur.execute("DROP TABLE IF EXISTS wl_users")
            cur.execute("DROP TABLE IF EXISTS wl_orders")
            cur.execute(
                "CREATE TABLE wl_users (id BIGINT PRIMARY KEY, name TEXT, tier INT, score DOUBLE PRECISION)"
            )
            cur.execute(
                "CREATE TABLE wl_orders (id BIGINT PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT)"
            )
            t0 = time.perf_counter()
            for batch in range(4):
                values = ",".join(
                    f"({batch * 500 + i}, 'user_{batch * 500 + i}', {i % 5}, {round((i % 97) * 1.5, 2)})"
                    for i in range(500)
                )
                cur.execute(f"INSERT INTO wl_users VALUES {values}")
                orders = ",".join(
                    f"({batch * 1000 + i}, {batch * 500 + i % 500}, {round((i % 53) * 2.25, 2)}, "
                    f"'{('paid','open','void')[i % 3]}')"
                    for i in range(1000)
                )
                cur.execute(f"INSERT INTO wl_orders VALUES {orders}")
            insert_ms = (time.perf_counter() - t0) * 1000
            cur.execute("SELECT COUNT(*) FROM wl_users")
            users = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wl_orders")
            orders = cur.fetchone()[0]
            check("OLTP bulk insert counts", (users, orders) == (2000, 4000), f"users={users} orders={orders}")

            t0 = time.perf_counter()
            for i in range(200):
                cur.execute("SELECT name, tier FROM wl_users WHERE id = %s", (i * 7 % 2000,))
                cur.fetchone()
            point_ms = (time.perf_counter() - t0) * 1000

            cur.execute("UPDATE wl_users SET tier = 9 WHERE id < 100")
            cur.execute("SELECT COUNT(*) FROM wl_users WHERE tier = 9")
            updated = cur.fetchone()[0]
            check("OLTP update visible", updated == 100, f"{updated}")

            cur.execute("DELETE FROM wl_orders WHERE status = 'void'")
            cur.execute("SELECT COUNT(*) FROM wl_orders WHERE status = 'void'")
            deleted = cur.fetchone()[0]
            check("OLTP delete visible", deleted == 0, f"{deleted}")

            # ---------------- OLAP.
            cur.execute("SELECT status, COUNT(*), SUM(amount) FROM wl_orders GROUP BY status ORDER BY status")
            grouped = cur.fetchall()
            print(f"NOTE olap group-by: {grouped}")
            check("OLAP group-by rows", len(grouped) == 2, f"{grouped}")
            total = sum(row[2] for row in grouped)
            cur.execute("SELECT SUM(amount) FROM wl_orders")
            check("OLAP sum matches groups", abs(cur.fetchone()[0] - total) < 1e-6, f"{total}")
            cur.execute("SELECT COUNT(DISTINCT user_id) FROM wl_orders")
            distinct_users = cur.fetchone()[0]
            check("OLAP distinct", distinct_users == 2000, f"{distinct_users}")

            # ---------------- Anti-overfitting: duplicates + NULLs.
            cur.execute("DROP TABLE IF EXISTS wl_dup")
            cur.execute("CREATE TABLE wl_dup (id INT PRIMARY KEY, tag TEXT, v INT)")
            cur.execute(
                "INSERT INTO wl_dup VALUES (1,'a',1),(2,'a',1),(3,'a',NULL),(4,'b',NULL),"
                "(5,'b',2),(6,NULL,2),(7,NULL,2),(8,'c',3)"
            )
            cur.execute("SELECT tag, COUNT(*) FROM wl_dup GROUP BY tag ORDER BY tag")
            dup_groups = cur.fetchall()
            print(f"NOTE duplicate/null groups: {dup_groups}")
            cur.execute("SELECT COUNT(*) FROM wl_dup WHERE v IS NULL")
            nulls = cur.fetchone()[0]
            check("NULL count", nulls == 2, f"{nulls}")
            cur.execute("SELECT COUNT(*) FROM wl_dup WHERE v = 1")
            check("duplicate equality count", cur.fetchone()[0] == 2)
            cur.execute("SELECT DISTINCT v FROM wl_dup ORDER BY v")
            distinct_v = [r[0] for r in cur.fetchall()]
            # PostgreSQL default: ASC puts NULLs last.
            check("DISTINCT with NULL", distinct_v == [1, 2, 3, None], f"{distinct_v}")

            # ---------------- HTAP: vector + relational in one query set.
            cur.execute("DROP TABLE IF EXISTS wl_items")
            cur.execute("CREATE TABLE wl_items (id BIGINT PRIMARY KEY, category TEXT, e VECTOR(FLOAT32, 8))")
            cur.execute("DROP INDEX wl_items_e_vector_idx")
            import math

            for batch in range(2):
                values = ",".join(
                    f"({batch * 500 + i}, '{('A','B','C','D')[i % 4]}', "
                    f"[{round(math.sin(i / 10.0), 6)},{round(math.cos(i / 10.0), 6)},"
                    f"{round(math.sin(i / 20.0), 6)},{round(math.cos(i / 20.0), 6)},"
                    f"{round(math.sin(i / 30.0), 6)},{round(math.cos(i / 30.0), 6)},"
                    f"{round(math.sin(i / 40.0), 6)},{round(math.cos(i / 40.0), 6)}])"
                    for i in range(500)
                )
                cur.execute(f"INSERT INTO wl_items VALUES {values}")
            cur.execute("CREATE INDEX wl_items_h ON wl_items USING HNSW (e)")
            cur.execute("SET vector.auto_ann_min_rows = 0")
            probe = "[0.5,0.5,0.25,0.5,0.125,0.5,0.0625,0.5]"
            cur.execute(
                f"SELECT id, category FROM wl_items ORDER BY l2_distance(e, {probe}) LIMIT 10"
            )
            htap = cur.fetchall()
            check("HTAP query returns 10", len(htap) == 10, f"{len(htap)}")
            cur.execute("SELECT category, COUNT(*) FROM wl_items GROUP BY category ORDER BY category")
            cats = cur.fetchall()
            check("HTAP relational side intact", [c[0] for c in cats] == ["A", "B", "C", "D"], f"{cats}")

            cur.execute("DROP TABLE IF EXISTS wl_users")
            cur.execute("DROP TABLE IF EXISTS wl_orders")
            cur.execute("DROP TABLE IF EXISTS wl_dup")
            cur.execute("DROP TABLE IF EXISTS wl_items")

    print(f"\n{PASS}/{PASS + FAIL} checks passed (insert={insert_ms:.0f}ms point200={point_ms:.0f}ms)")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
