#!/usr/bin/env python3
"""PLOMID release-readiness live probes, 2026-10-14 audit.
Each probe prints PASS/FAIL with expected-vs-actual. Exit 0 only if all pass.
"""
import random
import struct
import sys
import threading

import psycopg

DSN = "postgresql://plomid:plomid@127.0.0.1:55432/plomid"
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")


def q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        try:
            return cur.fetchall()
        except psycopg.ProgrammingError:
            return None

# ---- P0-002: float literals / division ----
c = psycopg.connect(DSN, autocommit=True)
r = q(c, "SELECT 3.0/2.0;")[0][0]
check("P0-002 3.0/2.0 == 1.5", r == 1.5, f"got {r!r}")
r = q(c, "SELECT 1/2;")[0][0]
check("P0-002 1/2 == 0 (int division)", r == 0, f"got {r!r}")
r = q(c, "SELECT 5.0/2, 0.1+0.2;")[0]
check("P0-002 5.0/2 == 2.5", r[0] == 2.5, f"got {r[0]!r}")

# ---- P0-001: extended-protocol multi-statement batch ----
c2 = psycopg.connect(DSN)  # NOT autocommit: extended protocol
with c2.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS p0_batch;")
    cur.execute(
        "BEGIN; CREATE TABLE p0_batch (id BIGINT PRIMARY KEY, v INT); "
        "INSERT INTO p0_batch VALUES (1, 10), (2, 20); COMMIT;"
    )
c2.commit()
r = q(c, "SELECT COUNT(*), SUM(v) FROM p0_batch;")[0]
check("P0-001 extended batch all applied", r == (2, 30), f"got {r!r}")
c2.close()

# ---- txn: rollback drops staged rows ----
c3 = psycopg.connect(DSN, autocommit=True)
q(c3, "DROP TABLE IF EXISTS p_txn; CREATE TABLE p_txn (id BIGINT PRIMARY KEY, v INT);")
c4 = psycopg.connect(DSN)
with c4.cursor() as cur:
    cur.execute("BEGIN; INSERT INTO p_txn VALUES (99, 1); ROLLBACK;")
c4.commit()
r = q(c3, "SELECT COUNT(*) FROM p_txn WHERE id = 99;")[0][0]
check("txn ROLLBACK drops row", r == 0, f"got {r!r}")

# ---- concurrent unique race: exactly one winner ----
q(c3, "DROP TABLE IF EXISTS p_race; CREATE TABLE p_race (id BIGINT PRIMARY KEY, w INT);")
wins = []
def racer(i):
    try:
        cc = psycopg.connect(DSN, autocommit=True)
        with cc.cursor() as cur:
            cur.execute("INSERT INTO p_race VALUES (7, %s);", (i,))
        wins.append(i)
    except Exception:
        pass
    finally:
        try:
            cc.close()
        except Exception:
            pass
threads = [threading.Thread(target=racer, args=(i,)) for i in range(8)]
[t.start() for t in threads]
[t.join() for t in threads]
r = q(c3, "SELECT COUNT(*) FROM p_race WHERE id = 7;")[0][0]
check("unique race exactly-once", r == 1 and len(wins) == 1, f"rows={r} winners={wins}")

# ---- catalog visibility to already-connected session ----
cA = psycopg.connect(DSN, autocommit=True)
q(c3, "DROP TABLE IF EXISTS p_vis; CREATE TABLE p_vis (id BIGINT PRIMARY KEY); INSERT INTO p_vis VALUES (5);")
try:
    r = q(cA, "SELECT COUNT(*) FROM p_vis;")[0][0]
    check("catalog visible without reconnect", r == 1, f"got {r!r}")
except Exception as e:
    check("catalog visible without reconnect", False, f"error: {e}")

# ---- RBAC: DATABASE DDL bypass probe (side-effect verified: a bare OK
# from a utility statement proves nothing, and q() swallows ProgrammingError)
def run_raw(conn, sql):
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
            try:
                return ("OK", cur.fetchall())
            except Exception:
                return ("OK", None)
        except Exception as e:
            return ("ERR", f"{type(e).__name__}")


def dblist(conn):
    st, rows = run_raw(conn, "SHOW DATABASES;")
    return sorted(r[0] for r in rows) if st == "OK" else st


try:
    q(c3, "CREATE ROLE lowpriv WITH LOGIN PASSWORD 'x';")
except Exception:
    pass  # already exists from a prior run; DROP ROLE is unsupported
lp = psycopg.connect("postgresql://lowpriv:x@127.0.0.1:55432/plomid", autocommit=True)
run_raw(c3, "CREATE DATABASE rbac_victim;")
st, _ = run_raw(lp, "CREATE DATABASE rbac_evil2;")
after_create = dblist(c3)
check(
    "RBAC CREATE DATABASE denied for lowpriv",
    st == "ERR" and "rbac_evil2" not in after_create,
    f"status={st} dblist={after_create}",
)
st, _ = run_raw(lp, "DROP DATABASE rbac_victim;")
after_drop = dblist(c3)
check(
    "RBAC DROP DATABASE denied for lowpriv",
    st == "ERR" and "rbac_victim" in after_drop,
    f"status={st} dblist={after_drop}",
)
names = dblist(lp)
check(
    "SHOW DATABASES hides unconnected DBs",
    isinstance(names, list) and "rbac_victim" not in names,
    f"got {names!r}",
)
lp.close()

# ---- vector: exact k-NN vs python oracle ----
rng = random.Random(20261014)
DIM = 8
N = 200
vecs = [[rng.uniform(-1, 1) for _ in range(DIM)] for _ in range(N)]
q(c3, "DROP TABLE IF EXISTS p_vec; CREATE TABLE p_vec (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8));")
with c3.cursor() as cur:
    for i, v in enumerate(vecs):
        cur.execute("INSERT INTO p_vec VALUES (%s, %s);", (i, "[" + ",".join(f"{x:.6f}" for x in v) + "]"))
query = [0.1 * (i + 1) for i in range(DIM)]
qlit = "[" + ",".join(str(x) for x in query) + "]"
def l2(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))
want = sorted(range(N), key=lambda i: (l2(vecs[i], query), i))[:5]
got = [row[0] for row in q(c3, f"SELECT id FROM p_vec ORDER BY l2_distance(e, {qlit}) LIMIT 5;")]
check("vector exact k-NN matches oracle", got == want, f"got {got} want {want}")
got2 = [row[0] for row in q(c3, f"SELECT id FROM p_vec ORDER BY e <-> {qlit} LIMIT 5;")]
check("vector <-> agrees with l2_distance", got2 == want, f"got {got2}")
def cos(a, b):
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return 1 - sum(x * y for x, y in zip(a, b)) / (na * nb)
want_cos = sorted(range(N), key=lambda i: (cos(vecs[i], query), i))[:5]
got_cos = [row[0] for row in q(c3, f"SELECT id FROM p_vec ORDER BY e <=> {qlit} LIMIT 5;")]
check("vector <=> matches cosine oracle", got_cos == want_cos, f"got {got_cos} want {want_cos}")
# ANN path if indexable
try:
    q(c3, "CREATE INDEX p_vec_hnsw ON p_vec USING HNSW (e);")
    got3 = [row[0] for row in q(c3, f"SELECT id FROM p_vec ORDER BY l2_distance(e, {qlit}) LIMIT 5;")]
    check("vector ANN k-NN matches oracle", got3 == want, f"got {got3}")
except Exception as e:
    check("vector ANN k-NN matches oracle", False, f"index/query error: {e}")
# NULL / zero-vector behavior
q(c3, "INSERT INTO p_vec VALUES (1000, [0,0,0,0,0,0,0,0]);")
r = q(c3, "SELECT COUNT(*) FROM (SELECT id FROM p_vec ORDER BY l2_distance(e, [0,0,0,0,0,0,0,0]) LIMIT 3) t;")[0][0]
check("vector zero-query top-3 returns 3", r == 3, f"got {r!r}")

failed = [n for n, ok, _ in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} probes passed")
sys.exit(1 if failed else 0)
