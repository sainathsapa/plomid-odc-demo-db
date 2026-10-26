#!/usr/bin/env python3
"""PLOMID vector full-qualification harness (production release gate).

Two layers, one exit code:

1. SQL suite -- executes ``tests/VECTOR_FULL_QUALIFICATION.sql``
   statement-by-statement against a scratch schema. Statements preceded
   by a ``-- EXPECT-ERROR:`` comment MUST fail; every other statement
   MUST succeed. ``SELECT 'CHECK <name>', CASE ...`` probe rows are
   collected and every probe must read PASS.
2. Programmatic probes -- recall benchmark (2000 rows, seeded), Top-K
   latency distribution, concurrency smoke, settings validation and
   observability shape checks that SQL alone cannot express.

Environment (same conventions as prod_test.py / vector_test.py):

    DB_DSN     PostgreSQL connection string
               (default postgresql://plomid:plomid@127.0.0.1:6000/plomid)
    TEST_SCHEMA  Schema for the programmatic layer (default vqual).
               The SQL suite manages its own ``vq`` schema.

    NEVER point this at a production database.
"""

import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg

DSN = os.environ.get("DB_DSN", "postgresql://plomid:plomid@127.0.0.1:6000/plomid")
SCHEMA = os.environ.get("TEST_SCHEMA", "vqual")
SQL_SUITE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "VECTOR_FULL_QUALIFICATION.sql",
)

PASSED: list[str] = []
FAILED: list[tuple[str, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSED.append(name)
        print(f"  ok: {name}", flush=True)
    else:
        FAILED.append((name, detail))
        print(f"  FAIL: {name} {detail}", flush=True)


def split_statements(text: str) -> list[tuple[str, bool]]:
    """Split SQL text into (statement, expect_error) pairs.

    Quote-aware (single quotes with '' escapes, double-quoted idents),
    ``--`` line comments honored. A statement is failure-expected when
    its immediately preceding non-empty line starts with -- EXPECT-ERROR.
    """
    stmts: list[tuple[str, bool]] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    i, n = 0, len(text)
    pending_expect = False
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if line.startswith("-- EXPECT-ERROR"):
            pending_expect = True
            continue
        if not line or line.startswith("--") or line.startswith("\\"):
            # psql meta-commands (e.g. \set) never reach the wire.
            continue
        j = 0
        while j < len(raw_line):
            ch = raw_line[j]
            nxt = raw_line[j + 1] if j + 1 < len(raw_line) else ""
            if in_single:
                buf.append(ch)
                if ch == "'" and nxt != "'":
                    in_single = False
                elif ch == "'" and nxt == "'":
                    buf.append(nxt)
                    j += 1
            elif in_double:
                buf.append(ch)
                if ch == '"':
                    in_double = False
            elif ch == "'":
                in_single = True
                buf.append(ch)
            elif ch == '"':
                in_double = True
                buf.append(ch)
            elif ch == ";" :
                statement = "".join(buf).strip()
                if statement:
                    stmts.append((statement, pending_expect))
                buf = []
                pending_expect = False
            else:
                buf.append(ch)
            j += 1
        buf.append("\n")
    tail = "".join(buf).strip()
    if tail:
        stmts.append((tail, pending_expect))
    return stmts


def run_sql_suite(conn) -> None:
    with open(SQL_SUITE, encoding="utf-8") as handle:
        statements = split_statements(handle.read())
    print(f"  sql suite: {len(statements)} statements from VECTOR_FULL_QUALIFICATION.sql",
          flush=True)
    for index, (sql, expect_error) in enumerate(statements):
        label = sql[:70].replace("\n", " ")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
            if expect_error:
                check(f"sql[{index}] must fail: {label}", False, "succeeded")
            else:
                for row in rows:
                    if (isinstance(row[0], str) and row[0].startswith("CHECK ")
                            and len(row) > 1):
                        check(f"sql:{row[0][6:]}", row[1] == "PASS",
                              "" if row[1] == "PASS" else str(row))
        except Exception as exc:  # noqa: BLE001 - harness records, never raises
            if expect_error:
                PASSED.append(f"sql[{index}] rejected")
            else:
                check(f"sql[{index}]: {label}", False,
                      f"{type(exc).__name__}: {exc}")
        if (index + 1) % 50 == 0:
            conn.commit()


def l2(a, b) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def run_programmatic() -> None:
    random.seed(20261013)
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            cur.execute(f"CREATE SCHEMA {SCHEMA}")
            cur.execute(
                f"CREATE TABLE {SCHEMA}.vqbig "
                "(id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))"
            )
            batch = []
            for i in range(2000):
                c = i % 8
                lanes = [1.0 if j == c else round(random.gauss(0, 0.05), 4)
                         for j in range(8)]
                batch.append(f"({i}, [{','.join(map(str, lanes))}])")
                if len(batch) == 200:
                    cur.execute(
                        f"INSERT INTO {SCHEMA}.vqbig VALUES {','.join(batch)}")
                    batch = []
            check("prog: 2000 rows loaded", True)

            probe = [1.0] + [0.0] * 7
            probe_lit = "[" + ",".join(map(str, probe)) + "]"
            lat = []
            ann_ids: list = []
            for _ in range(20):
                t0 = time.perf_counter()
                cur.execute(
                    f"SELECT id FROM {SCHEMA}.vqbig "
                    f"ORDER BY l2_distance(e, {probe_lit}) LIMIT 10")
                lat.append((time.perf_counter() - t0) * 1000)
                ann_ids = [r[0] for r in cur.fetchall()]
            lat.sort()
            check("prog: topk p50 < 500ms", lat[10] < 500, f"p50={lat[10]:.1f}ms")
            check("prog: topk max < 5000ms", lat[-1] < 5000, f"max={lat[-1]:.1f}ms")
            print(f"  top-k latency p50={lat[10]:.1f}ms p95={lat[18]:.1f}ms "
                  f"max={lat[-1]:.1f}ms", flush=True)

            idx = cur.execute(
                "SELECT index_name, method, state FROM plomid_vector_indexes()"
            ).fetchall()
            check("prog: indexes READY", any(r[2] == "READY" for r in idx),
                  str(idx))
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'vqbig'"
                " AND indexname LIKE '%_vector_idx'")
            found = cur.fetchall()
            check("prog: auto index present", len(found) >= 1, str(found))
            for (name,) in found:
                cur.execute(f"DROP INDEX {name}")
            cur.execute(
                f"SELECT id FROM {SCHEMA}.vqbig "
                f"ORDER BY l2_distance(e, {probe_lit}) LIMIT 10")
            exact_ids = [r[0] for r in cur.fetchall()]
            check("prog: ANN == exact set", set(ann_ids) == set(exact_ids),
                  f"ann={ann_ids} exact={exact_ids}")

            idrows = cur.execute(f"SELECT id, e FROM {SCHEMA}.vqbig").fetchall()
            scored = sorted(
                (l2([float(x) for x in t.strip("[]").split(",")], probe), i)
                for i, t in idrows)[:10]
            oracle = [i for _, i in scored]
            recall = len(set(ann_ids) & set(oracle))
            check("prog: recall@10 >= 9", recall >= 9,
                  f"ann={ann_ids} oracle={oracle}")

            # Concurrency smoke: disjoint id ranges across workers.
            def worker(w: int) -> None:
                with psycopg.connect(DSN, autocommit=True) as conn_w:
                    with conn_w.cursor() as cur_w:
                        rows = ",".join(
                            f"({100000 + w * 100 + k}, [0,0,0,0,0,0,0,0])"
                            for k in range(25))
                        cur_w.execute(
                            f"INSERT INTO {SCHEMA}.vqbig VALUES {rows}")

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(as_completed(
                    pool.submit(worker, w) for w in range(8)))
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.vqbig WHERE id >= 100000")
            got = cur.fetchone()[0]
            check("prog: concurrent +200", got == 200, str(got))

            # Settings + observability (wire contract).
            cur.execute("SHOW vector.ef_search")
            check("prog: show default 100", cur.fetchone()[0] == "100",
                  "")
            cur.execute("SET vector.ef_search = 64")
            cur.execute("SHOW vector.ef_search")
            check("prog: set applies", cur.fetchone()[0] == "64", "")
            cur.execute("SET vector.ef_search = 100")
            for bad in ("SET vector.ef_search = 99999", "SET vector.nope = 1"):
                try:
                    cur.execute(bad)
                    check(f"prog: reject {bad!r}", False, "accepted")
                except Exception:  # noqa: BLE001 - expected path
                    check(f"prog: reject {bad!r}", True)

            stats = cur.execute("SELECT * FROM plomid_vector_stats()").fetchall()
            check("prog: stats 12 cols", len(stats) == 1 and len(stats[0]) == 12,
                  str(stats))
            # Indexes READY was already proven before the auto index was
            # dropped for the exact-fallback comparison above; re-checking
            # here would assert on an intentionally index-free table.

            cur.execute(f"DROP SCHEMA {SCHEMA} CASCADE")


def main() -> int:
    print(f"vector full qualification against {DSN}", flush=True)
    with psycopg.connect(DSN, autocommit=True) as conn:
        run_sql_suite(conn)
    run_programmatic()
    print(f"\npassed {len(PASSED)}, failed {len(FAILED)}")
    for name, detail in FAILED:
        print(f"  FAILED {name}: {detail}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
