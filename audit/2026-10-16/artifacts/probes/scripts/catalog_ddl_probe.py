#!/usr/bin/env python3
"""Catalog/DDL probe for the 2026-10-16 audit.

Covers: cross-connection visibility, drop visibility, restart persistence, and
a FENCED live attempt at PLOMID-REAUDIT-001 (concurrent-DDL ID collision):
4 threads x 25 iterations of create/insert/read-own-marker/drop with a
barrier per iteration, plus a same-name CREATE race, plus OID uniqueness and
leftover checks. Disposable data dir only.

Usage: python3 catalog_ddl_probe.py [--bin BIN] [--root DIR] [--port P]
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import threading
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--bin", default="target/release/plomid-server")
parser.add_argument("--root", default="/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-probe/catalog")
parser.add_argument("--port", type=int, default=6103)
args = parser.parse_args()

BIN = os.path.abspath(args.bin)
ROOT = args.root
PORT = args.port
DATA = os.path.join(ROOT, "data")
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
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


def boot() -> subprocess.Popen:
    proc = subprocess.Popen(
        [BIN, "--data", DATA, "--host", "127.0.0.1", "--port", str(PORT),
         "--username", "plomid", "--password", "plomid", "--log-level", "warn"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited during startup")
        try:
            with psycopg.connect(DSN, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return proc
        except Exception:  # noqa: BLE001
            time.sleep(0.15)
    raise RuntimeError("server never ready")


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)
    proc = boot()
    try:
        conn_a = psycopg.connect(DSN, autocommit=True)
        conn_b = psycopg.connect(DSN, autocommit=True)
        with conn_a.cursor() as ca, conn_b.cursor() as cb:
            ca.execute("CREATE TABLE catvis (id INT PRIMARY KEY, s TEXT)")
            ca.execute("INSERT INTO catvis VALUES (1,'hello')")
            cb.execute("SELECT * FROM catvis")
            check("visibility: B sees A's new table rows", cb.fetchall() == [(1, "hello")])
            cb.execute("INSERT INTO catvis VALUES (2,'world')")
            ca.execute("SELECT COUNT(*) FROM catvis")
            check("visibility: A sees B's write", ca.fetchone()[0] == 2)

            ca.execute("CREATE INDEX catvis_s ON catvis (s)")
            cb.execute("SELECT id FROM catvis WHERE s = 'world'")
            check("visibility: index created on A usable on B", cb.fetchall() == [(2,)])

            cb.execute("SELECT s FROM catvis WHERE id = %s", (1,))
            check("visibility: B prepared/param query", cb.fetchone()[0] == "hello")

            ca.execute("DROP TABLE catvis")
            err = None
            try:
                cb.execute("SELECT * FROM catvis")
            except Exception as error:  # noqa: BLE001
                err = str(error).splitlines()[0]
            check("visibility: drop is immediate for B", err is not None and "does not exist" in err.lower(), err or "stale success?!")
        conn_a.close()
        conn_b.close()

        # ---- Fenced REAUDIT-001 attempt: interleaved create/insert/drop.
        THREADS = 4
        ITERATIONS = 25
        barrier = threading.Barrier(THREADS)
        errors: list[str] = []
        mismatches: list[str] = []
        lock = threading.Lock()

        def worker(tid: int) -> None:
            try:
                conn = psycopg.connect(DSN, autocommit=True)
            except Exception as error:  # noqa: BLE001
                with lock:
                    errors.append(f"connect t{tid}: {error}")
                return
            for iteration in range(ITERATIONS):
                barrier.wait()
                table = f"race_t{tid}_{iteration}"
                marker = f"t{tid}i{iteration}"
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, s TEXT)")
                        cur.execute(f"INSERT INTO {table} VALUES (1, '{marker}')")
                        cur.execute(f"SELECT s FROM {table} WHERE id = 1")
                        got = cur.fetchone()
                        if got is None or got[0] != marker:
                            with lock:
                                mismatches.append(f"{table}: {got!r} != {marker!r}")
                        cur.execute(f"DROP TABLE {table}")
                except Exception as error:  # noqa: BLE001
                    with lock:
                        errors.append(f"{table}: {str(error).splitlines()[0][:100]}")
            conn.close()

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(THREADS)]
        t0 = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - t0
        check("REAUDIT-001 interleaved create/drop: no marker mismatches", not mismatches, f"{mismatches[:3]}")
        check("REAUDIT-001 interleaved create/drop: no errors", not errors, f"{errors[:3]}")
        note("interleaved DDL", f"{THREADS}x{ITERATIONS} in {elapsed:.1f}s, errors={len(errors)}")

        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'race_%'")
                leftovers = cur.fetchall()
                check("no leftover race_ tables", leftovers == [], f"{leftovers[:5]}")

        # ---- Same-name CREATE race.
        results = []
        lock2 = threading.Lock()
        start = threading.Barrier(2)

        def namesake() -> None:
            conn = psycopg.connect(DSN, autocommit=True)
            start.wait()
            try:
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE namesake (id INT)")
                with lock2:
                    results.append("ok")
            except Exception as error:  # noqa: BLE001
                with lock2:
                    results.append(str(error).splitlines()[0][:80])
            conn.close()

        threads = [threading.Thread(target=namesake) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        ok_count = results.count("ok")
        check("same-name CREATE race: exactly one winner", ok_count == 1, f"{results}")
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename = 'namesake'")
                count = cur.fetchone()[0]
                check("same-name race: one catalog entry", count == 1, f"{count}")
                cur.execute("DROP TABLE IF EXISTS namesake")

        # ---- Restart persistence.
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE rst (id INT PRIMARY KEY, s TEXT)")
                cur.execute("INSERT INTO rst VALUES (1,'persisted')")
                cur.execute("CREATE TABLE todrop (id INT)")
                cur.execute("DROP TABLE todrop")
        stop(proc)
        proc = boot()
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT s FROM rst WHERE id = 1")
                check("restart: table persisted", cur.fetchone() == ("persisted",))
                err = None
                try:
                    cur.execute("SELECT * FROM todrop")
                except Exception as error:  # noqa: BLE001
                    err = str(error).splitlines()[0]
                check("restart: dropped stays dropped", err is not None, err or "stale?!")
                cur.execute("DROP TABLE rst")
    finally:
        stop(proc)

    print(f"\n{PASS}/{PASS + FAIL} checks passed")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
