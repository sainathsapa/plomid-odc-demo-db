#!/usr/bin/env python3
"""Scale probe for the 2026-10-16 audit.

Phase A  plain relational: 100k rows bulk insert, point selects, aggregate.
Phase B  vector 64-dim: 50k rows, HNSW build, cold/warm + recall vs exact.
Phase C  relational 1M-row attempt (G-02 partial: single-node, no vector),
         with wall-time budget; records outcome honestly either way.

Usage: python3 scale_probe.py [--bin BIN] [--root DIR] [--port P] [--budget SECONDS]
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--bin", default="target/release/plomid-server")
parser.add_argument("--root", default="/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-probe/scale")
parser.add_argument("--port", type=int, default=6104)
parser.add_argument("--budget", type=float, default=1800.0, help="1M phase wall budget (s)")
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


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)
    proc = boot()
    try:
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                # ---- Phase A: 100k relational.
                cur.execute("CREATE TABLE sc_rel (id BIGINT PRIMARY KEY, a INT, b DOUBLE PRECISION, s TEXT)")
                t0 = time.perf_counter()
                for batch in range(100):
                    values = ",".join(
                        f"({batch * 1000 + i}, {i % 1000}, {(i % 977) * 1.5}, 'row_{batch * 1000 + i}')"
                        for i in range(1000)
                    )
                    cur.execute(f"INSERT INTO sc_rel VALUES {values}")
                insert_ms = (time.perf_counter() - t0) * 1000
                cur.execute("SELECT COUNT(*) FROM sc_rel")
                count = cur.fetchone()[0]
                check("phase A insert 100k", count == 100000, f"count={count} in {insert_ms/1000:.1f}s ({100000/(insert_ms/1000):.0f} rows/s)")

                t0 = time.perf_counter()
                for i in range(500):
                    cur.execute("SELECT a FROM sc_rel WHERE id = %s", (i * 193 % 100000,))
                    cur.fetchone()
                point_ms = (time.perf_counter() - t0) * 1000
                note("phase A point selects x500", f"{point_ms:.0f}ms total ({point_ms/500:.2f}ms/op)")

                t0 = time.perf_counter()
                cur.execute("SELECT COUNT(*), SUM(a), AVG(b) FROM sc_rel WHERE a < 100")
                agg = cur.fetchone()
                agg_ms = (time.perf_counter() - t0) * 1000
                note("phase A aggregate", f"{agg} in {agg_ms:.0f}ms")

                # ---- Phase B: 50k vector 64-dim.
                import math

                cur.execute("CREATE TABLE sc_vec (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 64))")
                cur.execute("DROP INDEX sc_vec_e_vector_idx")
                t0 = time.perf_counter()
                for batch in range(50):
                    values = ",".join(
                        f"({batch * 1000 + i}, [{','.join(f'{(math.sin((batch*1000+i)/(k+7.0))):.6f}' for k in range(64))}])"
                        for i in range(1000)
                    )
                    cur.execute(f"INSERT INTO sc_vec VALUES {values}")
                vec_insert_ms = (time.perf_counter() - t0) * 1000
                note("phase B insert 50k dim64", f"{vec_insert_ms/1000:.1f}s ({50000/(vec_insert_ms/1000):.0f} rows/s)")
                cur.execute("CREATE INDEX sc_vec_h ON sc_vec USING HNSW (e)")
                cur.execute("SET vector.auto_ann_min_rows = 0")
                probe = "[" + ",".join("0.5" for _ in range(64)) + "]"
                query = f"SELECT id FROM sc_vec ORDER BY l2_distance(e, {probe}) LIMIT 10"
                t0 = time.perf_counter()
                cur.execute(query)
                ann_ids = [r[0] for r in cur.fetchall()]
                cold_ms = (time.perf_counter() - t0) * 1000
                warm = []
                for _ in range(10):
                    t0 = time.perf_counter()
                    cur.execute(query)
                    cur.fetchall()
                    warm.append((time.perf_counter() - t0) * 1000)
                cur.execute("SET vector.ann_enabled = off")
                cur.execute(query)
                exact_ids = [r[0] for r in cur.fetchall()]
                cur.execute("SET vector.ann_enabled = on")
                recall = len(set(ann_ids) & set(exact_ids)) / 10
                note("phase B HNSW 50k dim64", f"cold={cold_ms:.0f}ms warm_med={sorted(warm)[5]:.1f}ms recall@10={recall:.2f}")

                # ---- Phase C: 1M relational attempt.
                cur.execute("CREATE TABLE sc_big (id BIGINT PRIMARY KEY, a INT, s TEXT)")
                t0 = time.perf_counter()
                inserted = 0
                batch = 0
                while inserted < 1_000_000 and time.perf_counter() - t0 < args.budget:
                    values = ",".join(
                        f"({batch * 2000 + i}, {i % 1000}, 'x')" for i in range(2000)
                    )
                    cur.execute(f"INSERT INTO sc_big VALUES {values}")
                    inserted += 2000
                    batch += 1
                big_s = time.perf_counter() - t0
                cur.execute("SELECT COUNT(*) FROM sc_big")
                big_count = cur.fetchone()[0]
                if big_count == 1_000_000:
                    note("phase C 1M rows", f"inserted in {big_s:.1f}s ({1_000_000/big_s:.0f} rows/s)")
                else:
                    note("phase C partial", f"{big_count} rows in budget {args.budget}s ({big_count/big_s:.0f} rows/s) — NOT a completed 1M run")
                t0 = time.perf_counter()
                cur.execute("SELECT COUNT(*) FROM sc_big WHERE a = 7")
                filtered = cur.fetchone()[0]
                scan_ms = (time.perf_counter() - t0) * 1000
                note("phase C filter count", f"{filtered} rows in {scan_ms:.0f}ms")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
    print(f"\n{PASS}/{PASS + FAIL} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
