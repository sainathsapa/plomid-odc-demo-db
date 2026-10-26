#!/usr/bin/env python3
"""ef_search sweep on the hnsw64 investigation dir (same data, fresh boot)."""
import json
import math
import os
import signal
import subprocess
import sys
import time

import psycopg

BIN = os.path.abspath("target/release/plomid-server")
DATA = "/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-hnsw64-6175/data"
PORT = 6176
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
DIM = 64
PROBE = [0.5] * DIM
probe_sql = "[" + ",".join(str(v) for v in PROBE) + "]"


def l2(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


proc = subprocess.Popen(
    [BIN, "--data", DATA, "--host", "127.0.0.1", "--port", str(PORT),
     "--username", "plomid", "--password", "plomid", "--log-level", "warn"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
deadline = time.time() + 60
while time.time() < deadline:
    try:
        psycopg.connect(DSN, connect_timeout=2).close()
        break
    except Exception:  # noqa: BLE001
        time.sleep(0.2)
else:
    print("server never ready")
    proc.kill()
    sys.exit(1)

try:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET vector.auto_ann_min_rows = 0")
            for table in ("sc_sine", "sc_rand"):
                query = f"SELECT id FROM {table} ORDER BY l2_distance(e, {probe_sql}) LIMIT 10"
                cur.execute("SET vector.ann_enabled = off")
                cur.execute(query)
                exact_ids = [r[0] for r in cur.fetchall()]
                ids = ",".join(str(i) for i in exact_ids)
                cur.execute(f"SELECT id, e FROM {table} WHERE id IN ({ids})")
                vecs = {r[0]: [float(v) for v in json.loads(r[1])] for r in cur.fetchall()}
                exact_d = max(l2(PROBE, vecs[i]) for i in exact_ids)
                cur.execute("SET vector.ann_enabled = on")
                print(f"\n== {table}: exact top-10 worst d={exact_d:.4f}", flush=True)
                for ef in (100, 400, 1600, 6400):
                    cur.execute(f"SET vector.ef_search = {ef}")
                    t0 = time.perf_counter()
                    cur.execute(query)
                    ann_ids = [r[0] for r in cur.fetchall()]
                    ms = (time.perf_counter() - t0) * 1000
                    overlap = len(set(ann_ids) & set(exact_ids))
                    cur.execute(f"SELECT e FROM {table} WHERE id = {ann_ids[0]}")
                    row = cur.fetchone()
                    first_d = l2(PROBE, [float(v) for v in json.loads(row[0])]) if row else -1
                    print(
                        f"ef={ef:5d}: overlap={overlap}/10 best_d={first_d:.4f} ({ms:.0f}ms) ids={ann_ids}",
                        flush=True,
                    )
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
