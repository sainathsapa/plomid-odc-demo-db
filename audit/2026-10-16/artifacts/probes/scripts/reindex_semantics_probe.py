#!/usr/bin/env python3
"""Decisive REINDEX invalidation + ef-at-build test on a small table.

Boots with info logs, builds a 5k-row HNSW table, then walks:
  q1 (build) -> q2 (hit) -> REINDEX -> q3 -> SET ef=32 -> REINDEX -> q4
and records stats deltas, fresh/stale counters, and result stability.
"""

import json
import math
import os
import signal
import subprocess
import sys
import time

import psycopg

BIN = os.path.abspath("target/release/plomid-server")
ROOT = "/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-reindex-test"
DATA = os.path.join(ROOT, "data")
PORT = 6178
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
LOG = os.path.join(ROOT, "server.log")
probe_sql = "[" + ",".join(["0.5"] * 16) + "]"

os.makedirs(ROOT, exist_ok=True)


def stats(cur):
    cur.execute("SELECT * FROM plomid_vector_stats()")
    cols = [d.name for d in cur.description]
    return dict(zip(cols, cur.fetchone()))


def delta(a, b, keys):
    return " ".join(f"{k.split('_')[0]}={b.get(k,0)-a.get(k,0)}" for k in keys)


KEYS = ("ann_cache_misses_total", "index_builds_total", "ann_cache_hits_total", "fresh_total", "stale_total")

proc = subprocess.Popen(
    [BIN, "--data", DATA, "--host", "127.0.0.1", "--port", str(PORT),
     "--username", "plomid", "--password", "plomid", "--log-level", "info"],
    stdout=open(LOG, "wb"), stderr=subprocess.STDOUT,
)
deadline = time.time() + 40
while time.time() < deadline:
    try:
        psycopg.connect(DSN, connect_timeout=2).close()
        break
    except Exception:  # noqa: BLE001
        time.sleep(0.15)

try:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE rs (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 16))")
            cur.execute("DROP INDEX rs_e_vector_idx")
            rows = []
            for i in range(5000):
                lane = [f"{math.sin(i/(k+3.0)):.6f}" for k in range(16)]
                rows.append(f"({i}, [{','.join(lane)}])")
                if len(rows) == 1000:
                    cur.execute("INSERT INTO rs VALUES " + ",".join(rows))
                    rows = []
            cur.execute("CREATE INDEX rs_h ON rs USING HNSW (e)")
            cur.execute("SET vector.auto_ann_min_rows = 0")
            query = f"SELECT id FROM rs ORDER BY l2_distance(e, {probe_sql}) LIMIT 10"

            def run(label):
                s0 = stats(cur)
                t0 = time.perf_counter()
                cur.execute(query)
                ids = [r[0] for r in cur.fetchall()]
                ms = (time.perf_counter() - t0) * 1000
                s1 = stats(cur)
                print(f"{label:28s} {ms:8.1f}ms [{delta(s0, s1, KEYS)}]", flush=True)
                print(f"    ids={ids}", flush=True)
                return ids

            run("q1")
            run("q2")
            cur.execute("REINDEX")
            run("q3 after REINDEX")
            cur.execute("SET vector.ef_search = 32")
            cur.execute("REINDEX")
            run("q4 after ef=32 + REINDEX")
            run("q5")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)

print("\n--- server log reindex lines ---")
with open(LOG, "rb") as fh:
    for line in fh.read().decode(errors="replace").splitlines():
        if "reindex" in line.lower():
            print(line[:200])
