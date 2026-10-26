#!/usr/bin/env python3
"""REINDEX invalidation -> rebuild cycle + ef_search-at-build experiment.

Answers: (1) does REINDEX really drop the in-memory ANN cache (rebuild cost
reappears)?  (2) does vector.ef_search apply at rebuild time (results change)?
(3) does a larger ef rescue the sine-fixture recall cliff?

Usage: python3 hnsw_reindex_ef_probe.py
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
DATA = "/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-hnsw64-6175/data"
PORT = 6177
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
PROBE = [0.5] * 64
probe_sql = "[" + ",".join(str(v) for v in PROBE) + "]"


def l2(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def stats(cur):
    cur.execute("SELECT * FROM plomid_vector_stats()")
    cols = [d.name for d in cur.description]
    return dict(zip(cols, cur.fetchone()))


def delta(a, b):
    return (
        f"miss={b.get('ann_cache_misses_total',0)-a.get('ann_cache_misses_total',0)} "
        f"builds={b.get('index_builds_total',0)-a.get('index_builds_total',0)} "
        f"hits={b.get('ann_cache_hits_total',0)-a.get('ann_cache_hits_total',0)}"
    )


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


def timed_query(cur, table, label):
    query = f"SELECT id FROM {table} ORDER BY l2_distance(e, {probe_sql}) LIMIT 10"
    s0 = stats(cur)
    t0 = time.perf_counter()
    cur.execute(query)
    ids = [r[0] for r in cur.fetchall()]
    ms = (time.perf_counter() - t0) * 1000
    s1 = stats(cur)
    print(f"{label}: {ms:8.0f}ms [{delta(s0, s1)}] ids={ids}", flush=True)
    return ids


try:
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET vector.auto_ann_min_rows = 0")
            for table, ef in (("sc_rand", 100), ("sc_rand", 400), ("sc_sine", 100), ("sc_sine", 400)):
                print(f"\n===== {table} @ ef={ef} =====", flush=True)
                cur.execute("SET vector.ann_enabled = off")
                cur.execute(f"SELECT id FROM {table} ORDER BY l2_distance(e, {probe_sql}) LIMIT 10")
                exact_ids = [r[0] for r in cur.fetchall()]
                cur.execute("SET vector.ann_enabled = on")
                cur.execute(f"SET vector.ef_search = {ef}")
                # First query of this table in this process: builds at current ef.
                q1 = timed_query(cur, table, f"first query (ef={ef} at build)")
                print(f"  overlap vs exact: {len(set(q1) & set(exact_ids))}/10", flush=True)
                timed_query(cur, table, "warm query")
                cur.execute("REINDEX")
                q2 = timed_query(cur, table, "after REINDEX")
                print(f"  overlap vs exact: {len(set(q2) & set(exact_ids))}/10", flush=True)
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
