#!/usr/bin/env python3
"""Investigate HNSW recall@10 on 50k x dim-64 fixtures (scale-probe follow-up).

Rebuilds the scale probe's phase-B fixture (sin-structured vectors) and a
random-uniform variant, then compares ANN vs exact top-10 with actual L2
distances and cache statistics to classify the recall=0.00 observation.

Usage: python3 hnsw_dim64_investigate.py [--port P]
"""

import argparse
import json
import math
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, default=6175)
args = parser.parse_args()

BIN = os.path.abspath("target/release/plomid-server")
ROOT = f"/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-hnsw64-{args.port}"
DATA = os.path.join(ROOT, "data")
PORT = args.port
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
DIM = 64
N = 50000
PROBE = [0.5] * DIM


def l2(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def boot():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA)
    proc = subprocess.Popen(
        [BIN, "--data", DATA, "--host", "127.0.0.1", "--port", str(PORT),
         "--username", "plomid", "--password", "plomid", "--log-level", "warn"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    deadline = time.time() + 40
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError("server exited during startup")
        try:
            with psycopg.connect(DSN, connect_timeout=2):
                return proc
        except Exception:  # noqa: BLE001
            time.sleep(0.15)
    raise RuntimeError("server never became ready")


def stats(cur):
    cur.execute("SELECT * FROM plomid_vector_stats()")
    cols = [d.name for d in cur.description]
    return dict(zip(cols, cur.fetchone()))


def parse_vec(text):
    return [float(v) for v in json.loads(text)]


def run_case(cur, table, generator, label):
    print(f"\n===== {label} =====", flush=True)
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, {DIM}))")
    cur.execute(f"DROP INDEX {table}_e_vector_idx")
    t0 = time.perf_counter()
    for batch in range(N // 1000):
        values = ",".join(generator(batch * 1000 + i) for i in range(1000))
        cur.execute(f"INSERT INTO {table} VALUES {values}")
    print(f"insert: {time.perf_counter()-t0:.1f}s", flush=True)
    cur.execute(f"CREATE INDEX {table}_h ON {table} USING HNSW (e)")
    cur.execute("SET vector.auto_ann_min_rows = 0")
    probe_sql = "[" + ",".join(str(v) for v in PROBE) + "]"
    query = f"SELECT id FROM {table} ORDER BY l2_distance(e, {probe_sql}) LIMIT 10"

    cur.execute("EXPLAIN " + query)
    print("explain:", " | ".join(r[0] for r in cur.fetchall())[-150:], flush=True)

    s0 = stats(cur)
    t0 = time.perf_counter()
    cur.execute(query)
    ann_ids = [r[0] for r in cur.fetchall()]
    ann_ms = (time.perf_counter() - t0) * 1000
    s1 = stats(cur)

    cur.execute("SET vector.ann_enabled = off")
    cur.execute("EXPLAIN " + query)
    print("exact explain:", " | ".join(r[0] for r in cur.fetchall())[-150:], flush=True)
    t0 = time.perf_counter()
    cur.execute(query)
    exact_ids = [r[0] for r in cur.fetchall()]
    exact_ms = (time.perf_counter() - t0) * 1000
    cur.execute("SET vector.ann_enabled = on")

    print(f"ann  ids  ({ann_ms:.1f} ms): {ann_ids}")
    print(f"exact ids ({exact_ms:.1f} ms): {exact_ids}")
    print(f"overlap: {len(set(ann_ids) & set(exact_ids))}/10")
    print(
        "stats delta ann query: "
        f"miss={s1.get('ann_cache_misses_total',0)-s0.get('ann_cache_misses_total',0)} "
        f"builds={s1.get('index_builds_total',0)-s0.get('index_builds_total',0)} "
        f"hits={s1.get('ann_cache_hits_total',0)-s0.get('ann_cache_hits_total',0)}"
    )

    ids = sorted(set(ann_ids) | set(exact_ids))
    in_list = ",".join(str(i) for i in ids)
    cur.execute(f"SELECT id, e FROM {table} WHERE id IN ({in_list})")
    vecs = {rid: parse_vec(text) for rid, text in cur.fetchall()}
    print("distances (id: ann/exact):")
    for i in exact_ids:
        print(f"  exact {i}: {l2(PROBE, vecs[i]):.6f}")
    only_ann = [i for i in ann_ids if i not in set(exact_ids)]
    for i in only_ann:
        print(f"  ann-only {i}: {l2(PROBE, vecs[i]):.6f}")


def main():
    proc = boot()
    try:
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                def sine_row(i):
                    lane = [f"{math.sin(i/(k+7.0)):.6f}" for k in range(DIM)]
                    return f"({i}, [{','.join(lane)}])"

                run_case(cur, "sc_sine", sine_row, "sine fixture (scale-probe repro)")

                rng = random.Random(20261016)
                def rand_row(i):
                    lane = [f"{rng.uniform(-1, 1):.6f}" for _ in range(DIM)]
                    return f"({i}, [{','.join(lane)}])"

                run_case(cur, "sc_rand", rand_row, "random-uniform fixture (independent)")
        return 0
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
