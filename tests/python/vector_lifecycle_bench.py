#!/usr/bin/env python3
"""PLOMID vector lifecycle bench: cold start, warm cache, cache-miss rebuild,
and restart recovery for HNSW and DiskANN at increasing index sizes.

The script owns the server process: it boots ``plomid-server`` against a
scratch data directory, runs the measurement matrix, restarts the server
between phases, and writes JSON + a human-readable table into the output
directory. Raw server logs are kept per boot so recovery classification
lines (``diskann checkpoint``) stay attributable.

What is measured (per method x size):
  ddl_ms            CREATE INDEX wall time (catalog only; no graph build)
  cold_ms           first eligible query (pays the ANN build)
  warm_p50_ms       median of QUERIES warmed hits (cache hit)
  rebuild_ms        first query after REINDEX (cache invalidated, same boot)
  restart_accept_ms server boot -> TCP accept
  restart_ready_ms  server boot -> successful SELECT 1
  restart_cold_ms   first query after restart (recovery + rebuild)
  restart_warm_p50  median after restart
  recall@k          ANN vs exact (ann_enabled=off) ordered id overlap
  state             plomid_vector_indexes() state after restart

Counter deltas from plomid_vector_stats() prove which path ran (miss/build
vs hit) instead of trusting latency alone. The derived cache directory
listing shows whether DiskANN segments were persisted; the server log shows
its ``recover()`` classification. Both are reported even where the executor
still rebuilds from authoritative rows — the report states what happened.

Usage:
    python3 tests/python/vector_lifecycle_bench.py

Environment:
    PLOMID_BIN    server binary     (default target/release/plomid-server)
    PLOMID_DIR    scratch root      (default target/lifecycle-bench)
    PLOMID_PORT   port              (default 6100)
    SIZES         comma-separated   (default 2000,8000,32000,128000)
    DIM           vector dimension  (default 128)
    METHODS       comma-separated   (default hnsw,diskann)
    QUERIES       warm query count  (default 25)
    K             top-k             (default 10)
    OUT           output dir        (default $PLOMID_DIR/results)

    NEVER point this at a production database.
"""

import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time

import psycopg

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BIN = os.environ.get("PLOMID_BIN", os.path.join(ROOT, "target", "release", "plomid-server"))
SCRATCH = os.environ.get("PLOMID_DIR", os.path.join(ROOT, "target", "lifecycle-bench"))
PORT = int(os.environ.get("PLOMID_PORT", "6100"))
SIZES = [int(s) for s in os.environ.get("SIZES", "2000,8000,32000,128000").split(",")]
DIM = int(os.environ.get("DIM", "128"))
METHODS = os.environ.get("METHODS", "hnsw,diskann").split(",")
QUERIES = int(os.environ.get("QUERIES", "25"))
K = int(os.environ.get("K", "10"))
OUT = os.environ.get("OUT", os.path.join(SCRATCH, "results"))
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
DATA = os.path.join(SCRATCH, "data")

RESULTS = []
BOOT_LOGS = []


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------- server

def wipe_data() -> None:
    subprocess.run(["rm", "-rf", DATA], check=True)
    os.makedirs(DATA, exist_ok=True)


def spawn_server(boot: int) -> tuple[subprocess.Popen, float, float]:
    """Starts the server; returns (proc, accept_ms, ready_ms)."""
    log_path = os.path.join(OUT, f"server_{boot:02d}.log")
    BOOT_LOGS.append(log_path)
    handle = open(log_path, "wb")
    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [
            BIN,
            "--data", DATA,
            "--host", "127.0.0.1",
            "--port", str(PORT),
            "--username", "plomid",
            "--password", "plomid",
            "--log-level", "debug",
        ],
        stdout=handle,
        stderr=subprocess.STDOUT,
    )
    accept_ms = None
    ready_ms = None
    deadline = t0 + 120.0
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited during startup; see {log_path}")
        if accept_ms is None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.25)
                if s.connect_ex(("127.0.0.1", PORT)) == 0:
                    accept_ms = (time.perf_counter() - t0) * 1000.0
        if ready_ms is None:
            try:
                with psycopg.connect(DSN, connect_timeout=2) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                ready_ms = (time.perf_counter() - t0) * 1000.0
                break
            except Exception:  # noqa: BLE001 - startup is a retry loop
                pass
        time.sleep(0.02)
    if ready_ms is None:
        raise RuntimeError(f"server never became ready; see {log_path}")
    return proc, accept_ms, ready_ms


def stop_server(proc: subprocess.Popen) -> float:
    """Graceful SIGTERM; returns shutdown wall time in ms."""
    t0 = time.perf_counter()
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
    return (time.perf_counter() - t0) * 1000.0


def connect():
    conn = psycopg.connect(DSN, autocommit=True)
    with conn.cursor() as cur:
        cur.execute("SET vector.auto_ann_min_rows = 0")
    return conn


# ---------------------------------------------------------------- fixtures

class Rng:
    """Deterministic LCG (same family as the Rust benches, Numpy-free)."""

    def __init__(self, seed: int):
        self.state = seed & 0xFFFF_FFFF_FFFF_FFFF

    def next(self) -> float:
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) & 0xFFFF_FFFF_FFFF_FFFF
        return (self.state >> 33) / 4294967295.0 * 2.0 - 1.0


def probe_vector() -> list[float]:
    rng = Rng(0xBEEF)
    return [round(rng.next(), 6) for _ in range(DIM)]


def insert_rows(cur, table: str, count: int) -> None:
    """Cluster-structured deterministic rows: 8 centers + noise."""
    rng = Rng(0x51D ^ count)
    centers = [[round(rng.next(), 4) for _ in range(DIM)] for _ in range(8)]
    batch = []
    for i in range(count):
        c = centers[i % 8]
        lanes = [round(c[j] + rng.next() * 0.05, 6) for j in range(DIM)]
        batch.append(f"({i}, [{','.join(map(str, lanes))}])")
        if len(batch) == 1000:
            cur.execute(f"INSERT INTO {table} VALUES {','.join(batch)}")
            batch = []
    if batch:
        cur.execute(f"INSERT INTO {table} VALUES {','.join(batch)}")


def vector_stats(cur) -> dict:
    cur.execute("SELECT * FROM plomid_vector_stats()")
    row = cur.fetchone()
    cols = [d.name for d in cur.description]
    return dict(zip(cols, row))


def delta(before: dict, after: dict, key: str) -> int:
    return after[key] - before[key]


def timed_query(cur, sql: str) -> tuple[float, list]:
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return (time.perf_counter() - t0) * 1000.0, rows


def explain_vector_line(cur, sql: str) -> str:
    cur.execute("EXPLAIN " + sql)
    text = "\n".join(r[0] for r in cur.fetchall())
    for line in text.splitlines():
        if "Vector:" in line:
            return line.strip()
    return text.splitlines()[-1].strip() if text else ""


def derived_dir_listing() -> dict:
    root = os.path.join(DATA, "vector_ann")
    out = {}
    for base, _dirs, files in os.walk(root):
        for f in files:
            p = os.path.join(base, f)
            out[os.path.relpath(p, DATA)] = os.path.getsize(p)
    return out


def log_classification(log_path: str) -> list[str]:
    lines = []
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "diskann" in line.lower() and (
                    "checkpoint" in line.lower() or "recover" in line.lower()
                ):
                    lines.append(line.strip())
    except FileNotFoundError:
        pass
    return lines


# ---------------------------------------------------------------- phases

def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    log(f"vector lifecycle bench: bin={BIN}")
    log(f"  sizes={SIZES} dim={DIM} methods={METHODS} queries={QUERIES} out={OUT}")
    if not os.path.exists(BIN):
        log(f"missing server binary {BIN}; build with `cargo build --release`")
        return 2
    wipe_data()
    proc, accept_ms, ready_ms = spawn_server(0)
    log(f"  boot 0: accept={accept_ms:.0f}ms ready={ready_ms:.0f}ms")
    conn = connect()

    # Keep one table per (method, size) so the restart phase can re-query it
    # without rebuilding the fixture insert.
    for method in METHODS:
        for size in SIZES:
            table = f"lc_t_{method}_{size}"
            log(f"--- {method} n={size} ---")
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, {DIM}))"
                )
                # Drop the newborn column's auto HNSW index *before* the bulk
                # load. Incremental inserts into a vector index currently fail
                # past ~2–4k rows once keys are large (dim >= ~64): the
                # storage B+Tree's internal split is count-based and can leave
                # an oversized half when large separators mix with small ones
                # (reproduced and root-caused in the 2026-10-16 audit finding).
                # This bench measures the explicit method indexes it creates
                # below, so the load runs index-free and each measured index
                # is built once from the loaded rows.
                cur.execute(f"DROP INDEX IF EXISTS {table}_e_vector_idx")
                t0 = time.perf_counter()
                insert_rows(cur, table, size)
                insert_ms = (time.perf_counter() - t0) * 1000.0

                index_name = f"lc_{method}_{size}"
                t0 = time.perf_counter()
                cur.execute(f"CREATE INDEX {index_name} ON {table} USING {method} (e)")
                ddl_ms = (time.perf_counter() - t0) * 1000.0

                probe = probe_vector()
                literal = "[" + ",".join(map(str, probe)) + "]"
                query = (
                    f"SELECT id FROM {table} ORDER BY l2_distance(e, {literal}) LIMIT {K}"
                )
                explain_line = explain_vector_line(cur, query)

                s0 = vector_stats(cur)
                cold_ms, ann_rows = timed_query(cur, query)
                s1 = vector_stats(cur)
                warm = [timed_query(cur, query)[0] for _ in range(QUERIES)]
                s2 = vector_stats(cur)

                cur.execute("REINDEX")
                s3 = vector_stats(cur)
                rebuild_ms, _ = timed_query(cur, query)
                s4 = vector_stats(cur)
                warm2 = [timed_query(cur, query)[0] for _ in range(5)]
                s5 = vector_stats(cur)

                ann_ids = [r[0] for r in ann_rows]
                cur.execute("SET vector.ann_enabled = off")
                _, exact_rows = timed_query(cur, query)
                cur.execute("SET vector.ann_enabled = on")
                exact_ids = [r[0] for r in exact_rows]
                recall = len(set(ann_ids) & set(exact_ids)) / max(1, len(exact_ids))

                cell = {
                    "method": method,
                    "size": size,
                    "dim": DIM,
                    "insert_ms": insert_ms,
                    "ddl_ms": ddl_ms,
                    "cold_ms": cold_ms,
                    "cold_miss": delta(s0, s1, "ann_cache_misses_total"),
                    "cold_builds": delta(s0, s1, "index_builds_total"),
                    "cold_rebuild_us": delta(s0, s1, "rebuild_us_total"),
                    "warm_p50_ms": statistics.median(warm),
                    "warm_max_ms": max(warm),
                    "warm_hits": delta(s1, s2, "ann_cache_hits_total"),
                    "warm_fallbacks": delta(s1, s2, "ann_fallbacks_total"),
                    "rebuild_ms": rebuild_ms,
                    "rebuild_miss": delta(s3, s4, "ann_cache_misses_total"),
                    "rebuild_hits": delta(s4, s5, "ann_cache_hits_total"),
                    "recall_at_k": recall,
                    "explain": explain_line,
                    "cache_files_before_restart": derived_dir_listing(),
                }
                RESULTS.append(cell)
                log(
                    f"  ddl={ddl_ms:.0f}ms cold={cold_ms:.0f}ms "
                    f"warm_p50={cell['warm_p50_ms']:.1f}ms rebuild={rebuild_ms:.0f}ms "
                    f"recall@{K}={recall:.2f} miss={cell['cold_miss']} "
                    f"builds={cell['cold_builds']} hits={cell['warm_hits']}"
                )
                log(f"  explain: {explain_line}")
        # end sizes

    # ---- restart phase: one boot for all cells, queried in order.
    conn.close()
    shutdown_ms = stop_server(proc)
    log(f"shutdown_ms={shutdown_ms:.0f}")
    restart_log_index = len(BOOT_LOGS)
    proc, accept_ms, ready_ms = spawn_server(restart_log_index)
    log(f"  boot {restart_log_index}: accept={accept_ms:.0f}ms ready={ready_ms:.0f}ms")
    conn = connect()
    for method in METHODS:
        for size in SIZES:
            table = f"lc_t_{method}_{size}"
            probe = probe_vector()
            literal = "[" + ",".join(map(str, probe)) + "]"
            query = f"SELECT id FROM {table} ORDER BY l2_distance(e, {literal}) LIMIT {K}"
            with conn.cursor() as cur:
                s0 = vector_stats(cur)
                cold_ms, _ = timed_query(cur, query)
                s1 = vector_stats(cur)
                warm = [timed_query(cur, query)[0] for _ in range(QUERIES)]
                s2 = vector_stats(cur)
                cur.execute(
                    "SELECT state FROM plomid_vector_indexes() "
                    f"WHERE table_name = 'public.{table}'"
                )
                state_rows = cur.fetchall()
                state = state_rows[0][0] if state_rows else None
                cache_files = derived_dir_listing()
            cell = {
                "method": method,
                "size": size,
                "dim": DIM,
                "restart_accept_ms": accept_ms,
                "restart_ready_ms": ready_ms,
                "restart_shutdown_ms": shutdown_ms,
                "restart_cold_ms": cold_ms,
                "restart_cold_miss": delta(s0, s1, "ann_cache_misses_total"),
                "restart_cold_builds": delta(s0, s1, "index_builds_total"),
                "restart_cold_rebuild_us": delta(s0, s1, "rebuild_us_total"),
                "restart_warm_p50_ms": statistics.median(warm),
                "restart_warm_hits": delta(s1, s2, "ann_cache_hits_total"),
                "state_after_restart": state,
                "cache_files_after_restart": cache_files,
            }
            RESULTS.append(cell)
            log(
                f"  restart {method} n={size}: state={state} "
                f"cold={cold_ms:.0f}ms warm_p50={cell['restart_warm_p50_ms']:.1f}ms "
                f"miss={cell['restart_cold_miss']} builds={cell['restart_cold_builds']}"
            )
    conn.close()
    stop_server(proc)

    # ---- recovery classification from every boot log.
    classifications = {
        os.path.basename(p): log_classification(p) for p in BOOT_LOGS
    }

    report = {
        "dsn": DSN,
        "bin": BIN,
        "sizes": SIZES,
        "dim": DIM,
        "methods": METHODS,
        "queries": QUERIES,
        "k": K,
        "machine": os.uname().machine,
        "results": RESULTS,
        "diskann_recovery_lines": classifications,
    }
    with open(os.path.join(OUT, "lifecycle.json"), "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)

    # Human table.
    lines = []
    lines.append("| method | n | ddl_ms | cold_ms | warm_p50_ms | rebuild_ms | restart_cold_ms | restart_warm_p50 | recall@k | state |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    by_key = {(r["method"], r["size"]): {} for r in RESULTS}
    for r in RESULTS:
        by_key[(r["method"], r["size"])].update(r)
    for method in METHODS:
        for size in SIZES:
            r = by_key[(method, size)]
            lines.append(
                f"| {method} | {size} | {r.get('ddl_ms', 0):.0f} | {r.get('cold_ms', 0):.0f} | "
                f"{r.get('warm_p50_ms', 0):.1f} | {r.get('rebuild_ms', 0):.0f} | "
                f"{r.get('restart_cold_ms', 0):.0f} | {r.get('restart_warm_p50_ms', 0):.1f} | "
                f"{r.get('recall_at_k', 0):.2f} | {r.get('state_after_restart', '-')} |"
            )
    table = "\n".join(lines)
    with open(os.path.join(OUT, "lifecycle.md"), "w", encoding="utf-8") as fh:
        fh.write(table + "\n")
    print(table)
    log(f"wrote {OUT}/lifecycle.json and lifecycle.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
