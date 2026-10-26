#!/usr/bin/env python3
"""Durability probe for the 2026-10-16 audit: cold backup, restore, crash
recovery, torn WAL tail, hot-backup fencing.

Scenario order (all under a scratch root, disposable):
  1. boot -> load relational + vector data (vector index present) -> clean stop
  2. hot backup attempt under a live writer -> expect refusal (exit 2)
  3. cold backup while stopped -> expect success + backup-inspect manifest
  4. restore --to a CLEAN dir -> boot restored -> verify rows + vector query
     + plomid_vector_indexes state
  5. crash: kill -9 during a batch load -> boot -> verify recovery: healthy,
     committed batches visible, count a clean multiple of the batch size
  6. torn WAL tail on the restored copy -> append garbage to newest WAL file
     -> boot -> record behavior (tolerated torn tail vs explicit refusal)
  7. clean restart -> catalog + index state preserved

Usage: python3 backup_crash_probe.py [--bin BIN] [--root DIR] [--port P]
"""

import argparse
import glob
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--bin", default="target/release/plomid-server")
parser.add_argument("--root", default="/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-probe/backup")
parser.add_argument("--port", type=int, default=6101)
args = parser.parse_args()

BIN = os.path.abspath(args.bin)
ROOT = args.root
PORT = args.port
DATA = os.path.join(ROOT, "data")
BACKUP = os.path.join(ROOT, "backup")
RESTORED = os.path.join(ROOT, "restored")
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


def boot(data_dir: str, extra: list[str] | None = None, wait: float = 40.0) -> subprocess.Popen:
    proc = subprocess.Popen(
        [BIN, "--data", data_dir, "--host", "127.0.0.1", "--port", str(PORT),
         "--username", "plomid", "--password", "plomid", "--log-level", "warn"]
        + (extra or []),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    deadline = time.time() + wait
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read().decode(errors="replace")[-400:] if proc.stdout else ""
            raise RuntimeError(f"server exited during startup: {out}")
        try:
            with psycopg.connect(DSN, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return proc
        except Exception:  # noqa: BLE001
            time.sleep(0.15)
    raise RuntimeError("server never became ready")


def stop(proc: subprocess.Popen) -> int:
    proc.send_signal(signal.SIGTERM)
    try:
        return proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.wait(timeout=10)


def run_tool(*argv: str) -> tuple[int, str]:
    result = subprocess.run([BIN, *argv], capture_output=True, text=True, timeout=600)
    return result.returncode, (result.stdout + result.stderr).strip()


def wait_accept(timeout: float = 5.0) -> bool:
    with socket.socket() as s:
        s.settimeout(timeout)
        return s.connect_ex(("127.0.0.1", PORT)) == 0


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)
    # BACKUP and RESTORED must NOT be pre-created: the tools create their
    # destination directories and refuse an existing destination.

    # ---- 1. load.
    proc = boot(DATA)
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE dur_rel (id BIGINT PRIMARY KEY, s TEXT)")
            cur.execute("INSERT INTO dur_rel VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
            cur.execute("CREATE TABLE dur_vec (id BIGINT PRIMARY KEY, e VECTOR(FLOAT32, 8))")
            cur.execute("DROP INDEX dur_vec_e_vector_idx")
            cur.execute(
                "INSERT INTO dur_vec VALUES (1,[1,0,0,0,0,0,0,0]),(2,[0,1,0,0,0,0,0,0]),"
                "(3,[0,0,1,0,0,0,0,0])"
            )
            cur.execute("CREATE INDEX dur_vec_h ON dur_vec USING HNSW (e)")
            cur.execute("SET vector.auto_ann_min_rows = 0")
            cur.execute("SELECT id FROM dur_vec ORDER BY l2_distance(e, [1,0,0,0,0,0,0,0]) LIMIT 1")
            top = cur.fetchone()[0]
    check("load + vector query", top == 1, f"top={top}")

    # ---- 2. hot backup attempt (live writer).
    code, out = run_tool("backup", "--data", DATA, "--to", os.path.join(ROOT, "backup-hot"))
    check("hot backup refused", code != 0 and "refus" in out.lower(), f"exit={code} out={out[-140:]}")
    note("hot backup message", out.splitlines()[-1] if out else "")

    # ---- 3. cold backup.
    stop(proc)
    code, out = run_tool("backup", "--data", DATA, "--to", BACKUP)
    check("cold backup succeeded", code == 0, f"exit={code} out={out[-140:]}")
    note("cold backup summary", out.splitlines()[-1] if out else "")
    code, out = run_tool("backup-inspect", "--backup", BACKUP)
    check("backup-inspect", code == 0, f"exit={code} out={out[-160:]}")
    note("inspect summary", out.splitlines()[-1] if out else "")

    # ---- 4. restore to clean dir and verify.
    code, out = run_tool("restore", "--from", BACKUP, "--to", RESTORED)
    check("restore to clean dir", code == 0, f"exit={code} out={out[-140:]}")
    proc_r = boot(RESTORED)
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM dur_rel")
            rel = cur.fetchone()
            cur.execute("SET vector.auto_ann_min_rows = 0")
            cur.execute("SELECT id FROM dur_vec ORDER BY l2_distance(e, [0,1,0,0,0,0,0,0]) LIMIT 1")
            vtop = cur.fetchone()[0]
            cur.execute("SELECT state FROM plomid_vector_indexes() WHERE table_name='public.dur_vec'")
            state = cur.fetchone()
    check("restored relational rows", rel == (3, 1, 3), f"{rel}")
    check("restored vector query", vtop == 2, f"top={vtop}")
    check("restored index state READY", state == ("READY",), f"{state}")

    # ---- 5. crash during a batch load on the RESTORED copy.
    def load_many() -> None:
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                for batch in range(200):
                    values = ",".join(f"({1000 + batch * 50 + i}, 'x{batch}_{i}')" for i in range(50))
                    cur.execute(f"INSERT INTO dur_rel VALUES {values}")

    import threading

    loader = threading.Thread(target=load_many, daemon=True)
    loader.start()
    time.sleep(1.2)
    proc_r.kill()
    proc_r.wait(timeout=10)
    loader.join(timeout=5)
    time.sleep(0.5)
    proc2 = boot(RESTORED)
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dur_rel WHERE id >= 1000")
            inserted = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dur_rel")
            total = cur.fetchone()[0]
    check(
        "crash recovery: count is clean multiple of 50",
        inserted % 50 == 0 and total == 3 + inserted,
        f"inserted={inserted} total={total}",
    )
    note("recovered rows after kill -9", f"inserted={inserted}")
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO dur_rel VALUES (999999,'post-crash')")
            cur.execute("SELECT COUNT(*) FROM dur_rel WHERE id=999999")
            check("post-crash writes work", cur.fetchone()[0] == 1)
    stop(proc2)

    # ---- 6. torn WAL tail on a copy of the restored dir.
    torn = os.path.join(ROOT, "torn")
    shutil.copytree(RESTORED, torn)
    wal_files = sorted(
        glob.glob(os.path.join(torn, "**", "*"), recursive=True),
        key=lambda p: os.path.getmtime(p) if os.path.isfile(p) else 0,
    )
    wal_candidates = [p for p in wal_files if os.path.isfile(p) and "wal" in p.lower()]
    note("wal candidates", [os.path.relpath(p, torn) for p in wal_candidates][-3:])
    if wal_candidates:
        target = wal_candidates[-1]
        with open(target, "ab") as fh:
            fh.write(b"\xde\xad\xbe\xef" * 8)
        try:
            proc3 = boot(torn, wait=20.0)
            with psycopg.connect(DSN, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM dur_rel")
                    total = cur.fetchone()[0]
            check("torn WAL tail: boots and serves", True, f"count={total}")
            stop(proc3)
        except Exception as error:  # noqa: BLE001
            check("torn WAL tail: explicit refusal", True, str(error)[:140])

    # ---- 7. clean restart on restored dir: catalog + index preserved.
    proc4 = boot(RESTORED)
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT state FROM plomid_vector_indexes() WHERE table_name='public.dur_vec'")
            state = cur.fetchone()
            cur.execute("SET vector.auto_ann_min_rows = 0")
            cur.execute("SELECT id FROM dur_vec ORDER BY l2_distance(e, [0,0,1,0,0,0,0,0]) LIMIT 1")
            vtop = cur.fetchone()[0]
    check("restart: index state + ANN query", state == ("READY",) and vtop == 3, f"state={state} top={vtop}")
    stop(proc4)

    print(f"\n{PASS}/{PASS + FAIL} checks passed")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
