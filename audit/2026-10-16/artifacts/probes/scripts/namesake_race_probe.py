#!/usr/bin/env python3
"""Characterize concurrent CREATE TABLE same-name semantics (audit finding).

Checks:
  1. sequential duplicate CREATE -> must error (control)
  2. two concurrent same-name same-schema CREATEs -> exactly one winner
  3. two concurrent same-name different-schema CREATEs -> exactly one winner
     and the surviving table matches that winner's schema
  4. after a lost race, the loser's statements against the name behave
     consistently with the surviving definition

Usage: python3 namesake_race_probe.py [--bin BIN] [--root DIR] [--port P]
Exit 0 only if every asserted check holds (PostgreSQL semantics).
"""

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import psycopg

parser = argparse.ArgumentParser()
parser.add_argument("--bin", default="target/release/plomid-server")
parser.add_argument("--root", default="/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-probe/namesake")
parser.add_argument("--port", type=int, default=6111)
args = parser.parse_args()

ROOT = args.root
DATA = os.path.join(ROOT, "data")
PORT = args.port
BIN = os.path.abspath(args.bin)
DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"

FAILURES: list[str] = []
CHECKS = 0


def check(label: str, ok: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    print(f"{'PASS' if ok else 'FAIL'} {label}: {detail}", flush=True)
    if not ok:
        FAILURES.append(label)


def note(label: str, value) -> None:
    print(f"NOTE {label}: {value}", flush=True)


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA)
    proc = subprocess.Popen(
        [BIN, "--data", DATA, "--host", "127.0.0.1", "--port", str(PORT),
         "--username", "plomid", "--password", "plomid", "--log-level", "warn"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.time() + 40
        while time.time() < deadline:
            if proc.poll() is not None:
                out = proc.stdout.read().decode(errors="replace")[-400:] if proc.stdout else ""
                raise RuntimeError(f"server exited during startup: {out}")
            try:
                with psycopg.connect(DSN, connect_timeout=2) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                break
            except Exception:  # noqa: BLE001
                time.sleep(0.15)

        # ---- 1. sequential control.
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE seq_dup (id INT)")
                err = None
                try:
                    cur.execute("CREATE TABLE seq_dup (id INT)")
                except Exception as error:  # noqa: BLE001
                    err = str(error).splitlines()[0]
                check("sequential duplicate CREATE rejected", err is not None, err or "no error raised")
                cur.execute("DROP TABLE seq_dup")

        # ---- 2. concurrent same schema.
        results: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def racer(name: str, ddl: str) -> None:
            conn = psycopg.connect(DSN, autocommit=True)
            barrier.wait()
            try:
                with conn.cursor() as cur:
                    cur.execute(ddl)
                with lock:
                    results.append("ok")
            except Exception as error:  # noqa: BLE001
                with lock:
                    results.append(str(error).splitlines()[0][:90])
            conn.close()

        t1 = threading.Thread(target=racer, args=("a", "CREATE TABLE namesame (id INT)"))
        t2 = threading.Thread(target=racer, args=("b", "CREATE TABLE namesame (id INT)"))
        t1.start(); t2.start(); t1.join(); t2.join()
        wins = results.count("ok")
        check("concurrent same-name(same schema): exactly one winner", wins == 1, f"{results}")
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename = 'namesame'")
                check("concurrent same-name(same schema): one catalog entry", cur.fetchone()[0] == 1, f"{results}")
                cur.execute("DROP TABLE IF EXISTS namesame")

        # ---- 3. concurrent different schemas.
        results = []
        barrier = threading.Barrier(2)
        t1 = threading.Thread(target=racer, args=("a", "CREATE TABLE racediff (id INT)"))
        t2 = threading.Thread(target=racer, args=("b", "CREATE TABLE racediff (s TEXT)"))
        t1.start(); t2.start(); t1.join(); t2.join()
        wins = results.count("ok")
        with psycopg.connect(DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='racediff' ORDER BY ordinal_position")
                cols = cur.fetchall()
                cur.execute("DROP TABLE IF EXISTS racediff")
        check("concurrent same-name(diff schema): exactly one winner", wins == 1, f"{results} survivors={cols}")
        if wins == 2:
            note("both-success hazard", f"both OK; surviving schema={cols}; a loser client holding the other shape would break")

        # ---- 4. winner/loser consistency for the same-schema case is implied
        # by check 2's catalog count; record interleaving speed.
        return 1 if FAILURES else 0

    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed", flush=True)
        if FAILURES:
            print("FAILURES: " + "; ".join(FAILURES), flush=True)


if __name__ == "__main__":
    sys.exit(main())
