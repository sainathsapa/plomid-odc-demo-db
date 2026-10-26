#!/usr/bin/env python3
"""Security probe for the 2026-10-16 audit: RBAC enforcement, DATABASE DDL
authz re-test (former P0-003), DROP ROLE guards, TLS modes, non-loopback guard.

Usage: python3 rbac_probe.py [--bin BIN] [--root DIR] [--port P]
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
parser.add_argument("--root", default="/private/var/folders/49/bjpspnbx333d6_2g0tyjh7540000gn/T/opencode/audit16-probe/rbac")
parser.add_argument("--port", type=int, default=6102)
args = parser.parse_args()

BIN = os.path.abspath(args.bin)
ROOT = args.root
PORT = args.port
DATA = os.path.join(ROOT, "data")
PLAIN_DSN = f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid"
ROLE_DSN = f"postgresql://auditor:audit-secret-1@127.0.0.1:{PORT}/plomid"
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


def spawn(extra: list[str], data_dir: str = DATA):
    return subprocess.Popen(
        [BIN, "--data", data_dir, "--username", "plomid", "--password", "plomid",
         "--log-level", "warn", *extra],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )


def wait_ready(timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with psycopg.connect(PLAIN_DSN, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except Exception:  # noqa: BLE001
            time.sleep(0.15)
    return False


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()


def err_of(fn):
    try:
        fn()
        return None
    except Exception as error:  # noqa: BLE001
        return str(error).splitlines()[0]


def main() -> int:
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)

    # ---- Non-loopback guard: 0.0.0.0 without TLS must refuse to start.
    guard = spawn(["--host", "0.0.0.0", "--port", str(PORT)])
    try:
        out, _ = guard.communicate(timeout=15)
        text = out.decode(errors="replace")
        check(
            "non-loopback plaintext refused",
            guard.returncode != 0 and ("TLS" in text or "loopback" in text or "remote" in text),
            f"exit={guard.returncode} {text.strip().splitlines()[-1][:120] if text.strip() else ''}",
        )
    except subprocess.TimeoutExpired:
        guard.kill()
        check("non-loopback plaintext refused", False, "server kept running")

    # ---- Plain loopback server for RBAC.
    proc = spawn(["--host", "127.0.0.1", "--port", str(PORT)])
    if not wait_ready():
        print("server never became ready")
        stop(proc)
        return 1
    try:
        with psycopg.connect(PLAIN_DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS sec_t")
                cur.execute("CREATE TABLE sec_t (id INT PRIMARY KEY, s TEXT)")
                cur.execute("INSERT INTO sec_t VALUES (1,'one'),(2,'two')")
                cur.execute("DROP ROLE IF EXISTS auditor")
                cur.execute("CREATE ROLE auditor LOGIN PASSWORD 'audit-secret-1'")
                cur.execute("GRANT SELECT ON sec_t TO auditor")

        with psycopg.connect(ROLE_DSN, autocommit=True) as role_conn:
            with role_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM sec_t")
                allowed = cur.fetchone()[0]
                check("rbac granted SELECT works", allowed == 2, f"count={allowed}")
                denial = err_of(lambda: cur.execute("INSERT INTO sec_t VALUES (3,'three')"))
                check(
                    "rbac INSERT denied",
                    denial is not None and ("42501" in denial or "insufficient" in denial.lower() or "permission" in denial.lower() or "denied" in denial.lower()),
                    denial or "allowed?!",
                )
                denial = err_of(lambda: cur.execute("UPDATE sec_t SET s='x' WHERE id=1"))
                check("rbac UPDATE denied", denial is not None, denial or "allowed?!")
                denial = err_of(lambda: cur.execute("DELETE FROM sec_t"))
                check("rbac DELETE denied", denial is not None, denial or "allowed?!")
                # P0-003 re-test: DATABASE DDL must be denied for a plain role.
                denial = err_of(lambda: cur.execute("CREATE DATABASE evil_db"))
                check("P0-003 CREATE DATABASE denied", denial is not None, denial or "allowed?!")
                denial = err_of(lambda: cur.execute("DROP DATABASE plomid"))
                check("P0-003 DROP DATABASE denied", denial is not None, denial or "allowed?!")
                denial = err_of(lambda: cur.execute("DROP TABLE sec_t"))
                check("rbac DROP TABLE denied", denial is not None, denial or "allowed?!")

        # Revoke takes effect on the next connection (catalog images per connection).
        with psycopg.connect(PLAIN_DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("REVOKE SELECT ON sec_t FROM auditor")
        with psycopg.connect(ROLE_DSN, autocommit=True) as role_conn:
            with role_conn.cursor() as cur:
                denial = err_of(lambda: cur.execute("SELECT COUNT(*) FROM sec_t"))
                check("rbac revoke enforced on next connection", denial is not None, denial or "allowed?!")

        # DROP ROLE guards.
        with psycopg.connect(PLAIN_DSN, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP ROLE IF EXISTS owned_role")
                cur.execute("CREATE ROLE owned_role LOGIN PASSWORD 'owned-1'")
                # Ownership path: only grants are exercised here; ownership
                # transfer language (ALTER TABLE OWNER) may not exist.
                cur.execute("GRANT SELECT ON sec_t TO owned_role")
                denial = err_of(lambda: cur.execute("DROP ROLE owned_role"))
                note("DROP ROLE with granted privilege", denial or "succeeded (grants do not block)")
                cur.execute("REVOKE SELECT ON sec_t FROM owned_role")
                denial = err_of(lambda: cur.execute("DROP ROLE plomid"))
                check("DROP ROLE bootstrap refused", denial is not None, denial or "allowed?!")
                cur.execute("DROP ROLE IF EXISTS owned_role")
                cur.execute("DROP ROLE IF EXISTS auditor")
                cur.execute("DROP TABLE IF EXISTS sec_t")

        # ---- TLS modes.
        cert = os.path.join(ROOT, "cert.pem")
        key = os.path.join(ROOT, "key.pem")
        gen = subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", key, "-out", cert, "-days", "2", "-subj", "/CN=localhost"],
            capture_output=True, text=True,
        )
        check("self-signed cert generated", gen.returncode == 0, gen.stderr[-80:] if gen.returncode else "")
    finally:
        stop(proc)

    # TLS-enabled (opportunistic) then TLS-required.
    tls_proc = spawn(["--host", "127.0.0.1", "--port", str(PORT), "--tls", "--tls-cert", cert, "--tls-key", key])
    if wait_ready():
        with psycopg.connect(PLAIN_DSN, sslmode="require", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                check("TLS required-mode connection works", cur.fetchone()[0] == 1)
        note("sslmode=require negotiated", "ok")
        stop(tls_proc)
    else:
        check("TLS-enabled server boot", False)
        stop(tls_proc)

    tlsr_proc = spawn(["--host", "127.0.0.1", "--port", str(PORT), "--tls", "--tls-cert", cert, "--tls-key", key, "--tls-required"])
    if wait_ready():
        denial = err_of(lambda: psycopg.connect(f"postgresql://plomid:plomid@127.0.0.1:{PORT}/plomid?sslmode=disable"))
        check("tls-required refuses plaintext", denial is not None, denial or "connected?!")
        with psycopg.connect(PLAIN_DSN, sslmode="require", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                check("tls-required accepts TLS", cur.fetchone()[0] == 1)
        stop(tlsr_proc)
    else:
        check("tls-required server boot", False)
        stop(tlsr_proc)

    print(f"\n{PASS}/{PASS + FAIL} checks passed")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
