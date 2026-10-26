#!/usr/bin/env python3
"""Minimal SCRAM-SHA-256 raw client: parses the server-first message and
times each stage, printing the advertised iteration count and salt size.

Purpose: distinguish slow client-side PBKDF2 (huge advertised iterations)
from server-side delay. Usage:
    python3 mini_scram.py <host> <port> <user> <password>
"""

import base64
import hashlib
import hmac
import os
import socket
import struct
import sys
import time


def recv_msg(sock) -> tuple[bytes, bytes]:
    tag = sock.recv(1)
    if not tag:
        raise EOFError("server closed")
    (length,) = struct.unpack("!I", recvn(sock, 4))
    payload = recvn(sock, length - 4)
    return tag, payload


def recvn(sock, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("short read")
        buf += chunk
    return buf


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    sock = socket.create_connection((host, port), timeout=30)

    t0 = time.time()
    body = struct.pack("!I", 196608) + b"user\x00" + user.encode() + b"\x00database\x00plomid\x00\x00"
    sock.sendall(struct.pack("!I", len(body) + 4) + body)

    tag, payload = recv_msg(sock)  # AuthenticationSASL
    mechanisms = payload[4:].split(b"\x00")
    print(f"t={time.time()-t0:.3f}s {tag=} mechanisms={mechanisms}")

    nonce = base64.b64encode(os.urandom(18)).decode()
    client_first_bare = f"n={user},r={nonce}"
    client_first = "n,," + client_first_bare
    sasl_init = b"SCRAM-SHA-256\x00" + struct.pack("!I", len(client_first)) + client_first.encode()
    sock.sendall(b"p" + struct.pack("!I", len(sasl_init) + 4) + sasl_init)

    tag, payload = recv_msg(sock)  # SASLContinue
    t1 = time.time()
    code = struct.unpack("!I", payload[:4])[0]
    if tag != b"R" or code != 11:
        print(f"t={t1-t0:.3f}s UNEXPECTED response: tag={tag} code={code} payload={payload[:200]!r}")
        sock.close()
        return 2
    server_first = payload[4:].decode()
    print(f"t={t1-t0:.3f}s server-first: {server_first}")

    fields = dict(part.split("=", 1) for part in server_first.split(","))
    r = fields["r"]
    salt = base64.b64decode(fields["s"])
    iterations = int(fields["i"])
    print(f"ADVERTISED iterations={iterations} salt_len={len(salt)}")

    t2 = time.time()
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    pbkdf2_ms = (time.time() - t2) * 1000
    print(f"client PBKDF2 took {pbkdf2_ms:.1f} ms")

    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_final_wo_proof = f"c=biws,r={r}"
    auth_message = f"{client_first_bare},{server_first},{client_final_wo_proof}"
    client_sig = hmac.new(stored_key, auth_message.encode(), hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    client_final = f"{client_final_wo_proof},p={base64.b64encode(proof).decode()}"
    sock.sendall(b"p" + struct.pack("!I", len(client_final) + 4) + client_final.encode())

    tag, payload = recv_msg(sock)  # SASLFinal
    t3 = time.time()
    print(f"t={t3-t0:.3f}s server-final: {payload[4:][:60]!r}")
    # AuthenticationOk should follow
    tag, payload = recv_msg(sock)
    ok = tag == b"R" and struct.unpack("!I", payload[:4])[0] == 0
    print(f"t={time.time()-t0:.3f}s auth_ok={ok} tag={tag}")
    sock.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
