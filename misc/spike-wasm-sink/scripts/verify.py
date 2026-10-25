#!/usr/bin/env python3
"""THROWAWAY: compare Valkey sink state with the MV AS OF (progress - 1).

Usage: verify.py VALKEY_HOST VALKEY_PORT PREFIX
Prints both digests and row counts; exits 1 on mismatch.
"""

import hashlib
import os
import socket
import subprocess
import sys

host, port, prefix = sys.argv[1], int(sys.argv[2]), sys.argv[3]


class Resp:
    def __init__(self):
        self.s = socket.create_connection((host, port))
        self.f = self.s.makefile("rb")

    def send(self, cmds):
        out = bytearray()
        for args in cmds:
            out += b"*%d\r\n" % len(args)
            for a in args:
                a = a if isinstance(a, bytes) else str(a).encode()
                out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)
        return [self.read() for _ in cmds]

    def read(self):
        line = self.f.readline()[:-2]
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            d = self.f.read(n + 2)[:-2]
            return d
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read() for _ in range(n)]
        raise RuntimeError(f"bad tag {line!r}")


r = Resp()
progress = r.send([["GET", f"{prefix}:progress"]])[0]
if progress is None:
    print("no progress")
    sys.exit(1)
progress = int(progress)

# Query Materialize first: the sink's hold only guarantees progress - 1
# stays readable briefly once the sink moves on.
q = (
    "SELECT id, v, n FROM mv"
    if os.environ.get("NOASOF")
    else f"SELECT id, v, n FROM mv AS OF {progress - 1}"
)
out = subprocess.run(
    [
        "psql",
        "-h",
        "localhost",
        "-p",
        "6875",
        "-U",
        "materialize",
        "-At",
        "-F",
        "|",
        "-c",
        q,
    ],
    check=True,
    capture_output=True,
    text=True,
).stdout
mz_rows = sorted(l for l in out.splitlines() if l)

keys = []
cursor = b"0"
while True:
    cursor, ks = r.send([["SCAN", cursor, "MATCH", f"{prefix}:k:*", "COUNT", 50000]])[0]
    keys.extend(ks)
    if cursor == b"0":
        break

valkey_rows = []
for i in range(0, len(keys), 20000):
    batch = keys[i : i + 20000]
    for k, h in zip(batch, r.send([["HGETALL", k] for k in batch])):
        d = {h[j].decode(): h[j + 1].decode() for j in range(0, len(h), 2)}
        key = k.decode().split(":k:", 1)[1]
        valkey_rows.append(f"{key}|{d.get('v')}|{d.get('n')}")
valkey_rows.sort()


def digest(rows):
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()[:16]


dv, dm = digest(valkey_rows), digest(mz_rows)
print(
    f"progress={progress} valkey_rows={len(valkey_rows)} mz_rows={len(mz_rows)} valkey={dv} mz={dm}"
)
if dv != dm:
    sv, sm = set(valkey_rows), set(mz_rows)
    print("only_valkey", sorted(sv - sm)[:5])
    print("only_mz", sorted(sm - sv)[:5])
    print("MISMATCH")
    sys.exit(1)
print("MATCH")
