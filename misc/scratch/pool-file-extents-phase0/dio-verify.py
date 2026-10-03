# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Checks that O_DIRECT writes read back intact on a filesystem.

Each worker process owns one O_TMPFILE|O_DIRECT file with a few 64 KiB slots
and loops: optionally punch and fallocate a slot, write a pattern, read it
back, and compare. No two workers share a file or a range, so any mismatch is
a fault below the application, in the kernel, the block layer, or the device.
This is the I/O sequence the buffer pool's file store uses for demotion.

Usage: dio-verify.py DIR PROCS ITERS
Environment: PUNCH_P (default 0.5) is the probability of punching a slot
before a write. FALLOC=0 skips the fallocate after a punch.
"""

import ctypes
import ctypes.util
import mmap
import multiprocessing
import os
import random
import sys

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_long, ctypes.c_long]
FALLOC_FL_PUNCH_HOLE_KEEP_SIZE = 0x02 | 0x01
SLOT = 64 << 10
SLOTS_PER_WORKER = 8
PAGE = 4096


def worker(args):
    dirpath, wid, iters = args
    punch_p = float(os.environ.get("PUNCH_P", "0.5"))
    falloc = os.environ.get("FALLOC", "1") == "1"
    fd = os.open(dirpath, os.O_RDWR | os.O_TMPFILE | os.O_DIRECT, 0o600)
    # Anonymous mmaps are page aligned, as O_DIRECT requires.
    buf = mmap.mmap(-1, SLOT)
    rbuf = mmap.mmap(-1, SLOT)
    rng = random.Random(wid)
    for i in range(iters):
        slot = rng.randrange(SLOTS_PER_WORKER)
        off = slot * SLOT
        if rng.random() < punch_p:
            if libc.fallocate(fd, FALLOC_FL_PUNCH_HOLE_KEEP_SIZE, off, SLOT) != 0:
                return ("punch errno", ctypes.get_errno())
            if falloc and libc.fallocate(fd, 0, off, SLOT) != 0:
                return ("fallocate errno", ctypes.get_errno())
        seed = rng.randrange(251)
        pattern = bytes((j + seed) % 251 for j in range(251))
        data = (pattern * (SLOT // 251 + 1))[:SLOT]
        buf[:] = data
        assert os.pwritev(fd, [buf], off) == SLOT
        assert os.preadv(fd, [rbuf], off) == SLOT
        if rbuf[:] != data:
            bad = [
                j
                for j in range(0, SLOT, PAGE)
                if rbuf[j : j + PAGE] != data[j : j + PAGE]
            ]
            return ("MISMATCH", wid, i, slot, bad)
    return None


def main():
    dirpath, procs, iters = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    with multiprocessing.Pool(procs) as pool:
        results = pool.map(worker, [(dirpath, w, iters) for w in range(procs)])
    bad = [r for r in results if r]
    print("procs", procs, "iters", iters, "result:", bad if bad else "clean")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
