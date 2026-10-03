# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Probe a directory for the file-extent store's filesystem requirements.

Reports O_TMPFILE, O_DIRECT at 512 and 4096 alignment, fallocate, and
FALLOC_FL_PUNCH_HOLE support as JSON.
"""

import ctypes
import ctypes.util
import json
import mmap
import os
import sys

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_PUNCH_HOLE = 0x02


def fallocate(fd, mode, off, length):
    r = libc.fallocate(
        ctypes.c_int(fd), ctypes.c_int(mode), ctypes.c_long(off), ctypes.c_long(length)
    )
    if r != 0:
        e = ctypes.get_errno()
        raise OSError(e, os.strerror(e))


def attempt(fn):
    try:
        fn()
        return "ok"
    except OSError as e:
        return f"{e.errno} {e.strerror}"


def main(d):
    res = {"dir": d}
    with open("/proc/self/mounts") as f:
        best = ""
        for line in f:
            dev, mnt, fstype = line.split()[:3]
            if d.startswith(mnt) and len(mnt) >= len(best):
                best, res["fstype"], res["device"] = mnt, fstype, dev
    st = os.statvfs(d)
    res["bavail_bytes"] = st.f_bavail * st.f_frsize
    res["total_bytes"] = st.f_blocks * st.f_frsize

    fd = None

    def tmpfile():
        nonlocal fd
        fd = os.open(d, os.O_TMPFILE | os.O_RDWR | os.O_DIRECT, 0o600)

    res["o_tmpfile_direct"] = attempt(tmpfile)
    if fd is None:
        path = os.path.join(d, f".probe-{os.getpid()}")
        res["o_direct_named"] = attempt(
            lambda: globals().__setitem__(
                "_fd", os.open(path, os.O_CREAT | os.O_RDWR | os.O_DIRECT, 0o600)
            )
        )
        fd = globals().get("_fd")
        if fd is not None:
            os.unlink(path)
    if fd is None:
        print(json.dumps(res))
        return

    res["fallocate"] = attempt(lambda: fallocate(fd, 0, 0, 64 << 20))
    buf = mmap.mmap(-1, 1 << 20)
    buf.write(os.urandom(1 << 20))
    mv = memoryview(buf)
    for align in (512, 4096):

        def rw():
            n = os.pwrite(fd, mv[:align], align)
            assert n == align
            m = os.preadv(fd, [mv[align : 2 * align]], align)
            assert m == align

        res[f"o_direct_{align}"] = attempt(rw)
    res["o_direct_unaligned"] = attempt(lambda: os.pwrite(fd, mv[:100], 0))
    res["punch_hole"] = attempt(
        lambda: fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 0, 1 << 20)
    )
    os.close(fd)
    print(json.dumps(res))


if __name__ == "__main__":
    for d in sys.argv[1:]:
        main(d)
