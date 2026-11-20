# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Mid-run quiet periods.

`eventually_` and `finally_` commands end a timeline branch. A driver that
wants to check liveness and keep testing afterwards asks Antithesis to pause
faults instead. Outside Antithesis the request is a no-op, so the caller still
waits out its recovery budget.

Every request is recorded in state database `quiet`, table
`quiet_periods(id, requested_at_monotonic, seconds, requester)`, so any
command can tell whether faults are currently paused at the workload's request
and how much of the recent past was fault-free. The workload does not see
Antithesis's own fault schedule, so this is the only fault context it has.
`time.monotonic()` is comparable across processes in the workload container.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from materialize.antithesis import state

STATE_DB = "quiet"


def _open() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.execute(
        "CREATE TABLE IF NOT EXISTS quiet_periods ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " requested_at_monotonic REAL NOT NULL, seconds REAL NOT NULL,"
        " requester TEXT NOT NULL)"
    )
    db.commit()
    return db


def _default_requester() -> str:
    return Path(sys.argv[0]).stem if sys.argv and sys.argv[0] else "unknown"


def _stop_faults(seconds: int) -> bool:
    binary = os.environ.get("ANTITHESIS_STOP_FAULTS")
    if not binary:
        return False
    subprocess.run([binary, str(seconds)], check=False)
    return True


def _active(db: sqlite3.Connection, now: float) -> bool:
    row = db.execute(
        "SELECT 1 FROM quiet_periods WHERE requested_at_monotonic + seconds > ?"
        " LIMIT 1",
        (now,),
    ).fetchone()
    return row is not None


def request_quiet_period(seconds: int, requester: str | None = None) -> bool:
    """Pause all fault injection for `seconds`. Returns whether faults were paused.

    Antithesis restarts killed containers when the quiet period starts, but
    they take time to become operational, so callers must still allow a
    recovery budget before asserting liveness. Concurrent requests merge to
    the latest end.
    """
    db = _open()
    try:
        with db:
            db.execute(
                "INSERT INTO quiet_periods (requested_at_monotonic, seconds, requester)"
                " VALUES (?, ?, ?)",
                (time.monotonic(), float(seconds), requester or _default_requester()),
            )
    finally:
        db.close()
    return _stop_faults(seconds)


def active_quiet_period() -> bool:
    """Whether a recorded quiet period covers the present."""
    db = _open()
    try:
        return _active(db, time.monotonic())
    finally:
        db.close()


def recent_quiet_fraction(window_s: float) -> float:
    """Fraction of the last `window_s` seconds covered by recorded quiet periods."""
    if window_s <= 0:
        return 0.0
    now = time.monotonic()
    lo = now - window_s
    db = _open()
    try:
        rows = db.execute(
            "SELECT requested_at_monotonic, requested_at_monotonic + seconds"
            " FROM quiet_periods WHERE requested_at_monotonic + seconds > ?"
            " AND requested_at_monotonic < ? ORDER BY requested_at_monotonic",
            (lo, now),
        ).fetchall()
    finally:
        db.close()
    covered = 0.0
    end = lo
    for start, stop in rows:
        start, stop = max(start, end), min(stop, now)
        if stop > start:
            covered += stop - start
            end = stop
    return covered / window_s


def _try_claim(seconds: int, requester: str) -> bool:
    """Record a request unless another quiet period is active, atomically."""
    db = _open()
    try:
        db.execute("BEGIN IMMEDIATE")
        now = time.monotonic()
        if _active(db, now):
            db.rollback()
            return False
        db.execute(
            "INSERT INTO quiet_periods (requested_at_monotonic, seconds, requester)"
            " VALUES (?, ?, ?)",
            (now, float(seconds), requester),
        )
        db.commit()
        return True
    finally:
        db.close()


@contextmanager
def held(
    requester: str, chunk_s: int, exclusive: bool = False
) -> Iterator[bool | None]:
    """Keep faults paused while the block runs, in renewable chunks.

    Requests `chunk_s` seconds and re-requests every half chunk from a daemon
    thread, so faults resume at most `chunk_s` after the block exits however
    long the block took. Yields whether faults were paused, or None without
    requesting anything if `exclusive` and another quiet period was active.
    """
    if exclusive:
        if not _try_claim(chunk_s, requester):
            yield None
            return
        paused = _stop_faults(chunk_s)
    else:
        paused = request_quiet_period(chunk_s, requester)
    done = threading.Event()

    def renew() -> None:
        while not done.wait(chunk_s / 2):
            try:
                request_quiet_period(chunk_s, requester)
            except (sqlite3.Error, OSError) as e:
                print(f"quiet: renewing for {requester} failed: {e}", flush=True)

    renewer = threading.Thread(target=renew, name="quiet-renew", daemon=True)
    renewer.start()
    try:
        yield paused
    finally:
        done.set()
