# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Workload-wide holds on discretionary DDL.

A 0dt candidate reboots whenever DDL lands that it has to react to, so a
steady stream of DDL keeps a rollout in `Applying`. The rollout driver holds
DDL for some rollouts so they can complete; drivers whose DDL is discretionary
(a rotation, a re-creation, a race they start themselves) check `active()` and
defer it while a hold covers the present. Drivers whose DDL exists to overlap
rollouts do not check, so DDL during rollouts stays covered.

Holds are recorded in state database `ddl_hold`, table
`holds(id, requested_at_monotonic, seconds, requester)`. `time.monotonic()` is
comparable across processes in the workload container.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

from materialize.antithesis import state

STATE_DB = "ddl_hold"


def _open() -> sqlite3.Connection:
    db = state.open_db(STATE_DB)
    db.execute(
        "CREATE TABLE IF NOT EXISTS holds ("
        " id INTEGER PRIMARY KEY AUTOINCREMENT,"
        " requested_at_monotonic REAL NOT NULL, seconds REAL NOT NULL,"
        " requester TEXT NOT NULL)"
    )
    db.commit()
    return db


def _request(seconds: float, requester: str) -> None:
    db = _open()
    try:
        with db:
            db.execute(
                "INSERT INTO holds (requested_at_monotonic, seconds, requester)"
                " VALUES (?, ?, ?)",
                (time.monotonic(), seconds, requester),
            )
    finally:
        db.close()


def active() -> bool:
    """Whether a recorded hold covers the present. False if the state is unreadable."""
    try:
        db = _open()
    except (sqlite3.Error, OSError):
        return False
    try:
        row = db.execute(
            "SELECT 1 FROM holds WHERE requested_at_monotonic + seconds > ? LIMIT 1",
            (time.monotonic(),),
        ).fetchone()
    except sqlite3.Error:
        return False
    finally:
        db.close()
    return row is not None


@contextmanager
def held(requester: str, chunk_s: float = 60.0) -> Iterator[None]:
    """Hold discretionary DDL while the block runs, in renewable chunks.

    The hold lapses at most `chunk_s` after the block exits, including when
    the process dies inside it.
    """
    _request(chunk_s, requester)
    done = threading.Event()

    def renew() -> None:
        while not done.wait(chunk_s / 2):
            try:
                _request(chunk_s, requester)
            except (sqlite3.Error, OSError) as e:
                print(f"ddl_hold: renewing for {requester} failed: {e}", flush=True)

    renewer = threading.Thread(target=renew, name="ddl-hold-renew", daemon=True)
    renewer.start()
    try:
        yield
    finally:
        done.set()
