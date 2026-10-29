# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Workload-wide holds on DDL.

A 0dt candidate reboots whenever DDL lands that it has to react to, so a
steady stream of DDL keeps a rollout in `Applying`. The rollout driver holds
DDL for some rollouts so they can complete. There are two strengths:

* Any hold defers discretionary DDL: drivers whose DDL is a rotation, a
  re-creation, or a race they start themselves check `active()`.
* A strict hold also stops drivers whose DDL exists to overlap rollouts. They
  check `strict_active()` and skip the invocation, so a rollout under a strict
  hold sees only its own DDL. Outside strict holds those drivers keep running
  DDL during rollouts.

A command already past its check when a hold starts may still issue DDL, so a
hold reduces foreign DDL but does not guarantee its absence.

Command scopes renew rows in `holds`. Scenarios spanning commands use
`owned_holds`, whose owner remains in the scenario journal until release.
`time.monotonic()` is comparable across processes in the workload container.
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
        " requester TEXT NOT NULL, strict INTEGER NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS owned_holds ("
        " requester TEXT PRIMARY KEY, expires REAL NOT NULL, strict INTEGER NOT NULL)"
    )
    db.commit()
    return db


def _request(seconds: float, requester: str, strict: bool) -> None:
    db = _open()
    try:
        with db:
            db.execute(
                "INSERT INTO holds (requested_at_monotonic, seconds, requester, strict)"
                " VALUES (?, ?, ?, ?)",
                (time.monotonic(), seconds, requester, int(strict)),
            )
    finally:
        db.close()


def _covered(strict_only: bool) -> bool:
    try:
        db = _open()
    except (sqlite3.Error, OSError):
        return False
    try:
        row = db.execute(
            "SELECT 1 FROM holds WHERE requested_at_monotonic + seconds > ?"
            " AND strict >= ? UNION ALL"
            " SELECT 1 FROM owned_holds WHERE expires > ? AND strict >= ? LIMIT 1",
            (time.monotonic(), int(strict_only), time.monotonic(), int(strict_only)),
        ).fetchone()
    except sqlite3.Error:
        return False
    finally:
        db.close()
    return row is not None


def request_until(requester: str, expires: float, strict: bool = False) -> None:
    """Hold DDL until an absolute monotonic deadline, replacing this owner's hold."""
    db = _open()
    try:
        with db:
            db.execute(
                "DELETE FROM owned_holds WHERE expires <= ?", (time.monotonic(),)
            )
            db.execute(
                "INSERT INTO owned_holds VALUES (?, ?, ?)"
                " ON CONFLICT (requester) DO UPDATE"
                " SET expires = excluded.expires, strict = excluded.strict",
                (requester, expires, int(strict)),
            )
    finally:
        db.close()


def release(requester: str) -> None:
    """Release this owner's durable DDL hold."""
    db = _open()
    try:
        with db:
            db.execute("DELETE FROM owned_holds WHERE requester = ?", (requester,))
    finally:
        db.close()


def active() -> bool:
    """Whether any hold covers the present. False if the state is unreadable."""
    return _covered(strict_only=False)


def strict_active() -> bool:
    """Whether a strict hold covers the present. False if the state is unreadable."""
    return _covered(strict_only=True)


@contextmanager
def held(requester: str, strict: bool = False, chunk_s: float = 60.0) -> Iterator[None]:
    """Hold DDL while the block runs, in renewable chunks.

    The hold lapses at most `chunk_s` after the block exits, including when
    the process dies inside it.
    """
    _request(chunk_s, requester, strict)
    done = threading.Event()

    def renew() -> None:
        while not done.wait(chunk_s / 2):
            try:
                _request(chunk_s, requester, strict)
            except (sqlite3.Error, OSError) as e:
                print(f"ddl_hold: renewing for {requester} failed: {e}", flush=True)

    renewer = threading.Thread(target=renew, name="ddl-hold-renew", daemon=True)
    renewer.start()
    try:
        yield
    finally:
        done.set()
