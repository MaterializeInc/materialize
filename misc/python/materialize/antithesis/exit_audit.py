# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Audit the passive exit observer without importing Kubernetes or SQL clients."""

from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from typing import Any

from materialize.antithesis import observations, state

STATE_DB = "recovery"
OBSERVER_FRESH_S = 10.0
# Entry and cleanup share one claim across all commands and template profiles.
AUDIT_INTERVAL_S = 5.0
FALLBACK_INTERVAL_S = 30.0


def initialize(db: sqlite3.Connection) -> None:
    db.executescript("""
        CREATE TABLE IF NOT EXISTS exit_observer (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            sampled_at REAL NOT NULL, details TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS exit_observer_fallback (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            retry_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS exit_observer_audit (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            retry_at REAL NOT NULL);
        """)


def health(counts: Counter[str] | None = None) -> dict[str, Any]:
    durable = observations.durable_health()
    invalid = durable is not None or bool(counts and counts["forbidden"])
    return {
        "valid": not invalid,
        "errors": int(invalid),
        "diagnostics": [] if not invalid else [{"component": "exit observer"}],
        "first_error": durable,
    }


def _emit(details: dict[str, Any], counts: Counter[str]) -> None:
    observations.send_event("harness_health", health(counts))
    observations.send_event("antithesis_exit_observer", details)


def _fallback(age: float | None) -> int:
    from materialize.antithesis.drivers.recovery import _audit_fallback

    return _audit_fallback(age)


def _claim(db: sqlite3.Connection, table: str, interval: float, now: float) -> bool:
    with db:
        return bool(
            db.execute(
                f"INSERT INTO {table} VALUES (1, ?) "
                "ON CONFLICT (singleton) DO UPDATE SET retry_at = excluded.retry_at "
                f"WHERE {table}.retry_at <= ?",
                (now + interval, now),
            ).rowcount
        )


def audit(
    db: sqlite3.Connection,
    *,
    force: bool = False,
    emit: Callable[[dict[str, Any], Counter[str]], None] = _emit,
    fallback: Callable[[float | None], int] = _fallback,
) -> int:
    """Check one heartbeat, claiming a bounded fallback before loading its clients."""
    now = time.monotonic()
    if not force and not _claim(db, "exit_observer_audit", AUDIT_INTERVAL_S, now):
        return 0
    recent = db.execute(
        "SELECT sampled_at, details FROM exit_observer WHERE singleton = 1"
    ).fetchone()
    age = now - recent[0] if recent is not None else None
    if age is not None and 0 <= age <= OBSERVER_FRESH_S:
        details = json.loads(recent[1])
        emit(
            {**details, "mode": "audit", "sample_age_s": age},
            Counter(details.get("counts", {})),
        )
        return 0
    if not _claim(db, "exit_observer_fallback", FALLBACK_INTERVAL_S, now):
        return 0
    return fallback(age)


def main(*, force: bool = False) -> int:
    db = state.open_db(STATE_DB)
    try:
        initialize(db)
        return audit(db, force=force)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main(force=True))
