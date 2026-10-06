# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""State shared between test command invocations within one timeline.

Each test command is a separate process, and several run concurrently, so
anything an oracle needs across invocations (operations attempted and
acknowledged, model state, observations) lives in a SQLite database in the
workload pod's state directory. Antithesis makes branching transparent, so this
is written as if there were a single linear history.

Reset detection. Setup writes `state-epoch` (a random id) once, before
`setup-complete`, and never overwrites it. Each database records the epoch it
was first opened under. Oracles that lost their ledgers pass vacuously, so
`open_db` reports a state directory that has `setup-complete` but no epoch, or
a database whose recorded epoch differs from the current one. A wipe of the
whole directory is invisible here, because setup then starts from scratch.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    unreachable,
)

from materialize.antithesis.endpoints import Endpoints

EPOCH_FILE = "state-epoch"
SETUP_MARKER = "setup-complete"


def read_epoch(state_dir: Path) -> str | None:
    try:
        return (state_dir / EPOCH_FILE).read_text().strip() or None
    except FileNotFoundError:
        return None


def _check_epoch(conn: sqlite3.Connection, state_dir: Path, name: str) -> None:
    if not (state_dir / SETUP_MARKER).exists():
        return
    epoch = read_epoch(state_dir)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS workload_state_epoch (id TEXT PRIMARY KEY)"
    )
    conn.commit()
    recorded = [r[0] for r in conn.execute("SELECT id FROM workload_state_epoch")]
    if not recorded and epoch is not None:
        # The subquery runs under the write lock, so concurrent first opens
        # record one epoch.
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO workload_state_epoch (id)"
                " SELECT ? WHERE NOT EXISTS (SELECT 1 FROM workload_state_epoch)",
                (epoch,),
            )
        recorded = [r[0] for r in conn.execute("SELECT id FROM workload_state_epoch")]
    changed = epoch is not None and any(r != epoch for r in recorded)
    if epoch is None or changed:
        unreachable(
            "workload state was reset after setup",
            {
                "database": name,
                "epoch": epoch,
                "recorded": recorded,
                "epoch_missing": epoch is None,
            },
        )


def open_db(name: str, endpoints: Endpoints | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the named database in the state directory.

    Uses WAL mode and a long busy timeout because `parallel_driver_` commands
    write concurrently. Callers create their own tables idempotently.
    """
    state_dir = (endpoints or Endpoints.from_env()).state_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(Path(state_dir) / f"{name}.sqlite", timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    _check_epoch(conn, state_dir, name)
    return conn
