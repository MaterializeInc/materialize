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
a database whose recorded epoch differs from the current one. External identity
markers also distinguish a new lazy database from a deleted or replaced oracle
database. A wipe of the whole directory is invisible here, because setup then
starts from scratch.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from pathlib import Path
from typing import NoReturn

from materialize.antithesis import observations
from materialize.antithesis.endpoints import Endpoints

EPOCH_FILE = "state-epoch"
SETUP_MARKER = "setup-complete"


class StateResetError(RuntimeError):
    """The workload's oracle state no longer belongs to its setup epoch."""


def _invalid_database(name: str, reason: str) -> NoReturn:
    observations.harness_error(
        "state",
        "workload database identity is invalid",
        {"database": name, "reason": reason},
    )
    raise StateResetError(f"workload state reset: {name}: {reason}")


def _identity_marker(state_dir: Path, name: str) -> Path:
    return state_dir / f"{name}.identity"


def _valid_identity(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return uuid.UUID(value).hex == value
    except ValueError:
        return False


def _read_identity(state_dir: Path, name: str) -> str | None:
    try:
        record = json.loads(_identity_marker(state_dir, name).read_text())
    except FileNotFoundError:
        return None
    except (ValueError, UnicodeError):
        _invalid_database(name, "malformed identity marker")
    if (
        not isinstance(record, dict)
        or record.get("version") != 1
        or record.get("database") != name
    ):
        _invalid_database(name, "malformed identity marker")
    identity = record.get("identity")
    if not isinstance(identity, str) or not _valid_identity(identity):
        _invalid_database(name, "malformed identity marker")
    return identity


def _publish_identity(state_dir: Path, name: str, identity: str) -> None:
    marker = _identity_marker(state_dir, name)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=state_dir, prefix=f".{name}-", delete=False
        ) as output:
            temporary = Path(output.name)
            json.dump({"version": 1, "database": name, "identity": identity}, output)
            output.flush()
            os.fsync(output.fileno())
        try:
            # A hard link publishes the complete marker without replacing a concurrent first open's marker.
            os.link(temporary, marker)
        except FileExistsError:
            if _read_identity(state_dir, name) != identity:
                _invalid_database(name, "identity changed while opening database")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _check_identity(
    conn: sqlite3.Connection, state_dir: Path, name: str, expected: str | None
) -> None:
    if expected is not None:
        try:
            row = conn.execute(
                "SELECT id FROM workload_database_identity WHERE singleton = 1"
            ).fetchone()
        except sqlite3.OperationalError as error:
            if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_ERROR:
                raise
            _invalid_database(name, "database identity table is missing or malformed")
        if row is None or row[0] != expected:
            _invalid_database(name, "database was replaced after it was opened")
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS workload_database_identity (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), id TEXT NOT NULL)"
        )
        row = conn.execute(
            "SELECT id FROM workload_database_identity WHERE singleton = 1"
        ).fetchone()
        expected = _read_identity(state_dir, name)
        if expected is not None and (row is None or row[0] != expected):
            _invalid_database(name, "database was replaced after it was opened")
        if row is None:
            identity = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO workload_database_identity VALUES (1, ?)", (identity,)
            )
        else:
            identity = row[0]
            if not _valid_identity(identity):
                _invalid_database(name, "malformed database identity")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if expected is None:
        # Commit the database stamp before publishing its external identity. A first-open retry can then reuse it.
        _publish_identity(state_dir, name, identity)


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
        observations.harness_error(
            "state",
            "workload state was reset after setup",
            {
                "database": name,
                "epoch": epoch,
                "recorded": recorded,
                "epoch_missing": epoch is None,
            },
        )
        raise StateResetError("workload state was reset after setup")


def open_db(name: str, endpoints: Endpoints | None = None) -> sqlite3.Connection:
    """Open (creating if needed) the named database in the state directory.

    Uses WAL mode and a long busy timeout because `parallel_driver_` commands
    write concurrently. Callers create their own tables idempotently.
    Raises `StateResetError` if an oracle identity or setup epoch was lost or changed.
    """
    state_dir = (endpoints or Endpoints.from_env()).state_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    database = Path(state_dir) / f"{name}.sqlite"
    expected = _read_identity(state_dir, name)
    if expected is not None and not database.exists():
        _invalid_database(name, "database was deleted after it was opened")
    conn = sqlite3.connect(database, timeout=60)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _check_epoch(conn, state_dir, name)
        _check_identity(conn, state_dir, name, expected)
    except BaseException:
        conn.close()
        raise
    return conn
