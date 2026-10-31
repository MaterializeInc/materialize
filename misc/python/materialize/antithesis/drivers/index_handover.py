# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check physical index dependencies and a live reader across name reuse.

A durable importer blocks DROP, including CASCADE, when the effective system
flag disables dropping in-use indexes. A transient SUBSCRIBE does not block
DROP. Its closed output and a fresh read after same-name index recreation must
both equal the generated, acknowledged mutation ledger.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)

from materialize.antithesis import ddl_hold, sql, state, telemetry
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.drivers.access_control import exclusive
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

SCHEMA = "index_handover"
BUDGET_S = 60.0
STATEMENT_TIMEOUT_MS = 10_000
METADATA_WAIT_S = 5.0
MAX_STREAM_ROWS = 2048
MAX_LIVE_ROUNDS = 2
Row = tuple[int, int, int]


class Incomplete(RuntimeError):
    """A required observation was unavailable, so this round has no verdict."""


@dataclass(frozen=True)
class Mutation:
    key: int
    row: Row | None
    kind: str


def open_state() -> sqlite3.Connection:
    db = state.open_db(SCHEMA)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " writer_session TEXT, started INTEGER NOT NULL DEFAULT 0,"
            " settled INTEGER NOT NULL DEFAULT 0, cleanup_complete INTEGER NOT NULL DEFAULT 0)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS writes (id INTEGER PRIMARY KEY, run INTEGER,"
            " kind TEXT, row_key INTEGER, row_value TEXT, acknowledged INTEGER DEFAULT 0)"
        )
    return db


def schema_name(run: int) -> str:
    return f"{SCHEMA}_r{run}"


def _reserve(db: sqlite3.Connection) -> int:
    db.execute("BEGIN IMMEDIATE")
    try:
        count = db.execute(
            "SELECT count(*) FROM runs WHERE cleanup_complete=0"
        ).fetchone()
        assert count is not None
        if count[0] >= MAX_LIVE_ROUNDS:
            raise Incomplete("unresolved namespace cleanup applies backpressure")
        run = db.execute("INSERT INTO runs DEFAULT VALUES").lastrowid
        assert run is not None
        db.execute(
            "DELETE FROM writes WHERE run IN (SELECT id FROM runs WHERE id < ? AND cleanup_complete=1)",
            (run - 8,),
        )
        db.execute("DELETE FROM runs WHERE id < ? AND cleanup_complete=1", (run - 8,))
        db.commit()
        return run
    except BaseException:
        db.rollback()
        raise


def expected_rows(db: sqlite3.Connection, run: int) -> Counter[Row]:
    rows: dict[int, Row] = {}
    for kind, key, encoded in db.execute(
        "SELECT kind, row_key, row_value FROM writes"
        " WHERE run = ? AND acknowledged = 1 ORDER BY id",
        (run,),
    ):
        if kind == "delete":
            rows.pop(key, None)
        else:
            rows[key] = tuple(json.loads(encoded))
    return Counter(rows.values())


@dataclass
class Stream:
    start: int | None = None
    frontier: int | None = None
    bag: Counter[Row] = field(default_factory=Counter)
    pending: dict[int, Counter[Row]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    rows: int = 0

    def feed(self, batch: Sequence[Sequence[Any]]) -> None:
        for values in batch:
            self.rows += 1
            if self.rows > MAX_STREAM_ROWS:
                raise Incomplete("subscription exceeded its bounded capture")
            if not isinstance(values, Sequence) or len(values) != 6:
                self.errors.append("subscription row has the wrong arity")
                continue
            stamp, progressed, diff, key, group, value = values
            if type(progressed) is not bool:
                self.errors.append("subscription progress marker is not boolean")
                continue
            try:
                if stamp is None:
                    raise ValueError("NULL timestamp")
                stamp = int(stamp)
            except (TypeError, ValueError, OverflowError):
                self.errors.append("subscription timestamp is malformed")
                continue
            if progressed:
                if self.start is None:
                    self.start = stamp
                if self.frontier is not None and stamp < self.frontier:
                    self.errors.append("subscription progress regressed")
                for timestamp in sorted(list(self.pending)):
                    if timestamp < stamp:
                        self.bag.update(self.pending.pop(timestamp))
                        if any(count < 0 for count in self.bag.values()):
                            self.errors.append(
                                "closed output has a negative multiplicity"
                            )
                self.frontier = stamp
            else:
                if self.start is None:
                    self.errors.append("subscription data precedes initial progress")
                if self.frontier is not None and stamp < self.frontier:
                    self.errors.append(
                        "subscription update arrived below closed progress"
                    )
                try:
                    row = (int(key), int(group), int(value))
                    multiplicity = int(diff)
                except (TypeError, ValueError, OverflowError):
                    self.errors.append("subscription data fields are malformed")
                    continue
                self.pending.setdefault(stamp, Counter())[row] += multiplicity
        self.bag = Counter({row: count for row, count in self.bag.items() if count})

    @property
    def snapshot_closed(self) -> bool:
        return (
            self.start is not None
            and self.frontier is not None
            and self.frontier > self.start
        )


def _execute(
    conn: psycopg.Connection, deadline: float, statement: str, params: Any = None
) -> psycopg.Cursor:
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise Incomplete("round budget elapsed")
    conn.execute(
        f"SET statement_timeout = '{min(STATEMENT_TIMEOUT_MS, remaining)}ms'".encode()
    )
    return conn.execute(statement.encode(), params)


def _connect(
    host: str, *, internal: bool = False, deadline: float | None = None
) -> psycopg.Connection:
    timeout = STATEMENT_TIMEOUT_MS
    if deadline is not None:
        timeout = min(timeout, int(1000 * (deadline - time.monotonic())))
        if timeout <= 0:
            raise Incomplete("round budget elapsed")
    conn = sql.connect(
        host,
        internal=internal,
        connect_timeout=5,
        statement_timeout_ms=timeout,
        options={
            "cluster": "mz_catalog_server" if internal else configure.SHARED_CLUSTER,
            "idle_in_transaction_session_timeout": "0",
        },
    )
    if deadline is not None:
        remaining = int(1000 * (deadline - time.monotonic()))
        if remaining <= 0:
            conn.close()
            raise Incomplete("round budget elapsed")
        conn.execute(f"SET statement_timeout='{min(timeout, remaining)}ms'".encode())
    conn.execute("SET transaction_isolation = 'strict serializable'")
    return conn


def _ddl(conn: psycopg.Connection, deadline: float, statement: str) -> None:
    if ddl_hold.active():
        raise Incomplete("DDL is held")
    _execute(conn, deadline, statement)


def _identity(
    conn: psycopg.Connection, deadline: float, schema: str, name: str
) -> tuple[str, str]:
    row = _execute(
        conn,
        deadline,
        "SELECT o.id, g.global_id FROM mz_catalog.mz_objects o"
        " JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id"
        " JOIN mz_catalog.mz_databases d ON d.id = s.database_id"
        " JOIN mz_internal.mz_object_global_ids g ON g.id = o.id"
        " WHERE d.name = 'materialize' AND s.name = %s AND o.name = %s",
        (schema, name),
    ).fetchone()
    if row is None:
        raise Incomplete(f"catalog identity for {name} is unavailable")
    return str(row[0]), str(row[1])


def _drop_enabled(conn: psycopg.Connection, deadline: float) -> bool | None:
    try:
        row = _execute(conn, deadline, "SHOW enable_drop_in_use_index").fetchone()
    except psycopg.Error as error:
        if error.sqlstate == "42704":
            return None
        raise
    if row is None or str(row[0]).lower() not in ("on", "off", "true", "false"):
        return None
    return str(row[0]).lower() in ("on", "true")


def _importer(
    conn: psycopg.Connection,
    deadline: float,
    index: str,
    *,
    dependent: str | None = None,
    session: str | None = None,
) -> str | None:
    end = min(deadline, time.monotonic() + METADATA_WAIT_S)
    while time.monotonic() < end:
        if session is not None:
            row = _execute(
                conn,
                deadline,
                "SELECT s.id FROM mz_internal.mz_subscriptions s"
                " JOIN mz_internal.mz_compute_dependencies c ON c.object_id = s.id"
                " WHERE s.session_id::text = %s AND c.dependency_id = %s",
                (session, index),
            ).fetchone()
        else:
            row = _execute(
                conn,
                deadline,
                "SELECT object_id FROM mz_internal.mz_compute_dependencies"
                " WHERE object_id = %s AND dependency_id = %s",
                (dependent, index),
            ).fetchone()
        if row is not None:
            return str(row[0])
        time.sleep(min(0.1, max(0.0, end - time.monotonic())))
    return None


def _write(
    conn: psycopg.Connection,
    db: sqlite3.Connection,
    run: int,
    mutation: Mutation,
    deadline: float,
) -> None:
    schema = schema_name(run)
    with db:
        write = db.execute(
            "INSERT INTO writes (run,kind,row_key,row_value) VALUES (?,?,?,?)",
            (run, mutation.kind, mutation.key, json.dumps(mutation.row)),
        ).lastrowid
    if mutation.kind == "insert":
        _execute(
            conn, deadline, f"INSERT INTO {schema}.t VALUES (%s,%s,%s)", mutation.row
        )
    elif mutation.kind == "update":
        assert mutation.row is not None
        _execute(
            conn,
            deadline,
            f"UPDATE {schema}.t SET k=%s,v=%s WHERE id=%s",
            (mutation.row[1], mutation.row[2], mutation.key),
        )
    else:
        _execute(conn, deadline, f"DELETE FROM {schema}.t WHERE id=%s", (mutation.key,))
    # A lost write response leaves an intent, but never licenses a comparison
    # against an acknowledged-only model. Its exception aborts the whole round.
    with db:
        db.execute("UPDATE writes SET acknowledged = 1 WHERE id = ?", (write,))


def _compare(
    actual: Counter[Row], expected: Counter[Row], path: str, details: dict[str, Any]
) -> bool:
    passed = actual == expected
    telemetry.comparison("index_handover", path, passed)
    always(
        passed,
        "index handover: reads equal the acknowledged mutation ledger",
        {
            **details,
            "path": path,
            "expected": sorted(expected.items()),
            "actual": sorted(actual.items()),
        },
    )
    return passed


def _drain(
    conn: psycopg.Connection, stream: Stream, deadline: float, through: int | None
) -> None:
    while time.monotonic() < deadline:
        stream.feed(
            _execute(
                conn, deadline, "FETCH ALL handover_cursor WITH (timeout='100ms')"
            ).fetchall()
        )
        if stream.errors:
            always(
                False,
                "index handover: SUBSCRIBE respects frontier and multiplicity invariants",
                {"errors": stream.errors[:3]},
            )
            raise Incomplete("subscription violated its stream protocol")
        if stream.snapshot_closed and (
            through is None
            or (stream.frontier is not None and stream.frontier > through)
        ):
            return
    raise Incomplete(
        "subscription did not close the requested timestamp within this round"
    )


def _cleanup(
    host: str, db: sqlite3.Connection, run: int, deadline: float | None = None
) -> None:
    deadline = min(
        deadline if deadline is not None else float("inf"), time.monotonic() + 5
    )
    try:
        row = db.execute(
            "SELECT writer_session, started, settled, cleanup_complete FROM runs WHERE id=?",
            (run,),
        ).fetchone()
        assert row is not None
        writer_session, started, settled, complete = row
        if complete:
            return
        if not started:
            with db:
                db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
            return
        if ddl_hold.active():
            return
        if not settled:
            if writer_session is None:
                return
            # An acknowledged cleanup can precede an unacknowledged CREATE.
            # Retain the namespace until its original writer has retired.
            with _connect(host, internal=True, deadline=deadline) as meta:
                active = _execute(
                    meta,
                    deadline,
                    "SELECT 1 FROM mz_internal.mz_sessions WHERE id::text=%s",
                    (writer_session,),
                ).fetchone()
            if active is not None:
                return
        with _connect(host, deadline=deadline) as conn:
            _ddl(conn, deadline, f"DROP SCHEMA IF EXISTS {schema_name(run)} CASCADE")
        with db:
            db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
    except Exception as error:
        print(
            f"index handover: cleanup of {schema_name(run)} deferred: {error}",
            flush=True,
        )


def _reap(host: str, db: sqlite3.Connection, deadline: float) -> None:
    for (run,) in db.execute(
        "SELECT id FROM runs WHERE cleanup_complete=0 ORDER BY id"
    ).fetchall():
        _cleanup(host, db, run, deadline)


