# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check prepared bindings, begun portals, and session-local temporary shadows."""

from __future__ import annotations

import sqlite3
import time
from contextlib import ExitStack
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)

from materialize.antithesis import ddl_hold, sql, state, telemetry
from materialize.antithesis import session_rebinding_model as model
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.drivers.access_control import exclusive
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

FAMILY = "session_rebinding"
BUDGET_S = 60.0
STATEMENT_TIMEOUT_MS = 10_000
MAX_LIVE_ROUNDS = 2


class Incomplete(RuntimeError):
    """A required protocol boundary was not reached, so no round is credited."""


def open_state() -> sqlite3.Connection:
    db = state.open_db(FAMILY)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " admin_session TEXT, started INTEGER NOT NULL DEFAULT 0,"
            " settled INTEGER NOT NULL DEFAULT 0, cleanup_complete INTEGER NOT NULL DEFAULT 0)"
        )
    return db


def schema_name(run: int) -> str:
    return f"{FAMILY}_r{run}"


def _reserve(db: sqlite3.Connection) -> int:
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            "SELECT count(*) FROM runs WHERE cleanup_complete=0"
        ).fetchone()
        assert row is not None
        if row[0] >= MAX_LIVE_ROUNDS:
            raise Incomplete("unresolved namespace cleanup applies backpressure")
        run = db.execute("INSERT INTO runs DEFAULT VALUES").lastrowid
        assert run is not None
        with db:
            db.execute("DELETE FROM runs WHERE id<? AND cleanup_complete=1", (run - 8,))
        return run
    except BaseException:
        db.rollback()
        raise


def _execute(
    conn: psycopg.Connection, deadline: float, statement: str, params: Any = None
) -> psycopg.Cursor:
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise Incomplete("round budget elapsed")
    conn.execute(
        f"SET statement_timeout='{min(STATEMENT_TIMEOUT_MS, remaining)}ms'".encode()
    )
    return conn.execute(statement.encode(), params)


def _connect(
    host: str, deadline: float, *, internal: bool = False
) -> psycopg.Connection:
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise Incomplete("round budget elapsed")
    conn = sql.connect(
        host,
        internal=internal,
        connect_timeout=5,
        statement_timeout_ms=min(STATEMENT_TIMEOUT_MS, remaining),
        options={
            "cluster": "mz_catalog_server" if internal else configure.SHARED_CLUSTER,
            "idle_in_transaction_session_timeout": "0",
        },
    )
    conn.prepare_threshold = None
    try:
        _execute(conn, deadline, "SET transaction_isolation='strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def _ddl(conn: psycopg.Connection, deadline: float, statement: str) -> None:
    if ddl_hold.active():
        raise Incomplete("DDL is held")
    _execute(conn, deadline, statement)


def _classify(error: psycopg.Error, phase: str) -> None:
    classified = sql.classify(error)
    always(
        classified.outcome is not sql.Outcome.VIOLATION,
        "session rebinding: SQL returns only classified errors",
        {
            "sqlstate": classified.sqlstate,
            "template": classified.template,
            "phase": phase,
        },
    )


def _cleanup(host: str, db: sqlite3.Connection, run: int, deadline: float) -> None:
    deadline = min(deadline, time.monotonic() + 5)
    row = db.execute(
        "SELECT admin_session,started,settled,cleanup_complete FROM runs WHERE id=?",
        (run,),
    ).fetchone()
    assert row is not None
    session, started, settled, complete = row
    if complete:
        return
    try:
        if started:
            if ddl_hold.active():
                return
            if not settled:
                if session is None:
                    return
                # The namespace cannot be reclaimed while its original CREATE
                # can still run after an unacknowledged client response.
                with _connect(host, deadline, internal=True) as meta:
                    active = _execute(
                        meta,
                        deadline,
                        "SELECT 1 FROM mz_internal.mz_sessions WHERE id::text=%s",
                        (session,),
                    ).fetchone()
                if active is not None:
                    return
            with _connect(host, deadline) as conn:
                _ddl(
                    conn, deadline, f"DROP SCHEMA IF EXISTS {schema_name(run)} CASCADE"
                )
        with db:
            db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
    except (psycopg.Error, OSError, Incomplete) as error:
        if isinstance(error, psycopg.Error):
            _classify(error, "cleanup")
        print(f"session rebinding: cleanup deferred: {error}", flush=True)


def _query(source: str, parameters: model.Parameters) -> str:
    direction = "DESC" if parameters.descending else "ASC"
    return (
        "SELECT id,v*$1::bigint+$2::bigint AS projected,generation,mz_now()::text AS read_ts"
        f" FROM {source} ORDER BY id {direction}"
    )


def _check(
    actual: list[Any],
    expected: tuple[model.Row, ...],
    path: str,
    details: dict[str, Any],
    *,
    timestamp: str | None = None,
) -> str | None:
    valid_shape = all(len(row) == 4 for row in actual)
    timestamps = {str(row[3]) for row in actual} if valid_shape else set()
    payload = tuple(tuple(row[:3]) for row in actual)
    passed = (
        valid_shape
        and payload == expected
        and len(timestamps) <= 1
        and all(value.isdigit() for value in timestamps)
        and (timestamp is None or not timestamps or timestamps == {timestamp})
    )
    telemetry.comparison(FAMILY, path, passed)
    always(
        passed,
        "session rebinding: results match generated session bindings and begun snapshots",
        {
            **details,
            "path": path,
            "expected": expected,
            "actual": actual,
            "expected_timestamp": timestamp,
        },
    )
    if not passed:
        raise Incomplete("session result violated its binding or snapshot contract")
    return next(iter(timestamps)) if timestamps else timestamp


def _prepared(
    conn: psycopg.Connection, deadline: float, parameters: model.Parameters, name: str
) -> list[Any]:
    return _execute(
        conn, deadline, f'EXECUTE "{name}"({parameters.factor},{parameters.offset})'
    ).fetchall()


def _fresh(
    conn: psycopg.Connection, deadline: float, parameters: model.Parameters
) -> list[Any]:
    # A newly introduced temp shadow need not invalidate an older prepared plan.
    # Fresh preparation tests acknowledged name resolution without that assumption.
    _execute(conn, deadline, f'PREPARE "Scoped Plan" AS {_query("v", parameters)}')
    rows = _prepared(conn, deadline, parameters, "Scoped Plan")
    _execute(conn, deadline, 'DEALLOCATE "Scoped Plan"')
    return rows


def run_round(
    host: str,
    db: sqlite3.Connection,
    *,
    inputs: tuple[tuple[model.Row, ...], tuple[model.Row, ...]],
    parameters: model.Parameters,
    first_page: int,
    page_size: int,
    names: tuple[str, str] = ("Read Plan", "Snapshot Cursor"),
) -> int:
    deadline = time.monotonic() + BUDGET_S
    if ddl_hold.active():
        raise Incomplete("DDL is held")
    for (old_run,) in db.execute(
        "SELECT id FROM runs WHERE cleanup_complete=0 ORDER BY id"
    ).fetchall():
        _cleanup(host, db, old_run, deadline)
    run = _reserve(db)
    schema = schema_name(run)
    expected = model.Model(inputs)
    prepared, portal = names
    if not all(inputs) or not (0 < first_page < len(inputs[0])) or page_size <= 0:
        raise ValueError("a started portal requires a nonempty unread suffix")
    details: dict[str, Any] = {
        "run": run,
        "schema": schema,
        "factor": parameters.factor,
        "offset": parameters.offset,
        "descending": parameters.descending,
        "first_page": first_page,
        "page_size": page_size,
    }
    try:
        with ExitStack() as stack:
            admin, first, second = (
                stack.enter_context(_connect(host, deadline)) for _ in range(3)
            )
            session = _execute(
                admin,
                deadline,
                "SELECT id::text FROM mz_internal.mz_sessions WHERE connection_id=pg_backend_pid()",
            ).fetchone()
            if session is None:
                raise Incomplete("admin session identity unavailable")
            with db:
                db.execute(
                    "UPDATE runs SET admin_session=?,started=1 WHERE id=?",
                    (str(session[0]), run),
                )
            _ddl(admin, deadline, f"CREATE SCHEMA {schema}")
            for number, rows in enumerate(inputs):
                _ddl(
                    admin,
                    deadline,
                    f"CREATE TABLE {schema}.t{number}(id bigint,v bigint,generation text)",
                )
                marks = ",".join("(%s,%s,%s)" for _ in rows)
                _execute(
                    admin,
                    deadline,
                    f"INSERT INTO {schema}.t{number} VALUES {marks}",
                    [value for row in rows for value in row],
                )
            _ddl(
                admin, deadline, f"CREATE VIEW {schema}.v AS SELECT * FROM {schema}.t0"
            )
            for conn in (first, second):
                _execute(conn, deadline, f"SET search_path={schema}")
                _execute(
                    conn,
                    deadline,
                    f'PREPARE "{prepared}" AS {_query(schema + ".v", parameters)}',
                )
                _check(
                    _prepared(conn, deadline, parameters, prepared),
                    expected.rows("first", parameters, qualified=True),
                    "prepared",
                    {**details, "stage": "initial"},
                )
            _execute(first, deadline, "BEGIN")
            cursor_query = (
                _query(schema + ".v", parameters)
                .replace("$1", str(parameters.factor))
                .replace("$2", str(parameters.offset))
            )
            _execute(first, deadline, f'DECLARE "{portal}" CURSOR FOR {cursor_query}')
            expected.begin_cursor("first", parameters)
            original_timestamp = _check(
                _execute(first, deadline, f'FETCH {first_page} "{portal}"').fetchall(),
                expected.fetch("first", first_page),
                "portal",
                {**details, "stage": "first_page"},
            )
            _ddl(
                admin,
                deadline,
                f"CREATE OR REPLACE VIEW {schema}.v AS SELECT * FROM {schema}.t1",
            )
            expected.replace(1)
            new_timestamp = _check(
                _prepared(second, deadline, parameters, prepared),
                expected.rows("second", parameters, qualified=True),
                "prepared",
                {**details, "stage": "after_replacement"},
            )
            details.update(
                {
                    "portal_timestamp": original_timestamp,
                    "replacement_read_timestamp": new_timestamp,
                }
            )
            while expected.cursors["first"].offset < len(inputs[0]):
                _check(
                    _execute(
                        first, deadline, f'FETCH {page_size} "{portal}"'
                    ).fetchall(),
                    expected.fetch("first", page_size),
                    "portal",
                    {**details, "stage": "remaining_page"},
                    timestamp=original_timestamp,
                )
            _check(
                _execute(first, deadline, f'FETCH ALL "{portal}"').fetchall(),
                (),
                "portal",
                {**details, "stage": "exhausted"},
                timestamp=original_timestamp,
            )
            _execute(first, deadline, "COMMIT")
            expected.commit("first")
            portal_removed = False
            try:
                _execute(first, deadline, f'FETCH ALL "{portal}"')
            except psycopg.Error as error:
                if error.sqlstate != "34000":
                    raise
                portal_removed = True
            always(
                portal_removed,
                "session rebinding: COMMIT destroys its portals",
                details,
            )
            if not portal_removed:
                raise Incomplete("COMMIT retained a portal")
            _check(
                _prepared(first, deadline, parameters, prepared),
                expected.rows("first", parameters, qualified=True),
                "prepared",
                {**details, "stage": "after_commit"},
            )
            _ddl(first, deadline, f"CREATE TEMP VIEW v AS SELECT * FROM {schema}.t0")
            expected.create_temp("first", 0)
            _check(
                _fresh(first, deadline, parameters),
                expected.rows("first", parameters),
                "temporary",
                {**details, "stage": "own_shadow"},
            )
            _check(
                _fresh(second, deadline, parameters),
                expected.rows("second", parameters),
                "temporary",
                {**details, "stage": "foreign_shadow"},
            )
            _ddl(second, deadline, f"CREATE TEMP VIEW v AS SELECT * FROM {schema}.t1")
            expected.create_temp("second", 1)
            for name, conn in (("first", first), ("second", second)):
                _check(
                    _fresh(conn, deadline, parameters),
                    expected.rows(name, parameters),
                    "temporary",
                    {**details, "stage": "two_shadows", "session": name},
                )
            _ddl(first, deadline, "DROP VIEW mz_temp.v")
            expected.drop_temp("first")
            _check(
                _fresh(first, deadline, parameters),
                expected.rows("first", parameters),
                "temporary",
                {**details, "stage": "drop_shadow"},
            )
            _ddl(
                admin,
                deadline,
                f"CREATE OR REPLACE VIEW {schema}.v AS SELECT * FROM {schema}.t0",
            )
            expected.replace(0)
            for name, conn in (("first", first), ("second", second)):
                _check(
                    _fresh(conn, deadline, parameters),
                    expected.rows(name, parameters),
                    "temporary",
                    {**details, "stage": "replace_under_shadow", "session": name},
                )
            _check(
                _prepared(second, deadline, parameters, prepared),
                expected.rows("second", parameters, qualified=True),
                "prepared",
                {**details, "stage": "qualified_bypasses_shadow"},
            )
            _ddl(second, deadline, "DROP VIEW mz_temp.v")
            expected.drop_temp("second")
            _check(
                _fresh(second, deadline, parameters),
                expected.rows("second", parameters),
                "temporary",
                {**details, "stage": "drop_final_shadow"},
            )
        with db:
            db.execute("UPDATE runs SET settled=1 WHERE id=?", (run,))
        sometimes(
            True,
            "session rebinding: a begun portal survives acknowledged durable view replacement",
            details,
        )
        sometimes(
            True,
            "session rebinding: prepared execution revalidates after durable catalog changes",
            details,
        )
        sometimes(
            True,
            "session rebinding: temporary shadows remain session local across durable replacement",
            details,
        )
        return run
    finally:
        _cleanup(host, db, run, time.monotonic() + 5)


def main() -> int:
    history.start_watchdog(BUDGET_S + 30)
    env = Environment()
    env.endpoints.state_dir.mkdir(parents=True, exist_ok=True)
    with exclusive(str(env.endpoints.state_dir / f"{FAMILY}.lock")) as held:
        if not held or ddl_hold.active():
            return 0
        db = open_state()
        try:
            try:
                host = env.sql_host()
            except Exception as error:
                print(
                    f"session rebinding: SQL service unavailable: {error}", flush=True
                )
                return 0
            inputs = []
            for generation in range(2):
                rows = []
                for key in range(rng.randint(4, 9)):
                    value = (
                        None
                        if key == 0 or rng.randrange(4) == 0
                        else rng.randint(-100, 100)
                    )
                    row = (key, value, f"generation{generation}")
                    rows.extend([row] * rng.choice((1, 1, 2)))
                inputs.append(tuple(rows))
            run_round(
                host,
                db,
                inputs=(inputs[0], inputs[1]),
                parameters=model.Parameters(
                    rng.choice((-7, -1, 0, 2, 5)),
                    rng.randint(-30, 30),
                    rng.choice((False, True)),
                ),
                first_page=rng.randint(1, len(inputs[0]) - 1),
                page_size=rng.randint(1, 4),
                names=rng.choice(
                    (("ReadPlan", "SnapshotCursor"), ("Read Plan", "Snapshot Cursor"))
                ),
            )
        except (psycopg.Error, OSError, Incomplete) as error:
            telemetry.comparison(FAMILY, "portal", None)
            if isinstance(error, psycopg.Error):
                _classify(error, "round")
            print(f"session rebinding: incomplete round: {error}", flush=True)
        finally:
            db.close()
    return 0
