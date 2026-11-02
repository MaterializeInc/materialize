# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Compare maintained windows and live retractions with generated input semantics."""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from collections import Counter
from collections.abc import Sequence
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)

from materialize.antithesis import ddl_hold, sql, state, telemetry
from materialize.antithesis import window_retractions_model as model
from materialize.antithesis.drivers import configure, history
from materialize.antithesis.drivers.access_control import exclusive
from materialize.antithesis.environment import Environment
from materialize.antithesis.observations import sometimes
from materialize.antithesis.rng import rng

SCHEMA = "window_retractions"
BUDGET_S = 90.0
STATEMENT_TIMEOUT_MS = 10_000
MAX_LIVE_ROUNDS = 2
MAX_RESULT_ROWS = 128


class Incomplete(RuntimeError):
    """A required acknowledgement or closed frontier is unavailable."""


def open_state() -> sqlite3.Connection:
    db = state.open_db(SCHEMA)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " writer_session TEXT, started INTEGER DEFAULT 0, settled INTEGER DEFAULT 0,"
            " cleanup_complete INTEGER DEFAULT 0)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS writes (id INTEGER PRIMARY KEY, run INTEGER,"
            " mutations TEXT NOT NULL, acknowledged INTEGER DEFAULT 0)"
        )
    return db


def schema_name(run: int) -> str:
    return f"{SCHEMA}_r{run}"


def reserve(db: sqlite3.Connection) -> int:
    db.execute("BEGIN IMMEDIATE")
    try:
        if (
            db.execute("SELECT count(*) FROM runs WHERE cleanup_complete=0").fetchone()[
                0
            ]
            >= MAX_LIVE_ROUNDS
        ):
            raise Incomplete("pending namespace cleanup applies backpressure")
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


def expected_rows(db: sqlite3.Connection, run: int) -> tuple[model.Input, ...]:
    rows: tuple[model.Input, ...] = ()
    for encoded, acknowledged in db.execute(
        "SELECT mutations, acknowledged FROM writes WHERE run=? ORDER BY id", (run,)
    ):
        if not acknowledged:
            raise Incomplete(
                "a write outcome is unknown; this epoch has no oracle verdict"
            )
        mutations = [
            model.Mutation(key, None if row is None else tuple(row))
            for key, row in json.loads(encoded)
        ]
        rows = model.apply(rows, mutations)
    return rows


def _execute(
    conn: psycopg.Connection, deadline: float, text: str, params: Any = None
) -> psycopg.Cursor:
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise Incomplete("window round budget exhausted")
    conn.execute(
        f"SET statement_timeout='{min(STATEMENT_TIMEOUT_MS, remaining)}ms'".encode()
    )
    return conn.execute(text.encode(), params)


def _connect(
    host: str, deadline: float, *, internal: bool = False
) -> psycopg.Connection:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise Incomplete("window connection budget exhausted")
    conn = sql.connect_with_retry(
        host,
        min(10, remaining),
        internal=internal,
        statement_timeout_ms=min(STATEMENT_TIMEOUT_MS, max(1, int(remaining * 1000))),
        options={
            "cluster": configure.SHARED_CLUSTER,
            "idle_in_transaction_session_timeout": "0",
        },
    )
    try:
        _execute(conn, deadline, "SET transaction_isolation='strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def _write(
    conn: psycopg.Connection,
    db: sqlite3.Connection,
    run: int,
    mutations: Sequence[model.Mutation],
    deadline: float,
) -> None:
    existing = {row[0] for row in expected_rows(db, run)}
    with db:
        identifier = db.execute(
            "INSERT INTO writes(run,mutations) VALUES (?,?)",
            (run, json.dumps([(mutation.key, mutation.row) for mutation in mutations])),
        ).lastrowid
    # UPDATE and DELETE require autocommit. Publish the phase acknowledgement
    # only after every statement returns. Any unknown prefix aborts this epoch.
    deleted = [mutation.key for mutation in mutations if mutation.row is None]
    if deleted:
        _execute(
            conn,
            deadline,
            f"DELETE FROM {schema_name(run)}.t WHERE id IN ({','.join('%s' for _ in deleted)})",
            deleted,
        )
    inserted = []
    for mutation in mutations:
        if mutation.row is None:
            continue
        if mutation.key in existing:
            row = mutation.row
            _execute(
                conn,
                deadline,
                f"UPDATE {schema_name(run)}.t SET g=%s,o=%s,v=%s,off=%s,def=%s WHERE id=%s",
                (*row[1:], row[0]),
            )
        else:
            inserted.append(mutation.row)
    if inserted:
        _execute(
            conn,
            deadline,
            f"INSERT INTO {schema_name(run)}.t VALUES {','.join('(%s,%s,%s,%s,%s,%s)' for _ in inserted)}",
            [value for row in inserted for value in row],
        )
    with db:
        db.execute("UPDATE writes SET acknowledged=1 WHERE id=?", (identifier,))


def check_rows(
    actual: Sequence[Sequence[Any]],
    expected: Counter,
    path: str,
    details: dict | None = None,
) -> bool:
    messages = {
        "table": "window retractions: acknowledged mutations equal the base snapshot",
        "index": "window retractions: maintained index equals the input-derived window oracle",
        "mv": "window retractions: materialized windows equal the input-derived window oracle",
        "recompute": "window retractions: fresh window recomputation equals the input-derived oracle",
        "subscribe": "window retractions: closed SUBSCRIBE windows equal the input-derived oracle",
    }
    try:
        observed = Counter(tuple(row) for row in actual)
        valid = observed == expected
    except (TypeError, ValueError):
        observed = Counter()
        valid = False
    telemetry.comparison(SCHEMA, path, valid, max(1, sum(expected.values())))
    always(
        valid,
        messages[path],
        {
            **(details or {}),
            "expected": list(expected.items())[:4],
            "actual": list(observed.items())[:4],
        },
    )
    if not valid:
        raise Incomplete("recorded a window result violation")
    return True


def recompute_relation(rows: Sequence[model.Input]) -> str:
    types = ("bigint", "int", "int", "int", "int", "int")
    if not rows:
        return (
            "(SELECT "
            + ",".join(
                f"NULL::{kind} AS {name}"
                for name, kind in zip(model.INPUT_COLUMNS.split(","), types)
            )
            + " WHERE FALSE) AS generated"
        )
    values = ",".join(
        "("
        + ",".join(
            f"{'NULL' if value is None else value}::{kind}"
            for value, kind in zip(row, types)
        )
        + ")"
        for row in rows
    )
    return f"(VALUES {values}) AS generated({model.INPUT_COLUMNS})"


def _drain(
    conn: psycopg.Connection, stream: model.Stream, deadline: float, through: int | None
) -> None:
    while time.monotonic() < deadline:
        stream.feed(
            _execute(
                conn, deadline, "FETCH ALL window_cursor WITH(timeout='100ms')"
            ).fetchall()
        )
        always(
            not stream.errors,
            "window retractions: SUBSCRIBE progress and closed multiplicities remain valid",
            {"errors": stream.errors[:5]},
        )
        if stream.errors:
            raise Incomplete("recorded a subscription protocol violation")
        if (
            stream.start is not None
            and stream.frontier is not None
            and stream.frontier > (stream.start if through is None else through)
        ):
            return
    raise Incomplete("window subscription did not close its requested timestamp")


def _session(conn: psycopg.Connection, deadline: float) -> str:
    row = _execute(
        conn,
        deadline,
        "SELECT id::text FROM mz_internal.mz_sessions"
        f" WHERE connection_id={sql.SESSION_CONNECTION_ID}",
    ).fetchone()
    if row is None:
        raise Incomplete("SQL session identity unavailable")
    return str(row[0])


def _physical_import(
    meta: psycopg.Connection, session: str, schema: str, deadline: float
) -> bool:
    end = min(deadline, time.monotonic() + 3)
    while time.monotonic() < end:
        imported = _execute(
            meta,
            deadline,
            "SELECT 1 FROM mz_internal.mz_subscriptions AS s JOIN mz_internal.mz_compute_dependencies AS c ON c.object_id=s.id JOIN mz_internal.mz_object_global_ids AS g ON g.global_id=c.dependency_id JOIN mz_catalog.mz_objects AS o ON o.id=g.id JOIN mz_catalog.mz_schemas AS n ON n.id=o.schema_id WHERE s.session_id::text=%s AND n.name=%s AND o.name='i'",
            (session, schema),
        ).fetchone()
        if imported is not None:
            return True
        time.sleep(min(0.1, max(0.0, end - time.monotonic())))
    return False


def _checkpoint(
    reader: psycopg.Connection,
    subscription: psycopg.Connection,
    stream: model.Stream,
    db: sqlite3.Connection,
    run: int,
    plan: model.Plan,
    deadline: float,
    phase: int,
) -> None:
    schema = schema_name(run)
    rows = expected_rows(db, run)
    expected = model.evaluate(rows, plan.spec)
    details = {
        "run": run,
        "phase": phase,
        "rows": len(rows),
        "frame": plan.spec.frame,
        "order": plan.spec.order(unique=True),
    }
    with reader.transaction():
        barrier = _execute(
            reader,
            deadline,
            f"SELECT mz_now()::text FROM (SELECT count(*) FROM {schema}.t UNION ALL SELECT count(*) FROM {schema}.v UNION ALL SELECT count(*) FROM {schema}.m) AS held",
        ).fetchone()
        if barrier is None:
            raise Incomplete("window timestamp barrier is unavailable")
        timestamp = int(barrier[0])
        for name, columns, path, wanted in (
            ("t", model.INPUT_COLUMNS, "table", Counter(rows)),
            ("v", model.OUTPUT_COLUMNS, "index", expected),
            ("m", model.OUTPUT_COLUMNS, "mv", expected),
        ):
            result = _execute(
                reader,
                deadline,
                f"SELECT {columns} FROM {schema}.{name} LIMIT {MAX_RESULT_ROWS + 1}",
            ).fetchall()
            check_rows(result, wanted, path, details)
        result = _execute(
            reader, deadline, model.query(recompute_relation(rows), plan.spec)
        ).fetchall()
        check_rows(result, expected, "recompute", details)
        _drain(subscription, stream, deadline, timestamp)
        check_rows(
            list(stream.bag.elements()),
            expected,
            "subscribe",
            {**details, "barrier": timestamp, "frontier": stream.frontier},
        )


def _cleanup(host: str, db: sqlite3.Connection, run: int, deadline: float) -> bool:
    deadline = min(deadline, time.monotonic() + 5)
    row = db.execute(
        "SELECT writer_session,started,settled,cleanup_complete FROM runs WHERE id=?",
        (run,),
    ).fetchone()
    assert row is not None
    session, started, settled, complete = row
    if complete:
        return True
    if not started:
        with db:
            db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
        return True
    if ddl_hold.active():
        return False
    try:
        if not settled:
            if session is None:
                return False
            with _connect(host, deadline, internal=True) as meta:
                if (
                    _execute(
                        meta,
                        deadline,
                        "SELECT 1 FROM mz_internal.mz_sessions WHERE id::text=%s",
                        (session,),
                    ).fetchone()
                    is not None
                ):
                    return False
        with _connect(host, deadline) as conn:
            _execute(
                conn, deadline, f"DROP SCHEMA IF EXISTS {schema_name(run)} CASCADE"
            )
        with db:
            db.execute("UPDATE runs SET cleanup_complete=1 WHERE id=?", (run,))
        return True
    except (psycopg.Error, OSError) as error:
        classified = sql.classify(error)
        always(
            classified.outcome is not sql.Outcome.VIOLATION,
            "window retractions: SQL returns only classified errors",
            {
                "phase": "cleanup",
                "sqlstate": classified.sqlstate,
                "template": classified.template,
            },
        )
        return False
    except Incomplete:
        return False


def run_round(host: str, db: sqlite3.Connection, plan: model.Plan) -> int:
    deadline = time.monotonic() + BUDGET_S
    for (old,) in db.execute(
        "SELECT id FROM runs WHERE cleanup_complete=0 ORDER BY id"
    ).fetchall():
        _cleanup(host, db, old, deadline)
    run = reserve(db)
    schema = schema_name(run)
    completed = False
    try:
        with (
            _connect(host, deadline) as writer,
            _connect(host, deadline) as reader,
            _connect(host, deadline) as subscriber,
            _connect(host, deadline, internal=True) as meta,
        ):
            if ddl_hold.active():
                raise Incomplete("DDL is held")
            session = _session(writer, deadline)
            subscription_session = _session(subscriber, deadline)
            with db:
                db.execute(
                    "UPDATE runs SET writer_session=?,started=1 WHERE id=?",
                    (session, run),
                )
            _execute(writer, deadline, f"CREATE SCHEMA {schema}")
            _execute(
                writer,
                deadline,
                f"CREATE TABLE {schema}.t (id bigint,g int,o int,v int,off int,def int)",
            )
            _write(
                writer,
                db,
                run,
                [model.Mutation(row[0], row) for row in plan.seed],
                deadline,
            )
            _execute(
                writer,
                deadline,
                f"CREATE VIEW {schema}.v AS {model.query(f'{schema}.t',plan.spec)}",
            )
            _execute(
                writer,
                deadline,
                f"CREATE INDEX i IN CLUSTER {configure.SHARED_CLUSTER} ON {schema}.v (id)",
            )
            _execute(
                writer,
                deadline,
                f"CREATE MATERIALIZED VIEW {schema}.m IN CLUSTER {configure.SHARED_CLUSTER} AS {model.query(f'{schema}.t',plan.spec)}",
            )
            stream = model.Stream()
            with subscriber.transaction():
                _execute(
                    subscriber,
                    deadline,
                    f"DECLARE window_cursor CURSOR FOR SUBSCRIBE (SELECT {model.OUTPUT_COLUMNS} FROM {schema}.v) WITH(PROGRESS)",
                )
                _drain(subscriber, stream, deadline, None)
                imported = _physical_import(
                    meta, subscription_session, schema, deadline
                )
                _checkpoint(reader, subscriber, stream, db, run, plan, deadline, 0)
                for phase, mutations in enumerate(plan.phases, 1):
                    _write(writer, db, run, mutations, deadline)
                    _checkpoint(
                        reader, subscriber, stream, db, run, plan, deadline, phase
                    )
                _execute(subscriber, deadline, "CLOSE window_cursor")
            details = {
                "run": run,
                "schema": schema,
                "phases": len(plan.phases) + 1,
                "rows": len(plan.seed),
                "physical_import": imported,
                "retracted": stream.retracted,
                "frame": plan.spec.frame,
                "order": plan.spec.order(unique=True),
            }
            sometimes(
                True,
                "window retractions: checked peer ranks and nullable offsets against generated input",
                details,
            )
            sometimes(
                imported and stream.retracted,
                "window retractions: a physical subscriber matched changed partitions and retracted window rows",
                details,
            )
            sometimes(
                True,
                "window retractions: deleting and reinserting a partition preserved all window paths",
                details,
            )
            sometimes(
                plan.spec.lower > 0 or plan.spec.upper < 0,
                "window retractions: checked empty ROWS frames and default peer frames",
                details,
            )
            completed = True
            with db:
                db.execute("UPDATE runs SET settled=1 WHERE id=?", (run,))
            return 1
    finally:
        failure = sys.exception()
        try:
            _cleanup(host, db, run, time.monotonic() + 5)
        except Exception:
            if failure is None:
                raise
        if not completed:
            telemetry.comparison(SCHEMA, "subscribe", None)


def main() -> int:
    history.start_watchdog(BUDGET_S + 30)
    env = Environment()
    env.endpoints.state_dir.mkdir(parents=True, exist_ok=True)
    with exclusive(str(env.endpoints.state_dir / f"{SCHEMA}.lock")) as held:
        if not held or ddl_hold.active():
            return 0
        db = open_state()
        try:
            try:
                host = env.sql_host()
            except Exception as error:
                print(
                    f"window retractions: SQL service unavailable: {error}", flush=True
                )
                telemetry.comparison(SCHEMA, "subscribe", None)
                return 0
            try:
                run_round(host, db, model.generate(rng))
                return 0
            except (psycopg.Error, OSError) as error:
                classified = sql.classify(error)
                always(
                    classified.outcome is not sql.Outcome.VIOLATION,
                    "window retractions: SQL returns only classified errors",
                    {"sqlstate": classified.sqlstate, "template": classified.template},
                )
            except (Incomplete, OverflowError) as error:
                print(f"window retractions: incomplete round: {error}", flush=True)
            return 0
        finally:
            db.close()
