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


def run_round(
    host: str,
    db: sqlite3.Connection,
    *,
    seed: list[Row],
    mutations: list[Mutation],
    cuts: tuple[int, int],
    index_columns: str,
) -> int:
    """Run one isolated schedule. SQL interruption aborts its oracle verdict."""
    deadline = time.monotonic() + BUDGET_S
    _reap(host, db, deadline)
    run = _reserve(db)
    schema = schema_name(run)
    settled = False
    details: dict[str, Any] = {
        "run": run,
        "schema": schema,
        "cuts": cuts,
        "index_columns": index_columns,
    }
    try:
        with (
            _connect(host, deadline=deadline) as writer,
            _connect(host, deadline=deadline) as reader,
            _connect(host, internal=True, deadline=deadline) as meta,
        ):
            if ddl_hold.active():
                raise Incomplete("DDL is held")
            writer_session = _execute(
                writer,
                deadline,
                "SELECT id::text FROM mz_internal.mz_sessions WHERE connection_id = pg_backend_pid()",
            ).fetchone()
            if writer_session is None:
                raise Incomplete("writer session identity unavailable")
            with db:
                db.execute(
                    "UPDATE runs SET writer_session=?, started=1 WHERE id=?",
                    (str(writer_session[0]), run),
                )
            # Namespace names are never reused. A timed-out DROP can execute
            # after the next round starts, even after this client has closed.
            _ddl(writer, deadline, f"CREATE SCHEMA {schema}")
            _ddl(
                writer, deadline, f"CREATE TABLE {schema}.t (id bigint,k int,v bigint)"
            )
            for row in seed:
                _write(writer, db, run, Mutation(row[0], row, "insert"), deadline)
            _ddl(
                writer,
                deadline,
                f"CREATE INDEX i IN CLUSTER {configure.SHARED_CLUSTER} ON {schema}.t (k,id)",
            )
            old = _identity(meta, deadline, schema, "i")
            details["old_index"] = old
            _ddl(
                writer,
                deadline,
                f"CREATE MATERIALIZED VIEW {schema}.m IN CLUSTER {configure.SHARED_CLUSTER} AS SELECT id,k,v FROM {schema}.t WHERE id>0",
            )
            durable = _identity(meta, deadline, schema, "m")
            _compare(
                Counter(
                    tuple(row)
                    for row in _execute(
                        writer, deadline, f"SELECT id,k,v FROM {schema}.m"
                    ).fetchall()
                ),
                expected_rows(db, run),
                "mv",
                details,
            )
            imported = _importer(meta, deadline, old[1], dependent=durable[1])
            guarded = 0
            if imported is not None:
                for mode in ("RESTRICT", "CASCADE"):
                    if _drop_enabled(meta, deadline) is not False:
                        break
                    error_code = None
                    try:
                        _ddl(writer, deadline, f"DROP INDEX {schema}.i {mode}")
                    except psycopg.Error as error:
                        if error.sqlstate != "2BP01":
                            raise
                        error_code = error.sqlstate
                    if _drop_enabled(meta, deadline) is not False:
                        raise Incomplete(
                            "effective drop flag changed during the guard check"
                        )
                    blocked = error_code == "2BP01"
                    telemetry.comparison("index_handover", "index", blocked)
                    always(
                        blocked,
                        "index handover: a verified durable physical importer blocks index DROP",
                        {
                            **details,
                            "importer": durable,
                            "mode": mode,
                            "sqlstate": error_code,
                        },
                    )
                    if not blocked:
                        settled = True
                        return 0
                    unchanged = (
                        _identity(meta, deadline, schema, "i") == old
                        and _identity(meta, deadline, schema, "m") == durable
                    )
                    always(
                        unchanged,
                        "index handover: rejected index DROP preserves object incarnations",
                        {**details, "importer": durable, "mode": mode},
                    )
                    guarded += 1
            sometimes(
                guarded == 2,
                "index handover: checked both DROP modes against a verified durable physical importer",
                details,
            )
            _ddl(writer, deadline, f"DROP MATERIALIZED VIEW {schema}.m")
            session_row = _execute(
                reader,
                deadline,
                "SELECT id::text FROM mz_internal.mz_sessions WHERE connection_id = pg_backend_pid()",
            ).fetchone()
            if session_row is None:
                raise Incomplete("reader session identity unavailable")
            stream = Stream()
            with reader.transaction():
                _execute(
                    reader,
                    deadline,
                    f"DECLARE handover_cursor CURSOR FOR SUBSCRIBE (SELECT id,k,v FROM {schema}.t) WITH(PROGRESS)",
                )
                _drain(reader, stream, deadline, None)
                _compare(
                    stream.bag,
                    expected_rows(db, run),
                    "subscribe",
                    {**details, "phase": "snapshot"},
                )
                subscription = _importer(
                    meta, deadline, old[1], session=str(session_row[0])
                )
                if subscription is None:
                    raise Incomplete("live SUBSCRIBE physical import was not observed")
                details["subscription"] = subscription
                for mutation in mutations[: cuts[0]]:
                    _write(writer, db, run, mutation, deadline)
                _ddl(writer, deadline, f"DROP INDEX {schema}.i")
                details["drop_acknowledged"] = True
                for mutation in mutations[cuts[0] : cuts[1]]:
                    _write(writer, db, run, mutation, deadline)
                _ddl(
                    writer,
                    deadline,
                    f"CREATE INDEX i IN CLUSTER {configure.SHARED_CLUSTER} ON {schema}.t ({index_columns})",
                )
                new = _identity(meta, deadline, schema, "i")
                details["new_index"] = new
                distinct = old[0] != new[0] and old[1] != new[1]
                telemetry.comparison("index_handover", "index", distinct)
                always(
                    distinct,
                    "index handover: same-name recreation has a distinct catalog and compute identity",
                    details,
                )
                for mutation in mutations[cuts[1] :]:
                    _write(writer, db, run, mutation, deadline)
                snapshot = _execute(
                    writer, deadline, f"SELECT mz_now()::text,id,k,v FROM {schema}.t"
                ).fetchall()
                expected = expected_rows(db, run)
                peek_ok = _compare(
                    Counter(tuple(row[1:]) for row in snapshot),
                    expected,
                    "index",
                    details,
                )
                if not snapshot:
                    raise Incomplete("empty response has no closing timestamp")
                timestamp = int(snapshot[0][0])
                _drain(reader, stream, deadline, timestamp)
                stream_ok = _compare(
                    stream.bag,
                    expected,
                    "subscribe",
                    {
                        **details,
                        "closed_frontier": stream.frontier,
                        "barrier": timestamp,
                    },
                )
                sometimes(
                    distinct and peek_ok and stream_ok,
                    "index handover: an old physical subscriber and fresh reads agree after acknowledged same-name index replacement",
                    {
                        **details,
                        "closed_frontier": stream.frontier,
                        "barrier": timestamp,
                        "acknowledged_mutations": len(mutations),
                    },
                )
                _execute(reader, deadline, "CLOSE handover_cursor")
            settled = True
            return 1
    finally:
        if settled:
            with db:
                db.execute("UPDATE runs SET settled=1 WHERE id=?", (run,))
        _cleanup(host, db, run)


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
                telemetry.comparison("index_handover", "subscribe", None)
                print(f"index handover: SQL service unavailable: {error}", flush=True)
                return 0
            count = rng.choice((3, 4, 7))
            seed = [
                (i + 1, rng.randrange(3), rng.randint(-100, 100)) for i in range(count)
            ]
            mutations = [
                Mutation(
                    count + 1,
                    (count + 1, rng.randrange(3), rng.randint(-100, 100)),
                    "insert",
                ),
                Mutation(1, (1, rng.randrange(3), rng.randint(-100, 100)), "update"),
                Mutation(count, None, "delete"),
            ]
            rng.shuffle(mutations)
            first = rng.randrange(len(mutations) + 1)
            second = rng.randrange(first, len(mutations) + 1)
            run_round(
                host,
                db,
                seed=seed,
                mutations=mutations,
                cuts=(first, second),
                index_columns=rng.choice(("v,id", "k,id", "id,k")),
            )
        except (psycopg.Error, OSError, Incomplete) as error:
            telemetry.comparison("index_handover", "subscribe", None)
            if isinstance(error, psycopg.Error):
                classified = sql.classify(error)
                always(
                    classified.outcome is not sql.Outcome.VIOLATION,
                    "index handover: SQL returns only classified errors",
                    {"sqlstate": classified.sqlstate, "template": classified.template},
                )
            print(f"index handover: incomplete round: {error}", flush=True)
        finally:
            db.close()
    return 0
