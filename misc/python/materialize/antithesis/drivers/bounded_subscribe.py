# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Check finite subscriptions against acknowledged input across sparse table writes.

A hold transaction protects the lower timestamp. A later strict-serializable
read separates the included tail from an excluded canary. Hot sibling writes
exercise txn-WAL progress while the cold table receives no further writes.
Expected rows come only from generated, acknowledged INSERTs in the journal.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
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

SCHEMA = "bounded_subscribe"
BUDGET_S = 90.0
STATEMENT_TIMEOUT_MS = 20_000
MAX_TIMESTAMP = 2**64 - 1
MAX_RESULT_ROWS = 4096
Row = tuple[int, int]


def open_state() -> sqlite3.Connection:
    db = state.open_db(SCHEMA)
    with db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT)"
        )
        db.execute(
            "CREATE TABLE IF NOT EXISTS writes (run INTEGER, phase TEXT, rows TEXT,"
            " acknowledged INTEGER NOT NULL DEFAULT 0)"
        )
    return db


def expected_rows(db: sqlite3.Connection, run: int, snapshot: bool) -> Counter[Row]:
    result: Counter[Row] = Counter()
    for phase, encoded in db.execute(
        "SELECT phase, rows FROM writes WHERE run = ? AND acknowledged = 1", (run,)
    ):
        if phase == "tail" or (snapshot and phase == "seed"):
            result.update(tuple(row) for row in json.loads(encoded))
    return result


def validate_rows(
    rows: Sequence[Sequence[Any]], lower: int, upper: int, expected: Counter[Row]
) -> list[str]:
    actual: Counter[Row] = Counter()
    updates: dict[int, Counter[Row]] = defaultdict(Counter)
    errors = []
    if not rows:
        errors.append("initial progress is missing or differs from AS OF")
    progress = lower

    def integer(value: Any) -> int:
        if value is None or isinstance(value, bool) or int(value) != value:
            raise ValueError("not an integer")
        return int(value)

    for index, row in enumerate(rows):
        try:
            stamp, progressed, diff, key, value = row
            stamp = integer(stamp)
            if progressed is not True and progressed is not False:
                raise ValueError("not a progress flag")
            if not progressed:
                record = (integer(key), integer(value))
                diff = integer(diff)
        except (TypeError, ValueError, OverflowError):
            errors.append("malformed subscription row")
            continue
        if index == 0 and (stamp != lower or not progressed):
            errors.append("initial progress is missing or differs from AS OF")
        if progressed:
            if stamp < progress or stamp < lower:
                errors.append("progress precedes AS OF or regressed")
            progress = stamp
        else:
            if stamp < max(lower, progress) or stamp >= upper:
                errors.append("update outside the window or below closed progress")
            updates[stamp][record] += diff
    # EOF closes every timestamp. Consolidate each timestamp before testing
    # append-only state, so equivalent differential encodings have one oracle.
    for stamp in sorted(updates):
        actual.update(updates[stamp])
        if any(
            count not in (0, 1) or (count and row not in expected)
            for row, count in actual.items()
        ):
            errors.append(
                "closed append-only state has an unexpected multiplicity or row"
            )
    if actual != expected:
        errors.append(
            f"acknowledged input differs: expected={expected}, actual={actual}"
        )
    return errors


def _execute(
    conn: psycopg.Connection, deadline: float, text: str, params: Any = None
) -> psycopg.Cursor:
    remaining = int(1000 * (deadline - time.monotonic()))
    if remaining <= 0:
        raise TimeoutError("bounded subscription budget elapsed")
    conn.execute(
        f"SET statement_timeout = '{min(STATEMENT_TIMEOUT_MS, remaining)}ms'".encode()
    )
    return conn.execute(text.encode(), params)


def _connect(host: str, cluster: str) -> psycopg.Connection:
    conn = sql.connect_with_retry(
        host,
        10,
        statement_timeout_ms=STATEMENT_TIMEOUT_MS,
        options={
            "cluster": cluster,
            "idle_in_transaction_session_timeout": "0",
        },
    )
    try:
        conn.execute("SET transaction_isolation = 'strict serializable'")
    except BaseException:
        conn.close()
        raise
    return conn


def _insert(
    conn: psycopg.Connection,
    db: sqlite3.Connection,
    run: int,
    phase: str,
    rows: list[Row],
    deadline: float,
) -> None:
    if not rows:
        return
    with db:
        rowid = db.execute(
            "INSERT INTO writes (run, phase, rows) VALUES (?, ?, ?)",
            (run, phase, json.dumps(rows)),
        ).lastrowid
    values = ",".join("(%s,%s)" for _ in rows)
    _execute(
        conn,
        deadline,
        f"INSERT INTO {SCHEMA}.cold VALUES {values}",
        [value for row in rows for value in row],
    )
    with db:
        db.execute("UPDATE writes SET acknowledged = 1 WHERE rowid = ?", (rowid,))


def _cleanup_old_rows(conn: psycopg.Connection, deadline: float, run: int) -> None:
    for table in ("cold", "hot"):
        _execute(conn, deadline, f"DELETE FROM {SCHEMA}.{table} WHERE v < %s", (run,))


def _timestamp(conn: psycopg.Connection, deadline: float, table: str, run: int) -> int:
    row = _execute(
        conn,
        deadline,
        f"SELECT mz_now()::text FROM (SELECT count(*) FROM {SCHEMA}.{table} WHERE v = %s) AS c",
        (run,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def read_window(
    conn: psycopg.Connection,
    lower: int,
    upper: int,
    snapshot: bool,
    deadline: float,
    fetch_size: int,
    run: int,
) -> list[tuple]:
    result = []
    with conn.transaction():
        _execute(
            conn,
            deadline,
            f"DECLARE bounded_cursor CURSOR FOR SUBSCRIBE"
            f" (SELECT k,v FROM {SCHEMA}.cold WHERE v = {run})"
            f" WITH (PROGRESS, SNAPSHOT = {'true' if snapshot else 'false'})"
            f" AS OF {lower} UP TO {upper}",
        )
        while True:
            # An empty timed FETCH can mean no rows are ready. Without a FETCH
            # timeout, an empty batch proves the finite subscription reached EOF.
            batch = _execute(
                conn, deadline, f"FETCH {fetch_size} bounded_cursor"
            ).fetchall()
            result.extend(batch)
            if len(result) > MAX_RESULT_ROWS:
                raise TimeoutError("finite subscription exceeded capture budget")
            if not batch:
                break
        _execute(conn, deadline, "CLOSE bounded_cursor")
    return result


def run_round(
    host: str,
    db: sqlite3.Connection,
    *,
    seed_count: int,
    tail_count: int,
    hot_count: int,
    snapshot: bool,
    fetch_size: int,
    empty_at_max: bool = False,
    cluster: str = configure.SHARED_CLUSTER,
    before_read: Callable[[psycopg.Connection], None] | None = None,
) -> int:
    """Check one generated schedule. Interrupted SQL raises without completing it."""
    deadline = time.monotonic() + BUDGET_S
    with db:
        run = db.execute("INSERT INTO runs DEFAULT VALUES").lastrowid
        assert run is not None
        db.execute("DELETE FROM writes WHERE run < ?", (run - 2,))
        db.execute("DELETE FROM runs WHERE id < ?", (run - 2,))
    with _connect(host, cluster) as writer:
        if ddl_hold.active():
            return 0
        _execute(writer, deadline, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        for name in ("cold", "hot"):
            _execute(
                writer,
                deadline,
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{name} (k bigint, v bigint)",
            )
        # Old SQL can finish after its client exits. Cleanup must preserve
        # later run IDs, and reads must exclude late inserts from older runs.
        _cleanup_old_rows(writer, deadline, run)
        seed = [(i, run) for i in range(seed_count)]
        tail = [(seed_count + i, run) for i in range(tail_count)]
        _insert(writer, db, run, "seed", seed, deadline)
        with _connect(host, cluster) as hold, hold.transaction():
            lower = _timestamp(hold, deadline, "cold", run)
            _insert(writer, db, run, "tail", tail, deadline)
            for i in range(hot_count):
                _execute(
                    writer,
                    deadline,
                    f"INSERT INTO {SCHEMA}.hot VALUES (%s,%s)",
                    (i, run),
                )
            with _connect(host, cluster) as barrier:
                # A cold-table snapshot can physically fill its empty progress
                # interval (txn_read::DataSnapshot::unblock_read). Keep the
                # barrier on the hot sibling before exercising the cold reader.
                closed = _timestamp(barrier, deadline, "hot", run)
            if closed >= MAX_TIMESTAMP:
                raise ValueError("no finite timestamp after the barrier")
            upper = closed + 1
            with _connect(host, cluster) as reader:
                if before_read is not None:
                    before_read(reader)
                for case in ("sparse", "canary", "empty"):
                    if case == "canary":
                        _insert(
                            writer,
                            db,
                            run,
                            "canary",
                            [(seed_count + tail_count, run)],
                            deadline,
                        )
                    end = MAX_TIMESTAMP if case == "empty" and empty_at_max else upper
                    start = end if case == "empty" else lower
                    rows = read_window(
                        reader, start, end, snapshot, deadline, fetch_size, run
                    )
                    expected = (
                        Counter()
                        if case == "empty"
                        else expected_rows(db, run, snapshot)
                    )
                    errors = validate_rows(rows, start, end, expected)
                    telemetry.comparison("bounded_subscribe", "subscribe", not errors)
                    always(
                        not errors,
                        "bounded subscribe: finite windows contain exactly acknowledged input below the exclusive upper",
                        {
                            "run": run,
                            "lower": start,
                            "upper": end,
                            "snapshot": snapshot,
                            "case": case,
                            "errors": errors[:3],
                        },
                    )
                    guidance = {
                        "run": run,
                        "tail_rows": tail_count,
                        "hot_transactions": hot_count,
                        "snapshot": snapshot,
                        "fetch_size": fetch_size,
                        "empty_at_max": empty_at_max,
                    }
                    if case == "sparse":
                        sometimes(
                            not errors,
                            "bounded subscribe: checked a finite window after hot-only sibling transactions",
                            guidance,
                        )
                    elif case == "canary":
                        sometimes(
                            not errors,
                            "bounded subscribe: replayed a finite window after an excluded canary write",
                            guidance,
                        )
                    else:
                        sometimes(
                            not errors,
                            "bounded subscribe: completed an empty finite window",
                            guidance,
                        )
    return 3


def main() -> int:
    history.start_watchdog(BUDGET_S + 30)
    env = Environment()
    env.endpoints.state_dir.mkdir(parents=True, exist_ok=True)
    with exclusive(str(env.endpoints.state_dir / f"{SCHEMA}.lock")) as held:
        if not held:
            return 0
        db = open_state()
        try:
            run_round(
                env.sql_host(),
                db,
                seed_count=rng.choice((0, 1, 8)),
                tail_count=rng.choice((1, 8, 64)),
                hot_count=rng.choice((1, 4, 12)),
                snapshot=rng.choice((False, True)),
                fetch_size=rng.choice((1, 4, 128)),
                empty_at_max=rng.choice((False, True)),
            )
        except (psycopg.Error, OSError, TimeoutError) as error:
            telemetry.comparison("bounded_subscribe", "subscribe", None)
            if isinstance(error, psycopg.Error):
                classified = sql.classify_as_of_read(error)
                always(
                    classified.outcome is not sql.Outcome.VIOLATION,
                    "bounded subscribe: SQL returns only classified errors",
                    {"sqlstate": classified.sqlstate, "template": classified.template},
                )
            print(f"bounded subscribe: incomplete round: {error}", flush=True)
        finally:
            db.close()
    return 0
