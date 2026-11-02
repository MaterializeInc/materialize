# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cancel an attributed blocked peek and check the original session's recovery."""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always_or_unreachable as always,
)

from materialize.antithesis import sql, telemetry
from materialize.antithesis.observations import sometimes

FAMILY = "cancel_recovery"
FUTURE = 2**64 - 2
POLL_SECONDS = 0.1
JOIN_GRACE_SECONDS = 2.0
Execute = Callable[[psycopg.Connection, float, str, Any], psycopg.Cursor]
Row = tuple[int, int | None, str]


class Incomplete(RuntimeError):
    """A required cancellation boundary was not established."""


def _check(passed: bool, message: str, path: str, details: dict[str, Any]) -> None:
    telemetry.comparison(FAMILY, path, passed)
    always(passed, message, details)
    if not passed:
        raise Incomplete("cancellation violated its acknowledged contract")


def _identity(conn: psycopg.Connection, deadline: float, execute: Execute) -> Any:
    return execute(
        conn,
        deadline,
        "SELECT id::text,pg_backend_pid() FROM mz_internal.mz_sessions"
        f" WHERE connection_id={sql.SESSION_CONNECTION_ID}",
        None,
    ).fetchone()


def _peeks(
    observer: psycopg.Connection, deadline: float, index_id: str, execute: Execute
) -> set[str]:
    return {
        str(row[0])
        for row in execute(
            observer,
            deadline,
            "SELECT id::text FROM mz_introspection.mz_active_peeks"
            " WHERE object_id=%s AND type='index' AND time=%s::mz_timestamp",
            (index_id, str(FUTURE)),
        ).fetchall()
    }


def _rows_match(actual: Sequence[Any], expected: Sequence[Row]) -> bool:
    return (
        isinstance(actual, (list, tuple))
        and all(
            isinstance(row, (tuple, list))
            and len(row) == 3
            and type(row[0]) is int
            and (row[1] is None or type(row[1]) is int)
            and isinstance(row[2], str)
            for row in actual
        )
        and Counter(map(tuple, actual)) == Counter(expected)
    )


def _check_completed(
    result: dict[str, Any], expected: Sequence[Row], details: dict[str, Any]
) -> None:
    error = result.get("error")
    if isinstance(error, BaseException):
        raise error
    if "rows" in result:
        actual = result["rows"]
        _check(
            _rows_match(actual, expected),
            "cancel recovery: successful reads return the generated immutable input",
            "table",
            {
                **details,
                "expected_rows": len(expected),
                "actual_rows": (
                    len(actual) if isinstance(actual, (list, tuple)) else None
                ),
                "expected_sample": repr(expected[:3])[:512],
                "actual_sample": repr(
                    actual[:3] if isinstance(actual, (list, tuple)) else actual
                )[:512],
            },
        )


def run(
    worker: psycopg.Connection,
    controller: psycopg.Connection,
    observer: psycopg.Connection,
    *,
    deadline: float,
    table: str,
    index_id: str,
    expected_rows: Sequence[Row],
    execute: Execute,
) -> dict[str, Any]:
    """Cancel a future peek and verify rollback and prepared execution on its session.

    The table and index are private to this invocation. Only `worker` may issue
    the future query, and `observer` must target the same cluster. Connections
    must be idle on entry, with `controller` and `observer` distinct from
    `worker`. An incomplete invocation must discard the worker connection.
    Input rows must be committed below `FUTURE` and remain unchanged throughout.
    """
    if worker is controller or worker is observer:
        raise ValueError("cancellation requires a separate controller connection")
    connections = (worker, controller, observer)
    if any(
        conn.closed
        or conn.info.transaction_status == psycopg.pq.TransactionStatus.UNKNOWN
        for conn in connections
    ):
        raise Incomplete("a cancellation connection is unavailable")
    if any(
        not conn.autocommit
        or conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE
        for conn in connections
    ):
        raise ValueError("cancellation connections must be idle in autocommit mode")
    identity = _identity(worker, deadline, execute)
    if identity is None:
        raise Incomplete("original session identity unavailable")
    session, pid = identity
    pid = int(pid)
    details = {"session": str(session), "pid": pid, "index_id": index_id}
    execute(
        worker,
        deadline,
        f"PREPARE cancel_read AS SELECT id,v,generation FROM {table}",
        None,
    )
    execute(worker, deadline, "BEGIN", None)
    done = threading.Event()
    result: dict[str, Any] = {}

    def read() -> None:
        try:
            result["rows"] = execute(
                worker,
                deadline,
                f"SELECT id,v,generation FROM {table} AS OF {FUTURE}",
                None,
            ).fetchall()
        except BaseException as error:
            result["error"] = error
        finally:
            done.set()

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    peeks: set[str] = set()
    try:
        while time.monotonic() < deadline and not done.is_set():
            peeks = _peeks(observer, deadline, index_id, execute)
            if peeks:
                break
            time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
        if not peeks or done.is_set():
            _check_completed(result, expected_rows, details)
            raise Incomplete("future peek was not observed while the read was pending")
        current = execute(
            controller,
            deadline,
            "SELECT id::text FROM mz_internal.mz_sessions WHERE connection_id=%s::uint4",
            (str(pid & 0xFFFFFFFF),),
        ).fetchone()
        if current != (session,) or done.is_set():
            _check_completed(result, expected_rows, details)
            raise Incomplete("original pending session no longer matches the target")
        cancelled = execute(
            controller, deadline, "SELECT pg_cancel_backend(%s)", (pid,)
        ).fetchone()
        if cancelled != (True,):
            raise Incomplete("server did not acknowledge cancellation of the session")
        thread.join(max(0, deadline - time.monotonic()))
        if thread.is_alive():
            raise Incomplete("cancelled read did not retire before its deadline")
        error = result.get("error")
        if not isinstance(error, psycopg.errors.QueryCanceled):
            _check_completed(result, expected_rows, details)
            raise Incomplete(
                "pending future read completed without a cancellation response"
            )
        if error.diag.message_primary != "canceling statement due to user request":
            raise Incomplete("read ended with a different cancellation cause")
    finally:
        # No later SQL may race an outstanding cancel or an unfinished reader.
        # Out-of-band cleanup uses this connection's secret, not a recycled PID.
        if thread.is_alive():
            try:
                worker.cancel_safe(timeout=1)
            except (psycopg.Error, OSError):
                pass
            thread.join(JOIN_GRACE_SECONDS)
            if thread.is_alive():
                # Context-manager rollback takes the same lock as the reader.
                # Close first so the caller's ExitStack cannot wait on it.
                worker.close()
                thread.join(JOIN_GRACE_SECONDS)

    aborted = False
    try:
        execute(worker, deadline, "SELECT 1", None)
    except psycopg.errors.InFailedSqlTransaction:
        aborted = True
    _check(
        aborted,
        "cancel recovery: a cancelled statement leaves its explicit transaction aborted",
        "table",
        details,
    )
    execute(worker, deadline, "ROLLBACK", None)
    actual = execute(worker, deadline, "EXECUTE cancel_read", None).fetchall()
    _check(
        _rows_match(actual, expected_rows),
        "cancel recovery: prepared execution returns generated rows after cancellation and rollback",
        "prepared",
        details,
    )
    _check(
        _identity(worker, deadline, execute) == identity,
        "cancel recovery: rollback and reuse preserve the original SQL session",
        "point",
        details,
    )
    while time.monotonic() < deadline:
        if not _peeks(observer, deadline, index_id, execute):
            break
        time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
    else:
        raise Incomplete("cancelled peek retirement was not observed")
    execute(worker, deadline, "DEALLOCATE cancel_read", None)
    details["peek_ids"] = sorted(peeks)
    sometimes(
        True,
        "cancel recovery: an observed blocked peek is cancelled and its original session reused",
        details,
    )
    sometimes(
        True,
        "cancel recovery: cancelled peek resources retire after same-session rollback",
        details,
    )
    return details
