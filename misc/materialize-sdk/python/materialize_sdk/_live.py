# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the
# root of this repository, or online at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Live subscriptions over psycopg."""

from __future__ import annotations

import json
from collections import deque
from typing import Any

import psycopg
from psycopg import generators, pq

from materialize_sdk._core import Subscription
from materialize_sdk._errors import ServerError, UsageError
from materialize_sdk._values import Event, event_from_json


class LiveSubscription:
    """A snapshot, then every change, as events.

    Iterating yields each event in the core's JSON form, a
    ``{"event": "snapshot_chunk", ...}`` or ``{"event": "batch", ...}`` dict,
    with each timestamp and diff as an ``int``. The subscription owns its
    connection, and ``close`` ends the session and the subscribe on the server.
    """

    def __init__(
        self,
        conninfo: str,
        query: str,
        *,
        max_rows: int = 1000,
        fetch_timeout: float = 1.0,
        max_pending_bytes: int | None = 64 << 20,
        max_timestamp_bytes: int | None = 1 << 30,
    ) -> None:
        # Checked here, so that an invalid option fails before connecting.
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 1:
            raise UsageError(f"max_rows is a positive integer, not {max_rows!r}")
        if not fetch_timeout >= 0:
            raise UsageError(f"fetch_timeout is at least 0, not {fetch_timeout!r}")
        options: dict[str, Any] = {
            "subscribe": {
                "target": {"kind": "query", "sql": query},
                "envelope": {"kind": "diff"},
            },
            "start": {"kind": "snapshot"},
        }
        if max_pending_bytes is not None:
            options["max_pending_bytes"] = str(max_pending_bytes)
        if max_timestamp_bytes is not None:
            options["max_timestamp_bytes"] = str(max_timestamp_bytes)
        self._subscription = Subscription(json.dumps(options))
        self._fetch = json.dumps(
            {"max_rows": str(max_rows), "timeout_ms": str(round(fetch_timeout * 1000))}
        )
        self._events: deque[Event] = deque()
        self._failed: BaseException | None = None
        self._closed = False
        try:
            self._connection = psycopg.connect(conninfo, autocommit=True)
        # TODO: libpq keeps no SQLSTATE for a failed connection, so a refused
        # password classifies as transient here, where the Rust and Node
        # packages see 28P01 and classify it as credentials.
        except psycopg.Error as error:
            raise self._error(error) from error
        try:
            self._run("BEGIN", pq.ExecStatus.COMMAND_OK)
            self._run(self._subscription.declare_sql(), pq.ExecStatus.COMMAND_OK)
        except BaseException:
            self._connection.close()
            raise

    def __iter__(self) -> LiveSubscription:
        return self

    def __next__(self) -> Event:
        if self._failed is not None:
            raise self._failed
        if self._closed:
            raise StopIteration
        try:
            return self._next()
        except StopIteration:
            self.close()
            raise
        except BaseException as error:
            # The binding still waits for the failed FETCH, so every later
            # call raises the first failure again.
            self._failed = error
            self.close()
            raise

    def _next(self) -> Event:
        while not self._events:
            if self._subscription.is_finished:
                raise StopIteration
            fetch = json.loads(self._subscription.fetch(self._fetch))
            columns, rows = self._run(fetch["sql"], pq.ExecStatus.TUPLES_OK)
            pushed = self._subscription.push(
                json.dumps({"columns": columns, "rows": rows})
            )
            self._events.extend(
                event_from_json(event) for event in json.loads(pushed)["events"]
            )
        return self._events.popleft()

    @property
    def as_of(self) -> int | None:
        """The stream's ``AS OF``, once known."""
        return self._subscription.as_of

    def close(self) -> None:
        """Closes the connection, which ends the subscribe on the server and
        the iteration."""
        self._closed = True
        self._connection.close()

    def __enter__(self) -> LiveSubscription:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _run(
        self, sql: str, expected: pq.ExecStatus
    ) -> tuple[list[str], list[list[str | None]]]:
        """Runs ``sql`` and returns the column names and rows, or raises a
        ``ServerError`` with the core's classification."""
        # The extended protocol refuses text that holds more than one
        # statement, which a query could otherwise smuggle into the DECLARE.
        # Its text result format is the form the core decodes.
        # NOTE: psycopg's public `execute` sends a statement without parameters
        # through the simple protocol. `wait` is what turns Ctrl-C during an
        # untimed FETCH into a cancel on the server.
        pgconn = self._connection.pgconn
        try:
            pgconn.send_query_params(sql.encode(), None, result_format=pq.Format.TEXT)
            [result] = self._connection.wait(generators.execute(pgconn))
        except psycopg.Error as error:
            raise self._error(error) from error
        if result.status != expected:
            field = result.error_field
            sqlstate = field(pq.DiagnosticField.SQLSTATE)
            message = field(pq.DiagnosticField.MESSAGE_PRIMARY) or result.error_message
            raise self._failure(
                message.decode()
                or f"unexpected result status {pq.ExecStatus(result.status).name}",
                sqlstate.decode() if sqlstate else None,
            )
        columns = [(result.fname(i) or b"").decode() for i in range(result.nfields)]
        rows = [
            [
                None if value is None else value.decode()
                for value in (
                    result.get_value(row, col) for col in range(result.nfields)
                )
            ]
            for row in range(result.ntuples)
        ]
        return columns, rows

    def _error(self, error: psycopg.Error) -> ServerError:
        return self._failure(str(error), error.sqlstate)

    def _failure(self, message: str, sqlstate: str | None) -> ServerError:
        classified = self._subscription.classify(
            json.dumps({"sqlstate": sqlstate, "message": message})
        )
        return ServerError(message, sqlstate, json.loads(classified))
