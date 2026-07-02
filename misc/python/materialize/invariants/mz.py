# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SQL client for the system under test with bounded, classified operations.

Every query and write is bounded by a client-side watchdog that cancels the
statement server-side: statement_timeout does not bound plain SELECTs, and
during a metadata-store outage even writes block in group commit, so without
this no thread would be guaranteed to terminate.

Outcome contract for writes: COMMITTED only on a success reply. FAILED only
when the server definitely did not apply the write (an error reply to a
single autocommit statement, or any failure strictly before COMMIT was
submitted in an explicit transaction). Everything else (cancellation,
connection loss, timeout) is UNKNOWN.
"""

import threading
import time
from typing import Any

import psycopg
from psycopg.errors import QueryCanceled

from materialize.invariants.framework import (
    Outcome,
    ScenarioContext,
    TransientError,
)

# Reads racing concurrent DDL legitimately error, e.g. when a peek planned
# against an index that the DDL churn dropped mid-flight. The data is never
# wrong, the checker round is just skipped.
CONCURRENT_DDL_ERROR_SNIPPETS = [
    "was dropped",
    "unknown catalog item",
    "is not readable at any timestamp",
]

# Substrings of psycopg error messages that indicate the connection (not the
# statement) failed. Mirrors parallel-workload's classification.
CONNECTION_ERROR_SNIPPETS = [
    "server closed the connection unexpectedly",
    "Can't create a connection to host",
    "Connection refused",
    "connection timeout expired",
    "the connection is lost",
    "connection is closed",
    "EOF detected",
    "connection failed",
    "consuming input failed",
    "terminating connection",
    "current transaction is aborted",
]


class UnexpectedQueryError(Exception):
    """A definite server error we did not expect. Fails the run."""


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, psycopg.OperationalError):
        return True
    msg = str(e)
    return any(snippet in msg for snippet in CONNECTION_ERROR_SNIPPETS)


class _Watchdog(threading.Thread):
    """Cancels its client's running statement once the deadline passes.

    One persistent daemon thread per client instead of a timer per query,
    since workers issue hundreds of ops per second. Daemon so it never blocks
    process exit.
    """

    def __init__(self, client: "MzClient") -> None:
        super().__init__(name=f"watchdog-{client.name}", daemon=True)
        self.client = client

    def run(self) -> None:
        while True:
            time.sleep(0.5)
            self.client.maybe_cancel()


class MzClient:
    """One psycopg connection to Materialize, owned by a single thread.

    Reconnects lazily. Any transient failure drops the connection so the next
    use starts from a clean slate (no half-open transactions).
    """

    def __init__(
        self,
        ctx: ScenarioContext,
        name: str,
        user: str = "materialize",
        database: str = "materialize",
        port: int | None = None,
    ) -> None:
        self.host = ctx.endpoints.mz_host
        self.port = port if port is not None else ctx.endpoints.mz_port
        self.user = user
        self.database = database
        self.name = name
        self.log = ctx.log
        self.default_timeout = ctx.complexity.query_timeout
        self._conn: psycopg.Connection | None = None
        self._lock = threading.Lock()
        self._deadline: float | None = None
        ctx.clients.append(self)
        _Watchdog(self).start()

    def _connect(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            try:
                conn = psycopg.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    dbname=self.database,
                    connect_timeout=10,
                    autocommit=True,
                )
            except Exception as e:
                raise TransientError(f"connect failed: {e}") from e
            with self._lock:
                self._conn = conn
        return self._conn

    def reset(self) -> None:
        with self._lock:
            conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def hard_close(self) -> None:
        """Cross-thread escape hatch used by the shutdown ladder."""
        self.maybe_cancel(force=True)
        self.reset()

    def maybe_cancel(self, force: bool = False) -> None:
        with self._lock:
            conn = self._conn
            due = force or (
                self._deadline is not None and time.monotonic() > self._deadline
            )
            if due:
                self._deadline = None
        if conn is None or not due:
            return
        try:
            conn.cancel_safe(timeout=5.0)
        except Exception:
            # Cancellation itself needs a working network path. Cutting the
            # connection is the fallback that always unblocks the owner.
            try:
                conn.close()
            except Exception:
                pass

    def _bounded(self, sql: str, params: Any, timeout: float | None) -> list[tuple]:
        """Execute with the watchdog armed. Raises psycopg errors verbatim."""
        conn = self._connect()
        with self._lock:
            self._deadline = time.monotonic() + (timeout or self.default_timeout)
        try:
            cur = conn.execute(sql.encode(), params)
            return cur.fetchall() if cur.description is not None else []
        finally:
            with self._lock:
                self._deadline = None

    def query(
        self, sql: str, params: Any = None, timeout: float | None = None
    ) -> list[tuple]:
        """Run a read (or session command). Transient trouble drops the conn.

        Raises TransientError on connection loss, cancellation, or timeout,
        and UnexpectedQueryError on any other server error.
        """
        try:
            return self._bounded(sql, params, timeout)
        except Exception as e:
            self.reset()
            if isinstance(e, TransientError):
                raise
            if isinstance(e, QueryCanceled) or _is_connection_error(e):
                raise TransientError(f"{type(e).__name__}: {e}") from e
            if any(snippet in str(e) for snippet in CONCURRENT_DDL_ERROR_SNIPPETS):
                raise TransientError(f"{type(e).__name__}: {e}") from e
            raise UnexpectedQueryError(f"query failed: {sql[:200]}: {e}") from e

    def write(
        self, sql: str, params: Any = None, timeout: float | None = None
    ) -> Outcome:
        """Run one autocommit write statement and classify its outcome."""
        try:
            self._bounded(sql, params, timeout)
            return Outcome.COMMITTED
        except TransientError:
            self.reset()
            return Outcome.UNKNOWN
        except Exception as e:
            self.reset()
            if isinstance(e, QueryCanceled) or "canceling statement due to" in str(e):
                return Outcome.UNKNOWN
            if _is_connection_error(e):
                return Outcome.UNKNOWN
            # A definite server error reply: the statement was not applied.
            # We still surface it, since none of our fixed-shape writes have
            # a legitimate reason to be rejected.
            raise UnexpectedQueryError(f"write rejected: {sql[:200]}: {e}") from e

    def write_txn(
        self, statements: list[tuple[str, Any]], timeout: float | None = None
    ) -> Outcome:
        """Run BEGIN; <statements>; COMMIT and classify the outcome.

        FAILED is only sound before COMMIT is submitted: Materialize buffers
        INSERT-only transactions until commit, so any earlier failure means
        no effect. Once COMMIT is on the wire, any non-success is UNKNOWN.
        """
        try:
            self._bounded("BEGIN", None, timeout)
            for sql, params in statements:
                self._bounded(sql, params, timeout)
        except Exception as e:
            try:
                self._bounded("ROLLBACK", None, 10.0)
            except Exception:
                self.reset()
            if isinstance(e, TransientError | QueryCanceled) or _is_connection_error(e):
                return Outcome.FAILED
            self.reset()
            raise UnexpectedQueryError(f"txn statement rejected: {e}") from e
        try:
            self._bounded("COMMIT", None, timeout)
            return Outcome.COMMITTED
        except Exception:
            self.reset()
            return Outcome.UNKNOWN
