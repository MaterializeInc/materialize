# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""SQL sessions that never block a rule.

Each session owns one connection and one thread. A rule hands an operation to
an idle session and waits for its outcome only up to a short pacing timeout.
An operation that outlasts it stays on the session, and its completion
callback records the outcome in the model whenever it arrives. A session
stuck behind a paused or partitioned environmentd therefore stalls only
itself, never a round of the state machine.

Outcomes carry the real-time interval a checker needs. `start` is taken
after connecting and applying session settings, right before the
operation's first statement, so it is never later than the server receiving
it. `end` is taken after the last response, so it is never earlier than the
server sending it. Checks built on these bounds stay sound under any network
delay.
"""

from __future__ import annotations

import queue
import re
import threading
import time
import traceback
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, NoReturn, TypeVar

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    unreachable,
)

from materialize.antithesis import sql
from materialize.antithesis.endpoints import INTERNAL_SQL_PORT, SQL_PORT

T = TypeVar("T")

# libpq gives up on a connection whose sent data stays unacknowledged this
# long. A paused or partitioned environmentd acknowledges nothing, so without
# it a statement waits for the kernel's retransmission limit, many minutes.
TCP_USER_TIMEOUT_MS = 20_000
CONNECT_TIMEOUT_SECONDS = 10
CONNECT_BACKOFF_SECONDS = 1.0
CANCEL_AFTER_SECONDS = 60.0
"""A statement running this long is cancelled. `statement_timeout` does not
cover reads, which wait for as long as their inputs lag or their replica is
down."""

# Internal errors after which a write may or may not have committed: the
# coordinator or its group committer shut down with the write in flight
# (`coord.rs`, `frontend_read_then_write.rs`). `sql.classify` reports them as
# violations.
_UNKNOWN_OUTCOME_TEMPLATES = re.compile(
    r"^(statement execution abandoned, outcome unknown"
    r"|write outcome is indeterminate because the group committer shut down)"
)
# A read on a cluster that has no replica fails at once rather than waiting
# (`peek_sequencing.rs`). The driver changes replication factors, so a
# correct system returns this.
_NO_REPLICA_TEMPLATE = re.compile(
    r"^CLUSTER \S+ has no replicas available to service request"
)


def classify(error: BaseException) -> sql.Classified:
    """`sql.classify` with the errors above."""
    if isinstance(error, RolledBack):
        return sql.Classified(sql.Outcome.REJECTED, None, "COMMIT answered ROLLBACK")
    c = sql.classify(error)
    if c.sqlstate == "XX000" and _UNKNOWN_OUTCOME_TEMPLATES.search(c.template):
        return sql.Classified(sql.Outcome.INDETERMINATE, c.sqlstate, c.template)
    if c.outcome == sql.Outcome.VIOLATION and _NO_REPLICA_TEMPLATE.search(c.template):
        return sql.Classified(sql.Outcome.REJECTED, c.sqlstate, c.template)
    return c


NOT_SENT = sql.Classified(sql.Outcome.REJECTED, None, "not sent: the session was busy")


class RolledBack(Exception):
    """A `COMMIT` that the server answered with `ROLLBACK`."""


def commit(conn: psycopg.Connection) -> None:
    """End an explicit transaction, raising `RolledBack` unless it committed.

    The command tag is the acknowledgement: a `COMMIT` of a transaction the
    server already failed answers `ROLLBACK` without an error.
    """
    cursor = conn.execute(b"COMMIT")
    if cursor.statusmessage != "COMMIT":
        raise RolledBack(cursor.statusmessage)


class Abandoned(Exception):
    """The driver closed the connection without reading a statement's response."""


ABANDON_SHARE = 0.02
"""How many of a component's writes the driver abandons."""
ABANDON_LINGER_SECONDS = (0.0, 0.01, 0.2)
"""How long an abandoned connection stays open. The longest lets most
statements finish, the shortest closes while the server still reads."""
ABANDONED = sql.Classified(
    sql.Outcome.INDETERMINATE, None, "abandoned: the driver closed the connection"
)


def abandon(conn: psycopg.Connection, statement: bytes, linger: float) -> NoReturn:
    """Send `statement`, then close the connection `linger` seconds later
    without reading the response, as a client that dies mid-statement does.

    The server may or may not run the statement to completion, so its outcome
    is indeterminate. Faults lose a response this way only when they land in
    the few milliseconds a statement is in flight.
    """
    conn.pgconn.send_query(statement)
    # The connection is non-blocking: 1 means the kernel took only a part.
    while conn.pgconn.flush() == 1:
        time.sleep(0.001)
    time.sleep(linger)
    conn.close()
    raise Abandoned()


class Status(Enum):
    NOT_SENT = "not_sent"
    """The session was busy or gone. Nothing was sent."""
    DONE = "done"
    """The outcome arrived within the rule's wait and has been recorded."""
    PENDING = "pending"
    """The operation is still running. Its outcome is recorded when it arrives."""


@dataclass(frozen=True)
class Outcome(Generic[T]):
    value: T | None
    error: sql.Classified | None
    start: float
    end: float
    session: str
    """Identifies the SQL session: a reconnect starts a new one."""

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def rejected(self) -> bool:
        """The operation definitely did not take effect."""
        return self.error is not None and self.error.outcome == sql.Outcome.REJECTED

    @property
    def unknown(self) -> bool:
        """The operation may or may not have taken effect, now or later."""
        return (
            self.error is not None and self.error.outcome == sql.Outcome.INDETERMINATE
        )


@dataclass
class Op(Generic[T]):
    """One request: statements to run and what to record once they finish."""

    name: str
    execute: Callable[[psycopg.Connection], T]
    complete: Callable[[Outcome[T]], None]
    settings: dict[str, str] = field(default_factory=dict)
    """Session variables the operation needs, applied with `SET` when the
    session's current value differs."""
    classify: Callable[[BaseException], sql.Classified] = classify
    """Widens `classify` for operations that may see more errors, such as
    reads `AS OF` an explicit timestamp."""
    ddl: bool = False
    """Whether it changes the catalog, see `Pool.on_ddl`."""


@dataclass(frozen=True)
class Target:
    host: str
    internal: bool = False
    """Connect as `mz_system` on the internal port."""


@dataclass(frozen=True)
class Upstream:
    """A Postgres server outside Materialize, such as a source's upstream."""

    url: str


def connect(target: Target | Upstream, application_name: str) -> psycopg.Connection:
    if isinstance(target, Upstream):
        return psycopg.connect(
            target.url,
            autocommit=True,
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            tcp_user_timeout=TCP_USER_TIMEOUT_MS,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=3,
            application_name=application_name,
        )
    options = "-c cluster=mz_catalog_server" if target.internal else None
    return psycopg.connect(
        host=target.host,
        port=INTERNAL_SQL_PORT if target.internal else SQL_PORT,
        user="mz_system" if target.internal else "materialize",
        dbname="materialize",
        autocommit=True,
        connect_timeout=CONNECT_TIMEOUT_SECONDS,
        tcp_user_timeout=TCP_USER_TIMEOUT_MS,
        keepalives=1,
        keepalives_idle=5,
        keepalives_interval=2,
        keepalives_count=3,
        application_name=application_name,
        options=options,
    )


def report_driver_error(where: str, error: BaseException) -> None:
    unreachable(
        "the driver raises no unexpected exception",
        {
            "where": where,
            "error": f"{type(error).__name__}: {error}",
            "traceback": traceback.format_exc(limit=8),
        },
    )


class _Job:
    def __init__(self, op: Op) -> None:
        self.op = op
        self.done = threading.Event()


class Session:
    """One connection, used by one operation at a time on its own thread."""

    def __init__(
        self,
        name: str,
        target: Target | Upstream,
        statement_timeout_ms: int,
        lock: threading.Lock,
    ) -> None:
        self.name = name
        self.target = target
        self.statement_timeout_ms = statement_timeout_ms
        self._lock = lock
        self.busy_since: float | None = None
        """Guarded by the pool's lock."""
        self._conn: psycopg.Connection | None = None
        self._settings: dict[str, str] = {}
        self._jobs: queue.SimpleQueue[_Job] = queue.SimpleQueue()
        self.connections = 0
        """How many connections this session opened, for exploration assertions."""
        self.cancels = 0
        """How many cancel requests `cancel` sent."""
        self.before_ddl: Callable[[str], None] | None = None
        threading.Thread(target=self._loop, name=f"session-{name}", daemon=True).start()

    def submit(self, job: _Job) -> None:
        self._jobs.put(job)

    def cancel(self) -> None:
        """Ask the server to cancel the running statement. Thread safe."""
        conn = self._conn
        if conn is not None and not conn.closed:
            self.cancels += 1
            try:
                conn.cancel_safe(timeout=CONNECT_TIMEOUT_SECONDS)
            except (psycopg.Error, OSError):
                pass

    def _connection(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = connect(self.target, f"driver-{self.name}")
            self._settings = {}
            self.connections += 1
            self._conn.execute(
                f"SET statement_timeout = '{self.statement_timeout_ms}ms'".encode()
            )
        return self._conn

    def _apply(self, conn: psycopg.Connection, settings: dict[str, str]) -> None:
        # A setting an earlier operation needed must not leak into this one:
        # `real_time_recency` left on makes every later read of a source
        # contact its upstream.
        for name in [n for n in self._settings if n not in settings]:
            conn.execute(f"RESET {name}".encode())
            del self._settings[name]
        for name, value in settings.items():
            if self._settings.get(name) != value:
                conn.execute(f"SET {name} = {value}".encode())
                self._settings[name] = value

    def _discard(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except psycopg.Error:
                pass

    def _ident(self) -> str:
        return f"{self.name}.{self.connections}"

    def _run(self, op: Op) -> Outcome:
        start = time.monotonic()
        try:
            conn = self._connection()
            self._apply(conn, op.settings)
        except (psycopg.Error, OSError) as e:
            self._discard()
            classified = classify(e)
            # While environmentd is unreachable, every rule would reconnect at
            # once and as fast as it can. Pause this session instead.
            time.sleep(CONNECT_BACKOFF_SECONDS)
            if classified.outcome != sql.Outcome.VIOLATION:
                # None of the operation's statements were sent.
                classified = sql.Classified(
                    sql.Outcome.REJECTED, classified.sqlstate, classified.template
                )
            return Outcome(None, classified, start, time.monotonic(), self._ident())
        try:
            if op.ddl and self.before_ddl is not None:
                self.before_ddl(op.name)
            start = time.monotonic()
            value = op.execute(conn)
            return Outcome(value, None, start, time.monotonic(), self._ident())
        except (psycopg.Error, OSError, RolledBack, Abandoned) as e:
            classified = ABANDONED if isinstance(e, Abandoned) else op.classify(e)
            end = time.monotonic()
            conn = self._conn
            if conn is None or conn.broken or conn.closed:
                self._discard()
            elif conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
                try:
                    conn.rollback()
                except psycopg.Error:
                    self._discard()
            return Outcome(None, classified, start, end, self._ident())

    def _loop(self) -> None:
        while True:
            job = self._jobs.get()
            try:
                outcome = self._run(job.op)
                error = outcome.error
                if error is not None and error.outcome == sql.Outcome.VIOLATION:
                    unreachable(
                        "SQL statements fail only with errors a correct system returns",
                        {
                            "op": job.op.name,
                            "sqlstate": error.sqlstate,
                            "error": error.template,
                        },
                    )
                job.op.complete(outcome)
            except Exception as e:
                report_driver_error(job.op.name, e)
            finally:
                with self._lock:
                    self.busy_since = None
                job.done.set()


class Pool:
    """A fixed set of sessions that rules address by index."""

    def __init__(
        self,
        name: str,
        target: Target | Upstream,
        size: int,
        statement_timeout_ms: int = 30_000,
    ) -> None:
        self._lock = threading.Lock()
        self.sessions = [
            Session(f"{name}{i}", target, statement_timeout_ms, self._lock)
            for i in range(size)
        ]
        threading.Thread(
            target=self._watchdog, name=f"{name}-watchdog", daemon=True
        ).start()

    def _watchdog(self) -> None:
        cancelled: dict[str, float] = {}
        while True:
            time.sleep(5)
            with self._lock:
                busy = [
                    (s, s.busy_since) for s in self.sessions if s.busy_since is not None
                ]
            for session, since in busy:
                assert since is not None
                if (
                    time.monotonic() - since > CANCEL_AFTER_SECONDS
                    and cancelled.get(session.name) != since
                ):
                    cancelled[session.name] = since
                    session.cancel()

    def call(self, index: int, op: Op, wait: float) -> Status:
        """Run `op` on session `index` if it is idle, waiting up to `wait` seconds.

        `op.complete` runs exactly once: on the session's thread with the
        outcome, or right here, as rejected, if the session was busy.
        """
        session = self.sessions[index % len(self.sessions)]
        with self._lock:
            busy = session.busy_since is not None
            if not busy:
                session.busy_since = time.monotonic()
        if busy:
            # Complete it as not applied, so the caller's model resolves it
            # like any other rejected operation.
            now = time.monotonic()
            try:
                op.complete(Outcome(None, NOT_SENT, now, now, session.name))
            except Exception as e:
                report_driver_error(op.name, e)
            return Status.NOT_SENT
        job = _Job(op)
        session.submit(job)
        return Status.DONE if job.done.wait(wait) else Status.PENDING

    def on_ddl(self, hook: Callable[[str], None]) -> None:
        """Call `hook` with the operation's name on the session's thread,
        right before the statements of an operation that changes the catalog
        are sent."""
        for session in self.sessions:
            session.before_ddl = hook

    def busy(self) -> list[tuple[str, float]]:
        """Each busy session and how long its current operation has run."""
        now = time.monotonic()
        with self._lock:
            return [
                (s.name, now - s.busy_since)
                for s in self.sessions
                if s.busy_since is not None
            ]

    def wait_idle(self, deadline: float) -> list[tuple[str, float]]:
        """Wait until every session is idle or `deadline` passes. Returns the busy ones."""
        while True:
            busy = self.busy()
            if not busy or time.monotonic() >= deadline:
                return busy
            time.sleep(0.5)


class Worker:
    """One background thread for work that a rule must not wait on, such as
    an upstream's single session or a broker client."""

    def __init__(self, name: str) -> None:
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._busy: Future[None] | None = None
        self._lock = threading.Lock()
        self.name = name

    def run(self, action: Callable[[], None], wait: float) -> Status:
        """Run `action` unless the previous one still runs, and wait up to
        `wait` seconds for it."""
        with self._lock:
            if self._busy is not None and not self._busy.done():
                return Status.NOT_SENT

            def guarded() -> None:
                try:
                    action()
                except Exception as e:
                    report_driver_error(self.name, e)

            future = self._pool.submit(guarded)
            self._busy = future
        try:
            future.result(timeout=wait)
            return Status.DONE
        except TimeoutError:
            return Status.PENDING
