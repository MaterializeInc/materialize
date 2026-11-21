# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Cancel requests and statement timeouts that land while a statement runs.

A statement answered with a cancel or timeout error took no effect: the
adapter answers so only while the write is known not to be durable, and waits
for the definitive result otherwise (`WriteAttemptState` in
`frontend_read_then_write.rs`). The race is between the cancel and the commit,
so each operation plans one cancel a drawn delay after it sent a statement:
a cancel request on the wire, `pg_cancel_backend` from another connection, or
a `statement_timeout`.

Writes are batches of `ROWS` rows with a generation that updates increment,
so a read judges every batch on its own: whole or absent, at one generation
within the updates acknowledged and attempted. Indexes and materialized views
on the table are created and dropped under the same cancels, and a plain
`DROP` afterwards tells whether a cancelled statement took effect.
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import sql
from materialize.antithesis_driver.client import (
    CONNECT_TIMEOUT_SECONDS,
    Op,
    Outcome,
    RolledBack,
    Status,
    Target,
    classify,
    commit,
    connect,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

T = TypeVar("T")

SCHEMA = "cancel"
ROWS = 8
LIVE_BATCHES = 24
"""Acknowledged batches kept before the oldest are deleted."""
LIVE_OBJECTS = 2
"""Indexes and materialized views kept before the oldest are dropped."""
DELAYS = (0.0, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3)
"""Seconds from sending a statement to requesting its cancel."""
TIMEOUTS_MS = (1, 3, 10, 30, 100)
SUBSCRIBE_SECONDS = (0.05, 0.3, 1.0)
CANCEL_WINDOW_SECONDS = 120.0
"""How long after it was sent a cancel request may still reach its session,
where it then cancels a later statement."""
FINAL_SECONDS = 120.0
"""The least time the checks after faults stop get, whatever earlier checks
left of the shared deadline."""
CLUSTERS = ("quickstart", "driver")
SETTINGS = {"transaction_isolation": "'strict serializable'"}
_CANCELLED = re.compile(r"^canceling statement due to (user request|statement timeout)")
_REQUESTED = re.compile(r"^canceling statement due to user request")


def setup_statements() -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t (op bigint, i int, gen int)",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.digits (i int)",
        f"INSERT INTO {SCHEMA}.digits SELECT generate_series(0, {ROWS - 1})"
        f" WHERE NOT EXISTS (SELECT FROM {SCHEMA}.digits)",
    ]


def cancelled(error: sql.Classified | None) -> bool:
    """Whether a statement was answered as cancelled or timed out."""
    return (
        error is not None
        and error.sqlstate == "57014"
        and bool(_CANCELLED.search(error.template))
    )


def requested(error: sql.Classified | None) -> bool:
    """Whether a statement was answered as cancelled by a cancel request."""
    return (
        error is not None
        and error.sqlstate == "57014"
        and bool(_REQUESTED.search(error.template))
    )


@dataclass(frozen=True)
class Plan:
    mode: str
    """`none`, `protocol`, `sql` or `timeout`."""
    delay: float
    """Seconds after the statement is sent, or the timeout for `timeout`."""


@dataclass
class Result(Generic[T]):
    value: T | None
    error: sql.Classified | None
    """The statement's error, if the session survived it."""
    observed: int
    """Statements, probes included, answered as cancelled by a request."""
    seconds: float
    follow_up: str | None = None
    """What a plain `DROP` after a cancelled DDL statement found: `dropped`,
    `missing`, or None when it gave no answer."""


class Canceller:
    """Delivers a plan's cancel to a connection while a statement runs on it."""

    def __init__(
        self, host: str, conn: psycopg.Connection, plan: Plan, attempts: list[float]
    ) -> None:
        self.host = host
        self.conn = conn
        self.plan = plan
        self.attempts = attempts
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._finished = threading.Event()
        self._finished.set()
        self._done = False

    def arm(self) -> None:
        """Call right before sending the statement the cancel is meant for."""
        if self.plan.mode not in ("protocol", "sql") or self._timer is not None:
            return
        self._timer = threading.Timer(self.plan.delay, self._deliver)
        self._timer.daemon = True
        self._timer.start()

    def _deliver(self) -> None:
        with self._lock:
            if self._done:
                return
            self.attempts.append(time.monotonic())
            self._finished.clear()
        try:
            if self.plan.mode == "protocol":
                self.conn.cancel_safe(timeout=CONNECT_TIMEOUT_SECONDS)
            else:
                pid = self.conn.info.backend_pid
                with connect(Target(self.host), "driver-cancel") as other:
                    other.execute(f"SELECT pg_cancel_backend({pid})".encode())
        except (psycopg.Error, OSError):
            # Counted as an attempt all the same: a request whose response was
            # lost may have arrived.
            pass
        finally:
            self._finished.set()

    def finish(self) -> None:
        """Stop a cancel that was not sent yet and wait for one that is on
        its way, so it does not reach a later operation of this session from
        the driver's side. The server still handles it asynchronously."""
        with self._lock:
            self._done = True
        if self._timer is not None:
            self._timer.cancel()
        self._finished.wait(CONNECT_TIMEOUT_SECONDS + 5)


def run(
    host: str,
    conn: psycopg.Connection,
    plan: Plan,
    restore_timeout_ms: int,
    body: Callable[[psycopg.Connection, Canceller], T],
    attempts: list[float],
    follow_up: bytes | None = None,
) -> Result[T]:
    """Run `body` under `plan`, then check that the session still answers.

    Returns the statement's error when the session survived it. An error that
    broke the connection, or one no correct system returns, is raised for the
    session to classify. `attempts` receives one entry per cancel request the
    operation tried to deliver, also when it raises.
    """
    canceller = Canceller(host, conn, plan, attempts)
    start = time.monotonic()
    value: T | None = None
    error: sql.Classified | None = None
    try:
        if plan.mode == "timeout":
            ms = max(1, round(plan.delay * 1000))
            conn.execute(f"SET statement_timeout = '{ms}ms'".encode())
        try:
            value = body(conn, canceller)
        except (psycopg.Error, RolledBack) as e:
            error = classify(e)
            if error.outcome == sql.Outcome.VIOLATION or conn.broken or conn.closed:
                raise
    finally:
        canceller.finish()
        if not (conn.broken or conn.closed):
            _reset(conn, restore_timeout_ms if plan.mode == "timeout" else None)
    seconds = time.monotonic() - start
    observed = 1 if requested(error) else 0
    if plan.mode != "none":
        for _ in range(2):
            probe = _probe(conn)
            if probe != "cancelled":
                break
            observed += 1
    found = None
    if follow_up is not None and cancelled(error):
        found = _follow_up(conn, follow_up)
    return Result(value, error, observed, seconds, found)


def _reset(conn: psycopg.Connection, timeout_ms: int | None) -> None:
    """Leave the session idle and, if `timeout_ms` is given, with that
    `statement_timeout` again.

    A session left with a timeout of milliseconds fails every later statement
    of every component, so one that cannot be reset is closed.
    """
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.rollback()
    if timeout_ms is None:
        return
    for _ in range(3):
        try:
            conn.execute(f"SET statement_timeout = '{timeout_ms}ms'".encode())
            return
        except psycopg.Error:
            if conn.broken or conn.closed:
                raise
    conn.close()
    raise psycopg.OperationalError("could not restore statement_timeout")


def _probe(conn: psycopg.Connection) -> str:
    try:
        conn.execute(b"SELECT 1")
        return "ok"
    except psycopg.Error as e:
        c = classify(e)
        if requested(c) and not (conn.broken or conn.closed):
            return "cancelled"
        raise


def _follow_up(conn: psycopg.Connection, drop: bytes) -> str | None:
    try:
        conn.execute(drop)
        return "dropped"
    except psycopg.Error as e:
        c = classify(e)
        if c.race == sql.CatalogRace.MISSING:
            return "missing"
        if c.outcome == sql.Outcome.VIOLATION or conn.broken or conn.closed:
            raise
        return None


@dataclass
class Batch:
    insert: str = "pending"
    """`pending`, `ok`, `rejected` or `indeterminate`."""
    insert_cancelled: bool = False
    acked_at: float | None = None
    seen: bool = False
    updates_acked: list[float] = field(default_factory=list)
    updates_open: int = 0
    """Updates sent and not known to be rejected."""
    max_gen_seen: int = 0
    delete: str = "none"
    """`none`, `pending`, `ok` or `indeterminate`."""
    delete_cancelled: bool = False
    """A delete of this batch was answered as cancelled."""
    deleted_at: float | None = None
    missing_seen: bool = False
    """A read that began after the insert was acknowledged saw no row while a
    delete was unresolved."""


def judge(
    rows: list[tuple[int, int, int]], batches: dict[int, Batch], start: float
) -> dict[str, list[dict[str, Any]]]:
    """What a read that began at `start` and returned `rows` gets wrong.

    Also records in `batches` what the read saw, for the statements that
    resolve after it.
    """
    errors: dict[str, list[dict[str, Any]]] = {
        "torn": [],
        "presence": [],
        "generation": [],
        "cancelled": [],
        "rejected": [],
    }
    by_op: dict[int, list[tuple[int, int]]] = {}
    for op, i, gen in rows:
        by_op.setdefault(op, []).append((i, gen))
    for op, items in sorted(by_op.items()):
        b = batches.get(op)
        gens = {gen for _, gen in items}
        whole = sorted(i for i, _ in items) == list(range(ROWS)) and len(gens) == 1
        if b is None or not whole:
            errors["torn"].append({"op": op, "rows": sorted(items), "known": bool(b)})
            continue
        gen = gens.pop()
        b.seen = True
        b.max_gen_seen = max(b.max_gen_seen, gen)
        if b.insert == "rejected":
            kind = "cancelled" if b.insert_cancelled else "rejected"
            errors[kind].append({"op": op, "statement": "insert"})
        lo = sum(1 for at in b.updates_acked if at < start)
        hi = len(b.updates_acked) + b.updates_open
        if not lo <= gen <= hi:
            errors["generation"].append({"op": op, "gen": gen, "lo": lo, "hi": hi})
        if b.delete == "ok" and b.deleted_at is not None and b.deleted_at < start:
            errors["presence"].append({"op": op, "visible": "after its delete"})
    for op, b in batches.items():
        if op in by_op or b.insert != "ok":
            continue
        if b.acked_at is None or b.acked_at >= start:
            continue
        if b.delete == "none":
            kind = "cancelled" if b.delete_cancelled else "presence"
            errors[kind].append({"op": op, "statement": "delete", "visible": "no"})
        elif b.delete == "pending":
            b.missing_seen = True
    return errors


@dataclass
class Obj:
    kind: str
    """`INDEX` or `MATERIALIZED VIEW`."""
    cluster: str
    state: str = "pending"
    """`pending`, `live`, `dropping` or `unknown`."""


class Cancel:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next = 1
        self.batches: dict[int, Batch] = {}
        self.objects: dict[int, Obj] = {}
        self._reads: dict[int, float] = {}
        """Reads in flight, each with a time no later than its start."""
        self._next_read = 0
        self._requests: dict[int, list[float]] = {}
        """Per session index: when each cancel request was sent that no
        statement has been seen cancelled by yet, the pool's included."""
        self._sql_requests: list[float] = []
        """The same for `pg_cancel_backend`. It names a connection id, which
        after a restart belongs to another connection, so such a request can
        reach any session."""
        self._pool_cancels: dict[int, int] = {}
        self.cancel_share = 0.7
        self.modes: tuple[str, ...] = ("protocol", "sql", "timeout")

    def configure(self, d: Draw) -> None:
        self.cancel_share = d.choice((0.4, 0.7, 0.95))
        self.modes = d.choice(
            (
                ("protocol", "sql", "timeout"),
                ("protocol",),
                ("sql",),
                ("timeout",),
            )
        )

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def _plan(self, d: Draw, modes: tuple[str, ...] | None = None) -> Plan:
        if not d.boolean(self.cancel_share):
            return Plan("none", 0.0)
        mode = d.choice(modes or self.modes)
        if mode == "timeout":
            return Plan(mode, d.choice(TIMEOUTS_MS) / 1000)
        return Plan(mode, d.choice(DELAYS))

    def _run(
        self,
        index: int,
        conn: psycopg.Connection,
        plan: Plan,
        body: Callable[[psycopg.Connection, Canceller], T],
        attempts: list[float],
        follow_up: bytes | None = None,
    ) -> Result[T]:
        session = self.ctx.sessions.sessions[index % len(self.ctx.sessions.sessions)]
        return run(
            self.ctx.config.host,
            conn,
            plan,
            session.statement_timeout_ms,
            body,
            attempts,
            follow_up,
        )

    def _settle(
        self,
        index: int,
        plan: Plan,
        attempts: list[float],
        outcome: Outcome[Result[T]],
    ) -> sql.Classified | None:
        """Account an operation's cancels and return its statement's error."""
        session = self.ctx.sessions.sessions[index % len(self.ctx.sessions.sessions)]
        result = outcome.value if outcome.ok else None
        seen = result.observed if result else 0
        now = time.monotonic()
        with self._lock:
            own = self._requests.setdefault(index, [])
            pooled = session.cancels - self._pool_cancels.get(index, 0)
            self._pool_cancels[index] = session.cancels
            own.extend([now] * pooled)
            (self._sql_requests if plan.mode == "sql" else own).extend(attempts)
            for requests in (own, self._sql_requests):
                requests[:] = [t for t in requests if now - t <= CANCEL_WINDOW_SECONDS]
            available = len(own) + len(self._sql_requests)
            # Each cancelled statement used up one request, the oldest first.
            taken = min(seen, len(own))
            del own[:taken]
            del self._sql_requests[: seen - taken]
        if result is None:
            return outcome.error
        always(
            seen <= available,
            "cancel: a session sees no more cancelled statements than cancel requests that can reach it",
            {"session": session.name, "requests": available, "seen": seen},
        )
        if result.error is None and attempts:
            reachable(
                "cancel: a statement finished although its cancel was sent",
                {"mode": plan.mode},
            )
        return result.error

    def _no_effect(self, holds: bool, details: dict[str, Any]) -> None:
        always(
            holds,
            "cancel: a statement answered as cancelled or timed out took no effect",
            details,
        )

    def _rejected_no_effect(self, holds: bool, details: dict[str, Any]) -> None:
        always(
            holds,
            "cancel: a rejected statement took no effect",
            details,
        )

    def write(self, d: Draw) -> Status:
        """Insert one batch, by one of four statement kinds."""
        kind = d.choice(("values", "select", "txn", "copy"))
        plan = self._plan(d)
        target = d.integer(0, 2)
        index = self._session(d)
        batch = Batch()
        with self._lock:
            op = self._next
            self._next += 1
            self.batches[op] = batch
        sent_commit: list[bool] = []
        attempts: list[float] = []

        def values(lo: int, hi: int) -> bytes:
            rows = ", ".join(f"({op}, {i}, 0)" for i in range(lo, hi))
            return f"INSERT INTO {SCHEMA}.t VALUES {rows}".encode()

        def body(conn: psycopg.Connection, canceller: Canceller) -> None:
            if kind == "values":
                canceller.arm()
                conn.execute(values(0, ROWS))
            elif kind == "select":
                canceller.arm()
                conn.execute(
                    f"INSERT INTO {SCHEMA}.t SELECT {op}, i, 0 FROM {SCHEMA}.digits".encode()
                )
            elif kind == "txn":
                conn.execute(b"BEGIN")
                for n, statement in enumerate(
                    (values(0, ROWS // 2), values(ROWS // 2, ROWS))
                ):
                    if n == target:
                        canceller.arm()
                    conn.execute(statement)
                if target == 2:
                    canceller.arm()
                sent_commit.append(True)
                commit(conn)
            else:
                with conn.cursor() as cursor:
                    canceller.arm()
                    with cursor.copy(f"COPY {SCHEMA}.t FROM STDIN".encode()) as copy:
                        for i in range(ROWS):
                            # One row per write, sent at once, so the cancel
                            # lands between rows.
                            copy.write(f"{op}\t{i}\t0\n".encode())
                            time.sleep(plan.delay / ROWS)

        def execute(conn: psycopg.Connection) -> Result[None]:
            return self._run(index, conn, plan, body, attempts)

        def complete(outcome: Outcome[Result[None]]) -> None:
            error = self._settle(index, plan, attempts, outcome)
            was_cancelled = cancelled(error)
            with self._lock:
                if error is None:
                    batch.insert, batch.acked_at = "ok", outcome.end
                elif (
                    was_cancelled
                    or error.outcome == sql.Outcome.REJECTED
                    or (kind == "txn" and not sent_commit)
                ):
                    # A transaction that never sent COMMIT never commits.
                    batch.insert = "rejected"
                    batch.insert_cancelled = was_cancelled
                else:
                    batch.insert = "indeterminate"
                seen = batch.seen
            if batch.insert == "rejected":
                details = {"op": op, "kind": kind, "mode": plan.mode, "seen": seen}
                if was_cancelled:
                    self._no_effect(not seen, details)
                else:
                    self._rejected_no_effect(not seen, details)
            if was_cancelled and kind == "values":
                reachable("cancel: a blind insert was answered as cancelled", {})
            elif was_cancelled and kind == "select":
                reachable("cancel: an INSERT ... SELECT was answered as cancelled", {})
            elif was_cancelled and kind == "txn" and sent_commit:
                reachable("cancel: a COMMIT was answered as cancelled", {})
            elif was_cancelled and kind == "copy":
                reachable("cancel: a COPY FROM STDIN was answered as cancelled", {})
            if was_cancelled and plan.mode == "timeout":
                reachable("cancel: a statement timeout answered a write", {})
            elif was_cancelled and plan.mode == "sql":
                reachable("cancel: pg_cancel_backend cancelled a write", {})

        return self.ctx.sessions.call(
            index, Op("cancel write", execute, complete, SETTINGS), RULE_WAIT_SECONDS
        )

    def change(self, d: Draw) -> Status:
        """Update or delete an acknowledged batch."""
        with self._lock:
            live = [
                op
                for op, b in self.batches.items()
                if b.insert == "ok" and b.delete == "none"
            ]
            if not live:
                return Status.NOT_SENT
            delete = len(live) > LIVE_BATCHES or d.boolean(0.1)
            op = live[0] if delete else d.choice(live)
            batch = self.batches[op]
            if delete:
                batch.delete = "pending"
            else:
                batch.updates_open += 1
        plan = self._plan(d)
        index = self._session(d)
        statement = (
            f"DELETE FROM {SCHEMA}.t WHERE op = {op}"
            if delete
            else f"UPDATE {SCHEMA}.t SET gen = gen + 1 WHERE op = {op}"
        ).encode()

        attempts: list[float] = []

        def body(conn: psycopg.Connection, canceller: Canceller) -> int:
            canceller.arm()
            return conn.execute(statement).rowcount

        def execute(conn: psycopg.Connection) -> Result[int]:
            return self._run(index, conn, plan, body, attempts)

        def complete(outcome: Outcome[Result[int]]) -> None:
            error = self._settle(index, plan, attempts, outcome)
            was_cancelled = cancelled(error)
            definite = was_cancelled or (
                error is not None and error.outcome == sql.Outcome.REJECTED
            )
            details: dict[str, Any] = {
                "op": op,
                "statement": "delete" if delete else "update",
                "mode": plan.mode,
            }
            count = outcome.value.value if outcome.ok and outcome.value else None
            with self._lock:
                deleting = batch.delete != "none"
                if delete:
                    if error is None:
                        batch.delete, batch.deleted_at = "ok", outcome.end
                    elif definite:
                        batch.delete = "none"
                        batch.delete_cancelled |= was_cancelled
                    else:
                        batch.delete = "indeterminate"
                    took_effect = definite and batch.missing_seen
                else:
                    if error is None and count == ROWS:
                        batch.updates_open -= 1
                        batch.updates_acked.append(outcome.end)
                    elif error is None or definite:
                        # An update that found no row ran after a delete.
                        batch.updates_open -= 1
                    hi = len(batch.updates_acked) + batch.updates_open
                    took_effect = definite and batch.max_gen_seen > hi
                self._retire()
            if definite and was_cancelled:
                self._no_effect(not took_effect, details)
            elif definite:
                self._rejected_no_effect(not took_effect, details)
            if error is None and (delete or not deleting):
                always(
                    count == ROWS,
                    "cancel: an acknowledged update or delete changed every row of its batch",
                    {**details, "rows": count},
                )
            if was_cancelled:
                reachable("cancel: an UPDATE or DELETE was answered as cancelled", {})
            if was_cancelled and plan.mode == "timeout":
                reachable("cancel: a statement timeout answered a read-then-write", {})

        return self.ctx.sessions.call(
            index, Op("cancel change", execute, complete, SETTINGS), RULE_WAIT_SECONDS
        )

    def _retire(self) -> None:
        """Forget deleted batches that no read in flight may still see.
        Call with the lock held."""
        oldest = min(self._reads.values(), default=float("inf"))
        for op in [
            op
            for op, b in self.batches.items()
            if b.delete == "ok" and b.deleted_at is not None and b.deleted_at < oldest
        ]:
            del self.batches[op]

    def _read_op(self, d: Draw | None, index: int, done: list[bool]) -> Op:
        plan = self._plan(d, ("protocol", "sql")) if d is not None else Plan("none", 0)
        in_transaction = d is not None and d.boolean(0.3)
        with self._lock:
            views = [
                f"{SCHEMA}.mv_{n}"
                for n, o in self.objects.items()
                if o.kind == "MATERIALIZED VIEW" and o.state == "live"
            ]
            token = self._next_read
            self._next_read += 1
            self._reads[token] = time.monotonic()
        relation = (
            d.choice(views) if d is not None and views and d.boolean(0.3) else None
        )
        settings = dict(SETTINGS)
        if d is not None:
            settings["cluster"] = d.choice(CLUSTERS)
        query = f"SELECT op, i, gen FROM {relation or f'{SCHEMA}.t'}".encode()
        attempts: list[float] = []

        def body(
            conn: psycopg.Connection, canceller: Canceller
        ) -> list[tuple[Any, ...]]:
            if not in_transaction:
                canceller.arm()
                return conn.execute(query).fetchall()
            conn.execute(b"BEGIN")
            rows = conn.execute(query).fetchall()
            # A strict serializable read transaction is linearized at COMMIT.
            canceller.arm()
            commit(conn)
            return rows

        def execute(conn: psycopg.Connection) -> Result[list[tuple[Any, ...]]]:
            return self._run(index, conn, plan, body, attempts)

        def complete(outcome: Outcome[Result[list[tuple[Any, ...]]]]) -> None:
            error = self._settle(index, plan, attempts, outcome)
            rows = outcome.value.value if outcome.ok and outcome.value else None
            with self._lock:
                errors = (
                    judge(
                        [(int(o), int(i), int(g)) for o, i, g in rows],
                        self.batches,
                        outcome.start,
                    )
                    if error is None and rows is not None
                    else None
                )
                del self._reads[token]
                self._retire()
            if cancelled(error):
                reachable("cancel: a read was answered as cancelled", {})
            if errors is None:
                return
            always(
                not errors["torn"],
                "cancel: a batch is visible whole or not at all, at one generation",
                {"errors": errors["torn"][:5], "relation": relation},
            )
            always(
                not errors["presence"],
                "cancel: a read shows every batch acknowledged before it began and none deleted before it began",
                {"errors": errors["presence"][:5], "relation": relation},
            )
            always(
                not errors["generation"],
                "cancel: a batch's generation is within the updates acknowledged and attempted",
                {"errors": errors["generation"][:5], "relation": relation},
            )
            self._no_effect(
                not errors["cancelled"],
                {"errors": errors["cancelled"][:5], "relation": relation},
            )
            self._rejected_no_effect(
                not errors["rejected"],
                {"errors": errors["rejected"][:5], "relation": relation},
            )
            sometimes(
                relation is not None,
                "cancel: a read through a materialized view was checked",
                {},
            )
            done.append(True)

        return Op("cancel read", execute, complete, settings)

    def read(self, d: Draw) -> Status:
        index = self._session(d)
        return self.ctx.sessions.call(
            index, self._read_op(d, index, []), RULE_WAIT_SECONDS
        )

    def _subscribe_op(
        self, index: int, variant: str, plan: Plan, done: list[bool]
    ) -> Op:
        attempts: list[float] = []

        def body(conn: psycopg.Connection, canceller: Canceller) -> None:
            if variant == "cursor":
                conn.execute(b"BEGIN")
                conn.execute(f"DECLARE c CURSOR FOR SUBSCRIBE {SCHEMA}.t".encode())
                canceller.arm()
                # Without a timeout the fetch returns only when the
                # subscription ends, which a subscription on a table never
                # does by itself.
                conn.execute(b"FETCH ALL c")
            else:
                with conn.cursor() as cursor:
                    canceller.arm()
                    with cursor.copy(
                        f"COPY (SUBSCRIBE {SCHEMA}.t) TO STDOUT".encode()
                    ) as copy:
                        for _ in copy:
                            pass

        def execute(conn: psycopg.Connection) -> Result[None]:
            return self._run(index, conn, plan, body, attempts)

        def complete(outcome: Outcome[Result[None]]) -> None:
            error = self._settle(index, plan, attempts, outcome)
            seconds = outcome.value.seconds if outcome.ok and outcome.value else None
            if requested(error):
                reachable("cancel: a cancel ended a subscription", {})
                sometimes(
                    seconds is not None and seconds < plan.delay + 2.0,
                    "cancel: a subscription ended within two seconds of its cancel",
                    {"seconds": seconds},
                )
                done.append(True)

        return Op("cancel subscribe", execute, complete, SETTINGS)

    def subscribe(self, d: Draw) -> Status:
        """An endless subscription that only a cancel ends."""
        index = self._session(d)
        plan = Plan(d.choice(("protocol", "sql")), d.choice(SUBSCRIBE_SECONDS))
        variant = d.choice(("cursor", "copy"))
        return self.ctx.sessions.call(
            index, self._subscribe_op(index, variant, plan, []), RULE_WAIT_SECONDS
        )

    def ddl(self, d: Draw) -> Status:
        """Create or drop an index or a materialized view on the table."""
        plan = self._plan(d)
        index = self._session(d)
        with self._lock:
            live = [n for n, o in self.objects.items() if o.state == "live"]
            stale = [n for n, o in self.objects.items() if o.state == "unknown"]
            create = not stale and len(live) <= LIVE_OBJECTS
            if create:
                n = self._next
                self._next += 1
                obj = Obj(d.choice(("INDEX", "MATERIALIZED VIEW")), d.choice(CLUSTERS))
                self.objects[n] = obj
            elif stale or live:
                n = stale[0] if stale else live[0]
                obj = self.objects[n]
            else:
                return Status.NOT_SENT
            before = obj.state
            if not create:
                obj.state = "dropping"
        name = f"{SCHEMA}.{'ix' if obj.kind == 'INDEX' else 'mv'}_{n}"
        drop = f"DROP {obj.kind} {name}"
        if create and obj.kind == "INDEX":
            statement = (
                f"CREATE INDEX ix_{n} IN CLUSTER {obj.cluster} ON {SCHEMA}.t (op)"
            )
        elif create:
            statement = (
                f"CREATE MATERIALIZED VIEW {name} IN CLUSTER {obj.cluster}"
                f" AS SELECT op, i, gen FROM {SCHEMA}.t"
            )
        elif before == "unknown":
            # Whether an unresolved statement created or dropped it is not
            # known, so this drop asserts nothing.
            statement = f"DROP {obj.kind} IF EXISTS {name}"
            plan = Plan("none", 0.0)
        else:
            statement = drop
        attempts: list[float] = []

        def body(conn: psycopg.Connection, canceller: Canceller) -> None:
            canceller.arm()
            conn.execute(statement.encode())

        def execute(conn: psycopg.Connection) -> Result[None]:
            return self._run(index, conn, plan, body, attempts, drop.encode())

        def complete(outcome: Outcome[Result[None]]) -> None:
            error = self._settle(index, plan, attempts, outcome)
            found = outcome.value.follow_up if outcome.ok and outcome.value else None
            was_cancelled = cancelled(error)
            with self._lock:
                if error is None:
                    obj.state = "live" if create else "gone"
                elif was_cancelled and found is not None:
                    # The plain drop removed it, or found nothing to remove.
                    obj.state = "gone"
                elif error.outcome == sql.Outcome.REJECTED:
                    obj.state = "gone" if create else before
                else:
                    obj.state = "unknown"
                if obj.state == "gone":
                    del self.objects[n]
            details = {"statement": statement, "mode": plan.mode, "found": found}
            if was_cancelled and found is not None:
                # After a cancelled CREATE nothing exists, and after a
                # cancelled DROP the object still does.
                self._no_effect(found == ("missing" if create else "dropped"), details)
                reachable("cancel: a DDL statement was answered as cancelled", details)

        return self.ctx.sessions.call(
            index, Op("cancel ddl", execute, complete), RULE_WAIT_SECONDS
        )

    def eventually(self, deadline: float) -> None:
        deadline = max(deadline, time.monotonic() + FINAL_SECONDS)
        sessions = range(len(self.ctx.sessions.sessions))
        wrote: list[bool] = []
        read: list[bool] = []
        ended: list[bool] = []
        while time.monotonic() < deadline and not (wrote and read and ended):
            if not wrote:
                self._final_write(wrote)
            if not read:
                for i in sessions:
                    op = self._read_op(None, i, read)
                    if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                        break
            if not ended:
                for i in sessions:
                    op = self._subscribe_op(i, "copy", Plan("protocol", 1.0), ended)
                    if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                        break
            if not (wrote and read and ended):
                time.sleep(1)
        always(
            bool(wrote),
            "cancel: a write to the table succeeds after faults stop",
            {},
        )
        always(
            bool(read),
            "cancel: a read of the table succeeds after faults stop",
            {},
        )
        always(
            bool(ended),
            "cancel: a cancel ends a subscription after faults stop",
            {},
        )

    def _final_write(self, wrote: list[bool]) -> None:
        batch = Batch()
        with self._lock:
            op = self._next
            self._next += 1
            self.batches[op] = batch
        rows = ", ".join(f"({op}, {i}, 0)" for i in range(ROWS))

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(f"INSERT INTO {SCHEMA}.t VALUES {rows}".encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                if outcome.ok:
                    batch.insert, batch.acked_at = "ok", outcome.end
                elif outcome.rejected:
                    batch.insert = "rejected"
                else:
                    batch.insert = "indeterminate"
            if outcome.ok:
                wrote.append(True)

        for i in range(len(self.ctx.sessions.sessions)):
            op_ = Op("cancel final write", execute, complete, SETTINGS)
            if self.ctx.sessions.call(i, op_, 60.0) != Status.NOT_SENT:
                return
