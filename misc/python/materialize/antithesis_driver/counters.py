# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Contended counters: read-then-write statements racing on a few rows.

`UPDATE ... SET v = v + 1` reads a row and writes it back. Two increments of
one key that both read the old value would lose one, which the bounds on
every counter read catch: a read must include every increment acknowledged
before it began, and no more than were attempted, and not rejected, before it
ended. `INSERT ... SELECT` copies a counter's value into a log, recording what
one read-then-write read; that value must fall within the bounds of its own
execution, and each log row must appear once.

Counters live in generations of tables, so the model stays bounded: a new
generation starts once the current one has seen `GENERATION_OPS` operations.
The assertion messages are those of the legacy counters driver.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis_driver.client import Op, Outcome, Status
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

SCHEMA = "counters"
GENERATION_OPS = 2000
SETTINGS = {"transaction_isolation": "'strict serializable'", "cluster": "quickstart"}


@dataclass
class Write:
    k: int
    invoke: float
    complete: float | None = None
    outcome: str | None = None
    """`ok`, `rejected`, `unknown`, or None while pending."""


@dataclass
class Generation:
    number: int
    keys: int
    increments: list[Write] = field(default_factory=list)
    copies: dict[str, Write] = field(default_factory=dict)
    """`INSERT ... SELECT` log writes by their id."""
    ops: int = 0

    def counters(self) -> str:
        return f"{SCHEMA}.c_g{self.number}"

    def markers(self) -> str:
        return f"{SCHEMA}.m_g{self.number}"

    def log(self) -> str:
        return f"{SCHEMA}.log_g{self.number}"

    def statements(self) -> list[str]:
        return [
            f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
            f"CREATE TABLE {self.counters()} (k int, v bigint)",
            f"CREATE TABLE {self.markers()} (k int)",
            f"CREATE TABLE {self.log()} (op text, k int, v bigint)",
            f"INSERT INTO {self.counters()} SELECT k, 0 FROM generate_series(0, {self.keys - 1}) AS k",
            f"INSERT INTO {self.markers()} SELECT k FROM generate_series(0, {self.keys - 1}) AS k",
        ]


def bounds(
    writes: list[Write], k: int, invoke: float, complete: float
) -> tuple[int, int, int]:
    """Lower and upper bounds on key `k` for a read from `invoke` to
    `complete`, and how many increments in that window are unresolved."""
    lower = sum(
        1
        for w in writes
        if w.k == k
        and w.outcome == "ok"
        and w.complete is not None
        and w.complete < invoke
    )
    upper = sum(
        1
        for w in writes
        if w.k == k and w.invoke < complete and w.outcome != "rejected"
    )
    unresolved = sum(
        1
        for w in writes
        if w.k == k and w.invoke < complete and w.outcome in (None, "unknown")
    )
    return lower, upper, unresolved


class Counters:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.keys = 1
        self.current: Generation | None = None
        self._creating = False
        self._next_number = 0
        self._next_copy = 0

    def configure(self, d: Draw) -> None:
        """Draw the per-timeline parameters. The first generation is created
        by `rotate`, since its shape is one of them."""
        self.keys = d.choice((1, 2, 3, 5))

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def _resolve(self, w: Write, outcome: Outcome[Any]) -> None:
        with self._lock:
            w.complete = outcome.end
            w.outcome = (
                "ok" if outcome.ok else "rejected" if outcome.rejected else "unknown"
            )

    def increment(self, d: Draw) -> Status:
        g = self.current
        if g is None:
            return Status.NOT_SENT
        k = d.integer(0, g.keys - 1)
        gated = d.boolean()
        with self._lock:
            g.ops += 1
            w = Write(k, time.monotonic())
            g.increments.append(w)
        statement = f"UPDATE {g.counters()} SET v = v + 1 WHERE k = {k}"
        if gated:
            statement += f" AND k IN (SELECT k FROM {g.markers()})"

        def execute(conn: psycopg.Connection) -> int:
            return conn.execute(statement.encode()).rowcount

        def complete(outcome: Outcome[int]) -> None:
            self._resolve(w, outcome)
            if not outcome.ok:
                return
            if gated:
                always(
                    outcome.value == 1,
                    "counters: marker-gated UPDATE increment affects exactly the one seeded row",
                    {"gen": g.number, "k": k, "rowcount": outcome.value},
                )
            else:
                always(
                    outcome.value == 1,
                    "counters: UPDATE increment affects exactly the one seeded row",
                    {"gen": g.number, "k": k, "rowcount": outcome.value},
                )

        return self.ctx.sessions.call(
            self._session(d),
            Op("counters increment", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def copy(self, d: Draw) -> Status:
        g = self.current
        if g is None:
            return Status.NOT_SENT
        k = d.integer(0, g.keys - 1)
        with self._lock:
            g.ops += 1
            self._next_copy += 1
            op = str(uuid.UUID(int=self._next_copy))
            w = Write(k, time.monotonic())
            g.copies[op] = w
        statement = f"INSERT INTO {g.log()} SELECT '{op}', k, v FROM {g.counters()} WHERE k = {k}"

        def execute(conn: psycopg.Connection) -> int:
            return conn.execute(statement.encode()).rowcount

        def complete(outcome: Outcome[int]) -> None:
            self._resolve(w, outcome)
            if outcome.ok:
                always(
                    outcome.value == 1,
                    "counters: INSERT ... SELECT inserts exactly one log row",
                    {"gen": g.number, "k": k, "rowcount": outcome.value},
                )

        return self.ctx.sessions.call(
            self._session(d),
            Op("counters copy", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def read(self, d: Draw) -> Status:
        g = self.current
        if g is None:
            return Status.NOT_SENT
        with self._lock:
            g.ops += 1
        return self.ctx.sessions.call(
            self._session(d), self._read_op(g), RULE_WAIT_SECONDS
        )

    def _read_op(self, g: Generation) -> Op:
        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(f"SELECT k, v FROM {g.counters()}".encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            by_key: dict[int, list[int]] = {}
            for k, v in outcome.value:
                by_key.setdefault(int(k), []).append(int(v))
            always(
                sorted(by_key) == list(range(g.keys))
                and all(len(v) == 1 for v in by_key.values()),
                "counters: the counter table holds exactly one row per seeded key",
                {"gen": g.number, "keys": g.keys, "rows": len(outcome.value)},
            )
            with self._lock:
                writes = list(g.increments)
            for k, values in sorted(by_key.items()):
                v = values[0]
                lower, upper, unresolved = bounds(writes, k, outcome.start, outcome.end)
                details = {
                    "gen": g.number,
                    "k": k,
                    "v": v,
                    "lower": lower,
                    "upper": upper,
                    "unresolved": unresolved,
                }
                always(
                    v >= lower,
                    "counters: counter value includes every increment acknowledged before the read",
                    details,
                )
                always(
                    v <= upper,
                    "counters: counter value includes no increment beyond those attempted before the read completed",
                    details,
                )
                sometimes(
                    any(w.k == k and w.outcome == "unknown" for w in writes),
                    "counters: counter checked while an increment's outcome was indeterminate",
                    details,
                )
                sometimes(
                    unresolved >= 2,
                    "counters: counter checked while several increments of one key were unresolved",
                    details,
                )

        return Op("counters read", execute, complete, SETTINGS)

    def read_log(self, d: Draw) -> Status:
        g = self.current
        if g is None:
            return Status.NOT_SENT
        with self._lock:
            g.ops += 1

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(f"SELECT op, k, v FROM {g.log()}".encode()).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            with self._lock:
                copies = dict(g.copies)
                increments = list(g.increments)
            seen: dict[str, int] = {}
            for op, k, v in outcome.value:
                seen[op] = seen.get(op, 0) + 1
                w = copies.get(op)
                always(
                    w is not None
                    and w.invoke < outcome.end
                    and w.outcome != "rejected"
                    and w.k == int(k),
                    "counters: every log row comes from an INSERT ... SELECT attempted before the read completed",
                    {"gen": g.number, "op": op, "k": k},
                )
                if w is None:
                    continue
                # A copy whose outcome is unknown may have run on the server
                # after the client gave up on it, as late as this read.
                end = (
                    w.complete
                    if w.outcome == "ok" and w.complete is not None
                    else outcome.end
                )
                lower, upper, _ = bounds(increments, w.k, w.invoke, end)
                always(
                    lower <= int(v) <= upper,
                    "counters: INSERT ... SELECT logged a counter value within the bounds of its own execution",
                    {"gen": g.number, "op": op, "v": v, "lower": lower, "upper": upper},
                )
            duplicated = sorted(op for op, n in seen.items() if n > 1)
            always(
                not duplicated,
                "counters: each INSERT ... SELECT is applied at most once",
                {"gen": g.number, "duplicated": duplicated[:10]},
            )
            missing = sorted(
                op
                for op, w in copies.items()
                if w.outcome == "ok"
                and w.complete is not None
                and w.complete < outcome.start
                and op not in seen
            )
            always(
                not missing,
                "counters: acknowledged INSERT ... SELECT rows are visible to later reads",
                {"gen": g.number, "missing": missing[:10]},
            )

        return self.ctx.sessions.call(
            self._session(d),
            Op("counters read log", execute, complete, SETTINGS),
            RULE_WAIT_SECONDS,
        )

    def rotate(self, d: Draw) -> Status:
        with self._lock:
            g = self.current
            if self._creating or (g is not None and g.ops < GENERATION_OPS):
                return Status.NOT_SENT
            self._creating = True
            nxt = Generation(self._next_number, self.keys)

        def execute(conn: psycopg.Connection) -> None:
            for statement in nxt.statements():
                conn.execute(statement.encode())
            # Reads of the old generation still in flight fail as reads of a
            # dropped table, and their outcomes constrain nothing.
            if g is not None:
                for t in (g.counters(), g.markers(), g.log()):
                    conn.execute(f"DROP TABLE IF EXISTS {t}".encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._creating = False
                # Any outcome uses up the number: after an unknown one the
                # tables may exist with a partial seed.
                self._next_number = nxt.number + 1
                if outcome.ok:
                    self.current = nxt
            if outcome.ok:
                log(f"counters: generation {nxt.number} started")

        return self.ctx.sessions.call(
            self._session(d),
            Op("counters rotate", execute, complete, SETTINGS, ddl=True),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        """After faults stop, every counter still satisfies its bounds.

        Once no increment is pending, the bounds of a read are exactly the
        acknowledged increments and those plus the unknown ones.
        """
        g = self.current
        if g is None:
            return
        done: list[bool] = []
        while not done and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                op = self._read_op(g)
                original = op.complete

                def complete(outcome: Outcome[Any], original: Any = original) -> None:
                    original(outcome)
                    if outcome.ok:
                        done.append(True)

                op.complete = complete
                if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                    break
            if not done:
                time.sleep(1)
        always(
            bool(done),
            "counters: a read of every counter succeeds after faults stop",
            {"gen": g.number},
        )
