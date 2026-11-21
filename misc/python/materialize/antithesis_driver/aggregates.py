# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Grouped interval sums under retractions, including an erroring group.

A view sums an interval column per group, maintained as an indexed view and
as a materialized view. An interval sum is exact in each of its components
(months, days, microseconds) and errors once a component leaves its range.
Writes insert small values, NULLs, pairs of large values that cancel, and a
"poison" pair whose sum leaves the range of one component. While a poison is
in the table, a read of the view fails. Deleting it must make the view
readable again with the sums of the rows that remain.

A check reads the base rows and then one of the views in one read
transaction, so both share a timestamp. `aggregate_retractions_model.evaluate`
of those base rows says whether the view must fail and, if not, what it
holds. The expectation follows from the base rows the same transaction
returned, so no write outcome is needed. The assertion messages are those of
the legacy aggregate retractions driver where it had the same check.

Intervals are read as text and parsed by `parse_interval`: the SQL `extract`
of hours wraps at a day, so it cannot decompose a large interval.
"""

from __future__ import annotations

import re
import threading
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    sometimes,
)

from materialize.antithesis import aggregate_retractions_model as model
from materialize.antithesis_driver.client import (
    ABANDON_LINGER_SECONDS,
    ABANDON_SHARE,
    Op,
    Outcome,
    Status,
    abandon,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "agg"
CLUSTER = "driver"
GROUPS = 4
MAX_ROWS = 40
"""The widest span of ids the table may hold, see `Aggregates.write`."""
VIEWS = {"index": f"{SCHEMA}.sums_v", "mv": f"{SCHEMA}.sums_mv"}
VIEW_SQL = (
    "SELECT g, sum(v) AS s, count(v) AS n, count(*) AS total"
    f" FROM {SCHEMA}.t GROUP BY g"
)
BASE_QUERY = f"SELECT id, g, v::text FROM {SCHEMA}.t"
WRITE_KINDS = (
    "anchor",
    "anchor",
    "anchor",
    "null",
    "pair",
    "pair",
    "poison",
    "delete",
    "delete",
    "delete_group",
)
HEAL_SHARE = 0.5
"""How many writes delete the poison while a check has seen one. The rest
keep changing the table under the erroring view."""
# `EvalError::IntervalOutOfRange`, which a read of the view returns while a
# group's sum is out of range.
_OVERFLOW_SQLSTATE = "22015"
_OVERFLOW_TEXT = "interval out of range"
_TIME = re.compile(r"([+-]?)(\d+):(\d\d):(\d\d)(?:\.(\d{1,6}))?")


def large(axis: int) -> int:
    """The magnitude from which a component counts as large: two such values
    of one sign can leave the component's range."""
    return 1 << (model.BITS[axis] - 3)


def setup_statements() -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t (id bigint, g bigint, v interval)",
        f"CREATE VIEW IF NOT EXISTS {VIEWS['index']} AS {VIEW_SQL}",
        f"CREATE INDEX IF NOT EXISTS sums_v_idx IN CLUSTER {CLUSTER}"
        f" ON {VIEWS['index']} (g)",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {VIEWS['mv']}"
        f" IN CLUSTER {CLUSTER} AS {VIEW_SQL}",
    ]


def parse_interval(text: str) -> model.Interval:
    """The components of an interval in its text form, such as
    `-1 years -2 mons +3 days -04:05:06.000007`."""
    months = days = micros = 0
    tokens = text.split(" ")
    while tokens:
        token = tokens.pop(0)
        time_of_day = _TIME.fullmatch(token)
        if time_of_day is not None:
            sign, hours, minutes, seconds, fraction = time_of_day.groups()
            micros = ((int(hours) * 60 + int(minutes)) * 60 + int(seconds)) * 1_000_000
            micros += int((fraction or "").ljust(6, "0"))
            if sign == "-":
                micros = -micros
            continue
        unit = tokens.pop(0).removesuffix("s") if tokens else ""
        if unit == "year":
            months += 12 * int(token)
        elif unit == "mon":
            months += int(token)
        elif unit == "day":
            days += int(token)
        else:
            raise ValueError(f"unsupported interval text: {text!r}")
    return months, days, micros


def literal(value: model.Interval | None) -> str:
    if value is None:
        return "NULL"
    return f"'{value[0]} months {value[1]} days {value[2]} microseconds'::interval"


def values(kind: str, d: Draw) -> list[model.Input]:
    """The rows one insert of `kind` adds, all to one group."""
    group = d.integer(0, GROUPS - 1)
    if kind == "null":
        return [(group, None)]
    if kind == "anchor":
        anchor = (d.integer(-3, 3), d.integer(-3, 3), d.integer(-1000, 1000))
        return [(group, anchor)]
    if kind == "pair":
        parts = [
            d.choice((1, 10, (1 << (bits - 2)) + d.integer(1, 1000)))
            for bits in model.BITS
        ]
        return [
            (group, (parts[0], parts[1], parts[2])),
            (group, (-parts[0], -parts[1], -parts[2])),
        ]
    assert kind == "poison"
    # Each value alone is in range, and the sum of the two is not.
    axis = d.integer(0, 2)
    bits = model.BITS[axis]
    magnitude = d.integer(3 * large(axis), (1 << (bits - 1)) - 1000)
    value = [0, 0, 0]
    value[axis] = d.choice((-1, 1)) * magnitude
    poison = (group, (value[0], value[1], value[2]))
    return [poison, poison]


@dataclass(frozen=True)
class Read:
    base: Sequence[tuple[int, int, str | None]]
    """`(id, g, v::text)` of every base row."""
    rows: Sequence[tuple[Any, ...]] | None
    """`(g, s::text, n, total)` of the view, or None if its read reported an
    overflow."""


class Aggregates:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self._next_id = 1
        self._trimmed = 0
        """Every id up to this one has been deleted, or is about to be."""
        self.ids: list[int] = []
        """The ids the last check saw."""
        self.heal: list[int] = []
        """The ids of the large values in the groups the last check found out
        of range. Deleting them leaves only values too small to overflow."""
        self._errored: set[str] = set()
        """Views that reported an overflow and were not compared since."""
        self.serializable_share = 0.0

    def configure(self, d: Draw) -> None:
        self.serializable_share = d.choice((0.0, 0.3))

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def _send(self, name: str, statement: str, linger: float | None) -> Op:
        def execute(conn: psycopg.Connection) -> None:
            if linger is not None:
                abandon(conn, statement.encode(), linger)
            conn.execute(statement.encode())

        return Op(
            name,
            execute,
            lambda outcome: None,
            {"cluster": CLUSTER, "transaction_isolation": "'strict serializable'"},
        )

    def _delete(self, ids: list[int]) -> str:
        return f"DELETE FROM {SCHEMA}.t WHERE id IN ({', '.join(map(str, ids))})"

    def write(self, d: Draw) -> Status:
        with self._lock:
            ids, heal = list(self.ids), list(self.heal)
            # Ids only grow, so deleting below a bound caps the table whether
            # or not checks run and whichever earlier deletes applied.
            trim = self._next_id - 1 - self._trimmed > MAX_ROWS
            if trim:
                self._trimmed = self._next_id - 1 - MAX_ROWS // 2
            bound = self._trimmed
        if trim:
            kind, statement = "trim", f"DELETE FROM {SCHEMA}.t WHERE id <= {bound}"
        elif heal and d.boolean(HEAL_SHARE):
            kind, statement = "heal", self._delete(heal)
        else:
            kind = d.choice(WRITE_KINDS)
            if kind == "poison" and heal:
                # One erroring group at a time: the view is checked for
                # values only while no group is out of range.
                kind = "anchor"
            if kind == "delete" and not ids:
                kind = "anchor"
            if kind == "delete":
                statement = self._delete([d.choice(ids)])
            elif kind == "delete_group":
                statement = (
                    f"DELETE FROM {SCHEMA}.t WHERE g = {d.integer(0, GROUPS - 1)}"
                )
            else:
                rows = values(kind, d)
                with self._lock:
                    first = self._next_id
                    self._next_id += len(rows)
                statement = f"INSERT INTO {SCHEMA}.t VALUES " + ", ".join(
                    f"({first + i}, {group}, {literal(value)})"
                    for i, (group, value) in enumerate(rows)
                )
        linger = d.choice(ABANDON_LINGER_SECONDS) if d.boolean(ABANDON_SHARE) else None
        return self.ctx.sessions.call(
            self._session(d),
            self._send(f"aggregates {kind}", statement, linger),
            RULE_WAIT_SECONDS,
        )

    def _read_op(self, path: str, isolation: str, compared: list[bool]) -> Op:
        view_query = f"SELECT g, s::text, n, total FROM {VIEWS[path]}"

        def execute(conn: psycopg.Connection) -> Read:
            # One transaction: both reads run at its timestamp.
            conn.execute(b"BEGIN")
            base = conn.execute(BASE_QUERY.encode()).fetchall()
            try:
                rows = conn.execute(view_query.encode()).fetchall()
            except psycopg.Error as e:
                if e.sqlstate != _OVERFLOW_SQLSTATE or _OVERFLOW_TEXT not in str(e):
                    raise
                conn.rollback()
                return Read(base, None)
            conn.execute(b"COMMIT")
            return Read(base, rows)

        def complete(outcome: Outcome[Read]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            if self.judge(path, outcome.value):
                compared.append(True)

        return Op(
            "aggregates check",
            execute,
            complete,
            {"cluster": CLUSTER, "transaction_isolation": f"'{isolation}'"},
        )

    def check(self, d: Draw) -> Status:
        isolation = (
            "serializable"
            if d.boolean(self.serializable_share)
            else "strict serializable"
        )
        return self.ctx.sessions.call(
            self._session(d),
            self._read_op(d.choice(tuple(VIEWS)), isolation, []),
            RULE_WAIT_SECONDS,
        )

    def judge(self, path: str, read: Read) -> bool:
        """Judge one read of the base rows and a view. Returns whether the
        view's rows were compared."""
        base = [
            (key, group, None if text is None else parse_interval(text))
            for key, group, text in read.base
        ]
        expected = model.evaluate((group, value) for _, group, value in base)
        out_of_range = {group for group, _ in expected.overflows}
        heal = [
            key
            for key, group, value in base
            if group in out_of_range
            and value is not None
            and any(abs(part) >= large(axis) for axis, part in enumerate(value))
        ]
        with self._lock:
            self.ids = [key for key, _, _ in base]
            self.heal = heal
            if read.rows is None:
                self._errored.add(path)
            repaired = read.rows is not None and path in self._errored
            if read.rows is not None:
                self._errored.discard(path)
        details: dict[str, Any] = {
            "path": path,
            "rows": len(base),
            "overflow": expected.overflows,
        }

        if read.rows is None:
            always(
                bool(expected.overflows),
                "aggregate retractions: a view reports overflow only while a grouped interval sum is outside its component range",
                details,
            )
            sometimes(
                bool(expected.overflows),
                "aggregate retractions: a view reported the overflow of a modeled group",
                details,
            )
            return False
        always(
            not expected.overflows,
            "aggregate retractions: a grouped interval sum outside its component range reports overflow",
            {**details, "actual": list(read.rows[:8])},
        )
        if expected.overflows:
            return False
        observed: Counter[tuple[Any, ...]] = Counter(
            (group, *((None,) * 3 if s is None else parse_interval(s)), n, total)
            for group, s, n, total in read.rows
        )
        wanted: Counter[tuple[Any, ...]] = Counter(expected.rows)
        always(
            observed == wanted,
            "aggregate retractions: representable grouped interval sums equal the independent component model",
            {
                **details,
                "missing": list((wanted - observed).items())[:4],
                "unexpected": list((observed - wanted).items())[:4],
            },
        )
        sometimes(
            repaired and bool(wanted),
            "aggregate retractions: a modeled overflow was retracted and a nonempty maintained result verified",
            details,
        )
        sometimes(
            any(
                abs(part) >= large(axis)
                for _, _, value in base
                if value is not None
                for axis, part in enumerate(value)
            ),
            "aggregate retractions: a check compared sums over large values that cancel",
            details,
        )
        sometimes(
            any(row[1] is None and row[5] > 0 for row in wanted),
            "aggregate retractions: a check compared a group that holds only NULLs",
            details,
        )
        return True

    def _call(self, op: Op) -> None:
        for i in range(len(self.ctx.sessions.sessions)):
            if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                return

    def eventually(self, deadline: float) -> None:
        """Delete what keeps the views erroring, then compare each once more."""
        compared: dict[str, list[bool]] = {path: [] for path in VIEWS}
        while not all(compared.values()) and time.monotonic() < deadline:
            with self._lock:
                heal = list(self.heal)
            if heal:
                self._call(self._send("aggregates heal", self._delete(heal), None))
            for path, done in compared.items():
                if not done:
                    self._call(self._read_op(path, "strict serializable", done))
            if not all(compared.values()):
                time.sleep(1)
        always(
            all(compared.values()),
            "aggregate retractions: every view can be compared with a current reference after faults stop",
            {path: bool(done) for path, done in compared.items()},
        )
