# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Closed timestamps never change: a read `AS OF` a past timestamp returns
what a read at that timestamp returned before.

`observe` reads the table together with `mz_now()` and records its rows at
that logical timestamp. `reread` reads the table, an indexed view or a
materialized view over it `AS OF` a recorded timestamp, on any replica of the
`driver` cluster, and `subscribe` replays the updates between two recorded
timestamps. Each result must equal the record, or for the views their
definition applied to it. The model is built from reads only, so a write
whose outcome is unknown does not matter.

Every object retains `RETAIN_SECONDS` of history. Records are kept a minute
past that, so rereads also land near the compaction frontier, where a refusal
is expected and a wrong result is not. A refusal within half the window is
reported too, by the system's own clock: a read that is refused asks for
`mz_now()` on the same session.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    always_or_unreachable,
    sometimes,
)

from materialize.antithesis import sql
from materialize.antithesis_driver.client import Op, Outcome, Status, classify
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context
from materialize.antithesis_driver.draw import Draw

SCHEMA = "asof"
CLUSTER = "driver"
"""Holds the index and the materialized view. Its replicas are `r1` and,
while the driver raises its replication factor, `r2`."""
WRITE_CLUSTER = "quickstart"
"""For the reads inside writes and some observes, so they do not depend on
the replicas of `CLUSTER`."""
RETAIN_SECONDS = 600
READABLE_MS = RETAIN_SECONDS * 1000 // 2
"""A record younger than this must still be readable."""
EXPIRE_MS = RETAIN_SECONDS * 1000 + 60_000
OLD_MS = 120_000
MAX_RECORDS = 30
MAX_KEY = 199
GROUPS = 4
OBJECTS = ("t", "v", "mv")
QUERIES = {
    "t": f"SELECT k, v FROM {SCHEMA}.t",
    "v": f"SELECT k, w FROM {SCHEMA}.v",
    "mv": f"SELECT g, n, s FROM {SCHEMA}.mv",
}
TARGETS: tuple[str | None, ...] = (None, "r1", "r2")
ISOLATIONS = ("'strict serializable'", "serializable")

Rows = dict[tuple[int, ...], int]
"""A multiset of rows: each row with its multiplicity, never zero."""


def setup_statements() -> list[str]:
    retain = f"RETAIN HISTORY = FOR '{RETAIN_SECONDS}s'"
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.t (k bigint, v bigint) WITH ({retain})",
        f"CREATE VIEW IF NOT EXISTS {SCHEMA}.v AS SELECT k, v * 3 - k AS w FROM {SCHEMA}.t",
        f"CREATE DEFAULT INDEX IF NOT EXISTS IN CLUSTER {CLUSTER} ON {SCHEMA}.v",
        # `CREATE INDEX ... WITH (RETAIN HISTORY ...)` needs the
        # `enable_index_options` feature flag. The `ALTER` does not, and it
        # writes the option into the index's `create_sql`.
        f"ALTER INDEX {SCHEMA}.v_primary_idx SET ({retain})",
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {SCHEMA}.mv IN CLUSTER {CLUSTER}"
        f" WITH ({retain}) AS SELECT k % {GROUPS} AS g, count(*) AS n,"
        f" sum(v)::bigint AS s FROM {SCHEMA}.t GROUP BY 1",
    ]


def count(rows: Iterable[tuple[int, ...]], diff: int = 1) -> Rows:
    out: Rows = {}
    for row in rows:
        add(out, row, diff)
    return out


def add(rows: Rows, row: tuple[int, ...], diff: int) -> None:
    n = rows.get(row, 0) + diff
    if n:
        rows[row] = n
    else:
        rows.pop(row, None)


def derive(obj: str, table: Rows) -> Rows:
    """What `QUERIES[obj]` returns when the table holds `table`."""
    if obj == "t":
        return dict(table)
    if obj == "v":
        out: Rows = {}
        for (k, v), n in table.items():
            add(out, (k, v * 3 - k), n)
        return out
    groups: dict[int, tuple[int, int]] = {}
    for (k, v), n in table.items():
        c, s = groups.get(k % GROUPS, (0, 0))
        groups[k % GROUPS] = (c + n, s + v * n)
    return {(g, c, s): 1 for g, (c, s) in groups.items() if c}


def diff(expected: Rows, actual: Rows) -> dict[str, Any]:
    missing = {r: n - actual.get(r, 0) for r, n in expected.items()}
    extra = {r: n - expected.get(r, 0) for r, n in actual.items()}
    return {
        "missing": [[*r, n] for r, n in sorted(missing.items()) if n > 0][:10],
        "extra": [[*r, n] for r, n in sorted(extra.items()) if n > 0][:10],
        "expected_rows": sum(expected.values()),
        "actual_rows": sum(actual.values()),
    }


def classify_read(error: BaseException) -> sql.Classified:
    """`client.classify`, widened by `sql.classify_as_of_read`."""
    c = classify(error)
    return sql.classify_as_of_read(error) if c.outcome == sql.Outcome.VIOLATION else c


@dataclass(frozen=True)
class Refused:
    """A read refused because its timestamp is below a since."""

    now: int
    """The system's `mz_now()` right after the refusal."""


def refused(conn: psycopg.Connection, error: psycopg.Error) -> Refused:
    """What a refused read returns in place of rows. Raises `error` again if
    it is not a refusal. `classify_read` rejects no other error with this
    SQLSTATE."""
    c = classify_read(error)
    if c.outcome != sql.Outcome.REJECTED or c.sqlstate != "22000":
        raise error
    row = conn.execute(b"SELECT mz_now()::text::bigint").fetchone()
    assert row is not None
    return Refused(int(row[0]))


class AsOf:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.records: dict[int, Rows] = {}
        """The table's rows at each recorded logical timestamp."""
        self.keys_per_write = 4

    def configure(self, d: Draw) -> None:
        self.keys_per_write = d.choice((1, 4, 16))

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def _age_ms(self, ts: int) -> int:
        """How far the system's clock has moved past `ts`, never less.

        Compaction follows the write frontier, which tracks the wall clock
        unless the timestamp oracle ran ahead of it, and then the newest
        record shows the lead. Call with the lock held.
        """
        newest = max(self.records, default=ts)
        return max(int(time.time() * 1000) - ts, newest - ts)

    def _prune(self) -> None:
        """Drop expired records, then thin them out to `MAX_RECORDS`.
        Call with the lock held."""
        for ts in [ts for ts in self.records if self._age_ms(ts) > EXPIRE_MS]:
            del self.records[ts]
        while len(self.records) > MAX_RECORDS:
            # Dropping the record whose neighbours are closest keeps the
            # oldest records, so rereads reach every age up to expiry.
            times = sorted(self.records)
            i = min(range(1, len(times) - 1), key=lambda i: times[i + 1] - times[i - 1])
            del self.records[times[i]]

    def write(self, d: Draw) -> Status:
        keys = sorted({d.integer(0, MAX_KEY) for _ in range(self.keys_per_write)})
        listed = ", ".join(str(k) for k in keys)
        kind = d.choice(("insert", "update", "delete"))
        if kind == "insert":
            values = ", ".join(f"({k}, {d.integer(-1000, 1000)})" for k in keys)
            statement = (
                f"INSERT INTO {SCHEMA}.t SELECT x.k, x.v FROM (VALUES {values})"
                f" AS x (k, v) WHERE x.k NOT IN (SELECT k FROM {SCHEMA}.t)"
            )
        elif kind == "update":
            statement = f"UPDATE {SCHEMA}.t SET v = {d.integer(-1000, 1000)} WHERE k IN ({listed})"
        else:
            statement = f"DELETE FROM {SCHEMA}.t WHERE k IN ({listed})"

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        return self.ctx.sessions.call(
            self._session(d),
            Op("as of write", execute, lambda _: None, {"cluster": WRITE_CLUSTER}),
            RULE_WAIT_SECONDS,
        )

    def _observe_op(self, settings: dict[str, str], done: list[bool]) -> Op:
        # Joining to one row yields the timestamp also for an empty table.
        query = (
            f"SELECT mz_now()::text::bigint, t.k, t.v FROM (SELECT 1) AS one"
            f" LEFT JOIN {SCHEMA}.t AS t ON true"
        ).encode()

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]]:
            return conn.execute(query).fetchall()

        def complete(outcome: Outcome[list[tuple[Any, ...]]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value
            ts = int(outcome.value[0][0])
            table = count(
                (int(k), int(v)) for _, k, v in outcome.value if k is not None
            )
            with self._lock:
                earlier = self.records.setdefault(ts, table)
                self._prune()
            if earlier is not table:
                self._same_table(
                    earlier == table,
                    {"ts": ts, "path": "observe", **diff(earlier, table)},
                )
            done.append(True)

        return Op("as of observe", execute, complete, settings)

    def observe(self, d: Draw) -> Status:
        settings = {
            "cluster": d.choice((CLUSTER, WRITE_CLUSTER)),
            "transaction_isolation": d.choice(ISOLATIONS),
        }
        return self.ctx.sessions.call(
            self._session(d), self._observe_op(settings, []), RULE_WAIT_SECONDS
        )

    @staticmethod
    def _settings(replica: str | None) -> dict[str, str]:
        if replica is None:
            return {"cluster": CLUSTER}
        return {"cluster": CLUSTER, "cluster_replica": replica}

    def _same_table(self, holds: bool, details: dict[str, Any]) -> None:
        always(
            holds,
            "as of: a SELECT at a recorded timestamp returns the recorded table",
            details,
        )

    def _refused(self, ts: int, now: int, details: dict[str, Any]) -> None:
        """Drop the record of `ts`, which a read found compacted at `now`."""
        with self._lock:
            self.records.pop(ts, None)
        always_or_unreachable(
            now - ts > READABLE_MS,
            "as of: a timestamp recorded within half the retain window stays readable",
            {**details, "age_ms": now - ts},
        )

    def _reread_op(
        self, ts: int, table: Rows, obj: str, replica: str | None, done: list[bool]
    ) -> Op:
        statement = f"{QUERIES[obj]} AS OF {ts}".encode()
        details = {"ts": ts, "object": obj, "replica": replica}

        def execute(conn: psycopg.Connection) -> list[tuple[Any, ...]] | Refused:
            try:
                return conn.execute(statement).fetchall()
            except psycopg.Error as e:
                return refused(conn, e)

        def complete(outcome: Outcome[list[tuple[Any, ...]] | Refused]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            if isinstance(outcome.value, Refused):
                self._refused(ts, outcome.value.now, details)
                sometimes(
                    True,
                    "as of: a reread was refused because its timestamp was compacted",
                    details,
                )
                done.append(True)
                return
            actual = count(tuple(int(x) for x in row) for row in outcome.value)
            expected = derive(obj, table)
            with self._lock:
                age = self._age_ms(ts)
            judged = {**details, "age_ms": age, **diff(expected, actual)}
            if obj == "t":
                self._same_table(actual == expected, judged)
            else:
                always(
                    actual == expected,
                    "as of: the indexed view and the materialized view at a recorded timestamp equal their definitions applied to the recorded table",
                    judged,
                )
            sometimes(replica == "r2", "as of: a reread ran on replica r2", details)
            sometimes(
                age > OLD_MS,
                "as of: a reread succeeded at a timestamp recorded over 2 minutes earlier",
                {**details, "age_ms": age},
            )
            done.append(True)

        return Op(
            "as of reread", execute, complete, self._settings(replica), classify_read
        )

    def reread(self, d: Draw) -> Status:
        with self._lock:
            self._prune()
            if not self.records:
                return Status.NOT_SENT
            ts = d.choice(sorted(self.records))
            table = self.records[ts]
        op = self._reread_op(ts, table, d.choice(OBJECTS), d.choice(TARGETS), [])
        return self.ctx.sessions.call(self._session(d), op, RULE_WAIT_SECONDS)

    def _subscribe_op(
        self, between: dict[int, Rows], obj: str, replica: str | None, done: list[bool]
    ) -> Op:
        """`SUBSCRIBE` from the first to the last of `between`, checked at each."""
        a, b = min(between), max(between)
        # `UP TO` is exclusive.
        statement = (
            f"COPY (SUBSCRIBE ({QUERIES[obj]}) AS OF {a} UP TO {b + 1}) TO STDOUT"
        ).encode()
        details = {"as_of": a, "up_to": b + 1, "object": obj, "replica": replica}

        def execute(conn: psycopg.Connection) -> list[tuple[int, ...]] | Refused:
            try:
                with conn.cursor().copy(statement) as copy:
                    return [tuple(int(x) for x in row) for row in copy.rows()]
            except psycopg.Error as e:
                return refused(conn, e)

        def complete(outcome: Outcome[list[tuple[int, ...]] | Refused]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            if isinstance(outcome.value, Refused):
                self._refused(a, outcome.value.now, details)
                done.append(True)
                return
            rows = sorted(outcome.value)
            outside = [list(r) for r in rows if not a <= r[0] <= b]
            state: Rows = {}
            snapshot: dict[str, Any] = {}
            wrong: list[dict[str, Any]] = []
            i = 0
            for ts in sorted(between):
                while i < len(rows) and rows[i][0] <= ts:
                    add(state, rows[i][2:], rows[i][1])
                    i += 1
                expected = derive(obj, between[ts])
                if ts == a:
                    snapshot = {} if state == expected else diff(expected, state)
                elif state != expected:
                    wrong.append({"ts": ts, **diff(expected, state)})
            always(
                not snapshot,
                "as of: a SUBSCRIBE snapshot at a recorded timestamp equals the record",
                {**details, **snapshot},
            )
            always(
                not wrong and not outside,
                "as of: a SUBSCRIBE's snapshot plus updates equals the record at every later recorded timestamp up to its end",
                {**details, "wrong": wrong[:3], "outside": outside[:10]},
            )
            sometimes(
                any(a < r[0] for r in rows),
                "as of: a SUBSCRIBE saw an update between its two recorded timestamps",
                {**details, "rows": len(rows)},
            )
            done.append(True)

        return Op(
            "as of subscribe", execute, complete, self._settings(replica), classify_read
        )

    def subscribe(self, d: Draw) -> Status:
        with self._lock:
            self._prune()
            times = sorted(self.records)
            if len(times) < 2:
                return Status.NOT_SENT
            i = d.integer(0, len(times) - 2)
            j = d.integer(i + 1, len(times) - 1)
            between = {ts: self.records[ts] for ts in times[i : j + 1]}
        op = self._subscribe_op(between, d.choice(OBJECTS), d.choice(TARGETS), [])
        return self.ctx.sessions.call(self._session(d), op, RULE_WAIT_SECONDS)

    def _settle(self, make: Callable[[list[bool]], Op], deadline: float) -> bool:
        """Run ops from `make` until one marks itself done or `deadline`
        passes. Tries once even past `deadline`, which earlier checks share
        and may have used up."""
        done: list[bool] = []
        while True:
            for i in range(len(self.ctx.sessions.sessions)):
                if self.ctx.sessions.call(i, make(done), 70.0) != Status.NOT_SENT:
                    break
            if done or time.monotonic() >= deadline:
                return bool(done)
            time.sleep(1)

    def eventually(self, deadline: float) -> None:
        """One observe, then a reread of every recent record from each object
        and a subscribe across them. A reread refused for compaction counts
        as settled, `_refused` judges it."""
        settings = {"cluster": CLUSTER}
        observed = self._settle(lambda done: self._observe_op(settings, done), deadline)
        with self._lock:
            self._prune()
            recent = {
                ts: rows
                for ts, rows in self.records.items()
                if self._age_ms(ts) <= READABLE_MS
            }
        unsettled: list[str] = []
        for obj in OBJECTS:
            for ts, table in sorted(recent.items()):

                def reread(
                    done: list[bool], ts: int = ts, table: Rows = table, obj: str = obj
                ) -> Op:
                    return self._reread_op(ts, table, obj, None, done)

                if not self._settle(reread, deadline):
                    unsettled.append(f"reread {obj} {ts}")

            def subscribe(done: list[bool], obj: str = obj) -> Op:
                return self._subscribe_op(recent, obj, None, done)

            if recent and not self._settle(subscribe, deadline):
                unsettled.append(f"subscribe {obj}")
        always(
            observed and not unsettled,
            "as of: an observe and a reread of every recent recorded timestamp succeed after faults stop",
            {"observed": observed, "recent": len(recent), "unsettled": unsettled[:10]},
        )
