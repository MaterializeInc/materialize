# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Schema evolution: `ALTER TABLE ... ADD COLUMN` keeps rows and dependents.

`ADD COLUMN` gives a table a new version on the same persist shard. Objects
planned afterwards resolve to the new version, and existing dependents keep
the version they were planned against. Rules add columns to the current
table, insert rows that name a drawn subset of its columns or give a
positional prefix, create dependents, and read.

`schema_model` fixes the value of every cell from the row's key and the
column's name, so a read is judged row by row from the columns it returned.
The insert ledger only decides presence: a row acknowledged before a strict
serializable read began must be there, a row of an insert the system
definitely rejected must not, and an insert without a definite outcome may
go either way. A check reads the table and its dependents in one read
transaction, so at one timestamp, and each dependent must equal the
projection of the table onto the columns of its version. The assertion
messages are those of the legacy schema evolution driver, except the one on
`SELECT *`.

Only one column per table is in doubt at a time: an `ADD COLUMN` without a
definite answer is retried with `IF NOT EXISTS` before the next column is
attempted, so the order of a table's columns is known.

NOTE: the `SELECT *` assertion relies on a read seeing DDL acknowledged
before it began. A fenced environmentd that still answers a read from its
own catalog after its successor acknowledged the `ADD COLUMN` would fail it.
"""

from __future__ import annotations

import dataclasses
import threading
import time
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import psycopg
from antithesis.assertions import (  # pyright: ignore[reportMissingModuleSource]
    always,
    reachable,
    sometimes,
)

from materialize.antithesis import schema_model as model
from materialize.antithesis_driver.client import (
    ABANDON_LINGER_SECONDS,
    ABANDON_SHARE,
    Op,
    Outcome,
    Status,
    abandon,
)
from materialize.antithesis_driver.context import RULE_WAIT_SECONDS, Context, log
from materialize.antithesis_driver.draw import Draw

SCHEMA = "schema_evo"
CLUSTER = "driver"
"""Maintains the dependents, and serves the checks that read them."""
PLAIN_CLUSTER = "quickstart"
MAX_ADDED_COLUMNS = 6
MAX_INSERTS = 150
"""Ledger entries per table. A read is judged against all of them."""
MAX_DEPENDENTS = 4
RETIRE_AFTER_SECONDS = 30.0
"""How long a replaced table waits for its operations in flight before it is
dropped under them."""
ISOLATION = "'strict serializable'"
DEPENDENT_KINDS = ("mv", "view", "filter", "index")

Read = tuple[list[str], list[tuple[Any, ...]]]


def table_name(number: int) -> str:
    return f"{SCHEMA}.t{number}"


def setup_statements() -> list[str]:
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {table_name(0)} {model.BASE_DDL}",
    ]


def columns_ok(
    columns: Sequence[str],
    added: Sequence[tuple[str, float]],
    pending: str | None,
    start: float,
) -> bool:
    """Whether a `SELECT *` that began at `start` may return `columns`.

    `added` holds each acknowledged column with the time of its
    acknowledgement, in the order added, and `pending` the one column whose
    `ADD COLUMN` has no definite answer.
    """
    possible = [*model.BASE_COLUMNS, *(c for c, _ in added)]
    if pending is not None:
        possible.append(pending)
    required = len(model.BASE_COLUMNS) + sum(1 for _, at in added if at < start)
    return (
        required <= len(columns) <= len(possible)
        and list(columns) == possible[: len(columns)]
    )


def overlaps(intervals: Sequence[Sequence[float]], start: float, end: float) -> bool:
    return any(s <= end and e >= start for s, e in intervals)


@dataclass
class Dependent:
    name: str
    statements: tuple[str, ...]
    """Each `CREATE ... IF NOT EXISTS`, so they are sent again until all are
    acknowledged."""
    columns: tuple[str, ...] | None
    """The projection, or None for `SELECT *` over the version it was planned
    against."""
    not_null: str | None
    readable: bool
    """False for an index on the table itself."""
    created: bool = False
    shape: tuple[str, ...] | None = None
    """The columns its first read returned."""


@dataclass
class Table:
    number: int
    added: list[tuple[str, float]] = field(default_factory=list)
    """Each acknowledged column and when its `ADD COLUMN` was acknowledged."""
    pending: str | None = None
    pending_maybe: bool = False
    """An attempt to add `pending` ended without a definite answer."""
    altering: bool = False
    alters: list[list[float]] = field(default_factory=list)
    """When recent `ADD COLUMN` statements were in flight."""
    next_column: int = 1
    inserts: list[model.Insert] = field(default_factory=list)
    dependents: list[Dependent] = field(default_factory=list)
    creating: bool = False
    in_flight: int = 0
    retiring_since: float | None = None

    @property
    def name(self) -> str:
        return table_name(self.number)

    def shape(self) -> list[str]:
        return [*model.BASE_COLUMNS, *(c for c, _ in self.added)]


def select(conn: psycopg.Connection, relation: str) -> Read:
    # psycopg prepares a statement it has run a few times, and the prepared
    # `SELECT *` then fails for good once the table gains a column.
    cursor = conn.execute(f"SELECT * FROM {relation}".encode(), prepare=False)
    assert cursor.description is not None
    return [c.name for c in cursor.description], cursor.fetchall()


class SchemaEvolution:
    def __init__(self, ctx: Context) -> None:
        self.ctx = ctx
        self._lock = threading.Lock()
        self.tables = {0: Table(0)}
        self.current = 0
        self._next_key = 1
        self._rotating = False

    def _session(self, d: Draw) -> int:
        return d.integer(0, len(self.ctx.sessions.sessions) - 1)

    def _call(self, d: Draw, table: Table, op: Op) -> Status:
        """Send `op`, counted as in flight on `table` until it completes."""
        with self._lock:
            table.in_flight += 1
        inner = op.complete

        def complete(outcome: Outcome[Any]) -> None:
            try:
                inner(outcome)
            finally:
                with self._lock:
                    table.in_flight -= 1

        op.complete = complete
        return self.ctx.sessions.call(self._session(d), op, RULE_WAIT_SECONDS)

    def add_column(self, d: Draw) -> Status:
        code = d.choice(sorted(model.COLUMN_TYPES))
        guarded = d.boolean()
        with self._lock:
            table = self.tables[self.current]
            if table.altering:
                return Status.NOT_SENT
            retry = table.pending is not None
            if not retry:
                if len(table.added) >= MAX_ADDED_COLUMNS:
                    return Status.NOT_SENT
                table.pending = model.added_column(table.next_column, code)
                table.pending_maybe = False
                table.next_column += 1
            column = table.pending
            assert column is not None
            table.altering = True
            interval = [time.monotonic(), float("inf")]
            table.alters = [*table.alters[-7:], interval]
        kind = model.COLUMN_TYPES[column.rsplit("_", 1)[1]]
        statement = (
            f"ALTER TABLE {table.name} ADD COLUMN"
            f"{' IF NOT EXISTS' if retry or guarded else ''} {column} {kind}"
        )

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                table.altering = False
                interval[1] = outcome.end
                if outcome.ok:
                    table.added.append((column, outcome.end))
                    table.pending = None
                elif outcome.unknown:
                    table.pending_maybe = True
                elif not table.pending_maybe:
                    table.pending = None
                columns = len(table.added)
            if outcome.ok:
                reachable(
                    "schema evolution: an ADD COLUMN was acknowledged",
                    {"table": table.name, "column": column, "added": columns},
                )

        # Not marked as DDL: environmentd cannot boot after it dies between
        # the catalog commit of an ADD COLUMN and the registration of the new
        # schema with the shard (the panic of PER-59), so a pod deletion aimed at this
        # statement ends the timeline for every other check.
        op = Op("schema evolution add column", execute, complete)
        return self._call(d, table, op)

    def insert(self, d: Draw) -> Status:
        count = d.choice((1, 1, 2, 3))
        positional = d.boolean(0.3)
        prefix = d.integer(1, len(model.BASE_COLUMNS) + MAX_ADDED_COLUMNS)
        picks = [d.boolean(0.6) for _ in range(1 + MAX_ADDED_COLUMNS)]
        linger = d.choice(ABANDON_LINGER_SECONDS) if d.boolean(ABANDON_SHARE) else None
        with self._lock:
            table = self.tables[self.current]
            if len(table.inserts) >= MAX_INSERTS:
                return Status.NOT_SENT
            shape = table.shape()
            keys = tuple(range(self._next_key, self._next_key + count))
            self._next_key += count
            if positional:
                written = tuple(shape[:prefix])
            else:
                named = (c for c, pick in zip(shape[1:], picks) if pick)
                written = (model.KEY, *named)
            index = len(table.inserts)
            table.inserts.append(
                model.Insert(keys, written, "pending", time.monotonic(), None)
            )
        values = ", ".join(
            "(" + ", ".join(model.sql_literal(k, c) for c in written) + ")"
            for k in keys
        )
        target = "" if positional else f" ({', '.join(written)})"
        statement = f"INSERT INTO {table.name}{target} VALUES {values}".encode()

        def execute(conn: psycopg.Connection) -> None:
            if linger is not None:
                abandon(conn, statement, linger)
            conn.execute(statement)

        def complete(outcome: Outcome[None]) -> None:
            result = (
                "ok"
                if outcome.ok
                else "rejected" if outcome.rejected else "indeterminate"
            )
            with self._lock:
                table.inserts[index] = dataclasses.replace(
                    table.inserts[index], outcome=result, complete=outcome.end
                )
                raced = overlaps(table.alters, outcome.start, outcome.end)
                columns = len(table.shape())
            if not outcome.ok:
                return
            details = {"table": table.name, "written": list(written)}
            sometimes(
                raced,
                "schema evolution: an INSERT was acknowledged while an ADD COLUMN on its table was in flight",
                details,
            )
            sometimes(
                positional and len(written) < columns,
                "schema evolution: a positional INSERT with fewer values than the table has columns was acknowledged",
                details,
            )

        op = Op(
            "schema evolution insert",
            execute,
            complete,
            {"transaction_isolation": ISOLATION},
        )
        return self._call(d, table, op)

    def judge(self, path: str, table: Table, read: Read, outcome: Outcome[Any]) -> None:
        """Assert one full read of `table` against its ledger and columns."""
        columns, rows = read
        with self._lock:
            inserts = list(table.inserts)
            added = list(table.added)
            pending = table.pending
            raced = overlaps(table.alters, outcome.start, outcome.end)
        v = model.check_table(columns, rows, inserts, outcome.start, outcome.end)
        details = {
            "path": path,
            "table": table.name,
            "columns": columns,
            "rows": v.rows,
        }
        always(
            v.complete,
            "schema evolution: a read of an evolved table includes every row acknowledged before it began",
            {**details, "missing": v.missing},
        )
        always(
            v.sound,
            "schema evolution: a read of an evolved table holds each row at most once and only rows that were attempted",
            {**details, "unexpected": v.unexpected, "duplicated": v.duplicated},
        )
        always(
            v.values_ok,
            "schema evolution: every row of an evolved table holds its written values and NULL or the default elsewhere",
            {**details, "wrong": v.wrong, "unmodeled_columns": v.unmodeled_columns},
        )
        always(
            columns_ok(columns, added, pending, outcome.start),
            "schema evolution: SELECT * exposes every column whose ADD COLUMN was acknowledged before the read began, in the order added",
            {**details, "added": [c for c, _ in added], "pending": pending},
        )
        matched = v.complete and v.sound and v.values_ok
        sometimes(
            matched and v.with_added_column > 0 and v.rows > v.with_added_column,
            "schema evolution: a read saw rows written both with and without an added column",
            {**details, "with_added": v.with_added_column},
        )
        sometimes(
            matched and raced,
            "schema evolution: a read matched the ledger while an ADD COLUMN on its table was in flight",
            details,
        )

    def read(self, d: Draw) -> Status:
        cluster = d.choice((PLAIN_CLUSTER, CLUSTER))
        with self._lock:
            table = self.tables[self.current]

        def execute(conn: psycopg.Connection) -> Read:
            return select(conn, table.name)

        def complete(outcome: Outcome[Read]) -> None:
            if outcome.ok:
                assert outcome.value is not None
                self.judge(f"read on {cluster}", table, outcome.value, outcome)

        op = Op(
            "schema evolution read",
            execute,
            complete,
            {"cluster": cluster, "transaction_isolation": ISOLATION},
        )
        return self._call(d, table, op)

    def _define(self, table: Table, kind: str, column: str | None) -> Dependent:
        name = f"t{table.number}_d{len(table.dependents)}"
        relation = f"{SCHEMA}.{name}"
        if kind == "index":
            create = (
                f"CREATE INDEX IF NOT EXISTS {name} IN CLUSTER {CLUSTER}"
                f" ON {table.name} ({model.KEY})"
            )
            return Dependent(name, (create,), None, None, readable=False)
        if kind == "filter" and column is not None:
            create = (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS {relation} IN CLUSTER {CLUSTER}"
                f" AS SELECT {model.KEY}, {column} FROM {table.name}"
                f" WHERE {column} IS NOT NULL"
            )
            return Dependent(name, (create,), (model.KEY, column), column, True)
        if kind == "view":
            creates = (
                f"CREATE VIEW IF NOT EXISTS {relation} AS SELECT * FROM {table.name}",
                f"CREATE INDEX IF NOT EXISTS {name}_idx IN CLUSTER {CLUSTER}"
                f" ON {relation} ({model.KEY})",
            )
            return Dependent(name, creates, None, None, readable=True)
        create = (
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {relation} IN CLUSTER {CLUSTER}"
            f" AS SELECT * FROM {table.name}"
        )
        return Dependent(name, (create,), None, None, readable=True)

    def dependent(self, d: Draw) -> Status:
        """Create a dependent of the current table, pinned to its version."""
        kind = d.choice(DEPENDENT_KINDS)
        pick = d.integer(0, MAX_ADDED_COLUMNS - 1)
        with self._lock:
            table = self.tables[self.current]
            if table.creating:
                return Status.NOT_SENT
            dep = next((x for x in table.dependents if not x.created), None)
            if dep is None:
                if len(table.dependents) >= MAX_DEPENDENTS:
                    return Status.NOT_SENT
                column = (
                    table.added[pick % len(table.added)][0] if table.added else None
                )
                dep = self._define(table, kind, column)
                table.dependents.append(dep)
            table.creating = True

        def execute(conn: psycopg.Connection) -> None:
            for statement in dep.statements:
                conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                table.creating = False
                dep.created = dep.created or outcome.ok

        op = Op("schema evolution create dependent", execute, complete, ddl=True)
        return self._call(d, table, op)

    def _check_op(self, table: Table, done: list[bool]) -> Op:
        with self._lock:
            deps = [x for x in table.dependents if x.created and x.readable]

        def execute(conn: psycopg.Connection) -> list[Read]:
            # One read transaction reads everything at one timestamp.
            conn.execute(b"BEGIN")
            reads = [select(conn, table.name)]
            reads += [select(conn, f"{SCHEMA}.{dep.name}") for dep in deps]
            conn.execute(b"COMMIT")
            return reads

        def complete(outcome: Outcome[list[Read]]) -> None:
            if not outcome.ok:
                return
            assert outcome.value is not None
            base, *others = outcome.value
            self.judge("transaction", table, base, outcome)
            for dep, other in zip(deps, others):
                self.compare(table, dep, base, other)
            done.append(True)

        return Op(
            "schema evolution check",
            execute,
            complete,
            {"cluster": CLUSTER, "transaction_isolation": ISOLATION},
        )

    def compare(self, table: Table, dep: Dependent, base: Read, read: Read) -> None:
        """Assert a dependent's read against the table's at the same timestamp."""
        columns, rows = base
        d_columns, d_rows = read
        with self._lock:
            if dep.shape is None:
                dep.shape = tuple(d_columns)
            shape = dep.shape
        expected_columns = dep.columns or shape
        # A `SELECT *` dependent was planned before this read of the table,
        # and columns are only appended.
        pinned = tuple(d_columns) == expected_columns and (
            dep.columns is not None
            or (
                len(d_columns) >= len(model.BASE_COLUMNS)
                and d_columns == columns[: len(d_columns)]
            )
        )
        details = {
            "table": table.name,
            "dependent": dep.name,
            "definition": dep.statements[0],
            "columns": d_columns,
            "table_columns": columns,
        }
        always(
            pinned,
            "schema evolution: a dependent exposes exactly the columns it was defined over",
            {**details, "expected": list(expected_columns)},
        )
        if not pinned:
            return
        expected = model.project(columns, rows, d_columns, dep.not_null)
        observed = Counter(model.normalize(r) for r in d_rows)
        always(
            expected == observed,
            "schema evolution: every dependent of an evolved table agrees with the table at one timestamp",
            {
                **details,
                "rows": sum(expected.values()),
                "missing": [list(r) for r in (expected - observed)][:5],
                "extra": [list(r) for r in (observed - expected)][:5],
            },
        )
        sometimes(
            expected == observed
            and dep.columns is None
            and len(d_columns) < len(columns)
            and sum(expected.values()) > 0,
            "schema evolution: a dependent pinned to an older table version agreed with the table at one timestamp",
            details,
        )

    def check(self, d: Draw) -> Status:
        with self._lock:
            table = self.tables[self.current]
        return self._call(d, table, self._check_op(table, []))

    def rotate(self, d: Draw) -> Status:
        """Start the next table once the current one is full, and drop the
        one before it once its operations are done."""
        now = time.monotonic()
        with self._lock:
            if self._rotating:
                return Status.NOT_SENT
            current = self.tables[self.current]
            retiring = [t for t in self.tables.values() if t.retiring_since is not None]
            widest = len(current.added) >= MAX_ADDED_COLUMNS
            full = len(current.inserts) >= (MAX_INSERTS // 2 if widest else MAX_INSERTS)
            if retiring:
                table = min(retiring, key=lambda t: t.number)
                assert table.retiring_since is not None
                waited = now - table.retiring_since
                if table.in_flight > 0 and waited < RETIRE_AFTER_SECONDS:
                    return Status.NOT_SENT
                statement = f"DROP TABLE IF EXISTS {table.name} CASCADE"
            elif full:
                table = Table(current.number + 1)
                statement = f"CREATE TABLE IF NOT EXISTS {table.name} {model.BASE_DDL}"
            else:
                return Status.NOT_SENT
            self._rotating = True

        def execute(conn: psycopg.Connection) -> None:
            conn.execute(statement.encode())

        def complete(outcome: Outcome[None]) -> None:
            with self._lock:
                self._rotating = False
                if not outcome.ok:
                    return
                if table.retiring_since is not None:
                    self.tables.pop(table.number, None)
                    return
                current.retiring_since = time.monotonic()
                self.tables[table.number] = table
                self.current = table.number
            log(f"schema evolution: table {table.name} started")

        return self.ctx.sessions.call(
            self._session(d),
            Op("schema evolution rotate", execute, complete, ddl=True),
            RULE_WAIT_SECONDS,
        )

    def eventually(self, deadline: float) -> None:
        done: list[bool] = []
        with self._lock:
            table = self.tables[self.current]
        while not done and time.monotonic() < deadline:
            for i in range(len(self.ctx.sessions.sessions)):
                op = self._check_op(table, done)
                if self.ctx.sessions.call(i, op, 60.0) != Status.NOT_SENT:
                    break
            if not done:
                time.sleep(1)
        always(
            bool(done),
            "schema evolution: the current table and its dependents are read and compared after faults stop",
            {"table": table.name},
        )
